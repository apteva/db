package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// AggregateOptions controls bounded execution. Disk use is opt-in. TempDir must
// be on a filesystem with adequate space; MaxDiskBytes includes merge output
// while its inputs still exist. Zero selects a 1-GiB quota per execution.
// Results without an explicit sort have unspecified group order.
type AggregateOptions struct {
	MaxMemoryBytes int64
	AllowDiskUse   bool
	TempDir        string
	MaxDiskBytes   int64
	Hint           string
	Stats          *AggregateStats
}

// AggregateStats describes this execution, excluding caller-retained results,
// snapshots and the engine page cache. It is updated before the call returns.
type AggregateStats struct {
	Strategy, Index                              string
	InputRows, Groups, Returned                  int64
	PeakMemoryBytes, SpilledBytes, PeakDiskBytes int64
	SpillRuns, MergePasses                       int
	SpillPartitions, MaxPartitionDepth           int
}

var ErrDiskLimit = errors.New("aptevadb: aggregation temporary disk quota exceeded")
var errAggregateStopped = errors.New("aptevadb: aggregate consumer stopped")

type rowSource func(func(map[string]any) error) error

type aggregateExec struct {
	groupVisitor func(GroupView) error
	stateLimit   int64
	ctx          context.Context
	options      AggregateOptions
	stats        *AggregateStats
	budget       *memoryBudget
	limits       Limits
	disk         *aggregateDisk
}

// AggregateEach streams completed result rows from one snapshot. A callback
// may keep its map; retained caller allocations are outside the execution budget.
// False stops execution and releases temporary files and the owned snapshot.
func (c *Collection) AggregateEach(ctx context.Context, pipeline []byte, opts AggregateOptions, yield func(map[string]any) bool) error {
	p, err := ParsePipeline(pipeline)
	if err != nil {
		return err
	}
	return c.RunEach(ctx, p, opts, yield)
}
func (db *DB) AggregateEach(ctx context.Context, pipeline []byte, opts AggregateOptions, yield func(map[string]any) bool) error {
	return db.def.AggregateEach(ctx, pipeline, opts, yield)
}
func (s *Snapshot) AggregateEach(ctx context.Context, pipeline []byte, opts AggregateOptions, yield func(map[string]any) bool) error {
	return s.def().AggregateEach(ctx, pipeline, opts, yield)
}

// RunEach executes a compiled pipeline with bounded blocking operators. The
// existing Run/Aggregate methods continue to collect all results in memory.
func (c *Collection) RunEach(ctx context.Context, p *Pipeline, opts AggregateOptions, yield func(map[string]any) bool) error {
	return c.runEach(ctx, p, opts, yield, false)
}
func (c *Collection) runEach(ctx context.Context, p *Pipeline, opts AggregateOptions, yield func(map[string]any) bool, collect bool, visitors ...func(GroupView) error) (retErr error) {
	ctx = orBackground(ctx)
	if yield == nil || p == nil {
		return ErrPipeline
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return err
		}
		defer s.Close()
		return cc.runEach(ctx, p, opts, yield, collect, visitors...)
	}
	if err := c.check(); err != nil {
		return err
	}
	if opts.TempDir == "" {
		opts.TempDir = filepath.Join(c.db.dir, "aggregate-tmp")
	}
	x, err := newAggregateExecution(ctx, p, opts, c.db.memory)
	if err != nil {
		return err
	}
	if len(visitors) > 0 {
		x.groupVisitor = visitors[0]
	}
	defer func() { retErr = errors.Join(retErr, x.finish()) }()
	src, q, ordered, index, err := c.aggregateSource(ctx, p, opts)
	if err != nil {
		return err
	}
	if ordered {
		x.stats.Strategy, x.stats.Index = "ordered-index", index
	}
	e := &env{ctx: ctx, c: c, budget: x.budget, count: -1}
	spec := srcSpec{src: src}
	if !ordered {
		pl := q.plan()
		if ip, ok := c.indexPlanBudget(pl.preds, x.budget.max); ok {
			if err := x.budget.reserve(ip.memory); err != nil {
				return err
			}
			spec.src = c.rangesSource(ctx, ip)
			e.count = ip.count
			x.stats.Index = ip.name
		} else if pl.group != nil && !opts.AllowDiskUse {
			spec = srcSpec{c: c, ranged: true}
		}
	}
	return x.execute(q, spec, e, ordered, yield, collect)
}

// RunEach also supports caller-supplied sources. Collection.RunEach is required
// for index planning, native snapshot ownership and $lookup resolution.
func (p *Pipeline) RunEach(ctx context.Context, src Source, opts AggregateOptions, yield func(map[string]any) bool) error {
	return p.runEachSource(ctx, src, opts, yield, false)
}
func (p *Pipeline) runEachSource(ctx context.Context, src Source, opts AggregateOptions, yield func(map[string]any) bool, collect bool, visitors ...func(GroupView) error) (retErr error) {
	ctx = orBackground(ctx)
	if src == nil || yield == nil || p == nil {
		return ErrPipeline
	}
	if opts.Hint != "" {
		return ErrIndexOrder
	}
	x, err := newAggregateExecution(ctx, p, opts, nil)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, x.finish()) }()
	if len(visitors) > 0 {
		x.groupVisitor = visitors[0]
	}
	return x.execute(p, srcSpec{src: src}, &env{ctx: ctx, budget: x.budget, count: -1}, false, yield, collect)
}
func newAggregateExecution(ctx context.Context, p *Pipeline, opts AggregateOptions, owner *memoryTracker) (*aggregateExec, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.MaxMemoryBytes < 0 || opts.MaxDiskBytes < 0 {
		return nil, ErrPipeline
	}
	limits := p.limits
	if opts.MaxMemoryBytes != 0 {
		limits.MaxBytes = opts.MaxMemoryBytes
	}
	b, err := limits.budget()
	if err != nil {
		return nil, err
	}
	if owner != nil && owner.limit > 0 {
		available := owner.limit - owner.total()
		if available <= 0 {
			return nil, ErrMemoryPressure
		}
		b.max = min(b.max, available)
	}
	b.trackPeak = true
	b.owner = owner
	if owner != nil {
		owner.track(b)
	}
	stats := &AggregateStats{Strategy: "hash"}
	if opts.Stats != nil {
		*opts.Stats = *stats
		stats = opts.Stats
	}
	x := &aggregateExec{ctx: ctx, options: opts, stats: stats, budget: b, limits: limits}
	x.disk = &aggregateDisk{ctx: ctx, options: opts, stats: stats, budget: b}
	return x, nil
}
func (x *aggregateExec) finish() error {
	x.stats.PeakMemoryBytes = x.budget.peak
	err := x.disk.close()
	x.budget.close()
	return err
}
func (x *aggregateExec) execute(p *Pipeline, spec srcSpec, e *env, ordered bool, yield func(map[string]any) bool, collect bool) error {
	blocking := 0
	for _, st := range p.stages {
		if st.kind == stGroup || st.kind == stSort {
			blocking++
		}
	}
	blocking = max(blocking, 1)
	x.stateLimit = x.budget.max / int64(2*blocking)
	x.disk.stateLimit = x.stateLimit
	err := x.pipeline(p, spec, e, ordered)(func(row map[string]any) error {
		if err := x.ctx.Err(); err != nil {
			return err
		}
		if collect {
			if x.stats.Returned >= int64(x.limits.MaxRows) {
				return ErrLimit
			}
			if err := x.budget.reserve(valueMemory(row)); err != nil {
				return err
			}
		}
		x.stats.Returned++
		if !yield(row) {
			return errAggregateStopped
		}
		return nil
	})
	if errors.Is(err, errAggregateStopped) {
		return nil
	}
	if err == nil {
		return x.ctx.Err()
	}
	return err
}

// aggregateSource recognizes an explicit match/sort/group prefix first. With
// no explicit sort, only the new streaming API may select group-key order;
// first/last retain the existing source order and therefore disallow that choice.
func (c *Collection) aggregateSource(ctx context.Context, p *Pipeline, opts AggregateOptions) (Source, *Pipeline, bool, string, error) {
	fallback := func() (Source, *Pipeline, bool, string, error) {
		if opts.Hint != "" {
			return nil, nil, false, "", ErrIndexOrder
		}
		return c.Source(nil, nil), p, false, "", nil
	}
	i := 0
	var preds []pred
	for i < len(p.stages) && p.stages[i].kind == stMatch {
		preds = append(preds, p.stages[i].preds...)
		i++
	}
	explicit := false
	var order []IndexField
	if i < len(p.stages) && p.stages[i].kind == stSort {
		explicit = true
		for _, k := range p.stages[i].sortKeys {
			order = append(order, IndexField{Field: k.field, Desc: k.desc})
		}
		i++
	}
	if i >= len(p.stages) || p.stages[i].kind != stGroup || len(p.stages[i].group.idFields) == 0 {
		return fallback()
	}
	g := p.stages[i].group
	var access queryAccess
	found := false
	if explicit {
		a, err := c.chooseQueryAccess(preds, QueryOptions{Sort: order, Hint: opts.Hint, MaxMemoryBytes: opts.MaxMemoryBytes})
		if err != nil {
			return nil, nil, false, "", err
		}
		if groupIndexOrder(g, a, order) {
			access, found = a, true
		}
	} else {
		for _, a := range g.accs {
			if a.op == accFirst || a.op == accLast {
				return fallback()
			}
		}
		hintSeen := opts.Hint == ""
		for _, d := range c.indexDefs() {
			if opts.Hint != "" && d.Name != opts.Hint {
				continue
			}
			hintSeen = true
			if d.Encoding != 2 || len(d.Fields) == 0 {
				continue
			}
			candidate := append([]IndexField(nil), d.Fields...)
			a, err := c.chooseQueryAccess(preds, QueryOptions{Sort: candidate, Hint: d.Name, MaxMemoryBytes: opts.MaxMemoryBytes})
			if err != nil {
				if opts.Hint != "" {
					return nil, nil, false, "", err
				}
				continue
			}
			if groupIndexOrder(g, a, candidate) {
				access, found = a, true
				break
			}
		}
		if !hintSeen {
			return nil, nil, false, "", ErrNoSuchIndex
		}
	}
	if !found {
		return fallback()
	}
	q := *p
	q.stages = append([]stage(nil), p.stages[i:]...)
	src := func(yield func([]byte, []byte) bool) error {
		return c.queryAdvanced(ctx, preds, QueryOptions{}, access, yield)
	}
	return src, &q, true, access.def.Name, nil
}

// A group is contiguous when every varying group component is covered before
// any varying non-group sort component. Equality-bound scalar components may
// be skipped. Requiring a complete exact compound index also rules out arrays,
// sparse omissions and primary-key tie changes. Typed numeric/null ties are
// retained together until their coarser ordered index prefix changes.
func groupIndexOrder(g *groupSpec, a queryAccess, order []IndexField) bool {
	if a.info.Access != "index-order" || a.info.Reverse {
		return false
	}
	needed := map[string]bool{}
	for _, f := range g.idFields {
		needed[f] = true
	}
	fixed := map[string]bool{}
	indexed := map[string]bool{}
	for i, f := range a.def.Fields {
		indexed[f.Field] = true
		if i < a.info.EqualityPrefix {
			fixed[f.Field] = true
			delete(needed, f.Field)
		}
	}
	for _, f := range g.idFields {
		if !indexed[f] {
			return false
		}
	}
	for _, f := range order {
		if len(needed) == 0 {
			return true
		}
		if fixed[f.Field] {
			continue
		}
		if !needed[f.Field] {
			return false
		}
		delete(needed, f.Field)
	}
	return len(needed) == 0
}

func (x *aggregateExec) pipeline(p *Pipeline, src srcSpec, e *env, ordered bool) rowSource {
	pl := p.plan()
	pl.limits = x.limits
	var rows rowSource
	if pl.group != nil {
		rows = func(emit func(map[string]any) error) error { return x.group(&pl, src, e, ordered, emit) }
		if pl.countOnly {
			upstream := rows
			rows = func(emit func(map[string]any) error) error {
				seen := false
				err := upstream(func(r map[string]any) error { seen = true; delete(r, "_id"); return emit(r) })
				if err != nil {
					return err
				}
				if !seen {
					return emit(map[string]any{pl.group.accs[0].name: int64(0)})
				}
				return nil
			}
		}
	} else {
		rows = func(emit func(map[string]any) error) error {
			return pl.stream(x.ctx, src, e, true, nil, func(r *rec) error {
				x.stats.InputRows++
				m, err := DecodeDocument(r.doc)
				if err != nil {
					return err
				}
				if pl.join != nil && r.fdoc != nil {
					f, err := DecodeDocument(r.fdoc)
					if err != nil {
						return err
					}
					setRowField(m, pl.join.spec.as, f)
				}
				return emit(m)
			})
		}
	}
	for _, st := range p.stages[pl.next:] {
		rows = x.stage(rows, st, e)
	}
	return rows
}

func (x *aggregateExec) stage(up rowSource, st stage, e *env) rowSource {
	switch st.kind {
	case stSort:
		return func(emit func(map[string]any) error) error { return x.sortRows(up, st.sortKeys, emit) }
	case stGroup:
		return func(emit func(map[string]any) error) error {
			p := &Pipeline{stages: []stage{st}, limits: x.limits}
			pl := p.plan()
			src := func(yield func([]byte, []byte) bool) error {
				stop := errors.New("source stopped")
				err := up(func(r map[string]any) error {
					var b DocBuilder
					if err := encodeMap(&b, r, 0); err != nil {
						return err
					}
					if !yield(nil, b.Bytes()) {
						return stop
					}
					return nil
				})
				if err == stop {
					return nil
				}
				return err
			}
			return x.group(&pl, srcSpec{src: src}, e, false, emit)
		}
	case stSkip, stLimit:
		return func(emit func(map[string]any) error) error {
			n := 0
			stop := errors.New("window complete")
			if st.kind == stLimit && st.n == 0 {
				return nil
			}
			err := up(func(r map[string]any) error {
				n++
				if st.kind == stSkip && n <= st.n {
					return nil
				}
				if err := emit(r); err != nil {
					return err
				}
				if st.kind == stLimit && n >= st.n {
					return stop
				}
				return nil
			})
			if err == stop {
				return nil
			}
			return err
		}
	case stCount:
		return func(emit func(map[string]any) error) error {
			var n int64
			err := up(func(map[string]any) error { n++; return nil })
			if err != nil {
				return err
			}
			return emit(map[string]any{st.countName: n})
		}
	default:
		return func(emit func(map[string]any) error) error {
			return up(func(r map[string]any) error {
				if err := x.ctx.Err(); err != nil {
					return err
				}
				// Nonblocking operators own only the current row. Their temporary
				// allocations are checked against the remaining shared execution budget.
				local := &memoryBudget{max: x.budget.max - x.budget.used.Load(), trackPeak: true}
				ee := &env{ctx: x.ctx, c: e.c, budget: local, count: -1}
				out, err := applyRows(&st, []map[string]any{r}, x.limits, ee)
				x.budget.peak = max(x.budget.peak, x.budget.used.Load()+local.peak)
				if err != nil {
					return err
				}
				for _, row := range out {
					if err := emit(row); err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
}

func (x *aggregateExec) group(pl *plan, src srcSpec, e *env, ordered bool, emit func(map[string]any) error) error {
	estimate := int64(0)
	if e.c != nil {
		if n, err := e.c.approxCount(); err == nil {
			estimate = int64(n)
		}
	}
	makeGroup := func() *grouper {
		g := newGrouper(pl.group, x.limits.MaxGroups)
		g.budget = x.budget
		g.slab = 1
		if !ordered {
			g.slab = 16
			if x.budget.max >= 1<<20 {
				g.slab = 256
				unit := int64(64 + len(pl.group.idFields)*64 + len(pl.group.accs)*80)
				if x.options.AllowDiskUse {
					g.slab = max(1, min(256, int(x.stateLimit/(unit*4))))
				}
			}
		}
		if x.options.AllowDiskUse {
			g.capBytes = x.stateLimit
			if !ordered && g.compactCount() {
				g.counts = make(map[string]int64)
			}
		}
		return g
	}
	g := makeGroup()
	defer func() { g.release() }()
	var partitions *aggregatePartitions
	defer func() {
		if partitions != nil {
			partitions.abort()
		}
	}()
	var bucket []byte
	groups := 0
	output := func(gs *groupState) error {
		groups++
		if groups > x.limits.MaxGroups {
			return ErrLimit
		}
		x.stats.Groups++
		view := GroupView{spec: pl.group, state: gs}

		if x.groupVisitor != nil {
			if err := x.ctx.Err(); err != nil {
				return err
			}
			x.stats.Returned++
			return x.groupVisitor(view)
		}
		return emit(g.row(gs))
	}
	flush := func() error {
		if g.size() == 0 {
			return nil
		}
		if partitions == nil {
			unit := g.charge / int64(max(1, g.size()))
			partitions = x.disk.partitions(pl.group, estimate, unit)
		}
		if err := g.each(false, partitions.add); err != nil {
			return err
		}
		g.release()
		g = makeGroup()
		return nil
	}

	finishBucket := func() error {
		if partitions == nil {
			if err := g.each(true, output); err != nil {
				return err
			}
			if ordered && !g.resetOrdered() {
				g.release()
				g = makeGroup()
			}
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		runs, err := partitions.finish()
		if err != nil {
			return err
		}
		if ordered {
			x.stats.Strategy = "ordered-index+spill"
		} else {
			x.stats.Strategy = "external-hash"
		}
		x.stats.MergePasses++
		for _, run := range runs {
			if err := x.disk.partitionGroup(run, pl.group, 0, output); err != nil {
				return err
			}
		}
		partitions.abort()
		partitions = nil
		return nil
	}

	err := pl.stream(x.ctx, src, e, false, nil, func(r *rec) error {
		if err := x.ctx.Err(); err != nil {
			return err
		}
		x.stats.InputRows++
		if ordered {
			key := make([]byte, 0, 32)
			for _, i := range pl.idIdx {
				v := r.vals[i]
				if v.k == kMissing {
					v.k = kNull
				}
				key = encodeExactIndexValue(key, v)
			}
			if bucket != nil && !bytes.Equal(key, bucket) {
				if err := finishBucket(); err != nil {
					return err
				}
			}
			bucket = append(bucket[:0], key...)
		}
		if x.options.AllowDiskUse && !g.canAdd(r.vals, pl.idIdx, pl.accIdx) {
			if err := flush(); err != nil {
				return err
			}
			if !g.canAdd(r.vals, pl.idIdx, pl.accIdx) {
				return fmt.Errorf("%w: one aggregate state exceeds the working budget", ErrLimit)
			}
		}
		return g.addVals(r.vals, pl.idIdx, pl.accIdx)
	})
	if err != nil {
		return err
	}
	return finishBucket()
}

// canAdd preflights conservative worst-case storage before mutating any
// accumulator. A budget failure can therefore flush/retry the whole row once.
func (g *grouper) canAdd(vals []value, idIdx, accIdx []int) bool {
	if g.budget == nil {
		return true
	}
	keyBytes := 0
	for _, i := range idIdx {
		v := vals[i]
		keyBytes++
		switch v.k {
		case kBool, kInt, kFloat:
			keyBytes += 8
		case kString, kDoc, kArray:
			keyBytes += 4 + len(v.s)
		}
	}
	cost := int64(2*keyBytes + 32 + 160)
	if g.slab > 1 {
		cost = 160 + g.textCost([]int{keyBytes})
		if keyBytes > cap(g.keyBuf) {
			capacity := max(keyBytes, 2*cap(g.keyBuf))
			capacity = (capacity + 15) &^ 15
			cost += int64(capacity - cap(g.keyBuf))
		}
	}
	if g.counts != nil {
		cost = 128 + g.textCost([]int{keyBytes})
		if keyBytes > cap(g.keyBuf) {
			capacity := max(keyBytes, 2*cap(g.keyBuf))
			capacity = (capacity + 15) &^ 15
			cost += int64(capacity - cap(g.keyBuf))
		}
	}
	if g.counts == nil && g.used == len(g.states) {
		cost += int64(g.nextSlab()) * (64 + int64(len(g.spec.idFields))*64 + int64(len(g.spec.accs))*80)
	}
	for _, i := range accIdx {
		if g.counts == nil && i >= 0 {
			cost += int64(len(vals[i].s))
		}
	}
	if len(idIdx) == 0 {
		cost = 0
		if g.single == nil {
			cost = 160 + int64(len(g.spec.accs))*80
		}
		for _, i := range accIdx {
			if i >= 0 {
				cost += int64(len(vals[i].s))
			}
		}
	}
	return (g.capBytes == 0 || g.charge+cost <= g.capBytes) && cost <= g.budget.max-g.budget.used.Load()
}

// AggregateWithOptions retains the result slice within the same execution
// budget. Use AggregateEach when result cardinality exceeds the memory budget.
func (c *Collection) AggregateWithOptions(ctx context.Context, pipeline []byte, opts AggregateOptions) ([]map[string]any, error) {
	p, err := ParsePipeline(pipeline)
	if err != nil {
		return nil, err
	}
	return c.runCollect(ctx, p, opts)
}
func (db *DB) AggregateWithOptions(ctx context.Context, pipeline []byte, opts AggregateOptions) ([]map[string]any, error) {
	return db.def.AggregateWithOptions(ctx, pipeline, opts)
}
func (s *Snapshot) AggregateWithOptions(ctx context.Context, pipeline []byte, opts AggregateOptions) ([]map[string]any, error) {
	return s.def().AggregateWithOptions(ctx, pipeline, opts)
}
func (c *Collection) runCollect(ctx context.Context, p *Pipeline, opts AggregateOptions) ([]map[string]any, error) {
	rows := []map[string]any{}
	err := c.runEach(ctx, p, opts, func(r map[string]any) bool { rows = append(rows, r); return true }, true)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
func (p *Pipeline) orderedCandidate() bool {
	i := 0
	for i < len(p.stages) && p.stages[i].kind == stMatch {
		i++
	}
	return i+1 < len(p.stages) && p.stages[i].kind == stSort && p.stages[i+1].kind == stGroup
}

// Reuse the common one-group state at an ordered boundary. Result maps own
// their values; clearing this private slab cannot mutate emitted rows.
func (g *grouper) resetOrdered() bool {
	if len(g.order) != 1 || len(g.states) != 1 || g.order[0] != &g.states[0] {
		return false
	}
	clear(g.groups)
	clear(g.states)
	clear(g.ids)
	clear(g.accs)
	clear(g.order)
	g.order = g.order[:0]
	g.single = nil
	g.used = 0
	base := int64(64 + len(g.spec.idFields)*64 + len(g.spec.accs)*80 + cap(g.keyBuf))
	g.keyBuf = g.keyBuf[:0]
	_ = g.reserve(base - g.charge)
	return true
}

// RunWithOptions is the materializing counterpart of Pipeline.RunEach.
func (p *Pipeline) RunWithOptions(ctx context.Context, src Source, opts AggregateOptions) ([]map[string]any, error) {
	rows := []map[string]any{}
	err := p.runEachSource(ctx, src, opts, func(row map[string]any) bool { rows = append(rows, row); return true }, true)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
