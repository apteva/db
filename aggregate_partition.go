package db

import (
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"io"
	"math"
	"os"
	"strings"
)

type aggregateStore struct {
	run  *aggregateRun
	refs int
}

// Logical hash partitions share one append-only file. Each partition buffers
// whole records and remembers its extents. Fanout does not consume a descriptor
// per partition, including under concurrent queries and recursive repartitioning.
type aggregatePartitions struct {
	d           *aggregateDisk
	spec        *groupSpec
	seed        maphash.Seed
	file        *os.File
	store       *aggregateStore
	runs        []*aggregateRun
	buffers     [][]byte
	buffer      int64
	offset      int64
	blockHeader [16]byte
	scratch     []byte
	scratchCost int64
}

func (d *aggregateDisk) partitions(spec *groupSpec, estimate int64, unit int64) *aggregatePartitions {
	n := 2
	if estimate <= 0 {
		estimate = 1 << 20
	}
	// Cardinality is only a planning estimate, never a uniqueness assumption.
	// Underestimates remain correct through bounded recursive repartitioning.
	want := estimate * max(unit, 1) / max(d.stateLimit, 1)
	want += want/16 + 1
	for int64(n) < want && n < 256 {
		n *= 2
	}
	available := max(int64(0), d.budget.max-d.budget.used.Load())
	for n > 2 && int64(n)*(4096+512) > available/2 {
		n /= 2
	}
	buffer := min(int64(64<<10), max(int64(512), available/int64(n*2)))
	return &aggregatePartitions{d: d, spec: spec, seed: maphash.MakeSeed(), runs: make([]*aggregateRun, n), buffers: make([][]byte, n), buffer: buffer}
}
func fillAggregateBlock(b []byte, next int64, length int) {
	le.PutUint64(b, uint64(next))
	le.PutUint32(b[8:], uint32(length))
	le.PutUint32(b[12:], crc32.ChecksumIEEE(b[:12]))
}
func (p *aggregatePartitions) writeBlock(slot int, b []byte) error {
	r := p.runs[slot]
	if err := p.d.accountRecord(p.store.run, 16); err != nil {
		return err
	}
	fillAggregateBlock(b, -1, len(b)-16)
	if err := writeFull(p.file, b); err != nil {
		return err
	}
	if r.last >= 0 {
		fillAggregateBlock(p.blockHeader[:], p.offset, int(r.lastLength))
		n, err := p.file.WriteAt(p.blockHeader[:], r.last)
		if err != nil {
			return err
		}
		if n != 16 {
			return io.ErrShortWrite
		}
		p.d.stats.SpilledBytes += 16
	} else {
		r.first = p.offset
	}
	r.last = p.offset
	r.lastLength = int64(len(b) - 16)
	p.offset += int64(len(b))
	return nil
}
func (p *aggregatePartitions) flush(slot int) error {
	b := p.buffers[slot]
	if len(b) <= 16 {
		return nil
	}
	if err := p.writeBlock(slot, b); err != nil {
		return err
	}
	p.buffers[slot] = b[:16]
	return nil
}
func (p *aggregatePartitions) add(g *groupState) error {
	if err := p.d.ctx.Err(); err != nil {
		return err
	}
	slot := int(maphash.String(p.seed, g.key) % uint64(len(p.runs)))
	if p.file == nil {
		r, f, err := p.d.create()
		if err != nil {
			return err
		}
		p.file = f
		p.store = &aggregateStore{run: r}
	}
	if p.runs[slot] == nil {
		if err := p.d.budget.reserve(256 + p.buffer + 16); err != nil {
			return err
		}
		p.runs[slot] = &aggregateRun{path: p.store.run.path, store: p.store, first: -1, last: -1}
		p.store.refs++
		p.buffers[slot] = make([]byte, 16, int(p.buffer)+16)
		p.d.stats.SpillPartitions++
	}
	// Reserve reusable codec workspace before encoding. Twice the conservative
	// state size covers geometric append growth and wide variable payloads.
	need := int64(groupRecordSize(g, p.spec)) + 24
	if need > math.MaxUint32 {
		return ErrLimit
	}
	if need > p.scratchCost {
		if err := p.d.budget.reserve(need - p.scratchCost); err != nil {
			return err
		}
		p.scratchCost = need
		p.scratch = make([]byte, 24, int(need))
	}
	p.scratch = appendGroupRecord(p.scratch[:24], g, p.spec)
	data := p.scratch[24:]
	r := p.runs[slot]
	n := len(data) + 8
	if err := p.d.accountRecord(r, int64(n)); err != nil {
		return err
	}
	r.maxRecord = max(r.maxRecord, int64(len(data))*4+256)
	if int64(n) > p.buffer {
		if err := p.flush(slot); err != nil {
			return err
		}
		le.PutUint32(p.scratch[16:], uint32(len(data)))
		le.PutUint32(p.scratch[20:], crc32.ChecksumIEEE(data))
		return p.writeBlock(slot, p.scratch)
	}
	if len(p.buffers[slot])+n > cap(p.buffers[slot]) {
		if err := p.flush(slot); err != nil {
			return err
		}
	}
	b := p.buffers[slot]
	b = le.AppendUint32(b, uint32(len(data)))
	b = le.AppendUint32(b, uint32(crc32.ChecksumIEEE(data)))
	b = append(b, data...)
	p.buffers[slot] = b
	return nil
}
func (p *aggregatePartitions) abort() {
	if p.file != nil {
		p.file.Close()
		p.file = nil
	}
	for i, b := range p.buffers {
		if b != nil {
			p.d.budget.reserve(-p.buffer - 16)
			p.buffers[i] = nil
		}
	}
	if p.scratchCost != 0 {
		p.d.budget.reserve(-p.scratchCost)
		p.scratchCost = 0
		p.scratch = nil
	}
}
func (p *aggregatePartitions) finish() ([]*aggregateRun, error) {
	var runs []*aggregateRun
	for i, r := range p.runs {
		if r != nil {
			if err := p.flush(i); err != nil {
				return nil, err
			}
			runs = append(runs, r)
		}
	}
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			return nil, err
		}
		p.file = nil
	}
	p.abort()
	return runs, nil
}
func usePartialExt(op int, x, y *accState) bool {
	if !y.hasExt {
		return false
	}
	switch op {
	case int(accLast):
		return true
	case int(accFirst):
		return !x.hasExt
	case int(accMin), int(accMax):
		c := compareValues(y.ext.view(), x.ext.view())
		return !x.hasExt || (op == int(accMin) && c < 0) || (op == int(accMax) && c > 0)
	}
	return false
}
func (g *grouper) mergePartial(other *groupState) (bool, error) {
	if g.counts != nil {
		return g.mergeCount(other)
	}
	existing := g.groups[other.key]
	cost := int64(0)
	if existing == nil {
		cost = 160 + g.slabCost() + g.textCost([]int{len(other.key)})
		for _, a := range other.accs {
			cost += int64(len(a.ext.s))
		}
	} else {
		for i, a := range g.spec.accs {
			x, y := &existing.accs[i], &other.accs[i]
			if usePartialExt(int(a.op), x, y) {
				cost += int64(len(y.ext.s) - len(x.ext.s))
			}
		}
	}
	if g.charge+cost > g.capBytes || cost > g.budget.max-g.budget.used.Load() {
		return false, nil
	}
	if existing == nil {
		if err := g.reserve(160); err != nil {
			return false, err
		}
		gs, err := g.allocGroup()
		if err != nil {
			return false, err
		}
		gs.key, err = g.ownText(other.key)
		if err != nil {
			return false, err
		}
		if err = decodeGroupKey(gs.key, gs.id); err != nil {
			return false, err
		}
		copy(gs.accs, other.accs)
		for i := range gs.accs {
			s := &gs.accs[i]
			if s.ext.s != "" {
				if err = g.reserve(int64(len(s.ext.s))); err != nil {
					return false, err
				}
				s.ext.s = strings.Clone(s.ext.s)
			}
		}
		g.groups[gs.key] = gs
		g.order = append(g.order, gs)
	} else {
		if err := g.reserve(cost); err != nil {
			return false, err
		}
		for i, a := range g.spec.accs {
			x, y := &existing.accs[i], &other.accs[i]
			if usePartialExt(int(a.op), x, y) && y.ext.s != "" {
				y.ext.s = strings.Clone(y.ext.s)
			}
		}
		mergeState(g.spec, existing, other)
	}
	return true, nil
}

func (d *aggregateDisk) partitionGroup(run *aggregateRun, spec *groupSpec, depth int, emit func(*groupState) error) error {
	if depth > 32 {
		return fmt.Errorf("%w: aggregation partition depth exhausted", ErrLimit)
	}
	r, err := d.reader(run)
	if err != nil {
		return err
	}
	open := true
	defer func() {
		if open {
			r.close()
		}
	}()
	g := newGrouper(spec, int(^uint(0)>>1))
	g.budget = d.budget
	g.slab = max(1, min(256, int(d.stateLimit/int64((64+len(spec.idFields)*64+len(spec.accs)*80)*8))))
	g.capBytes = d.stateLimit
	if g.compactCount() {
		g.counts = make(map[string]int64)
	}
	defer func() {
		if g != nil {
			g.release()
		}
	}()
	for {
		err = r.nextGroup(spec)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(r.group.accs) != len(spec.accs) || len(r.group.id) != len(spec.idFields) {
			return ErrCorrupt
		}
		added, err := g.mergePartial(r.group)
		if err != nil {
			return err
		}
		if added {
			continue
		}
		// Already merged prefix states precede every remaining partial state.
		// Appending them first preserves first/last across recursive partitions.
		children := d.partitions(spec, max(2, int64(run.size)/32), int64(224+len(spec.idFields)*64+len(spec.accs)*80))
		d.stats.MaxPartitionDepth = max(d.stats.MaxPartitionDepth, depth+1)
		defer children.abort()
		if err := g.each(false, children.add); err != nil {
			return err
		}
		g.release()
		g = nil
		if err := children.add(r.group); err != nil {
			return err
		}
		for {
			err = r.nextGroup(spec)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if err = children.add(r.group); err != nil {
				return err
			}
		}
		runs, err := children.finish()
		if err != nil {
			return err
		}
		r.close()
		open = false
		if err = d.remove(run); err != nil {
			return err
		}
		d.stats.MergePasses++
		for _, child := range runs {
			if err = d.partitionGroup(child, spec, depth+1, emit); err != nil {
				return err
			}
		}
		return nil
	}
	if err := g.each(true, func(gs *groupState) error {
		if err := d.ctx.Err(); err != nil {
			return err
		}
		return emit(gs)
	}); err != nil {
		return err
	}
	r.close()
	open = false
	return d.remove(run)
}
