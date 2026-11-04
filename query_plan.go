package db

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
)

const DefaultQueryMemoryBytes int64 = 64 << 20

// QueryPlan describes access and retained query memory, excluding snapshots,
// page caches, caller allocations and the one current borrowed input document.
type QueryPlan struct {
	IndexOnly      bool // True only when document evaluation is provably unnecessary.
	Access         string
	Index          string
	Fields         []IndexField
	EqualityPrefix int
	Ranges         int // index ranges an index-count plan reads
	Reverse        bool
	SortInMemory   bool
	MaxMemoryBytes int64
}
type QueryStats struct {
	// DocumentFetches counts candidate documents loaded for evaluation.
	// IndexEntriesExamined counts entries visited by the index-only count path.
	DocumentFetches, IndexEntriesExamined int64
	Examined, Returned                    int64
	PeakMemoryBytes                       int64
	Plan                                  QueryPlan
}
type queryAccess struct {
	info   QueryPlan
	def    indexDef
	lo, hi []byte
}

func queryBudget(opts QueryOptions) (int64, error) {
	if opts.MaxMemoryBytes < 0 {
		return 0, ErrPipeline
	}
	if opts.MaxMemoryBytes == 0 {
		return DefaultQueryMemoryBytes, nil
	}
	return opts.MaxMemoryBytes, nil
}
func validateQueryOptions(opts QueryOptions) error {
	if opts.Skip < 0 || opts.Limit < 0 || opts.Limit > math.MaxInt-opts.Skip || len(opts.Sort) > 16 {
		return ErrPipeline
	}
	seen := map[string]bool{}
	for _, f := range opts.Sort {
		if f.Field == "" || seen[f.Field] {
			return ErrPipeline
		}
		seen[f.Field] = true
	}
	_, err := queryBudget(opts)
	return err
}
func (c *Collection) ExplainQuery(ctx context.Context, filter []byte, opts QueryOptions) (QueryPlan, error) {
	if err := validateQueryOptions(opts); err != nil {
		return QueryPlan{}, err
	}
	if err := orBackground(ctx).Err(); err != nil {
		return QueryPlan{}, err
	}
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return QueryPlan{}, err
		}
		defer s.Close()
		return cc.ExplainQuery(ctx, filter, opts)
	}
	preds, err := queryPredicates(filter)
	if err != nil {
		return QueryPlan{}, err
	}
	a, err := c.chooseQueryAccess(preds, opts)
	if err != nil {
		return a.info, err
	}
	if a.info.Index == "" && opts.Hint == "" && !opts.KeyDescending {
		if ip, ok := c.indexPlanBudget(preds, a.info.MaxMemoryBytes); ok {
			a.info.Access = "index-filter"
			a.info.Index = ip.name
		}
	}
	return a.info, nil
}
func (c *Collection) chooseQueryAccess(preds []pred, opts QueryOptions) (queryAccess, error) {
	budget, err := queryBudget(opts)
	if err != nil {
		return queryAccess{}, err
	}
	best := queryAccess{info: QueryPlan{Access: "collection-scan", SortInMemory: len(opts.Sort) > 0, MaxMemoryBytes: budget}}
	if opts.Hint == "$natural" {
		return best, nil
	}
	flat := flattenConjunction(preds)
	bestScore := -1
	hintFound := opts.Hint == ""
	for _, d := range c.indexDefs() {
		if opts.Hint != "" && d.Name != opts.Hint {
			continue
		}
		if d.Name == opts.Hint {
			hintFound = true
		}
		// Scalar multikey/legacy indexes remain available to the existing filter
		// planner, but cannot supply complete scalar document sort order.
		if d.Encoding != 2 || len(d.Fields) == 0 {
			if opts.Hint != "" {
				return best, ErrIndexOrder
			}
			continue
		}
		if d.Sparse {
			if !sparseCovers(d, flat) {
				if opts.Hint != "" {
					return best, fmt.Errorf("%w: sparse hint cannot cover this filter", ErrPipeline)
				}
				continue
			}
		}
		lo, hi, prefixCount, hasRange, tupleErr := exactIndexBounds(d, flat)
		if tupleErr != nil {
			return best, tupleErr
		}
		ordered, reverse := false, false
		if len(opts.Sort) > 0 {
			requested := opts.Sort
			// Sorting equality-bound fields changes no order.
			for len(requested) > 0 {
				bound := false
				for i := 0; i < prefixCount; i++ {
					bound = bound || requested[0].Field == d.Fields[i].Field
				}
				if !bound {
					break
				}
				requested = requested[1:]
			}
			if len(requested) <= len(d.Fields)-prefixCount {
				ordered = true
				for i, f := range requested {
					stored := d.Fields[prefixCount+i]
					if stored.Field != f.Field {
						ordered = false
						break
					}
					flip := stored.Desc != f.Desc
					if i == 0 {
						reverse = flip
					} else if flip != reverse {
						ordered = false
						break
					}
				}
				if reverse && !opts.KeyDescending {
					ordered = false
				}
				// Remaining unconstrained index components would alter primary-key ties.
				if prefixCount+len(requested) != len(d.Fields) {
					ordered = false
				}
				if !reverse && opts.KeyDescending {
					ordered = false
				}
			}
		}
		if prefixCount == 0 && !hasRange && !ordered && opts.Hint == "" {
			continue
		}
		score := prefixCount * 4
		if hasRange {
			score++
		}
		if ordered {
			score += 100
		}
		if score <= bestScore {
			continue
		}
		bestScore = score
		access := "index-filter"
		if ordered {
			access = "index-order"
		}
		best = queryAccess{def: d, lo: lo, hi: hi, info: QueryPlan{Access: access, Index: d.Name, Fields: append([]IndexField(nil), d.Fields...), EqualityPrefix: prefixCount, Reverse: reverse, SortInMemory: !ordered, MaxMemoryBytes: budget}}
	}
	if !hintFound {
		return best, ErrNoSuchIndex
	}
	return best, nil
}

// sparseCovers reports whether the filter requires a value for every field
// of a sparse index, so no matching document lacks an entry.
func sparseCovers(d indexDef, flat []pred) bool {
	for _, f := range d.Fields {
		found := false
		for _, p := range flat {
			if p.field == f.Field && (p.op == opEq && p.v.k != kNull || p.op >= opGt && p.op <= opLte || p.op == opPrefix || p.op == opIn && len(p.in) > 0 && !hasNull(p.in)) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func hasNull(vs []value) bool {
	for _, v := range vs {
		if v.k == kNull {
			return true
		}
	}
	return false
}

// exactIndexBounds narrows an exact index to the filter's equality prefix
// on its leading fields plus at most one range on the next field.
func exactIndexBounds(d indexDef, flat []pred) (lo, hi []byte, prefixCount int, hasRange bool, err error) {
	vals := []any{}
	for _, f := range d.Fields {
		found := false
		for _, p := range flat {
			if p.field == f.Field && p.op == opEq && indexable(p.v) {
				vals = append(vals, p.v.any())
				prefixCount++
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	lo, err = encodeIndexTuple(d, vals)
	if err != nil {
		return nil, nil, 0, false, err
	}
	hi = prefixEnd(lo)
	base := append([]byte(nil), lo...)
	if prefixCount < len(d.Fields) {
		f := d.Fields[prefixCount]
		for _, p := range flat {
			if p.field != f.Field {
				continue
			}
			r, ok := compoundRange(d, base, p, f.Desc)
			if !ok {
				continue
			}
			hasRange = true
			if bytes.Compare(r.lo, lo) > 0 {
				lo = r.lo
			}
			if bytes.Compare(r.hi, hi) < 0 {
				hi = r.hi
			}
		}
	}
	return lo, hi, prefixCount, hasRange, nil
}

// maxInRanges bounds the index ranges $in lists on an exact index's leading
// fields expand to.
const maxInRanges = 64

// exactIndexRanges extends exactIndexBounds to $in on the leading fields:
// each combination of listed values bounds one range, deduplicated and in
// key order, while there are at most maxInRanges of them. Distinct value
// tuples give disjoint ranges.
func exactIndexRanges(d indexDef, flat []pred) (ranges []indexRange, prefixCount int, hasRange bool, err error) {
	combos := [][]pred{nil}
	for _, f := range d.Fields {
		var choice []pred
		for _, p := range flat {
			if p.field != f.Field {
				continue
			}
			if p.op == opEq && indexable(p.v) {
				choice = []pred{p}
				break
			}
			if p.op == opIn && choice == nil && len(p.in) > 0 {
				for _, v := range p.in {
					if !indexable(v) {
						choice = nil
						break
					}
					choice = append(choice, pred{field: p.field, op: opEq, v: v})
				}
			}
		}
		if choice == nil || len(combos)*len(choice) > maxInRanges {
			break
		}
		next := make([][]pred, 0, len(combos)*len(choice))
		for _, c := range combos {
			for _, p := range choice {
				next = append(next, append(append([]pred(nil), c...), p))
			}
		}
		combos = next
	}
	seen := map[string]bool{}
	for _, c := range combos {
		// the combination's equalities come first, so they bind the prefix
		lo, hi, n, ranged, err := exactIndexBounds(d, append(c, flat...))
		if err != nil {
			return nil, 0, false, err
		}
		if k := string(lo) + "\x00" + string(hi); !seen[k] {
			seen[k] = true
			ranges = append(ranges, indexRange{lo, hi})
		}
		prefixCount, hasRange = n, ranged
	}
	sort.Slice(ranges, func(i, j int) bool { return bytes.Compare(ranges[i].lo, ranges[j].lo) < 0 })
	return ranges, prefixCount, hasRange, nil
}

func compoundRange(d indexDef, prefix []byte, p pred, desc bool) (indexRange, bool) {
	if p.op != opEq && p.op != opPrefix && !(p.op >= opGt && p.op <= opLte) {
		return indexRange{}, false
	}
	if !indexable(p.v) || p.op != opEq && p.op != opPrefix && !p.v.isNum() && p.v.k != kString {
		return indexRange{}, false
	}
	begin := append(append([]byte(nil), prefix...), indexTag(p.v))
	end := prefixEnd(begin)
	valueKey := encodeExactIndexValue(append([]byte(nil), prefix...), p.v)
	if p.op == opPrefix {
		if p.v.k != kString {
			return indexRange{}, false
		}
		valueKey = valueKey[:len(valueKey)-2]
		begin, end = valueKey, prefixEnd(valueKey)
	} else {
		switch p.op {
		case opEq:
			begin, end = valueKey, prefixEnd(valueKey)
		case opGt:
			begin = prefixEnd(valueKey)
		case opGte:
			begin = valueKey
		case opLt:
			end = valueKey
		case opLte:
			end = prefixEnd(valueKey)
		}
	}
	if desc {
		invert := func(k []byte) []byte {
			out := append([]byte(nil), prefix...)
			for _, b := range k[len(prefix):] {
				out = append(out, ^b)
			}
			return out
		}
		// Inversion reverses prefix ranges; prefixEnd converts boundaries back to
		// an inclusive lower and exclusive upper interval.
		typeStart := append(append([]byte(nil), prefix...), ^indexTag(p.v))
		typeEnd := prefixEnd(typeStart)
		vk := invert(valueKey)
		switch p.op {
		case opEq:
			begin, end = vk, prefixEnd(vk)
		case opPrefix:
			begin, end = vk, prefixEnd(vk)
		case opGt:
			begin, end = typeStart, vk
		case opGte:
			begin, end = typeStart, prefixEnd(vk)
		case opLt:
			begin, end = prefixEnd(vk), typeEnd
		case opLte:
			begin, end = vk, typeEnd
		}
	}
	return indexRange{begin, end}, true
}

type queryRow struct{ key, doc []byte }
type queryHeap struct {
	rows []queryRow
	less func(queryRow, queryRow) bool
}

func (h queryHeap) Len() int           { return len(h.rows) }
func (h queryHeap) Less(i, j int) bool { return h.less(h.rows[j], h.rows[i]) }
func (h queryHeap) Swap(i, j int)      { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *queryHeap) Push(v any)        { h.rows = append(h.rows, v.(queryRow)) }
func (h *queryHeap) Pop() any {
	last := len(h.rows) - 1
	v := h.rows[last]
	h.rows = h.rows[:last]
	return v
}
func rowCost(r queryRow) int64 { return int64(len(r.key) + len(r.doc) + 128) }

// queryIndexPlan narrows a query that found no single index range, such as $in
// on a leading field, when reading its documents by key is the cheaper route.
func (c *Collection) queryIndexPlan(preds []pred, opts QueryOptions, a queryAccess) (indexPlan, bool) {
	if opts.Hint != "" || opts.KeyDescending {
		return indexPlan{}, false
	}
	return c.indexPlanBudget(preds, a.info.MaxMemoryBytes)
}

func (c *Collection) queryAdvanced(ctx context.Context, preds []pred, opts QueryOptions, a queryAccess, yield func([]byte, []byte) bool) error {
	stats := opts.Stats
	if stats == nil {
		stats = &QueryStats{}
	}
	*stats = QueryStats{Plan: a.info}
	var src Source
	if a.info.Index != "" {
		cur, err := c.NewIndexCursor(ctx, a.info.Index, IndexCursorOptions{})
		if err != nil {
			return err
		}
		defer cur.Close()
		cur.lower, cur.upper = a.lo, a.hi
		src = func(fn func([]byte, []byte) bool) error {
			ok := cur.First()
			if a.info.Reverse {
				ok = cur.Last()
			}
			for ok {
				if !fn(cur.Key(), cur.Value()) {
					break
				}
				if a.info.Reverse {
					ok = cur.Prev()
				} else {
					ok = cur.Next()
				}
			}
			return cur.Err()
		}
	} else if ip, ok := c.queryIndexPlan(preds, opts, a); ok {
		stats.Plan.Access = "index-filter"
		stats.Plan.Index = ip.name
		stats.PeakMemoryBytes = ip.memory
		sort.Slice(ip.pks, func(i, j int) bool { return bytes.Compare(ip.pks[i], ip.pks[j]) < 0 })
		src = c.rangesSource(ctx, ip)
	} else {
		src = c.Source(nil, nil)
	}
	matches := func(doc []byte) bool {
		stats.Examined++
		stats.DocumentFetches++
		if _, _, ok := docDir(doc); !ok {
			return false
		}
		return matchAll(preds, func(field string) value { return pathValue(value{k: kDoc, s: doc}, field) })
	}
	if !a.info.SortInMemory {
		skipped := 0
		var cancelled error
		err := src(func(k, doc []byte) bool {
			if cancelled = ctx.Err(); cancelled != nil {
				return false
			}
			if !matches(doc) {
				return true
			}
			if skipped < opts.Skip {
				skipped++
				return true
			}
			stats.Returned++
			return yield(k, doc) && (opts.Limit == 0 || stats.Returned < int64(opts.Limit))
		})
		if err != nil {
			return err
		}
		return cancelled
	}
	less := func(x, y queryRow) bool {
		for _, f := range opts.Sort {
			a := pathValue(value{k: kDoc, s: x.doc}, f.Field)
			b := pathValue(value{k: kDoc, s: y.doc}, f.Field)
			cmp := compareValues(a, b)
			if cmp != 0 {
				if f.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		cmp := bytes.Compare(x.key, y.key)
		if opts.KeyDescending {
			return cmp > 0
		}
		return cmp < 0
	}
	h := queryHeap{less: less}
	keep := 0
	if opts.Limit > 0 {
		keep = opts.Skip + opts.Limit
	}
	retained := int64(0)
	var failure error
	err := src(func(k, doc []byte) bool {
		if failure = ctx.Err(); failure != nil {
			return false
		}
		if !matches(doc) {
			return true
		}
		candidate := queryRow{k, doc}
		if keep > 0 && len(h.rows) >= keep && !less(candidate, h.rows[0]) {
			return true
		}
		cost := rowCost(candidate)
		if keep > 0 && len(h.rows) >= keep {
			retained -= rowCost(h.rows[0])
		}
		if retained+cost > a.info.MaxMemoryBytes {
			failure = ErrLimit
			return false
		}
		owned := queryRow{append([]byte(nil), k...), append([]byte(nil), doc...)}
		retained += cost
		stats.PeakMemoryBytes = max(stats.PeakMemoryBytes, retained)
		if keep > 0 && len(h.rows) >= keep {
			h.rows[0] = owned
			heap.Fix(&h, 0)
		} else if keep > 0 {
			heap.Push(&h, owned)
		} else {
			h.rows = append(h.rows, owned)
		}
		return true
	})
	if err != nil {
		return err
	}
	if failure != nil {
		return failure
	}
	sort.Slice(h.rows, func(i, j int) bool { return less(h.rows[i], h.rows[j]) })
	for i := min(opts.Skip, len(h.rows)); i < len(h.rows); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.Returned++
		if !yield(h.rows[i].key, h.rows[i].doc) || opts.Limit > 0 && stats.Returned >= int64(opts.Limit) {
			break
		}
	}
	return nil
}
