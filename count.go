package db

import (
	"bytes"
	"context"
	"errors"
	"sort"
)

// CountOptions controls CountDocuments. Zero Limit is unlimited. Hint chooses a
// candidate covering index; $natural forces document evaluation. An existing
// hinted index that cannot prove coverage falls back to document evaluation.
// MaxMemoryBytes has the same exclusions and default as QueryOptions.
type CountOptions struct {
	Skip, Limit    int
	Hint           string
	MaxMemoryBytes int64
	Stats          *QueryStats
}

// CountDocuments counts documents matching Query's JSON filter at one snapshot.
// An exact scalar compound index counts entries alone when it covers every
// predicate: equality or $in on leading fields, then an optional bounded range,
// and $ne null where those prove it. Other filters evaluate documents, narrowed
// by an index when that is cheaper; multikey entries are never counted as
// separate documents. Counts exclude raw values that are not encoded documents.
// Errors return zero, not a partial count.
func (c *Collection) CountDocuments(ctx context.Context, filter []byte, opts CountOptions) (int64, error) {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q := QueryOptions{Skip: opts.Skip, Limit: opts.Limit, MaxMemoryBytes: opts.MaxMemoryBytes, Stats: opts.Stats}
	if err := validateQueryOptions(q); err != nil {
		return 0, err
	}
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return 0, err
		}
		defer s.Close()
		return cc.CountDocuments(ctx, filter, opts)
	}
	c.db.mu.RLock()
	closed := c.snap.closed || c.db.closed
	c.db.mu.RUnlock()
	if closed {
		return 0, ErrClosed
	}
	preds, err := queryPredicates(filter)
	if err != nil {
		return 0, err
	}
	budget, _ := queryBudget(q)
	a, ranges, covered, err := c.countAccess(preds, opts.Hint, budget)
	if err != nil {
		return 0, err
	}
	stats := opts.Stats
	if stats == nil {
		stats = &QueryStats{}
	}
	if covered {
		*stats = QueryStats{Plan: a.info}
		for _, r := range ranges {
			stats.PeakMemoryBytes += int64(len(r.lo) + len(r.hi))
		}
		if stats.PeakMemoryBytes > budget {
			return 0, ErrLimit
		}
		var count int64
		var cancelled error
		for _, r := range ranges {
			if opts.Limit > 0 && count >= int64(opts.Limit) {
				break
			}
			// Checkpointed scalar indexes can count bounded leaf slots directly;
			// no primary keys, values or per-row buffers need to be decoded.
			err = c.db.scanLeaves(c.view(), r.lo, r.hi, func(lv leafView) bool {
				if cancelled = ctx.Err(); cancelled != nil {
					return false
				}
				n := int64(lv.j - lv.i)
				if opts.Limit > 0 {
					n = min(n, int64(opts.Skip)+int64(opts.Limit)-stats.Examined)
				}
				remainingSkip := max(int64(opts.Skip)-stats.Examined, 0)
				matched := n - min(n, remainingSkip)
				stats.Examined += n
				stats.IndexEntriesExamined += n
				count += matched
				stats.Returned += matched
				return opts.Limit == 0 || count < int64(opts.Limit)
			})
			// A snapshot with buffered writes needs the merge scanner so inserts,
			// replacements and tombstones override the checkpointed index.
			if errors.Is(err, errNeedMerge) {
				err = c.rawScan(r.lo, r.hi, func(_, _ []byte) bool {
					if cancelled = ctx.Err(); cancelled != nil {
						return false
					}
					stats.IndexEntriesExamined++
					stats.Examined++
					if stats.Examined <= int64(opts.Skip) {
						return true
					}
					count++
					stats.Returned++
					return opts.Limit == 0 || count < int64(opts.Limit)
				})
			}
			if err != nil {
				return 0, err
			}
			if cancelled != nil {
				return 0, cancelled
			}
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return count, nil
	}
	// Reuse the bounded query planner for residual predicates, but counting
	// does not require ordering or retaining/sorting matching documents.
	if opts.Hint == "$natural" {
		q.Hint = "$natural"
	}
	a, err = c.chooseQueryAccess(preds, q)
	if err != nil {
		return 0, err
	}
	a.info.SortInMemory = false
	var count int64
	err = c.queryAdvanced(ctx, preds, q, a, func(_, _ []byte) bool { count++; return true })
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func (db *DB) CountDocuments(ctx context.Context, filter []byte, opts CountOptions) (int64, error) {
	return db.def.CountDocuments(ctx, filter, opts)
}
func (s *Snapshot) CountDocuments(ctx context.Context, filter []byte, opts CountOptions) (int64, error) {
	return s.def().CountDocuments(ctx, filter, opts)
}

// countField gathers the predicates on one field of a coverable conjunction.
type countField struct {
	hasSet  bool    // eq or $in limits the field to set
	set     []value // deduplicated
	ranges  []pred
	nonNull bool
}

// countFields groups a conjunction by field. Unlike flattenConjunction, this
// rejects OR rather than dropping its clauses: every predicate must be proven
// before document evaluation can be omitted.
func countFields(preds []pred, fields map[string]*countField) bool {
	for i := range preds {
		p := &preds[i]
		if p.op == opAnd {
			for _, child := range p.children {
				if !countFields(child, fields) {
					return false
				}
			}
			continue
		}
		f := fields[p.field]
		if f == nil {
			f = &countField{}
			fields[p.field] = f
		}
		switch p.op {
		case opEq:
			if !indexable(p.v) {
				return false
			}
			f.limit([]value{p.v})
		case opIn:
			if len(p.in) == 0 {
				return false
			}
			for _, v := range p.in {
				if !indexable(v) {
					return false
				}
			}
			f.limit(p.in)
		case opGt, opGte, opLt, opLte:
			if !p.v.isNum() && p.v.k != kString {
				return false
			}
			f.ranges = append(f.ranges, *p)
		case opNe:
			if p.v.k != kNull {
				return false
			}
			f.nonNull = true
		default:
			return false
		}
	}
	return true
}

func (f *countField) limit(values []value) {
	kept := []value{}
	for _, v := range values {
		if f.hasSet && !containsValue(f.set, v) || containsValue(kept, v) {
			continue
		}
		kept = append(kept, v)
	}
	f.hasSet, f.set = true, kept
}

func containsValue(values []value, v value) bool {
	for _, x := range values {
		if compareValues(x, v) == 0 {
			return true
		}
	}
	return false
}

// countRanges returns the entry ranges that hold exactly the documents matching
// fields, or false when d cannot prove that. prefix counts the fields bound to
// listed values.
func countRanges(d indexDef, fields map[string]*countField) (ranges []indexRange, prefix int, ranged bool, ok bool, err error) {
	combos := [][]any{{}}
	consumed := 0
	var next *countField
	var nextField IndexField
	for _, field := range d.Fields {
		f := fields[field.Field]
		if f == nil || !f.hasSet {
			next, nextField = f, field
			break
		}
		// A range on a listed field would need its own proof for each value.
		if len(f.ranges) > 0 || len(combos)*len(f.set) > maxInRanges {
			return nil, 0, false, false, nil
		}
		values := f.set
		if f.nonNull {
			values = nil
			for _, v := range f.set {
				if v.k != kNull {
					values = append(values, v)
				}
			}
		}
		// Sparse indexes omit missing fields, whereas equality to null also
		// matches missing.
		if d.Sparse && hasNull(values) {
			return nil, 0, false, false, nil
		}
		grown := make([][]any, 0, len(combos)*len(values))
		for _, combo := range combos {
			for _, v := range values {
				grown = append(grown, append(combo[:len(combo):len(combo)], v.any()))
			}
		}
		combos = grown
		prefix++
		consumed++
	}
	if next != nil {
		// Only a range can bind this field: nonNull alone would have to exclude
		// null entries by key. Ranges compare within one type, so they exclude
		// null and missing and prove any $ne null.
		if len(next.ranges) == 0 {
			return nil, 0, false, false, nil
		}
		ranged = true
		consumed++
	}
	if consumed != len(fields) || d.Sparse && consumed != len(d.Fields) {
		return nil, 0, false, false, nil
	}
	for _, combo := range combos {
		lo, err := encodeIndexTuple(d, combo)
		if err != nil {
			return nil, 0, false, false, err
		}
		hi := prefixEnd(lo)
		base := append([]byte(nil), lo...)
		if ranged {
			for _, p := range next.ranges {
				r, usable := compoundRange(d, base, p, nextField.Desc)
				if !usable {
					return nil, 0, false, false, nil
				}
				if bytes.Compare(r.lo, lo) > 0 {
					lo = r.lo
				}
				if bytes.Compare(r.hi, hi) < 0 {
					hi = r.hi
				}
			}
		}
		// Bounds of different types leave nothing, as document evaluation does.
		if bytes.Compare(lo, hi) < 0 {
			ranges = append(ranges, indexRange{lo, hi})
		}
	}
	sort.Slice(ranges, func(i, j int) bool { return bytes.Compare(ranges[i].lo, ranges[j].lo) < 0 })
	return ranges, prefix, ranged, true, nil
}

func (c *Collection) countAccess(preds []pred, hint string, budget int64) (queryAccess, []indexRange, bool, error) {
	best := queryAccess{}
	var bestRanges []indexRange
	if hint == "$natural" {
		return best, nil, false, nil
	}
	fields := map[string]*countField{}
	coverable := countFields(preds, fields)
	found := hint == ""
	score := -1
	for _, d := range c.indexDefs() {
		if hint != "" && d.Name != hint {
			continue
		}
		found = true
		// Fields enforce exactly one scalar tuple per encoded document. Scalar
		// multikey/legacy indexes can omit values or repeat primary keys.
		if !coverable || d.Encoding != 2 || len(d.Fields) == 0 {
			continue
		}
		ranges, prefix, ranged, ok, err := countRanges(d, fields)
		if err != nil {
			return best, nil, false, err
		}
		next := prefix * 2
		if ranged {
			next++
		}
		if !ok || next <= score {
			continue
		}
		score, bestRanges = next, ranges
		best = queryAccess{def: d, info: QueryPlan{Access: "index-count", Index: d.Name, Fields: append([]IndexField(nil), d.Fields...), EqualityPrefix: prefix, IndexOnly: true, MaxMemoryBytes: budget, Ranges: len(ranges)}}
	}
	if !found {
		return best, nil, false, ErrNoSuchIndex
	}
	return best, bestRanges, best.info.Index != "", nil
}
