package db

import (
	"context"
	"errors"
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
// Exact scalar compound equality prefixes use an index-only scan when every
// predicate is covered. Other predicates use document evaluation; multikey
// entries are never counted as separate documents. Counts exclude raw values
// that are not encoded documents. Errors return zero, not a partial count.
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
	a, covered, err := c.countAccess(preds, opts.Hint, budget)
	if err != nil {
		return 0, err
	}
	stats := opts.Stats
	if stats == nil {
		stats = &QueryStats{}
	}
	if covered {
		*stats = QueryStats{Plan: a.info, PeakMemoryBytes: int64(len(a.lo) + len(a.hi))}
		if stats.PeakMemoryBytes > budget {
			return 0, ErrLimit
		}
		var count int64
		var cancelled error
		// Checkpointed scalar indexes can count bounded leaf slots directly;
		// no primary keys, values or per-row buffers need to be decoded.
		err = c.db.scanLeaves(c.view(), a.lo, a.hi, func(lv leafView) bool {
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
			err = c.rawScan(a.lo, a.hi, func(_, _ []byte) bool {
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

// Unlike flattenConjunction, this rejects OR rather than dropping its clauses:
// every predicate must be proven before document evaluation can be omitted.
func countEqualities(preds []pred) ([]pred, bool) {
	var flat []pred
	for _, p := range preds {
		switch p.op {
		case opAnd:
			for _, child := range p.children {
				ps, ok := countEqualities(child)
				if !ok {
					return nil, false
				}
				flat = append(flat, ps...)
			}
		case opEq:
			if !indexable(p.v) {
				return nil, false
			}
			flat = append(flat, p)
		default:
			return nil, false
		}
	}
	return flat, true
}

func (c *Collection) countAccess(preds []pred, hint string, budget int64) (queryAccess, bool, error) {
	best := queryAccess{}
	if hint == "$natural" {
		return best, false, nil
	}
	flat, equalities := countEqualities(preds)
	found := hint == ""
	for _, d := range c.indexDefs() {
		if hint != "" && d.Name != hint {
			continue
		}
		found = true
		// Fields enforce exactly one scalar tuple per encoded document. Scalar
		// multikey/legacy indexes can omit values or repeat primary keys.
		if !equalities || d.Encoding != 2 || len(d.Fields) == 0 {
			continue
		}
		vals := []any{}
		matched := 0
		for _, f := range d.Fields {
			var bound *pred
			for i := range flat {
				p := &flat[i]
				if p.field != f.Field {
					continue
				}
				if bound != nil && compareValues(bound.v, p.v) != 0 {
					bound = nil
					break
				}
				bound = p
			}
			if bound == nil {
				break
			}
			for _, p := range flat {
				if p.field == f.Field {
					matched++
				}
			}
			// Sparse indexes omit missing fields, whereas equality to null also
			// matches missing. Require non-null bindings of every component.
			if d.Sparse && bound.v.k == kNull {
				break
			}
			vals = append(vals, bound.v.any())
		}
		if matched != len(flat) || d.Sparse && len(vals) != len(d.Fields) {
			continue
		}
		if best.info.Index != "" && len(vals) <= best.info.EqualityPrefix {
			continue
		}
		lo, err := encodeIndexTuple(d, vals)
		if err != nil {
			return best, false, err
		}
		best = queryAccess{def: d, lo: lo, hi: prefixEnd(lo), info: QueryPlan{Access: "index-count", Index: d.Name, Fields: append([]IndexField(nil), d.Fields...), EqualityPrefix: len(vals), IndexOnly: true, MaxMemoryBytes: budget}}
	}
	if !found {
		return best, false, ErrNoSuchIndex
	}
	return best, best.info.Index != "", nil
}
