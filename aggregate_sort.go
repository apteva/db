package db

import (
	"context"
	"math"
	"sort"
	"strings"
)

// sortWindow recognizes only adjacent sort/[skip]/limit stages. An intervening
// match, project, group or lookup can change the rows or ordering.
func (p *Pipeline) sortWindow(i int) (end, keep, skip int, ok bool) {
	if i >= len(p.stages) || p.stages[i].kind != stSort {
		return
	}
	j := i + 1
	if j < len(p.stages) && p.stages[j].kind == stSkip {
		skip = p.stages[j].n
		j++
	}
	if j >= len(p.stages) || p.stages[j].kind != stLimit {
		return
	}
	limit := p.stages[j].n
	if limit == 0 {
		return j, 0, 0, true
	}
	if skip > int(^uint(0)>>1)-limit {
		return
	}
	return j, skip + limit, skip, true
}

type sortEntry struct {
	index int
	keys  []value
}

func compareSort(a, b []value, keys []sortKey) int {
	for j, k := range keys {
		c := compareValues(a[j], b[j])
		if c != 0 {
			if k.desc {
				return -c
			}
			return c
		}
	}
	return 0
}

// selectTop keeps a worst-first heap backed by one flat key buffer. Ties use
// source position, matching the existing stable full sort. fill may decline a
// fast path (for example a grouped document value or non-total NaN ordering).
func selectTop(ctx context.Context, n, keep int, keys []sortKey, fill func(int, []value) bool) ([]int, bool, error) {
	keep = min(keep, n)
	if keep == 0 {
		return []int{}, true, ctx.Err()
	}
	entries := make([]sortEntry, 0, keep)
	storage := make([]value, (keep+1)*len(keys))
	scratch := storage[keep*len(keys):]
	worse := func(a, b sortEntry) bool {
		c := compareSort(a.keys, b.keys, keys)
		return c > 0 || (c == 0 && a.index > b.index)
	}
	down := func(root int) {
		for {
			child := 2*root + 1
			if child >= len(entries) {
				return
			}
			if child+1 < len(entries) && worse(entries[child+1], entries[child]) {
				child++
			}
			if !worse(entries[child], entries[root]) {
				return
			}
			entries[root], entries[child] = entries[child], entries[root]
			root = child
		}
	}
	for i := 0; i < n; i++ {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		if !fill(i, scratch) {
			return nil, false, nil
		}
		for _, v := range scratch {
			if v.k == kFloat && math.IsNaN(v.f) {
				return nil, false, nil
			}
		}
		if len(entries) < keep {
			slot := storage[len(entries)*len(keys) : (len(entries)+1)*len(keys)]
			copy(slot, scratch)
			entries = append(entries, sortEntry{i, slot})
			if len(entries) == keep {
				for root := len(entries)/2 - 1; root >= 0; root-- {
					down(root)
				}
			}
			continue
		}
		c := compareSort(scratch, entries[0].keys, keys)
		if c >= 0 {
			continue
		} // later ties cannot displace an earlier retained row
		copy(entries[0].keys, scratch)
		entries[0].index = i
		down(0)
	}
	sort.Slice(entries, func(i, j int) bool {
		c := compareSort(entries[i].keys, entries[j].keys, keys)
		return c < 0 || (c == 0 && entries[i].index < entries[j].index)
	})
	indices := make([]int, len(entries))
	for i, e := range entries {
		indices[i] = e.index
	}
	return indices, true, ctx.Err()
}

func topRows(ctx context.Context, rows []map[string]any, keys []sortKey, keep, skip int) ([]map[string]any, bool, error) {
	indices, ok, err := selectTop(ctx, len(rows), keep, keys, func(i int, dst []value) bool {
		for j, k := range keys {
			dst[j] = rowField(rows[i], k.field)
		}
		return true
	})
	if !ok || err != nil {
		return nil, ok, err
	}
	skip = min(skip, len(indices))
	out := make([]map[string]any, len(indices)-skip)
	for i, index := range indices[skip:] {
		out[i] = rows[index]
	}
	return out, true, nil
}

// groupSortRef addresses a scalar result without allocating a result map.
// Complex document/array results retain the existing materialized comparator.
type groupSortRef struct {
	kind  uint8
	index int
}

func (g *grouper) sortRefs(keys []sortKey) ([]groupSortRef, bool) {
	refs := make([]groupSortRef, len(keys))
	for j, k := range keys {
		switch {
		case k.field == "_id":
			if g.spec.idNames != nil {
				return nil, false
			}
			refs[j] = groupSortRef{1, 0}
		case strings.HasPrefix(k.field, "_id."):
			name := strings.TrimPrefix(k.field, "_id.")
			if strings.Contains(name, ".") {
				return nil, false
			}
			for i, n := range g.spec.idNames {
				if n == name {
					refs[j] = groupSortRef{1, i}
					break
				}
			}
			// A scalar/document ID may contain nested paths: use rowField for it.
			if g.spec.idNames == nil {
				return nil, false
			}
		default:
			if strings.Contains(k.field, ".") {
				return nil, false
			}
			for i, a := range g.spec.accs {
				if a.name == k.field {
					refs[j] = groupSortRef{2, i}
					break
				}
			}
		}
	}
	return refs, true
}

func (p *Pipeline) finishGroup(ctx context.Context, pl *plan, g *grouper, e *env) ([]map[string]any, error) {
	if end, keep, skip, ok := p.sortWindow(pl.next); ok && !pl.countOnly {
		refs, valid := g.sortRefs(p.stages[pl.next].sortKeys)
		if valid {
			cost := int64(min(keep, len(g.order))+1) * (32 + int64(len(p.stages[pl.next].sortKeys))*48)
			if err := e.budget.reserve(cost); err != nil {
				return nil, err
			}
			indices, valid, err := selectTop(ctx, len(g.order), keep, p.stages[pl.next].sortKeys, func(i int, dst []value) bool {
				gs := g.order[i]
				for j, ref := range refs {
					v := value{}
					switch ref.kind {
					case 1:
						v.k = kNull
						if len(gs.id) > 0 {
							v = gs.id[ref.index].view()
						}
					case 2:
						v = accValue(&g.spec.accs[ref.index], &gs.accs[ref.index])
					}
					if ref.kind != 0 && v.k == kMissing {
						v.k = kNull
					}
					if v.k == kDoc || v.k == kArray {
						return false
					}
					if v.k == kBool && v.i != 0 {
						v.i = 1
					}
					dst[j] = v
				}
				return true
			})
			if err != nil {
				return nil, err
			}
			if valid {
				skip = min(skip, len(indices))
				if err := e.budget.reserve(int64(len(indices)-skip) * (160 + int64(len(g.spec.accs)+len(g.spec.idFields))*80)); err != nil {
					return nil, err
				}
				rows := make([]map[string]any, len(indices)-skip)
				for i, index := range indices[skip:] {
					rows[i] = g.row(g.order[index])
				}
				next := *pl
				next.next = end + 1
				return p.finish(ctx, &next, rows, e)
			}
		}
	}
	if err := e.budget.reserve(int64(len(g.order)) * (160 + int64(len(g.spec.accs)+len(g.spec.idFields))*80)); err != nil {
		return nil, err
	}
	return p.finish(ctx, pl, g.rows(), e)
}
