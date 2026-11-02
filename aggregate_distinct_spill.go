package db

import (
	"fmt"
	"hash/maphash"
	"io"
	"strings"
)

func (s *groupSpec) hasDistinct() bool {
	for _, a := range s.accs {
		if a.op == accCountDistinct {
			return true
		}
	}
	return false
}

// distinctMergeCost charges only the members y adds to x: partial states of
// one group from several workers or spills overlap.
func distinctMergeCost(x, y map[string]struct{}) int64 {
	if y == nil {
		return 0
	}
	if x == nil {
		return distinctSetCost(y)
	}
	n := int64(0)
	for k := range y {
		if _, ok := x[k]; !ok {
			n += int64(len(k)) + distinctEntryCost
		}
	}
	return n
}

// maxDistinctSlices bounds how many passes distinctGroup makes over a run.
const maxDistinctSlices = 1024

// distinctGroup merges a run whose partial states all share one group key and
// whose distinct sets do not fit the state limit together. Members hash to
// disjoint slices; each pass reads the run once and holds one slice of every
// set, so the counts add up exactly. The first pass also merges the other
// accumulators. A slice that still does not fit doubles the slices and
// starts over with a new seed.
func (d *aggregateDisk) distinctGroup(run *aggregateRun, spec *groupSpec, emit func(*groupState) error) error {
	slices := int(min(int64(maxDistinctSlices), max(2, 2*run.size/max(d.stateLimit, 1)+1)))
	for ; slices <= maxDistinctSlices; slices *= 2 {
		gs, fits, err := d.countDistinctSlices(run, spec, slices)
		if err != nil {
			return err
		}
		if fits {
			d.stats.MergePasses += slices
			d.stats.DistinctSlices += slices
			if err := emit(gs); err != nil {
				return err
			}
			return d.remove(run)
		}
	}
	return fmt.Errorf("%w: distinct aggregation state exceeds its memory limit", ErrLimit)
}

func (d *aggregateDisk) countDistinctSlices(run *aggregateRun, spec *groupSpec, slices int) (*groupState, bool, error) {
	seed := maphash.MakeSeed()
	var out *groupState
	counts := make([]int64, len(spec.accs))
	for slice := 0; slice < slices; slice++ {
		fits, err := d.countDistinctSlice(run, spec, seed, slices, slice, counts, &out)
		if err != nil || !fits {
			return nil, false, err
		}
	}
	if out == nil {
		return nil, false, ErrCorrupt
	}
	for i, a := range spec.accs {
		if a.op == accCountDistinct {
			out.accs[i].set, out.accs[i].count = nil, counts[i]
		}
	}
	return out, true, nil
}

func (d *aggregateDisk) countDistinctSlice(run *aggregateRun, spec *groupSpec, seed maphash.Seed, slices, slice int, counts []int64, out **groupState) (bool, error) {
	r, err := d.reader(run)
	if err != nil {
		return false, err
	}
	defer r.close()
	sets := make([]map[string]struct{}, len(spec.accs))
	charged := int64(0)
	defer func() { d.budget.reserve(-charged) }()
	for {
		err = r.nextGroup(spec)
		if err != nil {
			break
		}
		g := r.group
		if len(g.accs) != len(spec.accs) || len(g.id) != len(spec.idFields) {
			return false, ErrCorrupt
		}
		for i, a := range spec.accs {
			if a.op != accCountDistinct {
				continue
			}
			for k := range g.accs[i].set {
				if int(maphash.String(seed, k)%uint64(slices)) != slice {
					continue
				}
				if sets[i] == nil {
					sets[i] = map[string]struct{}{}
				}
				if _, ok := sets[i][k]; ok {
					continue
				}
				cost := int64(len(k)) + distinctEntryCost
				if charged+cost > d.stateLimit {
					return false, nil
				}
				if err := d.budget.reserve(cost); err != nil {
					return false, nil
				}
				charged += cost
				sets[i][k] = struct{}{}
			}
			g.accs[i].set = nil
		}
		if slice != 0 {
			continue
		}
		// The other accumulators are merged once, in run order.
		for i := range g.accs {
			g.accs[i].ext.s = strings.Clone(g.accs[i].ext.s)
		}
		if *out == nil {
			gs := &groupState{key: strings.Clone(g.key), id: make([]scalar, len(spec.idFields)), accs: append([]accState(nil), g.accs...)}
			if err := decodeGroupKey(gs.key, gs.id); err != nil {
				return false, err
			}
			*out = gs
		} else if g.key != (*out).key {
			return false, ErrCorrupt
		} else {
			mergeState(spec, *out, g)
		}
	}
	if err != io.EOF {
		return false, err
	}
	for i, set := range sets {
		counts[i] += int64(len(set))
	}
	return true, nil
}
