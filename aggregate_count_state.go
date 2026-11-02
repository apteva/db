package db

import "math"

// Single count accumulators need a key and counter, not an ID/accumulator object
// graph. The full typed view is reconstructed only while serializing/emitting.
func (g *grouper) compactCount() bool {
	return len(g.spec.idFields) > 0 && len(g.spec.accs) == 1 && (g.spec.accs[0].op == accCount || g.spec.accs[0].op == accCountNonNull)
}
func (g *grouper) size() int {
	if g.counts != nil {
		return len(g.counts)
	}
	return len(g.order)
}
func (g *grouper) each(ids bool, emit func(*groupState) error) error {
	if g.counts == nil {
		for _, s := range g.order {
			if err := emit(s); err != nil {
				return err
			}
		}
		return nil
	}
	if g.countState == nil {
		if err := g.reserve(int64(64 + len(g.spec.idFields)*64 + 80)); err != nil {
			return err
		}
		g.countState = &groupState{id: make([]scalar, len(g.spec.idFields)), accs: make([]accState, 1)}
	}
	for i, key := range g.countKeys {
		s := g.countState
		s.key = key
		s.accs[0] = accState{count: g.countVals[i]}
		if ids {
			if err := decodeGroupKey(key, s.id); err != nil {
				return err
			}
		}
		if err := emit(s); err != nil {
			return err
		}
	}
	return nil
}
func (g *grouper) countKey(vals []value, idx []int) []byte {
	b := g.keyBuf[:0]
	for _, i := range idx {
		v := vals[i]
		b = append(b, byte(v.k))
		switch v.k {
		case kBool, kInt:
			b = le.AppendUint64(b, uint64(v.i))
		case kFloat:
			b = le.AppendUint64(b, math.Float64bits(v.f))
		case kString, kDoc, kArray:
			b = le.AppendUint32(b, uint32(len(v.s)))
			b = append(b, v.s...)
		}
	}
	g.keyBuf = b
	return b
}
func (g *grouper) addCount(vals []value, ids, accs []int) error {
	needed := 0
	for _, i := range ids {
		v := vals[i]
		needed++
		switch v.k {
		case kInt, kBool, kFloat:
			needed += 8
		case kString, kDoc, kArray:
			needed += 4 + len(v.s)
		}
	}
	if needed > cap(g.keyBuf) {
		capacity := max(needed, 2*cap(g.keyBuf))
		capacity = (capacity + 15) &^ 15
		if err := g.reserve(int64(capacity - cap(g.keyBuf))); err != nil {
			return err
		}
		g.keyBuf = make([]byte, 0, capacity)
	}
	// The key borrows keyBuf: it may only look the map up. Assigning through
	// it would replace an existing entry's stored key with the scratch bytes.
	key := borrowedString(g.countKey(vals, ids))
	idx, ok := g.counts[key]
	if !ok {
		if len(g.counts) >= g.max {
			return ErrLimit
		}
		if err := g.reserve(128); err != nil {
			return err
		}
		var err error
		if idx, err = g.newCount(key); err != nil {
			return err
		}
	}
	increment := int64(1)
	if accs[0] >= 0 && vals[accs[0]].k == kSkip {
		increment = 0
	} else if g.spec.accs[0].op == accCountNonNull {
		if accs[0] < 0 || vals[accs[0]].k == kMissing || vals[accs[0]].k == kNull {
			increment = 0
		}
	}
	g.countVals[idx] += increment
	return nil
}

// newCount adds a zero counter under an owned copy of key.
func (g *grouper) newCount(key string) (int, error) {
	owned, err := g.ownText(key)
	if err != nil {
		return 0, err
	}
	idx := len(g.countKeys)
	g.countKeys = append(g.countKeys, owned)
	g.countVals = append(g.countVals, 0)
	g.counts[owned] = idx
	return idx, nil
}
func (g *grouper) mergeCount(other *groupState) (bool, error) {
	key := other.key // borrows the reader's buffer: lookups only
	idx, ok := g.counts[key]
	if !ok {
		cost := 128 + g.textCost([]int{len(key)})
		if g.charge+cost > g.capBytes || cost > g.budget.max-g.budget.used.Load() {
			return false, nil
		}
		if err := g.reserve(128); err != nil {
			return false, err
		}
		var err error
		if idx, err = g.newCount(key); err != nil {
			return false, err
		}
	}
	g.countVals[idx] += other.accs[0].count
	return true, nil
}
