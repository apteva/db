package db

import (
	"math"
	"strings"
)

// kSkip marks an accumulator operand whose where filter rejected the row:
// the accumulator takes nothing from it. It exists only in derived slots.
const kSkip kind = 255

// gateDef derives a filtered accumulator's operand: the source slot (or
// the constant c) when the row passes the filter, else kSkip.
type gateDef struct {
	preds    []pred
	predIdx  []int    // slot of each leaf predicate's field; -1 for $and/$or
	fields   []string // every leaf field, for $and/$or
	fieldIdx []int
	src      int // operand slot; -1: the constant c
	c        value
	out      int
}

// truncDef derives a $dateTrunc group key part from its date slot.
type truncDef struct {
	t        *dateTrunc
	src, out int
}

func (g *gateDef) matches(vals []value) bool {
	for j := range g.preds {
		pr := &g.preds[j]
		var ok bool
		if ix := g.predIdx[j]; ix >= 0 {
			ok = pr.eval(vals[ix])
		} else {
			ok = pr.matches(func(f string) value {
				for i, n := range g.fields {
					if n == f {
						return vals[g.fieldIdx[i]]
					}
				}
				return value{}
			})
		}
		if !ok {
			return false
		}
	}
	return true
}

func (g *gateDef) apply(vals []value) {
	if !g.matches(vals) {
		vals[g.out] = value{k: kSkip}
	} else if g.src < 0 {
		vals[g.out] = g.c
	} else {
		vals[g.out] = vals[g.src]
	}
}

// deriver runs a plan's derivations for one stream; its evaluators hold the
// per-stream caches, so concurrent shards each get their own.
type deriver struct {
	gates  []gateDef
	truncs []truncDef
	evals  []truncEval
}

func (pl *plan) deriver() *deriver {
	if len(pl.gates) == 0 && len(pl.truncs) == 0 {
		return nil
	}
	d := &deriver{gates: pl.gates, truncs: pl.truncs, evals: make([]truncEval, len(pl.truncs))}
	for i := range d.evals {
		d.evals[i].t = pl.truncs[i].t
	}
	return d
}

func (d *deriver) apply(vals []value) {
	for i := range d.truncs {
		t := &d.truncs[i]
		vals[t.out] = d.evals[i].eval(vals[t.src])
	}
	for i := range d.gates {
		d.gates[i].apply(vals)
	}
}

func (g *groupSpec) hasTrunc() bool {
	for _, t := range g.idTrunc {
		if t != nil {
			return true
		}
	}
	return false
}

func (g *groupSpec) hasWhere() bool {
	for _, a := range g.accs {
		if a.where != nil {
			return true
		}
	}
	return false
}

// ---- $countDistinct

// distinctKey encodes v as a set member: the kind byte and payload, the
// identity $group keys use.
func distinctKey(b []byte, v value) []byte {
	b = append(b[:0], byte(v.k))
	switch v.k {
	case kBool, kInt:
		b = le.AppendUint64(b, uint64(v.i))
	case kFloat:
		b = le.AppendUint64(b, math.Float64bits(v.f))
	case kString, kDoc, kArray:
		b = append(b, v.s...)
	}
	return b
}

// distinctEntryCost is the budget charge of one set member beyond its bytes.
const distinctEntryCost = 48

func (g *grouper) addDistinct(st *accState, v value) error {
	if v.k == kMissing || v.k == kNull {
		return nil
	}
	g.distinctBuf = distinctKey(g.distinctBuf, v)
	if st.set == nil {
		if err := g.reserve(160); err != nil {
			return err
		}
		st.set = make(map[string]struct{})
	} else if _, ok := st.set[borrowedString(g.distinctBuf)]; ok {
		return nil
	}
	if err := g.reserve(distinctEntryCost); err != nil {
		return err
	}
	var key string
	var err error
	if g.slab == 0 {
		key, err = g.ownKey(g.distinctBuf)
	} else {
		key, err = g.ownText(borrowedString(g.distinctBuf))
	}
	if err != nil {
		return err
	}
	st.set[key] = struct{}{}
	return nil
}

func distinctSetCost(set map[string]struct{}) int64 {
	if set == nil {
		return 0
	}
	n := int64(160)
	for k := range set {
		n += int64(len(k)) + distinctEntryCost
	}
	return n
}

func mergeDistinct(x, y *accState) {
	if y.set == nil {
		return
	}
	if x.set == nil {
		x.set = y.set
		return
	}
	for k := range y.set {
		x.set[k] = struct{}{}
	}
}

func appendDistinct(b []byte, set map[string]struct{}) []byte {
	b = le.AppendUint32(b, uint32(len(set)))
	for k := range set {
		b = le.AppendUint32(b, uint32(len(k)))
		b = append(b, k...)
	}
	return b
}

func decodeDistinct(r *aggregateDecoder) map[string]struct{} {
	n := r.u32()
	if r.err != nil || n > len(r.b)/4 {
		r.err = ErrCorrupt
		return nil
	}
	if n == 0 {
		return nil
	}
	set := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		l := r.u32()
		if r.err != nil || l > len(r.b) {
			r.err = ErrCorrupt
			return nil
		}
		set[strings.Clone(borrowedString(r.take(l)))] = struct{}{}
	}
	return set
}

func distinctRecordSize(set map[string]struct{}) int {
	n := 4
	for k := range set {
		n += 4 + len(k)
	}
	return n
}
