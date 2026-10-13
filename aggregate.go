package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// Source streams key/value pairs into a pipeline; yield returns false to stop.
type Source func(yield func(key, val []byte) bool) error

// srcSpec is where a streaming pass reads from: a caller's Source, or a key
// range of a collection, which the pass may read leaf by leaf in place.
type srcSpec struct {
	src    Source
	c      *Collection
	lo, hi []byte
	ranged bool
}

func (s srcSpec) source() Source {
	if s.ranged {
		return s.c.Source(s.lo, s.hi)
	}
	return s.src
}

// fieldPath is a dotted field reference split once: the top-level name and
// the rest of the path.
type fieldPath struct {
	top, rest string
}

func splitPaths(need []string) []fieldPath {
	out := make([]fieldPath, len(need))
	for i, f := range need {
		out[i] = fieldPath{top: f}
		if j := strings.IndexByte(f, '.'); j >= 0 {
			out[i] = fieldPath{f[:j], f[j+1:]}
		}
	}
	return out
}

// Limits bound the memory an aggregation may use.
type Limits struct {
	MaxGroups int // distinct $group keys
	MaxRows   int // materialised rows at any stage
}

// DefaultLimits apply to every parsed pipeline until SetLimits is called.
var DefaultLimits = Limits{MaxGroups: 1 << 20, MaxRows: 1 << 20}

var (
	ErrPipeline = errors.New("aptevadb: bad pipeline")
	ErrLimit    = errors.New("aptevadb: aggregation exceeded its memory limit")
)

// Pipeline is a compiled MongoDB-style aggregation pipeline.
//
// Stages: $match (equality, $eq $ne $gt $gte $lt $lte $in; an array field
// matches when any element does), $group (_id as null, "$field" or an
// object of "$field" refs; $sum $avg $min $max $count $first $last), $sort
// (one or more keys, in order), $skip, $limit, $project (inclusion or
// exclusion), $count, $unwind, $lookup (from/localField/foreignField/as).
// Field references may be dotted paths into nested documents.
//
// Leading $match/$skip/$limit stages, a $lookup immediately unwound on its
// own field, further $match stages and a following $group run as one
// streaming pass over the source without decoding documents into maps;
// the join is a hash join over the foreign collection, or index probes
// when the pipeline is selective and the foreign field is indexed.
type Pipeline struct {
	stages []stage
	limits Limits
}

// SetLimits overrides the memory limits for this pipeline.
func (p *Pipeline) SetLimits(l Limits) { p.limits = l }

type stageKind uint8

const (
	stMatch stageKind = iota
	stGroup
	stSort
	stSkip
	stLimit
	stProject
	stCount
	stUnwind
	stLookup
)

type predOp uint8

const (
	opEq predOp = iota
	opNe
	opGt
	opGte
	opLt
	opLte
	opIn
)

type pred struct {
	field string
	op    predOp
	v     value
	in    []value
}

type accOp uint8

const (
	accSum accOp = iota
	accAvg
	accMin
	accMax
	accCount
	accFirst
	accLast
)

type accSpec struct {
	name  string
	op    accOp
	field string // "$field" operand; "" means constant c
	c     value
}

type groupSpec struct {
	idFields []string // empty: _id is null
	idNames  []string // non-nil: composite _id document
	accs     []accSpec
}

type sortKey struct {
	field string
	desc  bool
}

type stage struct {
	kind      stageKind
	preds     []pred
	group     *groupSpec
	sortKeys  []sortKey
	n         int
	project   map[string]bool
	countName string
	unwind    string
	preserve  bool
	lookup    *lookupSpec
}

// lookupSpec is the equality form of $lookup: for each document, the
// documents of collection from whose foreignField equals its localField
// are attached as an array under as (null and a missing field match each
// other; an array on either side matches when any element does).
type lookupSpec struct {
	from, localField, foreignField, as string
}

// ParsePipeline compiles a JSON array of stages.
func ParsePipeline(src []byte) (*Pipeline, error) {
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(src, &raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPipeline, err)
	}
	p := &Pipeline{limits: DefaultLimits}
	for i, st := range raw {
		if len(st) != 1 {
			return nil, fmt.Errorf("%w: stage %d must have exactly one operator", ErrPipeline, i)
		}
		for op, body := range st {
			s, err := parseStage(op, body)
			if err != nil {
				return nil, fmt.Errorf("%w: stage %d: %v", ErrPipeline, i, err)
			}
			p.stages = append(p.stages, s)
		}
	}
	return p, nil
}

func decodeAny(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// orderedObject decodes a JSON object keeping key order.
func orderedObject(raw json.RawMessage) ([]string, []any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, nil, errors.New("expected an object")
	}
	var keys []string
	var vals []any
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, nil, err
		}
		keys = append(keys, k.(string))
		vals = append(vals, v)
	}
	return keys, vals, nil
}

func asInt(a any) (int, bool) {
	n, ok := a.(json.Number)
	if !ok {
		return 0, false
	}
	i, err := n.Int64()
	return int(i), err == nil
}

func fieldRef(a any) (string, bool) {
	s, ok := a.(string)
	if !ok || !strings.HasPrefix(s, "$") || len(s) < 2 {
		return "", false
	}
	return s[1:], true
}

func parseStage(op string, body json.RawMessage) (stage, error) {
	switch op {
	case "$sort":
		keys, vals, err := orderedObject(body)
		if err != nil || len(keys) == 0 {
			return stage{}, errors.New("$sort needs an object with at least one key")
		}
		st := stage{kind: stSort}
		for i, k := range keys {
			d, ok := asInt(vals[i])
			if !ok || (d != 1 && d != -1) {
				return stage{}, errors.New("$sort direction must be 1 or -1")
			}
			st.sortKeys = append(st.sortKeys, sortKey{k, d < 0})
		}
		return st, nil
	}
	arg, err := decodeAny(body)
	if err != nil {
		return stage{}, err
	}
	switch op {
	case "$match":
		m, ok := arg.(map[string]any)
		if !ok {
			return stage{}, errors.New("$match needs an object")
		}
		preds, err := parseMatch(m)
		return stage{kind: stMatch, preds: preds}, err
	case "$group":
		m, ok := arg.(map[string]any)
		if !ok {
			return stage{}, errors.New("$group needs an object")
		}
		g, err := parseGroup(m)
		return stage{kind: stGroup, group: g}, err
	case "$skip", "$limit":
		n, ok := asInt(arg)
		if !ok || n < 0 {
			return stage{}, errors.New(op + " needs a non-negative integer")
		}
		k := stSkip
		if op == "$limit" {
			k = stLimit
		}
		return stage{kind: k, n: n}, nil
	case "$project":
		m, ok := arg.(map[string]any)
		if !ok || len(m) == 0 {
			return stage{}, errors.New("$project needs a non-empty object")
		}
		inc := map[string]bool{}
		for k, v := range m {
			switch x := v.(type) {
			case bool:
				inc[k] = x
			case json.Number:
				i, _ := x.Int64()
				inc[k] = i != 0
			default:
				return stage{}, errors.New("$project values must be 0/1 or booleans")
			}
		}
		return stage{kind: stProject, project: inc}, nil
	case "$count":
		s, ok := arg.(string)
		if !ok || s == "" {
			return stage{}, errors.New("$count needs a field name")
		}
		return stage{kind: stCount, countName: s}, nil
	case "$lookup":
		m, ok := arg.(map[string]any)
		if !ok {
			return stage{}, errors.New("$lookup needs an object")
		}
		if m["pipeline"] != nil || m["let"] != nil {
			return stage{}, errors.New("$lookup supports only the localField/foreignField form")
		}
		from, hasFrom := m["from"].(string)
		lf, _ := m["localField"].(string)
		ff, _ := m["foreignField"].(string)
		as, _ := m["as"].(string)
		if !hasFrom || lf == "" || ff == "" || as == "" {
			return stage{}, errors.New("$lookup needs from, localField, foreignField and as")
		}
		return stage{kind: stLookup, lookup: &lookupSpec{from, lf, ff, as}}, nil
	case "$unwind":
		switch x := arg.(type) {
		case string:
			f, ok := fieldRef(x)
			if !ok {
				return stage{}, errors.New("$unwind needs a \"$field\" path")
			}
			return stage{kind: stUnwind, unwind: f}, nil
		case map[string]any:
			f, ok := fieldRef(x["path"])
			if !ok {
				return stage{}, errors.New("$unwind needs a \"$field\" path")
			}
			pres, _ := x["preserveNullAndEmptyArrays"].(bool)
			return stage{kind: stUnwind, unwind: f, preserve: pres}, nil
		}
		return stage{}, errors.New("$unwind needs a path")
	}
	return stage{}, fmt.Errorf("unsupported stage %s", op)
}

func parseMatch(m map[string]any) ([]pred, error) {
	var preds []pred
	for field, cond := range m {
		ops, isOps := cond.(map[string]any)
		if !isOps {
			preds = append(preds, pred{field: field, op: opEq, v: valueOf(cond)})
			continue
		}
		for name, operand := range ops {
			var op predOp
			switch name {
			case "$eq":
				op = opEq
			case "$ne":
				op = opNe
			case "$gt":
				op = opGt
			case "$gte":
				op = opGte
			case "$lt":
				op = opLt
			case "$lte":
				op = opLte
			case "$in":
				list, ok := operand.([]any)
				if !ok {
					return nil, errors.New("$in needs an array")
				}
				p := pred{field: field, op: opIn}
				for _, x := range list {
					p.in = append(p.in, valueOf(x))
				}
				preds = append(preds, p)
				continue
			default:
				return nil, fmt.Errorf("unsupported $match operator %s", name)
			}
			preds = append(preds, pred{field: field, op: op, v: valueOf(operand)})
		}
	}
	return preds, nil
}

func parseGroup(m map[string]any) (*groupSpec, error) {
	idRaw, ok := m["_id"]
	if !ok {
		return nil, errors.New("$group needs _id")
	}
	g := &groupSpec{}
	switch id := idRaw.(type) {
	case nil:
	case string:
		f, ok := fieldRef(id)
		if !ok {
			return nil, errors.New("_id must be null, \"$field\" or an object of \"$field\" refs")
		}
		g.idFields = []string{f}
	case map[string]any:
		names := make([]string, 0, len(id))
		for k := range id {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			f, ok := fieldRef(id[k])
			if !ok {
				return nil, errors.New("composite _id values must be \"$field\" refs")
			}
			g.idNames = append(g.idNames, k)
			g.idFields = append(g.idFields, f)
		}
	default:
		return nil, errors.New("_id must be null, \"$field\" or an object")
	}
	names := make([]string, 0, len(m))
	for k := range m {
		if k != "_id" {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		spec, ok := m[name].(map[string]any)
		if !ok || len(spec) != 1 {
			return nil, fmt.Errorf("accumulator %s needs {$op: operand}", name)
		}
		for opName, operand := range spec {
			a := accSpec{name: name}
			switch opName {
			case "$sum":
				a.op = accSum
			case "$avg":
				a.op = accAvg
			case "$min":
				a.op = accMin
			case "$max":
				a.op = accMax
			case "$count":
				a.op = accCount
			case "$first":
				a.op = accFirst
			case "$last":
				a.op = accLast
			default:
				return nil, fmt.Errorf("unsupported accumulator %s", opName)
			}
			if f, ok := fieldRef(operand); ok {
				a.field = f
			} else if a.op == accCount {
				a.c = value{k: kInt, i: 1}
			} else {
				a.c = valueOf(operand)
				if a.c.k == kMissing || a.c.k == kString {
					return nil, fmt.Errorf("accumulator %s needs a \"$field\" ref or a number", name)
				}
			}
			g.accs = append(g.accs, a)
		}
	}
	return g, nil
}

// ---- predicates

func (p *pred) eval(v value) bool {
	if v.k == kArray {
		// an array field matches when any element does ($ne: when none does)
		if p.op == opNe {
			eq := pred{op: opEq, v: p.v}
			return !eq.eval(v)
		}
		hit := false
		walkArray(v.s, func(e value) bool {
			if p.evalScalar(e) {
				hit = true
				return false
			}
			return true
		})
		return hit || p.evalScalar(v)
	}
	return p.evalScalar(v)
}

func (p *pred) evalScalar(v value) bool {
	if p.op >= opGt && p.op <= opLte && v.isNum() && p.v.isNum() {
		var c int
		if v.k == kInt && p.v.k == kInt {
			switch {
			case v.i < p.v.i:
				c = -1
			case v.i > p.v.i:
				c = 1
			}
		} else {
			x, y := v.num(), p.v.num()
			switch {
			case x < y:
				c = -1
			case x > y:
				c = 1
			}
		}
		switch p.op {
		case opGt:
			return c > 0
		case opGte:
			return c >= 0
		case opLt:
			return c < 0
		}
		return c <= 0
	}
	switch p.op {
	case opEq:
		return compareValues(v, p.v) == 0
	case opNe:
		return compareValues(v, p.v) != 0
	case opIn:
		for i := range p.in {
			if compareValues(v, p.in[i]) == 0 {
				return true
			}
		}
		return false
	}
	if rank(v) != rank(p.v) || v.k == kMissing || v.k == kNull {
		return false
	}
	c := compareValues(v, p.v)
	switch p.op {
	case opGt:
		return c > 0
	case opGte:
		return c >= 0
	case opLt:
		return c < 0
	}
	return c <= 0
}

func matchAll(preds []pred, get func(string) value) bool {
	for i := range preds {
		if !preds[i].eval(get(preds[i].field)) {
			return false
		}
	}
	return true
}

// ---- grouping

type scalar struct {
	k kind
	i int64
	f float64
	s string
}

func toScalar(v value) scalar { return scalar{k: v.k, i: v.i, f: v.f, s: string(v.s)} }
func (s scalar) view() value  { return value{k: s.k, i: s.i, f: s.f, s: []byte(s.s)} }
func (s scalar) any() any     { return s.view().any() }

type accState struct {
	isum     int64
	fsum     float64
	hasFloat bool
	count    int64
	ext      scalar
	hasExt   bool
}

type groupState struct {
	key  string // owned encoded key, also backing the string/document ID parts
	id   []scalar
	accs []accState
}

type grouper struct {
	spec   *groupSpec
	groups map[string]*groupState
	single *groupState // the one group when _id is null
	order  []*groupState
	keyBuf []byte
	max    int
	states []groupState
	ids    []scalar
	accs   []accState
	used   int
}

// Groups own query-local slabs: addresses remain stable through shard merges,
// while large cardinalities do not allocate three objects for every group.
func (g *grouper) allocGroup() *groupState {
	if g.used == len(g.states) {
		n := 256
		if g.states == nil {
			n = 8
		}
		g.states = make([]groupState, n)
		g.ids = make([]scalar, n*len(g.spec.idFields))
		g.accs = make([]accState, n*len(g.spec.accs))
		g.used = 0
	}
	i := g.used
	g.used++
	gs := &g.states[i]
	ni, na := len(g.spec.idFields), len(g.spec.accs)
	gs.id = g.ids[i*ni : (i+1)*ni : (i+1)*ni]
	gs.accs = g.accs[i*na : (i+1)*na : (i+1)*na]
	return gs
}

func newGrouper(spec *groupSpec, maxGroups int) *grouper {
	return &grouper{spec: spec, groups: map[string]*groupState{}, max: maxGroups}
}

// add groups one record given a field getter (used for materialised rows).
func (g *grouper) add(get func(string) value) error {
	spec := g.spec
	vals := make([]value, len(spec.idFields)+len(spec.accs))
	idIdx := make([]int, len(spec.idFields))
	accIdx := make([]int, len(spec.accs))
	for i, f := range spec.idFields {
		vals[i] = get(f)
		idIdx[i] = i
	}
	for i, a := range spec.accs {
		if a.field == "" {
			accIdx[i] = -1
			continue
		}
		j := len(spec.idFields) + i
		vals[j] = get(a.field)
		accIdx[i] = j
	}
	return g.addVals(vals, idIdx, accIdx)
}

// addVals groups one record whose needed fields are already extracted:
// vals[idIdx[i]] is _id part i, vals[accIdx[j]] is accumulator j's operand
// (accIdx -1 means the accumulator's constant).
func (g *grouper) addVals(vals []value, idIdx, accIdx []int) error {
	spec := g.spec
	var gs *groupState
	if len(idIdx) == 0 {
		if g.single == nil {
			g.single = &groupState{accs: make([]accState, len(spec.accs))}
			g.order = append(g.order, g.single)
		}
		gs = g.single
	} else {
		var err error
		if gs, err = g.lookup(vals, idIdx); err != nil {
			return err
		}
	}
	for i := range spec.accs {
		a := &spec.accs[i]
		v := a.c
		if accIdx[i] >= 0 {
			v = vals[accIdx[i]]
		}
		st := &gs.accs[i]
		switch a.op {
		case accSum, accAvg:
			switch v.k {
			case kInt:
				st.isum += v.i
				st.count++
			case kFloat:
				st.fsum += v.f
				st.hasFloat = true
				st.count++
			}
		case accCount:
			st.count++
		case accMin, accMax:
			if v.k == kMissing || v.k == kNull {
				continue
			}
			if !st.hasExt {
				st.ext, st.hasExt = toScalar(v), true
				continue
			}
			c := compareValues(v, st.ext.view())
			if (a.op == accMin && c < 0) || (a.op == accMax && c > 0) {
				st.ext = toScalar(v)
			}
		case accFirst:
			if !st.hasExt {
				st.ext, st.hasExt = toScalar(v), true
			}
		case accLast:
			st.ext, st.hasExt = toScalar(v), true
		}
	}
	return nil
}

// lookup finds or creates the group for the _id built from vals.
func (g *grouper) lookup(vals []value, idIdx []int) (*groupState, error) {
	kb := g.keyBuf[:0]
	for _, ix := range idIdx {
		v := vals[ix]
		kb = append(kb, byte(v.k))
		switch v.k {
		case kBool, kInt:
			kb = le.AppendUint64(kb, uint64(v.i))
		case kFloat:
			kb = le.AppendUint64(kb, math.Float64bits(v.f))
		case kString, kDoc, kArray:
			kb = le.AppendUint16(kb, uint16(len(v.s)))
			kb = append(kb, v.s...)
		}
	}
	g.keyBuf = kb
	gs := g.groups[string(kb)]
	if gs == nil {
		if len(g.groups) >= g.max {
			return nil, fmt.Errorf("%w: more than %d groups", ErrLimit, g.max)
		}
		gs = g.allocGroup()
		gs.key = string(kb)
		off := 0
		for i, ix := range idIdx {
			v := vals[ix]
			s := scalar{k: v.k, i: v.i, f: v.f}
			switch v.k {
			case kBool, kInt, kFloat:
				off += 9
			case kString, kDoc, kArray:
				s.s = gs.key[off+3 : off+3+len(v.s)]
				off += 3 + len(v.s)
			default:
				off++
			}
			gs.id[i] = s
		}
		g.groups[gs.key] = gs
		g.order = append(g.order, gs)
	}
	return gs, nil
}

func (g *grouper) rows() []map[string]any {
	out := make([]map[string]any, 0, len(g.order))
	for _, gs := range g.order {
		out = append(out, g.row(gs))
	}
	return out
}

func (g *grouper) row(gs *groupState) map[string]any {
	spec := g.spec
	row := make(map[string]any, len(spec.accs)+1)
	switch {
	case len(spec.idFields) == 0:
		row["_id"] = nil
	case spec.idNames == nil:
		row["_id"] = gs.id[0].any()
	default:
		id := make(map[string]any, len(spec.idNames))
		for i, n := range spec.idNames {
			id[n] = gs.id[i].any()
		}
		row["_id"] = id
	}
	for i := range spec.accs {
		row[spec.accs[i].name] = accValue(&spec.accs[i], &gs.accs[i]).any()
	}
	return row
}

func accValue(a *accSpec, st *accState) value {
	switch a.op {
	case accSum:
		if st.hasFloat {
			return value{k: kFloat, f: st.fsum + float64(st.isum)}
		}
		return value{k: kInt, i: st.isum}
	case accAvg:
		if st.count > 0 {
			return value{k: kFloat, f: (st.fsum + float64(st.isum)) / float64(st.count)}
		}
	case accCount:
		return value{k: kInt, i: st.count}
	default:
		if st.hasExt {
			return st.ext.view()
		}
	}
	return value{k: kNull}
}

// merge folds o (which covered keys after g's range) into g.
func (g *grouper) merge(o *grouper) error {
	if o.single != nil {
		if g.single == nil {
			g.single = o.single
			g.order = append(g.order, o.single)
		} else {
			mergeState(g.spec, g.single, o.single)
		}
	}
	// Iterate encounter order, rather than map order, so stable sort ties and
	// first/last remain consistent between serial and sharded execution.
	for _, os := range o.order {
		if os == o.single {
			continue
		}
		k := os.key
		gs := g.groups[k]
		if gs == nil {
			if len(g.groups) >= g.max {
				return fmt.Errorf("%w: more than %d groups", ErrLimit, g.max)
			}
			g.groups[k] = os
			g.order = append(g.order, os)
			continue
		}
		mergeState(g.spec, gs, os)
	}
	return nil
}

func mergeState(spec *groupSpec, a, b *groupState) {
	for i := range spec.accs {
		x, y := &a.accs[i], &b.accs[i]
		switch spec.accs[i].op {
		case accSum, accAvg, accCount:
			x.isum += y.isum
			x.fsum += y.fsum
			x.hasFloat = x.hasFloat || y.hasFloat
			x.count += y.count
		case accMin, accMax:
			if !y.hasExt {
				continue
			}
			if !x.hasExt {
				x.ext, x.hasExt = y.ext, true
				continue
			}
			c := compareValues(y.ext.view(), x.ext.view())
			if (spec.accs[i].op == accMin && c < 0) || (spec.accs[i].op == accMax && c > 0) {
				x.ext = y.ext
			}
		case accFirst:
			if !x.hasExt && y.hasExt {
				x.ext, x.hasExt = y.ext, true
			}
		case accLast:
			if y.hasExt {
				x.ext, x.hasExt = y.ext, true
			}
		}
	}
}

// ---- execution

// plan is the analysed streaming prefix of a pipeline: leading $match
// stages, a $skip/$limit window, an optional $lookup+$unwind join with its
// own $match stages, an optional $group, and the fields they all need.
// Indices refer to one vector: local fields first, then the joined
// document's fields, then one slot standing for the joined document itself.
type plan struct {
	preds         []pred
	predIdx       []int
	skip, limit   int
	need          []string // local fields
	paths         []fieldPath
	join          *joinPlan
	group         *groupSpec
	idIdx, accIdx []int
	nvals         int
	next          int // first stage that runs on materialised rows
	limits        Limits
	countOnly     bool // the group stands for a trailing $count
}

// joinPlan is a streaming $lookup: each source document is joined with
// the foreign documents it matches, one output record per match, or one
// with the foreign fields missing when preserve is set and nothing matched.
type joinPlan struct {
	spec     *lookupSpec
	localIdx int      // index of localField among the local fields
	fneed    []string // foreign fields needed, paths relative to the foreign document
	fpaths   []fieldPath
	preserve bool
	post     []pred // $match stages after the join
	postIdx  []int
	wholeIdx int // vector slot standing for the whole joined document, or -1
}

// ref locates a field of the combined vector before its layout is final.
type ref struct {
	kind uint8 // 0 local, 1 foreign, 2 whole joined document
	i    int
}

func (p *Pipeline) plan() plan {
	pl := plan{limit: -1, limits: p.limits}
	i := 0
	for ; i < len(p.stages); i++ {
		s := &p.stages[i]
		if s.kind == stMatch && pl.skip == 0 && pl.limit < 0 {
			pl.preds = append(pl.preds, s.preds...)
			continue
		}
		if s.kind == stSkip {
			pl.skip += s.n
			if pl.limit >= 0 {
				pl.limit = max(pl.limit-s.n, 0)
			}
			continue
		}
		if s.kind == stLimit {
			if pl.limit < 0 || s.n < pl.limit {
				pl.limit = s.n
			}
			continue
		}
		break
	}
	var fneed []string
	index := func(list *[]string, f string) int {
		for j, n := range *list {
			if n == f {
				return j
			}
		}
		*list = append(*list, f)
		return len(*list) - 1
	}
	local := func(f string) ref { return ref{0, index(&pl.need, f)} }
	predRefs := make([]ref, len(pl.preds))
	for j := range pl.preds {
		predRefs[j] = local(pl.preds[j].field)
	}
	// a $lookup unwound on its own field joins in the stream
	var jp *joinPlan
	resolve := local
	if i+1 < len(p.stages) && p.stages[i].kind == stLookup && p.stages[i+1].kind == stUnwind && p.stages[i+1].unwind == p.stages[i].lookup.as {
		spec := p.stages[i].lookup
		jp = &joinPlan{spec: spec, preserve: p.stages[i+1].preserve, wholeIdx: -1}
		jp.localIdx = index(&pl.need, spec.localField)
		as := spec.as
		resolve = func(f string) ref {
			switch {
			case f == as:
				return ref{2, 0}
			case strings.HasPrefix(f, as+"."):
				return ref{1, index(&fneed, f[len(as)+1:])}
			}
			return local(f)
		}
		i += 2
	}
	var postRefs []ref
	if jp != nil {
		for ; i < len(p.stages) && p.stages[i].kind == stMatch; i++ {
			for _, pr := range p.stages[i].preds {
				jp.post = append(jp.post, pr)
				postRefs = append(postRefs, resolve(pr.field))
			}
		}
	}
	var idRefs, accRefs []ref
	if i < len(p.stages) && p.stages[i].kind == stGroup {
		g := p.stages[i].group
		ok := true
		for _, f := range g.idFields {
			r := resolve(f)
			ok = ok && r.kind != 2 // grouping by the whole joined document: materialise
			idRefs = append(idRefs, r)
		}
		for _, a := range g.accs {
			if a.field == "" {
				accRefs = append(accRefs, ref{0, -1})
				continue
			}
			r := resolve(a.field)
			ok = ok && r.kind != 2
			accRefs = append(accRefs, r)
		}
		if ok {
			pl.group = g
			i++
		} else {
			idRefs, accRefs = nil, nil
		}
	}
	if pl.group == nil && i < len(p.stages) && p.stages[i].kind == stCount {
		// a $count is a one-group count: it streams, and it shards
		pl.group = &groupSpec{accs: []accSpec{{name: p.stages[i].countName, op: accCount, c: value{k: kInt, i: 1}}}}
		accRefs = []ref{{0, -1}}
		pl.countOnly = true
		i++
	}
	pl.next = i
	pl.paths = splitPaths(pl.need)
	// final layout
	fbase := len(pl.need)
	whole := fbase + len(fneed)
	pl.nvals = fbase + len(fneed)
	fix := func(r ref) int {
		switch r.kind {
		case 1:
			return fbase + r.i
		case 2:
			return whole
		}
		return r.i
	}
	pl.predIdx = make([]int, len(predRefs))
	for j, r := range predRefs {
		pl.predIdx[j] = fix(r)
	}
	if jp != nil {
		jp.fneed = fneed
		jp.fpaths = splitPaths(fneed)
		jp.postIdx = make([]int, len(postRefs))
		for j, r := range postRefs {
			jp.postIdx[j] = fix(r)
			if r.kind == 2 {
				jp.wholeIdx = whole
			}
		}
		if jp.wholeIdx >= 0 {
			pl.nvals++
		}
		pl.join = jp
	}
	if pl.group != nil {
		for _, r := range idRefs {
			pl.idIdx = append(pl.idIdx, fix(r))
		}
		for _, r := range accRefs {
			if r.i < 0 && r.kind == 0 {
				pl.accIdx = append(pl.accIdx, -1)
			} else {
				pl.accIdx = append(pl.accIdx, fix(r))
			}
		}
	}
	return pl
}

// extractFields fills vals[j] with the field at path need[j]. hints[j] is
// the entry index where the top-level field was found in the previous
// document; documents built the same way share a layout, so the hint
// usually hits and no other entry is touched.
func extractFields(doc []byte, need []fieldPath, vals []value, hints []int) {
	for j := range vals {
		vals[j] = value{}
	}
	n, dir, ok := docDir(doc)
	if !ok {
		return
	}
	for j := range need {
		want, rest := need[j].top, need[j].rest
		var v value
		found := false
		if h := hints[j]; h < n {
			name, hv, ok := decodeEntry(doc, int(le.Uint16(dir[2*h:])))
			if ok && len(name) == len(want) && string(name) == want {
				v, found = hv, true
			}
		}
		if !found {
			for i := 0; i < n; i++ {
				off := int(le.Uint16(dir[2*i:]))
				if off >= len(doc) {
					break
				}
				nl := int(doc[off])
				if nl != len(want) || off+1+nl > len(doc) || (nl > 0 && doc[off+1] != want[0]) || string(doc[off+1:off+1+nl]) != want {
					continue
				}
				if _, hv, ok := decodeEntry(doc, off); ok {
					v, found = hv, true
					hints[j] = i
				}
				break
			}
		}
		if found && rest != "" {
			v = pathValue(v, rest)
		}
		if found {
			vals[j] = v
		}
	}
}

// shards reports whether the streaming prefix can run on key-range shards
// concurrently: a group (or count) with no window depending on order.
func (pl *plan) shards() bool { return pl.group != nil && pl.skip == 0 && pl.limit < 0 }

func (pl *plan) matches(vals []value) bool {
	for j := range pl.preds {
		if !pl.preds[j].eval(vals[pl.predIdx[j]]) {
			return false
		}
	}
	return true
}

func (jp *joinPlan) postMatches(vals []value) bool {
	for j := range jp.post {
		if !jp.post[j].eval(vals[jp.postIdx[j]]) {
			return false
		}
	}
	return true
}

// rec is one record of the streaming prefix: the extracted field vector,
// the source document, and the joined document when a materialising run
// asked for it.
type rec struct {
	vals []value
	doc  []byte
	fdoc []byte
}

// stream runs the streaming prefix over one source, calling emit for each
// record that survives the predicates, the window and the join.
//
// pre is a join executor prepared by the caller (a sharded run builds one
// table for all shards); nil lets the stream prepare its own.
func (pl *plan) stream(ctx context.Context, spec srcSpec, e *env, keepDoc bool, pre *lookupExec, emit func(r *rec) error) error {
	vals := make([]value, pl.nvals)
	hints := make([]int, len(pl.need))
	lk := pre
	if pl.join != nil && lk == nil {
		var err error
		if lk, err = e.lookup(pl.join.spec, pl.join.fneed, keepDoc, pl.limit, pl.limits); err != nil {
			return err
		}
		if err = lk.prepare(pl.preScan(ctx, spec.source())); err != nil {
			return err
		}
	}
	seen, emitted, visited := 0, 0, 0
	var eerr error
	r := rec{vals: vals}
	process := func(val []byte) bool {
		if visited++; visited&4095 == 0 {
			if eerr = ctx.Err(); eerr != nil {
				return false
			}
		}
		if _, _, ok := docDir(val); !ok {
			return true // not a document: invisible to pipelines
		}
		extractFields(val, pl.paths, vals[:len(pl.need)], hints)
		if !pl.matches(vals) {
			return true
		}
		seen++
		if seen <= pl.skip {
			return true
		}
		if pl.limit >= 0 && emitted >= pl.limit {
			return false
		}
		emitted++
		r.doc = val
		more := pl.limit < 0 || emitted < pl.limit
		if pl.join == nil {
			r.fdoc = nil
			eerr = emit(&r)
			return eerr == nil && more
		}
		jp := pl.join
		base := len(pl.need)
		matched := 0
		eerr = lk.probe(vals[jp.localIdx], func(fr *frow) bool {
			matched++
			copy(vals[base:], fr.vals)
			if jp.wholeIdx >= 0 {
				vals[jp.wholeIdx] = value{k: kDoc}
			}
			if !jp.postMatches(vals) {
				return true
			}
			r.fdoc = fr.doc
			eerr = emit(&r)
			return eerr == nil
		})
		// $unwind with preserveNullAndEmptyArrays: an unmatched document
		// passes through once with the joined field missing
		if eerr == nil && matched == 0 && jp.preserve {
			for k := range jp.fneed {
				vals[base+k] = value{}
			}
			if jp.wholeIdx >= 0 {
				vals[jp.wholeIdx] = value{}
			}
			if jp.postMatches(vals) {
				r.fdoc = nil
				eerr = emit(&r)
			}
		}
		return eerr == nil && more
	}
	// A collection range with nothing buffered is read leaf by leaf in
	// place, under the reader lock, when no step of the pass uses the
	// database itself (an index-probing join does).
	if spec.ranged && (lk == nil || !lk.useIdx) {
		if err := spec.c.check(); err != nil {
			return err
		}
		lo := spec.c.ikey(nil, spec.lo)
		hi := spec.c.end
		if spec.hi != nil {
			hi = spec.c.ikey(nil, spec.hi)
		}
		var ovf []byte
		var serr error
		err := spec.c.db.scanLeaves(spec.c.view(), lo, hi, func(lv leafView) bool {
			for c := lv.i; c < lv.j; c++ {
				val := lv.p.leafVal(c)
				if lv.p.leafIsOverflow(c) {
					full, err := lv.value(c, ovf)
					if err != nil {
						serr = err
						return false
					}
					ovf, val = full, full
				}
				if !process(val) {
					return false
				}
			}
			return true
		})
		if err != errNeedMerge {
			if err == nil {
				err = serr
			}
			if err == nil {
				err = eerr
			}
			return err
		}
	}
	err := spec.source()(func(_, val []byte) bool { return process(val) })
	if err == nil {
		err = eerr
	}
	return err
}

// streamGroup runs the streaming prefix plus $group over one source.
func (pl *plan) streamGroup(ctx context.Context, spec srcSpec, e *env, pre *lookupExec) (*grouper, error) {
	g := newGrouper(pl.group, pl.limits.MaxGroups)
	err := pl.stream(ctx, spec, e, false, pre, func(r *rec) error { return g.addVals(r.vals, pl.idIdx, pl.accIdx) })
	return g, err
}

// streamRows runs the streaming prefix over one source, decoding survivors.
func (pl *plan) streamRows(ctx context.Context, spec srcSpec, e *env) ([]map[string]any, error) {
	var rows []map[string]any
	err := pl.stream(ctx, spec, e, true, nil, func(r *rec) error {
		if len(rows) >= pl.limits.MaxRows {
			return fmt.Errorf("%w: more than %d rows", ErrLimit, pl.limits.MaxRows)
		}
		m, err := DecodeDocument(r.doc)
		if err != nil {
			return err
		}
		if pl.join != nil && r.fdoc != nil {
			fm, err := DecodeDocument(r.fdoc)
			if err != nil {
				return err
			}
			setRowField(m, pl.join.spec.as, fm)
		}
		rows = append(rows, m)
		return nil
	})
	return rows, err
}

func (p *Pipeline) finish(ctx context.Context, pl *plan, rows []map[string]any, e *env) ([]map[string]any, error) {
	if pl.countOnly {
		name := pl.group.accs[0].name
		if len(rows) == 0 {
			rows = []map[string]any{{name: int64(0)}}
		} else {
			delete(rows[0], "_id")
		}
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	var err error
	for i := pl.next; i < len(p.stages); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if end, keep, skip, ok := p.sortWindow(i); ok {
			selected, valid, sortErr := topRows(ctx, rows, p.stages[i].sortKeys, keep, skip)
			if sortErr != nil {
				return nil, sortErr
			}
			if valid {
				rows = selected
				i = end
				continue
			}
		}
		if rows, err = applyRows(&p.stages[i], rows, pl.limits, e); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// Run executes the pipeline over src on the calling goroutine. A $lookup
// stage needs a collection to resolve "from" against: use Collection.Run.
//
// ctx cancels a long run: the pass checks it every few thousand records and
// returns ctx.Err().
func (p *Pipeline) Run(ctx context.Context, src Source) ([]map[string]any, error) {
	ctx = orBackground(ctx)
	return p.run(ctx, srcSpec{src: src}, &env{ctx: ctx, count: -1})
}

// orBackground treats a nil context as Background.
func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (p *Pipeline) run(ctx context.Context, spec srcSpec, e *env) ([]map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pl := p.plan()
	if pl.group != nil {
		g, err := pl.streamGroup(ctx, spec, e, nil)
		if err != nil {
			return nil, err
		}
		return p.finishGroup(ctx, &pl, g, e)
	}
	rows, err := pl.streamRows(ctx, spec, e)
	if err != nil {
		return nil, err
	}
	return p.finish(ctx, &pl, rows, e)
}

// RunSharded executes the pipeline over key-ordered shards concurrently when
// the streaming prefix is a $match/$group (no $skip/$limit), merging the
// per-shard groups. Other pipelines run serially over the shards in order.
func (p *Pipeline) RunSharded(ctx context.Context, shards []Source) ([]map[string]any, error) {
	ctx = orBackground(ctx)
	specs := make([]srcSpec, len(shards))
	for i, s := range shards {
		specs[i] = srcSpec{src: s}
	}
	return p.runSharded(ctx, specs, &env{ctx: ctx, count: -1})
}

// concat chains shard sources into one, in order.
func concat(shards []srcSpec) Source {
	return func(yield func(k, v []byte) bool) error {
		for _, s := range shards {
			stop := false
			err := s.source()(func(k, v []byte) bool {
				if !yield(k, v) {
					stop = true
					return false
				}
				return true
			})
			if err != nil || stop {
				return err
			}
		}
		return nil
	}
}

func (p *Pipeline) runSharded(ctx context.Context, shards []srcSpec, e *env) ([]map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return []map[string]any{}, nil
	}
	pl := p.plan()
	if len(shards) == 1 {
		return p.run(ctx, shards[0], e)
	}
	if !pl.shards() {
		return p.run(ctx, srcSpec{src: concat(shards)}, e)
	}
	var shared *lookupExec
	if pl.join != nil {
		// build the join table once, before the shards race for it; the
		// pre-pass looks at the shards in order
		lk, err := e.lookup(pl.join.spec, pl.join.fneed, false, -1, pl.limits)
		if err != nil {
			return nil, err
		}
		shared = lk
		if err := lk.prepare(pl.preScan(ctx, concat(shards))); err != nil {
			return nil, err
		}
	}
	groupers := make([]*grouper, len(shards))
	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		var pre *lookupExec
		if shared != nil {
			pre = shared.clone()
		}
		go func(i int, pre *lookupExec) {
			defer wg.Done()
			groupers[i], errs[i] = pl.streamGroup(ctx, shards[i], e, pre)
		}(i, pre)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	// merge adjacent pairs in rounds, each round in parallel (key-range
	// order is kept, which $first, $last and the row order rely on): with
	// many groups the merge is a large part of the work
	for len(groupers) > 1 {
		half := (len(groupers) + 1) / 2
		merr := make([]error, half)
		next := make([]*grouper, half)
		var mg sync.WaitGroup
		for j := 0; j < half; j++ {
			next[j] = groupers[2*j]
			if 2*j+1 >= len(groupers) {
				continue
			}
			mg.Add(1)
			go func(j int) {
				defer mg.Done()
				merr[j] = groupers[2*j].merge(groupers[2*j+1])
			}(j)
		}
		mg.Wait()
		for _, err := range merr {
			if err != nil {
				return nil, err
			}
		}
		groupers = next
	}
	return p.finishGroup(ctx, &pl, groupers[0], e)
}

func applyRows(s *stage, rows []map[string]any, limits Limits, e *env) ([]map[string]any, error) {
	switch s.kind {
	case stLookup:
		lk, err := e.lookup(s.lookup, nil, true, len(rows), limits)
		if err != nil {
			return nil, err
		}
		// the outer side is in hand: a small one gets a filtered table
		pre := func() ([]value, bool) {
			if len(rows) > smallOuter {
				return nil, false
			}
			keys := make([]value, 0, len(rows))
			for _, r := range rows {
				keys = append(keys, rowField(r, s.lookup.localField))
			}
			return keys, true
		}
		if err := lk.prepare(pre); err != nil {
			return nil, err
		}
		for _, r := range rows {
			list := []any{}
			err := lk.probe(rowField(r, s.lookup.localField), func(fr *frow) bool {
				m, err := DecodeDocument(fr.doc)
				if err == nil {
					list = append(list, m)
				}
				return true
			})
			if err != nil {
				return nil, err
			}
			setRowField(r, s.lookup.as, list)
		}
		return rows, nil
	case stMatch:
		out := rows[:0]
		for _, r := range rows {
			if matchAll(s.preds, func(f string) value { return rowField(r, f) }) {
				out = append(out, r)
			}
		}
		return out, nil
	case stGroup:
		g := newGrouper(s.group, limits.MaxGroups)
		for _, r := range rows {
			if err := g.add(func(f string) value { return rowField(r, f) }); err != nil {
				return nil, err
			}
		}
		return g.rows(), nil
	case stSort:
		type keyed struct {
			ks  []value
			row map[string]any
		}
		ks := make([]keyed, len(rows))
		for i, r := range rows {
			k := keyed{row: r, ks: make([]value, len(s.sortKeys))}
			for j, sk := range s.sortKeys {
				k.ks[j] = rowField(r, sk.field)
			}
			ks[i] = k
		}
		sort.SliceStable(ks, func(a, b int) bool {
			for j, sk := range s.sortKeys {
				c := compareValues(ks[a].ks[j], ks[b].ks[j])
				if c == 0 {
					continue
				}
				if sk.desc {
					return c > 0
				}
				return c < 0
			}
			return false
		})
		for i := range ks {
			rows[i] = ks[i].row
		}
		return rows, nil
	case stSkip:
		if s.n >= len(rows) {
			return rows[:0], nil
		}
		return rows[s.n:], nil
	case stLimit:
		if s.n < len(rows) {
			return rows[:s.n], nil
		}
		return rows, nil
	case stProject:
		inclusion := false
		for k, v := range s.project {
			if v && k != "_id" {
				inclusion = true
			}
		}
		out := make([]map[string]any, len(rows))
		for i, r := range rows {
			m := make(map[string]any, len(r))
			for k, v := range r {
				keep := true
				if inclusion {
					keep = s.project[k]
					if k == "_id" {
						keep = true
						if x, ok := s.project["_id"]; ok {
							keep = x
						}
					}
				} else if x, ok := s.project[k]; ok && !x {
					keep = false
				}
				if keep {
					m[k] = v
				}
			}
			out[i] = m
		}
		return out, nil
	case stCount:
		return []map[string]any{{s.countName: int64(len(rows))}}, nil
	case stUnwind:
		var out []map[string]any
		for _, r := range rows {
			v := rowField(r, s.unwind)
			switch {
			case v.k == kArray:
				var elems []any
				walkArray(v.s, func(e value) bool { elems = append(elems, e.any()); return true })
				if len(elems) == 0 && s.preserve {
					out = append(out, r)
				}
				for _, e := range elems {
					if len(out) >= limits.MaxRows {
						return nil, fmt.Errorf("%w: more than %d rows", ErrLimit, limits.MaxRows)
					}
					c := make(map[string]any, len(r))
					for k, x := range r {
						c[k] = x
					}
					setRowField(c, s.unwind, e)
					out = append(out, c)
				}
			case v.k == kMissing || v.k == kNull:
				if s.preserve {
					out = append(out, r)
				}
			default:
				out = append(out, r) // a scalar unwinds to itself
			}
		}
		if out == nil {
			out = []map[string]any{}
		}
		return out, nil
	}
	return rows, nil
}

// Aggregate runs a JSON pipeline over the default collection on one
// goroutine, through a secondary index when the leading $match allows it.
func (db *DB) Aggregate(ctx context.Context, pipeline []byte) ([]map[string]any, error) {
	return db.def.Aggregate(ctx, pipeline)
}

// AggregateParallel runs a JSON pipeline over the default collection in up
// to shards key ranges at once.
func (db *DB) AggregateParallel(ctx context.Context, pipeline []byte, shards int) ([]map[string]any, error) {
	return db.def.AggregateParallel(ctx, pipeline, shards)
}
