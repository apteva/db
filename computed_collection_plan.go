package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
)

var (
	ErrComputedCollection           = errors.New("aptevadb: invalid computed collection definition or value")
	ErrComputedCollectionReadOnly   = errors.New("aptevadb: computed collection is maintained by its source")
	ErrComputedCollectionExists     = errors.New("aptevadb: computed collection destination already exists")
	ErrComputedCollectionDependency = errors.New("aptevadb: collection has computed dependents")
	ErrComputedCollectionLimit      = errors.New("aptevadb: computed collection staging exceeds 256 MiB")
)

const computedCollectionLimit = 256 << 20

type computedCollectionPlan struct {
	source, target *Collection
	pipeline       json.RawMessage
	preds          []pred
	project        map[string]bool
	inclusion      bool
	group          *groupSpec
}

func compileComputedCollection(src []byte) (*computedCollectionPlan, error) {
	raw, err := computedJSON(src)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrComputedCollection, err)
	}
	array, ok := raw.([]any)
	if !ok || len(array) > 16 {
		return nil, fmt.Errorf("%w: expected at most 16 pipeline stages", ErrComputedCollection)
	}
	canonical, err := json.Marshal(raw)
	if err != nil || len(canonical) > 65536 {
		return nil, ErrComputedCollection
	}
	pipeline, err := ParsePipeline(canonical)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrComputedCollection, err)
	}
	p := &computedCollectionPlan{pipeline: canonical}
	for i, st := range pipeline.stages {
		switch st.kind {
		case stMatch:
			if p.project != nil || p.group != nil {
				return nil, ErrComputedCollection
			}
			if len(p.preds)+len(st.preds) > 128 {
				return nil, ErrComputedCollection
			}
			for _, pred := range st.preds {
				if !computedPathValid(pred.field) || pred.v.k == kDoc || pred.v.k == kArray {
					return nil, ErrComputedCollection
				}
				for _, v := range pred.in {
					if !indexable(v) {
						return nil, ErrComputedCollection
					}
				}
			}
			// Validate original scalar literals as well: valueOf deliberately
			// permits float promotion in ordinary aggregation, which could round
			// an overflowing integer in a persistent filter.
			if err := validateComputedMatch(array[i]); err != nil {
				return nil, err
			}
			p.preds = append(p.preds, st.preds...)
		case stProject:
			if i != len(pipeline.stages)-1 || p.group != nil {
				return nil, ErrComputedCollection
			}
			p.project = st.project
			body := array[i].(map[string]any)["$project"].(map[string]any)
			for name, v := range body {
				if !computedOutputValid(name) {
					return nil, ErrComputedCollection
				}
				if n, ok := v.(json.Number); ok {
					j, err := n.Int64()
					if err != nil || (j != 0 && j != 1) {
						return nil, ErrComputedCollection
					}
				}
				if st.project[name] && name != "_id" {
					p.inclusion = true
				}
			}
		case stGroup:
			if i != len(pipeline.stages)-1 || st.group.idNames != nil || len(st.group.idFields) > 1 || len(st.group.accs) > 64 {
				return nil, ErrComputedCollection
			}
			p.group = st.group
			for _, path := range p.group.idFields {
				if !computedPathValid(path) {
					return nil, ErrComputedCollection
				}
			}
			body := array[i].(map[string]any)["$group"].(map[string]any)
			if _, composite := body["_id"].(map[string]any); composite {
				return nil, ErrComputedCollection
			}
			for ai, a := range p.group.accs {
				if !computedOutputValid(a.name) || (a.op != accSum && a.op != accCount) {
					return nil, ErrComputedCollection
				}
				if a.field != "" {
					if !computedPathValid(a.field) || a.op == accCount {
						return nil, ErrComputedCollection
					}
				} else if a.op == accCount {
					operand, ok := body[a.name].(map[string]any)["$count"].(map[string]any)
					if !ok || len(operand) != 0 {
						return nil, ErrComputedCollection
					}
				} else if a.op == accSum {
					operand := body[a.name].(map[string]any)["$sum"]
					v, err := computedLiteral(operand)
					if err != nil || v.k != kInt {
						return nil, ErrComputedCollection
					}
					p.group.accs[ai].c = v
				}
			}
		default:
			return nil, fmt.Errorf("%w: only leading $match and a final $project or integer $group are supported", ErrComputedCollection)
		}
	}
	return p, nil
}

func computedPathValid(path string) bool {
	if path == "" || len(path) > 1024 || strings.Contains(path, "$") {
		return false
	}
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return false
		}
	}
	return true
}
func computedOutputValid(name string) bool {
	return name != "" && len(name) <= 255 && !strings.ContainsAny(name, ".$")
}
func validateComputedMatch(stage any) error {
	body := stage.(map[string]any)["$match"].(map[string]any)
	for _, v := range body {
		vals := []any{v}
		if ops, ok := v.(map[string]any); ok {
			vals = nil
			for name, arg := range ops {
				if name == "$in" {
					xs, ok := arg.([]any)
					if !ok {
						return ErrComputedCollection
					}
					vals = append(vals, xs...)
				} else {
					vals = append(vals, arg)
				}
			}
		}
		for _, x := range vals {
			if _, err := computedLiteral(x); err != nil {
				return fmt.Errorf("%w: %w", ErrComputedCollection, err)
			}
		}
	}
	return nil
}

func (p *computedCollectionPlan) matches(doc []byte) bool {
	return matchAll(p.preds, func(path string) value { return pathValue(value{k: kDoc, s: doc}, path) })
}
func (p *computedCollectionPlan) projectDoc(doc []byte) ([]byte, error) {
	if p.project == nil {
		return doc, nil
	}
	n, dir, _ := docDir(doc)
	contentEnd := docContentEnd(doc, n, dir)
	out := make([]byte, 0, len(doc))
	var smallOffsets [32]uint32
	offsets := smallOffsets[:]
	if n > len(offsets) {
		offsets = make([]uint32, n)
	}
	count := 0
	for i := 0; i < n; i++ {
		off := docOffset(dir, n, i)
		end := contentEnd
		if i+1 < n {
			end = docOffset(dir, n, i+1)
		}
		name := string(doc[off+1 : off+1+int(doc[off])])
		keep := true
		if p.inclusion {
			keep = p.project[name]
			if name == "_id" {
				keep = true
				if x, ok := p.project["_id"]; ok {
					keep = x
				}
			}
		} else if x, ok := p.project[name]; ok {
			keep = x
		}
		if keep {
			offsets[count] = uint32(len(out))
			count++
			out = append(out, doc[off:end]...)
		}
	}
	return sealDocument(out, offsets[:count]), nil
}

// ComputedGroupKey returns the primary key used by a grouped computed
// collection. Keys encode scalar types exactly (unlike index float keys).
// Missing and null share a key; integral float keys normalize to int64.
func ComputedGroupKey(v any) ([]byte, error) {
	var x value
	switch v.(type) {
	case nil, bool, int, int64, float64, string:
		x = valueOf(v)
	case json.Number:
		var err error
		x, err = computedLiteral(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrComputedCollection, err)
		}
	default:
		return nil, ErrComputedCollection
	}
	k, _, err := computedGroupValueKey(x)
	return k, err
}
func computedGroupValueKey(v value) ([]byte, value, error) {
	if v.k == kMissing {
		v = value{k: kNull}
	}
	if v.k == kFloat {
		if math.IsNaN(v.f) || math.IsInf(v.f, 0) {
			return nil, value{}, ErrComputedCollection
		}
		if v.f >= -9223372036854775808.0 && v.f < 9223372036854775808.0 && math.Trunc(v.f) == v.f {
			v = value{k: kInt, i: int64(v.f)}
		}
	}
	k := []byte{byte(v.k)}
	switch v.k {
	case kNull:
	case kBool:
		k = append(k, byte(v.i))
	case kInt:
		k = le.AppendUint64(k, uint64(v.i))
	case kFloat:
		k = le.AppendUint64(k, math.Float64bits(v.f))
	case kString:
		k = append(k, v.s...)
	default:
		return nil, value{}, ErrComputedCollection
	}
	if len(k) > MaxKeySize-6 {
		return nil, value{}, fmt.Errorf("%w: group key too large", ErrComputedCollection)
	}
	return k, v, nil
}

// Signed 128-bit sums allow a batch's intermediate totals to overflow int64
// while its final total is valid (including moves/deletes of MinInt64).
type computedWide struct {
	lo uint64
	hi uint64
}

func (w *computedWide) add(x int64, negative bool) {
	hi := uint64(0)
	if x < 0 {
		hi = math.MaxUint64
	}
	if negative {
		var borrow uint64
		w.lo, borrow = bits.Sub64(w.lo, uint64(x), 0)
		w.hi, _ = bits.Sub64(w.hi, hi, borrow)
	} else {
		var carry uint64
		w.lo, carry = bits.Add64(w.lo, uint64(x), 0)
		w.hi, _ = bits.Add64(w.hi, hi, carry)
	}
}
func (w computedWide) int64() (int64, error) {
	expected := uint64(0)
	if int64(w.lo) < 0 {
		expected = math.MaxUint64
	}
	if w.hi != expected {
		return 0, fmt.Errorf("%w: aggregate overflow", ErrComputedCollection)
	}
	return int64(w.lo), nil
}

func computedViewKey(tag byte, id uint32, suffix []byte) []byte {
	k := []byte{reservedPrefix, tag, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(k[2:], id)
	return append(k, suffix...)
}

// Scan one bounded collection under the writer lock. This is only used for
// atomic creation/drop, not for normal maintenance. Buffered entries shadow
// the tree, including tombstones.
func (db *DB) computedScanLocked(ctx context.Context, start, end []byte, fn func([]byte, []byte) error) error {
	return db.scanLocked(ctx, start, end, false, fn)
}

func (db *DB) scanLocked(ctx context.Context, start, end []byte, keysOnly bool, fn func([]byte, []byte) error) error {
	v := view{db, nil}
	mem := v.memRange(start, end)
	mi := 0
	emitMem := func(until []byte) error {
		for mi < len(mem) && (until == nil || mem[mi].k < string(until)) {
			entry := mem[mi]
			mi++
			if err := ctx.Err(); err != nil {
				return err
			}
			if !entry.del {
				if err := fn([]byte(entry.k), entry.v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var walk func(uint32, int) error
	walk = func(id uint32, depth int) error {
		if depth > 16 {
			return ErrCorrupt
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := v.page(id)
		if err != nil {
			return err
		}
		if page.typ() == pageInternal {
			first, last := page.searchInternal(start), page.numCells()
			if end != nil {
				last = page.searchInternal(end)
			}
			for i := first; i <= last; i++ {
				if err := walk(page.child(i), depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if page.typ() != pageLeaf {
			return ErrCorrupt
		}
		var buf []byte
		first, _ := page.searchLeaf(start)
		for i := first; i < page.numCells(); i++ {
			key := page.leafKey(i)
			if end != nil && bytes.Compare(key, end) >= 0 {
				break
			}
			if err := emitMem(key); err != nil {
				return err
			}
			if mi < len(mem) && mem[mi].k == string(key) {
				entry := mem[mi]
				mi++
				if !entry.del {
					if err := fn(key, entry.v); err != nil {
						return err
					}
				}
				continue
			}
			if !keysOnly {
				buf, err = v.leafValue(page, i, buf)
				if err != nil {
					return err
				}
			}
			if err = fn(key, buf); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(v.root(), 0); err != nil {
		return err
	}
	return emitMem(nil)
}
