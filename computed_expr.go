package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

var (
	ErrComputedField    = errors.New("aptevadb: invalid computed field definition or value")
	ErrComputedNonEmpty = errors.New("aptevadb: computed definitions require an empty collection")
)

const maxComputedFields = 64

type computedExpr struct {
	op      string
	path    string
	ref     int // computed field index, or -1 for a source path
	literal value
	args    []*computedExpr
}

type computedField struct {
	name string
	expr *computedExpr
}

type computedPlan struct {
	fields []computedField
	order  []int
	byName map[string]int
	defs   map[string]json.RawMessage
}

// Decode definitions strictly: duplicate names/operators are ambiguous, and
// definitions are persisted executable data rather than arbitrary documents.
func computedJSON(src []byte) (any, error) {
	if len(src) > 65536 {
		return nil, fmt.Errorf("%w: definition exceeds 64 KiB", ErrComputedField)
	}
	d := json.NewDecoder(bytes.NewReader(src))
	d.UseNumber()
	var read func(int) (any, error)
	read = func(depth int) (any, error) {
		if depth > 64 {
			return nil, fmt.Errorf("%w: expression too deep", ErrComputedField)
		}
		t, err := d.Token()
		if err != nil {
			return nil, err
		}
		switch t {
		case json.Delim('{'):
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				name, ok := k.(string)
				if !ok {
					return nil, ErrComputedField
				}
				if _, ok := m[name]; ok {
					return nil, fmt.Errorf("%w: duplicate key %q", ErrComputedField, name)
				}
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				m[name] = v
			}
			_, err = d.Token()
			return m, err
		case json.Delim('['):
			var a []any
			for d.More() {
				v, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		}
		if _, ok := t.(json.Delim); ok {
			return nil, ErrComputedField
		}
		return t, nil
	}
	v, err := read(0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrComputedField, err)
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON", ErrComputedField)
	}
	return v, nil
}

func compileComputedFields(src []byte) (*computedPlan, error) {
	raw, err := computedJSON(src)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrComputedField, err)
	}
	defs, ok := raw.(map[string]any)
	if !ok || len(defs) > maxComputedFields {
		return nil, fmt.Errorf("%w: expected an object with at most 64 fields", ErrComputedField)
	}
	if len(defs) == 0 {
		return nil, nil
	}
	p := &computedPlan{byName: map[string]int{}, defs: map[string]json.RawMessage{}}
	names := make([]string, 0, len(defs))
	for n := range defs {
		if n == "" || len(n) > 255 || strings.ContainsAny(n, ".$") {
			return nil, fmt.Errorf("%w: invalid output name %q", ErrComputedField, n)
		}
		names = append(names, n)
	}
	sort.Strings(names)
	for i, n := range names {
		p.byName[n] = i
	}
	nodes := 0
	for _, n := range names {
		e, err := compileComputedExpr(defs[n], p.byName, 0, &nodes)
		if err != nil {
			return nil, fmt.Errorf("computed field %q: %w", n, err)
		}
		p.fields = append(p.fields, computedField{n, e})
		p.defs[n], err = json.Marshal(defs[n])
		if err != nil {
			return nil, err
		}
	}
	canonical, err := json.Marshal(p.defs)
	if err != nil || len(canonical) > 65536 {
		return nil, fmt.Errorf("%w: canonical definition exceeds 64 KiB", ErrComputedField)
	}
	state := make([]uint8, len(names))
	var visit func(int) error
	var refs func(*computedExpr) error
	refs = func(e *computedExpr) error {
		if e.op == "field" && e.ref >= 0 {
			if err := visit(e.ref); err != nil {
				return err
			}
		}
		for _, a := range e.args {
			if err := refs(a); err != nil {
				return err
			}
		}
		return nil
	}
	visit = func(i int) error {
		if state[i] == 1 {
			return fmt.Errorf("%w: dependency cycle at %q", ErrComputedField, names[i])
		}
		if state[i] == 2 {
			return nil
		}
		state[i] = 1
		if err := refs(p.fields[i].expr); err != nil {
			return err
		}
		state[i] = 2
		p.order = append(p.order, i)
		return nil
	}
	for i := range names {
		if err := visit(i); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func computedLiteral(x any) (value, error) {
	switch v := x.(type) {
	case nil:
		return value{k: kNull}, nil
	case bool:
		return value{k: kBool, i: int64(b2u(v))}, nil
	case string:
		if len(v) > MaxDocumentSize {
			return value{}, ErrComputedField
		}
		return value{k: kString, s: []byte(v)}, nil
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return value{k: kInt, i: i}, nil
		}
		if !strings.ContainsAny(string(v), ".eE") {
			return value{}, fmt.Errorf("%w: integer literal overflow", ErrComputedField)
		}
		f, err := v.Float64()
		if err == nil && !math.IsInf(f, 0) && !math.IsNaN(f) {
			return value{k: kFloat, f: f}, nil
		}
	}
	return value{}, fmt.Errorf("%w: literals must be finite scalars", ErrComputedField)
}

func compileComputedExpr(x any, names map[string]int, depth int, nodes *int) (*computedExpr, error) {
	*nodes++
	if depth > 32 || *nodes > 1024 {
		return nil, fmt.Errorf("%w: expression complexity limit", ErrComputedField)
	}
	e := &computedExpr{ref: -1}
	if s, ok := x.(string); ok && strings.HasPrefix(s, "$") {
		path := s[1:]
		if path == "" {
			return nil, ErrComputedField
		}
		for _, part := range strings.Split(path, ".") {
			if part == "" {
				return nil, ErrComputedField
			}
		}
		e.op, e.path = "field", path
		root, _, _ := strings.Cut(path, ".")
		if i, ok := names[root]; ok {
			if root != path {
				return nil, fmt.Errorf("%w: computed outputs are scalar", ErrComputedField)
			}
			e.ref = i
		}
		return e, nil
	}
	m, isOp := x.(map[string]any)
	if !isOp {
		v, err := computedLiteral(x)
		e.op, e.literal = "literal", v
		return e, err
	}
	if len(m) != 1 {
		return nil, fmt.Errorf("%w: one operator per expression", ErrComputedField)
	}
	var arg any
	for e.op, arg = range m {
	}
	if e.op == "$literal" {
		v, err := computedLiteral(arg)
		e.op, e.literal = "literal", v
		return e, err
	}
	var args []any
	switch e.op {
	case "$lower", "$upper":
		args = []any{arg}
	case "$add", "$multiply", "$subtract", "$divide", "$concat", "$ifNull":
		var ok bool
		args, ok = arg.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: %s requires an array", ErrComputedField, e.op)
		}
		minArgs, maxArgs := 2, 16
		if e.op == "$concat" {
			minArgs = 1
		}
		if e.op == "$subtract" || e.op == "$divide" || e.op == "$ifNull" {
			maxArgs = 2
		}
		if len(args) < minArgs || len(args) > maxArgs {
			return nil, fmt.Errorf("%w: wrong arity for %s", ErrComputedField, e.op)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported operator %s", ErrComputedField, e.op)
	}
	for _, a := range args {
		child, err := compileComputedExpr(a, names, depth+1, nodes)
		if err != nil {
			return nil, err
		}
		e.args = append(e.args, child)
	}
	return e, nil
}

func (e *computedExpr) eval(doc []byte, fields []value) (value, error) {
	switch e.op {
	case "literal":
		return e.literal, nil
	case "field":
		if e.ref >= 0 {
			return fields[e.ref], nil
		}
		return pathValue(value{k: kDoc, s: doc}, e.path), nil
	case "$ifNull":
		v, err := e.args[0].eval(doc, fields)
		if err != nil || (v.k != kMissing && v.k != kNull) {
			return v, err
		}
		return e.args[1].eval(doc, fields)
	case "$lower", "$upper":
		v, err := e.args[0].eval(doc, fields)
		if err != nil {
			return value{}, err
		}
		if v.k == kMissing || v.k == kNull {
			return value{k: kNull}, nil
		}
		if v.k != kString {
			return value{}, fmt.Errorf("%w: %s needs a string", ErrComputedField, e.op)
		}
		// Version-one casing is ASCII, independent of Unicode table changes.
		var out []byte
		for i, c := range v.s {
			n := c
			if e.op == "$lower" && c >= 'A' && c <= 'Z' {
				n = c + 32
			}
			if e.op == "$upper" && c >= 'a' && c <= 'z' {
				n = c - 32
			}
			if n != c {
				if out == nil {
					out = append([]byte(nil), v.s...)
				}
				out[i] = n
			}
		}
		if out != nil {
			v.s = out
		}
		return v, nil
	case "$concat":
		var parts [16][]byte
		n := 0
		for i, a := range e.args {
			v, err := a.eval(doc, fields)
			if err != nil {
				return value{}, err
			}
			if v.k == kMissing || v.k == kNull {
				return value{k: kNull}, nil
			}
			if v.k != kString {
				return value{}, fmt.Errorf("%w: concat needs strings", ErrComputedField)
			}
			n += len(v.s)
			if n > MaxDocumentSize {
				return value{}, fmt.Errorf("%w: string too long", ErrComputedField)
			}
			parts[i] = v.s
		}
		s := make([]byte, 0, n)
		for _, p := range parts[:len(e.args)] {
			s = append(s, p...)
		}
		return value{k: kString, s: s}, nil
	}
	var result value
	for i, a := range e.args {
		v, err := a.eval(doc, fields)
		if err != nil {
			return value{}, err
		}
		if v.k == kMissing || v.k == kNull {
			return value{k: kNull}, nil
		}
		if !v.isNum() {
			return value{}, fmt.Errorf("%w: %s needs numbers", ErrComputedField, e.op)
		}
		if i == 0 {
			result = v
			continue
		}
		result, err = computedArithmetic(e.op, result, v)
		if err != nil {
			return value{}, err
		}
	}
	return result, nil
}

func computedArithmetic(op string, a, b value) (value, error) {
	if a.k == kInt && b.k == kInt && op != "$divide" {
		var r int64
		overflow := false
		switch op {
		case "$add":
			r = a.i + b.i
			overflow = (b.i > 0 && r < a.i) || (b.i < 0 && r > a.i)
		case "$subtract":
			r = a.i - b.i
			overflow = (b.i > 0 && r > a.i) || (b.i < 0 && r < a.i)
		case "$multiply":
			r = a.i * b.i
			overflow = (a.i == math.MinInt64 && b.i == -1) || (b.i == math.MinInt64 && a.i == -1) || (b.i != 0 && r/b.i != a.i)
		}
		if overflow {
			return value{}, fmt.Errorf("%w: integer overflow", ErrComputedField)
		}
		return value{k: kInt, i: r}, nil
	}
	x, y := a.num(), b.num()
	var r float64
	switch op {
	case "$add":
		r = x + y
	case "$subtract":
		r = x - y
	case "$multiply":
		r = x * y
	case "$divide":
		if y == 0 {
			return value{}, fmt.Errorf("%w: division by zero", ErrComputedField)
		}
		r = x / y
	}
	if math.IsInf(r, 0) || math.IsNaN(r) {
		return value{}, fmt.Errorf("%w: non-finite result", ErrComputedField)
	}
	return value{k: kFloat, f: r}, nil
}
