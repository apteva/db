package aptevadb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Flat document encoding, built for field access without parsing:
//
//	entries: per field [nameLen u8][name][type u8][payload]
//	trailer: [offset u16 of entry 0] ... [offset u16 of entry n-1][nfields u8]
//	null: nothing   bool: u8   int: int64 LE   float: float64 bits LE
//	string: [len u16][bytes]   document: [len u16][nested document]
//	array: [len u16][count u16][elements: type u8 + payload]
//
// The trailer lets a reader jump to the i-th entry without walking earlier
// ones. Fields are stored in the order given; EncodeJSON sorts them by name.
const (
	tNull   byte = 1
	tBool   byte = 2
	tInt    byte = 3
	tFloat  byte = 4
	tString byte = 5
	tDoc    byte = 6
	tArray  byte = 7

	maxNesting = 16
)

var ErrDocument = errors.New("aptevadb: malformed document")

// DocBuilder encodes flat documents with no per-field allocation.
type DocBuilder struct {
	buf    []byte
	offs   []uint16
	sealed bool
}

// Reset starts a new document, reusing the buffer.
func (b *DocBuilder) Reset() {
	b.buf = b.buf[:0]
	b.offs = b.offs[:0]
	b.sealed = false
}

func (b *DocBuilder) head(name string, t byte) {
	b.offs = append(b.offs, uint16(len(b.buf)))
	b.buf = append(b.buf, byte(len(name)))
	b.buf = append(b.buf, name...)
	b.buf = append(b.buf, t)
}

func (b *DocBuilder) Null(name string) { b.head(name, tNull) }

func (b *DocBuilder) Bool(name string, v bool) {
	b.head(name, tBool)
	if v {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
}

func (b *DocBuilder) Int(name string, v int64) {
	b.head(name, tInt)
	b.buf = le.AppendUint64(b.buf, uint64(v))
}

func (b *DocBuilder) Float(name string, v float64) {
	b.head(name, tFloat)
	b.buf = le.AppendUint64(b.buf, math.Float64bits(v))
}

func (b *DocBuilder) String(name string, v string) {
	b.head(name, tString)
	b.buf = le.AppendUint16(b.buf, uint16(len(v)))
	b.buf = append(b.buf, v...)
}

// Doc adds a nested document (bytes from another DocBuilder).
func (b *DocBuilder) Doc(name string, sub []byte) {
	b.head(name, tDoc)
	b.buf = le.AppendUint16(b.buf, uint16(len(sub)))
	b.buf = append(b.buf, sub...)
}

// Array adds an array (bytes from an ArrayBuilder).
func (b *DocBuilder) Array(name string, arr []byte) {
	b.head(name, tArray)
	b.buf = le.AppendUint16(b.buf, uint16(len(arr)))
	b.buf = append(b.buf, arr...)
}

// ArrayBuilder encodes array elements: [count u16] then type+payload each.
type ArrayBuilder struct {
	buf []byte
	n   int
}

func (a *ArrayBuilder) Reset()      { a.buf = append(a.buf[:0], 0, 0); a.n = 0 }
func (a *ArrayBuilder) Null()       { a.buf = append(a.buf, tNull); a.n++ }
func (a *ArrayBuilder) Bool(v bool) { a.buf = append(a.buf, tBool, b2u(v)); a.n++ }
func (a *ArrayBuilder) Int(v int64) { a.buf = le.AppendUint64(append(a.buf, tInt), uint64(v)); a.n++ }
func (a *ArrayBuilder) Float(v float64) {
	a.buf = le.AppendUint64(append(a.buf, tFloat), math.Float64bits(v))
	a.n++
}
func (a *ArrayBuilder) String(v string) {
	a.buf = append(le.AppendUint16(append(a.buf, tString), uint16(len(v))), v...)
	a.n++
}
func (a *ArrayBuilder) Doc(sub []byte) {
	a.buf = append(le.AppendUint16(append(a.buf, tDoc), uint16(len(sub))), sub...)
	a.n++
}
func (a *ArrayBuilder) Array(arr []byte) {
	a.buf = append(le.AppendUint16(append(a.buf, tArray), uint16(len(arr))), arr...)
	a.n++
}
func (a *ArrayBuilder) Bytes() []byte { le.PutUint16(a.buf, uint16(a.n)); return a.buf }

func b2u(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// Bytes seals and returns the encoded document; valid until the next Reset.
func (b *DocBuilder) Bytes() []byte {
	if !b.sealed {
		for _, o := range b.offs {
			b.buf = le.AppendUint16(b.buf, o)
		}
		b.buf = append(b.buf, byte(len(b.offs)))
		b.sealed = true
	}
	return b.buf
}

// EncodeJSON converts a JSON object (nested objects and arrays allowed, up
// to 16 levels) into the binary document format.
func EncodeJSON(dst []byte, src []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(src))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	b := DocBuilder{buf: dst[:0]}
	if err := encodeMap(&b, m, 0); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func encodeMap(b *DocBuilder, m map[string]any, depth int) error {
	if depth > maxNesting {
		return fmt.Errorf("%w: nesting too deep", ErrDocument)
	}
	if len(m) > 255 {
		return fmt.Errorf("%w: too many fields", ErrDocument)
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	b.Reset()
	for _, k := range names {
		if len(k) > 255 {
			return fmt.Errorf("%w: field name too long", ErrDocument)
		}
		switch v := m[k].(type) {
		case nil:
			b.Null(k)
		case bool:
			b.Bool(k, v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				b.Int(k, i)
			} else if f, err := v.Float64(); err == nil {
				b.Float(k, f)
			} else {
				return fmt.Errorf("%w: bad number %q", ErrDocument, v)
			}
		case float64:
			b.Float(k, v)
		case int64:
			b.Int(k, v)
		case int:
			b.Int(k, int64(v))
		case string:
			if len(v) > 65535 {
				return fmt.Errorf("%w: string too long", ErrDocument)
			}
			b.String(k, v)
		case map[string]any:
			var sub DocBuilder
			if err := encodeMap(&sub, v, depth+1); err != nil {
				return err
			}
			if len(sub.Bytes()) > 65535 {
				return fmt.Errorf("%w: nested document too large", ErrDocument)
			}
			b.Doc(k, sub.Bytes())
		case []any:
			var arr ArrayBuilder
			if err := encodeArray(&arr, v, depth+1); err != nil {
				return err
			}
			if len(arr.Bytes()) > 65535 {
				return fmt.Errorf("%w: array too large", ErrDocument)
			}
			b.Array(k, arr.Bytes())
		default:
			return fmt.Errorf("%w: unsupported value in field %q", ErrDocument, k)
		}
	}
	return nil
}

func encodeArray(a *ArrayBuilder, list []any, depth int) error {
	if depth > maxNesting {
		return fmt.Errorf("%w: nesting too deep", ErrDocument)
	}
	if len(list) > 65535 {
		return fmt.Errorf("%w: array too long", ErrDocument)
	}
	a.Reset()
	for _, e := range list {
		switch v := e.(type) {
		case nil:
			a.Null()
		case bool:
			a.Bool(v)
		case json.Number:
			if i, err := v.Int64(); err == nil {
				a.Int(i)
			} else if f, err := v.Float64(); err == nil {
				a.Float(f)
			} else {
				return fmt.Errorf("%w: bad number %q", ErrDocument, v)
			}
		case float64:
			a.Float(v)
		case int64:
			a.Int(v)
		case int:
			a.Int(int64(v))
		case string:
			a.String(v)
		case map[string]any:
			var sub DocBuilder
			if err := encodeMap(&sub, v, depth+1); err != nil {
				return err
			}
			a.Doc(sub.Bytes())
		case []any:
			var inner ArrayBuilder
			if err := encodeArray(&inner, v, depth+1); err != nil {
				return err
			}
			a.Array(inner.Bytes())
		default:
			return fmt.Errorf("%w: unsupported array element", ErrDocument)
		}
	}
	return nil
}

// kind is the runtime type of a field value inside the executor.
type kind uint8

const (
	kMissing kind = iota
	kNull
	kBool
	kInt
	kFloat
	kString
	kDoc   // s holds a nested document
	kArray // s holds array bytes
)

// value is a borrowed view of a field; s aliases the document.
type value struct {
	k kind
	i int64
	f float64
	s []byte
}

func (v value) isNum() bool { return v.k == kInt || v.k == kFloat }

func (v value) num() float64 {
	if v.k == kInt {
		return float64(v.i)
	}
	return v.f
}

// docDir returns the field count and the offset table of a document.
func docDir(doc []byte) (n int, dir []byte, ok bool) {
	if len(doc) < 1 {
		return 0, nil, false
	}
	n = int(doc[len(doc)-1])
	d := len(doc) - 1 - 2*n
	if d < 0 {
		return 0, nil, false
	}
	return n, doc[d : len(doc)-1], true
}

// decodePayload reads a typed payload at p; returns the value and the
// position after it.
func decodePayload(doc []byte, t byte, p int) (v value, next int, ok bool) {
	switch t {
	case tNull:
		v.k = kNull
	case tBool:
		if p+1 > len(doc) {
			return v, 0, false
		}
		v.k, v.i = kBool, int64(doc[p])
		p++
	case tInt:
		if p+8 > len(doc) {
			return v, 0, false
		}
		v.k, v.i = kInt, int64(le.Uint64(doc[p:]))
		p += 8
	case tFloat:
		if p+8 > len(doc) {
			return v, 0, false
		}
		v.k, v.f = kFloat, math.Float64frombits(le.Uint64(doc[p:]))
		p += 8
	case tString, tDoc, tArray:
		if p+2 > len(doc) {
			return v, 0, false
		}
		sl := int(le.Uint16(doc[p:]))
		p += 2
		if p+sl > len(doc) {
			return v, 0, false
		}
		v.s = doc[p : p+sl]
		switch t {
		case tString:
			v.k = kString
		case tDoc:
			v.k = kDoc
		default:
			v.k = kArray
		}
		p += sl
	default:
		return v, 0, false
	}
	return v, p, true
}

// decodeEntry reads the field entry starting at off.
func decodeEntry(doc []byte, off int) (name []byte, v value, ok bool) {
	if off >= len(doc) {
		return nil, v, false
	}
	nl := int(doc[off])
	p := off + 1
	if p+nl+1 > len(doc) {
		return nil, v, false
	}
	name = doc[p : p+nl]
	v, _, ok = decodePayload(doc, doc[p+nl], p+nl+1)
	return name, v, ok
}

// walkArray calls fn for each element; fn returns false to stop.
func walkArray(arr []byte, fn func(v value) bool) error {
	if len(arr) < 2 {
		return ErrDocument
	}
	n := int(le.Uint16(arr))
	p := 2
	for i := 0; i < n; i++ {
		if p >= len(arr) {
			return ErrDocument
		}
		v, next, ok := decodePayload(arr, arr[p], p+1)
		if !ok {
			return ErrDocument
		}
		p = next
		if !fn(v) {
			return nil
		}
	}
	return nil
}

// walkDoc calls fn for each field in order; fn returns false to stop early.
func walkDoc(doc []byte, fn func(name []byte, v value) bool) error {
	n, dir, ok := docDir(doc)
	if !ok {
		return ErrDocument
	}
	for i := 0; i < n; i++ {
		name, v, ok := decodeEntry(doc, int(le.Uint16(dir[2*i:])))
		if !ok {
			return ErrDocument
		}
		if !fn(name, v) {
			return nil
		}
	}
	return nil
}

// docField returns the named field, or a kMissing value.
func docField(doc []byte, name string) value {
	var out value
	walkDoc(doc, func(n []byte, v value) bool {
		if string(n) == name {
			out = v
			return false
		}
		return true
	})
	return out
}

// DecodeDocument converts a binary document into a map.
func DecodeDocument(doc []byte) (map[string]any, error) {
	m := map[string]any{}
	err := walkDoc(doc, func(n []byte, v value) bool {
		m[string(n)] = v.any()
		return true
	})
	return m, err
}

func (v value) any() any {
	switch v.k {
	case kBool:
		return v.i != 0
	case kInt:
		return v.i
	case kFloat:
		return v.f
	case kString:
		return string(v.s)
	case kDoc:
		m, _ := DecodeDocument(v.s)
		return m
	case kArray:
		out := []any{}
		walkArray(v.s, func(e value) bool { out = append(out, e.any()); return true })
		return out
	}
	return nil
}

// valueOf views a Go value (from JSON or a result row) as a value.
func valueOf(a any) value {
	switch x := a.(type) {
	case nil:
		return value{k: kNull}
	case bool:
		if x {
			return value{k: kBool, i: 1}
		}
		return value{k: kBool}
	case int:
		return value{k: kInt, i: int64(x)}
	case int64:
		return value{k: kInt, i: x}
	case float64:
		return value{k: kFloat, f: x}
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return value{k: kInt, i: i}
		}
		f, _ := x.Float64()
		return value{k: kFloat, f: f}
	case string:
		return value{k: kString, s: []byte(x)}
	case map[string]any:
		var b DocBuilder
		if encodeMap(&b, x, 0) == nil {
			return value{k: kDoc, s: append([]byte(nil), b.Bytes()...)}
		}
	case []any:
		var a ArrayBuilder
		if encodeArray(&a, x, 0) == nil {
			return value{k: kArray, s: append([]byte(nil), a.Bytes()...)}
		}
	}
	return value{k: kMissing}
}

// pathValue resolves a dotted path inside a document value.
func pathValue(v value, path string) value {
	for path != "" {
		if v.k != kDoc {
			return value{k: kMissing}
		}
		seg := path
		if i := strings.IndexByte(path, '.'); i >= 0 {
			seg, path = path[:i], path[i+1:]
		} else {
			path = ""
		}
		v = docField(v.s, seg)
	}
	return v
}

// rowField resolves a dotted path in a materialised row.
func rowField(r map[string]any, path string) value {
	var cur any = r
	for path != "" {
		m, ok := cur.(map[string]any)
		if !ok {
			return value{k: kMissing}
		}
		seg := path
		if i := strings.IndexByte(path, '.'); i >= 0 {
			seg, path = path[:i], path[i+1:]
		} else {
			path = ""
		}
		cur, ok = m[seg]
		if !ok {
			return value{k: kMissing}
		}
	}
	return valueOf(cur)
}

// setRowField assigns a dotted path in a materialised row, creating maps.
func setRowField(r map[string]any, path string, v any) {
	for {
		i := strings.IndexByte(path, '.')
		if i < 0 {
			r[path] = v
			return
		}
		seg, rest := path[:i], path[i+1:]
		sub, ok := r[seg].(map[string]any)
		if !ok {
			sub = map[string]any{}
			r[seg] = sub
		}
		r, path = sub, rest
	}
}

// compareValues orders values the way MongoDB does across types:
// missing/null < bool < numbers < strings. Returns -1, 0, 1.
func compareValues(a, b value) int {
	ra, rb := rank(a), rank(b)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	switch {
	case a.isNum():
		if a.k == kInt && b.k == kInt {
			switch {
			case a.i < b.i:
				return -1
			case a.i > b.i:
				return 1
			}
			return 0
		}
		x, y := a.num(), b.num()
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	case a.k == kString, a.k == kDoc, a.k == kArray:
		return bytes.Compare(a.s, b.s)
	case a.k == kBool:
		switch {
		case a.i < b.i:
			return -1
		case a.i > b.i:
			return 1
		}
	}
	return 0
}

func rank(v value) int {
	switch v.k {
	case kMissing, kNull:
		return 0
	case kBool:
		return 1
	case kInt, kFloat:
		return 2
	case kString:
		return 3
	case kDoc:
		return 4
	}
	return 5
}

// InsertJSON encodes a flat JSON object and stores it under key.
func (db *DB) InsertJSON(key, doc []byte) error {
	enc, err := EncodeJSON(db.docBuf[:0], doc)
	if err != nil {
		return err
	}
	db.docBuf = enc[:0]
	return db.Insert(key, enc)
}

// FindJSON returns the document under key as JSON.
func (db *DB) FindJSON(key []byte) ([]byte, bool, error) {
	raw, ok, err := db.Find(key, nil)
	if err != nil || !ok {
		return nil, ok, err
	}
	m, err := DecodeDocument(raw)
	if err != nil {
		return nil, true, err
	}
	out, err := json.Marshal(m)
	return out, true, err
}
