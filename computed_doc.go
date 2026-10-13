package db

import (
	"bytes"
	"fmt"
	"math"
)

// Computed collections accept canonical binary documents, not opaque values.
// Validate the complete document (including unused fields) before evaluating,
// so a bad input cannot be partly written or interpreted differently on reads.
func validateComputedDoc(doc []byte, depth int) error {
	if depth > maxNesting || len(doc) > 65535 {
		return ErrDocument
	}
	n, dir, ok := docDir(doc)
	if !ok {
		return ErrDocument
	}
	content := doc[:len(doc)-1-2*n]
	end := 0
	var small [16][]byte
	names := small[:0]
	for i := 0; i < n; i++ {
		off := int(le.Uint16(dir[2*i:]))
		if off != end || off >= len(content) {
			return ErrDocument
		}
		nl := int(content[off])
		p := off + 1 + nl
		if p >= len(content) {
			return ErrDocument
		}
		name := content[off+1 : p]
		for _, previous := range names {
			if bytes.Equal(previous, name) {
				return ErrDocument
			}
		}
		names = append(names, name)
		v, next, ok := decodePayload(content, content[p], p+1)
		if !ok {
			return ErrDocument
		}
		if v.k == kBool && v.i > 1 {
			return ErrDocument
		}
		if err := validateComputedValue(v, depth); err != nil {
			return err
		}
		end = next
	}
	if end != len(content) {
		return ErrDocument
	}
	return nil
}

func validateComputedValue(v value, depth int) error {
	switch v.k {
	case kFloat:
		if math.IsInf(v.f, 0) || math.IsNaN(v.f) {
			return ErrDocument
		}
	case kDoc:
		return validateComputedDoc(v.s, depth+1)
	case kArray:
		if depth+1 > maxNesting || len(v.s) < 2 {
			return ErrDocument
		}
		n, p := int(le.Uint16(v.s)), 2
		for i := 0; i < n; i++ {
			if p >= len(v.s) {
				return ErrDocument
			}
			item, next, ok := decodePayload(v.s, v.s[p], p+1)
			if !ok || (item.k == kBool && item.i > 1) {
				return ErrDocument
			}
			if err := validateComputedValue(item, depth+1); err != nil {
				return err
			}
			p = next
		}
		if p != len(v.s) {
			return ErrDocument
		}
	}
	return nil
}

func (p *computedPlan) apply(doc []byte) ([]byte, error) {
	return p.applyInto(doc, nil)
}

// applyInto may reuse dst. The input must not alias dst; the batch caller
// copies each result into its final log record before reusing this buffer.
func (p *computedPlan) applyInto(doc, dst []byte) ([]byte, error) {
	if err := validateComputedDoc(doc, 0); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrComputedField, err)
	}
	var small [8]value
	values := small[:]
	if len(p.fields) > len(small) {
		values = make([]value, len(p.fields))
	}
	values = values[:len(p.fields)]
	for _, i := range p.order {
		v, err := p.fields[i].expr.eval(doc, values)
		if err != nil {
			return nil, fmt.Errorf("computed field %q: %w", p.fields[i].name, err)
		}
		if v.k == kMissing {
			v.k = kNull
		}
		if v.k == kDoc || v.k == kArray {
			return nil, fmt.Errorf("%w: output %q must be scalar", ErrComputedField, p.fields[i].name)
		}
		values[i] = v
	}
	// Preserve existing fields byte-for-byte. Supplied generated values are
	// discarded: the stored value is always derived from the source fields.
	// Keep offsets on the stack. Passing them through DocBuilder methods
	// makes the compiler move the temporary directory to the heap.
	var offsets [255]uint16
	count := 0
	if cap(dst) < len(doc)+32*len(p.fields) {
		dst = make([]byte, 0, len(doc)+32*len(p.fields))
	}
	out := dst[:0]
	n, dir, _ := docDir(doc)
	contentEnd := len(doc) - 1 - 2*n
	for i := 0; i < n; i++ {
		off := int(le.Uint16(dir[2*i:]))
		end := contentEnd
		if i+1 < n {
			end = int(le.Uint16(dir[2*(i+1):]))
		}
		name := doc[off+1 : off+1+int(doc[off])]
		if _, computed := p.byName[string(name)]; computed {
			continue
		}
		offsets[count] = uint16(len(out))
		count++
		out = append(out, doc[off:end]...)
	}
	if count+len(p.fields) > len(offsets) {
		return nil, fmt.Errorf("%w: too many document fields", ErrComputedField)
	}
	for i, f := range p.fields {
		v := values[i]
		if len(out) > 65535 {
			return nil, fmt.Errorf("%w: computed document too large", ErrComputedField)
		}
		offsets[count] = uint16(len(out))
		count++
		out = append(out, byte(len(f.name)))
		out = append(out, f.name...)
		switch v.k {
		case kNull:
			out = append(out, tNull)
		case kBool:
			out = append(out, tBool, byte(v.i))
		case kInt:
			out = append(out, tInt)
			out = le.AppendUint64(out, uint64(v.i))
		case kFloat:
			out = append(out, tFloat)
			out = le.AppendUint64(out, math.Float64bits(v.f))
		case kString:
			if len(v.s) > 65535 {
				return nil, fmt.Errorf("%w: string too long", ErrComputedField)
			}
			out = append(out, tString)
			out = le.AppendUint16(out, uint16(len(v.s)))
			out = append(out, v.s...)
		default:
			return nil, ErrComputedField
		}
	}
	if len(out)+2*count+1 > 65535 {
		return nil, fmt.Errorf("%w: computed document too large", ErrComputedField)
	}
	for _, off := range offsets[:count] {
		out = le.AppendUint16(out, off)
	}
	return append(out, byte(count)), nil
}
