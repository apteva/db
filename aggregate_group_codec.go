package db

import (
	"math"
	"unsafe"
)

// Scratch records are private to one execution. The known accumulator schema
// determines the payload: counts have no unused numeric/extremum fields. IDs
// are reconstructed from their lossless typed hash key rather than duplicated.
func appendGroupRecord(b []byte, g *groupState, spec *groupSpec) []byte {
	b = le.AppendUint32(b, uint32(len(g.key)))
	b = append(b, g.key...)
	for i, a := range spec.accs {
		s := &g.accs[i]
		switch a.op {
		case accCount, accCountNonNull:
			b = le.AppendUint64(b, uint64(s.count))
		case accSum, accAvg:
			var flags byte
			if s.hasFloat {
				flags = 1
			}
			b = append(b, flags)
			b = le.AppendUint64(b, uint64(s.isum))
			b = le.AppendUint64(b, uint64(s.ext.i))
			if s.hasFloat {
				b = le.AppendUint64(b, math.Float64bits(s.fsum))
			}
			if a.op == accAvg {
				b = le.AppendUint64(b, uint64(s.count))
			}
		default:
			if !s.hasExt {
				b = append(b, 0)
			} else {
				b = append(b, 1)
				b = appendCompactScalar(b, s.ext)
			}
		}
	}
	return b
}
func appendCompactScalar(b []byte, s scalar) []byte {
	b = append(b, byte(s.k))
	switch s.k {
	case kBool, kInt:
		b = le.AppendUint64(b, uint64(s.i))
	case kFloat:
		b = le.AppendUint64(b, math.Float64bits(s.f))
	case kString, kDoc, kArray:
		b = le.AppendUint32(b, uint32(len(s.s)))
		b = append(b, s.s...)
	}
	return b
}
func borrowedString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
func compactScalar(r *aggregateDecoder) scalar {
	if len(r.b) == 0 {
		r.err = ErrCorrupt
		return scalar{}
	}
	s := scalar{k: kind(r.take(1)[0])}
	switch s.k {
	case kMissing, kNull:
	case kBool, kInt:
		s.i = int64(r.u64())
	case kFloat:
		s.f = math.Float64frombits(r.u64())
	case kString, kDoc, kArray:
		n := r.u32()
		if n > len(r.b) {
			r.err = ErrCorrupt
			return s
		}
		s.s = borrowedString(r.take(n))
	default:
		r.err = ErrCorrupt
	}
	return s
}
func decodeGroupKey(key string, ids []scalar) error {
	r := aggregateDecoder{b: unsafe.Slice(unsafe.StringData(key), len(key))}
	for i := range ids {
		ids[i] = compactScalar(&r)
	}
	if r.err != nil || len(r.b) != 0 {
		return ErrCorrupt
	}
	return nil
}
func decodeGroupRecord(b []byte, spec *groupSpec, g *groupState) error {
	r := aggregateDecoder{b: b}
	n := r.u32()
	if n > len(r.b) {
		return ErrCorrupt
	}
	g.key = borrowedString(r.take(n))
	if err := decodeGroupKey(g.key, g.id); err != nil {
		return err
	}
	clear(g.accs)
	for i, a := range spec.accs {
		s := &g.accs[i]
		switch a.op {
		case accCount, accCountNonNull:
			s.count = int64(r.u64())
		case accSum, accAvg:
			if len(r.b) < 1 {
				return ErrCorrupt
			}
			flag := r.take(1)[0]
			if flag > 1 {
				return ErrCorrupt
			}
			s.hasFloat = flag == 1
			s.isum = int64(r.u64())
			s.ext.i = int64(r.u64())
			if s.hasFloat {
				s.fsum = math.Float64frombits(r.u64())
			}
			if a.op == accAvg {
				s.count = int64(r.u64())
			}
		default:
			if len(r.b) < 1 {
				return ErrCorrupt
			}
			flag := r.take(1)[0]
			if flag > 1 {
				return ErrCorrupt
			}
			s.hasExt = flag == 1
			if s.hasExt {
				s.ext = compactScalar(&r)
			}
		}
	}
	if r.err != nil || len(r.b) != 0 {
		return ErrCorrupt
	}
	return nil
}

func compactScalarSize(s scalar) int {
	switch s.k {
	case kBool, kInt, kFloat:
		return 9
	case kString, kDoc, kArray:
		return 5 + len(s.s)
	}
	return 1
}
func groupRecordSize(g *groupState, spec *groupSpec) int {
	n := 4 + len(g.key)
	for i, a := range spec.accs {
		s := &g.accs[i]
		switch a.op {
		case accCount, accCountNonNull:
			n += 8
		case accSum, accAvg:
			n += 17
			if s.hasFloat {
				n += 8
			}
			if a.op == accAvg {
				n += 8
			}
		default:
			n++
			if s.hasExt {
				n += compactScalarSize(s.ext)
			}
		}
	}
	return n
}
