package db

import (
	"math"
	"math/bits"
)

// Integer query sums share the fixed-width exact accumulator used by maintained
// collections. Even MaxInt64 inputs across MaxInt64 rows fit signed 128 bits.
// Numeric operators reuse the otherwise unused extremum payload for the high
// limb, retaining the existing 80-byte state and memory-budget accounting.
func (st *accState) integerSum() computedWide {
	return computedWide{lo: uint64(st.isum), hi: uint64(st.ext.i)}
}

func (st *accState) addInteger(x int64) {
	lo, carry := bits.Add64(uint64(st.isum), uint64(x), 0)
	st.isum = int64(lo)
	if delta := x>>63 + int64(carry); delta != 0 {
		st.ext.i += delta
	}
}

func (w *computedWide) merge(other computedWide) {
	var carry uint64
	w.lo, carry = bits.Add64(w.lo, other.lo, 0)
	w.hi, _ = bits.Add64(w.hi, other.hi, carry)
}

func (w computedWide) fitsInt64() bool {
	expected := uint64(0)
	if int64(w.lo) < 0 {
		expected = math.MaxUint64
	}
	return w.hi == expected
}

func (w computedWide) float64() float64 {
	if w.fitsInt64() {
		return float64(int64(w.lo))
	}
	lo, hi := w.lo, w.hi
	negative := int64(hi) < 0
	if negative {
		var carry uint64
		lo, carry = bits.Add64(^lo, 1, 0)
		hi = ^hi + carry
	}
	// Round the full magnitude once. Converting the two limbs separately
	// can double-round a low limb near a halfway boundary.
	f := float64(lo)
	if hi != 0 {
		shift := bits.Len64(hi) + 11
		var mant uint64
		var round, sticky bool
		if shift < 64 {
			mant = hi<<uint(64-shift) | lo>>uint(shift)
			half := uint64(1) << uint(shift-1)
			round, sticky = lo&half != 0, lo&(half-1) != 0
		} else {
			n := shift - 64
			mant = hi >> uint(n)
			if n == 0 {
				round, sticky = lo>>63 != 0, lo&math.MaxInt64 != 0
			} else {
				half := uint64(1) << uint(n-1)
				round, sticky = hi&half != 0, hi&(half-1) != 0 || lo != 0
			}
		}
		if round && (sticky || mant&1 != 0) {
			mant++
		}
		f = math.Ldexp(float64(mant), shift)
	}
	if negative {
		return -f
	}
	return f
}
