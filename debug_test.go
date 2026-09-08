package aptevadb

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

// verifyLeaf checks a leaf's slots and cells are sane and ordered.
func verifyLeaf(p page) error {
	if p.typ() != pageLeaf {
		return fmt.Errorf("not a leaf")
	}
	n := p.numCells()
	var prev []byte
	for i := 0; i < n; i++ {
		off := p.slot(i)
		if off < hdrSize+slotSize*n || off+4 > PageSize {
			return fmt.Errorf("cell %d: bad offset %d (n=%d)", i, off, n)
		}
		klen := int(le.Uint16(p[off:]))
		vlen := int(le.Uint16(p[off+2:]))
		if klen != p.slotKlen(i) || off+4+klen+vlen > PageSize {
			return fmt.Errorf("cell %d: klen %d slotKlen %d vlen %d off %d", i, klen, p.slotKlen(i), vlen, off)
		}
		k := p.leafKey(i)
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			return fmt.Errorf("cell %d out of order", i)
		}
		prev = k
	}
	return nil
}

func TestBalanceKeepsLeavesConsistent(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(1))
	db.debugVerify = func(where string, pages ...page) {
		for i, p := range pages {
			if err := verifyLeaf(p); err != nil {
				t.Fatalf("%s: page %d: %v", where, i, err)
			}
		}
	}
	for i := 0; i < 200_000; i++ {
		k := make([]byte, 1+rng.Intn(40))
		rng.Read(k)
		v := make([]byte, rng.Intn(300))
		rng.Read(v)
		if err := db.Insert(k, v); err != nil {
			t.Fatal(err)
		}
	}
}
