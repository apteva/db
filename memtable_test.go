package aptevadb

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestMemtableOrderAndRanges(t *testing.T) {
	rng := rand.New(rand.NewSource(77))
	mt := newMemtable()
	ref := map[string]mtValue{}
	for i := 0; i < 200_000; i++ {
		k := make([]byte, 1+rng.Intn(12))
		rng.Read(k)
		if rng.Intn(4) == 0 && len(ref) > 0 {
			// update an existing key, sometimes to a tombstone
			for e := range ref {
				k = []byte(e)
				break
			}
		}
		del := rng.Intn(10) == 0
		v := []byte(fmt.Sprint(i))
		mt.put(k, v, del)
		if del {
			ref[string(k)] = mtValue{del: true}
		} else {
			ref[string(k)] = mtValue{v: v}
		}
	}
	if mt.len() != len(ref) {
		t.Fatalf("len %d want %d", mt.len(), len(ref))
	}
	keys := make([]string, 0, len(ref))
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	snap := mt.snapshot()
	if len(snap) != len(keys) {
		t.Fatalf("snapshot has %d entries, want %d", len(snap), len(keys))
	}
	for i, e := range snap {
		if e.k != keys[i] || e.del != ref[e.k].del || string(e.v) != string(ref[e.k].v) {
			t.Fatalf("entry %d: %q del=%v v=%q", i, e.k, e.del, e.v)
		}
	}
	for _, k := range keys[:1000] {
		v, del, ok := mt.get([]byte(k))
		if !ok || del != ref[k].del || string(v) != string(ref[k].v) {
			t.Fatalf("get %q: %q %v %v", k, v, del, ok)
		}
	}
	// ranges, including bounds that equal separators and empty ranges
	for r := 0; r < 2000; r++ {
		a, b := keys[rng.Intn(len(keys))], keys[rng.Intn(len(keys))]
		if a > b {
			a, b = b, a
		}
		var lo, hi []byte
		if rng.Intn(5) > 0 {
			lo = []byte(a)
		}
		if rng.Intn(5) > 0 {
			hi = []byte(b)
		}
		got := mt.rangeInto(nil, lo, hi)
		i := 0
		if lo != nil {
			i = sort.SearchStrings(keys, string(lo))
		}
		j := len(keys)
		if hi != nil {
			j = sort.SearchStrings(keys, string(hi))
		}
		if j < i {
			j = i
		}
		if len(got) != j-i {
			t.Fatalf("range [%q,%q): %d entries, want %d", lo, hi, len(got), j-i)
		}
		for x, e := range got {
			if e.k != keys[i+x] {
				t.Fatalf("range [%q,%q) entry %d: %q want %q", lo, hi, x, e.k, keys[i+x])
			}
		}
	}
	mt.reset()
	if mt.len() != 0 || len(mt.snapshot()) != 0 || len(mt.rangeInto(nil, nil, nil)) != 0 {
		t.Fatal("reset left entries")
	}
}
