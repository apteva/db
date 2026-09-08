package aptevadb

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

func TestCursorForwardBackwardWithMemtable(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 64 * PageSize, MemtableBytes: 256 << 10, SyncMode: SyncOS}} {
		db, err := Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(101))
		m := randKV(rng, 30_000)
		insertAll(t, db, m)
		// some large values, some deletes and some updates pending in the memtable
		i := 0
		for k := range m {
			switch i % 7 {
			case 0:
				v := make([]byte, 20_000)
				rng.Read(v)
				m[k] = v
				db.Insert([]byte(k), v)
			case 1:
				db.Delete([]byte(k))
				delete(m, k)
			}
			i++
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		c := db.NewCursor()
		var got []string
		for ok := c.First(); ok; ok = c.Next() {
			if !bytes.Equal(c.Value(), m[string(c.Key())]) {
				t.Fatalf("forward: wrong value for %x", c.Key())
			}
			got = append(got, string(c.Key()))
		}
		if c.Err() != nil || len(got) != len(keys) {
			t.Fatalf("forward: err=%v got %d keys want %d", c.Err(), len(got), len(keys))
		}
		for i := range keys {
			if got[i] != keys[i] {
				t.Fatalf("forward: key %d differs", i)
			}
		}
		got = got[:0]
		for ok := c.Last(); ok; ok = c.Prev() {
			got = append(got, string(c.Key()))
		}
		if len(got) != len(keys) {
			t.Fatalf("backward: got %d keys want %d", len(got), len(keys))
		}
		for i := range keys {
			if got[len(got)-1-i] != keys[i] {
				t.Fatalf("backward: key %d differs", i)
			}
		}
		// seek to an absent key lands on the next present one, and Prev from
		// there returns the previous present one
		probe := []byte(keys[1234] + "\x00")
		if !c.Seek(probe) || string(c.Key()) != keys[1235] {
			t.Fatalf("seek landed on %q, want %q", c.Key(), keys[1235])
		}
		if !c.Prev() || string(c.Key()) != keys[1234] {
			t.Fatalf("prev after seek landed on %q", c.Key())
		}
		if c.Seek([]byte("\xff\xff\xff\xff\xff")) {
			t.Fatal("seek past the end should be invalid")
		}
		db.Close()
	}
}
