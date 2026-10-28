package db

import (
	"math/rand"
	"sort"
	"testing"
)

// TestFreelistPagesFromFreeIDs captures free lists whose size sits just above
// a page boundary. Every page of the persisted list takes its id from the
// free list it records, so sizing the list before allocating its pages left
// the last page starting past the end of the ids: the checkpointer panicked
// under the writer lock and the database hung (macOS soak, alpha.7/alpha.8).
func TestFreelistPagesFromFreeIDs(t *testing.T) {
	const per = idsPerFreelistPage
	for _, target := range []int{per, per + 1, per + 2, 2*per + 1, 2*per + 2, 2*per + 3} {
		db, err := Open(t.TempDir(), Options{MemtableBytes: -1, SyncMode: SyncOS})
		if err != nil {
			t.Fatal(err)
		}
		insertAll(t, db, randKV(rand.New(rand.NewSource(int64(target))), 200))
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		db.mu.Lock()
		pg := db.pager
		// the list will hold exactly target ids before its own pages are allocated
		extra := make([]uint32, target-len(pg.free)-len(pg.pending)-len(db.freelistPages))
		for i := range extra {
			extra[i] = pg.allocID() // may pop pg.free: collect before appending
		}
		pg.free = append(pg.free, extra...)
		sort.Slice(pg.free, func(i, j int) bool { return pg.free[i] > pg.free[j] })
		cs := &captureSet{}
		head, err := db.captureFreelist(cs)
		if err != nil {
			t.Fatal(err)
		}
		want := map[uint32]bool{}
		for _, ids := range [][]uint32{pg.free, pg.pending, cs.toRelease} {
			for _, id := range ids {
				want[id] = true
			}
		}
		got := map[uint32]bool{}
		for i, id := range db.freelistPages {
			if want[id] {
				t.Fatalf("%d ids: list page %d is recorded as free", target, id)
			}
			if i == 0 && id != head {
				t.Fatalf("%d ids: head %d, first page %d", target, head, id)
			}
			p := pg.loaded(id).data
			if p.chainLen() > per {
				t.Fatalf("%d ids: page %d holds %d ids", target, id, p.chainLen())
			}
			for j := 0; j < p.chainLen(); j++ {
				got[le.Uint32(p[hdrSize+4*j:])] = true
			}
		}
		if len(got) != len(want) {
			t.Fatalf("%d ids: list records %d ids, want %d", target, len(got), len(want))
		}
		for id := range want {
			if !got[id] {
				t.Fatalf("%d ids: free id %d is not recorded", target, id)
			}
		}
		db.mu.Unlock()
		db.crash() // the capture was never written
	}
}
