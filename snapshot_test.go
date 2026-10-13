package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"testing"
)

func TestSnapshotIsolation(t *testing.T) {
	for _, opts := range []Options{{SyncMode: SyncOS}, {CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10, SyncMode: SyncOS}} {
		db, err := Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(131))
		m := randKV(rng, 30_000)
		insertAll(t, db, m)
		db.Checkpoint()
		frozen := map[string][]byte{}
		for k, v := range m {
			frozen[k] = v
		}
		s1, err := db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		// churn: updates, deletes, inserts, checkpoints that move pages
		i := 0
		for k := range m {
			switch i % 4 {
			case 0:
				v := make([]byte, rng.Intn(300))
				rng.Read(v)
				m[k] = v
				db.Upsert([]byte(k), v)
			case 1:
				db.Delete([]byte(k))
				delete(m, k)
			}
			i++
			if i%5000 == 0 {
				db.Checkpoint()
			}
		}
		more := randKV(rng, 5_000)
		insertAll(t, db, more)
		for k, v := range more {
			m[k] = v
		}
		s2, _ := db.Snapshot()
		big := make([]byte, 50_000)
		rng.Read(big)
		db.Upsert([]byte("big"), big)
		m["big"] = big
		db.Checkpoint()

		// s1 sees exactly the frozen state, s2 the middle one, live the latest
		checkView := func(name string, find func([]byte, []byte) ([]byte, bool, error), scan func([]byte, []byte, func([]byte, []byte) bool) error, want map[string][]byte) {
			var buf []byte
			for k, v := range want {
				got, ok, err := find([]byte(k), buf)
				if err != nil || !ok || !bytes.Equal(got, v) {
					t.Fatalf("%s: key %x: ok=%v err=%v len=%d want %d", name, k, ok, err, len(got), len(v))
				}
				buf = got
			}
			n := 0
			var prev []byte
			scan(nil, nil, func(k, v []byte) bool {
				if prev != nil && bytes.Compare(prev, k) >= 0 {
					t.Fatalf("%s: scan out of order", name)
				}
				prev = append(prev[:0], k...)
				if !bytes.Equal(v, want[string(k)]) {
					t.Fatalf("%s: scan value mismatch for %x", name, k)
				}
				n++
				return true
			})
			if n != len(want) {
				t.Fatalf("%s: scan saw %d keys, want %d", name, n, len(want))
			}
		}
		checkView("s1", s1.Find, s1.Scan, frozen)
		checkView("live", db.Find, db.Scan, m)
		if _, ok, _ := s1.Find([]byte("big"), nil); ok {
			t.Fatal("s1 sees a key inserted after it")
		}
		if v, ok, _ := s2.Find([]byte("big"), nil); ok || v != nil && len(v) > 0 {
			t.Fatal("s2 sees a key inserted after it")
		}
		// cursor and aggregation over the snapshot
		c := s1.NewCursor()
		defer c.Close()
		cnt := 0
		for ok := c.First(); ok; ok = c.Next() {
			if !bytes.Equal(c.Value(), frozen[string(c.Key())]) {
				t.Fatal("snapshot cursor value mismatch")
			}
			cnt++
		}
		if cnt != len(frozen) {
			t.Fatalf("snapshot cursor saw %d keys, want %d", cnt, len(frozen))
		}
		if err := db.Compact(bg); !errors.Is(err, ErrSnapshotsOpen) {
			t.Fatalf("compaction with open snapshots should be refused, got %v", err)
		}
		st := db.Stats()
		if st.Snapshots != 2 || st.QuarantinedPages == 0 {
			t.Fatalf("expected 2 snapshots and quarantined pages: %+v", st)
		}
		s1.Close()
		s2.Close()
		s2.Close() // idempotent
		if st := db.Stats(); st.Snapshots != 0 || st.QuarantinedPages != 0 {
			t.Fatalf("quarantine not released: %+v", st)
		}
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		if err := db.Verify(bg); err != nil {
			t.Fatal(err)
		}
		checkView("live after compact", db.Find, db.Scan, m)
		db.Close()
	}
}

func TestSnapshotUnderConcurrentWrites(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 128 * PageSize, MemtableBytes: 64 << 10, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const n = 20_000
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	val := func(i, gen int) []byte { return []byte{byte(gen), byte(i), byte(i >> 8)} }
	for i := 0; i < n; i++ {
		db.Upsert(key(i), val(i, 0))
	}
	db.Checkpoint()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// readers each take a snapshot and verify it stays internally consistent
	// (every value carries the same generation) while the writer moves on
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for round := 0; round < 6; round++ {
				s, err := db.Snapshot()
				if err != nil {
					t.Error(err)
					return
				}
				gen := -1
				var buf []byte
				for i := 0; i < 3000; i++ {
					j := rng.Intn(n)
					got, ok, err := s.Find(key(j), buf)
					if err != nil || !ok {
						t.Errorf("snapshot find %d: ok=%v err=%v", j, ok, err)
						s.Close()
						return
					}
					buf = got
					if gen < 0 {
						gen = int(got[0])
					} else if int(got[0]) != gen {
						t.Errorf("snapshot mixed generations %d and %d", gen, got[0])
						s.Close()
						return
					}
				}
				cnt := 0
				s.Scan(nil, nil, func(k, v []byte) bool {
					if int(v[0]) != gen {
						t.Errorf("snapshot scan mixed generations")
						return false
					}
					cnt++
					return true
				})
				if cnt != n {
					t.Errorf("snapshot scan saw %d keys", cnt)
				}
				s.Close()
			}
		}(int64(r))
	}
	// the writer rewrites every key generation by generation, checkpointing as it goes
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for gen := 1; gen <= 4; gen++ {
			b := db.NewBatch()
			for i := 0; i < n; i++ {
				b.Upsert(key(i), val(i, gen))
			}
			if err := b.Commit(); err != nil {
				t.Error(err)
				return
			}
			db.Checkpoint()
		}
	}()
	wg.Wait()
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
}
