package db

import (
	"encoding/binary"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestWhereWritersStall reports the longest write wait during checkpoints
// alone and during compactions alone, to attribute stalls.
func TestWhereWritersStall(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{CacheBytes: 64 << 20, MemtableBytes: 16 << 20, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(161))
	val := make([]byte, 200)
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	for i := 0; i < 100_000; i++ {
		rng.Read(val)
		db.Upsert(key(rng.Intn(200_000)), val)
	}
	measure := func(name string, work func()) {
		var wg sync.WaitGroup
		stop := make(chan struct{})
		var waits []time.Duration
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := make([]byte, 200)
			r := rand.New(rand.NewSource(1))
			for len(waits) < 2_000_000 {
				select {
				case <-stop:
					return
				default:
				}
				r.Read(v)
				start := time.Now()
				db.Upsert(key(r.Intn(200_000)), v)
				waits = append(waits, time.Since(start))
			}
		}()
		work()
		close(stop)
		wg.Wait()
		sort.Slice(waits, func(i, j int) bool { return waits[i] > waits[j] })
		gens, _ := walGens(dir)
		var walBytes int64
		for _, g := range gens {
			if st, err := os.Stat(walPath(dir, g)); err == nil {
				walBytes += st.Size()
			}
		}
		st := db.Stats()
		t.Logf("%s: %d writes, worst waits %v; wal gens=%d (%d MB) memtable=%d entries stalls=%d", name, len(waits), waits[:min(5, len(waits))], len(gens), walBytes>>20, st.MemtableEntries, st.WriteStalls)
	}
	measure("checkpoints", func() {
		for i := 0; i < 6; i++ {
			db.Checkpoint()
		}
	})
	measure("compactions", func() {
		for i := 0; i < 2; i++ {
			if err := db.Compact(bg); err != nil {
				t.Fatal(err)
			}
		}
	})
	measure("idle", func() { time.Sleep(300 * time.Millisecond) })
	t.Logf("longest lock holds: %v", db.Stats().LockHolds)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
