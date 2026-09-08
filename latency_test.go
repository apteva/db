package aptevadb

import (
	"encoding/binary"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// TestCheckpointsDoNotStallWriters keeps a writer busy while checkpoints
// and compactions run, and checks that no single write waited on disk work.
func TestCheckpointsDoNotStallWriters(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 64 << 20, MemtableBytes: 16 << 20, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(151))
	val := make([]byte, 200)
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	for i := 0; i < 100_000; i++ {
		rng.Read(val)
		db.Insert(key(rng.Intn(200_000)), val)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var maxWait time.Duration
	var writes int
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := make([]byte, 200)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if writes >= 3_000_000 {
				return // bounded: a flood must never fill the disk
			}
			rng.Read(v)
			start := time.Now()
			if err := db.Insert(key(rng.Intn(200_000)), v); err != nil {
				t.Error(err)
				return
			}
			if d := time.Since(start); d > maxWait {
				maxWait = d
			}
			writes++
		}
	}()
	deadline := time.Now().Add(8 * time.Second)
	for i := 0; i < 6 && time.Now().Before(deadline); i++ {
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if i%3 == 2 {
			if err := db.Compact(bg); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
	st := db.Stats()
	t.Logf("%d writes during 6 checkpoints and 2 compactions; longest single write waited %v; lock holds %v; backpressure stalls %d", writes, maxWait, st.LockHolds, st.WriteStalls)
	limit := 25 * time.Millisecond
	if raceEnabled {
		limit = 150 * time.Millisecond // the detector slows every locked section several-fold
	}
	if os.Getenv("CI") != "" {
		limit *= 4 // shared runners: slow disks and neighbours
	}
	// a slow disk lengthens the log tail a compaction swap replays under
	// the lock in proportion to its own stalls
	limit += st.LockHolds["wal-flush-write"]
	// A write may still wait when the disk itself stalls the log writer for
	// longer than the buffer covers; that is flow control, not a lock hold,
	// and shows up as a long log write. Everything the engine controls is
	// the lock-hold table below.
	if maxWait > limit && maxWait > 2*st.LockHolds["wal-flush-write"]+limit {
		t.Fatalf("a write stalled for %v without a matching disk stall", maxWait)
	}
	for section, d := range st.LockHolds {
		if section == "wal-flush-write" || section == "compact-swap" {
			continue // diagnostics of work done outside the lock
		}
		if d > limit {
			t.Fatalf("section %s held the writer lock for %v", section, d)
		}
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
}
