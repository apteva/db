package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// TestCursorStress drives cursors hard against writers, memtable applies,
// checkpoints, index churn and compactions for the duration in
// CURSOR_STRESS (skipped when unset). Every value a cursor returns must be
// the document of the key it reports.
func TestCursorStress(t *testing.T) {
	spec := os.Getenv("CURSOR_STRESS")
	if spec == "" {
		t.Skip("set CURSOR_STRESS to a duration")
	}
	total, err := time.ParseDuration(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, opts := range []Options{{CacheBytes: 16 << 20, SyncMode: SyncOS}, {SyncMode: SyncOS}} {
		db, err := Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		c, _ := db.Collection("c")
		const keys = 40_000
		doc := func(i, v int) []byte { return []byte(fmt.Sprintf(`{"k":%d,"v":%d,"g":%d}`, i, v, v%11)) }
		for i := 0; i < keys; i++ {
			c.UpsertJSON([]byte(fmt.Sprintf("k%06d", i)), doc(i, 0))
		}
		ctx, cancel := context.WithTimeout(context.Background(), total/2)
		var wg sync.WaitGroup
		fail := make(chan error, 8)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(w)))
				for v := 1; ctx.Err() == nil; v++ {
					i := rng.Intn(keys)
					var err error
					if rng.Intn(5) == 0 {
						err = c.Delete([]byte(fmt.Sprintf("k%06d", i)))
					} else {
						err = c.UpsertJSON([]byte(fmt.Sprintf("k%06d", i)), doc(i, v))
					}
					if err != nil {
						fail <- err
						return
					}
				}
			}(w)
		}
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(100 + r)))
				for ctx.Err() == nil {
					cur := c.NewCursor()
					n := 0
					start := rng.Intn(keys)
					var step func() bool
					var ok bool
					if rng.Intn(2) == 0 {
						ok, step = cur.Seek([]byte(fmt.Sprintf("k%06d", start))), cur.Next
					} else {
						ok, step = cur.Last(), cur.Prev
						if rng.Intn(2) == 0 {
							ok = cur.Seek([]byte(fmt.Sprintf("k%06d", start)))
						}
					}
					for ; ok && n < 300; ok = step() {
						var i int
						fmt.Sscanf(string(cur.Key()), "k%06d", &i)
						if k := docField(cur.Value(), "k"); k.k != kInt || int(k.i) != i {
							fail <- fmt.Errorf("cursor at %s returned document of %d", cur.Key(), k.i)
							return
						}
						n++
					}
					cur.Close()
					if cur.Err() != nil {
						fail <- cur.Err()
						return
					}
				}
			}(r)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				time.Sleep(300 * time.Millisecond)
				var err error
				switch i % 4 {
				case 0:
					err = db.Checkpoint()
				case 1:
					if _, has := c.Indexes()["by_g"]; has {
						err = c.DropIndex(ctx, "by_g")
					} else {
						err = c.CreateIndex(ctx, "by_g", "g")
					}
				case 2:
					err = db.Compact(ctx)
				default:
					s, serr := db.Snapshot()
					if serr == nil {
						cur := s.NewCursor()
						for ok := cur.First(); ok; ok = cur.Next() {
						}
						err = cur.Err()
						cur.Close()
						s.Close()
					} else {
						err = serr
					}
				}
				if err != nil && ctx.Err() == nil && !errors.Is(err, ErrSnapshotsOpen) && !errors.Is(err, ErrIndexBuilding) {
					fail <- err
					return
				}
			}
		}()
		wg.Wait()
		cancel()
		select {
		case err := <-fail:
			t.Fatalf("cache=%d: %v", opts.CacheBytes, err)
		default:
		}
		if db.Stats().Snapshots != 0 {
			t.Fatal("cursor snapshots leaked")
		}
		if err := db.Compact(bg); err != nil {
			t.Fatal("quiet compaction", err)
		}
		if err := db.Verify(bg); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
