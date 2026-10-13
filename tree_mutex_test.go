package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTreeMutexWriterExclusion(t *testing.T) {
	var mu treeMutex
	mu.Lock()
	g, _ := mu.register()
	mu.Unlock()
	var active atomic.Int64
	var writerInside atomic.Bool
	var readers, writers sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 32; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lane := g.readLock(&mu, &g.lanes[r%pointReadLanes])
				active.Add(1)
				if writerInside.Load() {
					t.Error("point reader overlapped writer")
				}
				runtime.Gosched()
				if writerInside.Load() {
					t.Error("writer entered during point read")
				}
				active.Add(-1)
				g.readUnlock(&mu, lane)
			}
		}(r)
	}
	for w := 0; w < 4; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 200; i++ {
				mu.Lock()
				if writerInside.Swap(true) {
					t.Error("writers overlapped")
				}
				if active.Load() != 0 {
					t.Error("writer admitted before point readers drained")
				}
				runtime.Gosched()
				writerInside.Store(false)
				mu.Unlock()
			}
		}()
	}
	writers.Wait()
	close(stop)
	readers.Wait()
}

func TestTreeMutexCondWaitReleasesPointReaders(t *testing.T) {
	var mu treeMutex
	mu.Lock()
	g, registeredLane := mu.register()
	mu.Unlock()
	cond := sync.NewCond(&mu)
	ready, done := make(chan struct{}), make(chan struct{})
	released := false
	go func() {
		mu.Lock()
		close(ready)
		for !released {
			cond.Wait()
		}
		mu.Unlock()
		close(done)
	}()
	<-ready
	lane := g.readLock(&mu, registeredLane)
	g.readUnlock(&mu, lane)
	mu.Lock()
	released = true
	cond.Signal()
	mu.Unlock()
	<-done
}

// Exercise point reads of both a hot key and many keys while batches replace
// whole values, checkpoints evict pages, a snapshot retains old versions,
// compaction replaces the mapping, and Close releases it.
func TestPointReadsAcrossWriterLifetimes(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprintf("bounded=%v", bounded), func(t *testing.T) {
			opts := Options{}
			if bounded {
				opts.CacheBytes = 8 * PageSize
				opts.MemtableBytes = 2 * PageSize
			}
			db, err := Open(t.TempDir(), opts)
			if err != nil {
				t.Fatal(err)
			}
			const n = 256
			key := func(i int) []byte {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i))
				return k
			}
			value := func(i, generation int) []byte {
				v := bytes.Repeat([]byte{byte(generation)}, 3000)
				binary.BigEndian.PutUint64(v, uint64(i))
				return v
			}
			for i := 0; i < n; i++ {
				if err := db.Upsert(key(i), value(i, 0)); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			snap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			var live, snapshots, ready sync.WaitGroup
			stop := make(chan struct{})
			defer func() { close(stop); live.Wait(); snapshots.Wait(); snap.Close(); db.Close() }()
			ready.Add(32)
			for r := 0; r < 32; r++ {
				isSnapshot := r >= 16
				wg := &live
				if isSnapshot {
					wg = &snapshots
				}
				wg.Add(1)
				go func(r int, isSnapshot bool, wg *sync.WaitGroup) {
					defer wg.Done()
					first := true
					defer func() {
						if first {
							ready.Done()
						}
					}()
					find := db.Find
					if isSnapshot {
						find = snap.Find
					}
					if r%4 >= 2 {
						newReader := db.NewReader
						if isSnapshot {
							newReader = snap.NewReader
						}
						handle, err := newReader()
						if err != nil {
							t.Error(err)
							return
						}
						defer handle.Close()
						find = handle.Find
					}
					var buf []byte
					for iteration := 0; ; iteration++ {
						if isSnapshot && iteration >= 1500 {
							return
						}
						select {
						case <-stop:
							return
						default:
						}
						i := (iteration*37 + r) % n
						if r%2 == 0 {
							i = 0
						} // hot-key traffic must also distribute
						got, ok, err := find(key(i), buf)
						if errors.Is(err, ErrClosed) && !isSnapshot {
							return
						}
						if err != nil || !ok || len(got) != 3000 {
							t.Errorf("reader %d: found=%v len=%d err=%v", r, ok, len(got), err)
							return
						}
						if binary.BigEndian.Uint64(got) != uint64(i) {
							t.Errorf("reader %d returned wrong key", r)
							return
						}
						generation := got[8]
						if isSnapshot && generation != 0 {
							t.Errorf("snapshot reader %d returned generation %d", r, generation)
							return
						}
						for _, b := range got[8:] {
							if b != generation {
								t.Errorf("reader %d observed a torn value", r)
								return
							}
						}
						buf = got
						if first {
							first = false
							ready.Done()
						}
					}
				}(r, isSnapshot, wg)
			}
			ready.Wait()
			for generation := 1; generation <= 8; generation++ {
				b := db.NewBatch()
				for i := 0; i < n; i++ {
					if err := b.Upsert(key(i), value(i, generation)); err != nil {
						t.Fatal(err)
					}
				}
				if err := b.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			snapshots.Wait()
			snap.Close()
			if err := db.Compact(bg); err != nil {
				t.Fatal(err)
			}
			if err := db.Verify(bg); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			live.Wait()
			if _, _, err := db.Find(key(0), nil); !errors.Is(err, ErrClosed) {
				t.Fatalf("read after close: %v", err)
			}
		})
	}
}
