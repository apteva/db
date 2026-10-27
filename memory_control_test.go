package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryAccountingLeasesAndCompaction(t *testing.T) {
	d, err := Open(t.TempDir(), Options{CacheBytes: 8 << 20})
	mustCompat(t, err)
	defer d.Close()
	for i := 0; i < 100; i++ {
		mustCompat(t, d.Upsert([]byte(fmt.Sprint(i)), bytes.Repeat([]byte{byte(i)}, 4096)))
	}
	s, err := d.Snapshot()
	mustCompat(t, err)
	cur := s.NewCursor()
	before, err := d.MemoryStats(bg)
	mustCompat(t, err)
	if before.ProcessResidentBytes == 0 || before.MemtableBytes == 0 || before.SnapshotPinnedBytes == 0 || before.PinnedBytes > before.ManagedBufferBytes {
		t.Fatalf("%+v", before)
	}
	mustCompat(t, d.Checkpoint())
	s.Close()
	held, err := d.MemoryStats(bg)
	mustCompat(t, err)
	if held.RetainedMemtableBytes == 0 {
		t.Fatalf("cursor lease missing: %+v", held)
	}
	mustCompat(t, cur.Close())
	released, err := d.MemoryStats(bg)
	mustCompat(t, err)
	if released.RetainedMemtableBytes != 0 {
		t.Fatalf("lease leaked: %+v", released)
	}
	mustCompat(t, d.Compact(bg))
	after, err := d.MemoryStats(bg)
	mustCompat(t, err)
	if after.ManagedBufferBytes != after.PrivatePageBytes+after.DescriptorBytes+after.MemtableBytes+after.WALBytes || after.QueryWorkingBytes != 0 || after.PrivatePageBytes < 0 || after.MemtableBytes < 0 || after.WALBytes < 0 {
		t.Fatal(after)
	}
	mustCompat(t, d.Verify(bg))
}
func TestColdMappedReclamationPreservesViews(t *testing.T) {
	d, err := Open(t.TempDir(), Options{CacheBytes: 4 << 20, MappedCacheBytes: 1 << 20})
	mustCompat(t, err)
	defer d.Close()
	val := bytes.Repeat([]byte{91}, 2048)
	for i := 0; i < 8000; i++ {
		mustCompat(t, d.Upsert([]byte(fmt.Sprintf("%06d", i)), val))
	}
	mustCompat(t, d.Checkpoint())
	mustCompat(t, d.Compact(bg))
	s, err := d.Snapshot()
	mustCompat(t, err)
	defer s.Close()
	mustCompat(t, s.Scan(nil, nil, func(_, v []byte) bool {
		if !bytes.Equal(v, val) {
			t.Fatal("value changed")
		}
		return true
	}))
	before, err := d.MemoryStats(bg)
	mustCompat(t, err)
	_, err = d.TrimMemory(bg, 1<<20)
	mustCompat(t, err)
	n, err := d.TrimMemory(bg, 1<<20)
	mustCompat(t, err)
	after, err := d.MemoryStats(bg)
	mustCompat(t, err)
	if n == 0 || after.MappedResidentEstimateBytes >= before.MappedResidentEstimateBytes {
		t.Fatalf("before=%+v after=%+v reclaimed=%d", before, after, n)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				v, ok, e := s.Find([]byte(fmt.Sprintf("%06d", (i*7+g)%8000)), nil)
				if e != nil || !ok || !bytes.Equal(v, val) {
					t.Error(e, ok)
					return
				}
			}
		}(g)
	}
	for i := 0; i < 4; i++ {
		_, err = d.TrimMemory(bg, 1<<20)
		mustCompat(t, err)
	}
	wg.Wait()
	mustCompat(t, d.Verify(bg))
}
func TestMemoryPressureBeforeLoggingAndCancellation(t *testing.T) {
	// Metadata/one memtable arena fit normally. Snapshots retain many completed
	// tables; checkpointing must not conceal those retained allocations.
	d, err := Open(t.TempDir(), Options{CacheBytes: 1 << 20, ManagedMemoryBytes: 4 << 20})
	mustCompat(t, err)
	defer d.Close()
	var views []*Snapshot
	defer func() {
		for _, s := range views {
			s.Close()
		}
	}()
	for i := 0; i < 8; i++ {
		mustCompat(t, d.Upsert([]byte("held"), bytes.Repeat([]byte{byte(i)}, 256<<10)))
		s, e := d.Snapshot()
		mustCompat(t, e)
		views = append(views, s)
		mustCompat(t, d.Checkpoint())
		if d.memory.total() > 4<<20 {
			break
		}
	}
	if d.memory.total() <= 4<<20 {
		t.Fatal("fixture did not retain enough storage")
	}
	before := d.revision
	if err := d.Upsert([]byte("rejected"), []byte("x")); !errors.Is(err, ErrMemoryPressure) {
		t.Fatal(err)
	}
	if d.revision != before {
		t.Fatal("rejected write was applied")
	}
	b := d.NewBatch()
	mustCompat(t, b.Upsert([]byte("cancelled"), []byte("x")))
	ctx, cancel := context.WithTimeout(bg, 30*time.Millisecond)
	defer cancel()
	if err := b.CommitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, ok, e := d.Find([]byte("cancelled"), nil); e != nil || ok {
		t.Fatal(e, ok)
	}
	ctx2, cancel2 := context.WithTimeout(bg, time.Second)
	defer cancel2()
	done := make(chan error, 1)
	go func() { done <- b.CommitContext(ctx2) }()
	time.Sleep(10 * time.Millisecond)
	for _, s := range views {
		s.Close()
	}
	views = nil
	select {
	case e := <-done:
		mustCompat(t, e)
	case <-ctx2.Done():
		t.Fatal("writer did not resume")
	}
	if _, ok, e := d.Find([]byte("cancelled"), nil); e != nil || !ok {
		t.Fatal(e, ok)
	}
}
func TestAggregateBudgetOwnershipReleased(t *testing.T) {
	c := aggregateFixture(t, 2000)
	var st AggregateStats
	eachRows(t, c, `[{"$group":{"_id":"$g","n":{"$sum":1}}}]`, AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: 64 << 10, Stats: &st})
	if c.db.memory.queryBytes() != 0 || st.PeakMemoryBytes == 0 {
		t.Fatal(c.db.memory.queryBytes(), st)
	}
	_, err := c.Aggregate(bg, []byte(`[{"$group":{"_id":"$g","n":{"$sum":1}}}]`))
	mustCompat(t, err)
	if c.db.memory.queryBytes() != 0 {
		t.Fatal("legacy aggregate reservation leaked")
	}
}
