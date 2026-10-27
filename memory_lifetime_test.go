package db

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMemtableStorageImmutableAndAligned(t *testing.T) {
	mt := newMemtable(16 << 20)
	defer mt.releaseBuffers()
	want := map[string][]byte{}
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("key-%d-%s", i, bytes.Repeat([]byte{'x'}, i%7)))
		value := bytes.Repeat([]byte{byte(i)}, 1+i%37)
		want[string(key)] = append([]byte(nil), value...)
		mt.put(key, value, false)
		for j := range key {
			key[j] = '!'
		}
		for j := range value {
			value[j] = '!'
		}
	}
	old := mt.snapshot()
	storage := mt.storage
	storage.retain()
	defer storage.release()
	for _, e := range old {
		if !bytes.Equal(e.v, want[e.k]) {
			t.Fatal("input mutation changed storage")
		}
	}
	for i := 0; i < 512; i++ {
		key := []byte(fmt.Sprintf("key-%d-%s", i, bytes.Repeat([]byte{'x'}, i%7)))
		mt.put(key, []byte("replacement"), i%3 == 0)
	}
	if mt.bufferedBytes() <= mt.bytes {
		t.Fatal("obsolete record storage was not charged to the budget")
	}
	mt.reset()
	runtime.GC()
	for _, e := range old {
		if !bytes.Equal(e.v, want[e.k]) || e.del {
			t.Fatal("frozen entries changed")
		}
	}
	if len(storage.arenas) == 0 {
		t.Fatal("retained storage was reclaimed")
	}
}

func TestMemtableHashCollisionsAndGrowth(t *testing.T) {
	mt := newMemtable()
	defer mt.releaseBuffers()
	var keys [][]byte
	for i := 0; len(keys) < 300; i++ {
		k := []byte(fmt.Sprintf("collision-%d", i))
		if maphash.Bytes(mt.seed, k)&15 == 0 {
			keys = append(keys, k)
		}
	}
	for i, k := range keys {
		mt.put(k, []byte(fmt.Sprint(i)), false)
	}
	if len(mt.slots) <= 16 {
		t.Fatal("hash table did not grow")
	}
	for i, k := range keys {
		v, del, ok := mt.get(k)
		if !ok || del || string(v) != fmt.Sprint(i) {
			t.Fatalf("key %d: %q %v %v", i, v, del, ok)
		}
		mt.put(k, nil, true)
	}
	for _, k := range keys {
		_, del, ok := mt.get(k)
		if !ok || !del {
			t.Fatal("lost tombstone")
		}
	}
	if _, _, ok := mt.get([]byte("absent")); ok {
		t.Fatal("false hit")
	}
}

func TestMemtableStoragePinnedDuringScan(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	value := bytes.Repeat([]byte{13}, 8193)
	if err = d.Upsert([]byte("a"), value); err != nil {
		t.Fatal(err)
	}
	storage := d.mt.storage
	s, err := d.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	err = s.Scan(nil, nil, func(k, v []byte) bool {
		s.Close()
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		if storage.refs.Load() != 1 || !bytes.Equal(v, value) {
			t.Fatal("scan lost retired record storage")
		}
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	if storage.refs.Load() != 0 || len(storage.arenas) != 0 {
		t.Fatal("scan retained retired record storage")
	}
}

func TestCursorRetainsStorageAfterDatabaseClose(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Upsert([]byte("a"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	storage := d.mt.storage
	s, err := d.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	c := s.def().NewCursor()
	if !c.First() {
		t.Fatal(c.Err())
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if storage.refs.Load() != 1 || len(storage.arenas) == 0 {
		t.Fatal("cursor storage reclaimed before close")
	}
	if c.Next() || !errors.Is(c.Err(), ErrClosed) {
		t.Fatalf("closed database cursor: %v", c.Err())
	}
	c.Close()
	if storage.refs.Load() != 0 || len(storage.arenas) != 0 {
		t.Fatal("cursor retained record storage")
	}
}

func TestTinyMemtableBudgetMakesProgress(t *testing.T) {
	d, err := Open(t.TempDir(), Options{CacheBytes: 8 * PageSize, MemtableBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 32; i++ {
		if err = d.Upsert([]byte(fmt.Sprint(i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = d.Verify(nil); err != nil {
		t.Fatal(err)
	}
}

func TestCapturedDescriptorsSurviveConcurrentDeletion(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	value := bytes.Repeat([]byte{1}, 2*PageSize+1)
	for i := 0; i < 128; i++ {
		if err = d.Upsert([]byte(fmt.Sprint(i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	paused := &pausedWALWrite{fileLike: d.pager.f, started: make(chan struct{}), resume: make(chan struct{})}
	d.pager.f = paused
	d.mu.Unlock()
	var once sync.Once
	resume := func() { once.Do(func() { close(paused.resume) }) }
	defer resume()
	value[0] = 2
	for i := 0; i < 128; i++ {
		if err = d.Upsert([]byte(fmt.Sprint(i)), value); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- d.Checkpoint() }()
	select {
	case <-paused.started:
	case <-time.After(10 * time.Second):
		t.Fatal("capture I/O did not start")
	}
	if st := d.Stats(); st.InFlightPages == 0 {
		t.Fatal("in-flight checkpoint pages were omitted from cache accounting")
	}
	for i := 0; i < 128; i++ {
		if err = d.Delete([]byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	d.mu.Lock()
	d.pager.releaseUnusedNodes()
	for _, a := range d.pager.nodeArenas {
		if a.live < 0 {
			t.Fatal("negative descriptor reference count")
		}
	}
	d.mu.Unlock()
	runtime.GC()
	resume()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if st := d.Stats(); st.InFlightPages != 0 {
		t.Fatal("completed checkpoint retained in-flight page accounting")
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = d.Verify(nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if _, found, err := d.Find([]byte(fmt.Sprint(i)), nil); err != nil || found {
			t.Fatalf("deleted key %d: %v %v", i, found, err)
		}
	}
}

// A valid large budget must not overflow the backpressure threshold and
// prevent even the first write from completing.
func TestLargeMemtableBudgetMakesProgress(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan error, 1)
	go func() { done <- d.Upsert([]byte("key"), []byte("value")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		d.Close()
		<-done
		t.Fatal("large memtable budget stalled a write")
	}
	if v, ok, err := d.Find([]byte("key"), nil); err != nil || !ok || string(v) != "value" {
		t.Fatalf("large-budget read: %q %v %v", v, ok, err)
	}
}
