package db

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestCompactionPinRetainsWALReplayRange(t *testing.T) {
	w := newWAL(t.TempDir())
	defer w.close()
	if err := w.start(1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.append([]byte("base"), []byte("value"), false); err != nil {
		t.Fatal(err)
	}
	gen, off, err := w.flushAndPosition()
	if err != nil {
		t.Fatal(err)
	}
	w.compactionPin.Store(gen + 1)
	for i := 0; i < 3; i++ {
		if err = w.rotate(); err != nil {
			t.Fatal(err)
		}
		if _, err = w.append([]byte(fmt.Sprint(i)), []byte("delta"), false); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	w.dropBefore(4)
	n := 0
	if _, _, err = w.replayFrom(gen, off, func(k, v []byte, del bool) error {
		n++
		if string(v) != "delta" || del {
			t.Fatal("incorrect delta")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("replayed %d deltas", n)
	}
	w.compactionPin.Store(0)
	w.dropBefore(4)
	if _, err = os.Stat(walPath(w.dir, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpinned log retained: %v", err)
	}
}

func TestConcurrentCompactionsSerialize(t *testing.T) {
	d, err := Open(t.TempDir(), Options{CacheBytes: 8 * PageSize, MemtableBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 2000; i++ {
		if err = d.Upsert([]byte(fmt.Sprint(i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; done <- d.Compact(nil) }()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err = <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Verify(nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		v, found, err := d.Find([]byte(fmt.Sprint(i)), nil)
		if err != nil || !found || string(v) != "value" {
			t.Fatalf("key %d: %v %v %q", i, found, err, v)
		}
	}
}

func TestCompactionReplayStopsAtFlushedPosition(t *testing.T) {
	w := newWAL(t.TempDir())
	defer w.close()
	if err := w.start(1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.append([]byte("first"), []byte("1"), false); err != nil {
		t.Fatal(err)
	}
	gen, off, err := w.flushAndPosition()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.append([]byte("later"), []byte("2"), false); err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	if err = w.rotate(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.append([]byte("future-generation"), []byte("3"), false); err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	var keys []string
	_, _, err = w.replayFrom(1, walHeaderSize, func(k, v []byte, del bool) error { keys = append(keys, string(k)); return nil }, lsn{gen, off})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "first" {
		t.Fatalf("replayed past bounded position: %v", keys)
	}
}
