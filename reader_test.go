package db

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestReaderViewsAndLifecycle(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 8 * PageSize, MemtableBytes: 2 * PageSize})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.Collection("native")
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{[]byte("key"), bytes.Repeat([]byte{1}, 63), bytes.Repeat([]byte{2}, 64), bytes.Repeat([]byte{3}, MaxKeySize)}
	for _, key := range keys {
		if err := db.Upsert(key, []byte("default-old")); err != nil {
			t.Fatal(err)
		}
		if err := c.Upsert(key, []byte("collection-old")); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	sc, err := snap.Collection("native")
	if err != nil {
		t.Fatal(err)
	}
	live, err := db.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	named, err := c.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer named.Close()
	old, err := snap.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	oldNamed, err := sc.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer oldNamed.Close()
	for _, key := range keys {
		if err := db.Upsert(key, []byte("default-new")); err != nil {
			t.Fatal(err)
		}
		if err := c.Upsert(key, []byte("collection-new")); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		r    *Reader
		want string
	}{{live, "default-new"}, {named, "collection-new"}, {old, "default-old"}, {oldNamed, "collection-old"}} {
		for _, key := range keys {
			v, ok, err := test.r.Find(key, make([]byte, 4, 128))
			if err != nil || !ok || string(v) != test.want {
				t.Fatalf("find: %q %v %v want %q", v, ok, err, test.want)
			}
		}
		if _, ok, err := test.r.Find([]byte("missing"), nil); err != nil || ok {
			t.Fatalf("missing: %v %v", ok, err)
		}
		if _, _, err := test.r.Find(make([]byte, MaxKeySize+1), nil); !errors.Is(err, ErrKeySize) {
			t.Fatal(err)
		}
	}
	snap.Close()
	if _, _, err := old.Find(keys[0], nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot: %v", err)
	}
	if _, err := snap.NewReader(); !errors.Is(err, ErrClosed) {
		t.Fatalf("reader on closed snapshot: %v", err)
	}
	for _, r := range []*Reader{live, named, old, oldNamed} {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Find(keys[0], nil); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
	if err := db.Upsert([]byte("after-readers"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewReader(); !errors.Is(err, ErrClosed) {
		t.Fatalf("reader on closed DB: %v", err)
	}
}

func TestReaderRegistrationAndCloseDuringWrites(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("hot")
	value := bytes.Repeat([]byte{7}, 100)
	if err := db.Upsert(key, value); err != nil {
		t.Fatal(err)
	}
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; i < 3000; i++ {
			if err := db.Upsert(key, value); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for round := 0; round < 40; round++ {
		r, err := db.NewReader()
		if err != nil {
			t.Fatal(err)
		}
		var readers sync.WaitGroup
		for g := 0; g < 8; g++ {
			readers.Add(1)
			go func() {
				defer readers.Done()
				var buf []byte
				for i := 0; i < 500; i++ {
					v, ok, err := r.Find(key, buf)
					if errors.Is(err, ErrClosed) {
						return
					}
					if err != nil || !ok || !bytes.Equal(v, value) {
						t.Errorf("read during close: %v %v", ok, err)
						return
					}
					buf = v
				}
			}()
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		readers.Wait()
	}
	writer.Wait()
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
}
