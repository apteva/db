package db

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// Exercise overflow pages, replacements, snapshot aliases and reclamation
// together: an arena must survive checkpoint publication while old versions
// are readable, then be returned once no view can reference its buffers.
func TestWritableBufferLifetimeAcrossSnapshots(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 8 * PageSize, MemtableBytes: -1}} {
		t.Run(fmt.Sprint(opts.CacheBytes), func(t *testing.T) { testWritableBufferLifetime(t, opts) })
	}
}

func testWritableBufferLifetime(t *testing.T, opts Options) {
	dir := t.TempDir()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const count = 512
	value := func(i, gen int) []byte {
		return bytes.Repeat([]byte{byte(i), byte(i >> 8), byte(gen)}, 12000+gen)
	}
	load := func(gen int) {
		for i := 0; i < count; i++ {
			if err := db.Upsert([]byte(fmt.Sprint(i)), value(i, gen)); err != nil {
				t.Fatal(err)
			}
		}
	}
	checkView := func(find func([]byte, []byte) ([]byte, bool, error), gen int) {
		for i := 0; i < count; i++ {
			v, found, err := find([]byte(fmt.Sprint(i)), nil)
			if err != nil || !found || !bytes.Equal(v, value(i, gen)) {
				t.Fatalf("generation %d key %d: found=%v err=%v", gen, i, found, err)
			}
		}
	}
	load(0)
	s0, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	load(1)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkView(s0.Find, 0)
	checkView(db.Find, 1)
	s1, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	load(2)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkView(s0.Find, 0)
	checkView(s1.Find, 1)
	checkView(db.Find, 2)
	s0.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkView(s1.Find, 1)
	s1.Close()
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if len(db.pager.bufferArenas) != 0 || len(db.pager.oldMaps) != 0 {
		t.Fatalf("checkpoint retained unused buffers: arenas=%d maps=%d", len(db.pager.bufferArenas), len(db.pager.oldMaps))
	}
	checkView(db.Find, 2)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkView(db.Find, 2)
}

func TestStreamingBackupEmptyTree(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(bg, dir); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	if copyDB.pager.hwm != db.pager.hwm {
		t.Fatalf("empty backup grew from %d to %d pages", db.pager.hwm, copyDB.pager.hwm)
	}
	if err := copyDB.Verify(bg); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCloseDuringScanAndReclamation(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 2000; i++ {
		if err := db.Upsert([]byte(fmt.Sprintf("%06d", i)), bytes.Repeat([]byte{1}, 100)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Upsert([]byte("000000"), []byte("changed")); err != nil {
		t.Fatal(err)
	}
	closed := false
	err = s.Scan(nil, nil, func(k, v []byte) bool {
		if !closed {
			closed = true
			s.Close()
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
		return true
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("scan of closed snapshot returned %v", err)
	}
}
