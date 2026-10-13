package db

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type failingWriteFile struct {
	fileLike
	enabled *atomic.Bool
	short   bool
}

func (f *failingWriteFile) WriteAt(b []byte, off int64) (int, error) {
	if !f.enabled.Load() {
		return f.fileLike.WriteAt(b, off)
	}
	if f.short {
		n := len(b) / 2
		_, err := f.fileLike.WriteAt(b[:n], off)
		return n, err
	}
	return 0, syscall.ENOSPC
}
func TestWALIOFailureIsSticky(t *testing.T) {
	for _, short := range []bool{false, true} {
		original := openFile
		t.Cleanup(func() { openFile = original })
		enabled := &atomic.Bool{}
		openFile = func(path string) (fileLike, error) {
			f, err := original(path)
			if err != nil {
				return nil, err
			}
			if strings.HasPrefix(filepath.Base(path), "wal-") {
				return &failingWriteFile{f, enabled, short}, nil
			}
			return f, nil
		}
		dir := t.TempDir()
		db, err := Open(dir, Options{LogFlushInterval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Upsert([]byte("safe"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err = db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		enabled.Store(true)
		if err = db.Upsert([]byte("lost"), bytes.Repeat([]byte("x"), 2<<20)); err != nil {
			t.Fatal(err)
		}
		err = db.wal.flushAsync()
		want := error(syscall.ENOSPC)
		if short {
			want = io.ErrShortWrite
		}
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
		if err = db.Upsert([]byte("after"), nil); !errors.Is(err, want) {
			t.Fatal("continued after failed log write", err)
		}
		if err = db.Health(); !errors.Is(err, want) {
			t.Fatal(err)
		}
		db.crash()
		enabled.Store(false)
		openFile = original
		db, err = Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		v, ok, err := db.Find([]byte("safe"), nil)
		if err != nil || !ok || string(v) != "value" {
			t.Fatal(string(v), ok, err)
		}
		if _, ok, _ = db.Find([]byte("after"), nil); ok {
			t.Fatal("unacknowledged later write was accepted")
		}
		db.Close()
	}
}
