package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

type pausedWALWrite struct {
	fileLike
	once            sync.Once
	started, resume chan struct{}
}

func (f *pausedWALWrite) WriteAt(p []byte, off int64) (int, error) {
	f.once.Do(func() { close(f.started); <-f.resume })
	return f.fileLike.WriteAt(p, off)
}

// Force active-buffer growth while another buffer is held by off-lock I/O.
// Replay then verifies full values, including the bytes that were still
// being written when the active buffer's old mapping was released.
func TestWALBufferGrowthDuringInflightWrite(t *testing.T) {
	for _, heapOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(heapOnly), func(t *testing.T) { testWALBufferGrowth(t, heapOnly) })
	}
}

func testWALBufferGrowth(t *testing.T, heapOnly bool) {
	dir := t.TempDir()
	w := newWAL(dir)
	w.heapOnly = heapOnly
	if err := w.start(1, 0); err != nil {
		t.Fatal(err)
	}
	f := &pausedWALWrite{fileLike: w.f, started: make(chan struct{}), resume: make(chan struct{})}
	w.f = f
	var once sync.Once
	resume := func() { once.Do(func() { close(f.resume) }) }
	defer resume()
	want := map[string][]byte{}
	appendRange := func(lo, hi int) {
		for i := lo; i < hi; i++ {
			key := fmt.Sprintf("key-%03d", i)
			val := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 320<<10)
			want[key] = val
			if _, err := w.append([]byte(key), val, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendRange(0, 32)
	done := make(chan error, 1)
	go func() { done <- w.flushAsync() }()
	select {
	case <-f.started:
	case <-time.After(10 * time.Second):
		t.Fatal("WAL write did not start")
	}
	appendRange(32, 96)
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	r := newWAL(dir)
	defer r.close()
	seen := 0
	_, _, err := r.replayFrom(1, walHeaderSize, func(k, v []byte, del bool) error {
		if del || !bytes.Equal(v, want[string(k)]) {
			return fmt.Errorf("incorrect replayed value for %s", k)
		}
		seen++
		return nil
	})
	if err != nil || seen != len(want) {
		t.Fatalf("replay: seen=%d want=%d err=%v", seen, len(want), err)
	}
}
