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
	appendRange(0, 4)
	done := make(chan error, 1)
	go func() { done <- w.flushAsync() }()
	select {
	case <-f.started:
	case <-time.After(10 * time.Second):
		t.Fatal("WAL write did not start")
	}
	appendRange(4, 8)
	// Force mapped-to-mapped growth independently of append backpressure:
	// the paused I/O still owns its original mapping throughout this copy.
	w.mu.Lock()
	w.reserveBuffer(8 << 20)
	w.mu.Unlock()
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

func TestWALCheckpointReleasesBurstStorage(t *testing.T) {
	w := newWAL(t.TempDir())
	if err := w.start(1, 0); err != nil {
		t.Fatal(err)
	}
	defer w.close()
	value := bytes.Repeat([]byte{0x73}, 8<<20)
	l, err := w.append([]byte("large"), value, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.bufStore) <= walHeapBufferBytes {
		t.Fatal("test did not allocate a burst buffer")
	}
	gen, off, err := w.flushAndPosition()
	if err != nil || gen != l.gen || off != l.off {
		t.Fatalf("barrier: %d/%d, want %v: %v", gen, off, l, err)
	}
	if len(w.bufStore) > walHeapBufferBytes || len(w.spareStore) > walHeapBufferBytes {
		t.Fatal("checkpoint retained oversized WAL storage")
	}
	if _, err := w.append([]byte("small"), []byte("after barrier"), false); err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	seen := 0
	_, _, err = w.replayFrom(1, walHeaderSize, func(k, v []byte, del bool) error {
		want := value
		if string(k) == "small" {
			want = []byte("after barrier")
		}
		if del || !bytes.Equal(v, want) {
			return fmt.Errorf("incorrect replay after buffer retirement for %s", k)
		}
		seen++
		return nil
	})
	if err != nil || seen != 2 {
		t.Fatalf("replay: seen=%d err=%v", seen, err)
	}
}
