package db

import "syscall"

const walHeapBufferBytes = 2 << 20

// Flush a complete prefix before crossing the ordinary backlog bound. Growing
// first would double a nearly full buffer only to flush it immediately. A
// single oversized record/batch still stays intact and is allowed to exceed it.
func (w *wal) prepareAppendLocked(extra int) error {
	if len(w.buf) > 0 && extra > walMaxBuffered-len(w.buf) {
		return w.flushLocked()
	}
	return nil
}

// Keep normal flush buffers on the Go heap. Removing all of that live
// storage lowers Go's GC target and adds collection overhead to allocation-heavy
// queries on small databases. Large log bursts still use reclaimable mappings.
func newWALBuffer(size int) []byte {
	if size <= walHeapBufferBytes {
		return make([]byte, size)
	}
	buf, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		panic("aptevadb: cannot allocate WAL buffer: " + err.Error())
	}
	return buf
}

// reserveBuffer is called under w.mu before appending. The active buffer
// is never used by the off-lock I/O worker: that worker owns inflightStore.
// Geometric growth copies once and immediately returns the old mapping,
// avoiding accumulated obsolete Go arrays during a fast log burst.
func (w *wal) reserveBuffer(extra int) {
	if extra <= cap(w.buf)-len(w.buf) {
		return
	}
	size := max(len(w.buf)+extra, 2*cap(w.buf), 2*walFlushBytes)
	var buf []byte
	if w.heapOnly {
		buf = make([]byte, size)
	} else {
		buf = newWALBuffer(size)
	}
	copy(buf, w.buf)
	old := w.bufStore
	if w.memory != nil {
		w.memory.wal.Add(int64(len(buf) - len(old)))
	}
	w.buf, w.bufStore = buf[:len(w.buf)], buf
	if !w.heapOnly {
		releaseWALBuffer(old)
	}
}

func (w *wal) releaseBuffers() {
	if w.memory != nil {
		w.memory.wal.Add(-int64(len(w.bufStore) + len(w.spareStore)))
	}
	if w.bufStore != nil && !w.heapOnly {
		releaseWALBuffer(w.bufStore)
	}
	if w.spareStore != nil && !w.heapOnly {
		releaseWALBuffer(w.spareStore)
	}
	w.buf, w.spare, w.bufStore, w.spareStore = nil, nil, nil, nil
}

// A checkpoint barrier has drained both pending and in-flight I/O. Return
// oversized burst storage instead of retaining it throughout a read-only phase.
// Ordinary heap buffers remain reusable; no live record or I/O slice is retired.
func (w *wal) releaseBurstBuffers() {
	if w.heapOnly || len(w.buf) != 0 || w.inflight != nil {
		return
	}
	if len(w.bufStore) > walHeapBufferBytes {
		if w.memory != nil {
			w.memory.wal.Add(-int64(len(w.bufStore)))
		}
		releaseWALBuffer(w.bufStore)
		w.buf, w.bufStore = nil, nil
	}
	if len(w.spareStore) > walHeapBufferBytes {
		if w.memory != nil {
			w.memory.wal.Add(-int64(len(w.spareStore)))
		}
		releaseWALBuffer(w.spareStore)
		w.spare, w.spareStore = nil, nil
	}
}

// Only buffers larger than the heap-buffer threshold are mapped. Stores
// always retain their full allocation length, even when the active slice is empty.
func releaseWALBuffer(store []byte) {
	if len(store) > walHeapBufferBytes {
		syscall.Munmap(store)
	}
}
