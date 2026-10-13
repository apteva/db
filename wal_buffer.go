package db

import "syscall"

const walHeapBufferBytes = 16 << 20

// Keep the small and moderate buffers on the Go heap. Removing all of that live
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
	size := max(len(w.buf)+extra, 2*cap(w.buf))
	var buf []byte
	if w.heapOnly {
		buf = make([]byte, size)
	} else {
		buf = newWALBuffer(size)
	}
	copy(buf, w.buf)
	old := w.bufStore
	w.buf, w.bufStore = buf[:len(w.buf)], buf
	if !w.heapOnly {
		releaseWALBuffer(old)
	}
}

func (w *wal) releaseBuffers() {
	if w.bufStore != nil && !w.heapOnly {
		releaseWALBuffer(w.bufStore)
	}
	if w.spareStore != nil && !w.heapOnly {
		releaseWALBuffer(w.spareStore)
	}
	w.buf, w.spare, w.bufStore, w.spareStore = nil, nil, nil, nil
}

// Only buffers larger than the heap-buffer threshold are mapped. Stores
// always retain their full allocation length, even when the active slice is empty.
func releaseWALBuffer(store []byte) {
	if len(store) > walHeapBufferBytes {
		syscall.Munmap(store)
	}
}
