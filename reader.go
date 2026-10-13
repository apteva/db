package db

import "sync/atomic"

// Reader is a reusable live or snapshot point-read handle. Give each worker
// its own Reader to avoid modifying a shared per-read counter. A Reader is
// safe for concurrent use, including Close, but separate worker handles
// provide better scaling. Close it when finished to restore the fast writer
// path when no handles remain. No lock is held between calls.
type Reader struct {
	c      *Collection
	gate   *readerGate
	lane   *pointReadLane
	closed atomic.Bool
}

// NewReader returns a reusable point-read handle for the default collection.
func (db *DB) NewReader() (*Reader, error) { return db.def.NewReader() }

// NewReader returns a reusable point-read handle for this collection's view.
func (c *Collection) NewReader() (*Reader, error) {
	db := c.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || (c.snap != nil && c.snap.closed) {
		return nil, ErrClosed
	}
	g, lane := db.mu.register()
	return &Reader{c: c, gate: g, lane: lane}, nil
}

// NewReader returns a reusable point-read handle for this snapshot.
func (s *Snapshot) NewReader() (*Reader, error) { return s.def().NewReader() }

// Find appends the value for key to dst[:0], matching Collection.Find.
func (r *Reader) Find(key, dst []byte) ([]byte, bool, error) {
	if r.closed.Load() {
		return dst[:0], false, ErrClosed
	}
	if len(key) > MaxKeySize {
		return dst[:0], false, ErrKeySize
	}
	var kb [64]byte
	key = r.c.ikey(kb[:0], key)
	db := r.c.db
	lane := r.gate.readLock(&db.mu, r.lane)
	defer r.gate.readUnlock(&db.mu, lane)
	// Check the handle first: a closed handle may attempt admission after
	// its last registration was released, while ordinary writers are active.
	if r.closed.Load() || db.closed || (r.c.snap != nil && r.c.snap.closed) {
		return dst[:0], false, ErrClosed
	}
	if err := r.c.check(); err != nil {
		return dst[:0], false, err
	}
	return db.findLocked(r.c.view(), key, dst)
}

// Close releases this handle. Repeated calls are harmless. It waits for
// in-flight lookups before removing the registration used by writers.
func (r *Reader) Close() error {
	db := r.c.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if !r.closed.Load() {
		r.closed.Store(true)
		r.gate.registered.Add(-1)
	}
	return nil
}
