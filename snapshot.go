package db

import (
	"context"
	"errors"
	"sort"
)

// Snapshot is a consistent read view: the tree and memtable as they were
// when it was taken. Pages modified afterwards are copied first and their
// old version kept in the snapshot's overlay, so readers never see a
// later write. While a snapshot is open, page ids it may reference are
// quarantined instead of reused.
type Snapshot struct {
	db          *DB
	revision    uint64
	epoch       uint64
	root        uint32
	mem         []mtEntry          // memtables as of the snapshot, merged and sorted
	overlay     map[uint32]page    // page id -> its contents at the snapshot
	buffers     []*pageBufferArena // retained private-page storage for overlay entries
	pos         lsn                // log position at the snapshot
	collections map[string]*Collection
	indexes     []indexDef
	closed      bool
}

var ErrSnapshotsOpen = errors.New("aptevadb: operation not allowed while snapshots are open")

// quarantined ids were freed at a given epoch; they may be reused once no
// snapshot from that epoch or earlier is open.
type quarantine struct {
	epoch uint64
	ids   []uint32
}

// Snapshot takes a consistent view. Close it when done.
func (db *DB) Snapshot() (*Snapshot, error) { return db.snapshot(true) }

func (db *DB) readSnapshot() (*Snapshot, error) { return db.snapshot(false) }

func (db *DB) snapshot(logPosition bool) (*Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.failed != nil {
		return nil, db.failed
	}
	db.epoch++
	s := &Snapshot{db: db, epoch: db.epoch, root: db.root, overlay: map[uint32]page{}}
	s.revision = db.revision
	s.mem = db.memSnapshotLocked()
	var g, off uint64
	var err error
	if logPosition {
		g, off, err = db.wal.flushAndPosition()
	}
	if err != nil {
		return nil, err
	}
	s.pos = lsn{g, off}
	s.collections = make(map[string]*Collection)
	db.colls.mu.RLock()
	for name, c := range db.colls.byName {
		s.collections[name] = c
	}
	s.collections[""] = db.def
	db.colls.mu.RUnlock()
	for _, d := range db.indexDefs() {
		if db.idx.touched(d.ID) == nil {
			s.indexes = append(s.indexes, d)
		}
	}

	if db.snaps == nil {
		db.snaps = map[uint64]*Snapshot{}
	}
	db.snaps[s.epoch] = s
	return s, nil
}

// Close releases the snapshot and any page ids it kept from reuse.
func (s *Snapshot) Close() {
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	delete(db.snaps, s.epoch)
	for _, a := range s.buffers {
		a.live--
	}
	s.buffers, s.overlay = nil, nil
	// Captures retain their own references, so unused private arenas can
	// be released even if checkpoint I/O is in progress. File mappings
	// wait until that I/O has committed.
	db.pager.releaseUnusedBuffers(false)
	db.releaseQuarantineLocked()
}

// minSnapEpoch is the oldest open snapshot's epoch, or 0 when none.
func (db *DB) minSnapEpoch() uint64 {
	var m uint64
	for e := range db.snaps {
		if m == 0 || e < m {
			m = e
		}
	}
	return m
}

// preserveForSnapshots records n's current contents for every open
// snapshot that may still read it, before it is modified or its id moves.
func (db *DB) preserveForSnapshots(n *node) {
	for _, s := range db.snaps {
		if s.epoch > n.modEpoch {
			if _, ok := s.overlay[n.id]; !ok {
				s.overlay[n.id] = n.data
				if n.buffer != nil {
					n.buffer.live++
					s.buffers = append(s.buffers, n.buffer)
				}
			}
		}
	}
}

// writable returns a private buffer for modifying n. If a snapshot taken
// after n's last modification may still read the current buffer, that
// buffer is left to the snapshot and a copy is modified instead.
func (db *DB) writable(n *node) page {
	preserve := len(db.snaps) > 0 && db.epoch > n.modEpoch
	if preserve {
		db.preserveForSnapshots(n)
	}
	if preserve || n.frozen || n.mapped {
		db.pager.copyPageBuffer(n)
		n.mapped, n.frozen = false, false
	}
	db.pager.markDirty(n)
	n.modEpoch = db.epoch
	return n.data
}

// freePage releases a page that left the tree, keeping it for snapshots.
func (db *DB) freePage(n *node) {
	if len(db.snaps) > 0 {
		db.preserveForSnapshots(n)
		db.pager.releasePageBuffer(n)
		if db.pager.pages[n.id].Load() == n {
			db.pager.pages[n.id].Store(nil)
			db.pager.resident.Add(-1)
		}
		if n.dirty {
			db.pager.dirtyN--
			n.dirty = false
		}
		db.quarantineIDs([]uint32{n.id})
		return
	}
	db.pager.freePage(n)
}

func (db *DB) quarantineIDs(ids []uint32) {
	if len(ids) == 0 {
		return
	}
	db.quar = append(db.quar, quarantine{epoch: db.epoch, ids: append([]uint32(nil), ids...)})
}

// releaseQuarantineLocked returns ids no open snapshot can reference.
func (db *DB) releaseQuarantineLocked() {
	min := db.minSnapEpoch()
	kept := db.quar[:0]
	var freed []uint32
	for _, q := range db.quar {
		if min == 0 || min > q.epoch {
			freed = append(freed, q.ids...)
		} else {
			kept = append(kept, q)
		}
	}
	db.quar = kept
	if len(freed) > 0 {
		pg := db.pager
		pg.free = append(pg.free, freed...)
		sort.Slice(pg.free, func(i, j int) bool { return pg.free[i] > pg.free[j] })
	}
}

// ---- read views

// view is what the read paths operate on: a root and a way to fetch pages
// and memtable entries, either live or as of a snapshot.
type view struct {
	db   *DB
	snap *Snapshot
}

func (v view) root() uint32 {
	if v.snap != nil {
		return v.snap.root
	}
	return v.db.root
}

func (v view) page(id uint32) (page, error) {
	if v.db.failed != nil {
		return nil, v.db.failed
	}
	if v.snap != nil {
		if v.snap.closed {
			return nil, ErrClosed
		}
		if p, ok := v.snap.overlay[id]; ok {
			return p, nil
		}
	}
	n, err := v.db.pager.get(id)
	if err != nil {
		return nil, err
	}
	return n.data, nil
}

// memGet looks a key up in the memtable view: active first, then the one
// being applied.
func (v view) memGet(key []byte) (val []byte, del, ok bool) {
	if v.snap != nil {
		m := v.snap.mem
		i := sort.Search(len(m), func(i int) bool { return m[i].k >= string(key) })
		if i < len(m) && m[i].k == string(key) {
			return m[i].v, m[i].del, true
		}
		return nil, false, false
	}
	if v.db.mt == nil {
		return nil, false, false
	}
	if val, del, ok = v.db.mt.get(key); ok {
		return val, del, true
	}
	if v.db.imm != nil {
		return v.db.imm.get(key)
	}
	return nil, false, false
}

// memSnapshot returns the sorted memtable entries of the view.
func (v view) memSnapshot() []mtEntry {
	if v.snap != nil {
		return v.snap.mem
	}
	return v.db.memSnapshotLocked()
}

// memRange returns copies of the view's buffered entries in [lo, hi), in
// key order: a slice of the snapshot's frozen entries, or the active and
// applying memtables' ranges merged (active winning). Reader lock held.
func (v view) memRange(lo, hi []byte) []mtEntry {
	if v.snap != nil {
		m := v.snap.mem
		if lo != nil {
			i := sort.Search(len(m), func(i int) bool { return m[i].k >= string(lo) })
			m = m[i:]
		}
		if hi != nil {
			i := sort.Search(len(m), func(i int) bool { return m[i].k >= string(hi) })
			m = m[:i]
		}
		return m
	}
	db := v.db
	if db.mt == nil {
		return nil
	}
	a := db.mt.rangeInto(nil, lo, hi)
	if db.imm == nil || db.imm.len() == 0 {
		return a
	}
	b := db.imm.rangeInto(nil, lo, hi)
	if len(a) == 0 {
		return b
	}
	return mergeEntries(a, b)
}

// mergeEntries merges two sorted entry slices, a winning on equal keys.
func mergeEntries(a, b []mtEntry) []mtEntry {
	out := make([]mtEntry, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].k < b[j].k:
			out = append(out, a[i])
			i++
		case a[i].k > b[j].k:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// memSnapshotLocked merges the active and applying memtables, active winning.
func (db *DB) memSnapshotLocked() []mtEntry {
	if db.mt == nil {
		return nil
	}
	a := db.mt.snapshot()
	if db.imm == nil {
		return a
	}
	b := db.imm.snapshot()
	out := make([]mtEntry, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i].k < b[j].k:
			out = append(out, a[i])
			i++
		case a[i].k > b[j].k:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// leafValue reads the full value of a leaf cell through the view.
func (v view) leafValue(p page, i int, dst []byte) ([]byte, error) {
	raw := p.leafVal(i)
	if !p.leafIsOverflow(i) {
		return append(dst[:0], raw...), nil
	}
	total := int(le.Uint32(raw))
	id := le.Uint32(raw[4:])
	dst = dst[:0]
	for id != 0 {
		op, err := v.page(id)
		if err != nil {
			return nil, err
		}
		if op.typ() != pageOverflow {
			return nil, ErrCorrupt
		}
		dst = append(dst, op[hdrSize:hdrSize+op.chainLen()]...)
		id = op.chainNext()
		if len(dst) > total {
			return nil, ErrCorrupt
		}
	}
	if len(dst) != total {
		return nil, ErrCorrupt
	}
	return dst, nil
}

// Collection returns the named collection as it stands in the snapshot;
// "" is the default collection. Later catalog changes are invisible.
func (s *Snapshot) Collection(name string) (*Collection, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.closed || s.db.closed {
		return nil, ErrClosed
	}
	c, ok := s.collections[name]
	if !ok {
		return nil, ErrNoSuchCollection
	}
	return c.bound(s), nil
}

func (s *Snapshot) def() *Collection { return s.db.def.bound(s) }

// Find reads the default collection through the snapshot.
func (s *Snapshot) Find(key, dst []byte) ([]byte, bool, error) { return s.def().Find(key, dst) }

// Scan iterates the default collection of the snapshot in key order.
func (s *Snapshot) Scan(start, end []byte, fn func(key, val []byte) bool) error {
	return s.def().Scan(start, end, fn)
}

// Source adapts a key range of the snapshot's default collection to the
// aggregation executor.
func (s *Snapshot) Source(start, end []byte) Source { return s.def().Source(start, end) }

// Aggregate runs a pipeline over the snapshot's default collection,
// through an index when the leading $match allows it.
func (s *Snapshot) Aggregate(ctx context.Context, pipeline []byte) ([]map[string]any, error) {
	return s.def().Aggregate(ctx, pipeline)
}

// AggregateParallel runs a pipeline over the snapshot's default collection
// in key-range shards.
func (s *Snapshot) AggregateParallel(ctx context.Context, pipeline []byte, shards int) ([]map[string]any, error) {
	return s.def().AggregateParallel(ctx, pipeline, shards)
}

// NewCursor iterates the snapshot's default collection.
func (s *Snapshot) NewCursor() *Cursor { return s.def().NewCursor() }
