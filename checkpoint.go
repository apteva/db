package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The checkpointer runs in the background. A checkpoint has three phases:
//
//  1. apply: the frozen memtable is folded into the tree in small chunks,
//     taking the writer lock only per chunk;
//  2. capture: under the writer lock, but in memory only, dirty pages get
//     their final ids, parents are patched, freelist pages and the meta page
//     are built, and every captured buffer is frozen;
//  3. write: outside the lock, the captured buffers are written and synced,
//     then the meta page; a short locked commit follows.
//
// A writer touching a frozen page copies it first, so the checkpoint never
// blocks writers or readers on disk I/O.

type ckptRequest struct {
	done chan error
}

// requestCheckpoint asks the background checkpointer for a full checkpoint
// without waiting.
func (db *DB) requestCheckpoint() {
	select {
	case db.ckptReq <- ckptRequest{}:
	default:
	}
}

// Checkpoint runs a full checkpoint and waits for it.
func (db *DB) Checkpoint() error {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	done := make(chan error, 1)
	select {
	case db.ckptReq <- ckptRequest{done: done}:
	case <-db.ckptDone:
		return ErrClosed
	}
	select {
	case err := <-done:
		return err
	case <-db.ckptDone:
		return ErrClosed
	}
}

func (db *DB) checkpointer() {
	defer close(db.ckptDone)
	for req := range db.ckptReq {
		waiters := []chan error{}
		if req.done != nil {
			waiters = append(waiters, req.done)
		}
		// coalesce requests that queued up meanwhile
	drain:
		for {
			select {
			case r, ok := <-db.ckptReq:
				if !ok {
					break drain
				}
				if r.done != nil {
					waiters = append(waiters, r.done)
				}
			default:
				break drain
			}
		}
		err := db.runCheckpoint(true)
		for _, w := range waiters {
			w <- err
		}
	}
}

func (db *DB) stopCheckpointer() {
	db.ckptOnce.Do(func() { close(db.ckptReq) })
	<-db.ckptDone
}

// runCheckpoint performs a checkpoint on the calling goroutine.
func (db *DB) runCheckpoint(full bool) error {
	db.ckptMu.Lock()
	defer db.ckptMu.Unlock()
	if full && db.wal.size() >= walRotateBytes {
		if err := db.wal.rotate(); err != nil {
			return err
		}
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		db.mu.Unlock()
		return db.failed
	}
	if full && db.mt != nil && db.imm == nil && db.mt.len() > 0 {
		db.imm, db.mt = db.mt, newMemtable()
	}
	db.mu.Unlock()

	err := db.applyImmutable()
	if err == nil {
		err = db.flushPass(full)
	}
	if err == nil && full {
		db.mu.Lock()
		t := time.Now()
		moved, rerr := db.relocateTailLocked()
		db.noteHold("relocate", t)
		db.mu.Unlock()
		err = rerr
		if err == nil && moved > 0 {
			err = db.flushPass(true)
		}
	}
	if err != nil {
		db.mu.Lock()
		if db.failed == nil {
			db.failed = fmt.Errorf("aptevadb: checkpoint failed, writes disabled: %w", err)
		}
		db.cond.Broadcast()
		db.mu.Unlock()
	}
	return err
}

// applyImmutable folds db.imm into the tree, a chunk at a time, flushing
// dirty pages whenever they crowd the cache.
func (db *DB) applyImmutable() error {
	db.mu.Lock()
	imm := db.imm
	db.mu.Unlock()
	if imm == nil {
		return nil
	}
	entries := imm.snapshot()
	const chunk = 256
	for start := 0; start < len(entries); start += chunk {
		end := min(len(entries), start+chunk)
		db.mu.Lock()
		t := time.Now()
		db.sortedApply = true // the chunk is in key order: split leaves at the insertion point
		si := sortedInserter{db: db}
		for _, e := range entries[start:end] {
			var err error
			if e.del {
				err = db.deleteTree([]byte(e.k))
				si.valid = false
			} else {
				err = si.insert([]byte(e.k), e.v)
			}
			if err != nil {
				db.sortedApply = false
				db.mu.Unlock()
				return err
			}
		}
		db.sortedApply = false
		pressure := db.dirtyPressure()
		db.noteHold("apply-chunk", t)
		db.mu.Unlock()
		if pressure {
			if err := db.flushPass(false); err != nil {
				return err
			}
		}
	}
	db.mu.Lock()
	db.imm = nil
	db.cond.Broadcast()
	db.mu.Unlock()
	return nil
}

// ioPage is one buffer to write at a page id.
type ioPage struct {
	id     uint32
	data   page
	buffer *pageBufferArena
}

type captureSet struct {
	pages     []ioPage
	nodes     []*node
	meta      meta
	metaBuf   page
	toRelease []uint32
	full      bool
}

// flushPass captures the dirty set, writes it, and commits.
func (db *DB) flushPass(full bool) error {
	db.mu.Lock()
	t := time.Now()
	cs, err := db.captureLocked(full)
	db.noteHold("capture", t)
	db.mu.Unlock()
	if err != nil {
		db.mu.Lock()
		db.releaseCaptureBuffers(cs)
		db.mu.Unlock()
		return err
	}
	if err := db.writeCapture(cs); err != nil {
		db.mu.Lock()
		db.releaseCaptureBuffers(cs)
		db.mu.Unlock()
		return err
	}
	db.mu.Lock()
	t = time.Now()
	truncateTo, dropGen, err := db.commitCaptureLocked(cs)
	db.noteHold("commit", t)
	db.mu.Unlock()
	if err != nil {
		return err
	}
	// file-system housekeeping outside the lock: page ids past the new end
	// are free and nobody writes pages but this goroutine
	if truncateTo > 0 {
		if err := db.pager.f.Truncate(truncateTo); err != nil {
			return err
		}
	}
	if dropGen > 0 {
		db.wal.dropBefore(dropGen)
	}
	return nil
}

// captureLocked prepares everything a checkpoint will write, in memory.
func (db *DB) captureLocked(full bool) (*captureSet, error) {
	cs := &captureSet{full: full}
	walGen, walOff := db.wal.position()
	switch {
	case db.metaLogPos != nil:
		// a compaction copy: its records come from the parent's log
		walGen, walOff = db.metaLogPos.gen, db.metaLogPos.off
	case !full:
		// entries still buffered must replay: point at the oldest log on disk
		if gens, _ := walGens(db.dir); len(gens) > 0 {
			walGen, walOff = gens[0], walHeaderSize
		}
	case db.mt != nil && db.mt.len() > 0:
		// the active memtable's records are not in the tree: keep them
		walGen, walOff = db.activeStart.gen, db.activeStart.off
	}
	// overflow pages created since the last checkpoint keep their ids
	for _, n := range db.dirtyOverflow {
		if db.pager.pages[n.id].Load() != n || !n.dirty {
			continue
		}
		db.captureNode(cs, n)
	}
	db.dirtyOverflow = db.dirtyOverflow[:0]
	root, err := db.pager.get(db.root)
	if err != nil {
		return cs, err
	}
	newRoot, err := db.captureSubtree(cs, root)
	if err != nil {
		return cs, err
	}
	db.root = newRoot
	// free list: ids reusable once this meta is durable
	freeHead, err := db.captureFreelist(cs)
	if err != nil {
		return cs, err
	}
	cs.toRelease = append(cs.toRelease, db.pager.pending...)
	db.pager.pending = db.pager.pending[:0]
	cs.meta = meta{db.ckpt + 1, db.root, db.pager.hwm, walGen, walOff, freeHead}
	cs.metaBuf = make(page, PageSize)
	db.encodeMeta(cs.metaBuf, cs.meta)
	return cs, nil
}

// captureNode assigns n its on-disk id (copy-on-write when it already has
// one), checksums and freezes its buffer, and queues it for writing.
func (db *DB) captureNode(cs *captureSet, n *node) uint32 {
	pg := db.pager
	if n.onDisk {
		if len(db.snaps) > 0 {
			db.preserveForSnapshots(n)
		}
		pg.pages[n.id].Store(nil)
		cs.toRelease = append(cs.toRelease, n.id)
		n.id = pg.allocID()
		pg.pages[n.id].Store(n)
	}
	// leaves are checksummed on a scratch copy in the I/O phase (the bulk of
	// the work); a mapped page's on-disk checksum is already right
	if n.pageType() != pageLeaf && !n.mapped {
		n.data.setCrc(pageCrc(n.data))
	}
	if n.dirty {
		pg.dirtyN--
	}
	n.dirty, n.onDisk, n.frozen, n.writing = false, true, true, true
	if n.buffer != nil {
		n.buffer.live++
	}
	cs.pages = append(cs.pages, ioPage{id: n.id, data: n.data, buffer: n.buffer})
	cs.nodes = append(cs.nodes, n)
	return n.id
}

// captureSubtree walks resident internal nodes, capturing dirty pages
// children first so parents point at final ids.
func (db *DB) captureSubtree(cs *captureSet, n *node) (uint32, error) {
	if n.pageType() == pageInternal {
		nc := n.data.numCells()
		for i := 0; i <= nc; i++ {
			cid := n.data.child(i)
			cn := db.pager.loaded(cid)
			if cn == nil {
				continue
			}
			var newID uint32
			switch {
			case cn.pageType() == pageInternal:
				id, err := db.captureSubtree(cs, cn)
				if err != nil {
					return 0, err
				}
				newID = id
			case cn.dirty:
				newID = db.captureNode(cs, cn)
			default:
				continue
			}
			if newID != cid {
				db.writable(n).setChild(i, newID)
			}
		}
	}
	if n.dirty {
		return db.captureNode(cs, n), nil
	}
	return n.id, nil
}

// captureFreelist builds the persisted free list for this checkpoint.
func (db *DB) captureFreelist(cs *captureSet) (uint32, error) {
	pg := db.pager
	for _, id := range db.freelistPages {
		if n := pg.loaded(id); n != nil {
			db.freePage(n)
		} else if len(db.snaps) > 0 {
			db.quarantineIDs([]uint32{id})
		} else {
			pg.pending = append(pg.pending, id)
		}
	}
	db.freelistPages = db.freelistPages[:0]
	count := len(pg.free) + len(pg.pending) + len(cs.toRelease)
	need := (count + idsPerFreelistPage - 1) / idsPerFreelistPage
	if need == 0 {
		return 0, nil
	}
	pages := make([]*node, need)
	for i := range pages {
		pages[i] = pg.alloc(pageFreelist, 0)
	}
	ids := make([]uint32, 0, count)
	ids = append(ids, pg.free...)
	ids = append(ids, pg.pending...)
	ids = append(ids, cs.toRelease...)
	for i, n := range pages {
		lo := i * idsPerFreelistPage
		hi := min(len(ids), lo+idsPerFreelistPage)
		for j, id := range ids[lo:hi] {
			le.PutUint32(n.data[hdrSize+4*j:], id)
		}
		n.data.setChainLen(hi - lo)
		if i+1 < len(pages) {
			n.data.setChainNext(pages[i+1].id)
		}
		db.captureNode(cs, n)
		db.freelistPages = append(db.freelistPages, n.id)
	}
	return pages[0].id, nil
}

// writeCapture does the I/O of a checkpoint without any lock held.
func (db *DB) writeCapture(cs *captureSet) error {
	f := db.pager.f
	scratch := make(page, PageSize)
	for _, p := range cs.pages {
		buf := p.data
		if buf.typ() == pageLeaf {
			// never write into a frozen buffer: a writer may be copying it
			copy(scratch, buf)
			scratch.setCrc(pageCrc(scratch))
			buf = scratch
		}
		if err := writeAt(f, buf, int64(p.id)*PageSize); err != nil {
			return err
		}
	}
	if err := db.wal.syncFn(f); err != nil {
		return err
	}
	if err := writeAt(f, cs.metaBuf, int64(cs.meta.checkpoint%2)*PageSize); err != nil {
		return err
	}
	return db.wal.syncFn(f)
}

// commitCaptureLocked publishes a written checkpoint: unfreezes pages,
// hands untouched ones back to the file mapping, releases freed ids. It
// returns the size to truncate the file to (0: none) and the log generation
// below which files may be deleted (0: none); both happen after unlocking.
func (db *DB) commitCaptureLocked(cs *captureSet) (truncateTo int64, dropGen uint64, err error) {
	db.treeGen++ // page ids change here
	pg := db.pager
	db.ckpt = cs.meta.checkpoint
	db.pager.writes += uint64(len(cs.pages))
	for _, n := range cs.nodes {
		n.writing = false
		pg.markVerified(n.id)
		if n.frozen && !n.dirty && pg.pages[n.id].Load() == n {
			pg.releasePageBuffer(n)
			n.data, n.mapped = pg.mappedPage(n.id), true
		}
		n.frozen = false
	}
	// only ids freed by this capture: anything freed while the I/O ran is
	// still referenced by the meta just written and waits for the next one
	if len(db.snaps) > 0 {
		db.quarantineIDs(cs.toRelease)
	} else if trailing := pg.releaseIDs(cs.toRelease); trailing > 0 {
		truncateTo = int64(pg.hwm) * PageSize
	}
	if cs.full && db.metaLogPos == nil {
		dropGen = cs.meta.walGen
	}
	// The capture's I/O has finished. Readers are excluded by the writer
	// lock; snapshots are the only remaining possible aliases of old
	// private buffers or superseded file mappings.
	db.releaseCaptureBuffers(cs)
	pg.releaseUnusedBuffers(len(db.snaps) == 0)
	db.cond.Broadcast()
	return truncateTo, dropGen, nil
}

// Capture aliases are released on success and on any completed I/O/error
// path. Current nodes and snapshots retain their own independent references.
// The writer lock is held; file mappings wait for successful publication.
func (db *DB) releaseCaptureBuffers(cs *captureSet) {
	if cs == nil {
		return
	}
	for _, p := range cs.pages {
		if p.buffer != nil {
			p.buffer.live--
		}
	}
	cs.pages, cs.nodes = nil, nil
	db.pager.releaseUnusedBuffers(false)
}

// ---- lifecycle

// stopFlusher ends the background log flusher exactly once.
func (db *DB) stopFlusher() {
	db.stopOnce.Do(func() { close(db.stop) })
	<-db.done
}

// Close checkpoints and releases the files.
func (db *DB) Close() error {
	db.stopFlusher()
	db.stopCheckpointer()
	err := db.runCheckpoint(true)
	if err == nil {
		err = db.trimLog()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	db.cond.Broadcast()
	if e := db.wal.close(); err == nil {
		err = e
	}
	if e := db.pager.close(); err == nil {
		err = e
	}
	return err
}

// trimLog drops a log generation that a full checkpoint has made entirely
// dead: the log rotates and a second, empty checkpoint points the meta page
// past the old file, which is then deleted. Only worth it past logTrimBytes;
// a generation is otherwise dropped when the next one fills.
func (db *DB) trimLog() error {
	db.mu.RLock()
	w := db.wal
	dead := db.mt == nil || (db.mt.len() == 0 && db.imm == nil)
	db.mu.RUnlock()
	if !dead || w.size() < logTrimBytes {
		return nil
	}
	if err := w.rotate(); err != nil {
		return err
	}
	return db.runCheckpoint(true)
}

const logTrimBytes = 4 << 20

// crash drops the handles without checkpointing, for recovery tests.
func (db *DB) crash() {
	db.stopFlusher()
	db.stopCheckpointer()
	db.mu.Lock()
	db.closed = true
	db.cond.Broadcast()
	db.wal.close()
	db.pager.close()
	db.mu.Unlock()
}

// ---- compaction and backup, from a snapshot

// Compact rewrites the database into a fresh, densely packed file and
// swaps it in. The copy is made from a snapshot while writers continue.
// Writes made during the copy are replayed from the log into the new file
// in rounds without the lock; the lock is held only to apply the last few
// records in memory and swap the files. The new file's meta points back
// into the existing log, so no flush happens under the lock.
//
// ctx cancels it between its rounds; the copy is discarded and the
// database untouched.
func (db *DB) Compact(ctx context.Context) error {
	ctx = orBackground(ctx)
	if err := db.compact(ctx); err != nil {
		return err
	}
	return db.trimLog()
}

func (db *DB) compact(ctx context.Context) error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		db.mu.Unlock()
		return db.failed
	}
	if db.bulkLoads > 0 {
		db.mu.Unlock()
		return ErrIndexBuilding // its inserts are not in the log the copy would replay
	}
	if len(db.snaps) > 0 {
		db.mu.Unlock()
		return ErrSnapshotsOpen
	}
	db.mu.Unlock()
	s, err := db.Snapshot()
	if err != nil {
		return err
	}
	tmp := filepath.Join(db.dir, "compact.tmp")
	os.RemoveAll(tmp)
	ndb, err := db.copySnapshot(ctx, s, tmp, db.opts)
	s.Close()
	if err != nil {
		os.RemoveAll(tmp)
		return err
	}
	fail := func(err error) error {
		ndb.closeQuiet()
		os.RemoveAll(tmp)
		return err
	}
	// catch up without the lock: replay the log delta into the copy in
	// rounds until it is small, checkpoint the copy once, then shrink the
	// delta that accumulated during that checkpoint
	pos := s.pos
	catchUp := func() (int, error) {
		g, off, err := db.wal.flushAndPosition()
		if err != nil {
			return 0, err
		}
		n := 0
		if _, _, err := db.wal.replayFrom(pos.gen, pos.off, func(k, v []byte, del bool) error {
			n++
			return ndb.putDirect(k, v, del)
		}); err != nil {
			return 0, err
		}
		pos = lsn{g, off}
		return n, nil
	}
	for round := 0; round < 20; round++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, err := catchUp()
		if err != nil {
			return fail(err)
		}
		if n < 500 {
			break
		}
	}
	ndb.metaLogPos = &pos
	if err := ndb.runCheckpoint(true); err != nil {
		return fail(err)
	}
	// from here no checkpoint may be in flight against the old file
	db.ckptMu.Lock()
	defer db.ckptMu.Unlock()
	for round := 0; round < 20; round++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, err := catchUp()
		if err != nil {
			return fail(err)
		}
		if n < 500 {
			break
		}
	}
	// final tail under the lock, in memory only, then swap
	db.mu.Lock()
	defer db.mu.Unlock()
	t := time.Now()
	defer db.noteHold("compact-swap", t)
	db.treeGen++
	if len(db.snaps) > 0 {
		return fail(ErrSnapshotsOpen)
	}
	t1 := time.Now()
	if _, _, err := db.wal.flushAndPosition(); err != nil {
		return fail(err)
	}
	db.noteHold("swap-flush", t1)
	t1 = time.Now()
	if _, _, err := db.wal.replayFrom(pos.gen, pos.off, ndb.putDirect); err != nil {
		return fail(err)
	}
	db.noteHold("swap-replay", t1)
	t1 = time.Now()
	ndb.wal.close()
	db.pager.close()
	db.noteHold("swap-close", t1)
	t1 = time.Now()
	// two renames, no unlink: deleting the old file frees its extents and
	// takes tens of milliseconds, so it happens after the lock is released
	old := filepath.Join(db.dir, "data.old")
	os.Remove(old)
	if err := os.Rename(filepath.Join(db.dir, "data"), old); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(tmp, "data"), filepath.Join(db.dir, "data")); err != nil {
		return err
	}
	db.noteHold("swap-rename", t1)
	// the new tree lives on; the existing log stays the log
	db.pager, db.root, db.ckpt = ndb.pager, ndb.root, ndb.ckpt
	db.freelistPages, db.dirtyOverflow = ndb.freelistPages, ndb.dirtyOverflow
	if db.mt != nil {
		db.mt = newMemtable() // every record is in the new tree now
	}
	db.imm = nil
	g, off := db.wal.position()
	db.activeStart = lsn{g, off}
	db.cond.Broadcast()
	db.noteHold("compact-swap-hold", t)
	pg, warmLimit := db.pager, db.pager.hwm
	db.mu.Unlock()
	os.Remove(old)
	os.RemoveAll(tmp)
	if d, err := os.Open(db.dir); err == nil {
		d.Sync()
		d.Close()
	}
	var warmErr error
	if pg.limit == 0 {
		warmErr = db.warmCompactedPages(pg, warmLimit)
	}
	db.mu.Lock() // matched by the deferred unlock
	return warmErr
}

// warmCompactedPages restores the pre-existing warm read layout after the old
// mapping has been closed, so both trees never need to be resident together.
// Small read-locked batches let concurrent writers proceed between faults.
// ckptMu excludes another compaction/checkpoint/close while this runs.
func (db *DB) warmCompactedPages(pg *pager, limit uint32) error {
	for start := 0; ; start += 32 {
		db.mu.RLock()
		end := min(start+32, int(limit), int(pg.hwm))
		for i := start; i < end; i++ {
			n := pg.pages[i].Load()
			if n != nil && n.mapped && n.kind != 0 && n.data.typ() != n.kind {
				db.mu.RUnlock()
				return fmt.Errorf("%w: compacted page %d type", ErrCorrupt, n.id)
			}
		}
		done := end >= min(int(limit), int(pg.hwm))
		db.mu.RUnlock()
		if done {
			return nil
		}
	}
}

// copySnapshot writes the contents of s into a new database at dir and
// returns it open, without its background goroutines.
func (db *DB) copySnapshot(ctx context.Context, s *Snapshot, dir string, opts Options) (*DB, error) {
	opts.MemtableBytes = -1
	opts.Sync = false
	ndb, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	ndb.stopFlusher()
	ndb.stopCheckpointer()
	// The initial empty root is durable. Start the private builder on a
	// fresh id so completed leaves can be written once without relocating
	// their ids or patching already-built parents.
	stream := opts.CacheBytes <= 0
	ndb.sortedLoad, ndb.streamCopy = stream, stream
	si := sortedInserter{db: ndb}
	writer := copyPageWriter{pg: ndb.pager}
	n := 0
	var cerr error
	err = db.scan(view{db, s}, nil, nil, func(k, v []byte) bool {
		if n++; n&4095 == 0 {
			if cerr = ctx.Err(); cerr != nil {
				return false
			}
		}
		if !stream {
			cerr = ndb.insertTree(k, v)
			return cerr == nil
		}
		if n == 1 {
			oldRoot, rerr := ndb.pager.get(ndb.root)
			if rerr != nil {
				cerr = rerr
				return false
			}
			ndb.root = ndb.pager.alloc(pageLeaf, 0).id
			ndb.freePage(oldRoot)
		}
		previous := si.leaf
		if cerr = si.insert(k, v); cerr != nil {
			return false
		}
		if previous != nil && previous != si.leaf {
			if cerr = writer.add(previous); cerr != nil {
				return false
			}
		}
		// Overflow chains are immutable and already written by streamCopy.
		// Drop the tracking entries after each row, keeping the builder's
		// working set bounded even for very large values.
		clear(ndb.dirtyOverflow)
		ndb.dirtyOverflow = ndb.dirtyOverflow[:0]
		return true
	})
	ndb.sortedLoad, ndb.streamCopy = false, false
	if err == nil {
		err = cerr
	}
	if err == nil {
		err = writer.flush()
	}
	if err == nil {
		err = ndb.runCheckpoint(true)
	}
	if err != nil {
		ndb.closeQuiet()
		return nil, err
	}
	return ndb, nil
}

// putDirect applies a replayed record straight to the tree.
func (db *DB) putDirect(key, val []byte, del bool) error {
	if del {
		return db.deleteTree(key)
	}
	return db.insertTree(key, val)
}

func (db *DB) closeQuiet() {
	db.mu.Lock()
	db.closed = true
	db.wal.close()
	db.pager.close()
	db.mu.Unlock()
}

// Backup writes a compact copy of the database into dir, which must not
// exist or must be empty. It works from a snapshot, so writers continue.
//
// ctx cancels the copy, which is then removed.
func (db *DB) Backup(ctx context.Context, dir string) error {
	ctx = orBackground(ctx)
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return fmt.Errorf("aptevadb: backup directory %s is not empty", dir)
	}
	s, err := db.Snapshot()
	if err != nil {
		return err
	}
	defer s.Close()
	ndb, err := db.copySnapshot(ctx, s, dir, Options{})
	if err != nil {
		os.RemoveAll(dir)
		return err
	}
	ndb.closeQuiet()
	return nil
}
