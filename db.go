package db

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// SyncMode selects the flush primitive used when Options.Sync is on.
type SyncMode uint8

const (
	// SyncFull survives power loss: F_FULLFSYNC on macOS, fsync elsewhere.
	SyncFull SyncMode = iota
	// SyncBarrier is F_BARRIERFSYNC on macOS: ordered and handed to the drive,
	// but the drive cache is not forced to stable media. fsync elsewhere.
	SyncBarrier
	// SyncOS is plain fsync(2). On macOS this survives an OS crash, not a
	// power loss; it is SQLite's default behaviour there.
	SyncOS
)

// Options tunes durability and checkpointing.
type Options struct {
	// Sync flushes the log before a write returns. Default false (async).
	// Use DurableOptions for power-loss-safe writes. Concurrent writes share flushes.
	Sync bool
	// SyncMode picks the flush primitive; default SyncFull.
	SyncMode SyncMode
	// CheckpointLogBytes is the log size that triggers a checkpoint. Default 256MB.
	CheckpointLogBytes uint64
	// LogFlushInterval is how often buffered log records are handed to the OS
	// when Sync is off. Default 10ms.
	LogFlushInterval time.Duration
	// CacheBytes targets the logical page cache, not total process memory; clean pages are evicted past it and a
	// checkpoint runs when dirty pages reach three quarters of it. 0 keeps
	// everything resident, which is fastest when the data fits in RAM.
	CacheBytes int64
	// VerifyChecksums checks every page checksum when a page is first read
	// from the file. Default true; set NoVerifyChecksums to turn it off.
	NoVerifyChecksums bool
	// Parallelism is how many goroutines Aggregate uses for a pipeline that
	// can run in shards. 0 means runtime.GOMAXPROCS(0) at Open; 1 runs on
	// the calling goroutine. AggregateParallel overrides it per call.
	Parallelism int
	// MemtableBytes buffers inserts in memory and applies them to the tree
	// in key order once the budget is reached, cutting write amplification
	// when the file is larger than the cache. Default: CacheBytes/2 with a
	// cache limit; 0 (inserts go straight to the tree) without one. Set to
	// -1 to disable with a cache limit.
	MemtableBytes int64
}

var (
	ErrKeySize       = errors.New("aptevadb: key must be 1..1024 bytes")
	ErrCellSize      = errors.New("aptevadb: key+value too large")
	ErrClosed        = errors.New("aptevadb: closed")
	ErrIndexBuilding = errors.New("aptevadb: an index is being built")
	ErrOptions       = errors.New("aptevadb: invalid options")
	ErrFormat        = errors.New("aptevadb: unsupported file format version")
)

// Version is the library version.
const Version = "0.2.0-alpha.2"

// FormatVersion is the on-disk format this build writes. Open also reads
// formats 3 through 6 and upgrades them before returning; see FORMAT.md for the policy.
const FormatVersion = 7

// DB is a single-writer, multi-reader key-value store.
type DB struct {
	mu          treeMutex // tree structure; writers exclude every read lane
	dirLock     *os.File  // remains locked across data-file swaps
	dir         string
	pager       *pager
	wal         *wal
	mt          *memtable // active memtable; nil unless MemtableBytes > 0
	imm         *memtable // memtable being applied by the checkpointer, or nil
	activeStart lsn       // log position of the active memtable's first record
	metaLogPos  *lsn      // compaction copies: where in the parent's log their meta points
	root        uint32
	ckpt        uint64
	opts        Options

	scratch       page
	scratch2      []byte // two pages, for sibling balancing
	cellSizes     []int  // reusable cell lengths for sibling balancing; writer lock held
	sepBufs       [16][]byte
	ptrBuf        []byte
	balances      uint64
	merges        uint64
	relocations   uint64
	dirtyOverflow []*node  // overflow pages created since the last checkpoint
	freelistPages []uint32 // pages holding the persisted free list
	idx           indexCatalog
	colls         collCatalog
	def           *Collection // the default collection, id 0
	revision      uint64      // logical writes; writer lock held
	readFormat    uint32      // selected persisted format; zero for private copies
	treeGen       uint64      // bumped whenever a page or page id of the tree changes
	noReorder     bool        // tests: run joins as written
	sortedLoad    bool        // a bulk load is inserting ascending keys: split leaves at the end
	sortedApply   bool        // a memtable is being applied in key order: split leaves at the insertion point
	streamCopy    bool        // private compaction/backup builder: persist completed overflow pages
	bulkLoads     int         // index builds in progress (their inserts are not logged)
	epoch         uint64      // bumped per snapshot; nodes record the epoch of their last write
	snaps         map[uint64]*Snapshot
	quar          []quarantine
	debugVerify   func(where string, pages ...page)

	stop          chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	closed        bool
	ckptMu        sync.Mutex               // one checkpoint or compaction at a time
	holds         map[string]time.Duration // longest writer-lock hold per section (diagnostics)
	stalls        uint64                   // writer waits for the memtable to drain
	ckptReq       chan ckptRequest         // to the background checkpointer
	ckptDone      chan struct{}            // closed when the checkpointer exits
	ckptOnce      sync.Once
	cond          *sync.Cond // signalled when imm drains or a checkpoint commits
	failed        error      // set after an I/O error left in-memory state unsure; writes refuse
	computedCount int        // configured collections; writer lock held
	legacyFormat  bool       // recovery selected format 3; upgraded before Open returns
	upgradeFormat bool       // either valid meta slot carries an older format
	legacyViews   bool       // selected meta predates computed collections
	derivedCount  int        // writer lock held
}

const metaMagic = "NDBMETA1"

type meta struct {
	checkpoint uint64
	root, hwm  uint32
	walGen     uint64
	walOffset  uint64
	freeHead   uint32
}

// Open opens or creates the database in dir.
func Open(dir string, opts Options) (*DB, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: directory is required", ErrOptions)
	}
	if opts.SyncMode > SyncOS || opts.CacheBytes < 0 || opts.MemtableBytes < -1 || opts.LogFlushInterval < 0 || opts.Parallelism < 0 {
		return nil, ErrOptions
	}
	absoluteDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir = absoluteDir
	if _, err := os.Lstat(filepath.Join(dir, restoreMarker)); err == nil {
		return nil, ErrRestoreIncomplete
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	if opts.CheckpointLogBytes == 0 {
		opts.CheckpointLogBytes = 256 << 20
	}
	if opts.LogFlushInterval == 0 {
		opts.LogFlushInterval = 10 * time.Millisecond
	}
	if opts.Parallelism <= 0 {
		opts.Parallelism = runtime.GOMAXPROCS(0)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	directoryLock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	lockOwned := true
	defer func() {
		if lockOwned {
			directoryLock.Close()
		}
	}()

	dataPath, oldPath := filepath.Join(dir, "data"), filepath.Join(dir, "data.old")
	dataInfo, dataErr := os.Stat(dataPath)
	if dataErr != nil && !os.IsNotExist(dataErr) {
		return nil, dataErr
	}
	if os.IsNotExist(dataErr) {
		if _, oldErr := os.Stat(oldPath); oldErr == nil {
			version, err := formatVersionAt(oldPath)
			if err != nil {
				return nil, err
			}
			if version < 3 || version > FormatVersion {
				return nil, ErrFormat
			}
			if err = os.Rename(oldPath, dataPath); err != nil {
				return nil, err
			}
			if err = syncRestoreDirectory(dir); err != nil {
				return nil, err
			}
			dataInfo, dataErr = os.Stat(dataPath)
			if dataErr != nil {
				return nil, dataErr
			}
		} else if !os.IsNotExist(oldErr) {
			return nil, oldErr
		}
	}
	if dataInfo == nil || dataInfo.Size() == 0 {
		gens, err := walGens(dir)
		if err != nil {
			return nil, err
		}
		if len(gens) > 0 {
			return nil, fmt.Errorf("%w: data file missing or empty with an existing WAL", ErrCorrupt)
		}
		if _, err = os.Stat(filepath.Join(dir, "compact.tmp")); err == nil {
			return nil, fmt.Errorf("%w: reserved compaction path in an uninitialized directory", ErrCorrupt)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	f, err := openFile(filepath.Join(dir, "data"))
	if err != nil {
		return nil, err
	}
	w := newWAL(dir)
	w.heapOnly = opts.CacheBytes > 0
	switch opts.SyncMode {
	case SyncBarrier:
		w.syncFn = syncBarrier
	case SyncOS:
		w.syncFn = syncOS
	default:
		w.syncFn = syncFull
	}
	limit := 0
	if opts.CacheBytes > 0 {
		limit = max(int(opts.CacheBytes/PageSize), 8)
		if opts.MemtableBytes == 0 {
			opts.MemtableBytes = opts.CacheBytes / 2
		}
	}

	pg, err := newPager(f, limit, !opts.NoVerifyChecksums)
	if err != nil {
		f.Close()
		return nil, err
	}
	db := &DB{dirLock: directoryLock, dir: dir, pager: pg, wal: w, opts: opts, scratch: make(page, PageSize), scratch2: make([]byte, 0, 2*PageSize),
		stop: make(chan struct{}), done: make(chan struct{}), ckptReq: make(chan ckptRequest, 16), ckptDone: make(chan struct{})}
	db.cond = sync.NewCond(&db.mu)
	if opts.MemtableBytes > 0 {
		db.mt = newMemtable()
	}
	st, err := f.Stat()
	if err == nil {
		if st.Size() == 0 {
			err = db.create()
		} else if st.Size() < 2*PageSize {
			err = ErrCorrupt
		} else {
			err = db.recover()
		}
	}
	if err == nil {
		err = db.loadCollections()
	}
	if err == nil {
		err = db.loadIndexes()
	}
	if err == nil {
		err = db.loadComputedFields()
	}
	if err == nil {
		err = db.loadComputedCollections()
	}
	if err == nil && db.upgradeFormat {
		// Publish the current format durably before any computed catalog record can
		// reach the WAL; an older writer must refuse the database even after
		// a crash before the first post-definition checkpoint.
		err = db.runCheckpoint(true)
		if err == nil {
			// Upgrade both alternating slots. Falling back after a damaged
			// newest meta page must not expose a valid older-format writer path.
			err = db.runCheckpoint(true)
		}
		db.legacyFormat = false
		db.legacyViews = false
		db.upgradeFormat = false
	}
	if err != nil {
		// release the lock and descriptors: a failed open must not wedge the next one
		w.close()
		pg.close()
		return nil, err
	}
	// Cleanup only after validating/recovering an owned database. A rejected
	// format/corrupt directory must keep its fallback and unfinished copy.
	os.RemoveAll(filepath.Join(dir, "compact.tmp"))
	os.Remove(oldPath)
	lockOwned = false
	go db.flusher()
	go db.checkpointer()
	return db, nil
}

func (db *DB) create() error {
	db.pager.hwm = 2
	db.pager.growTo(2)
	if err := db.pager.ensureMapped(2); err != nil {
		return err
	}
	root := db.pager.alloc(pageLeaf, 0)
	db.root = root.id
	if err := db.wal.start(1, 0); err != nil {
		return err
	}
	return db.runCheckpoint(true)
}

func (db *DB) recover() error {
	m, err := db.readMeta()
	if err != nil {
		return err
	}
	db.root, db.pager.hwm, db.ckpt = m.root, m.hwm, m.checkpoint
	db.pager.growTo(m.hwm)
	if err := db.pager.ensureMapped(m.hwm); err != nil {
		return err
	}
	ok, err := db.loadFreelist(m.freeHead, m.hwm)
	if err != nil {
		return err
	}
	if !ok {
		if err := db.rebuildFreelist(m.hwm); err != nil {
			return err
		}
	}
	db.activeStart = lsn{m.walGen, m.walOffset}
	lastGen, end, err := db.wal.replayFrom(m.walGen, m.walOffset, db.putLocked)
	if err != nil {
		return err
	}
	// continue appending to the last generation, dropping a torn tail
	if err := db.wal.start(lastGen, end); err != nil {
		return err
	}
	if f, err := os.OpenFile(walPath(db.dir, lastGen), os.O_RDWR, 0); err == nil {
		f.Truncate(int64(end))
		f.Close()
	}
	db.wal.dropBefore(m.walGen)
	if db.memtableFull() {
		return db.runCheckpoint(true)
	}
	return nil
}

// putLocked applies one upsert or tombstone to the memtable if there is
// one, else to the tree. Writer lock held; the record is already in the log.
func (db *DB) putLocked(key, val []byte, del bool) error {
	db.revision++
	if db.mt != nil {
		db.mt.put(key, val, del)
		return nil
	}
	if del {
		return db.deleteTree(key)
	}
	return db.insertTree(key, val)
}

func (db *DB) memtableFull() bool {
	return db.mt != nil && int64(db.mt.bytes) >= db.opts.MemtableBytes
}

func (db *DB) flusher() {
	defer close(db.done)
	t := time.NewTicker(db.opts.LogFlushInterval)
	defer t.Stop()
	for {
		db.mu.RLock()
		w := db.wal
		db.mu.RUnlock()
		select {
		case <-db.stop:
			db.markFailed(w.flush())
			return
		case <-w.kick:
			db.markFailed(w.flushAsync())
		case <-t.C:
			db.markFailed(w.flushAsync())
		}
	}
}

// Insert creates a key in the default collection; duplicates return
// ErrKeyExists. Values that do not fit a cell are stored in overflow
// pages, up to MaxValueSize.
func (db *DB) Insert(key, val []byte) error { return db.def.Insert(key, val) }

// Upsert creates or replaces a key in the default collection.
func (db *DB) Upsert(key, val []byte) error { return db.def.Upsert(key, val) }

// Replace replaces an existing key in the default collection.
func (db *DB) Replace(key, val []byte) error { return db.def.Replace(key, val) }

// Delete removes key from the default collection. Deleting an absent key
// is not an error.
func (db *DB) Delete(key []byte) error { return db.def.Delete(key) }

// noteHold records the longest time a section held the writer lock.
func (db *DB) noteHold(section string, since time.Time) {
	if d := time.Since(since); d > db.holds[section] {
		if db.holds == nil {
			db.holds = map[string]time.Duration{}
		}
		db.holds[section] = d
	}
}

// noteActiveStartLocked remembers where in the log the active memtable's
// records begin: a checkpoint's meta may not point past it while the active
// memtable holds anything. Called before the record is appended.
func (db *DB) noteActiveStartLocked() {
	if db.mt != nil && db.mt.len() == 0 {
		g, off := db.wal.position()
		db.activeStart = lsn{g, off}
	}
}

// stallLocked is the engine's only flow control: a writer waits when the
// memtable being applied has not drained and the active one has grown to
// four times its budget, i.e. when writes outrun the disk for a sustained
// stretch. Writer lock held on entry and exit.
func (db *DB) stallLocked() {
	for db.mt != nil && int64(db.mt.bytes) >= 4*db.opts.MemtableBytes && !db.closed && db.failed == nil {
		db.stalls++
		if db.imm == nil {
			db.requestCheckpoint()
		}
		db.cond.Wait()
	}
}

// Batch aggregates writes into one log write and one flush, applied
// atomically: readers see nothing until Commit returns, and a crash
// replays all of it or none. It is not safe for concurrent use. Writes go
// to the batch's collection; With returns a view of the same batch bound
// to another collection, so one batch can span collections.
type Batch struct {
	coll *Collection
	*batchState
}

type batchState struct {
	db         *DB
	buf        []byte
	lastOff    int
	n          int
	internal   bool // may write reserved keys (catalog and index maintenance)
	mixed      bool
	first      *Collection
	conditions []writeCondition
	limit      int           // staged bytes; zero for internal/single-value writes
	views      []*Collection // other collections with queued records
}

// NewBatch returns an empty batch bound to the default collection.
func (db *DB) NewBatch() *Batch { return db.def.NewBatch() }

// With returns the same batch bound to collection c: writes queued through
// either handle commit together.
func (b *Batch) With(c *Collection) *Batch {
	return &Batch{coll: c, batchState: b.batchState}
}

// Upsert queues a create or replacement in the batch.
func (b *Batch) Upsert(key, val []byte) error {
	if len(val) > MaxValueSize {
		return ErrValueSize
	}
	return b.add(key, val, false)
}

// Delete queues a tombstone in the batch.
func (b *Batch) Delete(key []byte) error { return b.add(key, nil, true) }

// UpsertJSON queues a create or replacement from JSON.
func (b *Batch) UpsertJSON(key, doc []byte) error {
	enc, err := EncodeJSON(nil, doc)
	if err != nil {
		return err
	}
	return b.Upsert(key, enc)
}

func (b *Batch) add(key, val []byte, del bool) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		// The public limit applies to user keys. Internal collection rows
		// also carry a one/three-byte tag; reserved catalog/index keys retain
		// their existing total-length limit.
		_, user, ok := splitKey(key)
		if !b.internal || !ok || len(user) == 0 || len(user) > MaxKeySize {
			return ErrKeySize
		}
	}
	if !b.internal {
		if b.coll.db != b.db {
			return errors.New("aptevadb: batch collection belongs to another database")
		}
		if b.coll.snap != nil {
			return ErrReadOnly
		}
		if b.coll.state.derived != nil {
			return ErrComputedCollectionReadOnly
		}
		if err := b.coll.check(); err != nil {
			return err
		}
		if b.n == 0 {
			b.first, b.mixed, b.views = b.coll, false, b.views[:0]
		} else if b.coll != b.first {
			b.mixed = true
			seen := false
			for _, c := range b.views {
				if c == b.coll {
					seen = true
					break
				}
			}
			if !seen {
				b.views = append(b.views, b.coll)
			}
		}
		var kb [64]byte
		key = b.coll.ikey(kb[:0], key)
	}
	if b.limit != 0 && len(b.buf)+len(key)+len(val)+11 > b.limit {
		return ErrTransactionTooLarge
	}
	b.lastOff = len(b.buf)
	b.buf = encodeRecord(b.buf, key, val, true, del, b.db.wal.crcTab)
	b.n++
	return nil
}

// Len reports the number of queued inserts.
func (b *Batch) Len() int { return b.n }

// Commit applies the batch atomically and, in sync mode, waits for one flush.
func (b *Batch) Commit() error { return b.CommitContext(nil) }

// CommitContext checks cancellation before logging; once logging starts the
// atomic write and required flush are completed.
func (b *Batch) CommitContext(ctx context.Context) error {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}

	if b.n == 0 {
		return nil
	}
	db := b.db
	db.mu.Lock()
	if db.closed || db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		if db.closed {
			return ErrClosed
		}
		return err
	}
	db.stallLocked()
	if err := ctx.Err(); err != nil {
		db.mu.Unlock()
		return err
	}
	w := db.wal
	l, err := b.commitLocked()
	needCkpt := db.needCheckpoint()
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if db.opts.Sync {
		if err := db.syncWrite(w, l); err != nil {
			return err
		}
	}
	if needCkpt {
		db.requestCheckpoint()
	}
	return nil
}

// commitLocked appends the batch to the log and applies it, adding the
// index maintenance its records require. Writer lock held. The batch is
// reset afterwards.
func (b *Batch) commitLocked() (lsn, error) {
	db := b.db
	if b.n == 0 {
		return lsn{}, nil
	}
	if !b.internal {
		if err := b.validateConditionsLocked(); err != nil {
			b.reset()
			return lsn{}, err
		}
		check := func(c *Collection) error {
			var err error
			if c.db != db {
				err = errors.New("aptevadb: batch collection belongs to another database")
			} else if c.snap != nil {
				err = ErrReadOnly
			} else if c.state.derived != nil {
				err = ErrComputedCollectionReadOnly
			} else {
				err = c.check()
			}
			return err
		}
		if err := check(b.coll); err != nil {
			b.reset()
			return lsn{}, err
		}
		if b.first != b.coll {
			if err := check(b.first); err != nil {
				b.reset()
				return lsn{}, err
			}
		}
		for _, c := range b.views {
			if err := check(c); err != nil {
				b.reset()
				return lsn{}, err
			}
		}
		if db.computedCount > 0 && (b.mixed || b.first.state.computed != nil) {
			if err := b.addComputedFieldsLocked(); err != nil {
				b.reset()
				return lsn{}, err
			}
		}
		if db.derivedCount > 0 && (b.mixed || len(b.first.state.dependents) > 0) {
			if err := b.addComputedCollectionOpsLocked(); err != nil {
				b.reset()
				return lsn{}, err
			}
		}
	}
	if b.limit != 0 && len(b.buf) > b.limit {
		b.reset()
		return lsn{}, ErrTransactionTooLarge
	}
	if !b.internal && db.hasIndexes() {
		if err := b.addIndexOpsLocked(); err != nil {
			b.reset()
			return lsn{}, err
		}
	}
	if b.limit != 0 && len(b.buf) > b.limit {
		b.reset()
		return lsn{}, ErrTransactionTooLarge
	}
	// clear the "more" flag on the last record and refresh its checksum
	last := b.buf[b.lastOff:]
	last[8] &^= flagMore
	le.PutUint32(last[4:], crc32.Checksum(last[8:], db.wal.crcTab))
	db.noteActiveStartLocked()
	l, err := db.wal.appendRaw(b.buf)
	if err == nil {
		for off := 0; off < len(b.buf); {
			plen := int(le.Uint32(b.buf[off:]))
			rec := b.buf[off+8 : off+8+plen]
			klen := int(le.Uint16(rec[1:]))
			if err = db.putLocked(rec[3:3+klen], rec[3+klen:], rec[0]&flagDelete != 0); err != nil {
				break
			}
			off += 8 + plen
		}
	}
	if err != nil {
		db.failed = err
	}
	b.reset()
	return l, err
}

// addIndexOpsLocked appends the index entry changes for every user record
// in the batch, reading each key's current document (or the batch's own
// earlier write to the same key). Only the indexes of the record's
// collection apply. Writer lock held.
func (b *Batch) addIndexOpsLocked() error {
	db := b.db
	ib := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1}}
	if b.limit != 0 {
		ib.limit = b.limit - len(b.buf)
		if ib.limit == 0 {
			ib.limit = -1
		}
	}
	pending := map[string][]byte{}
	perColl := map[uint32][]indexDef{}
	var buf []byte
	for off := 0; off < len(b.buf); {
		plen := int(le.Uint32(b.buf[off:]))
		rec := b.buf[off+8 : off+8+plen]
		klen := int(le.Uint16(rec[1:]))
		key, val, del := rec[3:3+klen], rec[3+klen:], rec[0]&flagDelete != 0
		off += 8 + plen
		cid, uk, ok := splitKey(key)
		if !ok {
			continue
		}
		defs, seenColl := perColl[cid]
		if !seenColl {
			for _, d := range db.indexDefs() {
				if d.Coll == cid {
					defs = append(defs, d)
				}
			}
			perColl[cid] = defs
		}
		if len(defs) == 0 {
			continue
		}
		old, seen := pending[string(key)]
		if !seen {
			cur, ok, err := db.findLocked(view{db, nil}, key, buf)
			if err != nil {
				return err
			}
			if ok {
				old = append([]byte(nil), cur...)
				buf = cur
			}
		}
		var newDoc []byte
		if !del {
			newDoc = val
		}
		if err := indexOps(defs, ib, uk, old, newDoc); err != nil {
			return err
		} // entries carry the user key
		pending[string(key)] = newDoc
	}
	if err := db.validateUniqueDocumentsLocked(pending, db.indexDefs()); err != nil {
		return err
	}
	for key := range pending {
		cid, uk, ok := splitKey([]byte(key))
		if !ok {
			continue
		}
		for _, d := range perColl[cid] {
			if t := db.idx.touched(d.ID); t != nil {
				t[string(uk)] = struct{}{}
			}
		}
	}
	if ib.n > 0 {
		if b.limit != 0 && len(b.buf)+len(ib.buf) > b.limit {
			return ErrTransactionTooLarge
		}
		b.buf = append(b.buf, ib.buf...)
		b.lastOff = len(b.buf) - (len(ib.buf) - ib.lastOff)
		b.n += ib.n
	}
	return nil
}

// Find appends the value for key in the default collection to dst and
// reports whether it exists.
func (db *DB) Find(key, dst []byte) ([]byte, bool, error) { return db.def.Find(key, dst) }

func (db *DB) find(v view, key, dst []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return dst[:0], false, ErrClosed
	}
	return db.findLocked(v, key, dst)
}

// findLocked is Find with the lock already held (either mode).
func (db *DB) findLocked(v view, key, dst []byte) ([]byte, bool, error) {
	if db.failed != nil {
		return dst[:0], false, db.failed
	}
	if v.snap != nil && v.snap.closed {
		return dst[:0], false, ErrClosed
	}

	if mv, del, ok := v.memGet(key); ok {
		if del {
			return dst[:0], false, nil
		}
		return append(dst[:0], mv...), true, nil
	}
	p, err := v.page(v.root())
	if err != nil {
		return dst[:0], false, err
	}
	for p.typ() == pageInternal {
		if p, err = v.page(p.child(p.searchInternal(key))); err != nil {
			return dst[:0], false, err
		}
	}
	i, found := p.searchLeaf(key)
	if !found {
		return dst[:0], false, nil
	}
	out, err := v.leafValue(p, i, dst)
	return out, err == nil, err
}

// relocateTailLocked marks live pages above the compact high-water mark
// dirty so the next write pass moves them into low free ids. Bounded per
// call so dirty pages stay within the cache budget.
func (db *DB) relocateTailLocked() (int, error) {
	pg := db.pager
	target := pg.hwm - uint32(len(pg.free)) // every live page fits below this
	if pg.hwm-target < 64 {
		return 0, nil
	}
	budget := 4096
	if pg.limit > 0 {
		budget = pg.limit / 2
	}
	// pg.free is sorted descending; walk it alongside the ids from the top
	fi := 0
	moved := 0
	for id := pg.hwm - 1; id >= target && moved < budget; id-- {
		for fi < len(pg.free) && pg.free[fi] > id {
			fi++
		}
		if fi < len(pg.free) && pg.free[fi] == id {
			continue // free slot, nothing to move
		}
		n, err := pg.get(id)
		if err != nil {
			return moved, err
		}
		if t := n.data.typ(); t != pageLeaf && t != pageInternal {
			continue // overflow chains are pointed at from cells; freelist pages are rewritten anyway
		}
		// the content does not change: mark it for rewriting at a new id
		// without copying; a later modification still copies first
		if len(db.snaps) > 0 {
			db.preserveForSnapshots(n)
		}
		db.pager.markDirty(n)
		moved++
	}
	db.relocations += uint64(moved)
	return moved, nil
}

func (db *DB) writeMeta(m meta) error {
	buf := make(page, PageSize)
	db.encodeMeta(buf, m)
	err := writeAt(db.pager.f, buf, int64(m.checkpoint%2)*PageSize)
	return err
}

func (db *DB) encodeMeta(buf page, m meta) {
	clear(buf)
	copy(buf, metaMagic)
	le.PutUint32(buf[8:], FormatVersion)
	le.PutUint32(buf[12:], PageSize)
	le.PutUint64(buf[16:], m.checkpoint)
	le.PutUint32(buf[24:], m.root)
	le.PutUint32(buf[28:], m.hwm)
	le.PutUint64(buf[32:], m.walGen)
	le.PutUint64(buf[40:], m.walOffset)
	le.PutUint32(buf[48:], m.freeHead)
	le.PutUint32(buf[52:], crc32.Checksum(buf[:52], db.wal.crcTab))
}

func (db *DB) readMeta() (meta, error) {
	var best meta
	found := false
	buf := db.scratch
	for slot := int64(0); slot < 2; slot++ {
		if _, err := db.pager.f.ReadAt(buf, slot*PageSize); err != nil {
			continue
		}
		if string(buf[:8]) != metaMagic || le.Uint32(buf[12:]) != PageSize ||
			crc32.Checksum(buf[:52], db.wal.crcTab) != le.Uint32(buf[52:]) {
			continue
		}
		version := le.Uint32(buf[8:])
		if version != FormatVersion && version != 3 && version != 4 && version != 5 && version != 6 {
			return meta{}, fmt.Errorf("%w: file is format %d, this build reads %d", ErrFormat, le.Uint32(buf[8:]), FormatVersion)
		}
		if version < FormatVersion {
			db.upgradeFormat = true
		}
		m := meta{le.Uint64(buf[16:]), le.Uint32(buf[24:]), le.Uint32(buf[28:]), le.Uint64(buf[32:]), le.Uint64(buf[40:]), le.Uint32(buf[48:])}
		if !found || m.checkpoint > best.checkpoint {
			best, found = m, true
			db.readFormat = version
			db.legacyFormat = version == 3
			db.legacyViews = version < 5
		}
	}
	if !found {
		return meta{}, errors.New("aptevadb: no valid meta page")
	}
	return best, nil
}

// FormatVersionOf reports the on-disk format version of the database in
// dir, read from its meta pages without opening it, so a file this build
// refuses can still be identified. It does not take the database lock.
func FormatVersionOf(dir string) (uint32, error) { return formatVersionAt(filepath.Join(dir, "data")) }

func formatVersionAt(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	tab := crc32.MakeTable(crc32.Castagnoli)
	buf := make([]byte, PageSize)
	var best uint32
	var bestCkpt uint64
	found := false
	for slot := int64(0); slot < 2; slot++ {
		if _, err := f.ReadAt(buf, slot*PageSize); err != nil {
			continue
		}
		if string(buf[:8]) != metaMagic || crc32.Checksum(buf[:52], tab) != le.Uint32(buf[52:]) {
			continue
		}
		if ckpt := le.Uint64(buf[16:]); !found || ckpt > bestCkpt {
			best, bestCkpt, found = le.Uint32(buf[8:]), ckpt, true
		}
	}
	if !found {
		return 0, errors.New("aptevadb: no valid meta page")
	}
	return best, nil
}

// dirtyPressure reports whether dirty pages fill three quarters of the cache
// limit; the checkpoint that follows makes them evictable again.
func (db *DB) dirtyPressure() bool {
	return db.pager.limit > 0 && db.pager.dirtyN > db.pager.limit*3/4
}

// needCheckpoint decides, after a write, whether a full checkpoint is due:
// the log is large, the memtable is full, or (with no memtable) dirty pages
// press on the cache. Writer lock held.
func (db *DB) needCheckpoint() bool {
	if db.wal.size() >= db.opts.CheckpointLogBytes || (db.memtableFull() && db.imm == nil) {
		return true
	}
	return db.mt == nil && db.dirtyPressure()
}

// FlushLog hands buffered log records to the OS without waiting for an
// fsync; see DURABILITY.md.
func (db *DB) FlushLog() error {
	db.mu.RLock()
	w := db.wal
	db.mu.RUnlock()
	return w.flush()
}

// walkLeaves visits every key/value in order, reading overflow values in
// full. Writer lock held.
func (db *DB) walkLeaves(n *node, fn func(key, val []byte) error) error {
	p := n.data
	if p.typ() == pageLeaf {
		var buf []byte
		for i := 0; i < p.numCells(); i++ {
			v, err := db.leafValue(p, i, buf)
			if err != nil {
				return err
			}
			buf = v
			if err := fn(p.leafKey(i), v); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i <= p.numCells(); i++ {
		c, err := db.pager.get(p.child(i))
		if err != nil {
			return err
		}
		if err := db.walkLeaves(c, fn); err != nil {
			return err
		}
	}
	return nil
}

// Stats reports resident and file-level counters.
type Stats struct {
	ResidentPages, HighWaterPages, FreePages, DirtyPages    int
	Loads, Writes, Evictions, Balances, Merges, Relocations uint64
	Height                                                  int
	MemtableEntries, MemtableBytes                          int
	Snapshots, QuarantinedPages                             int
	Collections, Indexes                                    int
	LockHolds                                               map[string]time.Duration
	WriteStalls, IndexScans                                 uint64
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed || db.failed != nil {
		return Stats{}
	}
	s := Stats{HighWaterPages: int(db.pager.hwm), FreePages: len(db.pager.free), DirtyPages: db.pager.dirtyN,
		Loads: db.pager.loads, Writes: db.pager.writes, Evictions: db.pager.evictions, Balances: db.balances, Merges: db.merges, Relocations: db.relocations,
		ResidentPages: int(db.pager.resident.Load())}
	if n := db.pager.loaded(db.root); n != nil {
		s.Height = int(n.data.level()) + 1
	}
	if db.mt != nil {
		s.MemtableEntries, s.MemtableBytes = db.mt.len(), db.mt.bytes
	}
	s.Snapshots = len(db.snaps)
	for _, q := range db.quar {
		s.QuarantinedPages += len(q.ids)
	}
	s.WriteStalls = db.stalls
	db.idx.mu.RLock()
	s.IndexScans = db.idx.scans
	s.Indexes = len(db.idx.defs)
	db.idx.mu.RUnlock()
	db.colls.mu.RLock()
	s.Collections = len(db.colls.byName)
	db.colls.mu.RUnlock()
	db.wal.mu.Lock()
	maxFlush := db.wal.maxFlush
	db.wal.mu.Unlock()
	s.LockHolds = map[string]time.Duration{"wal-flush-write": maxFlush}
	for k, v := range db.holds {
		s.LockHolds[k] = v
	}
	return s
}
