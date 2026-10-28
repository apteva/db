package db

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
)

// node is a resident page plus its bookkeeping.
type node struct {
	data     page
	id       uint32 // physical page id in the data file
	dirty    bool   // modified since last checkpoint; written under the writer lock
	onDisk   bool   // this id holds a copy in the last durable tree
	mapped   bool   // data aliases the read-only file mapping
	frozen   bool   // data is being written by a checkpoint: copy before modifying
	writing  bool   // captured by a checkpoint not yet on disk: do not evict
	kind     byte   // immutable page type; maintenance need not fault clean contents
	modEpoch uint64 // snapshot epoch current at the last modification
	ref      atomic.Bool
	arena    *nodeArena       // descriptor storage, owned by pager/capture/staging
	buffer   *pageBufferArena // private writable storage; nil for file-backed pages
}

// pageType lets maintenance inspect clean descriptors without faulting
// their mapped contents. kind is recorded when a node is allocated/loaded.
func (n *node) pageType() byte {
	if n.kind != 0 {
		return n.kind
	}
	return n.data.typ()
}

// pager owns the data file, the resident page table and page-id allocation.
//
// Clean pages are views into a read-only mapping of the file: loading one
// costs a node struct and a page fault served by the OS cache, which is
// shared by every thread and every process. The writer copies a page into a
// private buffer before touching it (writable), and a checkpoint hands the
// page back to the mapping once it is on disk.
//
// Checkpoints are copy-on-write: a dirty page that already lives in the
// durable tree is written to a fresh id, and its old id is recycled only
// after the meta page that no longer references it is durable.
//
// With a page limit, clean pages are evicted by a CLOCK sweep. Dirty pages
// are never evicted, and a checkpoint runs when they crowd the limit.
type pager struct {
	memory      *memoryTracker
	trackMapped bool
	hot         []atomic.Uint64
	cold        []atomic.Uint64
	f           fileLike
	maps        [][]byte               // stable read-only file segments
	pages       []atomic.Pointer[node] // indexed by physical id; grown only under the writer lock
	copiedPages []uint32               // old file versions replaced by private writable copies
	discarded   []uint64               // recycled ranges already reclaimed; writer lock held
	verified    []atomic.Uint64        // bit per page id: checksum checked since open, or written by us
	hwm         uint32                 // first never-used page id
	free        []uint32               // ids safe to reuse now
	pending     []uint32               // ids freed by the in-progress checkpoint

	loadMu sync.Mutex // serialises table inserts and eviction on the read path

	verify   bool // check page checksums on load
	limit    int  // max resident pages; 0 means unlimited
	resident atomic.Int64
	dirtyN   int // dirty pages, maintained under the writer lock
	hand     int

	loads, writes, evictions   uint64
	bufferArenas               []*pageBufferArena
	bufferTail                 *pageBufferArena
	spareInternal              []*pageBufferArena // released one-page mappings kept for reuse
	nodeArenas                 []*nodeArena
	nodeTail, internalNodeTail *nodeArena
}

const minMapBytes = 64 << 20

// ErrCorrupt reports a page whose checksum does not match its contents.
var ErrCorrupt = errors.New("aptevadb: corrupt page")

// ErrLocked reports that another process holds the database.
var ErrLocked = errors.New("aptevadb: database is locked by another process")

func newPager(f fileLike, limit int, verify bool) (*pager, error) {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, ErrLocked
	}
	return &pager{f: f, limit: limit, verify: verify}, nil
}

// ensureMapped adds fixed-size segments. Existing addresses stay valid for
// snapshots and in-flight checkpoint I/O, and file growth never creates a
// second resident mapping of pages already in the cache. Writer lock held.
func (pg *pager) ensureMapped(n uint32) error {
	count := (uint64(n)*PageSize + minMapBytes - 1) / minMapBytes
	for uint64(len(pg.maps)) < count {
		mm, err := syscall.Mmap(int(pg.f.Fd()), int64(len(pg.maps))*minMapBytes,
			minMapBytes, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return err
		}
		pg.maps = append(pg.maps, mm)
	}
	return nil
}

func (pg *pager) close() error {
	for _, a := range pg.nodeArenas {
		syscall.Munmap(a.data)
		if pg.memory != nil {
			pg.memory.nodes.Add(-int64(len(a.data)))
		}
	}
	pg.nodeArenas = nil
	pg.nodeTail, pg.internalNodeTail = nil, nil
	for _, a := range pg.bufferArenas {
		syscall.Munmap(a.data)
		if pg.memory != nil {
			pg.memory.pages.Add(-int64(len(a.data)))
		}
	}
	for _, a := range pg.spareInternal {
		syscall.Munmap(a.data)
		if pg.memory != nil {
			pg.memory.pages.Add(-int64(len(a.data)))
		}
	}
	pg.bufferArenas, pg.bufferTail, pg.spareInternal = nil, nil, nil
	for _, m := range pg.maps {
		syscall.Munmap(m)
	}
	pg.maps = nil
	syscall.Flock(int(pg.f.Fd()), syscall.LOCK_UN)
	return pg.f.Close()
}

func (pg *pager) mappedPage(id uint32) page {
	const pagesPerMap = minMapBytes / PageSize
	off := int(id%pagesPerMap) * PageSize
	return pg.maps[id/pagesPerMap][off : off+PageSize : off+PageSize]
}

func (pg *pager) growTo(n uint32) {
	pg.growHot()
	if int(n) > len(pg.pages) {
		size := int(n) + int(n)/2 + 64
		np := make([]atomic.Pointer[node], size)
		for i := range pg.pages {
			np[i].Store(pg.pages[i].Load())
		}
		pg.pages = np
		nv := make([]atomic.Uint64, size/64+1)
		for i := range pg.verified {
			nv[i].Store(pg.verified[i].Load())
		}
		pg.verified = nv
		nd := make([]uint64, size/64+1)
		copy(nd, pg.discarded)
		pg.discarded = nd
	}
}

func (pg *pager) isVerified(id uint32) bool {
	return pg.verified[id/64].Load()&(1<<(id%64)) != 0
}

func (pg *pager) markVerified(id uint32) {
	w := &pg.verified[id/64]
	for {
		old := w.Load()
		if old&(1<<(id%64)) != 0 || w.CompareAndSwap(old, old|1<<(id%64)) {
			return
		}
	}
}

func (pg *pager) loaded(id uint32) *node {
	if int(id) < len(pg.pages) {
		return pg.pages[id].Load()
	}
	return nil
}

// get returns the resident node for id, loading it if needed.
func (pg *pager) get(id uint32) (*node, error) {
	if n := pg.pages[id].Load(); n != nil {
		if pg.hot != nil && n.mapped {
			pg.noteMappedRead(id)
		}
		if pg.limit > 0 && !n.ref.Load() {
			n.ref.Store(true)
		}
		return n, nil
	}
	return pg.load(id)
}

// read returns page bytes without allocating a node for an already verified
// clean cache miss. The file mapping owns these bytes; evicting a descriptor
// does not evict mapped file pages, so read-only cache churn needs no objects.
// Writers still use get to obtain and pin mutable node state.
func (pg *pager) read(id uint32) (page, error) {
	if n := pg.pages[id].Load(); n != nil {
		if pg.hot != nil && n.mapped {
			pg.noteMappedRead(id)
		}
		return n.data, nil
	}
	if pg.hot != nil {
		pg.noteMappedRead(id)
	}
	if !pg.verify || pg.isVerified(id) {
		return pg.mappedPage(id), nil
	}
	n, err := pg.load(id)
	if err != nil {
		return nil, err
	}
	return n.data, nil
}

// load makes a node for a page in the durable file, viewed through the mapping.
func (pg *pager) load(id uint32) (*node, error) {
	pg.loadMu.Lock()
	defer pg.loadMu.Unlock()
	if n := pg.pages[id].Load(); n != nil {
		return n, nil
	}
	if pg.hot != nil {
		pg.noteMappedRead(id)
	}
	p := pg.mappedPage(id)
	n := pg.newNode(p.typ())
	n.data, n.id, n.onDisk, n.mapped, n.kind = p, id, true, true, p.typ()
	if pg.verify && !pg.isVerified(id) {
		if pageCrc(n.data) != n.data.crc() {
			releaseNode(n)
			return nil, fmt.Errorf("%w: page %d", ErrCorrupt, id)
		}
		pg.markVerified(id)
	}
	n.ref.Store(true)
	pg.loads++
	pg.pages[id].Store(n)
	pg.resident.Add(1)
	pg.evictLocked(n)
	return n, nil
}

// evictLocked drops clean, unreferenced pages until under the limit, never
// touching keep (the page whose arrival triggered the sweep). loadMu held.
func (pg *pager) evictLocked(keep *node) {
	if pg.limit == 0 {
		return
	}
	over := int(pg.resident.Load()) - pg.limit
	if over <= 0 {
		return
	}
	target := over + pg.limit/16
	evicted := 0
	hwm := int(pg.hwm)
	for scanned := 0; evicted < target && scanned < 2*hwm; scanned++ {
		pg.hand++
		if pg.hand >= hwm {
			pg.hand = 0
		}
		n := pg.pages[pg.hand].Load()
		// Internal pages stay resident: the checkpoint walk reaches dirty
		// leaves only through resident parents, and they are <1% of the tree.
		if n == nil || n == keep || n.dirty || n.writing || n.pageType() == pageInternal {
			continue
		}
		if n.ref.Load() {
			n.ref.Store(false)
			continue
		}
		pg.pages[pg.hand].Store(nil)
		releaseNode(n)
		pg.resident.Add(-1)
		pg.evictions++
		evicted++
	}
}

// allocID hands out a page id that is not part of the durable tree, lowest
// free id first so that freed space gathers at the end of the file.
func (pg *pager) allocID() uint32 {
	if k := len(pg.free); k > 0 {
		id := pg.free[k-1]
		pg.free = pg.free[:k-1]
		pg.discarded[id/64] &^= uint64(1) << (id % 64)
		return id
	}
	id := pg.hwm
	pg.hwm++
	pg.growTo(pg.hwm)
	pg.discarded[id/64] &^= uint64(1) << (id % 64)
	if err := pg.ensureMapped(pg.hwm); err != nil {
		panic("aptevadb: cannot grow file mapping: " + err.Error())
	}
	return id
}

// alloc creates a fresh dirty page. Writer lock held.
func (pg *pager) alloc(typ byte, level byte) *node {
	id := pg.allocID()
	n := pg.newNode(typ)
	n.id, n.dirty, n.kind = id, true, typ
	if typ == pageInternal {
		// Sparse parents can outlive hundreds of completed leaves. Keeping
		// these small buffers separate avoids pinning whole leaf arenas.
		n.data, n.buffer = pg.allocInternalPageBuffer(true)
	} else {
		n.data, n.buffer = pg.allocPageBuffer()
	}
	n.ref.Store(true)
	initPage(n.data, typ, level)
	pg.pages[n.id].Store(n)
	pg.loadMu.Lock()
	pg.resident.Add(1)
	pg.dirtyN++
	pg.evictLocked(n)
	pg.loadMu.Unlock()
	return n
}

// writable returns a private, dirty copy of the page for modification: a
// mapped page is copied out of the read-only mapping first. Writer lock held.
func (pg *pager) writable(n *node) page {
	if n.mapped {
		pg.copyPageBuffer(n)
		n.mapped = false
	}
	pg.markDirty(n)
	return n.data
}

// markDirty flags a page as modified and pins it in the table: a clean page
// fetched a moment ago may have been evicted by a sweep on this same path
// (a split allocates before the leaf is marked), and the checkpoint must
// find it. Writer lock held.
func (pg *pager) markDirty(n *node) {
	if !n.dirty {
		n.dirty = true
		pg.dirtyN++
		if pg.pages[n.id].Load() != n {
			pg.pages[n.id].Store(n)
			retainNode(n)
			pg.resident.Add(1)
		}
	}
}

// writeNode persists a dirty node copy-on-write, returns its (possibly new)
// id, and hands the node back to the file mapping.
func (pg *pager) writeNode(n *node) (uint32, error) {
	if n.onDisk {
		pg.pages[n.id].Store(nil)
		pg.pending = append(pg.pending, n.id)
		n.id = pg.allocID()
		pg.pages[n.id].Store(n)
	}
	n.data.setCrc(pageCrc(n.data))
	if err := writeAt(pg.f, n.data, int64(n.id)*PageSize); err != nil {
		return 0, err
	}
	pg.markVerified(n.id)
	pg.writes++
	if n.dirty {
		pg.dirtyN--
	}
	n.dirty = false
	n.onDisk = true
	pg.releasePageBuffer(n)
	n.data, n.mapped = pg.mappedPage(n.id), true
	return n.id, nil
}

// freePage drops a page that left the tree. Writer lock held. A page that
// never reached the durable tree is reusable at once; one that did must
// wait for the next meta flip.
func (pg *pager) freePage(n *node) {
	pg.releasePageBuffer(n)
	if pg.pages[n.id].Load() == n {
		pg.pages[n.id].Store(nil)
		releaseNode(n)
		pg.resident.Add(-1)
	}
	if n.dirty {
		pg.dirtyN--
		n.dirty = false
	}
	if n.onDisk {
		pg.pending = append(pg.pending, n.id)
	} else {
		pg.free = append(pg.free, n.id)
		sort.Slice(pg.free, func(i, j int) bool { return pg.free[i] > pg.free[j] })
	}
}

// releaseIDs makes the given ids reusable and reports a free tail.
func (pg *pager) releaseIDs(ids []uint32) (trailingFree uint32) {
	pg.free = append(pg.free, ids...)
	sort.Slice(pg.free, func(i, j int) bool { return pg.free[i] > pg.free[j] })
	if len(ids) >= 64 {
		pg.discardFreePages()
	}
	for len(pg.free) > 0 && pg.free[0] == pg.hwm-1 {
		pg.free = pg.free[1:]
		pg.hwm--
		trailingFree++
	}
	return trailingFree
}

// releasePending makes ids freed by the last checkpoint reusable, keeps the
// free list sorted descending (so pops yield the lowest id), and reports how
// many pages at the end of the file are free so the caller can truncate.
func (pg *pager) releasePending() (trailingFree uint32) {
	pg.free = append(pg.free, pg.pending...)
	pg.pending = pg.pending[:0]
	sort.Slice(pg.free, func(i, j int) bool { return pg.free[i] > pg.free[j] })
	for len(pg.free) > 0 && pg.free[0] == pg.hwm-1 {
		pg.free = pg.free[1:]
		pg.hwm--
		trailingFree++
	}
	return trailingFree
}
