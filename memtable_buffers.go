package db

import (
	"hash/maphash"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Memtable records are immutable. A table owns one reference; snapshots and
// unlocked scans retain another before the table can be retired. This lets
// checkpoints return record storage immediately without waiting for heap GC.
const memtableArenaBytes = 1 << 20

type memtableStorage struct {
	memory *memoryTracker
	refs   atomic.Int64
	arenas [][]byte
	tail   []byte
	used   int
}

func newMemtableStorage() *memtableStorage {
	s := &memtableStorage{}
	s.refs.Store(1)
	return s
}

func (s *memtableStorage) alloc(n int) []byte {
	if n == 0 {
		return nil
	}
	if len(s.tail) < n {
		size := max(memtableArenaBytes, (n+PageSize-1)/PageSize*PageSize)
		data, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			panic("aptevadb: cannot allocate memtable arena: " + err.Error())
		}
		if s.memory != nil {
			s.memory.memtables.Add(int64(len(data)))
		}
		s.arenas = append(s.arenas, data)
		s.tail = data
	}
	data := s.tail[:n:n]
	s.tail = s.tail[n:]
	s.used += n
	return data
}

func (s *memtableStorage) retain() { s.refs.Add(1) }
func (s *memtableStorage) release() {
	if s.refs.Add(-1) == 0 {
		for _, data := range s.arenas {
			syscall.Munmap(data)
			if s.memory != nil {
				s.memory.memtables.Add(-int64(len(data)))
				s.memory.signal()
			}
		}
		s.arenas, s.tail = nil, nil
	}
}

func (t *memtable) releaseBuffers() {
	if t != nil && t.storage != nil {
		t.storage.release()
		t.storage = nil
	}
}

// Record replacement also consumes space until a table retires. Account for
// it when triggering a checkpoint, so overwrite-heavy workloads stay bounded.
func (t *memtable) bufferedBytes() int {
	if t.n == 0 {
		return 0
	}
	return max(t.bytes, t.storage.used)
}

// These arrays contain only strings/slices/pointers into this storage, never
// pointers to Go heap objects. Node links and map buckets remain on the heap
// so the garbage collector can trace them normally.
type mtMappedElement interface {
	string | *mtValue | mtValue | mtEntry | mtSlot
}

func memtableArray[T mtMappedElement](s *memtableStorage, n int) []T {
	var zero T
	alignment := int(unsafe.Alignof(zero))
	if n == 0 {
		return nil
	}
	data := s.alloc(n*int(unsafe.Sizeof(zero)) + alignment - 1)
	padding := (alignment - int(uintptr(unsafe.Pointer(unsafe.SliceData(data)))%uintptr(alignment))) % alignment
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(data[padding:]))), n)
}

func (t *memtable) newLeaf() *mtNode {
	return &mtNode{keys: memtableArray[string](t.storage, mtFanout+1)[:0],
		vals: memtableArray[*mtValue](t.storage, mtFanout+1)[:0]}
}

// Caller holds the tree lock. References are atomic because several scans
// can retain the same table under a shared read lock.
func (v view) retainMemtables() []*memtableStorage {
	var stores []*memtableStorage
	if v.snap != nil {
		stores = append(stores, v.snap.memStores...)
	} else {
		for _, t := range []*memtable{v.db.mt, v.db.imm} {
			if t != nil && t.storage != nil {
				stores = append(stores, t.storage)
			}
		}
	}
	for _, s := range stores {
		s.retain()
	}
	return stores
}

func releaseMemtableStores(stores []*memtableStorage) {
	for _, s := range stores {
		s.release()
	}
}

// Database closure invalidates every read view. Scans already in callbacks
// hold their own references until they return.
func (db *DB) releaseMemtables() {
	db.mt.releaseBuffers()
	db.imm.releaseBuffers()
	for _, s := range db.snaps {
		releaseMemtableStores(s.memStores)
		s.memStores = nil
	}
}

// Open-addressed keyed hashes provide point lookup without retaining a heap
// map after every apply. All slot contents refer only to owned record storage.
// A fresh randomized seed prevents adversarial user keys from choosing probes.
type mtSlot struct {
	hash uint64
	key  string
	val  *mtValue
}

func (t *memtable) slot(key []byte) (*mtSlot, uint64) {
	h := maphash.Bytes(t.seed, key)
	mask := uint64(len(t.slots) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		s := &t.slots[i]
		if s.val == nil || (s.hash == h && s.key == string(key)) {
			return s, h
		}
	}
}

func (t *memtable) growSlots(capacity int) {
	slots := memtableArray[mtSlot](t.storage, capacity)
	mask := uint64(capacity - 1)
	for _, s := range t.slots {
		if s.val == nil {
			continue
		}
		i := s.hash & mask
		for slots[i].val != nil {
			i = (i + 1) & mask
		}
		slots[i] = s
	}
	t.slots = slots
}

// Apply borrows immutable mapped keys while the immutable table owns its
// storage. Tree insertion/deletion copy keys into pages and separator buffers;
// none retains the incoming slice after the operation returns.
func immutableMemtableKey(k string) []byte {
	return unsafe.Slice(unsafe.StringData(k), len(k))
}
