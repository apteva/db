package db

import (
	"hash/maphash"
	"sort"
	"sync"
	"unsafe"
)

type mtEntry struct {
	k   string
	v   []byte
	del bool
}

type mtValue struct {
	v   []byte
	del bool
}

// memtable absorbs inserts in memory ahead of the tree, so that they reach
// leaves in sorted batches: one leaf write then carries many records instead
// of one. Every entry is already in the log, so nothing here is at risk.
//
// Keys are kept in order by a small in-memory B+tree (wide nodes, contiguous
// arrays: an insert is a few binary searches and a short move), and a keyed hash table
// serves point reads and updates. A scan copies out just the entries of its
// range. Values are replaced, never mutated, so a copy taken by a reader
// stays valid while the writer keeps going. Every method runs under the
// database's writer lock (put) or reader lock (the rest).
type memtable struct {
	slots []mtSlot
	seed  maphash.Seed
	root  *mtNode
	first *mtNode // leftmost leaf; leaves are linked for range reads
	n     int
	bytes int      // key+value bytes, the budget that triggers an apply
	byTag [256]int // keys per leading byte: a collection's buffered keys, for size estimates

	snapMu sync.Mutex
	sorted []mtEntry // every entry in order; cached until the next put
	valid  bool

	budget  int64
	storage *memtableStorage
	values  []mtValue // current slab; map/tree pointers keep earlier slabs alive
}

// mtFanout is the most keys a node holds before it splits.
const mtFanout = 128

type mtNode struct {
	keys     []string
	vals     []*mtValue // leaves
	children []*mtNode  // internal nodes: len(keys)+1
	next     *mtNode    // leaves
}

func newMemtable(budget ...int64) *memtable {
	t := &memtable{storage: newMemtableStorage(), seed: maphash.MakeSeed()}
	if len(budget) > 0 {
		t.budget = budget[0]
	}
	t.root = t.newLeaf()
	t.first = t.root
	return t
}

// put stores a copy of val, or a tombstone. Writer lock held.
func (t *memtable) put(key, val []byte, del bool) {
	if len(t.slots) == 0 {
		capacity := 16
		if t.budget > 0 {
			expected := int(t.budget / int64(len(key)+len(val)+192))
			for capacity < min(expected*4/3+1, 1<<20) {
				capacity *= 2
			}
		}
		t.growSlots(capacity)
	}
	slot, hash := t.slot(key)
	if p := slot.val; p != nil {
		var v []byte
		if !del {
			v = t.storage.alloc(len(val))
			copy(v, val)
		}
		t.bytes += len(v) - len(p.v)
		*p = mtValue{v: v, del: del}
	} else {
		// One immutable allocation holds both key and initial value. Neither
		// is modified: replacing a value leaves snapshot aliases intact.
		n := len(key)
		if !del {
			n += len(val)
		}
		if (t.n+1)*4 >= len(t.slots)*3 {
			t.growSlots(2 * len(t.slots))
			slot, hash = t.slot(key)
		}
		record := t.storage.alloc(n)
		copy(record, key)
		k := unsafe.String(unsafe.SliceData(record), len(key))
		var v []byte
		if !del {
			copy(record[len(key):], val)
			v = record[len(key):n:n]
		}
		t.bytes += len(k) + len(v)
		t.n++
		if len(k) > 0 {
			t.byTag[k[0]]++
		}
		// Stable pointers without one separately allocated object per key.
		if len(t.values) == 0 {
			t.values = memtableArray[mtValue](t.storage, mtFanout)
		}
		p := &t.values[0]
		t.values = t.values[1:]
		*p = mtValue{v: v, del: del}
		*slot = mtSlot{hash: hash, key: k, val: p}
		t.insert(k, p)
	}
	t.snapMu.Lock()
	t.valid = false
	t.snapMu.Unlock()
}

// insert adds a key absent from the tree.
func (t *memtable) insert(k string, p *mtValue) {
	if sk, right := t.insertInto(t.root, k, p); right != nil {
		t.root = &mtNode{keys: []string{sk}, children: []*mtNode{t.root, right}}
	}
}

// insertInto inserts under n and, when n splits, returns the separator and
// the new right sibling.
func (t *memtable) insertInto(n *mtNode, k string, p *mtValue) (string, *mtNode) {
	i := sort.SearchStrings(n.keys, k)
	if n.children == nil {
		n.keys = append(n.keys, "")
		n.vals = append(n.vals, nil)
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.vals[i+1:], n.vals[i:])
		n.keys[i], n.vals[i] = k, p
		if len(n.keys) <= mtFanout {
			return "", nil
		}
		mid := len(n.keys) / 2
		r := t.newLeaf()
		r.keys = append(r.keys, n.keys[mid:]...)
		r.vals = append(r.vals, n.vals[mid:]...)
		r.next = n.next
		clear(n.keys[mid:])
		clear(n.vals[mid:])
		n.keys, n.vals, n.next = n.keys[:mid], n.vals[:mid], r
		return r.keys[0], r
	}
	sk, right := t.insertInto(n.children[i], k, p)
	if right == nil {
		return "", nil
	}
	n.keys = append(n.keys, "")
	n.children = append(n.children, nil)
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.children[i+2:], n.children[i+1:])
	n.keys[i], n.children[i+1] = sk, right
	if len(n.keys) <= mtFanout {
		return "", nil
	}
	mid := len(n.keys) / 2
	up := n.keys[mid]
	r := &mtNode{keys: append([]string(nil), n.keys[mid+1:]...), children: append([]*mtNode(nil), n.children[mid+1:]...)}
	n.keys, n.children = n.keys[:mid:mid], n.children[:mid+1:mid+1]
	return up, r
}

// get reports the buffered value; del is true for a tombstone.
func (t *memtable) get(key []byte) (v []byte, del, ok bool) {
	if len(t.slots) == 0 {
		return nil, false, false
	}
	slot, _ := t.slot(key)
	p := slot.val
	if p == nil {
		return nil, false, false
	}
	return p.v, p.del, true
}

func (t *memtable) len() int { return t.n }

// seek returns the leaf holding the first key >= k and its index there.
func (t *memtable) seek(k string) (*mtNode, int) {
	n := t.root
	for n.children != nil {
		i := sort.SearchStrings(n.keys, k)
		if i < len(n.keys) && n.keys[i] == k {
			i++ // equal separator: the key lives in the right child
		}
		n = n.children[i]
	}
	i := sort.SearchStrings(n.keys, k)
	if i == len(n.keys) {
		return n.next, 0
	}
	return n, i
}

// rangeInto appends copies of the entries in [lo, hi) (nil bounds open) to
// dst, in key order.
func (t *memtable) rangeInto(dst []mtEntry, lo, hi []byte) []mtEntry {
	n, i := t.first, 0
	if lo != nil {
		n, i = t.seek(string(lo))
	}
	for ; n != nil; n, i = n.next, 0 {
		for ; i < len(n.keys); i++ {
			if hi != nil && n.keys[i] >= string(hi) {
				return dst
			}
			p := n.vals[i]
			dst = append(dst, mtEntry{n.keys[i], p.v, p.del})
		}
	}
	return dst
}

// snapshot returns every entry in key order; cached until the next put.
func (t *memtable) snapshot() []mtEntry {
	t.snapMu.Lock()
	defer t.snapMu.Unlock()
	if t.valid {
		return t.sorted
	}
	t.sorted, t.valid = t.rangeInto(memtableArray[mtEntry](t.storage, t.n)[:0], nil, nil), true
	return t.sorted
}

func (t *memtable) reset() {
	t.releaseBuffers()
	t.storage = newMemtableStorage()
	leaf := t.newLeaf()
	t.slots = nil
	t.root, t.first = leaf, leaf
	t.n, t.bytes = 0, 0
	t.values = nil
	t.byTag = [256]int{}
	t.snapMu.Lock()
	t.sorted, t.valid = nil, false
	t.snapMu.Unlock()
}
