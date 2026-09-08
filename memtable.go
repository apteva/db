package aptevadb

import (
	"sort"
	"sync"
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
// arrays: an insert is a few binary searches and a short move), and a map
// serves point reads and updates. A scan copies out just the entries of its
// range. Values are replaced, never mutated, so a copy taken by a reader
// stays valid while the writer keeps going. Every method runs under the
// database's writer lock (put) or reader lock (the rest).
type memtable struct {
	m     map[string]*mtValue
	root  *mtNode
	first *mtNode // leftmost leaf; leaves are linked for range reads
	n     int
	bytes int      // key+value bytes, the budget that triggers an apply
	byTag [256]int // keys per leading byte: a collection's buffered keys, for size estimates

	snapMu sync.Mutex
	sorted []mtEntry // every entry in order; cached until the next put
	valid  bool
}

// mtFanout is the most keys a node holds before it splits.
const mtFanout = 128

type mtNode struct {
	keys     []string
	vals     []*mtValue // leaves
	children []*mtNode  // internal nodes: len(keys)+1
	next     *mtNode    // leaves
}

func newMemtable() *memtable {
	leaf := &mtNode{}
	return &memtable{m: map[string]*mtValue{}, root: leaf, first: leaf}
}

// put stores a copy of val, or a tombstone. Writer lock held.
func (t *memtable) put(key, val []byte, del bool) {
	k := string(key)
	var nv mtValue
	if !del {
		nv.v = append([]byte(nil), val...)
	} else {
		nv.del = true
	}
	if p, ok := t.m[k]; ok {
		t.bytes += len(nv.v) - len(p.v)
		*p = nv
	} else {
		t.bytes += len(k) + len(nv.v)
		t.n++
		if len(k) > 0 {
			t.byTag[k[0]]++
		}
		p := &mtValue{v: nv.v, del: nv.del}
		t.m[k] = p
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
		r := &mtNode{keys: append([]string(nil), n.keys[mid:]...), vals: append([]*mtValue(nil), n.vals[mid:]...), next: n.next}
		n.keys, n.vals, n.next = n.keys[:mid:mid], n.vals[:mid:mid], r
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
	p, ok := t.m[string(key)]
	if !ok {
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
	t.sorted, t.valid = t.rangeInto(make([]mtEntry, 0, t.n), nil, nil), true
	return t.sorted
}

func (t *memtable) reset() {
	leaf := &mtNode{}
	t.m = map[string]*mtValue{}
	t.root, t.first = leaf, leaf
	t.n, t.bytes = 0, 0
	t.byTag = [256]int{}
	t.snapMu.Lock()
	t.sorted, t.valid = nil, false
	t.snapMu.Unlock()
}
