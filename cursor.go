package db

import (
	"bytes"
	"sort"
)

// Cursor iterates keys in order, forwards or backwards, merging the tree
// with the memtable at one snapshot taken when the cursor was created.
// Close the cursor to release its view. Sequential steps reuse the current
// leaf while its generation is unchanged; other movements descend the tree.
// No lock is held between calls.
type Cursor struct {
	owned     *Snapshot
	closed    bool
	c         *Collection
	v         view
	memStores []*memtableStorage
	snap      []mtEntry
	ikey      []byte // internal key of the current position
	key       []byte // user key
	val       []byte
	valid     bool
	err       error

	// the leaf and slot of the tree key found last, valid while the tree
	// generation is unchanged: a step then moves within the leaf instead
	// of descending from the root
	leafID  uint32
	leafIdx int
	leafGen uint64
	kbuf    []byte // key copied out of the cached leaf
}

// NewCursor returns a cursor over the default collection positioned
// nowhere; call Seek, First or Last.
func (db *DB) NewCursor() *Cursor { return db.def.NewCursor() }

func (c *Cursor) Valid() bool   { return c.valid }
func (c *Cursor) Err() error    { return c.err }
func (c *Cursor) Key() []byte   { return c.key }
func (c *Cursor) Value() []byte { return c.val }

// Seek positions the cursor at the first key >= key.
func (c *Cursor) Seek(key []byte) bool { return c.move(c.c.ikey(nil, key), true, true) }

// First positions the cursor at the smallest key.
func (c *Cursor) First() bool { return c.move(c.c.pfx, true, true) }

// Last positions the cursor at the largest key.
func (c *Cursor) Last() bool {
	if c.c.end == nil {
		return c.move(nil, false, true)
	}
	return c.move(c.c.end, false, false)
}

// Next advances to the following key.
func (c *Cursor) Next() bool {
	if !c.valid {
		return false
	}
	return c.step(c.ikey, true, false)
}

// Prev moves to the preceding key.
func (c *Cursor) Prev() bool {
	if !c.valid {
		return false
	}
	return c.step(c.ikey, false, false)
}

// move finds the nearest key in direction fwd from `from` (inclusive when
// incl), skipping tombstones.
func (c *Cursor) move(from []byte, fwd, incl bool) bool {
	// Only arbitrary seeks need to compare their position with the cached
	// slot. Next and Prev already know they start at the current position.
	if !incl && !bytes.Equal(from, c.ikey) {
		c.leafID = 0
	}
	return c.step(from, fwd, incl)
}

func (c *Cursor) step(from []byte, fwd, incl bool) bool {
	if c.closed || c.err != nil {
		c.valid = false
		return false
	}
	if err := c.c.check(); err != nil {
		c.err, c.valid = err, false
		return false
	}
	for {
		tk, tv, tok, err := c.treeStep(from, fwd, incl)
		if err != nil {
			c.err, c.valid = err, false
			return false
		}
		mi := c.memNeighbour(from, fwd, incl)
		var k, v []byte
		var del bool
		buffered := false
		switch {
		case !tok && mi < 0:
			c.valid = false
			return false
		case mi < 0:
			k, v = tk, tv
		case !tok:
			k, v, del = []byte(c.snap[mi].k), c.snap[mi].v, c.snap[mi].del
			buffered = true
		default:
			cmp := bytes.Compare([]byte(c.snap[mi].k), tk)
			if cmp == 0 || (fwd && cmp < 0) || (!fwd && cmp > 0) {
				k, v, del = []byte(c.snap[mi].k), c.snap[mi].v, c.snap[mi].del
				buffered = true
			} else {
				k, v = tk, tv
			}
		}
		if del {
			from, incl = k, false
			continue
		}
		if !bytes.HasPrefix(k, c.c.pfx) {
			c.leafID = 0    // the cached slot is outside the last returned position
			c.valid = false // outside the collection: nothing further this way
			return false
		}
		c.ikey = append(c.ikey[:0], k...)
		c.key = append(c.key[:0], k[len(c.c.pfx):]...)
		if buffered {
			// Buffered values belong to the snapshot and must remain private.
			c.val = append(c.val[:0], v...)
		} else {
			// treeStep already copied the value out under the reader lock.
			c.val = v
		}
		c.valid = true
		return true
	}
}

// treeStep is treeNeighbour with the leaf cache: an exclusive step from a
// key inside the cached leaf (the neighbour is provably in it: a key on
// each side of from) reads it in place; anything else descends.
func (c *Cursor) treeStep(from []byte, fwd, incl bool) (k, v []byte, ok bool, err error) {
	db := c.c.db
	if from != nil && !incl && c.leafID != 0 {
		db.mu.RLock()
		if !db.closed && c.leafGen == db.treeGen {
			if p, perr := c.v.page(c.leafID); perr == nil && p.typ() == pageLeaf {
				nc := p.numCells()
				i := c.leafIdx
				if fwd {
					if len(c.snap) == 0 {
						// Without buffered entries the previous result is exactly
						// this slot. Arbitrary seeks invalidate the cache in move.
						i++
					} else {
						for i < nc && bytes.Compare(p.leafKey(i), from) <= 0 {
							i++
						}
					}
					if i >= 1 && i < nc {
						c.val, err = c.v.leafValue(p, i, c.val[:0])
						c.kbuf = append(c.kbuf[:0], p.leafKey(i)...)
						c.leafIdx = i
						db.mu.RUnlock()
						return c.kbuf, c.val, err == nil, err
					}
				} else {
					if i >= nc {
						i = nc - 1
					}
					if len(c.snap) == 0 {
						i--
					} else {
						for i >= 0 && bytes.Compare(p.leafKey(i), from) >= 0 {
							i--
						}
					}
					if i >= 0 && i < nc-1 {
						c.val, err = c.v.leafValue(p, i, c.val[:0])
						c.kbuf = append(c.kbuf[:0], p.leafKey(i)...)
						c.leafIdx = i
						db.mu.RUnlock()
						return c.kbuf, c.val, err == nil, err
					}
				}
			}
		}
		db.mu.RUnlock()
	}
	// the generation must be the one the descent saw: taken later, under a
	// fresh lock, it could describe a tree in which the leaf has moved
	var id uint32
	var idx int
	var gen uint64
	k, v, id, idx, gen, ok, err = db.treeNeighbour(c.v, from, fwd, incl)
	if ok {
		if v == nil {
			// Empty boundary values must not discard the reusable private
			// buffer and force another allocation on the next step.
			v = c.val[:0]
		}
		c.leafID, c.leafIdx, c.leafGen = id, idx, gen
	} else {
		c.leafID = 0
	}
	return k, v, ok, err
}

// memNeighbour returns the snapshot index of the nearest entry, or -1.
func (c *Cursor) memNeighbour(from []byte, fwd, incl bool) int {
	if len(c.snap) == 0 {
		return -1
	}
	if from == nil {
		if fwd {
			return 0
		}
		return len(c.snap) - 1
	}
	f := string(from)
	if fwd {
		i := sort.Search(len(c.snap), func(i int) bool {
			if incl {
				return c.snap[i].k >= f
			}
			return c.snap[i].k > f
		})
		if i == len(c.snap) {
			return -1
		}
		return i
	}
	i := sort.Search(len(c.snap), func(i int) bool {
		if incl {
			return c.snap[i].k > f
		}
		return c.snap[i].k >= f
	})
	return i - 1
}

// treeNeighbour returns the nearest tree key in direction fwd from `from`
// (nil: from the end), with a copy of its full value, the leaf page id and
// slot it was found at, and the tree generation those are valid for.
func (db *DB) treeNeighbour(vw view, from []byte, fwd, incl bool) (k, v []byte, leafID uint32, slot int, gen uint64, ok bool, err error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, nil, 0, 0, 0, false, ErrClosed
	}
	gen = db.treeGen
	type frame struct {
		n   page
		idx int
	}
	var stack [maxTreeDepth]frame
	depth := 0
	id := vw.root()
	n, err := vw.page(id)
	if err != nil {
		return nil, nil, 0, 0, gen, false, err
	}
	for n.typ() == pageInternal {
		p := n
		idx := 0
		if from != nil {
			idx = p.searchInternal(from)
		} else if !fwd {
			idx = p.numCells()
		}
		stack[depth] = frame{n, idx}
		depth++
		id = p.child(idx)
		if n, err = vw.page(id); err != nil {
			return nil, nil, 0, 0, gen, false, err
		}
	}
	leaf := n
	var i int
	if from == nil {
		if fwd {
			i = 0
		} else {
			i = leaf.numCells() - 1
		}
	} else {
		pos, found := leaf.searchLeaf(from)
		if fwd {
			i = pos
			if found && !incl {
				i++
			}
		} else {
			i = pos - 1
			if found && incl {
				i = pos
			}
		}
	}
	// step to a neighbouring leaf when this one has nothing in that direction
	for i < 0 || i >= leaf.numCells() {
		d := depth - 1
		for d >= 0 {
			f := &stack[d]
			if fwd && f.idx < f.n.numCells() {
				f.idx++
				break
			}
			if !fwd && f.idx > 0 {
				f.idx--
				break
			}
			d--
		}
		if d < 0 {
			return nil, nil, 0, 0, gen, false, nil
		}
		id = stack[d].n.child(stack[d].idx)
		if n, err = vw.page(id); err != nil {
			return nil, nil, 0, 0, gen, false, err
		}
		depth = d + 1
		for n.typ() == pageInternal {
			idx := 0
			if !fwd {
				idx = n.numCells()
			}
			stack[depth] = frame{n, idx}
			depth++
			id = n.child(idx)
			if n, err = vw.page(id); err != nil {
				return nil, nil, 0, 0, gen, false, err
			}
		}
		leaf = n
		if fwd {
			i = 0
		} else {
			i = leaf.numCells() - 1
		}
		if leaf.numCells() == 0 {
			// an empty leaf can only be the root
			return nil, nil, 0, 0, gen, false, nil
		}
	}
	v, err = vw.leafValue(leaf, i, nil)
	if err != nil {
		return nil, nil, 0, 0, gen, false, err
	}
	return append([]byte(nil), leaf.leafKey(i)...), v, id, i, gen, true, nil
}

// Close releases the cursor's read view. Close is idempotent. A cursor is
// owned by one goroutine; Key and Value are valid until its next operation.
func (c *Cursor) Close() error {
	if !c.closed {
		c.closed = true
		c.valid = false
		if c.owned != nil {
			c.owned.Close()
			c.owned = nil
		}
		releaseMemtableStores(c.memStores)
		c.memStores = nil
		c.snap = nil
		c.key, c.val = nil, nil
	}
	return nil
}
