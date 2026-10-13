package db

import (
	"bytes"
	"errors"
)

// Scan calls fn for every key in [start, end) of the default collection in
// key order at one snapshot; nil bounds are open. fn returns false to stop.
// The snapshot is released before returning. Writers may run between leaves
// and callbacks without changing the scan's view.
// Callback keys and values are read-only and valid only during fn.
func (db *DB) Scan(start, end []byte, fn func(key, val []byte) bool) error {
	return db.def.Scan(start, end, fn)
}

func (db *DB) scan(v view, start, end []byte, fn func(key, val []byte) bool) error {
	// Merge buffered entries (newer) with the tree (older). The snapshot is
	// taken once; values in it are never mutated by the writer.
	db.mu.RLock()
	snap := v.memRange(start, end) // db.mt is swapped by the checkpointer under the lock
	db.mu.RUnlock()
	si := 0
	// emit yields buffered keys below k, then k itself (buffered value wins;
	// a buffered tombstone hides the tree's value).
	emit := func(k, v []byte) bool {
		for si < len(snap) && snap[si].k < string(k) {
			if !snap[si].del && !fn([]byte(snap[si].k), snap[si].v) {
				return false
			}
			si++
		}
		if si < len(snap) && snap[si].k == string(k) {
			e := snap[si]
			si++
			if e.del {
				return true
			}
			v = e.v
		}
		return fn(k, v)
	}
	if len(snap) == 0 {
		emit = fn
	}
	drain := func() error {
		for ; si < len(snap); si++ {
			if !snap[si].del && !fn([]byte(snap[si].k), snap[si].v) {
				return nil
			}
		}
		return nil
	}
	cur := start
	var curBuf, flat []byte
	var cells []cellRef
	type frame struct {
		n   page
		idx int
	}
	var stack [maxTreeDepth]frame
	for {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrClosed
		}
		n, err := v.page(v.root())
		if err != nil {
			db.mu.RUnlock()
			return err
		}
		depth := 0
		for n.typ() == pageInternal {
			idx := 0
			if cur != nil {
				idx = n.searchInternal(cur)
			}
			stack[depth] = frame{n, idx}
			depth++
			if n, err = v.page(n.child(idx)); err != nil {
				db.mu.RUnlock()
				return err
			}
		}
		leaf := n
		i := 0
		if cur != nil {
			i, _ = leaf.searchLeaf(cur)
		}
		if i >= leaf.numCells() {
			// every key here is below cur: hop to the next leaf, if any
			d := depth - 1
			for d >= 0 && stack[d].idx >= stack[d].n.numCells() {
				d--
			}
			if d < 0 {
				db.mu.RUnlock()
				return drain()
			}
			stack[d].idx++
			if n, err = v.page(stack[d].n.child(stack[d].idx)); err != nil {
				db.mu.RUnlock()
				return err
			}
			for n.typ() == pageInternal {
				if n, err = v.page(n.child(0)); err != nil {
					db.mu.RUnlock()
					return err
				}
			}
			leaf, i = n, 0
			if leaf.numCells() == 0 {
				db.mu.RUnlock()
				return drain()
			}
		}
		nc := leaf.numCells()
		if nc == 0 { // empty root
			db.mu.RUnlock()
			return drain()
		}
		// Copy the cells in range out flat, then release the lock before
		// calling fn: callbacks may take as long as they like and may use
		// the database themselves without deadlocking against a waiting
		// writer. A short range copies only its own cells, so a point probe
		// does not pay for the whole leaf.
		jEnd := nc
		flat, cells = flat[:0], cells[:0]
		for j := i; j < nc; j++ {
			// Inline keys and values are contiguous in the cell. Decode its
			// header once and copy them together, avoiding repeated slot loads
			// and two buffer growth checks for every row.
			off, kl := leaf.slot(j), leaf.slotKlen(j)
			if end != nil && bytes.Compare(leaf[off+leafCellHdr:off+leafCellHdr+kl], end) >= 0 {
				jEnd = j
				break
			}
			vl := int(le.Uint16(leaf[off+2:]))
			ko := len(flat)
			if vl == overflowMark {
				full, err := v.leafValue(leaf, j, nil)
				if err != nil {
					db.mu.RUnlock()
					return err
				}
				vl = len(full)
				flat = append(flat, leaf[off+leafCellHdr:off+leafCellHdr+kl]...)
				flat = append(flat, full...)
			} else {
				flat = append(flat, leaf[off+leafCellHdr:off+leafCellHdr+kl+vl]...)
			}
			cells = append(cells, cellRef{ko, kl, vl})
		}
		// resume just past the last key of this leaf
		curBuf = append(curBuf[:0], leaf.leafKey(nc-1)...)
		curBuf = append(curBuf, 0)
		db.mu.RUnlock()
		for _, c := range cells {
			vo := c.ko + c.kl
			if !emit(flat[c.ko:vo], flat[vo:vo+c.vl]) {
				return nil
			}
		}
		if jEnd < nc {
			return drain() // the range ends inside this leaf
		}
		cur = curBuf
		continue
	}
}

// cellRef locates one copied cell in a scan's flat buffer.
type cellRef struct{ ko, kl, vl int }

// Source adapts a key range of the default collection to the aggregation
// executor.
func (db *DB) Source(start, end []byte) Source { return db.def.Source(start, end) }

// ShardBounds returns up to n-1 keys that split the default collection into
// ranges of roughly equal page count.
func (db *DB) ShardBounds(n int) ([][]byte, error) { return db.def.ShardBounds(n) }

// Shards returns key-ordered sources covering the default collection.
func (db *DB) Shards(n int) ([]Source, error) { return db.def.Shards(n) }

// shardBounds returns up to n-1 keys of collection c that split it into
// ranges of roughly equal page count, taken from the shallowest tree level
// with enough separators inside the collection.
func (db *DB) shardBounds(c *Collection, n int) ([][]byte, error) {
	if n < 2 {
		return nil, nil
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	v := c.view()
	root, err := v.page(v.root())
	if err != nil {
		return nil, err
	}
	level := []page{root}
	var seps [][]byte
	for {
		seps = seps[:0]
		if level[0].typ() == pageLeaf {
			break
		}
		for _, nd := range level {
			for i := 0; i < nd.numCells(); i++ {
				if sp := nd.internalKey(i); bytes.HasPrefix(sp, c.pfx) {
					seps = append(seps, append([]byte(nil), sp[len(c.pfx):]...))
				}
			}
		}
		if len(seps) >= n-1 || level[0].level() == 1 {
			break
		}
		var next []page
		for _, nd := range level {
			for i := 0; i <= nd.numCells(); i++ {
				ch, err := v.page(nd.child(i))
				if err != nil {
					return nil, err
				}
				next = append(next, ch)
			}
		}
		level = next
	}
	if len(seps) == 0 {
		return nil, nil
	}
	k := min(n-1, len(seps))
	out := make([][]byte, 0, k)
	for i := 1; i <= k; i++ {
		out = append(out, seps[i*len(seps)/(k+1)])
	}
	return out, nil
}

// leafView is one leaf's cells [i, j), read in place under the reader lock.
type leafView struct {
	p    page
	i, j int
	v    view
}

// value reads the full value of cell c, following an overflow chain.
func (lv leafView) value(c int, dst []byte) ([]byte, error) { return lv.v.leafValue(lv.p, c, dst) }

// errNeedMerge reports that a view has buffered records a leaf-level scan
// cannot merge in.
var errNeedMerge = errors.New("aptevadb: buffered records need merging")

// scanLeaves calls fn for the cells of each leaf that fall in [start, end),
// in key order, holding the reader lock throughout the call: fn reads the
// page in place and must not use the database. It refuses with errNeedMerge
// when the view has buffered records, which only the copying scan merges.
func (db *DB) scanLeaves(v view, start, end []byte, fn func(lv leafView) bool) error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	buffered := false
	if v.snap != nil {
		buffered = len(v.snap.mem) > 0
	} else if db.mt != nil {
		buffered = db.mt.len() > 0 || (db.imm != nil && db.imm.len() > 0)
	}
	db.mu.RUnlock()
	if buffered {
		return errNeedMerge
	}
	cur := start
	var curBuf []byte
	type frame struct {
		n   page
		idx int
	}
	var stack [maxTreeDepth]frame
	for {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrClosed
		}
		n, err := v.page(v.root())
		if err != nil {
			db.mu.RUnlock()
			return err
		}
		depth := 0
		for n.typ() == pageInternal {
			idx := 0
			if cur != nil {
				idx = n.searchInternal(cur)
			}
			stack[depth] = frame{n, idx}
			depth++
			if n, err = v.page(n.child(idx)); err != nil {
				db.mu.RUnlock()
				return err
			}
		}
		leaf := n
		i := 0
		if cur != nil {
			i, _ = leaf.searchLeaf(cur)
		}
		if i >= leaf.numCells() {
			d := depth - 1
			for d >= 0 && stack[d].idx >= stack[d].n.numCells() {
				d--
			}
			if d < 0 {
				db.mu.RUnlock()
				return nil
			}
			stack[d].idx++
			if n, err = v.page(stack[d].n.child(stack[d].idx)); err != nil {
				db.mu.RUnlock()
				return err
			}
			for n.typ() == pageInternal {
				if n, err = v.page(n.child(0)); err != nil {
					db.mu.RUnlock()
					return err
				}
			}
			leaf, i = n, 0
		}
		nc := leaf.numCells()
		if nc == 0 {
			db.mu.RUnlock()
			return nil
		}
		jEnd := nc
		if end != nil {
			if e, _ := leaf.searchLeaf(end); e < nc {
				jEnd = e
			}
		}
		more := fn(leafView{leaf, i, jEnd, v})
		curBuf = append(curBuf[:0], leaf.leafKey(nc-1)...)
		curBuf = append(curBuf, 0)
		db.mu.RUnlock()
		if !more || jEnd < nc {
			return nil
		}
		cur = curBuf
	}
}
