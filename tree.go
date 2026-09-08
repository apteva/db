package aptevadb

import "bytes"

// insertTree applies an upsert to the in-memory tree. A value too large for
// a cell is spilled into overflow pages first and the cell keeps a pointer.
// Writer lock held.
func (db *DB) insertTree(key, val []byte) error {
	db.treeGen++
	ovf := false
	if leafCellHdr+len(key)+len(val) > MaxCellSize {
		ptr, err := db.spill(val)
		if err != nil {
			return err
		}
		val, ovf = ptr, true
	}
	root, err := db.pager.get(db.root)
	if err != nil {
		return err
	}
	var sep []byte
	var right uint32
	var split bool
	if root.data.typ() == pageLeaf {
		ok, err := db.tryInsertLeaf(root, key, val, ovf)
		if err != nil || ok {
			return err
		}
		sep, right, err = db.splitLeafInsert(root, key, val, ovf, true)
		if err != nil {
			return err
		}
		split = true
	} else {
		sep, right, split, err = db.insertInto(root, key, val, ovf, true)
		if err != nil || !split {
			return err
		}
	}
	nr := db.pager.alloc(pageInternal, root.data.level()+1)
	nr.data.setLeftChild(root.id)
	nr.data.insertInternalCell(0, sep, right)
	db.root = nr.id
	return nil
}

// insertInto descends from internal node n to the leaf for key. rightmost is
// true while the path has only taken last children, which lets appends
// split lopsidedly. A leaf that overflows first tries to shed cells to a
// sibling; it splits only when both neighbours are full too.
func (db *DB) insertInto(n *node, key, val []byte, ovf, rightmost bool) (sep []byte, right uint32, split bool, err error) {
	p := n.data
	idx := p.searchInternal(key)
	child, err := db.pager.get(p.child(idx))
	if err != nil {
		return nil, 0, false, err
	}
	if child.data.typ() == pageInternal {
		csep, cright, csplit, err := db.insertInto(child, key, val, ovf, rightmost && idx == p.numCells())
		if err != nil || !csplit {
			return nil, 0, false, err
		}
		return db.insertInternal(n, idx, csep, cright)
	}
	ok, err := db.tryInsertLeaf(child, key, val, ovf)
	if err != nil || ok {
		return nil, 0, false, err
	}
	need := leafCellHdr + len(key) + len(val) + slotSize
	balanced := false
	if !db.sortedLoad { // a sorted load packs leaves full and moves on
		if balanced, err = db.balanceLeaf(n, idx, child, need); err != nil {
			return nil, 0, false, err
		}
	}
	if balanced {
		// balancing made the parent writable: its page buffer is a new copy
		p = n.data
		idx = p.searchInternal(key)
		if child, err = db.pager.get(p.child(idx)); err != nil {
			return nil, 0, false, err
		}
		if ok, err := db.tryInsertLeaf(child, key, val, ovf); err != nil || ok {
			return nil, 0, false, err
		}
	}
	csep, cright, err := db.splitLeafInsert(child, key, val, ovf, rightmost && idx == p.numCells())
	if err != nil {
		return nil, 0, false, err
	}
	return db.insertInternal(n, idx, csep, cright)
}

// tryInsertLeaf upserts key into leaf n if it fits. A stale value of a
// different size is removed either way, so the caller must complete the
// insert when it returns false.
func (db *DB) tryInsertLeaf(n *node, key, val []byte, ovf bool) (bool, error) {
	p := db.writable(n)
	i, found := p.searchLeaf(key)
	if found {
		if p.leafIsOverflow(i) {
			if err := db.freeOverflow(p.leafVal(i)); err != nil {
				return false, err
			}
			p.removeCell(i)
		} else if old := p.leafVal(i); !ovf && len(old) == len(val) {
			copy(old, val)
			return true, nil
		} else {
			p.removeCell(i)
		}
	}
	need := leafCellHdr + len(key) + len(val) + slotSize
	if p.freeSpace() < need && p.garbage() > 0 {
		p.compact(db.scratch)
	}
	if p.freeSpace() < need {
		return false, nil
	}
	p.insertLeafCell(i, key, val, ovf)
	return true, nil
}

// splitLeafInsert splits full leaf n and inserts key into the right half.
func (db *DB) splitLeafInsert(n *node, key, val []byte, ovf, appending bool) ([]byte, uint32, error) {
	p := db.writable(n)
	rn := db.pager.alloc(pageLeaf, 0)
	nc := p.numCells()
	if (appending || db.sortedLoad || db.sortedApply) && nc > 0 && bytes.Compare(key, p.leafKey(nc-1)) > 0 {
		// Appending past the last key in the tree, or in a sorted run:
		// leave this leaf full and start a fresh one, so sequential loads
		// pack pages tightly.
		rn.data.insertLeafCell(0, key, val, ovf)
		sep := append(db.sepBufs[0][:0], key...)
		db.sepBufs[0] = sep
		return sep, rn.id, nil
	}
	if db.sortedApply && nc > 1 {
		// A sorted batch is being applied: the keys after this one land on
		// its right, so split at its position and keep the left part full.
		if i, _ := p.searchLeaf(key); i > nc/2 && i < nc {
			first := p.splitLeafAt(rn.data, db.scratch, i)
			if p.freeSpace() >= leafCellHdr+len(key)+len(val)+slotSize {
				p.insertLeafCell(i, key, val, ovf)
				sep := append(db.sepBufs[0][:0], first...)
				db.sepBufs[0] = sep
				return sep, rn.id, nil
			}
			// no room even so: undo by merging back and split evenly
			for j := 0; j < rn.data.numCells(); j++ {
				p.appendRawCell(rn.data.rawCell(j))
			}
			initPage(rn.data, pageLeaf, 0)
		}
	}
	first := p.splitLeaf(rn.data, db.scratch)
	sep := append(db.sepBufs[0][:0], first...)
	db.sepBufs[0] = sep
	target := p
	if bytes.Compare(key, sep) >= 0 {
		target = rn.data
	}
	j, _ := target.searchLeaf(key)
	target.insertLeafCell(j, key, val, ovf)
	return sep, rn.id, nil
}

// balanceLeaf redistributes cells between full leaf child (index idx under
// parent) and a sibling with room, and rewrites the separator between them.
// It reports false when neither sibling can take enough, or the parent
// cannot hold the new separator.
func (db *DB) balanceLeaf(parent *node, idx int, child *node, need int) (bool, error) {
	pp := parent.data
	for _, side := range [2]int{1, -1} {
		sidx := idx + side
		if sidx < 0 || sidx > pp.numCells() {
			continue
		}
		sib, err := db.pager.get(pp.child(sidx))
		if err != nil {
			return false, err
		}
		// only worth it when the neighbour has real room: a nearly full
		// neighbour would take a cell or two and overflow again at once
		if sib.data.freeSpace()+sib.data.garbage() < max(2*need, PageSize/8) {
			continue
		}
		if db.debugVerify != nil {
			db.debugVerify("before balance", child.data, sib.data)
		}
		left, right := child, sib
		if side < 0 {
			left, right = sib, child
		}
		// lay out both leaves' cells in order, then pick the most balanced cut
		// at which both halves fit (cells plus slots) with room for the new record
		buf := db.scratch2[:0]
		var sizes []int
		for _, ln := range [2]*node{left, right} {
			for i := 0; i < ln.data.numCells(); i++ {
				c := ln.data.rawCell(i)
				buf = append(buf, c...)
				sizes = append(sizes, len(c))
			}
		}
		db.scratch2 = buf
		const capacity = PageSize - hdrSize
		total := 0
		for _, sz := range sizes {
			total += sz + slotSize
		}
		mid, bestDiff := -1, total
		acc := 0
		for i := 1; i < len(sizes); i++ {
			acc += sizes[i-1] + slotSize
			if l, r := acc, total-acc; l <= capacity-need && r <= capacity-need {
				if d := max(l, r) - min(l, r); d < bestDiff {
					mid, bestDiff = i, d
				}
			}
		}
		if mid < 0 {
			continue
		}
		off := 0
		for i := 0; i < mid; i++ {
			off += sizes[i]
		}
		newSep := buf[off+leafCellHdr : off+leafCellHdr+int(le.Uint16(buf[off:]))]
		// the parent must be able to hold the new separator before we commit
		sepCell := min(idx, sidx)
		oldSepSize := pp.cellSize(sepCell) + slotSize
		newSepSize := internalCellHdr + len(newSep) + slotSize
		if pp.freeSpace()+pp.garbage()+oldSepSize < newSepSize {
			continue
		}
		lp, rp := db.writable(left), db.writable(right)
		initPage(db.scratch, pageLeaf, 0)
		pos := 0
		for i := 0; i < mid; i++ {
			db.scratch.appendRawCell(buf[pos : pos+sizes[i]])
			pos += sizes[i]
		}
		copy(lp, db.scratch)
		initPage(db.scratch, pageLeaf, 0)
		for i := mid; i < len(sizes); i++ {
			db.scratch.appendRawCell(buf[pos : pos+sizes[i]])
			pos += sizes[i]
		}
		copy(rp, db.scratch)
		if db.debugVerify != nil {
			db.debugVerify("after rebuild", lp, rp)
		}
		sepKey := append(db.sepBufs[15][:0], rp.leafKey(0)...)
		db.sepBufs[15] = sepKey
		ppw := db.writable(parent)
		rightID := ppw.child(sepCell + 1)
		ppw.removeCell(sepCell)
		if ppw.freeSpace() < newSepSize {
			ppw.compact(db.scratch)
		}
		ppw.insertInternalCell(sepCell, sepKey, rightID)
		db.balances++
		if db.debugVerify != nil {
			db.debugVerify("after balance", lp, rp)
		}
		return true, nil
	}
	return false, nil
}

func (db *DB) insertInternal(n *node, idx int, sep []byte, right uint32) ([]byte, uint32, bool, error) {
	p := db.writable(n)
	need := internalCellHdr + len(sep) + slotSize
	if p.freeSpace() < need && p.garbage() > 0 {
		p.compact(db.scratch)
	}
	if p.freeSpace() < need {
		lvl := int(p.level())
		rn := db.pager.alloc(pageInternal, p.level())
		promote := p.splitInternal(rn.data, db.scratch, db.sepBufs[lvl])
		db.sepBufs[lvl] = promote
		target := p
		if bytes.Compare(sep, promote) > 0 {
			target = rn.data
		}
		j := target.searchInternal(sep)
		target.insertInternalCell(j, sep, right)
		return promote, rn.id, true, nil
	}
	p.insertInternalCell(idx, sep, right)
	return nil, 0, false, nil
}

// writeSubtree checkpoints every dirty resident page below n, children first
// so parents always point at final ids. Returns n's (possibly new) id.
func (db *DB) writeSubtree(n *node) (uint32, error) {
	if n.data.typ() == pageInternal {
		nc := n.data.numCells()
		for i := 0; i <= nc; i++ {
			cid := n.data.child(i)
			cn := db.pager.loaded(cid)
			if cn == nil {
				continue
			}
			var newID uint32
			var err error
			switch {
			case cn.data.typ() == pageInternal:
				newID, err = db.writeSubtree(cn)
			case cn.dirty:
				if cn.onDisk && len(db.snaps) > 0 {
					db.preserveForSnapshots(cn) // its id is about to move
				}
				newID, err = db.pager.writeNode(cn)
			default:
				continue
			}
			if err != nil {
				return 0, err
			}
			if newID != cid {
				db.writable(n).setChild(i, newID)
			}
		}
	}
	if n.dirty {
		if n.onDisk && len(db.snaps) > 0 {
			db.preserveForSnapshots(n)
		}
		return db.pager.writeNode(n)
	}
	return n.id, nil
}

// deleteTree removes key from the tree if present, releasing its overflow
// chain. An emptied leaf is unlinked from its parent and its page freed; an
// internal node left without children is unlinked the same way, and a root
// with a single child collapses. Writer lock held.
func (db *DB) deleteTree(key []byte) error {
	db.treeGen++
	root, err := db.pager.get(db.root)
	if err != nil {
		return err
	}
	if root.data.typ() == pageLeaf {
		i, found := root.data.searchLeaf(key)
		if found {
			p := db.writable(root)
			if p.leafIsOverflow(i) {
				if err := db.freeOverflow(p.leafVal(i)); err != nil {
					return err
				}
			}
			p.removeCell(i)
		}
		return nil
	}
	if _, err := db.deleteFrom(root, key); err != nil {
		return err
	}
	for root.data.typ() == pageInternal && root.data.numCells() == 0 {
		child, err := db.pager.get(root.data.leftChild())
		if err != nil {
			return err
		}
		db.freePage(root)
		db.root = child.id
		root = child
	}
	return nil
}

// deleteFrom descends from internal node n and reports whether n itself is
// left without any child.
func (db *DB) deleteFrom(n *node, key []byte) (bool, error) {
	idx := n.data.searchInternal(key)
	child, err := db.pager.get(n.data.child(idx))
	if err != nil {
		return false, err
	}
	var emptied bool
	if child.data.typ() == pageLeaf {
		i, found := child.data.searchLeaf(key)
		if !found {
			return false, nil
		}
		cp := db.writable(child)
		if cp.leafIsOverflow(i) {
			if err := db.freeOverflow(cp.leafVal(i)); err != nil {
				return false, err
			}
		}
		cp.removeCell(i)
		emptied = cp.numCells() == 0
		if !emptied && PageSize-cp.freeSpace()-cp.garbage() < PageSize/4 {
			_, err := db.mergeLeaf(n, idx, child)
			return false, err // n keeps at least one child either way
		}
	} else {
		if emptied, err = db.deleteFrom(child, key); err != nil {
			return false, err
		}
	}
	if !emptied {
		return false, nil
	}
	p := db.writable(n)
	if p.numCells() == 0 {
		db.freePage(child)
		return true, nil
	}
	if idx == 0 {
		p.setLeftChild(p.internalChild(0))
		p.removeCell(0)
	} else {
		p.removeCell(idx - 1)
	}
	db.freePage(child)
	return false, nil
}

// mergeLeaf folds an underfull leaf and a neighbour into one page when both
// fit, unlinking the right-hand page from the parent and freeing it.
func (db *DB) mergeLeaf(parent *node, idx int, child *node) (bool, error) {
	pp := parent.data
	usedOf := func(p page) int { return PageSize - p.freeSpace() - p.garbage() }
	for _, side := range [2]int{-1, 1} {
		sidx := idx + side
		if sidx < 0 || sidx > pp.numCells() {
			continue
		}
		sib, err := db.pager.get(pp.child(sidx))
		if err != nil {
			return false, err
		}
		if sib.data.typ() != pageLeaf || usedOf(child.data)+usedOf(sib.data)-hdrSize > PageSize {
			continue
		}
		left, right := child, sib
		if side < 0 {
			left, right = sib, child
		}
		initPage(db.scratch, pageLeaf, 0)
		for _, ln := range [2]*node{left, right} {
			for i := 0; i < ln.data.numCells(); i++ {
				db.scratch.appendRawCell(ln.data.rawCell(i))
			}
		}
		copy(db.writable(left), db.scratch)
		db.writable(parent).removeCell(min(idx, sidx))
		db.freePage(right)
		db.merges++
		return true, nil
	}
	return false, nil
}

// sortedInserter inserts strictly ascending keys, reusing the leaf reached
// for the previous key while the next one still belongs to it, so a run of
// keys costs one descent per leaf rather than one per key. Writer lock held
// for the life of the inserter; it must not survive a release of the lock.
type sortedInserter struct {
	db    *DB
	leaf  *node
	upper []byte // first key past the cached leaf's range; nil: none
	valid bool
}

func (si *sortedInserter) insert(key, val []byte) error {
	db := si.db
	if si.valid && leafCellHdr+len(key)+len(val) <= MaxCellSize && (si.upper == nil || bytes.Compare(key, si.upper) < 0) {
		ok, err := db.tryInsertLeaf(si.leaf, key, val, false)
		if err != nil {
			return err
		}
		if ok {
			db.treeGen++
			return nil
		}
		si.valid = false
	}
	if err := db.insertTree(key, val); err != nil {
		return err
	}
	// cache the leaf holding key and the separator bounding it above
	n, err := db.pager.get(db.root)
	if err != nil {
		return err
	}
	si.upper = si.upper[:0]
	hasUpper := false
	for n.data.typ() == pageInternal {
		p := n.data
		idx := p.searchInternal(key)
		if idx < p.numCells() {
			si.upper = append(si.upper[:0], p.internalKey(idx)...)
			hasUpper = true
		}
		if n, err = db.pager.get(p.child(idx)); err != nil {
			return err
		}
	}
	if !hasUpper {
		si.upper = nil
	}
	si.leaf, si.valid = n, true
	return nil
}
