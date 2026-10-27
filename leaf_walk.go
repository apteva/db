package db

// leafWalk retains the descent between complete leaves. Logical mutations or
// checkpoint publication invalidate cached page bytes; the next step then seeks
// from the saved exclusive key.
// The caller holds the read lock for every method call and checks DB closure.
type leafWalk struct {
	stack [maxTreeDepth]struct {
		p     page
		child int
	}
	depth int
	id    uint32
	gen   uint64
	valid bool
}

func (w *leafWalk) next(v view, key []byte) (page, int, error) {
	if v.db.failed != nil {
		return nil, 0, v.db.failed
	}
	if v.snap != nil && v.snap.closed {
		return nil, 0, ErrClosed
	}
	if w.valid && w.gen == v.db.treeGen {
		return w.advance(v)
	}
	w.depth = 0
	w.id = v.root()
	n, err := v.page(w.id)
	if err != nil {
		return nil, 0, err
	}
	for n.typ() == pageInternal {
		idx := 0
		if key != nil {
			idx = n.searchInternal(key)
		}
		w.stack[w.depth].p, w.stack[w.depth].child = n, idx
		w.depth++
		w.id = n.child(idx)
		n, err = v.page(w.id)
		if err != nil {
			return nil, 0, err
		}
	}
	w.valid, w.gen = true, v.db.treeGen
	i := 0
	if key != nil {
		i, _ = n.searchLeaf(key)
	}
	if i >= n.numCells() {
		return w.advance(v)
	}
	return n, i, nil
}

func (w *leafWalk) advance(v view) (page, int, error) {
	for {
		d := w.depth - 1
		for d >= 0 && w.stack[d].child >= w.stack[d].p.numCells() {
			d--
		}
		if d < 0 {
			return nil, 0, nil
		}
		f := &w.stack[d]
		f.child++
		w.id = f.p.child(f.child)
		w.depth = d + 1
		n, err := v.page(w.id)
		if err != nil {
			return nil, 0, err
		}
		for n.typ() == pageInternal {
			w.stack[w.depth].p, w.stack[w.depth].child = n, 0
			w.depth++
			w.id = n.child(0)
			n, err = v.page(w.id)
			if err != nil {
				return nil, 0, err
			}
		}
		if n.numCells() != 0 {
			return n, 0, nil
		}
	}
}
