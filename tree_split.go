package db

// splitPageInsert includes the incoming cell when choosing a split. Splitting
// the old page first can leave too little space on the insertion side when
// keys approach half a page. Writer lock held; both output pages are writable.
func (db *DB) splitPageInsert(p, r page, key, val []byte, ovf bool, child uint32) []byte {
	internal := p.typ() == pageInternal
	var idx int
	if internal {
		idx = p.searchInternal(key)
	} else {
		idx, _ = p.searchLeaf(key)
	}
	buf, sizes := db.scratch2[:0], db.cellSizes[:0]
	for i := 0; i <= p.numCells(); i++ {
		if i == idx {
			start := len(buf)
			buf = le.AppendUint16(buf, uint16(len(key)))
			if internal {
				buf = le.AppendUint32(buf, child)
			} else if ovf {
				buf = le.AppendUint16(buf, overflowMark)
			} else {
				buf = le.AppendUint16(buf, uint16(len(val)))
			}
			buf = append(buf, key...)
			if !internal {
				buf = append(buf, val...)
			}
			sizes = append(sizes, len(buf)-start)
		}
		if i < p.numCells() {
			cell := p.rawCell(i)
			buf = append(buf, cell...)
			sizes = append(sizes, len(cell))
		}
	}
	db.scratch2, db.cellSizes = buf, sizes
	total := len(buf) + len(sizes)*slotSize
	const capacity = PageSize - hdrSize
	cut, best, left := -1, total, 0
	for i, sz := range sizes {
		if internal {
			right := total - left - sz - slotSize
			if i > 0 && i < len(sizes)-1 && left <= capacity && right <= capacity {
				if diff := max(left, right) - min(left, right); diff < best {
					cut, best = i, diff
				}
			}
		} else if i > 0 && left <= capacity && total-left <= capacity {
			if diff := max(left, total-left) - min(left, total-left); diff < best {
				cut, best = i, diff
			}
		}
		left += sz + slotSize
	}
	if cut < 0 {
		panic("aptevadb: invalid cell size during page split")
	}
	level, leftChild := p.level(), p.leftChild()
	initPage(p, p.typ(), level)
	if internal {
		p.setLeftChild(leftChild)
	}
	initPage(r, p.typ(), level)
	off := 0
	for i, sz := range sizes {
		cell := buf[off : off+sz]
		if internal && i == cut {
			r.setLeftChild(le.Uint32(cell[2:]))
			off += sz
			continue
		}
		target := p
		if i >= cut {
			target = r
		}
		target.appendRawCell(cell)
		off += sz
	}
	if !internal {
		db.sepBufs[0] = append(db.sepBufs[0][:0], r.leafKey(0)...)
		return db.sepBufs[0]
	}
	off = 0
	for _, sz := range sizes[:cut] {
		off += sz
	}
	lvl := int(level)
	db.sepBufs[lvl] = append(db.sepBufs[lvl][:0], buf[off+internalCellHdr:off+sizes[cut]]...)
	return db.sepBufs[lvl]
}
