package db

import (
	"context"
	"errors"
	"fmt"
)

// MaxValueSize bounds a value; larger ones are refused.
const MaxValueSize = 1 << 30

var ErrValueSize = errors.New("aptevadb: value too large")

// spill writes val into a chain of overflow pages and returns the 8-byte
// pointer to store in the leaf cell. Pages are dirty until the next
// checkpoint writes them; they are immutable afterwards. Writer lock held.
func (db *DB) spill(val []byte) ([]byte, error) {
	if len(val) > MaxValueSize {
		return nil, ErrValueSize
	}
	var first, prev *node
	for off := 0; off < len(val) || first == nil; {
		n := db.pager.alloc(pageOverflow, 0)
		chunk := min(overflowData, len(val)-off)
		copy(n.data[hdrSize:], val[off:off+chunk])
		n.data.setChainLen(chunk)
		if prev != nil {
			prev.data.setChainNext(n.id)
			if db.streamCopy {
				if _, err := db.pager.writeNode(prev); err != nil {
					return nil, err
				}
				if off%(pageBufferArenaBytes/PageSize*overflowData) == 0 {
					db.pager.releaseUnusedBuffers(true)
				}
			}
		} else {
			first = n
		}
		db.dirtyOverflow = append(db.dirtyOverflow, n)
		prev = n
		off += chunk
	}
	if db.streamCopy {
		if _, err := db.pager.writeNode(prev); err != nil {
			return nil, err
		}
	}
	ptr := db.ptrBuf[:0]
	ptr = le.AppendUint32(ptr, uint32(len(val)))
	ptr = le.AppendUint32(ptr, first.id)
	db.ptrBuf = ptr
	return ptr, nil
}

// readOverflow appends the value behind ptr to dst.
func (db *DB) readOverflow(ptr, dst []byte) ([]byte, error) {
	total := int(le.Uint32(ptr))
	id := le.Uint32(ptr[4:])
	dst = dst[:0]
	for id != 0 {
		n, err := db.pager.get(id)
		if err != nil {
			return nil, err
		}
		if n.data.typ() != pageOverflow {
			return nil, fmt.Errorf("%w: page %d is not an overflow page", ErrCorrupt, id)
		}
		dst = append(dst, n.data[hdrSize:hdrSize+n.data.chainLen()]...)
		id = n.data.chainNext()
		if len(dst) > total {
			return nil, fmt.Errorf("%w: overflow chain longer than its value", ErrCorrupt)
		}
	}
	if len(dst) != total {
		return nil, fmt.Errorf("%w: overflow chain shorter than its value", ErrCorrupt)
	}
	return dst, nil
}

// freeOverflow releases the chain behind ptr. Writer lock held.
func (db *DB) freeOverflow(ptr []byte) error {
	id := le.Uint32(ptr[4:])
	for id != 0 {
		n, err := db.pager.get(id)
		if err != nil {
			return err
		}
		next := n.data.chainNext()
		db.freePage(n)
		id = next
	}
	return nil
}

// leafValue returns the full value of cell i, reading overflow chains.
func (db *DB) leafValue(p page, i int, dst []byte) ([]byte, error) {
	v := p.leafVal(i)
	if p.leafIsOverflow(i) {
		return db.readOverflow(v, dst)
	}
	return append(dst[:0], v...), nil
}

// writeDirtyOverflow persists overflow pages created since the last
// checkpoint. They were never on disk, so they keep their ids and no leaf
// needs patching. Writer lock held.
func (db *DB) writeDirtyOverflow() error {
	for _, n := range db.dirtyOverflow {
		if db.pager.pages[n.id].Load() != n || !n.dirty {
			continue // freed again before the checkpoint, id may be reused
		}
		if _, err := db.pager.writeNode(n); err != nil {
			return err
		}
	}
	db.dirtyOverflow = db.dirtyOverflow[:0]
	return nil
}

// ---- persisted free list

const idsPerFreelistPage = overflowData / 4

// writeFreelist persists the ids that become free once the coming meta is
// durable, and returns the head page. Pages holding the previous list are
// released into pending first. Writer lock held.
func (db *DB) writeFreelist() (uint32, error) {
	pg := db.pager
	for _, id := range db.freelistPages {
		if n := pg.loaded(id); n != nil {
			db.freePage(n)
		} else if len(db.snaps) > 0 {
			db.quarantineIDs([]uint32{id})
		} else {
			pg.pending = append(pg.pending, id)
		}
	}
	db.freelistPages = db.freelistPages[:0]
	count := len(pg.free) + len(pg.pending)
	need := (count + idsPerFreelistPage - 1) / idsPerFreelistPage
	if need == 0 {
		return 0, nil
	}
	pages := make([]*node, need)
	for i := range pages {
		pages[i] = pg.alloc(pageFreelist, 0)
	}
	ids := make([]uint32, 0, count)
	ids = append(ids, pg.free...)
	ids = append(ids, pg.pending...)
	for i, n := range pages {
		lo := i * idsPerFreelistPage
		hi := min(len(ids), lo+idsPerFreelistPage)
		for j, id := range ids[lo:hi] {
			le.PutUint32(n.data[hdrSize+4*j:], id)
		}
		n.data.setChainLen(hi - lo)
		if i+1 < len(pages) {
			n.data.setChainNext(pages[i+1].id)
		}
		if _, err := pg.writeNode(n); err != nil {
			return 0, err
		}
		db.freelistPages = append(db.freelistPages, n.id)
	}
	return pages[0].id, nil
}

// loadFreelist reads the persisted list. ok is false when it is missing or
// fails validation, in which case the caller rebuilds it by walking.
func (db *DB) loadFreelist(head uint32, hwm uint32) (ok bool, err error) {
	if head == 0 {
		return true, nil
	}
	seen := make([]bool, hwm)
	var free []uint32
	for id := head; id != 0; {
		if id < 2 || id >= hwm || seen[id] {
			return false, nil
		}
		seen[id] = true
		n, err := db.pager.get(id)
		if err != nil {
			if errors.Is(err, ErrCorrupt) {
				return false, nil
			}
			return false, err
		}
		if n.data.typ() != pageFreelist {
			return false, nil
		}
		db.freelistPages = append(db.freelistPages, id)
		cnt := n.data.chainLen()
		if cnt > idsPerFreelistPage {
			return false, nil
		}
		for j := 0; j < cnt; j++ {
			fid := le.Uint32(n.data[hdrSize+4*j:])
			if fid < 2 || fid >= hwm || seen[fid] {
				return false, nil
			}
			seen[fid] = true
			free = append(free, fid)
		}
		id = n.data.chainNext()
	}
	db.pager.free = free
	db.pager.releasePending() // sorts, trims a free tail
	return true, nil
}

// rebuildFreelist marks every reachable page (tree, overflow chains) and
// frees the rest. It reads the whole file; used only when the persisted
// list is unusable.
func (db *DB) rebuildFreelist(hwm uint32) error {
	used := make([]bool, hwm)
	used[0], used[1] = true, true
	if err := db.markAll(db.root, used); err != nil {
		return err
	}
	db.pager.free = db.pager.free[:0]
	for id := hwm - 1; id >= 2; id-- {
		if !used[id] {
			db.pager.free = append(db.pager.free, id)
		}
	}
	db.freelistPages = db.freelistPages[:0]
	return nil
}

func (db *DB) markAll(id uint32, used []bool) error {
	used[id] = true
	n, err := db.pager.get(id)
	if err != nil {
		return err
	}
	p := n.data
	switch p.typ() {
	case pageLeaf:
		for i := 0; i < p.numCells(); i++ {
			if !p.leafIsOverflow(i) {
				continue
			}
			for oid := le.Uint32(p.leafVal(i)[4:]); oid != 0; {
				used[oid] = true
				on, err := db.pager.get(oid)
				if err != nil {
					return err
				}
				oid = on.data.chainNext()
			}
		}
	case pageInternal:
		for i := 0; i <= p.numCells(); i++ {
			if err := db.markAll(p.child(i), used); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: page %d has type %d inside the tree", ErrCorrupt, id, p.typ())
	}
	return nil
}

// Verify checks the whole database: page checksums, key order within and
// across pages, overflow chains, and that no page is both reachable and
// free. It reads every page.
func (db *DB) Verify(ctx context.Context) error {
	ctx = orBackground(ctx)
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	hwm := db.pager.hwm
	used := make([]bool, hwm)
	used[0], used[1] = true, true
	if err := db.verifyNode(ctx, db.root, nil, nil, -1, used); err != nil {
		return err
	}
	for _, id := range db.freelistPages {
		if id >= hwm || used[id] {
			return fmt.Errorf("aptevadb: verify: freelist page %d is invalid or reachable", id)
		}
		used[id] = true
	}
	for _, id := range db.pager.free {
		if id >= hwm || used[id] {
			return fmt.Errorf("aptevadb: verify: free page %d is reachable or out of range", id)
		}
	}
	for _, id := range db.pager.pending {
		if id >= hwm || used[id] {
			return fmt.Errorf("aptevadb: verify: pending page %d is reachable or out of range", id)
		}
	}
	return nil
}

func (db *DB) verifyNode(ctx context.Context, id uint32, lo, hi []byte, level int, used []bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id >= db.pager.hwm || used[id] {
		return fmt.Errorf("aptevadb: verify: page %d out of range or reached twice", id)
	}
	used[id] = true
	n, err := db.pager.get(id)
	if err != nil {
		return err
	}
	p := n.data
	if n.mapped && pageCrc(p) != p.crc() {
		return fmt.Errorf("%w: page %d checksum", ErrCorrupt, id)
	}
	if level >= 0 && int(p.level()) != level {
		return fmt.Errorf("aptevadb: verify: page %d at level %d, expected %d", id, p.level(), level)
	}
	var prev []byte
	switch p.typ() {
	case pageLeaf:
		for i := 0; i < p.numCells(); i++ {
			k := p.leafKey(i)
			if (prev != nil && string(prev) >= string(k)) || (lo != nil && string(k) < string(lo)) || (hi != nil && string(k) >= string(hi)) {
				return fmt.Errorf("aptevadb: verify: leaf %d key %d out of order or outside its range", id, i)
			}
			prev = k
			if p.leafIsOverflow(i) {
				ptr := p.leafVal(i)
				total := int(le.Uint32(ptr))
				got := 0
				for oid := le.Uint32(ptr[4:]); oid != 0; {
					if oid >= db.pager.hwm || used[oid] {
						return fmt.Errorf("aptevadb: verify: overflow page %d out of range or reached twice", oid)
					}
					used[oid] = true
					on, err := db.pager.get(oid)
					if err != nil {
						return err
					}
					if on.data.typ() != pageOverflow {
						return fmt.Errorf("aptevadb: verify: page %d is not an overflow page", oid)
					}
					got += on.data.chainLen()
					oid = on.data.chainNext()
				}
				if got != total {
					return fmt.Errorf("aptevadb: verify: overflow chain of leaf %d cell %d holds %d bytes, expected %d", id, i, got, total)
				}
			}
		}
	case pageInternal:
		childLo := lo
		for i := 0; i <= p.numCells(); i++ {
			var childHi []byte
			if i < p.numCells() {
				childHi = p.internalKey(i)
				if (prev != nil && string(prev) >= string(childHi)) || (lo != nil && string(childHi) < string(lo)) || (hi != nil && string(childHi) >= string(hi)) {
					return fmt.Errorf("aptevadb: verify: internal %d separator %d out of order", id, i)
				}
				prev = childHi
			} else {
				childHi = hi
			}
			if err := db.verifyNode(ctx, p.child(i), childLo, childHi, int(p.level())-1, used); err != nil {
				return err
			}
			childLo = childHi
		}
	default:
		return fmt.Errorf("aptevadb: verify: page %d has type %d inside the tree", id, p.typ())
	}
	return nil
}
