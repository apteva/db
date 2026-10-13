// Package engine is a from-scratch embedded key-value engine: a B+tree over
// fixed-size slotted pages, a write-ahead log with group commit, and
// copy-on-write checkpoints so the data file is never torn.
package db

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

const (
	// PageSize is the fixed on-disk and in-memory page size.
	PageSize = 16384
	// MaxCellSize bounds key+value so a page always holds at least three cells,
	// which the split logic relies on.
	MaxCellSize = PageSize / 4
	// MaxKeySize bounds keys; lengths are stored as uint16.
	MaxKeySize = 1024

	pageLeaf     byte = 1
	pageInternal byte = 2
	pageOverflow byte = 3 // value bytes of a large value; header field numCells = length, leftChild = next
	pageFreelist byte = 4 // free page ids; header field numCells = count, leftChild = next

	// overflowMark in a leaf cell's vlen field means the 8-byte "value" is a
	// pointer: [total length u32][first overflow page u32].
	overflowMark = 0xFFFF
	overflowPtr  = 8
	overflowData = PageSize - hdrSize

	// Page header layout.
	hdrType      = 0  // u8: pageLeaf | pageInternal
	hdrLevel     = 1  // u8: 0 for leaves, 1 for parents of leaves, ...
	hdrNumCells  = 2  // u16
	hdrCellStart = 4  // u16: lowest cell offset; cells grow down from PageSize
	hdrGarbage   = 6  // u16: bytes held by dead cells (leaf only)
	hdrLeftChild = 8  // u32: child 0 (internal only)
	hdrCrc       = 12 // u32: crc32c of the page with this field zeroed
	hdrSize      = 16

	// Slot layout: [cell offset u16][key length u16][key prefix u64 big-endian].
	// The prefix lets binary search run over the compact slot array and touch
	// the cell itself only when the first 8 bytes tie.
	slotSize        = 12
	leafCellHdr     = 4 // klen u16, vlen u16
	internalCellHdr = 6 // klen u16, child u32
)

var le = binary.LittleEndian

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// pageCrc checksums a page, skipping the checksum field itself.
func pageCrc(p page) uint32 {
	c := crc32.Update(0, crcTable, p[:hdrCrc])
	return crc32.Update(c, crcTable, p[hdrCrc+4:])
}

func (p page) crc() uint32     { return le.Uint32(p[hdrCrc:]) }
func (p page) setCrc(c uint32) { le.PutUint32(p[hdrCrc:], c) }

// page is a PageSize-byte slotted page.
//
// Layout: [header][slot array grows up ...][free ...][... cells grow down]
// Slots are kept in key order, so a binary search over slots gives ordered
// access without moving cell bytes.
type page []byte

func initPage(p page, typ byte, level byte) {
	clear(p[:hdrSize])
	p[hdrType] = typ
	p[hdrLevel] = level
	le.PutUint16(p[hdrCellStart:], uint16(PageSize))
}

// prefixOf packs the first 8 bytes of k big-endian, zero padded, so that
// unsigned comparison of prefixes agrees with bytes.Compare whenever they differ.
func prefixOf(k []byte) uint64 {
	if len(k) >= 8 {
		return binary.BigEndian.Uint64(k)
	}
	var b [8]byte
	copy(b[:], k)
	return binary.BigEndian.Uint64(b[:])
}

func (p page) typ() byte             { return p[hdrType] }
func (p page) level() byte           { return p[hdrLevel] }
func (p page) numCells() int         { return int(le.Uint16(p[hdrNumCells:])) }
func (p page) setNumCells(n int)     { le.PutUint16(p[hdrNumCells:], uint16(n)) }
func (p page) cellStart() int        { return int(le.Uint16(p[hdrCellStart:])) }
func (p page) setCellStart(o int)    { le.PutUint16(p[hdrCellStart:], uint16(o)) }
func (p page) garbage() int          { return int(le.Uint16(p[hdrGarbage:])) }
func (p page) setGarbage(g int)      { le.PutUint16(p[hdrGarbage:], uint16(g)) }
func (p page) leftChild() uint32     { return le.Uint32(p[hdrLeftChild:]) }
func (p page) setLeftChild(c uint32) { le.PutUint32(p[hdrLeftChild:], c) }

func (p page) slot(i int) int          { return int(le.Uint16(p[hdrSize+slotSize*i:])) }
func (p page) slotKlen(i int) int      { return int(le.Uint16(p[hdrSize+slotSize*i+2:])) }
func (p page) slotPrefix(i int) uint64 { return le.Uint64(p[hdrSize+slotSize*i+4:]) }
func (p page) setSlot(i, off int, key []byte) {
	s := p[hdrSize+slotSize*i:]
	le.PutUint16(s, uint16(off))
	le.PutUint16(s[2:], uint16(len(key)))
	le.PutUint64(s[4:], prefixOf(key))
}

func (p page) freeSpace() int { return p.cellStart() - hdrSize - slotSize*p.numCells() }

// shiftSlots opens a hole at slot i (n is the current cell count).
func (p page) shiftSlots(i, n int) {
	s := hdrSize + slotSize*i
	copy(p[s+slotSize:hdrSize+slotSize*(n+1)], p[s:hdrSize+slotSize*n])
}

// ---- leaf cells: [klen u16][vlen u16][key][value]

func (p page) leafKey(i int) []byte {
	off := p.slot(i)
	return p[off+leafCellHdr : off+leafCellHdr+p.slotKlen(i)]
}

// leafVlen is the byte length of the value stored in the cell: the inline
// value, or the 8-byte pointer of a spilled one.
func (p page) leafVlen(i int) int {
	v := int(le.Uint16(p[p.slot(i)+2:]))
	if v == overflowMark {
		return overflowPtr
	}
	return v
}

func (p page) leafIsOverflow(i int) bool { return le.Uint16(p[p.slot(i)+2:]) == overflowMark }

// leafVal returns the inline value, or the overflow pointer for a spilled one.
func (p page) leafVal(i int) []byte {
	off := p.slot(i)
	s := off + leafCellHdr + p.slotKlen(i)
	return p[s : s+p.leafVlen(i)]
}

// overflow and freelist pages reuse header fields
func (p page) chainLen() int         { return int(le.Uint16(p[hdrNumCells:])) }
func (p page) setChainLen(n int)     { le.PutUint16(p[hdrNumCells:], uint16(n)) }
func (p page) chainNext() uint32     { return le.Uint32(p[hdrLeftChild:]) }
func (p page) setChainNext(n uint32) { le.PutUint32(p[hdrLeftChild:], n) }

// searchLeaf returns the index of key, or the insertion point if absent.
func (p page) searchLeaf(key []byte) (int, bool) {
	kp := prefixOf(key)
	lo, hi := 0, p.numCells()
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		sp := p.slotPrefix(mid)
		var c int
		if sp < kp {
			c = -1
		} else if sp > kp {
			c = 1
		} else {
			c = bytes.Compare(p.leafKey(mid), key)
		}
		if c < 0 {
			lo = mid + 1
		} else if c > 0 {
			hi = mid
		} else {
			return mid, true
		}
	}
	return lo, false
}

func (p page) insertLeafCell(i int, key, val []byte, overflow bool) {
	n := p.numCells()
	size := leafCellHdr + len(key) + len(val)
	off := p.cellStart() - size
	if off < hdrSize+slotSize*(n+1) {
		panic("aptevadb: page overflow in insertLeafCell")
	}
	le.PutUint16(p[off:], uint16(len(key)))
	if overflow {
		le.PutUint16(p[off+2:], overflowMark)
	} else {
		le.PutUint16(p[off+2:], uint16(len(val)))
	}
	copy(p[off+leafCellHdr:], key)
	copy(p[off+leafCellHdr+len(key):], val)
	p.setCellStart(off)
	p.shiftSlots(i, n)
	p.setSlot(i, off, key)
	p.setNumCells(n + 1)
}

// ---- internal cells: [klen u16][child u32][key]
// Cell i holds separator k_{i+1} and child c_{i+1}; child 0 lives in the header.
// Subtree c_i holds keys with k_i <= key < k_{i+1}.

func (p page) internalKey(i int) []byte {
	off := p.slot(i)
	return p[off+internalCellHdr : off+internalCellHdr+p.slotKlen(i)]
}

func (p page) internalChild(i int) uint32 { return le.Uint32(p[p.slot(i)+2:]) }

func (p page) child(idx int) uint32 {
	if idx == 0 {
		return p.leftChild()
	}
	return p.internalChild(idx - 1)
}

func (p page) setChild(idx int, id uint32) {
	if idx == 0 {
		p.setLeftChild(id)
		return
	}
	le.PutUint32(p[p.slot(idx-1)+2:], id)
}

// searchInternal returns the child index to descend into: the number of
// separator keys <= key.
func (p page) searchInternal(key []byte) int {
	kp := prefixOf(key)
	lo, hi := 0, p.numCells()
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		sp := p.slotPrefix(mid)
		le := sp < kp || (sp == kp && bytes.Compare(p.internalKey(mid), key) <= 0)
		if le {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (p page) insertInternalCell(i int, key []byte, child uint32) {
	n := p.numCells()
	size := internalCellHdr + len(key)
	off := p.cellStart() - size
	if off < hdrSize+slotSize*(n+1) {
		panic("aptevadb: page overflow in insertInternalCell")
	}
	le.PutUint16(p[off:], uint16(len(key)))
	le.PutUint32(p[off+2:], child)
	copy(p[off+internalCellHdr:], key)
	p.setCellStart(off)
	p.shiftSlots(i, n)
	p.setSlot(i, off, key)
	p.setNumCells(n + 1)
}

// ---- generic cell plumbing used by remove/compact/split

func (p page) cellHdr() int {
	if p.typ() == pageLeaf {
		return leafCellHdr
	}
	return internalCellHdr
}

func (p page) cellSize(i int) int {
	klen := p.slotKlen(i)
	if p.typ() == pageLeaf {
		return leafCellHdr + klen + p.leafVlen(i)
	}
	return internalCellHdr + klen
}

func (p page) rawCell(i int) []byte {
	off := p.slot(i)
	return p[off : off+p.cellSize(i)]
}

// appendRawCell adds a cell as the last slot; caller guarantees order and space.
func (p page) appendRawCell(raw []byte) {
	n := p.numCells()
	off := p.cellStart() - len(raw)
	if off < hdrSize+slotSize*(n+1) {
		panic("aptevadb: page overflow in appendRawCell")
	}
	copy(p[off:], raw)
	p.setCellStart(off)
	hdr := p.cellHdr()
	klen := int(le.Uint16(raw))
	p.setSlot(n, off, raw[hdr:hdr+klen])
	p.setNumCells(n + 1)
}

func (p page) removeCell(i int) {
	n := p.numCells()
	off := p.slot(i)
	size := p.cellSize(i)
	copy(p[hdrSize+slotSize*i:], p[hdrSize+slotSize*(i+1):hdrSize+slotSize*n])
	p.setNumCells(n - 1)
	if off == p.cellStart() {
		p.setCellStart(off + size)
	} else {
		p.setGarbage(p.garbage() + size)
	}
}

// compact rewrites p without dead cells. scratch is a spare page buffer.
func (p page) compact(scratch page) {
	n := p.numCells()
	initPage(scratch, p.typ(), p.level())
	scratch.setLeftChild(p.leftChild())
	for i := 0; i < n; i++ {
		scratch.appendRawCell(p.rawCell(i))
	}
	copy(p, scratch)
}

// splitPoint picks the first index of the right half so bytes are balanced.
func (p page) splitPoint(lo, hi int) int {
	n := p.numCells()
	total := 0
	for i := 0; i < n; i++ {
		total += p.cellSize(i)
	}
	mid := hi
	acc := 0
	for i := 0; i < n; i++ {
		acc += p.cellSize(i)
		if acc*2 >= total {
			mid = i + 1
			break
		}
	}
	if mid < lo {
		mid = lo
	}
	if mid > hi {
		mid = hi
	}
	return mid
}

// splitLeaf moves the upper half of p into the fresh leaf r and returns r's
// first key (a slice into r).
func (p page) splitLeaf(r, scratch page) []byte {
	n := p.numCells()
	mid := p.splitPoint(1, n-1)
	for i := mid; i < n; i++ {
		r.appendRawCell(p.rawCell(i))
	}
	initPage(scratch, pageLeaf, 0)
	for i := 0; i < mid; i++ {
		scratch.appendRawCell(p.rawCell(i))
	}
	copy(p, scratch)
	return r.leafKey(0)
}

// splitLeafAt moves cells [at, n) of p into fresh leaf r and returns r's
// first key; 1 <= at <= n-1.
func (p page) splitLeafAt(r, scratch page, at int) []byte {
	n := p.numCells()
	for i := at; i < n; i++ {
		r.appendRawCell(p.rawCell(i))
	}
	initPage(scratch, pageLeaf, 0)
	for i := 0; i < at; i++ {
		scratch.appendRawCell(p.rawCell(i))
	}
	copy(p, scratch)
	return r.leafKey(0)
}

// splitInternal moves the upper part of p into fresh internal page r and
// returns the promoted middle key, copied into dst.
func (p page) splitInternal(r, scratch page, dst []byte) []byte {
	n := p.numCells()
	mid := p.splitPoint(1, n-2)
	promote := append(dst[:0], p.internalKey(mid)...)
	r.setLeftChild(p.internalChild(mid))
	for i := mid + 1; i < n; i++ {
		r.appendRawCell(p.rawCell(i))
	}
	initPage(scratch, pageInternal, p.level())
	scratch.setLeftChild(p.leftChild())
	for i := 0; i < mid; i++ {
		scratch.appendRawCell(p.rawCell(i))
	}
	copy(p, scratch)
	return promote
}
