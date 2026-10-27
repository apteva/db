package db

import "syscall"

// Writable pages are allocated in anonymous, zero-filled arenas. A checkpoint
// can return an unused arena to the OS immediately instead of retaining a
// second tree's worth of obsolete Go heap pages until a future GC/scavenge.
// Allocation is sequential: a released slot is never overwritten while a
// frozen checkpoint or snapshot may still hold its bytes.
const pageBufferArenaBytes = 4 << 20

type pageBufferArena struct {
	data []byte
	next int
	live int // references from current nodes, snapshots and checkpoint captures
}

// All accounting and allocations happen under the database writer lock.
// Capture and snapshot aliases retain an explicit reference. Reclamation
// under the writer lock therefore cannot invalidate an active read or I/O.
func (pg *pager) allocPageBuffer() (page, *pageBufferArena) {
	a := pg.bufferTail
	if a == nil || a.next+PageSize > len(a.data) {
		size := pageBufferArenaBytes
		if pg.limit > 0 {
			size = 256 << 10
		}
		data, err := syscall.Mmap(-1, 0, size,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			panic("aptevadb: cannot allocate writable page arena: " + err.Error())
		}
		a = &pageBufferArena{data: data}
		if pg.memory != nil {
			pg.memory.pages.Add(int64(len(data)))
		}
		pg.bufferArenas = append(pg.bufferArenas, a)
		pg.bufferTail = a
	}
	start := a.next
	a.next += PageSize
	a.live++
	return page(a.data[start:a.next:a.next]), a
}

// Sparse parents use one-page mappings so a retained parent cannot pin a
// mostly empty leaf arena. They still share the same explicit view/I/O leases.
func (pg *pager) allocInternalPageBuffer() (page, *pageBufferArena) {
	data, err := syscall.Mmap(-1, 0, PageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		panic("aptevadb: cannot allocate internal page buffer: " + err.Error())
	}
	a := &pageBufferArena{data: data, next: PageSize, live: 1}
	if pg.memory != nil {
		pg.memory.pages.Add(int64(len(data)))
	}
	pg.bufferArenas = append(pg.bufferArenas, a)
	return page(data), a
}

func (pg *pager) releasePageBuffer(n *node) {
	if n.buffer != nil {
		n.buffer.live--
		n.buffer = nil
	}
}

func (pg *pager) copyPageBuffer(n *node) {
	var buf page
	var a *pageBufferArena
	if n.pageType() == pageInternal {
		buf, a = pg.allocInternalPageBuffer()
	} else {
		buf, a = pg.allocPageBuffer()
	}
	copy(buf, n.data)
	pg.releasePageBuffer(n)
	n.data, n.buffer = buf, a
}

// releaseUnusedBuffers runs under the writer lock. Explicit references keep
// current nodes, snapshot overlays and captured I/O alive.
func (pg *pager) releaseUnusedBuffers() {
	kept := pg.bufferArenas[:0]
	for _, a := range pg.bufferArenas {
		if a.live == 0 && syscall.Munmap(a.data) == nil {
			if pg.memory != nil {
				pg.memory.pages.Add(-int64(len(a.data)))
				pg.memory.signal()
			}
			if pg.bufferTail == a {
				pg.bufferTail = nil
			}
			continue
		}
		kept = append(kept, a)
	}
	clear(pg.bufferArenas[len(kept):])
	pg.bufferArenas = kept
}
