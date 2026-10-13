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
		data, err := syscall.Mmap(-1, 0, pageBufferArenaBytes,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			panic("aptevadb: cannot allocate writable page arena: " + err.Error())
		}
		a = &pageBufferArena{data: data}
		pg.bufferArenas = append(pg.bufferArenas, a)
		pg.bufferTail = a
	}
	start := a.next
	a.next += PageSize
	a.live++
	return page(a.data[start:a.next:a.next]), a
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
	if pg.limit > 0 || n.kind == pageInternal {
		buf = make(page, PageSize)
	} else {
		buf, a = pg.allocPageBuffer()
	}
	copy(buf, n.data)
	pg.releasePageBuffer(n)
	n.data, n.buffer = buf, a
}

// releaseUnusedBuffers runs under the writer lock. Explicit references keep
// current nodes, snapshot overlays and captured I/O alive. File mappings may
// be released only after a committed checkpoint with no snapshots, once
// ensureMapped has repointed resident mapped nodes.
func (pg *pager) releaseUnusedBuffers(releaseMappings bool) {
	kept := pg.bufferArenas[:0]
	for _, a := range pg.bufferArenas {
		if a.live == 0 && syscall.Munmap(a.data) == nil {
			if pg.bufferTail == a {
				pg.bufferTail = nil
			}
			continue
		}
		kept = append(kept, a)
	}
	clear(pg.bufferArenas[len(kept):])
	pg.bufferArenas = kept
	if !releaseMappings {
		return
	}
	old := pg.oldMaps[:0]
	for _, m := range pg.oldMaps {
		if syscall.Munmap(m) != nil {
			old = append(old, m)
		}
	}
	clear(pg.oldMaps[len(old):])
	pg.oldMaps = old
}
