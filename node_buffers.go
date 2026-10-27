package db

import (
	"syscall"
	"unsafe"
)

// Node descriptors contain mapped page bytes and pointers to arenas rooted in
// the pager's arena lists. They contain no otherwise unowned Go heap pointers.
// Current page-table entries, checkpoint captures and overflow staging own
// references. Retired arenas are reclaimed only at a checkpoint's exclusive
// lock boundary, after ordinary read/write stacks have finished using nodes.
type nodeArena struct {
	data       []byte
	nodes      []node
	next, live int
}

func (pg *pager) newNode(kind byte) *node {
	tail := &pg.nodeTail
	if kind == pageInternal {
		tail = &pg.internalNodeTail
	}
	a := *tail
	if a == nil || a.next == len(a.nodes) {
		data, err := syscall.Mmap(-1, 0, PageSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			panic("aptevadb: cannot allocate page descriptors: " + err.Error())
		}
		a = &nodeArena{data: data, nodes: unsafe.Slice((*node)(unsafe.Pointer(unsafe.SliceData(data))), PageSize/int(unsafe.Sizeof(node{})))}
		if pg.memory != nil {
			pg.memory.nodes.Add(int64(len(data)))
		}
		pg.nodeArenas = append(pg.nodeArenas, a)
		*tail = a
	}
	n := &a.nodes[a.next]
	a.next++
	a.live++
	n.arena = a
	return n
}

func retainNode(n *node) {
	if n.arena != nil {
		n.arena.live++
	}
}
func releaseNode(n *node) {
	if n.arena != nil {
		n.arena.live--
	}
}

func (pg *pager) releaseUnusedNodes() {
	kept := pg.nodeArenas[:0]
	for _, a := range pg.nodeArenas {
		if a.live == 0 && syscall.Munmap(a.data) == nil {
			if pg.memory != nil {
				pg.memory.nodes.Add(-int64(len(a.data)))
				pg.memory.signal()
			}
			if pg.nodeTail == a {
				pg.nodeTail = nil
			}
			if pg.internalNodeTail == a {
				pg.internalNodeTail = nil
			}
			continue
		}
		kept = append(kept, a)
	}
	clear(pg.nodeArenas[len(kept):])
	pg.nodeArenas = kept
}
