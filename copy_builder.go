package db

import (
	"fmt"
	"syscall"
)

// copyPageWriter belongs exclusively to a private sorted backup/compaction
// builder. Completed leaves never change again; contiguous ids share one
// write instead of one syscall per page. The final checkpoint syncs these
// writes before publishing metadata. No public view sees staged buffers.
type copyPageWriter struct {
	pg    *pager
	data  []byte
	store []byte
	nodes []*node
}

func (w *copyPageWriter) add(n *node) error {
	if n.onDisk {
		return fmt.Errorf("aptevadb: sorted copy revisited completed leaf %d", n.id)
	}
	if len(w.nodes) > 0 && (n.id != w.nodes[0].id+uint32(len(w.nodes)) || len(w.data)+PageSize > pageBufferArenaBytes) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	if w.data == nil {
		var err error
		w.store, err = syscall.Mmap(-1, 0, pageBufferArenaBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			return err
		}
		w.data = w.store[:0]
		w.nodes = make([]*node, 0, pageBufferArenaBytes/PageSize)
	}
	n.data.setCrc(pageCrc(n.data))
	start := len(w.data)
	w.data = append(w.data, n.data...)
	w.pg.releasePageBuffer(n)
	n.data = page(w.data[start:len(w.data):len(w.data)])
	w.nodes = append(w.nodes, n)
	return nil
}

func (w *copyPageWriter) flush() error {
	if len(w.nodes) == 0 {
		return nil
	}
	if _, err := w.pg.f.WriteAt(w.data, int64(w.nodes[0].id)*PageSize); err != nil {
		return err
	}
	for _, n := range w.nodes {
		w.pg.markVerified(n.id)
		w.pg.writes++
		if n.dirty {
			w.pg.dirtyN--
		}
		n.dirty, n.onDisk, n.mapped = false, true, true
		n.data = w.pg.mappedPage(n.id)
	}
	clear(w.nodes)
	w.nodes, w.data = w.nodes[:0], w.data[:0]
	w.pg.releaseUnusedBuffers()
	return nil
}

func (w *copyPageWriter) close() {
	if w.store != nil {
		syscall.Munmap(w.store)
		w.store, w.data = nil, nil
	}
}
