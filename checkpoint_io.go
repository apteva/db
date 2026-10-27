package db

import (
	"sort"
	"syscall"
)

// A capture is immutable and its data pages can be written in any order before
// the data sync and meta publication. Group neighbouring physical ids so a large
// checkpoint does not pay one filesystem syscall per page. Checksums are made
// on the staging copy: snapshot/writer aliases of frozen buffers stay immutable.
func (db *DB) writeCheckpointPages(pages []ioPage) error {
	if len(pages) < 4 {
		if db.checkpointScratch == nil {
			db.checkpointScratch = make(page, PageSize)
		}
		for _, p := range pages {
			data := p.data
			if data.typ() == pageLeaf {
				copy(db.checkpointScratch, data)
				db.checkpointScratch.setCrc(pageCrc(db.checkpointScratch))
				data = db.checkpointScratch
			}
			if err := writeAt(db.pager.f, data, int64(p.id)*PageSize); err != nil {
				return err
			}
		}
		return nil
	}
	ordered := true
	for i := 1; i < len(pages); i++ {
		if pages[i].id < pages[i-1].id {
			ordered = false
			break
		}
	}
	if !ordered {
		sort.Slice(pages, func(i, j int) bool { return pages[i].id < pages[j].id })
	}
	const maxBatchBytes = 1 << 20
	data, err := syscall.Mmap(-1, 0, min(len(pages)*PageSize, maxBatchBytes), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)
	count := 0
	var first uint32
	flush := func() error {
		if count == 0 {
			return nil
		}
		err := writeAt(db.pager.f, data[:count*PageSize], int64(first)*PageSize)
		count = 0
		return err
	}
	for _, p := range pages {
		if count > 0 && (p.id != first+uint32(count) || (count+1)*PageSize > len(data)) {
			if err := flush(); err != nil {
				return err
			}
		}
		if count == 0 {
			first = p.id
		}
		dest := page(data[count*PageSize : (count+1)*PageSize])
		copy(dest, p.data)
		if dest.typ() == pageLeaf {
			dest.setCrc(pageCrc(dest))
		}
		count++
	}
	return flush()
}
