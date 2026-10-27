package db

import (
	"runtime"
	"sort"
	"syscall"
	"unsafe"
)

// Discard only recycled file pages, after their checkpoint has committed (or
// their last snapshot has closed). Remapping the same read-only file range
// preserves addresses and contents but drops obsolete resident translations.
// File data remains in the OS cache; live tree pages keep their warm mappings.
func (pg *pager) discardFreePages() {
	pg.discardPages(pg.free)
}

func (pg *pager) discardPages(ids []uint32) {
	const pagesPerMap = minMapBytes / PageSize
	for i := 0; i < len(ids); {
		id := ids[i]
		if pg.discarded[id/64]&(uint64(1)<<(id%64)) != 0 {
			i++
			continue
		}
		lo, hi := id, id+1
		i++
		for i < len(ids) && ids[i]+1 == lo && ids[i]/pagesPerMap == id/pagesPerMap && pg.discarded[ids[i]/64]&(uint64(1)<<(ids[i]%64)) == 0 {
			lo = ids[i]
			i++
		}
		segment := pg.maps[id/pagesPerMap]
		off := int(lo%pagesPerMap) * PageSize
		address := uintptr(unsafe.Pointer(&segment[off]))
		_, _, errno := syscall.Syscall6(syscall.SYS_MMAP, address, uintptr(hi-lo)*PageSize,
			syscall.PROT_READ, syscall.MAP_SHARED|syscall.MAP_FIXED, pg.f.Fd(), uintptr(lo)*PageSize)
		runtime.KeepAlive(pg.f)
		if errno != 0 {
			// POSIX guarantees unchanged mappings for these errors. Other failures
			// may have partially unmapped the range, just like an allocation failure:
			// https://pubs.opengroup.org/onlinepubs/9799919799/functions/mmap.html
			if errno == syscall.EBADF || errno == syscall.EINVAL || errno == syscall.ENOTSUP {
				return
			}
			panic("aptevadb: cannot reclaim page mapping safely: " + errno.Error())
		}
		for page := lo; page < hi; page++ {
			pg.discarded[page/64] |= uint64(1) << (page % 64)
		}
	}
}

// A writable copy supersedes its old mapped bytes when no earlier snapshot
// or in-flight I/O needs them. Keep the file contents and addresses intact,
// but return the old resident mapping promptly rather than holding two copies.
func (pg *pager) reclaimCopiedPages() {
	if len(pg.copiedPages) == 0 {
		return
	}
	sort.Slice(pg.copiedPages, func(i, j int) bool { return pg.copiedPages[i] > pg.copiedPages[j] })
	pg.discardPages(pg.copiedPages)
	pg.copiedPages = pg.copiedPages[:0]
}

// A compaction copy has consumed these leaves. Clear the recycle markers
// because scanning may have faulted a previously discarded mapping back in.
// The caller holds the writer lock; concurrent views keep identical addresses.
func (pg *pager) discardScannedPages(ids []uint32) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for _, id := range ids {
		pg.discarded[id/64] &^= uint64(1) << (id % 64)
	}
	pg.discardPages(ids)
}
