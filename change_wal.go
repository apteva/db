package db

import (
	"bytes"
	"errors"
	"hash/crc32"
	"os"
	"time"
)

// Metadata and referenced records share one atomic WAL group. No document
// value is written a second time for the feed.
func (w *wal) appendFeed(raw, marker []byte) (lsn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return lsn{}, w.err
	}
	if !w.heapOnly {
		w.reserveBuffer(len(raw) + len(marker))
	}
	w.buf = append(w.buf, raw...)
	w.buf = append(w.buf, marker...)
	if w.heapOnly {
		w.bufStore = w.buf[:cap(w.buf)]
	}
	l := lsn{w.gen, w.base + uint64(len(w.buf))}
	return l, w.afterAppendLocked()
}

type changeGeneration struct {
	gen, size uint64
	modified  time.Time
}

// initChangeHistory is used during Open before background workers are started.
func (w *wal) initChangeHistory() error {
	gens, err := walGens(w.dir)
	if err != nil {
		return err
	}
	history := make([]changeGeneration, 0, len(gens))
	for _, g := range gens {
		st, err := os.Stat(walPath(w.dir, g))
		if err != nil {
			return err
		}
		history = append(history, changeGeneration{g, uint64(st.Size()), st.ModTime()})
	}
	w.mu.Lock()
	w.changeGens = history
	w.mu.Unlock()
	return nil
}
func (w *wal) changeFloorLocked() lsn {
	if len(w.changeGens) == 0 {
		return lsn{w.gen, walHeaderSize}
	}
	if w.retain == nil {
		return lsn{w.gen, w.base + uint64(len(w.buf))}
	}
	remaining := uint64(w.retain.RetentionBytes)
	floor := lsn{w.changeGens[0].gen, walHeaderSize}
	for i := len(w.changeGens) - 1; i >= 0; i-- {
		g := w.changeGens[i]
		size := g.size
		if g.gen == w.gen {
			size = w.base + uint64(len(w.buf))
		}
		if size > remaining {
			floor = lsn{g.gen, max(uint64(walHeaderSize), size-remaining)}
			break
		}
		remaining -= size
	}
	return floor
}
func (w *wal) changeFloor() lsn { w.mu.Lock(); defer w.mu.Unlock(); return w.changeFloorLocked() }
func (w *wal) changeSize(gen uint64) (uint64, error) {
	w.mu.Lock()
	if gen == w.gen {
		n := w.base + uint64(len(w.buf))
		w.mu.Unlock()
		return n, nil
	}
	w.mu.Unlock()
	st, err := os.Stat(walPath(w.dir, gen))
	if os.IsNotExist(err) {
		return 0, ErrHistoryExpired
	}
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

// readChange reads hot WAL bytes directly. Cold bytes use an independent file
// descriptor; stalled consumers never keep deleted WAL generations open.
func (w *wal) readChange(gen, off uint64, dst []byte) error {
	if len(dst) > 64<<10 {
		// Large reads must not copy documents while holding the append mutex.
		if err := w.prepareLargeChangeRead(gen, off+uint64(len(dst))); err != nil {
			return err
		}
	}

	w.mu.Lock()
	if gen == w.gen {
		end := off + uint64(len(dst))
		if off >= w.base && end <= w.base+uint64(len(w.buf)) {
			copy(dst, w.buf[off-w.base:end-w.base])
			w.mu.Unlock()
			return nil
		}
		if w.inflight != nil && off >= w.inflightAt && end <= w.inflightAt+uint64(len(w.inflight)) {
			copy(dst, w.inflight[off-w.inflightAt:end-w.inflightAt])
			w.mu.Unlock()
			return nil
		}
		// A span can cross flushed/in-flight/current buffers. Flush once only for
		// this uncommon boundary case, rather than forcing I/O for every reader.
		if end > w.base || (w.inflight != nil && end > w.inflightAt) {
			if err := w.flushLocked(); err != nil {
				w.mu.Unlock()
				return err
			}
		}
	}
	w.mu.Unlock()
	file, err := os.Open(walPath(w.dir, gen))
	if os.IsNotExist(err) {
		return ErrHistoryExpired
	}
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.ReadAt(dst, int64(off))
	return err
}
func (w *wal) changeNextGeneration(gen uint64) (uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, g := range w.changeGens {
		if g.gen > gen {
			return g.gen, true
		}
	}
	return 0, false
}

// Retention affects only fully checkpointed generations. A smaller byte/age
// budget can expire logical tokens inside a file before physical reclamation.
func (w *wal) dropChangeBefore(checkpointGen uint64) {
	w.mu.Lock()
	floor := w.changeFloorLocked()
	options := w.retain
	history := append([]changeGeneration(nil), w.changeGens...)
	w.mu.Unlock()
	cutoff := time.Now().Add(-options.RetentionAge)
	removed := map[uint64]bool{}
	for _, g := range history {
		if g.gen < checkpointGen && (g.gen < floor.gen || g.modified.Before(cutoff)) {
			if err := os.Remove(walPath(w.dir, g.gen)); err == nil || os.IsNotExist(err) {
				removed[g.gen] = true
			}
		}
	}
	if len(removed) > 0 {
		w.mu.Lock()
		kept := w.changeGens[:0]
		for _, g := range w.changeGens {
			if !removed[g.gen] {
				kept = append(kept, g)
			}
		}
		w.changeGens = kept
		w.mu.Unlock()
	}
}

// readMarkerAt checks a token against the original persisted commit marker.
func (f *changeFeed) readMarkerAt(at lsn, maxBytes int64, reserve func(*changeFrame, int64) error) (*changeFrame, error) {
	var h [11]byte
	if err := f.db.wal.readChange(at.gen, at.off, h[:]); err != nil {
		return nil, err
	}
	plen := uint64(le.Uint32(h[:4]))
	if plen < 3 || plen > MaxValueSize+MaxIndexKeySize+6 || h[8] != flagFeed {
		return nil, ErrResumeInvalid
	}
	if int64(plen+8) > maxBytes {
		return nil, ErrLimit
	}
	var prefix [changeHeaderBytes]byte
	if err := f.db.wal.readChange(at.gen, at.off+11, prefix[:]); err != nil {
		return nil, err
	}
	charge := changeFrameCharge(int(plen+8), int(le.Uint32(prefix[56:60])))
	if charge > maxBytes {
		return nil, ErrLimit
	}
	if reserve != nil {
		if err := reserve(nil, charge); err != nil {
			return nil, err
		}
	}
	b := make([]byte, int(plen+8))
	copy(b, h[:])
	if err := f.db.wal.readChange(at.gen, at.off+11, b[11:]); err != nil {
		return nil, err
	}
	frame, err := f.parseMarker(b, at)
	if err != nil {
		return nil, err
	}
	if frame.charge > maxBytes {
		return nil, ErrLimit
	}
	// Decoded collection-name strings are in addition to the encoded marker.
	if reserve != nil && frame.charge > charge {
		if err = reserve(nil, frame.charge-charge); err != nil {
			return nil, err
		}
	}
	return frame, nil
}
func (f *changeFeed) nextFrame(pos, ready lsn, maxBytes int64, reserve func(*changeFrame, int64) error) (*changeFrame, error) {
	// Cache frames are immutable and safe to share after releasing the mutex.
	f.mu.Lock()
	if fr := f.pinned[pos]; fr != nil && lsnLE(fr.end, ready) {
		f.mu.Unlock()
		if fr.charge > maxBytes {
			return nil, ErrLimit
		}
		if reserve != nil {
			if err := reserve(fr, fr.charge); err != nil {
				return nil, err
			}
		}
		return fr, nil
	}
	if f.head < len(f.frames) {
		first := f.frames[f.head]
		if !lsnLess(pos, first.start) {
			for lo, hi := f.head, len(f.frames); lo < hi; {
				mid := (lo + hi) / 2
				if lsnLess(f.frames[mid].start, pos) {
					lo = mid + 1
				} else {
					hi = mid
				}
				if lo == hi && lo < len(f.frames) {
					fr := f.frames[lo]
					if lsnLE(fr.end, ready) {
						f.mu.Unlock()
						if fr.charge > maxBytes {
							return nil, ErrLimit
						}
						if reserve != nil {
							if err := reserve(fr, fr.charge); err != nil {
								return nil, err
							}
						}
						return fr, nil
					}
				}
			}
		}
	}
	f.mu.Unlock()
	for lsnLess(pos, ready) {
		size, err := f.db.wal.changeSize(pos.gen)
		if err != nil {
			return nil, err
		}
		limit := size
		if pos.gen == ready.gen {
			limit = min(limit, ready.off)
		}
		if pos.off+11 > limit {
			g, ok := f.db.wal.changeNextGeneration(pos.gen)
			if !ok {
				return nil, nil
			}
			pos = lsn{g, walHeaderSize}
			continue
		}
		var h [11]byte
		if err = f.db.wal.readChange(pos.gen, pos.off, h[:]); err != nil {
			return nil, err
		}
		plen := uint64(le.Uint32(h[:4]))
		if plen < 3 || plen > MaxValueSize+MaxIndexKeySize+6 || pos.off+8+plen > limit {
			return nil, ErrCorrupt
		}
		if h[8] == flagFeed {
			frame, err := f.readMarkerAt(pos, maxBytes, reserve)
			if errors.Is(err, ErrResumeInvalid) {
				pos.off += 8 + plen
				continue
			}
			return frame, err
		}
		pos.off += 8 + plen
	}
	return nil, nil
}

// Mark the durability watermark outside the tree's writer lock. A later fsync
// can make several earlier commits ready without waiting for their callers.
func (w *wal) changeDurable() lsn { w.mu.Lock(); defer w.mu.Unlock(); return lsn{w.gen, w.durable} }

func (w *wal) appendOneFeed(key, val []byte, del bool, marker []byte) (lsn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return lsn{}, w.err
	}
	if w.heapOnly {
		w.buf = encodeRecordHeap(w.buf, key, val, true, del, w.crcTab)
	} else {
		w.reserveBuffer(11 + len(key) + len(val) + len(marker))
		w.buf = encodeRecord(w.buf, key, val, true, del, w.crcTab)
	}
	w.buf = append(w.buf, marker...)
	if w.heapOnly {
		w.bufStore = w.buf[:cap(w.buf)]
	}
	l := lsn{w.gen, w.base + uint64(len(w.buf))}
	return l, w.afterAppendLocked()
}

func (w *wal) prepareLargeChangeRead(gen, end uint64) error {
	for {
		w.mu.Lock()
		if gen != w.gen {
			w.mu.Unlock()
			return nil
		}
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		written := w.base
		if w.inflight != nil {
			written = w.inflightAt
		}
		if end <= written {
			w.mu.Unlock()
			return nil
		}
		if w.inflight != nil {
			w.inflightOn.Wait()
			w.mu.Unlock()
			continue
		}
		if end > w.base+uint64(len(w.buf)) {
			w.mu.Unlock()
			return ErrCorrupt
		}
		w.mu.Unlock()
		if err := w.flushAsync(); err != nil {
			return err
		}
	}
}

// Boundary recovery never allocates an entire old commit's metadata just to
// discover its token, and lowering a capture budget cannot strand old data.
func (f *changeFeed) readMarkerSummary(at lsn) (*changeFrame, error) {
	var prefix [11 + changeHeaderBytes + 4]byte
	if err := f.db.wal.readChange(at.gen, at.off, prefix[:]); err != nil {
		return nil, err
	}
	plen := uint64(le.Uint32(prefix[:4]))
	length := plen + 8
	if plen < 3+changeHeaderBytes+16 || plen > MaxValueSize+MaxIndexKeySize+6 || prefix[8] != flagFeed || le.Uint16(prefix[9:]) != 0 {
		return nil, ErrCorrupt
	}
	p := prefix[11:]
	if string(p[:8]) != changeMagic {
		return nil, ErrCorrupt
	}
	if !bytes.Equal(p[8:24], f.id[:]) {
		return nil, ErrResumeInvalid
	}
	rawLen := le.Uint64(p[48:56])
	if at.off < walHeaderSize || rawLen > at.off-walHeaderSize {
		return nil, ErrCorrupt
	}
	var footer [12]byte
	if err := f.db.wal.readChange(at.gen, at.off+length-12, footer[:]); err != nil {
		return nil, err
	}
	if string(footer[4:]) != changeFooter || uint64(le.Uint32(footer[:4])) != length {
		return nil, ErrCorrupt
	}
	// CRC the whole marker with bounded scratch, including the flag/key header.
	scratch := make([]byte, 64<<10)
	sum := uint32(0)
	for off := uint64(8); off < length; {
		n := min(uint64(len(scratch)), length-off)
		if err := f.db.wal.readChange(at.gen, at.off+off, scratch[:n]); err != nil {
			return nil, err
		}
		sum = crc32.Update(sum, f.db.wal.crcTab, scratch[:n])
		off += n
	}
	if sum != le.Uint32(prefix[4:8]) {
		return nil, ErrCorrupt
	}
	var run [16]byte
	copy(run[:], p[24:40])
	return &changeFrame{start: lsn{at.gen, at.off - rawLen}, marker: at, end: lsn{at.gen, at.off + length}, stamp: int64(le.Uint64(p[40:48])), token: f.token(at, run)}, nil
}
