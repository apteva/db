package aptevadb

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	walMagic       = "NDBWAL01"
	walHeaderSize  = 16
	walFlushBytes  = 1 << 20
	walRotateBytes = 64 << 20
)

// wal is an append-only log of logical upserts and tombstones, kept as one
// file per generation: wal-<gen>. A checkpoint records (gen, offset) of the
// first record not yet in the tree; once its meta page is durable, older
// generation files are deleted. Rotation never blocks appends for long:
// it opens the next file and switches.
//
// Record: [payloadLen u32][crc32c u32][flags u8][klen u16][key][value].
// flags bit 0 set means "more records follow in the same batch"; replay
// applies a batch only once its final record is intact, so a batch is
// all-or-nothing after a crash. flags bit 1 marks a tombstone.
type wal struct {
	dir    string
	f      fileLike
	crcTab *crc32.Table

	mu      sync.Mutex // guards buf, base, durable, gen, f, inflight
	buf     []byte
	base    uint64 // file offset of buf[0] in the current generation
	durable uint64 // everything below this offset of the current generation is fsynced
	gen     uint64

	// The flusher goroutine writes full buffers so that appenders never
	// wait on a write syscall: it swaps buf out as inflight and writes it
	// without the lock. Synchronous flushes wait for it first.
	inflight   []byte
	inflightAt uint64
	inflightOn *sync.Cond
	spare      []byte
	kick       chan struct{}

	syncing  bool       // an fsync is in progress; its leader holds no lock meanwhile
	syncDone *sync.Cond // broadcast when it finishes: every covered waiter returns at once
	waiters  int        // writers waiting on the fsync in progress
	grouped  bool       // the last fsync had waiters: the next leader lets a group form
	syncFn   func(fileLike) error
	maxFlush time.Duration // longest single flush write, for diagnostics
}

func walPath(dir string, gen uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%08d", gen))
}

// walGens lists existing generation files in ascending order.
func walGens(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var gens []uint64
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "wal-") {
			continue
		}
		g, err := strconv.ParseUint(strings.TrimPrefix(e.Name(), "wal-"), 10, 64)
		if err == nil {
			gens = append(gens, g)
		}
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	return gens, nil
}

// walMaxBuffered bounds memory when the disk falls behind: past it an
// appender writes synchronously (backpressure).
const walMaxBuffered = 64 << 20

func newWAL(dir string) *wal {
	w := &wal{dir: dir, crcTab: crc32.MakeTable(crc32.Castagnoli), buf: make([]byte, 0, walFlushBytes+MaxCellSize+16),
		spare: make([]byte, 0, walFlushBytes+MaxCellSize+16), syncFn: syncFull, kick: make(chan struct{}, 1)}
	w.inflightOn = sync.NewCond(&w.mu)
	w.syncDone = sync.NewCond(&w.mu)
	// both buffers start at their working size: appends never grow them
	w.buf = make([]byte, 0, 2*walFlushBytes)
	w.spare = make([]byte, 0, 2*walFlushBytes)
	return w
}

// prepareGen opens (or creates with a synced header) the file for gen.
// No lock is needed: nothing appends to it yet.
func (w *wal) prepareGen(gen uint64) (fileLike, uint64, error) {
	f, err := openFile(walPath(w.dir, gen))
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	size := uint64(st.Size())
	if size < walHeaderSize {
		var hdr [walHeaderSize]byte
		copy(hdr[:], walMagic)
		le.PutUint64(hdr[8:], gen)
		if _, err := f.WriteAt(hdr[:], 0); err != nil {
			f.Close()
			return nil, 0, err
		}
		if err := w.syncFn(f); err != nil {
			f.Close()
			return nil, 0, err
		}
		size = walHeaderSize
	}
	return f, size, nil
}

// openGenLocked makes a prepared generation current for appends. Caller
// holds w.mu.
func (w *wal) openGenLocked(gen uint64) error {
	f, size, err := w.prepareGen(gen)
	if err != nil {
		return err
	}
	w.switchLocked(f, gen, size)
	return nil
}

func (w *wal) switchLocked(f fileLike, gen, size uint64) {
	if w.f != nil {
		w.f.Close()
	}
	w.f, w.gen = f, gen
	w.base, w.durable = size, size
	w.buf = w.buf[:0]
}

// start makes gen current: a fresh log or continuing after recovery.
func (w *wal) start(gen uint64, size uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.openGenLocked(gen); err != nil {
		return err
	}
	if size > 0 {
		w.base, w.durable = size, size
	}
	return nil
}

// rotate flushes the current generation and switches to the next one. The
// old generation is not synced here: records that must be durable already
// are, and a checkpoint's meta only advances past them once the tree is.
func (w *wal) rotate() error {
	w.mu.Lock()
	next := w.gen + 1
	w.mu.Unlock()
	f, size, err := w.prepareGen(next) // header written and synced without the lock
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil { // also waits for an in-flight write
		f.Close()
		return err
	}
	w.switchLocked(f, next, size)
	return nil
}

// flushAndPosition writes buffered records to the OS and returns the exact
// generation and offset they end at, atomically with respect to appends.
func (w *wal) flushAndPosition() (gen, off uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flushLocked(); err != nil {
		return 0, 0, err
	}
	return w.gen, w.base, nil
}

// position returns the generation and end offset of everything appended.
func (w *wal) position() (gen, off uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen, w.base + uint64(len(w.buf))
}

// size of the current generation file including buffered bytes.
func (w *wal) size() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.base + uint64(len(w.buf))
}

// dropBefore deletes generation files older than gen.
func (w *wal) dropBefore(gen uint64) {
	gens, err := walGens(w.dir)
	if err != nil {
		return
	}
	for _, g := range gens {
		if g < gen {
			os.Remove(walPath(w.dir, g))
		}
	}
}

const (
	flagMore   = 1 // another record of the same batch follows
	flagDelete = 2 // the record is a tombstone; value is empty
)

// encodeRecord appends one framed record to dst.
func encodeRecord(dst []byte, key, val []byte, more, del bool, tab *crc32.Table) []byte {
	start := len(dst)
	var flags byte
	if more {
		flags |= flagMore
	}
	if del {
		flags |= flagDelete
	}
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0, flags, byte(len(key)), byte(len(key)>>8))
	dst = append(dst, key...)
	dst = append(dst, val...)
	payload := dst[start+8:]
	le.PutUint32(dst[start:], uint32(len(payload)))
	le.PutUint32(dst[start+4:], crc32.Checksum(payload, tab))
	return dst
}

// lsn identifies a durability point: generation and end offset.
type lsn struct{ gen, off uint64 }

// append buffers one record and returns its LSN.
func (w *wal) append(key, val []byte, del bool) (lsn, error) {
	w.mu.Lock()
	w.buf = encodeRecord(w.buf, key, val, false, del, w.crcTab)
	l := lsn{w.gen, w.base + uint64(len(w.buf))}
	err := w.afterAppendLocked()
	w.mu.Unlock()
	return l, err
}

// appendRaw buffers pre-encoded records (a batch) and returns the LSN.
func (w *wal) appendRaw(rec []byte) (lsn, error) {
	w.mu.Lock()
	w.buf = append(w.buf, rec...)
	l := lsn{w.gen, w.base + uint64(len(w.buf))}
	err := w.afterAppendLocked()
	w.mu.Unlock()
	return l, err
}

// afterAppendLocked asks the flusher to write a full buffer, or writes it
// here when the flusher has fallen far behind.
func (w *wal) afterAppendLocked() error {
	if len(w.buf) < walFlushBytes {
		return nil
	}
	if len(w.buf) >= walMaxBuffered {
		return w.flushLocked()
	}
	select {
	case w.kick <- struct{}{}:
	default:
	}
	return nil
}

// flushAsync is called by the flusher goroutine: it takes the buffer and
// writes it without holding the lock.
func (w *wal) flushAsync() error {
	w.mu.Lock()
	if w.inflight != nil || len(w.buf) == 0 || w.f == nil {
		w.mu.Unlock()
		return nil
	}
	w.inflight, w.buf = w.buf, w.spare[:0]
	w.inflightAt = w.base
	w.base += uint64(len(w.inflight))
	f := w.f
	w.mu.Unlock()
	t := time.Now()
	_, err := f.WriteAt(w.inflight, int64(w.inflightAt))
	w.mu.Lock()
	if d := time.Since(t); d > w.maxFlush {
		w.maxFlush = d
	}
	w.spare = w.inflight[:0]
	w.inflight = nil
	w.inflightOn.Broadcast()
	w.mu.Unlock()
	return err
}

// flushLocked writes everything buffered, waiting for an in-flight write.
func (w *wal) flushLocked() error {
	for w.inflight != nil {
		w.inflightOn.Wait()
	}
	if len(w.buf) == 0 {
		return nil
	}
	t := time.Now()
	_, err := w.f.WriteAt(w.buf, int64(w.base))
	if d := time.Since(t); d > w.maxFlush {
		w.maxFlush = d
	}
	if err != nil {
		return err
	}
	w.base += uint64(len(w.buf))
	w.buf = w.buf[:0]
	return nil
}

// flush hands buffered records to the OS without fsync.
func (w *wal) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// sync makes every record up to l durable. Concurrent callers share one fsync.
// sync makes everything up to l durable. Group commit: while one fsync
// runs, arriving writers wait on syncDone; when it finishes, every waiter
// whose record it covered returns at once, and one of the others leads the
// next fsync, which covers all records appended during the previous one.
// The leader writes the buffer out without holding the append lock.
func (w *wal) sync(l lsn) error {
	w.mu.Lock()
	for {
		if w.gen > l.gen || w.durable >= l.off {
			w.mu.Unlock()
			return nil // done by an earlier fsync, or rotation synced the older generation
		}
		if !w.syncing {
			break
		}
		w.waiters++
		w.syncDone.Wait()
		w.waiters--
	}
	w.syncing = true
	if w.grouped {
		// concurrent writers: the ones released by the last fsync are
		// appending right now; a short pause lets them join this group
		// instead of each starting the next fsync alone
		w.mu.Unlock()
		time.Sleep(groupCommitWait)
		w.mu.Lock()
	}
	// write out what is buffered, off the lock; an in-flight write of the
	// flusher's is waited for first
	for w.inflight != nil {
		w.inflightOn.Wait()
	}
	var werr error
	if len(w.buf) > 0 {
		w.inflight, w.buf = w.buf, w.spare[:0]
		w.inflightAt = w.base
		w.base += uint64(len(w.inflight))
		f := w.f
		w.mu.Unlock()
		t := time.Now()
		_, werr = f.WriteAt(w.inflight, int64(w.inflightAt))
		w.mu.Lock()
		if d := time.Since(t); d > w.maxFlush {
			w.maxFlush = d
		}
		w.spare = w.inflight[:0]
		w.inflight = nil
		w.inflightOn.Broadcast()
	}
	flushed := w.base
	f := w.f
	w.mu.Unlock()
	err := werr
	if err == nil {
		err = w.syncFn(f)
	}
	w.mu.Lock()
	if err == nil && flushed > w.durable {
		w.durable = flushed
	}
	w.grouped = w.waiters > 0
	w.syncing = false
	w.syncDone.Broadcast()
	w.mu.Unlock()
	return err
}

// groupCommitWait is how long a leader waits for concurrent writers to
// append before it fsyncs, once concurrency has shown itself; a small
// fraction of an fsync, which it lets many more records share.
const groupCommitWait = 150 * time.Microsecond

// replayFrom feeds records from (gen, off) and every later generation to fn.
// It returns the generation and intact end offset of the last file read.
func (w *wal) replayFrom(gen, off uint64, fn func(key, val []byte, del bool) error) (lastGen, end uint64, err error) {
	gens, err := walGens(w.dir)
	if err != nil {
		return 0, 0, err
	}
	lastGen, end = gen, walHeaderSize
	for _, g := range gens {
		if g < gen {
			continue
		}
		start := uint64(walHeaderSize)
		if g == gen {
			start = max(off, walHeaderSize)
		}
		f, err := os.Open(walPath(w.dir, g))
		if err != nil {
			return 0, 0, err
		}
		e, err := w.replayFile(f, start, fn)
		f.Close()
		if err != nil {
			return 0, 0, err
		}
		lastGen, end = g, e
		if e < start {
			end = start
		}
	}
	return lastGen, end, nil
}

// replayFile reads one generation from off to its end and returns the
// offset after the last intact batch. A torn tail simply ends replay.
func (w *wal) replayFile(f *os.File, off uint64, fn func(key, val []byte, del bool) error) (uint64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := uint64(st.Size())
	if off >= size {
		return size, nil
	}
	r := bufio.NewReaderSize(io.NewSectionReader(f, int64(off), int64(size-off)), 1<<20)
	pos := off // end of the last complete batch
	cur := off // end of the last intact record
	var hdr [8]byte
	buf := make([]byte, MaxCellSize+16)
	var pending [][]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break
		}
		plen := int(le.Uint32(hdr[:]))
		crc := le.Uint32(hdr[4:])
		if plen < 3 || plen > MaxValueSize+MaxKeySize+3 {
			break
		}
		if plen > len(buf) {
			buf = make([]byte, plen)
		}
		rec := buf[:plen]
		if _, err := io.ReadFull(r, rec); err != nil {
			break
		}
		if crc32.Checksum(rec, w.crcTab) != crc {
			break
		}
		flags := rec[0]
		klen := int(le.Uint16(rec[1:]))
		if 3+klen > plen {
			break
		}
		cur += 8 + uint64(plen)
		if flags&flagMore != 0 {
			pending = append(pending, append([]byte(nil), rec...))
			continue
		}
		for _, p := range pending {
			k := int(le.Uint16(p[1:]))
			if err := fn(p[3:3+k], p[3+k:], p[0]&flagDelete != 0); err != nil {
				return pos, err
			}
		}
		pending = pending[:0]
		if err := fn(rec[3:3+klen], rec[3+klen:], flags&flagDelete != 0); err != nil {
			return pos, err
		}
		pos = cur
	}
	return pos, nil
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.inflight != nil {
		w.inflightOn.Wait()
	}
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
