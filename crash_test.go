package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The fault layer models a file system that may lose unsynced writes.
// State is tracked per inode, shared by every descriptor opened on it:
// a sync through any descriptor makes all earlier writes durable, and
// renames keep the inode. On crash, every inode reverts a random suffix
// of its unsynced writes, the first lost write possibly torn.

type undoRec struct {
	off      int64
	prev     []byte // bytes that were there before (short if the write grew the file)
	sizePrev int64
	trunc    bool
}

type inodeLog struct {
	f    *os.File // one descriptor kept open for reverting
	undo []undoRec
}

var errTripped = errors.New("fault: simulated I/O failure")

type faultCtl struct {
	mu      sync.Mutex
	writes  int
	trip    int // fail writes once this many have happened; 0 = never
	tripped bool
	logs    map[uint64]*inodeLog
	fds     []*os.File
	rng     *rand.Rand
}

type faultFile struct {
	*os.File
	ctl *faultCtl
	log *inodeLog
}

func inodeOf(f *os.File) uint64 {
	st, _ := f.Stat()
	return st.Sys().(*syscall.Stat_t).Ino
}

func (c *faultCtl) open(path string) (fileLike, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logs == nil {
		c.logs = map[uint64]*inodeLog{}
	}
	ino := inodeOf(f)
	lg := c.logs[ino]
	if lg == nil {
		keep, _ := os.OpenFile(path, os.O_RDWR, 0)
		lg = &inodeLog{f: keep}
		c.logs[ino] = lg
	}
	c.fds = append(c.fds, f)
	return &faultFile{File: f, ctl: c, log: lg}, nil
}

func (f *faultFile) tripped() bool {
	f.ctl.mu.Lock()
	defer f.ctl.mu.Unlock()
	return f.ctl.tripped
}

func (f *faultFile) size() int64 {
	st, _ := f.File.Stat()
	return st.Size()
}

func (f *faultFile) WriteAt(b []byte, off int64) (int, error) {
	f.ctl.mu.Lock()
	f.ctl.writes++
	if f.ctl.trip > 0 && f.ctl.writes >= f.ctl.trip {
		f.ctl.tripped = true
	}
	tripped := f.ctl.tripped
	if tripped {
		f.ctl.mu.Unlock()
		return 0, errTripped
	}
	prev := make([]byte, len(b))
	n, _ := f.File.ReadAt(prev, off)
	f.log.undo = append(f.log.undo, undoRec{off: off, prev: prev[:n], sizePrev: f.size()})
	f.ctl.mu.Unlock()
	return f.File.WriteAt(b, off)
}

func (f *faultFile) Truncate(size int64) error {
	f.ctl.mu.Lock()
	f.ctl.writes++
	if f.ctl.trip > 0 && f.ctl.writes >= f.ctl.trip {
		f.ctl.tripped = true
	}
	if f.ctl.tripped {
		f.ctl.mu.Unlock()
		return errTripped
	}
	cur := f.size()
	rec := undoRec{off: size, sizePrev: cur, trunc: true}
	if size < cur {
		rec.prev = make([]byte, cur-size)
		f.File.ReadAt(rec.prev, size)
	}
	f.log.undo = append(f.log.undo, rec)
	f.ctl.mu.Unlock()
	return f.File.Truncate(size)
}

// Sync makes everything written to this inode so far durable in the model.
func (f *faultFile) Sync() error {
	f.ctl.mu.Lock()
	defer f.ctl.mu.Unlock()
	if f.ctl.tripped {
		return errTripped
	}
	f.log.undo = f.log.undo[:0]
	return nil
}

// Close closes this descriptor; the inode log keeps its own.
func (f *faultFile) Close() error { return f.File.Close() }

// crash reverts a random suffix of each inode's unsynced writes; the first
// lost write may be torn, its head surviving.
func (c *faultCtl) crash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tripped = true
	for _, lg := range c.logs {
		keep := c.rng.Intn(len(lg.undo) + 1)
		g := lg.f
		for i := len(lg.undo) - 1; i >= keep; i-- {
			u := lg.undo[i]
			if u.trunc {
				g.Truncate(u.sizePrev)
				if len(u.prev) > 0 {
					g.WriteAt(u.prev, u.off)
				}
				continue
			}
			tear := 0
			if i == keep && c.rng.Intn(2) == 0 {
				tear = c.rng.Intn(len(u.prev) + 1)
			}
			if len(u.prev) > tear {
				g.WriteAt(u.prev[tear:], u.off+int64(tear))
			}
			g.Truncate(max(u.sizePrev, u.off+int64(tear)))
		}
		lg.undo = nil
	}
}

// release closes every descriptor the layer holds.
func (c *faultCtl) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lg := range c.logs {
		lg.f.Close()
	}
	for _, f := range c.fds {
		f.Close()
	}
	c.logs, c.fds = nil, nil
}

// TestCrashRecoveryWithFaultInjection runs many rounds of random writes,
// trips the file layer at a random write, crashes, recovers, and checks:
// every write acknowledged before the trip is present (Sync mode), a
// batch in flight is all-or-nothing, the file verifies, and the database
// keeps working afterwards.
func TestCrashRecoveryWithFaultInjection(t *testing.T) {
	dir := t.TempDir()
	ctl := &faultCtl{rng: rand.New(rand.NewSource(7))}
	orig := openFile
	openFile = ctl.open
	defer func() { openFile = orig }()

	rng := rand.New(rand.NewSource(11))
	durable := map[string][]byte{} // key -> value (nil = deleted), acknowledged
	type pending struct {
		keys   []string
		before map[string][]byte
		after  map[string][]byte
	}
	keyOf := func(i int) string { return fmt.Sprintf("k%05d", i) }
	rounds := 60
	if testing.Short() {
		rounds = 15
	}
	if n, _ := strconv.Atoi(os.Getenv("CRASH_ROUNDS")); n > 0 {
		rounds = n
	}
	var history []string
	recoveryCrashes := 0
	note := func(format string, args ...any) { history = append(history, fmt.Sprintf(format, args...)) }
	defer func() {
		if t.Failed() {
			for _, h := range history {
				t.Log(h)
			}
		}
	}()
	for round := 0; round < rounds; round++ {
		ctl.mu.Lock()
		ctl.writes, ctl.tripped = 0, false
		ctl.trip = 1 + rng.Intn(300)
		if round%10 == 5 {
			ctl.trip = 1 + rng.Intn(4) // interrupt recovery itself now and then
		}
		ctl.mu.Unlock()
		opts := Options{Sync: true, SyncMode: SyncFull, LogFlushInterval: time.Hour} // no timer flushes: deterministic
		if round%3 == 1 {
			opts.CacheBytes, opts.MemtableBytes = 64*PageSize, 128<<10
		}
		// recovery itself may be interrupted: then crash again and recover
		db, err := Open(dir, opts)
		for errors.Is(err, errTripped) {
			note("round %d: crash during recovery", round)
			recoveryCrashes++
			ctl.crash()
			ctl.mu.Lock()
			ctl.trip, ctl.tripped = 0, false
			ctl.mu.Unlock()
			db, err = Open(dir, opts)
		}
		if err != nil {
			t.Fatalf("round %d: open after crash failed: %v", round, err)
		}
		note("round %d: open memtable=%v trip=%d stats=%+v", round, opts.MemtableBytes > 0, ctl.trip, db.Stats())
		if err := db.Verify(bg); err != nil {
			t.Fatalf("round %d: verify after recovery: %v", round, err)
		}
		for k, v := range durable {
			got, ok, err := db.Find([]byte(k), nil)
			if err != nil {
				t.Fatalf("round %d: find %s: %v", round, k, err)
			}
			if (v == nil) != !ok || (ok && !bytes.Equal(got, v)) {
				t.Fatalf("round %d: key %s: durable value lost or wrong (ok=%v len=%d want %d)", round, k, ok, len(got), len(v))
			}
		}
		// random work until the file layer trips
		var inflight *pending
		for op := 0; op < 400; op++ {
			kind := rng.Intn(10)
			var err error
			var p pending
			switch {
			case kind < 5: // insert, sometimes large
				k := keyOf(rng.Intn(3000))
				n := 1 + rng.Intn(200)
				if rng.Intn(8) == 0 {
					n = 5_000 + rng.Intn(40_000)
				}
				v := make([]byte, n)
				rng.Read(v)
				p = pending{keys: []string{k}, before: map[string][]byte{k: durable[k]}, after: map[string][]byte{k: v}}
				inflight = &p
				err = db.Upsert([]byte(k), v)
			case kind < 7: // delete
				k := keyOf(rng.Intn(3000))
				p = pending{keys: []string{k}, before: map[string][]byte{k: durable[k]}, after: map[string][]byte{k: nil}}
				inflight = &p
				err = db.Delete([]byte(k))
			case kind < 9: // batch of inserts and deletes
				b := db.NewBatch()
				p = pending{before: map[string][]byte{}, after: map[string][]byte{}}
				for j := 0; j < 1+rng.Intn(20); j++ {
					k := keyOf(rng.Intn(3000))
					p.keys = append(p.keys, k)
					p.before[k] = durable[k]
					if rng.Intn(4) == 0 {
						b.Delete([]byte(k))
						p.after[k] = nil
					} else {
						v := make([]byte, 1+rng.Intn(300))
						rng.Read(v)
						b.Upsert([]byte(k), v)
						p.after[k] = v
					}
				}
				inflight = &p
				err = b.Commit()
			default: // checkpoint, rarely compaction
				inflight = nil
				if rng.Intn(5) == 0 {
					err = db.Compact(bg)
				} else {
					err = db.Checkpoint()
				}
			}
			if inflight != nil {
				note("op %d kind %d keys %v err %v", op, kind, inflight.keys, err)
			} else {
				note("op %d kind %d (checkpoint/compact) err %v", op, kind, err)
			}
			if err == nil {
				if inflight != nil {
					for k, v := range inflight.after {
						durable[k] = v
					}
					inflight = nil
				}
				continue
			}
			if !errors.Is(err, errTripped) && !errors.Is(err, ErrClosed) && !isFailed(err) {
				t.Fatalf("round %d op %d: unexpected error %v", round, op, err)
			}
			break
		}
		note("crash after %d writes; wal before revert: %s", ctl.writes, walRecords(dir))
		ctl.crash()
		db.crash()
		note("wal after revert: %s", walRecords(dir))
		ctl.mu.Lock()
		ctl.trip, ctl.tripped = 0, false // the machine is back: files work again
		ctl.mu.Unlock()
		// the in-flight write is either fully there or fully absent after recovery
		if inflight != nil {
			db2, err := Open(dir, Options{LogFlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("round %d: reopen: %v", round, err)
			}
			state := ""
			for _, k := range inflight.keys {
				got, ok, _ := db2.Find([]byte(k), nil)
				var cur []byte
				if ok {
					cur = got
				}
				isAfter := (inflight.after[k] == nil) == !ok && (!ok || bytes.Equal(cur, inflight.after[k]))
				isBefore := (inflight.before[k] == nil) == !ok && (!ok || bytes.Equal(cur, inflight.before[k]))
				switch {
				case isAfter && isBefore:
				case isAfter:
					if state == "before" {
						t.Fatalf("round %d: batch applied partially", round)
					}
					state = "after"
				case isBefore:
					if state == "after" {
						t.Fatalf("round %d: batch applied partially", round)
					}
					state = "before"
				default:
					t.Fatalf("round %d: key %s has a value that is neither old nor new", round, k)
				}
			}
			note("in-flight resolved as %q; wal now: %s", state, walRecords(dir))
			if state == "after" {
				for k, v := range inflight.after {
					durable[k] = v
				}
			}
			db2.crash()
		}
	}
	t.Logf("%d rounds, %d of them with recovery itself interrupted", rounds, recoveryCrashes)
	// a clean open at the end, without faults, must still verify
	ctl.release()
	openFile = orig
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(bg); err != nil {
		t.Fatal(err)
	}
	db.Close()
	os.RemoveAll(filepath.Join(dir, "compact.tmp"))
}

// walRecords counts intact records in a log file, for diagnostics.
func walRecords(dir string) string {
	w := &wal{dir: dir, crcTab: crcTable}
	gens, _ := walGens(dir)
	n := 0
	lastGen, end, err := w.replayFrom(0, 0, func(k, v []byte, del bool) error { n++; return nil })
	return fmt.Sprintf("gens=%v records=%d lastGen=%d intactEnd=%d err=%v", gens, n, lastGen, end, err)
}

func isFailed(err error) bool {
	return err != nil && (bytes.Contains([]byte(err.Error()), []byte("writes disabled")) || errors.Is(err, errTripped))
}

func TestBackup(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	m := randKV(rand.New(rand.NewSource(121)), 20_000)
	insertAll(t, db, m)
	dst := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(bg, dst); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(bg, dst); err == nil {
		t.Fatal("backup into a non-empty directory must fail")
	}
	more := randKV(rand.New(rand.NewSource(122)), 1_000)
	insertAll(t, db, more) // after the backup: must not be in it
	db.Close()
	b, err := Open(dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Verify(bg); err != nil {
		t.Fatal(err)
	}
	check(t, b, m)
	for k := range more {
		if _, ok, _ := b.Find([]byte(k), nil); ok {
			if _, inM := m[k]; !inM {
				t.Fatalf("backup contains a key written after it")
			}
		}
	}
	b.Close()
}
