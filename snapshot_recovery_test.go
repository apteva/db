package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// A durable tree must survive a crash after writing replacement data pages
// but before publishing their metadata, including after snapshots close.
type snapshotRecoveryGate struct {
	fileLike
	once            sync.Once
	started, resume chan struct{}
}

func (f *snapshotRecoveryGate) WriteAt(p []byte, off int64) (int, error) {
	f.once.Do(func() { close(f.started); <-f.resume })
	return f.fileLike.WriteAt(p, off)
}
func TestSnapshotRetirementBeforeMetaPublication(t *testing.T) {
	for _, duringIO := range []bool{false, true} {
		t.Run(fmt.Sprint(duringIO), func(t *testing.T) { testSnapshotRetirementRecovery(t, duringIO) })
	}
}
func testSnapshotRetirementRecovery(t *testing.T, duringIO bool) {
	lab := newCrashLab(t, 713)
	opts := Options{Sync: true, SyncMode: SyncOS, CheckpointLogBytes: 1 << 30, LogFlushInterval: time.Hour}
	d := lab.open(opts)
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }
	val := bytes.Repeat([]byte{41}, 256)
	for i := 0; i < 3000; i++ {
		if err := d.Upsert(key(i), val); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if duringIO {
		if err := d.Upsert(key(0), []byte("checkpoint")); err != nil {
			t.Fatal(err)
		}
	}
	s, err := d.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var done chan error
	var resume func()
	if duringIO {
		gate := &snapshotRecoveryGate{fileLike: d.pager.f, started: make(chan struct{}), resume: make(chan struct{})}
		var once sync.Once
		resume = func() { once.Do(func() { close(gate.resume) }) }
		defer resume()
		d.pager.f = gate
		done = make(chan error, 1)
		go func() { d.ckptMu.Lock(); err := d.flushPass(true); d.ckptMu.Unlock(); done <- err }()
		select {
		case <-gate.started:
		case <-time.After(10 * time.Second):
			t.Fatal("checkpoint did not reach I/O")
		}
	}
	for i := 0; i < 1700; i++ {
		if err = d.Delete(key(i)); err != nil {
			t.Fatal(err)
		}
	}
	if duringIO {
		resume()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	for i := 9000; i < 12000; i++ {
		if err = d.Upsert(key(i), val); err != nil {
			t.Fatal(err)
		}
	}
	d.mu.Lock()
	cs, err := d.captureLocked(true)
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = d.writeCheckpointPages(cs.pages); err != nil {
		t.Fatal(err)
	}
	if err = d.pager.f.Sync(); err != nil {
		t.Fatal(err)
	}
	// No meta write. All acknowledged writes are in the durable WAL.
	lab.crash(d)
	d = lab.open(opts)
	defer d.Close()
	if err = d.Verify(nil); err != nil {
		t.Fatal(err)
	}
	seen := 0
	err = d.Scan(nil, nil, func(k, v []byte) bool {
		seen++
		if !bytes.Equal(v, val) {
			t.Errorf("wrong recovered value for %q", k)
		}
		return true
	})
	if err != nil || seen != 4300 {
		t.Fatalf("recovered rows=%d err=%v", seen, err)
	}
}
