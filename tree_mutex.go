package db

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const pointReadLanes = 16

// treeMutex keeps ordinary reads on the original RWMutex. A readerGate is
// allocated only when a persistent Reader is requested. With no registered
// handles, writers take primary without scanning point-read counters.
// primary and heldGate also implement the Locker used by db.cond.
type treeMutex struct {
	primary  sync.RWMutex
	gate     atomic.Pointer[readerGate]
	heldGate *readerGate // owned by the writer holding primary
}

type readerGate struct {
	registered atomic.Int64 // changed only while primary is held exclusively
	nextLane   uint64       // likewise; evenly distribute new handles
	pending    writerIntent
	lanes      [pointReadLanes]pointReadLane
	drainMu    sync.Mutex
	drained    *sync.Cond
}

type writerIntent struct {
	atomic.Int64
	_ [128 - unsafe.Sizeof(atomic.Int64{})]byte
}

// Separate counters by 128 bytes, including on Apple silicon.
type pointReadLane struct {
	readers atomic.Int64
	_       [128 - unsafe.Sizeof(atomic.Int64{})]byte
}

func (m *treeMutex) Lock() {
	g := m.gate.Load()
	if g != nil && g.registered.Load() != 0 {
		g.pending.Add(1)
	} else {
		g = nil
	}
	m.primary.Lock()
	// A first Reader may have been registered while this writer waited for
	// primary. Recheck under the lock before accessing protected state.
	if g == nil {
		current := m.gate.Load()
		if current != nil && current.registered.Load() != 0 {
			g = current
			g.pending.Add(1)
		}
	}
	m.heldGate = g
	if g != nil {
		g.drain()
	}
}

func (m *treeMutex) Unlock() {
	if g := m.heldGate; g != nil {
		g.pending.Add(-1)
	}
	m.primary.Unlock()
}
func (m *treeMutex) RLock()   { m.primary.RLock() }
func (m *treeMutex) RUnlock() { m.primary.RUnlock() }

// register requires the writer lock. The gate remains allocated for the
// lifetime of the DB, so a queued writer's announcement cannot be retired.
func (m *treeMutex) register() (*readerGate, *pointReadLane) {
	g := m.gate.Load()
	if g == nil {
		g = new(readerGate)
		g.drained = sync.NewCond(&g.drainMu)
		m.gate.Store(g)
	}
	lane := &g.lanes[g.nextLane&(pointReadLanes-1)]
	g.nextLane++
	g.registered.Add(1)
	return g, lane
}

// A point reader registers BEFORE checking pending. A writer announces
// itself before taking primary, then drains previously admitted point reads.
// Readers that see a pending writer undo registration and keep primary's
// RLock through the lookup, preserving RWMutex scheduling under busy writes.
func (g *readerGate) readLock(m *treeMutex, lane *pointReadLane) *pointReadLane {
	lane.readers.Add(1)
	if g.pending.Load() == 0 {
		return lane
	}
	g.readUnlock(m, lane)
	m.primary.RLock()
	return nil
}

func (g *readerGate) readUnlock(m *treeMutex, lane *pointReadLane) {
	if lane == nil {
		m.primary.RUnlock()
		return
	}
	if lane.readers.Add(-1) == 0 && g.pending.Load() != 0 {
		g.signalDrained()
	}
}

func (g *readerGate) hasReaders() bool {
	for i := range g.lanes {
		if g.lanes[i].readers.Load() != 0 {
			return true
		}
	}
	return false
}
func (g *readerGate) drain() {
	if !g.hasReaders() {
		return
	}
	g.drainMu.Lock()
	for g.hasReaders() {
		g.drained.Wait()
	}
	g.drainMu.Unlock()
}
func (g *readerGate) signalDrained() {
	// Holding drainMu prevents a notification from being lost between the
	// writer's count check and entering Wait. Read-only traffic skips it.
	g.drainMu.Lock()
	g.drained.Signal()
	g.drainMu.Unlock()
}
