package db

import (
	"context"
	"errors"
)

var (
	ErrKeyExists           = errors.New("aptevadb: key already exists")
	ErrNotFound            = errors.New("aptevadb: key not found")
	ErrTransactionTooLarge = errors.New("aptevadb: transaction exceeds 256 MiB staging limit")
)

// MaxTransactionBytes bounds staged atomic maintenance and transaction writes.
const MaxTransactionBytes = 256 << 20

type writeRequirement uint8

const (
	requireAbsent writeRequirement = iota + 1
	requirePresent
)

type writeCondition struct {
	offset  int
	require writeRequirement
}

func (c *Collection) conditionalWrite(key, val []byte, req writeRequirement) error {
	b := c.NewBatch()
	b.limit = 0 // single values retain MaxValueSize
	if err := b.conditionalAdd(key, val, req); err != nil {
		return err
	}
	return b.Commit()
}

func (b *Batch) conditionalAdd(key, val []byte, req writeRequirement) error {
	if len(val) > MaxValueSize {
		return ErrValueSize
	}
	off := len(b.buf)
	if err := b.add(key, val, false); err != nil {
		return err
	}
	b.conditions = append(b.conditions, writeCondition{off, req})
	return nil
}

// Insert queues a create. Conditions are checked in operation order at Commit.
func (b *Batch) Insert(key, val []byte) error { return b.conditionalAdd(key, val, requireAbsent) }

// Replace queues a replacement; an absent key fails the entire batch.
func (b *Batch) Replace(key, val []byte) error { return b.conditionalAdd(key, val, requirePresent) }

// InsertJSON queues a new JSON document.
func (b *Batch) InsertJSON(key, doc []byte) error {
	enc, err := EncodeJSON(nil, doc)
	if err != nil {
		return err
	}
	return b.Insert(key, enc)
}

// ReplaceJSON queues a replacement JSON document.
func (b *Batch) ReplaceJSON(key, doc []byte) error {
	enc, err := EncodeJSON(nil, doc)
	if err != nil {
		return err
	}
	return b.Replace(key, enc)
}

func (b *Batch) reset() {
	b.buf, b.n, b.lastOff = b.buf[:0], 0, -1
	b.conditions = b.conditions[:0]
	b.views = b.views[:0]
	b.first = nil
}

func (b *Batch) validateConditionsLocked() error {
	if len(b.conditions) == 0 {
		return nil
	}
	pending := map[string]bool{}
	ci := 0
	for off := 0; off < len(b.buf); {
		plen := int(le.Uint32(b.buf[off:]))
		rec := b.buf[off+8 : off+8+plen]
		klen := int(le.Uint16(rec[1:]))
		key := rec[3 : 3+klen]
		if ci < len(b.conditions) && b.conditions[ci].offset == off {
			exists, seen := pending[string(key)]
			if !seen {
				_, found, err := b.db.findLocked(view{b.db, nil}, key, nil)
				if err != nil {
					return err
				}
				exists = found
			}
			req := b.conditions[ci].require
			ci++
			if req == requireAbsent && exists {
				return ErrKeyExists
			}
			if req == requirePresent && !exists {
				return ErrNotFound
			}
		}
		pending[string(key)] = rec[0]&flagDelete == 0
		off += plen + 8
	}
	return nil
}

func (c *Collection) visibleIndexDefs() []indexDef {
	if c.snap != nil {
		return c.snap.indexes
	}
	var out []indexDef
	for _, d := range c.db.indexDefs() {
		if c.db.idx.touched(d.ID) == nil {
			out = append(out, d)
		}
	}
	return out
}
func (c *Collection) lookupCollection(name string) (*Collection, bool) {
	if c.snap != nil {
		cc, ok := c.snap.collections[name]
		if ok {
			return cc.bound(c.snap), true
		}
		return nil, false
	}
	return c.db.lookupCollection(name)
}

// stageRangeLocked collects tombstones without changing any live data.
func (db *DB) stageRangeLocked(ctx context.Context, b *Batch, lo, hi []byte) error {
	return db.scanLocked(ctx, lo, hi, true, func(k, _ []byte) error {
		if len(b.buf)+len(k)+16 > MaxTransactionBytes {
			return ErrTransactionTooLarge
		}
		return b.add(k, nil, true)
	})
}

func (db *DB) maintenanceReadyLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	db.stallLocked()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	return ctx.Err()
}

// create mutex held by the caller. Publish storage and in-memory catalogs
// under one writer lock; snapshots retain the old catalogs and page contents.
func (db *DB) dropCollectionAtomic(ctx context.Context, c *Collection) error {
	db.mu.Lock()
	if err := db.maintenanceReadyLocked(ctx); err != nil {
		db.mu.Unlock()
		return err
	}
	if err := c.check(); err != nil {
		db.mu.Unlock()
		return err
	}
	if len(c.state.dependents) > 0 {
		db.mu.Unlock()
		return ErrComputedCollectionDependency
	}
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1, limit: MaxTransactionBytes}}
	err := db.stageRangeLocked(ctx, b, c.pfx, c.end)
	defs := c.indexDefs()
	for _, d := range defs {
		if err != nil {
			break
		}
		if db.idx.touched(d.ID) != nil {
			err = ErrIndexBuilding
			break
		}
		prefix := indexPrefix(d.ID)
		err = db.stageRangeLocked(ctx, b, prefix, prefixEnd(prefix))
		if err == nil {
			err = b.add(catalogKey(c.id, d.Name), nil, true)
		}
	}
	if err == nil && c.state.derived != nil {
		prefix := computedViewKey('r', c.id, nil)
		err = db.stageRangeLocked(ctx, b, prefix, prefixEnd(prefix))
		if err == nil {
			err = b.add(computedViewKey('m', c.id, nil), nil, true)
		}
	}
	if err == nil {
		err = b.add(collCatalogKey(c.name), nil, true)
	}
	if err == nil {
		err = b.add(computedCatalogKey(c.id), nil, true)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		db.mu.Unlock()
		return err
	}
	w := db.wal
	l, err := b.commitLocked()
	if err != nil {
		db.failed = err
		db.mu.Unlock()
		return err
	}
	c.state.dropped.Store(true)
	if c.state.computed != nil {
		c.state.computed = nil
		db.computedCount--
	}
	if p := c.state.derived; p != nil {
		src := p.source
		for i, x := range src.state.dependents {
			if x == p {
				src.state.dependents = append(src.state.dependents[:i], src.state.dependents[i+1:]...)
				break
			}
		}
		db.derivedCount--
	}
	db.idx.mu.Lock()
	for _, d := range defs {
		db.idx.defs = withoutDef(db.idx.defs, d.ID)
	}
	db.idx.mu.Unlock()
	db.colls.mu.Lock()
	delete(db.colls.byName, c.name)
	delete(db.colls.byID, c.id)
	db.colls.mu.Unlock()
	need := db.needCheckpoint()
	db.mu.Unlock()
	if db.opts.Sync {
		if err = db.syncWrite(w, l); err != nil {
			return err
		}
	}
	if need {
		db.requestCheckpoint()
	}
	return nil
}

func (c *Collection) dropIndexAtomic(ctx context.Context, name string) error {
	if c.snap != nil {
		return ErrReadOnly
	}
	db := c.db
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	db.mu.Lock()
	if err := db.maintenanceReadyLocked(ctx); err != nil {
		db.mu.Unlock()
		return err
	}
	if err := c.check(); err != nil {
		db.mu.Unlock()
		return err
	}
	var def indexDef
	found := false
	for _, d := range db.indexDefs() {
		if d.Coll == c.id && d.Name == name {
			def, found = d, true
			break
		}
	}
	if !found {
		db.mu.Unlock()
		return ErrNoSuchIndex
	}
	if db.idx.touched(def.ID) != nil {
		db.mu.Unlock()
		return ErrIndexBuilding
	}
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1, limit: MaxTransactionBytes}}
	pfx := indexPrefix(def.ID)
	err := db.stageRangeLocked(ctx, b, pfx, prefixEnd(pfx))
	if err == nil {
		err = b.add(catalogKey(c.id, name), nil, true)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		db.mu.Unlock()
		return err
	}
	w := db.wal
	l, err := b.commitLocked()
	if err != nil {
		db.failed = err
		db.mu.Unlock()
		return err
	}
	db.idx.mu.Lock()
	db.idx.defs = withoutDef(db.idx.defs, def.ID)
	db.idx.mu.Unlock()
	need := db.needCheckpoint()
	db.mu.Unlock()
	if db.opts.Sync {
		if err = db.syncWrite(w, l); err != nil {
			return err
		}
	}
	if need {
		db.requestCheckpoint()
	}
	return nil
}

// readView validates that a live handle belongs to the captured catalog.
func (c *Collection) readView() (*Snapshot, *Collection, error) {
	if err := c.check(); err != nil {
		return nil, nil, err
	}
	s, err := c.db.readSnapshot()
	if err != nil {
		return nil, nil, err
	}
	cc, ok := s.collections[c.name]
	if !ok || cc.id != c.id {
		s.Close()
		return nil, nil, ErrNoSuchCollection
	}
	return s, cc.bound(s), nil
}

func (db *DB) markFailed(err error) {
	if err != nil {
		db.mu.Lock()
		if !db.closed && db.failed == nil {
			db.failed = err
			db.cond.Broadcast()
		}
		db.mu.Unlock()
	}
}
func (db *DB) syncWrite(w *wal, l lsn) error { err := w.sync(l); db.markFailed(err); return err }

// Health reports whether the database can safely continue accepting requests.
func (db *DB) Health() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	db.wal.mu.Lock()
	defer db.wal.mu.Unlock()
	return db.wal.err
}

// OpenCollection returns an existing collection without creating one.
func (db *DB) OpenCollection(name string) (*Collection, error) {
	c, ok := db.lookupCollection(name)
	if !ok {
		return nil, ErrNoSuchCollection
	}
	return c, nil
}
