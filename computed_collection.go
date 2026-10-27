package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type computedCollectionCatalog struct {
	Version  int             `json:"version"`
	Source   uint32          `json:"source"`
	Pipeline json.RawMessage `json:"pipeline"`
}

// ComputedCollectionInfo describes an immutable, stored collection definition.
type ComputedCollectionInfo struct {
	Source   string          `json:"source"`
	Pipeline json.RawMessage `json:"pipeline"`
}

// CreateComputedCollection creates and atomically backfills a read-only
// collection. The source must exist and cannot itself be computed. A pipeline
// may have leading $match stages and one final $project or $group. Groups allow
// a scalar/null _id and integer $sum / $count / $countNonNull; stored sums
// reject final int64 overflow and floating inputs.
// Inserts, overwrites, deletes and batches maintain stored results and indexes
// in the same WAL transaction. Creation/drop pause writers while staging at
// most 256 MiB. On failure before logging, neither catalog nor rows are added.
func (db *DB) CreateComputedCollection(ctx context.Context, name, source string, pipeline []byte) (*Collection, error) {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == "" || !validCollectionName(name) || name == source {
		return nil, ErrCollectionName
	}
	plan, err := compileComputedCollection(pipeline)
	if err != nil {
		return nil, err
	}
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	if _, ok := db.lookupCollection(name); ok {
		return nil, ErrComputedCollectionExists
	}
	src, ok := db.lookupCollection(source)
	if !ok {
		return nil, ErrNoSuchCollection
	}
	if src.state.derived != nil {
		return nil, fmt.Errorf("%w: computed sources/chains are not supported", ErrComputedCollection)
	}
	db.mu.Lock()
	if db.closed || db.failed != nil {
		err := db.failed
		if db.closed {
			err = ErrClosed
		}
		db.mu.Unlock()
		return nil, err
	}
	if err := db.stallLocked(ctx); err != nil {
		db.mu.Unlock()
		return nil, err
	}
	if db.closed || db.failed != nil {
		err := db.failed
		if db.closed {
			err = ErrClosed
		}
		db.mu.Unlock()
		return nil, err
	}
	if err = src.check(); err != nil {
		db.mu.Unlock()
		return nil, err
	}
	if len(src.state.dependents) >= 64 {
		db.mu.Unlock()
		return nil, fmt.Errorf("%w: at most 64 dependents per source", ErrComputedCollection)
	}
	db.colls.mu.RLock()
	id := db.colls.nextID
	db.colls.mu.RUnlock()
	if id >= maxCollections {
		db.mu.Unlock()
		return nil, ErrTooManyCollections
	}
	target := newCollection(db, name, id)
	plan.source, plan.target = src, target
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1}}
	cat, _ := json.Marshal(struct {
		ID uint32 `json:"id"`
	}{id})
	err = b.add(collCatalogKey(name), cat, false)
	if err == nil {
		data, e := json.Marshal(computedCollectionCatalog{1, src.id, plan.pipeline})
		err = e
		if err == nil {
			err = b.add(computedViewKey('m', id, nil), data, false)
		}
	}
	delta := newComputedCollectionDelta(db, b)
	if err == nil {
		err = db.computedScanLocked(ctx, src.pfx, src.end, func(key, doc []byte) error {
			if err := validateComputedDoc(doc, 0); err != nil {
				return fmt.Errorf("%w: %w", ErrComputedCollection, err)
			}
			_, uk, _ := splitKey(key)
			if err := delta.change(plan, uk, nil, doc); err != nil {
				return err
			}
			return delta.limit()
		})
	}
	if err == nil {
		err = delta.finish(ctx)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		db.mu.Unlock()
		return nil, err
	}
	w := db.wal
	l, err := b.commitLocked()
	if err != nil {
		db.failed = err
		db.mu.Unlock()
		return nil, err
	}
	target.state.derived = plan // immutable, installed before publication
	src.state.dependents = append(src.state.dependents, plan)
	db.derivedCount++
	db.colls.mu.Lock()
	db.colls.byName[name] = target
	db.colls.byID[id] = target
	db.colls.nextID = id + 1
	db.colls.mu.Unlock()
	need := db.needCheckpoint()
	db.mu.Unlock()
	if db.opts.Sync {
		if err = db.syncWrite(w, l); err != nil {
			return nil, err
		}
	}
	if need {
		db.requestCheckpoint()
	}
	return target, nil
}

// ComputedCollection returns a detached definition, or nil for an ordinary
// collection. Snapshot handles read the catalog as it stood at that snapshot.
func (c *Collection) ComputedCollection() (*ComputedCollectionInfo, error) {
	db := c.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed || (c.snap != nil && c.snap.closed) {
		return nil, ErrClosed
	}
	if err := c.check(); err != nil && c.snap == nil {
		return nil, err
	}
	data, found, err := db.findLocked(c.view(), computedViewKey('m', c.id, nil), nil)
	if err != nil || !found {
		return nil, err
	}
	var cat computedCollectionCatalog
	if json.Unmarshal(data, &cat) != nil || cat.Version != 1 {
		return nil, ErrCorrupt
	}
	source, ok := db.collectionByID(cat.Source)
	if !ok { // a held snapshot may outlive both collections' removal
		if c.state.derived == nil {
			return nil, ErrCorrupt
		}
		source = c.state.derived.source
	}
	return &ComputedCollectionInfo{source.name, append(json.RawMessage(nil), cat.Pipeline...)}, nil
}

func (db *DB) loadComputedCollections() error {
	var invalid error
	err := db.scan(view{db, nil}, []byte{reservedPrefix, 'm'}, []byte{reservedPrefix, 'n'}, func(k, v []byte) bool {
		if len(k) != 6 || db.legacyViews {
			invalid = ErrCorrupt
			return false
		}
		target, ok := db.collectionByID(binary.BigEndian.Uint32(k[2:]))
		if !ok || target.id == 0 || target.state.computed != nil {
			invalid = ErrCorrupt
			return false
		}
		var cat computedCollectionCatalog
		if json.Unmarshal(v, &cat) != nil || cat.Version != 1 {
			invalid = ErrCorrupt
			return false
		}
		source, ok := db.collectionByID(cat.Source)
		if !ok || source.id == target.id {
			invalid = ErrCorrupt
			return false
		}
		plan, e := compileComputedCollection(cat.Pipeline)
		if e != nil {
			invalid = fmt.Errorf("%w: %v", ErrCorrupt, e)
			return false
		}
		plan.source, plan.target = source, target
		target.state.derived = plan
		source.state.dependents = append(source.state.dependents, plan)
		db.derivedCount++
		return true
	})
	if err != nil {
		return err
	}
	if invalid != nil {
		return invalid
	}
	for _, c := range db.colls.byID {
		if len(c.state.dependents) > 64 || (c.state.derived != nil && len(c.state.dependents) > 0) {
			return ErrCorrupt
		}
	}
	return nil
}
