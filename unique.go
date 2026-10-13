package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

var ErrUniqueValue = errors.New("aptevadb: unique indexes require finite scalar numbers")

var ErrDuplicateKey = errors.New("aptevadb: duplicate unique index value")

// IndexOptions controls secondary-index constraints. Missing fields are not
// indexed; null is a value. Multikey uniqueness is across documents, not within
// one array. Integers compare exactly; mixed int/float comparisons use
// the engine's numeric semantics. Non-finite floating-point values are refused.
type IndexOptions struct {
	Unique        bool
	NullsDistinct bool
	Sparse        bool
}

// IndexInfo describes a persisted secondary index.
type IndexInfo struct {
	Name, Field   string
	Unique        bool
	Fields        []IndexField
	NullsDistinct bool
	Sparse        bool
	Encoding      uint8
}

func (c *Collection) ListIndexes() []IndexInfo {
	var out []IndexInfo
	for _, d := range c.indexDefs() {
		out = append(out, IndexInfo{Name: d.Name, Field: d.Field, Unique: d.Unique, Fields: append([]IndexField(nil), d.Fields...), NullsDistinct: d.NullsDistinct, Sparse: d.Sparse, Encoding: d.Encoding})
	}
	return out
}
func (db *DB) ListIndexes() []IndexInfo { return db.def.ListIndexes() }
func (c *Collection) CreateUniqueIndex(ctx context.Context, name, field string) error {
	return c.CreateIndex(ctx, name, field, IndexOptions{Unique: true})
}
func (db *DB) CreateUniqueIndex(ctx context.Context, name, field string) error {
	return db.def.CreateUniqueIndex(ctx, name, field)
}

// Build and validate under the writer lock. A failed build has no visible
// definition or entries; the successful definition and backfill share a WAL
// transaction. Large builds fail intact at the explicit staging limit.
func (c *Collection) createUniqueIndex(ctx context.Context, name, field string) error {
	if c.snap != nil {
		return ErrReadOnly
	}
	if !validIndexName(name) || field == "" || !utf8.ValidString(field) {
		return fmt.Errorf("%w: invalid index name or field", ErrPipeline)
	}
	ctx = orBackground(ctx)
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
	for _, d := range db.indexDefs() {
		if d.Coll == c.id && d.Name == name {
			db.mu.Unlock()
			return ErrIndexExists
		}
	}
	db.idx.mu.Lock()
	def := indexDef{ID: db.idx.nextID, Coll: c.id, Name: name, Field: field, Unique: true}
	db.idx.nextID++
	db.idx.mu.Unlock()
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1}}
	owners := map[string][]uniqueOwner{}
	err := db.computedScanLocked(ctx, c.pfx, c.end, func(key, doc []byte) error {
		pk := key[len(c.pfx):]
		var invalid error
		indexedValues(doc, field, func(v value) {
			if invalid != nil {
				return
			}
			ek := indexEntryKey(def.ID, v, pk)
			vk := string(ek[:len(ek)-len(pk)-2])
			if err := checkUniqueValue(v); err != nil {
				invalid = err
				return
			}
			for _, owner := range owners[vk] {
				if owner.pk != string(pk) && compareValues(owner.value, v) == 0 {
					invalid = fmt.Errorf("%w: %s", ErrDuplicateKey, name)
					return
				}
			}
			if len(b.buf)+len(ek)+len(vk)+len(pk)+64 > MaxTransactionBytes {
				invalid = ErrTransactionTooLarge
				return
			}
			owned := v
			owned.s = append([]byte(nil), v.s...)
			owners[vk] = append(owners[vk], uniqueOwner{string(pk), owned})
			invalid = b.add(ek, nil, false)
		})
		return invalid
	})
	if err == nil {
		meta, _ := json.Marshal(def)
		err = b.add(catalogKey(c.id, name), meta, false)
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
	db.idx.defs = withDef(db.idx.defs, def)
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

type uniqueOwner struct {
	pk    string
	value value
}

func checkUniqueValue(v value) error {
	if v.k == kFloat && (math.IsNaN(v.f) || math.IsInf(v.f, 0)) {
		return ErrUniqueValue
	}
	return nil
}

// Validate final documents, not only their approximate numeric index keys.
// Large distinct int64 values may share float64 index buckets. Comparing the
// original scalars preserves exact integer uniqueness, including atomic swaps.
func (db *DB) validateUniqueDocumentsLocked(pending map[string][]byte, defs []indexDef) error {
	hasUnique := false
	for _, d := range defs {
		if d.Unique {
			hasUnique = true
			break
		}
	}
	if !hasUnique {
		return nil
	}
	for _, d := range defs {
		if !d.Unique {
			continue
		}
		if d.Encoding == 2 {
			if err := db.validateExactUniqueLocked(pending, d); err != nil {
				return err
			}
			continue
		}
		owners := map[string][]uniqueOwner{}
		for internal, doc := range pending {
			cid, pk, ok := splitKey([]byte(internal))
			if !ok || cid != d.Coll || doc == nil {
				continue
			}
			var conflict error
			indexedValues(doc, d.Field, func(v value) {
				if conflict != nil {
					return
				}
				if conflict = checkUniqueValue(v); conflict != nil {
					return
				}
				vk := encodeIndexValue(indexPrefix(d.ID), v)
				for _, owner := range owners[string(vk)] {
					if owner.pk != string(pk) && compareValues(owner.value, v) == 0 {
						conflict = fmt.Errorf("%w: %s", ErrDuplicateKey, d.Name)
						return
					}
				}
				owners[string(vk)] = append(owners[string(vk)], uniqueOwner{string(pk), v})
				lo, hi := vk, prefixEnd(vk)
				if v.isNum() && v.num() == 0 {
					lo = legacyZeroPrefix(d.ID)
				}
				conflict = db.scanLocked(context.Background(), lo, hi, true, func(existing, _ []byte) error {
					otherPK := primaryKeyOf(existing)
					if bytes.Equal(otherPK, pk) {
						return nil
					}
					otherKey := append(collPrefix(d.Coll), otherPK...)
					other, changed := pending[string(otherKey)]
					if !changed {
						var err error
						other, _, err = db.findLocked(view{db, nil}, otherKey, nil)
						if err != nil {
							return err
						}
					}
					var duplicate error
					indexedValues(other, d.Field, func(candidate value) {
						if compareValues(candidate, v) == 0 {
							duplicate = fmt.Errorf("%w: %s", ErrDuplicateKey, d.Name)
						}
					})
					return duplicate
				})
			})
			if conflict != nil {
				return conflict
			}
		}
	}
	return nil
}
