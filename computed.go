package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type computedCatalog struct {
	Version int                        `json:"version"`
	Fields  map[string]json.RawMessage `json:"fields"`
}

func computedCatalogKey(id uint32) []byte {
	k := []byte{reservedPrefix, 'g', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(k[2:], id)
	return k
}

// DefineComputedFields installs or replaces the default collection's stored
// generated fields. See Collection.DefineComputedFields.
func (db *DB) DefineComputedFields(ctx context.Context, definitions []byte) error {
	return db.def.DefineComputedFields(ctx, definitions)
}

// DefineComputedFields installs a JSON object mapping output names to
// deterministic expressions, for example:
//
//	{"total":{"$multiply":["$qty","$price"]},"email_key":{"$lower":"$email"}}
//
// The collection must be empty. Pass {} to remove its definitions. Outputs
// are top-level scalars, may depend on other computed fields, and overwrite
// any supplied values. Missing/null inputs propagate null (use $ifNull for
// defaults). Integer arithmetic checks overflow; $divide produces a float.
// $lower/$upper use ASCII casing. Definitions are versioned and persisted.
// All upserts, including binary Insert and cross-collection batches, compute
// their outputs before logging; reads do no additional work.
func (c *Collection) DefineComputedFields(ctx context.Context, definitions []byte) error {
	if c.state.derived != nil {
		return ErrComputedCollectionReadOnly
	}
	if c.snap != nil {
		return ErrReadOnly
	}
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := compileComputedFields(definitions)
	if err != nil {
		return err
	}
	var encoded []byte
	if plan != nil {
		encoded, err = json.Marshal(computedCatalog{Version: 1, Fields: plan.defs})
		if err != nil {
			return err
		}
	}
	db := c.db
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		err = db.failed
		db.mu.Unlock()
		return err
	}
	if err = c.check(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.stallLocked()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		err = db.failed
		db.mu.Unlock()
		return err
	}
	if err = c.check(); err != nil {
		db.mu.Unlock()
		return err
	}
	if err = ctx.Err(); err != nil {
		db.mu.Unlock()
		return err
	}
	empty, err := c.computedEmptyLocked(ctx)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if !empty {
		db.mu.Unlock()
		return ErrComputedNonEmpty
	}
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1}}
	if err = b.add(computedCatalogKey(c.id), encoded, plan == nil); err != nil {
		db.mu.Unlock()
		return err
	}
	w := db.wal
	l, err := b.commitLocked()
	if err != nil {
		// A log record may already exist; refuse further writes rather than
		// allow the in-memory plan and persistent catalog to disagree.
		db.failed = err
		db.mu.Unlock()
		return err
	}
	if c.state.computed != nil {
		db.computedCount--
	}
	c.state.computed = plan
	if plan != nil {
		db.computedCount++
	}
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

// ComputedFields returns a detached copy of the default collection's
// expression definitions.
func (db *DB) ComputedFields() (map[string]json.RawMessage, error) { return db.def.ComputedFields() }

// ComputedFields returns the default collection's definitions at the snapshot.
func (s *Snapshot) ComputedFields() (map[string]json.RawMessage, error) {
	return s.def().ComputedFields()
}

// ComputedFields reads definitions through this collection's live/snapshot
// view. Changing the returned map or its bytes cannot change stored rules.
func (c *Collection) ComputedFields() (map[string]json.RawMessage, error) {
	db := c.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed || (c.snap != nil && c.snap.closed) {
		return nil, ErrClosed
	}
	if err := c.check(); err != nil && c.snap == nil {
		return nil, err
	}
	data, found, err := db.findLocked(c.view(), computedCatalogKey(c.id), nil)
	if err != nil {
		return nil, err
	}
	if !found {
		return map[string]json.RawMessage{}, nil
	}
	var cat computedCatalog
	if json.Unmarshal(data, &cat) != nil || cat.Version != 1 || len(cat.Fields) == 0 {
		return nil, ErrCorrupt
	}
	return cat.Fields, nil
}

func (db *DB) loadComputedFields() error {
	lo, hi := []byte{reservedPrefix, 'g'}, []byte{reservedPrefix, 'h'}
	var invalid error
	err := db.scan(view{db, nil}, lo, hi, func(k, v []byte) bool {
		if len(k) != 6 {
			invalid = ErrCorrupt
			return false
		}
		c, ok := db.collectionByID(binary.BigEndian.Uint32(k[2:]))
		var cat computedCatalog
		if !ok || json.Unmarshal(v, &cat) != nil || cat.Version != 1 || len(cat.Fields) == 0 || db.legacyFormat {
			invalid = ErrCorrupt
			return false
		}
		defs, err := json.Marshal(cat.Fields)
		if err != nil {
			invalid = err
			return false
		}
		p, err := compileComputedFields(defs)
		if err != nil {
			invalid = fmt.Errorf("%w: computed catalog: %v", ErrCorrupt, err)
			return false
		}
		c.state.computed = p
		db.computedCount++
		return true
	})
	if err != nil {
		return err
	}
	return invalid
}

// Writer lock held. Check buffered keys and tree keys without scanning
// other collections or allocating a full collection snapshot.
func (c *Collection) computedEmptyLocked(ctx context.Context) (bool, error) {
	db := c.db
	v := view{db, nil}
	for _, mt := range [2]*memtable{db.mt, db.imm} {
		if mt == nil {
			continue
		}
		node, i := mt.seek(string(c.pfx))
		for node != nil {
			for ; i < len(node.keys); i++ {
				k := []byte(node.keys[i])
				if c.end != nil && bytes.Compare(k, c.end) >= 0 {
					node = nil
					break
				}
				if _, del, ok := v.memGet(k); ok && !del {
					return false, nil
				}
				if err := ctx.Err(); err != nil {
					return false, err
				}
			}
			if node != nil {
				node = node.next
				i = 0
			}
		}
	}
	cur := c.pfx
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		k, err := db.computedTreeKeyLocked(cur, c.end)
		if err != nil {
			return false, err
		}
		if k == nil {
			return true, nil
		}
		if _, del, ok := v.memGet(k); !ok || !del {
			return false, nil
		}
		cur = append(append([]byte(nil), k...), 0)
	}
}

func (db *DB) computedTreeKeyLocked(start, end []byte) ([]byte, error) {
	v := view{db, nil}
	type frame struct {
		p page
		i int
	}
	var stack [16]frame
	depth := 0
	p, err := v.page(v.root())
	if err != nil {
		return nil, err
	}
	for p.typ() == pageInternal {
		i := p.searchInternal(start)
		if depth == len(stack) {
			return nil, ErrCorrupt
		}
		stack[depth] = frame{p, i}
		depth++
		p, err = v.page(p.child(i))
		if err != nil {
			return nil, err
		}
	}
	i, _ := p.searchLeaf(start)
	for i >= p.numCells() {
		d := depth - 1
		for d >= 0 && stack[d].i >= stack[d].p.numCells() {
			d--
		}
		if d < 0 {
			return nil, nil
		}
		stack[d].i++
		p, err = v.page(stack[d].p.child(stack[d].i))
		if err != nil {
			return nil, err
		}
		depth = d + 1
		for p.typ() == pageInternal {
			if depth == len(stack) {
				return nil, ErrCorrupt
			}
			stack[depth] = frame{p, 0}
			depth++
			p, err = v.page(p.child(0))
			if err != nil {
				return nil, err
			}
		}
		i = 0
	}
	k := p.leafKey(i)
	if end != nil && bytes.Compare(k, end) >= 0 {
		return nil, nil
	}
	return k, nil
}

// Transform before index maintenance and before appendRaw. An evaluation
// failure therefore aborts the entire batch, across all collections.
func (b *Batch) addComputedFieldsLocked() error {
	var out []byte
	var scratch []byte
	lastOff := -1
	for off := 0; off < len(b.buf); {
		plen := int(le.Uint32(b.buf[off:]))
		end := off + 8 + plen
		rec := b.buf[off+8 : end]
		klen := int(le.Uint16(rec[1:]))
		key, val := rec[3:3+klen], rec[3+klen:]
		changed := false
		if rec[0]&flagDelete == 0 {
			if id, _, ok := splitKey(key); ok {
				c, ok := b.db.collectionByID(id)
				if !ok {
					return ErrNoSuchCollection
				}
				if p := c.state.computed; p != nil {
					var err error
					val, err = p.applyInto(val, scratch)
					if err != nil {
						return err
					}
					scratch = val
					changed = true
				}
			}
		}
		if changed && out == nil {
			out = append(make([]byte, 0, len(b.buf)), b.buf[:off]...)
		}
		if out != nil {
			lastOff = len(out)
			if changed {
				out = encodeRecord(out, key, val, true, false, b.db.wal.crcTab)
			} else {
				out = append(out, b.buf[off:end]...)
			}
		}
		off = end
	}
	if out != nil {
		b.buf, b.lastOff = out, lastOff
	}
	return nil
}
