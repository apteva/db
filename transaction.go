package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
)

var ErrConflict = errors.New("aptevadb: transaction conflicted with a concurrent write")

// Tx is an optimistic serializable read/write transaction. Reads use one
// snapshot plus its staged writes (including computed outputs). A writing
// transaction fails with ErrConflict if any database write committed since
// Begin. The caller may retry the whole transaction. Tx is not goroutine-safe.
// Collections must be created before Begin; DDL is outside transactions.
type Tx struct {
	db            *DB
	snap          *Snapshot
	ctx           context.Context
	batch         *Batch
	pending       map[string]txValue
	dirty, closed bool
}
type txValue struct {
	value   []byte
	deleted bool
}
type TxCollection struct {
	tx *Tx
	c  *Collection
}

func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := db.readSnapshot()
	if err != nil {
		return nil, err
	}
	return &Tx{db: db, snap: s, ctx: ctx, batch: db.NewBatch()}, nil
}

// Update runs fn once and commits on success; it does not retry callbacks.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (tx *Tx) check() error {
	if tx.closed {
		return ErrClosed
	}
	return tx.ctx.Err()
}
func (tx *Tx) Collection(name string) (*TxCollection, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	c, ok := tx.snap.collections[name]
	if !ok {
		return nil, ErrNoSuchCollection
	}
	return &TxCollection{tx: tx, c: c}, nil
}
func (tx *Tx) def() *TxCollection                         { return &TxCollection{tx: tx, c: tx.snap.collections[""]} }
func (tx *Tx) Find(key, dst []byte) ([]byte, bool, error) { return tx.def().Find(key, dst) }
func (tx *Tx) FindJSON(key []byte) ([]byte, bool, error)  { return tx.def().FindJSON(key) }
func (tx *Tx) Insert(key, val []byte) error               { return tx.def().Insert(key, val) }
func (tx *Tx) Upsert(key, val []byte) error               { return tx.def().Upsert(key, val) }
func (tx *Tx) Replace(key, val []byte) error              { return tx.def().Replace(key, val) }
func (tx *Tx) Delete(key []byte) error                    { return tx.def().Delete(key) }
func (tx *Tx) InsertJSON(key, val []byte) error           { return tx.def().InsertJSON(key, val) }
func (tx *Tx) UpsertJSON(key, val []byte) error           { return tx.def().UpsertJSON(key, val) }
func (tx *Tx) ReplaceJSON(key, val []byte) error          { return tx.def().ReplaceJSON(key, val) }
func (tx *Tx) Scan(start, end []byte, fn func([]byte, []byte) bool) error {
	return tx.def().Scan(start, end, fn)
}

func (tx *Tx) refresh() error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.dirty {
		return nil
	}
	db := tx.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	if db.revision != tx.snap.revision {
		return ErrConflict
	}
	// A detached batch is evaluated without logging or changing storage.
	bs := *tx.batch.batchState
	bs.buf = append([]byte(nil), bs.buf...)
	b := &Batch{coll: tx.batch.coll, batchState: &bs}
	if db.computedCount > 0 {
		if err := b.addComputedFieldsLocked(); err != nil {
			return err
		}
	}
	if db.derivedCount > 0 {
		if err := b.addComputedCollectionOpsLocked(); err != nil {
			return err
		}
	}
	if len(b.buf) > MaxTransactionBytes {
		return ErrTransactionTooLarge
	}
	pending := map[string]txValue{}
	for off := 0; off < len(b.buf); {
		n := int(le.Uint32(b.buf[off:]))
		r := b.buf[off+8 : off+8+n]
		kl := int(le.Uint16(r[1:]))
		key := r[3 : 3+kl]
		pending[string(key)] = txValue{append([]byte(nil), r[3+kl:]...), r[0]&flagDelete != 0}
		off += 8 + n
	}
	tx.pending = pending
	tx.dirty = false
	return nil
}
func (c *TxCollection) Find(key, dst []byte) ([]byte, bool, error) {
	if len(key) == 0 || len(key) > MaxKeySize {
		return dst[:0], false, ErrKeySize
	}
	if err := c.tx.refresh(); err != nil {
		return dst[:0], false, err
	}
	if v, ok := c.tx.pending[string(c.c.ikey(nil, key))]; ok {
		if v.deleted {
			return dst[:0], false, nil
		}
		return append(dst[:0], v.value...), true, nil
	}
	return c.c.bound(c.tx.snap).Find(key, dst)
}
func (c *TxCollection) FindJSON(key []byte) ([]byte, bool, error) {
	v, ok, err := c.Find(key, nil)
	if err != nil || !ok {
		return nil, ok, err
	}
	doc, err := DecodeDocument(v)
	if err != nil {
		return nil, true, err
	}
	v, err = json.Marshal(doc)
	return v, true, err
}
func (c *TxCollection) write(key, val []byte, del bool, req writeRequirement) error {
	if err := c.tx.check(); err != nil {
		return err
	}
	if c.c.state.derived != nil {
		return ErrComputedCollectionReadOnly
	}
	if req != 0 {
		_, found, err := c.Find(key, nil)
		if err != nil {
			return err
		}
		if req == requireAbsent && found {
			return ErrKeyExists
		}
		if req == requirePresent && !found {
			return ErrNotFound
		}
	}
	if len(val) > MaxValueSize {
		return ErrValueSize
	}
	if len(c.tx.batch.buf)+len(key)+len(val)+32 > MaxTransactionBytes {
		return ErrTransactionTooLarge
	}
	b := c.tx.batch.With(c.c)
	var err error
	if req != 0 {
		err = b.conditionalAdd(key, val, req)
	} else {
		err = b.add(key, val, del)
	}
	if err == nil {
		c.tx.dirty = true
	}
	return err
}
func (c *TxCollection) Insert(key, val []byte) error { return c.write(key, val, false, requireAbsent) }
func (c *TxCollection) Upsert(key, val []byte) error { return c.write(key, val, false, 0) }
func (c *TxCollection) Replace(key, val []byte) error {
	return c.write(key, val, false, requirePresent)
}
func (c *TxCollection) Delete(key []byte) error { return c.write(key, nil, true, 0) }
func (c *TxCollection) jsonWrite(key, doc []byte, req writeRequirement) error {
	enc, err := EncodeJSON(nil, doc)
	if err != nil {
		return err
	}
	return c.write(key, enc, false, req)
}
func (c *TxCollection) InsertJSON(key, doc []byte) error { return c.jsonWrite(key, doc, requireAbsent) }
func (c *TxCollection) UpsertJSON(key, doc []byte) error { return c.jsonWrite(key, doc, 0) }
func (c *TxCollection) ReplaceJSON(key, doc []byte) error {
	return c.jsonWrite(key, doc, requirePresent)
}
func (c *TxCollection) Scan(start, end []byte, fn func([]byte, []byte) bool) error {
	if err := c.tx.refresh(); err != nil {
		return err
	}
	pending := c.tx.pending
	lo := c.c.ikey(nil, start)
	hi := c.c.end
	if end != nil {
		hi = c.c.ikey(nil, end)
	}
	var keys []string
	for k := range pending {
		if bytes.Compare([]byte(k), lo) >= 0 && (hi == nil || bytes.Compare([]byte(k), hi) < 0) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	i := 0
	stopped := false
	var cancelled error
	yield := func(k, v []byte) bool {
		if cancelled = c.tx.ctx.Err(); cancelled != nil {
			stopped = true
			return false
		}
		stopped = !fn(k[len(c.c.pfx):], v)
		return !stopped
	}
	emit := func(k string) bool { v := pending[k]; return v.deleted || yield([]byte(k), v.value) }
	err := c.tx.db.scan(view{c.tx.db, c.tx.snap}, lo, hi, func(k, v []byte) bool {
		for i < len(keys) && keys[i] < string(k) {
			kk := keys[i]
			i++
			if !emit(kk) {
				return false
			}
		}
		if i < len(keys) && keys[i] == string(k) {
			kk := keys[i]
			i++
			return emit(kk)
		}
		return yield(k, v)
	})
	if err != nil {
		return err
	}
	if cancelled != nil {
		return cancelled
	}
	for !stopped && i < len(keys) {
		kk := keys[i]
		i++
		if !emit(kk) {
			break
		}
	}
	return cancelled
}

// Commit closes the transaction whether it succeeds or fails. Validation,
// constraints, computed outputs and all writes share one writer lock/WAL group.
// CommitContext also observes cancellation of the original Begin context.
func (tx *Tx) CommitContext(ctx context.Context) error {
	if tx.closed {
		return ErrClosed
	}
	ctx = orBackground(ctx)
	combined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(tx.ctx, cancel)
	defer cancel()
	defer stop()
	if err := tx.ctx.Err(); err != nil {
		tx.Rollback()
		return err
	}
	tx.ctx = combined
	return tx.Commit()
}

func (tx *Tx) Commit() error {
	if err := tx.check(); err != nil {
		tx.Rollback()
		return err
	}
	defer tx.Rollback()
	if tx.batch.n == 0 {
		return nil
	}
	db := tx.db
	db.mu.Lock()
	if err := db.maintenanceReadyLocked(tx.ctx); err != nil {
		db.mu.Unlock()
		return err
	}
	if db.revision != tx.snap.revision {
		db.mu.Unlock()
		return ErrConflict
	}
	w := db.wal
	l, err := tx.batch.commitLocked()
	need := db.needCheckpoint()
	db.mu.Unlock()
	if err != nil {
		return err
	}
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

// Rollback discards writes and releases the read snapshot; repeated calls are safe.
func (tx *Tx) Rollback() {
	if !tx.closed {
		tx.closed = true
		tx.snap.Close()
		tx.pending = nil
		tx.batch.reset()
	}
}
