package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
)

// Collections are key ranges of the one B+tree. Every user key carries a
// collection tag, so a collection scan is a range scan, a cross-collection
// batch is one log record group, and the pager, log, checkpoints, snapshots
// and compaction know nothing about collections.
//
//	default collection (the DB methods): 0x01 <key>
//	collection id 1..253:                <0x01+id> <key>
//	collection id 254..65535:            0xFF <id u16 BE> <key>
//	catalog:                             \x00 n <name> -> {"id":n}
//
// The single-byte tag keeps a named collection's keys exactly as long as the
// default collection's, so the eight-byte slot prefix that drives leaf
// search covers the same seven key bytes.
const (
	maxSingleByteColl = 0xFE - 0x01 // ids up to this use one tag byte
	collEscape        = 0xFF        // tag of the three-byte form
	maxCollections    = 1 << 16
)

var (
	ErrNoSuchCollection   = errors.New("aptevadb: no such collection")
	ErrCollectionName     = errors.New("aptevadb: bad collection name")
	ErrTooManyCollections = errors.New("aptevadb: too many collections")
	ErrReadOnly           = errors.New("aptevadb: a snapshot is read-only")
)

// Collection is a named key space of a DB. The zero-cost default
// collection is what the DB's own Insert/Find/Scan/Aggregate methods use.
// A Collection obtained from a Snapshot reads through that snapshot.
type Collection struct {
	db    *DB
	snap  *Snapshot // nil: live reads
	name  string
	id    uint32
	pfx   []byte // internal key prefix
	end   []byte // first internal key past the collection; nil: none
	state *collState
}

type collState struct {
	dropped    atomic.Bool
	computed   *computedPlan             // protected by db.mu; shared by bound handles
	derived    *computedCollectionPlan   // immutable after handle publication
	dependents []*computedCollectionPlan // protected by db.mu
}

type collCatalog struct {
	mu     sync.RWMutex
	byName map[string]*Collection
	byID   map[uint32]*Collection
	nextID uint32
	create sync.Mutex // serialises creation and dropping
}

func collPrefix(id uint32) []byte {
	if id <= maxSingleByteColl {
		return []byte{byte(0x01 + id)}
	}
	return []byte{collEscape, byte(id >> 8), byte(id)}
}

// splitKey separates an internal key into its collection id and user key.
// ok is false for reserved keys.
func splitKey(k []byte) (id uint32, user []byte, ok bool) {
	if len(k) == 0 || k[0] == reservedPrefix {
		return 0, nil, false
	}
	if k[0] == collEscape {
		if len(k) < 3 {
			return 0, nil, false
		}
		return uint32(binary.BigEndian.Uint16(k[1:])), k[3:], true
	}
	return uint32(k[0]) - 1, k[1:], true
}

func collCatalogKey(name string) []byte { return append([]byte{reservedPrefix, 'n'}, name...) }

func newCollection(db *DB, name string, id uint32) *Collection {
	pfx := collPrefix(id)
	return &Collection{db: db, name: name, id: id, pfx: pfx, end: prefixEnd(pfx), state: &collState{}}
}

// ikey appends the internal form of a user key to dst[:0].
func (c *Collection) ikey(dst, key []byte) []byte {
	n := len(c.pfx) + len(key)
	if cap(dst) < n {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	copy(dst, c.pfx)
	copy(dst[len(c.pfx):], key)
	return dst
}

// bound returns the collection reading through s (nil: live).
func (c *Collection) bound(s *Snapshot) *Collection {
	if c.snap == s {
		return c
	}
	cc := *c
	cc.snap = s
	return &cc
}

func (c *Collection) view() view { return view{c.db, c.snap} }

// Name returns the collection's name; the default collection is "".
func (c *Collection) Name() string { return c.name }

// DB returns the database the collection belongs to.
func (c *Collection) DB() *DB { return c.db }

func (c *Collection) check() error {
	if c.snap == nil && c.state.dropped.Load() {
		return ErrNoSuchCollection
	}
	return nil
}

// loadCollections reads the catalog. Called during open, before any writer.
func (db *DB) loadCollections() error {
	db.colls.byName = map[string]*Collection{}
	db.colls.byID = map[uint32]*Collection{}
	db.colls.nextID = 1
	db.def = newCollection(db, "", 0)
	db.colls.byID[0] = db.def
	lo := []byte{reservedPrefix, 'n'}
	hi := []byte{reservedPrefix, 'o'}
	var invalid error
	err := db.scan(view{db, nil}, lo, hi, func(k, v []byte) bool {
		var d struct {
			ID uint32 `json:"id"`
		}
		if json.Unmarshal(v, &d) != nil || d.ID == 0 || d.ID >= maxCollections || len(k) < 3 || len(k)-2 > 255 || db.colls.byID[d.ID] != nil {
			invalid = ErrCorrupt
			return false
		}
		c := newCollection(db, string(k[2:]), d.ID)
		db.colls.byName[c.name] = c
		db.colls.byID[c.id] = c
		if d.ID >= db.colls.nextID {
			db.colls.nextID = d.ID + 1
		}
		return true
	})
	if err != nil {
		return err
	}
	return invalid
}

// Collections lists the named collections, sorted.
func (db *DB) Collections() []string {
	db.colls.mu.RLock()
	defer db.colls.mu.RUnlock()
	out := make([]string, 0, len(db.colls.byName))
	for n := range db.colls.byName {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// lookupCollection finds a collection without creating it; "" is the
// default collection.
func (db *DB) lookupCollection(name string) (*Collection, bool) {
	if name == "" {
		return db.def, true
	}
	db.colls.mu.RLock()
	c, ok := db.colls.byName[name]
	db.colls.mu.RUnlock()
	return c, ok
}

func (db *DB) collectionByID(id uint32) (*Collection, bool) {
	db.colls.mu.RLock()
	c, ok := db.colls.byID[id]
	db.colls.mu.RUnlock()
	return c, ok
}

// Collection returns the named collection, creating it when absent. The
// empty name is the default collection.
func (db *DB) Collection(name string) (*Collection, error) {
	if c, ok := db.lookupCollection(name); ok {
		return c, nil
	}
	if !validCollectionName(name) {
		return nil, ErrCollectionName
	}
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	if c, ok := db.lookupCollection(name); ok {
		return c, nil
	}
	db.colls.mu.RLock()
	id := db.colls.nextID
	db.colls.mu.RUnlock()
	if id >= maxCollections {
		return nil, ErrTooManyCollections
	}
	// the catalog entry goes to the log before the collection is visible,
	// so no key can exist for a collection recovery would not know
	meta, _ := json.Marshal(struct {
		ID uint32 `json:"id"`
	}{id})
	b := db.NewBatch()
	b.internal = true
	b.add(collCatalogKey(name), meta, false)
	db.mu.Lock()
	if err := db.maintenanceReadyLocked(context.Background()); err != nil {
		db.mu.Unlock()
		return nil, err
	}
	w := db.wal
	l, err := b.commitLocked()
	if err != nil {
		db.mu.Unlock()
		return nil, err
	}
	c := newCollection(db, name, id)
	db.colls.mu.Lock()
	db.colls.byName[name] = c
	db.colls.byID[id] = c
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
	return c, nil
}

// DropCollection atomically removes a named collection, its indexes and catalog.
// Cancellation and staging-limit errors leave it intact. Existing snapshots
// continue to see their original collection. The default collection cannot drop.
func (db *DB) DropCollection(ctx context.Context, name string) error {
	if name == "" {
		return ErrCollectionName
	}
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	c, ok := db.lookupCollection(name)
	if !ok {
		return ErrNoSuchCollection
	}
	return db.dropCollectionAtomic(orBackground(ctx), c)
}

// deleteRange removes every internal key in [lo, hi) in batches of 1000,
// without index maintenance: callers drop the indexes first.
func (db *DB) deleteRange(ctx context.Context, lo, hi []byte) error {
	cur := lo
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var keys [][]byte
		err := db.scan(view{db, nil}, cur, hi, func(k, _ []byte) bool {
			keys = append(keys, append([]byte(nil), k...))
			return len(keys) < 1000
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		b := db.NewBatch()
		b.internal = true
		for _, k := range keys {
			b.add(k, nil, true)
		}
		if err := b.Commit(); err != nil {
			return err
		}
		if len(keys) < 1000 {
			return nil
		}
		cur = append(keys[len(keys)-1], 0)
	}
}

// ---- reads and writes

// Insert creates key. An existing key returns ErrKeyExists without changing it.
func (c *Collection) Insert(key, val []byte) error {
	return c.conditionalWrite(key, val, requireAbsent)
}

// Upsert creates or replaces key. It retains the pre-alpha Insert behavior.
func (c *Collection) Upsert(key, val []byte) error {
	if len(val) > MaxValueSize {
		return ErrValueSize
	}
	return c.write(key, val, false)
}

// Replace replaces an existing key; an absent key returns ErrNotFound.
func (c *Collection) Replace(key, val []byte) error {
	return c.conditionalWrite(key, val, requirePresent)
}

// Delete removes key. Deleting an absent key is not an error.
func (c *Collection) Delete(key []byte) error { return c.write(key, nil, true) }

// InsertJSON creates a document from a JSON object; duplicates fail.
func (c *Collection) InsertJSON(key, doc []byte) error { return c.writeJSON(key, doc, requireAbsent) }

// UpsertJSON creates or replaces a document from a JSON object.
func (c *Collection) UpsertJSON(key, doc []byte) error { return c.writeJSON(key, doc, 0) }

// ReplaceJSON replaces an existing document from a JSON object.
func (c *Collection) ReplaceJSON(key, doc []byte) error { return c.writeJSON(key, doc, requirePresent) }

func (c *Collection) write(key, val []byte, del bool) error {
	if len(key) == 0 || len(key) > MaxKeySize {
		return ErrKeySize
	}
	if err := c.check(); err != nil {
		return err
	}
	if c.snap != nil {
		return ErrReadOnly
	}
	if c.state.derived != nil {
		return ErrComputedCollectionReadOnly
	}
	db := c.db
	var kb [64]byte
	key = c.ikey(kb[:0], key)
	db.mu.Lock()
	if db.closed || db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		if db.closed {
			return ErrClosed
		}
		return err
	}
	db.stallLocked()
	if err := c.check(); err != nil {
		db.mu.Unlock()
		return err
	}
	var l lsn
	var err error
	w := db.wal
	if db.hasIndexes() || len(c.state.dependents) > 0 {
		b := c.NewBatch()
		b.limit = 0 // a single value may exceed the public batch limit
		b.first, b.n, b.lastOff = c, 1, 0
		b.buf = encodeRecord(nil, key, val, true, del, w.crcTab)
		l, err = b.commitLocked()
	} else {
		if !del && c.state.computed != nil {
			var err error
			val, err = c.state.computed.apply(val)
			if err != nil {
				db.mu.Unlock()
				return err
			}
		}
		var frame *changeFrame
		if db.feed != nil {
			frame, err = db.feed.captureOne(c, key[len(c.pfx):], key, val, del, nil)
			if err != nil {
				db.mu.Unlock()
				return err
			}
		}
		db.noteActiveStartLocked()
		if frame != nil {
			l, err = w.appendOneFeed(key, val, del, frame.encoded)
		} else {
			l, err = w.append(key, val, del)
		}
		if err == nil {
			err = db.putLocked(key, val, del)
		}
		if err == nil && frame != nil {
			db.feed.stage(frame, l)
		}
		if err != nil {
			db.failed = err
			if db.feed != nil {
				db.feed.stop(err)
			}
		}
	}
	needCkpt := db.needCheckpoint()
	db.mu.Unlock()
	if err != nil {
		return err
	}
	if db.opts.Sync {
		if err := db.syncWrite(w, l); err != nil {
			return err
		}
	}
	if needCkpt {
		db.requestCheckpoint()
	}
	return nil
}

// Find appends the value for key to dst and reports whether it exists.
func (c *Collection) Find(key, dst []byte) ([]byte, bool, error) {
	if err := c.check(); err != nil {
		return dst[:0], false, err
	}
	if len(key) > MaxKeySize {
		return dst[:0], false, ErrKeySize
	}
	var kb [64]byte
	return c.db.find(c.view(), c.ikey(kb[:0], key), dst)
}

// FindJSON returns the document under key rendered as JSON.
func (c *Collection) FindJSON(key []byte) ([]byte, bool, error) {
	v, ok, err := c.Find(key, nil)
	if err != nil || !ok {
		return nil, ok, err
	}
	m, err := DecodeDocument(v)
	if err != nil {
		return nil, true, err
	}
	out, err := json.Marshal(m)
	return out, true, err
}

// Scan calls fn for every key in [start, end) of the collection in key
// order; nil bounds are open. fn returns false to stop.
// Callback keys and values are read-only and valid only during fn.
func (c *Collection) Scan(start, end []byte, fn func(key, val []byte) bool) error {
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return err
		}
		defer s.Close()
		return cc.Scan(start, end, fn)
	}
	if err := c.check(); err != nil {
		return err
	}
	lo := c.ikey(nil, start)
	hi := c.end
	if end != nil {
		hi = c.ikey(nil, end)
	}
	n := len(c.pfx)
	return c.db.scan(c.view(), lo, hi, func(k, val []byte) bool { return fn(k[n:], val) })
}

// Source adapts a key range of the collection to the aggregation executor.
func (c *Collection) Source(start, end []byte) Source {
	return func(yield func(key, val []byte) bool) error { return c.Scan(start, end, yield) }
}

// NewBatch returns an empty batch whose writes go to this collection; see
// Batch.With for writing several collections atomically.
func (c *Collection) NewBatch() *Batch {
	return &Batch{coll: c, batchState: &batchState{db: c.db, lastOff: -1, limit: MaxTransactionBytes}}
}

// NewCursor returns a cursor over the collection positioned nowhere.
func (c *Collection) NewCursor() *Cursor {
	cur := &Cursor{c: c, v: c.view()}
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			cur.err = err
			return cur
		}
		cur.owned = s
		cur.c, cur.v = cc, view{c.db, s}
	}
	cur.snap = cur.c.snap.mem
	return cur
}

// ShardBounds returns up to n-1 keys splitting the collection into ranges
// of roughly equal page count.
func (c *Collection) ShardBounds(n int) ([][]byte, error) {
	return c.db.shardBounds(c, n)
}

// Shards returns key-ordered sources covering the collection.
func (c *Collection) Shards(n int) ([]Source, error) {
	bounds, err := c.ShardBounds(n)
	if err != nil {
		return nil, err
	}
	var out []Source
	var lo []byte
	for _, b := range bounds {
		out = append(out, c.Source(lo, b))
		lo = b
	}
	return append(out, c.Source(lo, nil)), nil
}

// Aggregate runs a JSON pipeline over the collection, through a secondary
// index when the leading $match allows it, and on every core when the
// pipeline's streaming part is a $match/$group (or $count) without a window:
// AggregateParallel with GOMAXPROCS shards.
func (c *Collection) Aggregate(ctx context.Context, pipeline []byte) ([]map[string]any, error) {
	return c.AggregateParallel(ctx, pipeline, c.db.opts.Parallelism)
}

// AggregateParallel runs a JSON pipeline over up to shards key ranges (or
// shards of an index's primary keys) at once; 1 runs it on the calling
// goroutine.
func (c *Collection) AggregateParallel(ctx context.Context, pipeline []byte, shards int) ([]map[string]any, error) {
	p, err := ParsePipeline(pipeline)
	if err != nil {
		return nil, err
	}
	return c.RunParallel(ctx, p, shards)
}

// Run executes a compiled pipeline over the collection on the calling
// goroutine.
func (c *Collection) Run(ctx context.Context, p *Pipeline) ([]map[string]any, error) {
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return nil, err
		}
		defer s.Close()
		return cc.Run(ctx, p)
	}
	ctx = orBackground(ctx)
	if err := c.check(); err != nil {
		return nil, err
	}
	pl := p.plan()
	if q, from, ok := c.reorder(p, &pl); ok {
		return from.Run(ctx, q)
	}
	budget, err := p.limits.budget()
	if err != nil {
		return nil, err
	}
	e := &env{ctx: ctx, c: c, count: -1, budget: budget}
	if ip, ok := c.indexPlanBudget(pl.preds, budget.max); ok {
		if err := budget.reserve(ip.memory); err != nil {
			return nil, err
		}
		e.count = ip.count
		return p.run(ctx, srcSpec{src: c.rangesSource(ctx, ip)}, e)
	}
	e.count = c.sourceBound(&pl)
	return p.run(ctx, srcSpec{c: c, ranged: true}, e)
}

// reorder turns a streaming join around when that is cheaper: the pipeline
// then starts from the foreign collection, through an index its own
// $match stages can use, and probes this collection's index on the local
// field. Only an inner join followed by a $group qualifies (rows would
// change shape otherwise), and only when the foreign side is selective
// enough for the probes to fit their budget. The rewritten pipeline reads
// this collection's fields under a synthetic name; results are identical.
func (c *Collection) reorder(p *Pipeline, pl *plan) (*Pipeline, *Collection, bool) {
	jp := pl.join
	if pl.limits.MaxBytes > 0 && pl.limits.MaxBytes < DefaultQueryMemoryBytes {
		return nil, nil, false
	}
	if hasLogical(pl.preds) || (jp != nil && hasLogical(jp.post)) {
		return nil, nil, false
	}
	if jp == nil || pl.group == nil || pl.countOnly || jp.preserve || pl.skip > 0 || pl.limit >= 0 || jp.wholeIdx >= 0 || c.db.noReorder {
		return nil, nil, false
	}
	for _, a := range pl.group.accs {
		if a.op == accFirst || a.op == accLast {
			return nil, nil, false // order-dependent: run as written
		}
	}
	if _, ok := c.indexOn(jp.spec.localField); !ok {
		return nil, nil, false
	}
	from, ok := c.lookupCollection(jp.spec.from)
	if !ok {
		return nil, nil, false
	}
	from = from.bound(c.snap)
	as := jp.spec.as + "."
	const mine = "\x00s" // this collection's document, as the other side sees it
	rename := func(f string) string {
		if strings.HasPrefix(f, as) {
			return f[len(as):]
		}
		return mine + "." + f
	}
	var fpreds, spreds []pred
	for _, pr := range jp.post {
		q := pr
		q.field = rename(pr.field)
		if strings.HasPrefix(pr.field, as) {
			fpreds = append(fpreds, q)
		} else {
			spreds = append(spreds, q)
		}
	}
	for _, pr := range pl.preds {
		q := pr
		q.field = mine + "." + pr.field
		spreds = append(spreds, q)
	}
	ipF, ok := from.indexPlan(fpreds)
	if !ok {
		return nil, nil, false
	}
	if ipS, ok := c.indexPlan(pl.preds); ok && ipS.count <= ipF.count {
		return nil, nil, false // this side is already the selective one
	}
	size, err := c.approxCount()
	if err != nil || ipF.count > max(64, size/probeRatio) {
		return nil, nil, false
	}
	g := &groupSpec{idNames: pl.group.idNames}
	for _, f := range pl.group.idFields {
		g.idFields = append(g.idFields, rename(f))
	}
	for _, a := range pl.group.accs {
		if a.field != "" {
			a.field = rename(a.field)
		}
		g.accs = append(g.accs, a)
	}
	stages := []stage{{kind: stMatch, preds: fpreds},
		{kind: stLookup, lookup: &lookupSpec{from: c.name, localField: jp.spec.foreignField, foreignField: jp.spec.localField, as: mine}},
		{kind: stUnwind, unwind: mine}}
	if len(spreds) > 0 {
		stages = append(stages, stage{kind: stMatch, preds: spreds})
	}
	stages = append(stages, stage{kind: stGroup, group: g})
	stages = append(stages, p.stages[pl.next:]...)
	return &Pipeline{stages: stages, limits: p.limits}, from, true
}

// sourceBound is an upper bound on the documents a full scan yields to the
// pipeline's join: the collection's estimated size, or the window's limit
// when smaller; -1 when the pipeline has no join to size.
func (c *Collection) sourceBound(pl *plan) int {
	if pl.join == nil {
		return -1
	}
	n, err := c.approxCount()
	if err != nil {
		return -1
	}
	if pl.limit >= 0 && pl.limit < n {
		n = pl.limit
	}
	return n
}

// RunParallel executes a compiled pipeline over the collection in shards.
func (c *Collection) RunParallel(ctx context.Context, p *Pipeline, shards int) ([]map[string]any, error) {
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return nil, err
		}
		defer s.Close()
		return cc.RunParallel(ctx, p, shards)
	}
	ctx = orBackground(ctx)
	if shards <= 1 {
		return c.Run(ctx, p)
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	pl := p.plan()
	if q, from, ok := c.reorder(p, &pl); ok {
		return from.RunParallel(ctx, q, shards)
	}
	budget, err := p.limits.budget()
	if err != nil {
		return nil, err
	}
	e := &env{ctx: ctx, c: c, count: -1, budget: budget}
	if ip, ok := c.indexPlanBudget(pl.preds, budget.max); ok {
		if err := budget.reserve(ip.memory); err != nil {
			return nil, err
		}
		e.count = ip.count
		if !pl.shards() {
			return p.run(ctx, srcSpec{src: c.rangesSource(ctx, ip)}, e)
		}
		srcs, err := c.rangesShards(ctx, ip, shards)
		if err != nil {
			return nil, err
		}
		specs := make([]srcSpec, len(srcs))
		for i, s := range srcs {
			specs[i] = srcSpec{src: s}
		}
		return p.runSharded(ctx, specs, e)
	}
	e.count = c.sourceBound(&pl)
	bounds, err := c.ShardBounds(shards)
	if err != nil {
		return nil, err
	}
	var specs []srcSpec
	var lo []byte
	for _, b := range bounds {
		specs = append(specs, srcSpec{c: c, lo: lo, hi: b, ranged: true})
		lo = b
	}
	specs = append(specs, srcSpec{c: c, lo: lo, ranged: true})
	return p.runSharded(ctx, specs, e)
}

// ---- index API on the collection

// Indexes lists the collection's index names and the fields they cover.
func (c *Collection) Indexes() map[string]string {
	out := map[string]string{}
	for _, d := range c.visibleIndexDefs() {
		if d.Coll == c.id {
			out[d.Name] = d.Field
		}
	}
	return out
}

func (c *Collection) indexDefs() []indexDef {
	var out []indexDef
	for _, d := range c.visibleIndexDefs() {
		if d.Coll == c.id {
			out = append(out, d)
		}
	}
	return out
}

// indexOn returns the collection's index on field, if any.
func (c *Collection) indexOn(field string) (indexDef, bool) {
	for _, d := range c.visibleIndexDefs() {
		if d.Coll == c.id && d.Field == field && len(d.Fields) <= 1 && (len(d.Fields) == 0 || !d.Fields[0].Desc) {
			return d, true
		}
	}
	return indexDef{}, false
}

// approxCount estimates the number of keys in the collection from the
// tree's shape, reading only the two root-to-leaf paths that bound its key
// range: at each level the children strictly between the two paths are
// whole subtrees, sized by the fan-outs seen at the levels below, and the
// two boundary leaves are sampled for their fill. Buffered records are
// added.
func (c *Collection) approxCount() (int, error) {
	db := c.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, ErrClosed
	}
	v := c.view()
	root, err := v.page(v.root())
	if err != nil {
		return 0, err
	}
	lo, hi := c.pfx, c.end
	type step struct {
		between int     // children strictly between the two paths at this level
		fanout  float64 // children per page at this level
	}
	var steps []step
	lp, hp := root, root // the pages on the lo and hi paths at this level
	same := true         // the paths have not parted yet
	var interior page    // a leaf wholly inside the range, sampled for its fill
	for lp.typ() == pageInternal {
		li := lp.searchInternal(lo)
		hj := hp.numCells()
		if hi != nil {
			hj = hp.searchInternal(hi)
		}
		st := step{fanout: (fanoutOf(lp) + fanoutOf(hp)) / 2}
		wasSame := same
		if same {
			if hj > li {
				st.between = hj - li - 1
				same = false
			}
		} else {
			st.between = (lp.numCells() - li) + hj
		}
		steps = append(steps, st)
		if lp.level() == 1 && interior == nil {
			// the leaf after the lo path's leaf lies inside the range when
			// this level has any child strictly between the two paths
			next := li + 1
			if inside := next <= lp.numCells() && (!wasSame || next < hj); inside {
				if p, err := v.page(lp.child(next)); err == nil && p.typ() == pageLeaf {
					interior = p
				}
			}
		}
		if lp, err = v.page(lp.child(li)); err != nil {
			return 0, err
		}
		if same {
			hp = lp
		} else if hp, err = v.page(hp.child(hj)); err != nil {
			return 0, err
		}
	}
	// leaves under a child at level i: the product of the fan-outs below it
	leaves := 0.0
	per := 1.0
	for i := len(steps) - 1; i >= 0; i-- {
		leaves += float64(steps[i].between) * per
		per *= steps[i].fanout
	}
	li, _ := lp.searchLeaf(lo)
	hj := hp.numCells()
	if hi != nil {
		hj, _ = hp.searchLeaf(hi)
	}
	var est float64
	if same {
		est = float64(hj - li)
	} else {
		// boundary leaves may hold other ranges' keys too: take the fill
		// from an interior leaf when there is one
		fill := float64(lp.numCells()+hp.numCells()) / 2
		if interior != nil {
			fill = float64(interior.numCells())
		}
		est = leaves*fill + float64(lp.numCells()-li) + float64(hj)
	}
	n := int(math.Max(0, est))
	if c.snap == nil && db.mt != nil {
		// buffered keys of this collection (by leading byte; collections
		// past the single-byte tags share one bucket)
		n += db.mt.byTag[c.pfx[0]]
		if db.imm != nil {
			n += db.imm.byTag[c.pfx[0]]
		}
	}
	return n, nil
}

// fanoutOf is the number of children of an internal page.
func fanoutOf(p page) float64 { return float64(p.numCells() + 1) }

func (c *Collection) String() string {
	if c.name == "" {
		return "collection(default)"
	}
	return fmt.Sprintf("collection(%s)", c.name)
}

func validCollectionName(name string) bool {
	return len(name) <= 255 && utf8.ValidString(name) && strings.IndexByte(name, 0) < 0
}

func validIndexName(name string) bool {
	return name != "" && len(name) <= MaxKeySize-6 && utf8.ValidString(name) && strings.IndexByte(name, 0) < 0
}
