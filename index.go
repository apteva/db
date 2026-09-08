package aptevadb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Secondary indexes live in the same B+tree under a reserved key prefix, so
// they are maintained in the same batch as the document (atomic on replay),
// checkpointed, snapshotted, compacted and recovered with it.
//
//	catalog entry: \x00 c <coll u32 BE> <name>             -> {"id":n,"field":"path"}
//	index entry:   \x00 i <id u32 BE> <valkey> <pk> <pklen u16 BE> -> (empty)
//
// Indexes belong to a collection: the catalog key carries the collection
// id, and entries hold the primary key within that collection.
//
// <valkey> is an order-preserving encoding of the indexed value whose byte
// order matches compareValues, so a prefix scan yields primary keys ordered
// by the indexed field. The primary key length trails the entry, so the
// primary key is recovered without parsing the value encoding. An array
// field yields one entry per scalar element (a multikey index). Numbers are
// indexed as float64: integers beyond 2^53 may share an entry, but the
// executor re-checks every predicate on the fetched document.
// reservedPrefix leads catalog and index entries; user keys start at 0x01.
const reservedPrefix = 0x00

var (
	ErrIndexExists = errors.New("aptevadb: index already exists")
	ErrNoSuchIndex = errors.New("aptevadb: no such index")
)

type indexDef struct {
	ID    uint32 `json:"id"`
	Field string `json:"field"`
	Name  string `json:"-"`
	Coll  uint32 `json:"-"` // owning collection, from the catalog key
}

func catalogKey(coll uint32, name string) []byte {
	k := []byte{reservedPrefix, 'c', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(k[2:], coll)
	return append(k, name...)
}

func indexPrefix(id uint32) []byte {
	b := []byte{reservedPrefix, 'i', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[2:], id)
	return b
}

// indexTag is the leading byte of a value's encoding; it follows rank order.
func indexTag(v value) byte { return byte(rank(v)) }

// encodeIndexValue appends the order-preserving encoding of a scalar.
func encodeIndexValue(dst []byte, v value) []byte {
	dst = append(dst, indexTag(v))
	switch v.k {
	case kBool:
		dst = append(dst, byte(v.i))
	case kInt, kFloat:
		bits := math.Float64bits(v.num())
		if bits&(1<<63) != 0 {
			bits = ^bits // negatives: reverse their order
		} else {
			bits |= 1 << 63 // positives above negatives
		}
		dst = binary.BigEndian.AppendUint64(dst, bits)
	case kString:
		for _, c := range v.s {
			if c == 0 {
				dst = append(dst, 0, 0xff)
			} else {
				dst = append(dst, c)
			}
		}
		dst = append(dst, 0, 0)
	}
	return dst
}

func indexable(v value) bool {
	switch v.k {
	case kNull, kBool, kInt, kFloat, kString:
		return true
	}
	return false
}

// indexEntryKey builds the entry key for (value, primary key).
func indexEntryKey(id uint32, v value, pk []byte) []byte {
	k := encodeIndexValue(indexPrefix(id), v)
	k = append(k, pk...)
	return binary.BigEndian.AppendUint16(k, uint16(len(pk)))
}

// primaryKeyOf recovers the primary key from an entry key.
func primaryKeyOf(k []byte) []byte {
	if len(k) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(k[len(k)-2:]))
	if n > len(k)-2 {
		return nil
	}
	return k[len(k)-2-n : len(k)-2]
}

// prefixEnd returns the smallest key greater than every key with prefix p,
// or nil when there is none within a byte-string order.
func prefixEnd(p []byte) []byte { return prefixEndInto(nil, p) }

// prefixEndInto is prefixEnd writing into dst[:0].
func prefixEndInto(dst, p []byte) []byte {
	out := append(dst[:0], p...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// indexedValues lists the scalar values of a document field that get
// entries: the value itself, or each scalar element of an array.
func indexedValues(doc []byte, field string, fn func(v value)) {
	if _, _, ok := docDir(doc); !ok {
		return
	}
	v := pathValue(value{k: kDoc, s: doc}, field)
	switch {
	case v.k == kArray:
		walkArray(v.s, func(e value) bool {
			if indexable(e) {
				fn(e)
			}
			return true
		})
	case indexable(v):
		fn(v)
	}
}

// indexOps appends to b the entry deletions and insertions that changing
// key from oldDoc to newDoc (nil: absent) requires.
func indexOps(defs []indexDef, b *Batch, key, oldDoc, newDoc []byte) {
	for _, d := range defs {
		if oldDoc != nil {
			indexedValues(oldDoc, d.Field, func(v value) { b.add(indexEntryKey(d.ID, v, key), nil, true) })
		}
		if newDoc != nil {
			indexedValues(newDoc, d.Field, func(v value) { b.add(indexEntryKey(d.ID, v, key), nil, false) })
		}
	}
}

// ---- catalog

type indexCatalog struct {
	mu       sync.RWMutex
	defs     []indexDef
	nextID   uint32
	scans    uint64                         // index-driven aggregations, for tests and stats
	building map[uint32]map[string]struct{} // indexes being built: keys written meanwhile
}

// touched returns the set of keys written while index id is being built,
// or nil. The set itself is only touched under the writer lock.
func (ic *indexCatalog) touched(id uint32) map[string]struct{} {
	ic.mu.RLock()
	defer ic.mu.RUnlock()
	return ic.building[id]
}

// loadIndexes reads the catalog and removes the entries of any index that
// has none: a build interrupted by a crash. Called during open, before any
// writer.
func (db *DB) loadIndexes() error {
	db.idx.defs = nil
	db.idx.nextID = 1
	lo := []byte{reservedPrefix, 'c'}
	hi := []byte{reservedPrefix, 'd'}
	err := db.scan(view{db, nil}, lo, hi, func(k, v []byte) bool {
		var d indexDef
		if len(k) >= 6 && json.Unmarshal(v, &d) == nil {
			d.Coll = binary.BigEndian.Uint32(k[2:])
			d.Name = string(k[6:])
			db.idx.defs = append(db.idx.defs, d)
			if d.ID >= db.idx.nextID {
				db.idx.nextID = d.ID + 1
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	return db.sweepOrphanEntries()
}

// sweepOrphanEntries deletes index entries whose index is not in the
// catalog. It seeks once per index id present, so a clean file costs one
// short scan per index.
func (db *DB) sweepOrphanEntries() error {
	known := map[uint32]bool{}
	for _, d := range db.idx.defs {
		known[d.ID] = true
	}
	cur := []byte{reservedPrefix, 'i'}
	end := []byte{reservedPrefix, 'j'}
	for {
		var first []byte
		err := db.scan(view{db, nil}, cur, end, func(k, _ []byte) bool {
			first = append([]byte(nil), k...)
			return false
		})
		if err != nil {
			return err
		}
		if first == nil || len(first) < 6 {
			return nil
		}
		id := binary.BigEndian.Uint32(first[2:])
		pfx := indexPrefix(id)
		if !known[id] {
			if err := db.deleteRange(context.Background(), pfx, prefixEnd(pfx)); err != nil {
				return err
			}
		}
		cur = prefixEnd(pfx)
	}
}

func (db *DB) indexDefs() []indexDef {
	db.idx.mu.RLock()
	defer db.idx.mu.RUnlock()
	return db.idx.defs
}

// hasIndexes reports whether any collection has an index.
func (db *DB) hasIndexes() bool {
	db.idx.mu.RLock()
	defer db.idx.mu.RUnlock()
	return len(db.idx.defs) > 0
}

// Indexes lists the default collection's index names and fields.
func (db *DB) Indexes() map[string]string { return db.def.Indexes() }

// CreateIndex builds an index on a document field of the default
// collection.
func (db *DB) CreateIndex(ctx context.Context, name, field string) error {
	return db.def.CreateIndex(ctx, name, field)
}

// DropIndex removes an index of the default collection.
func (db *DB) DropIndex(ctx context.Context, name string) error { return db.def.DropIndex(ctx, name) }

// CreateIndex builds an index on a document field (a dotted path is
// allowed). The definition is registered first, so writes made during the
// build maintain it and their keys are left to that maintenance; the
// existing documents are read from a snapshot, their entries sorted and
// inserted in order (leaves fill completely and each costs one descent),
// without log records: a checkpoint makes them durable before the catalog
// entry is logged. A crash before that leaves entries with no catalog
// entry, which the next open removes. Compaction waits for the build.
//
// ctx cancels the build between its steps; a cancelled build leaves no
// trace but entries the next open sweeps.
func (c *Collection) CreateIndex(ctx context.Context, name, field string) error {
	ctx = orBackground(ctx)
	if name == "" || field == "" {
		return fmt.Errorf("%w: index name and field are required", ErrPipeline)
	}
	if err := c.check(); err != nil {
		return err
	}
	db := c.db
	db.idx.mu.Lock()
	for _, d := range db.idx.defs {
		if d.Name == name && d.Coll == c.id {
			db.idx.mu.Unlock()
			return ErrIndexExists
		}
	}
	def := indexDef{ID: db.idx.nextID, Field: field, Name: name, Coll: c.id}
	db.idx.nextID++
	db.idx.defs = append(db.idx.defs, def)
	if db.idx.building == nil {
		db.idx.building = map[uint32]map[string]struct{}{}
	}
	db.idx.building[def.ID] = map[string]struct{}{}
	db.idx.mu.Unlock()
	db.mu.Lock()
	db.bulkLoads++
	db.mu.Unlock()
	done := func(keep bool) {
		db.idx.mu.Lock()
		delete(db.idx.building, def.ID)
		if !keep {
			for i, d := range db.idx.defs {
				if d.ID == def.ID {
					db.idx.defs = append(db.idx.defs[:i], db.idx.defs[i+1:]...)
					break
				}
			}
		}
		db.idx.mu.Unlock()
		db.mu.Lock()
		db.bulkLoads--
		db.mu.Unlock()
	}
	fail := func(err error) error {
		done(false)
		return err
	}
	// 0. apply what is buffered: the bulk inserts below go straight to the
	// tree, and a buffered tombstone for one of their keys (a swept or
	// dropped index that used this id) would otherwise shadow them
	if err := db.Checkpoint(); err != nil {
		return fail(err)
	}
	// 1. entries from a snapshot
	s, err := db.Snapshot()
	if err != nil {
		return fail(err)
	}
	var arena []byte
	var refs []entryRef
	paths := splitPaths([]string{field})
	vals := make([]value, 1)
	hints := make([]int, 1)
	pfx := indexPrefix(def.ID)
	add := func(k []byte, v value) {
		off := len(arena)
		arena = encodeIndexValue(append(arena, pfx...), v)
		arena = append(arena, k...)
		arena = binary.BigEndian.AppendUint16(arena, uint16(len(k)))
		refs = append(refs, newEntryRef(arena, off, len(arena)-off))
	}
	var cerr error
	err = c.bound(s).Scan(nil, nil, func(k, doc []byte) bool {
		if len(refs)&4095 == 4095 {
			if cerr = ctx.Err(); cerr != nil {
				return false
			}
		}
		if _, _, ok := docDir(doc); !ok {
			return true
		}
		extractFields(doc, paths, vals, hints)
		v := vals[0]
		switch {
		case v.k == kArray:
			var seen []value
			walkArray(v.s, func(e value) bool {
				if !indexable(e) {
					return true
				}
				for _, sv := range seen {
					if compareValues(sv, e) == 0 && sv.k == e.k {
						return true
					}
				}
				seen = append(seen, e)
				add(k, e)
				return true
			})
		case indexable(v):
			add(k, v)
		}
		return true
	})
	s.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return fail(err)
	}
	// 2. sort: mostly by the fixed-width head of the encoded value
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].head != refs[j].head {
			return refs[i].head < refs[j].head
		}
		return bytes.Compare(refs[i].key(arena), refs[j].key(arena)) < 0
	})
	// 3. insert in order, in chunks under the writer lock, skipping keys
	// that maintenance has handled since the snapshot
	const chunk = 2000
	for start := 0; start < len(refs); start += chunk {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		db.mu.Lock()
		if db.closed || db.failed != nil {
			db.mu.Unlock()
			return fail(ErrClosed)
		}
		touched := db.idx.touched(def.ID)
		db.sortedLoad = true
		si := sortedInserter{db: db}
		var last []byte
		for _, r := range refs[start:min(len(refs), start+chunk)] {
			ek := r.key(arena)
			if _, skip := touched[string(primaryKeyOf(ek))]; skip {
				continue
			}
			if bytes.Equal(ek, last) {
				continue // an array holding the same value twice
			}
			last = ek
			if err := si.insert(ek, nil); err != nil {
				db.sortedLoad = false
				db.mu.Unlock()
				return fail(err)
			}
		}
		db.sortedLoad = false
		needCkpt := db.needCheckpoint()
		db.mu.Unlock()
		if needCkpt {
			db.requestCheckpoint()
		}
	}
	arena, refs = nil, nil
	// 4. make the entries durable, then announce the index
	if err := db.Checkpoint(); err != nil {
		return fail(err)
	}
	meta, _ := json.Marshal(def)
	b := db.NewBatch()
	b.internal = true
	b.add(catalogKey(c.id, name), meta, false)
	if err := b.Commit(); err != nil {
		return fail(err)
	}
	done(true)
	return nil
}

// entryRef locates one entry key in the build arena, with the first eight
// bytes after the index prefix and tag (the value's fixed-width encoding
// for numbers, the start of a string) for cheap ordering.
type entryRef struct {
	off, n uint32
	head   uint64
}

func newEntryRef(arena []byte, off, n int) entryRef {
	r := entryRef{off: uint32(off), n: uint32(n)}
	var h [8]byte
	copy(h[:], arena[off+7:off+n]) // past the 6-byte prefix and the tag
	r.head = binary.BigEndian.Uint64(h[:])
	return r
}

func (r entryRef) key(arena []byte) []byte { return arena[r.off : r.off+r.n] }

// DropIndex removes an index and all its entries.
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	ctx = orBackground(ctx)
	db := c.db
	db.idx.mu.Lock()
	var def indexDef
	found := false
	for i, d := range db.idx.defs {
		if d.Name == name && d.Coll == c.id {
			def, found = d, true
			db.idx.defs = append(db.idx.defs[:i], db.idx.defs[i+1:]...)
			break
		}
	}
	db.idx.mu.Unlock()
	if !found {
		return ErrNoSuchIndex
	}
	b := db.NewBatch()
	b.internal = true
	b.add(catalogKey(c.id, name), nil, true)
	if err := b.Commit(); err != nil {
		return err
	}
	pfx := indexPrefix(def.ID)
	return db.deleteRange(ctx, pfx, prefixEnd(pfx))
}

// ---- planning

// indexRange is one contiguous entry-key range of an index.
type indexRange struct{ lo, hi []byte }

// indexRangesFor turns a predicate on an indexed field into entry ranges.
func indexRangesFor(def indexDef, p pred) ([]indexRange, bool) {
	pfx := indexPrefix(def.ID)
	eq := func(v value) indexRange {
		lo := encodeIndexValue(pfx, v)
		return indexRange{lo, prefixEnd(lo)}
	}
	tagEnd := func(v value) []byte { return prefixEnd(append(append([]byte(nil), pfx...), indexTag(v))) }
	tagStart := func(v value) []byte { return append(append([]byte(nil), pfx...), indexTag(v)) }
	switch p.op {
	case opEq:
		if !indexable(p.v) {
			return nil, false
		}
		return []indexRange{eq(p.v)}, true
	case opIn:
		var rs []indexRange
		for _, v := range p.in {
			if !indexable(v) {
				return nil, false
			}
			rs = append(rs, eq(v))
		}
		sort.Slice(rs, func(i, j int) bool { return bytes.Compare(rs[i].lo, rs[j].lo) < 0 })
		return rs, true
	case opGt, opGte, opLt, opLte:
		if !p.v.isNum() && p.v.k != kString {
			return nil, false
		}
		v := eq(p.v)
		switch p.op {
		case opGt:
			return []indexRange{{v.hi, tagEnd(p.v)}}, true
		case opGte:
			return []indexRange{{v.lo, tagEnd(p.v)}}, true
		case opLt:
			return []indexRange{{tagStart(p.v), v.lo}}, true
		default:
			return []indexRange{{tagStart(p.v), v.hi}}, true
		}
	}
	return nil, false
}

// rawScan iterates internal keys through the collection's view, for index
// entries.
func (c *Collection) rawScan(lo, hi []byte, fn func(key, val []byte) bool) error {
	return c.db.scan(c.view(), lo, hi, fn)
}

// indexedSource picks one indexable predicate of the pipeline's leading
// $match and returns a source over just the matching documents; see
// indexPlan for when that is done.
func (c *Collection) indexedSource(preds []pred) (src Source, count int, ok bool) {
	ip, ok := c.indexPlan(preds)
	if !ok {
		return nil, -1, false
	}
	return c.rangesSource(context.Background(), ip), ip.count, true
}

// indexPlan is a chosen index access: the primary keys its entry ranges
// name, each once, in entry order, collected during the counting pass so
// the entries are read only once.
type indexPlan struct {
	pks   [][]byte
	count int
}

// indexPlan picks one indexable predicate of a leading $match. Equality and
// $in are preferred over ranges. The entries are counted first (a key-only
// pass): the index is used only while it names at most an eighth of the
// collection, since reading documents one by one costs more than scanning
// them past that. The executor still checks every predicate on each
// document.
func (c *Collection) indexPlan(preds []pred) (indexPlan, bool) {
	if len(preds) == 0 || !c.db.hasIndexes() {
		return indexPlan{}, false
	}
	defs := c.indexDefs()
	if len(defs) == 0 {
		return indexPlan{}, false
	}
	size, err := c.approxCount()
	if err != nil {
		return indexPlan{}, false
	}
	cap := max(indexCountMin, size/8)
	// collect each candidate's primary keys (bounded) and take the fewest;
	// several ranges, or a range a multikey document may fall in more than
	// once, are deduplicated
	var best indexPlan
	found := false
	var pkbuf []byte
	for _, p := range preds {
		for _, d := range defs {
			if d.Field != p.field {
				continue
			}
			rs, ok := indexRangesFor(d, p)
			if !ok {
				continue
			}
			limit := cap
			if found {
				limit = best.count
			}
			var seen map[string]struct{}
			if len(rs) > 1 || p.op != opEq {
				seen = map[string]struct{}{}
			}
			pkbuf = pkbuf[:0]
			var offs []int
			over := false
			for _, r := range rs {
				err := c.rawScan(r.lo, r.hi, func(k, _ []byte) bool {
					pk := primaryKeyOf(k)
					if pk == nil {
						return true
					}
					if seen != nil {
						if _, dup := seen[string(pk)]; dup {
							return true
						}
						seen[string(pk)] = struct{}{}
					}
					if len(offs)/2 >= limit {
						over = true
						return false
					}
					offs = append(offs, len(pkbuf), len(pkbuf)+len(pk))
					pkbuf = append(pkbuf, pk...)
					return true
				})
				if err != nil {
					return indexPlan{}, false
				}
				if over {
					break
				}
			}
			if over {
				continue // a scan reads that many documents faster, or another index does better
			}
			pks := make([][]byte, 0, len(offs)/2)
			flat := append([]byte(nil), pkbuf...)
			for i := 0; i < len(offs); i += 2 {
				pks = append(pks, flat[offs[i]:offs[i+1]:offs[i+1]])
			}
			best, found = indexPlan{pks: pks, count: len(pks)}, true
		}
	}
	if !found {
		return indexPlan{}, false
	}
	c.db.idx.mu.Lock()
	c.db.idx.scans++
	c.db.idx.mu.Unlock()
	return best, true
}

// indexCountMin is the smallest entry count an index is used up to, however
// small the collection.
const indexCountMin = 1 << 12

// rangesSource yields the documents the plan's primary keys name, in
// entry order.
func (c *Collection) rangesSource(ctx context.Context, ip indexPlan) Source {
	return pksSource(ctx, c, ip.pks)
}

func pksSource(ctx context.Context, c *Collection, pks [][]byte) Source {
	return func(yield func(k, v []byte) bool) error {
		var vbuf []byte
		for i, pk := range pks {
			if i&1023 == 1023 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			doc, ok, err := c.Find(pk, vbuf)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			vbuf = doc
			if !yield(pk, doc) {
				return nil
			}
		}
		return nil
	}
}

// rangesShards splits an index access into n sources of primary keys, for
// a parallel run, at least 256 documents each.
func (c *Collection) rangesShards(ctx context.Context, ip indexPlan, n int) ([]Source, error) {
	pks := ip.pks
	n = max(1, min(n, (len(pks)+255)/256))
	var out []Source
	for i := 0; i < n; i++ {
		out = append(out, pksSource(ctx, c, pks[len(pks)*i/n:len(pks)*(i+1)/n]))
	}
	return out, nil
}

// VerifyIndexes checks every index of every collection against the
// documents: each entry's document exists and carries the value, and each
// document's indexed values have entries. It reads everything.
func (db *DB) VerifyIndexes(ctx context.Context) error {
	ctx = orBackground(ctx)
	defs := db.indexDefs()
	for _, d := range defs {
		if err := ctx.Err(); err != nil {
			return err
		}
		c, ok := db.collectionByID(d.Coll)
		if !ok {
			return fmt.Errorf("aptevadb: index %s belongs to unknown collection %d", d.Name, d.Coll)
		}
		pfx := indexPrefix(d.ID)
		entries := map[string]int{}
		err := db.scan(view{db, nil}, pfx, prefixEnd(pfx), func(k, _ []byte) bool {
			entries[string(k)]++
			return true
		})
		if err != nil {
			return err
		}
		var missing error
		err = c.Scan(nil, nil, func(k, doc []byte) bool {
			seen := map[string]struct{}{} // one entry per distinct value of a document
			indexedValues(doc, d.Field, func(v value) {
				ek := string(indexEntryKey(d.ID, v, k))
				if _, dup := seen[ek]; dup {
					return
				}
				seen[ek] = struct{}{}
				if entries[ek] == 0 {
					missing = fmt.Errorf("aptevadb: index %s: missing entry for key %q", d.Name, k)
				} else {
					entries[ek]--
				}
			})
			return missing == nil
		})
		if err == nil {
			err = missing
		}
		if err != nil {
			return err
		}
		for k, n := range entries {
			if n > 0 {
				return fmt.Errorf("aptevadb: index %s: stale entry for primary key %q", d.Name, primaryKeyOf([]byte(k)))
			}
		}
	}
	return nil
}
