package db

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
)

// env is what one pipeline execution knows about its surroundings: the
// collection it runs over (and so the snapshot, if any, that a $lookup's
// foreign collection must be read through) and the join tables built so
// far, shared between shards.
type env struct {
	ctx    context.Context
	c      *Collection
	budget *memoryBudget
	count  int // documents the source yields when index-driven, else -1
	mu     sync.Mutex
	tables map[string]*lookupTable
}

// probeRatio sets when a join probes the foreign index rather than hashing
// the foreign collection: an index probe (entry scan plus a document read)
// costs about this many times the per-document cost of building the table;
// entryRatio is the same for a probe that needs no document, only the
// entry. smallOuter is the largest outer side for which the table is built
// filtered to the outer keys, after a bounded pre-pass over the source.
const (
	probeRatio    = 32
	entryRatio    = 4
	smallOuter    = 4096
	prePassBudget = 1 << 18
)

func (e *env) collection(name string) (*Collection, error) {
	if e == nil || e.c == nil {
		return nil, fmt.Errorf("%w: $lookup needs a collection to run in (use Collection.Aggregate)", ErrPipeline)
	}
	c, ok := e.c.lookupCollection(name)
	if !ok {
		return nil, fmt.Errorf("%w: $lookup from %q", ErrNoSuchCollection, name)
	}
	return c.bound(e.c.snap), nil
}

// frow is one foreign document as a join sees it: the fields the pipeline
// needs from it, copied out (their bytes live in the executor's arena),
// and the document itself when rows are being materialised.
type frow struct {
	vals []value
	doc  []byte
}

// lookupTable maps a foreign field value to the documents carrying it, or,
// when the join needs nothing from them but their number, to that number.
// Numbers are keyed by their bits, everything else by its order-preserving
// encoding. Built once per (collection, field, needed fields) and read
// concurrently.
type lookupTable struct {
	nrows  map[uint64][]*frow
	srows  map[string][]*frow
	ncount map[uint64]int32
	scount map[string]int32
	n      int
}

func (t *lookupTable) add(k joinKey, fr *frow) {
	if k.isNum {
		t.nrows[k.num] = append(t.nrows[k.num], fr)
	} else {
		t.srows[string(k.str)] = append(t.srows[string(k.str)], fr)
	}
}

func (t *lookupTable) count(k joinKey) {
	if k.isNum {
		t.ncount[k.num]++
	} else {
		t.scount[string(k.str)]++
	}
}

// emptyRow stands for every foreign document of a count-only table.
var emptyRow frow

// joinKey is the hash key of a join value: numbers of both kinds compare
// by value and use their bits; null and a missing field coincide.
type joinKey struct {
	num   uint64
	str   []byte
	isNum bool
}

// joinKeyOf builds the key of v, using dst for the encoded form; ok is
// false for documents and arrays, which never match.
func joinKeyOf(dst []byte, v value) (joinKey, bool) {
	switch v.k {
	case kDoc, kArray:
		return joinKey{}, false
	case kInt, kFloat:
		if v.k == kInt && compareIntFloat(v.i, float64(v.i)) != 0 {
			return joinKey{str: encodeExactIndexValue(dst[:0], v)}, true
		}
		number := v.num()
		if number == 0 {
			number = 0
		}
		bits := math.Float64bits(number)
		if bits&(1<<63) != 0 {
			bits = ^bits
		} else {
			bits |= 1 << 63
		}
		return joinKey{num: bits, isNum: true}, true
	case kMissing:
		v = value{k: kNull}
	}
	return joinKey{str: encodeIndexValue(dst[:0], v)}, true
}

// joinFilter is a set of join keys: the outer side of a small join.
type joinFilter struct {
	nums map[uint64]struct{}
	strs map[string]struct{}
}

func (f *joinFilter) add(k joinKey) {
	if k.isNum {
		f.nums[k.num] = struct{}{}
	} else {
		f.strs[string(k.str)] = struct{}{}
	}
}

func (f *joinFilter) has(k joinKey) bool {
	if k.isNum {
		_, ok := f.nums[k.num]
		return ok
	}
	_, ok := f.strs[string(k.str)]
	return ok
}

// lookupExec resolves one $lookup for one run: through the table, or by
// probing the foreign collection's index on the field while the number
// of probes stays small.
type lookupExec struct {
	e       *env
	from    *Collection
	spec    *lookupSpec
	fneed   []string
	fpaths  []fieldPath
	keepDoc bool
	limits  Limits
	table   *lookupTable
	idx     indexDef
	useIdx  bool
	probes  int
	budget  int // index probes allowed before hashing instead
	vals    []value
	hints   []int
	kb      []byte
	lo, hi  []byte // probe range buffers
	cGen    uint64 // probe leaf cache: tree generation it was taken at
	cID     uint32 // ... the leaf's page id
	cUpper  []byte // ... the separator bounding it above (nil: none)
	pkbuf   []byte // primary keys of one probe, flat
	pkoffs  []int
	fkey    []fieldPath // the foreign field, for the table build
	fkval   []value
	fkhint  []int
	chunk   []frow  // rows are carved from blocks, not allocated one by one
	vchunk  []value // likewise their field vectors
	dbuf    []byte  // document read buffer for probes
	arena   []byte  // and the bytes of their string fields
	filter  *joinFilter
}

// entryOnly reports whether the join needs nothing from foreign documents
// but their existence and number.
func (lk *lookupExec) entryOnly() bool { return len(lk.fneed) == 0 && !lk.keepDoc }

// lookup prepares a join. limit is the number of source records that can
// probe it (-1: unknown). With an index on the foreign field, the join
// probes it when the expected probes cost less than hashing the foreign
// collection (its size is estimated from the tree); an unknown number of
// probes means hashing. A run that exceeds its probe budget switches to
// the table.
func (e *env) lookup(spec *lookupSpec, fneed []string, keepDoc bool, limit int, limits Limits) (*lookupExec, error) {
	from, err := e.collection(spec.from)
	if err != nil {
		return nil, err
	}
	lk := &lookupExec{e: e, from: from, spec: spec, fneed: fneed, fpaths: splitPaths(fneed), keepDoc: keepDoc, limits: limits,
		vals: make([]value, len(fneed)), hints: make([]int, len(fneed)),
		fkey: splitPaths([]string{spec.foreignField}), fkval: make([]value, 1), fkhint: make([]int, 1)}
	if limit < 0 || (e.count >= 0 && e.count < limit) {
		limit = e.count
	}

	if def, ok := from.indexOn(spec.foreignField); ok && limit >= 0 {
		size, err := from.approxCount()
		if err != nil {
			return nil, err
		}

		ratio := probeRatio
		if lk.entryOnly() {
			ratio = entryRatio
		}
		lk.budget = max(64, size/ratio)
		if limit <= lk.budget {
			lk.idx, lk.useIdx = def, true
		}
	}
	return lk, nil
}

// prepare settles how a hashing join gets its table: from the env when a
// shard built it already, filtered to the outer keys when pre reports the
// outer side small, else over the whole foreign collection. pre runs the
// streaming prefix over the source and returns the local join values when
// there are at most smallOuter of them within prePassBudget documents.
func (lk *lookupExec) prepare(pre func() ([]value, bool)) error {
	if lk.useIdx || lk.table != nil {
		return nil
	}
	e := lk.e
	key := lk.tableKey()
	e.mu.Lock()
	t := e.tables[key]
	e.mu.Unlock()
	if t != nil {
		lk.table = t
		return nil
	}
	if pre != nil {
		if keys, ok := pre(); ok {
			lk.filter = &joinFilter{nums: map[uint64]struct{}{}, strs: map[string]struct{}{}}
			var kb []byte
			for _, v := range keys {
				if v.k == kArray {
					walkArray(v.s, func(e value) bool {
						if k, ok := joinKeyOf(kb, e); ok {
							kb = k.str
							lk.filter.add(k)
						}
						return true
					})
					continue
				}
				if k, ok := joinKeyOf(kb, v); ok {
					kb = k.str
					lk.filter.add(k)
				}
			}
		}
	}
	return lk.ensureTable()
}

func (lk *lookupExec) tableKey() string {
	return lk.from.name + "\x00" + lk.spec.foreignField + "\x00" + strings.Join(lk.fneed, "\x00") + fmt.Sprint(lk.keepDoc)
}

// preScan is the pre-pass of a streaming join: it runs the plan's leading
// predicates and window over src and collects the local join values, giving
// up as soon as there are more than smallOuter of them or the pass has
// examined prePassBudget documents without finishing.
func (pl *plan) preScan(ctx context.Context, src Source, budget *memoryBudget) func() ([]value, bool) {
	return func() ([]value, bool) {
		vals := make([]value, len(pl.need))
		hints := make([]int, len(pl.need))
		var keys []value
		reserved := int64(0)
		seen, emitted, examined, done := 0, 0, 0, true
		err := src(func(_, val []byte) bool {
			examined++
			if examined > prePassBudget || (examined&4095 == 0 && ctx.Err() != nil) {
				done = false
				return false
			}
			if _, _, ok := docDir(val); !ok {
				return true
			}
			extractFields(val, pl.paths, vals, hints)
			if !pl.matches(vals) {
				return true
			}
			seen++
			if seen <= pl.skip {
				return true
			}
			if pl.limit >= 0 && emitted >= pl.limit {
				return false
			}
			emitted++
			if len(keys) >= smallOuter {
				done = false
				return false
			}
			v := vals[pl.join.localIdx]
			cost := int64(len(v.s))*2 + 96
			if err := budget.reserve(cost); err != nil {
				done = false
				return false
			}
			reserved += cost
			keys = append(keys, toScalar(v).view())
			return true
		})
		if err != nil || !done {
			_ = budget.reserve(-reserved)
			return nil, false
		}
		return keys, true
	}
}

// ensureTable builds (or fetches from the env) the hash table over the
// foreign collection.
func (lk *lookupExec) ensureTable() error {
	if lk.table != nil {
		return nil
	}
	e := lk.e
	key := lk.tableKey()
	e.mu.Lock()
	defer e.mu.Unlock()
	if t := e.tables[key]; t != nil {
		lk.table = t
		return nil
	}
	t, err := lk.buildTable()
	if err != nil {
		return err
	}
	if lk.filter == nil { // a filtered table serves one outer set only
		if e.tables == nil {
			e.tables = map[string]*lookupTable{}
		}
		e.tables[key] = t
	}
	lk.table = t
	return nil
}

// clone returns an executor sharing the table (read-only) with its own
// scratch state, for a shard.
func (lk *lookupExec) clone() *lookupExec {
	c := *lk
	c.kb, c.chunk, c.vchunk, c.arena, c.lo, c.hi, c.dbuf = nil, nil, nil, nil, nil, nil, nil
	c.cGen, c.cID, c.cUpper, c.pkbuf, c.pkoffs = 0, 0, nil, nil, nil
	c.fkval, c.fkhint = make([]value, 1), make([]int, 1)
	c.vals = make([]value, len(lk.fneed))
	c.hints = make([]int, len(lk.fneed))
	return &c
}

// foreignKeys calls fn with each key the foreign field of doc matches
// under: the value itself, or each element of an array, once each. The
// field is read with a layout hint, like every other extracted field.
func (lk *lookupExec) foreignKeys(doc []byte, kb []byte, fn func(k joinKey)) []byte {
	extractFields(doc, lk.fkey, lk.fkval, lk.fkhint)
	v := lk.fkval[0]
	if v.k != kArray {
		if k, ok := joinKeyOf(kb, v); ok {
			fn(k)
			if !k.isNum {
				return k.str
			}
		}
		return kb
	}
	var seen []joinKey
	count, _, _ := arrayHeader(v.s)
	var numeric map[uint64]bool
	var textual map[string]bool
	if count > 16 {
		numeric = map[uint64]bool{}
		textual = map[string]bool{}
	}
	walkArray(v.s, func(e value) bool {
		k, ok := joinKeyOf(kb, e)
		if !ok {
			return true
		}
		if numeric != nil {
			if k.isNum {
				if numeric[k.num] {
					return true
				}
				numeric[k.num] = true
			} else {
				if textual[string(k.str)] {
					return true
				}
				textual[string(k.str)] = true
			}
		}
		if !k.isNum {
			// keep this key's bytes: the next element needs its own buffer
			k.str = append([]byte(nil), k.str...)
		}
		for _, s := range seen {
			if s.isNum == k.isNum && s.num == k.num && string(s.str) == string(k.str) {
				return true
			}
		}
		if numeric == nil {
			seen = append(seen, k)
		}
		fn(k)
		return true
	})
	return kb
}

func (lk *lookupExec) buildTable() (*lookupTable, error) {
	t := &lookupTable{}
	countOnly := lk.entryOnly()
	if countOnly {
		t.ncount, t.scount = map[uint64]int32{}, map[string]int32{}
	} else {
		t.nrows, t.srows = map[uint64][]*frow{}, map[string][]*frow{}
	}
	var kb []byte
	keyWorking := int64(0)
	var lerr error
	var fr *frow
	ctx := lk.e.ctx
	err := lk.from.Scan(nil, nil, func(_, doc []byte) bool {
		if t.n&4095 == 4095 {
			if lerr = ctx.Err(); lerr != nil {
				return false
			}
		}
		if _, _, ok := docDir(doc); !ok {
			return true
		}
		if t.n >= lk.limits.MaxRows {
			lerr = fmt.Errorf("%w: $lookup from %q holds more than %d documents", ErrLimit, lk.spec.from, lk.limits.MaxRows)
			return false
		}
		fr = nil
		foreign := pathValue(value{k: kDoc, s: doc}, lk.spec.foreignField)
		required := int64(len(foreign.s))*2 + 128
		if foreign.k == kArray {
			count, _, valid := arrayHeader(foreign.s)
			if !valid {
				lerr = ErrDocument
				return false
			}
			required += int64(count) * 64
		}
		if required > keyWorking {
			if err := lk.e.budget.reserve(required - keyWorking); err != nil {
				lerr = err
				return false
			}
			keyWorking = required
		}
		kb = lk.foreignKeys(doc, kb, func(k joinKey) {
			if lk.filter != nil && !lk.filter.has(k) {
				return
			}
			if lerr != nil {
				return
			}
			if err := lk.e.budget.reserve(int64(len(k.str)) + 80); err != nil {
				lerr = err
				return
			}
			if countOnly {
				t.count(k)
				return
			}
			if fr == nil {
				cost := int64(512 + len(lk.fneed)*96)
				if lk.keepDoc {
					cost += int64(len(doc))
				}
				for _, field := range lk.fneed {
					cost += int64(len(pathValue(value{k: kDoc, s: doc}, field).s))
				}
				if err := lk.e.budget.reserve(cost); err != nil {
					lerr = err
					return
				}
				fr = lk.row(doc)
			}
			t.add(k, fr)
		})
		t.n++
		return lerr == nil
	})
	if err == nil {
		err = lerr
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

// row copies what the join needs out of a foreign document.
func (lk *lookupExec) row(doc []byte) *frow {
	if len(lk.chunk) == 0 {
		lk.chunk = make([]frow, 512)
	}
	fr := &lk.chunk[0]
	lk.chunk = lk.chunk[1:]
	if n := len(lk.fneed); n > 0 {
		extractFields(doc, lk.fpaths, lk.vals, lk.hints)
		if len(lk.vchunk) < n {
			lk.vchunk = make([]value, 512*n)
		}
		fr.vals = lk.vchunk[:n:n]
		lk.vchunk = lk.vchunk[n:]
		for i, v := range lk.vals {
			if len(v.s) > 0 {
				// the document's bytes are the scan's: keep our own copy
				if len(lk.arena)+len(v.s) > cap(lk.arena) {
					lk.arena = make([]byte, 0, max(64<<10, len(v.s)))
				}
				start := len(lk.arena)
				lk.arena = append(lk.arena, v.s...)
				v.s = lk.arena[start:len(lk.arena):len(lk.arena)]
			}
			fr.vals[i] = v
		}
	}
	if lk.keepDoc {
		fr.doc = append([]byte(nil), doc...)
	}
	return fr
}

// probe calls fn with every foreign document matching v, each once.
func (lk *lookupExec) probe(v value, fn func(fr *frow) bool) error {
	if v.k != kArray {
		return lk.probeOne(v, fn)
	}
	var seen []*frow
	var perr error
	walkArray(v.s, func(e value) bool {
		perr = lk.probeOne(e, func(fr *frow) bool {
			for _, s := range seen {
				if s == fr {
					return true
				}
			}
			seen = append(seen, fr)
			return fn(fr)
		})
		return perr == nil
	})
	return perr
}

func (lk *lookupExec) probeOne(v value, fn func(fr *frow) bool) error {
	if v.k == kDoc || v.k == kArray {
		return nil // documents never match
	}
	if v.k == kMissing {
		v = value{k: kNull}
	}
	// the index has no entry for a missing foreign field, which null must
	// match: null probes go through the table
	if lk.useIdx && (lk.probes >= lk.budget || v.k == kNull) {
		if err := lk.ensureTable(); err != nil {
			return err
		}
		lk.useIdx = false
	}
	if !lk.useIdx {
		if lk.table == nil {
			if err := lk.ensureTable(); err != nil {
				return err
			}
		}
		k, ok := joinKeyOf(lk.kb, v)
		if !ok {
			return nil
		}
		if !k.isNum {
			lk.kb = k.str
		}
		t := lk.table
		if t.ncount != nil {
			var n int32
			if k.isNum {
				n = t.ncount[k.num]
			} else {
				n = t.scount[string(k.str)]
			}
			for ; n > 0; n-- {
				if !fn(&emptyRow) {
					return nil
				}
			}
			return nil
		}
		var rows []*frow
		if k.isNum {
			rows = t.nrows[k.num]
		} else {
			rows = t.srows[string(k.str)]
		}
		for _, fr := range rows {
			if !fn(fr) {
				return nil
			}
		}
		return nil
	}
	if !indexable(v) {
		return nil
	}
	lk.probes++
	// the entry range of one value: prefix, encoded value, and its end
	if lk.lo == nil {
		lk.lo = indexPrefix(lk.idx.ID)
	}
	lk.lo = indexScalarPrefix(lk.idx, lk.lo[:6], v)
	lk.hi = prefixEndInto(lk.hi, lk.lo)
	if lk.idx.Encoding != 2 && v.isNum() && v.num() == 0 {
		lk.lo = legacyZeroPrefix(lk.idx.ID)
	}
	lk.e.c.db.idx.mu.Lock()
	lk.e.c.db.idx.scans++
	lk.e.c.db.idx.mu.Unlock()
	entryOnly := lk.entryOnly() && !(v.isNum() && lk.idx.Encoding != 2)
	n, err := lk.probeEntries(lk.lo, lk.hi, !entryOnly)
	if err != nil {
		return err
	}
	if entryOnly {
		// one entry per matching document: no document read needed
		for ; n > 0; n-- {
			if !fn(&emptyRow) {
				return nil
			}
		}
		return nil
	}
	for i := 0; i < len(lk.pkoffs); i += 2 {
		pk := lk.pkbuf[lk.pkoffs[i]:lk.pkoffs[i+1]]
		doc, ok, err := lk.from.Find(pk, lk.dbuf)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		lk.dbuf = doc
		if _, _, ok := docDir(doc); !ok {
			continue
		}
		matches := false
		indexedValues(doc, lk.spec.foreignField, func(candidate value) {
			if compareValues(candidate, v) == 0 {
				matches = true
			}
		})
		if !matches {
			continue
		}
		if !fn(lk.row(doc)) {
			return nil
		}
	}
	return nil
}

// probeEntries counts the index entries in [lo, hi) and, when keep is set,
// collects their primary keys into pkbuf/pkoffs. Consecutive probes usually
// land in the same index leaf (a sorted outer side walks the index in
// order), so the leaf reached last time is tried first, validated by the
// tree generation, its first key and the separator above it; a range that
// runs past the leaf continues through a scan.
func (lk *lookupExec) probeEntries(lo, hi []byte, keep bool) (int, error) {
	c := lk.from
	db := c.db
	v := c.view()
	lk.pkbuf, lk.pkoffs = lk.pkbuf[:0], lk.pkoffs[:0]
	n := 0
	take := func(k []byte) {
		n++
		if keep {
			if pk := primaryKeyOfDef(lk.idx, k); pk != nil {
				lk.pkoffs = append(lk.pkoffs, len(lk.pkbuf), len(lk.pkbuf)+len(pk))
				lk.pkbuf = append(lk.pkbuf, pk...)
			}
		}
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return 0, ErrClosed
	}
	var leaf page
	if lk.cGen == db.treeGen && lk.cID != 0 {
		if p, err := v.page(lk.cID); err == nil && p.typ() == pageLeaf && p.numCells() > 0 &&
			bytes.Compare(lo, p.leafKey(0)) >= 0 && (lk.cUpper == nil || bytes.Compare(lo, lk.cUpper) < 0) {
			leaf = p
		}
	}
	if leaf == nil {
		p, err := v.page(v.root())
		if err != nil {
			db.mu.RUnlock()
			return 0, err
		}
		lk.cUpper = lk.cUpper[:0]
		hasUpper := false
		id := v.root()
		for p.typ() == pageInternal {
			idx := p.searchInternal(lo)
			if idx < p.numCells() {
				lk.cUpper = append(lk.cUpper[:0], p.internalKey(idx)...)
				hasUpper = true
			}
			id = p.child(idx)
			if p, err = v.page(id); err != nil {
				db.mu.RUnlock()
				return 0, err
			}
		}
		if !hasUpper {
			lk.cUpper = nil
		}
		leaf = p
		lk.cGen, lk.cID = db.treeGen, id
	}
	i, _ := leaf.searchLeaf(lo)
	nc := leaf.numCells()
	for ; i < nc; i++ {
		k := leaf.leafKey(i)
		if bytes.Compare(k, hi) >= 0 {
			db.mu.RUnlock()
			return n, nil
		}
		take(k)
	}
	upper := lk.cUpper
	db.mu.RUnlock()
	if upper == nil || bytes.Compare(hi, upper) <= 0 {
		return n, nil
	}
	// the range continues in the following leaves
	err := c.rawScan(upper, hi, func(k, _ []byte) bool { take(k); return true })
	return n, err
}
