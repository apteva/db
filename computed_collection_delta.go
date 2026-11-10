package db

import (
	"bytes"
	"context"
	"fmt"
	"sort"
)

type computedGroupState struct {
	plan    *computedCollectionPlan
	key     []byte
	id      value
	parts   []value
	members computedWide
	sums    []computedWide
}
type computedCollectionDelta struct {
	db         *DB
	batch      *Batch
	groups     map[string]*computedGroupState
	references map[string]*computedReference
	bytes      int
}

func newComputedCollectionDelta(db *DB, b *Batch) *computedCollectionDelta {
	return &computedCollectionDelta{db: db, batch: b, groups: map[string]*computedGroupState{}}
}
func (d *computedCollectionDelta) limit() error {
	if len(d.batch.buf)+d.bytes > computedCollectionLimit {
		return ErrComputedCollectionLimit
	}
	return nil
}

func (d *computedCollectionDelta) group(p *computedCollectionPlan, doc []byte) (*computedGroupState, error) {
	var parts []value
	var uk []byte
	var err error
	id := value{k: kNull}
	if p.group.idNames != nil {
		parts = make([]value, len(p.group.idFields))
		for i, field := range p.group.idFields {
			parts[i] = pathValue(value{k: kDoc, s: doc}, field)
		}
		uk, err = computedTupleKey(parts)
	} else {
		if len(p.group.idFields) > 0 {
			id = pathValue(value{k: kDoc, s: doc}, p.group.idFields[0])
		}
		uk, id, err = computedGroupValueKey(id)
	}
	if err != nil {
		return nil, err
	}
	key := p.target.ikey(nil, uk)
	if g := d.groups[string(key)]; g != nil {
		return g, nil
	}
	// Own string IDs: callers may reuse a read buffer while backfilling.
	if id.k == kString {
		id.s = append([]byte(nil), id.s...)
	}
	partBytes := len(parts) * 64
	for i := range parts {
		if parts[i].k == kString {
			parts[i].s = append([]byte(nil), parts[i].s...)
			partBytes += len(parts[i].s)
		}
	}
	g := &computedGroupState{plan: p, key: key, id: id, parts: parts, sums: make([]computedWide, len(p.output()))}
	row, found, err := d.db.findLocked(view{d.db, nil}, key, nil)
	if err != nil {
		return nil, err
	}
	counter, exists, err := d.db.findLocked(view{d.db, nil}, computedViewKey('r', p.target.id, uk), nil)
	if err != nil {
		return nil, err
	}
	if found != exists || (exists && (len(counter) != 8 || int64(le.Uint64(counter)) <= 0)) {
		return nil, ErrCorrupt
	}
	if found {
		if err := validateComputedDoc(row, 0); err != nil {
			return nil, ErrCorrupt
		}
		g.members.add(int64(le.Uint64(counter)), false)
		for i, a := range p.output() {
			v := docField(row, a.name)
			if v.k != kInt {
				return nil, ErrCorrupt
			}
			g.sums[i].add(v.i, false)
		}
	}
	d.groups[string(key)] = g
	d.bytes += 128 + len(key)*2 + len(g.sums)*16 + len(id.s) + partBytes
	return g, d.limit()
}
func (d *computedCollectionDelta) contribute(p *computedCollectionPlan, doc []byte, negative bool) error {
	if doc == nil || !p.matches(doc) {
		return nil
	}
	g, err := d.group(p, doc)
	if err != nil {
		return err
	}
	g.members.add(1, negative)
	for i, a := range p.group.accs {
		get := func(path string) value { return pathValue(value{k: kDoc, s: doc}, path) }
		if a.where != nil && !matchAll(a.where, get) {
			continue
		}
		slot := i
		if p.slots != nil {
			slot = p.slots[i]
		}
		v := a.c
		if a.field != "" {
			v = get(a.field)
		}
		if v.k == kMissing || v.k == kNull {
			continue
		}
		if a.op == accCountDistinct {
			if err := d.reference(g, slot, v, negative); err != nil {
				return err
			}
			continue
		}
		if a.op == accCountNonNull {
			g.sums[slot].add(1, negative)
			continue
		}
		if v.k != kInt {
			return fmt.Errorf("%w: sum %q needs int64 values", ErrComputedCollection, a.name)
		}
		g.sums[slot].add(v.i, negative)
	}
	return nil
}
func (d *computedCollectionDelta) change(p *computedCollectionPlan, uk, old, new []byte) error {
	if p.group != nil {
		if err := d.contribute(p, old, true); err != nil {
			return err
		}
		return d.contribute(p, new, false)
	}
	key := p.target.ikey(nil, uk)
	if new == nil || !p.matches(new) {
		if old == nil || !p.matches(old) {
			return nil
		}
		return d.batch.add(key, nil, true)
	}
	projected, err := p.projectDoc(new)
	if err != nil {
		return err
	}
	return d.batch.add(key, projected, false)
}
func (d *computedCollectionDelta) finish(ctx context.Context) error {
	if err := d.finishReferences(ctx); err != nil {
		return err
	}
	keys := make([]string, 0, len(d.groups))
	for k := range d.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		g := d.groups[key]
		members, err := g.members.int64()
		if err != nil {
			return err
		}
		if members < 0 {
			return ErrCorrupt
		}
		_, uk, _ := splitKey(g.key)
		counterKey := computedViewKey('r', g.plan.target.id, uk)
		if members == 0 {
			for _, sum := range g.sums {
				n, err := sum.int64()
				if err != nil || n != 0 {
					return ErrCorrupt
				}
			}
			if err = d.batch.add(g.key, nil, true); err != nil {
				return err
			}
			if err = d.batch.add(counterKey, nil, true); err != nil {
				return err
			}
		} else {
			var builder DocBuilder
			if g.parts == nil {
				addComputedGroupValue(&builder, "_id", g.id)
			} else {
				var tuple DocBuilder
				for i, name := range g.plan.group.idNames {
					addComputedGroupValue(&tuple, name, g.parts[i])
					addComputedGroupValue(&builder, name, g.parts[i])
				}
				builder.Doc("_id", tuple.Bytes())
			}
			for i, a := range g.plan.output() {
				n, err := g.sums[i].int64()
				if err != nil {
					return err
				}
				builder.Int(a.name, n)
			}
			if err = d.batch.add(g.key, builder.Bytes(), false); err != nil {
				return err
			}
			if err = d.batch.add(counterKey, le.AppendUint64(nil, uint64(members)), false); err != nil {
				return err
			}
		}
		if err = d.limit(); err != nil {
			return err
		}
	}
	return nil
}

func addComputedGroupValue(builder *DocBuilder, name string, v value) {
	switch v.k {
	case kNull:
		builder.Null(name)
	case kBool:
		builder.Bool(name, v.i != 0)
	case kInt:
		builder.Int(name, v.i)
	case kFloat:
		builder.Float(name, v.f)
	case kString:
		builder.String(name, string(v.s))
	}
}

func (b *Batch) addComputedCollectionOpsLocked() error {
	db := b.db
	derived := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1}}
	delta := newComputedCollectionDelta(db, derived)
	pending := map[string][]byte{}
	for off := 0; off < len(b.buf); {
		plen := int(le.Uint32(b.buf[off:]))
		rec := b.buf[off+8 : off+8+plen]
		off += 8 + plen
		klen := int(le.Uint16(rec[1:]))
		key, val := rec[3:3+klen], rec[3+klen:]
		cid, uk, ok := splitKey(key)
		if !ok {
			continue
		}
		c, ok := db.collectionByID(cid)
		if !ok {
			return ErrNoSuchCollection
		}
		if len(c.state.dependents) == 0 {
			continue
		}
		old, seen := pending[string(key)]
		if !seen {
			var found bool
			var err error
			old, found, err = db.findLocked(view{db, nil}, key, nil)
			if err != nil {
				return err
			}
			if !found {
				old = nil
			}
		}
		var new []byte
		if rec[0]&flagDelete == 0 {
			new = val
			if err := validateComputedDoc(new, 0); err != nil {
				return fmt.Errorf("%w: %w", ErrComputedCollection, err)
			}
		}
		for _, plan := range c.state.dependents {
			// The online builder owns the unscanned suffix. Writes behind
			// its durable cursor maintain the already accumulated prefix.
			if plan.build != nil && !plan.progress.Done && bytes.Compare(uk, plan.progress.After) > 0 {
				continue
			}
			if err := delta.change(plan, uk, old, new); err != nil {
				return err
			}
		}
		if !seen {
			delta.bytes += len(key) + len(old) + 64
		}
		pending[string(key)] = new
		if err := delta.limit(); err != nil {
			return err
		}
	}
	if err := delta.finish(context.Background()); err != nil {
		return err
	}
	if derived.n > 0 {
		if len(b.buf)+len(derived.buf) > computedCollectionLimit {
			return ErrComputedCollectionLimit
		}
		b.lastOff = len(b.buf) + derived.lastOff
		b.buf = append(b.buf, derived.buf...)
		b.n += derived.n
	}
	return nil
}
