package db

import (
	"context"
	"encoding/binary"
)

const computedDistinctTag = 'D'

type computedReference struct {
	group  *computedGroupState
	slot   int
	before int64
	after  computedWide
}

const computedDistinctPrefixOverhead = 9

func computedDistinctPrefix(id uint32, group []byte, slot int) []byte {
	key := make([]byte, 0, computedDistinctPrefixOverhead+len(group))
	return appendComputedDistinctPrefix(key, id, group, slot)
}

func appendComputedDistinctPrefix(key []byte, id uint32, group []byte, slot int) []byte {
	key = append(key, reservedPrefix, computedDistinctTag)
	key = binary.BigEndian.AppendUint32(key, id)
	key = binary.BigEndian.AppendUint16(key, uint16(len(group)))
	key = append(key, group...)
	return append(key, byte(slot))
}

func (d *computedCollectionDelta) reference(g *computedGroupState, slot int, v value, negative bool) error {
	_, group, _ := splitKey(g.key)
	key := append(computedDistinctPrefix(g.plan.target.id, group, slot), distinctKey(nil, v)...)
	if len(key) > MaxIndexKeySize {
		return ErrKeySize
	}
	ref := d.references[string(key)]
	if ref == nil {
		data, found, err := d.db.findLocked(view{d.db, nil}, key, nil)
		if err != nil {
			return err
		}
		ref = &computedReference{group: g, slot: slot}
		if found {
			if len(data) != 8 || int64(le.Uint64(data)) <= 0 {
				return ErrCorrupt
			}
			ref.before = int64(le.Uint64(data))
		}
		ref.after.add(ref.before, false)
		if d.references == nil {
			d.references = make(map[string]*computedReference)
		}
		d.references[string(key)] = ref
		d.bytes += 128 + len(key)
	}
	ref.after.add(1, negative)
	return d.limit()
}

func (d *computedCollectionDelta) finishReferences(ctx context.Context) error {
	for key, ref := range d.references {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := ref.after.int64()
		if err != nil {
			return err
		}
		if n < 0 {
			return ErrCorrupt
		}
		if n == ref.before {
			continue
		}
		if ref.before == 0 {
			ref.group.sums[ref.slot].add(1, false)
		} else if n == 0 {
			ref.group.sums[ref.slot].add(1, true)
		}
		var data []byte
		if n > 0 {
			data = le.AppendUint64(nil, uint64(n))
		}
		if err := d.batch.add([]byte(key), data, n == 0); err != nil {
			return err
		}
		if err := d.limit(); err != nil {
			return err
		}
	}
	return nil
}

// ScanComputedDistinct enumerates exact, type-tagged identities for one
// stored group and measure in the collection's snapshot. The identity is
// borrowed until yield returns. Equal identities have identical encodings.
func (c *Collection) ScanComputedDistinct(ctx context.Context, group []byte, measure string, yield func([]byte) bool) error {
	if err := c.check(); err != nil {
		return err
	}
	p := c.state.derived
	if p == nil || p.build == nil || yield == nil || len(group) == 0 || len(group) > MaxKeySize {
		return ErrComputedCollection
	}
	slot := -1
	for i, a := range p.build.measures {
		if a.name == measure && a.op == accCountDistinct {
			slot = i
			break
		}
	}
	if slot < 0 {
		return ErrComputedCollection
	}
	ctx = orBackground(ctx)
	if c.snap == nil {
		s, err := c.db.readSnapshot()
		if err != nil {
			return err
		}
		defer s.Close()
		cc, err := s.Collection(c.name)
		if err != nil {
			return err
		}
		return cc.ScanComputedDistinct(ctx, group, measure, yield)
	}
	prefix := computedDistinctPrefix(c.id, group, slot)
	var scanErr error
	err := c.db.scan(c.view(), prefix, prefixEnd(prefix), func(key, ref []byte) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		if len(key) <= len(prefix) || len(ref) != 8 || int64(le.Uint64(ref)) <= 0 {
			scanErr = ErrCorrupt
			return false
		}
		return yield(key[len(prefix):])
	})
	if err != nil {
		return err
	}
	if scanErr != nil {
		return scanErr
	}
	return ctx.Err()
}
