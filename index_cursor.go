package db

import (
	"bytes"
	"context"
	"fmt"
)

// IndexCursorOptions bounds tuples in the index's physical order, including its
// mixed directions. Prefix restricts leading components. Zero bounds are open.
type IndexCursorOptions struct {
	Prefix, Lower, Upper           []any
	LowerExclusive, UpperInclusive bool
}

// IndexCursor streams physical index entries at one snapshot. A compound index
// has one entry per document; a scalar multikey index can repeat a document.
// Key, Value and Position are borrowed until the next movement. Close promptly.
type IndexCursor struct {
	raw           *Cursor
	collection    *Collection
	owned         *Snapshot
	ctx           context.Context
	def           indexDef
	lower, upper  []byte
	doc           []byte
	key           []byte
	valid, closed bool
	err           error
}

func encodeIndexTuple(d indexDef, values []any) ([]byte, error) {
	fields := d.Fields
	count := len(fields)
	if count == 0 {
		count = 1
	}
	if len(values) > count {
		return nil, ErrPipeline
	}
	k := indexPrefix(d.ID)
	for i, item := range values {
		if err := validateFilterNumbers(item); err != nil {
			return nil, err
		}
		v := valueOf(item)
		if !indexable(v) || checkUniqueValue(v) != nil {
			return nil, ErrIndexValue
		}
		start := len(k)
		k = encodeExactIndexValue(k, v)
		if len(fields) > 0 && fields[i].Desc {
			for j := start; j < len(k); j++ {
				k[j] = ^k[j]
			}
		}
	}
	if len(k) > MaxKeySize {
		return nil, ErrKeySize
	}
	return k, nil
}
func (c *Collection) NewIndexCursor(ctx context.Context, name string, opts IndexCursorOptions) (*IndexCursor, error) {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cc := c
	var owned *Snapshot
	if c.snap == nil {
		s, bound, err := c.readView()
		if err != nil {
			return nil, err
		}
		owned, cc = s, bound
	}
	fail := func(err error) (*IndexCursor, error) {
		if owned != nil {
			owned.Close()
		}
		return nil, err
	}
	var d indexDef
	found := false
	for _, candidate := range cc.indexDefs() {
		if candidate.Name == name {
			d, found = candidate, true
			break
		}
	}
	if !found {
		return fail(ErrNoSuchIndex)
	}
	if d.Encoding != 2 {
		return fail(ErrIndexOrder)
	}
	lo := indexPrefix(d.ID)
	hi := prefixEnd(lo)
	if opts.Prefix != nil {
		prefix, err := encodeIndexTuple(d, opts.Prefix)
		if err != nil {
			return fail(err)
		}
		lo, hi = prefix, prefixEnd(prefix)
	}
	if opts.Lower != nil {
		bound, err := encodeIndexTuple(d, opts.Lower)
		if err != nil {
			return fail(err)
		}
		if opts.LowerExclusive {
			bound = prefixEnd(bound)
		}
		if bytes.Compare(bound, lo) > 0 {
			lo = bound
		}
	}
	if opts.Upper != nil {
		bound, err := encodeIndexTuple(d, opts.Upper)
		if err != nil {
			return fail(err)
		}
		if opts.UpperInclusive {
			bound = prefixEnd(bound)
		}
		if bytes.Compare(bound, hi) < 0 {
			hi = bound
		}
	}
	fake := *cc
	fake.pfx = indexPrefix(d.ID)
	fake.end = prefixEnd(fake.pfx)
	raw := fake.NewCursor()
	return &IndexCursor{raw: raw, collection: cc, owned: owned, ctx: ctx, def: d, lower: lo, upper: hi}, nil
}
func (c *IndexCursor) read(ok bool) bool {
	c.valid = false
	if c.closed || c.err != nil {
		return false
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		return false
	}
	if !ok {
		c.err = c.raw.Err()
		return false
	}
	ek := c.raw.ikey
	if bytes.Compare(ek, c.lower) < 0 || bytes.Compare(ek, c.upper) >= 0 {
		return false
	}
	pk := primaryKeyOfDef(c.def, ek)
	if len(pk) == 0 {
		c.err = fmt.Errorf("%w: index primary key", ErrCorrupt)
		return false
	}
	c.key = append(c.key[:0], pk...)
	doc, found, err := c.collection.Find(pk, c.doc)
	if err != nil {
		c.err = err
		return false
	}
	if !found {
		c.err = fmt.Errorf("%w: index document missing", ErrCorrupt)
		return false
	}
	c.doc = doc
	c.valid = true
	return true
}
func (c *IndexCursor) First() bool {
	if c.closed {
		return false
	}
	return c.read(c.raw.move(c.lower, true, true))
}
func (c *IndexCursor) Last() bool {
	if c.closed {
		return false
	}
	return c.read(c.raw.move(c.upper, false, false))
}
func (c *IndexCursor) Next() bool {
	if !c.valid {
		return false
	}
	return c.read(c.raw.Next())
}
func (c *IndexCursor) Prev() bool {
	if !c.valid {
		return false
	}
	return c.read(c.raw.Prev())
}
func (c *IndexCursor) Seek(values []any) bool {
	if c.closed {
		return false
	}
	k, err := encodeIndexTuple(c.def, values)
	if err != nil {
		c.err = err
		c.valid = false
		return false
	}
	if bytes.Compare(k, c.lower) < 0 {
		k = c.lower
	}
	return c.read(c.raw.move(k, true, true))
}
func (c *IndexCursor) SeekAfter(position []byte) bool  { return c.seekPosition(position, true) }
func (c *IndexCursor) SeekBefore(position []byte) bool { return c.seekPosition(position, false) }
func (c *IndexCursor) seekPosition(position []byte, forward bool) bool {
	if c.closed {
		return false
	}
	if !bytes.HasPrefix(position, indexPrefix(c.def.ID)) || len(position) > MaxKeySize || len(primaryKeyOfDef(c.def, position)) == 0 {
		c.err = ErrPipeline
		c.valid = false
		return false
	}
	return c.read(c.raw.move(position, forward, false))
}
func (c *IndexCursor) Valid() bool   { return c.valid }
func (c *IndexCursor) Key() []byte   { return c.key }
func (c *IndexCursor) Value() []byte { return c.doc }
func (c *IndexCursor) Position() []byte {
	if !c.valid {
		return nil
	}
	return c.raw.ikey
}
func (c *IndexCursor) Err() error { return c.err }
func (c *IndexCursor) Close() error {
	if !c.closed {
		c.closed = true
		c.valid = false
		c.raw.Close()
		if c.owned != nil {
			c.owned.Close()
			c.owned = nil
		}
		c.doc, c.key = nil, nil
	}
	return nil
}

func exactEntryKey(d indexDef, v value, pk []byte) []byte {
	k := encodeExactIndexValue(indexPrefix(d.ID), v)
	return appendExactPrimaryKey(k, pk)
}
