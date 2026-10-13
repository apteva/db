package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// IndexField specifies one component in a lexicographic compound index.
type IndexField struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc,omitempty"`
}

var ErrIndexValue = errors.New("aptevadb: compound indexes require finite scalar fields")
var ErrIndexOrder = errors.New("aptevadb: ordered access requires an exact index; rebuild the legacy index")

func encodeExactIndexValue(dst []byte, v value) []byte {
	if !v.isNum() {
		return encodeIndexValue(dst, v)
	}
	dst = append(dst, indexTag(v))
	negative, exponent, significand := numericParts(v)
	sign := byte(2)
	if significand == 0 && exponent == 0 {
		sign = 1
	} else if negative {
		sign = 0
		exponent = ^exponent
		significand = ^significand
	}
	dst = append(dst, sign)
	dst = binary.BigEndian.AppendUint16(dst, exponent)
	return binary.BigEndian.AppendUint64(dst, significand)
}
func indexScalarPrefix(d indexDef, dst []byte, v value) []byte {
	if d.Encoding == 2 {
		return encodeExactIndexValue(dst, v)
	}
	return encodeIndexValue(dst, v)
}
func validIndexDef(d indexDef) bool {
	if d.Field == "" || !utf8.ValidString(d.Field) || d.Encoding != 0 && d.Encoding != 2 || len(d.Fields) > 16 {
		return false
	}
	if len(d.Fields) > 0 && (d.Encoding != 2 || d.Fields[0].Field != d.Field) {
		return false
	}
	seen := map[string]bool{}
	for _, f := range d.Fields {
		if f.Field == "" || !utf8.ValidString(f.Field) || seen[f.Field] {
			return false
		}
		seen[f.Field] = true
	}
	return true
}

// indexEntries includes the complete encoded tuple followed by the primary key.
// Compound indexes normalize missing fields to null, unless Sparse is set.
// The boolean tells unique validation whether this tuple has a null component.
func indexEntries(doc []byte, d indexDef, pk []byte, fn func([]byte, bool) error) error {
	if doc == nil {
		return nil
	}
	if _, _, ok := docDir(doc); !ok {
		return nil
	}
	if len(d.Fields) == 0 {
		var failure error
		indexedValues(doc, d.Field, func(v value) {
			if failure != nil {
				return
			}
			if checkUniqueValue(v) != nil {
				failure = ErrIndexValue
				return
			}
			ek := encodeExactIndexValue(indexPrefix(d.ID), v)
			ek = appendExactPrimaryKey(ek, pk)
			failure = fn(ek, v.k == kNull)
		})
		return failure
	}
	ek := indexPrefix(d.ID)
	nullable := false
	for _, f := range d.Fields {
		v := pathValue(value{k: kDoc, s: doc}, f.Field)
		if v.k == kMissing {
			if d.Sparse {
				return nil
			}
			v.k = kNull
		}
		if !indexable(v) || checkUniqueValue(v) != nil {
			return fmt.Errorf("%w: %s", ErrIndexValue, f.Field)
		}
		nullable = nullable || v.k == kNull
		start := len(ek)
		ek = encodeExactIndexValue(ek, v)
		if f.Desc {
			for i := start; i < len(ek); i++ {
				ek[i] = ^ek[i]
			}
		}
	}
	ek = appendExactPrimaryKey(ek, pk)
	return fn(ek, nullable)
}

func (c *Collection) CreateCompoundIndex(ctx context.Context, name string, fields []IndexField, options ...IndexOptions) error {
	if len(options) > 1 {
		return ErrPipeline
	}
	opts := IndexOptions{}
	if len(options) == 1 {
		opts = options[0]
	}
	return c.createExactIndex(ctx, name, fields, opts, true)
}
func (db *DB) CreateCompoundIndex(ctx context.Context, name string, fields []IndexField, options ...IndexOptions) error {
	return db.def.CreateCompoundIndex(ctx, name, fields, options...)
}

func (c *Collection) createExactIndex(ctx context.Context, name string, fields []IndexField, opts IndexOptions, compound bool) error {
	if c.snap != nil {
		return ErrReadOnly
	}
	if len(fields) == 0 || !validIndexName(name) {
		return ErrPipeline
	}
	d := indexDef{Coll: c.id, Name: name, Field: fields[0].Field, Encoding: 2, Unique: opts.Unique, NullsDistinct: opts.NullsDistinct, Sparse: opts.Sparse}
	if compound {
		d.Fields = append([]IndexField(nil), fields...)
	}
	if !validIndexDef(d) {
		return ErrPipeline
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
	for _, existing := range db.indexDefs() {
		if existing.Coll == c.id && existing.Name == name {
			db.mu.Unlock()
			return ErrIndexExists
		}
	}
	db.idx.mu.Lock()
	d.ID = db.idx.nextID
	db.idx.nextID++
	db.idx.mu.Unlock()
	b := &Batch{coll: db.def, batchState: &batchState{db: db, internal: true, lastOff: -1, limit: MaxTransactionBytes}}
	owners := map[string]string{}
	err := db.computedScanLocked(ctx, c.pfx, c.end, func(key, doc []byte) error {
		pk := key[len(c.pfx):]
		return indexEntries(doc, d, pk, func(ek []byte, nullable bool) error {
			if len(ek) > MaxKeySize {
				return ErrKeySize
			}
			if d.Unique && !(d.NullsDistinct && nullable) {
				vk := string(indexValuePart(ek))
				owner, exists := owners[vk]
				if exists && owner != string(pk) {
					return fmt.Errorf("%w: %s", ErrDuplicateKey, name)
				}
				if int64(len(b.buf))+int64(len(owners))*128+int64(len(ek))*2 > MaxTransactionBytes {
					return ErrTransactionTooLarge
				}
				owners[vk] = string(pk)
			}
			return b.add(ek, nil, false)
		})
	})
	if err == nil {
		meta, _ := json.Marshal(d)
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
	db.idx.defs = withDef(db.idx.defs, d)
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

func (db *DB) validateExactUniqueLocked(pending map[string][]byte, d indexDef) error {
	owners := map[string]string{}
	for internal, doc := range pending {
		cid, pk, ok := splitKey([]byte(internal))
		if !ok || cid != d.Coll || doc == nil {
			continue
		}
		err := indexEntries(doc, d, pk, func(ek []byte, nullable bool) error {
			if d.NullsDistinct && nullable {
				return nil
			}
			vk := indexValuePart(ek)
			owner, exists := owners[string(vk)]
			if exists && owner != string(pk) {
				return fmt.Errorf("%w: %s", ErrDuplicateKey, d.Name)
			}
			owners[string(vk)] = string(pk)
			return db.scanLocked(context.Background(), vk, prefixEnd(vk), true, func(existing, _ []byte) error {
				otherPK := primaryKeyOfDef(d, existing)
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
				return indexEntries(other, d, otherPK, func(candidate []byte, _ bool) error {
					cv := indexValuePart(candidate)
					if bytes.Equal(cv, vk) {
						return fmt.Errorf("%w: %s", ErrDuplicateKey, d.Name)
					}
					return nil
				})
			})
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func verifyExactIndex(ctx context.Context, c *Collection, d indexDef) error {
	entries := map[string]bool{}
	owners := map[string]string{}
	pfx := indexPrefix(d.ID)
	if err := c.rawScan(pfx, prefixEnd(pfx), func(k, _ []byte) bool { entries[string(k)] = true; return true }); err != nil {
		return err
	}
	var failure error
	err := c.Scan(nil, nil, func(pk, doc []byte) bool {
		if failure = ctx.Err(); failure != nil {
			return false
		}
		failure = indexEntries(doc, d, pk, func(ek []byte, nullable bool) error {
			if _, exists := entries[string(ek)]; !exists {
				return fmt.Errorf("%w: index %s missing entry", ErrCorrupt, d.Name)
			}
			if d.Unique && !(d.NullsDistinct && nullable) {
				vk := string(indexValuePart(ek))
				if old, ok := owners[vk]; ok && old != string(pk) {
					return ErrDuplicateKey
				}
				owners[vk] = string(pk)
			}
			// Repeated scalar values in one array name the same physical entry.
			entries[string(ek)] = false
			return nil
		})
		return failure == nil
	})
	if err != nil {
		return err
	}
	if failure != nil {
		return failure
	}
	for _, remaining := range entries {
		if remaining {
			return fmt.Errorf("%w: index %s orphan entry", ErrCorrupt, d.Name)
		}
	}
	return nil
}

func appendExactPrimaryKey(k, pk []byte) []byte {
	start := len(k)
	for _, b := range pk {
		if b == 0 {
			k = append(k, 0, 255)
		} else {
			k = append(k, b)
		}
	}
	k = append(k, 0, 0)
	return binary.BigEndian.AppendUint16(k, uint16(len(k)-start))
}
func indexValuePart(k []byte) []byte {
	n := int(binary.BigEndian.Uint16(k[len(k)-2:]))
	return k[:len(k)-2-n]
}
func primaryKeyOfDef(d indexDef, k []byte) []byte {
	raw := primaryKeyOf(k)
	if d.Encoding != 2 {
		return raw
	}
	if len(raw) < 2 || raw[len(raw)-2] != 0 || raw[len(raw)-1] != 0 {
		return nil
	}
	raw = raw[:len(raw)-2]
	if bytes.IndexByte(raw, 0) < 0 {
		return raw
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if b == 0 {
			if i+1 >= len(raw) || raw[i+1] != 255 {
				return nil
			}
			i++
		}
		out = append(out, b)
	}
	return out
}
