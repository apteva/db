package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
)

// DurableOptions selects acknowledged writes that survive power loss on
// supported local filesystems and drives honoring flushes. Options{} is async.
func DurableOptions() Options { return Options{Sync: true, SyncMode: SyncFull} }

// Encode marshals a Go struct/map into the engine's binary document format.
func Encode(document any) ([]byte, error) {
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return EncodeJSON(nil, raw)
}

// Decode decodes a binary document. Maps preserve int64/float64 values;
// typed fields use encoding/json rules, with json.Number in interface fields.
func Decode(document []byte, out any) error {
	m, err := DecodeDocument(document)
	if err != nil {
		return err
	}
	switch destination := out.(type) {
	case *map[string]any:
		if destination == nil {
			return ErrDocument
		}
		*destination = m
		return nil
	case *any:
		if destination == nil {
			return ErrDocument
		}
		*destination = m
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(out)
}
func (c *Collection) InsertDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Insert(key, v)
}
func (c *Collection) UpsertDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Upsert(key, v)
}
func (c *Collection) ReplaceDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Replace(key, v)
}
func (c *Collection) FindDocument(key []byte, out any) (bool, error) {
	v, ok, err := c.Find(key, nil)
	if err != nil || !ok {
		return ok, err
	}
	return true, Decode(v, out)
}
func (c *TxCollection) InsertDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Insert(key, v)
}
func (c *TxCollection) UpsertDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Upsert(key, v)
}
func (c *TxCollection) ReplaceDocument(key []byte, document any) error {
	v, err := Encode(document)
	if err != nil {
		return err
	}
	return c.Replace(key, v)
}
func (c *TxCollection) FindDocument(key []byte, out any) (bool, error) {
	v, ok, err := c.Find(key, nil)
	if err != nil || !ok {
		return ok, err
	}
	return true, Decode(v, out)
}

// QueryOptions controls a key-ordered streaming query. Zero Limit is unlimited.
type QueryOptions struct {
	Skip, Limit    int
	Sort           []IndexField
	Hint           string
	KeyDescending  bool
	MaxMemoryBytes int64
	Stats          *QueryStats
}

// Query streams documents matching a JSON filter (the $match syntax) in key
// order. It uses an applicable index and one snapshot for the whole operation.
// No result array is retained; yield returns false to stop. Callback bytes are
// valid only during the callback. Aggregation still materializes its results.
func (c *Collection) Query(ctx context.Context, filter []byte, opts QueryOptions, yield func(key, document []byte) bool) error {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateQueryOptions(opts); err != nil {
		return err
	}
	if c.snap == nil {
		s, cc, err := c.readView()
		if err != nil {
			return err
		}
		defer s.Close()
		return cc.Query(ctx, filter, opts, yield)
	}
	preds, err := queryPredicates(filter)
	if err != nil {
		return err
	}
	access, err := c.chooseQueryAccess(preds, opts)
	if err != nil {
		return err
	}
	if len(opts.Sort) > 0 || opts.Hint != "" || opts.KeyDescending || access.info.Index != "" {
		return c.queryAdvanced(ctx, preds, opts, access, yield)
	}
	stats := opts.Stats
	if stats == nil {
		stats = &QueryStats{}
	}
	*stats = QueryStats{Plan: access.info}
	var src Source = c.Source(nil, nil)
	budget, _ := queryBudget(opts)
	if ip, ok := c.indexPlanBudget(preds, budget); ok {
		stats.Plan.Access = "index-filter"
		stats.Plan.Index = ip.name
		stats.PeakMemoryBytes = ip.memory
		sort.Slice(ip.pks, func(i, j int) bool { return bytes.Compare(ip.pks[i], ip.pks[j]) < 0 })
		src = c.rangesSource(ctx, ip)
	}
	skipped, emitted := 0, 0
	var cancelled error
	err = src(func(key, doc []byte) bool {
		if cancelled = ctx.Err(); cancelled != nil {
			return false
		}
		stats.Examined++
		stats.DocumentFetches++
		if _, _, ok := docDir(doc); !ok {
			return true
		}
		if !matchAll(preds, func(field string) value { return pathValue(value{k: kDoc, s: doc}, field) }) {
			return true
		}
		if skipped < opts.Skip {
			skipped++
			return true
		}
		emitted++
		stats.Returned++
		return yield(key, doc) && (opts.Limit == 0 || emitted < opts.Limit)
	})
	if err != nil {
		return err
	}
	return cancelled
}

// FindOne decodes the first matching document into out; absence is (false,nil).
func (c *Collection) FindOne(ctx context.Context, filter []byte, out any) (bool, error) {
	found := false
	var decodeErr error
	err := c.Query(ctx, filter, QueryOptions{Limit: 1}, func(_, doc []byte) bool { found = true; decodeErr = Decode(doc, out); return false })
	if err != nil {
		return false, err
	}
	return found, decodeErr
}

// UpdateDocument runs a safe read/modify/replace transaction once. ErrConflict
// means the caller should retry; fn must not retain or mutate database handles.
func (c *Collection) UpdateDocument(ctx context.Context, key []byte, fn func(map[string]any) error) error {
	if c.snap != nil {
		return ErrReadOnly
	}
	return c.db.Update(ctx, func(tx *Tx) error {
		tc, err := tx.Collection(c.name)
		if err != nil {
			return err
		}
		// A drop/recreation must not silently redirect an old handle to a new ID.
		if tc.c.id != c.id {
			return ErrNoSuchCollection
		}
		v, ok, err := tc.Find(key, nil)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		m, err := DecodeDocument(v)
		if err != nil {
			return err
		}
		if err = fn(m); err != nil {
			return err
		}
		return tc.ReplaceDocument(key, m)
	})
}

var jsonWriteBuffers = sync.Pool{New: func() any { b := []byte(nil); return &b }}

func (c *Collection) writeJSON(key, doc []byte, req writeRequirement) error {
	holder := jsonWriteBuffers.Get().(*[]byte)
	enc, err := EncodeJSON((*holder)[:0], doc)
	defer func() {
		if cap(enc) <= 64<<10 {
			*holder = enc[:0]
			jsonWriteBuffers.Put(holder)
		}
	}()
	if err != nil {
		return err
	}
	if req == 0 {
		return c.Upsert(key, enc)
	}
	return c.conditionalWrite(key, enc, req)
}

func queryPredicates(filter []byte) ([]pred, error) {
	if len(filter) == 0 {
		filter = []byte(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(filter))
	decoder.UseNumber()
	var match map[string]any
	if err := decoder.Decode(&match); err != nil || match == nil {
		return nil, fmt.Errorf("%w: filter must be an object", ErrPipeline)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, ErrPipeline
	}
	preds, err := parseMatch(match)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPipeline, err)
	}
	return preds, nil
}
