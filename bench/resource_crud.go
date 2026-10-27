package main

import (
	"fmt"
)

// Only the resource runner uses these adapters. Scan delivers both key and full
// value through each engine's public Go API; legacy count-only scans stay intact.
func resourceCRUD(k kv) (func(func([]byte, []byte) bool) error, func([]byte) error, error) {
	switch d := k.(type) {
	case *ours:
		return func(fn func([]byte, []byte) bool) error { return d.db.Scan(nil, nil, fn) }, d.db.Delete, nil
	case *oursBatch:
		scan := func(fn func([]byte, []byte) bool) error {
			if err := d.b.Commit(); err != nil {
				return err
			}
			return d.db.Scan(nil, nil, fn)
		}
		del := func(key []byte) error {
			if err := d.b.Delete(key); err != nil {
				return err
			}
			if d.b.Len() >= d.n {
				return d.b.Commit()
			}
			return nil
		}
		return scan, del, nil
	case *peb:
		scan := func(fn func([]byte, []byte) bool) error {
			if err := d.flushBatch(); err != nil {
				return err
			}
			it, err := d.db.NewIter(nil)
			if err != nil {
				return err
			}
			defer it.Close()
			for ok := it.First(); ok; ok = it.Next() {
				v, err := it.ValueAndErr()
				if err != nil {
					return err
				}
				if !fn(it.Key(), v) {
					break
				}
			}
			return it.Error()
		}
		del := func(key []byte) error {
			if d.bsize == 0 {
				return d.db.Delete(key, d.wo)
			}
			if d.batch == nil {
				d.batch = d.db.NewBatch()
			}
			if err := d.batch.Delete(key, nil); err != nil {
				return err
			}
			d.inb++
			if d.inb >= d.bsize {
				return d.flushBatch()
			}
			return nil
		}
		return scan, del, nil
	case *sqliteKV:
		deletion := d.c.Prep("DELETE FROM kv WHERE k = ?")
		scan := func(fn func([]byte, []byte) bool) error {
			if err := d.endTxn(); err != nil {
				return err
			}
			st := d.c.Prep("SELECT k, v FROM kv ORDER BY k")
			defer st.Reset()
			var key, val []byte
			for {
				ok, err := st.Step()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
				kl, vl := st.ColumnLen(0), st.ColumnLen(1)
				if cap(key) < kl {
					key = make([]byte, kl)
				}
				key = key[:kl]
				if cap(val) < vl {
					val = make([]byte, vl)
				}
				val = val[:vl]
				if st.ColumnBytes(0, key) != kl || st.ColumnBytes(1, val) != vl {
					return fmt.Errorf("short SQLite scan value")
				}
				if !fn(key, val) {
					return nil
				}
			}
		}
		del := func(key []byte) error {
			if d.txn > 1 && !d.open {
				if _, err := d.begin.Step(); err != nil {
					return err
				}
				if err := d.begin.Reset(); err != nil {
					return err
				}
				d.open = true
			}
			deletion.BindBytes(1, key)
			if _, err := deletion.Step(); err != nil {
				return err
			}
			if err := deletion.Reset(); err != nil {
				return err
			}
			if d.open {
				d.inTxn++
				if d.inTxn >= d.txn {
					return d.endTxn()
				}
			}
			return nil
		}
		return scan, del, nil
	}
	return nil, nil, fmt.Errorf("resource CRUD not supported for %T", k)
}
