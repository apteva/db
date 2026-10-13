package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	db "github.com/apteva/db"
	"github.com/cockroachdb/pebble/v2"
)

// Native indexes, application-maintained Pebble indexes and SQL indexes use
// identical ASCII key/value/document payloads. Each page checks every row
// against an independent ordinal model. All writes are asynchronous batches.
func runWideIndexBench(dir string, n, reps int) {
	if n < 500 {
		panic("wide index benchmark requires n >= 500")
	}
	keys, values, docs := make([][]byte, n), make([]string, n), make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%08d", i) + strings.Repeat("k", 1492))
		values[i] = fmt.Sprintf("%08d", i) + strings.Repeat("v", 2992)
		var b db.DocBuilder
		b.String("v", values[i])
		docs[i] = append([]byte(nil), b.Bytes()...)
	}
	var measurements []compatibilityMeasurement
	record := func(engine, phase string, trial, ops int, fn func()) {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		start := time.Now()
		fn()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&b)
		m := compatibilityMeasurement{Engine: engine, Phase: phase, Trial: trial, Ops: ops, NsPerOp: float64(elapsed.Nanoseconds()) / float64(ops), AllocatedBytesPerOp: float64(b.TotalAlloc-a.TotalAlloc) / float64(ops)}
		measurements = append(measurements, m)
		fmt.Printf("%-16s %-24s %.3f ns/op %.1f Go bytes/op\n", engine, phase, m.NsPerOp, m.AllocatedBytesPerOp)
	}
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	verify := func(i int, k, v []byte) {
		if !bytes.Equal(k, keys[i]) || !bytes.Equal(v, docs[i]) {
			panic(fmt.Sprintf("wide page row mismatch %d", i))
		}
	}
	const queries = 100
	for trial := 1; trial <= reps; trial++ {
		for _, engine := range []string{"ours", "pebble-manual", "sqlite-c", "sqlite-go"} {
			ed := filepath.Join(dir, fmt.Sprintf("wide-%s-%d", engine, trial))
			must(os.MkdirAll(ed, 0755))
			switch engine {
			case "ours":
				d, err := db.Open(ed, db.Options{})
				must(err)
				c, err := d.Collection("rows")
				must(err)
				record(engine, "load", trial, n, func() {
					b := c.NewBatch()
					for i := range keys {
						must(b.Upsert(keys[i], docs[i]))
						if (i+1)%1000 == 0 {
							must(b.Commit())
						}
					}
					must(b.Commit())
				})
				record(engine, "build-wide-index", trial, n, func() { must(c.CreateCompoundIndex(context.Background(), "v", []db.IndexField{{Field: "v"}})) })
				must(d.Checkpoint())
				record(engine, "ordered-100-row-page", trial, queries, func() {
					for j := 0; j < queries; j++ {
						start := j * 17 % (n - 101)
						cur, err := c.NewIndexCursor(context.Background(), "v", db.IndexCursorOptions{})
						must(err)
						count := 0
						for ok := cur.Seek([]any{values[start]}); ok && count < 100; ok = cur.Next() {
							verify(start+count, cur.Key(), cur.Value())
							count++
						}
						must(cur.Err())
						must(cur.Close())
						if count != 100 {
							panic(count)
						}
					}
				})
				cur, err := c.NewIndexCursor(context.Background(), "v", db.IndexCursorOptions{})
				must(err)
				record(engine, "reused-resume-100-row-page", trial, queries, func() {
					for j := 0; j < queries; j++ {
						start := j * 17 % (n - 301)
						if !cur.Seek([]any{values[start]}) {
							panic("seek")
						}
						pos := append([]byte(nil), cur.Position()...)
						if !cur.Seek([]any{values[start+200]}) {
							panic("later seek")
						}
						count := 0
						for ok := cur.SeekAfter(pos); ok && count < 100; ok = cur.Next() {
							verify(start+count+1, cur.Key(), cur.Value())
							count++
						}
						must(cur.Err())
						if count != 100 {
							panic(count)
						}
					}
				})
				must(cur.Close())
				must(d.VerifyIndexes(context.Background()))
				must(d.Close())
			case "pebble-manual":
				p, err := pebble.Open(ed, &pebble.Options{CacheSize: *flagCache << 20, MemTableSize: 64 << 20, Logger: quiet{}})
				must(err)
				record(engine, "load", trial, n, func() {
					b := p.NewBatch()
					for i := range keys {
						must(b.Set(append([]byte{'d'}, keys[i]...), docs[i], pebble.NoSync))
						if (i+1)%1000 == 0 {
							must(b.Commit(pebble.NoSync))
							b.Reset()
						}
					}
					must(b.Commit(pebble.NoSync))
					must(b.Close())
				})
				record(engine, "build-wide-index", trial, n, func() {
					b := p.NewBatch()
					for i := range keys {
						k := append([]byte{'i'}, values[i]...)
						k = append(k, keys[i]...)
						must(b.Set(k, nil, pebble.NoSync))
						if (i+1)%1000 == 0 {
							must(b.Commit(pebble.NoSync))
							b.Reset()
						}
					}
					must(b.Commit(pebble.NoSync))
					must(b.Close())
				})
				must(p.Flush())
				record(engine, "ordered-100-row-page", trial, queries, func() {
					for j := 0; j < queries; j++ {
						start := j * 17 % (n - 101)
						it, err := p.NewIter(&pebble.IterOptions{LowerBound: []byte{'i'}, UpperBound: []byte{'j'}})
						must(err)
						count := 0
						for ok := it.SeekGE(append([]byte{'i'}, values[start]...)); ok && count < 100; ok = it.Next() {
							pk := it.Key()[3001:]
							v, closer, err := p.Get(append([]byte{'d'}, pk...))
							must(err)
							verify(start+count, pk, v)
							must(closer.Close())
							count++
						}
						must(it.Error())
						must(it.Close())
						if count != 100 {
							panic(count)
						}
					}
				})
				must(p.Close())
			default:
				var storage kv
				var err error
				if engine == "sqlite-c" {
					storage, err = openSQLiteC(ed, false, 1)
				} else {
					storage, err = openSQLiteGo(ed, false, 1)
				}
				must(err)
				s := storage.(*sqliteKV)
				execute := func(sql string) { st := s.c.Prep(sql); _, err := st.Step(); must(err); must(st.Reset()) }
				execute("CREATE TABLE wide(pk TEXT PRIMARY KEY,v TEXT,payload BLOB) WITHOUT ROWID")
				ins := s.c.Prep("INSERT INTO wide VALUES(?1,?2,?3)")
				record(engine, "load", trial, n, func() {
					execute("BEGIN")
					for i := range keys {
						ins.BindText(1, string(keys[i]))
						ins.BindText(2, values[i])
						ins.BindBytes(3, docs[i])
						_, err := ins.Step()
						must(err)
						must(ins.Reset())
						if (i+1)%1000 == 0 {
							execute("COMMIT")
							execute("BEGIN")
						}
					}
					execute("COMMIT")
				})
				record(engine, "build-wide-index", trial, n, func() { execute("CREATE INDEX wide_v ON wide(v,pk)") })
				must(storage.Settle())
				query := s.c.Prep("SELECT pk,payload FROM wide WHERE v>=?1 ORDER BY v,pk LIMIT 100")
				record(engine, "ordered-100-row-page", trial, queries, func() {
					var buf []byte
					for j := 0; j < queries; j++ {
						start := j * 17 % (n - 101)
						query.BindText(1, values[start])
						count := 0
						for {
							ok, err := query.Step()
							must(err)
							if !ok {
								break
							}
							size := query.ColumnLen(1)
							if cap(buf) < size {
								buf = make([]byte, size)
							}
							buf = buf[:size]
							query.ColumnBytes(1, buf)
							verify(start+count, []byte(query.ColumnText(0)), buf)
							count++
						}
						must(query.Reset())
						if count != 100 {
							panic(count)
						}
					}
				})
				must(storage.Close())
			}
		}
	}
	data, err := json.MarshalIndent(measurements, "", "  ")
	must(err)
	must(os.WriteFile(filepath.Join(dir, "wide-index-measurements.json"), append(data, '\n'), 0644))
}
