package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	db "github.com/apteva/db"
	"github.com/cockroachdb/pebble/v2"
)

type compatibilityRow struct {
	key           []byte
	tenant, score int64
	title         string
	doc           []byte
}
type compatibilityMeasurement struct {
	Engine, Phase       string
	Trial, Ops          int
	NsPerOp             float64
	AllocatedBytesPerOp float64
	PeakQueryBytes      int64 `json:",omitempty"`
}

func compatibilityDoc(r compatibilityRow) []byte {
	var b db.DocBuilder
	b.Int("score", r.score)
	b.Int("tenant", r.tenant)
	b.String("title", r.title)
	b.Null("nullable")
	return append([]byte(nil), b.Bytes()...)
}
func compatibilityIndexKey(r compatibilityRow) []byte {
	k := []byte{'i', byte(r.tenant)}
	k = binary.BigEndian.AppendUint64(k, ^(uint64(r.score) ^ (1 << 63)))
	return append(k, r.key...)
}
func compatibilityPrefixEnd(p []byte) []byte {
	k := append([]byte(nil), p...)
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] != 255 {
			k[i]++
			return k[:i+1]
		}
	}
	return nil
}

// Fresh engine directories, identical binary payloads and exact int64 oracle.
// SQL uses typed index columns plus the same binary document payload; Pebble
// maintains a secondary index in application batches, as it has no native one.
func runCompatibilityBench(dir string, n, reps int) {
	if n < 1000 {
		panic("compatibility benchmark requires n >= 1000")
	}
	rows := make([]compatibilityRow, n)
	base := int64(1 << 53)
	for i := range rows {
		title := fmt.Sprintf("speed.%%_[%08d", i)
		if i%3 == 0 {
			title = "other" + title
		}
		r := compatibilityRow{key: []byte(fmt.Sprintf("%08d", i)), tenant: int64(i % 32), score: base + int64((i*137)%n), title: title}
		r.doc = compatibilityDoc(r)
		rows[i] = r
	}
	tenant := int64(12)
	cutoff := base + int64(n/2)
	prefix := "speed.%_["
	all := append([]compatibilityRow(nil), rows...)
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score > all[j].score
		}
		return bytes.Compare(all[i].key, all[j].key) < 0
	})
	cursorWant, pageWant := []compatibilityRow{}, []compatibilityRow{}
	for _, r := range all {
		if r.tenant == tenant {
			if len(cursorWant) < 100 {
				cursorWant = append(cursorWant, r)
			}
			if r.score >= cutoff && strings.HasPrefix(r.title, prefix) && len(pageWant) < 100 {
				pageWant = append(pageWant, r)
			}
		}
	}
	filter := []byte(fmt.Sprintf(`{"$and":[{"tenant":%d},{"score":{"$gte":%d}},{"$or":[{"title":{"$prefix":%q}},{"score":%d}]}]}`, tenant, cutoff, prefix, base-1))
	measurements := []compatibilityMeasurement{}
	record := func(engine, phase string, trial, ops int, fn func()) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		fn()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		m := compatibilityMeasurement{Engine: engine, Phase: phase, Trial: trial, Ops: ops, NsPerOp: float64(elapsed.Nanoseconds()) / float64(ops), AllocatedBytesPerOp: float64(after.TotalAlloc-before.TotalAlloc) / float64(ops)}
		measurements = append(measurements, m)
		fmt.Printf("%-24s %-24s trial=%d %.3f ns/op %.1f Go bytes/op\n", engine, phase, trial, m.NsPerOp, m.AllocatedBytesPerOp)
	}
	validator := func(want []compatibilityRow) (func([]byte, []byte) bool, func()) {
		position := 0
		return func(key, doc []byte) bool {
				if position >= len(want) || !bytes.Equal(key, want[position].key) || len(doc) < 15 || int64(binary.LittleEndian.Uint64(doc[7:])) != want[position].score {
					panic("compatibility result/order mismatch")
				}
				position++
				return true
			}, func() {
				if position != len(want) {
					panic(fmt.Sprintf("compatibility row count %d != %d", position, len(want)))
				}
			}
	}
	const queries = 30
	for trial := 1; trial <= reps; trial++ {
		for _, engine := range []string{"ours", "pebble-manual", "sqlite-c", "sqlite-go"} {
			ed := filepath.Join(dir, fmt.Sprintf("compat-%s-%d", engine, trial))
			if err := os.MkdirAll(ed, 0755); err != nil {
				panic(err)
			}
			switch engine {
			case "ours":
				database, err := db.Open(ed, db.Options{})
				if err != nil {
					panic(err)
				}
				c, err := database.Collection("rows")
				if err != nil {
					panic(err)
				}
				record(engine, "load", trial, n, func() {
					batch := c.NewBatch()
					for i, r := range rows {
						if err := batch.Upsert(r.key, r.doc); err != nil {
							panic(err)
						}
						if (i+1)%1000 == 0 {
							if err := batch.Commit(); err != nil {
								panic(err)
							}
						}
					}
					if err := batch.Commit(); err != nil {
						panic(err)
					}
				})
				record(engine, "build-compound", trial, n, func() {
					if err := c.CreateCompoundIndex(context.Background(), "tenant_score", []db.IndexField{{Field: "tenant"}, {Field: "score", Desc: true}}); err != nil {
						panic(err)
					}
				})
				record(engine, "build-nullable-unique", trial, n, func() {
					if err := c.CreateCompoundIndex(context.Background(), "nullable", []db.IndexField{{Field: "nullable"}}, db.IndexOptions{Unique: true, NullsDistinct: true}); err != nil {
						panic(err)
					}
				})
				if err := database.Checkpoint(); err != nil {
					panic(err)
				}
				for _, phase := range []string{"compound-page", "fallback-sort-page", "ordered-cursor"} {
					record(engine, phase, trial, queries, func() {
						for j := 0; j < queries; j++ {
							want := pageWant
							if phase == "ordered-cursor" {
								want = cursorWant
							}
							yield, verify := validator(want)
							if phase == "ordered-cursor" {
								cur, err := c.NewIndexCursor(context.Background(), "tenant_score", db.IndexCursorOptions{Prefix: []any{tenant}})
								if err != nil {
									panic(err)
								}
								count := 0
								for ok := cur.First(); ok && count < len(want); ok = cur.Next() {
									yield(cur.Key(), cur.Value())
									count++
								}
								if err := cur.Err(); err != nil {
									panic(err)
								}
								cur.Close()
							} else {
								stats := db.QueryStats{}
								opts := db.QueryOptions{Sort: []db.IndexField{{Field: "score", Desc: true}}, Limit: 100, Stats: &stats}
								if phase == "fallback-sort-page" {
									opts.Hint = "$natural"
								}
								if err := c.Query(context.Background(), filter, opts, yield); err != nil {
									panic(err)
								}
								if phase == "compound-page" && (stats.Plan.Access != "index-order" || stats.PeakMemoryBytes != 0) {
									panic("compound page did not stream through index")
								}
							}
							verify()
						}
					})
				}
				record(engine, "memory-limit-rejection", trial, queries, func() {
					for j := 0; j < queries; j++ {
						if err := c.Query(context.Background(), nil, db.QueryOptions{Hint: "$natural", Sort: []db.IndexField{{Field: "score"}}, MaxMemoryBytes: 128}, func(_, _ []byte) bool { return true }); err != db.ErrLimit {
							panic("query budget not enforced")
						}
					}
				})
				if err := database.VerifyIndexes(context.Background()); err != nil {
					panic(err)
				}
				if err := database.Close(); err != nil {
					panic(err)
				}
			case "pebble-manual":
				p, err := pebble.Open(ed, &pebble.Options{CacheSize: *flagCache << 20, MemTableSize: 64 << 20, Logger: quiet{}})
				if err != nil {
					panic(err)
				}
				record(engine, "load", trial, n, func() {
					b := p.NewBatch()
					defer b.Close()
					for i, r := range rows {
						if err := b.Set(append([]byte{'d'}, r.key...), r.doc, pebble.NoSync); err != nil {
							panic(err)
						}
						if (i+1)%1000 == 0 {
							if err := b.Commit(pebble.NoSync); err != nil {
								panic(err)
							}
							b.Reset()
						}
					}
					if err := b.Commit(pebble.NoSync); err != nil {
						panic(err)
					}
				})
				record(engine, "build-compound", trial, n, func() {
					b := p.NewBatch()
					defer b.Close()
					for i, r := range rows {
						if err := b.Set(compatibilityIndexKey(r), nil, pebble.NoSync); err != nil {
							panic(err)
						}
						if (i+1)%1000 == 0 {
							if err := b.Commit(pebble.NoSync); err != nil {
								panic(err)
							}
							b.Reset()
						}
					}
					if err := b.Commit(pebble.NoSync); err != nil {
						panic(err)
					}
				})
				record(engine, "build-nullable-unique", trial, n, func() {
					b := p.NewBatch()
					defer b.Close()
					for i, r := range rows {
						if err := b.Set(append([]byte{'u'}, r.key...), nil, pebble.NoSync); err != nil {
							panic(err)
						}
						if (i+1)%1000 == 0 {
							if err := b.Commit(pebble.NoSync); err != nil {
								panic(err)
							}
							b.Reset()
						}
					}
					if err := b.Commit(pebble.NoSync); err != nil {
						panic(err)
					}
				})
				if err := p.Flush(); err != nil {
					panic(err)
				}
				for _, phase := range []string{"compound-page", "ordered-cursor"} {
					record(engine, phase, trial, queries, func() {
						for j := 0; j < queries; j++ {
							want := pageWant
							if phase == "ordered-cursor" {
								want = cursorWant
							}
							yield, verify := validator(want)
							count := 0
							prefixKey := []byte{'i', byte(tenant)}
							it, err := p.NewIter(&pebble.IterOptions{LowerBound: prefixKey, UpperBound: compatibilityPrefixEnd(prefixKey)})
							if err != nil {
								panic(err)
							}
							for ok := it.First(); ok && count < len(want); ok = it.Next() {
								key := it.Key()
								pk := key[len(key)-8:]
								doc, closer, err := p.Get(append([]byte{'d'}, pk...))
								if err != nil {
									panic(err)
								}
								score := int64(binary.LittleEndian.Uint64(doc[7:]))
								titleLen := int(binary.LittleEndian.Uint16(doc[38:]))
								title := doc[40 : 40+titleLen]
								match := phase == "ordered-cursor" || score >= cutoff && bytes.HasPrefix(title, []byte(prefix))
								if match {
									yield(pk, doc)
									count++
								}
								closer.Close()
								if phase != "ordered-cursor" && score < cutoff {
									break
								}
							}
							if err := it.Error(); err != nil {
								panic(err)
							}
							it.Close()
							verify()
						}
					})
				}
				if err := p.Close(); err != nil {
					panic(err)
				}
			default:
				var storage kv
				var err error
				if engine == "sqlite-c" {
					storage, err = openSQLiteC(ed, false, 1)
				} else {
					storage, err = openSQLiteGo(ed, false, 1)
				}
				if err != nil {
					panic(err)
				}
				s := storage.(*sqliteKV)
				execute := func(sql string) {
					st := s.c.Prep(sql)
					if _, err := st.Step(); err != nil {
						panic(err)
					}
					st.Reset()
				}
				execute("CREATE TABLE records(pk TEXT PRIMARY KEY, tenant INTEGER, score INTEGER, title TEXT, nullable TEXT, payload BLOB) WITHOUT ROWID")
				insert := s.c.Prep("INSERT INTO records VALUES(?1,?2,?3,?4,NULL,?5)")
				record(engine, "load", trial, n, func() {
					execute("BEGIN")
					for i, r := range rows {
						insert.BindText(1, string(r.key))
						insert.BindInt64(2, r.tenant)
						insert.BindInt64(3, r.score)
						insert.BindText(4, r.title)
						insert.BindBytes(5, r.doc)
						if _, err := insert.Step(); err != nil {
							panic(err)
						}
						insert.Reset()
						if (i+1)%1000 == 0 {
							execute("COMMIT")
							execute("BEGIN")
						}
					}
					execute("COMMIT")
				})
				record(engine, "build-compound", trial, n, func() { execute("CREATE INDEX tenant_score ON records(tenant ASC,score DESC,pk ASC)") })
				record(engine, "build-nullable-unique", trial, n, func() { execute("CREATE UNIQUE INDEX nullable_unique ON records(nullable)") })
				for _, phase := range []string{"compound-page", "ordered-cursor"} {
					query := "SELECT pk,payload FROM records WHERE tenant=?1 ORDER BY score DESC,pk ASC LIMIT 100"
					want := cursorWant
					if phase == "compound-page" {
						query = "SELECT pk,payload FROM records WHERE tenant=?1 AND score>=?2 AND (substr(title,1,?3)=?4 OR score=?5) ORDER BY score DESC,pk ASC LIMIT 100"
						want = pageWant
					}
					st := s.c.Prep(query)
					record(engine, phase, trial, queries, func() {
						for j := 0; j < queries; j++ {
							st.BindInt64(1, tenant)
							if phase == "compound-page" {
								st.BindInt64(2, cutoff)
								st.BindInt64(3, int64(len(prefix)))
								st.BindText(4, prefix)
								st.BindInt64(5, base-1)
							}
							yield, verify := validator(want)
							var payload []byte
							for {
								ok, err := st.Step()
								if err != nil {
									panic(err)
								}
								if !ok {
									break
								}
								if cap(payload) < st.ColumnLen(1) {
									payload = make([]byte, st.ColumnLen(1))
								}
								payload = payload[:st.ColumnLen(1)]
								st.ColumnBytes(1, payload)
								yield([]byte(st.ColumnText(0)), payload)
							}
							st.Reset()
							verify()
						}
					})
				}
				if err := storage.Close(); err != nil {
					panic(err)
				}
			}
		}
	}
	// Large binary documents, decoded independently before each engine is timed.
	var b db.DocBuilder
	b.String("a", strings.Repeat("a", 40000))
	b.String("b", strings.Repeat("b", 40000))
	b.Int("z", 9007199254740993)
	large := append([]byte(nil), b.Bytes()...)
	if m, err := db.DecodeDocument(large); err != nil || m["z"] != int64(9007199254740993) {
		panic("large document codec failed")
	}
	for trial := 1; trial <= reps; trial++ {
		for _, engine := range []string{"ours", "pebble", "sqlite-c", "sqlite-go"} {
			ed := filepath.Join(dir, fmt.Sprintf("compat-large-%s-%d", engine, trial))
			os.MkdirAll(ed, 0755)
			var storage kv
			var err error
			switch engine {
			case "ours":
				storage, err = openOurs(ed, false)
			case "pebble":
				storage, err = openPebble(ed, false, 0)
			case "sqlite-c":
				storage, err = openSQLiteC(ed, false, 1)
			case "sqlite-go":
				storage, err = openSQLiteGo(ed, false, 1)
			}
			if err != nil {
				panic(err)
			}
			record(engine, "large-document-write", trial, 128, func() {
				for i := 0; i < 128; i++ {
					if err := storage.Upsert([]byte(fmt.Sprintf("%08d", i)), large); err != nil {
						panic(err)
					}
				}
			})
			storage.Settle()
			record(engine, "large-document-read", trial, 512, func() {
				var buffer []byte
				for i := 0; i < 512; i++ {
					value, found, err := storage.Find([]byte(fmt.Sprintf("%08d", i%128)), buffer)
					if err != nil || !found || !bytes.Equal(value, large) {
						panic("large stored document mismatch")
					}
					buffer = value
				}
			})
			storage.Close()
		}
	}
	out, err := json.MarshalIndent(measurements, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "compatibility-measurements.json"), append(out, '\n'), 0644); err != nil {
		panic(err)
	}
}
