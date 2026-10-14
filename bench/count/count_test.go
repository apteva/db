// Package countbench compares embedded filtered counts on identical fixtures.
package countbench

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	csqlite "crawshaw.io/sqlite"
	db "github.com/apteva/db"
	"github.com/cockroachdb/pebble/v2"
	gosqlite "zombiezen.com/go/sqlite"
)

const rows = 100000
const matches = 5000

var filter = []byte(`{"bucket":"b00"}`)

func BenchmarkCountApteva(b *testing.B) {
	d, err := db.Open(b.TempDir(), db.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("rows")
	if err != nil {
		b.Fatal(err)
	}
	if err = c.CreateCompoundIndex(context.Background(), "bucket_rank", []db.IndexField{{Field: "bucket"}, {Field: "rank"}}); err != nil {
		b.Fatal(err)
	}
	batch := c.NewBatch()
	var key [8]byte
	for i := 0; i < rows; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		doc, err := db.Encode(map[string]any{"bucket": fmt.Sprintf("b%02d", i%20), "rank": int64(i), "payload": strings.Repeat("x", 100)})
		if err != nil {
			b.Fatal(err)
		}
		if err = batch.Insert(key[:], doc); err != nil {
			b.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			if err = batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err = d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	pipe, err := db.ParsePipeline([]byte(`[{"$count":"n"}]`))
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"QueryPipeline", "IndexCursor", "NativeCount", "NaturalCount", "ResidualCount"} {
		b.Run(mode, func(b *testing.B) {
			var stats db.QueryStats
			run := func() int64 {
				switch mode {
				case "NativeCount", "NaturalCount", "ResidualCount":
					f := filter
					hint := ""
					if mode == "NaturalCount" {
						hint = "$natural"
					}
					if mode == "ResidualCount" {
						f = []byte(`{"bucket":"b00","payload":{"$prefix":"x"}}`)
					}
					n, err := c.CountDocuments(context.Background(), f, db.CountOptions{Hint: hint, Stats: &stats})
					if err != nil {
						b.Fatal(err)
					}
					if mode == "NativeCount" && (!stats.Plan.IndexOnly || stats.DocumentFetches != 0 || stats.IndexEntriesExamined != matches) {
						b.Fatal(stats)
					}
					return n
				case "QueryPipeline":
					source := db.Source(func(yield func([]byte, []byte) bool) error {
						return c.Query(context.Background(), filter, db.QueryOptions{Stats: &stats}, yield)
					})
					result, err := pipe.Run(context.Background(), source)
					if err != nil {
						b.Fatal(err)
					}
					if len(result) != 1 {
						b.Fatal(result)
					}
					return result[0]["n"].(int64)
				default:
					cur, err := c.NewIndexCursor(context.Background(), "bucket_rank", db.IndexCursorOptions{Prefix: []any{"b00"}})
					if err != nil {
						b.Fatal(err)
					}
					defer cur.Close()
					var n int64
					for ok := cur.First(); ok; ok = cur.Next() {
						n++
					}
					if err = cur.Err(); err != nil {
						b.Fatal(err)
					}
					return n
				}
			}
			for i := 0; i < 5; i++ {
				if n := run(); n != matches {
					b.Fatal(n)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if n := run(); n != matches {
					b.Fatal(n)
				}
			}
			b.StopTimer()
			if mode == "NativeCount" {
				b.ReportMetric(float64(stats.DocumentFetches), "doc-fetches/op")
				b.ReportMetric(float64(stats.IndexEntriesExamined), "index-entries/op")
				b.ReportMetric(float64(stats.PeakMemoryBytes), "retained-B/op")
			}
		})
	}
}

type stmt interface {
	BindText(int, string)
	BindInt64(int, int64)
	Step() (bool, error)
	Reset() error
	ColumnInt64(int) int64
}

func BenchmarkCountSQLite(b *testing.B) {
	for _, engine := range []string{"C", "Go"} {
		b.Run(engine, func(b *testing.B) {
			var prep func(string) stmt
			if engine == "C" {
				c, err := csqlite.OpenConn(filepath.Join(b.TempDir(), "sqlite"), csqlite.SQLITE_OPEN_READWRITE|csqlite.SQLITE_OPEN_CREATE)
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				prep = func(q string) stmt { return c.Prep(q) }
			} else {
				c, err := gosqlite.OpenConn(filepath.Join(b.TempDir(), "sqlite"), gosqlite.OpenReadWrite|gosqlite.OpenCreate)
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				prep = func(q string) stmt { return c.Prep(q) }
			}
			exec := func(q string) {
				s := prep(q)
				if _, err := s.Step(); err != nil {
					b.Fatal(err)
				}
				if err := s.Reset(); err != nil {
					b.Fatal(err)
				}
			}
			exec("PRAGMA cache_size=-65536")
			exec("CREATE TABLE rows(id INTEGER PRIMARY KEY, bucket TEXT, rank INTEGER, payload TEXT)")
			exec("CREATE INDEX bucket_rank ON rows(bucket ASC,rank ASC)")
			exec("BEGIN")
			ins := prep("INSERT INTO rows VALUES(?1,?2,?3,?4)")
			for i := 0; i < rows; i++ {
				ins.BindInt64(1, int64(i))
				ins.BindText(2, fmt.Sprintf("b%02d", i%20))
				ins.BindInt64(3, int64(i))
				ins.BindText(4, strings.Repeat("x", 100))
				if _, err := ins.Step(); err != nil {
					b.Fatal(err)
				}
				if err := ins.Reset(); err != nil {
					b.Fatal(err)
				}
			}
			exec("COMMIT")
			count := prep("SELECT COUNT(*) FROM rows INDEXED BY bucket_rank WHERE bucket=?1")
			run := func() {
				count.BindText(1, "b00")
				ok, err := count.Step()
				if err != nil || !ok || count.ColumnInt64(0) != matches {
					b.Fatal(ok, err)
				}
				if err = count.Reset(); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < 5; i++ {
				run()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				run()
			}
		})
	}
}

// Pebble has no document-count API; this is an application-maintained covering
// secondary index with one entry per scalar document, counted using an iterator.
func BenchmarkCountPebble(b *testing.B) {
	d, err := pebble.Open(b.TempDir(), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	batch := d.NewBatch()
	defer batch.Close()
	for i := 0; i < rows; i++ {
		key := []byte(fmt.Sprintf("b%02d/%08d", i%20, i))
		if err = batch.Set(key, nil, nil); err != nil {
			b.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			if err = batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err = d.Flush(); err != nil {
		b.Fatal(err)
	}
	if err = d.Compact(context.Background(), []byte("b00/"), []byte("b20/"), true); err != nil {
		b.Fatal(err)
	}
	run := func() {
		s := d.NewSnapshot()
		defer s.Close()
		it, err := s.NewIter(&pebble.IterOptions{LowerBound: []byte("b00/"), UpperBound: []byte("b01/")})
		if err != nil {
			b.Fatal(err)
		}
		defer it.Close()
		n := 0
		for ok := it.First(); ok; ok = it.Next() {
			n++
		}
		if err = it.Error(); err != nil || n != matches {
			b.Fatal(n, err)
		}
	}
	for i := 0; i < 5; i++ {
		run()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		run()
	}
}
