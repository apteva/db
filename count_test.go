package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestCountDocumentsCoverageAndFallback(t *testing.T) {
	d := compatDB(t)
	c := mustCollection(t, d, "counts")
	docs := []string{
		`{"bucket":"b00","rank":9007199254740993,"extra":true}`,
		`{"bucket":"b00","rank":9007199254740992,"extra":false}`,
		`{"bucket":"b00","rank":1.0,"extra":true}`,
		`{"bucket":"b01","rank":0}`,
		`{"bucket":null,"rank":0}`,
		`{"rank":0}`,
		`{"bucket":"b00"}`,
		`{"bucket":"b00","rank":null}`,
	}
	for i, doc := range docs {
		mustCompat(t, c.InsertJSON([]byte(fmt.Sprint(i)), []byte(doc)))
	}
	mustCompat(t, c.Insert([]byte("raw"), []byte("not a document")))
	mustCompat(t, c.CreateCompoundIndex(bg, "bucket_rank", []IndexField{{Field: "bucket", Desc: true}, {Field: "rank"}}))
	cases := []struct {
		filter  string
		want    int64
		covered bool
	}{
		{`{"bucket":"b00"}`, 5, true}, {`{"bucket":"absent"}`, 0, true},
		{`{"bucket":"b00","rank":9007199254740993}`, 1, true},
		{`{"bucket":"b00","rank":9007199254740992}`, 1, true},
		{`{"bucket":"b00","rank":1}`, 1, true},
		{`{"bucket":null}`, 2, true}, {`{"bucket":"b00","rank":null}`, 2, true},
		{`{"$and":[{"bucket":"b00"},{"bucket":{"$eq":"b00"}}]}`, 5, true},
		{`{"$and":[{"bucket":"b00"},{"bucket":"b01"}]}`, 0, false},
		{`{"bucket":"b00","extra":true}`, 2, false},
		{`{"bucket":"b00","$or":[{"rank":1},{"rank":null}]}`, 3, false},
		{`{"$or":[{"bucket":"b00"},{"bucket":null}]}`, 7, false},
		{`{"rank":0}`, 3, false}, {`{"bucket":{"$prefix":"b0"}}`, 6, false},
		{`{}`, 8, true},
	}
	for _, tt := range cases {
		t.Run(tt.filter, func(t *testing.T) {
			var stats QueryStats
			n, err := c.CountDocuments(bg, []byte(tt.filter), CountOptions{Stats: &stats})
			mustCompat(t, err)
			var scanned int64
			mustCompat(t, c.Query(bg, []byte(tt.filter), QueryOptions{Hint: "$natural"}, func(_, _ []byte) bool { scanned++; return true }))
			if n != tt.want || n != scanned || stats.Plan.IndexOnly != tt.covered || stats.Returned != n {
				t.Fatalf("n=%d scan=%d want=%d stats=%+v", n, scanned, tt.want, stats)
			}
			if tt.covered && (stats.DocumentFetches != 0 || stats.IndexEntriesExamined != n || stats.Plan.Access != "index-count") {
				t.Fatalf("not covered: %+v", stats)
			}
			if !tt.covered && stats.DocumentFetches == 0 {
				t.Fatalf("no fallback evaluation: %+v", stats)
			}
		})
	}
	for _, opts := range []CountOptions{{Skip: 2, Limit: 2}, {Skip: 20}, {Limit: 1}, {Hint: "$natural"}, {Hint: "bucket_rank"}} {
		var want int64
		mustCompat(t, c.Query(bg, []byte(`{"bucket":"b00"}`), QueryOptions{Hint: "$natural", Skip: opts.Skip, Limit: opts.Limit}, func(_, _ []byte) bool { want++; return true }))
		got, err := c.CountDocuments(nil, []byte(`{"bucket":"b00"}`), opts)
		mustCompat(t, err)
		if got != want {
			t.Fatal(opts, got, want)
		}
	}
}

func TestCountSparseAndMultikey(t *testing.T) {
	d := compatDB(t)
	sparse := mustCollection(t, d, "sparse")
	for i, doc := range []string{`{"a":"x","b":1}`, `{"a":"x"}`, `{"a":null,"b":1}`, `{"b":1}`, `{"a":"x","b":null}`} {
		mustCompat(t, sparse.InsertJSON([]byte(fmt.Sprint(i)), []byte(doc)))
	}
	mustCompat(t, sparse.CreateCompoundIndex(bg, "sparse", []IndexField{{Field: "a"}, {Field: "b", Desc: true}}, IndexOptions{Sparse: true}))
	for _, tt := range []struct {
		filter  string
		want    int64
		covered bool
	}{
		{`{"a":"x"}`, 3, false}, {`{"a":null}`, 2, false}, {`{"a":"x","b":1}`, 1, true}, {`{"a":"x","b":null}`, 2, false}, {`{}`, 5, false},
	} {
		var stats QueryStats
		n, err := sparse.CountDocuments(bg, []byte(tt.filter), CountOptions{Hint: "sparse", Stats: &stats})
		mustCompat(t, err)
		if n != tt.want || stats.Plan.IndexOnly != tt.covered {
			t.Fatal(tt, n, stats)
		}
	}
	multi := mustCollection(t, d, "multi")
	for i, doc := range []string{`{"tags":["x","x","y"]}`, `{"tags":["x","y"]}`, `{"tags":null}`, `{}`, `{"tags":[]}`, `{"tags":{"nested":1}}`} {
		mustCompat(t, multi.InsertJSON([]byte(fmt.Sprint(i)), []byte(doc)))
	}
	mustCompat(t, multi.CreateIndex(bg, "tags", "tags"))
	for _, filter := range []string{`{"tags":"x"}`, `{"tags":{"$in":["x","y"]}}`, `{"tags":null}`, `{}`} {
		var want int64
		mustCompat(t, multi.Query(bg, []byte(filter), QueryOptions{Hint: "$natural"}, func(_, _ []byte) bool { want++; return true }))
		var stats QueryStats
		n, err := multi.CountDocuments(bg, []byte(filter), CountOptions{Hint: "tags", Stats: &stats})
		mustCompat(t, err)
		if n != want || stats.Plan.IndexOnly || stats.DocumentFetches == 0 {
			t.Fatal(filter, n, want, stats)
		}
	}
}

func TestCountSnapshotWritesAndCatalog(t *testing.T) {
	d := compatDB(t)
	c := mustCollection(t, d, "counts")
	mustCompat(t, c.CreateCompoundIndex(bg, "ab", []IndexField{{Field: "a"}, {Field: "b"}}))
	for i := 0; i < 100; i++ {
		mustCompat(t, c.InsertDocument([]byte(fmt.Sprint(i)), map[string]any{"a": "x", "b": i}))
	}
	s, err := d.Snapshot()
	mustCompat(t, err)
	defer s.Close()
	sc, err := s.Collection("counts")
	mustCompat(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := c.ReplaceDocument([]byte(fmt.Sprint(i)), map[string]any{"a": "y", "b": i}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		var stats QueryStats
		n, err := sc.CountDocuments(bg, []byte(`{"a":"x"}`), CountOptions{Stats: &stats})
		if err != nil || n != 100 || stats.DocumentFetches != 0 {
			t.Fatal(n, err, stats)
		}
	}
	wg.Wait()
	mustCompat(t, c.DropIndex(bg, "ab"))
	mustCompat(t, c.CreateCompoundIndex(bg, "ab", []IndexField{{Field: "b"}, {Field: "a"}}))
	n, err := sc.CountDocuments(bg, []byte(`{"a":"x"}`), CountOptions{})
	mustCompat(t, err)
	if n != 100 {
		t.Fatal(n)
	}
	n, err = c.CountDocuments(bg, []byte(`{"a":"x"}`), CountOptions{})
	mustCompat(t, err)
	if n != 0 {
		t.Fatal(n)
	}
	s.Close()
	if _, err = sc.CountDocuments(bg, nil, CountOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

type countCancelContext struct {
	context.Context
	remaining int
}

func (c *countCancelContext) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}
func TestCountLimitsAndCancellation(t *testing.T) {
	d := compatDB(t)
	c := mustCollection(t, d, "counts")
	mustCompat(t, c.CreateCompoundIndex(bg, "a", []IndexField{{Field: "a"}}))
	for i := 0; i < 1000; i++ {
		mustCompat(t, c.InsertDocument([]byte(fmt.Sprint(i)), map[string]any{"a": 1}))
	}
	for _, o := range []CountOptions{{Skip: -1}, {Limit: -1}, {MaxMemoryBytes: -1}, {Skip: int(^uint(0) >> 1), Limit: 1}} {
		if _, err := c.CountDocuments(bg, nil, o); !errors.Is(err, ErrPipeline) {
			t.Fatal(o, err)
		}
	}
	if _, err := c.CountDocuments(bg, nil, CountOptions{Hint: "missing"}); !errors.Is(err, ErrNoSuchIndex) {
		t.Fatal(err)
	}
	if _, err := c.CountDocuments(bg, []byte(`{"a":1}`), CountOptions{MaxMemoryBytes: 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for _, filter := range []string{`{`, `[]`, `null`, `{} {}`, `{"a":{"$wat":1}}`} {
		if _, err := c.CountDocuments(bg, []byte(filter), CountOptions{}); !errors.Is(err, ErrPipeline) {
			t.Fatal(filter, err)
		}
	}
	mustCompat(t, d.Checkpoint())
	for _, hint := range []string{"", "$natural"} {
		var stats QueryStats
		n, err := c.CountDocuments(&countCancelContext{Context: bg, remaining: 5}, []byte(`{"a":1}`), CountOptions{Hint: hint, Stats: &stats})
		if n != 0 || !errors.Is(err, context.Canceled) || stats.Examined == 0 || stats.Examined >= 1000 {
			t.Fatal(n, err, stats)
		}
	}
	if d.Stats().Snapshots != 0 {
		t.Fatal("leaked count snapshot")
	}
	mustCompat(t, d.InsertJSON([]byte("default"), []byte(`{"a":1}`)))
	n, err := d.CountDocuments(bg, nil, CountOptions{})
	mustCompat(t, err)
	if n != 1 {
		t.Fatal(n)
	}
	s, err := d.Snapshot()
	mustCompat(t, err)
	defer s.Close()
	n, err = s.CountDocuments(bg, nil, CountOptions{})
	mustCompat(t, err)
	if n != 1 {
		t.Fatal(n)
	}
}

func TestCountBufferedCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{})
	mustCompat(t, err)
	c := mustCollection(t, d, "counts")
	mustCompat(t, c.CreateCompoundIndex(bg, "ab", []IndexField{{Field: "a"}, {Field: "b", Desc: true}}))
	for i := 0; i < 400; i++ {
		mustCompat(t, c.UpsertDocument([]byte(fmt.Sprint(i)), map[string]any{"a": i % 7, "b": i % 13}))
	}
	check := func() {
		for a := 0; a < 8; a++ {
			for b := 0; b < 15; b++ {
				filter := []byte(fmt.Sprintf(`{"a":%d,"b":%d}`, a, b))
				var want int64
				mustCompat(t, c.Query(bg, filter, QueryOptions{Hint: "$natural"}, func(_, _ []byte) bool { want++; return true }))
				var stats QueryStats
				got, err := c.CountDocuments(bg, filter, CountOptions{Stats: &stats})
				mustCompat(t, err)
				if got != want || !stats.Plan.IndexOnly || stats.DocumentFetches != 0 {
					t.Fatal(a, b, got, want, stats)
				}
			}
		}
	}
	check()
	mustCompat(t, d.Checkpoint())
	check()
	for i := 0; i < 400; i++ {
		if i%3 == 0 {
			mustCompat(t, c.Delete([]byte(fmt.Sprint(i))))
		} else {
			mustCompat(t, c.ReplaceDocument([]byte(fmt.Sprint(i)), map[string]any{"a": i % 5, "b": i % 11}))
		}
	}
	check()
	mustCompat(t, d.Checkpoint())
	check()
	mustCompat(t, d.Close())
	d, err = Open(dir, Options{})
	mustCompat(t, err)
	defer d.Close()
	c, err = d.OpenCollection("counts")
	mustCompat(t, err)
	check()
}
