package db

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

const countBenchDay = int64(86400000)

// Wide documents, as an application's records are: the cost being measured is
// reading them, compared with reading only index entries.
func countRangeFixture(b *testing.B, n int) *Collection {
	b.Helper()
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { d.Close() })
	c, err := d.Collection("events")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err := c.CreateCompoundIndex(ctx, "by_scope_at", []IndexField{{Field: "scope"}, {Field: "at"}}); err != nil {
		b.Fatal(err)
	}
	rng := rand.New(rand.NewSource(3))
	pad := strings.Repeat("x", 2000)
	var w DocBuilder
	for i := 0; i < n; i++ {
		w.Reset()
		w.String("scope", fmt.Sprintf("S%02d", rng.Intn(30)))
		w.Int("at", 1767225600000+rng.Int63n(365*countBenchDay))
		w.String("pad", pad)
		if err := c.Insert([]byte(fmt.Sprintf("e-%07d", i)), w.Bytes()); err != nil {
			b.Fatal(err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	return c
}

func BenchmarkCountRanges(b *testing.B) {
	c := countRangeFixture(b, 200000)
	ctx := context.Background()
	in := func(n int) string {
		v := []string{}
		for i := 0; i < n; i++ {
			v = append(v, fmt.Sprintf("%q", fmt.Sprintf("S%02d", i)))
		}
		return "[" + strings.Join(v, ",") + "]"
	}
	const at = `{"$and":[{"at":{"$ne":null}},{"at":{"$gte":1775001600000}}]},{"$and":[{"at":{"$ne":null}},{"at":{"$lt":1782777600000}}]}`
	shapes := []struct{ name, filter string }{
		{"eq", `{"scope":{"$eq":"S07"}}`},
		{"eq+range", `{"$and":[{"scope":{"$eq":"S07"}},` + at + `]}`},
		{"in2+range", `{"$and":[{"$and":[{"scope":{"$ne":null}},{"scope":{"$in":` + in(2) + `}}]},` + at + `]}`},
		{"in30+range", `{"$and":[{"$and":[{"scope":{"$ne":null}},{"scope":{"$in":` + in(30) + `}}]},` + at + `]}`},
		{"in2+range+search", `{"$and":[{"$and":[{"scope":{"$ne":null}},{"scope":{"$in":` + in(2) + `}}]},` + at + `,{"$like":{"fields":["pad"],"separator":" ","pattern":"%y%"}}]}`},
	}
	for _, s := range shapes {
		b.Run(s.name, func(b *testing.B) {
			var stats QueryStats
			var n int64
			for i := 0; i < b.N; i++ {
				var err error
				if n, err = c.CountDocuments(ctx, []byte(s.filter), CountOptions{Stats: &stats}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n), "rows")
			b.ReportMetric(float64(stats.DocumentFetches), "fetches")
			b.ReportMetric(float64(stats.IndexEntriesExamined), "entries")
		})
	}
}
