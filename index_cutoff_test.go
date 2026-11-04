package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// The cutoff follows the collection's real size. Record sizes that grow make
// the estimate several times too high, which would send a scope of half the
// collection through the index.
func TestIndexPlanCutoffUsesRealSize(t *testing.T) {
	ctx := context.Background()
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("e")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateCompoundIndex(ctx, "i", []IndexField{{Field: "s"}, {Field: "at"}}); err != nil {
		t.Fatal(err)
	}
	const n = 40000
	for i := 0; i < n; i++ {
		var b DocBuilder
		// s00 holds just under a quarter, s01 just over, s02 half; s03 the rest
		switch {
		case i < n/4-100:
			b.String("s", "s00")
		case i < n/4-100+n/4+200:
			b.String("s", "s01")
		case i < n/4-100+n/4+200+n/4:
			b.String("s", "s02")
		default:
			b.String("s", "s03")
		}
		b.Int("at", int64(i))
		b.String("pad", strings.Repeat("x", i/200))
		if err := c.Insert([]byte(fmt.Sprintf("k%06d", i)), b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	check := func(stage string) {
		t.Helper()
		if size, err := c.collectionSize(c.indexDefs()); err != nil || size != n {
			t.Fatalf("%s: size = %d, %v, want %d", stage, size, err, n)
		}
		for _, tc := range []struct {
			filter string
			used   bool
		}{
			{`{"s":"s00"}`, true},
			{`{"s":"s01"}`, false},
			{`{"s":{"$in":["s00","s02"]}}`, false},
		} {
			preds, err := queryPredicates([]byte(tc.filter))
			if err != nil {
				t.Fatal(err)
			}
			if _, used := c.indexPlan(preds); used != tc.used {
				t.Fatalf("%s: %s: index used = %v, want %v", stage, tc.filter, used, tc.used)
			}
			want, err := c.CountDocuments(ctx, []byte(tc.filter), CountOptions{Hint: "$natural"})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.CountDocuments(ctx, []byte(`{"$and":[`+tc.filter+`,{"pad":{"$ne":"none"}}]}`), CountOptions{})
			if err != nil || got != want {
				t.Fatalf("%s: %s: count = %d, %v, want %d", stage, tc.filter, got, err, want)
			}
		}
	}
	check("buffered")
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check("checkpointed")
	approx, err := c.approxCount()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("estimate %d for %d documents", approx, n)
}

// Without an index holding one entry per document the size falls back on the
// estimate rather than counting entries that omit or repeat documents.
func TestCollectionSizeSkipsInexactIndexes(t *testing.T) {
	ctx := context.Background()
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("e")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateIndex(ctx, "tags", "tags"); err != nil {
		t.Fatal(err)
	}
	const n = 500
	for i := 0; i < n; i++ {
		raw := fmt.Sprintf(`{"tags":["a%d","b%d","c%d"]}`, i, i, i)
		if err := c.InsertJSON([]byte(fmt.Sprintf("k%06d", i)), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.collectionSize(c.indexDefs())
	if err != nil {
		t.Fatal(err)
	}
	want, err := c.approxCount()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("size = %d, want the estimate %d", got, want)
	}
}
