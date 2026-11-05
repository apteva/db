package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

type likeRow struct {
	centre  string
	created int64
	f       [3]*string
}

func likeFixture(tb testing.TB, n int) (*Collection, []likeRow) {
	tb.Helper()
	d, err := Open(tb.TempDir(), Options{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { d.Close() })
	c, err := d.Collection("partners")
	if err != nil {
		tb.Fatal(err)
	}
	ctx := context.Background()
	if err := c.CreateCompoundIndex(ctx, "by_centre_created", []IndexField{{Field: "centre"}, {Field: "created_ms"}}); err != nil {
		tb.Fatal(err)
	}
	if err := c.CreateCompoundIndex(ctx, "by_created", []IndexField{{Field: "created_ms"}}); err != nil {
		tb.Fatal(err)
	}
	rng := rand.New(rand.NewSource(5))
	odd := []string{"Café", "50%_off", "a_b", "x\\y", "  pad  ", "ÉTÉ", "Op"}
	rows := make([]likeRow, n)
	var b DocBuilder
	for i := range rows {
		r := likeRow{centre: fmt.Sprintf("C%02d", rng.Intn(30)), created: 1767225600000 + rng.Int63n(365*86400000)}
		vals := [3]string{
			fmt.Sprintf("RES-%s-%06d", r.centre, i),
			fmt.Sprintf("opp-2026-%05d", rng.Intn(90000)),
			fmt.Sprintf("Commercial %d %s", rng.Intn(400), odd[rng.Intn(len(odd))]),
		}
		b.Reset()
		b.String("centre", r.centre)
		b.Int("created_ms", r.created)
		names := [3]string{"resource_key", "opportunity_key", "commercial_id"}
		for j := range vals {
			switch rng.Intn(12) {
			case 0: // missing
			case 1:
				b.Null(names[j])
			default:
				v := vals[j]
				r.f[j] = &v
				b.String(names[j], v)
			}
		}
		if err := c.Insert([]byte(fmt.Sprintf("p-%07d", i)), b.Bytes()); err != nil {
			tb.Fatal(err)
		}
		rows[i] = r
	}
	return c, rows
}

func lowerASCII(s string) string { return string(asciiLower(nil, []byte(s))) }

// referenceLike is SQLite's LIKE (no ESCAPE) via a regexp over ASCII-lowered text.
func referenceLike(pattern, text string) bool {
	var re strings.Builder
	re.WriteString("(?s)^")
	for _, r := range lowerASCII(pattern) {
		switch r {
		case '%':
			re.WriteString(".*")
		case '_':
			re.WriteString(".")
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString("$")
	return regexp.MustCompile(re.String()).MatchString(lowerASCII(text))
}

func likeFilter(value string) string {
	b, _ := json.Marshal(map[string]any{"$like": map[string]any{
		"fields": []string{"resource_key", "opportunity_key", "commercial_id"}, "separator": " ", "pattern": "%" + value + "%"}})
	return string(b)
}

func TestLikeMatchesSQLiteSemantics(t *testing.T) {
	c, rows := likeFixture(t, 6000)
	ctx := context.Background()
	values := []string{"", "23 op", "RES-c07", "opp-2026-0", "café", "CAFÉ", "caf_", "50%_", "a_b", "%", "_", "x\\y", "  pad  ", " pad ", "pad  ", "été", "op", "-2026-00012 commercial", "zzz", "c1_-0", "é"}
	for _, value := range values {
		for _, centre := range []string{"", "C07"} {
			want := 0
			for _, r := range rows {
				if centre != "" && r.centre != centre {
					continue
				}
				parts := make([]string, 3)
				for j, f := range r.f {
					if f != nil {
						parts[j] = *f
					}
				}
				if referenceLike("%"+value+"%", strings.Join(parts, " ")) {
					want++
				}
			}
			filter := likeFilter(value)
			if centre != "" {
				filter = fmt.Sprintf(`{"$and":[{"centre":%q},%s]}`, centre, filter)
			}
			got, err := c.CountDocuments(ctx, []byte(filter), CountOptions{})
			if err != nil {
				t.Fatal(err)
			}
			found := 0
			if err := c.Query(ctx, []byte(filter), QueryOptions{}, func(_, _ []byte) bool { found++; return true }); err != nil {
				t.Fatal(err)
			}
			agg, err := c.Aggregate(ctx, []byte(fmt.Sprintf(`[{"$match":%s},{"$group":{"_id":null,"n":{"$sum":1}}}]`, filter)))
			if err != nil {
				t.Fatal(err)
			}
			gated, err := c.Aggregate(ctx, []byte(fmt.Sprintf(`[{"$group":{"_id":null,"n":{"$sum":1,"where":%s}}}]`, filter)))
			if err != nil {
				t.Fatal(err)
			}
			n := func(rows []map[string]any) int {
				if len(rows) == 0 {
					return 0
				}
				v, _ := json.Marshal(rows[0]["n"])
				var x int
				json.Unmarshal(v, &x)
				return x
			}
			if int(got) != want || found != want || n(agg) != want || n(gated) != want {
				t.Fatalf("value %q centre %q: want %d, count %d query %d aggregate %d where %d", value, centre, want, got, found, n(agg), n(gated))
			}
		}
	}
	if _, err := c.CountDocuments(ctx, []byte(`{"$like":{"fields":[],"pattern":"%"}}`), CountOptions{}); err == nil {
		t.Fatal("empty fields accepted")
	}
}

func BenchmarkLike(b *testing.B) {
	c, _ := likeFixture(b, 200000)
	ctx := context.Background()
	const centre = `{"centre":"C07"}`
	quarter := `{"created_ms":{"$gte":1775001600000,"$lt":1782777600000}}`
	cases := []struct{ name, filter string }{
		{"centre+90d", fmt.Sprintf(`{"$and":[%s,%s]}`, centre, quarter)},
		{"centre+90d+like", fmt.Sprintf(`{"$and":[%s,%s,%s]}`, centre, quarter, likeFilter("23 op"))},
		{"centre+90d+like_wild", fmt.Sprintf(`{"$and":[%s,%s,%s]}`, centre, quarter, likeFilter("2_ o%p"))},
		{"90d", quarter},
		{"90d+like", fmt.Sprintf(`{"$and":[%s,%s]}`, quarter, likeFilter("23 op"))},
		{"all", `{}`},
		{"all+like", likeFilter("23 op")},
		{"all+like_wild", likeFilter("2_ o%p")},
		{"all+prefix", `{"opportunity_key":{"$prefix":"opp-2026-1"}}`},
	}
	for _, tc := range cases {
		b.Run("count/"+tc.name, func(b *testing.B) {
			var n int64
			for i := 0; i < b.N; i++ {
				var err error
				if n, err = c.CountDocuments(ctx, []byte(tc.filter), CountOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n), "rows")
		})
	}
	for _, tc := range cases {
		b.Run("find50/"+tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				err := c.Query(ctx, []byte(tc.filter), QueryOptions{Limit: 50, Sort: []IndexField{{Field: "created_ms", Desc: true}}}, func(_, _ []byte) bool { return true })
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, tc := range cases {
		b.Run("groupCentre/"+tc.name, func(b *testing.B) {
			p := []byte(fmt.Sprintf(`[{"$match":%s},{"$group":{"_id":"$centre","n":{"$sum":1}}}]`, tc.filter))
			for i := 0; i < b.N; i++ {
				if _, err := c.Aggregate(ctx, p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestLikeMatchRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	alphabet := []string{"a", "B", "b", "%", "_", "é", "É", " ", "\\", "x"}
	gen := func(n int) string {
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	for i := 0; i < 200000; i++ {
		p, s := gen(rng.Intn(7)), gen(rng.Intn(10))
		got := likeMatch(asciiLower(nil, []byte(p)), asciiLower(nil, []byte(s)))
		if want := referenceLike(p, s); got != want {
			t.Fatalf("%q LIKE %q: got %v want %v", s, p, got, want)
		}
	}
}

func TestLikeRejectsInvalid(t *testing.T) {
	c, _ := likeFixture(t, 1)
	for _, f := range []string{
		`{"$like":"x"}`,
		`{"$like":{"fields":[],"pattern":"%"}}`,
		`{"$like":{"fields":["a"]}}`,
		`{"$like":{"fields":["a"],"pattern":1}}`,
		`{"$like":{"fields":["$a"],"pattern":"%"}}`,
		`{"$like":{"fields":[""],"pattern":"%"}}`,
		`{"$like":{"fields":["a"],"pattern":"%","escape":"\\"}}`,
		`{"$like":{"fields":["a"],"separator":1,"pattern":"%"}}`,
		`{"$like":{"fields":["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"],"pattern":"%"}}`,
		fmt.Sprintf(`{"$like":{"fields":["a"],"pattern":%q}}`, strings.Repeat("x", 4097)),
	} {
		if _, err := c.CountDocuments(context.Background(), []byte(f), CountOptions{}); err == nil {
			t.Errorf("accepted %s", f)
		}
	}
}

func TestLikeLiteral(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("labels")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rows := map[string]any{"percent": "Remise 100% ok", "digits": "100 points", "underscore": "x A_B y", "letter": "AxB", "backslash": `a\b`, "accent": "ÉNERGIE", "null": nil}
	for key, label := range rows {
		var b DocBuilder
		if label == nil {
			b.Null("label")
		} else {
			b.String("label", label.(string))
		}
		if err := c.Insert([]byte(key), b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		literal string
		want    []string
	}{
		{"100%", []string{"percent"}},
		{"%", []string{"percent"}},
		{"a_b", []string{"underscore"}},
		{"_", []string{"underscore"}},
		{`\`, []string{"backslash"}},
		{"REMISE", []string{"percent"}},
		{"énergie", nil},
		{"ÉNERGIE", []string{"accent"}},
		{"", []string{"accent", "backslash", "digits", "letter", "null", "percent", "underscore"}},
	} {
		literal, _ := json.Marshal(tc.literal)
		filter := []byte(fmt.Sprintf(`{"$like":{"fields":["label"],"literal":%s}}`, literal))
		var got []string
		err := c.Query(ctx, filter, QueryOptions{}, func(key, _ []byte) bool {
			got = append(got, string(key))
			return true
		})
		if err != nil {
			t.Fatal(tc.literal, err)
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("query %q: got %v want %v", tc.literal, got, tc.want)
		}
		n, err := c.CountDocuments(ctx, filter, CountOptions{})
		if err != nil || n != int64(len(tc.want)) {
			t.Errorf("count %q: got %d, %v, want %d", tc.literal, n, err, len(tc.want))
		}
		agg, err := c.Aggregate(ctx, []byte(fmt.Sprintf(`[{"$match":%s},{"$group":{"_id":null,"n":{"$sum":1}}}]`, filter)))
		if err != nil {
			t.Fatal(tc.literal, err)
		}
		total := "0"
		if len(agg) > 0 {
			total = fmt.Sprint(agg[0]["n"])
		}
		if total != fmt.Sprint(len(tc.want)) {
			t.Errorf("aggregate %q: %v want %d", tc.literal, agg, len(tc.want))
		}
	}
}

func TestLikeLiteralRejectsInvalid(t *testing.T) {
	c, _ := likeFixture(t, 1)
	for _, f := range []string{
		`{"$like":{"fields":["a"],"literal":1}}`,
		`{"$like":{"fields":["a"],"literal":null}}`,
		`{"$like":{"fields":["a"],"literal":"x","pattern":"%x%"}}`,
		fmt.Sprintf(`{"$like":{"fields":["a"],"literal":%q}}`, strings.Repeat("x", 4097)),
	} {
		if _, err := c.CountDocuments(context.Background(), []byte(f), CountOptions{}); err == nil {
			t.Errorf("accepted %s", f)
		}
	}
}
