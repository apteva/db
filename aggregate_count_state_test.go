package db

import (
	"fmt"
	"strings"
	"testing"
)

// A lone count runs on the compact key->counter state when disk use is
// allowed. Alpha.9 and alpha.10 assigned counters through the scratch key
// buffer, which replaced each existing entry's stored key with those bytes:
// later rows missed their group and most counts came back 0.
func TestCompactCountKeepsKeys(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 6000; i++ {
		var b DocBuilder
		b.Int("g", int64(i%600))
		b.String("s", fmt.Sprintf("group-%04d", i%600))
		if (i/600)%2 == 0 { // half of every group's rows
			b.Int("v", int64(i))
		}
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%06d", i)), b.Bytes()))
	}
	for _, q := range []string{
		`[{"$group":{"_id":"$g","n":{"$count":{}}}}]`,
		`[{"$group":{"_id":"$s","n":{"$count":{}}}}]`,
		`[{"$group":{"_id":"$s","n":{"$countNonNull":"$v"}}}]`,
	} {
		want := int64(10)
		if strings.Contains(q, "$countNonNull") {
			want = 5
		}
		// in memory, and with a budget small enough to spill and merge partials
		for _, budget := range []int64{0, 24 << 10} {
			var st AggregateStats
			seen := 0
			err := c.AggregateEach(bg, []byte(q), AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: budget, Stats: &st}, func(r map[string]any) bool {
				seen++
				if r["n"] != want {
					t.Fatalf("%s budget=%d: %v", q, budget, r)
				}
				return true
			})
			mustCompat(t, err)
			if seen != 600 || (budget > 0 && st.SpillPartitions < 2) {
				t.Fatalf("%s budget=%d: %d groups, stats %+v", q, budget, seen, st)
			}
		}
	}
}
