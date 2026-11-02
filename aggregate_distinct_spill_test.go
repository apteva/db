package db

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// One global group with eight filtered distinct counts whose sets together
// exceed the state limit. Partitioning by group key cannot divide one key;
// alpha.13 recursed until "aggregation partition depth exhausted". Counts
// and the other accumulators must match the in-memory reference exactly.
func TestGlobalDistinctSpillIsExact(t *testing.T) {
	const rows, callers = 64084, 24443
	c := aggregateFixture(t, 0)
	b := c.DB().NewBatch()
	for i := 0; i < rows; i++ {
		rank := i % callers
		skew := "big"
		if i%8 == 0 {
			skew = fmt.Sprint("s", i%500)
		}
		mustCompat(t, b.With(c).InsertJSON([]byte(fmt.Sprintf("%08d", i)), []byte(fmt.Sprintf(`{"caller":"c%08d","rank":%d,"g":"g%d","skew":%q}`, rank, rank, i%3, skew))))
	}
	mustCompat(t, b.Commit())
	thresholds := []int{21180, 21181, 21182, 21183, 21184, 21187, 21920, 24443}
	var accs []string
	for i, th := range thresholds {
		accs = append(accs, fmt.Sprintf(`"u%d":{"$countDistinct":"$caller","where":{"rank":{"$lt":%d}}}`, i, th))
	}
	accs = append(accs, `"n":{"$sum":1}`, `"total":{"$sum":"$rank"}`, `"lo":{"$min":"$caller"}`, `"hi":{"$max":"$caller"}`)
	for _, tc := range []struct {
		name, id string
		memory   int64
	}{
		// The Database app's default: 16 MiB, so an 8 MiB state limit.
		{"global/16MiB", "null", 16 << 20},
		{"global/2MiB", "null", 2 << 20},
		// A few groups: each group's run is mixed until key repartitioning
		// isolates it, then its sets are sliced the same way.
		{"grouped/2MiB", `"$g"`, 2 << 20},
		// One large group among 500 small ones: key repartitioning first
		// isolates the large group, which is then sliced.
		{"skewed/2MiB", `"$skew"`, 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := `[{"$group":{"_id":` + tc.id + `,` + strings.Join(accs, ",") + `}}]`
			want, err := referenceAggregate(c, p)
			mustCompat(t, err)
			if tc.id == "null" {
				for i, th := range thresholds {
					if got := want[0][fmt.Sprint("u", i)]; got != int64(th) {
						t.Fatalf("reference u%d = %v", i, got)
					}
				}
			}
			var st AggregateStats
			got := eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: tc.memory, AllowDiskUse: true, Stats: &st})
			canonicalRows(want)
			canonicalRows(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v\nwant %v", got, want)
			}
			t.Logf("%+v", st)
			if st.SpillRuns == 0 || st.DistinctSlices == 0 {
				t.Fatalf("expected sliced distinct state: %+v", st)
			}
			// GroupView reads the same final state.
			var groups []map[string]any
			mustCompat(t, c.AggregateGroups(bg, []byte(p), AggregateOptions{MaxMemoryBytes: tc.memory, AllowDiskUse: true}, func(v GroupView) error {
				row := map[string]any{}
				for i := range thresholds {
					row[fmt.Sprint("u", i)], _ = v.Int64(fmt.Sprint("u", i))
				}
				groups = append(groups, row)
				return nil
			}))
			var wantU []map[string]any
			for _, w := range want {
				row := map[string]any{}
				for i := range thresholds {
					row[fmt.Sprint("u", i)] = w[fmt.Sprint("u", i)]
				}
				wantU = append(wantU, row)
			}
			canonicalRows(groups)
			canonicalRows(wantU)
			if !reflect.DeepEqual(groups, wantU) {
				t.Fatalf("group view %v\nwant %v", groups, wantU)
			}
		})
	}
}
