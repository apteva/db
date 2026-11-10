package db

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestComputedDistinctBulkMerge(t *testing.T) {
	for _, mem := range []int64{-1, 2 << 20} {
		t.Run(fmt.Sprint(mem), func(t *testing.T) {
			ctx := context.Background()
			d, err := Open(t.TempDir(), Options{CacheBytes: 8 << 20, MemtableBytes: mem})
			mustCompat(t, err)
			t.Cleanup(func() { d.Close() })
			facts, err := d.Collection("facts")
			mustCompat(t, err)
			batch := facts.NewBatch()
			for group := 0; group < 80; group++ {
				for person := 0; person < 40; person++ {
					doc := fmt.Appendf(nil, `{"day":%d,"caller":%d,"client":%d}`, group, (group+person)%55, (group+person)%13)
					mustCompat(t, batch.UpsertJSON(fmt.Appendf(nil, "%03d/%03d", group, person), doc))
				}
			}
			mustCompat(t, batch.Commit())
			status, err := d.BeginComputedCollection(ctx, "metrics", []ComputedCollectionInput{
				{Source: "facts", Pipeline: []byte(`[{"$group":{"_id":{"day":"$day"},"callers":{"$countDistinct":"$caller"},"clients":{"$countDistinct":"$client"}}}]`)},
			})
			mustCompat(t, err)
			for !status.Ready {
				status, err = d.AdvanceComputedCollection(ctx, "metrics", 127)
				mustCompat(t, err)
			}
			mustCompat(t, d.Checkpoint())
			// Replace the only occurrence of an identity in a bucket.
			// The old membership is a tombstone on the buffered path.
			mustCompat(t, facts.UpsertJSON([]byte("000/000"), []byte(`{"day":0,"caller":100,"client":0}`)))
			snapshot, err := d.Snapshot()
			mustCompat(t, err)
			defer snapshot.Close()
			view, err := snapshot.Collection("metrics")
			mustCompat(t, err)
			var buckets []ComputedDistinctBucket
			for group := 79; group >= 0; group-- {
				key, err := ComputedGroupTupleKey(int64(group))
				mustCompat(t, err)
				if group%3 == 0 {
					buckets = append(buckets, ComputedDistinctBucket{key, "callers", 0}, ComputedDistinctBucket{key, "callers", 1})
				}
				if group%4 == 0 {
					buckets = append(buckets, ComputedDistinctBucket{key, "clients", 0})
				}
				if group == 0 {
					buckets = append(buckets, ComputedDistinctBucket{key, "callers", 2})
				}
			}
			buckets = append(buckets, buckets[0])
			want := make([]int64, 4)
			sets := make([]map[string]bool, len(want))
			for _, b := range buckets {
				if sets[b.Result] == nil {
					sets[b.Result] = map[string]bool{}
				}
				mustCompat(t, view.ScanComputedDistinct(ctx, b.Key, b.Measure, func(k []byte) bool {
					sets[b.Result][string(k)] = true
					return true
				}))
				want[b.Result] = int64(len(sets[b.Result]))
			}
			mustCompat(t, facts.Delete([]byte("000/000")))
			stats := &ComputedDistinctMergeStats{}
			got, err := view.MergeComputedDistinct(ctx, buckets, len(want), ComputedDistinctMergeOptions{Stats: stats})
			mustCompat(t, err)
			if !reflect.DeepEqual(got, want) || got[2] != 40 || stats.SingletonResults != 1 || stats.Identities == 0 {
				t.Fatalf("got %v (%+v), want %v", got, stats, want)
			}
			if mem < 0 && (stats.Leaves == 0 || stats.Seeks >= stats.Prefixes) {
				t.Fatalf("expected prefixes to share leaf walks: %+v", stats)
			}
			// More than 64 result states use the sparse membership path.
			wide := append([]ComputedDistinctBucket(nil), buckets...)
			for _, bucket := range buckets {
				if bucket.Result == 1 {
					bucket.Result = 64
					wide = append(wide, bucket)
				}
			}
			merged, err := view.MergeComputedDistinct(ctx, wide, 65, ComputedDistinctMergeOptions{})
			mustCompat(t, err)
			if !reflect.DeepEqual(merged[:4], want) || merged[64] != want[1] {
				t.Fatalf("wide merge: %v", merged)
			}
			_, err = view.MergeComputedDistinct(ctx, buckets, len(want), ComputedDistinctMergeOptions{MaxMemoryBytes: 16})
			if !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = view.MergeComputedDistinct(canceled, buckets, len(want), ComputedDistinctMergeOptions{})
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
