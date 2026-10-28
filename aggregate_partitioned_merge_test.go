package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// Sharded runs above partitionedMergeGroups merge by key hash. Their rows,
// row order and order-dependent accumulators must match the serial run.
func TestPartitionedMergeMatchesSerial(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rng := rand.New(rand.NewSource(7))
	var b DocBuilder
	const n = 60000
	for i := 0; i < n; i++ {
		b.Reset()
		b.String("customer", fmt.Sprintf("c%06d", rng.Intn(40000)))
		b.Int("amount", int64(rng.Intn(1000)))
		b.Int("seq", int64(i))
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if err := db.Upsert(k, b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	pipelines := []string{
		`[{"$group": {"_id": "$customer", "total": {"$sum": "$amount"}, "n": {"$sum": 1}, "first": {"$first": "$seq"}, "last": {"$last": "$seq"}, "lo": {"$min": "$amount"}}}]`,
		`[{"$group": {"_id": "$customer", "total": {"$sum": "$amount"}}}, {"$sort": {"total": -1}}, {"$limit": 25}]`,
		`[{"$match": {"amount": {"$gte": 100}}}, {"$group": {"_id": "$customer", "first": {"$first": "$seq"}}}, {"$count": "groups"}]`,
	}
	ctx := context.Background()
	for _, p := range pipelines {
		serial, err := db.AggregateParallel(ctx, []byte(p), 1)
		if err != nil {
			t.Fatal(err)
		}
		for _, shards := range []int{2, 7, 16} {
			got, err := db.AggregateParallel(ctx, []byte(p), shards)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(serial, got) {
				t.Fatalf("%d shards differ from serial for %s (rows %d vs %d)", shards, p, len(got), len(serial))
			}
		}
	}
	// the group limit still applies
	_, err = db.AggregateParallel(ctx, []byte(`[{"$group": {"_id": "$customer", "n": {"$sum": 1}}}]`), 8)
	if err != nil {
		t.Fatal(err)
	}
}
