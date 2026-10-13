package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func BenchmarkAggregateQ3(b *testing.B) {
	db, _ := Open(b.TempDir(), Options{})
	defer db.Close()
	rng := rand.New(rand.NewSource(3))
	regions := []string{"north", "south", "east", "west", "central", "coast", "plains", "hills"}
	statuses := []string{"new", "shipped", "returned", "cancelled"}
	var d DocBuilder
	for i := 0; i < 1_000_000; i++ {
		d.Reset()
		d.Int("id", int64(i))
		d.String("region", regions[rng.Intn(8)])
		d.String("status", statuses[rng.Intn(4)])
		d.Float("amount", math.Round(rng.Float64()*100000)/100)
		d.Int("qty", int64(1+rng.Intn(20)))
		d.String("customer", fmt.Sprintf("c%06d", rng.Intn(100000)))
		k := make([]byte, 16)
		binary.BigEndian.PutUint64(k, uint64(i))
		db.Upsert(k, d.Bytes())
	}
	p, _ := ParsePipeline([]byte(`[{"$match": {"amount": {"$gt": 500}}}, {"$group": {"_id": null, "n": {"$sum": 1}, "avg": {"$avg": "$amount"}}}]`))
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.Run(bg, db.Source(nil, nil)); err != nil {
			b.Fatal(err)
		}
	}
}
