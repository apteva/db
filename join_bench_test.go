package aptevadb

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkJoin loads 100k users and 1M orders once (BENCH_JOIN_DIR keeps
// the directory between runs) and times the join pipelines.
func BenchmarkJoin(b *testing.B) {
	dir := os.Getenv("BENCH_JOIN_DIR")
	fresh := false
	if dir == "" {
		dir = b.TempDir()
		fresh = true
	} else if _, err := os.Stat(filepath.Join(dir, "data")); err != nil {
		fresh = true
	}
	db, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	users, _ := db.Collection("users")
	orders, _ := db.Collection("orders")
	if fresh {
		rng := rand.New(rand.NewSource(5))
		countries := []string{"FR", "DE", "US", "JP", "GB", "IT", "ES", "NL", "SE", "PL"}
		var d DocBuilder
		key := func(i int) []byte { k := make([]byte, 8); binary.BigEndian.PutUint64(k, uint64(i)); return k }
		for i := 0; i < 100_000; i++ {
			d.Reset()
			d.Int("id", int64(i))
			d.String("name", fmt.Sprintf("user%06d", i))
			d.String("country", countries[rng.Intn(10)])
			users.Insert(key(i), d.Bytes())
		}
		for i := 0; i < 1_000_000; i++ {
			d.Reset()
			d.Int("id", int64(i))
			d.Int("user_id", int64(rng.Intn(80_000)))
			d.Float("amount", float64(rng.Intn(100000))/100)
			d.String("status", "shipped")
			orders.Insert(key(i), d.Bytes())
		}
		db.Checkpoint()
	}
	if _, ok := orders.Indexes()["by_user"]; !ok {
		if err := orders.CreateIndex(bg, "by_user", "user_id"); err != nil {
			b.Fatal(err)
		}
		if err := users.CreateIndex(bg, "by_country", "country"); err != nil {
			b.Fatal(err)
		}
	}
	for _, q := range []struct{ name, from, p string }{
		{"group-by-country", "orders", `[{"$lookup":{"from":"users","localField":"user_id","foreignField":"id","as":"u"}},{"$unwind":"$u"},{"$group":{"_id":"$u.country","n":{"$sum":1},"total":{"$sum":"$amount"}}}]`},
		{"anti-join", "users", `[{"$lookup":{"from":"orders","localField":"id","foreignField":"user_id","as":"os"}},{"$unwind":{"path":"$os","preserveNullAndEmptyArrays":true}},{"$match":{"os":null}},{"$count":"n"}]`},
		{"scan-only", "orders", `[{"$group":{"_id":"$status","n":{"$sum":1},"total":{"$sum":"$amount"}}}]`},
		{"range-filter", "orders", `[{"$match":{"amount":{"$gt":500}}},{"$group":{"_id":null,"n":{"$sum":1},"avg":{"$avg":"$amount"}}}]`},
		{"fr-users-orders", "users", `[{"$match":{"country":"FR"}},{"$lookup":{"from":"orders","localField":"id","foreignField":"user_id","as":"os"}},{"$unwind":"$os"},{"$group":{"_id":"$id","n":{"$sum":1},"total":{"$sum":"$os.amount"}}}]`},
	} {
		c := orders
		if q.from == "users" {
			c = users
		}
		b.Run(q.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := c.AggregateParallel(bg, []byte(q.p), 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
