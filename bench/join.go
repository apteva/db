package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	aptevadb "github.com/apteva/db"
)

// Join benchmark: a users collection (N/10 documents) and an orders
// collection (N documents), queried with $lookup pipelines against
// SQLite's JOIN over typed tables carrying the same indexes. Pebble has no
// joins. Every engine's results are compared with the first engine's; a
// mismatch is printed on stderr.
//
// The first five queries run before any secondary index exists (SQLite
// still has its INTEGER PRIMARY KEY rowids); all seven run again once
// users(country), orders(user_id) and orders(amount) are indexed.

type joinQuery struct {
	name     string
	from     string // collection the pipeline runs over
	pipeline string
	sql      string
}

const lookupUsers = `{"$lookup":{"from":"users","localField":"user_id","foreignField":"id","as":"u"}},{"$unwind":"$u"}`
const lookupOrders = `{"$lookup":{"from":"orders","localField":"id","foreignField":"user_id","as":"os"}}`

var joinQueries = []joinQuery{
	{"orders JOIN users, group by country: count, sum", "orders",
		`[` + lookupUsers + `,{"$group":{"_id":"$u.country","n":{"$sum":1},"total":{"$sum":"$amount"}}}]`,
		`SELECT u.country, COUNT(*), SUM(o.amount) FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.country`},
	{"shipped orders of FR users: count, avg", "orders",
		`[{"$match":{"status":"shipped"}},` + lookupUsers + `,{"$match":{"u.country":"FR"}},{"$group":{"_id":null,"n":{"$sum":1},"avg":{"$avg":"$amount"}}}]`,
		`SELECT NULL, COUNT(*), AVG(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE o.status = 'shipped' AND u.country = 'FR'`},
	{"top 10 spenders (existing users only)", "orders",
		`[` + lookupUsers + `,{"$group":{"_id":"$user_id","total":{"$sum":"$amount"}}},{"$sort":{"total":-1}},{"$limit":10}]`,
		`SELECT o.user_id, SUM(o.amount) AS total FROM orders o JOIN users u ON u.id = o.user_id GROUP BY o.user_id ORDER BY total DESC LIMIT 10`},
	{"one user's orders (point join)", "users",
		`[{"$match":{"id":4242}},` + lookupOrders + `,{"$unwind":"$os"},{"$group":{"_id":"$os.id","amount":{"$sum":"$os.amount"}}}]`,
		`SELECT o.id, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 4242`},
	{"users with no orders (anti-join)", "users",
		`[` + lookupOrders + `,{"$unwind":{"path":"$os","preserveNullAndEmptyArrays":true}},{"$match":{"os":null}},{"$count":"n"}]`,
		`SELECT NULL, COUNT(*) FROM users u LEFT JOIN orders o ON o.user_id = u.id WHERE o.id IS NULL`},
}

var joinIndexQueries = []joinQuery{
	{"indexed: orders > 999.9 JOIN users, by country", "orders",
		`[{"$match":{"amount":{"$gt":999.9}}},` + lookupUsers + `,{"$group":{"_id":"$u.country","n":{"$sum":1}}}]`,
		`SELECT u.country, COUNT(*) FROM orders o JOIN users u ON u.id = o.user_id WHERE o.amount > 999.9 GROUP BY u.country`},
	{"indexed: FR users JOIN their orders, per user", "users",
		`[{"$match":{"country":"FR"}},` + lookupOrders + `,{"$unwind":"$os"},{"$group":{"_id":"$id","n":{"$sum":1},"total":{"$sum":"$os.amount"}}}]`,
		`SELECT u.id, COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON o.user_id = u.id WHERE u.country = 'FR' GROUP BY u.id`},
}

func runJoinBench(dir string, n int, engines []string) {
	nu, no := n/10, n
	countries := []string{"FR", "DE", "US", "JP", "GB", "IT", "ES", "NL", "SE", "PL", "BR", "MX", "CA", "AU", "IN", "CN", "KR", "ZA", "AR", "CH"}
	statuses := []string{"new", "shipped", "returned", "cancelled"}
	rng := rand.New(rand.NewSource(5))
	type user struct {
		id      int64
		name    string
		country string
		age     int64
	}
	type order struct {
		id     int64
		user   int64
		amount float64
		status string
	}
	users := make([]user, nu)
	for i := range users {
		users[i] = user{int64(i), fmt.Sprintf("user%06d", i), countries[rng.Intn(len(countries))], int64(18 + rng.Intn(60))}
	}
	orders := make([]order, no)
	for i := range orders {
		// a fifth of the users never order
		orders[i] = order{int64(i), int64(rng.Intn(nu * 4 / 5)), math.Round(rng.Float64()*100000) / 100, statuses[rng.Intn(4)]}
	}
	var b aptevadb.DocBuilder
	encUser := func(u user) []byte {
		b.Reset()
		b.Int("id", u.id)
		b.String("name", u.name)
		b.String("country", u.country)
		b.Int("age", u.age)
		return b.Bytes()
	}
	encOrder := func(o order) []byte {
		b.Reset()
		b.Int("id", o.id)
		b.Int("user_id", o.user)
		b.Float("amount", o.amount)
		b.String("status", o.status)
		return b.Bytes()
	}
	key := func(i int64) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	all := append(append([]joinQuery{}, joinQueries...), joinIndexQueries...)
	// phases: queries 0..4 without indexes, then all with indexes
	type phase struct {
		label   string
		queries []joinQuery
	}
	phases := []phase{{"no secondary indexes", joinQueries}, {"with indexes", all}}
	type timing struct {
		load, index time.Duration
		q           [][]time.Duration // per phase
	}
	results := map[string]timing{}
	var reference [][]aggResult
	const reps = 5

	for _, name := range engines {
		edir := filepath.Join(dir, "join-"+name)
		os.RemoveAll(edir)
		os.MkdirAll(edir, 0o755)
		runtime.GC()
		tm := timing{q: make([][]time.Duration, len(phases))}
		outs := make([][]aggResult, len(phases))
		par := strings.HasSuffix(name, "-par")
		base := strings.TrimSuffix(name, "-par")
		nshards := runtime.GOMAXPROCS(0)
		switch base {
		case "ours":
			db, err := aptevadb.Open(edir, aptevadb.Options{})
			if err != nil {
				panic(err)
			}
			cu, _ := db.Collection("users")
			co, _ := db.Collection("orders")
			start := time.Now()
			for _, u := range users {
				if err := cu.Upsert(key(u.id), encUser(u)); err != nil {
					panic(err)
				}
			}
			for _, o := range orders {
				if err := co.Upsert(key(o.id), encOrder(o)); err != nil {
					panic(err)
				}
			}
			db.Checkpoint()
			tm.load = time.Since(start)
			run := func(q joinQuery) aggResult {
				c := co
				if q.from == "users" {
					c = cu
				}
				var out []map[string]any
				var err error
				if par {
					out, err = c.AggregateParallel(context.Background(), []byte(q.pipeline), nshards)
				} else {
					out, err = c.AggregateParallel(context.Background(), []byte(q.pipeline), 1)
				}
				if err != nil {
					panic(q.name + ": " + err.Error())
				}
				return canon(out)
			}
			for pi, ph := range phases {
				if pi == 1 {
					start := time.Now()
					for _, ix := range []struct {
						c           *aptevadb.Collection
						name, field string
					}{
						{cu, "by_id", "id"}, {cu, "by_country", "country"}, {co, "by_user", "user_id"}, {co, "by_amount", "amount"},
					} {
						if err := ix.c.CreateIndex(context.Background(), ix.name, ix.field); err != nil {
							panic(err)
						}
					}
					tm.index = time.Since(start)
				}
				for _, q := range ph.queries {
					var out aggResult
					tm.q[pi] = append(tm.q[pi], best(reps, func() { out = run(q) }))
					outs[pi] = append(outs[pi], out)
				}
			}
			db.Close()
		case "sqlite-c", "sqlite-go":
			opener := openSQLiteC
			if base == "sqlite-go" {
				opener = openSQLiteGo
			}
			k, err := opener(edir, false, n)
			if err != nil {
				panic(err)
			}
			s := k.(*sqliteKV)
			exec := func(q string) {
				st := s.c.Prep(q)
				if _, err := st.Step(); err != nil {
					panic(err)
				}
				st.Reset()
			}
			exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, country TEXT, age INTEGER)")
			exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, status TEXT)")
			insU := s.c.Prep("INSERT INTO users (id, name, country, age) VALUES (?, ?, ?, ?)")
			insO := s.c.Prep("INSERT INTO orders (id, user_id, amount, status) VALUES (?, ?, ?, ?)")
			start := time.Now()
			exec("BEGIN")
			for _, u := range users {
				insU.BindInt64(1, u.id)
				insU.BindText(2, u.name)
				insU.BindText(3, u.country)
				insU.BindInt64(4, u.age)
				if _, err := insU.Step(); err != nil {
					panic(err)
				}
				insU.Reset()
			}
			for _, o := range orders {
				insO.BindInt64(1, o.id)
				insO.BindInt64(2, o.user)
				insO.BindFloat(3, o.amount)
				insO.BindText(4, o.status)
				if _, err := insO.Step(); err != nil {
					panic(err)
				}
				insO.Reset()
			}
			exec("COMMIT")
			tm.load = time.Since(start)
			for pi, ph := range phases {
				if pi == 1 {
					start := time.Now()
					exec("CREATE INDEX by_country ON users(country)")
					exec("CREATE INDEX by_user ON orders(user_id)")
					exec("CREATE INDEX by_amount ON orders(amount)")
					exec("ANALYZE")
					tm.index = time.Since(start)
				}
				for _, q := range ph.queries {
					st := s.c.Prep(q.sql)
					var out aggResult
					tm.q[pi] = append(tm.q[pi], best(reps, func() { out = sqlRows(st) }))
					outs[pi] = append(outs[pi], out)
				}
			}
			s.c.Close()
		default:
			panic("unknown join engine " + name)
		}
		results[name] = tm
		if *flagDir == "" {
			if err := os.RemoveAll(edir); err != nil {
				panic(err)
			}
		}
		if reference == nil {
			reference = outs
		} else {
			for pi := range outs {
				for qi := range outs[pi] {
					if msg := compareAgg(reference[pi][qi], outs[pi][qi]); msg != "" {
						panic(fmt.Sprintf("MISMATCH %s on %q (%s): %s", name, phases[pi].queries[qi].name, phases[pi].label, msg))
					}
				}
			}
		}
		fmt.Fprintf(os.Stderr, "%-10s load %.2fs index %.2fs", name, tm.load.Seconds(), tm.index.Seconds())
		for _, d := range tm.q[1] {
			fmt.Fprintf(os.Stderr, "  %.1fms", d.Seconds()*1000)
		}
		fmt.Fprintln(os.Stderr)
	}

	fmt.Printf("\njoins: %d users, %d orders (ms per query, best of %d; lower is better; ours = one core, ours-par = all cores)\n", nu, no, reps)
	for pi, ph := range phases {
		fmt.Printf("\n%s\n%-52s", ph.label, "query")
		for _, e := range engines {
			fmt.Printf(" %12s", e)
		}
		fmt.Println()
		for qi, q := range ph.queries {
			fmt.Printf("%-52s", q.name)
			for _, e := range engines {
				fmt.Printf(" %12.1f", results[e].q[pi][qi].Seconds()*1000)
			}
			fmt.Println()
		}
	}
	fmt.Printf("\n%-52s", "load both collections (seconds)")
	for _, e := range engines {
		fmt.Printf(" %12.2f", results[e].load.Seconds())
	}
	fmt.Println()
	fmt.Printf("%-52s", "build indexes (seconds)")
	for _, e := range engines {
		fmt.Printf(" %12.2f", results[e].index.Seconds())
	}
	fmt.Println()
}

// sqlRows runs a prepared statement to completion, keyed by its first
// column with the remaining numeric columns as values.
func sqlRows(st stmt) aggResult {
	out := aggResult{}
	for {
		ok, err := st.Step()
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		id := st.ColumnText(0)
		var vals []float64
		for c := 1; c < st.ColumnCount(); c++ {
			vals = append(vals, st.ColumnFloat(c))
		}
		out[id] = vals
	}
	st.Reset()
	return out
}
