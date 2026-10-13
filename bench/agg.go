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
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"

	aptevadb "github.com/apteva/db"
)

// Aggregation benchmark: N order documents, four pipelines, three engines.
// Pebble has no aggregation, so it runs the engine's executor over its own
// iterator: that measures the storage layer only. SQLite runs the SQL
// equivalent over a typed table, its best case.

type aggQuery struct {
	name     string
	pipeline string
	sql      string
}

var aggQueries = []aggQuery{
	{"group by region: sum, count",
		`[{"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "n": {"$sum": 1}}}]`,
		`SELECT region, SUM(amount), COUNT(*) FROM orders GROUP BY region`},
	{"filter + group + sort + limit",
		`[{"$match": {"status": "shipped"}},
		  {"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "avg": {"$avg": "$amount"}, "maxq": {"$max": "$qty"}}},
		  {"$sort": {"total": -1}}, {"$limit": 3}]`,
		`SELECT region, SUM(amount) AS total, AVG(amount), MAX(qty) FROM orders WHERE status = 'shipped' GROUP BY region ORDER BY total DESC LIMIT 3`},
	{"range filter, global count + avg",
		`[{"$match": {"amount": {"$gt": 500}}}, {"$group": {"_id": null, "n": {"$sum": 1}, "avg": {"$avg": "$amount"}}}]`,
		`SELECT NULL, COUNT(*), AVG(amount) FROM orders WHERE amount > 500`},
	{"group by customer (100k groups), top 10",
		`[{"$group": {"_id": "$customer", "total": {"$sum": "$amount"}}}, {"$sort": {"total": -1}}, {"$limit": 10}]`,
		`SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer ORDER BY total DESC LIMIT 10`},
}

// indexQueries run after an index on customer and on amount exists.
var indexQueries = []aggQuery{
	{"indexed: one customer's orders, sum",
		`[{"$match": {"customer": "c012345"}}, {"$group": {"_id": null, "n": {"$sum": 1}, "total": {"$sum": "$amount"}}}]`,
		`SELECT NULL, COUNT(*), SUM(amount) FROM orders WHERE customer = 'c012345'`},
	{"indexed: 20 customers ($in), count",
		`[{"$match": {"customer": {"$in": ["c000001","c000002","c000003","c000004","c000005","c000006","c000007","c000008","c000009","c000010","c000011","c000012","c000013","c000014","c000015","c000016","c000017","c000018","c000019","c000020"]}}}, {"$count": "n"}]`,
		`SELECT NULL, COUNT(*) FROM orders WHERE customer IN ('c000001','c000002','c000003','c000004','c000005','c000006','c000007','c000008','c000009','c000010','c000011','c000012','c000013','c000014','c000015','c000016','c000017','c000018','c000019','c000020')`},
	{"indexed: amount > 999.5 (0.05%), group by region",
		`[{"$match": {"amount": {"$gt": 999.5}}}, {"$group": {"_id": "$region", "n": {"$sum": 1}}}]`,
		`SELECT region, COUNT(*) FROM orders WHERE amount > 999.5 GROUP BY region`},
}

type aggResult map[string][]float64 // group id -> numeric columns

func runAggBench(dir string, n int, engines []string) {
	regions := []string{"north", "south", "east", "west", "central", "coast", "plains", "hills"}
	statuses := []string{"new", "shipped", "returned", "cancelled"}
	rng := rand.New(rand.NewSource(3))
	type row struct {
		id       int64
		region   string
		status   string
		amount   float64
		qty      int64
		customer string
	}
	rows := make([]row, n)
	for i := range rows {
		rows[i] = row{int64(i), regions[rng.Intn(8)], statuses[rng.Intn(4)],
			math.Round(rng.Float64()*100000) / 100, int64(1 + rng.Intn(20)), fmt.Sprintf("c%06d", rng.Intn(100000))}
	}
	var b aptevadb.DocBuilder
	encode := func(r row) []byte {
		b.Reset()
		b.Int("id", r.id)
		b.String("region", r.region)
		b.String("status", r.status)
		b.Float("amount", r.amount)
		b.Int("qty", r.qty)
		b.String("customer", r.customer)
		return b.Bytes()
	}
	key := func(i int) []byte {
		k := make([]byte, 16)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	pipes := make([]*aptevadb.Pipeline, len(aggQueries))
	for i, q := range aggQueries {
		p, err := aptevadb.ParsePipeline([]byte(q.pipeline))
		if err != nil {
			panic(err)
		}
		pipes[i] = p
	}

	type timing struct {
		load, index time.Duration
		q           []time.Duration
	}
	results := map[string]timing{}
	var reference []aggResult
	const reps = 5

	for _, name := range engines {
		edir := filepath.Join(dir, "agg-"+name)
		os.RemoveAll(edir)
		os.MkdirAll(edir, 0o755)
		runtime.GC()
		tm := timing{q: make([]time.Duration, len(aggQueries))}
		var outs []aggResult
		par := strings.HasSuffix(name, "-par")
		base := strings.TrimSuffix(name, "-par")
		nshards := runtime.GOMAXPROCS(0)
		switch base {
		case "ours":
			db, err := aptevadb.Open(edir, aptevadb.Options{})
			if err != nil {
				panic(err)
			}
			start := time.Now()
			for i := range rows {
				if err := db.Upsert(key(i), encode(rows[i])); err != nil {
					panic(err)
				}
			}
			db.Checkpoint()
			tm.load = time.Since(start)
			for qi, q := range aggQueries {
				var out []map[string]any
				tm.q[qi] = best(reps, func() {
					var err error
					if par {
						out, err = db.AggregateParallel(context.Background(), []byte(q.pipeline), nshards)
					} else {
						out, err = db.AggregateParallel(context.Background(), []byte(q.pipeline), 1)
					}
					if err != nil {
						panic(err)
					}
				})
				outs = append(outs, canon(out))
			}
			if !par {
				start := time.Now()
				if err := db.CreateIndex(context.Background(), "by_customer", "customer"); err != nil {
					panic(err)
				}
				if err := db.CreateIndex(context.Background(), "by_amount", "amount"); err != nil {
					panic(err)
				}
				tm.index = time.Since(start)
				for _, q := range indexQueries {
					var out []map[string]any
					tm.q = append(tm.q, best(reps, func() {
						var err error
						if out, err = db.AggregateParallel(context.Background(), []byte(q.pipeline), 1); err != nil {
							panic(err)
						}
					}))
					outs = append(outs, canon(out))
				}
			}
			db.Close()
		case "pebble":
			opts := &pebble.Options{CacheSize: 512 << 20, MemTableSize: 64 << 20, Logger: quiet{}}
			db, err := pebble.Open(edir, opts)
			if err != nil {
				panic(err)
			}
			start := time.Now()
			batch := db.NewBatch()
			for i := range rows {
				batch.Set(key(i), encode(rows[i]), nil)
				if batch.Count() >= 1000 {
					if err := batch.Commit(pebble.NoSync); err != nil {
						panic(err)
					}
					batch.Close()
					batch = db.NewBatch()
				}
			}
			batch.Commit(pebble.NoSync)
			batch.Close()
			db.Flush()
			db.Compact(ctx(), key(0), key(n), false)
			tm.load = time.Since(start)
			mkSrc := func(lo, hi []byte) aptevadb.Source {
				return func(yield func(k, v []byte) bool) error {
					it, err := db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
					if err != nil {
						return err
					}
					defer it.Close()
					for ok := it.First(); ok; ok = it.Next() {
						if !yield(it.Key(), it.Value()) {
							break
						}
					}
					return it.Error()
				}
			}
			// keys are dense 8-byte ids, so even id ranges are even shards
			var shards []aptevadb.Source
			for i := 0; i < nshards; i++ {
				var lo, hi []byte
				if i > 0 {
					lo = key(n * i / nshards)
				}
				if i < nshards-1 {
					hi = key(n * (i + 1) / nshards)
				}
				shards = append(shards, mkSrc(lo, hi))
			}
			for qi, p := range pipes {
				var out []map[string]any
				tm.q[qi] = best(reps, func() {
					var err error
					if par {
						out, err = p.RunSharded(context.Background(), shards)
					} else {
						out, err = p.Run(context.Background(), mkSrc(nil, nil))
					}
					if err != nil {
						panic(err)
					}
				})
				outs = append(outs, canon(out))
			}
			db.Close()
		case "sqlite-c":
			k, err := openSQLiteC(edir, false, n)
			if err != nil {
				panic(err)
			}
			s := k.(*sqliteKV)
			for _, q := range []string{
				"CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT, status TEXT, amount REAL, qty INTEGER, customer TEXT)",
			} {
				st := s.c.Prep(q)
				st.Step()
				st.Reset()
			}
			ins := s.c.Prep("INSERT INTO orders (id, region, status, amount, qty, customer) VALUES (?, ?, ?, ?, ?, ?)")
			start := time.Now()
			s.begin.Step()
			s.begin.Reset()
			for _, r := range rows {
				ins.BindInt64(1, r.id)
				ins.BindText(2, r.region)
				ins.BindText(3, r.status)
				ins.BindFloat(4, r.amount)
				ins.BindInt64(5, r.qty)
				ins.BindText(6, r.customer)
				if _, err := ins.Step(); err != nil {
					panic(err)
				}
				ins.Reset()
			}
			s.commit.Step()
			s.commit.Reset()
			tm.load = time.Since(start)
			runSQL := func(sql string) aggResult {
				st := s.c.Prep(sql)
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
			for qi, q := range aggQueries {
				st := s.c.Prep(q.sql)
				var out aggResult
				tm.q[qi] = best(reps, func() {
					out = aggResult{}
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
				})
				outs = append(outs, out)
			}
			start = time.Now()
			for _, q := range []string{"CREATE INDEX by_customer ON orders(customer)", "CREATE INDEX by_amount ON orders(amount)"} {
				st := s.c.Prep(q)
				if _, err := st.Step(); err != nil {
					panic(err)
				}
				st.Reset()
			}
			tm.index = time.Since(start)
			for _, q := range indexQueries {
				var out aggResult
				sql := q.sql
				tm.q = append(tm.q, best(reps, func() { out = runSQL(sql) }))
				outs = append(outs, out)
			}
			s.c.Close()
		default:
			panic("unknown aggregation engine " + name)
		}
		results[name] = tm
		if *flagDir == "" {
			if err := os.RemoveAll(edir); err != nil {
				panic(err)
			}
		}
		allQueries := append(append([]aggQuery{}, aggQueries...), indexQueries...)
		if reference == nil || len(outs) > len(reference) {
			if reference != nil {
				for qi := range reference {
					if msg := compareAgg(reference[qi], outs[qi]); msg != "" {
						fmt.Fprintf(os.Stderr, "MISMATCH %s on %q: %s\n", name, allQueries[qi].name, msg)
					}
				}
			}
			reference = outs
		} else {
			for qi := range outs {
				if msg := compareAgg(reference[qi], outs[qi]); msg != "" {
					fmt.Fprintf(os.Stderr, "MISMATCH %s on %q: %s\n", name, allQueries[qi].name, msg)
				}
			}
		}
		fmt.Fprintf(os.Stderr, "%-10s load %.2fs", name, tm.load.Seconds())
		for qi := range aggQueries {
			fmt.Fprintf(os.Stderr, "  q%d %.1fms", qi+1, tm.q[qi].Seconds()*1000)
		}
		fmt.Fprintln(os.Stderr)
	}

	fmt.Printf("\naggregation over %d documents (ms per query, best of %d; lower is better; ours = one core, ours-par = all cores)\n\n", n, reps)
	fmt.Printf("%-48s", "query")
	for _, e := range engines {
		fmt.Printf(" %12s", e)
	}
	fmt.Println()
	all := append(append([]aggQuery{}, aggQueries...), indexQueries...)
	for qi, q := range all {
		fmt.Printf("%-48s", q.name)
		for _, e := range engines {
			if qi < len(results[e].q) {
				fmt.Printf(" %12.1f", results[e].q[qi].Seconds()*1000)
			} else {
				fmt.Printf(" %12s", "-")
			}
		}
		fmt.Println()
	}
	fmt.Printf("%-48s", "load (seconds)")
	for _, e := range engines {
		fmt.Printf(" %12.2f", results[e].load.Seconds())
	}
	fmt.Println()
	fmt.Printf("%-48s", "build two indexes (seconds)")
	for _, e := range engines {
		if results[e].index > 0 {
			fmt.Printf(" %12.2f", results[e].index.Seconds())
		} else {
			fmt.Printf(" %12s", "-")
		}
	}
	fmt.Println()
}

func best(reps int, fn func()) time.Duration {
	var b time.Duration
	for i := 0; i < reps; i++ {
		start := time.Now()
		fn()
		if d := time.Since(start); i == 0 || d < b {
			b = d
		}
	}
	return b
}

// canon flattens executor rows into id -> numeric columns, sorted by name.
func canon(rows []map[string]any) aggResult {
	out := aggResult{}
	for _, r := range rows {
		id := ""
		if v, ok := r["_id"]; ok && v != nil {
			id = fmt.Sprint(v)
		}
		names := make([]string, 0, len(r))
		for k := range r {
			if k != "_id" {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		var vals []float64
		for _, k := range names {
			switch x := r[k].(type) {
			case int64:
				vals = append(vals, float64(x))
			case float64:
				vals = append(vals, x)
			}
		}
		out[id] = vals
	}
	return out
}

func compareAgg(a, b aggResult) string {
	if len(a) != len(b) {
		return fmt.Sprintf("%d groups vs %d", len(a), len(b))
	}
	for id, va := range a {
		vb, ok := b[id]
		if !ok {
			return "missing group " + id
		}
		if len(va) != len(vb) {
			return fmt.Sprintf("group %s: %d cols vs %d", id, len(va), len(vb))
		}
		// SQL and pipeline column order differ; compare as multisets
		sa, sb := append([]float64(nil), va...), append([]float64(nil), vb...)
		sort.Float64s(sa)
		sort.Float64s(sb)
		for i := range sa {
			if math.Abs(sa[i]-sb[i]) > 1e-6*math.Max(1, math.Abs(sa[i])) {
				return fmt.Sprintf("group %s: %v vs %v", id, va, vb)
			}
		}
	}
	return ""
}
