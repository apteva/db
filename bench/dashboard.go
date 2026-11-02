package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	csqlite "crawshaw.io/sqlite"
	"github.com/cockroachdb/pebble/v2"
	gosqlite "zombiezen.com/go/sqlite"

	aptevadb "github.com/apteva/db"
)

// Dashboard benchmark: call records with campaigns, statuses, callers and
// timestamps, plus a prospects collection keyed by record key. The queries
// are what a reporting dashboard asks for: several filtered metrics in one
// pass, local-time buckets, distinct counts and key joins. SQLite runs the
// SQL equivalent (FILTER clauses, COUNT(DISTINCT), JOIN on the primary key);
// it has no IANA zones, so local-time buckets call a Go function, which is
// how an application gets daylight-saving-correct buckets from it. Pebble
// runs the engine's executor over its iterator, so it has no joins.

type dashQuery struct {
	name     string
	pipeline string
	sql      string
	join     bool // needs the prospects collection
}

const dashParisDay = `{"$dateTrunc":{"date":"$created_at","unit":"day","timezone":"Europe/Paris"}}`

var dashQueries = []dashQuery{
	{"3 filtered metrics by campaign",
		`[{"$group":{"_id":"$campaign","calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}},"revenue":{"$sum":"$amount","where":{"kind":"sale"}}}}]`,
		`SELECT campaign, COUNT(*), COUNT(*) FILTER (WHERE status = 'answered'), COALESCE(SUM(amount) FILTER (WHERE kind = 'sale'), 0) FROM calls GROUP BY campaign`, false},
	{"6-metric dashboard by campaign",
		`[{"$group":{"_id":"$campaign","calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}},"revenue":{"$sum":"$amount","where":{"kind":"sale"}},
		  "avg_answered":{"$avg":"$amount","where":{"status":"answered"}},"max_sale":{"$max":"$amount","where":{"kind":"sale"}},"callers":{"$countDistinct":"$caller"}}}]`,
		`SELECT campaign, COUNT(*), COUNT(*) FILTER (WHERE status = 'answered'), COALESCE(SUM(amount) FILTER (WHERE kind = 'sale'), 0),
		  AVG(amount) FILTER (WHERE status = 'answered'), MAX(amount) FILTER (WHERE kind = 'sale'), COUNT(DISTINCT caller) FROM calls GROUP BY campaign`, false},
	{"daily UTC buckets x campaign",
		`[{"$group":{"_id":{"c":"$campaign","d":{"$dateTrunc":{"date":"$created_at","unit":"day"}}},"calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}}}}]`,
		`SELECT campaign || '|' || date(created_at), COUNT(*), COUNT(*) FILTER (WHERE status = 'answered') FROM calls GROUP BY campaign, date(created_at)`, false},
	{"daily Paris buckets x campaign",
		`[{"$group":{"_id":{"c":"$campaign","d":` + dashParisDay + `},"calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}}}}]`,
		`SELECT campaign || '|' || paris(created_at, 'day'), COUNT(*), COUNT(*) FILTER (WHERE status = 'answered') FROM calls GROUP BY campaign, paris(created_at, 'day')`, false},
	{"weekly Paris revenue (Monday)",
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$created_at","unit":"week","timezone":"Europe/Paris","startOfWeek":"monday"}},"revenue":{"$sum":"$amount","where":{"kind":"sale"}},"calls":{"$count":{}}}}]`,
		`SELECT paris(created_at, 'week'), COALESCE(SUM(amount) FILTER (WHERE kind = 'sale'), 0), COUNT(*) FROM calls GROUP BY 1`, false},
	{"hourly UTC buckets",
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$created_at","unit":"hour"}},"calls":{"$count":{}}}}]`,
		`SELECT strftime('%Y-%m-%dT%H:00:00Z', created_at), COUNT(*) FROM calls GROUP BY 1`, false},
	{"distinct callers by campaign",
		`[{"$group":{"_id":"$campaign","callers":{"$countDistinct":"$caller"}}}]`,
		`SELECT campaign, COUNT(DISTINCT caller) FROM calls GROUP BY campaign`, false},
	{"distinct answered callers by campaign",
		`[{"$group":{"_id":"$campaign","callers":{"$countDistinct":"$caller","where":{"status":"answered"}}}}]`,
		`SELECT campaign, COUNT(DISTINCT caller) FILTER (WHERE status = 'answered') FROM calls GROUP BY campaign`, false},
	{"key join: calls by prospect city",
		`[{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$unwind":"$p"},{"$group":{"_id":"$p.city","calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}}}}]`,
		`SELECT p.city, COUNT(*), COUNT(*) FILTER (WHERE c.status = 'answered') FROM calls c JOIN prospects p ON p.id = c.prospect GROUP BY p.city`, true},
	{"key join, one campaign (1%)",
		`[{"$match":{"campaign":"camp007"}},{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$unwind":"$p"},{"$group":{"_id":"$p.segment","calls":{"$count":{}}}}]`,
		`SELECT p.segment, COUNT(*) FROM calls c JOIN prospects p ON p.id = c.prospect WHERE c.campaign = 'camp007' GROUP BY p.segment`, true},
}

type dashCallRow struct {
	campaign, status, kind, caller, created, prospect string
	amount                                            int64
}

// parisBucket is the SQLite UDF: the local Paris day or Monday week of an
// RFC 3339 timestamp. It remembers the last input, as rows arrive in time order.
func parisBucket() func(ts, unit string) (string, bool) {
	paris, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		panic(err)
	}
	var lastTS, lastUnit, lastOut string
	return func(ts, unit string) (string, bool) {
		if ts == lastTS && unit == lastUnit {
			return lastOut, true
		}
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return "", false
		}
		l := t.In(paris)
		if unit == "week" {
			back := (int(l.Weekday()) + 6) % 7
			l = time.Date(l.Year(), l.Month(), l.Day()-back, 12, 0, 0, 0, paris)
		}
		lastTS, lastUnit, lastOut = ts, unit, l.Format("2006-01-02")
		return lastOut, true
	}
}

func runDashboardBench(dir string, n int, engines []string, reps int) {
	rng := rand.New(rand.NewSource(17))
	nprospects := max(1000, n/5)
	statuses := []string{"answered", "answered", "answered", "missed", "missed", "voicemail"}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	span := int64(300 * 86400)
	rows := make([]dashCallRow, n)
	for i := range rows {
		r := dashCallRow{
			campaign: fmt.Sprintf("camp%03d", rng.Intn(100)),
			status:   statuses[rng.Intn(len(statuses))],
			kind:     "none",
			caller:   fmt.Sprintf("+3361%06d", rng.Intn(max(100, n/20))),
			amount:   int64(rng.Intn(100000)),
			// calls arrive in time order, with jitter
			created:  time.Unix(start+span*int64(i)/int64(n)+rng.Int63n(600), 0).UTC().Format(time.RFC3339),
			prospect: fmt.Sprintf("p%07d", rng.Intn(nprospects)),
		}
		if rng.Intn(5) == 0 {
			r.kind = "sale"
		}
		if rng.Intn(50) == 0 {
			r.prospect = "unknown"
		}
		rows[i] = r
	}
	cities := []string{"Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims"}
	segments := []string{"smb", "mid", "enterprise", "public"}
	var b aptevadb.DocBuilder
	encodeCall := func(r dashCallRow) []byte {
		b.Reset()
		b.String("campaign", r.campaign)
		b.String("status", r.status)
		b.String("kind", r.kind)
		b.Int("amount", r.amount)
		b.String("caller", r.caller)
		b.String("created_at", r.created)
		b.String("prospect", r.prospect)
		return b.Bytes()
	}
	encodeProspect := func(i int) []byte {
		b.Reset()
		b.String("city", cities[i%len(cities)])
		b.String("segment", segments[(i/7)%len(segments)])
		return b.Bytes()
	}
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	pkey := func(i int) []byte { return []byte(fmt.Sprintf("p%07d", i)) }

	type timing struct {
		load time.Duration
		size int64
		q    []time.Duration
	}
	results := map[string]timing{}
	reference := make([]map[string][]float64, len(dashQueries))
	nshards := runtime.GOMAXPROCS(0)
	for _, name := range engines {
		edir := filepath.Join(dir, "dash-"+name)
		os.RemoveAll(edir)
		os.MkdirAll(edir, 0o755)
		runtime.GC()
		tm := timing{q: make([]time.Duration, len(dashQueries))}
		outs := make([]map[string][]float64, len(dashQueries))
		par := strings.HasSuffix(name, "-par")
		switch strings.TrimSuffix(name, "-par") {
		case "ours":
			db, err := aptevadb.Open(edir, aptevadb.Options{})
			must(err)
			calls, err := db.Collection("calls")
			must(err)
			prospects, err := db.Collection("prospects")
			must(err)
			t0 := time.Now()
			for i := 0; i < nprospects; i++ {
				must(prospects.Upsert(pkey(i), encodeProspect(i)))
			}
			for i := range rows {
				must(calls.Upsert(key(i), encodeCall(rows[i])))
			}
			must(db.Checkpoint())
			tm.load = time.Since(t0)
			tm.size = dirSize(edir)
			shards := 1
			if par {
				shards = nshards
			}
			for qi, q := range dashQueries {
				var out []map[string]any
				tm.q[qi] = best(reps, func() {
					var err error
					out, err = calls.AggregateParallel(context.Background(), []byte(q.pipeline), shards)
					mustq(q, err)
				})
				outs[qi] = dashCanon(out)
			}
			db.Close()
		case "pebble":
			db, err := pebble.Open(edir, &pebble.Options{CacheSize: 512 << 20, MemTableSize: 64 << 20, Logger: quiet{}})
			must(err)
			t0 := time.Now()
			batch := db.NewBatch()
			for i := range rows {
				batch.Set(key(i), encodeCall(rows[i]), nil)
				if batch.Count() >= 1000 {
					must(batch.Commit(pebble.NoSync))
					batch.Close()
					batch = db.NewBatch()
				}
			}
			must(batch.Commit(pebble.NoSync))
			batch.Close()
			must(db.Flush())
			must(db.Compact(ctx(), key(0), key(n), false))
			tm.load = time.Since(t0)
			tm.size = dirSize(edir)
			src := func(lo, hi []byte) aptevadb.Source {
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
			var shards []aptevadb.Source
			for i := 0; i < nshards; i++ {
				var lo, hi []byte
				if i > 0 {
					lo = key(n * i / nshards)
				}
				if i < nshards-1 {
					hi = key(n * (i + 1) / nshards)
				}
				shards = append(shards, src(lo, hi))
			}
			for qi, q := range dashQueries {
				if q.join {
					continue
				}
				p, err := aptevadb.ParsePipeline([]byte(q.pipeline))
				must(err)
				var out []map[string]any
				tm.q[qi] = best(reps, func() {
					var err error
					if par {
						out, err = p.RunSharded(context.Background(), shards)
					} else {
						out, err = p.Run(context.Background(), src(nil, nil))
					}
					mustq(q, err)
				})
				outs[qi] = dashCanon(out)
			}
			db.Close()
		case "sqlite-c", "sqlite-go":
			opener := openSQLiteC
			if strings.HasPrefix(name, "sqlite-go") {
				opener = openSQLiteGo
			}
			k, err := opener(edir, false, n)
			must(err)
			s := k.(*sqliteKV)
			bucket := parisBucket()
			switch c := s.c.(type) {
			case cconn:
				must(c.c.CreateFunction("paris", true, 2, func(ctx csqlite.Context, args ...csqlite.Value) {
					if out, ok := bucket(args[0].Text(), args[1].Text()); ok {
						ctx.ResultText(out)
					} else {
						ctx.ResultNull()
					}
				}, nil, nil))
			case gconn:
				must(c.c.CreateFunction("paris", &gosqlite.FunctionImpl{NArgs: 2, Deterministic: true, Scalar: func(_ gosqlite.Context, args []gosqlite.Value) (gosqlite.Value, error) {
					if out, ok := bucket(args[0].Text(), args[1].Text()); ok {
						return gosqlite.TextValue(out), nil
					}
					return gosqlite.Value{}, nil
				}}))
			}
			exec := func(q string) {
				st := s.c.Prep(q)
				_, err := st.Step()
				must(err)
				st.Reset()
			}
			exec("CREATE TABLE calls (id INTEGER PRIMARY KEY, campaign TEXT, status TEXT, kind TEXT, amount INTEGER, caller TEXT, created_at TEXT, prospect TEXT)")
			exec("CREATE TABLE prospects (id TEXT PRIMARY KEY, city TEXT, segment TEXT) WITHOUT ROWID")
			t0 := time.Now()
			exec("BEGIN")
			pins := s.c.Prep("INSERT INTO prospects (id, city, segment) VALUES (?, ?, ?)")
			for i := 0; i < nprospects; i++ {
				pins.BindText(1, string(pkey(i)))
				pins.BindText(2, cities[i%len(cities)])
				pins.BindText(3, segments[(i/7)%len(segments)])
				_, err := pins.Step()
				must(err)
				pins.Reset()
			}
			ins := s.c.Prep("INSERT INTO calls (id, campaign, status, kind, amount, caller, created_at, prospect) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
			for i, r := range rows {
				ins.BindInt64(1, int64(i))
				ins.BindText(2, r.campaign)
				ins.BindText(3, r.status)
				ins.BindText(4, r.kind)
				ins.BindInt64(5, r.amount)
				ins.BindText(6, r.caller)
				ins.BindText(7, r.created)
				ins.BindText(8, r.prospect)
				_, err := ins.Step()
				must(err)
				ins.Reset()
			}
			exec("COMMIT")
			tm.load = time.Since(t0)
			tm.size = dirSize(edir)
			for qi, q := range dashQueries {
				st := s.c.Prep(q.sql)
				var out map[string][]float64
				tm.q[qi] = best(reps, func() {
					out = map[string][]float64{}
					for {
						ok, err := st.Step()
						must(err)
						if !ok {
							break
						}
						var vals []float64
						for c := 1; c < st.ColumnCount(); c++ {
							vals = append(vals, st.ColumnFloat(c))
						}
						out[st.ColumnText(0)] = vals
					}
					st.Reset()
				})
				outs[qi] = out
			}
			s.c.Close()
		default:
			panic("unknown dashboard engine " + name)
		}
		results[name] = tm
		if *flagDir == "" {
			must(os.RemoveAll(edir))
		}
		for qi, out := range outs {
			if out == nil {
				continue
			}
			if reference[qi] == nil {
				reference[qi] = out
				continue
			}
			if msg := compareAgg(reference[qi], out); msg != "" {
				panic(fmt.Sprintf("MISMATCH %s on %q: %s", name, dashQueries[qi].name, msg))
			}
		}
		fmt.Fprintf(os.Stderr, "%-10s load %.2fs size %.1f MiB\n", name, tm.load.Seconds(), float64(tm.size)/(1<<20))
	}

	fmt.Printf("\ndashboard queries over %d calls and %d prospects (ms, best of %d; lower is better; ours = one core, -par = %d cores)\n\n", n, nprospects, reps, nshards)
	fmt.Printf("%-40s", "query")
	for _, e := range engines {
		fmt.Printf(" %11s", e)
	}
	fmt.Println()
	for qi, q := range dashQueries {
		fmt.Printf("%-40s", q.name)
		for _, e := range engines {
			if d := results[e].q[qi]; d > 0 {
				fmt.Printf(" %11.1f", d.Seconds()*1000)
			} else {
				fmt.Printf(" %11s", "-")
			}
		}
		fmt.Println()
	}
	fmt.Printf("%-40s", "load (s)")
	for _, e := range engines {
		fmt.Printf(" %11.2f", results[e].load.Seconds())
	}
	fmt.Println()
	fmt.Printf("%-40s", "on-disk size (MiB)")
	for _, e := range engines {
		fmt.Printf(" %11.1f", float64(results[e].size)/(1<<20))
	}
	fmt.Println()
}

// dashCanon flattens pipeline rows like the SQL side: id (composite parts
// joined by "|" in name order) -> numeric columns in accumulator name order.
func dashCanon(rows []map[string]any) map[string][]float64 {
	out := map[string][]float64{}
	for _, r := range rows {
		id := ""
		switch v := r["_id"].(type) {
		case nil:
		case map[string]any:
			names := make([]string, 0, len(v))
			for k := range v {
				names = append(names, k)
			}
			sort.Strings(names)
			var parts []string
			for _, k := range names {
				parts = append(parts, fmt.Sprint(v[k]))
			}
			id = strings.Join(parts, "|")
		default:
			id = fmt.Sprint(v)
		}
		acc := make(map[string]any, len(r))
		for k, v := range r {
			if k != "_id" {
				acc[k] = v
			}
		}
		out[id] = canon([]map[string]any{acc})[""]
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustq(q dashQuery, err error) {
	if err != nil {
		panic(q.name + ": " + err.Error())
	}
}
