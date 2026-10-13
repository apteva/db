package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	aptevadb "github.com/apteva/db"
	"github.com/cockroachdb/pebble/v2"
)

const collectionPipeline = `[{"$match":{"paid":true}},{"$group":{"_id":"$customer","total":{"$sum":"$amount"},"n":{"$count":{}}}}]`
const collectionGroups = 1000

type collectionOrder struct {
	customer, amount int64
	paid             bool
}
type collectionTotals map[int64][2]int64

func collectionFixture(i, version int) collectionOrder {
	return collectionOrder{int64((i*17 + version*7) % collectionGroups), int64((i*31+version*11)%10007) - 5000, (i+version)%4 != 0}
}
func collectionDoc(b *aptevadb.DocBuilder, o collectionOrder) []byte {
	b.Reset()
	b.Int("customer", o.customer)
	b.Int("amount", o.amount)
	b.Bool("paid", o.paid)
	return b.Bytes()
}
func collectionDecode(doc []byte) collectionOrder {
	return collectionOrder{computedInt(doc, "customer"), computedInt(doc, "amount"), computedPayload(doc, "paid")[0] != 0}
}
func collectionGroupDoc(b *aptevadb.DocBuilder, id int64, x [2]int64) []byte {
	b.Reset()
	b.Int("_id", id)
	b.Int("total", x[0])
	b.Int("n", x[1])
	return b.Bytes()
}
func collectionTotalDoc(doc []byte) [2]int64 {
	return [2]int64{computedInt(doc, "total"), computedInt(doc, "n")}
}
func collectionAdd(m collectionTotals, o collectionOrder, sign int64) {
	if !o.paid {
		return
	}
	x := m[o.customer]
	x[0] += sign * o.amount
	x[1] += sign
	if x[1] == 0 {
		delete(m, o.customer)
	} else {
		m[o.customer] = x
	}
}
func collectionVerify(got, want collectionTotals) {
	if len(got) != len(want) {
		panic(fmt.Sprintf("summary count got=%d want=%d", len(got), len(want)))
	}
	for id, x := range want {
		if got[id] != x {
			panic(fmt.Sprintf("summary %d got=%v want=%v", id, got[id], x))
		}
	}
}

type collectionStore struct {
	name                 string
	native               bool
	db                   *aptevadb.DB
	source, summary      *aptevadb.Collection
	peb                  *peb
	sql                  *sqliteKV
	ins, del, all, point stmt
	pb                   *pebble.Batch
	ab                   *aptevadb.Batch
	pending              collectionTotals
}

func collectionExec(c conn, q string) {
	s := c.Prep(q)
	_, err := s.Step()
	computedCheck(err)
	computedCheck(s.Reset())
}
func openCollectionStore(dir, name string) *collectionStore {
	s := &collectionStore{name: name, native: strings.HasSuffix(name, "-native") || name == "pebble-manual"}
	switch {
	case strings.HasPrefix(name, "ours-"):
		var err error
		s.db, err = aptevadb.Open(dir, aptevadb.Options{CacheBytes: *flagOursMB << 20})
		computedCheck(err)
		s.source, err = s.db.Collection("orders")
		computedCheck(err)
		if s.native {
			s.summary, err = s.db.CreateComputedCollection(context.Background(), "totals", "orders", []byte(collectionPipeline))
			computedCheck(err)
		}
		s.ab = s.source.NewBatch()
	case strings.HasPrefix(name, "pebble-"):
		k, err := openPebble(dir, false, 0)
		computedCheck(err)
		s.peb = k.(*peb)
		s.pb = s.peb.db.NewBatch()
		s.pending = collectionTotals{}
	case strings.HasPrefix(name, "sqlite-"):
		var k kv
		var err error
		if strings.HasPrefix(name, "sqlite-c-") {
			k, err = openSQLiteC(dir, false, 1000)
		} else {
			k, err = openSQLiteGo(dir, false, 1000)
		}
		computedCheck(err)
		s.sql = k.(*sqliteKV)
		collectionExec(s.sql.c, `CREATE TABLE orders(k BLOB PRIMARY KEY,customer INTEGER NOT NULL,amount INTEGER NOT NULL,paid INTEGER NOT NULL) WITHOUT ROWID`)
		if s.native {
			collectionExec(s.sql.c, `CREATE TABLE totals(customer INTEGER PRIMARY KEY,total INTEGER NOT NULL,n INTEGER NOT NULL) WITHOUT ROWID`)
			collectionExec(s.sql.c, `CREATE TRIGGER orders_ai AFTER INSERT ON orders WHEN NEW.paid=1 BEGIN INSERT INTO totals VALUES(NEW.customer,NEW.amount,1) ON CONFLICT(customer) DO UPDATE SET total=total+NEW.amount,n=n+1; END`)
			collectionExec(s.sql.c, `CREATE TRIGGER orders_ad AFTER DELETE ON orders WHEN OLD.paid=1 BEGIN UPDATE totals SET total=total-OLD.amount,n=n-1 WHERE customer=OLD.customer; DELETE FROM totals WHERE customer=OLD.customer AND n=0; END`)
			collectionExec(s.sql.c, `CREATE TRIGGER orders_au AFTER UPDATE ON orders BEGIN UPDATE totals SET total=total-OLD.amount,n=n-1 WHERE customer=OLD.customer AND OLD.paid=1; DELETE FROM totals WHERE customer=OLD.customer AND n=0; INSERT INTO totals SELECT NEW.customer,NEW.amount,1 WHERE NEW.paid=1 ON CONFLICT(customer) DO UPDATE SET total=total+NEW.amount,n=n+1; END`)
			s.all = s.sql.c.Prep(`SELECT customer,total,n FROM totals`)
			s.point = s.sql.c.Prep(`SELECT total,n FROM totals WHERE customer=?`)
		} else {
			s.all = s.sql.c.Prep(`SELECT customer,SUM(amount),COUNT(*) FROM orders WHERE paid=1 GROUP BY customer`)
			s.point = s.sql.c.Prep(`SELECT SUM(amount),COUNT(*) FROM orders WHERE paid=1 AND customer=? GROUP BY customer`)
		}
		s.ins = s.sql.c.Prep(`INSERT INTO orders VALUES(?,?,?,?) ON CONFLICT(k) DO UPDATE SET customer=excluded.customer,amount=excluded.amount,paid=excluded.paid`)
		s.del = s.sql.c.Prep(`DELETE FROM orders WHERE k=?`)
	default:
		panic(name)
	}
	return s
}
func (s *collectionStore) beginSQL() {
	if !s.sql.open {
		_, err := s.sql.begin.Step()
		computedCheck(err)
		computedCheck(s.sql.begin.Reset())
		s.sql.open = true
	}
}
func (s *collectionStore) pebGet(key []byte) ([]byte, bool) {
	v, closer, err := s.peb.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false
	}
	computedCheck(err)
	out := append([]byte(nil), v...)
	computedCheck(closer.Close())
	return out, true
}
func (s *collectionStore) changeGroup(o collectionOrder, sign int64) {
	if !o.paid {
		return
	}
	x, ok := s.pending[o.customer]
	if !ok {
		key, _ := aptevadb.ComputedGroupKey(o.customer)
		if doc, found := s.pebGet(append([]byte{'t'}, key...)); found {
			x = collectionTotalDoc(doc)
		}
	}
	x[0] += sign * o.amount
	x[1] += sign
	s.pending[o.customer] = x
}
func (s *collectionStore) write(key []byte, o collectionOrder, doc []byte, del bool) {
	if s.db != nil {
		if del {
			computedCheck(s.ab.Delete(key))
		} else {
			computedCheck(s.ab.Upsert(key, doc))
		}
		return
	}
	if s.peb != nil {
		pk := append([]byte{'o'}, key...)
		if s.native {
			if old, ok := s.pebGet(pk); ok {
				s.changeGroup(collectionDecode(old), -1)
			}
			if !del {
				s.changeGroup(o, 1)
			}
		}
		if del {
			computedCheck(s.pb.Delete(pk, nil))
		} else {
			computedCheck(s.pb.Set(pk, doc, nil))
		}
		return
	}
	s.beginSQL()
	st := s.ins
	if del {
		st = s.del
	}
	st.BindBytes(1, key)
	if !del {
		st.BindInt64(2, o.customer)
		st.BindInt64(3, o.amount)
		paid := int64(0)
		if o.paid {
			paid = 1
		}
		st.BindInt64(4, paid)
	}
	_, err := st.Step()
	computedCheck(err)
	computedCheck(st.Reset())
}
func (s *collectionStore) flush() {
	if s.db != nil {
		computedCheck(s.ab.Commit())
		return
	}
	if s.peb != nil {
		var b aptevadb.DocBuilder
		for id, x := range s.pending {
			key, _ := aptevadb.ComputedGroupKey(id)
			key = append([]byte{'t'}, key...)
			if x[1] == 0 {
				computedCheck(s.pb.Delete(key, nil))
			} else {
				computedCheck(s.pb.Set(key, collectionGroupDoc(&b, id, x), nil))
			}
		}
		computedCheck(s.pb.Commit(pebble.NoSync))
		computedCheck(s.pb.Close())
		s.pb = s.peb.db.NewBatch()
		clear(s.pending)
		return
	}
	computedCheck(s.sql.endTxn())
}
func (s *collectionStore) settle() {
	if s.db != nil {
		computedCheck(s.db.Checkpoint())
	} else if s.peb != nil {
		computedCheck(s.peb.Settle())
	} else {
		computedCheck(s.sql.Settle())
	}
}
func (s *collectionStore) close() {
	if s.db != nil {
		computedCheck(s.db.Close())
	} else if s.peb != nil {
		computedCheck(s.pb.Close())
		computedCheck(s.peb.Close())
	} else {
		computedCheck(s.sql.Close())
	}
}
func (s *collectionStore) queryAll() collectionTotals {
	out := collectionTotals{}
	if s.db != nil {
		if s.native {
			computedCheck(s.summary.Scan(nil, nil, func(k, v []byte) bool { out[computedInt(v, "_id")] = collectionTotalDoc(v); return true }))
		} else {
			rows, err := s.source.Aggregate(context.Background(), []byte(collectionPipeline))
			computedCheck(err)
			for _, r := range rows {
				out[r["_id"].(int64)] = [2]int64{r["total"].(int64), r["n"].(int64)}
			}
		}
		return out
	}
	if s.peb != nil {
		prefix := byte('o')
		if s.native {
			prefix = 't'
		}
		it, err := s.peb.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefix}, UpperBound: []byte{prefix + 1}})
		computedCheck(err)
		for it.First(); it.Valid(); it.Next() {
			if s.native {
				out[computedInt(it.Value(), "_id")] = collectionTotalDoc(it.Value())
			} else {
				collectionAdd(out, collectionDecode(it.Value()), 1)
			}
		}
		computedCheck(it.Error())
		computedCheck(it.Close())
		return out
	}
	for {
		row, err := s.all.Step()
		computedCheck(err)
		if !row {
			break
		}
		out[s.all.ColumnInt64(0)] = [2]int64{s.all.ColumnInt64(1), s.all.ColumnInt64(2)}
	}
	computedCheck(s.all.Reset())
	return out
}
func (s *collectionStore) pointRead(id int64, key []byte, buf []byte) ([2]int64, []byte) {
	if s.db != nil {
		if s.native {
			v, ok, err := s.summary.Find(key, buf)
			computedCheck(err)
			if !ok {
				return [2]int64{}, v
			}
			return collectionTotalDoc(v), v
		}
		p := fmt.Sprintf(`[{"$match":{"customer":%d,"paid":true}},{"$group":{"_id":null,"total":{"$sum":"$amount"},"n":{"$count":{}}}}]`, id)
		rows, err := s.source.Aggregate(context.Background(), []byte(p))
		computedCheck(err)
		if len(rows) == 0 {
			return [2]int64{}, buf
		}
		return [2]int64{rows[0]["total"].(int64), rows[0]["n"].(int64)}, buf
	}
	if s.peb != nil {
		if s.native {
			v, closer, err := s.peb.db.Get(append([]byte{'t'}, key...))
			if errors.Is(err, pebble.ErrNotFound) {
				return [2]int64{}, buf
			}
			computedCheck(err)
			out := collectionTotalDoc(v)
			computedCheck(closer.Close())
			return out, buf
		}
		var x [2]int64
		it, err := s.peb.db.NewIter(&pebble.IterOptions{LowerBound: []byte{'o'}, UpperBound: []byte{'p'}})
		computedCheck(err)
		for it.First(); it.Valid(); it.Next() {
			o := collectionDecode(it.Value())
			if o.paid && o.customer == id {
				x[0] += o.amount
				x[1]++
			}
		}
		computedCheck(it.Error())
		computedCheck(it.Close())
		return x, buf
	}
	s.point.BindInt64(1, id)
	row, err := s.point.Step()
	computedCheck(err)
	var x [2]int64
	if row {
		x = [2]int64{s.point.ColumnInt64(0), s.point.ColumnInt64(1)}
	}
	computedCheck(s.point.Reset())
	return x, buf
}

func runComputedCollectionsBench(dir string, n int, engines []string) {
	if n < collectionGroups*4 {
		panic("computed collections require n >= 4000")
	}
	var names []string
	for _, name := range engines {
		switch name {
		case "ours":
			names = append(names, "ours-native", "ours-query")
		case "pebble":
			names = append(names, "pebble-manual", "pebble-query")
		case "sqlite-c", "sqlite-go":
			names = append(names, name+"-native", name+"-query")
		default:
			names = append(names, name)
		}
	}
	keys := make([]byte, n*16)
	rand.New(rand.NewSource(55971)).Read(keys)
	gkeys := make([][]byte, collectionGroups)
	for id := range gkeys {
		var err error
		gkeys[id], err = aptevadb.ComputedGroupKey(int64(id))
		computedCheck(err)
	}
	var measurements []computedMeasurement
	record := func(name, phase string, trial, ops int, start time.Time, disk int64) {
		r := computedMeasurement{name, phase, trial, ops, float64(time.Since(start)) / float64(ops), disk}
		measurements = append(measurements, r)
		fmt.Printf("%-20s %-22s trial=%d %.3f ns/op\n", name, phase, trial, r.NsPerOp)
		data, err := json.MarshalIndent(measurements, "", "  ")
		computedCheck(err)
		computedCheck(os.WriteFile(filepath.Join(dir, "computed-collections-measurements.json"), data, 0644))
	}
	fmt.Printf("computed collections: n=%d groups=%d batch=1000 WAL on sync off GOMAXPROCS=%d; exact checks, all trials\n", n, collectionGroups, runtime.GOMAXPROCS(0))
	for trial := 1; trial <= *flagReps; trial++ {
		runNames := append([]string(nil), names...)
		if trial%2 == 0 {
			for lo, hi := 0, len(runNames)-1; lo < hi; lo, hi = lo+1, hi-1 {
				runNames[lo], runNames[hi] = runNames[hi], runNames[lo]
			}
		}
		for _, name := range runNames {
			runtime.GC()
			edir := filepath.Join(dir, fmt.Sprintf("%s-%d", name, trial))
			computedCheck(os.MkdirAll(edir, 0755))
			s := openCollectionStore(edir, name)
			var b aptevadb.DocBuilder
			want := collectionTotals{}
			for version, phase := range []string{"insert", "upsert"} {
				start := time.Now()
				for i := 0; i < n; i++ {
					o := collectionFixture(i, version)
					doc := collectionDoc(&b, o)
					s.write(keys[i*16:(i+1)*16], o, doc, false)
					if (i+1)%1000 == 0 {
						s.flush()
					}
				}
				s.flush()
				record(name, phase, trial, n, start, 0)
				clear(want)
				for i := 0; i < n; i++ {
					collectionAdd(want, collectionFixture(i, version), 1)
				}
				collectionVerify(s.queryAll(), want)
			}
			start := time.Now()
			deleted := 0
			for i := 0; i < n; i += 10 {
				s.write(keys[i*16:(i+1)*16], collectionOrder{}, nil, true)
				deleted++
				if deleted%1000 == 0 {
					s.flush()
				}
			}
			s.flush()
			record(name, "delete", trial, deleted, start, 0)
			for i := 0; i < n; i += 10 {
				collectionAdd(want, collectionFixture(i, 1), -1)
			}
			collectionVerify(s.queryAll(), want)
			s.settle()
			// Give on-demand point queries an ordinary source index where the DB has one.
			if s.db != nil && !s.native {
				start = time.Now()
				rebuilt, err := s.db.CreateComputedCollection(context.Background(), "backfilled", "orders", []byte(collectionPipeline))
				computedCheck(err)
				record(name, "backfill", trial, n-deleted, start, 0)
				got := collectionTotals{}
				computedCheck(rebuilt.Scan(nil, nil, func(k, v []byte) bool { got[computedInt(v, "_id")] = collectionTotalDoc(v); return true }))
				collectionVerify(got, want)
				computedCheck(s.db.DropCollection(context.Background(), "backfilled"))
				start = time.Now()
				projected, err := s.db.CreateComputedCollection(context.Background(), "projected", "orders", []byte(`[{"$match":{"paid":true}},{"$project":{"customer":1,"amount":1}}]`))
				computedCheck(err)
				record(name, "projection-backfill", trial, n-deleted, start, 0)
				var matched int64
				computedCheck(projected.Scan(nil, nil, func(k, v []byte) bool {
					matched++
					_ = computedInt(v, "customer")
					_ = computedInt(v, "amount")
					return true
				}))
				var expected int64
				for _, x := range want {
					expected += x[1]
				}
				if matched != expected {
					panic("projection count")
				}
				computedCheck(s.db.DropCollection(context.Background(), "projected"))
				s.settle()
			}
			if !s.native {
				if s.db != nil {
					computedCheck(s.source.CreateIndex(context.Background(), "customer", "customer"))
				} else if s.sql != nil {
					collectionExec(s.sql.c, `CREATE INDEX by_customer ON orders(customer,paid,amount) WHERE paid=1`)
				}
			}
			queries := 21
			if s.native {
				queries = 1000
			}
			start = time.Now()
			for i := 0; i < queries; i++ {
				collectionVerify(s.queryAll(), want)
			}
			record(name, "query-all", trial, queries, start, 0)
			pointOps := 1000
			if s.native {
				pointOps = 1000000
			} else if s.peb != nil {
				pointOps = 50
			}
			var buf []byte
			start = time.Now()
			for i := 0; i < pointOps; i++ {
				id := int64((i * 97) % collectionGroups)
				var x [2]int64
				x, buf = s.pointRead(id, gkeys[id], buf)
				if x != want[id] {
					panic(fmt.Sprintf("point %d %v != %v", id, x, want[id]))
				}
			}
			record(name, "point-query", trial, pointOps, start, 0)
			if s.native {
				for _, workers := range []int{10, 32} {
					var wg sync.WaitGroup
					startCh := make(chan struct{})
					var readFns []func(int64, []byte, []byte) ([2]int64, []byte)
					var closes []func()
					for g := 0; g < workers; g++ {
						if s.db != nil {
							r, err := s.summary.NewReader()
							computedCheck(err)
							readFns = append(readFns, func(id int64, k, buf []byte) ([2]int64, []byte) {
								v, ok, err := r.Find(k, buf)
								computedCheck(err)
								if !ok {
									return [2]int64{}, v
								}
								return collectionTotalDoc(v), v
							})
							closes = append(closes, func() { computedCheck(r.Close()) })
						} else if s.sql != nil {
							c, err := s.sql.reopen()
							computedCheck(err)
							collectionExec(c, fmt.Sprintf("PRAGMA cache_size=-%d", *flagCache*1024))
							st := c.Prep(`SELECT total,n FROM totals WHERE customer=?`)
							readFns = append(readFns, func(id int64, k, buf []byte) ([2]int64, []byte) {
								st.BindInt64(1, id)
								row, err := st.Step()
								computedCheck(err)
								var x [2]int64
								if row {
									x = [2]int64{st.ColumnInt64(0), st.ColumnInt64(1)}
								}
								computedCheck(st.Reset())
								return x, buf
							})
							closes = append(closes, func() { computedCheck(c.Close()) })
						} else {
							readFns = append(readFns, s.pointRead)
						}
					}
					start = time.Now()
					for g := 0; g < workers; g++ {
						wg.Add(1)
						go func(g int) {
							defer wg.Done()
							var buf []byte
							<-startCh
							for i := g; i < 1000000; i += workers {
								id := int64((i * 97) % collectionGroups)
								var x [2]int64
								x, buf = readFns[g](id, gkeys[id], buf)
								if x != want[id] {
									panic("parallel summary differs")
								}
							}
						}(g)
					}
					close(startCh)
					wg.Wait()
					record(name, fmt.Sprintf("point-par%d", workers), trial, 1000000, start, 0)
					for _, close := range closes {
						close()
					}
				}
			}
			if s.db != nil {
				computedCheck(s.db.VerifyIndexes(context.Background()))
			}
			record(name, "disk", trial, 1, time.Now(), dirSize(edir))
			s.close()
		}
	}
}
