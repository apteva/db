package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	aptevadb "github.com/apteva/db"
)

// All stores receive the same integer source values and ASCII email strings.
// Ours/Pebble use binary documents and reuse a source builder/read buffer;
// SQLite uses typed columns. Timings include computation and encoding, exclude
// setup/checkpoint/index build unless named, and retain EVERY repetition.
// Non-durable writes: WAL enabled, synchronous off, batch/transaction of 1000.
// "manual" is application materialization; Pebble has no generated-field API.
const computedDefinitions = `{"total":{"$multiply":["$qty","$price"]},"email_key":{"$lower":"$email"}}`

type computedMeasurement struct {
	Engine    string  `json:"engine"`
	Phase     string  `json:"phase"`
	Trial     int     `json:"trial"`
	Ops       int     `json:"ops"`
	NsPerOp   float64 `json:"ns_per_op"`
	DiskBytes int64   `json:"disk_bytes,omitempty"`
}

type computedStore struct {
	name    string
	db      *aptevadb.DB
	k       kv
	sql     *sqliteKV
	derived stmt
	mode    string
}

func computedCheck(err error) {
	if err != nil {
		panic(err)
	}
}

func openComputedStore(dir, name string) *computedStore {
	s := &computedStore{name: name}
	switch name {
	case "ours-native", "ours-manual", "ours-readtime":
		var err error
		s.db, err = aptevadb.Open(dir, aptevadb.Options{CacheBytes: *flagOursMB << 20})
		computedCheck(err)
		s.k = &ours{s.db}
		s.mode = strings.TrimPrefix(name, "ours-")
		if s.mode == "native" {
			computedCheck(s.db.DefineComputedFields(context.Background(), []byte(computedDefinitions)))
		}
	case "pebble-manual", "pebble-readtime":
		var err error
		s.k, err = openPebble(dir, false, 1000)
		computedCheck(err)
		s.mode = strings.TrimPrefix(name, "pebble-")
	case "sqlite-c", "sqlite-go":
		var err error
		if name == "sqlite-c" {
			s.k, err = openSQLiteC(dir, false, 1000)
		} else {
			s.k, err = openSQLiteGo(dir, false, 1000)
		}
		computedCheck(err)
		s.sql = s.k.(*sqliteKV)
		s.mode = "native"
		st := s.sql.c.Prep(`CREATE TABLE orders(
   k BLOB PRIMARY KEY,qty INTEGER NOT NULL,price INTEGER NOT NULL,email TEXT NOT NULL,
   total INTEGER GENERATED ALWAYS AS(qty*price) STORED,
   email_key TEXT GENERATED ALWAYS AS(lower(email)) STORED) WITHOUT ROWID`)
		_, err = st.Step()
		computedCheck(err)
		computedCheck(st.Reset())
		s.sql.ins = s.sql.c.Prep(`INSERT INTO orders(k,qty,price,email) VALUES(?,?,?,?)
    ON CONFLICT(k) DO UPDATE SET qty=excluded.qty,price=excluded.price,email=excluded.email`)
		s.derived = s.sql.c.Prep("SELECT total,email_key FROM orders WHERE k=?")
	default:
		panic("unknown computed engine " + name)
	}
	return s
}

// Direct scalar access to the documented binary trailer, shared by stored and
// read-time variants. Avoids giving read-time derivation an artificial penalty
// from allocating a decoded map. Inputs here are generated/validated fixtures.
func computedPayload(doc []byte, name string) []byte {
	n := int(doc[len(doc)-1])
	dir := doc[len(doc)-1-2*n:]
	for i := 0; i < n; i++ {
		off := int(binary.LittleEndian.Uint16(dir[2*i:]))
		size := int(doc[off])
		if string(doc[off+1:off+1+size]) == name {
			return doc[off+2+size:]
		}
	}
	panic("missing benchmark field " + name)
}
func computedInt(doc []byte, name string) int64 {
	return int64(binary.LittleEndian.Uint64(computedPayload(doc, name)))
}
func computedString(doc []byte, name string) []byte {
	p := computedPayload(doc, name)
	n := int(binary.LittleEndian.Uint16(p))
	return p[2 : 2+n]
}

func runComputedBench(dir string, n int, engines []string) {
	if n < 1 {
		panic("n must be positive")
	}
	var names []string
	for _, name := range engines {
		switch name {
		case "ours":
			names = append(names, "ours-native", "ours-manual", "ours-readtime")
		case "pebble":
			names = append(names, "pebble-manual", "pebble-readtime")
		default:
			names = append(names, name)
		}
	}
	emails := make([]string, 1024)
	normalized := make([][]byte, len(emails))
	for i := range emails {
		emails[i] = fmt.Sprintf("USER%04d@EXAMPLE.COM", i)
		normalized[i] = []byte(strings.ToLower(emails[i]))
	}
	keys := make([]byte, n*16)
	rng := rand.New(rand.NewSource(90217))
	rng.Read(keys)
	order := rand.New(rand.NewSource(2233)).Perm(n)
	qty := func(i, version int) int64 { return int64(1 + (i+version)%23) }
	price := func(i int) int64 { return int64(100 + i%10007) }
	emailID := func(i, version int) int { return (i + version) % len(emails) }
	var measurements []computedMeasurement
	record := func(name, phase string, trial, ops int, d time.Duration, disk int64) {
		r := computedMeasurement{name, phase, trial, ops, float64(d) / float64(ops), disk}
		measurements = append(measurements, r)
		fmt.Printf("%-18s %-20s trial=%d %.3f ns/op\n", name, phase, trial, r.NsPerOp)
		data, err := json.MarshalIndent(measurements, "", "  ")
		computedCheck(err)
		computedCheck(os.WriteFile(filepath.Join(dir, "computed-measurements.json"), data, 0644))
	}
	fmt.Printf("computed fields: n=%d GOMAXPROCS=%d batch=1000 sync=off; native/manual/readtime; retain all trials\n", n, runtime.GOMAXPROCS(0))
	for trial := 1; trial <= *flagReps; trial++ {
		// Reverse engine order between trials to reduce fixed-order bias.
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
			s := openComputedStore(edir, name)
			var builder aptevadb.DocBuilder
			for version, phase := range []string{"insert", "upsert"} {
				var batch *aptevadb.Batch
				if s.db != nil {
					batch = s.db.NewBatch()
				}
				start := time.Now()
				for i := 0; i < n; i++ {
					key := keys[i*16 : (i+1)*16]
					q, p := qty(i, version), price(i)
					e := emailID(i, version)
					if s.sql != nil {
						sql := s.sql
						if !sql.open {
							_, err := sql.begin.Step()
							computedCheck(err)
							computedCheck(sql.begin.Reset())
							sql.open = true
						}
						sql.ins.BindBytes(1, key)
						sql.ins.BindInt64(2, q)
						sql.ins.BindInt64(3, p)
						sql.ins.BindText(4, emails[e])
						_, err := sql.ins.Step()
						computedCheck(err)
						computedCheck(sql.ins.Reset())
						sql.inTxn++
						if sql.inTxn >= 1000 {
							computedCheck(sql.endTxn())
						}
					} else {
						builder.Reset()
						builder.Int("qty", q)
						builder.Int("price", p)
						builder.String("email", emails[e])
						if s.mode == "manual" {
							builder.Int("total", q*p)
							builder.String("email_key", strings.ToLower(emails[e]))
						}
						if batch != nil {
							computedCheck(batch.Upsert(key, builder.Bytes()))
							if batch.Len() == 1000 {
								computedCheck(batch.Commit())
							}
						} else {
							computedCheck(s.k.Upsert(key, builder.Bytes()))
						}
					}
				}
				if batch != nil {
					computedCheck(batch.Commit())
				}
				if s.sql != nil {
					computedCheck(s.sql.endTxn())
				}
				if s.k != nil {
					if p, ok := s.k.(*peb); ok && p.batch != nil {
						computedCheck(p.flushBatch())
					}
				}
				record(name, phase, trial, n, time.Since(start), 0)
				computedCheck(s.k.Settle())
			}
			// Warm reads and then four random passes, validating BOTH computed outputs
			// for every row (not merely successful storage calls).
			buf := make([]byte, 0, 160)
			read := func(i int) {
				key := keys[i*16 : (i+1)*16]
				want := qty(i, 1) * price(i)
				e := emailID(i, 1)
				if s.sql != nil {
					s.derived.BindBytes(1, key)
					ok, err := s.derived.Step()
					computedCheck(err)
					if !ok || s.derived.ColumnInt64(0) != want || s.derived.ColumnText(1) != string(normalized[e]) {
						panic("SQLite computed mismatch")
					}
					computedCheck(s.derived.Reset())
					return
				}
				var ok bool
				var err error
				buf, ok, err = s.k.Find(key, buf)
				computedCheck(err)
				if !ok {
					panic("computed missing row")
				}
				var total int64
				var email []byte
				if s.mode == "readtime" {
					total = computedInt(buf, "qty") * computedInt(buf, "price")
					email = bytes.ToLower(computedString(buf, "email"))
				} else {
					total = computedInt(buf, "total")
					email = computedString(buf, "email_key")
				}
				if total != want || !bytes.Equal(email, normalized[e]) {
					panic("computed output mismatch")
				}
			}
			for _, i := range order {
				read(i)
			}
			start := time.Now()
			for pass := 0; pass < 4; pass++ {
				for _, i := range order {
					read(i)
				}
			}
			record(name, "read-derived", trial, 4*n, time.Since(start), dirSize(edir))
			if s.sql == nil {
				for _, workers := range []int{10, 32} {
					readers := make([]reader, workers)
					for i := range readers {
						var err error
						readers[i], err = s.k.NewReader()
						computedCheck(err)
					}
					var wg sync.WaitGroup
					start = time.Now()
					for worker, r := range readers {
						wg.Add(1)
						go func(worker int, r reader) {
							defer wg.Done()
							dst := make([]byte, 0, 160)
							for pass := 0; pass < 4; pass++ {
								for j := worker; j < n; j += workers {
									i := order[j]
									var ok bool
									var err error
									dst, ok, err = r.Find(keys[i*16:(i+1)*16], dst)
									computedCheck(err)
									if !ok {
										panic("parallel computed missing row")
									}
									var total int64
									var email []byte
									if s.mode == "readtime" {
										total = computedInt(dst, "qty") * computedInt(dst, "price")
										email = bytes.ToLower(computedString(dst, "email"))
									} else {
										total = computedInt(dst, "total")
										email = computedString(dst, "email_key")
									}
									if total != qty(i, 1)*price(i) || !bytes.Equal(email, normalized[emailID(i, 1)]) {
										panic("parallel computed mismatch")
									}
								}
							}
						}(worker, r)
					}
					wg.Wait()
					record(name, fmt.Sprintf("read-derived-%d", workers), trial, 4*n, time.Since(start), 0)
					for _, r := range readers {
						computedCheck(r.Close())
					}
				}
			}
			// Native/materialized output can use an ordinary index. Read-time fields
			// have to be derived across the collection. No Pebble index claim.
			if s.db != nil || s.sql != nil {
				if s.mode != "readtime" {
					start = time.Now()
					if s.db != nil {
						computedCheck(s.db.CreateIndex(context.Background(), "by_email_key", "email_key"))
						computedCheck(s.db.VerifyIndexes(context.Background()))
					} else {
						st := s.sql.c.Prep("CREATE INDEX by_email_key ON orders(email_key)")
						_, err := st.Step()
						computedCheck(err)
						computedCheck(st.Reset())
					}
					phase := "index-build+verify"
					if s.sql != nil {
						phase = "index-build"
					}
					record(name, phase, trial, n, time.Since(start), 0)
				}
				queries := 20
				var query stmt
				if s.sql != nil {
					query = s.sql.c.Prep("SELECT count(*) FROM orders WHERE email_key=?")
				}
				start = time.Now()
				for q := 0; q < queries; q++ {
					target := q % len(emails)
					want := int64(0)
					first := (target - 1 + len(emails)) % len(emails)
					if first < n {
						want = int64(1 + (n-1-first)/len(emails))
					}
					var count int64
					if s.sql != nil {
						query.BindText(1, string(normalized[target]))
						ok, err := query.Step()
						computedCheck(err)
						if !ok {
							panic("no SQL count")
						}
						count = query.ColumnInt64(0)
						computedCheck(query.Reset())
					} else if s.mode == "readtime" {
						computedCheck(s.db.Scan(nil, nil, func(k, v []byte) bool {
							if bytes.Equal(bytes.ToLower(computedString(v, "email")), normalized[target]) {
								count++
							}
							return true
						}))
					} else {
						pipe := []byte(fmt.Sprintf(`[{"$match":{"email_key":%q}},{"$count":"n"}]`, normalized[target]))
						rows, err := s.db.Aggregate(context.Background(), pipe)
						computedCheck(err)
						if len(rows) > 0 {
							count = rows[0]["n"].(int64)
						}
					}
					if count != want {
						panic(fmt.Sprintf("computed filter mismatch %s target=%d got=%d want=%d", name, target, count, want))
					}
				}
				phase := "indexed-filter"
				if s.mode == "readtime" {
					phase = "derive-filter-scan"
				}
				record(name, phase, trial, queries, time.Since(start), 0)
			}
			computedCheck(s.k.Close())
			computedCheck(os.RemoveAll(edir))
		}
	}
}
