package aptevadb

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSoak runs a mixed workload for the duration in the SOAK environment
// variable (for example SOAK=30m; skipped when unset): concurrent writers
// with disjoint key sets, readers checking every document they see,
// aggregations and joins compared with scans of the same snapshot, index
// builds and drops, compactions, backups, and process-style crashes with
// recovery between phases. Every writer keeps its own model, so the whole
// database is checked exactly at each phase boundary and at the end.
func TestSoak(t *testing.T) {
	spec := os.Getenv("SOAK")
	if spec == "" {
		t.Skip("set SOAK to a duration, e.g. SOAK=30m")
	}
	total, err := time.ParseDuration(spec)
	if err != nil {
		t.Fatal(err)
	}
	const (
		writers  = 3
		userKeys = 30_000
		orderKys = 120_000
	)
	dir := t.TempDir()
	opts := Options{CacheBytes: 64 << 20, SyncMode: SyncOS}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	phases := 4
	phaseDur := total / time.Duration(phases)
	var ops [6]atomic.Int64 // inserts, deletes, finds, scans, aggregations, maintenance
	// models: key -> version (0 = absent), one per writer, disjoint keys
	type model struct {
		users, orders map[string]uint32
	}
	models := make([]*model, writers)
	for i := range models {
		models[i] = &model{users: map[string]uint32{}, orders: map[string]uint32{}}
	}
	sum := func(key string, v uint32) uint32 {
		h := fnv.New32a()
		h.Write([]byte(key))
		var b [4]byte
		b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
		h.Write(b[:])
		return h.Sum32()
	}
	userDoc := func(key string, v uint32) []byte {
		return []byte(fmt.Sprintf(`{"k":%q,"v":%d,"g":%d,"country":"c%d","sum":%d}`, key, v, v%13, v%7, sum(key, v)))
	}
	orderDoc := func(key string, v uint32, user string) []byte {
		return []byte(fmt.Sprintf(`{"k":%q,"v":%d,"user":%q,"amount":%d,"sum":%d}`, key, v, user, v%1000, sum(key, v)))
	}
	checkDoc := func(where string, key, doc []byte) error {
		k := docField(doc, "k")
		v := docField(doc, "v")
		s := docField(doc, "sum")
		if k.k != kString || string(k.s) != string(key) || v.k != kInt || s.k != kInt || uint32(s.i) != sum(string(key), uint32(v.i)) {
			return fmt.Errorf("%s: document %s is inconsistent: %s", where, key, doc)
		}
		return nil
	}
	userKey := func(w, i int) string { return fmt.Sprintf("u%06d", w+writers*i) }
	orderKey := func(w, i int) string { return fmt.Sprintf("o%06d", w+writers*i) }

	// checkModel compares the whole database with the writers' models.
	checkModel := func(where string) {
		t.Helper()
		users, _ := db.Collection("users")
		orders, _ := db.Collection("orders")
		for w, m := range models {
			for key, v := range m.users {
				doc, ok, err := users.Find([]byte(key), nil)
				if err != nil {
					t.Fatal(err)
				}
				if (v == 0) == ok {
					t.Fatalf("%s: writer %d user %s: version %d, present=%v", where, w, key, v, ok)
				}
				if ok {
					if got := docField(doc, "v"); got.k != kInt || uint32(got.i) != v {
						t.Fatalf("%s: user %s: version %d, want %d", where, key, got.i, v)
					}
				}
			}
			for key, v := range m.orders {
				doc, ok, err := orders.Find([]byte(key), nil)
				if err != nil {
					t.Fatal(err)
				}
				if (v == 0) == ok {
					t.Fatalf("%s: writer %d order %s: version %d, present=%v", where, w, key, v, ok)
				}
				if ok {
					if got := docField(doc, "v"); got.k != kInt || uint32(got.i) != v {
						t.Fatalf("%s: order %s: version %d, want %d", where, key, got.i, v)
					}
				}
			}
		}
		if err := db.Verify(bg); err != nil {
			t.Fatalf("%s: verify: %v", where, err)
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatalf("%s: verify indexes: %v", where, err)
		}
	}

	// checkSnapshot compares aggregations with scans of the same snapshot.
	checkSnapshot := func(s *Snapshot) error {
		users, err := s.Collection("users")
		if err != nil {
			return err
		}
		orders, err := s.Collection("orders")
		if err != nil {
			return err
		}
		userCountry := map[string]string{}
		if err := users.Scan(nil, nil, func(k, doc []byte) bool {
			userCountry[string(k)] = string(docField(doc, "country").s)
			return true
		}); err != nil {
			return err
		}
		nOrders, amount := 0, int64(0)
		joined := 0
		perCountry := map[string]int64{}
		if err := orders.Scan(nil, nil, func(k, doc []byte) bool {
			nOrders++
			amount += docField(doc, "amount").i
			if c, ok := userCountry[string(docField(doc, "user").s)]; ok {
				joined++
				perCountry[c] += docField(doc, "amount").i
			}
			return true
		}); err != nil {
			return err
		}
		rows, err := orders.Aggregate(bg, []byte(`[{"$group":{"_id":null,"n":{"$sum":1},"amount":{"$sum":"$amount"}}}]`))
		if err != nil {
			return err
		}
		if nOrders == 0 {
			if len(rows) != 0 {
				return fmt.Errorf("aggregate over no orders returned %v", rows)
			}
		} else if rows[0]["n"] != int64(nOrders) || rows[0]["amount"] != amount {
			return fmt.Errorf("aggregate %v, scan says n=%d amount=%d", rows, nOrders, amount)
		}
		rows, err = orders.Aggregate(bg, []byte(`[{"$lookup":{"from":"users","localField":"user","foreignField":"k","as":"u"}},{"$unwind":"$u"},{"$group":{"_id":"$u.country","amount":{"$sum":"$amount"}}}]`))
		if err != nil {
			return err
		}
		if len(rows) != len(perCountry) {
			return fmt.Errorf("join: %d countries, scan says %d", len(rows), len(perCountry))
		}
		got := 0
		for _, r := range rows {
			c, _ := r["_id"].(string)
			if r["amount"] != perCountry[c] {
				return fmt.Errorf("join: country %s amount %v, scan says %d", c, r["amount"], perCountry[c])
			}
			got++
		}
		_ = joined
		return nil
	}

	deadline := time.Now().Add(total)
	for phase := 0; phase < phases; phase++ {
		phaseEnd := time.Now().Add(phaseDur)
		if phase == phases-1 {
			phaseEnd = deadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), phaseEnd)
		var wg sync.WaitGroup
		fail := make(chan error, 16)
		report := func(err error) {
			select {
			case fail <- err:
			default:
			}
		}
		users, _ := db.Collection("users")
		orders, _ := db.Collection("orders")
		// writers: each owns a disjoint key set and its model
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int, m *model) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(phase*100 + w)))
				for ctx.Err() == nil {
					if rng.Intn(4) == 0 {
						b := users.NewBatch()
						ob := b.With(orders)
						type change struct {
							user bool
							key  string
							v    uint32
						}
						var changes []change
						for j := 0; j < 1+rng.Intn(8); j++ {
							if rng.Intn(3) == 0 {
								key := userKey(w, rng.Intn(userKeys/writers))
								v := m.users[key] + 1
								if rng.Intn(6) == 0 && m.users[key] > 0 {
									b.Delete([]byte(key))
									changes = append(changes, change{true, key, 0})
								} else {
									b.InsertJSON([]byte(key), userDoc(key, v))
									changes = append(changes, change{true, key, v})
								}
							} else {
								key := orderKey(w, rng.Intn(orderKys/writers))
								v := m.orders[key] + 1
								if rng.Intn(6) == 0 && m.orders[key] > 0 {
									ob.Delete([]byte(key))
									changes = append(changes, change{false, key, 0})
								} else {
									ob.InsertJSON([]byte(key), orderDoc(key, v, userKey(w, rng.Intn(userKeys/writers))))
									changes = append(changes, change{false, key, v})
								}
							}
						}
						if err := b.Commit(); err != nil {
							report(fmt.Errorf("writer %d batch: %w", w, err))
							return
						}
						for _, c := range changes {
							if c.user {
								m.users[c.key] = c.v
							} else {
								m.orders[c.key] = c.v
							}
						}
						ops[0].Add(int64(len(changes)))
						continue
					}
					if rng.Intn(2) == 0 {
						key := userKey(w, rng.Intn(userKeys/writers))
						v := m.users[key] + 1
						if rng.Intn(6) == 0 && m.users[key] > 0 {
							if err := users.Delete([]byte(key)); err != nil {
								report(err)
								return
							}
							m.users[key] = 0
							ops[1].Add(1)
						} else {
							if err := users.InsertJSON([]byte(key), userDoc(key, v)); err != nil {
								report(err)
								return
							}
							m.users[key] = v
							ops[0].Add(1)
						}
					} else {
						key := orderKey(w, rng.Intn(orderKys/writers))
						v := m.orders[key] + 1
						if rng.Intn(6) == 0 && m.orders[key] > 0 {
							if err := orders.Delete([]byte(key)); err != nil {
								report(err)
								return
							}
							m.orders[key] = 0
							ops[1].Add(1)
						} else {
							if err := orders.InsertJSON([]byte(key), orderDoc(key, v, userKey(w, rng.Intn(userKeys/writers)))); err != nil {
								report(err)
								return
							}
							m.orders[key] = v
							ops[0].Add(1)
						}
					}
				}
			}(w, models[w])
		}
		// readers: finds, scans and cursors, every document self-checked
		for r := 0; r < 2; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(1000 + phase*10 + r)))
				for ctx.Err() == nil {
					switch rng.Intn(3) {
					case 0:
						key := userKey(rng.Intn(writers), rng.Intn(userKeys/writers))
						doc, ok, err := users.Find([]byte(key), nil)
						if err != nil {
							report(err)
							return
						}
						if ok {
							if err := checkDoc("find", []byte(key), doc); err != nil {
								report(err)
								return
							}
						}
						ops[2].Add(1)
					case 1:
						lo := []byte(orderKey(0, rng.Intn(orderKys/writers)))
						n := 0
						err := orders.Scan(lo, nil, func(k, doc []byte) bool {
							if err := checkDoc("scan", k, doc); err != nil {
								report(err)
								return false
							}
							n++
							return n < 500
						})
						if err != nil {
							report(err)
							return
						}
						ops[3].Add(1)
					default:
						cur := users.NewCursor()
						n := 0
						for ok := cur.Seek([]byte(userKey(0, rng.Intn(userKeys/writers)))); ok && n < 200; ok = cur.Next() {
							if err := checkDoc("cursor", cur.Key(), cur.Value()); err != nil {
								report(err)
								return
							}
							n++
						}
						if cur.Err() != nil {
							report(cur.Err())
							return
						}
						ops[3].Add(1)
					}
				}
			}(r)
		}
		// aggregations on snapshots, compared with scans of the same snapshot
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				s, err := db.Snapshot()
				if err != nil {
					report(err)
					return
				}
				err = checkSnapshot(s)
				s.Close()
				if err != nil {
					report(err)
					return
				}
				ops[4].Add(1)
				time.Sleep(2 * time.Second)
			}
		}()
		// maintenance: indexes come and go, checkpoints, compactions, backups
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(5000 + phase)))
			for i := 0; ctx.Err() == nil; i++ {
				time.Sleep(3 * time.Second)
				var err error
				switch i % 6 {
				case 0, 3:
					c, field := users, "g"
					if rng.Intn(2) == 0 {
						c, field = orders, "user"
					}
					name := "by_" + field
					if _, has := c.Indexes()[name]; has {
						err = c.DropIndex(ctx, name)
					} else {
						err = c.CreateIndex(ctx, name, field)
					}
				case 1, 4:
					err = db.Checkpoint()
				case 2:
					err = db.Compact(ctx)
					if errors.Is(err, ErrSnapshotsOpen) || errors.Is(err, ErrIndexBuilding) {
						err = nil
					}
				case 5:
					dst := filepath.Join(t.TempDir(), "backup")
					if err = db.Backup(ctx, dst); err == nil {
						b, oerr := Open(dst, Options{})
						if oerr == nil {
							err = b.Verify(bg)
							if err == nil {
								err = b.VerifyIndexes(bg)
							}
							b.Close()
						} else {
							err = oerr
						}
						os.RemoveAll(dst)
					}
				}
				if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					report(fmt.Errorf("maintenance step %d: %w", i, err))
					return
				}
				ops[5].Add(1)
			}
		}()
		wg.Wait()
		cancel()
		select {
		case err := <-fail:
			t.Fatalf("phase %d: %v", phase, err)
		default:
		}
		// the phase ends in a checkpoint and a crash without close; the
		// model is exact after recovery
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		st := db.Stats()
		db.crash()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatalf("phase %d: reopen: %v", phase, err)
		}
		checkModel(fmt.Sprintf("phase %d", phase))
		t.Logf("phase %d: inserts=%d deletes=%d finds=%d scans=%d aggregations=%d maintenance=%d; pages=%d free=%d indexes=%d",
			phase, ops[0].Load(), ops[1].Load(), ops[2].Load(), ops[3].Load(), ops[4].Load(), ops[5].Load(), st.HighWaterPages, st.FreePages, st.Indexes)
	}
	if err := db.Compact(bg); err != nil {
		t.Fatal(err)
	}
	checkModel("after final compaction")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	checkModel("after clean reopen")
	db.Close()
}
