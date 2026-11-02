package db

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

// dashCall is one generated call record, kept so the tests can compute the
// expected dashboard metrics without the engine.
type dashCall struct {
	campaign string // "" = missing
	status   string
	kind     string
	amount   int64
	caller   string // "" = null
	created  time.Time
	stamp    string // created_at as stored ("" = unparseable value)
}

func dashFixture(t *testing.T, n int) (*Collection, []dashCall) {
	t.Helper()
	d, err := Open(t.TempDir(), Options{CacheBytes: 8 << 20})
	mustCompat(t, err)
	t.Cleanup(func() { d.Close() })
	c, err := d.Collection("calls")
	mustCompat(t, err)
	rng := rand.New(rand.NewSource(11))
	statuses := []string{"answered", "missed", "voicemail"}
	zones := []*time.Location{time.UTC, time.FixedZone("", 2*3600), time.FixedZone("", -5*3600-1800)}
	base := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC).Unix()
	calls := make([]dashCall, n)
	for i := range calls {
		k := dashCall{status: statuses[rng.Intn(3)], kind: "none", amount: int64(rng.Intn(10000))}
		if i%97 != 0 {
			k.campaign = fmt.Sprintf("camp%02d", rng.Intn(40))
		}
		if rng.Intn(4) == 0 {
			k.kind = "sale"
		}
		if i%13 != 0 {
			k.caller = fmt.Sprintf("+3361%05d", rng.Intn(700))
		}
		k.created = time.Unix(base+rng.Int63n(240*86400), 0)
		k.stamp = k.created.In(zones[rng.Intn(3)]).Format(time.RFC3339)
		if i%211 == 0 {
			k.stamp = "not a date"
		}
		var b DocBuilder
		if k.campaign != "" {
			b.String("campaign", k.campaign)
		}
		b.String("status", k.status)
		b.String("kind", k.kind)
		b.Int("amount", k.amount)
		if k.caller != "" {
			b.String("caller", k.caller)
		} else {
			b.Null("caller")
		}
		b.String("created_at", k.stamp)
		b.Int("created_ms", k.created.UnixMilli())
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("call-%07d", i)), b.Bytes()))
		calls[i] = k
	}
	return c, calls
}

// spillRows streams a bounded run out, so only its working state counts
// against the budget (collected results would too).
func spillRows(c *Collection, pipeline string, budget int64, st *AggregateStats) ([]map[string]any, error) {
	rows := []map[string]any{}
	err := c.AggregateEach(bg, []byte(pipeline), AggregateOptions{MaxMemoryBytes: budget, AllowDiskUse: true, Stats: st}, func(r map[string]any) bool {
		rows = append(rows, r)
		return true
	})
	return rows, err
}

// dashModes runs one pipeline through every execution path and fails unless
// they all agree with want (rows compared order-insensitively).
func dashModes(t *testing.T, c *Collection, pipeline string, want []map[string]any) AggregateStats {
	t.Helper()
	p, err := ParsePipeline([]byte(pipeline))
	mustCompat(t, err)
	mid := []byte("call-0005000")
	// a $project in front forces the materialised-row group path
	materialised := strings.Replace(pipeline, "[", `[{"$project":{"campaign":1,"status":1,"kind":1,"amount":1,"caller":1,"created_at":1,"created_ms":1}},`, 1)
	mp, err := ParsePipeline([]byte(materialised))
	mustCompat(t, err)
	var st AggregateStats
	modes := map[string]func() ([]map[string]any, error){
		"run":      func() ([]map[string]any, error) { return c.Aggregate(bg, []byte(pipeline)) },
		"parallel": func() ([]map[string]any, error) { return c.AggregateParallel(bg, []byte(pipeline), 4) },
		"each": func() ([]map[string]any, error) {
			return c.AggregateWithOptions(bg, []byte(pipeline), AggregateOptions{})
		},
		"spill":  func() ([]map[string]any, error) { return spillRows(c, pipeline, 96<<10, &st) },
		"source": func() ([]map[string]any, error) { return p.Run(bg, c.Source(nil, nil)) },
		"sharded": func() ([]map[string]any, error) {
			return p.RunSharded(bg, []Source{c.Source(nil, mid), c.Source(mid, nil)})
		},
		"materialised-run": func() ([]map[string]any, error) { return mp.Run(bg, c.Source(nil, nil)) },
		"materialised-each": func() ([]map[string]any, error) {
			return c.AggregateWithOptions(bg, []byte(materialised), AggregateOptions{})
		},
	}
	canonicalRows(want)
	for name, run := range modes {
		got, err := run()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		canonicalRows(got)
		if !reflect.DeepEqual(want, got) {
			for i := 0; i < len(want) && i < len(got); i++ {
				if !reflect.DeepEqual(want[i], got[i]) {
					t.Fatalf("%s: %d rows vs %d; first difference:\n want %v\n  got %v", name, len(want), len(got), want[i], got[i])
				}
			}
			t.Fatalf("%s: %d rows, want %d", name, len(got), len(want))
		}
	}
	return st
}

func TestFilteredAccumulatorsAllPaths(t *testing.T) {
	c, calls := dashFixture(t, 10000)
	p := `[{"$group":{"_id":"$campaign",
		"calls":{"$count":{}},
		"answered":{"$count":{},"where":{"status":"answered"}},
		"answered_or_vm":{"$sum":1,"where":{"$or":[{"status":"answered"},{"status":"voicemail"}]}},
		"revenue":{"$sum":"$amount","where":{"kind":"sale"}},
		"avg_answered":{"$avg":"$amount","where":{"status":"answered"}},
		"max_sale":{"$max":"$amount","where":{"kind":"sale","amount":{"$gt":9990}}},
		"min_missed":{"$min":"$amount","where":{"status":"missed"}},
		"with_caller":{"$countNonNull":"$caller","where":{"status":"missed"}},
		"unique_answered":{"$countDistinct":"$caller","where":{"status":"answered"}},
		"unique":{"$countDistinct":"$caller"}}}]`
	type agg struct {
		calls, answered, anvm, revenue, ansSum, ansN, withCaller int64
		maxSale, minMissed                                       *int64
		uniqueAns, unique                                        map[string]bool
	}
	groups := map[string]*agg{}
	for _, k := range calls {
		g := groups[k.campaign]
		if g == nil {
			g = &agg{uniqueAns: map[string]bool{}, unique: map[string]bool{}}
			groups[k.campaign] = g
		}
		g.calls++
		if k.caller != "" {
			g.unique[k.caller] = true
		}
		if k.status == "answered" {
			g.answered++
			g.ansSum += k.amount
			g.ansN++
			if k.caller != "" {
				g.uniqueAns[k.caller] = true
			}
		}
		if k.status == "answered" || k.status == "voicemail" {
			g.anvm++
		}
		if k.kind == "sale" {
			g.revenue += k.amount
			if k.amount > 9990 && (g.maxSale == nil || k.amount > *g.maxSale) {
				v := k.amount
				g.maxSale = &v
			}
		}
		if k.status == "missed" {
			if g.minMissed == nil || k.amount < *g.minMissed {
				v := k.amount
				g.minMissed = &v
			}
			if k.caller != "" {
				g.withCaller++
			}
		}
	}
	var want []map[string]any
	for id, g := range groups {
		row := map[string]any{"_id": any(id), "calls": g.calls, "answered": g.answered, "answered_or_vm": g.anvm,
			"revenue": g.revenue, "with_caller": g.withCaller, "unique_answered": int64(len(g.uniqueAns)), "unique": int64(len(g.unique)),
			"avg_answered": nil, "max_sale": nil, "min_missed": nil}
		if id == "" {
			row["_id"] = nil
		}
		if g.ansN > 0 {
			row["avg_answered"] = float64(g.ansSum) / float64(g.ansN)
		}
		if g.maxSale != nil {
			row["max_sale"] = *g.maxSale
		}
		if g.minMissed != nil {
			row["min_missed"] = *g.minMissed
		}
		want = append(want, row)
	}
	if st := dashModes(t, c, p, want); st.SpillPartitions < 2 {
		t.Fatalf("spill mode did not spill: %+v", st)
	}
}

// A group whose rows all miss an accumulator's filter still exists; the
// accumulator reports its empty value.
func TestFilteredAccumulatorEmptyAndCompactCount(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 3000; i++ {
		var b DocBuilder
		b.Int("g", int64(i%600))
		b.Int("v", int64(i))
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%06d", i)), b.Bytes()))
	}
	p := `[{"$group":{"_id":"$g","n":{"$count":{},"where":{"v":{"$lt":0}}},"s":{"$sum":"$v","where":{"v":{"$lt":0}}},
		"a":{"$avg":"$v","where":{"v":{"$lt":0}}},"f":{"$first":"$v","where":{"v":{"$gte":600}}},"l":{"$last":"$v","where":{"v":{"$lt":1200}}},
		"d":{"$countDistinct":"$v","where":{"v":{"$lt":0}}}}}]`
	for _, budget := range []int64{0, 24 << 10} {
		var st AggregateStats
		rows, err := spillRows(c, p, budget, &st)
		mustCompat(t, err)
		if len(rows) != 600 || (budget > 0 && st.SpillPartitions < 2) {
			t.Fatal(len(rows), st)
		}
		for _, r := range rows {
			g := r["_id"].(int64)
			if r["n"] != int64(0) || r["s"] != int64(0) || r["a"] != nil || r["d"] != int64(0) || r["f"] != g+600 || r["l"] != g+600 {
				t.Fatalf("budget=%d row=%v", budget, r)
			}
		}
	}
	// a lone count with a filter uses the compact counter state
	q := `[{"$group":{"_id":"$g","n":{"$count":{},"where":{"v":{"$gte":2400}}}}}]`
	for _, budget := range []int64{0, 16 << 10} {
		rows, err := spillRows(c, q, budget, nil)
		mustCompat(t, err)
		total := int64(0)
		for _, r := range rows {
			total += r["n"].(int64)
		}
		if len(rows) != 600 || total != 600 {
			t.Fatalf("budget=%d groups=%d total=%d", budget, len(rows), total)
		}
	}
}

func TestFilteredAccumulatorOnJoinedField(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	mustCompat(t, err)
	defer d.Close()
	camps, _ := d.Collection("campaigns")
	calls, _ := d.Collection("calls")
	for i := 0; i < 20; i++ {
		var b DocBuilder
		b.String("code", fmt.Sprintf("c%d", i))
		b.String("channel", []string{"ads", "seo"}[i%2])
		mustCompat(t, camps.Insert([]byte(fmt.Sprintf("c%d", i)), b.Bytes()))
	}
	want := map[any]int64{}
	for i := 0; i < 2000; i++ {
		var b DocBuilder
		b.String("campaign", fmt.Sprintf("c%d", i%20))
		b.Int("amount", int64(i))
		mustCompat(t, calls.Insert([]byte(fmt.Sprintf("%05d", i)), b.Bytes()))
		if (i%20)%2 == 0 {
			want[fmt.Sprintf("c%d", i%20)] += int64(i)
		}
	}
	for _, ff := range []string{"code", keyField} {
		p := `[{"$lookup":{"from":"campaigns","localField":"campaign","foreignField":"` + ff + `","as":"c"}},{"$unwind":"$c"},
			{"$group":{"_id":"$campaign","ads":{"$sum":"$amount","where":{"c.channel":"ads"}}}}]`
		rows, err := calls.Aggregate(bg, []byte(p))
		mustCompat(t, err)
		if len(rows) != 20 {
			t.Fatal(ff, rows)
		}
		for _, r := range rows {
			if r["ads"] != want[r["_id"]] {
				t.Fatalf("%s: %v want %d", ff, r, want[r["_id"]])
			}
		}
	}
}

// expectedTrunc is $dateTrunc computed with the time package.
func expectedTrunc(u time.Time, unit string, loc *time.Location, weekStart time.Weekday) string {
	l := u.In(loc)
	switch unit {
	case "hour":
		start := time.Date(l.Year(), l.Month(), l.Day(), l.Hour(), 0, 0, 0, loc)
		// a repeated local hour: time.Date picks one offset; use the instant's
		start = u.Truncate(time.Second).Add(-time.Duration(l.Minute())*time.Minute - time.Duration(l.Second())*time.Second)
		return start.UTC().Format("2006-01-02T15:04:05Z")
	case "day":
		return l.Format("2006-01-02")
	case "week":
		back := (int(l.Weekday()) - int(weekStart) + 7) % 7
		return time.Date(l.Year(), l.Month(), l.Day()-back, 12, 0, 0, 0, time.UTC).Format("2006-01-02")
	case "month":
		return time.Date(l.Year(), l.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	}
	return time.Date(l.Year(), 1, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

func TestDateTruncMatchesTimePackage(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, zone := range []string{"UTC", "Europe/Paris", "America/New_York", "Asia/Kolkata", "Australia/Lord_Howe", "Pacific/Chatham"} {
		loc, err := time.LoadLocation(zone)
		mustCompat(t, err)
		for _, unit := range []string{"hour", "day", "week", "month", "year"} {
			for _, ws := range []string{"sunday", "monday"} {
				tr, err := parseExpr(map[string]any{"$dateTrunc": map[string]any{"date": "$d", "unit": unit, "timezone": zone, "startOfWeek": ws}})
				mustCompat(t, err)
				var ev truncEval
				ev.t = tr
				u := time.Date(1969, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
				for i := 0; i < 20000; i++ {
					// mostly small steps (cache hits), sometimes long jumps
					if rng.Intn(50) == 0 {
						u += rng.Int63n(400 * 86400)
					} else {
						u += rng.Int63n(5400)
					}
					at := time.Unix(u, 0)
					want := expectedTrunc(at, unit, loc, map[string]time.Weekday{"sunday": time.Sunday, "monday": time.Monday}[ws])
					for _, in := range []value{{k: kString, s: []byte(at.In(loc).Format(time.RFC3339Nano))}, {k: kInt, i: u*1000 + 999}} {
						got := ev.eval(in)
						if got.k != kString || string(got.s) != want {
							t.Fatalf("%s %s %s: %s (%v) -> %q, want %q", zone, unit, ws, at.UTC(), in.k, got.s, want)
						}
					}
				}
			}
		}
	}
}

func TestDateTruncParisDaylightSaving(t *testing.T) {
	trunc := func(unit, in string) any {
		tr, err := parseExpr(map[string]any{"$dateTrunc": map[string]any{"date": "$d", "unit": unit, "timezone": "Europe/Paris", "startOfWeek": "monday"}})
		mustCompat(t, err)
		ev := truncEval{t: tr}
		return ev.eval(value{k: kString, s: []byte(in)}).any()
	}
	cases := []struct{ unit, in, want string }{
		// spring forward 2026-03-29: 02:00 local does not exist
		{"day", "2026-03-28T23:30:00Z", "2026-03-29"}, // 00:30 +01:00
		{"hour", "2026-03-29T00:59:59Z", "2026-03-29T00:00:00Z"},
		{"hour", "2026-03-29T01:00:00Z", "2026-03-29T01:00:00Z"}, // 03:00 +02:00
		{"day", "2026-03-29T21:59:59Z", "2026-03-29"},
		{"day", "2026-03-29T22:00:00Z", "2026-03-30"},
		// fall back 2026-10-25: 02:00–03:00 local happens twice
		{"hour", "2026-10-25T00:30:00Z", "2026-10-25T00:00:00Z"}, // 02:30 +02:00
		{"hour", "2026-10-25T01:30:00Z", "2026-10-25T01:00:00Z"}, // 02:30 +01:00
		{"day", "2026-10-24T22:00:00Z", "2026-10-25"},
		{"day", "2026-10-25T22:59:59Z", "2026-10-25"},
		{"day", "2026-10-25T23:00:00Z", "2026-10-26"},
		{"week", "2026-11-01T23:00:00Z", "2026-11-02"}, // Monday 00:00 local
		{"week", "2026-11-01T22:59:59Z", "2026-10-26"},
		{"month", "2026-10-31T23:30:00Z", "2026-11-01"},
		{"year", "2026-12-31T23:00:00Z", "2027-01-01"},
		{"day", "2026-11-02", "2026-11-02"}, // a bare date is a local date
		{"day", "2026-11-02T10:00:00+05:30", "2026-11-02"},
	}
	for _, c := range cases {
		if got := trunc(c.unit, c.in); got != c.want {
			t.Errorf("%s %s = %v, want %s", c.unit, c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "2026-02-30T00:00:00Z", "2026-11-02T24:00:00Z", "2026-11-02T10:00:00", "2026-11-02 10:00:00Z", "yesterday", "2026-11-02T10:00:00.Z"} {
		if got := trunc("day", bad); got != nil {
			t.Errorf("%q = %v, want null", bad, got)
		}
	}
}

func TestDateTruncPipelines(t *testing.T) {
	c, calls := dashFixture(t, 10000)
	paris, _ := time.LoadLocation("Europe/Paris")
	type key struct{ campaign, day string }
	counts := map[key]int64{}
	answered := map[key]int64{}
	weekly := map[string]int64{}
	for _, k := range calls {
		day := ""
		if k.stamp != "not a date" {
			day = expectedTrunc(k.created, "day", paris, time.Monday)
		}
		kk := key{k.campaign, day}
		counts[kk]++
		if k.status == "answered" {
			answered[kk]++
		}
		weekly[expectedTrunc(k.created, "week", paris, time.Monday)] += k.amount
	}
	var want []map[string]any
	for kk, n := range counts {
		id := map[string]any{"campaign": any(kk.campaign), "day": any(kk.day)}
		if kk.campaign == "" {
			id["campaign"] = nil
		}
		if kk.day == "" {
			id["day"] = nil // unparseable dates form a null bucket; rows are not dropped
		}
		want = append(want, map[string]any{"_id": id, "calls": n, "answered": answered[kk]})
	}
	if st := dashModes(t, c, `[{"$group":{"_id":{"campaign":"$campaign","day":{"$dateTrunc":{"date":"$created_at","unit":"day","timezone":"Europe/Paris"}}},
		"calls":{"$count":{}},"answered":{"$count":{},"where":{"status":"answered"}}}}]`, want); st.SpillPartitions < 2 {
		t.Fatalf("spill mode did not spill: %+v", st)
	}

	// epoch-millisecond input, single expression _id
	want = nil
	for w, s := range weekly {
		want = append(want, map[string]any{"_id": w, "revenue": s})
	}
	dashModes(t, c, `[{"$group":{"_id":{"$dateTrunc":{"date":"$created_ms","unit":"week","timezone":"Europe/Paris","startOfWeek":"monday"}},"revenue":{"$sum":"$amount"}}}]`, want)

	// $project computes the bucket; later stages see it as a field
	rows, err := c.Aggregate(bg, []byte(`[{"$match":{"status":"answered"}},{"$project":{"m":{"$dateTrunc":{"date":"$created_ms","unit":"month","timezone":"Europe/Paris"}},"amount":1}},
		{"$group":{"_id":"$m","n":{"$count":{}}}},{"$sort":{"_id":1}}]`))
	mustCompat(t, err)
	months := map[string]int64{}
	for _, k := range calls {
		if k.status == "answered" {
			months[expectedTrunc(k.created, "month", paris, 0)]++
		}
	}
	if len(rows) != len(months) {
		t.Fatal(rows, months)
	}
	for i, r := range rows {
		if r["n"] != months[r["_id"].(string)] || (i > 0 && r["_id"].(string) <= rows[i-1]["_id"].(string)) {
			t.Fatal(rows)
		}
	}
}

// An index on the date must not let $dateTrunc keys run in index order.
func TestDateTruncIgnoresIndexOrder(t *testing.T) {
	c, _ := dashFixture(t, 3000)
	mustCompat(t, c.CreateCompoundIndex(bg, "by_created", []IndexField{{Field: "created_at"}}))
	p := `[{"$sort":{"created_at":1}},{"$group":{"_id":{"$dateTrunc":{"date":"$created_at","unit":"day","timezone":"Europe/Paris"}},"n":{"$count":{}}}}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	var st AggregateStats
	got, err := c.AggregateWithOptions(bg, []byte(p), AggregateOptions{Stats: &st})
	mustCompat(t, err)
	canonicalRows(want)
	canonicalRows(got)
	if !reflect.DeepEqual(want, got) || strings.HasPrefix(st.Strategy, "ordered") {
		t.Fatalf("strategy %q, %d vs %d rows", st.Strategy, len(got), len(want))
	}
}

func TestDashboardParseErrors(t *testing.T) {
	for _, p := range []string{
		`[{"$group":{"_id":null,"n":{"$count":{},"where":3}}}]`,
		`[{"$group":{"_id":null,"n":{"where":{"a":1}}}}]`,
		`[{"$group":{"_id":null,"n":{"$count":{},"$sum":1}}}]`,
		`[{"$group":{"_id":null,"n":{"$count":{},"where":{"a":{"$regex":"x"}}}}}]`,
		`[{"$group":{"_id":null,"n":{"$countDistinct":1}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"minute"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"day","timezone":"Mars/Olympus"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"day","timezone":"Local"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"unit":"day"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"day","binSize":2}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"week","startOfWeek":"someday"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"day","epochUnit":"us"}},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"$toUpper":"$d"},"n":{"$count":{}}}}]`,
		`[{"$group":{"_id":{"a":"$a","$b":"$b"},"n":{"$count":{}}}}]`,
		`[{"$project":{"x":0,"d":{"$dateTrunc":{"date":"$d","unit":"day"}}}}]`,
		`[{"$project":{"_id":{"$dateTrunc":{"date":"$d","unit":"day"}}}}]`,
	} {
		if _, err := ParsePipeline([]byte(p)); !errors.Is(err, ErrPipeline) {
			t.Errorf("%s: %v", p, err)
		}
	}
}

func TestComputedCollectionsRejectDashboardFeatures(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	mustCompat(t, err)
	defer d.Close()
	_, err = d.Collection("calls")
	mustCompat(t, err)
	for i, p := range []string{
		`[{"$group":{"_id":"$c","n":{"$count":{},"where":{"s":"a"}}}}]`,
		`[{"$group":{"_id":"$c","n":{"$countDistinct":"$x"}}}]`,
		`[{"$group":{"_id":{"$dateTrunc":{"date":"$d","unit":"day"}},"n":{"$count":{}}}}]`,
		`[{"$project":{"d":{"$dateTrunc":{"date":"$d","unit":"day"}}}}]`,
	} {
		if _, err := d.CreateComputedCollection(bg, fmt.Sprintf("view%d", i), "calls", []byte(p)); !errors.Is(err, ErrComputedCollection) {
			t.Errorf("%s: %v", p, err)
		}
	}
}

func TestCountDistinctSpillAndLimits(t *testing.T) {
	c := aggregateFixture(t, 0)
	want := map[int64]map[string]bool{}
	for i := 0; i < 20000; i++ {
		var b DocBuilder
		g := int64(i % 50)
		b.Int("g", g)
		switch i % 4 {
		case 0:
			b.Int("v", int64(i%3000))
		case 1:
			b.String("v", fmt.Sprint(i%3000)) // "7" and 7 are different members
		case 2:
			b.Null("v")
		}
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%06d", i)), b.Bytes()))
		if want[g] == nil {
			want[g] = map[string]bool{}
		}
		if i%4 < 2 {
			want[g][fmt.Sprintf("%d:%d", i%4, i%3000)] = true
		}
	}
	p := `[{"$group":{"_id":"$g","d":{"$countDistinct":"$v"},"n":{"$count":{}}}}]`
	var st AggregateStats
	for _, budget := range []int64{0, 64 << 10} {
		rows, err := spillRows(c, p, budget, &st)
		mustCompat(t, err)
		par, err := c.AggregateParallel(bg, []byte(p), 4)
		mustCompat(t, err)
		canonicalRows(rows)
		canonicalRows(par)
		if !reflect.DeepEqual(rows, par) || len(rows) != 50 {
			t.Fatal("parallel differs")
		}
		for _, r := range rows {
			if r["d"] != int64(len(want[r["_id"].(int64)])) || r["n"] != int64(400) {
				t.Fatalf("%v want %d", r, len(want[r["_id"].(int64)]))
			}
		}
	}
	if st.SpillPartitions < 2 {
		t.Fatalf("did not spill: %+v", st)
	}
	// One group's set larger than the whole budget: in memory an error, never
	// an estimate; with disk, counted exactly in disjoint value slices.
	q := `[{"$group":{"_id":null,"d":{"$countDistinct":"$i"}}}]`
	big := aggregateFixture(t, 20000)
	if _, err := big.AggregateWithOptions(bg, []byte(q), AggregateOptions{MaxMemoryBytes: 128 << 10}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	var sliced AggregateStats
	rows, err := big.AggregateWithOptions(bg, []byte(q), AggregateOptions{MaxMemoryBytes: 128 << 10, AllowDiskUse: true, Stats: &sliced})
	mustCompat(t, err)
	if rows[0]["d"] != int64(20000) || sliced.DistinctSlices == 0 {
		t.Fatal(rows, sliced)
	}
	rows, err = big.AggregateWithOptions(bg, []byte(q), AggregateOptions{})
	mustCompat(t, err)
	if rows[0]["d"] != int64(20000) {
		t.Fatal(rows)
	}
	// the typed visitor reads it as an int
	mustCompat(t, big.AggregateGroups(bg, []byte(q), AggregateOptions{}, func(v GroupView) error {
		if n, ok := v.Int64("d"); !ok || n != 20000 {
			t.Fatal(n, ok)
		}
		return nil
	}))
}

func TestLookupByRecordKey(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	mustCompat(t, err)
	defer d.Close()
	prospects, _ := d.Collection("prospects")
	calls, _ := d.Collection("calls")
	const np = 20000
	for i := 0; i < np; i++ {
		var b DocBuilder
		b.String("pid", fmt.Sprintf("p%06d", i))
		b.String("city", []string{"Paris", "Lyon", "Lille"}[i%3])
		mustCompat(t, prospects.Insert([]byte(fmt.Sprintf("p%06d", i)), b.Bytes()))
	}
	mustCompat(t, prospects.CreateIndex(bg, "by_pid", "pid"))
	for i := 0; i < 30000; i++ {
		var b DocBuilder
		switch {
		case i%101 == 0:
			b.String("prospect", "nobody")
		case i%103 == 0:
			b.Int("prospect", int64(i)) // a number never names a record key
		default:
			b.String("prospect", fmt.Sprintf("p%06d", (i*7)%np))
		}
		b.Int("v", int64(i))
		b.String("status", []string{"answered", "missed"}[i%2])
		mustCompat(t, calls.Insert([]byte(fmt.Sprintf("c%06d", i)), b.Bytes()))
	}
	// buffered writes are read through the merging scan; after a checkpoint
	// the outer side is read in place, the point reads under its lock
	for _, state := range []string{"buffered", "checkpointed"} {
		if state == "checkpointed" {
			mustCompat(t, d.Checkpoint())
		}
		checkLookupByRecordKey(t, calls, state)
	}
	// a materialised $lookup attaches the record under its key
	rows, err := calls.Aggregate(bg, []byte(`[{"$match":{"v":{"$in":[1,101]}}},{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$sort":{"v":1}}]`))
	mustCompat(t, err)
	if len(rows) != 2 || len(rows[0]["p"].([]any)) != 1 || len(rows[1]["p"].([]any)) != 0 || rows[0]["p"].([]any)[0].(map[string]any)["pid"] != "p000007" {
		t.Fatal(rows)
	}

	// point reads need no table: a budget far below the foreign
	// collection's size still runs the join
	opts := AggregateOptions{MaxMemoryBytes: 1 << 20}
	got, err := calls.AggregateWithOptions(bg, []byte(`[{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$unwind":"$p"},{"$count":"n"}]`), opts)
	mustCompat(t, err)
	if n := got[0]["n"]; n != int64(30000-298-289) { // less "nobody" and numbers
		t.Fatalf("tight budget: %v", got)
	}

	// an array local value matches each named record once
	mustCompat(t, calls.UpsertJSON([]byte("multi"), []byte(`{"prospect":["p000001","p000002","p000001","nobody",5],"v":-1}`)))
	got, err = calls.Aggregate(bg, []byte(`[{"$match":{"v":-1}},{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$unwind":"$p"},{"$group":{"_id":"$p.pid","n":{"$count":{}}}}]`))
	mustCompat(t, err)
	canonicalRows(got)
	if len(got) != 2 || got[0]["_id"] != "p000001" || got[0]["n"] != int64(1) || got[1]["_id"] != "p000002" {
		t.Fatalf("array local value: %v", got)
	}
	mustCompat(t, calls.Delete([]byte("multi")))

	// writers keep going while joins read in place
	mustCompat(t, d.Checkpoint())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			var b DocBuilder
			b.String("city", "Nice")
			if err := prospects.Upsert([]byte(fmt.Sprintf("q%06d", i)), b.Bytes()); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if _, err := calls.AggregateParallel(bg, []byte(`[{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},{"$unwind":"$p"},{"$group":{"_id":"$p.city","n":{"$count":{}}}}]`), 4); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}

func checkLookupByRecordKey(t *testing.T, calls *Collection, state string) {
	for _, tail := range []string{
		// selective outer side
		`{"$match":{"v":{"$lt":300}}},`,
		// whole outer side
		``,
	} {
		for _, shape := range []string{
			`{"$unwind":"$p"},{"$group":{"_id":"$p.city","n":{"$count":{}},"s":{"$sum":"$v","where":{"status":"answered"}}}}`,
			`{"$unwind":{"path":"$p","preserveNullAndEmptyArrays":true}},{"$group":{"_id":"$p.city","n":{"$count":{}}}}`,
			`{"$match":{"v":{"$lt":2000}}},{"$project":{"v":1,"n":{"$size":"$p"}}},{"$count":"rows"}`,
			`{"$unwind":"$p"},{"$count":"n"}`,
		} {
			if strings.Contains(shape, "$size") {
				shape = `{"$match":{"v":{"$lt":2000}}},{"$project":{"v":1,"p.city":1}},{"$count":"rows"}`
			}
			byKey := `[` + tail + `{"$lookup":{"from":"prospects","localField":"prospect","foreignField":"$key","as":"p"}},` + shape + `]`
			byField := strings.Replace(byKey, `"foreignField":"$key"`, `"foreignField":"pid"`, 1)
			want, err := calls.Aggregate(bg, []byte(byField))
			mustCompat(t, err)
			canonicalRows(want)
			for name, run := range map[string]func() ([]map[string]any, error){
				"run":      func() ([]map[string]any, error) { return calls.Aggregate(bg, []byte(byKey)) },
				"parallel": func() ([]map[string]any, error) { return calls.AggregateParallel(bg, []byte(byKey), 4) },
				"each": func() ([]map[string]any, error) {
					return calls.AggregateWithOptions(bg, []byte(byKey), AggregateOptions{})
				},
			} {
				got, err := run()
				mustCompat(t, err)
				canonicalRows(got)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("%s %s %s:\n got %v\nwant %v", state, name, byKey, got, want)
				}
			}
		}
	}
}
