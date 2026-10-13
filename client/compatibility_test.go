package client_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	db "github.com/apteva/db"
	"github.com/apteva/db/server"
)

func TestRemoteCompatibilityFeatures(t *testing.T) {
	_, remote, _ := fixture(t, server.Options{MaxQueryMemoryBytes: 4096})
	ctx := context.Background()
	if err := remote.CreateCollection(ctx, "rows"); err != nil {
		t.Fatal(err)
	}
	rows := remote.Collection("rows")
	fields := []db.IndexField{{Field: "tenant"}, {Field: "n", Desc: true}}
	if err := rows.CreateCompoundIndex(ctx, "tenant_n", fields, db.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := rows.CreateIndex(ctx, "email", "email", db.IndexOptions{Unique: true, NullsDistinct: true}); err != nil {
		t.Fatal(err)
	}
	for i, n := range []int64{9007199254740992, 9007199254740993} {
		if err := rows.InsertDocument(ctx, []byte{byte('a' + i)}, map[string]any{"tenant": "a", "n": n, "email": nil, "body": strings.Repeat("x", 100000)}); err != nil {
			t.Fatal(err)
		}
	}
	filter := []byte(`{"$and":[{"tenant":"a"},{"$or":[{"n":9007199254740993},{"body":{"$prefix":"does not match"}}]}]}`)
	stats := db.QueryStats{}
	opts := db.QueryOptions{Sort: []db.IndexField{{Field: "n", Desc: true}}, Stats: &stats, MaxMemoryBytes: 8192}
	plan, err := rows.ExplainQuery(ctx, filter, opts)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Access != "index-order" || plan.MaxMemoryBytes != 4096 {
		t.Fatal(plan)
	}
	cur, err := rows.Query(ctx, filter, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	var keys []string
	for cur.Next() {
		keys = append(keys, string(cur.Key()))
		var got struct {
			N    int64  `json:"n"`
			Body string `json:"body"`
		}
		if err := cur.Decode(&got); err != nil {
			t.Fatal(err)
		}
		if got.N != 9007199254740993 || len(got.Body) != 100000 {
			t.Fatal("remote precision or large document failure")
		}
	}
	if cur.Err() != nil {
		t.Fatal(cur.Err())
	}
	if !reflect.DeepEqual(keys, []string{"b"}) || stats.Returned != 1 || stats.PeakMemoryBytes != 0 {
		t.Fatal(keys, stats)
	}
	if err := rows.DropIndex(ctx, "tenant_n"); err != nil {
		t.Fatal(err)
	}
	cur, err = rows.Query(ctx, nil, db.QueryOptions{Sort: []db.IndexField{{Field: "n"}}, MaxMemoryBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	if cur.Next() || !errors.Is(cur.Err(), db.ErrLimit) {
		t.Fatal("remote memory budget ignored", cur.Err())
	}
}
