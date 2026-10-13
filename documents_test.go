package db

import (
	"context"
	"errors"
	"testing"
)

func TestTypedAndStreamingQueries(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := mustCollection(t, db, "users")
	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	for k, u := range map[string]user{"a": {"Ada", 30}, "b": {"Bob", 20}, "c": {"Cam", 40}} {
		if err = c.InsertDocument([]byte(k), u); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.CreateIndex(bg, "age", "age"); err != nil {
		t.Fatal(err)
	}
	var got user
	ok, err := c.FindOne(bg, []byte(`{"age":{"$gte":25}}`), &got)
	if err != nil || !ok || got.Name != "Ada" {
		t.Fatal(got, ok, err)
	}
	keys := ""
	err = c.Query(bg, []byte(`{"age":{"$gte":25}}`), QueryOptions{}, func(k, doc []byte) bool {
		keys += string(k)
		if err := Decode(doc, &got); err != nil {
			t.Error(err)
		}
		return true
	})
	if err != nil || keys != "ac" {
		t.Fatal(keys, err)
	}
	err = c.UpdateDocument(bg, []byte("a"), func(m map[string]any) error { m["age"] = int64(31); return nil })
	if err != nil {
		t.Fatal(err)
	}
	ok, err = c.FindDocument([]byte("a"), &got)
	if err != nil || !ok || got.Age != 31 {
		t.Fatal(got, ok, err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err = c.Query(ctx, nil, QueryOptions{}, func(_, _ []byte) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if db.Stats().Snapshots != 0 {
		t.Fatal("query leaked view")
	}
}
