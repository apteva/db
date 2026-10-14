package main

import (
	"context"
	"fmt"
	db "github.com/apteva/db"
	"os"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	opts := db.DurableOptions()
	opts.SyncMode = db.SyncOS
	opts.ChangeFeed = &db.ChangeFeedOptions{}
	d, e := db.Open(os.Args[1], opts)
	must(e)
	c, e := d.Collection("orders")
	must(e)
	must(c.UpsertJSON([]byte("a"), []byte(`{"n":9007199254740993,"omit":"large"}`)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, w, e := d.SnapshotAndWatch(ctx, db.WatchOptions{Collections: []string{"orders"}, Filter: []byte(`{"n":{"$gte":9007199254740993}}`), Fields: []string{"n"}, IncludeValues: true})
	must(e)
	must(c.UpsertJSON([]byte("a"), []byte(`{"n":9007199254740994,"omit":"large"}`)))
	sc, e := s.Collection("orders")
	must(e)
	var initial map[string]any
	ok, e := sc.FindDocument([]byte("a"), &initial)
	must(e)
	if !ok || initial["n"] != int64(9007199254740993) {
		panic(initial)
	}
	if !w.Next() {
		panic(w.Err())
	}
	change := w.Change()
	var row map[string]any
	must(change.Decode(&row))
	if !change.EndOfCommit || len(row) != 1 || row["n"] != int64(9007199254740994) {
		panic(row)
	}
	token := change.CommitToken
	must(w.Close())
	s.Close()
	must(d.Compact(context.Background()))
	must(d.Close())
	d, e = db.Open(os.Args[1], db.Options{})
	must(e)
	defer d.Close()
	c, e = d.OpenCollection("orders")
	must(e)
	w, e = d.Watch(ctx, db.WatchOptions{Collections: []string{"orders"}, After: token, IncludeValues: true})
	must(e)
	defer w.Close()
	must(c.UpsertJSON([]byte("b"), []byte(`{"n":42}`)))
	if !w.Next() {
		panic(w.Err())
	}
	if string(w.Change().Key) != "b" {
		panic(w.Change())
	}
	fmt.Println("embedded snapshot, exact filter, projection, compaction, persisted settings and replay passed", db.Version, db.FormatVersion)
}
