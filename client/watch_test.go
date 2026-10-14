package client_test

import (
	"context"
	"errors"
	db "github.com/apteva/db"
	"github.com/apteva/db/client"
	"github.com/apteva/db/server"
	"net/http/httptest"
	"testing"
	"time"
)

func watchFixture(t *testing.T, opts server.Options) (*db.DB, *client.Client) {
	t.Helper()
	database, err := db.Open(t.TempDir(), db.Options{ChangeFeed: &db.ChangeFeedOptions{}, Sync: true, SyncMode: db.SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	opts.Token = token
	handler, err := server.New(database, opts)
	if err != nil {
		t.Fatal(err)
	}
	transport := httptest.NewUnstartedServer(handler)
	transport.Config = handler.NewHTTPServer("")
	transport.StartTLS()
	httpClient := transport.Client()
	httpClient.Timeout = 100 * time.Millisecond
	c, err := client.New(transport.URL, client.Options{Token: token, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); handler.Close(); transport.Close(); database.Close() })
	return database, c
}
func remoteChange(t *testing.T, c *client.ChangeCursor) db.Change {
	t.Helper()
	if !c.Next() {
		t.Fatal(c.Err())
	}
	return c.Change().Clone()
}
func TestRemoteChangeFeedSnapshotResumeAndFiltering(t *testing.T) {
	database, c := watchFixture(t, server.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.CreateCollection(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	orders := c.Collection("orders")
	if err := orders.InsertJSON(ctx, []byte("a"), []byte(`{"status":"paid","n":1}`)); err != nil {
		t.Fatal(err)
	}
	opts := db.WatchOptions{Collections: []string{"orders"}, Filter: []byte(`{"status":"paid"}`), IncludeValues: true, Fields: []string{"n"}, HeartbeatInterval: time.Millisecond * 20}
	snapshot, feed, err := c.SnapshotAndWatch(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	defer snapshot.Close()
	if err = orders.UpsertJSON(ctx, []byte("a"), []byte(`{"status":"paid","n":2}`)); err != nil {
		t.Fatal(err)
	}
	if err = orders.InsertJSON(ctx, []byte("b"), []byte(`{"status":"paid","n":3}`)); err != nil {
		t.Fatal(err)
	}
	var initial map[string]any
	ok, err := snapshot.Collection("orders").FindDocument(ctx, []byte("a"), &initial)
	if err != nil || !ok || initial["n"] != int64(1) {
		t.Fatal(initial, ok, err)
	}
	query, err := snapshot.Collection("orders").Query(ctx, []byte(`{}`), db.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for query.Next() {
		n++
	}
	if query.Err() != nil || n != 1 {
		t.Fatal(query.Err(), n)
	}
	query.Close()
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	var last string
	for _, want := range []int64{2, 3} {
		r := remoteChange(t, feed)
		for r.Operation == db.ChangeProgress {
			r = remoteChange(t, feed)
		}
		var doc map[string]any
		if err = r.Decode(&doc); err != nil {
			t.Fatal(err)
		}
		if len(doc) != 1 || doc["n"] != want || !r.EndOfCommit {
			t.Fatal(doc, r)
		}
		last = r.CommitToken
	}
	if err = feed.Close(); err != nil {
		t.Fatal(err)
	}
	opts.After = last
	replay, err := c.Watch(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	// The pooled client's 100ms overall timeout must not terminate the watch.
	time.Sleep(150 * time.Millisecond)
	if err = orders.InsertJSON(ctx, []byte("c"), []byte(`{"status":"paid","n":4}`)); err != nil {
		t.Fatal(err)
	}
	for {
		r := remoteChange(t, replay)
		if r.Operation == db.ChangeProgress {
			continue
		}
		if string(r.Key) != "c" {
			t.Fatal(r)
		}
		break
	}
	replay.Close()
	deadline := time.Now().Add(time.Second)
	for database.ChangeFeedStats().Watchers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := database.ChangeFeedStats(); s.Watchers != 0 || s.ReaderBytes != 0 || database.Stats().Snapshots != 0 {
		t.Fatal(s, database.Stats())
	}
}
func TestRemoteWatchCapacityDoesNotBlockCRUD(t *testing.T) {
	_, c := watchFixture(t, server.Options{MaxConcurrentRequests: 1, MaxWatchers: 2})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	one, err := c.Watch(ctx, db.WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	two, err := c.Watch(ctx, db.WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	if _, err = c.Watch(ctx, db.WatchOptions{}); !errors.Is(err, client.ErrBusy) {
		t.Fatal(err)
	}
	if err = c.Collection("").UpsertJSON(ctx, []byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	for _, feed := range []*client.ChangeCursor{one, two} {
		r := remoteChange(t, feed)
		if r.Operation != db.ChangePut {
			t.Fatal(r)
		}
	}
}
func TestRemoteWatchUsesSeparateConnections(t *testing.T) {
	database, c := watchFixture(t, server.Options{MaxWatchers: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	feeds := make([]*client.ChangeCursor, 70)
	for i := range feeds {
		var err error
		feeds[i], err = c.Watch(ctx, db.WatchOptions{})
		if err != nil {
			t.Fatal(i, err)
		}
	}
	defer func() {
		for _, f := range feeds {
			f.Close()
		}
	}()
	if err := c.Collection("").UpsertJSON(ctx, []byte("a"), []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	for _, f := range feeds {
		r := remoteChange(t, f)
		if r.Operation != db.ChangePut {
			t.Fatal(r)
		}
	}
	c.Close()
	deadline := time.Now().Add(time.Second)
	for database.ChangeFeedStats().Watchers != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := database.ChangeFeedStats(); s.Watchers != 0 || s.ReaderBytes != 0 {
		t.Fatal(s)
	}
}
