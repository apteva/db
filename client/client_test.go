package client_test

import (
	"context"
	"errors"
	db "github.com/apteva/db"
	"github.com/apteva/db/client"
	"github.com/apteva/db/server"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const token = "test-only-token-32-bytes-123456789"

func TestRemoteWideAggregationAndNonNullCount(t *testing.T) {
	_, c, _ := fixture(t, server.Options{})
	ctx := context.Background()
	if err := c.CreateCollection(ctx, "numbers"); err != nil {
		t.Fatal(err)
	}
	numbers := c.Collection("numbers")
	for i, doc := range []string{`{"v":9223372036854775807}`, `{"v":9223372036854775807}`, `{"v":null}`, `{}`} {
		if err := numbers.InsertJSON(ctx, []byte{byte('a' + i)}, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := numbers.Aggregate(ctx, []byte(`[{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"rows":{"$count":{}},"present":{"$countNonNull":"$v"}}}]`))
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if rows[0]["sum"] != float64(math.MaxInt64)*2 || rows[0]["avg"] != float64(math.MaxInt64) || rows[0]["rows"] != int64(4) || rows[0]["present"] != int64(2) {
		t.Fatal(rows)
	}
}

func fixture(t *testing.T, options server.Options) (*db.DB, *client.Client, *server.Server) {
	t.Helper()
	database, err := db.Open(t.TempDir(), db.Options{Sync: true, SyncMode: db.SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	options.Token = token
	handler, err := server.New(database, options)
	if err != nil {
		t.Fatal(err)
	}
	transport := httptest.NewTLSServer(handler)
	c, err := client.New(transport.URL, client.Options{Token: token, HTTPClient: transport.Client()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.Close(); handler.Close(); database.Close() })
	return database, c, handler
}
func TestRemoteCRUDTransactionsAndQueries(t *testing.T) {
	database, c, _ := fixture(t, server.Options{})
	ctx := context.Background()
	if err := c.CreateCollection(ctx, "users"); err != nil {
		t.Fatal(err)
	}
	users := c.Collection("users")
	if err := users.DefineComputedFields(ctx, []byte(`{"key":{"$lower":"$email"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex(ctx, "key", "key", db.IndexOptions{Unique: true}); err != nil {
		t.Fatal(err)
	}
	if err := users.InsertJSON(ctx, []byte("a"), []byte(`{"email":"A@X","age":30}`)); err != nil {
		t.Fatal(err)
	}
	if err := users.InsertJSON(ctx, []byte("a"), []byte(`{"email":"B@X"}`)); !errors.Is(err, db.ErrKeyExists) {
		t.Fatal(err)
	}
	if err := users.UpsertJSON(ctx, []byte("b"), []byte(`{"email":"a@x"}`)); !errors.Is(err, db.ErrDuplicateKey) {
		t.Fatal(err)
	}
	err := c.Update(ctx, func(tx *client.Tx) error {
		return tx.Collection("users").ReplaceJSON(ctx, []byte("a"), []byte(`{"email":"C@X","age":31}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	var u struct {
		Email, Key string
		Age        int
	}
	ok, err := users.FindDocument(ctx, []byte("a"), &u)
	if err != nil || !ok || u.Key != "c@x" || u.Age != 31 {
		t.Fatal(u, ok, err)
	}
	cur, err := users.Query(ctx, []byte(`{"age":{"$gte":30}}`), db.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	n := 0
	for cur.Next() {
		n++
		if err = cur.Decode(&u); err != nil {
			t.Fatal(err)
		}
	}
	if cur.Err() != nil || n != 1 {
		t.Fatal(n, cur.Err())
	}
	rows, err := users.Aggregate(ctx, []byte(`[{"$count":"n"}]`))
	if err != nil || rows[0]["n"] != int64(1) {
		t.Fatal(rows, err)
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx.Collection("users").InsertJSON(ctx, []byte("b"), []byte(`{"email":"B@X"}`))
	users.UpsertJSON(ctx, []byte("a"), []byte(`{"email":"D@X"}`))
	if err = tx.Commit(ctx); !errors.Is(err, db.ErrConflict) {
		t.Fatal(err)
	}
	if _, ok, err = users.Find(ctx, []byte("b")); err != nil || ok {
		t.Fatal(ok, err)
	}
	b := c.NewBatch()
	v, _ := db.EncodeJSON(nil, []byte(`{"email":"E@X"}`))
	b.Add("users", "insert", []byte("e"), v)
	b.Add("users", "insert", []byte("e"), v)
	if err = b.Commit(ctx); !errors.Is(err, db.ErrKeyExists) {
		t.Fatal(err)
	}
	if _, ok, err = users.Find(ctx, []byte("e")); err != nil || ok {
		t.Fatal(ok, err)
	}
	if database.Stats().Snapshots != 0 {
		t.Fatal("remote transactions leaked")
	}
}
func TestRemoteAuthenticationLimitsAndExpiry(t *testing.T) {
	database, c, handler := fixture(t, server.Options{MaxRequestBytes: 1024, MaxTransactions: 1, TransactionTTL: 50 * time.Millisecond})
	ctx := context.Background()
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Begin(ctx); !errors.Is(err, client.ErrBusy) {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if err = tx.Commit(ctx); !errors.Is(err, db.ErrClosed) {
		t.Fatal(err)
	}
	if database.Stats().Snapshots != 0 {
		t.Fatal("expired transaction leaked")
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"op":"collection","collection":"intruder"}`))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, request)
	if w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
	if _, err = database.OpenCollection("intruder"); !errors.Is(err, db.ErrNoSuchCollection) {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(strings.Repeat("x", 2048)))
	request.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, request)
	if w.Code != http.StatusBadRequest {
		t.Fatal(w.Code)
	}
	if _, err = client.New("http://example.com", client.Options{Token: token}); err == nil {
		t.Fatal("remote plaintext accepted")
	}
}
func TestRemoteDoesNotFollowRedirectOrRetryWrites(t *testing.T) {
	calls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Redirect(w, r, "/other", http.StatusTemporaryRedirect)
	}))
	defer target.Close()
	c, err := client.New(target.URL, client.Options{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Collection("").Insert(context.Background(), []byte("k"), nil); err == nil {
		t.Fatal("invalid response accepted")
	}
	if calls != 1 {
		t.Fatal("write retried or redirected", calls)
	}
}
func TestStreamingProtocolRequiresTerminalFrame(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"found":true,"key":"aw==","value":"dg=="}`+"\n")
	}))
	defer target.Close()
	c, _ := client.New(target.URL, client.Options{Token: token})
	defer c.Close()
	cur, err := c.Collection("").Query(context.Background(), nil, db.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !cur.Next() || cur.Next() || !errors.Is(cur.Err(), client.ErrProtocol) {
		t.Fatal(cur.Err())
	}
}

func TestRemoteCountDocuments(t *testing.T) {
	_, c, _ := fixture(t, server.Options{MaxQueryMemoryBytes: 1024})
	ctx := context.Background()
	if err := c.CreateCollection(ctx, "counts"); err != nil {
		t.Fatal(err)
	}
	rows := c.Collection("counts")
	if err := rows.CreateCompoundIndex(ctx, "ab", []db.IndexField{{Field: "a"}, {Field: "b"}}, db.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{`{"a":"x","b":1}`, `{"a":"x","b":2}`, `{"a":"y","b":1}`} {
		if err := rows.InsertJSON(ctx, []byte{byte('a' + i)}, []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		filter  string
		opts    db.CountOptions
		want    int64
		covered bool
	}{
		{`{"a":"x"}`, db.CountOptions{}, 2, true},
		{`{"a":"missing"}`, db.CountOptions{}, 0, true},
		{`{"a":"x"}`, db.CountOptions{Skip: 1, Limit: 1}, 1, true},
		{`{"b":1}`, db.CountOptions{}, 2, false},
	} {
		var stats db.QueryStats
		tt.opts.Stats = &stats
		n, err := rows.CountDocuments(ctx, []byte(tt.filter), tt.opts)
		if err != nil || n != tt.want || stats.Plan.IndexOnly != tt.covered || stats.Plan.MaxMemoryBytes != 1024 {
			t.Fatal(tt, n, err, stats)
		}
		if tt.covered && stats.DocumentFetches != 0 {
			t.Fatal(stats)
		}
	}
	if _, err := rows.CountDocuments(ctx, nil, db.CountOptions{Hint: "absent"}); !errors.Is(err, db.ErrNoSuchIndex) {
		t.Fatal(err)
	}
	if _, err := rows.CountDocuments(ctx, []byte(`{"a":"x"}`), db.CountOptions{MaxMemoryBytes: 1}); !errors.Is(err, db.ErrLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := rows.CountDocuments(ctx, nil, db.CountOptions{}); !errors.Is(err, context.Canceled) || errors.Is(err, client.ErrCommitUnknown) {
		t.Fatal(err)
	}
}
