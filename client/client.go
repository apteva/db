// Package client is the Go driver for an AptevaDB HTTP v1 server. Connections
// are pooled by net/http. Writes are never retried automatically: after a
// transport failure their outcome can be unknown. Tx and Batch are single-user.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	db "github.com/apteva/db"
	"github.com/apteva/db/internal/protocol"
)

var ErrUnauthorized = protocol.ErrUnauthorized
var ErrBusy = protocol.ErrBusy
var ErrProtocol = protocol.ErrProtocol
var ErrCommitUnknown = errors.New("aptevadb: write outcome unknown; do not retry blindly")

type Error = protocol.Error
type Options struct {
	Token      string
	HTTPClient *http.Client
}
type Client struct {
	baseURL, token string
	http           *http.Client
	owned          bool
	closed         atomic.Bool
}
type Collection struct {
	client *Client
	name   string
	tx     *Tx
}

func New(baseURL string, opts Options) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, protocol.ErrBadRequest
	}
	if u.Scheme == "http" && !protocol.LoopbackURL(u) {
		return nil, errors.New("aptevadb: remote connections require HTTPS")
	}
	if len(opts.Token) < 16 {
		return nil, protocol.ErrUnauthorized
	}
	h := opts.HTTPClient
	owned := h == nil
	if h == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 128
		transport.MaxIdleConnsPerHost = 64
		transport.MaxConnsPerHost = 64
		h = &http.Client{Transport: transport, Timeout: 35 * time.Second}
	}
	// Refuse redirects so a server cannot forward authorization or change a
	// write's method/location. Copy a provided client without altering it.
	copyClient := *h
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimSuffix(baseURL, "/"), token: opts.Token, http: &copyClient, owned: owned}, nil
}
func (c *Client) Close() error {
	c.closed.Store(true)
	if c.owned {
		c.http.CloseIdleConnections()
	}
	return nil
}
func (c *Client) Collection(name string) *Collection { return &Collection{client: c, name: name} }
func (c *Client) CreateCollection(ctx context.Context, name string) error {
	_, err := c.call(ctx, protocol.Request{Op: "collection", Collection: name})
	return err
}
func (c *Client) DropCollection(ctx context.Context, name string) error {
	_, err := c.call(ctx, protocol.Request{Op: "drop_collection", Collection: name})
	return err
}
func (c *Client) Collections(ctx context.Context) ([]string, error) {
	r, err := c.call(ctx, protocol.Request{Op: "collections"})
	return r.Collections, err
}
func (c *Client) Stats(ctx context.Context) (*db.Stats, error) {
	r, err := c.call(ctx, protocol.Request{Op: "stats"})
	return r.Stats, err
}
func (c *Client) CreateComputedCollection(ctx context.Context, name, source string, pipeline []byte) error {
	_, err := c.call(ctx, protocol.Request{Op: "computed_collection", Collection: name, Source: source, Pipeline: pipeline})
	return err
}
func (c *Collection) request(op string) (protocol.Request, error) {
	r := protocol.Request{Op: op, Collection: c.name}
	if c.tx != nil {
		if c.tx.closed {
			return r, db.ErrClosed
		}
		r.Transaction = c.tx.id
	}
	return r, nil
}
func (c *Collection) Find(ctx context.Context, key []byte) ([]byte, bool, error) {
	r, err := c.request("get")
	if err != nil {
		return nil, false, err
	}
	r.Key = key
	out, err := c.client.call(ctx, r)
	return out.Value, out.Found, err
}
func (c *Collection) FindDocument(ctx context.Context, key []byte, out any) (bool, error) {
	v, ok, err := c.Find(ctx, key)
	if err != nil || !ok {
		return ok, err
	}
	return true, db.Decode(v, out)
}
func (c *Collection) FindJSON(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, ok, err := c.Find(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	m, err := db.DecodeDocument(v)
	if err != nil {
		return nil, true, err
	}
	v, err = json.Marshal(m)
	return v, true, err
}
func (c *Collection) write(ctx context.Context, op string, key, value []byte) error {
	r, err := c.request(op)
	if err != nil {
		return err
	}
	r.Key, r.Value = key, value
	_, err = c.client.call(ctx, r)
	return err
}
func (c *Collection) Insert(ctx context.Context, key, value []byte) error {
	return c.write(ctx, "insert", key, value)
}
func (c *Collection) Upsert(ctx context.Context, key, value []byte) error {
	return c.write(ctx, "upsert", key, value)
}
func (c *Collection) Replace(ctx context.Context, key, value []byte) error {
	return c.write(ctx, "replace", key, value)
}
func (c *Collection) Delete(ctx context.Context, key []byte) error {
	return c.write(ctx, "delete", key, nil)
}
func (c *Collection) jsonWrite(ctx context.Context, op string, key, doc []byte) error {
	v, err := db.EncodeJSON(nil, doc)
	if err != nil {
		return err
	}
	return c.write(ctx, op, key, v)
}
func (c *Collection) InsertJSON(ctx context.Context, key, doc []byte) error {
	return c.jsonWrite(ctx, "insert", key, doc)
}
func (c *Collection) UpsertJSON(ctx context.Context, key, doc []byte) error {
	return c.jsonWrite(ctx, "upsert", key, doc)
}
func (c *Collection) ReplaceJSON(ctx context.Context, key, doc []byte) error {
	return c.jsonWrite(ctx, "replace", key, doc)
}
func (c *Collection) InsertDocument(ctx context.Context, key []byte, doc any) error {
	v, err := db.Encode(doc)
	if err != nil {
		return err
	}
	return c.Insert(ctx, key, v)
}
func (c *Collection) UpsertDocument(ctx context.Context, key []byte, doc any) error {
	v, err := db.Encode(doc)
	if err != nil {
		return err
	}
	return c.Upsert(ctx, key, v)
}
func (c *Collection) ReplaceDocument(ctx context.Context, key []byte, doc any) error {
	v, err := db.Encode(doc)
	if err != nil {
		return err
	}
	return c.Replace(ctx, key, v)
}
func (c *Collection) CreateIndex(ctx context.Context, name, field string, opts db.IndexOptions) error {
	r, err := c.request("create_index")
	if err != nil {
		return err
	}
	r.Name, r.Field, r.Unique = name, field, opts.Unique
	_, err = c.client.call(ctx, r)
	return err
}
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	r, err := c.request("drop_index")
	if err != nil {
		return err
	}
	r.Name = name
	_, err = c.client.call(ctx, r)
	return err
}
func (c *Collection) Indexes(ctx context.Context) ([]db.IndexInfo, error) {
	r, err := c.request("indexes")
	if err != nil {
		return nil, err
	}
	out, err := c.client.call(ctx, r)
	return out.Indexes, err
}
func (c *Collection) DefineComputedFields(ctx context.Context, definitions []byte) error {
	r, err := c.request("computed_fields")
	if err != nil {
		return err
	}
	r.Definitions = definitions
	_, err = c.client.call(ctx, r)
	return err
}
func (c *Collection) Aggregate(ctx context.Context, pipeline []byte) ([]map[string]any, error) {
	r, err := c.request("aggregate")
	if err != nil {
		return nil, err
	}
	r.Pipeline = pipeline
	out, err := c.client.call(ctx, r)
	return out.Rows, err
}

type notSentError struct{ cause error }

func (e *notSentError) Error() string { return e.cause.Error() }
func (e *notSentError) Unwrap() error { return e.cause }

func (c *Client) send(ctx context.Context, path string, req protocol.Request) (*http.Response, error) {
	if c.closed.Load() {
		return nil, &notSentError{db.ErrClosed}
	}
	estimated := len(req.Key) + len(req.Value)*4/3 + len(req.Pipeline) + len(req.Definitions) + len(req.Filter) + len(req.Collection)*6 + len(req.Name)*6 + len(req.Field)*6 + len(req.Source)*6 + len(req.Transaction) + 1024
	for _, op := range req.Operations {
		estimated += len(op.Key)*4/3 + len(op.Value)*4/3 + len(op.Collection)*6 + len(op.Method)*6 + 128
	}
	if estimated > protocol.MaxBodyBytes {
		return nil, &notSentError{db.ErrLimit}
	}

	if ctx == nil {
		ctx = context.Background()
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, &notSentError{err}
	}
	if len(b) > protocol.MaxBodyBytes {
		return nil, &notSentError{db.ErrLimit}
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, &notSentError{err}
	}
	r.Header.Set("Authorization", "Bearer "+c.token)
	r.Header.Set("Content-Type", "application/json")
	return c.http.Do(r)
}
func mutation(op string) bool {
	switch op {
	case "get", "collections", "indexes", "stats", "aggregate", "begin", "rollback":
		return false
	}
	return true
}
func (c *Client) call(ctx context.Context, req protocol.Request) (protocol.Response, error) {
	var out protocol.Response
	response, err := c.send(ctx, "/v1/rpc", req)
	if err != nil {
		var unsent *notSentError
		if errors.As(err, &unsent) {
			return out, unsent.cause
		}
		if mutation(req.Op) {
			return out, fmt.Errorf("%w: %w", ErrCommitUnknown, err)
		}
		return out, err
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, protocol.MaxBodyBytes+1))
	decoder.UseNumber()
	if err = decoder.Decode(&out); err != nil {
		if mutation(req.Op) {
			return out, fmt.Errorf("%w: %w", ErrCommitUnknown, err)
		}
		return out, fmt.Errorf("%w: %v", ErrProtocol, err)
	}
	if out.Error != nil {
		return out, out.Error
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return out, fmt.Errorf("%w: HTTP %d", ErrProtocol, response.StatusCode)
	}
	for _, row := range out.Rows {
		normalize(row)
	}
	return out, nil
}
func normalize(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, v := range x {
			if n, ok := v.(json.Number); ok {
				if i, err := n.Int64(); err == nil {
					x[k] = i
				} else if f, err := n.Float64(); err == nil {
					x[k] = f
				}
			} else {
				normalize(v)
			}
		}
	case []any:
		for i, v := range x {
			if n, ok := v.(json.Number); ok {
				if ii, err := n.Int64(); err == nil {
					x[i] = ii
				} else if f, err := n.Float64(); err == nil {
					x[i] = f
				}
			} else {
				normalize(v)
			}
		}
	}
}

// Cursor streams a filter query. Close it even when stopping before EOF.
type Cursor struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	row     protocol.Response
	err     error
	closed  bool
}

func (c *Collection) Query(ctx context.Context, filter []byte, opts db.QueryOptions) (*Cursor, error) {
	r, err := c.request("query")
	if err != nil {
		return nil, err
	}
	r.Filter, r.Skip, r.Limit = filter, opts.Skip, opts.Limit
	response, err := c.client.send(ctx, "/v1/query", r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		var out protocol.Response
		if err = json.NewDecoder(io.LimitReader(response.Body, protocol.MaxBodyBytes)).Decode(&out); err != nil {
			return nil, ErrProtocol
		}
		if out.Error != nil {
			return nil, out.Error
		}
		return nil, ErrProtocol
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64<<10), protocol.MaxBodyBytes)
	return &Cursor{body: response.Body, scanner: scanner}, nil
}
func (c *Cursor) Next() bool {
	if c.closed {
		return false
	}
	if !c.scanner.Scan() {
		c.err = c.scanner.Err()
		if c.err == nil {
			c.err = ErrProtocol
		}
		c.Close()
		return false
	}
	c.row = protocol.Response{}
	if err := json.Unmarshal(c.scanner.Bytes(), &c.row); err != nil {
		c.err = fmt.Errorf("%w: %v", ErrProtocol, err)
		c.Close()
		return false
	}
	if c.row.Error != nil {
		c.err = c.row.Error
		c.Close()
		return false
	}
	if c.row.Done {
		c.Close()
		return false
	}
	if !c.row.Found {
		c.err = ErrProtocol
		c.Close()
		return false
	}
	return true
}
func (c *Cursor) Key() []byte          { return c.row.Key }
func (c *Cursor) Value() []byte        { return c.row.Value }
func (c *Cursor) Decode(out any) error { return db.Decode(c.row.Value, out) }
func (c *Cursor) Err() error           { return c.err }
func (c *Cursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.body.Close()
}
func (c *Collection) FindOne(ctx context.Context, filter []byte, out any) (bool, error) {
	cur, err := c.Query(ctx, filter, db.QueryOptions{Limit: 1})
	if err != nil {
		return false, err
	}
	defer cur.Close()
	if !cur.Next() {
		return false, cur.Err()
	}
	return true, cur.Decode(out)
}

type Tx struct {
	client *Client
	id     string
	closed bool
}

func (c *Client) Begin(ctx context.Context) (*Tx, error) {
	r, err := c.call(ctx, protocol.Request{Op: "begin"})
	if err != nil {
		return nil, err
	}
	if r.Transaction == "" {
		return nil, ErrProtocol
	}
	return &Tx{client: c, id: r.Transaction}, nil
}
func (tx *Tx) Collection(name string) *Collection {
	return &Collection{client: tx.client, name: name, tx: tx}
}
func (tx *Tx) Commit(ctx context.Context) error {
	if tx.closed {
		return db.ErrClosed
	}
	tx.closed = true
	_, err := tx.client.call(ctx, protocol.Request{Op: "commit", Transaction: tx.id})
	return err
}
func (tx *Tx) Rollback(ctx context.Context) error {
	if tx.closed {
		return nil
	}
	tx.closed = true
	_, err := tx.client.call(ctx, protocol.Request{Op: "rollback", Transaction: tx.id})
	return err
}
func (c *Client) Update(ctx context.Context, fn func(*Tx) error) error {
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Batch struct {
	client     *Client
	operations []protocol.Operation
}

func (c *Client) NewBatch() *Batch { return &Batch{client: c} }
func (b *Batch) Add(collection, method string, key, value []byte) error {
	switch method {
	case "insert", "upsert", "replace", "delete":
	default:
		return protocol.ErrBadRequest
	}
	if len(b.operations) >= 10000 {
		return db.ErrLimit
	}
	b.operations = append(b.operations, protocol.Operation{Method: method, Collection: collection, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	return nil
}
func (b *Batch) Commit(ctx context.Context) error {
	_, err := b.client.call(ctx, protocol.Request{Op: "batch", Operations: b.operations})
	b.operations = nil
	return err
}
