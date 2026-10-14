// Package server exposes an embedded AptevaDB through the version-one HTTP API.
// It owns request/transaction limits; the caller owns the DB and HTTP listener.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	db "github.com/apteva/db"
	"github.com/apteva/db/internal/protocol"
)

type Options struct {
	Token                                                    string
	MaxRequestBytes                                          int64
	MaxQueryMemoryBytes                                      int64
	MaxConcurrentRequests, MaxTransactions, MaxAggregateRows int
	MaxWatchers                                              int
	WatchWriteTimeout                                        time.Duration
	RequestTimeout, TransactionTTL                           time.Duration
}
type Server struct {
	db          *db.DB
	opts        Options
	token       [32]byte
	watchSlots  chan struct{}
	watchCtx    context.Context
	stopWatches context.CancelFunc
	slots       chan struct{}
	mu          sync.Mutex
	sessions    map[string]*session
	closed      bool
	stop, done  chan struct{}
	wg          sync.WaitGroup
}
type session struct {
	mu       sync.Mutex
	tx       *db.Tx
	snapshot *db.Snapshot
	expires  time.Time
	closed   bool
}

func New(database *db.DB, opts Options) (*Server, error) {
	if database == nil || len(opts.Token) < 16 {
		return nil, errors.New("aptevadb: server requires a database and a token of at least 16 bytes")
	}
	if opts.MaxWatchers == 0 {
		opts.MaxWatchers = 128
	}
	if opts.WatchWriteTimeout == 0 {
		opts.WatchWriteTimeout = 30 * time.Second
	}
	if opts.MaxWatchers < 1 || opts.WatchWriteTimeout < time.Millisecond {
		return nil, protocol.ErrBadRequest
	}
	if opts.MaxQueryMemoryBytes == 0 {
		opts.MaxQueryMemoryBytes = db.DefaultQueryMemoryBytes
	}
	if opts.MaxRequestBytes == 0 {
		opts.MaxRequestBytes = protocol.MaxBodyBytes
	}
	if opts.MaxConcurrentRequests == 0 {
		opts.MaxConcurrentRequests = 32
	}
	if opts.MaxTransactions == 0 {
		opts.MaxTransactions = 64
	}
	if opts.MaxAggregateRows == 0 {
		opts.MaxAggregateRows = 10000
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	if opts.TransactionTTL == 0 {
		opts.TransactionTTL = time.Minute
	}
	if opts.MaxQueryMemoryBytes < 1 || opts.MaxRequestBytes < 1 || opts.MaxConcurrentRequests < 1 || opts.MaxTransactions < 1 || opts.MaxAggregateRows < 1 || opts.RequestTimeout < 1 || opts.TransactionTTL < 1 {
		return nil, protocol.ErrBadRequest
	}
	s := &Server{db: database, opts: opts, token: sha256.Sum256([]byte(opts.Token)), slots: make(chan struct{}, opts.MaxConcurrentRequests), sessions: map[string]*session{}, stop: make(chan struct{}), done: make(chan struct{})}
	s.watchCtx, s.stopWatches = context.WithCancel(context.Background())
	s.watchSlots = make(chan struct{}, opts.MaxWatchers)
	s.opts.Token = ""
	go s.reap()
	return s, nil
}
func (s *Server) reap() {
	defer close(s.done)
	interval := min(time.Second, s.opts.TransactionTTL/2)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			var expired []*session
			s.mu.Lock()
			for id, x := range s.sessions {
				if !now.Before(x.expires) {
					delete(s.sessions, id)
					expired = append(expired, x)
				}
			}
			s.mu.Unlock()
			for _, x := range expired {
				x.close()
			}
		}
	}
}
func (x *session) close() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.closed {
		x.closed = true
		if x.snapshot != nil {
			x.snapshot.Close()
		} else {
			x.tx.Rollback()
		}
	}
}

// Close rejects new requests, cancels watches, drains active handlers and
// releases transaction snapshots. Then shut down the HTTP listener.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.stopWatches()
	close(s.stop)
	all := s.sessions
	s.sessions = map[string]*session{}
	s.mu.Unlock()
	<-s.done
	s.wg.Wait()
	for _, x := range all {
		x.close()
	}
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	supplied := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || subtle.ConstantTimeCompare(supplied[:], s.token[:]) != 1 {
		s.respond(w, http.StatusUnauthorized, protocol.Response{Error: protocol.FromError(protocol.ErrUnauthorized)})
		return
	}
	s.mu.Lock()
	closed := s.closed
	if !closed {
		s.wg.Add(1)
	}
	s.mu.Unlock()
	if closed {
		s.respond(w, http.StatusServiceUnavailable, protocol.Response{Error: protocol.FromError(db.ErrClosed)})
		return
	}
	defer s.wg.Done()
	requestCtx := r.Context()
	watchRequest := r.Method == http.MethodPost && r.URL.Path == "/v1/watch"
	slots := s.slots
	if watchRequest {
		slots = s.watchSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		s.respond(w, http.StatusServiceUnavailable, protocol.Response{Error: protocol.FromError(protocol.ErrBusy)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.opts.RequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	if r.URL.Path == "/v1/health" && r.Method == http.MethodGet {
		err := s.db.Health()
		status := http.StatusOK
		if err != nil {
			status = http.StatusServiceUnavailable
		}
		s.respond(w, status, protocol.Response{Version: db.Version, Format: db.FormatVersion, Error: protocol.FromError(err)})
		return
	}
	if r.Method != http.MethodPost || (r.URL.Path != "/v1/rpc" && r.URL.Path != "/v1/query" && r.URL.Path != "/v1/watch") {
		s.respond(w, http.StatusNotFound, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	bodyLimit := s.opts.MaxRequestBytes
	if watchRequest {
		bodyLimit = min(bodyLimit, 1<<20)
	}
	r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req protocol.Request
	if err := dec.Decode(&req); err != nil {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	var tail any
	if err := dec.Decode(&tail); err != io.EOF {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	if err := s.db.Health(); err != nil {
		s.respond(w, http.StatusServiceUnavailable, protocol.Response{Error: protocol.FromError(err)})
		return
	}
	if watchRequest {
		s.watch(w, r.WithContext(requestCtx), req)
		return
	}
	if req.Watch != nil || req.WatchSnapshot {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	if r.URL.Path == "/v1/query" {
		s.query(w, r, req)
		return
	}
	response, err := s.execute(ctx, req)
	response.Error = protocol.FromError(err)
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadRequest
		if errors.Is(err, db.ErrConflict) || errors.Is(err, db.ErrDuplicateKey) || errors.Is(err, db.ErrKeyExists) {
			status = http.StatusConflict
		}
		if errors.Is(err, protocol.ErrBusy) {
			status = http.StatusServiceUnavailable
		}
	}
	s.respond(w, status, response)
}
func (s *Server) respond(w http.ResponseWriter, status int, response protocol.Response) {
	if len(response.Value) > protocol.MaxBodyBytes {
		status = http.StatusRequestEntityTooLarge
		response = protocol.Response{Error: protocol.FromError(db.ErrLimit)}
	}
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > protocol.MaxBodyBytes {
		status = http.StatusRequestEntityTooLarge
		encoded, _ = json.Marshal(protocol.Response{Error: protocol.FromError(db.ErrLimit)})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
func (s *Server) execute(ctx context.Context, r protocol.Request) (protocol.Response, error) {
	var out protocol.Response
	if r.Transaction != "" {
		return s.transaction(ctx, r)
	}
	switch r.Op {
	case "begin":
		tx, err := s.db.Begin(context.Background())
		if err != nil {
			return out, err
		}
		var random [24]byte
		if _, err = rand.Read(random[:]); err != nil {
			tx.Rollback()
			return out, err
		}
		id := hex.EncodeToString(random[:])
		s.mu.Lock()
		if s.closed || len(s.sessions) >= s.opts.MaxTransactions {
			s.mu.Unlock()
			tx.Rollback()
			return out, protocol.ErrBusy
		}
		s.sessions[id] = &session{tx: tx, expires: time.Now().Add(s.opts.TransactionTTL)}
		s.mu.Unlock()
		out.Transaction = id
		return out, nil
	case "stats":
		stats := s.db.Stats()
		out.Stats = &stats
		return out, nil
	case "collections":
		out.Collections = s.db.Collections()
		return out, nil
	case "collection":
		_, err := s.db.Collection(r.Collection)
		return out, err
	case "drop_collection":
		return out, s.db.DropCollection(ctx, r.Collection)
	case "computed_collection":
		_, err := s.db.CreateComputedCollection(ctx, r.Collection, r.Source, r.Pipeline)
		return out, err
	case "batch":
		if len(r.Operations) > 10000 {
			return out, db.ErrLimit
		}
		b := s.db.NewBatch()
		for _, op := range r.Operations {
			c, err := s.db.OpenCollection(op.Collection)
			if err != nil {
				return out, err
			}
			if err = queue(b.With(c), op); err != nil {
				return out, err
			}
		}
		return out, b.CommitContext(ctx)
	}
	if r.Transaction != "" {
		return s.transaction(ctx, r)
	}
	c, err := s.db.OpenCollection(r.Collection)
	if err != nil {
		return out, err
	}
	switch r.Op {
	case "get":
		out.Value, out.Found, err = c.Find(r.Key, nil)
	case "insert", "upsert", "replace", "delete":
		b := c.NewBatch()
		err = queue(b, protocol.Operation{Method: r.Op, Key: r.Key, Value: r.Value})
		if err == nil {
			err = b.CommitContext(ctx)
		}
	case "indexes":
		out.Indexes = c.ListIndexes()
	case "create_index":
		options := db.IndexOptions{Unique: r.Unique, NullsDistinct: r.NullsDistinct, Sparse: r.Sparse}
		if len(r.Fields) > 0 {
			err = c.CreateCompoundIndex(ctx, r.Name, r.Fields, options)
		} else {
			err = c.CreateIndex(ctx, r.Name, r.Field, options)
		}
	case "drop_index":
		err = c.DropIndex(ctx, r.Name)
	case "computed_fields":
		err = c.DefineComputedFields(ctx, r.Definitions)
	case "explain":
		var plan db.QueryPlan
		plan, err = c.ExplainQuery(ctx, r.Filter, s.queryOptions(r, nil))
		out.Plan = &plan
	case "aggregate":
		var p *db.Pipeline
		p, err = db.ParsePipeline(r.Pipeline)
		if err == nil {
			p.SetLimits(db.Limits{MaxRows: s.opts.MaxAggregateRows, MaxGroups: s.opts.MaxAggregateRows, MaxBytes: s.opts.MaxQueryMemoryBytes})
			out.Rows, err = c.Run(ctx, p)
		}
	default:
		err = protocol.ErrBadRequest
	}
	return out, err
}
func queue(b *db.Batch, op protocol.Operation) error {
	switch op.Method {
	case "insert":
		return b.Insert(op.Key, op.Value)
	case "upsert":
		return b.Upsert(op.Key, op.Value)
	case "replace":
		return b.Replace(op.Key, op.Value)
	case "delete":
		return b.Delete(op.Key)
	default:
		return protocol.ErrBadRequest
	}
}
func (s *Server) transaction(ctx context.Context, r protocol.Request) (protocol.Response, error) {
	var out protocol.Response
	s.mu.Lock()
	x := s.sessions[r.Transaction]
	s.mu.Unlock()
	if x == nil {
		return out, db.ErrClosed
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed || !time.Now().Before(x.expires) {
		if x.snapshot != nil {
			x.snapshot.Close()
		} else {
			x.tx.Rollback()
		}
		x.closed = true
		return out, db.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if r.Op == "commit" || r.Op == "rollback" {
		x.closed = true
		s.mu.Lock()
		delete(s.sessions, r.Transaction)
		s.mu.Unlock()
		if x.snapshot != nil {
			x.snapshot.Close()
			if r.Op == "rollback" {
				return out, nil
			}
			return out, db.ErrReadOnly
		}
		if r.Op == "rollback" {
			x.tx.Rollback()
			return out, nil
		}
		return out, x.tx.CommitContext(ctx)
	}
	if x.snapshot != nil {
		if r.Op != "get" {
			return out, db.ErrReadOnly
		}
		c, err := x.snapshot.Collection(r.Collection)
		if err != nil {
			return out, err
		}
		out.Value, out.Found, err = c.Find(r.Key, nil)
		return out, err
	}
	c, err := x.tx.Collection(r.Collection)
	if err != nil {
		return out, err
	}
	switch r.Op {
	case "get":
		out.Value, out.Found, err = c.Find(r.Key, nil)
	case "insert":
		err = c.Insert(r.Key, r.Value)
	case "upsert":
		err = c.Upsert(r.Key, r.Value)
	case "replace":
		err = c.Replace(r.Key, r.Value)
	case "delete":
		err = c.Delete(r.Key)
	default:
		err = protocol.ErrBadRequest
	}
	return out, err
}
func (s *Server) query(w http.ResponseWriter, r *http.Request, req protocol.Request) {
	var c *db.Collection
	var err error
	if req.Transaction != "" {
		s.mu.Lock()
		x := s.sessions[req.Transaction]
		s.mu.Unlock()
		if x == nil {
			s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(db.ErrClosed)})
			return
		}
		x.mu.Lock()
		defer x.mu.Unlock()
		if x.closed || x.snapshot == nil || !time.Now().Before(x.expires) {
			s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(db.ErrClosed)})
			return
		}
		c, err = x.snapshot.Collection(req.Collection)
	} else {
		c, err = s.db.OpenCollection(req.Collection)
	}
	if err != nil {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(err)})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	encoder := json.NewEncoder(w)
	var writeErr error
	stats := db.QueryStats{}
	err = c.Query(r.Context(), req.Filter, s.queryOptions(req, &stats), func(key, doc []byte) bool {
		if len(doc) > protocol.MaxBodyBytes/2 {
			writeErr = db.ErrLimit
			return false
		}
		writeErr = encoder.Encode(protocol.Response{Key: key, Value: doc, Found: true})
		if writeErr != nil {
			return false
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		return true
	})
	if writeErr != nil {
		err = writeErr
	}
	_ = encoder.Encode(protocol.Response{Done: true, Error: protocol.FromError(err), QueryStats: &stats})
}

// NewHTTPServer supplies transport timeouts. ServeTLS is required for remote
// addresses by cmd/aptevadb-server; embedders must enforce the same policy.
func (s *Server) NewHTTPServer(address string) *http.Server {
	return &http.Server{Addr: address, Handler: s, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: s.opts.RequestTimeout, WriteTimeout: s.opts.RequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
}
func (s *Server) String() string {
	return fmt.Sprintf("AptevaDB HTTP v1 (%d request slots)", cap(s.slots))
}

func (s *Server) queryOptions(r protocol.Request, stats *db.QueryStats) db.QueryOptions {
	memory := r.MaxMemoryBytes
	if memory == 0 || memory > s.opts.MaxQueryMemoryBytes {
		memory = s.opts.MaxQueryMemoryBytes
	}
	return db.QueryOptions{Skip: r.Skip, Limit: r.Limit, Sort: r.Sort, Hint: r.Hint, KeyDescending: r.KeyDescending, MaxMemoryBytes: memory, Stats: stats}
}
