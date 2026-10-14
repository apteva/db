package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	db "github.com/apteva/db"
	"github.com/apteva/db/internal/protocol"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ChangeCursor reads committed changes. A disconnect never resumes silently;
// callers reconnect with the last token saved after processing a whole commit.
// Close is safe concurrently with Next and Client.Close cancels all watches.
type ChangeCursor struct {
	body      io.ReadCloser
	reader    *bufio.Reader
	cancel    context.CancelFunc
	stop      func() bool
	client    *Client
	closeOnce sync.Once
	stepMu    sync.Mutex
	closed    atomic.Bool
	token     atomic.Value
	row       db.Change
	pending   string
	err       error
}

func (c *Client) Watch(ctx context.Context, o db.WatchOptions) (*ChangeCursor, error) {
	_, cur, err := c.openWatch(ctx, o, false)
	return cur, err
}
func (c *Collection) Watch(ctx context.Context, o db.WatchOptions) (*ChangeCursor, error) {
	if c.tx != nil {
		return nil, db.ErrReadOnly
	}
	if len(o.Collections) > 0 {
		return nil, db.ErrOptions
	}
	o.Collections = []string{c.name}
	return c.client.Watch(ctx, o)
}

// SnapshotAndWatch returns an expiring read-only snapshot lease and a feed
// starting immediately after it. Close the snapshot after initial reads. The
// server's TransactionTTL also applies to this lease. Closing the live stream
// releases its snapshot automatically.
func (c *Client) SnapshotAndWatch(ctx context.Context, o db.WatchOptions) (*Snapshot, *ChangeCursor, error) {
	return c.openWatch(ctx, o, true)
}
func (c *Client) openWatch(ctx context.Context, o db.WatchOptions, snapshot bool) (*Snapshot, *ChangeCursor, error) {
	if c.closed.Load() {
		return nil, nil, db.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.watchCtx, cancel)
	select {
	case c.watchSlots <- struct{}{}:
	default:
		stop()
		cancel()
		return nil, nil, db.ErrWatchLimit
	}
	release := func() { <-c.watchSlots; stop(); cancel() }
	response, err := c.sendUsing(ctx, "/v1/watch", protocol.Request{Op: "watch", Watch: &o, WatchSnapshot: snapshot}, c.watchHTTP)
	if err != nil {
		release()
		return nil, nil, err
	}
	fail := func(err error) (*Snapshot, *ChangeCursor, error) {
		response.Body.Close()
		release()
		return nil, nil, err
	}
	if response.StatusCode != http.StatusOK {
		var out protocol.Response
		if err = json.NewDecoder(io.LimitReader(response.Body, protocol.MaxBodyBytes+1)).Decode(&out); err != nil {
			return fail(ErrProtocol)
		}
		if out.Error != nil {
			return fail(out.Error)
		}
		return fail(ErrProtocol)
	}
	reader := bufio.NewReaderSize(response.Body, 4096)
	frame, err := readChangeFrame(reader)
	if err != nil {
		return fail(err)
	}
	if !frame.WatchReady || frame.ChangeToken == "" || len(frame.ChangeToken) > 128 || frame.Error != nil || frame.Change != nil || frame.Done {
		return fail(ErrProtocol)
	}
	if snapshot != (frame.Transaction != "") {
		return fail(ErrProtocol)
	}
	cur := &ChangeCursor{body: response.Body, reader: reader, cancel: cancel, stop: stop, client: c}
	cur.token.Store(frame.ChangeToken)
	var s *Snapshot
	if snapshot {
		s = &Snapshot{tx: &Tx{client: c, id: frame.Transaction}}
	}
	return s, cur, nil
}
func readChangeFrame(reader *bufio.Reader) (protocol.Response, error) {
	var out protocol.Response
	var data []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(data)+len(part) > protocol.MaxBodyBytes {
			return out, ErrProtocol
		}
		if err == nil {
			if data == nil {
				data = part
			} else {
				data = append(data, part...)
			}
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return out, err
		}
		data = append(data, part...)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, ErrProtocol
	}
	return out, nil
}
func (c *ChangeCursor) closeTransport() {
	c.closeOnce.Do(func() { c.closed.Store(true); c.cancel(); c.stop(); c.body.Close(); <-c.client.watchSlots })
}
func (c *ChangeCursor) Close() error {
	c.closeTransport()
	c.stepMu.Lock()
	c.row = db.Change{}
	c.reader = nil
	c.stepMu.Unlock()
	return nil
}
func (c *ChangeCursor) Next() bool {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	if c.closed.Load() {
		return false
	}
	frame, err := readChangeFrame(c.reader)
	if err != nil {
		if !c.closed.Load() {
			if errors.Is(err, io.EOF) {
				err = ErrProtocol
			}
			c.err = err
		}
		c.closeTransport()
		return false
	}
	if frame.Error != nil {
		c.err = frame.Error
		c.closeTransport()
		return false
	}
	if frame.Done {
		if c.pending != "" {
			c.err = ErrProtocol
		}
		c.closeTransport()
		return false
	}
	if frame.Change == nil || frame.WatchReady {
		c.err = ErrProtocol
		c.closeTransport()
		return false
	}
	r := *frame.Change
	valid := r.CommitToken != "" && len(r.CommitToken) <= 128
	switch r.Operation {
	case db.ChangePut:
		valid = valid && len(r.Key) > 0 && len(r.Key) <= db.MaxKeySize
	case db.ChangeDelete:
		valid = valid && len(r.Key) > 0 && len(r.Key) <= db.MaxKeySize && !r.HasValue && len(r.Value) == 0
	case db.ChangeReset, db.ChangeDrop:
		valid = valid && len(r.Key) == 0 && !r.HasValue && len(r.Value) == 0
	case db.ChangeProgress:
		valid = valid && r.EndOfCommit && len(r.Key) == 0 && !r.HasValue && len(r.Value) == 0
	default:
		valid = false
	}
	if c.pending != "" && c.pending != r.CommitToken {
		valid = false
	}
	if !valid {
		c.err = ErrProtocol
		c.closeTransport()
		return false
	}
	c.row = r
	c.pending = r.CommitToken
	if r.EndOfCommit {
		c.token.Store(r.CommitToken)
		c.pending = ""
	}
	return true
}
func (c *ChangeCursor) Change() db.Change { return c.row }
func (c *ChangeCursor) Err() error        { return c.err }
func (c *ChangeCursor) ResumeToken() string {
	v := c.token.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// Snapshot is a read-only lease, separate from the live stream.
type Snapshot struct{ tx *Tx }
type SnapshotCollection struct{ inner *Collection }

func (s *Snapshot) Collection(name string) *SnapshotCollection {
	return &SnapshotCollection{s.tx.Collection(name)}
}
func (s *Snapshot) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.tx.Rollback(ctx)
	if errors.Is(err, db.ErrClosed) {
		return nil
	}
	return err
}
func (c *SnapshotCollection) Find(ctx context.Context, key []byte) ([]byte, bool, error) {
	return c.inner.Find(ctx, key)
}
func (c *SnapshotCollection) FindDocument(ctx context.Context, key []byte, out any) (bool, error) {
	return c.inner.FindDocument(ctx, key, out)
}
func (c *SnapshotCollection) FindJSON(ctx context.Context, key []byte) ([]byte, bool, error) {
	return c.inner.FindJSON(ctx, key)
}
func (c *SnapshotCollection) Query(ctx context.Context, filter []byte, o db.QueryOptions) (*Cursor, error) {
	return c.inner.Query(ctx, filter, o)
}

func (c *SnapshotCollection) CountDocuments(ctx context.Context, filter []byte, opts db.CountOptions) (int64, error) {
	return c.inner.CountDocuments(ctx, filter, opts)
}
