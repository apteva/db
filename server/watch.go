package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	db "github.com/apteva/db"
	"github.com/apteva/db/internal/protocol"
	"net/http"
	"reflect"
	"time"
)

func (s *Server) watch(w http.ResponseWriter, r *http.Request, req protocol.Request) {
	if req.Watch == nil || req.Transaction != "" || req.Op != "watch" {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	extra := req
	extra.Op = ""
	extra.Watch = nil
	extra.WatchSnapshot = false
	if !reflect.DeepEqual(extra, protocol.Request{}) {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(protocol.ErrBadRequest)})
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.watchCtx, cancel)
	defer stop()
	opts := *req.Watch
	if opts.MaxMemoryBytes == 0 || opts.MaxMemoryBytes > s.opts.MaxQueryMemoryBytes {
		opts.MaxMemoryBytes = s.opts.MaxQueryMemoryBytes
	}
	maxHeartbeat := s.opts.WatchWriteTimeout / 2
	if opts.HeartbeatInterval == 0 || opts.HeartbeatInterval > maxHeartbeat {
		opts.HeartbeatInterval = min(15*time.Second, maxHeartbeat)
	}
	var feed *db.ChangeCursor
	var snapshot *db.Snapshot
	var err error
	if req.WatchSnapshot {
		snapshot, feed, err = s.db.SnapshotAndWatch(ctx, opts)
	} else {
		feed, err = s.db.Watch(ctx, opts)
	}
	if err != nil {
		s.respond(w, http.StatusBadRequest, protocol.Response{Error: protocol.FromError(err)})
		return
	}
	defer feed.Close()
	var snapshotID string
	if snapshot != nil {
		var random [24]byte
		if _, err = rand.Read(random[:]); err != nil {
			snapshot.Close()
			s.respond(w, http.StatusServiceUnavailable, protocol.Response{Error: protocol.FromError(err)})
			return
		}
		snapshotID = hex.EncodeToString(random[:])
		lease := &session{snapshot: snapshot, expires: time.Now().Add(s.opts.TransactionTTL)}
		s.mu.Lock()
		if s.closed || len(s.sessions) >= s.opts.MaxTransactions {
			s.mu.Unlock()
			snapshot.Close()
			s.respond(w, http.StatusServiceUnavailable, protocol.Response{Error: protocol.FromError(protocol.ErrBusy)})
			return
		}
		s.sessions[snapshotID] = lease
		s.mu.Unlock()
		defer func() { s.mu.Lock(); delete(s.sessions, snapshotID); s.mu.Unlock(); lease.close() }()
	}
	controller := http.NewResponseController(w)
	if err = controller.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return
	}
	writeDeadline := func() error {
		err := controller.SetWriteDeadline(time.Now().Add(s.opts.WatchWriteTimeout))
		if errors.Is(err, http.ErrNotSupported) {
			return nil
		}
		return err
	}
	if err = writeDeadline(); err != nil {
		return
	}
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "application/x-ndjson")
	emit := func(frame protocol.Response) error {
		if err := writeDeadline(); err != nil {
			return err
		}
		// Marshal under a hard per-frame limit before writing a partial envelope.
		b, err := json.Marshal(frame)
		if err != nil {
			return err
		}
		if len(b) > protocol.MaxBodyBytes {
			return db.ErrLimit
		}
		if _, err = w.Write(append(b, '\n')); err != nil {
			return err
		}
		return controller.Flush()
	}
	if err = emit(protocol.Response{WatchReady: true, ChangeToken: feed.ResumeToken(), Transaction: snapshotID}); err != nil {
		return
	}
	for feed.Next() {
		change := feed.Change()
		if len(change.Value) > protocol.MaxBodyBytes/2 {
			err = db.ErrLimit
			break
		}
		if err = emit(protocol.Response{Change: &change}); err != nil {
			break
		}
	}
	if err == nil {
		err = feed.Err()
	}
	if ctx.Err() == nil {
		_ = emit(protocol.Response{Done: true, Error: protocol.FromError(err)})
	}
}
