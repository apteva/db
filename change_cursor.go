package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type watchPlan struct {
	id          uint64
	charge      int64
	key         string
	users       int // feed mutex
	collections map[string]bool
	operations  map[ChangeOp]bool
	prefix      []byte
	preds       []pred
	project     *computedCollectionPlan
}

// ChangeCursor is pull based. Next/Change/Err belong to one goroutine; Close and
// ResumeToken are safe concurrently. Next blocks until a change, a coalesced
// progress heartbeat, cancellation, history expiry, or DB close.
type ChangeCursor struct {
	f                               *changeFeed
	ctx                             context.Context
	cancel                          context.CancelFunc
	stepMu                          sync.Mutex
	closed                          atomic.Bool
	framePin                        *changeFrame
	closeOnce                       sync.Once
	opts                            WatchOptions
	needsValues                     bool
	projectKey                      [32]byte
	projectPlan                     *computedCollectionPlan
	plan                            *watchPlan
	pos                             lsn
	frame                           *changeFrame
	index                           int
	matches                         []uint64
	change                          Change
	err                             error
	valueBuf                        []byte
	projectionBuf                   []byte
	held, frameHeld                 int64
	compiledHeld, privatePlanCharge int64
	token                           atomic.Value // string, updated only at completed commit boundaries
	lastProgress                    time.Time
	projectionCharge                int64
	frameDone, deliveredInFrame     bool
}

func compileWatch(o WatchOptions) (*watchPlan, WatchOptions, error) {
	if o.MaxMemoryBytes < 0 || o.HeartbeatInterval < 0 || len(o.After) > 128 || len(o.KeyPrefix) > MaxKeySize || len(o.Operations) > 4 {
		return nil, o, ErrOptions
	}
	if o.MaxMemoryBytes == 0 {
		o.MaxMemoryBytes = 64 << 20
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 15 * time.Second
	}
	if o.MaxMemoryBytes > 2<<30 || len(o.Filter) > 64<<10 || len(o.Fields) > MaxDocumentFields || len(o.Collections) > maxCollections {
		return nil, o, ErrLimit
	}
	input := len(o.Filter) + len(o.KeyPrefix)
	for _, n := range o.Collections {
		input += len(n) + 16
		if n != "" && !validCollectionName(n) {
			return nil, o, ErrCollectionName
		}
	}
	matchInput := input
	for _, field := range o.Fields {
		input += len(field) + 16
	}
	if input > 64<<10 || int64(input)*4 > o.MaxMemoryBytes {
		return nil, o, ErrLimit
	}
	preds, err := queryPredicates(o.Filter)
	if err != nil {
		return nil, o, err
	}
	nodes := 0
	var visit func([]pred, int) error
	visit = func(ps []pred, depth int) error {
		if depth > 32 {
			return ErrLimit
		}
		for _, p := range ps {
			nodes += 1 + len(p.in)
			if nodes > 1024 {
				return ErrLimit
			}
			for _, child := range p.children {
				if err := visit(child, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = visit(preds, 0); err != nil {
		return nil, o, err
	}
	p := &watchPlan{collections: map[string]bool{}, operations: map[ChangeOp]bool{}, prefix: bytes.Clone(o.KeyPrefix), preds: preds}
	for _, n := range o.Collections {
		p.collections[strings.Clone(n)] = true
	}
	for _, op := range o.Operations {
		if op != ChangePut && op != ChangeDelete && op != ChangeReset && op != ChangeDrop {
			return nil, o, ErrOptions
		}
		p.operations[op] = true
	}
	if len(o.Fields) > 0 {
		if !o.IncludeValues {
			return nil, o, ErrOptions
		}
		fields := map[string]bool{"_id": false}
		for _, name := range o.Fields {
			if !computedOutputValid(name) {
				return nil, o, ErrOptions
			}
			fields[strings.Clone(name)] = true
		}
		p.project = &computedCollectionPlan{project: fields, inclusion: true}
	}
	// Semantically identical subscription filters share compiled plans and hot
	// commit bitsets. Values/projection/heartbeat are independent delivery choices.
	var filter bytes.Buffer
	if len(o.Filter) > 0 {
		if err = json.Compact(&filter, o.Filter); err != nil {
			return nil, o, ErrPipeline
		}
	}
	names := append([]string(nil), o.Collections...)
	sort.Strings(names)
	ops := append([]ChangeOp(nil), o.Operations...)
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	key, _ := json.Marshal(struct {
		Names  []string
		Ops    []ChangeOp
		Prefix []byte
		Filter string
	}{names, ops, o.KeyPrefix, filter.String()})
	sum := sha256.Sum256(key)
	p.key = string(sum[:])
	p.charge = int64(matchInput)*4 + int64(nodes)*96 + 512
	return p, o, nil
}
func (db *DB) Watch(ctx context.Context, o WatchOptions) (*ChangeCursor, error) {
	p, o, err := compileWatch(o)
	if err != nil {
		return nil, err
	}
	ctx = orBackground(ctx)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if o.After == "" {
		if err = db.ensureChangeBoundary(ctx); err != nil {
			return nil, err
		}
	}
	db.mu.RLock()
	if db.closed || db.failed != nil {
		err = db.failed
		if db.closed {
			err = ErrClosed
		}
		db.mu.RUnlock()
		return nil, err
	}
	f := db.feed
	if f == nil {
		db.mu.RUnlock()
		return nil, ErrChangeFeedDisabled
	}
	projection := p.project
	p.project = nil // delivery projection belongs to this cursor, not the shared match plan
	var privatePlanCharge int64
	if projection != nil {
		privatePlanCharge = 512
		for _, field := range o.Fields {
			privatePlanCharge += int64(len(field)+16) * 4
		}
	}
	fieldList := append([]string(nil), o.Fields...)
	sort.Strings(fieldList)
	fieldJSON, _ := json.Marshal(fieldList)
	fieldKey := sha256.Sum256(fieldJSON)
	f.mu.Lock()
	if f.watchers >= f.cfg.Options.MaxWatchers {
		f.mu.Unlock()
		db.mu.RUnlock()
		return nil, ErrWatchLimit
	}
	existing := f.plans[p.key]
	extraPlan := privatePlanCharge
	if existing != nil {
		p = existing
	} else {
		extraPlan += p.charge
	}
	if p.charge+privatePlanCharge > o.MaxMemoryBytes || extraPlan > f.cfg.Options.MaxWatchBytes-f.readerBytes-f.planBytes {
		f.mu.Unlock()
		db.mu.RUnlock()
		return nil, ErrLimit
	}
	f.planBytes += extraPlan
	if existing == nil {
		f.nextPlanID++
		p.id = f.nextPlanID
		f.plans[p.key] = p
	}
	p.users++
	f.watchers++
	needsValues := (o.IncludeValues || len(p.preds) > 0) && (len(p.operations) == 0 || p.operations[ChangePut])
	if needsValues {
		if len(p.collections) == 0 {
			f.valueGlobal++
		} else {
			for name := range p.collections {
				f.valueCollections[name]++
			}
		}
	}
	pos := f.applied
	token := f.latest
	f.mu.Unlock()
	db.mu.RUnlock()
	ctx, cancel := context.WithCancel(ctx)
	after := strings.Clone(o.After)
	// The compiled plans own the filter/projection data. Retaining the caller
	// slices here would both duplicate their cost and keep oversized capacities alive.
	o.Filter, o.Fields, o.Collections, o.Operations, o.KeyPrefix, o.After = nil, nil, nil, nil, nil, ""
	c := &ChangeCursor{held: p.charge + privatePlanCharge, compiledHeld: p.charge + privatePlanCharge, privatePlanCharge: privatePlanCharge, f: f, ctx: ctx, cancel: cancel, opts: o, plan: p, projectPlan: projection, projectKey: fieldKey, needsValues: needsValues, pos: pos, lastProgress: time.Now()}
	c.token.Store(token)
	if after != "" {
		pos, err = c.resume(after)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.pos = pos
		c.token.Store(after)
	}
	return c, nil
}
func (c *ChangeCursor) reserveFrame(fr *changeFrame, n int64) error {
	if n+c.held > c.opts.MaxMemoryBytes {
		return ErrLimit
	}
	if fr == nil {
		if err := c.f.reserveReader(n); err != nil {
			return err
		}
	} else {
		c.f.mu.Lock()
		if fr.pins == 0 {
			if n > c.f.cfg.Options.MaxWatchBytes-c.f.readerBytes-c.f.planBytes {
				c.f.mu.Unlock()
				return ErrLimit
			}
			c.f.readerBytes += n
			c.f.peakReaderBytes = max(c.f.peakReaderBytes, c.f.readerBytes)
			fr.pinCharge = n
		}
		fr.pins++
		c.f.pinned[fr.start] = fr
		c.f.mu.Unlock()
		c.framePin = fr
	}
	c.frameHeld += n
	c.held += n
	return nil
}
func (c *ChangeCursor) bindColdFrame(fr *changeFrame) *changeFrame {
	if c.framePin != nil {
		return fr
	}
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	if existing := c.f.pinned[fr.start]; existing != nil {
		c.f.readerBytes -= c.frameHeld
		existing.pins++
		c.framePin = existing
		return existing
	}
	fr.pins = 1
	fr.pinCharge = c.frameHeld
	c.f.pinned[fr.start] = fr
	c.framePin = fr
	return fr
}
func (c *ChangeCursor) releaseFrame() {
	if c.framePin != nil {
		c.f.mu.Lock()
		c.framePin.pins--
		if c.framePin.pins == 0 {
			c.f.readerBytes -= c.framePin.pinCharge + c.framePin.payloadCharge
			c.framePin.pinCharge = 0
			c.framePin.payloadCharge = 0
			c.framePin.payload.Store(nil)
			delete(c.f.pinned, c.framePin.start)
		}
		c.f.mu.Unlock()
		c.framePin = nil
	} else {
		c.f.releaseReader(c.frameHeld)
	}
	c.held -= c.frameHeld
	c.frameHeld = 0
	c.frame = nil
	c.matches = nil
	c.index = 0
}
func (c *ChangeCursor) resume(token string) (lsn, error) {
	at, run, err := c.f.decodeToken(token)
	if err != nil {
		return lsn{}, err
	}
	floor := c.f.db.wal.changeFloor()
	if run == ([16]byte{}) {
		start := lsn{c.f.cfg.StartGen, c.f.cfg.StartOff}
		if at != start {
			return lsn{}, ErrResumeInvalid
		}
		if lsnLess(at, floor) {
			return lsn{}, ErrHistoryExpired
		}
		return at, nil
	}
	// A token points at the marker, not a user record or arbitrary byte offset.
	c.f.mu.Lock()
	latest := c.f.applied
	c.f.mu.Unlock()
	if !lsnLess(at, latest) {
		return lsn{}, ErrResumeInvalid
	}
	c.f.mu.Lock()
	var cached *changeFrame
	if token == c.f.latest {
		cached = &changeFrame{end: c.f.applied, token: c.f.latest, stamp: c.f.latestStamp}
	} else {
		for lo, hi := c.f.head, len(c.f.frames); lo < hi; {
			mid := (lo + hi) / 2
			if lsnLess(c.f.frames[mid].marker, at) {
				lo = mid + 1
			} else {
				hi = mid
			}
			if lo == hi && lo < len(c.f.frames) && c.f.frames[lo].marker == at {
				cached = c.f.frames[lo]
			}
		}
	}
	c.f.mu.Unlock()
	fr := cached
	if fr == nil {
		fr, err = c.f.readMarkerSummary(at)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return lsn{}, ErrResumeInvalid
	}
	if err != nil {
		return lsn{}, err
	}
	if lsnLess(fr.end, floor) {
		return lsn{}, ErrHistoryExpired
	}
	if fr.token != token {
		return lsn{}, ErrResumeInvalid
	}
	if time.Since(time.Unix(0, fr.stamp)) > c.f.cfg.Options.RetentionAge {
		return lsn{}, ErrHistoryExpired
	}
	return fr.end, nil
}
func (c *ChangeCursor) growValue(n int) ([]byte, error) {
	if int64(n) > c.opts.MaxMemoryBytes {
		return nil, ErrLimit
	}
	if n > cap(c.valueBuf) {
		delta := int64(n - cap(c.valueBuf))
		if c.held+delta > c.opts.MaxMemoryBytes {
			return nil, ErrLimit
		}
		if err := c.f.reserveReader(delta); err != nil {
			return nil, err
		}
		c.held += delta
		c.valueBuf = make([]byte, n)
	} else {
		c.valueBuf = c.valueBuf[:n]
	}
	return c.valueBuf, nil
}
func (c *ChangeCursor) value(fr *changeFrame, r changeRef) ([]byte, error) {
	if r.op != ChangePut {
		return nil, nil
	}
	raw := fr.raw
	if raw == nil {
		if loaded := fr.payload.Load(); loaded != nil {
			raw = loaded.data
		} else {
			if err := c.loadSharedPayload(fr); err != nil {
				return nil, err
			}
			if loaded := fr.payload.Load(); loaded != nil {
				raw = loaded.data
			}
		}
	}
	if raw != nil {
		off := int(r.off)
		if off+11 > len(raw) {
			return nil, ErrCorrupt
		}
		plen := int(le.Uint32(raw[off:]))
		kl := int(le.Uint16(raw[off+9:]))
		if plen < 3+kl || off+8+plen > len(raw) {
			return nil, ErrCorrupt
		}
		if int64(plen-3-kl) > c.opts.MaxMemoryBytes {
			return nil, ErrLimit
		}
		_, uk, ok := splitKey(raw[off+11 : off+11+kl])
		if !ok || !bytes.Equal(uk, r.key) {
			return nil, ErrCorrupt
		}
		return raw[off+11+kl : off+8+plen], nil
	}
	at := fr.start.off + uint64(r.off)
	var h [11]byte
	if err := c.f.db.wal.readChange(fr.start.gen, at, h[:]); err != nil {
		return nil, err
	}
	plen := uint64(le.Uint32(h[:4]))
	kl := uint64(le.Uint16(h[9:]))
	if plen < 3+kl || at+8+plen > fr.marker.off || h[8]&flagDelete != 0 {
		return nil, ErrCorrupt
	}
	// Read the entire original record for checksum verification, including key.
	buf, err := c.growValue(int(plen + 8))
	if err != nil {
		return nil, err
	}
	copy(buf, h[:])
	if err = c.f.db.wal.readChange(fr.start.gen, at+11, buf[11:]); err != nil {
		return nil, err
	}
	if crc32.Checksum(buf[8:], c.f.db.wal.crcTab) != le.Uint32(buf[4:]) {
		return nil, ErrCorrupt
	}
	cid, uk, ok := splitKey(buf[11 : 11+kl])
	_ = cid
	if !ok || !bytes.Equal(uk, r.key) {
		return nil, ErrCorrupt
	}
	return buf[11+kl:], nil
}
func (c *ChangeCursor) metadataMatch(r changeRef) bool {
	p := c.plan
	if len(p.collections) > 0 && !p.collections[r.collection] {
		return false
	}
	if r.op == ChangeReset || r.op == ChangeDrop {
		return true
	}
	if len(p.operations) > 0 && !p.operations[r.op] {
		return false
	}
	if !bytes.HasPrefix(r.key, p.prefix) {
		return false
	}
	if len(p.preds) > 0 && r.op != ChangePut {
		return false
	}
	return true
}
func (c *ChangeCursor) match(fr *changeFrame, r changeRef) (bool, error) {
	if !c.metadataMatch(r) {
		return false, nil
	}
	if len(c.plan.preds) == 0 {
		return true, nil
	}
	v, err := c.value(fr, r)
	if err != nil {
		return false, err
	}
	if _, _, ok := docDir(v); !ok {
		return false, nil
	}
	return matchAll(c.plan.preds, func(path string) value { return pathValue(value{k: kDoc, s: v}, path) }), nil
}
func (c *ChangeCursor) prepareMatches(fr *changeFrame) error {
	if len(fr.refs) > 1024 {
		return nil
	}
	fr.evalMu.Lock()
	defer fr.evalMu.Unlock()
	if bits := fr.matches[c.plan.id]; bits != nil {
		c.matches = bits
		return nil
	}
	if len(fr.matches) >= 16 {
		return nil
	}
	bits := make([]uint64, (len(fr.refs)+63)/64)
	for i, r := range fr.refs {
		match, err := c.match(fr, r)
		if err != nil {
			return err
		}
		if match {
			bits[i/64] |= 1 << uint(i%64)
		}
	}
	if fr.matches == nil {
		fr.matches = map[uint64][]uint64{}
	}
	fr.matches[c.plan.id] = bits
	c.matches = bits
	return nil
}
func (c *ChangeCursor) project(v []byte) ([]byte, error) {
	if c.projectPlan == nil {
		return v, nil
	}
	if err := validateComputedDoc(v, 0); err != nil {
		return nil, err
	}
	if c.projectPlan.projectKeepsAll(v) {
		return v, nil
	}
	// Projection output is bounded by the input; charge before allocating it.
	n := int64(c.projectPlan.projectCapacity(v))
	if c.held+n > c.opts.MaxMemoryBytes {
		return nil, ErrLimit
	}
	if err := c.f.reserveReader(n); err != nil {
		return nil, err
	}
	out, err := c.projectPlan.projectDocCapacity(v, int(n))
	if err != nil {
		c.f.releaseReader(n)
		return nil, err
	}
	c.projectionBuf = out
	c.held += n
	c.projectionCharge = n
	return out, nil
}
func (c *ChangeCursor) finishFrame() {
	c.pos = c.frame.end
	c.token.Store(c.frame.token)
	c.releaseFrame()
	c.dropLargeValueBuffer()
}

// ResumeToken returns the last fully delivered commit (or safely skipped
// filtered commit). Saving it before processing the current commit is unsafe.
func (c *ChangeCursor) ResumeToken() string {
	v := c.token.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}
func (c *ChangeCursor) Change() Change { return c.change }
func (c *ChangeCursor) Err() error     { return c.err }
func (c *ChangeCursor) detach() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.f.mu.Lock()
		c.f.watchers--
		if c.needsValues {
			if len(c.plan.collections) == 0 {
				c.f.valueGlobal--
			} else {
				for name := range c.plan.collections {
					c.f.valueCollections[name]--
					if c.f.valueCollections[name] == 0 {
						delete(c.f.valueCollections, name)
					}
				}
			}
		}
		c.f.planBytes -= c.privatePlanCharge
		c.plan.users--
		if c.plan.users == 0 {
			c.f.planBytes -= c.plan.charge
			delete(c.f.plans, c.plan.key)
		}
		c.f.mu.Unlock()
		c.plan = nil
		c.projectPlan = nil
	})
}
func (c *ChangeCursor) releaseMemory() {
	c.releaseFrame()
	c.f.releaseReader(c.held - c.compiledHeld)
	c.held, c.compiledHeld = 0, 0
	c.valueBuf = nil
	c.projectionBuf = nil
	c.projectionCharge = 0
	c.change = Change{}
}
func (c *ChangeCursor) Close() error {
	c.closed.Store(true)
	c.cancel()
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	c.releaseMemory()
	c.detach()
	return nil
}
func (c *ChangeCursor) cleanup() { c.releaseMemory(); c.detach(); c.cancel() }
func (c *ChangeCursor) Next() bool {
	c.stepMu.Lock()
	defer c.stepMu.Unlock()
	if c.closed.Load() {
		return false
	}
	if c.err != nil {
		return false
	}
	// Projection was charged using input length, recorded independently below.
	if c.projectionCharge > 0 {
		c.f.releaseReader(c.projectionCharge)
		c.held -= c.projectionCharge
		c.projectionCharge = 0
		c.projectionBuf = nil
	}
	c.change = Change{}
	for {
		if err := c.ctx.Err(); err != nil {
			if !c.closed.Load() {
				c.err = err
			}
			c.cleanup()
			return false
		}
		if c.f.halt.Load() {
			c.f.mu.Lock()
			c.err = c.f.failure
			c.f.mu.Unlock()
			if c.err == nil {
				c.err = ErrClosed
			}
			c.cleanup()
			return false
		}
		if c.frame != nil {
			fr := c.frame
			for c.index < len(fr.refs) {
				i := c.index
				c.index++
				r := fr.refs[i]
				matched := false
				var err error
				if c.matches != nil {
					matched = c.matches[i/64]&(1<<uint(i%64)) != 0
				} else {
					matched, err = c.match(fr, r)
				}
				if err != nil {
					c.err = err
					c.cleanup()
					return false
				}
				if !matched {
					continue
				}
				change := Change{Operation: r.op, Collection: r.collection, Key: r.key, CommitToken: fr.token}
				if c.opts.IncludeValues && r.op == ChangePut {
					v, e := c.deliveryValue(fr, i, r)
					if e != nil {
						c.err = e
						c.cleanup()
						return false
					}

					change.Value = v
					change.HasValue = true
				}
				// Look ahead using shared bitsets; otherwise send an explicit progress
				// boundary on the following Next rather than prematurely ending a commit.
				if c.matches != nil {
					last := true
					for j := c.index; j < len(fr.refs); j++ {
						if c.matches[j/64]&(1<<uint(j%64)) != 0 {
							last = false
							break
						}
					}
					if last {
						change.EndOfCommit = true
						c.pos = fr.end
						c.token.Store(fr.token)
						c.index = len(fr.refs)
						c.frameDone = true
					}
				} else if c.index == len(fr.refs) {
					change.EndOfCommit = true
					c.pos = fr.end
					c.token.Store(fr.token)
					c.frameDone = true
				}
				c.deliveredInFrame = true
				if change.EndOfCommit {
					c.lastProgress = time.Now()
				}
				c.change = change
				return true
			}
			token := fr.token
			done := c.frameDone
			c.frameDone = false
			c.finishFrame()
			if !done && c.deliveredInFrame {
				c.deliveredInFrame = false
				c.change = Change{Operation: ChangeProgress, CommitToken: token, EndOfCommit: true}
				return true
			}
			c.deliveredInFrame = false
		}
		if c.frame == nil && time.Since(c.lastProgress) >= c.opts.HeartbeatInterval {
			c.lastProgress = time.Now()
			c.change = Change{Operation: ChangeProgress, CommitToken: c.ResumeToken(), EndOfCommit: true}
			return true
		}
		floor := c.f.db.wal.changeFloor()
		if lsnLess(c.pos, floor) {
			c.err = ErrHistoryExpired
			c.cleanup()
			return false
		}
		c.f.mu.Lock()
		ready := c.f.ready
		notify := c.f.notify
		stopped := c.f.stopped
		failure := c.f.failure
		c.f.mu.Unlock()
		if stopped {
			c.err = failure
			if c.err == nil {
				c.err = ErrClosed
			}
			c.cleanup()
			return false
		}
		if lsnLess(c.pos, ready) {
			fr, err := c.f.nextFrame(c.pos, ready, c.opts.MaxMemoryBytes-c.held, c.reserveFrame)
			if err != nil {
				c.err = err
				c.cleanup()
				return false
			}
			if fr != nil {
				if time.Since(time.Unix(0, fr.stamp)) > c.f.cfg.Options.RetentionAge {
					c.err = ErrHistoryExpired
					c.cleanup()
					return false
				}
				fr = c.bindColdFrame(fr)
				c.frame = fr
				c.index = 0
				if err = c.prepareMatches(fr); err != nil {
					c.err = err
					c.cleanup()
					return false
				}
				continue
			}
			c.pos = ready
		}
		delay := c.opts.HeartbeatInterval - time.Since(c.lastProgress)
		if delay <= 0 {
			c.lastProgress = time.Now()
			c.change = Change{Operation: ChangeProgress, CommitToken: c.ResumeToken(), EndOfCommit: true}
			return true
		}
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			c.lastProgress = time.Now()
			c.change = Change{Operation: ChangeProgress, CommitToken: c.ResumeToken(), EndOfCommit: true}
			return true
		}
	}
}

// SnapshotAndWatch captures the initial state and feed boundary under one writer
// lock. After must be empty. Closing the snapshot does not close the feed.
func (db *DB) SnapshotAndWatch(ctx context.Context, o WatchOptions) (*Snapshot, *ChangeCursor, error) {
	if o.After != "" {
		return nil, nil, ErrOptions
	}
	if _, _, err := compileWatch(o); err != nil {
		return nil, nil, err
	}
	ctx = orBackground(ctx)
	if err := db.ensureChangeBoundary(ctx); err != nil {
		return nil, nil, err
	}
	db.mu.Lock()
	if db.feed == nil {
		db.mu.Unlock()
		return nil, nil, ErrChangeFeedDisabled
	}
	s, err := db.snapshotLocked(true)
	if err != nil {
		db.mu.Unlock()
		return nil, nil, err
	}
	f := db.feed
	f.mu.Lock()
	token := f.latest
	target := f.applied
	f.mu.Unlock()
	db.mu.Unlock()
	if db.opts.Sync {
		if err = db.syncWrite(db.wal, target); err != nil {
			s.Close()
			return nil, nil, err
		}
	}
	o.After = token
	cursor, err := db.Watch(ctx, o)
	if err != nil {
		s.Close()
		return nil, nil, err
	}
	return s, cursor, nil
}

func (c *ChangeCursor) dropLargeValueBuffer() {
	if cap(c.valueBuf) > 64<<10 {
		n := int64(cap(c.valueBuf))
		c.f.releaseReader(n)
		c.held -= n
		c.valueBuf = nil
	}
}
func (c *ChangeCursor) loadSharedPayload(fr *changeFrame) error {
	n := int64(fr.marker.off - fr.start.off)
	if n <= 0 || n > c.f.cfg.Options.CacheBytes/2 || c.held+n > c.opts.MaxMemoryBytes {
		return nil
	}
	fr.loadMu.Lock()
	defer fr.loadMu.Unlock()
	if fr.payload.Load() != nil {
		return nil
	}
	if err := c.f.reserveReader(n); err != nil {
		return nil
	} // optional optimization
	data := make([]byte, int(n))
	if err := c.f.db.wal.readChange(fr.start.gen, fr.start.off, data); err != nil {
		c.f.releaseReader(n)
		return err
	}
	for off := 0; off < len(data); {
		if off+11 > len(data) {
			c.f.releaseReader(n)
			return ErrCorrupt
		}
		plen := int(le.Uint32(data[off:]))
		if plen < 3 || off+8+plen > len(data) || crc32.Checksum(data[off+8:off+8+plen], c.f.db.wal.crcTab) != le.Uint32(data[off+4:]) {
			c.f.releaseReader(n)
			return ErrCorrupt
		}
		off += 8 + plen
	}
	c.f.mu.Lock()
	fr.payloadCharge = n
	fr.payload.Store(&changePayload{data})
	c.f.mu.Unlock()
	return nil
}
func (c *ChangeCursor) deliveryValue(fr *changeFrame, i int, r changeRef) ([]byte, error) {
	if c.projectPlan != nil {
		key := changeProjectionKey{c.projectKey, i}
		fr.evalMu.Lock()
		cached, ok := fr.projections[key]
		fr.evalMu.Unlock()
		if ok {
			return cached, nil
		}
	}
	v, err := c.value(fr, r)
	if err != nil {
		return nil, err
	}
	v, err = c.project(v)
	if err != nil {
		return nil, err
	}
	if c.projectPlan != nil && len(v) <= 256 && len(fr.refs) <= 1024 {
		fr.evalMu.Lock()
		if len(fr.projections) < 16 && fr.projectionBytes+len(v) <= 4096 {
			if fr.projections == nil {
				fr.projections = map[changeProjectionKey][]byte{}
			}
			key := changeProjectionKey{c.projectKey, i}
			if _, exists := fr.projections[key]; !exists {
				fr.projections[key] = bytes.Clone(v)
				fr.projectionBytes += len(v)
			}
		}
		fr.evalMu.Unlock()
	}
	return v, nil
}
