package db

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrChangeFeedDisabled = errors.New("aptevadb: change feed is disabled")
	ErrHistoryExpired     = errors.New("aptevadb: change history expired; take a new snapshot")
	ErrResumeInvalid      = errors.New("aptevadb: invalid change resume token")
	ErrWatchLimit         = errors.New("aptevadb: change cursor limit reached")
)

// ChangeFeedOptions configures persistent capture. Nil in Options leaves a fresh
// database disabled and preserves an existing database's stored settings.
// Disabled explicitly ends the old history and invalidates its tokens.
// Retention is logical: WAL files are reclaimed at segment boundaries once no
// longer needed for database recovery. The active recovery tail is never lost.
type ChangeFeedOptions struct {
	Disabled       bool          `json:"disabled,omitempty"`
	RetentionBytes int64         `json:"retention_bytes,omitempty"`
	RetentionAge   time.Duration `json:"retention_age,omitempty"`
	// CacheBytes bounds shared hot commit data/metadata; zero selects 8 MiB.
	CacheBytes  int64 `json:"cache_bytes,omitempty"`
	MaxWatchers int   `json:"max_watchers,omitempty"`
	// MaxWatchBytes bounds aggregate cursor working buffers, default 64 MiB.
	MaxWatchBytes int64 `json:"max_watch_bytes,omitempty"`
	// MaxCaptureBytes bounds commit metadata/coalescing work, excluding existing
	// staged values. Exhaustion rejects the write before any record is logged.
	MaxCaptureBytes int64 `json:"max_capture_bytes,omitempty"`
}

// WatchOptions applies filters inside the engine before delivery. Filter uses
// Query's predicate language against Put's exact committed value. Delete has no
// post-image and cannot match a document predicate; Reset/Drop always pass for a
// selected collection. Use a computed collection for live query membership.
type WatchOptions struct {
	Collections    []string        `json:"collections,omitempty"`
	Operations     []ChangeOp      `json:"operations,omitempty"`
	KeyPrefix      []byte          `json:"key_prefix,omitempty"`
	Filter         json.RawMessage `json:"filter,omitempty"`
	Fields         []string        `json:"fields,omitempty"`
	IncludeValues  bool            `json:"include_values,omitempty"`
	After          string          `json:"after,omitempty"`
	MaxMemoryBytes int64           `json:"max_memory_bytes,omitempty"`
	// HeartbeatInterval coalesces progress through filtered commits; default 15s.
	HeartbeatInterval time.Duration `json:"heartbeat_interval,omitempty"`
}

type ChangeOp string

const (
	ChangePut    ChangeOp = "put"
	ChangeDelete ChangeOp = "delete"
	ChangeReset  ChangeOp = "reset"
	ChangeDrop   ChangeOp = "drop"
	// ChangeProgress is a control frame, not a row mutation.
	ChangeProgress ChangeOp = "progress"
)

// Change buffers are read-only and valid until the cursor's next Next call. Clone for asynchronous
// processing. A CommitToken is safe to save only after EndOfCommit is processed.
// Put describes a final write; unchanged upserts and absent deletes can appear.
type Change struct {
	Operation   ChangeOp `json:"operation"`
	Collection  string   `json:"collection,omitempty"`
	Key         []byte   `json:"key,omitempty"`
	Value       []byte   `json:"value,omitempty"`
	HasValue    bool     `json:"has_value,omitempty"`
	CommitToken string   `json:"commit_token"`
	EndOfCommit bool     `json:"end_of_commit,omitempty"`
}

func (c Change) Clone() Change { c.Key = bytes.Clone(c.Key); c.Value = bytes.Clone(c.Value); return c }
func (c Change) Decode(out any) error {
	if !c.HasValue {
		return ErrDocument
	}
	return Decode(c.Value, out)
}

var changeConfigKey = []byte{reservedPrefix, 'F'}

const changeMagic = "CFMARK10"
const changeFooter = "CFEND010"
const flagFeed byte = 4
const changeHeaderBytes = 60 // magic, stream UUID, run UUID, time, raw length, count

type changeConfig struct {
	Version  int               `json:"version"`
	ID       string            `json:"id"`
	Options  ChangeFeedOptions `json:"options"`
	StartGen uint64            `json:"start_gen"`
	StartOff uint64            `json:"start_off"`
}
type changeRef struct {
	op         ChangeOp
	collection string
	key        []byte
	off        uint32
}
type changeFrame struct {
	start, end, marker lsn
	stamp              int64
	token              string
	refs               []changeRef
	raw                []byte
	encoded            []byte
	charge             int64
	evalMu             sync.Mutex
	matches            map[uint64][]uint64
	pins               int
	pinCharge          int64
	loadMu             sync.Mutex
	payload            atomic.Pointer[changePayload]
	payloadCharge      int64
	projections        map[changeProjectionKey][]byte
	projectionBytes    int
	one                [1]changeRef
}
type changePayload struct{ data []byte }
type changeProjectionKey struct {
	fields [32]byte
	row    int
}

type changeFeed struct {
	db               *DB
	cfg              changeConfig
	id, run          [16]byte
	mu               sync.Mutex
	applied, ready   lsn
	latest           string
	latestStamp      int64
	notify           chan struct{}
	halt             atomic.Bool
	stopped          bool
	failure          error
	watchers         int
	frames           []*changeFrame
	head             int
	bytes            int64
	readerBytes      int64
	peakReaderBytes  int64
	nextPlanID       uint64
	plans            map[string]*watchPlan
	pinned           map[lsn]*changeFrame
	planBytes        int64
	valueGlobal      int
	valueCollections map[string]int
}

func normalizeChangeOptions(o ChangeFeedOptions) (ChangeFeedOptions, error) {
	if o.RetentionBytes < 0 || o.RetentionAge < 0 || o.CacheBytes < 0 || o.MaxWatchers < 0 || o.MaxCaptureBytes < 0 || o.MaxWatchBytes < 0 {
		return o, ErrOptions
	}
	if o.RetentionBytes == 0 {
		o.RetentionBytes = 256 << 20
	}
	if o.RetentionAge == 0 {
		o.RetentionAge = time.Hour
	}
	if o.CacheBytes == 0 {
		o.CacheBytes = 8 << 20
	}
	if o.MaxWatchers == 0 {
		o.MaxWatchers = 256
	}
	if o.MaxWatchBytes == 0 {
		o.MaxWatchBytes = 64 << 20
	}
	if o.MaxCaptureBytes == 0 {
		o.MaxCaptureBytes = 256 << 20
	}
	if o.CacheBytes > 1<<30 || o.MaxWatchers > 1<<20 || o.MaxCaptureBytes > MaxValueSize {
		return o, ErrOptions
	}
	return o, nil
}
func lsnLess(a, b lsn) bool { return a.gen < b.gen || (a.gen == b.gen && a.off < b.off) }
func lsnLE(a, b lsn) bool   { return !lsnLess(b, a) }

// Recovery reads the durable configuration before deleting checkpointed WALs.
func (db *DB) loadChangeRetention() error {
	v, ok, err := db.findLocked(view{db, nil}, changeConfigKey, nil)
	if err != nil || !ok {
		return err
	}
	var cfg changeConfig
	if json.Unmarshal(v, &cfg) != nil || cfg.Version != 1 {
		return ErrCorrupt
	}
	id, e := hex.DecodeString(cfg.ID)
	if e != nil || len(id) != 16 {
		return ErrCorrupt
	}
	opts, e := normalizeChangeOptions(cfg.Options)
	if e != nil {
		return ErrCorrupt
	}
	if !opts.Disabled {
		db.wal.retain = &opts
	}
	return nil
}
func (db *DB) openChangeFeed() error {
	v, ok, err := db.findLocked(view{db, nil}, changeConfigKey, nil)
	if err != nil {
		return err
	}
	var cfg changeConfig
	if ok {
		if json.Unmarshal(v, &cfg) != nil || cfg.Version != 1 {
			return ErrCorrupt
		}
	}
	if db.opts.ChangeFeed != nil {
		opts, err := normalizeChangeOptions(*db.opts.ChangeFeed)
		if err != nil {
			return err
		}
		if !ok || cfg.Options.Disabled != opts.Disabled {
			var id [16]byte
			if _, err = rand.Read(id[:]); err != nil {
				return err
			}
			cfg.ID = hex.EncodeToString(id[:])
			cfg.StartGen, cfg.StartOff = db.wal.position()
		}
		cfg.Version = 1
		cfg.Options = opts
		enc, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		if !ok || !bytes.Equal(v, enc) {
			b := db.NewBatch()
			b.internal = true
			if err = b.add(changeConfigKey, enc, false); err != nil {
				return err
			}
			if _, err = b.commitLocked(); err != nil {
				return err
			}
			// Identity/settings must survive a crash even with asynchronous data writes.
			if !opts.Disabled {
				db.wal.retain = &cfg.Options
			}
			if err = db.runCheckpoint(true); err != nil {
				return err
			}
			if err = db.runCheckpoint(true); err != nil {
				return err
			}
		}
		ok = true
	}
	if !ok || cfg.Options.Disabled {
		db.wal.retain = nil
		return nil
	}
	opts, err := normalizeChangeOptions(cfg.Options)
	if err != nil {
		return ErrCorrupt
	}
	cfg.Options = opts
	id, err := hex.DecodeString(cfg.ID)
	if err != nil || len(id) != 16 {
		return ErrCorrupt
	}
	f := &changeFeed{db: db, cfg: cfg, notify: make(chan struct{}), plans: map[string]*watchPlan{}, pinned: map[lsn]*changeFrame{}, valueCollections: map[string]int{}}
	copy(f.id[:], id)
	if _, err = rand.Read(f.run[:]); err != nil {
		return err
	}
	f.applied = lsn{cfg.StartGen, cfg.StartOff}
	f.ready = f.applied
	f.latest = f.token(f.applied, [16]byte{})
	db.wal.retain = &f.cfg.Options
	if err = db.wal.initChangeHistory(); err != nil {
		return err
	}
	if err = f.recoverBoundary(); err != nil {
		return err
	}
	db.feed = f
	return nil
}
func (f *changeFeed) token(marker lsn, run [16]byte) string {
	var b [53]byte
	b[0] = 1
	copy(b[1:17], f.id[:])
	copy(b[17:33], run[:])
	le.PutUint64(b[33:41], marker.gen)
	le.PutUint64(b[41:49], marker.off)
	le.PutUint32(b[49:], crc32.Checksum(b[:49], f.db.wal.crcTab))
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func (f *changeFeed) decodeToken(s string) (lsn, [16]byte, error) {
	var run [16]byte
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != 53 || b[0] != 1 || !bytes.Equal(b[1:17], f.id[:]) || le.Uint32(b[49:]) != crc32.Checksum(b[:49], f.db.wal.crcTab) {
		return lsn{}, run, ErrResumeInvalid
	}
	copy(run[:], b[17:33])
	return lsn{le.Uint64(b[33:41]), le.Uint64(b[41:49])}, run, nil
}
func changeCode(op ChangeOp) byte {
	switch op {
	case ChangePut:
		return 1
	case ChangeDelete:
		return 2
	case ChangeReset:
		return 3
	case ChangeDrop:
		return 4
	}
	return 0
}
func changeOperation(code byte) ChangeOp {
	switch code {
	case 1:
		return ChangePut
	case 2:
		return ChangeDelete
	case 3:
		return ChangeReset
	case 4:
		return ChangeDrop
	}
	return ""
}

// Capture is confined to logical commit construction. It never calls watchers.
func (f *changeFeed) capture(raw []byte) (*changeFrame, error) {
	if len(raw) >= 11 && int(le.Uint32(raw[:4]))+8 == len(raw) {
		kl := int(le.Uint16(raw[9:]))
		if 11+kl <= len(raw) {
			if cid, uk, ok := splitKey(raw[11 : 11+kl]); ok {
				if c, found := f.db.collectionByID(cid); found {
					return f.captureOne(c, uk, raw[11:11+kl], raw[11+kl:], raw[8]&flagDelete != 0, raw)
				}
			}
		}
	}

	frame := &changeFrame{stamp: time.Now().UnixNano()}
	names := map[uint32]string{0: ""}
	schema := map[uint32]ChangeOp{}
	// Catalog creation/drop includes backfill/tombstones; expose one reset/drop.
	for off := 0; off < len(raw); {
		plen := int(le.Uint32(raw[off:]))
		r := raw[off+8 : off+8+plen]
		kl := int(le.Uint16(r[1:]))
		key, val := r[3:3+kl], r[3+kl:]
		off += 8 + plen
		if len(key) > 2 && key[0] == reservedPrefix && key[1] == 'n' {
			name := string(key[2:])
			var id uint32
			op := ChangeReset
			if r[0]&flagDelete != 0 {
				op = ChangeDrop
				c, ok := f.db.lookupCollection(name)
				if !ok {
					return nil, ErrCorrupt
				}
				id = c.id
			} else {
				var cat struct {
					ID uint32 `json:"id"`
				}
				if json.Unmarshal(val, &cat) != nil {
					return nil, ErrCorrupt
				}
				id = cat.ID
			}
			names[id] = name
			schema[id] = op
		} else if len(key) == 6 && key[0] == reservedPrefix && key[1] == 'g' {
			id := binary.BigEndian.Uint32(key[2:])
			if c, ok := f.db.collectionByID(id); ok {
				names[id] = c.name
				if schema[id] != ChangeDrop {
					schema[id] = ChangeReset
				}
			}
		}
	}
	seen := make(map[string]int)
	metadataBytes := int64(0)
	for off := 0; off < len(raw); {
		begin := off
		plen := int(le.Uint32(raw[off:]))
		r := raw[off+8 : off+8+plen]
		kl := int(le.Uint16(r[1:]))
		key := r[3 : 3+kl]
		off += 8 + plen
		cid, uk, ok := splitKey(key)
		if !ok {
			continue
		}
		if _, skip := schema[cid]; skip {
			continue
		}
		name, known := names[cid]
		if !known {
			c, ok := f.db.collectionByID(cid)
			if !ok {
				return nil, ErrCorrupt
			}
			name = c.name
			names[cid] = name
		}
		op := ChangePut
		if r[0]&flagDelete != 0 {
			op = ChangeDelete
		}
		ref := changeRef{op: op, collection: name, key: uk, off: uint32(begin)}
		k := string(key)
		if idx, ok := seen[k]; ok {
			frame.refs[idx] = ref
		} else {
			metadataBytes += int64(len(k) + len(uk) + 96)
			if metadataBytes > f.cfg.Options.MaxCaptureBytes {
				return nil, ErrLimit
			}
			seen[k] = len(frame.refs)
			frame.refs = append(frame.refs, ref)
		}
	}
	ids := make([]int, 0, len(schema))
	for id := range schema {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		frame.refs = append(frame.refs, changeRef{op: schema[uint32(id)], collection: names[uint32(id)], off: math.MaxUint32})
	}
	// Names are dictionary encoded once per commit, rather than once per row.
	dict := []string{}
	ni := map[string]uint32{}
	for _, r := range frame.refs {
		if _, ok := ni[r.collection]; !ok {
			ni[r.collection] = uint32(len(dict))
			dict = append(dict, r.collection)
		}
	}
	p := make([]byte, changeHeaderBytes)
	copy(p, changeMagic)
	copy(p[8:24], f.id[:])
	copy(p[24:40], f.run[:])
	le.PutUint64(p[40:48], uint64(frame.stamp))
	le.PutUint64(p[48:56], uint64(len(raw)))
	le.PutUint32(p[56:60], uint32(len(frame.refs)))
	p = le.AppendUint32(p, uint32(len(dict)))
	for _, name := range dict {
		p = le.AppendUint16(p, uint16(len(name)))
		p = append(p, name...)
	}
	for _, r := range frame.refs {
		p = append(p, changeCode(r.op))
		p = le.AppendUint32(p, ni[r.collection])
		p = le.AppendUint32(p, r.off)
		p = le.AppendUint16(p, uint16(len(r.key)))
		p = append(p, r.key...)
	}
	if int64(len(p))+metadataBytes > f.cfg.Options.MaxCaptureBytes {
		return nil, ErrLimit
	}
	p = le.AppendUint32(p, uint32(len(p)+11+12))
	p = append(p, changeFooter...)
	enc := encodeRecord(nil, nil, p, false, false, f.db.wal.crcTab)
	enc[8] = flagFeed
	le.PutUint32(enc[4:], crc32.Checksum(enc[8:], f.db.wal.crcTab))
	frame.encoded = enc
	// Own key slices by decoding the marker rather than retaining the write batch.
	parsed, err := f.parseMarker(enc, lsn{1, uint64(len(raw) + walHeaderSize)})
	if err != nil {
		return nil, err
	}
	frame.refs = parsed.refs
	frame.charge = parsed.charge
	f.mu.Lock()
	active := false
	for _, ref := range frame.refs {
		if ref.op == ChangePut && (f.valueGlobal > 0 || f.valueCollections[ref.collection] > 0) {
			active = true
			break
		}
	}
	f.mu.Unlock()
	if active && int64(len(raw))+frame.charge <= f.cfg.Options.CacheBytes/2 {
		frame.raw = bytes.Clone(raw)
		frame.charge += int64(len(frame.raw))
	}
	return frame, nil
}
func (f *changeFeed) parseMarker(enc []byte, at lsn) (*changeFrame, error) {
	if len(enc) < 11+changeHeaderBytes+4+12 || enc[8] != flagFeed || le.Uint16(enc[9:]) != 0 || int(le.Uint32(enc))+8 != len(enc) || crc32.Checksum(enc[8:], f.db.wal.crcTab) != le.Uint32(enc[4:]) {
		return nil, ErrCorrupt
	}
	p := enc[11:]
	if string(p[:8]) != changeMagic || string(p[len(p)-8:]) != changeFooter || int(le.Uint32(p[len(p)-12:])) != len(enc) {
		return nil, ErrCorrupt
	}
	if !bytes.Equal(p[8:24], f.id[:]) {
		return nil, ErrResumeInvalid
	}
	rawLen := le.Uint64(p[48:56])
	if at.off < walHeaderSize || rawLen > at.off-uint64(walHeaderSize) {
		return nil, ErrCorrupt
	}
	frame := &changeFrame{start: lsn{at.gen, at.off - rawLen}, marker: at, end: lsn{at.gen, at.off + uint64(len(enc))}, stamp: int64(le.Uint64(p[40:48])), encoded: enc}
	var run [16]byte
	copy(run[:], p[24:40])
	frame.token = f.token(at, run)
	n := uint64(le.Uint32(p[56:60]))
	dictN := uint64(le.Uint32(p[60:64]))
	p = p[64 : len(p)-12]
	if n > uint64(len(p)/11) || dictN > uint64(len(p)/2) {
		return nil, ErrCorrupt
	}
	nameBytes := 0
	names := make([]string, int(dictN))
	for i := range names {
		if len(p) < 2 {
			return nil, ErrCorrupt
		}
		l := int(le.Uint16(p))
		p = p[2:]
		if l > len(p) || l > 255 {
			return nil, ErrCorrupt
		}
		names[i] = string(p[:l])
		nameBytes += l
		p = p[l:]
	}
	frame.refs = make([]changeRef, int(n))
	for i := range frame.refs {
		if len(p) < 11 {
			return nil, ErrCorrupt
		}
		op := changeOperation(p[0])
		idx := le.Uint32(p[1:5])
		off := le.Uint32(p[5:9])
		kl := int(le.Uint16(p[9:11]))
		p = p[11:]
		if op == "" || uint64(idx) >= dictN || kl > len(p) || kl > MaxKeySize {
			return nil, ErrCorrupt
		}
		if (op == ChangePut || op == ChangeDelete) && uint64(off) >= rawLen {
			return nil, ErrCorrupt
		}
		frame.refs[i] = changeRef{op: op, collection: names[idx], key: p[:kl], off: off}
		p = p[kl:]
	}
	if len(p) != 0 {
		return nil, ErrCorrupt
	}
	frame.charge = changeFrameCharge(len(enc), len(frame.refs)) + int64(nameBytes)
	return frame, nil
}
func (f *changeFeed) stage(frame *changeFrame, end lsn) {
	frame.end = end
	frame.marker = lsn{end.gen, end.off - uint64(len(frame.encoded))}
	frame.start = lsn{end.gen, 0}
	// encoded[11+48:] is the raw group length.
	frame.start.off = frame.marker.off - le.Uint64(frame.encoded[11+48:])
	frame.token = f.token(frame.marker, f.run)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = end
	f.latest = frame.token
	f.latestStamp = frame.stamp
	if frame.charge <= f.cfg.Options.CacheBytes {
		f.frames = append(f.frames, frame)
		f.bytes += frame.charge
		for f.bytes > f.cfg.Options.CacheBytes && f.head < len(f.frames) {
			old := f.frames[f.head]
			f.bytes -= old.charge
			f.frames[f.head] = nil
			f.head++
		}
		if f.head > 1024 && f.head*2 >= len(f.frames) {
			copy(f.frames, f.frames[f.head:])
			f.frames = f.frames[:len(f.frames)-f.head]
			f.head = 0
		}
	} else {
		for i := range f.frames {
			f.frames[i] = nil
		}
		f.frames = nil
		f.head = 0
		f.bytes = 0
	}
	if !f.db.opts.Sync {
		f.ready = end
		f.wakeLocked()
	}
}
func (f *changeFeed) wakeLocked() {
	if f.watchers > 0 {
		close(f.notify)
		f.notify = make(chan struct{})
	}
}
func (f *changeFeed) syncReady(l lsn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lsnLess(f.ready, l) {
		if lsnLess(f.applied, l) {
			l = f.applied
		}
		f.ready = l
		f.wakeLocked()
	}
}
func (f *changeFeed) stop(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.stopped {
		f.stopped = true
		f.failure = err
		f.halt.Store(true)
		close(f.notify)
	}
}
func (f *changeFeed) recoverBoundary() error {
	gens, err := walGens(f.db.dir)
	if err != nil {
		return err
	}
	for i := len(gens) - 1; i >= 0; i-- {
		g := gens[i]
		size, err := f.db.wal.changeSize(g)
		if err != nil {
			return err
		}
		if size <= walHeaderSize+12 {
			continue
		}
		var footer [12]byte
		if err = f.db.wal.readChange(g, size-12, footer[:]); err != nil {
			return err
		}
		if string(footer[4:]) != changeFooter {
			continue
		}
		length := uint64(le.Uint32(footer[:4]))
		if length > size-walHeaderSize || length < 11+changeHeaderBytes+16 {
			return ErrCorrupt
		}
		frame, err := f.readMarkerSummary(lsn{g, size - length})
		if errors.Is(err, ErrResumeInvalid) {
			continue
		}
		if err != nil {
			return err
		}
		f.applied = frame.end
		f.ready = frame.end
		f.latest = frame.token
		f.latestStamp = frame.stamp
		return nil
	}
	return nil
}

// ChangeFeedStats reports shared cache usage, not total process RSS.
type ChangeFeedStats struct {
	Enabled         bool
	Watchers        int
	CacheBytes      int64
	ReaderBytes     int64
	PeakReaderBytes int64
	PlanBytes       int64
	FilterGroups    int
	RetentionBytes  int64
	RetentionAge    time.Duration
}

func (db *DB) ChangeFeedStats() ChangeFeedStats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.feed == nil {
		return ChangeFeedStats{}
	}
	f := db.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	return ChangeFeedStats{true, f.watchers, f.bytes, f.readerBytes, f.peakReaderBytes, f.planBytes, len(f.plans), f.cfg.Options.RetentionBytes, f.cfg.Options.RetentionAge}
}

// Collection.Watch is a convenience scoped to the collection's name. A drop
// is explicit; a later recreation with that name emits a new Reset.
func (c *Collection) Watch(ctx context.Context, o WatchOptions) (*ChangeCursor, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if c.snap != nil {
		return nil, ErrReadOnly
	}
	if len(o.Collections) != 0 {
		return nil, ErrOptions
	}
	o.Collections = []string{c.name}
	return c.db.Watch(ctx, o)
}

func changeFrameCharge(n, refs int) int64 {
	extra := 0
	if refs <= 1024 {
		extra = 8192
	}
	return int64(n + refs*64 + 256 + extra)
}
func (f *changeFeed) reserveReader(n int64) error {
	if n <= 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if n > f.cfg.Options.MaxWatchBytes-f.readerBytes-f.planBytes {
		return ErrLimit
	}
	f.readerBytes += n
	f.peakReaderBytes = max(f.peakReaderBytes, f.readerBytes)
	return nil
}
func (f *changeFeed) releaseReader(n int64) {
	if n <= 0 {
		return
	}
	f.mu.Lock()
	f.readerBytes -= n
	f.mu.Unlock()
}

func resetCopiedFeed(v []byte) ([]byte, error) {
	var cfg changeConfig
	if json.Unmarshal(v, &cfg) != nil || cfg.Version != 1 {
		return nil, ErrCorrupt
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	cfg.ID = hex.EncodeToString(id[:])
	cfg.StartGen = 1
	cfg.StartOff = walHeaderSize
	return json.Marshal(cfg)
}

// Joining after long inactivity must still permit a fresh snapshot handoff.
// This empty commit renews the boundary; it does not change rows or revisions.
func (db *DB) ensureChangeBoundary(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	if db.failed != nil {
		err := db.failed
		db.mu.Unlock()
		return err
	}
	f := db.feed
	if f == nil {
		db.mu.Unlock()
		return ErrChangeFeedDisabled
	}
	f.mu.Lock()
	stamp := f.latestStamp
	f.mu.Unlock()
	if stamp != 0 && time.Since(time.Unix(0, stamp)) < f.cfg.Options.RetentionAge {
		db.mu.Unlock()
		return nil
	}
	frame, err := f.capture(nil)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.noteActiveStartLocked()
	w := db.wal
	l, err := w.appendFeed(nil, frame.encoded)
	if err == nil {
		f.stage(frame, l)
	} else {
		db.failed = err
		f.stop(err)
	}
	db.mu.Unlock()
	if err == nil && db.opts.Sync {
		err = db.syncWrite(w, l)
	}
	return err
}

// The common one-key path needs neither a batch allocation nor hash tables.
func (f *changeFeed) captureOne(c *Collection, uk, key, val []byte, del bool, raw []byte) (*changeFrame, error) {
	op := ChangePut
	if del {
		op = ChangeDelete
	}
	n := 11 + changeHeaderBytes + 4 + 2 + len(c.name) + 11 + len(uk) + 12
	if int64(n+256) > f.cfg.Options.MaxCaptureBytes {
		return nil, ErrLimit
	}
	frame := &changeFrame{stamp: time.Now().UnixNano()}
	enc := make([]byte, n)
	p := enc[11:]
	copy(p, changeMagic)
	copy(p[8:24], f.id[:])
	copy(p[24:40], f.run[:])
	le.PutUint64(p[40:48], uint64(frame.stamp))
	le.PutUint64(p[48:56], uint64(11+len(key)+len(val)))
	le.PutUint32(p[56:60], 1)
	le.PutUint32(p[60:64], 1)
	off := 64
	le.PutUint16(p[off:], uint16(len(c.name)))
	off += 2
	copy(p[off:], c.name)
	off += len(c.name)
	p[off] = changeCode(op)
	off++
	le.PutUint32(p[off:], 0)
	off += 4
	le.PutUint32(p[off:], 0)
	off += 4
	le.PutUint16(p[off:], uint16(len(uk)))
	off += 2
	copy(p[off:], uk)
	frame.one[0] = changeRef{op: op, collection: c.name, key: p[off : off+len(uk)], off: 0}
	off += len(uk)
	le.PutUint32(p[off:], uint32(n))
	copy(p[off+4:], changeFooter)
	le.PutUint32(enc[:4], uint32(n-8))
	enc[8] = flagFeed
	le.PutUint32(enc[4:8], crc32.Checksum(enc[8:], f.db.wal.crcTab))
	frame.encoded = enc
	frame.refs = frame.one[:]
	frame.charge = changeFrameCharge(n, 1)
	f.mu.Lock()
	active := !del && (f.valueGlobal > 0 || f.valueCollections[c.name] > 0)
	f.mu.Unlock()
	rawLen := 11 + len(key) + len(val)
	if active && int64(rawLen)+frame.charge <= f.cfg.Options.CacheBytes/2 {
		if raw != nil {
			frame.raw = bytes.Clone(raw)
		} else {
			frame.raw = encodeRecord(nil, key, val, false, del, f.db.wal.crcTab)
		}
		frame.charge += int64(len(frame.raw))
	}
	return frame, nil
}
