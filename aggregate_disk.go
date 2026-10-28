package db

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type aggregateRun struct {
	store                   *aggregateStore
	first, last, lastLength int64
	path                    string
	size                    int64
	order                   uint64
	maxRecord               int64
	level                   int
}
type aggregateDisk struct {
	header     []byte
	stateLimit int64
	lease      *os.File
	leasePath  string
	ctx        context.Context
	options    AggregateOptions
	stats      *AggregateStats
	budget     *memoryBudget
	dir        string
	used       int64
	next       uint64
}

func (d *aggregateDisk) close() error {
	var err error
	if d.dir != "" {
		err = os.RemoveAll(d.dir)
	}
	if d.lease != nil {
		if err == nil {
			err = os.Remove(d.leasePath)
		}
		if e := d.lease.Close(); err == nil {
			err = e
		}
		d.lease = nil
	}
	return err
}
func (d *aggregateDisk) init() error {
	root := d.options.TempDir
	if root == "" {
		root = os.TempDir()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	lease, err := os.CreateTemp(root, ".aptevadb-lease-")
	if err != nil {
		return err
	}
	old := lease.Name()
	published := false
	defer func() {
		if !published {
			lease.Close()
			os.Remove(old)
		}
	}()
	if err := syscall.Flock(int(lease.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	path := filepath.Join(root, "aptevadb-aggregate-"+strings.TrimPrefix(filepath.Base(old), ".aptevadb-lease-")+".lock")
	if err := os.Rename(old, path); err != nil {
		return err
	}
	dir := path + ".d"
	if err := os.Mkdir(dir, 0700); err != nil {
		os.Remove(path)
		return err
	}
	d.dir, d.lease, d.leasePath = dir, lease, path
	published = true
	return nil
}

// CleanupAggregationTemp removes only abandoned, published execution leases.
// Active executions hold an exclusive OS lock and are never removed. Default
// per-database scratch is cleaned on successful Open; custom roots can use this
// function after a process crash. It does not inspect arbitrary temporary files.
func CleanupAggregationTemp(ctx context.Context, root string) (int, error) {
	ctx = orBackground(ctx)
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "aptevadb-aggregate-") || !strings.HasSuffix(name, ".lock") || !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(root, name)
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return removed, err
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			f.Close()
			continue
		}
		if err != nil {
			f.Close()
			return removed, err
		}
		err = os.RemoveAll(path + ".d")
		if err == nil {
			err = os.Remove(path)
		}
		f.Close()
		if err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}
func (d *aggregateDisk) create() (*aggregateRun, *os.File, error) {
	if err := d.ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !d.options.AllowDiskUse {
		return nil, nil, ErrLimit
	}
	if d.dir == "" {
		if err := d.init(); err != nil {
			return nil, nil, err
		}
	}
	if err := d.budget.reserve(256); err != nil {
		return nil, nil, err
	}
	f, err := os.CreateTemp(d.dir, "run-")
	if err != nil {
		return nil, nil, err
	}
	r := &aggregateRun{path: f.Name(), order: d.next}
	d.next++
	d.stats.SpillRuns++
	return r, f, nil
}
func (d *aggregateDisk) remove(r *aggregateRun) error {
	if r.store != nil {
		d.budget.reserve(-256)
		r.store.refs--
		if r.store.refs == 0 {
			return d.remove(r.store.run)
		}
		return nil
	}
	if err := os.Remove(r.path); err != nil {
		return err
	}
	d.used -= r.size
	d.budget.reserve(-256)
	return nil
}
func (d *aggregateDisk) writeRecord(w io.Writer, r *aggregateRun, data []byte) error {
	if err := d.ctx.Err(); err != nil {
		return err
	}
	if uint64(len(data)) > math.MaxUint32 {
		return ErrLimit
	}
	if err := d.accountRecord(r, int64(len(data)+8)); err != nil {
		return err
	}
	d.header = le.AppendUint32(d.header[:0], uint32(len(data)))
	d.header = le.AppendUint32(d.header, uint32(crc32.ChecksumIEEE(data)))
	if err := writeFull(w, d.header); err != nil {
		return err
	}
	return writeFull(w, data)
}
func (d *aggregateDisk) accountRecord(r *aggregateRun, n int64) error {
	quota := d.options.MaxDiskBytes
	if quota == 0 {
		quota = 1 << 30
	}
	if n > quota-d.used {
		return ErrDiskLimit
	}
	d.used += n
	r.size += n
	if r.store != nil {
		r.store.run.size += n
	}
	d.stats.SpilledBytes += n
	d.stats.PeakDiskBytes = max(d.stats.PeakDiskBytes, d.used)
	return nil
}
func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// Internal temporary records preserve typed payloads, both signed-128 limbs,
// floating bits, counts and flags. No JSON or finalized numeric values are used.
type aggregateDecoder struct {
	b   []byte
	err error
}

func (r *aggregateDecoder) take(n int) []byte {
	if n < 0 || n > len(r.b) {
		r.err = ErrCorrupt
		return make([]byte, max(0, min(n, 24)))
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}
func (r *aggregateDecoder) u32() int {
	if len(r.b) < 4 {
		r.err = ErrCorrupt
		return 0
	}
	return int(le.Uint32(r.take(4)))
}
func (r *aggregateDecoder) u64() uint64 {
	if len(r.b) < 8 {
		r.err = ErrCorrupt
		return 0
	}
	return le.Uint64(r.take(8))
}
func groupStorage(g *groupState) int64 {
	n := int64(128 + len(g.key) + len(g.id)*64 + len(g.accs)*80)
	for _, s := range g.id {
		n += int64(len(s.s))
	}
	for _, s := range g.accs {
		n += int64(len(s.ext.s))
	}
	return n
}

type aggregateRead struct {
	header      [8]byte
	state       groupState
	stateSpec   *groupSpec
	stateCost   int64
	payload     []byte
	payloadCost int64
	buffer      int64
	f           *os.File
	in          *bufio.Reader
	run         *aggregateRun
	data        []byte
	cost        int64
	group       *groupState
	row         map[string]any
	seq         uint64
	d           *aggregateDisk
}

func (d *aggregateDisk) bufferBytes() int {
	return int(min(int64(64<<10), max(int64(512), d.budget.max/64)))
}
func (d *aggregateDisk) reader(run *aggregateRun) (*aggregateRead, error) {
	buffer := int64(d.bufferBytes())
	if err := d.budget.reserve(buffer); err != nil {
		return nil, err
	}
	f, err := os.Open(run.path)
	if err != nil {
		d.budget.reserve(-buffer)
		return nil, err
	}
	var input io.Reader = f
	if run.store != nil {
		input = &aggregateSegmentReader{f: f, next: run.first, left: run.size, physical: run.store.run.size}
	}
	return &aggregateRead{f: f, in: bufio.NewReaderSize(input, int(buffer)), run: run, d: d, buffer: buffer}, nil
}
func (r *aggregateRead) close() {
	r.d.budget.reserve(-r.cost - r.buffer - r.stateCost - r.payloadCost)
	r.cost = 0
	_ = r.f.Close()
}

func (r *aggregateRead) next(groups bool) error {
	if err := r.d.ctx.Err(); err != nil {
		return err
	}
	r.d.budget.reserve(-r.cost)
	r.cost = 0
	r.data = nil
	r.group = nil
	r.row = nil
	head := r.header[:]
	_, err := io.ReadFull(r.in, head)
	if err != nil {
		return err
	}
	n := int64(le.Uint32(head[:]))
	if n <= 0 || n > r.d.budget.max || n > r.run.size {
		return ErrCorrupt
	}
	cost := n*4 + 256
	if r.stateSpec != nil {
		cost = 0
	}
	if err := r.d.budget.reserve(cost); err != nil {
		return err
	}
	r.cost = cost
	borrowed := n <= int64(r.in.Size())
	if borrowed {
		r.data, err = r.in.Peek(int(n))
	} else {
		if r.stateSpec != nil {
			if n > r.payloadCost {
				if err := r.d.budget.reserve(n - r.payloadCost); err != nil {
					return err
				}
				r.payloadCost = n
				r.payload = make([]byte, int(n))
			}
			r.data = r.payload[:int(n)]
		} else {
			r.data = make([]byte, int(n))
		}
		_, err = io.ReadFull(r.in, r.data)
	}
	if err != nil {
		return err
	}
	if crc32.ChecksumIEEE(r.data) != le.Uint32(head[4:]) {
		return ErrCorrupt
	}
	if groups {
		if r.stateSpec != nil {
			err = decodeGroupRecord(r.data, r.stateSpec, &r.state)
			r.group = &r.state
		} else {
			return ErrCorrupt
		}
	} else {
		if n < 8 {
			return ErrCorrupt
		}
		r.seq = le.Uint64(r.data)
		r.row, err = DecodeDocument(r.data[8:])
	}
	if borrowed {
		if _, e := r.in.Discard(int(n)); err == nil {
			err = e
		}
	}
	return err
}

type aggregateHeap struct {
	readers []*aggregateRead
	keys    []sortKey
}

func (h aggregateHeap) Len() int { return len(h.readers) }
func (h aggregateHeap) Less(i, j int) bool {
	a, b := h.readers[i], h.readers[j]

	for _, k := range h.keys {
		c := compareValues(rowField(a.row, k.field), rowField(b.row, k.field))
		if c != 0 {
			if k.desc {
				return c > 0
			}
			return c < 0
		}
	}
	return a.seq < b.seq
}
func (h aggregateHeap) Swap(i, j int) { h.readers[i], h.readers[j] = h.readers[j], h.readers[i] }
func (h *aggregateHeap) Push(v any)   { h.readers = append(h.readers, v.(*aggregateRead)) }
func (h *aggregateHeap) Pop() any {
	n := len(h.readers) - 1
	r := h.readers[n]
	h.readers = h.readers[:n]
	return r
}

// Consolidate consecutive runs with bounded fan-in. Each generation covers a
// contiguous source interval, so first/last and stable sort ties survive merges.
func (d *aggregateDisk) consolidate(runs []*aggregateRun, keys []sortKey) ([]*aggregateRun, error) {
	for len(runs) > 1 {
		available := d.budget.max - d.budget.used.Load()
		maxCost := int64(1)
		for _, r := range runs {
			maxCost = max(maxCost, r.maxRecord)
		}
		fan := min(32, int(available/(maxCost*4+int64(d.bufferBytes())+512)))
		if fan < 2 {
			return nil, fmt.Errorf("%w: merge needs two bounded records", ErrLimit)
		}
		if len(runs) <= fan {
			return runs, nil
		}
		d.stats.MergePasses++
		var next []*aggregateRun
		for lo := 0; lo < len(runs); lo += fan {
			batch := runs[lo:min(lo+fan, len(runs))]
			if len(batch) == 1 {
				next = append(next, batch[0])
				continue
			}
			out, err := d.mergeBatch(batch, keys)
			if err != nil {
				return nil, err
			}
			next = append(next, out)
		}
		runs = next
	}
	return runs, nil
}
func (d *aggregateDisk) mergeRows(runs []*aggregateRun, keys []sortKey, emit func(map[string]any, uint64) error) error {
	h := &aggregateHeap{keys: keys}
	var readers []*aggregateRead
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()
	for _, run := range runs {
		r, err := d.reader(run)
		if err != nil {
			return err
		}
		readers = append(readers, r)
		err = r.next(false)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		heap.Push(h, r)
	}
	for h.Len() > 0 {
		r := heap.Pop(h).(*aggregateRead)
		if err := emit(r.row, r.seq); err != nil {
			return err
		}
		err := r.next(false)
		if err == nil {
			heap.Push(h, r)
		} else if err != io.EOF {
			return err
		}
	}
	return nil
}

func (x *aggregateExec) sortRows(up rowSource, keys []sortKey, emit func(map[string]any) error) error {
	type item struct {
		row map[string]any
		seq uint64
	}
	var rows []item
	var runs []*aggregateRun
	var charge int64
	var seq uint64
	defer func() { x.budget.reserve(-charge) }()
	less := func(i, j int) bool {
		for _, k := range keys {
			c := compareValues(rowField(rows[i].row, k.field), rowField(rows[j].row, k.field))
			if c != 0 {
				if k.desc {
					return c > 0
				}
				return c < 0
			}
		}
		return rows[i].seq < rows[j].seq
	}
	flush := func() error {
		sort.Slice(rows, less)
		run, f, err := x.disk.create()
		if err != nil {
			return err
		}
		w := bufio.NewWriterSize(f, 512)
		if err = x.budget.reserve(512); err != nil {
			f.Close()
			return err
		}
		for _, item := range rows {
			var b DocBuilder
			err = encodeMap(&b, item.row, 0)
			if err != nil {
				break
			}
			data := le.AppendUint64(nil, item.seq)
			data = append(data, b.Bytes()...)
			cost := int64(len(data))*2 + 256
			if err = x.budget.reserve(cost); err != nil {
				break
			}
			err = x.disk.writeRecord(w, run, data)
			x.budget.reserve(-cost)
			run.maxRecord = max(run.maxRecord, cost)
			if err != nil {
				break
			}
		}
		if err == nil {
			err = w.Flush()
		}
		if e := f.Close(); err == nil {
			err = e
		}
		x.budget.reserve(-512)
		if err != nil {
			return err
		}
		runs = append(runs, run)
		rows = nil
		x.budget.reserve(-charge)
		charge = 0
		var err2 error
		runs, err2 = x.disk.compactRuns(runs, keys)
		return err2
	}
	err := up(func(r map[string]any) error {
		cost := valueMemory(r) + 48
		limit := x.budget.max
		if x.options.AllowDiskUse {
			limit = x.stateLimit
		}
		headroom := int64(0)
		if x.options.AllowDiskUse {
			headroom = max(int64(4096), 4*cost)
		}
		if cost+headroom > x.budget.max-x.budget.used.Load() || charge+cost > limit {
			if !x.options.AllowDiskUse || len(rows) == 0 {
				return ErrLimit
			}
			if err := flush(); err != nil {
				return err
			}
		}
		if err := x.budget.reserve(cost); err != nil {
			return err
		}
		charge += cost
		rows = append(rows, item{r, seq})
		seq++
		return nil
	})
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		sort.Slice(rows, less)
		for i := range rows {
			if err := x.ctx.Err(); err != nil {
				return err
			}
			r := rows[i].row
			if err := emit(r); err != nil {
				return err
			}
			cost := valueMemory(r) + 48
			x.budget.reserve(-cost)
			charge -= cost
			rows[i].row = nil
		}
		return nil
	}

	if len(rows) > 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	runs, err = x.disk.consolidate(runs, keys)
	if err != nil {
		return err
	}
	x.stats.MergePasses++
	return x.disk.mergeRows(runs, keys, func(r map[string]any, _ uint64) error { return emit(r) })
}

func (d *aggregateDisk) mergeBatch(batch []*aggregateRun, keys []sortKey) (*aggregateRun, error) {
	out, f, err := d.create()
	if err != nil {
		return nil, err
	}
	out.order = batch[0].order
	w := bufio.NewWriterSize(f, 512)
	err = d.budget.reserve(512)
	if err == nil {
		err = d.mergeRows(batch, keys, func(row map[string]any, seq uint64) error {
			var builder DocBuilder
			if err := encodeMap(&builder, row, 0); err != nil {
				return err
			}
			data := le.AppendUint64(nil, seq)
			data = append(data, builder.Bytes()...)
			cost := int64(len(data))*2 + 256
			if e := d.budget.reserve(cost); e != nil {
				return e
			}
			e := d.writeRecord(w, out, data)
			d.budget.reserve(-cost)
			out.maxRecord = max(out.maxRecord, cost)
			return e
		})
		d.budget.reserve(-512)
	}
	if err == nil {
		err = w.Flush()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return nil, err
	}
	for _, r := range batch {
		if err = d.remove(r); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Online leveled merging bounds path/file metadata logarithmically as well as
// open descriptors. Adjacent source intervals are never permuted.
func (d *aggregateDisk) compactRuns(runs []*aggregateRun, keys []sortKey) ([]*aggregateRun, error) {
	for len(runs) > 1 {
		end := len(runs)
		level := runs[end-1].level
		start := end - 1
		for start > 0 && runs[start-1].level == level {
			start--
		}
		maxCost := int64(1)
		for _, r := range runs[start:] {
			maxCost = max(maxCost, r.maxRecord)
		}
		fan := min(8, int((d.budget.max-d.budget.used.Load())/(maxCost*4+int64(d.bufferBytes())+512)))
		if fan < 2 {
			return nil, fmt.Errorf("%w: merge workspace cannot fit two records", ErrLimit)
		}
		if end-start < fan {
			return runs, nil
		}
		batch := runs[end-fan:]
		out, err := d.mergeBatch(batch, keys)
		if err != nil {
			return nil, err
		}
		out.level = level + 1
		d.stats.MergePasses++
		clear(runs[end-fan:])
		runs = append(runs[:end-fan], out)
	}
	return runs, nil
}
