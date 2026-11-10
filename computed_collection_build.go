package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync/atomic"
)

const computedProgressTag = 'B'

var ErrComputedCollectionBuilding = errors.New("aptevadb: computed collection is building")

// ComputedCollectionInput contributes a grouped pipeline from an ordinary
// collection. All pipelines must produce the same named group dimensions.
// Measures with the same output name must use the same reducer.
type ComputedCollectionInput struct {
	Source   string          `json:"source"`
	Pipeline json.RawMessage `json:"pipeline"`
}

type computedInputCatalog struct {
	Source   uint32          `json:"source"`
	Pipeline json.RawMessage `json:"pipeline"`
}

type computedProgress struct {
	After []byte `json:"after,omitempty"`
	Done  bool   `json:"done,omitempty"`
	Rows  uint64 `json:"rows,omitempty"`
}

type computedBuild struct {
	inputs   []*computedCollectionPlan
	measures []accSpec
	sumSlots []int
	ready    atomic.Bool
}

// ComputedBuildStatus reports committed scan progress for each input.
type ComputedBuildStatus struct {
	Ready   bool              `json:"ready"`
	Scanned map[string]uint64 `json:"scanned"`
}

func computedProgressKey(id, source uint32) []byte {
	return binary.BigEndian.AppendUint32(computedViewKey(computedProgressTag, id, nil), source)
}

func (db *DB) prepareComputedBuild(target *Collection, specs []computedInputCatalog) (*computedBuild, error) {
	if len(specs) < 1 || len(specs) > 16 {
		return nil, ErrComputedCollection
	}
	build := &computedBuild{}
	sources := map[uint32]bool{}
	byName := map[string]accSpec{}
	for _, spec := range specs {
		source, ok := db.collectionByID(spec.Source)
		if !ok || target == source || sources[spec.Source] || source.state.derived != nil {
			return nil, ErrComputedCollectionDependency
		}
		if len(source.state.dependents) >= 64 {
			return nil, ErrComputedCollectionDependency
		}
		sources[spec.Source] = true
		p, err := compileComputedPlan(spec.Pipeline, true)
		if err != nil {
			return nil, err
		}
		if len(build.inputs) != 0 && !slices.Equal(build.inputs[0].group.idNames, p.group.idNames) {
			return nil, fmt.Errorf("%w: input group names differ", ErrComputedCollection)
		}
		for _, a := range p.group.accs {
			if old, ok := byName[a.name]; ok && old.op != a.op {
				return nil, fmt.Errorf("%w: input reducers differ for %q", ErrComputedCollection, a.name)
			}
			byName[a.name] = accSpec{name: a.name, op: a.op}
		}
		p.source, p.target, p.build = source, target, build
		build.inputs = append(build.inputs, p)
	}
	if len(byName) > 64 {
		return nil, ErrComputedCollection
	}
	for _, a := range byName {
		build.measures = append(build.measures, a)
	}
	sort.Slice(build.measures, func(i, j int) bool { return build.measures[i].name < build.measures[j].name })
	for i, a := range build.measures {
		if a.op == accSum {
			build.sumSlots = append(build.sumSlots, i)
		}
	}
	for _, p := range build.inputs {
		for _, a := range p.group.accs {
			slot := slices.IndexFunc(build.measures, func(out accSpec) bool { return a.name == out.name })
			p.slots = append(p.slots, slot)
		}
	}
	return build, nil
}

func (db *DB) registerComputedBuild(build *computedBuild) {
	for _, p := range build.inputs {
		p.source.state.dependents = append(p.source.state.dependents, p)
	}
	build.inputs[0].target.state.derived = build.inputs[0]
	db.derivedCount++
}

func (c *Collection) computedBuilding() bool {
	p := c.state.derived
	return p != nil && p.build != nil && !p.build.ready.Load()
}

func (build *computedBuild) status() ComputedBuildStatus {
	status := ComputedBuildStatus{Ready: build.ready.Load(), Scanned: map[string]uint64{}}
	for _, p := range build.inputs {
		status.Scanned[p.source.name] = p.progress.Rows
	}
	return status
}

// BeginComputedCollection persists an online build. An identical request can
// reopen a build or ready view. Call AdvanceComputedCollection to do bounded
// work. Until it completes, the target is absent from read snapshots.
func (db *DB) BeginComputedCollection(ctx context.Context, name string, inputs []ComputedCollectionInput) (ComputedBuildStatus, error) {
	ctx = orBackground(ctx)
	if name == "" || !validCollectionName(name) || len(inputs) == 0 || len(inputs) > 16 {
		return ComputedBuildStatus{}, ErrComputedCollection
	}
	db.colls.create.Lock()
	defer db.colls.create.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.maintenanceReadyLocked(ctx); err != nil {
		return ComputedBuildStatus{}, err
	}
	specs := make([]computedInputCatalog, 0, len(inputs))
	for _, input := range inputs {
		source, ok := db.lookupCollection(input.Source)
		if !ok {
			return ComputedBuildStatus{}, ErrNoSuchCollection
		}
		plan, err := compileComputedPlan(input.Pipeline, true)
		if err != nil {
			return ComputedBuildStatus{}, err
		}
		specs = append(specs, computedInputCatalog{source.id, plan.pipeline})
	}
	if target, exists := db.lookupCollection(name); exists {
		p := target.state.derived
		if p == nil || p.build == nil || len(p.build.inputs) != len(specs) {
			return ComputedBuildStatus{}, ErrComputedCollectionExists
		}
		for i, spec := range specs {
			input := p.build.inputs[i]
			if input.source.id != spec.Source || !bytes.Equal(input.pipeline, spec.Pipeline) {
				return ComputedBuildStatus{}, ErrComputedCollectionExists
			}
		}
		return p.build.status(), nil
	}
	id := db.colls.nextID
	if id >= maxCollections {
		return ComputedBuildStatus{}, ErrTooManyCollections
	}
	target := newCollection(db, name, id)
	build, err := db.prepareComputedBuild(target, specs)
	if err != nil {
		return ComputedBuildStatus{}, err
	}
	meta, _ := json.Marshal(struct {
		ID uint32 `json:"id"`
	}{id})
	catalog, _ := json.Marshal(computedCollectionCatalog{Version: 2, Inputs: specs})
	batch := db.NewBatch()
	batch.internal = true
	if err = batch.add(collCatalogKey(name), meta, false); err != nil {
		return ComputedBuildStatus{}, err
	}
	if err = batch.add(computedViewKey('m', id, nil), catalog, false); err != nil {
		return ComputedBuildStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return ComputedBuildStatus{}, err
	}
	wal := db.wal
	position, err := batch.commitLocked()
	if err != nil {
		db.failed = err
		return ComputedBuildStatus{}, err
	}
	db.registerComputedBuild(build)
	db.colls.mu.Lock()
	db.colls.byName[name], db.colls.byID[id], db.colls.nextID = target, target, id+1
	db.colls.mu.Unlock()
	return build.status(), db.completeComputedChunkLocked(wal, position)
}

// completeComputedChunkLocked temporarily releases the writer lock for a
// durability wait. The caller holds it again on return.
func (db *DB) completeComputedChunkLocked(wal *wal, position lsn) error {
	need := db.needCheckpoint()
	db.mu.Unlock()
	var err error
	if db.opts.Sync {
		err = db.syncWrite(wal, position)
	}
	if need {
		db.requestCheckpoint()
	}
	db.mu.Lock()
	return err
}

// ComputedCollectionBuild returns the progress of an online view.
func (db *DB) ComputedCollectionBuild(name string) (ComputedBuildStatus, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ComputedBuildStatus{}, ErrClosed
	}
	c, ok := db.lookupCollection(name)
	if !ok {
		return ComputedBuildStatus{}, ErrNoSuchCollection
	}
	if c.state.derived == nil || c.state.derived.build == nil {
		return ComputedBuildStatus{}, ErrComputedCollection
	}
	return c.state.derived.build.status(), nil
}

// AdvanceComputedCollection advances one source by at most maxRows, releasing
// the writer lock between calls. Cursor and contributions commit together.
// Writes behind the cursor maintain the scanned prefix, including inserts
// before the cursor. Writes ahead of it will be observed on the later scan.
func (db *DB) AdvanceComputedCollection(ctx context.Context, name string, maxRows int) (ComputedBuildStatus, error) {
	ctx = orBackground(ctx)
	if maxRows < 1 || maxRows > 1024 {
		return ComputedBuildStatus{}, ErrComputedCollection
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.maintenanceReadyLocked(ctx); err != nil {
		return ComputedBuildStatus{}, err
	}
	target, ok := db.lookupCollection(name)
	if !ok {
		return ComputedBuildStatus{}, ErrNoSuchCollection
	}
	if target.state.derived == nil || target.state.derived.build == nil {
		return ComputedBuildStatus{}, ErrComputedCollection
	}
	build := target.state.derived.build
	if build.ready.Load() {
		return build.status(), nil
	}
	var plan *computedCollectionPlan
	for _, p := range build.inputs {
		if !p.progress.Done {
			plan = p
			break
		}
	}
	batch := db.NewBatch()
	batch.internal = true
	delta := newComputedCollectionDelta(db, batch)
	progress := plan.progress
	start := plan.source.pfx
	if len(progress.After) > 0 {
		start = append(plan.source.ikey(nil, progress.After), 0)
	}
	rows, bytesSeen := 0, 0
	pause := errors.New("chunk complete")
	err := db.computedScanLocked(ctx, start, plan.source.end, func(key, doc []byte) error {
		if rows >= maxRows || bytesSeen >= 4<<20 {
			return pause
		}
		if err := validateComputedDoc(doc, 0); err != nil {
			return fmt.Errorf("%w: %w", ErrComputedCollection, err)
		}
		_, uk, _ := splitKey(key)
		if err := delta.change(plan, uk, nil, doc); err != nil {
			return err
		}
		progress.After = append(progress.After[:0:0], uk...)
		progress.Rows++
		rows++
		bytesSeen += len(key) + len(doc)
		return nil
	})
	if err != nil && err != pause {
		return ComputedBuildStatus{}, err
	}
	progress.Done = err == nil
	if err = delta.finish(ctx); err != nil {
		return ComputedBuildStatus{}, err
	}
	data, _ := json.Marshal(progress)
	if err = batch.add(computedProgressKey(target.id, plan.source.id), data, false); err != nil {
		return ComputedBuildStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return ComputedBuildStatus{}, err
	}
	wal := db.wal
	position, err := batch.commitLocked()
	if err != nil {
		db.failed = err
		return ComputedBuildStatus{}, err
	}
	plan.progress = progress
	ready := true
	for _, p := range build.inputs {
		ready = ready && p.progress.Done
	}
	build.ready.Store(ready)
	return build.status(), db.completeComputedChunkLocked(wal, position)
}
