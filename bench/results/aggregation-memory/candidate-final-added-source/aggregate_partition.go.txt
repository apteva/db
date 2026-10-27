package db

import (
	"bufio"
	"fmt"
	"hash/maphash"
	"io"
	"os"
)

// A spill partition contains chronological partial states, not finalized rows.
// This avoids sorting every flushed hash table. Oversized partitions are
// repartitioned with a fresh keyed hash; hot groups consume only one state.
type aggregatePartitions struct {
	d       *aggregateDisk
	seed    maphash.Seed
	files   []*os.File
	writers []*bufio.Writer
	runs    []*aggregateRun
	buffer  int64
}

func (d *aggregateDisk) partitions() *aggregatePartitions {
	n := min(16, max(2, int(d.stateLimit/(32<<10))))
	buffer := min(int64(16<<10), max(int64(512), d.stateLimit/int64(n*8)))
	return &aggregatePartitions{d: d, seed: maphash.MakeSeed(), files: make([]*os.File, n), writers: make([]*bufio.Writer, n), runs: make([]*aggregateRun, n), buffer: buffer}
}
func (p *aggregatePartitions) add(g *groupState) error {
	slot := int(maphash.String(p.seed, g.key) % uint64(len(p.files)))
	if p.files[slot] == nil {
		r, f, err := p.d.create()
		if err != nil {
			return err
		}
		if err = p.d.budget.reserve(p.buffer); err != nil {
			f.Close()
			return err
		}
		p.files[slot], p.runs[slot], p.writers[slot] = f, r, bufio.NewWriterSize(f, int(p.buffer))
	}
	cost := groupStorage(g) * 2
	if err := p.d.budget.reserve(cost); err != nil {
		return err
	}
	data := groupRecord(g)
	err := p.d.writeRecord(p.writers[slot], p.runs[slot], data)
	p.d.budget.reserve(-cost)
	p.runs[slot].maxRecord = max(p.runs[slot].maxRecord, cost)
	return err
}
func (p *aggregatePartitions) abort() {
	for i, f := range p.files {
		if f != nil {
			f.Close()
			p.files[i] = nil
			p.d.budget.reserve(-p.buffer)
		}
	}
}
func (p *aggregatePartitions) finish() ([]*aggregateRun, error) {
	var runs []*aggregateRun
	for i, f := range p.files {
		if f == nil {
			continue
		}
		err := p.writers[i].Flush()
		if e := f.Close(); err == nil {
			err = e
		}
		p.files[i] = nil
		p.d.budget.reserve(-p.buffer)
		if err != nil {
			return nil, err
		}
		runs = append(runs, p.runs[i])
	}
	return runs, nil
}
func (g *grouper) mergePartial(other *groupState) (bool, error) {
	existing := g.groups[other.key]
	cost := groupStorage(other) + 160
	if existing != nil {
		cost = 0
		for i, a := range g.spec.accs {
			x, y := existing.accs[i], other.accs[i]
			switch a.op {
			case accLast:
				if y.hasExt {
					cost += int64(len(y.ext.s) - len(x.ext.s))
				}
			case accFirst:
				if !x.hasExt && y.hasExt {
					cost += int64(len(y.ext.s) - len(x.ext.s))
				}
			case accMin, accMax:
				if y.hasExt {
					c := compareValues(y.ext.view(), x.ext.view())
					if !x.hasExt || (a.op == accMin && c < 0) || (a.op == accMax && c > 0) {
						cost += int64(len(y.ext.s) - len(x.ext.s))
					}
				}
			}
		}
	}
	if cost > 0 && (g.charge+cost > g.capBytes || cost > g.budget.max-g.budget.used.Load()) {
		return false, nil
	}
	if err := g.reserve(cost); err != nil {
		return false, err
	}
	if existing == nil {
		g.groups[other.key] = other
		g.order = append(g.order, other)
	} else {
		mergeState(g.spec, existing, other)
	}
	return true, nil
}
func (d *aggregateDisk) partitionGroup(run *aggregateRun, spec *groupSpec, depth int, emit func(*groupState) error) error {
	if depth > 32 {
		return fmt.Errorf("%w: aggregation partition depth exhausted", ErrLimit)
	}
	r, err := d.reader(run)
	if err != nil {
		return err
	}
	open := true
	defer func() {
		if open {
			r.close()
		}
	}()
	g := newGrouper(spec, int(^uint(0)>>1))
	g.budget = d.budget
	g.slab = 1
	g.capBytes = d.stateLimit
	defer func() {
		if g != nil {
			g.release()
		}
	}()
	for {
		err = r.next(true)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(r.group.accs) != len(spec.accs) || len(r.group.id) != len(spec.idFields) {
			return ErrCorrupt
		}
		added, err := g.mergePartial(r.group)
		if err != nil {
			return err
		}
		if added {
			continue
		}
		// Already merged prefix states precede every remaining partial state.
		// Appending them first preserves first/last across recursive partitions.
		children := d.partitions()
		defer children.abort()
		for _, gs := range g.order {
			if err := children.add(gs); err != nil {
				return err
			}
		}
		g.release()
		g = nil
		if err := children.add(r.group); err != nil {
			return err
		}
		for {
			err = r.next(true)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if err = children.add(r.group); err != nil {
				return err
			}
		}
		runs, err := children.finish()
		if err != nil {
			return err
		}
		r.close()
		open = false
		if err = d.remove(run); err != nil {
			return err
		}
		d.stats.MergePasses++
		for _, child := range runs {
			if err = d.partitionGroup(child, spec, depth+1, emit); err != nil {
				return err
			}
		}
		return nil
	}
	for _, gs := range g.order {
		if err := d.ctx.Err(); err != nil {
			return err
		}
		if err := emit(gs); err != nil {
			return err
		}
	}
	r.close()
	open = false
	return d.remove(run)
}
