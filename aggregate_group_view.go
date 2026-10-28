package db

import (
	"context"
	"strings"
)

// GroupView borrows one completed group's typed state. It and any string from
// String are valid only during the callback. Any returns an owned Go value.
// Int64 never rounds an overflowing integer subtotal through float64.
type GroupView struct {
	spec  *groupSpec
	state *groupState
}

func (g GroupView) field(name string) value {
	if name == "_id" && g.spec.idNames == nil {
		if len(g.state.id) == 0 {
			return value{k: kNull}
		}
		return g.state.id[0].view()
	}
	if strings.HasPrefix(name, "_id.") {
		n := strings.TrimPrefix(name, "_id.")
		for i, k := range g.spec.idNames {
			if k == n {
				return g.state.id[i].view()
			}
		}
	}
	for i, a := range g.spec.accs {
		if a.name == name {
			return accValue(&a, &g.state.accs[i])
		}
	}
	return value{}
}
func (g GroupView) Int64(name string) (int64, bool) { v := g.field(name); return v.i, v.k == kInt }
func (g GroupView) Float64(name string) (float64, bool) {
	v := g.field(name)
	if v.k == kInt {
		return float64(v.i), true
	}
	return v.f, v.k == kFloat
}
func (g GroupView) String(name string) (string, bool) {
	v := g.field(name)
	return borrowedString(v.s), v.k == kString
}
func (g GroupView) Bool(name string) (bool, bool) { v := g.field(name); return v.i != 0, v.k == kBool }
func (g GroupView) IsNull(name string) bool {
	v := g.field(name)
	return v.k == kMissing || v.k == kNull
}
func (g GroupView) Any(name string) any { return g.field(name).any() }
func (g GroupView) IDCount() int        { return len(g.state.id) }
func (g GroupView) IDName(index int) string {
	if index < 0 || index >= len(g.spec.idNames) {
		return ""
	}
	return g.spec.idNames[index]
}
func (g GroupView) ID(index int) any {
	if index < 0 || index >= len(g.state.id) {
		return nil
	}
	return g.state.id[index].any()
}

// IntegerSum exposes the exact signed-128 integer subtotal for SUM/AVG when no
// floating inputs contributed. The low limb is unsigned; high is sign-extended.
func (g GroupView) IntegerSum(name string) (high int64, low uint64, ok bool) {
	for i, a := range g.spec.accs {
		s := &g.state.accs[i]
		if a.name == name && (a.op == accSum || a.op == accAvg) && !s.hasFloat {
			return s.ext.i, uint64(s.isum), true
		}
	}
	return 0, 0, false
}

// RunGroups visits every completed group without allocating owned result maps.
// The pipeline must end in $group and have a grouping streaming prefix. Return
// an error to reject a group/stop execution; results must be staged by callers
// requiring atomic output. One snapshot and all execution budgets still apply.
// RunGroups runs a compiled pipeline ending in one $group and calls yield
// with each completed group as a borrowed GroupView, without building row
// maps. No earlier stage may be a $group or $count. A non-nil error from
// yield stops execution and is returned unchanged; opts apply as for RunEach.
func (c *Collection) RunGroups(ctx context.Context, p *Pipeline, opts AggregateOptions, yield func(GroupView) error) error {
	if p == nil || yield == nil || len(p.stages) == 0 || p.stages[len(p.stages)-1].kind != stGroup {
		return ErrPipeline
	}
	for _, st := range p.stages[:len(p.stages)-1] {
		if st.kind == stGroup || st.kind == stCount {
			return ErrPipeline
		}
	}
	return c.runEach(ctx, p, opts, func(map[string]any) bool { return true }, false, yield)
}

// AggregateGroups parses pipeline and runs it with RunGroups.
func (c *Collection) AggregateGroups(ctx context.Context, pipeline []byte, opts AggregateOptions, yield func(GroupView) error) error {
	p, err := ParsePipeline(pipeline)
	if err != nil {
		return err
	}
	return c.RunGroups(ctx, p, opts, yield)
}

// AggregateGroups runs AggregateGroups on the default collection.
func (d *DB) AggregateGroups(ctx context.Context, p []byte, o AggregateOptions, y func(GroupView) error) error {
	return d.def.AggregateGroups(ctx, p, o, y)
}

// AggregateGroups runs AggregateGroups on the snapshot's default collection.
func (s *Snapshot) AggregateGroups(ctx context.Context, p []byte, o AggregateOptions, y func(GroupView) error) error {
	return s.def().AggregateGroups(ctx, p, o, y)
}

// Kind distinguishes missing/null and exact integer/floating results without boxing.
func (g GroupView) Kind(name string) string {
	switch g.field(name).k {
	case kMissing:
		return "missing"
	case kNull:
		return "null"
	case kBool:
		return "bool"
	case kInt:
		return "int"
	case kFloat:
		return "float"
	case kString:
		return "string"
	case kDoc:
		return "document"
	case kArray:
		return "array"
	}
	return "missing"
}

// MetricCount and MetricName list the accumulators in pipeline order.
func (g GroupView) MetricCount() int { return len(g.spec.accs) }
func (g GroupView) MetricName(index int) string {
	if index < 0 || index >= len(g.spec.accs) {
		return ""
	}
	return g.spec.accs[index].name
}

// RunGroups provides the same borrowed visitor for caller-supplied sources.
// Collection.RunGroups supplies native index planning, snapshots and lookups.
func (p *Pipeline) RunGroups(ctx context.Context, src Source, opts AggregateOptions, yield func(GroupView) error) error {
	if p == nil || yield == nil || len(p.stages) == 0 || p.stages[len(p.stages)-1].kind != stGroup {
		return ErrPipeline
	}
	for _, st := range p.stages[:len(p.stages)-1] {
		if st.kind == stGroup || st.kind == stCount {
			return ErrPipeline
		}
	}
	return p.runEachSource(ctx, src, opts, func(map[string]any) bool { return true }, false, yield)
}
