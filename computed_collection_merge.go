package db

import (
	"bytes"
	"context"
	"slices"
	"time"
)

// ComputedDistinctBucket contributes one stored bucket's exact membership
// to a result. Repeated buckets and identities count once per result.
type ComputedDistinctBucket struct {
	Key     []byte
	Measure string
	Result  int
}

type ComputedDistinctMergeOptions struct {
	MaxMemoryBytes int64
	Stats          *ComputedDistinctMergeStats
}

type ComputedDistinctMergeStats struct {
	Prefixes, Seeks, Leaves, Identities, SingletonResults int64
	PeakMemoryBytes                                       int64
	Duration                                              time.Duration
}

type computedMembershipRange struct {
	prefix []byte
	result int
}

// MergeComputedDistinct unions selected bucket memberships on one read
// snapshot. Each result index has its own exact accumulator. The caller
// selects the bucket keys, including any access restrictions, on the same
// snapshot. No source collection is scanned.
func (c *Collection) MergeComputedDistinct(ctx context.Context, buckets []ComputedDistinctBucket, results int, opts ComputedDistinctMergeOptions) ([]int64, error) {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	if results < 0 || opts.MaxMemoryBytes < 0 || c.state.derived == nil || c.state.derived.build == nil {
		return nil, ErrComputedCollection
	}
	if c.snap == nil {
		s, err := c.db.readSnapshot()
		if err != nil {
			return nil, err
		}
		defer s.Close()
		cc, err := s.Collection(c.name)
		if err != nil {
			return nil, err
		}
		return cc.MergeComputedDistinct(ctx, buckets, results, opts)
	}
	c.db.mu.RLock()
	closed := c.db.closed || c.snap.closed
	c.db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	maxBytes := opts.MaxMemoryBytes
	if maxBytes == 0 {
		maxBytes = DefaultQueryMemoryBytes
	}
	stats := opts.Stats
	if stats != nil {
		*stats = ComputedDistinctMergeStats{}
		start := time.Now()
		defer func() { stats.Duration = time.Since(start) }()
	}
	budget := &memoryBudget{max: maxBytes, owner: c.db.memory, trackPeak: stats != nil}
	c.db.memory.track(budget)
	defer budget.close()
	if stats != nil {
		defer func() { stats.PeakMemoryBytes = budget.peak }()
	}
	if int64(results) > maxBytes/48 || int64(len(buckets)) > maxBytes/96 {
		return nil, ErrLimit
	}
	if err := budget.reserve(int64(results)*48 + int64(len(buckets))*96); err != nil {
		return nil, err
	}
	measures := c.state.derived.build.measures
	slots := make(map[string]int, len(measures))
	for i, a := range measures {
		if a.op == accCountDistinct {
			slots[a.name] = i
		}
	}
	counts := make([]int64, results)
	sources := make([]uint8, results)
	for _, b := range buckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, ok := slots[b.Measure]; !ok || b.Result < 0 || b.Result >= results || len(b.Key) == 0 || len(b.Key) > MaxKeySize {
			return nil, ErrComputedCollection
		}
		if sources[b.Result] < 2 {
			sources[b.Result]++
		}
	}
	var prefixBytes int64
	multiBuckets := 0
	for _, b := range buckets {
		if sources[b.Result] > 1 {
			prefixBytes += int64(computedDistinctPrefixOverhead + len(b.Key))
			multiBuckets++
		}
	}
	if err := budget.reserve(prefixBytes); err != nil {
		return nil, err
	}
	prefixes := make([]byte, 0, int(prefixBytes))
	ranges := make([]computedMembershipRange, 0, multiBuckets)
	for _, b := range buckets {
		if sources[b.Result] == 1 {
			doc, found, err := c.Find(b.Key, nil)
			if err != nil {
				return nil, err
			}
			if found {
				v := docField(doc, b.Measure)
				if v.k != kInt || v.i < 0 {
					return nil, ErrCorrupt
				}
				counts[b.Result] = v.i
			}
			if stats != nil {
				stats.SingletonResults++
			}
			continue
		}
		start := len(prefixes)
		prefixes = appendComputedDistinctPrefix(prefixes, c.id, b.Key, slots[b.Measure])
		ranges = append(ranges, computedMembershipRange{prefix: prefixes[start:], result: b.Result})
	}
	slices.SortFunc(ranges, func(a, b computedMembershipRange) int {
		if n := bytes.Compare(a.prefix, b.prefix); n != 0 {
			return n
		}
		return a.result - b.result
	})
	// Most selected measures describe overlapping populations. Intern an
	// identity once per merge and retain membership as a bit or an integer
	// ID. Exact equality still compares the full type-tagged identity.
	identities := map[string]uint32{}
	var identityArena []byte
	var masks []uint64
	var sets []map[uint32]struct{}
	if results > 64 {
		sets = make([]map[uint32]struct{}, results)
	}
	add := func(targets []computedMembershipRange, key, ref []byte) error {
		if len(key) <= len(targets[0].prefix) || len(ref) != 8 || int64(le.Uint64(ref)) <= 0 {
			return ErrCorrupt
		}
		if stats != nil {
			stats.Identities++
		}
		identity := key[len(targets[0].prefix):]
		id, known := identities[string(identity)]
		if !known {
			if err := budget.reserve(56); err != nil {
				return err
			}
			if cap(identityArena)-len(identityArena) < len(identity) {
				size := max(len(identity), int(min(32<<10, max(128, maxBytes/32))))
				if err := budget.reserve(int64(size)); err != nil {
					return err
				}
				identityArena = make([]byte, 0, size)
			}
			start := len(identityArena)
			identityArena = append(identityArena, identity...)
			id = uint32(len(identities))
			if int(id) != len(identities) {
				return ErrLimit
			}
			// Arena bytes backing map keys are immutable for this merge.
			identities[borrowedString(identityArena[start:])] = id
			if sets == nil {
				masks = append(masks, 0)
			}
		}
		if sets == nil {
			mask := masks[id]
			for _, t := range targets {
				bit := uint64(1) << t.result
				if mask&bit == 0 {
					counts[t.result]++
					mask |= bit
				}
			}
			masks[id] = mask
			return nil
		}
		for _, t := range targets {
			set := sets[t.result]
			if _, ok := set[id]; ok {
				continue
			}
			if set == nil {
				if err := budget.reserve(160); err != nil {
					return err
				}
				set = make(map[uint32]struct{})
				sets[t.result] = set
			}
			if err := budget.reserve(24); err != nil {
				return err
			}
			set[id] = struct{}{}
			counts[t.result]++
		}
		return nil
	}
	if len(ranges) != 0 {
		if err := c.scanComputedMemberships(ctx, ranges, add, stats); err != nil {
			return nil, err
		}
	}
	return counts, ctx.Err()
}

func membershipRangeEnd(ranges []computedMembershipRange, i int) int {
	end := i + 1
	for end < len(ranges) && bytes.Equal(ranges[i].prefix, ranges[end].prefix) {
		end++
	}
	return end
}

// Walk adjacent selected ranges together. A leaf is visited once even when
// several bucket/measure prefixes end inside it. Large holes seek directly
// to the next requested prefix. Borrowed keys never escape the read lock.
func (c *Collection) scanComputedMemberships(ctx context.Context, ranges []computedMembershipRange, add func([]computedMembershipRange, []byte, []byte) error, stats *ComputedDistinctMergeStats) error {
	db, v := c.db, c.view()
	db.mu.RLock()
	buffered := len(v.memRange(ranges[0].prefix, prefixEnd(ranges[len(ranges)-1].prefix))) > 0
	db.mu.RUnlock()
	if buffered {
		// Use the ordinary snapshot scan to honour memtable replacements
		// and tombstones. Duplicate requests still share each prefix.
		for i := 0; i < len(ranges); {
			end := membershipRangeEnd(ranges, i)
			if stats != nil {
				stats.Prefixes++
				stats.Seeks++
			}
			var readErr error
			err := db.scan(v, ranges[i].prefix, prefixEnd(ranges[i].prefix), func(k, ref []byte) bool {
				if readErr = ctx.Err(); readErr == nil {
					readErr = add(ranges[i:end], k, ref)
				}
				return readErr == nil
			})
			if err != nil {
				return err
			}
			if readErr != nil {
				return readErr
			}
			i = end
		}
		return ctx.Err()
	}

	var walk leafWalk
	var resume []byte
	next := ranges[0].prefix
	i, end := 0, membershipRangeEnd(ranges, 0)
	if stats != nil {
		stats.Prefixes = 1
	}
	for i < len(ranges) {
		if err := ctx.Err(); err != nil {
			return err
		}
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrClosed
		}
		if stats != nil && (!walk.valid || walk.gen != db.treeGen) {
			stats.Seeks++
		}
		leaf, pos, err := walk.next(v, next)
		if err != nil || leaf == nil || leaf.numCells() == 0 {
			db.mu.RUnlock()
			return err
		}
		if stats != nil {
			stats.Leaves++
		}
		n := leaf.numCells()
		for pos < n && i < len(ranges) {
			key := leaf.leafKey(pos)
			compare := func(prefix []byte) int {
				return bytes.Compare(key[:min(len(key), len(prefix))], prefix)
			}
			for compare(ranges[i].prefix) > 0 {
				i = end
				if i == len(ranges) {
					break
				}
				end = membershipRangeEnd(ranges, i)
				if stats != nil {
					stats.Prefixes++
				}
			}
			if i == len(ranges) {
				break
			}
			if compare(ranges[i].prefix) < 0 {
				pos, _ = leaf.searchLeaf(ranges[i].prefix)
				continue
			}
			if leaf.leafIsOverflow(pos) {
				db.mu.RUnlock()
				return ErrCorrupt
			}
			if err = add(ranges[i:end], key, leaf.leafVal(pos)); err != nil {
				db.mu.RUnlock()
				return err
			}
			pos++
		}
		if i < len(ranges) {
			last := leaf.leafKey(n - 1)
			if bytes.Compare(last, ranges[i].prefix) < 0 {
				walk.valid = false
				next = ranges[i].prefix
			} else {
				resume = append(resume[:0], last...)
				resume = append(resume, 0)
				next = resume
			}
		}
		db.mu.RUnlock()
	}
	return nil
}
