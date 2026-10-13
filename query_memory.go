package db

import (
	"fmt"
	"sync/atomic"
)

// Budgets count conservative retained execution allocations, including group
// slabs, row maps, join keys and sort storage. They are not a process RSS cap.
type memoryBudget struct {
	max  int64
	used atomic.Int64
}

func (b *memoryBudget) reserve(bytes int64) error {
	if b == nil || bytes == 0 {
		return nil
	}
	used := b.used.Add(bytes)
	if used > b.max {
		b.used.Add(-bytes)
		return fmt.Errorf("%w: query working memory exceeds %d bytes", ErrLimit, b.max)
	}
	return nil
}
func (l Limits) budget() (*memoryBudget, error) {
	if l.MaxRows < 0 || l.MaxGroups < 0 || l.MaxBytes < 0 {
		return nil, ErrPipeline
	}
	n := l.MaxBytes
	if n == 0 {
		n = DefaultQueryMemoryBytes
	}
	return &memoryBudget{max: n}, nil
}
func valueMemory(v any) int64 {
	switch x := v.(type) {
	case string:
		return int64(len(x)) + 32
	case []any:
		n := int64(24 + len(x)*24)
		for _, item := range x {
			n += valueMemory(item)
		}
		return n
	case map[string]any:
		n := int64(128 + len(x)*64)
		for k, item := range x {
			n += int64(len(k)) + valueMemory(item)
		}
		return n
	default:
		return 16
	}
}
