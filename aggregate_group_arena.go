package db

import "unsafe"

func (g *grouper) arenaSize() int {
	limit := g.capBytes
	if limit == 0 && g.budget != nil {
		limit = g.budget.max
	}
	return int(min(int64(16<<10), max(int64(128), limit/32)))
}
func (g *grouper) ownText(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if g.slab <= 1 {
		if err := g.reserve(int64(len(s))); err != nil {
			return "", err
		}
		return string(append([]byte(nil), s...)), nil
	}
	if len(s) > cap(g.arena)-len(g.arena) {
		n := max(g.arenaSize(), len(s))
		if err := g.reserve(int64(n)); err != nil {
			return "", err
		}
		g.arena = make([]byte, 0, n)
	}
	start := len(g.arena)
	g.arena = append(g.arena, s...)
	return unsafe.String(&g.arena[start], len(s)), nil
}
func (g *grouper) textCost(lengths []int) int64 {
	if g.slab <= 1 {
		var n int64
		for _, l := range lengths {
			n += int64(l)
		}
		return n
	}
	left := cap(g.arena) - len(g.arena)
	var n int64
	for _, l := range lengths {
		if l > left {
			size := max(g.arenaSize(), l)
			n += int64(size)
			left = size
		}
		left -= l
	}
	return n
}
func (g *grouper) slabCost() int64 {
	if g.used < len(g.states) {
		return 0
	}
	return int64(g.nextSlab()) * (64 + int64(len(g.spec.idFields))*64 + int64(len(g.spec.accs))*80)
}
