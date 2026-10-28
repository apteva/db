//go:build !alpha8baseline

package main

import (
	"context"
	"fmt"
	db "github.com/apteva/db"
)

func execute(p *db.Pipeline, src db.Source, o db.AggregateOptions, typed bool, check func(int64, int64, int64, int64, int64, float64) error) error {
	if !typed {
		return owned(p, src, o, check)
	}
	return p.RunGroups(context.Background(), src, o, func(g db.GroupView) error {
		k, ok := g.Int64("_id.i")
		if !ok {
			return fmt.Errorf("noninteger ID")
		}
		n, ok := g.Int64("count")
		if !ok {
			return fmt.Errorf("noninteger count")
		}
		s, _ := g.Int64("sum_i")
		lo, _ := g.Int64("min_i")
		hi, _ := g.Int64("max_i")
		avg, _ := g.Float64("avg_n")
		return check(k, n, s, lo, hi, avg)
	})
}
