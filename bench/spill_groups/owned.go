package main

import (
	"context"
	db "github.com/apteva/db"
)

func owned(p *db.Pipeline, src db.Source, o db.AggregateOptions, check func(int64, int64, int64, int64, int64, float64) error) error {
	var stop error
	e := p.RunEach(context.Background(), src, o, func(r map[string]any) bool {
		k := r["_id"].(map[string]any)["i"].(int64)
		n := r["count"].(int64)
		s, _ := r["sum_i"].(int64)
		lo, _ := r["min_i"].(int64)
		hi, _ := r["max_i"].(int64)
		avg, _ := r["avg_n"].(float64)
		stop = check(k, n, s, lo, hi, avg)
		return stop == nil
	})
	if e != nil {
		return e
	}
	return stop
}
