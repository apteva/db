//go:build !alpha7

package main

import (
	"context"
	db "github.com/apteva/db"
)

func openDatabase(dir string, cold bool) (*db.DB, error) {
	o := db.Options{CacheBytes: 64 << 20}
	if cold {
		o.MappedCacheBytes = 16 << 20
	}
	return db.Open(dir, o)
}
func execute(c *db.Collection, p *db.Pipeline, api string, memory int64, spill bool, emit func(map[string]any) bool) (any, error) {
	if api == "legacy" || api == "parallel" {
		var rows []map[string]any
		var e error
		if api == "parallel" {
			rows, e = c.RunParallel(context.Background(), p, 10)
		} else {
			rows, e = c.Run(context.Background(), p)
		}
		if e != nil {
			return nil, e
		}
		for _, r := range rows {
			if !emit(r) {
				break
			}
		}
		return nil, nil
	}
	var st db.AggregateStats
	e := c.RunEach(context.Background(), p, db.AggregateOptions{MaxMemoryBytes: memory, AllowDiskUse: spill, MaxDiskBytes: 2 << 30, Stats: &st}, emit)
	return st, e
}
func trim(d *db.DB) (any, error) {
	if _, e := d.TrimMemory(context.Background(), 16<<20); e != nil {
		return nil, e
	}
	if _, e := d.TrimMemory(context.Background(), 16<<20); e != nil {
		return nil, e
	}
	return d.MemoryStats(context.Background())
}
