//go:build alpha7

package main

import (
	"context"
	db "github.com/apteva/db"
)

func openDatabase(dir string, _ bool) (*db.DB, error) {
	return db.Open(dir, db.Options{CacheBytes: 64 << 20})
}
func execute(c *db.Collection, p *db.Pipeline, api string, _ int64, _ bool, emit func(map[string]any) bool) (any, error) {
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
func trim(_ *db.DB) (any, error) { return nil, nil }
