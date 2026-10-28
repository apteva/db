//go:build alpha8baseline

package main

import (
	db "github.com/apteva/db"
)

func execute(p *db.Pipeline, src db.Source, o db.AggregateOptions, _ bool, check func(int64, int64, int64, int64, int64, float64) error) error {
	return owned(p, src, o, check)
}
