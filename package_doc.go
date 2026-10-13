// Package db provides the embedded AptevaDB document/key-value database.
// Open owns one local directory exclusively. Use DurableOptions for full-sync
// acknowledged-write durability; Options{} selects asynchronous writes.
//
// Collections support create-only Insert, replacement Upsert, existing-only
// Replace, point reads, consistent snapshot scans/queries, unique secondary
// indexes and stored computed fields/collections. Begin/Update provide optimistic
// serializable read/write transactions; NewBatch commits writes across collections.
// Writing transactions return ErrConflict after any concurrent committed write.
//
// Close snapshots, transactions, cursors and Reader handles promptly. DB and
// Collection are concurrency-safe; transactions, batches and cursors are owned
// by one goroutine. Callback/cursor buffers are borrowed; copy retained bytes.
// Format upgrades are one-way; back up before upgrading. Only Linux and macOS
// are supported. See github.com/apteva/db/client for the remote Go driver.
package db
