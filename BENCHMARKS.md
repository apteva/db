# Benchmarks

The separate bench module compares the same fixtures against AptevaDB, Pebble,
SQLite C and SQLite Go. It includes sequential/random KV operations, misses,
parallel reads, collection variants, batches, durable writes, aggregations,
joins, computed fields/collections and per-process CPU/RSS/OS memory footprint.

```sh
cd bench
go build -o aptevadb-bench .
./aptevadb-bench -n 1000000 -par 10
./aptevadb-bench -n 1000000 -engines ours-batch1000,pebble-batch1000,sqlite-c-txn1000,sqlite-go-txn1000
./aptevadb-bench -n 50000 -agg
./aptevadb-bench -n 10000 -join
./aptevadb-bench -n 50000 -computed
./aptevadb-bench -n 20000 -computedcollections
./aptevadb-bench -n 1000 -durable -engines ours-sync,pebble-sync,sqlite-c-sync,sqlite-go-sync
python3 run_resources.py --binary ./aptevadb-bench --out /tmp/aptevadb-resources --n 1000000 --reps 3
```

Use Upsert for KV comparisons: Pebble Set and SQLite replace rows too. Create-only
Insert adds a duplicate-key check and must be compared with equivalent semantics.
The default KV tests are asynchronous; durable tests select their flush modes
explicitly. SQLite cache settings apply per connection, not to the whole process.
AptevaDB CacheBytes targets logical pages; memtables/snapshots/WAL/maps add memory.
Resource trials include fixture payloads, startup and settle/compaction peaks.

Run comparisons serially in fresh processes, retain repetitions and report medians
and ranges. State source revision, compiler, OS/CPU, key/value sizes, caches,
memtables, readers and sync policy. Record background load; do not benchmark beside
builds, tests or soak runs. Historical results under bench/results refer to their
original builds. They do not establish release performance or a universal lead.
