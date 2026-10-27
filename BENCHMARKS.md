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

## Bounded aggregation and resident-memory controls (alpha.8)

The [validation report](bench/results/aggregation-memory/REPORT.md) compares
released alpha.7 with the final source in 110 fresh processes, including
one-million-document guards. Ordered aggregation reduces CPU and peak memory;
opt-in reclamation reduces idle RSS. Disk spill completes constrained workloads
at a documented speed cost. Some guard medians remain slower, so these results
do not establish zero regressions or a universal advantage. All trials, ranges,
source identities and correctness/build evidence are retained, with the earlier
90-process candidate cohort kept separate.

## Memory optimization validation (alpha.7)

The [latest validation](bench/results/memory-followup/native/REPORT.md) keeps
36 final native trials (original alpha.6, the prior memory candidate and final
code) separate from the preceding 48-trial competitor matrix. It preserves the
prior buffering policy after rejecting smaller-table tuning that hurt ingestion.
The report lists all CPU/elapsed differences and RSS/footprint gaps, with every
sample retained. Background activity was present; small differences do not
establish significance or a universal lead. WAL boundary and snapshot-retirement
recovery regressions fail on earlier code and pass on the final source.
A separate complete-key read shortcut was rejected after its small hit-read gain
raised peak RSS by 95 MiB; its raw trials are retained with the report.

The [memory/CPU comparison](bench/results/memory-optimization/REPORT.md) compares
unchanged alpha.6 with the candidate, Pebble and SQLite C using three fresh
processes per fixture/order. A subsequent native-only cohort verifies the final
post-compaction warm-up fix; the report keeps both candidate versions separate. It covers one million 100-byte values (random and
sequential insertion) and 100,000/250,000 1-KiB values, including updates,
deletes, scans and ten concurrent readers. Completed write work includes
checkpoint/compaction costs. RSS and macOS physical footprint include fixtures.
The report retains CPU and elapsed time separately and identifies remaining gaps.
These measurements validate the alpha.7 implementation; results do not establish a universal lead.

## Public-alpha measurements

See [the completed alpha report](bench/results/public-alpha/VALIDATION.md) and
[raw observations and methodology](bench/results/public-alpha/README.md). The
release engine passed all 30 resource trials; the corrected feature harness passed
all 24 trials. Post-tag fixture fixes add SQLite Go aggregation/join support,
match empty-aggregate semantics, choose an existing indexed customer and fail
on result mismatches. These changes do not alter the released library.

The bounded-cache profile uses more peak memory than Pebble. Default AptevaDB
uses less memory than its bounded profile on this fixture, but the profiles have
different cache/memtable strategies. These tests ran with background applications
and swapping, and do not prove performance without regressions or a universal lead.

## Compatibility feature workload

```sh
cd bench
go build -o aptevadb-bench .
./aptevadb-bench -compat -n 50000 -reps 3
```

This workload checks exact ordered results above 2^53 against an independent
integer model. It compares native compound pages/cursors with Pebble application
indexes and equivalent typed SQLite C/Go indexes, includes nullable unique-index
builds, measures a bounded fallback sort and memory-limit rejection, and compares
80 KB binary document reads/writes across all engines. Native/SQLite/Pebble loads
use 1,000-record batches/transactions; the large-value phase uses individual async
writes for each engine. Pebble index maintenance is application work; it does not
provide the native schema/constraint API. Every result and repetition is retained;
Go allocation metrics exclude native/C/mapped allocations and are not RSS.

The completed [alpha.2 report](bench/results/alpha2/VALIDATION.md) retains all
comparisons and paired baseline observations. It uses the corrected compatibility
cache settings, reports costs/ranges, and links the release/install checks.
