# Alpha.4 aggregation and release validation

Runtime source: `bff9d0e956989bb566c48addb34377955a402f50`. Baseline: public `v0.2.0-alpha.3`, `26220edae0452a03a5cb5375016ca9eaf1e95209`.

The baseline probe reproduced SUM -2 and AVG -1 for two maximum int64 values. Alpha.4 accumulates and merges integer subtotals in signed 128 bits without additional accumulator state or allocations. SUM stays int64 when the final total fits, otherwise returns float64; AVG returns float64. Floating output has IEEE-754 precision. Maintained computed sums keep their existing final-int64-overflow rejection.

`$countNonNull: "$field.path"` excludes missing/null and counts every other field value once, including containers. Existing row count is unchanged. Embedded queries, the Go HTTP client and maintained computed collections are covered. Format 9 protects persisted definitions; make an original-binary backup before the one-way upgrade.

## Correctness

- Local `go vet ./...`, `go build ./...`, and benchmark-module vet/build passed.
- Full local `go test -race -count=1 -timeout 45m ./...` passed (engine 350.759s, client 1.538s).
- Two-minute cursor stress plus two-minute soak under race detection passed in 250.798s.
- Regression coverage includes maximum/minimum integer sums, positive/negative intermediate overflow and cancellation, serial and 2/4/10-shard merges, streaming/materialized pipelines, random exact integer model checks, correctly rounded conversion from arbitrary signed 128-bit totals, and HTTP transport.
- Non-null counters cover missing/null, zero/false/empty strings/containers, dotted paths, invalid operands, updates/deletes, checkpoint, compaction and reopen.
- A genuine alpha.3 format-8 data/WAL fixture upgrades both metadata slots to format 9, preserves queries and existing maintained collections, and accepts the new counter.
- [Linux/macOS CI, including race and fuzz](https://github.com/apteva/db/actions/runs/38457763285). Final release checks are recorded below.

## Benchmark conditions

Apple M1 Pro, 10 logical CPUs, 16 GiB RAM, Go version and OS in metadata.json. All local compile/test/stress processes finished before timing. Precompiled baseline/current binaries use identical harnesses and fresh directories. Every trial is retained; common comparisons use three processes per version in alternating order. Query benches operate on 100,000 documents; raw KV checks use 1,000,000 16-byte keys / 100-byte values. These are in-memory/resident, non-durable-write measurements, with harness defaults and explicit commands/load samples in runs.json. Go allocations are not peak RSS.

**Background interference:** macOS storage/indexing tasks and a separate database-adapter benchmark were active. Keep the measured slowdowns as well as gains; these trials do not establish performance equivalence or universal improvement. Parallel timings varied especially strongly. An initial benchmark-name filter accidentally selected the known-broken wide-integer baseline case; its failed output is retained and excluded from comparison. The corrected filter selected only common supported cases.

### Common aggregation (median ns per row; lower is better)

| SUM/AVG or count | Shards | Alpha.3 | Alpha.4 | Change |
|---|---:|---:|---:|---:|
| Integers | 1 | 46.670 | 46.730 | +0.1% |
| Integers | 10 | 11.610 | 12.710 | +9.5% |
| Floats | 1 | 47.110 | 46.930 | -0.4% |
| Floats | 10 | 11.930 | 12.870 | +7.9% |
| RowCount | 1 | 22.750 | 23.720 | +4.3% |
| RowCount | 10 | 6.645 | 7.826 | +17.8% |

Serial integer SUM+AVG measured essentially flat (46.67 → 46.73 ns/row), with the same 5,745 B/op and 89 allocations/op. Parallel medians were higher in this run, including the unchanged floating path; background activity prevents isolating that difference. The existing 80-byte accumulator layout and working-memory accounting are retained.

### New operations (100,000 rows; three trials)

| Operation | Shards | Median ms/query | Median ns/row |
|---|---:|---:|---:|
| WideIntegers | 1 | 4.622 | 46.220 |
| WideIntegers | 10 | 1.086 | 10.860 |
| NonNullCount | 1 | 4.157 | 41.570 |
| NonNullCount | 10 | 1.008 | 10.080 |

The widened-sum workload uses maximum int64 values and checks both SUM and AVG. Non-null counting uses alternating null and zero values and checks the 50,000 result.

### Raw KV guards (median ns/op)

| Workload/operation | Alpha.3 | Alpha.4 |
|---|---:|---:|
| random/insert | 764 | 798 |
| random/settle | 830 | 1065 |
| random/find-hit | 713 | 720 |
| random/find-miss | 483 | 504 |
| random/scan | 49 | 39 |
| random/find-hit-par10 | 104 | 101 |
| seq/insert | 188 | 214 |
| seq/settle | 653 | 870 |
| seq/find-hit | 1014 | 1045 |
| seq/find-miss | 925 | 946 |
| seq/scan | 50 | 39 |
| seq/find-hit-par10 | 158 | 150 |

Raw scans improved approximately 20–22% against alpha.3 with the scan changes already validated on main. Several other medians were slower; this is not evidence that every workload improves.

### Comparisons and compatibility

Three fresh aggregation comparisons per version check results against Pebble and C SQLite. Pebble uses the AptevaDB query executor over Pebble storage because Pebble has no native aggregation; SQLite uses equivalent SQL over typed columns. Inner query timings use best-of-five; outer trials are all retained. Alpha.4 also passed the join, stored-computed-field, maintained-collection, exact/compound-query compatibility and large-index benchmark suites with the configured comparison engines. See agg-*.txt, join.txt, computed*.txt, compat.txt and wideindex.txt for all results.

Total successful benchmark processes: 24. Commands, durations, return codes and load averages are in runs.json. Baseline/current binary hashes are in benchmark-binary-sha256.json.

## Published release verification

The public module resolves through proxy.golang.org with checksum verification and no local replace directive. A fresh consumer verified serial/10-shard overflow results and the persisted maintained counter after reopen. A public `go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.4` into an isolated directory verified the database and ran the new counter. The alpha.3 binary refuses format 9 before changing any data/WAL hashes (old-binary-refusal.txt).

The tag points to the exact commit that passed source CI. Duplicate tag-triggered CI was cancelled to free a macOS runner; the independent release workflow retains its full race and stress/soak verification. [Release workflow](https://github.com/apteva/db/actions/runs/38459112750) completed successfully: Linux/macOS full race tests and 60-second cursor stress plus 60-second soak on the exact tag, followed by all four archive builds and publication.

[Public release](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.4) is a published prerelease with Linux/macOS amd64/arm64 CLI/server archives and SHA256SUMS. Downloaded all four archives and verified their SHA256 hashes. The downloaded Darwin arm64 CLI/server report alpha.4 / format 9; CLI database verification passed. The downloaded daemon and public Go client verified positive SUM/AVG, row/non-null counts and a maintained counter including false, followed by graceful shutdown. Logs and checksums are in release-archive-smoke.txt and release-checksums.json.

Release tag remains on the validated runtime commit; this evidence-only follow-up does not alter the runtime.
