# Native change feeds: validation and performance

Benchmark runtime: `a6040dc781d5b29337e1d6fc231ed93ffdbb184e`. Public alpha.4 baseline: `bff9d0e956989bb566c48addb34377955a402f50`. Alpha.5 was subsequently released at `2dbc4d428c82e281f1489dd450004a6d33b3adfc`; the engine/server/client Go source is unchanged. Release verification is below. See [the API and contracts](../../../CHANGE_FEEDS.md).

Native embedded and HTTP Go watches support collection/operation/key-prefix filters, Query document predicates, projection, commit-boundary resume tokens, and atomic SnapshotAndWatch. Source and computed-collection events share their original committed WAL group; values are not logged twice. Capture is opt-in and persistent. Default shared hot cache is 8 MiB, aggregate plan/reader budget is 64 MiB, and history is a logical 256 MiB / one hour. These are accounting limits, not total RSS caps. Segment boundaries and the active recovery tail can make physical WAL retention larger.

## Correctness

- Local full race pass completed before the final memory-accounting follow-up (engine 363.094s, client 3.214s). Two-minute change-feed stress plus cursor stress/soak under race detection passed in 370.494s. A further 30-second feed stress and targeted race tests passed after value-routing optimizations; the memory-accounting regression and all watch tests passed under race detection after the budget fix.
- Local vet/build passed for the root and benchmark modules. Change-marker fuzzing completed 1,498,760 executions in 20 seconds. CI also fuzzes documents, pipelines, pages and wide splits.
- Coverage includes coalescing, rollback, rejected writes, exact integers, logical/prefix/nested filters, projection, source/view commit ordering, Reset/Drop, reopen, checkpoint, compaction, backup identities, byte/age expiry, slow readers, capture/reader/plan limits, blocked fsync, and a near-16-MiB document. Torn-marker recovery rejects the entire group; an old token cannot alias a replacement write at a reused offset.
- HTTP tests cover TLS, snapshots, replay, separate connection/request limits, 70 simultaneous streams plus CRUD, client cancellation and bounded frames.
- A genuine unmodified alpha.4 format-9 fixture upgrades to format 10 while preserving exact integers and maintained non-null counts. The separate Go consumer checks embedded snapshot handoff, exact filtering, projection, compaction, reopening with persisted settings and replay.
- The daemon smoke test checks watch/replay and SIGTERM with an idle live stream (17 ms, exit 0). The public alpha.4 CLI refuses the format-10 directory before changing any data/WAL hashes. The current CLI verifies it. See initial/daemon-smoke.txt.
- Initial complete Linux/macOS CI: [0178fcf](https://github.com/apteva/db/actions/runs/38533993351). Final exact-source CI: [a6040dc](https://github.com/apteva/db/actions/runs/38536232661); final status is recorded below.

## Measurement conditions

Apple M1 Pro, 10 logical CPUs, 16 GiB RAM, Go 1.25.1, macOS 27.0. All measurements use precompiled binaries and fresh database directories, with no own compile/test/stress process running during timing. Background macOS indexing/storage tasks, application activity and other local work remained active; load/process samples are retained. These are repeated observations, not statistically established equivalence or worst-case latency guarantees.

All 291 recorded benchmark processes are retained: initial/ has 109 processes at 0178fcf; budget-fix/ has 100 at b03726f; verified/ has 82 at final runtime a6040dc (291 total). The last runtime change owns option strings and occurs at watch setup, outside timed write/delivery loops. The complete existing comparison suites were repeated three times both before and after the budget fix. They use unchanged storage/query/computed code. Final native/disabled/resource checks are from a6040dc. Pebble/SQLite resource measurements are reused from the initial set, because their bindings/harness are unchanged; their source is explicitly recorded in summary.json. No trial was discarded for being slow.

Feeds use async, non-fsync writes; durability was tested separately for correctness. Documents have 100 bytes of padding plus integer tenant/seq fields. A batch contains every one of 100 tenants. Active readers acknowledge each selected commit before the next measured batch; full-value and projection modes also decode application maps. Feed resource probes time 10,000 active-reader rows or 20,000 off/capture/idle rows. Peak RSS and CPU are per fresh child process via wait4. Process CPU includes startup, setup, consumers and Close; writer time excludes setup, staging and Close. Go allocated bytes are separate from RSS and omit mapped engine buffers.

Latency samples start before batch staging, then include Commit and all selected acknowledgements (despite the historical raw field name commit_to_last_subscriber_ns). The p99 estimates below have only 30 or 60 batch samples. Go allocation benchmarks additionally run 100 batches of 1,000 rows; their cache reaches its configured bound. Raw guards use 100,000 resident rows; larger raw/CPU/RSS tests use 1,000,000 16-byte keys and 100-byte values.

## Batched feeds (three process medians)

| Mode | Source rows/s | Writer ns/row | Peak RSS MiB | Peak reader accounting KiB | Batch p50 / p99 ms |
|---|---:|---:|---:|---:|---:|
| Off | 6,578,947 | 117.2 | 14.52 | 0.00 | 0.147 / 0.240 |
| Capture | 2,841,178 | 314.5 | 20.62 | 0.00 | 0.341 / 17.549 |
| Metadata1 | 2,386,065 | 332.2 | 16.97 | 89.39 | 0.401 / 0.840 |
| Metadata10 | 1,728,571 | 341.7 | 17.25 | 89.39 | 0.535 / 1.924 |
| Metadata100 | 702,673 | 387.0 | 18.39 | 89.39 | 1.163 / 3.058 |
| Filtered100 | 944,157 | 361.6 | 19.80 | 252.48 | 0.979 / 1.633 |
| Tenants100 | 230,987 | 424.7 | 19.38 | 252.48 | 4.254 / 5.599 |
| Values10 | 555,342 | 298.7 | 22.95 | 252.48 | 1.880 / 5.165 |
| Projection100 | 903,859 | 369.2 | 21.11 | 252.63 | 1.038 / 1.495 |
| Slow100 | 2,537,226 | 354.8 | 20.72 | 0.00 | 0.340 / 1.059 |

Metadata100 delivers every row to 100 readers: 702,673 source rows/s means about 70.3 million aggregate notifications/s in this barrier-style workload. Filtered100 shares one 1%-selective predicate; Projection100 shares that predicate and sends/decodes only seq. Tenants100 uses 100 different 1%-selective predicates and costs more CPU. Filters reduce delivery, not retained WAL bytes or capture work. Values10 includes ten map decodes per row, which dominate its Go allocations (81.1 allocations per source row).

Capture without readers adds about 197 ns to the median batched write (117.2 to 314.5 ns/row). It is not free when enabled. Capture throughput varied from 0.30 to 3.26 million source rows/s; the slow run and its high p99 remain included. Identical match groups share a bounded cache; 100 distinct filters do not receive an indexed-field routing guarantee.

One hundred consumers which never call Next allow about 2.54 million source writes/s in batches and consume zero reader buffers. They can later fail with explicit history expiry. This is an idle-consumer throughput measurement, not a promise that active slow consumers can keep up indefinitely.

## Single-row commits (three process medians)

| Mode | Source commits/s | Writer ns/row | Peak RSS MiB | End-to-end p50 / p99 microseconds |
|---|---:|---:|---:|---:|
| Off | 2,386,172 | 233.1 | 13.42 | 0.29 / 1.71 |
| Capture | 1,335,611 | 561.0 | 19.89 | 0.54 / 2.46 |
| Metadata1 | 306,483 | 1070.8 | 21.70 | 2.38 / 11.83 |
| Metadata10 | 37,330 | 3352.4 | 22.41 | 19.92 / 87.75 |
| Metadata100 | 4,247 | 16153.9 | 22.56 | 224.50 / 615.46 |
| Slow100 | 1,315,047 | 573.4 | 20.69 | 0.54 / 2.50 |

Batching matters: the single-row Metadata100 test waits for 100 goroutines after every commit and achieves only about 4,247 commits/s, not the batched throughput. Unbatched capture adds about 328 ns per write (233.1 to 561.0 ns). Delivery latency and throughput will also change with fsync, HTTP serialization, network cost, document size and consumer work; these measurements do not establish HTTP watch throughput.

## Disabled-feed guards against alpha.4

| Operation | Alpha.4 ns | Current ns | Change | Current allocs/op |
|---|---:|---:|---:|---:|
| Upsert | 540.200 | 528.900 | -2.1% | 0 |
| Find | 431.900 | 438.400 | +1.5% | 0 |
| Scan (per row) | 16.721 | 16.850 | +0.8% | 27 |

Five alternating process trials per version; allocations are unchanged. Scan allocations are per complete 100,000-row scan. The final small guard shows +0.13 ns per scanned row and +1.5% Find time. Do not claim universally faster reads or proven zero regression. Earlier sets show different directions under the background load; all are retained.

| Million-row workload/operation | Alpha.4 ns/op | Current ns/op | Change |
|---|---:|---:|---:|
| random/insert | 779 | 743 | -4.6% |
| random/settle | 582 | 587 | +0.9% |
| random/find-hit | 653 | 661 | +1.2% |
| random/find-miss | 441 | 429 | -2.7% |
| random/scan | 38 | 39 | +2.6% |
| random/find-hit-par10 | 90 | 105 | +16.7% |
| seq/insert | 190 | 191 | +0.5% |
| seq/settle | 536 | 556 | +3.7% |
| seq/find-hit | 947 | 952 | +0.5% |
| seq/find-miss | 861 | 860 | -0.1% |
| seq/scan | 39 | 38 | -2.6% |
| seq/find-hit-par10 | 169 | 140 | -17.2% |

In particular random parallel hits measured +16.7%, while sequential parallel hits measured -17.2%. Random scans rounded to 38 versus 39 ns/row. Earlier current-source raw trials varied more widely. These results warrant a quiet-machine follow-up before claiming performance equivalence; they are not concealed by selecting the fastest repeat.

## CPU and memory against other engines

| Engine | Peak RSS MiB | Peak macOS footprint MiB | Insert CPU ns/op | Hit CPU ns/op | Scan CPU ns/row | Parallel hit wall ns/op |
|---|---:|---:|---:|---:|---:|---:|
| ours | 322.9 | 302.4 | 793.8 | 725.5 | 38.4 | 110.7 |
| pebble | 367.7 | 465.3 | 2662.9 | 2351.2 | 74.4 | 657.3 |
| sqlite-c | 1821.5 | 2098.9 | 21925.0 | 2987.4 | 153.4 | 2284.6 |
| sqlite-go | 2439.2 | 2993.6 | 46874.1 | 3982.8 | 130.3 | 10954.2 |

These raw-storage tests have feeds disabled. Final AptevaDB measured 12.2% lower peak RSS and 35.0% lower peak footprint than Pebble in these conditions. Peaks include the 140 MB fixture and 10-reader setup and are process-lifetime high-water marks, not phase-local engine-only memory. RSS and macOS footprint measure different things and should not be subtracted from one another. Pebble/SQLite use the harness's 512 MiB cache setting; AptevaDB uses its resident page layout without a configured cache cap. SQLite reader connection caches contribute strongly to its parallel peak. This is a measured configuration, not a minimum-memory claim for another DB.

## Existing comparison suites

Both initial/ and budget-fix/ contain three successful fresh processes for aggregation, joins, stored computed fields, maintained collections, exact/compound-query compatibility and 1,500-byte-primary-key / 3,000-byte-index-value workloads, with all configured comparison engines. Each validates results as well as timings. Pebble aggregation uses the AptevaDB executor over Pebble storage because Pebble has no native query executor; SQLite runs equivalent SQL. Existing suites may report best-of-five internal query timings, but every outer process is retained. Mixed-engine process RSS from these suites cannot be assigned to any individual engine.

## Final CI status

Exact-source CI completed successfully on a6040dc: Linux/macOS vet, builds, unit tests, full race suites and two-minute feed stress under race detection; Ubuntu fuzz targets all passed. ci.json and ci-log.txt retain the job results and logs. The scheduled hour-long soak on the earlier 0178fcf commit is independent and is not counted as final-source validation.


## Reproducing

From a checkout containing the baseline Git tag/history, use Python 3.12+ and
Go 1.25+ with the native SQLite C build prerequisites:

```sh
python3 bench/results/change-feeds/prepare_benchmarks.py /path/to/db /path/to/fresh-output
python3 /path/to/fresh-output/run_verified_benchmarks.py /path/to/db
```

The preparation script archives genuine alpha.4 and adds only the two identical
guard harnesses. It builds the comparison module against that source and the
current checkout separately. The runner starts fresh processes serially and
retains all results. Resource/footprint collection here targets macOS; Linux
needs the runner's sysctl metadata calls replaced with its hardware metadata.
The embedded consumer can also be run from native-consumer/ with
`GOWORK=off go run . /path/to/fresh-data`.


## Published alpha.5 release verification

[v0.2.0-alpha.5](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.5) is a
published public prerelease, tagged at `2dbc4d428c82e281f1489dd450004a6d33b3adfc`.
The release preparation updates documentation/packaging and adds change-feed
stress to release verification; production Go source is unchanged from the
benchmarked/validated runtime. The tag remains on the verified release commit;
this evidence follow-up changes no runtime code.

- [Exact release-commit source CI](https://github.com/apteva/db/actions/runs/38547185723)
  passed Linux/macOS vet, build, unit tests, full race suites, two-minute feed
  stress and Ubuntu fuzzing. Local 60-second cursor/feed/soak tests under race
  detection passed in 190.058 seconds before the tag was pushed.
- [Release workflow](https://github.com/apteva/db/actions/runs/38548706415)
  passed tag/version matching, Linux/macOS vet/full race and 60-second
  cursor/feed/soak checks, all four builds and publication. Duplicate
  tag-triggered source CI was canceled because the same commit had already
  passed source CI; independent release verification completed successfully.
- Downloaded all Linux/macOS amd64/arm64 archives and verified every SHA256SUMS
  entry. Each archive includes CHANGE_FEEDS.md and the alpha.5 README.
- Downloaded native CLI/server report alpha.5, format 10. The CLI verified both
  the embedded smoke database and the stopped daemon database.
- A fresh public consumer resolves alpha.5 through proxy.golang.org with
  sum.golang.org checksum verification, no local replace and no workspace.
  Its origin hash equals the release commit. Public go install also passes.
- The public embedded API verified exact integer filtering, projection,
  consistent snapshot handoff, source/view events in one commit, compaction,
  reopen with persistent capture, resume and computed-membership Delete.
- The downloaded daemon plus public HTTP Go client verified snapshot find/query,
  filtered/projected delivery and reconnect/resume. SIGTERM with an idle watch
  completed in 17.5 ms with exit code zero.

Metadata, checksums, logs, the public consumer source and verification script
are retained under release-alpha5/. The original benchmark caveats above remain
applicable; publication does not turn noisy timings into a no-regression claim.
