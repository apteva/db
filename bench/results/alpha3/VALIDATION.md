# AptevaDB alpha.3 validation

Public tag `v0.2.0-alpha.3` at `26220edae0452a03a5cb5375016ca9eaf1e95209`. Library benchmark source `e975c28f1572dd91daf19aaae42aa7cd1b8c6c32`; the tag adds a snapshot regression test. Baseline is the alpha.2 library at `41115aaa65a1c81de471e7f6e94c1db9f833f3af`.

All 64 fresh processes succeeded. Final medians use the seven matched-harness native pairs, three matched-harness compatibility pairs and three optimized wide-entry processes. Earlier generic-split/older-harness samples are retained as preliminary; they are not silently discarded.

Apple M1 Pro, 16 GiB RAM, Go 1.25.1, GOMAXPROCS=10. Processes/directories are fresh and serial; paired order rotates. No local builds/tests run during measurements. Background applications remain active and their CPU activity is retained. Cache settings are not equal process-memory budgets. These observations cannot establish a universal no-regression or memory lead.

## Reported failures and contracts

- Reused snapshot index cursors now resume correctly with SeekAfter/SeekBefore after unrelated traversal in either direction, including same-leaf/cross-leaf moves, memtables, tombstones and large keys. Saved Position bytes must still be copied; the API returns borrowed bytes.
- Complete encoded index entries support 8,160 bytes (`MaxIndexKeySize`), independent of the 4,032-byte user-key limit. A 1,500-byte primary key plus a 3,000-byte indexed ASCII value fits. The limit includes prefixes, every component, NUL escaping, terminators and the primary-key length suffix; oversized inserts/replacements/backfills fail atomically.
- Existing key validation and leaf/internal cuts are retained when entries fit. Wider entries trigger a split that includes the incoming cell size. Small empty index values remain inline.
- Format 8 upgrades formats 3–7 with two durable checkpoints before writes; older binaries refuse the directory. Back up with the original binary first. Page/WAL layouts and logical export version 2 remain unchanged.

## Large-entry comparison

10,000 rows; identical 1,500-byte ASCII keys, 3,000-byte indexed ASCII strings and binary document payloads. Loads use 1,000-row batches/transactions and asynchronous writes. Native scalar compound index, application-maintained Pebble index, and SQL index `(v,pk)`. Each timed page validates every key/payload against an independent ordinal model. Pages average 100 inner queries, returning 100 rows each.

| Engine | Load µs/row | Build index µs/row | 100-row page µs |
|---|---:|---:|---:|
| ours | 3.46 | 12.71 | 271.71 |
| pebble-manual | 6.37 | 6.14 | 344.43 |
| sqlite-c | 71.36 | 51.10 | 486.82 |
| sqlite-go | 87.68 | 54.40 | 1989.25 |

Native reused-cursor resumption plus a 100-row page took 247.66 µs. It includes saving an earlier position, seeking 200 rows later, then resuming the earlier position on the same cursor. This is a different operation mix from the fresh-cursor page.

Native reads/load lead these samples; Pebble manual index construction is faster than native construction. SQLite uses typed index columns plus the same binary payload; Pebble does not provide the native schema/constraint API. Go allocation counters exclude C/native/mapped memory and are not RSS.

## Paired ordinary workloads

Seven paired 1,000,000-key trials; 16-byte keys and 100-byte values. Both libraries use the identical current harness and compiler. Positive change means slower. Integer ns/op in the legacy output are rounded.

| Workload / phase | Alpha.2 ns/op | Alpha.3 ns/op | Change | Alpha.3 min–max |
|---|---:|---:|---:|---:|
| random/find-hit | 673 | 674 | +0.1% | 653–889 |
| random/find-hit-par10 | 94 | 87 | -7.4% | 79–99 |
| random/find-miss | 441 | 436 | -1.1% | 420–484 |
| random/insert | 769 | 747 | -2.9% | 726–823 |
| random/scan | 47 | 48 | +2.1% | 47–48 |
| random/settle | 1049 | 946 | -9.8% | 908–1752 |
| seq/find-hit | 970 | 954 | -1.6% | 937–2275 |
| seq/find-hit-par10 | 141 | 136 | -3.5% | 129–158 |
| seq/find-miss | 877 | 879 | +0.2% | 865–1093 |
| seq/insert | 201 | 189 | -6.0% | 183–191 |
| seq/scan | 47 | 49 | +4.3% | 48–50 |
| seq/settle | 824 | 763 | -7.4% | 733–1018 |

Write and parallel-read medians improved; single-reader hits were essentially unchanged. Raw scans were 1–2 ns/row (about 2–4%) slower. Active-workstation variation is substantial: earlier runs included larger slowdowns in other engines too. All trials/ranges are retained.

## Existing compatibility features

Three matched-harness 50,000-document pairs retain exact integer filters, mixed sort directions, literal prefix/logical filters, nullable uniqueness, query plans/budgets and 80 KB binary documents. Independent key/score checks run for every timed query.

| Native phase | Alpha.2 median ns/op | Alpha.3 median ns/op | Change | Alpha.3 min–max |
|---|---:|---:|---:|---:|
| build-compound | 725.9 | 742.5 | +2.3% | 707.1–748.4 |
| build-nullable-unique | 385.8 | 377.3 | -2.2% | 374.6–381.0 |
| compound-page | 98776.4 | 96425.0 | -2.4% | 93047.2–99363.9 |
| fallback-sort-page | 5700426.4 | 5634902.8 | -1.1% | 5582156.9–5721477.8 |
| large-document-read | 5033.8 | 5024.7 | -0.2% | 4694.4–5099.7 |
| large-document-write | 26988.9 | 31412.8 | +16.4% | 24755.2–33223.6 |
| load | 141.7 | 137.2 | -3.2% | 136.8–137.4 |
| memory-limit-rejection | 2941.7 | 2695.8 | -8.4% | 2647.2–3540.3 |
| ordered-cursor | 24897.2 | 24530.6 | -1.5% | 23829.2–27744.4 |

Native small-entry ordered cursors took about 24.5 µs per 100-row page; indexed query pages about 96.4 µs. Index build was about 2.3% slower; the short 128-document large-write sample median was about 16.4% slower, with overlapping ranges and unchanged value-spill semantics. These costs are disclosed; no universally-faster claim is made. Detailed competitor medians and Go allocation counters are in validation.json. The existing bounded-cache footprint limitation versus Pebble is not claimed fixed.

## Verification

Local full race suites passed (348.5 s for the initial change; 346.5 s for the fast-path split revision), plus targeted race checks of the final inline-value predicate, remote driver, saved-position regressions, boundary/WAL recovery and snapshots. More than 517,000 split fuzz cases passed. Tests cover randomized mixed cells through 8,160 bytes, updates/deletes/checkpoints/compaction, ordinary/compound/descending/unique backfills, NUL escaping and atomic size rejection.

A genuine format-7 fixture generated by alpha.2 verifies the two metadata-slot upgrade and existing 80 KB documents/exact indexes. Wide snapshot cursors preserve rows across updates/checkpoint/index deletion; compaction correctly refuses active snapshots and succeeds after Close. A fresh public Go-proxy consumer verified alpha.3/format 8, wide keys/indexes, reused resume positions, atomic limit rejection and restart.

[Engine CI](https://github.com/apteva/db/actions/runs/38424057564) passed on Linux/macOS including race and fuzz. [Exact-tag CI](https://github.com/apteva/db/actions/runs/38426766854) and [release verification/build/publish](https://github.com/apteva/db/actions/runs/38426766835) passed, including full Linux/macOS race and 60-second stress/soak gates. All four downloaded archives matched SHA256SUMS; the native downloaded commands and fresh tagged command installations reported alpha.3/format 8. The downloaded daemon passed health/version, wide compound keys, 80 KB documents, exact integers, ordered streaming query plans/counts with the public tagged driver, and graceful shutdown. An alpha.2 writer refused the format-8 test directory without altering data.

The first tagged Ubuntu unit attempt exposed a pre-existing nondeterministic checksum test: random lookups did not guarantee eviction of the deliberately damaged cached page, while checksum validation is specified on first load. The tag CI passed on retry (attempt 2; original logs remain available). Main commit `04a9fda831c911f9b01144af20c0cbe4420babd5` makes eviction explicit; 20 local race repetitions passed. This is a test correction; production checksum behavior and tagged library code are unchanged.
