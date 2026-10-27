# Bounded aggregation and resident memory: validation against alpha.7

2026-10-27. **Pre-publication benchmark evidence; file format remains 10.**

Ordered aggregation substantially reduces CPU and memory. Opt-in cold-page reclamation substantially reduces idle RSS. Unordered spill completes a budget-constrained workload alpha.7 rejects, at a real CPU/I/O cost compared with an unconstrained RAM execution. **These measurements do not establish zero regressions everywhere.**

## Implementation

- `AggregateEach`/`RunEach` stream owned results under one snapshot. Proven exact scalar compound-index order emits completed groups; equality prefixes, mixed directions and typed numeric/null ties are supported. Explicit ordered prefixes also accelerate the existing materializing API. A limit can stop input early.
- Opt-in bounded hash partitions preserve complete typed accumulator state (including both signed-128 limbs), with recursive repartitioning and stable external row sorts. Shared execution memory/disk budgets, context cancellation, private scratch leases and crash cleanup cover all blocking stages.
- `MemoryStats` separates allocated private page/descriptor/memtable/WAL buffers, leased/pinned storage, aggregate reservations, mapped residency estimates and actual process RSS. `MappedCacheBytes`/`TrimMemory` reclaim cold file translations; `ManagedMemoryBytes` applies soft pre-WAL admission/backpressure. Defaults retain the existing cache policy.
- A measured follow-up moved the disabled-tracking test outside the read helper and reduced warm tracking to one hot-bit load. It also avoids marking private-page accesses as mapped reads. The first cohort is retained separately.

See [usage](../../../USAGE.md#bounded-native-aggregation-alpha8), [memory contracts](../../../OPERATIONS.md#resident-memory-controls-alpha8) and [harness instructions](../../aggregation_memory/README.md).

## Method and identities

Baseline: released `v0.2.0-alpha.7`, commit `4124a0f592168c6109e06d864d7c9535f4486001`, extracted with `git archive`. Only the identical benchmark harness was copied into that source. Candidate: working tree based on `9ff2cd0`, engine source unchanged during each cohort. See [identities](source-identity.json), [engine hashes](candidate-source-sha256.json), [patch](candidate.patch), [added source](candidate-final-added-source/) and [build metadata](build-info-final.txt). Alpha.7 remains the Version string in both builds; **use binary/source hashes to distinguish them**.

Host: MacBookPro18,1, Apple M1 Pro, 10 physical cores, 16 GiB RAM; Darwin 27.0.0 arm64; Go 1.25.1. Asynchronous writes, CacheBytes=64 MiB, GOMAXPROCS=10. No agent builds/tests ran during timing. Other user/system applications remained active; every run retains a process/swap sample. Filesystem cache, scheduling and CPU frequency are uncontrolled. Ranges overlap on several short cases; no significance or idle-machine claim is made.

The final matrix has **90 fresh processes**: 15 cases × 2 builds × 3 repetitions, 300,000 documents, 100,000 groups (100 in low-cardinality cases). Keys are 9 bytes; documents contain integer group/id/value fields and 128 bytes of padding. Cold-page fixtures use 512 bytes of padding. A separate **20-process guard cohort** uses 1,000,000 documents, sequential/random insertion and 5 repetitions per build. Thus the final source has 110 trials: 107 successful, plus exactly 3 expected alpha.7 ErrLimit results. Every successful emitted group/count/sum, scanned key/value checksum, serial hit and parallel hit is checked.

One explicit GC follows fixture checkpoint/compaction in both builds, outside the measured workload; none is forced during measured phases. Caller fixtures, permutations, checksums and group-validation bitsets are included in RSS/footprint. CPU is process user+system time; wall time includes scheduler delays. Lifetime peak RSS and macOS physical footprint include load/index/maintenance/close. A post-trim RSS value is an idle measurement, not a lifetime peak. The core builds without CGO; only the benchmark libproc footprint probe uses CGO.

Raw trials are losslessly compressed, including per-process JSON/stdout/stderr, aggregate runs.json and background samples; [archive checksums](raw-archive-sha256.json) are retained. Final raw trials: [300k matrix](final-read-guard-matrix/raw-trials.tar.gz), [all phase medians/ranges](final-read-guard-matrix/summary.csv), [one-million guards](million-document-guards/raw-trials.tar.gz), [guard medians/ranges](million-document-guards/summary.csv). The [earlier 90-run cohort](final-matrix/raw-trials.tar.gz) uses the pre-read-guard candidate and is **not pooled** with final measurements. Two earlier 100k exploratory cohorts remain in the external evidence directory recorded in source-identity.json.

## Final 300,000-document results

Medians, alpha.7 → final candidate. CPU/wall are milliseconds. Peak measures include fixtures. Legacy APIs use equal budgets; streaming comparisons use the existing alpha.7 materializing API because that version has no group streaming interface.

| Case | CPU ms | Wall ms | Lifetime peak RSS MiB | Lifetime peak footprint MiB |
|---|---:|---:|---:|---:|
| cold | 312.171 → 296.299 (-5.1%) | 311.765 → 296.928 (-4.8%) | 200.8 → 201.7 (+0.4%) | 151.9 → 157.3 (+3.6%) |
| crud | 254.718 → 248.271 (-2.5%) | 255.254 → 247.356 (-3.1%) | 100.3 → 99.9 (-0.4%) | 91.5 → 90.7 (-0.8%) |
| crud-random | 253.046 → 254.890 (+0.7%) | 252.432 → 254.801 (+0.9%) | 99.7 → 100.6 (+0.8%) | 92.9 → 94.5 (+1.7%) |
| hash-legacy | 109.095 → 106.697 (-2.2%) | 79.286 → 76.379 (-3.7%) | 137.9 → 138.6 (+0.5%) | 92.6 → 90.8 (-1.9%) |
| hash-parallel | 309.240 → 315.207 (+1.9%) | 84.134 → 88.223 (+4.9%) | 216.7 → 210.1 (-3.1%) | 152.0 → 145.2 (-4.5%) |
| hash-stream | 98.056 → 98.166 (+0.1%) | 78.973 → 73.125 (-7.4%) | 138.5 → 131.7 (-4.9%) | 92.1 → 91.5 (-0.7%) |
| low-legacy | 25.771 → 25.103 (-2.6%) | 25.519 → 24.868 (-2.6%) | 97.0 → 98.6 (+1.6%) | 91.0 → 92.6 (+1.7%) |
| low-parallel | 44.677 → 37.725 (-15.6%) | 8.371 → 7.457 (-10.9%) | 97.3 → 101.0 (+3.8%) | 91.4 → 94.9 (+3.8%) |
| low-stream | 25.296 → 27.534 (+8.8%) | 27.298 → 27.228 (-0.3%) | 106.5 → 95.7 (-10.1%) | 100.6 → 89.6 (-10.9%) |
| ordered-legacy | 640.342 → 172.791 (-73.0%) | 328.968 → 138.063 (-58.0%) | 338.3 → 229.6 (-32.1%) | 258.3 → 174.4 (-32.5%) |
| ordered-limit-legacy | 625.830 → 0.092 (-100.0%) | 300.803 → 0.088 (-100.0%) | 333.9 → 232.5 (-30.4%) | 252.6 → 171.8 (-32.0%) |
| ordered-limit-stream | 605.958 → 0.104 (-100.0%) | 296.578 → 0.099 (-100.0%) | 349.3 → 230.0 (-34.1%) | 266.5 → 172.8 (-35.2%) |
| ordered-stream | 620.751 → 133.495 (-78.5%) | 310.465 → 130.518 (-58.0%) | 348.8 → 234.1 (-32.9%) | 267.5 → 170.9 (-36.1%) |
| spill-same-budget | ErrLimit → completes | ErrLimit → completes | Failed baseline; see raw | Failed baseline; see raw |
| spill-versus-ram | 102.892 → 414.626 (+303.0%) | 75.354 → 262.430 (+248.3%) | 138.0 → 96.8 (-29.8%) | 90.4 → 90.7 (+0.3%) |

`cold` CPU/wall in this table describe warm point hits before explicit trimming. CRUD CPU/wall describe checked point hits; complete write/maintenance and all other phases are in the CSV. The lower-budget disk-vs-RAM row is deliberately a tradeoff comparison, not an equal-budget speed claim.

Ordered legacy execution: CPU **640.3 → 172.8 ms (-73.0%)**, wall **329.0 → 138.1 ms (-58.0%)**, lifetime peak RSS **338.3 → 229.6 MiB (-32.1%)**, lifetime peak physical footprint **258.3 → 174.4 MiB (-32.5%)**. Its post-aggregation RSS is **338.3 → 138.9 MiB (-58.9%)**. New ordered streaming uses 473 bytes of accounted group state on this scalar fixture, excluding borrowed input/snapshots/cache/caller storage. It still allocates owned output maps, so that number is not total process memory or cumulative allocation.

The ordered streaming limit reads **31 input rows** to return 10 complete groups; full traversal reads 300,000. Both report `Strategy=ordered-index`, `Index=g`, zero spill files. The extra input is group-boundary lookahead.

At the same **16 MiB** budget, alpha.7 returns `ErrLimit` on all three trials, while final streaming returns all 100,000 groups with 8,655,419 peak accounted bytes and 42,600,000 temporary bytes, 16 spill files and one partition-merge pass. The baseline failure is not treated as faster completed work.

Against alpha.7 allowed **128 MiB RAM**, final 16 MiB spill lowers lifetime peak RSS **138.0 → 96.8 MiB (-29.8%)**, but uses **4.03× CPU** and **3.48× elapsed time**. Post-aggregation physical footprint drops **73.0 → 17.0 MiB**, while lifetime peak footprint is essentially flat (**90.4 → 90.7 MiB**, fixture work dominates). Cumulative allocated Go bytes rise about **331%**. Disk use is opt-in; no claim that spill beats RAM execution is justified.

## Cold mapped pages

| Measurement | Alpha.7 → candidate |
|---|---:|
| RSS after first trim, MiB | 200.7 → 42.7 (-78.7%) |
| RSS after reread and second trim, MiB | 200.8 → 42.7 (-78.7%) |
| Warm point-hit CPU ms | 312.171 → 296.299 (-5.1%) |
| Checked full reread after trim, CPU ms | 308.989 → 311.282 (+0.7%) |
| Checked full reread after trim, wall ms | 307.769 → 310.263 (+0.8%) |

Candidate trimming costs about **14–16 ms CPU per explicit sweep** on this fixture. RSS falls about **79%**, but a full reread faults pages back in, restoring roughly 201 MiB RSS before another trim. Lifetime peak RSS stays essentially unchanged (~201 MiB); lifetime peak footprint is **151.9 → 157.3 MiB (+3.6%)**, not an improvement. No hard RSS cap is claimed. `mincore` measures file page-cache residency; MemoryStats reports that separately from process RSS.

## One-million-document default-policy guards

Five fresh trials per cell. These cases keep both new memory targets disabled. Each entry below gives median [min, max], in milliseconds, and the median delta. Completed writes are load + settle checkpoint/compaction + 100,000 updates and their checkpoint/compaction; calculate each run's sum before taking the median. Individual phases remain available in the CSV.

| Fixture / phase | CPU ms, alpha.7 → candidate | Wall ms, alpha.7 → candidate |
|---|---:|---:|
| crud / completed-writes | 1323.58 [1263.37, 1488.73] → 1336.08 [1270.77, 1407.03] (+0.9%) | 1763.70 [1457.32, 2107.14] → 1669.76 [1382.85, 2124.18] (-5.3%) |
| crud / point-hits | 993.54 [981.71, 1051.12] → 1001.24 [996.70, 1021.03] (+0.8%) | 993.72 [981.17, 1062.46] → 1002.92 [996.49, 1037.72] (+0.9%) |
| crud / parallel-hits-10 | 1121.13 [1115.09, 1171.38] → 1140.56 [1125.00, 1171.29] (+1.7%) | 149.02 [142.44, 186.25] → 148.71 [146.38, 153.78] (-0.2%) |
| crud / scan | 44.12 [43.07, 48.19] → 43.88 [43.45, 44.44] (-0.6%) | 44.09 [43.03, 49.43] → 43.73 [43.53, 44.29] (-0.8%) |
| crud / point-hits-after-update | 1004.24 [989.10, 1084.17] → 1001.82 [981.42, 1115.06] (-0.2%) | 1003.60 [989.08, 1163.38] → 1001.41 [979.66, 1245.29] (-0.2%) |
| crud-random / completed-writes | 2841.69 [2830.95, 2901.27] → 2896.37 [2745.86, 3055.14] (+1.9%) | 3708.51 [3458.64, 4028.23] → 3840.01 [3317.97, 4174.62] (+3.5%) |
| crud-random / point-hits | 982.20 [974.75, 997.54] → 997.81 [988.23, 1028.42] (+1.6%) | 982.13 [975.57, 997.80] → 997.86 [988.81, 1090.69] (+1.6%) |
| crud-random / parallel-hits-10 | 1138.92 [1114.02, 1162.58] → 1151.98 [1117.77, 1159.32] (+1.1%) | 146.43 [139.03, 160.32] → 155.48 [146.82, 210.17] (+6.2%) |
| crud-random / scan | 44.00 [43.19, 44.30] → 43.92 [43.41, 45.25] (-0.2%) | 43.92 [43.05, 44.21] → 43.89 [43.29, 45.29] (-0.1%) |
| crud-random / point-hits-after-update | 1001.64 [987.89, 1040.18] → 996.02 [992.15, 1180.96] (-0.6%) | 1000.91 [988.16, 1375.89] → 994.72 [990.45, 1455.73] (-0.6%) |

Completed sequential-write wall median improves **5.3%** with **0.9%** more CPU. Completed random-write medians use **1.9% more CPU** and **3.5% more wall time**. Serial read CPU medians are **0.8–1.6% slower**, scans about **0.2–0.6% faster**, ten-reader CPU **1.1–1.7% slower**; random-fixture ten-reader elapsed median is **6.2% slower**. Ranges overlap; that does not prove a regression, and it also does not prove equivalence. Lifetime peak RSS is within ~0.4% in these guards, with no blanket memory win.

On the shorter 300k matrix, parallel hash aggregation is **1.9% CPU / 4.9% wall slower**; new low-cardinality streaming has an **8.8% CPU** median increase (about 2.2 ms) and a flat wall median. Legacy low-cardinality aggregation improves about 2.6%. Every measured path and range is retained. **The “faster with less memory everywhere” acceptance target is not established on this machine.**

## Correctness and build evidence

- [Full race suite](final-race.txt): root 580.423 s, client 2.713 s, all packages passed on the implementation before the final read-guard optimization.
- [Final full ordinary suite](final-normal.txt): root 222.993 s, client 0.972 s, all packages passed after that optimization.
- [Final focused race suite](final-read-guard-race.txt): 24.237 s, covering aggregation/spill/temp cleanup, memory/reclamation, snapshots, persistent readers and reads across writer lifetimes after the final read change.
- Final `go vet ./...` and CGO-disabled `linux/arm64` and `darwin/amd64` builds passed; host runtime tests are macOS ARM64. This is not a Linux runtime validation claim.
- Regression coverage includes signed-128 cancellation after intermediate overflow, integer/float/null/missing index ties, equality/mixed index order, first/last and non-null counts across partitions, chained blocking operators, collector budgets, quota/I/O/cancellation/early-stop cleanup, live/abandoned scratch leases, corrupted record lengths, concurrent snapshot writes, cold remapping with active readers, leased memtable accounting, and pre-WAL pressure rejection/cancellation/resume.

## Contracts and remaining limits

Budgets conservatively account retained execution state and I/O workspace, excluding the current borrowed input, snapshots/page cache, Go runtime overhead and caller-retained maps. One state/record plus minimum workspace must fit. Collecting APIs charge retained results and can still return ErrLimit after spill. Float partial merge order may change IEEE-754 rounding; integer state remains exact. Unsorted group order is unspecified. A streaming callback can observe output before a later error. New APIs/options are native Go features without a new HTTP streaming endpoint.

The managed target is soft pre-write admission, not a hard RSS limit; one atomic write/checkpoint/copy can overshoot and metadata/mappings/caller allocations are separate. Hot mapped chunks may exceed the soft mapped target. Default options preserve the old warm-map policy. The disk quota includes input and output coexistence, defaulting to 1 GiB per execution. Spill data are ephemeral; successful return closes/removes execution scratch, and recovery cleanup skips active OS leases. See the usage/operations documents for the full API contract.

Measurements preceded publication. Alpha.8 packaging updates Version and release documentation after the benchmark freeze; the measured engine source is otherwise unchanged. Publication verification is recorded separately.
