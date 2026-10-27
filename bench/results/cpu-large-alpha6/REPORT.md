# Large-operation CPU benchmark

Measured on Apple M1 Pro (10 logical CPUs, 16 GiB RAM), Go 1.25.1, macOS. Three fresh serial processes per engine/dataset/order. CPU is process user + system time from getrusage, including all engine/GC/background threads. Lower CPU seconds per million operations is better. These are controlled embedded key/value operations, not a comparison of SQL/document query languages. All 48 successful processes passed 864 measured phases; every engine/dataset/order group has three trials.

The C binding is crawshaw.io/sqlite v0.3.2 and bundles SQLite 3.32.2; the pure-Go binding uses SQLite 3.46.0 (zombiezen v1.4.0 / modernc v1.33.1). The C result is version-specific and is not a latest-SQLite claim. SQLite stores BLOB keys/values in a WITHOUT ROWID table. This layout, page size, compression and caches differ from the other engines and affect storage/CPU. All engines use asynchronous/non-fsync writes and per-row atomic mutations. 64 MiB cache settings are used; SQLite has a separate cache per connection, Pebble also has a 64 MiB memtable, and AptevaDB derives a 32 MiB memtable from its configured cache. These are not equal total-process memory limits. Native file mappings, WAL/page buffers, fixtures and C allocations are included in process memory measurements.

Reads repeat two complete shuffled passes. Scans repeat five passes and access full keys/values through public Go APIs, checking lengths and first/last-byte sums. SQLite copies columns into Go buffers; AptevaDB/Pebble expose borrowed callback slices. The ten-reader phase includes reader/connection setup and newly opened SQLite caches. Fixtures remain alive throughout each process. No cold OS-cache claim is made.

Updates change 10% of existing values. Mixed operations perform 80% point reads and 20% updates. Deletes remove 10% of the rows. Write tables include the following maintenance phase: full checkpoint/compaction for AptevaDB, flush/compaction for Pebble, WAL checkpoint for SQLite. Result validation, value checks, GC and background engine work within measured intervals count toward CPU; initial fixture generation and compilation do not.

Background applications, memory pressure and disk-space fluctuations were present. All successful trials are retained, including slower samples, with median/min/max, CPU/wall, allocations, GC, sampled RSS, lifetime peaks and background snapshots. A disk-space preflight interrupted the initial run before the third fixture; it resumed without rerunning or discarding the two completed SQLite trials. A live disk-space monitor stops fixtures below 128 MiB free and removes only their disposable databases. The initial 250K-row SQLite C load was aborted by that guard, with no completed trial counted. We measured the additional 100K x 1KiB case while space was constrained, then resumed and completed the original 250K case after disk space recovered. The aborted reason remains beside the completed retry; all three successful trials are used.

## CPU seconds per million logical operations

### 100,000 rows, 1024 bytes/value, random insertion

| Operation | AptevaDB | Pebble | SQLite C | SQLite Go |
|---|---:|---:|---:|---:|
| Load + maintenance | 7.810 | 12.497 | 74.780 | 126.791 |
| Point read hit | 0.730 | 9.662 | 16.899 | 19.291 |
| Point read miss | 0.302 | 5.660 | 15.650 | 19.202 |
| Full value scan | 0.085 | 0.550 | 2.266 | 1.287 |
| 10-reader point hits | 1.423 | 25.763 | 198.546 | 167.257 |
| Update + maintenance | 42.841 | 94.965 | 63.088 | 85.432 |
| 80% read / 20% update + maintenance | 5.511 | 15.266 | 26.866 | 34.637 |
| Delete + maintenance | 36.357 | 55.986 | 48.325 | 53.153 |
| Scan after delete | 0.090 | 0.555 | 2.368 | 1.255 |

| Engine | Trials | Peak RSS MiB | Peak physical footprint MiB |
|---|---:|---:|---:|
| AptevaDB alpha.6 | 3 | 675.5 | 437.6 |
| Pebble 2.1.7 | 3 | 260.8 | 250.3 |
| SQLite C 3.32.2 | 3 | 1012.3 | 1021.9 |
| SQLite Go 3.46.0 | 3 | 947.5 | 946.1 |

Peaks are process-lifetime high-water marks across load, reads, mutations, maintenance and close. They include shared workload fixture arrays, not just database memory. Physical footprint accounts for compressed memory and differs from RSS in how file-backed pages are charged. Neither peak is a phase-only peak.

### 250,000 rows, 1024 bytes/value, random insertion

| Operation | AptevaDB | Pebble | SQLite C | SQLite Go |
|---|---:|---:|---:|---:|
| Load + maintenance | 8.279 | 18.222 | 58.259 | 138.743 |
| Point read hit | 0.984 | 9.120 | 30.237 | 24.920 |
| Point read miss | 0.437 | 8.531 | 24.759 | 23.997 |
| Full value scan | 0.087 | 0.563 | 2.614 | 1.610 |
| 10-reader point hits | 2.213 | 23.816 | 211.250 | 184.809 |
| Update + maintenance | 38.878 | 51.938 | 79.454 | 83.563 |
| 80% read / 20% update + maintenance | 5.803 | 14.620 | 54.831 | 49.632 |
| Delete + maintenance | 33.970 | 39.425 | 48.031 | 53.300 |
| Scan after delete | 0.095 | 0.563 | 2.588 | 1.888 |

| Engine | Trials | Peak RSS MiB | Peak physical footprint MiB |
|---|---:|---:|---:|
| AptevaDB alpha.6 | 3 | 1576.7 | 1155.4 |
| Pebble 2.1.7 | 3 | 519.6 | 513.4 |
| SQLite C 3.32.2 | 3 | 1126.7 | 1222.2 |
| SQLite Go 3.46.0 | 3 | 1638.5 | 1646.9 |

Peaks are process-lifetime high-water marks across load, reads, mutations, maintenance and close. They include shared workload fixture arrays, not just database memory. Physical footprint accounts for compressed memory and differs from RSS in how file-backed pages are charged. Neither peak is a phase-only peak.

### 1,000,000 rows, 100 bytes/value, random insertion

| Operation | AptevaDB | Pebble | SQLite C | SQLite Go |
|---|---:|---:|---:|---:|
| Load + maintenance | 3.665 | 5.293 | 23.057 | 44.252 |
| Point read hit | 0.774 | 8.489 | 3.834 | 4.917 |
| Point read miss | 0.476 | 7.909 | 3.241 | 4.503 |
| Full value scan | 0.020 | 0.102 | 0.409 | 0.191 |
| 10-reader point hits | 1.651 | 22.618 | 15.003 | 31.520 |
| Update + maintenance | 9.095 | 12.702 | 18.415 | 22.614 |
| 80% read / 20% update + maintenance | 1.973 | 9.605 | 6.994 | 8.543 |
| Delete + maintenance | 8.166 | 11.160 | 17.036 | 19.468 |
| Scan after delete | 0.020 | 0.103 | 0.424 | 0.195 |

| Engine | Trials | Peak RSS MiB | Peak physical footprint MiB |
|---|---:|---:|---:|
| AptevaDB alpha.6 | 3 | 975.4 | 811.7 |
| Pebble 2.1.7 | 3 | 325.7 | 319.9 |
| SQLite C 3.32.2 | 3 | 1162.0 | 1163.2 |
| SQLite Go 3.46.0 | 3 | 1525.1 | 1526.3 |

Peaks are process-lifetime high-water marks across load, reads, mutations, maintenance and close. They include shared workload fixture arrays, not just database memory. Physical footprint accounts for compressed memory and differs from RSS in how file-backed pages are charged. Neither peak is a phase-only peak.

### 1,000,000 rows, 100 bytes/value, seq insertion

| Operation | AptevaDB | Pebble | SQLite C | SQLite Go |
|---|---:|---:|---:|---:|
| Load + maintenance | 1.989 | 2.556 | 14.164 | 16.802 |
| Point read hit | 1.023 | 7.972 | 3.773 | 4.935 |
| Point read miss | 0.884 | 7.564 | 3.464 | 4.547 |
| Full value scan | 0.021 | 0.101 | 0.397 | 0.177 |
| 10-reader point hits | 2.010 | 21.663 | 13.421 | 31.589 |
| Update + maintenance | 11.118 | 11.603 | 18.539 | 24.577 |
| 80% read / 20% update + maintenance | 2.144 | 9.194 | 7.215 | 8.680 |
| Delete + maintenance | 8.594 | 10.534 | 17.826 | 19.916 |
| Scan after delete | 0.020 | 0.112 | 0.423 | 0.186 |

| Engine | Trials | Peak RSS MiB | Peak physical footprint MiB |
|---|---:|---:|---:|
| AptevaDB alpha.6 | 3 | 895.8 | 614.3 |
| Pebble 2.1.7 | 3 | 324.7 | 358.3 |
| SQLite C 3.32.2 | 3 | 1099.8 | 1163.3 |
| SQLite Go 3.46.0 | 3 | 1522.3 | 1526.6 |

Peaks are process-lifetime high-water marks across load, reads, mutations, maintenance and close. They include shared workload fixture arrays, not just database memory. Physical footprint accounts for compressed memory and differs from RSS in how file-backed pages are charged. Neither peak is a phase-only peak.

## Files and reproduction

`cpu-summary.csv` includes median throughput and average cores alongside CPU. `cpu-summary.json` retains every per-operation sample and ranges. Dataset directories contain full raw phase measurements, stdout/stderr, metadata, source copies/patches, binary SHA256 and module dependency versions. The standard harness uses Bloom filters (10 bits/key) at each Pebble level and default compression settings. The root database implementation was unchanged from released alpha.6.

```sh
cd bench
go vet ./...
go build -o /tmp/apteva-large-cpu-bench .
python3 run_large_cpu.py --binary /tmp/apteva-large-cpu-bench --out /tmp/apteva-cpu-results
python3 run_resources.py --binary /tmp/apteva-large-cpu-bench --out /tmp/apteva-cpu-results/100k-1KiB --n 100000 --vsize 1024 --reps 3 --readers 10 --cachemb 64 --ourscachemb 64 --no-default --crud --readpasses 2 --scanpasses 5 --workloads random
python3 summarize_large_cpu.py /tmp/apteva-cpu-results
python3 report_large_cpu.py /tmp/apteva-cpu-results
```

The runner refuses fixtures that cannot fit its estimated temporary database/WAL/compaction allowance and reserve. It supports resuming completed trials unchanged. This run was constrained by local free disk space; its measured settings, commands and successful/aborted records are preserved.
