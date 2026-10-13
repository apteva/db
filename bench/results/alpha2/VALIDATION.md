# AptevaDB alpha.2 validation

Library source: `177a74a8ae0a6998930aef5ec64aab9a7a00faff`; release tag `v0.2.0-alpha.2` at `41115aaa65a1c81de471e7f6e94c1db9f833f3af` (only a benchmark cache-setting fix differs).

All 54 fresh-process comparisons succeeded: 33 initial processes plus 21 follow-ups. Three initial compatibility processes used Pebble default cache and are retained as preliminary; all feature medians below use the three corrected 512 MiB-cache comparisons. No failed result is counted as success.

Apple M1 Pro; 16 GiB RAM; go version go1.25.1 darwin/arm64; GOMAXPROCS=10.

The feature fixture has 50,000 documents, 32 tenants, exact int64 scores above 2^53 and literal-prefix strings containing punctuation/percent/underscore characters. Pages return up to 100 rows. Every ordered key/score is validated against an independent integer model, for every timed query. Query timings average 30 inner queries; medians/ranges retain all three fresh-process runs. Loads use 1,000-row batches/transactions; async/WAL enabled. SQLite uses typed index columns plus the same binary payload; Pebble maintains indexes in application batches and does not supply the native schema/constraint API.

Native uses its default unlimited page cache; the explicit Pebble/SQLite cache setting is 512 MiB (SQLite per connection), Pebble memtable 64 MiB. These are not equal process-memory budgets. Background applications were active; their CPU activity is recorded before the initial trials. No cold-cache/isolated-machine or universal/no-regression claim is made.

| Engine | 100-row compound page µs | 100-row cursor µs | 80 KB raw document read µs |
|---|---:|---:|---:|
| ours | 92.95 | 24.74 | 4.62 |
| pebble-manual | 227.57 | 135.11 | 14.52 |
| sqlite-c | 100.01 | 73.29 | 28.96 |
| sqlite-go | 138.16 | 82.75 | 117.70 |

Native indexed pages are 60.4× faster than its forced collection-scan/fallback-sort page on this fixture. Ordered pages retain no result array; cursor/input/snapshot storage is separate from accounted retained row memory. Large-document timings copy/store the validated binary payload; JSON decoding is outside that timing. Go allocation counters exclude C/native/mapped storage and are not RSS.

## Paired alpha.1 versus alpha.2

Same fixtures/compiler, fresh processes, alternating order, three retained trials. Positive change means slower. These small samples on an active workstation show costs and variability; they do not establish statistical significance.

| Feature | Phase | Alpha.1 median ns/op | Alpha.2 median ns/op | Change | Alpha.2 min–max |
|---|---|---:|---:|---:|---:|
| kv | random/find-hit | 656.0 | 660.0 | +0.6% | 659.0–702.0 |
| kv | random/find-hit-par10 | 87.0 | 88.0 | +1.1% | 80.0–92.0 |
| kv | random/find-miss | 427.0 | 431.0 | +0.9% | 423.0–436.0 |
| kv | random/insert | 763.0 | 764.0 | +0.1% | 759.0–783.0 |
| kv | random/scan | 46.0 | 48.0 | +4.3% | 47.0–48.0 |
| kv | random/settle | 555.0 | 665.0 | +19.8% | 596.0–1623.0 |
| kv | seq/find-hit | 952.0 | 941.0 | -1.2% | 915.0–967.0 |
| kv | seq/find-hit-par10 | 136.0 | 150.0 | +10.3% | 136.0–163.0 |
| kv | seq/find-miss | 860.0 | 871.0 | +1.3% | 824.0–871.0 |
| kv | seq/insert | 186.0 | 189.0 | +1.6% | 185.0–211.0 |
| kv | seq/scan | 47.0 | 48.0 | +2.1% | 47.0–51.0 |
| kv | seq/settle | 1259.0 | 592.0 | -53.0% | 430.0–1266.0 |
| computed-fields | insert | 929.1 | 999.9 | +7.6% | 918.6–1021.3 |
| computed-fields | upsert | 755.6 | 747.9 | -1.0% | 739.1–772.2 |
| computed-fields | read-derived | 270.0 | 273.4 | +1.3% | 253.8–274.8 |
| computed-fields | read-derived-10 | 38.0 | 42.4 | +11.7% | 38.9–51.5 |
| computed-fields | read-derived-32 | 32.9 | 33.0 | +0.3% | 27.4–39.9 |
| computed-fields | index-build+verify | 1354.4 | 1454.5 | +7.4% | 1364.1–1482.3 |
| computed-fields | indexed-filter | 36716.7 | 33731.2 | -8.1% | 32123.0–37675.0 |
| computed-collections | insert | 1714.4 | 1726.7 | +0.7% | 1678.3–1733.9 |
| computed-collections | upsert | 2103.9 | 2118.8 | +0.7% | 2057.3–2214.3 |
| computed-collections | delete | 1002.7 | 850.8 | -15.2% | 819.8–931.7 |
| computed-collections | query-all | 67587.0 | 65606.1 | -2.9% | 64977.5–68028.9 |
| computed-collections | point-query | 127.3 | 129.6 | +1.8% | 127.2–130.6 |
| computed-collections | point-par10 | 19.2 | 20.1 | +4.8% | 18.1–21.6 |
| computed-collections | point-par32 | 21.4 | 21.0 | -2.3% | 18.1–21.2 |

Most raw KV write/hit/miss medians differ by about 1–2%. Some parallel-read medians and computed-field inserts/index build+verify are slower; indexed computed-field filters and computed-collection deletes are faster. Exact index keys and safe validation have real costs. Settle timings have wide variation from asynchronous background maintenance and filesystem activity; no interpretation as a pure CPU regression is warranted. The bounded-cache footprint limitation versus Pebble from alpha.1 is not claimed fixed by query budgets.

## Detailed feature measurements

| Engine | Phase | Median ns/op | Min–max ns/op | Go allocated bytes/op |
|---|---|---:|---:|---:|
| ours | build-compound | 707.6 | 655.8–725.5 | 345.1 |
| ours | build-nullable-unique | 374.6 | 369.5–377.6 | 135.7 |
| ours | compound-page | 92954.2 | 89073.6–94983.3 | 9238.4 |
| ours | fallback-sort-page | 5613095.8 | 5516427.8–5715779.2 | 128344.0 |
| ours | large-document-read | 4617.1 | 4479.7–4999.0 | 304.0 |
| ours | large-document-write | 22178.7 | 21961.6–23537.4 | 615.8 |
| ours | load | 131.9 | 122.2–132.0 | 5.3 |
| ours | memory-limit-rejection | 3236.1 | 3123.6–3893.1 | 5968.0 |
| ours | ordered-cursor | 24743.1 | 24045.8–26179.2 | 2144.0 |
| pebble | large-document-read | 14517.0 | 13301.0–15943.2 | 216.8 |
| pebble | large-document-write | 373100.9 | 368462.9–426043.9 | 27184.4 |
| pebble-manual | build-compound | 574.0 | 565.4–623.7 | 49.6 |
| pebble-manual | build-nullable-unique | 89.3 | 87.7–89.4 | 16.0 |
| pebble-manual | compound-page | 227565.3 | 218694.4–269316.7 | 3919.5 |
| pebble-manual | load | 816.0 | 708.1–835.9 | 34.9 |
| pebble-manual | ordered-cursor | 135105.5 | 130544.5–151390.3 | 1712.5 |
| sqlite-c | build-compound | 395.4 | 356.9–400.8 | 0.0 |
| sqlite-c | build-nullable-unique | 219.3 | 218.8–239.6 | 0.0 |
| sqlite-c | compound-page | 100005.6 | 98607.0–107155.6 | 3376.0 |
| sqlite-c | large-document-read | 28960.1 | 28395.2–35965.7 | 208.0 |
| sqlite-c | large-document-write | 326835.9 | 266571.3–334785.5 | 64.0 |
| sqlite-c | load | 1959.3 | 1655.3–2198.6 | 104.1 |
| sqlite-c | ordered-cursor | 73291.7 | 69052.8–80818.1 | 3328.0 |
| sqlite-go | build-compound | 708.8 | 685.9–791.2 | 0.0 |
| sqlite-go | build-nullable-unique | 406.4 | 397.3–519.4 | 0.0 |
| sqlite-go | compound-page | 138159.7 | 137494.4–139709.7 | 1680.0 |
| sqlite-go | large-document-read | 117703.7 | 113975.2–124006.8 | 176.0 |
| sqlite-go | large-document-write | 447893.2 | 423648.8–545419.9 | 16.0 |
| sqlite-go | load | 1839.3 | 1803.5–2483.2 | 8.0 |
| sqlite-go | ordered-cursor | 82748.6 | 81015.3–87102.8 | 1696.0 |

## Correctness and release checks

The reproduced alpha.1 integer-filter and 80 KB document failures pass as regressions. Tests cover exact numeric order against a rational model; all four compound direction combinations; literal/NUL prefix ranges; nullable/sparse uniqueness and atomic swaps; ordered cursor snapshots/cancellation; memory rejection and shared parallel budgets; extended/nested/large-array computed documents; maximum-size key splits/compaction; restart/export/import; embedded/remote parity; and the preserved real format-5 fixture. Local short, full race, targeted race and document/pipeline/page fuzz runs passed. Linux/macOS CI for the tagged engine and benchmark cache fix passed. All four published archives matched SHA256SUMS; native commands reported alpha.2/format 7. A fresh tagged-module consumer passed embedded/remote CRUD plus exact map decoding, logical/prefix filters, compound order, plans/stats and 80 KB documents against the downloaded daemon. Tagged command installations and graceful daemon shutdown passed.

Legacy aggregation/join timings remain best-of-five inner query timings, rounded to 0.1 ms, with all three outer observations retained. Raw outputs and JSON manifests contain all measurements and source/binary provenance. The benchmark runner retained initial preliminary feature outputs rather than overwriting them.

Release evidence: [tag CI](https://github.com/apteva/db/actions/runs/38412864116), [release verify/build/publish](https://github.com/apteva/db/actions/runs/38412864233).
