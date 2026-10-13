# Public alpha validation

[Validation report](VALIDATION.md) for `v0.2.0-alpha.1`. All 30 resource trials and 24 corrected feature trials completed successfully on October 13, 2026. The root engine matches the release tag; later source changes fix benchmark fixtures and supported engine selection only.

`validation.json` contains median/min/max resource and feature measurements. `resources-runs.json` retains every resource trial including phase CPU/wall/allocations/GC, sampled and OS-reported peaks, and background activity. `resources-metadata.json` records hardware and settings. `features-corrected.json` records every feature process; `raw/` holds its stdout/stderr and computed measurements. Local paths are replaced with placeholders.

Aggregation and join queries use the best of five inner runs, with three outer runs retained. Their printed timings round to 0.1 ms. The other feature and resource tables report medians across all three fresh-process repetitions. CPU/RSS resource comparisons use async writes; durable comparisons are separate. Background applications and swapping were present, so these results are not an isolated-machine comparison or proof of no regression.

The first aggregation attempt failed because the harness lacked SQLite Go support and used SQL empty-aggregate semantics that differed from the document pipeline. It was excluded from the corrected 24 successful trials. The original failed run remains in the local validation output; no failed run is counted as a successful measurement.

Reproduce using the commands/settings in BENCHMARKS.md, `bench/run_resources.py`, and the command arrays in the feature manifest, with fresh output directories. All SQL/document results are checked; aggregation/join mismatches now fail the process.

Release/install verification: [release workflow](https://github.com/apteva/db/actions/runs/38394476107) and [tag CI](https://github.com/apteva/db/actions/runs/38394477415) passed. All four archives matched the published SHA256SUMS. The darwin/arm64 CLI/server reported version 0.2.0-alpha.1. A fresh consumer fetched the tag from the Go proxy and passed embedded plus remote CRUD against the downloaded durable daemon. Both tagged `go install` commands and graceful daemon shutdown passed.
