# Alpha.8 publication verification

Published public prerelease: [v0.2.0-alpha.8](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.8).
Tag commit: `4f57eb9b66baa03f292977e442323cedd821f68e`. Format remains 10.

- [Pre-tag source CI](https://github.com/apteva/db/actions/runs/40564786211): Linux/macOS vet, builds, unit tests, full race checks, change-feed stress and Ubuntu fuzz smoke passed.
- Local release-commit cursor, mixed-operation soak and change-feed stress ran for two minutes each under the race detector: passed in 368.493 seconds.
- [Tag CI](https://github.com/apteva/db/actions/runs/40567418435): all Linux/macOS jobs passed.
- [Release workflow](https://github.com/apteva/db/actions/runs/40567418410): both OS race/stress/soak gates, all four archive builds and publication passed. The Linux verification runner took longer than the other jobs; it completed successfully.
- A fresh consumer resolved the tagged module through proxy.golang.org/sum.golang.org without a replace or workspace, verified its source commit, built the embedded/HTTP Go APIs and installed the CLI.
- Public native APIs passed exact signed-128 integer spill/cancellation arithmetic, disk quota/cleanup, ordered snapshot aggregation/early limit, memory accounting, cold remapping and Verify checks. Existing count, computed collection, snapshot, change-feed replay and large-value/compaction checks also passed.
- Downloaded and checked all four Linux/macOS amd64/arm64 archives against SHA256SUMS. Static build metadata from all eight binaries matches the named OS/architecture, CGO=0, the clean tag commit and compiler metadata. This does not claim runtime execution on every architecture.
- Ran the downloaded macOS ARM64 CLI/server and public Go HTTP driver, including snapshot count, filtered/projected watch and idle-watch shutdown. SIGTERM closed the watch and exited successfully in about 18 ms.
- Created a format-10 directory with the public alpha.7 module; public alpha.8 opened, checked, updated, reopened and verified its large values. No new migration was needed.

[Raw verification output](release-verification.txt), [module identity](public-module.json), [archive checksums](release-checksums.json), [binary build metadata](archive-build-metadata.json) and [release metadata](release-metadata.json) are retained beside the verification programs and compressed CI logs. Downloaded executables, runtime databases and module caches remain outside the repository.

Benchmark results and tradeoffs are in the [aggregation/memory report](../REPORT.md). Measurements preceded publication; release packaging changed the version/documentation after the benchmark freeze, with the measured engine otherwise unchanged. No claim of zero performance regressions or universally lower memory is made.
