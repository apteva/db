# Alpha.7 publication verification

Published tag: `v0.2.0-alpha.7`, commit `4124a0f592168c6109e06d864d7c9535f4486001`, format 10.

Source CI and the independent release workflow passed Linux/macOS vet, full unit/race suites, fuzz checks and stress/soak gates. All four Linux/macOS amd64/arm64 archive hashes match the published SHA256SUMS. Downloaded Darwin/arm64 CLI/server binaries report the expected version. The public Go proxy and sumdb resolve the exact tagged commit without a workspace or local replacement; public `go install` succeeds.

The public embedded consumer checks covering/fallback/snapshot counts, exact integers, computed collections, filtered/projected change feeds, snapshot handoff, compaction and replay. A bounded 8-MiB cache exercise checks 10 MiB of large values through WAL writes, snapshot retention, checkpoints, compaction and reopen. The downloaded daemon passes public HTTP-driver count/watch/snapshot tests and clean idle-watch shutdown. Public alpha.7 opens, verifies, updates and reopens a format-10 directory produced by public alpha.6 with 80-KiB values.

## Duplicate tag CI failure and test-only correction

The duplicate tag CI run 40502212749 failed on Linux even though source CI and release verification passed. Its data race was a diagnostic read of `faultCtl.writes` in `crash_test.go`, concurrent with the fault injector's locked counter update. Two HTTP fixture calls also exceeded their 100-ms overall timeout. Both test-harness defects exist in alpha.6 as well; neither required a production engine or driver change.

Commit `e3f7de9e674a799c36503e0451c9f426d16d5efd` locks the diagnostic counter read and allows ordinary fixture RPCs five seconds. A separate client retains the 100-ms timeout for the existing long-lived watch test, preserving that assertion. Fifteen repeated race-instrumented fault-injection tests and fifteen complete HTTP-driver package passes succeeded locally. The follow-up full Linux/macOS CI run is 40505284518; its status at evidence capture is retained explicitly. The published engine, assets and immutable tag are unchanged. The failed CI log is retained as compressed evidence.

`verify_release.py` reproduces publication checks. `collect_evidence.py` captures the successful source/release CI metadata and logs. Database directories, module caches, executables and downloaded archives remain outside the repository. Consumer Go source is retained as `.go.txt`.
