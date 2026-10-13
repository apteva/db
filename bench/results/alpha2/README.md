# Alpha.2 validation

See [the report](VALIDATION.md), `validation.json` summaries/ranges, and every raw output/measurement in `raw/`. `runs.json`/`followups.json` record all 54 successful subprocesses. Three initial compatibility cache-profile trials are explicitly preliminary and excluded from the corrected medians.

Paired raw KV compares unchanged alpha.1 engine source with alpha.2 using the same compiler/fixtures and alternating order. The initial binary was built from the working source immediately before library commit 177a74a; later differences in 41115aa only set the compatibility Pebble cache to 512 MiB. Binary hashes and revisions are recorded. Workstation background activity precludes isolated-machine/no-regression claims. Paths are sanitized.

Native compound cursor/page, nullable unique-index builds, logical/prefix filters, budget rejection and large document storage are compared to Pebble application indexes and SQLite C/Go. Baseline/release versions share fresh fixtures; exact large-integer result validation runs on every timed feature query. Typed/JSON decoding is excluded from raw large-document storage timings.

Reproduce with BENCHMARKS.md commands, `bench/compatibility.go` and manifests (replace <repo>/<output> placeholders with your paths). All outer observations are retained; legacy aggregation/join select inner best-of-five rounded to 0.1 ms. Go allocation metrics omit C/mapped memory and are not RSS.
