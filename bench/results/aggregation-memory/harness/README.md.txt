# Aggregation and resident-memory comparison

This nested module runs identical, validated fixtures against the candidate and
released `v0.2.0-alpha.7`. It adds no engine dependencies. The macOS-only benchmark
footprint probe uses libproc through CGO; the engine itself builds without CGO.

Build this directory normally for the candidate. Copy this **harness directory
only** into a clean alpha.7 source archive and build there with `-tags alpha7`.
Name the binaries `current-bench` and `alpha7-bench` in a separate output root.

```sh
GOWORK=off go build -o /tmp/db-comparison/current-bench .
# In alpha.7's copy of this directory:
GOWORK=off go build -tags alpha7 -o /tmp/db-comparison/alpha7-bench .
# Back here:
python3 run.py --binaries /tmp/db-comparison --out /tmp/db-comparison/results \
  --n 300000 --reps 3
```

Each trial is a fresh process and fixture, with deterministic interleaved ordering.
The runner retains every result, stderr, command, binary SHA, background process
sample and swap sample. It deletes only its successful owned fixture, checks free
space before each run, and fails on unexpected errors. The same-budget alpha.7
spill comparison is expected to return ErrLimit; every other trial must succeed.
Do not run builds/tests beside resource measurements.

Common settings: asynchronous durability, CacheBytes=64 MiB, GOMAXPROCS=10,
9-byte keys, encoded documents containing integer group/id/value fields and a
128-byte padding string (512 bytes in the cold-page case). One explicit GC follows
fixture checkpoint/compaction in both builds. No explicit GC occurs inside timed
phases. Checksums, input permutations and group-validation bitsets are identical
fixture costs and remain included in memory measurements. All emitted group
IDs/counts/sums and full KV value checksums are validated.

Cases include legacy serial/parallel aggregation, new streaming counterparts,
ordered full/early-limited groups, 100 low-cardinality groups, 100,000 groups,
sequential/random writes, serial/ten-reader hits, scans, updates plus completed
checkpoint/compaction, and cold mapping reclamation with a full checked reread.
Budgets and requested API appear in every result. Alpha.7's streaming counterpart
uses its existing materializing API; it has no streaming group interface.
Disk-vs-RAM compares 16 MiB spilling with 128 MiB in-memory execution and must
be reported as a memory/time tradeoff, separately from equal-budget completion.

CPU is user+system process time; elapsed includes scheduler delays. RSS/footprint
are process measures including fixtures. Lifetime peaks include maintenance/close.
Cold-page post-trim RSS is **idle RSS**, not a lower lifetime peak; rereads can
refault reclaimed pages. Maxrss units and libproc footprint in this harness are
validated for macOS; do not interpret another platform's output identically.
Background applications and filesystem cache are uncontrolled; report all medians
and ranges, and do not infer universal no-regression from small sample differences.
