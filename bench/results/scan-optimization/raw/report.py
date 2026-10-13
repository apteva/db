from pathlib import Path
import hashlib, json, re, statistics, subprocess

OUT = Path(__file__).resolve().parent
REPO = Path("<repo>")
DEST = REPO / "bench/results/scan-optimization"
PHASES = ["insert", "settle", "find-hit", "find-miss", "scan", "find-hit-par10"]

def stats(values):
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "samples": values, "n": len(values)}

def standard(pattern):
    values = {}
    for path in sorted(OUT.glob(pattern)):
        workload = None
        for line in path.read_text().splitlines():
            if line.startswith("workload="):
                workload = line.split("=")[1].split()[0]
            fields = line.split()
            if workload and len(fields) == 8 and fields[0] in {"ours", "pebble", "sqlite-c", "sqlite-go"}:
                for phase, value in zip(PHASES, fields[1:7]):
                    values.setdefault(f"{fields[0]}/{workload}/{phase}", []).append(float(value))
    return {k: stats(v) for k, v in values.items()}

def micro(pattern):
    values = {}
    for path in sorted(OUT.glob(pattern)):
        for line in path.read_text().splitlines():
            match = re.match(r"(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op(?:\s+([\d.]+) ns/row)?\s+(\d+) B/op\s+(\d+) allocs/op", line)
            if match:
                name, ns, perrow, allocated, allocs = match.groups()
                for metric, value in {"ns/op": float(ns), "B/op": int(allocated), "allocs/op": int(allocs), **({"ns/row": float(perrow)} if perrow else {})}.items():
                    values.setdefault(f"{name}/{metric}", []).append(value)
    return {k: stats(v) for k, v in values.items()}

def features(pattern):
    values = {}
    for path in sorted(OUT.glob(pattern)):
        for item in json.loads(path.read_text()):
            engine = item.get("Engine", item.get("engine"))
            phase = item.get("Phase", item.get("phase"))
            for field, spellings in {"NsPerOp": ["NsPerOp", "ns_per_op"], "AllocatedBytesPerOp": ["AllocatedBytesPerOp", "allocated_bytes_per_op"], "DiskBytes": ["DiskBytes", "disk_bytes"]}.items():
                for spelling in spellings:
                    if spelling in item:
                        values.setdefault(f"{engine}/{phase}/{field}", []).append(item[spelling])
                        break
    return {k: stats(v) for k, v in values.items()}

primary = {
    "source_commit": (OUT / "finalized-code-commit.txt").read_text().strip(),
    "baseline_alpha3": "26220edae0452a03a5cb5375016ca9eaf1e95209",
    "baseline_alpha2": "41115aaa65a1c81de471e7f6e94c1db9f833f3af",
    "native": {n: standard(f"finalized-million-{n}-bench-r*.stdout") for n in ["alpha3", "finalized"]},
    "micro": {n: micro(f"finalized-api-{n}-raw.test-r*.stdout") for n in ["alpha2-complete", "alpha3-complete", "finalized"]},
    "four_engines": standard("finalized-all-engines-finalized-bench-r1.stdout"),
    "final_features": features("finalized-*-finalized-bench-r1-*measurements.json"),
    "candidate": {"native": {n: standard(f"native-million-{n}-bench-r*.stdout") for n in ["alpha2", "alpha3", "final"]}, "micro": {n: micro(f"raw-api-{n}-raw.test-r*.stdout") for n in ["alpha2", "alpha3", "final"]}},
    "intermediate": {"native": {n: standard(f"protected-million-{n}-bench-r*.stdout") for n in ["alpha3", "protected"]}},
    "benchmark_processes": len(list(OUT.glob("*.stdout"))),
    "ci": json.loads((OUT / "ci.json").read_text()),
}
assert primary["ci"]["conclusion"] == "success"
assert all(v["n"] == 7 for group in primary["native"].values() for v in group.values())
assert len(primary["four_engines"]) == 48
assert all(hashlib.sha256((REPO / name).read_bytes()).hexdigest() == digest for name, digest in json.loads((OUT / "finalized-source.json").read_text()).items())

def number(value):
    return f"{value:,.2f}".rstrip("0").rstrip(".")

lines = ["# Scan optimization validation", "", f"Unreleased `main` implementation at `{primary['source_commit']}`. The published alpha.3 tag is unchanged. Format 8 and the public APIs remain compatible.", "", "Sequential cursor steps avoid arbitrary-seek position comparisons, advance the known leaf slot when no buffered entries participate, and reuse one private value buffer. Arbitrary resume seeks still invalidate unrelated cached positions. Crossing a collection boundary clears the cache, and empty boundary values preserve buffer reuse.", "", "Callback scans copy contiguous inline keys/values together, use three-word cell references, and call the callback directly when the buffered merge is empty. The final implementation preserves the original narrow-range end checks and releases the reader lock before callbacks.", "", "## Measurement conditions", "", "Apple M1 Pro, 16 GiB RAM, darwin/arm64, Go 1.25.1, GOMAXPROCS=10. Standard workloads use 1,000,000 16-byte keys and 100-byte values; asynchronous writes, native default resident cache, Pebble/SQLite 512 MiB configured caches (SQLite per connection). Reads follow native checkpoints, Pebble compactions and SQLite settling. These are steady read-layout workloads, not concurrent write/read throughput tests.", "", "Seven paired native processes use the identical comparison harness linked against alpha.3 and the final library, alternating execution order. The API benchmarks use an identical copied harness for alpha.2, alpha.3 and final, three fresh processes per library, rotating order, one-second measurements and model-checked keys/values. Ranges return 1, 8 or 128 rows; sustained point reads use 1,000,000 random keys. Aggregate/join harnesses retain their existing best-of-five inner-query method, with every outer process retained.", "", "No local builds, tests or soak processes ran during reported benchmark processes. Other user apps, services, builds and tests remained active; every process has a background CPU snapshot. This machine was not isolated. Min/max and every trial are retained, including severe outliers. Measurements do not establish universal regression-free performance.", "", "## Final native cursor walks", "", "Median ns/operation; scan means a counted `Cursor.Next` walk. Other phases are ns/key. Standard output rounds to whole nanoseconds.", "", "| Workload/phase | alpha.3 | Final | Change | alpha.3 range | Final range |", "|---|---:|---:|---:|---:|---:|"]
for workload in ["random", "seq"]:
    for phase in PHASES:
        key = f"ours/{workload}/{phase}"
        a, b = [primary["native"][n][key] for n in ["alpha3", "finalized"]]
        lines.append(f"| {workload}/{phase} | {number(a['median'])} | {number(b['median'])} | {100*(b['median']/a['median']-1):+.1f}% | {number(a['min'])}–{number(a['max'])} | {number(b['min'])}–{number(b['max'])} |")
lines += ["", "Cursor scan time falls from 49 to 38 ns/row for random-built trees and 50 to 39 ns/row for sequential-built trees (about 22%). Random insert and hit medians are 8.6% and 8.0% slower; sequential hits and parallel hits are 3.8% and 3.0% slower. Ranges overlap and background outliers are large. Those slower medians are disclosed rather than attributed definitively to either the engine or CPU activity. No point-read or ordinary insert algorithm was changed.", "", "## Final API measurements", "", "Iteration values are ns/row over 100,000 rows; ranges are ns/query; point reads are ns/read. These include validation overhead and are different operation mixes from the standard counted walks.", "", "| API | alpha.2 median | alpha.3 median | Final median | Final range |", "|---|---:|---:|---:|---:|"]
for name, unit in [("BenchmarkRawIteration/Scan-10", "ns/row"), ("BenchmarkRawIteration/Cursor-10", "ns/row"), ("BenchmarkRawRange/1-10", "ns/op"), ("BenchmarkRawRange/8-10", "ns/op"), ("BenchmarkRawRange/128-10", "ns/op"), ("BenchmarkReaderSteady/Single-10", "ns/op"), ("BenchmarkReaderSteady/Parallel-10", "ns/op")]:
    items = [primary["micro"][n][f"{name}/{unit}"] for n in ["alpha2-complete", "alpha3-complete", "finalized"]]
    lines.append(f"| {name.removeprefix('Benchmark').removesuffix('-10')} | {' | '.join(number(v['median']) for v in items)} | {number(items[-1]['min'])}–{number(items[-1]['max'])} |")
lines += ["", "The callback-scan final median is only modestly better in this noisy series; its range is 17.43–25.95 ns/row. Sustained single/parallel reader medians are slower too, with very large parallel outliers. Small-range and point-read timings need a quiet workstation before making tighter claims.", "", "Go allocations per complete 100,000-row callback scan fall from 78,608 to 74,512 bytes and 28 to 27 allocations. Cursor walks fall from 99,608 to 99,352 bytes and 1,551 to 1,549 allocations. These are Go allocation counts, not peak resident memory or total C/mmap memory.", "", "## Final four-engine comparison", "", "One fresh final process, 1,000,000 rows per workload; ns/row for counted cursor walks. This table is a single-trial comparison. Three earlier candidate four-engine processes are also retained.", "", "| Engine | Random-built scan | Sequential-built scan |", "|---|---:|---:|"]
for engine in ["ours", "pebble", "sqlite-c", "sqlite-go"]:
    values = [primary["four_engines"][f"{engine}/{w}/scan"]["median"] for w in ["random", "seq"]]
    lines.append(f"| {engine} | {' | '.join(number(v) for v in values)} |")
lines += ["", "## Wider feature validation", "", "The first candidate ran three paired outer trials of compatibility, 1,500-byte primary keys with 3,000-byte indexed values, aggregation, joins, computed fields and computed collections against the existing comparison engines. All completed their correctness checks. The final source then ran every one of those modes again; all passed. Its compatibility/wide-index measurements are in `validation.json`; complete aggregate/join/computed tables are in the raw stdout files. Short single runs are checks, not evidence that every query got faster.", "", "The binary-search end-bound candidate was discarded to keep narrow-range comparisons unchanged. Its favorable bulk-scan results are preliminary, not substituted for the final measurements. An intermediate run additionally checked exhausted-cursor cache invalidation. All preliminary, intermediate and final observations are retained.", "", "## Correctness and build checks", "", "- Full local `go test -race ./...`: passed, 364.394 s. Local `CURSOR_STRESS=2m SOAK=2m go test -race -run 'TestCursorStress|TestSoak' -timeout 10m .`: passed, 248.765 s. These were before the final empty-value buffer-retention guard; all cursor/scan/isolation race tests were rerun after that adjustment and passed, 15.080 s.", "- Root vet/build and benchmark-module vet/build: passed.", "- New regressions cover forward/reverse resume at the last valid position after collection exhaustion, mixed-size inline/overflow values, cursor value-buffer isolation, frozen buffered values, bounded scans and repeated snapshot reads.", f"- Exact source commit [CI run 38441188141](https://github.com/apteva/db/actions/runs/38441188141): Linux and macOS vet/build/unit tests and full race suites passed; 45-second document, pipeline, page and wide-page-split fuzz runs passed.", "", "## Evidence and reproduction", "", f"{primary['benchmark_processes']} fresh benchmark processes are retained in `raw/`, including six early screening processes. `validation.json` retains parsed samples, medians and ranges. `raw/finalized-source.json` fingerprints the measured runtime/test source; the files match the implementation commit. Binaries were built from that source before commit stamping; the source was then verified unchanged. Raw runners/commands/build paths are sanitized; binary hashes and complete outputs are retained.", "", "To rerun API measurements on this source:", "", "```sh", "GOMAXPROCS=10 go test -run '^$' -bench 'BenchmarkRaw(Iteration|Range)|BenchmarkReaderSteady' -benchtime=1s -count=3 .", "cd bench", "go build -o /tmp/apteva-scan-bench .", "/tmp/apteva-scan-bench -n 1000000 -vsize 100 -engines ours,pebble,sqlite-c,sqlite-go -workloads random,seq -par 10 -reps 1", "```", "", "For paired libraries, archive each baseline commit, copy the current root benchmark test files into each archive, and compile `go test -c` binaries before measurement. For the standard harness, build identical benchmark-module sources with only their `github.com/apteva/db` replace target changed. Run serial fresh processes/directories, rotate order and retain all outputs. Avoid compiler/test CPU load during measurements.", ""]
DEST.mkdir(parents=True, exist_ok=True)
(DEST / "raw").mkdir(exist_ok=True)
replacements = [(str(REPO), "<repo>"), (str(OUT), "<output>"), (str(Path.home()), "<home>")]
for source in sorted(OUT.iterdir()):
    if source.is_file() and source.suffix in {".stdout", ".stderr", ".background", ".json", ".log", ".txt", ".py"}:
        content = source.read_text()
        for before, after in replacements:
            content = content.replace(before, after)
        (DEST / "raw" / source.name).write_text(content)
(DEST / "validation.json").write_text(json.dumps(primary, indent=2) + "\n")
(DEST / "VALIDATION.md").write_text("\n".join(lines))
print(json.dumps({"report": str(DEST / "VALIDATION.md"), "processes": primary['benchmark_processes'], "ci": primary['ci']['conclusion']}))
