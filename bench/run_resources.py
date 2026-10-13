#!/usr/bin/env python3
"""Run resource benchmarks serially in isolated processes; preserve every trial."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import time


def capture(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def background():
    rows = []
    for line in capture(["ps", "-Ao", "pid=,pcpu=,comm="]).splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) == 3:
            rows.append({"pid": int(parts[0]), "cpu_percent": float(parts[1]), "command": parts[2]})
    rows.sort(key=lambda row: row["cpu_percent"], reverse=True)
    return {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summed_cpu_percent": sum(row["cpu_percent"] for row in rows), "top": rows[:8],
            "swap": capture(["sysctl", "vm.swapusage", "kern.memorystatus_vm_pressure_level"]),
            "vm_stat": capture(["vm_stat"])}


def summarize(out, runs):
    groups = {}
    for run in runs:
        groups.setdefault((run["profile"], run["workload"], run["engine"]), []).append(run)
    summary = []
    for (profile, workload, engine), trials in sorted(groups.items()):
        entry = {"profile": profile, "workload": workload, "engine": engine, "trials": len(trials)}
        for field in ["process_peak_rss_bytes", "fixture_rss_bytes", "baseline_rss_bytes", "disk_bytes",
                      "process_peak_memory_footprint_bytes", "fixture_memory_footprint_bytes", "baseline_memory_footprint_bytes"]:
            values = [t[field] for t in trials]
            entry[field] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
        entry["phases"] = {}
        load_cpu = [sum(p["user_seconds"] + p["system_seconds"] for p in t["phases"]
                        if p["phase"] in ("insert", "settle")) for t in trials]
        entry["load_cpu_seconds"] = {"median": statistics.median(load_cpu), "min": min(load_cpu), "max": max(load_cpu)}
        for p in trials[0]["phases"]:
            measures = [next(x for x in t["phases"] if x["phase"] == p["phase"]) for t in trials]
            metrics = {}
            for key in p:
                if key == "phase":
                    continue
                values = [x[key] for x in measures]
                metrics[key] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
            entry["phases"][p["phase"]] = metrics
        summary.append(entry)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    for s in summary:
        cpu = s["load_cpu_seconds"]["median"]
        print(f'SUMMARY {s["profile"]} {s["workload"]} {s["engine"]}: load_cpu={cpu:.3f}s '
              f'peak_rss={s["process_peak_rss_bytes"]["median"]/2**20:.1f}MiB '
              f'peak_footprint={s["process_peak_memory_footprint_bytes"]["median"]/2**20:.1f}MiB', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--readers", type=int, default=10)
    parser.add_argument("--cachemb", type=int, default=64)
    parser.add_argument("--ourscachemb", type=int, default=64)
    parser.add_argument("--workloads", default="random,seq")
    parser.add_argument("--no-default", action="store_true", help="skip the additional AptevaDB default-cache trials")
    args = parser.parse_args()
    if min(args.n, args.reps, args.readers) < 1:
        parser.error("n, reps and readers must be positive")
    workloads = args.workloads.split(",")
    if args.cachemb < 1 or args.ourscachemb < 0 or any(w not in ("random", "seq") for w in workloads):
        parser.error("invalid cache or workload selection")
    profile = "bounded64" if (args.cachemb, args.ourscachemb) == (64, 64) else f"cache{args.cachemb}-ours{args.ourscachemb}"
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    bench = Path(__file__).resolve().parent
    binary = str(Path(args.binary).resolve())
    metadata = {
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "n": args.n, "reps": args.reps, "readers": args.readers, "value_bytes": 100,
        "cache_mb": args.cachemb, "ours_cache_mb": args.ourscachemb, "workloads": workloads,
        "cpu": capture(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "logical_cpus": capture(["sysctl", "-n", "hw.logicalcpu"]),
        "memory_bytes": capture(["sysctl", "-n", "hw.memsize"]),
        "os": capture(["sw_vers"]), "go": capture(["go", "version"]),
        "git_head": capture(["git", "rev-parse", "HEAD"], bench),
        "git_status": capture(["git", "status", "--short"], bench),
        "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "background_initial": background(),
        "profiles": {profile: f"{args.cachemb} MiB Pebble/SQLite cache setting (SQLite per connection); {args.ourscachemb} MiB AptevaDB cache (0=default/unlimited); Pebble additionally has 64 MiB memtable",
                     "ours-default": "AptevaDB default unlimited page cache; comparison to its bounded64 setting"},
        "method": "Serial fresh subprocesses/directories; deterministic rotated trial order; retain all observations, report median/min/max. Async writes. Workload fixtures included in process RSS. No cold OS-cache claim.",
    }
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (out / "build-info.txt").write_text(capture(["go", "version", "-m", binary]) + "\n")
    (out / "bench-go.mod").write_text((bench / "go.mod").read_text())
    (out / "source.patch").write_text(capture(["git", "diff"], bench))
    for name in ["resources.go", "resources_memory_darwin.go", "resources_memory_other.go", "run_resources.py"]:
        (out / name).write_text((bench / name).read_text())
    runs = []
    entries = [(profile, "ours"), (profile, "pebble"), (profile, "sqlite-c"), (profile, "sqlite-go")]
    if not args.no_default:
        entries.append(("ours-default", "ours"))
    total = args.reps * len(workloads) * len(entries)
    for rep in range(1, args.reps + 1):
        jobs = [(profile, engine, workload) for workload in workloads for profile, engine in entries]
        random.Random(42 + rep).shuffle(jobs)
        for profile, engine, workload in jobs:
            stem = f"{profile}-{engine}-{workload}-r{rep}"
            scratch = out / (stem + "-data")
            scratch.mkdir()
            command = [binary, "-resources", "-engines", engine, "-workloads", workload,
                       "-n", str(args.n), "-vsize", "100", "-par", str(args.readers),
                       "-cachemb", str(args.cachemb), "-ourscachemb", "0" if profile == "ours-default" else str(args.ourscachemb),
                       "-dir", str(scratch)]
            bg = background()
            print(f"RUN {len(runs)+1}/{total} {stem}", flush=True)
            start = time.monotonic()
            with (out / (stem + ".stderr.txt")).open("w") as stderr:
                proc = subprocess.run(command, stdout=subprocess.PIPE, stderr=stderr, text=True,
                                      env={**os.environ, "GOMAXPROCS": str(args.readers)}, timeout=600)
            elapsed = time.monotonic() - start
            (out / (stem + ".stdout.json")).write_text(proc.stdout)
            if proc.returncode:
                raise RuntimeError(f"{stem} failed ({proc.returncode}); see stderr log")
            run = json.loads(proc.stdout)
            run.update(profile=profile, repetition=rep, command=command, elapsed_seconds=elapsed, background_before=bg)
            (out / (stem + ".json")).write_text(json.dumps(run, indent=2) + "\n")
            runs.append(run)
            scratch.rmdir()
            (out / "runs.json").write_text(json.dumps(runs, indent=2) + "\n")
            peak = run["process_peak_rss_bytes"] / 2**20
            print(f"DONE {len(runs)}/{total} {stem}: {elapsed:.1f}s, peak_rss={peak:.1f}MiB", flush=True)
    metadata["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata["background_final"] = background()
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summarize(out, runs)


if __name__ == "__main__":
    main()
