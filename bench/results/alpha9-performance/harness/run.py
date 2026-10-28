#!/usr/bin/env python3
"""alpha.8 vs alpha.9 candidate: fresh process per trial, interleaved, every output kept."""
import argparse, datetime, json, os, shutil, subprocess, time
from pathlib import Path

SUITES = {
    # name: (binary kind, args, which builds); "comp" runs once per trial on the candidate harness
    "kv": ("bench", ["-n", "1000000", "-engines", "ours"], ["alpha8", "cand"]),
    "kv-comp": ("bench", ["-n", "1000000", "-engines", "pebble,sqlite-c"], ["cand"]),
    "batch": ("bench", ["-n", "1000000", "-nopar", "-engines", "ours-batch1000"], ["alpha8", "cand"]),
    "batch-comp": ("bench", ["-n", "1000000", "-nopar", "-engines", "pebble-batch1000,sqlite-c-txn1000"], ["cand"]),
    "agg": ("bench", ["-n", "50000", "-agg", "-engines", "ours,ours-par,pebble,sqlite-c"], ["alpha8", "cand"]),
    "durable": ("bench", ["-n", "1000", "-durable", "-workloads", "random", "-engines", "ours-sync,pebble-sync,sqlite-c-sync"], ["alpha8", "cand"]),
    "spill-count": ("spill", [], ["alpha8", "cand"]),
    "spill-numeric": ("spill", ["-numeric"], ["alpha8", "cand"]),
    "spill-count-typed": ("spill", ["-typed"], ["cand"]),
    "spill-numeric-typed": ("spill", ["-numeric", "-typed"], ["cand"]),
}


def background():
    load = subprocess.check_output(["sysctl", "-n", "vm.loadavg"], text=True).strip()
    top = subprocess.check_output(["ps", "-Ao", "pcpu=,comm="], text=True).splitlines()
    top = sorted((l.strip().split(None, 1) for l in top if l.strip()), key=lambda r: -float(r[0]))[:5]
    return {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "loadavg": load,
            "top": [{"cpu": float(c), "command": n} for c, n in top]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--scratch", required=True)
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--suites", default=",".join(SUITES))
    a = ap.parse_args()
    out, scratch = Path(a.out), Path(a.scratch)
    (out / "raw").mkdir(parents=True, exist_ok=True)
    log = []
    for trial in range(1, a.trials + 1):
        for name in a.suites.split(","):
            kind, args, builds = SUITES[name]
            order = builds if trial % 2 else list(reversed(builds))  # alternate who goes first
            for build in order:
                if scratch.exists():
                    shutil.rmtree(scratch)
                scratch.mkdir(parents=True)
                cmd = [str(Path(a.bin) / f"{kind}-{build}")] + args
                if kind == "bench":
                    cmd += ["-dir", str(scratch)]
                env = dict(os.environ, TMPDIR=str(scratch))
                before = background()
                t = time.time()
                p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=3600)
                stem = f"{name}-{build}-t{trial}"
                (out / "raw" / f"{stem}.txt").write_text(p.stdout + ("\n--- stderr\n" + p.stderr if p.stderr else ""))
                log.append({"suite": name, "build": build, "trial": trial, "command": cmd[1:], "exit": p.returncode,
                            "elapsed_seconds": round(time.time() - t, 3), "background_before": before})
                print(f"{stem}: exit={p.returncode} {time.time() - t:.1f}s load={before['loadavg']}", flush=True)
                (out / "trials.json").write_text(json.dumps(log, indent=1) + "\n")
    shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
