#!/usr/bin/env python3
"""Interleaved fresh-process trials: dashboard (all engines, current build)
and -agg/-join regression (ours/ours-par, alpha.10 vs current). Writes raw
outputs and a medians/ranges summary."""
import os, re, statistics, subprocess, sys, json

S = os.path.dirname(os.path.abspath(__file__))
out = sys.argv[1]
trials = int(sys.argv[2]) if len(sys.argv) > 2 else 5
os.makedirs(out, exist_ok=True)
builds = {"alpha10": os.path.join(S, "kvbench-a10"), "alpha11": os.path.join(S, "kvbench")}
runs = [
    ("dashboard", ["alpha11"], ["-dashboard", "-n", "1000000", "-reps", "3"]),
    ("agg", ["alpha10", "alpha11"], ["-agg", "-n", "50000", "-engines", "ours,ours-par"]),
    ("join", ["alpha10", "alpha11"], ["-join", "-n", "100000", "-engines", "ours,ours-par"]),
]
row = re.compile(r"^(.+?)\s{2,}((?:[-\d.]+\s*)+)$")
data = {}
only = sys.argv[3].split(",") if len(sys.argv) > 3 else None
for t in range(trials):
    for name, bs, args in runs:
        if only and name not in only:
            continue
        order = bs if t % 2 == 0 else list(reversed(bs))
        for b in order:
            load = os.getloadavg()[0]
            r = subprocess.run([builds[b]] + args, capture_output=True, text=True)
            raw = os.path.join(out, f"{name}-{b}-{t}.txt")
            with open(raw, "w") as f:
                f.write(f"# load1 {load:.2f}\n" + r.stdout + r.stderr)
            if r.returncode:
                print("FAILED", raw, file=sys.stderr)
                continue
            header, prev, section = None, "", ""
            for line in r.stdout.splitlines():
                if line.startswith("query"):
                    header, section = line.split()[1:], prev.strip()[:20]
                    continue
                if line.strip():
                    prev = line
                m = row.match(line.rstrip())
                if header and m:
                    vals = m.group(2).split()
                    if len(vals) != len(header):
                        continue
                    for e, v in zip(header, vals):
                        if v != "-":
                            data.setdefault((name, (section + ": " if name == "join" else "") + m.group(1).strip(), b, e), []).append(float(v))
            print(f"trial {t} {name} {b} load {load:.1f}", flush=True)
summary = []
for (name, q, b, e), v in sorted(data.items()):
    summary.append({"bench": name, "query": q, "build": b, "engine": e,
                    "median": statistics.median(v), "min": min(v), "max": max(v), "n": len(v)})
with open(os.path.join(out, "summary.json"), "w") as f:
    json.dump(summary, f, indent=1)
for s in summary:
    print(f"{s['bench']:9} {s['build']:7} {s['engine']:9} {s['query'][:44]:44} {s['median']:9.2f} [{s['min']:.2f}-{s['max']:.2f}]")
