#!/usr/bin/env python3
"""Parse raw trial outputs into summary.json: median, min and max per metric."""
import json, re, statistics, sys
from pathlib import Path

out = Path(sys.argv[1])
trials = json.loads((out / "trials.json").read_text())
series = {}  # (suite, build, engine, metric) -> [values]


def add(suite, build, engine, metric, v):
    series.setdefault((suite, build, engine, metric), []).append(float(v))


for t in trials:
    if t["exit"] != 0:
        sys.exit(f"trial failed: {t}")
    text = (out / "raw" / f"{t['suite']}-{t['build']}-t{t['trial']}.txt").read_text().split("\n--- stderr")[0]
    s, b = t["suite"], t["build"]
    if s.startswith("spill"):
        d = json.loads(text[text.index("{"):text.rindex("}") + 1])
        for k in ("wall", "user", "system", "allocated", "gc", "peak_rss"):
            add(s, b, "ours", k, d[k])
        add(s, b, "ours", "spilled_bytes", d["stats"]["SpilledBytes"])
        add(s, b, "ours", "merge_passes", d["stats"]["MergePasses"])
        continue
    if s == "agg":
        lines = text.splitlines()
        h = next(i for i, l in enumerate(lines) if l.startswith("query"))
        engines = lines[h].split()[1:]
        for l in lines[h + 1:]:
            if not l.strip() or l.startswith("--- stderr"):
                break
            m = re.match(r"^(.*?)\s{2,}(\S.*)$", l)
            if not m:
                continue
            for e, v in zip(engines, m.group(2).split()):
                if v != "-":
                    add(s, b, e, m.group(1).strip(), v)
        continue
    workload, cols = None, None
    for l in text.splitlines():
        m = re.match(r"^workload=(\w+)", l)
        if m:
            workload, cols = m.group(1), None
            continue
        if workload and l.startswith("engine"):
            cols = l.split()[1:]
            continue
        if workload and cols and l.strip():
            parts = l.split()
            for c, v in zip(cols, parts[1:]):
                add(s, b, parts[0], f"{workload} {c}", v.rstrip("MB"))
        elif not l.strip():
            cols = None

summary = [{"suite": s, "build": b, "engine": e, "metric": m, "trials": len(v),
            "median": statistics.median(v), "min": min(v), "max": max(v), "values": v}
           for (s, b, e, m), v in sorted(series.items())]
(out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
print(f"{len(summary)} series from {len(trials)} trials")
