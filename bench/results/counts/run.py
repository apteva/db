#!/usr/bin/env python3
"""Run precompiled binaries serially; retain every sample and CPU snapshot."""
import json, pathlib, subprocess, sys, time
out=pathlib.Path(sys.argv[1]); out.mkdir(parents=True,exist_ok=True)
bench=('/private/tmp/apteva-count-alpha5-bench','/private/tmp/apteva-count-bench')
guards=('/private/tmp/apteva-count-alpha5-guard','/private/tmp/apteva-count-current-guard')
for trial in range(1,4):
    # Alternate order to reduce drift; each process builds a fresh fixture.
    order=(0,1) if trial%2 else (1,0)
    for version in order:
        label='alpha5' if version==0 else 'alpha6'
        cpu=subprocess.check_output(['ps','-Ao','pid,pcpu,comm'],text=True)
        (out/f'{trial}-{label}-cpu.txt').write_text(cpu)
        cmd=[bench[version],'-test.run=^$','-test.bench=BenchmarkCount','-test.benchtime=1s','-test.count=1','-test.timeout=10m']
        with (out/f'{trial}-{label}-count.txt').open('w') as f:
            subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True)
        print(f'{trial} {label} counts complete',flush=True)
    for version in order:
        label='alpha5' if version==0 else 'alpha6'
        cmd=[guards[version],'-test.run=^$','-test.bench=BenchmarkRawIteration|BenchmarkRawRange|BenchmarkChangeFeedDisabledGuard','-test.benchtime=1s','-test.count=1','-test.timeout=10m']
        with (out/f'{trial}-{label}-guards.txt').open('w') as f:
            subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True)
        print(f'{trial} {label} guards complete',flush=True)
