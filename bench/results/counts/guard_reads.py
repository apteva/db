#!/usr/bin/env python3
"""Isolate read guards from the preceding timed write phase; keep all trials."""
import pathlib,subprocess,sys
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
for trial in range(1,4):
    order=('alpha5','alpha6') if trial%2 else ('alpha6','alpha5')
    for version in order:
        binary='/private/tmp/apteva-count-alpha5-guard' if version=='alpha5' else '/private/tmp/apteva-count-current-guard'
        cmd=[binary,'-test.run=^$','-test.bench=BenchmarkChangeFeedDisabledGuard/Find$|BenchmarkRawIteration/Cursor$','-test.benchtime=3s','-test.count=1','-test.timeout=10m']
        with (out/f'{trial}-{version}-isolated-reads.txt').open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True)
        print(trial,version,'isolated reads complete',flush=True)
