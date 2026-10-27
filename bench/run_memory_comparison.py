#!/usr/bin/env python3
"""Interleaved unchanged/candidate AptevaDB, Pebble and C SQLite; serial fixtures."""
import argparse, hashlib, json, pathlib, subprocess
p = argparse.ArgumentParser()
p.add_argument('--binary', required=True)
p.add_argument('--baseline-binary', required=True)
p.add_argument('--prior-binary')
p.add_argument('--out', required=True)
p.add_argument('--engines', default='ours,pebble,sqlite-c')
a = p.parse_args()
root = pathlib.Path(a.out).resolve()
root.mkdir(exist_ok=True)
runner = pathlib.Path(__file__).with_name('run_resources.py').resolve()
cases = [('million-100B', 1000000, 100, 'random,seq'),
         ('100k-1KiB', 100000, 1024, 'random'),
         ('quarter-million-1KiB', 250000, 1024, 'random')]
commands = []
for label, n, size, order in cases:
    cmd = ['python3', str(runner), '--binary', a.binary, '--baseline-binary', a.baseline_binary,
           '--out', str(root / label), '--n', str(n), '--vsize', str(size), '--reps', '3',
           '--readers', '10', '--cachemb', '64', '--ourscachemb', '64', '--engines', a.engines,
           '--no-default', '--crud', '--readpasses', '2', '--scanpasses', '5', '--workloads', order]
    commands.append(cmd)
    if a.prior_binary: cmd.extend(['--prior-binary', a.prior_binary])
(root / 'matrix.json').write_text(json.dumps({'commands': commands, 'expected_processes': 12*(len(a.engines.split(','))+1+bool(a.prior_binary)),
    'candidate_sha256': hashlib.sha256(pathlib.Path(a.binary).read_bytes()).hexdigest(),
    'baseline_sha256': hashlib.sha256(pathlib.Path(a.baseline_binary).read_bytes()).hexdigest(),
    'prior_sha256': hashlib.sha256(pathlib.Path(a.prior_binary).read_bytes()).hexdigest() if a.prior_binary else None,
    'method': 'Three trials; unchanged alpha.6 interleaved with candidate and selected competitors. Same fixture/cache option. Retain all trials. No builds/tests/profiling during measurement.'}, indent=2) + '\n')
for cmd in commands:
    if pathlib.Path(cmd[cmd.index('--out') + 1]).exists():
        cmd.append('--resume')
    subprocess.run(cmd, check=True)
