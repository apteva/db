"""Build matching baseline/current binaries before running measurements."""
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

if len(sys.argv) != 3:
    raise SystemExit('usage: python3 prepare_benchmarks.py /path/to/db /path/to/output')
repo, out = (Path(p).resolve() for p in sys.argv[1:])
out.mkdir(parents=True, exist_ok=True)
old = out / 'alpha4-source'
if old.exists():
    raise SystemExit(f'{old} already exists; choose a fresh output directory')
old.mkdir()
baseline = 'bff9d0e956989bb566c48addb34377955a402f50'
archive = subprocess.check_output(['git', 'archive', baseline], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(old, filter='data')
for name in ['change_feed_guard_bench_test.go', 'change_feed_resource_guard_test.go']:
    shutil.copyfile(repo / name, old / name)
matched = out / 'matched-bench'
matched.mkdir()
for source in (repo / 'bench').iterdir():
    if source.is_file() and (source.suffix == '.go' or source.name in ['go.mod', 'go.sum']):
        shutil.copyfile(source, matched / source.name)
env = {**os.environ, 'GOWORK': 'off'}
def go(args, cwd):
    subprocess.run(['go', *args], cwd=cwd, env=env, check=True)
go(['mod', 'edit', '-replace=github.com/apteva/db='+str(old)], matched)
go(['test', '-c', '-o', str(out/'alpha4.test'), '.'], old)
go(['build', '-o', str(out/'alpha4-bench'), '.'], matched)
go(['test', '-c', '-o', str(out/'verified.test'), '.'], repo)
go(['build', '-o', str(out/'verified-bench'), '.'], repo/'bench')
(out/'baseline.json').write_text(json.dumps({'baseline':baseline}, indent=2)+'\n')
shutil.copyfile(repo/'bench/results/change-feeds/run_verified_benchmarks.py', out/'run_verified_benchmarks.py')
print('Builds complete. Stop other tests/builds, then run:')
print(f'python3 {out/"run_verified_benchmarks.py"} {repo}')
