#!/usr/bin/env python3
"""Serial large CPU/CRUD matrix; no build or agent tests during measured runs."""
import argparse,json,pathlib,subprocess
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--out',required=True);args=p.parse_args()
root=pathlib.Path(args.out).resolve();root.mkdir(exist_ok=True)
runner=pathlib.Path(__file__).resolve().with_name('run_resources.py')
profiles=[('million-100B',1000000,100,'random,seq'),('quarter-million-1KiB',250000,1024,'random')]
commands=[]
for label,n,size,work in profiles:
 cmd=['python3',str(runner),'--binary',args.binary,'--out',str(root/label),'--n',str(n),'--vsize',str(size),'--reps','3','--readers','10','--cachemb','64','--ourscachemb','64','--engines','ours,pebble,sqlite-c,sqlite-go','--no-default','--crud','--readpasses','2','--scanpasses','5','--workloads',work]
 commands.append(cmd)
(root/'matrix.json').write_text(json.dumps({'commands':commands,'measured_processes':36,'notes':'4 engines, 3 trials, random/sequential 1M x 100B plus random 250K x 1KiB. No fsync, per-row commits, 10 parallel readers. C SQLite is the primary SQLite comparison; pure-Go SQLite is additional.'},indent=2)+'\n')
for cmd in commands:
 if pathlib.Path(cmd[cmd.index('--out')+1]).exists():cmd.append('--resume')
 subprocess.run(cmd,check=True)
