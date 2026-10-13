from pathlib import Path
import datetime,hashlib,json,os,shutil,subprocess,time

ROOT=Path(__file__).resolve().parent
ENV={**os.environ,'GOMAXPROCS':'10'}
def capture(cmd):return subprocess.check_output(cmd,text=True).strip()
meta={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_revision':'e975c28f1572dd91daf19aaae42aa7cd1b8c6c32','baseline_revision':'41115aaa65a1c81de471e7f6e94c1db9f833f3af','cpu':capture(['sysctl','-n','machdep.cpu.brand_string']),'memory_bytes':capture(['sysctl','-n','hw.memsize']),'go':capture(['go','version']),'method':'Serial fresh processes/directories; three outer trials; rotating paired order; no concurrent local builds/tests. Background workstation activity remains uncontrolled. Default native cache; Pebble/SQLite cache 512 MiB (SQLite per connection), Pebble memtable 64 MiB.','binaries':{n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in ['alpha2-bench','alpha3-fast-bench']}}
(ROOT/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
runs=[]
def run(case,trial,binary,args):
 stem=f'{case}-{binary}-r{trial}';data=ROOT/(stem+'-data');data.mkdir()
 command=[str(ROOT/binary),*args,'-reps','1','-par','10','-dir',str(data)]
 background=capture(['ps','-A','-o','pcpu,comm'])
 (ROOT/(stem+'.background')).write_text(background+'\n')
 print('RUN',stem,flush=True);start=time.monotonic()
 with (ROOT/(stem+'.stdout')).open('w') as stdout,(ROOT/(stem+'.stderr')).open('w') as stderr:
  process=subprocess.run(['/usr/bin/time','-l',*command],env=ENV,stdout=stdout,stderr=stderr,timeout=1200)
 item={'case':case,'trial':trial,'binary':binary,'command':command,'seconds':time.monotonic()-start,'returncode':process.returncode}
 runs.append(item);(ROOT/'runs.json').write_text(json.dumps(runs,indent=2)+'\n')
 for f in data.glob('*measurements.json'):shutil.copy2(f,ROOT/(stem+'-'+f.name))
 if process.returncode:raise RuntimeError(stem+' failed; outputs retained')
 shutil.rmtree(data);print('DONE',stem,round(item['seconds'],2),flush=True)
for trial in range(1,8):
 order=['alpha2-bench','alpha3-fast-bench'] if trial%2 else ['alpha3-fast-bench','alpha2-bench']
 for binary in order:run('native-million',trial,binary,['-n','1000000','-engines','ours'])
for trial in range(1,4):
 for case,args in [('kv',['-n','250000','-engines','ours,pebble,sqlite-c,sqlite-go']),('compat',['-compat','-n','50000'])]:
  order=['alpha2-bench','alpha3-fast-bench'] if trial%2 else ['alpha3-fast-bench','alpha2-bench']
  for binary in order:run(case,trial,binary,args)
 run('wide',trial,'alpha3-fast-bench',['-wideindex','-n','10000'])
meta['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();(ROOT/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n');print('ALL COMPLETE',flush=True)
