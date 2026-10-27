#!/usr/bin/env python3
"""Serial fresh-process alpha.7/candidate comparisons with complete raw samples."""
import argparse,datetime,hashlib,json,os,random,shutil,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--out',required=True);p.add_argument('--binaries',required=True);p.add_argument('--n',type=int,default=300000);p.add_argument('--reps',type=int,default=3);p.add_argument('--cases',default='all');a=p.parse_args()
root=Path(a.out).resolve();root.mkdir(parents=True,exist_ok=False);bins=Path(a.binaries).resolve()
def background():
 return {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'processes':subprocess.check_output(['ps','-Ao','pcpu=,comm='],text=True),'swap':subprocess.check_output(['sysctl','vm.swapusage'],text=True)}
cases=[('ordered-legacy','ordered-all','legacy',512,512,False,None),('ordered-limit-legacy','ordered-limit','legacy',512,512,False,None),('ordered-stream','ordered-all','stream',16,512,False,None),('ordered-limit-stream','ordered-limit','stream',1,512,False,None),('hash-legacy','unordered','legacy',128,128,False,None),('hash-parallel','unordered','parallel',512,512,False,None),('low-parallel','unordered','parallel',64,64,False,100),('hash-stream','unordered','stream',128,128,False,None),('spill-same-budget','unordered','stream',16,16,True,None),('spill-versus-ram','unordered','stream',16,128,True,None),('low-legacy','unordered','legacy',64,64,False,100),('low-stream','unordered','stream',64,64,False,100),('crud','crud','legacy',64,64,False,None),('crud-random','crud-random','legacy',64,64,False,None),('cold','cold','legacy',64,64,False,None)]
if a.cases!='all':cases=[x for x in cases if x[0] in a.cases.split(',')]
meta={'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'n':a.n,'reps':a.reps,'cases':cases,'method':'Interleaved serial fresh subprocesses, no agent builds or tests during timing. One GC after fixture maintenance in both builds. All successful values/counts checked. Preserve budget errors separately. Background CPU/OS-cache state uncontrolled and recorded.','go':subprocess.check_output(['go','version'],text=True),'binaries':{name:hashlib.sha256((bins/(name+'-bench')).read_bytes()).hexdigest() for name in ['alpha7','current']},'background_initial':background()}
(root/'metadata.json').write_text(json.dumps(meta,indent=2));runs=[]
for rep in range(1,a.reps+1):
 jobs=[(c,version) for c in cases for version in ['alpha7','current']];random.Random(42+rep).shuffle(jobs)
 for c,version in jobs:
  name,mode,api,currentmb,oldmb,disk,g=c;stem=f'{name}-{version}-r{rep}';fixture=root/(stem+'-data')
  if shutil.disk_usage(root).free<a.n*600*4+(1<<30):raise RuntimeError('Insufficient free space for fresh fixture and reserve')
  command=[str(bins/(version+'-bench')),'-dir',str(fixture),'-mode',mode,'-api',api,'-n',str(a.n),'-groups',str(g or a.n//3),'-memorymb',str(currentmb if version=='current' else oldmb)]
  if mode=='cold':command+=['-pad','512']
  if disk and version=='current':command+=['-spill']
  bg=background();print('RUN',stem,flush=True);start=time.monotonic()
  with (root/(stem+'.stderr.txt')).open('w') as f:
   r=subprocess.run(command,stdout=subprocess.PIPE,stderr=f,text=True,env={**os.environ,'GOMAXPROCS':'10'},timeout=600)
  (root/(stem+'.stdout.json')).write_text(r.stdout)
  if r.returncode:raise RuntimeError(f'{stem} failed: {r.returncode}; see stderr; fixture retained')
  result=json.loads(r.stdout);expected_error=name=='spill-same-budget' and version=='alpha7'
  if expected_error and not result.get('budget_error'):raise RuntimeError(f'{stem}: expected ErrLimit, got {result["error"]}')
  if bool(result['error'])!=expected_error:raise RuntimeError(f'{stem}: unexpected result error {result["error"]}')
  result.update({'case':name,'build':version,'rep':rep,'command':command,'background':bg,'elapsed_seconds':time.monotonic()-start})
  (root/(stem+'.json')).write_text(json.dumps(result,indent=2));runs.append(result);(root/'runs.json').write_text(json.dumps(runs,indent=2));shutil.rmtree(fixture)
  phase=next((x for x in result['phases'] if x['Name']=='aggregate'),result['phases'][-2]);print('DONE',stem,'CPU',round(phase['CPU'],4),'wall',round(phase['Wall'],4),'RSS',round(phase['RSS']/2**20,1),'error',bool(result['error']),flush=True)
meta['finished']=datetime.datetime.now(datetime.timezone.utc).isoformat();meta['background_final']=background();(root/'metadata.json').write_text(json.dumps(meta,indent=2))
