import os, sys, subprocess, time, json, platform, hashlib
from pathlib import Path
BIN=Path(__file__).resolve().parent
OUT=BIN/"final"
OUT.mkdir(exist_ok=True)
REPO=Path(sys.argv[1]); RUNS=[]
def sample():
    return {'load':os.getloadavg(),'top':sorted(subprocess.check_output(['ps','-axo','pid,pcpu,comm'],text=True).splitlines()[1:],key=lambda line:float(line.split()[1]),reverse=True)[:15]}
def run(name,args,env=None):
    before=sample(); start=time.monotonic(); target=OUT/(name+'.txt')
    merged=os.environ.copy(); merged.update(env or {})
    with target.open('w') as log:
        child=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,env=merged,cwd=OUT)
        _,status,usage=os.wait4(child.pid,0); code=os.waitstatus_to_exitcode(status);child.returncode=code
    elapsed=time.monotonic()-start
    rec={'name':name,'command':args,'env':env or {},'exit_code':code,'wall_seconds':elapsed,'user_seconds':usage.ru_utime,'system_seconds':usage.ru_stime,'peak_rss_bytes':usage.ru_maxrss if platform.system()=='Darwin' else usage.ru_maxrss*1024,'before':before,'after':sample()}
    RUNS.append(rec);(OUT/'runs.json').write_text(json.dumps(RUNS,indent=2))
    print(name,code,round(elapsed,3),'s',round(rec['peak_rss_bytes']/2**20,2),'MiB',flush=True)
    if code: raise RuntimeError(name)

def exe(name):return str(BIN/({'current.test':'final.test','current-bench':'final-bench'}.get(name,name)))
meta={'runtime_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),'baseline':json.loads((BIN/'baseline.json').read_text())['baseline'],'os':platform.platform(),'go':subprocess.check_output(['go','version'],cwd=REPO,text=True).strip(),'cpu':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string'],text=True).strip(),'memory':subprocess.check_output(['sysctl','-n','hw.memsize'],text=True).strip(),'date':time.strftime('%Y-%m-%d %H:%M:%S %z'),'binary_sha256':{n:hashlib.sha256(Path(exe(n)).read_bytes()).hexdigest() for n in ['alpha4.test','current.test','alpha4-bench','current-bench']}}
(OUT/'metadata.json').write_text(json.dumps(meta,indent=2))
# Alternating fresh processes; never select the fastest outer repeat.
for i in range(5):
    for version in (['alpha4','current'] if i%2==0 else ['current','alpha4']):
        run(f'guard-{version}-{i+1}',[exe(version+'.test'),'-test.run','^$','-test.bench','^BenchmarkChangeFeedDisabledGuard$','-test.benchtime','500ms','-test.benchmem'])
for i in range(3):
    for single in [False,True]:
        for version in (['alpha4','current'] if i%2==0 else ['current','alpha4']):
            run(f'baseline-{version}-batch{1 if single else 1000}-{i+1}',[exe(version+'.test'),'-test.run','^TestChangeFeedBaselineProbe$','-test.v'],{'APTEVA_FEED_BASELINE':'1','APTEVA_FEED_SINGLE':'1' if single else '0'})
    for mode in ['Off','Capture','Metadata1','Metadata10','Metadata100','Filtered100','Tenants100','Values10','Projection100','Slow100']:
        run(f'feed-{mode}-batch1000-{i+1}',[exe('current.test'),'-test.run','^TestChangeFeedResourceProbe$','-test.v','-test.timeout','90s'],{'APTEVA_FEED_PROBE':mode})
    for mode in ['Off','Capture','Metadata1','Metadata10','Metadata100','Slow100']:
        run(f'feed-{mode}-batch1-{i+1}',[exe('current.test'),'-test.run','^TestChangeFeedResourceProbe$','-test.v','-test.timeout','90s'],{'APTEVA_FEED_PROBE':mode,'APTEVA_FEED_SINGLE':'1'})
    run(f'native-allocs-{i+1}',[exe('current.test'),'-test.run','^$','-test.bench','^BenchmarkNativeChangeFeeds$','-test.benchtime','100x','-test.benchmem','-test.timeout','90s'])
# Large paired raw workload; all outer trials retained, inner reps=1.
for i in range(3):
    for version in (['alpha4','current'] if i%2==0 else ['current','alpha4']):
        run(f'kv-{version}-{i+1}',[exe(version+'-bench'),'-engines','ours','-n','1000000','-par','10','-reps','1'])
# Existing comparison suites exercise correctness as well as speed.
for i in range(3):
    for mode in ['agg','join','computed','computedcollections','compat','wideindex']:
        run(f'{mode}-{i+1}',[exe('current-bench'),'-'+mode,'-n','50000','-reps','1'])
# Phase CPU / footprint / RSS comparisons: fresh processes per engine/workload.
for i in range(3):
    for engine in ['ours']:
        run(f'resources-{engine}-{i+1}',[exe('current-bench'),'-resources','-engines',engine,'-workloads','random','-n','1000000','-par','10','-reps','1'])
print('ALL BENCHMARKS PASSED',len(RUNS),flush=True)
