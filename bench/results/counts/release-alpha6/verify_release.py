from pathlib import Path
import hashlib,json,os,secrets,signal,socket,subprocess,tarfile,time,urllib.request
ROOT=Path(__file__).resolve().parent
TAG='v0.2.0-alpha.6';DL=ROOT/'release-download';DL.mkdir(exist_ok=True)
subprocess.run(['gh','release','download',TAG,'--repo','apteva/db','--dir',str(DL),'--pattern','*.tar.gz','--pattern','SHA256SUMS'],check=True)
records=[]
for line in (DL/'SHA256SUMS').read_text().splitlines():
 expected,name=line.split();name=name.lstrip('*');p=DL/name
 actual=hashlib.sha256(p.read_bytes()).hexdigest();assert actual==expected,(name,actual,expected)
 dest=DL/name.removesuffix('.tar.gz');dest.mkdir(exist_ok=True)
 with tarfile.open(p) as tar:tar.extractall(dest,filter='data')
 assert (dest/'CHANGE_FEEDS.md').is_file(),name
 assert 'CountDocuments' in (dest/'USAGE.md').read_text(),name
 assert 'Document count RPC' in (dest/'SERVER.md').read_text(),name
 assert 'v0.2.0-alpha.6' in (dest/'README.md').read_text(),name
 records.append({'archive':name,'sha256':actual,'change_feed_guide':True,'count_guides':True})
assert len(records)==4,records
(ROOT/'release-checksums.json').write_text(json.dumps(records,indent=2))
native=DL/'aptevadb_0.2.0-alpha.6_darwin_arm64'
for binary,flag in [('aptevadbctl','version'),('aptevadb-server','-version')]:
 result=subprocess.check_output([str(native/binary),flag],text=True);assert '0.2.0-alpha.6' in result and 'format 10' in result;print(result.strip(),flush=True)
# Build/install against the public module without a local replace or workspace.
consumer=ROOT/'public-consumer';(consumer/'go.mod').write_text('module apteva-alpha6-release-smoke\n\ngo 1.25\n\nrequire github.com/apteva/db v0.2.0-alpha.6\n')
env={**os.environ,'GOWORK':'off','GOPROXY':'https://proxy.golang.org','GOSUMDB':'sum.golang.org','GOPRIVATE':'','GONOPROXY':'','GONOSUMDB':'','GOPATH':str(ROOT/'public-gopath'),'GOBIN':str(ROOT/'public-bin')}
for attempt in range(12):
 download=subprocess.run(['go','mod','download','-json','github.com/apteva/db@'+TAG],cwd=consumer,env=env,capture_output=True,text=True)
 if download.returncode==0:break
 (ROOT/'module-propagation-attempts.txt').open('a').write(download.stdout+download.stderr+'\n')
 time.sleep(5)
else:raise RuntimeError('public module did not resolve')
info=json.loads(download.stdout);assert info.get('Sum') and info.get('GoModSum');(ROOT/'public-module.json').write_text(json.dumps(info,indent=2))
subprocess.run(['go','build','-o',str(ROOT/'public-consumer-smoke'),'.'],cwd=consumer,env=env,check=True)
subprocess.run(['go','install','github.com/apteva/db/cmd/aptevadbctl@'+TAG],cwd=consumer,env=env,check=True)
result=subprocess.check_output([str(ROOT/'public-bin/aptevadbctl'),'version'],text=True);assert '0.2.0-alpha.6' in result;print('Public go install:',result.strip(),flush=True)
subprocess.run([str(ROOT/'public-consumer-smoke'),'embedded',str(ROOT/'public-smoke-db')],check=True)
subprocess.run([str(native/'aptevadbctl'),'verify',str(ROOT/'public-smoke-db')],check=True)
# Downloaded daemon + independently installed public Go HTTP driver.
sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close();url=f'http://127.0.0.1:{port}';token=secrets.token_urlsafe(32)
def request(path,body=None):
 return urllib.request.urlopen(urllib.request.Request(url+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}),timeout=10)
with (ROOT/'daemon-log.txt').open('w') as log:
 daemon=subprocess.Popen([str(native/'aptevadb-server'),'-dir',str(ROOT/'daemon-db'),'-listen',f'127.0.0.1:{port}','-change-feed'],env={**os.environ,'APTEVADB_TOKEN':token},stdout=log,stderr=log)
 stream=None
 try:
  for _ in range(100):
   try:
    with request('/v1/health') as h:health=json.load(h)
    assert health['version']=='0.2.0-alpha.6' and health['format']==10
    break
   except Exception:time.sleep(.05)
  else:raise RuntimeError('daemon startup failed')
  subprocess.run([str(ROOT/'public-consumer-smoke'),'remote',url],env={**env,'APTEVADB_TOKEN':token},check=True)
  stream=request('/v1/watch',{'op':'watch','watch':{}});assert json.loads(stream.readline())['watch_ready']
  start=time.monotonic();daemon.send_signal(signal.SIGTERM);code=daemon.wait(timeout=10);elapsed=time.monotonic()-start;assert code==0;assert stream.read()==b''
  print(json.dumps({'downloaded_daemon_idle_watch_shutdown_seconds':elapsed,'exit_code':code,'public_module_version':TAG}),flush=True)
 finally:
  if stream:stream.close()
  if daemon.poll() is None:daemon.kill();daemon.wait()
# The CLI opens with nil capture options; stored capture must survive it.
subprocess.run([str(native/'aptevadbctl'),'verify',str(ROOT/'daemon-db')],check=True)
print('All four archives/checksums, public module/install, embedded API, public HTTP driver and downloaded daemon verified.',flush=True)
