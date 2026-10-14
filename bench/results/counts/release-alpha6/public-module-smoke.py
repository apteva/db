import json,os,pathlib,subprocess,time
root=pathlib.Path(__file__).resolve().parent;consumer=root/'public-consumer';tag='v0.2.0-alpha.6'
(consumer/'go.mod').write_text('module apteva-alpha6-release-smoke\n\ngo 1.25\n\nrequire github.com/apteva/db v0.2.0-alpha.6\n')
env={**os.environ,'GOWORK':'off','GOPROXY':'https://proxy.golang.org','GOSUMDB':'sum.golang.org','GOPRIVATE':'','GONOPROXY':'','GONOSUMDB':'','GOPATH':str(root/'public-gopath'),'GOBIN':str(root/'public-bin')}
for _ in range(12):
 d=subprocess.run(['go','mod','download','-json','github.com/apteva/db@'+tag],cwd=consumer,env=env,capture_output=True,text=True)
 if d.returncode==0:break
 with (root/'module-propagation-attempts.txt').open('a') as f:f.write(d.stdout+d.stderr+'\n')
 time.sleep(5)
else:raise RuntimeError('public module propagation did not complete')
info=json.loads(d.stdout);assert info.get('Sum') and info.get('GoModSum');assert info['Origin']['Hash']=='fe9f71a82bb3019ee9ad526b1f3441f2f609bb83'
(root/'public-module.json').write_text(json.dumps(info,indent=2)+'\n')
subprocess.run(['go','build','-o',str(root/'public-consumer-smoke'),'.'],cwd=consumer,env=env,check=True)
subprocess.run(['go','install','github.com/apteva/db/cmd/aptevadbctl@'+tag],cwd=consumer,env=env,check=True)
v=subprocess.check_output([str(root/'public-bin/aptevadbctl'),'version'],text=True);assert '0.2.0-alpha.6' in v;print(v.strip(),flush=True)
subprocess.run([str(root/'public-consumer-smoke'),'embedded',str(root/'public-module-smoke-db')],check=True)
print('Public proxy/sumdb, tagged commit, CLI install and embedded covering/fallback/snapshot count verified.',flush=True)
