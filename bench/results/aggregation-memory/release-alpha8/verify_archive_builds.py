from pathlib import Path
import json,subprocess
root=Path(__file__).resolve().parent
records=[]
for archive in json.loads((root/'release-checksums.json').read_text()):
 name=archive['archive'];stem=name.removesuffix('.tar.gz');_,version,goos,goarch=stem.split('_')
 assert version=='0.2.0-alpha.8'
 for command in ['aptevadbctl','aptevadb-server']:
  binary=root/'release-download'/stem/command
  output=subprocess.check_output(['go','version','-m',str(binary)],text=True)
  settings={}
  for line in output.splitlines():
   fields=line.strip().split('\t')
   if len(fields)==2 and fields[0]=='build' and '=' in fields[1]:
    key,value=fields[1].split('=',1);settings[key]=value
  for key,value in {'GOOS':goos,'GOARCH':goarch,'CGO_ENABLED':'0','vcs.revision':'4f57eb9b66baa03f292977e442323cedd821f68e','vcs.modified':'false'}.items():assert settings.get(key)==value,(name,command,key,settings)
  records.append({'archive':name,'command':command,'compiler':output.splitlines()[0].split(': ')[-1],'settings':settings})
assert len(records)==8
(root/'archive-build-metadata.json').write_text(json.dumps(records,indent=2))
print('All eight archive binaries match their OS/architecture, CGO=0 and clean release commit.')
