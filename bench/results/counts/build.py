#!/usr/bin/env python3
"""Build paired benchmark binaries from this checkout and the alpha.5 tag."""
import io,pathlib,subprocess,tarfile
repo=pathlib.Path(__file__).resolve().parents[3]
baseline=pathlib.Path('/private/tmp/apteva-count-alpha5-source');baseline.mkdir(exist_ok=True)
archive=subprocess.check_output(['git','archive','v0.2.0-alpha.5'],cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive)) as t:t.extractall(baseline,filter='data')
s=(repo/'bench/count/count_test.go').read_text()
s=s.replace('"QueryPipeline", "IndexCursor", "NativeCount", "NaturalCount", "ResidualCount"','"QueryPipeline", "IndexCursor"')
a=s.index('\t\t\t\tcase "NativeCount"');b=s.index('\t\t\t\tcase "QueryPipeline"',a);s=s[:a]+s[b:]
a=s.index('\n\t\t\tif mode == "NativeCount"');b=s.index('\n\t\t})',a);s=s[:a]+s[b:]
p=baseline/'bench/count';p.mkdir(exist_ok=True);(p/'count_test.go').write_text(s)
for source,label in ((baseline,'alpha5'),(repo,'current')):
    subprocess.run(['go','test','-c','-o',f'/private/tmp/apteva-count-{label}-guard'],cwd=source,check=True)
    output='/private/tmp/apteva-count-alpha5-bench' if label=='alpha5' else '/private/tmp/apteva-count-bench'
    subprocess.run(['go','test','-c','-o',output,'./count'],cwd=source/'bench',check=True)
