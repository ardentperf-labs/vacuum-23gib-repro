import json,pathlib,os
r=pathlib.Path('/sys/block/sdb'); out={'cpu_count':os.cpu_count(),'queues':[]}
for p in sorted((r/'mq').glob('*')):
 d={'name':p.name}
 for k in ('cpu_list','nr_tags','nr_reserved_tags'):
  q=p/k
  if q.exists(): d[k]=q.read_text().strip()
 out['queues'].append(d)
for k in ('nr_requests','scheduler'):
 out[k]=(r/'queue'/k).read_text().strip()
print(json.dumps(out,indent=2))
