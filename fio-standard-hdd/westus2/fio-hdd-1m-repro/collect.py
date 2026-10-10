#!/usr/bin/env python3
"""Copy guest artifacts and fetch Azure's one-minute platform metrics."""
import datetime as dt, json, pathlib, subprocess, time
ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT/'artifacts'
state = json.loads((ROOT/'state.json').read_text())
def copy(args):
    for attempt in range(3):
        result = subprocess.run(args)
        if result.returncode == 0:
            return
        if attempt < 2:
            time.sleep(5)
    raise subprocess.CalledProcessError(result.returncode, args)
copy(['scp','-q','-r','-i',str(ROOT/'ssh-key'),
    f"fioadmin@{state['ip']}:/home/fioadmin/results/.",str(ART)])
copy(['scp','-q','-i',str(ROOT/'ssh-key'),
    f"fioadmin@{state['ip']}:/home/fioadmin/guest-test.log",str(ART/'guest-test.log')])
metas = [json.loads(p.read_text()) for p in ART.glob('run-*/meta.json')]
metas += [json.loads(p.read_text()) for p in (ART/'single-validation').glob('run-*/meta.json')]
if not metas:
    raise SystemExit('No completed guest stages')
start = min(m['start'] for m in metas)
end = max(m['end'] for m in metas)
def call(args,path):
    result = subprocess.run(['az',*args,'-o','json'],capture_output=True,text=True)
    (ART/(path+'.stderr')).write_text(result.stderr)
    (ART/path).write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(f'Azure metric query failed: {path}')
    return json.loads(result.stdout)
for prefix, rid in [('vm',state['vm_id']),('disk',state['disk_id'])]:
    defs = call(['monitor','metrics','list-definitions','--resource',rid],prefix+'-definitions.json')
    available = {d['name']['value'] for d in defs}
    candidates = (['Data Disk Latency','Data Disk Queue Depth','Data Disk Read Operations/Sec',
        'Data Disk Read Bytes/sec','Data Disk Write Operations/Sec','Data Disk Write Bytes/sec',
        'Data Disk IOPS Consumed Percentage','Data Disk Bandwidth Consumed Percentage',
        'Data Disk Target IOPS','Data Disk Target Bandwidth',
        'Data Disk Max Burst Bandwidth','Data Disk Used Burst BPS Credits Percentage'] if prefix == 'vm' else
        ['Composite Disk Read Operations/sec','Composite Disk Read Bytes/sec',
        'Composite Disk Write Operations/sec','Composite Disk Write Bytes/sec'])
    metrics = [m for m in candidates if m in available]
    args = ['monitor','metrics','list','--resource',rid,'--metric',','.join(metrics),
        '--start-time',start,'--end-time',end,'--interval','PT1M','--aggregation','Average']
    if prefix == 'vm':
        args += ['--filter',"LUN eq '0'"]
    call(args,prefix+'-metrics.json')
host = ['Percentage CPU','VM Uncached IOPS Consumed Percentage','VM Uncached Bandwidth Consumed Percentage',
        'VM Remote Used Burst IO Credits Percentage','VM Remote Used Burst BPS Credits Percentage']
call(['monitor','metrics','list','--resource',state['vm_id'],'--metric',','.join(host),
      '--start-time',start,'--end-time',end,'--interval','PT1M','--aggregation','Average'],'host-metrics.json')
print(f'Collected {len(metas)} attempt records, {start} through {end}')
