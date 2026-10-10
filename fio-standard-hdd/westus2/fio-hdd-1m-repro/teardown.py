#!/usr/bin/env python3
"""Delete only the owned experiment RG, and refuse if ANY snapshots exist there."""
import json, pathlib, subprocess, time
root = pathlib.Path(__file__).resolve().parent
state = json.loads((root/'state.json').read_text())
def az(*args):
    return json.loads(subprocess.check_output(['az',*args,'-o','json'],text=True))
assert az('account','show')['id'] == state['subscription']
group = az('group','show','-n',state['rg'])
assert group['tags']['reprotoken'] == state['token']
assert group['tags']['managed-by'] == 'fio-hdd-repro'
resources = az('resource','list','-g',state['rg'])
assert not any(r['type'].lower() == 'microsoft.compute/snapshots' for r in resources), 'Preserve all snapshots'
before = az('snapshot','list')
subprocess.run(['az','group','delete','-n',state['rg'],'--yes','--no-wait'],check=True)
while az('group','exists','-n',state['rg']):
    time.sleep(20)
after = az('snapshot','list')
assert {s['id'].lower() for s in before} <= {s['id'].lower() for s in after}, 'Snapshot inventory changed'
(root/'artifacts'/'cleanup.json').write_text(json.dumps(dict(deleted_rg=state['rg'],
     snapshots_before=[s['id'] for s in before],snapshots_after=[s['id'] for s in after]),indent=2))
