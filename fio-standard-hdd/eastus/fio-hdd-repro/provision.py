#!/usr/bin/env python3
"""Create an isolated cheap VM and a read-only clone of the keeper disk."""
import json, pathlib, secrets, subprocess
ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT / 'artifacts'
ART.mkdir(exist_ok=True)
def az(*args):
    output = subprocess.check_output(['az', *args, '-o', 'json'], text=True)
    return json.loads(output) if output.strip() else None
account = az('account', 'show')
keeper = json.loads((ROOT.parent / '.state/prepared-snapshot-keeper.json').read_text())
assert account['id'] == keeper['subscriptionId']
snapshot = az('snapshot', 'show', '--ids', keeper['retainedSnapshot']['id'])
token = secrets.token_hex(12)
state = dict(subscription=account['id'], token=token, rg='rg-fio-hdd-' + token,
             location=snapshot['location'], vm='fio-vm', disk='fio-data', source=snapshot['id'])
if (ROOT / 'state.json').exists():
    state = json.loads((ROOT / 'state.json').read_text())
    token = state['token']
    assert state['subscription'] == account['id']
    group = az('group', 'show', '-n', state['rg'])
    assert group['tags']['reprotoken'] == token
    if 'vm_id' in state:
        raise SystemExit('Provisioning already complete')
(ROOT / 'state.json').write_text(json.dumps(state, indent=2))
if not (ART / 'snapshots-before.json').exists():
    (ART / 'snapshots-before.json').write_text(json.dumps(az('snapshot', 'list'), indent=2))
az('group', 'create', '-n', state['rg'], '-l', state['location'], '--tags',
   'managed-by=fio-hdd-repro', 'reprotoken=' + token)
if not (ROOT / 'ssh-key').exists():
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(ROOT / 'ssh-key')], check=True)
(ROOT / 'cloud-init.yml').write_text('''#cloud-config
package_update: true
packages:
  - fio
  - sysstat
runcmd:
  - [touch, /var/tmp/fio-ready]
''')
vm = az('vm', 'create', '-g', state['rg'], '-n', state['vm'], '-l', state['location'],
        '--size', 'Standard_D4s_v4', '--image', 'Ubuntu2204', '--admin-username', 'fioadmin',
        '--ssh-key-values', str(ROOT / 'ssh-key.pub'), '--storage-sku', 'StandardSSD_LRS',
        '--os-disk-size-gb', '32', '--public-ip-sku', 'Standard', '--nsg-rule', 'SSH',
        '--custom-data', str(ROOT / 'cloud-init.yml'), '--tags', 'reprotoken=' + token)
state['ip'] = vm['publicIpAddress']
(ROOT / 'state.json').write_text(json.dumps(state, indent=2))
disk = az('disk', 'create', '-g', state['rg'], '-n', state['disk'], '-l', state['location'],
          '--source', snapshot['id'], '--sku', 'Standard_LRS', '--tags', 'reprotoken=' + token)
(ART / 'disk.json').write_text(json.dumps(disk, indent=2))
az('vm', 'disk', 'attach', '-g', state['rg'], '--vm-name', state['vm'], '--name', disk['id'],
   '--lun', '0', '--caching', 'None')
state['disk_id'] = disk['id']
state['vm_id'] = az('vm', 'show', '-g', state['rg'], '-n', state['vm'])['id']
(ROOT / 'state.json').write_text(json.dumps(state, indent=2))
(ART / 'vm.json').write_text(json.dumps(az('vm', 'show', '-g', state['rg'], '-n', state['vm']), indent=2))
print(json.dumps(state, indent=2))
