#!/usr/bin/env python3
"""Capture and verify actual VM SKU, disk type, size, and caching before VACUUM."""
import json, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'
s = json.loads((SD / 'state.json').read_text())
expected_size = s['nodeVmSize']
expected_disk_size = int(s.get('diskSizeGB', 512))
expected_disk_sku = s.get('diskSku', 'Standard_LRS')
expected_caching = str(s.get('cachingMode', 'None')).strip().casefold()
if expected_caching in ('', 'none', 'null'):
    expected_caching = 'none'
dest = SD / 'attachment-verification'; dest.mkdir(exist_ok=True)

def cmd(argv):
    p = subprocess.run(argv, capture_output=True, text=True, timeout=300)
    if p.returncode: raise RuntimeError(p.stderr[-2000:])
    return json.loads(p.stdout)

def az(*args): return cmd(['az', *args, '--subscription', s['subscriptionId'], '-o', 'json'])
def k(*args): return cmd(['kubectl', '--kubeconfig', str(SD / 'kubeconfig'), *args, '-o', 'json'])
def save(name, data): (dest / name).write_text(json.dumps(data, indent=2) + '\n')

pods = k('get', 'pods', '-n', 'repro', '-l', 'cnpg.io/cluster=hdd-pg')['items']
nodes = {n['metadata']['name']: n for n in k('get', 'nodes')['items']}
pvs = {p['metadata']['name']: p for p in k('get', 'pv')['items']}
pvcs = {p['metadata']['name']: p for p in k('get', 'pvc', '-n', 'repro')['items']}
save('nodes.json', list(nodes.values())); save('pods.json', pods)
roles = {}
for pod in pods:
    role = pod['metadata']['labels'].get('cnpg.io/instanceRole')
    if role not in ('primary', 'replica'): continue
    assert role not in roles
    node = nodes[pod['spec']['nodeName']]
    assert node['metadata']['labels']['node.kubernetes.io/instance-type'] == expected_size
    vmid = node['spec']['providerID'].removeprefix('azure://')
    owned_prefix = f"/subscriptions/{s['subscriptionId']}/resourceGroups/{s['nodeResourceGroup']}/providers/Microsoft.Compute/virtualMachineScaleSets/"
    assert vmid.lower().startswith(owned_prefix.lower()), 'VM is outside the owned node resource group'
    vm = az('rest', '--method', 'get', '--url', 'https://management.azure.com' + vmid + '?api-version=2024-07-01')
    save(role + '-vm.json', vm)
    size = vm.get('properties', {}).get('hardwareProfile', {}).get('vmSize')
    if size is None:
        vmss_id = vmid.split('/virtualMachines/')[0]
        model = az('rest', '--method', 'get', '--url', 'https://management.azure.com' + vmss_id + '?api-version=2024-07-01')
        save(role + '-vmss.json', model); size = model['sku']['name']
    assert size == expected_size, size
    attached = {d['managedDisk']['id'].lower(): d for d in vm['properties']['storageProfile']['dataDisks']}
    disks = {}
    postgres = next(x for x in pod['spec']['containers'] if x['name'] == 'postgres')
    mounts = {x['name']: x['mountPath'] for x in postgres.get('volumeMounts', [])}
    for vol in pod['spec']['volumes']:
        claim = vol.get('persistentVolumeClaim', {}).get('claimName')
        if not claim: continue
        mount = mounts.get(vol['name'], '')
        kind = 'wal' if '/wal' in mount else 'data' if '/data' in mount else None
        assert kind and kind not in disks, 'Unexpected or duplicate PostgreSQL PVC mount: ' + mount
        pvc = pvcs[claim]; pv = pvs[pvc['spec']['volumeName']]
        diskid = pv['spec']['csi']['volumeHandle']
        disk_prefix = f"/subscriptions/{s['subscriptionId']}/resourceGroups/{s['nodeResourceGroup']}/providers/Microsoft.Compute/disks/"
        assert diskid.lower().startswith(disk_prefix.lower()), 'Disk is outside the owned node resource group'
        disk = az('disk', 'show', '--ids', diskid)
        attachment = attached[diskid.lower()]
        assert disk['sku']['name'] == expected_disk_sku and int(disk['diskSizeGB']) == expected_disk_size
        actual_vm_caching = str(attachment.get('caching') or 'None').strip().casefold()
        actual_csi_caching = str(pv['spec']['csi'].get('volumeAttributes', {}).get('cachingMode') or 'None').strip().casefold()
        if actual_vm_caching in ('', 'none', 'null'):
            actual_vm_caching = 'none'
        if actual_csi_caching in ('', 'none', 'null'):
            actual_csi_caching = 'none'
        assert actual_vm_caching == expected_caching, {'attachment': attachment, 'expectedCaching': expected_caching}
        assert actual_csi_caching == expected_caching, {
            'volumeAttributes': pv['spec']['csi'].get('volumeAttributes', {}),
            'expectedCaching': expected_caching,
        }
        save(role + '-' + kind + '-disk.json', disk)
        save(role + '-' + kind + '-pv.json', pv)
        disks[kind] = {
            'id': diskid,
            'lun': str(attachment['lun']),
            'caching': attachment.get('caching') or 'None',
            'sku': disk['sku']['name'],
            'sizeGB': int(disk['diskSizeGB']),
            'claim': claim,
            'pv': pv['metadata']['name'],
            'mountPath': mount,
        }
    assert set(disks) == {'data', 'wal'}
    roles[role] = {'pod': pod['metadata']['name'], 'node': pod['spec']['nodeName'], 'vmResourceId': vmid, 'disks': disks}
assert set(roles) == {'primary', 'replica'}
assert roles['primary']['node'] != roles['replica']['node']
save('verified.json', {
    'passed': True,
    'subscriptionId': s['subscriptionId'],
    'nodeVmSize': expected_size,
    'diskSku': expected_disk_sku,
    'diskSizeGB': expected_disk_size,
    'cachingMode': 'None' if expected_caching == 'none' else expected_caching,
    'roles': roles,
})
print(json.dumps(roles, indent=2))
