#!/usr/bin/env python3
"""Read snapshot identities and locks, checking preservation of every prior snapshot."""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--label', default='current')
args = parser.parse_args()
if not args.label.replace('-', '').replace('_', '').isalnum():
    parser.error('label must contain letters, digits, hyphens or underscores')
subscription = json.loads((ROOT / 'experiment-plan.json').read_text()).get('subscriptionId',
                '00000000-0000-0000-0000-000000000000')
token = json.loads(subprocess.check_output(['az', 'account', 'get-access-token',
        '--resource', 'https://management.azure.com/', '-o', 'json'],
        text=True, timeout=900))['accessToken']


def collection(path, version):
    url = 'https://management.azure.com' + path + '?api-version=' + version
    result = []
    while url:
        if not url.startswith('https://management.azure.com/'):
            raise RuntimeError('ARM pagination changed authority')
        request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
        with urllib.request.urlopen(request, timeout=60) as response:
            body = json.load(response)
        result.extend(body['value'])
        url = body.get('nextLink')
    return result


prefix = '/subscriptions/' + subscription
snapshots = collection(prefix + '/providers/Microsoft.Compute/snapshots', '2025-01-02')
locks = collection(prefix + '/providers/Microsoft.Authorization/locks', '2016-09-01')
protected = {lock['id'].lower().split('/providers/microsoft.authorization/locks/')[0]
             for lock in locks if lock.get('properties', {}).get('level') == 'CanNotDelete'}
inventory = []
for snapshot in snapshots:
    properties = snapshot['properties']
    snapshot_id = snapshot['id']
    group_id = '/'.join(snapshot_id.split('/')[:5])
    inventory.append({'id': snapshot_id, 'name': snapshot['name'], 'uniqueId': properties.get('uniqueId'),
        'location': snapshot['location'], 'incremental': properties.get('incremental'),
        'sizeGiB': properties.get('diskSizeGB'), 'state': properties.get('provisioningState'),
        'accessState': properties.get('snapshotAccessState'), 'completionPercent': properties.get('completionPercent'),
        'tags': snapshot.get('tags', {}), 'directCanNotDelete': snapshot_id.lower() in protected,
        'groupCanNotDelete': group_id.lower() in protected})
current = {snapshot['id'].lower(): snapshot for snapshot in inventory}
before = json.loads((ROOT / 'preflight/all-snapshots-before.json').read_text())
missing = [snapshot['id'] for snapshot in before if snapshot['id'].lower() not in current]
changed = [snapshot['id'] for snapshot in before if snapshot['id'].lower() in current and
           current[snapshot['id'].lower()]['uniqueId'] != snapshot['uniqueId']]
unprotected = [snapshot['id'] for snapshot in inventory if not
               (snapshot['directCanNotDelete'] or snapshot['groupCanNotDelete'])]
receipt = {'checkedAtUtc': dt.datetime.now(dt.timezone.utc).isoformat(),
           'preservedPriorSnapshots': not missing and not changed,
           'allSnapshotsProtected': not unprotected, 'priorSnapshotCount': len(before),
           'currentSnapshotCount': len(inventory), 'missing': missing,
           'changedIdentities': changed, 'unprotected': unprotected, 'snapshots': inventory}
destination = ROOT / 'preflight' / ('snapshot-audit-' + args.label + '.json')
destination.write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({key: value for key, value in receipt.items() if key != 'snapshots'}))
for snapshot in inventory:
    if snapshot.get('tags', {}).get('reproPurpose', '').startswith('premiumv2'):
        print(snapshot['name'], snapshot['state'], snapshot['accessState'], snapshot['completionPercent'])
if missing or changed or unprotected:
    raise SystemExit(2)
