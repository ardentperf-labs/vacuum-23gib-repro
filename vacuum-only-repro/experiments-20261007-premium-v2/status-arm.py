#!/usr/bin/env python3
"""Read current stack state using a broker-issued Azure ARM access token."""
import concurrent.futures as cf
import datetime as dt
import json
from pathlib import Path
import subprocess
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
TOKEN = json.loads(subprocess.check_output([
    'az', 'account', 'get-access-token', '--resource',
    'https://management.azure.com/', '-o', 'json'
], text=True, timeout=180))['accessToken']


def get(resource_id, version):
    request = urllib.request.Request(
        'https://management.azure.com' + resource_id + '?api-version=' + version,
        headers={'Authorization': 'Bearer ' + TOKEN})
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return {'missing': True}
        raise RuntimeError(f'ARM GET HTTP {error.code} for {resource_id}') from None


def stack_status(name):
    path = ROOT / name / '.state/state.json'
    if not path.exists():
        return {'name': name, 'status': 'no-local-state'}
    state = json.loads(path.read_text())
    subscription = '/subscriptions/' + state['subscriptionId']
    cluster = get(subscription + '/resourceGroups/' + state['resourceGroup'] +
                  '/providers/Microsoft.ContainerService/managedClusters/' + state['clusterName'],
                  '2025-07-01')
    properties = cluster.get('properties', {})
    if not cluster.get('missing'):
        assert cluster.get('tags', {}).get('reprotoken') == state['ownerToken']
    return {'name': name, 'localStatus': state['status'],
            'location': state['location'], 'clusterMissing': cluster.get('missing', False),
            'provisioningState': properties.get('provisioningState'),
            'powerState': properties.get('powerState'),
            'nodeResourceGroup': properties.get('nodeResourceGroup'),
            'nodePools': [{'name': pool['name'], 'count': pool['count'],
                           'vmSize': pool['vmSize'], 'zones': pool.get('availabilityZones'),
                           'provisioningState': pool.get('provisioningState')}
                          for pool in properties.get('agentPoolProfiles', [])]}


with cf.ThreadPoolExecutor(max_workers=3) as pool:
    status = list(pool.map(stack_status, ['baseline-23gib', '50gib', '100gib']))
receipt = {'checkedAtUtc': dt.datetime.now(dt.timezone.utc).isoformat(), 'stacks': status}
(ROOT / 'preflight/current-stack-status.json').write_text(json.dumps(receipt, indent=2) + '\n')
for stack in status:
    print(json.dumps(stack))
