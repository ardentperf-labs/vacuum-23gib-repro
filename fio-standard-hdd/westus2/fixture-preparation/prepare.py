#!/usr/bin/env python3
"""Copy the prepared full VHD across regions; retain both Azure snapshots."""
import datetime as dt
import argparse
import json
import pathlib
import re
import secrets
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--completed-fast-copy', action='store_true',
                    help='Import the verified AzCopy blob instead of starting a server-side copy')
opts = parser.parse_args()
ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT / 'artifacts'
ART.mkdir(exist_ok=True)
HIDDEN = []

def sanitized(value):
    if isinstance(value, dict):
        return {k: sanitized(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitized(v) for v in value]
    if isinstance(value, str):
        for secret in HIDDEN:
            value = value.replace(secret, '<redacted>')
        return re.sub(r'(https://[^\s"<>?]+)\?[^\s"<>]+', r'\1?<redacted>', value)
    return value

def az(*args, optional=False):
    result = subprocess.run(['az', *args, '-o', 'json'], text=True, capture_output=True)
    if result.returncode:
        if optional:
            return None
        raise RuntimeError('Azure operation failed: ' + ' '.join(args[:3]) + '\n' + sanitized(result.stderr))
    return json.loads(result.stdout) if result.stdout.strip() else None

def save(name, value):
    (ART / name).write_text(json.dumps(sanitized(value), indent=2))

def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()

account = az('account', 'show')
keeper = json.loads((ROOT.parent / '.state/prepared-snapshot-keeper.json').read_text())
assert account['id'] == keeper['subscriptionId']
source = az('snapshot', 'show', '--ids', keeper['retainedSnapshot']['id'])
assert source['location'] == 'eastus' and source['diskSizeGB'] == 512
assert not source.get('incremental', False), 'Expected the prepared full snapshot'
state_file = ROOT / 'state.json'
if state_file.exists():
    state = json.loads(state_file.read_text())
else:
    token = secrets.token_hex(12)
    state = dict(subscription=account['id'], token=token, location='westus2',
                 source_snapshot=source['id'], source_location=source['location'],
                 transport_rg='rg-fio-hdd-transport-' + token,
                 keeper_rg='rg-fio-hdd-keeper-westus2-' + token,
                 account='fiow2' + token[:16], disk='fixture-export', snapshot='prepared-full',
                 started=now())
    state_file.write_text(json.dumps(state, indent=2))
assert state['subscription'] == account['id'] and state['source_snapshot'] == source['id']
if not (ART / 'snapshots-before.json').exists():
    save('snapshots-before.json', az('snapshot', 'list'))
save('source-snapshot.json', source)
for group_name, managed_by in [(state['transport_rg'], 'fio-hdd-region-transport'),
                               (state['keeper_rg'], 'fio-hdd-regional-fixture')]:
    group = az('group', 'show', '-n', group_name, optional=True)
    if group is None:
        group = az('group', 'create', '-n', group_name, '-l', state['location'],
                   '--tags', 'managed-by=' + managed_by, 'reprotoken=' + state['token'],
                   'preserve-all-snapshots=true')
    assert group['tags']['reprotoken'] == state['token']
    assert group['tags']['managed-by'] == managed_by

target = az('snapshot', 'show', '-g', state['keeper_rg'], '-n', state['snapshot'], optional=True)
if target is None:
    # Export an owned clone so the original keeper's access policy stays intact.
    disk = az('disk', 'show', '-g', state['transport_rg'], '-n', state['disk'], optional=True)
    if disk is None:
        disk = az('disk', 'create', '-g', state['transport_rg'], '-n', state['disk'],
                  '-l', state['source_location'], '--source', source['id'], '--sku', 'Standard_LRS',
                  '--network-access-policy', 'AllowAll', '--public-network-access', 'Enabled',
                  '--tags', 'reprotoken=' + state['token'])
    assert disk['tags']['reprotoken'] == state['token']
    assert not disk.get('managedBy')
    state['export_disk_id'] = disk['id']
    save('export-disk.json', disk)
    storage = az('storage', 'account', 'show', '-g', state['transport_rg'], '-n', state['account'], optional=True)
    if storage is None:
        storage = az('storage', 'account', 'create', '-g', state['transport_rg'], '-n', state['account'],
                     '-l', state['location'], '--sku', 'Standard_LRS', '--kind', 'StorageV2',
                     '--allow-blob-public-access', 'false', '--https-only', 'true',
                     '--min-tls-version', 'TLS1_2', '--tags', 'reprotoken=' + state['token'])
    assert storage['tags']['reprotoken'] == state['token']
    state['storage_id'] = storage['id']
    save('storage-account.json', storage)
    state_file.write_text(json.dumps(state, indent=2))
    key = az('storage', 'account', 'keys', 'list', '-g', state['transport_rg'], '-n', state['account'])[0]['value']
    HIDDEN.append(key)
    auth = ['--account-name', state['account'], '--account-key', key]
    az('storage', 'container', 'create', '-n', 'vhds', *auth)
    if opts.completed_fast_copy:
        marker = json.loads((ART / 'fast-copy-complete.json').read_text())
        assert marker['status'] == 'Succeeded'
        assert marker['ownershipToken'] == state['token']
        assert marker['subscriptionId'] == state['subscription']
        assert marker['sourceSnapshotId'].lower() == source['id'].lower()
        assert marker['sourceDiskId'].lower() == disk['id'].lower()
        assert marker['storageAccountId'].lower() == storage['id'].lower()
        assert marker['container'] == 'vhds'
        assert marker['destinationBlob'] in ('prepared-azcopy.vhd', 'prepared-azcopy-retry.vhd')
        assert marker['contentLengthBytes'] == 512 * 1024**3 + 512
        uri = storage['primaryEndpoints']['blob'] + 'vhds/' + marker['destinationBlob']
        assert marker['destinationBlobUri'] == uri
        blob = az('storage', 'blob', 'show', '--container-name', 'vhds',
                  '--name', marker['destinationBlob'], *auth)
        assert blob['properties']['blobType'] == 'PageBlob'
        assert blob['properties']['contentLength'] == 512 * 1024**3 + 512
        assert (blob['properties'].get('copy') or {}).get('status') not in ('pending', 'failed', 'aborted')
        save('adopted-fast-copy.json', dict(marker=marker, blob=blob))
    else:
        blob_args = ['--container-name', 'vhds', '--name', 'prepared.vhd', *auth]
        blob = az('storage', 'blob', 'show', *blob_args, optional=True)
        copy = (blob or {}).get('properties', {}).get('copy', {})
        if copy.get('status') not in ('success', 'pending'):
            az('disk', 'revoke-access', '--ids', disk['id'])
            grant = az('disk', 'grant-access', '--ids', disk['id'], '--access-level', 'Read', '--duration-in-seconds', '21600')
            sas = grant.get('accessSAS') or grant.get('accessSas')
            assert isinstance(sas, str) and sas.startswith('https://'), list(grant)
            HIDDEN.append(sas)
            started = az('storage', 'blob', 'copy', 'start', '--destination-container', 'vhds',
                         '--destination-blob', 'prepared.vhd', '--source-uri', sas, *auth)
            save('copy-start.json', started)
        while True:
            blob = az('storage', 'blob', 'show', *blob_args)
            copy = blob['properties']['copy']
            save('copy-progress.json', blob)
            print(json.dumps(dict(time=now(), status=copy['status'], progress=copy.get('progress'))), flush=True)
            if copy['status'] == 'success':
                break
            assert copy['status'] == 'pending', sanitized(copy)
            time.sleep(30)
        uri = storage['primaryEndpoints']['blob'] + 'vhds/prepared.vhd'
    az('disk', 'revoke-access', '--ids', disk['id'])
    target = az('snapshot', 'create', '-g', state['keeper_rg'], '-n', state['snapshot'],
                '-l', state['location'], '--source', uri, '--source-storage-account-id', storage['id'],
                '--sku', 'Standard_LRS', '--incremental', 'false',
                '--network-access-policy', 'DenyAll', '--public-network-access', 'Disabled',
                '--tags', 'reprotoken=' + state['token'], 'preserve-all-snapshots=true')

assert target['provisioningState'] == 'Succeeded'
assert target['location'] == state['location'] and target['diskSizeGB'] == 512
assert not target.get('incremental', False)
assert target['tags']['reprotoken'] == state['token']
save('retained-regional-snapshot.json', target)
after = az('snapshot', 'list')
before = json.loads((ART / 'snapshots-before.json').read_text())
assert {s['id'].lower() for s in before} <= {s['id'].lower() for s in after}
save('snapshots-after-copy.json', after)
manifest = dict(subscriptionId=state['subscription'], location=state['location'],
                retainedSnapshot=dict(id=target['id'], diskSizeGiB=512, incremental=False),
                originalSourceSnapshot=source['id'], keeperResourceGroup=state['keeper_rg'],
                keeperOwnerToken=state['token'], preparedAtUtc=now(), method='VHD export/AzCopy/import' if opts.completed_fast_copy else 'VHD export/server-side copy/import')
(ROOT.parent / '.state/fio-hdd-westus2-fixture.json').write_text(json.dumps(manifest, indent=2))
save('fixture-manifest.json', manifest)
save('complete.json', dict(end=now(), retained_snapshot_id=target['id']))
print('Regional prepared snapshot verified and retained', flush=True)
