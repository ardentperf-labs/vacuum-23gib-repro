#!/usr/bin/env python3
"""Clean owned experiment compute after successful measurements, preserving snapshots."""
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
RUNS = ('baseline-23gib', '50gib', '100gib')
STATE = ROOT / 'cleanup-state.json'


def read(path):
    return json.loads(path.read_text())


def save(phase, **fields):
    temporary = STATE.with_suffix('.json.tmp')
    temporary.write_text(json.dumps({'phase': phase, 'updatedAtUtc':
        dt.datetime.now(dt.timezone.utc).isoformat(), **fields}, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(STATE)


def audit(label):
    subprocess.run(['python3', str(ROOT / 'retention-audit.py'), '--label', label], check=True)
    return read(ROOT / 'preflight' / ('snapshot-audit-' + label + '.json'))


def verify_preservation(before, after):
    current = {snapshot['id'].lower(): snapshot['uniqueId'] for snapshot in after['snapshots']}
    for snapshot in before['snapshots']:
        if current.get(snapshot['id'].lower()) != snapshot['uniqueId']:
            raise RuntimeError('Snapshot identity lost or changed during cleanup: ' + snapshot['id'])


def main():
    gate = read(ROOT / 'cleanup-gate.json')
    if gate.get('authorized') is not True or gate.get('stacks') != list(RUNS):
        raise RuntimeError('Root has not approved this exact cleanup scope')
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != gate.get('postCleanupScriptSha256'):
        raise RuntimeError('Cleanup orchestrator differs from the reviewed version')
    if not read(ROOT / 'comparison.json').get('complete'):
        raise RuntimeError('Measurements are incomplete; preserving experiment compute')
    for name in RUNS:
        stack = ROOT / name
        if read(stack / '.state/test-process.json').get('exitCode') != 0:
            raise RuntimeError('Measurement process did not exit successfully: ' + name)
        actual = hashlib.sha256((stack / '04-teardown.sh').read_bytes()).hexdigest()
        if actual != gate['teardownScriptSha256']:
            raise RuntimeError('Cleanup script differs from the reviewed version: ' + name)
    save('auditing-before-cleanup')
    before = audit('before-compute-cleanup')
    guard_path = ROOT / 'recovery/wal-cleanup-guard/verify_wal_retention.py'
    reviewed_guard_hash = '8acfa114c4ac9927ac47c31bf17108fadc65963713d9fd3c393064436a27a7fb'
    if (gate.get('walRetentionVerifierSha256') != reviewed_guard_hash or
            hashlib.sha256(guard_path.read_bytes()).hexdigest() != reviewed_guard_hash):
        raise RuntimeError('WAL retention verifier differs from the reviewed version')
    guard_spec = importlib.util.spec_from_file_location('wal_retention_guard', guard_path)
    guard_module = importlib.util.module_from_spec(guard_spec)
    guard_spec.loader.exec_module(guard_module)
    wal_retention = guard_module.verify()
    wal_receipt = ROOT / 'preflight/wal-retention-before-cleanup.json'
    temporary = wal_receipt.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(wal_retention, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(wal_receipt)
    save('removing-owned-compute', walArchiveRetentionVerified=True,
         walArchiveRetentionReceipt=str(wal_receipt))
    processes = []
    for name in RUNS:
        stack = ROOT / name
        with (stack / 'teardown.log').open('ab', buffering=0) as log:
            process = subprocess.Popen(['bash', str(stack / '04-teardown.sh')],
                cwd=stack, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            processes.append((name, process))
    codes = {name: process.wait() for name, process in processes}
    if any(code != 0 for code in codes.values()):
        raise RuntimeError('Compute cleanup did not complete: ' + json.dumps(codes))
    for name in RUNS:
        if read(ROOT / name / '.state/teardown.json').get('status') != 'complete':
            raise RuntimeError('Missing completed teardown receipt: ' + name)
    after = audit('after-compute-cleanup')
    verify_preservation(before, after)

    # Shared watchers were created by root for this batch and are removed only
    # once every experiment cluster, test RG and node RG is absent.
    token = json.loads(subprocess.check_output(['az', 'account', 'get-access-token',
        '--resource', 'https://management.azure.com/', '-o', 'json'],
        text=True, timeout=900))['accessToken']

    def request(method, resource_id, version, missing_ok=False):
        query = urllib.request.Request('https://management.azure.com' + resource_id +
            '?api-version=' + version, method=method,
            headers={'Authorization': 'Bearer ' + token})
        try:
            with urllib.request.urlopen(query, timeout=60) as response:
                data = response.read()
            return json.loads(data) if data else {}
        except urllib.error.HTTPError as error:
            if missing_ok and error.code == 404:
                return None
            raise RuntimeError(f'ARM {method} HTTP {error.code} for {resource_id}') from None

    for name in RUNS:
        infrastructure = read(ROOT / name / '.state/state.json')
        prefix = '/subscriptions/' + infrastructure['subscriptionId'] + '/resourceGroups/'
        for group in (infrastructure['resourceGroup'], infrastructure['requestedNodeResourceGroup']):
            if request('GET', prefix + group, '2022-09-01', missing_ok=True) is not None:
                raise RuntimeError('Experiment RG is still present; preserving shared watcher: ' + group)
    save('removing-owned-shared-watchers')
    watchers = read(ROOT / 'preflight/shared-network-watchers.json')
    for watcher in watchers['regions']:
        group_id = '/subscriptions/' + watchers['subscriptionId'] + '/resourceGroups/' + watcher['resourceGroup']
        group = request('GET', group_id, '2022-09-01', missing_ok=True)
        if group is None:
            continue
        if (group.get('tags', {}).get('reprotoken') != watchers['ownerToken'] or
                group.get('tags', {}).get('reproPurpose') != 'shared-network-watcher'):
            raise RuntimeError('Shared watcher RG ownership changed')
        listing = request('GET', group_id + '/resources', '2021-04-01')
        if listing.get('nextLink'):
            raise RuntimeError('Shared watcher resource inventory is paginated; preserving group for review')
        resources = listing['value']
        if any(resource['id'].lower() != watcher['id'].lower() for resource in resources):
            raise RuntimeError('Shared watcher RG contains another resource; preserving it')
        resource = request('GET', watcher['id'], '2025-09-01', missing_ok=True)
        if resource is not None:
            if resource.get('tags', {}).get('reprotoken') != watchers['ownerToken']:
                raise RuntimeError('Shared watcher ownership changed')
            request('DELETE', watcher['id'], '2025-09-01')
            deadline = time.monotonic() + 1800
            while request('GET', watcher['id'], '2025-09-01', missing_ok=True) is not None:
                if time.monotonic() > deadline:
                    raise RuntimeError('Shared watcher deletion did not finish')
                time.sleep(10)
        listing = request('GET', group_id + '/resources', '2021-04-01')
        if listing.get('nextLink'):
            raise RuntimeError('Shared watcher resource inventory is paginated; preserving group for review')
        remaining = listing['value']
        if remaining:
            raise RuntimeError('Shared watcher RG is not empty; preserving it')
        request('DELETE', group_id, '2022-09-01')
        deadline = time.monotonic() + 1800
        while request('GET', group_id, '2022-09-01', missing_ok=True) is not None:
            if time.monotonic() > deadline:
                raise RuntimeError('Shared watcher RG deletion did not finish')
            time.sleep(10)
    final = audit('after-shared-cleanup')
    verify_preservation(before, final)
    save('complete', snapshotCount=final['currentSnapshotCount'],
         priorSnapshotsPreserved=True, allExperimentSnapshotsPreserved=True,
         teardownExitCodes=codes, walArchiveRetentionVerified=True,
         walArchiveRetentionReceipt=str(wal_receipt))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        save('failed', error=str(error), snapshotsPreservedByLocks=True)
        raise
