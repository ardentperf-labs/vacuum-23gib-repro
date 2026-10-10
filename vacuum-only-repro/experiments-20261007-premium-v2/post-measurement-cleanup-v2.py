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
TIMELINE_DIR = ROOT / 'recovery/cleanup-timeline'
PHASE_JOURNAL = TIMELINE_DIR / 'phase-events.jsonl'
RUN_ID = (dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ') +
          '-' + str(os.getpid()))
SCRIPT_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def now_utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def ensure_timeline_dir():
    if TIMELINE_DIR.is_symlink():
        raise RuntimeError('Cleanup timeline directory cannot be a symlink')
    TIMELINE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(TIMELINE_DIR, 0o700)


def append_timeline(event, **fields):
    ensure_timeline_dir()
    record = {'observedAtUtc': now_utc(), 'runId': RUN_ID,
              'event': event, 'scriptSha256': SCRIPT_SHA256, **fields}
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    fd = os.open(PHASE_JOURNAL, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        payload = (json.dumps(record, sort_keys=True) + '\n').encode()
        with os.fdopen(fd, 'ab', closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def write_json_receipt(path, value, *, replace=False):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    if path.is_symlink() or (path.exists() and not replace):
        raise RuntimeError('Refusing to overwrite cleanup timeline receipt: ' + str(path))
    temporary = path.with_name(path.name + '.tmp.' + str(os.getpid()))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    fd = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(fd, 'w', closefd=True) as stream:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def read(path):
    return json.loads(path.read_text())


def save(phase, **fields):
    temporary = STATE.with_suffix('.json.tmp')
    temporary.write_text(json.dumps({'phase': phase, 'updatedAtUtc':
        now_utc(), **fields}, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(STATE)
    append_timeline('cleanup_phase', phase=phase, stateFields=fields)


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
        teardown_script = stack / '04-teardown.sh'
        receipt_path = TIMELINE_DIR / 'teardown-observations' / (name + '-' + RUN_ID + '.json')
        command = ['bash', str(teardown_script)]
        teardown_script_sha256 = hashlib.sha256(teardown_script.read_bytes()).hexdigest()
        with (stack / 'teardown.log').open('ab', buffering=0) as log:
            popen_called_at = now_utc()
            process = subprocess.Popen(command,
                cwd=stack, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            popen_returned_at = now_utc()
        receipt = {
            'version': 1, 'runId': RUN_ID, 'stack': name,
            'phase': 'launched', 'scriptSha256': SCRIPT_SHA256,
            'teardownScriptSha256': teardown_script_sha256,
            'command': command, 'popenCalledAtUtc': popen_called_at,
            'popenReturnedAtUtc': popen_returned_at, 'pid': process.pid,
            'exitCode': None, 'exitObservedAtUtc': None,
            'teardownReceiptStatus': None, 'teardownCompletedAtUtc': None,
        }
        write_json_receipt(receipt_path, receipt)
        append_timeline('teardown_process_launched', stack=name, pid=process.pid,
                        popenCalledAtUtc=popen_called_at,
                        popenReturnedAtUtc=popen_returned_at,
                        teardownReceiptPath=str(receipt_path.relative_to(ROOT)))
        processes.append((name, process, receipt_path, receipt, stack))
    codes = {}
    for name, process, receipt_path, receipt, stack in processes:
        code = process.wait()
        exit_observed_at = now_utc()
        canonical_path = stack / '.state/teardown.json'
        canonical = read(canonical_path) if canonical_path.exists() else {}
        receipt.update({
            'phase': 'exit_observed', 'exitCode': code,
            'exitObservedAtUtc': exit_observed_at,
            'teardownReceiptStatus': canonical.get('status'),
            'teardownCompletedAtUtc': canonical.get('completedAtUtc'),
        })
        write_json_receipt(receipt_path, receipt, replace=True)
        append_timeline('teardown_process_exit_observed', stack=name, pid=process.pid,
                        exitCode=code, exitObservedAtUtc=exit_observed_at,
                        teardownReceiptStatus=canonical.get('status'),
                        teardownCompletedAtUtc=canonical.get('completedAtUtc'),
                        teardownReceiptPath=str(receipt_path.relative_to(ROOT)))
        codes[name] = code
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
