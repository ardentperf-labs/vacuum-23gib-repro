#!/usr/bin/env python3
"""Wait for the three verified preparations, then run measurements together."""
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import importlib.util

ROOT = Path(__file__).resolve().parent
RUNS = ('baseline-23gib', '50gib', '100gib')
STATE = ROOT / 'coordinator-state.json'


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def state(phase, **fields):
    save(STATE, {'phase': phase, 'pid': os.getpid(), 'updatedAtUtc': now(), **fields})
    print(now(), phase, flush=True)


def verify_preparations():
    evidence = {}
    pending = False
    for name in RUNS:
        stack = ROOT / name
        process = read(stack / '.state/prepare-process.json')
        if process.get('status') == 'failed':
            raise RuntimeError(f'{name} preparation failed; preserving its resources for diagnosis')
        if process.get('status') != 'complete':
            pending = True
            continue
        if process.get('exitCode') != 0:
            raise RuntimeError(f'{name} preparation receipt lacks exit 0')
        metadata = read(stack / '.state/prepare.json')
        infrastructure = read(stack / '.state/state.json')
        configuration = read(stack / 'run-config.json')
        if (metadata.get('status') != 'complete' or infrastructure.get('status') != 'ready' or
                metadata.get('fixtureTargetHeapGiB') != configuration['targetHeapGiB'] or
                metadata.get('instances') != 2 or metadata.get('clientCreated') is not False):
            raise RuntimeError(f'{name} prepared metadata does not match its assigned experiment')
        actual = hashlib.sha256((stack / '02-prepare.sh').read_bytes()).hexdigest()
        if actual != process['scriptSha256']['02-prepare.sh']:
            raise RuntimeError(f'{name} preparation script changed during its run')
        helper_hash = process.get('sourceStageHelperSha256')
        if helper_hash and hashlib.sha256((ROOT / 'recovery/source-restore/stage-allowlisted-source.sh').read_bytes()).hexdigest() != helper_hash:
            raise RuntimeError(f'{name} source-staging helper changed during its run')
        evidence[name] = {'preparedAtUtc': process['finishedAtUtc'],
                          'prepareScriptSha256': actual, 'heapBytes': metadata['heapBytes'],
                          'sourceStageHelperSha256': helper_hash,
                          'sourceSnapshotId': metadata['sourceSnapshotId'],
                          'replicaCaughtUpToLsn': metadata['replicaCaughtUpToLsn']}
    return None if pending else evidence


def main():
    lock = (ROOT / 'coordinator.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (ROOT / 'measurement-gate.json').exists():
        raise RuntimeError('measurement barrier already released; refusing duplicate VACUUM')
    state('waiting-for-preparation')
    deadline = time.monotonic() + 36 * 3600
    evidence = None
    next_update = time.monotonic() + 300
    while time.monotonic() < deadline:
        evidence = verify_preparations()
        if evidence:
            break
        if time.monotonic() >= next_update:
            state('waiting-for-preparation', preparations={name:
                  read(ROOT / name / '.state/prepare-process.json').get('status', 'not-started')
                  for name in RUNS})
            subprocess.run(['python3', str(ROOT / 'aggregate-results.py')], check=True,
                           cwd=ROOT, stdout=subprocess.DEVNULL)
            next_update = time.monotonic() + 300
        time.sleep(10)
    if evidence is None:
        raise RuntimeError('preparation wait exceeded 36 hours; preserving resources')
    module_spec = importlib.util.spec_from_file_location('hydration_barrier', ROOT / 'hydration-barrier.py')
    hydration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(hydration_module)
    state('waiting-for-live-hydration-audit')
    hydration_deadline = time.monotonic() + 24 * 3600
    while True:
        hydration = hydration_module.verify()
        if hydration['complete']:
            break
        if time.monotonic() >= hydration_deadline:
            raise RuntimeError('Live hydration barrier did not clear; no measurements started')
        time.sleep(30)
    save(ROOT / 'measurement-gate.json', {'authorized': True, 'releasedAtUtc': now(),
          'stacks': list(RUNS), 'preparationEvidence': evidence,
          'liveHydrationEvidence': hydration,
          'basis': 'User authorized all three runs; root reviewed and integrated the common framework. '
                   'Every preparation completed its storage, fixture, configuration and replay gates.'})
    state('launching-measurements', preparationEvidence=evidence)
    followers = []
    tests = []
    for name in RUNS:
        stack = ROOT / name
        with (stack / 'follower-supervisor.log').open('ab', buffering=0) as log:
            follower = subprocess.Popen([str(ROOT / '.venv/bin/python'),
                str(ROOT / 'follow-replication.py'), str(stack)],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, cwd=stack)
            followers.append((name, follower))
        with (stack / 'test-supervisor.log').open('ab', buffering=0) as log:
            test = subprocess.Popen(['python3', str(ROOT / 'run-phase.py'), str(stack), 'test'],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, cwd=stack)
            tests.append((name, test))
    state('measuring', processes={name: process.pid for name, process in tests},
          followers={name: process.pid for name, process in followers})
    while any(process.poll() is None for _, process in tests):
        time.sleep(10)
    codes = {name: process.returncode for name, process in tests}
    for name, follower in followers:
        try:
            follower.wait(timeout=60)
        except subprocess.TimeoutExpired:
            # Record a follower failure without stopping an unrelated process.
            state('log-follower-pending', stack=name, followerPid=follower.pid)
    subprocess.run(['python3', str(ROOT / 'aggregate-results.py')], check=True, cwd=ROOT)
    comparison = read(ROOT / 'comparison.json')
    if any(code != 0 for code in codes.values()) or not comparison.get('complete'):
        state('failed', exitCodes=codes, resourcesPreserved=True)
        return 2
    state('measurements-complete', exitCodes=codes)
    cleanup = read(ROOT / 'cleanup-gate.json')
    if cleanup.get('authorized'):
        state('cleaning-owned-compute', exitCodes=codes)
        subprocess.run(['python3', str(ROOT / 'post-measurement-cleanup.py')], check=True, cwd=ROOT)
        subprocess.run(['python3', str(ROOT / 'aggregate-results.py')], check=True, cwd=ROOT)
        state('complete', exitCodes=codes, snapshotsPreserved=True)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        state('failed', error=str(error), snapshotsProtectedByLocks=True)
        try:
            subprocess.run(['python3', str(ROOT / 'aggregate-results.py')], check=True, cwd=ROOT)
        except Exception:
            pass
        raise
