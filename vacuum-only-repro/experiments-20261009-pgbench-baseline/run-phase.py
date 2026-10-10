#!/usr/bin/env python3
"""Run one isolated experiment phase with a durable exit-status receipt."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stack', type=Path)
    parser.add_argument('phase', choices=['provision', 'prepare', 'restore', 'seed', 'finalize', 'condition', 'test'])
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    stack = args.stack.resolve()
    root = Path(__file__).resolve().parent
    if stack.parent != root:
        raise RuntimeError('stack must be a direct child of this experiment workspace')
    configuration = json.loads((stack / 'run-config.json').read_text())
    state = stack / '.state'
    state.mkdir(exist_ok=True)
    if args.attempt < 1 or (args.attempt != 1 and args.phase not in ('restore', 'finalize', 'condition', 'test')):
        raise RuntimeError('additional attempts are supported only for restore/finalize/condition/test')
    suffix = '' if args.attempt == 1 else f'-attempt-{args.attempt}'
    receipt_path = state / (args.phase + '-process' + ('' if args.phase == 'test' else suffix) + '.json')
    if args.attempt > 1:
        previous_suffix = (f'-attempt-{args.attempt-1}' if args.phase == 'test' else ('' if args.attempt == 2 else f'-attempt-{args.attempt-1}'))
        previous = json.loads((state / (args.phase + '-process' + previous_suffix + '.json')).read_text())
        if previous.get('status') != 'failed' or previous.get('exitCode') in (None, 0):
            raise RuntimeError('previous attempt must have a terminal failure receipt')
    if args.phase == 'test' and args.attempt > 1:
        failed_events = root / 'run' / f'results-attempt-{args.attempt-1}' / 'events.jsonl'
        events = [json.loads(line) for line in failed_events.read_text().splitlines() if line.strip()]
        if any(event.get('event') in ('observation_started', 'vacuum_started') for event in events):
            raise RuntimeError('test retries are allowed only before VACUUM launch')
        prior_dir = failed_events.parent
        if (prior_dir / 'client').exists():
            recovered = json.loads((prior_dir / 'pre-vacuum-launch-recovery.json').read_text())
            if not (recovered.get('vacuumStarted') is False and recovered.get('pgbenchExitCode') == 0 and recovered.get('remainingAppSessions') == 0 and recovered.get('logsCaptured') and recovered.get('credentialRemoved') and recovered.get('remoteTempRemoved')):
                raise RuntimeError('previous client launch must be gracefully drained and its evidence retained')
    if receipt_path.exists():
        raise RuntimeError(f'phase receipt already exists: {receipt_path}')
    environment = os.environ.copy()
    environment['TEST_HEAP_GIB'] = str(configuration['targetHeapGiB'])
    environment['SOURCE_MANIFEST'] = str(stack / 'source-manifest.json')
    environment['LOCATION'] = configuration['location']
    environment['AZURE_ZONE'] = configuration['zone']
    environment['NODE_VM_SIZE'] = 'Standard_D2s_v4'
    if args.phase == 'provision':
        command = ['bash', str(stack / '01-provision.sh')]
    elif args.phase == 'prepare':
        command = ['bash', str(stack / '02-prepare.sh')]
    elif args.phase == 'restore':
        infrastructure = json.loads((state / 'state.json').read_text())
        command = ['python3', str(root / 'recovery/restore-pending-disks.py'),
                   '--apply', '--owner-token', infrastructure['ownerToken']]
    elif args.phase == 'seed':
        infrastructure = json.loads((state / 'state.json').read_text())
        command = ['python3', str(stack / 'recovery/seed-replica-and-finalize.py'),
                   '--apply', '--owner-token', infrastructure['ownerToken']]
    elif args.phase == 'condition':
        infrastructure = json.loads((state / 'state.json').read_text())
        command = ['python3', str(root / ('recovery/condition-caches' + suffix + '.py')),
                   '--apply', '--owner-token', infrastructure['ownerToken']]
    elif args.phase == 'finalize':
        infrastructure = json.loads((state / 'state.json').read_text())
        finalizer_name = 'finalize-prepared-seed' + suffix + '.py'
        command = ['python3', str(stack / 'recovery' / finalizer_name),
                   '--apply', '--owner-token', infrastructure['ownerToken']]
    else:
        gate = json.loads((root / 'measurement-gate.json').read_text())
        if not gate.get('authorized') or stack.name not in gate.get('stacks', []):
            raise RuntimeError('measurement barrier has not been released for this stack')
        infrastructure = json.loads((state / 'state.json').read_text())
        if gate.get('ownerToken') != infrastructure['ownerToken']:
            raise RuntimeError('measurement gate belongs to a different stack owner')
        for phase in ('finalize', 'condition'):
            phase_paths = sorted(state.glob(f'{phase}-process*.json'),
                                 key=lambda path: json.loads(path.read_text()).get('attempt', 1))
            completed = json.loads(phase_paths[-1].read_text())
            if completed.get('status') != 'complete' or completed.get('exitCode') != 0:
                raise RuntimeError(f'{phase} must complete before measurement')
        proofs = gate.get('inputSha256', {})
        required = ('run/03-test.py', 'run/run-config.json', 'run/.state/prepare.json',
                    'run/.state/recovery/prepare-audit.json',
                    'run/.state/recovery/cache-conditioning.json')
        if any(proofs.get(name) != hashlib.sha256((root / name).read_bytes()).hexdigest()
               for name in required):
            raise RuntimeError('measurement inputs differ from the reviewed gate')
        command = [str(root / '.venv/bin/python'), str(stack / '03-test.py'),
                   '--out', str(stack / 'results'), '--interval', '5',
                   '--safety-hours', '24', '--catchup-hours', '24']
    scripts = {'provision': ['01-provision.sh', '04-teardown.sh'],
               'prepare': ['02-prepare.sh', 'recovery/prepare-source.py',
                           'run-config.json', 'source-manifest.json', 'snapshot.json'],
               'restore': ['../recovery/restore-pending-disks.py',
                           'run-config.json', 'source-manifest.json', 'snapshot.json'],
               'seed': ['recovery/seed-replica-and-finalize.py',
                        'run-config.json', 'source-manifest.json', 'snapshot.json'],
               'condition': ['../recovery/condition-caches' + suffix + '.py',
                             'run-config.json', 'source-manifest.json', 'snapshot.json'],
               'finalize': ['recovery/finalize-prepared-seed' + suffix + '.py',
                            'recovery/seed-replica-and-finalize.py',
                            'run-config.json', 'source-manifest.json', 'snapshot.json'],
               'test': ['03-test.py', 'plot.py', '../client/setup.sql',
                        '../client/transaction.sql', 'run-config.json', 'snapshot.json']}[args.phase]
    receipt = {'phase': args.phase, 'attempt': args.attempt, 'stack': stack.name, 'status': 'starting',
               'supervisorPid': os.getpid(), 'startedAtUtc': now(), 'command': command,
               'environment': {'TEST_HEAP_GIB': environment['TEST_HEAP_GIB'],
                               'SOURCE_MANIFEST': environment['SOURCE_MANIFEST'],
                               'LOCATION': environment['LOCATION'],
                               'AZURE_ZONE': environment['AZURE_ZONE'],
                               'NODE_VM_SIZE': environment['NODE_VM_SIZE']},
               'scriptSha256': {name: hashlib.sha256((stack / name).read_bytes()).hexdigest()
                                for name in scripts}}
    if args.phase == 'prepare' and 'STAGE_HELPER=' in (stack / '02-prepare.sh').read_text():
        helper = root / 'recovery/source-restore/stage-allowlisted-source.sh'
        receipt['sourceStageHelperSha256'] = hashlib.sha256(helper.read_bytes()).hexdigest()
    save(receipt_path, receipt)
    with (stack / (args.phase + suffix + '.log')).open('ab', buffering=0) as log:
        process = subprocess.Popen(command, cwd=stack, env=environment,
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        receipt.update(status='running', childPid=process.pid)
        save(receipt_path, receipt)
        exit_code = process.wait()
    receipt.update(status='complete' if exit_code == 0 else 'failed',
                   finishedAtUtc=now(), exitCode=exit_code)
    save(receipt_path, receipt)
    return exit_code


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(2)
