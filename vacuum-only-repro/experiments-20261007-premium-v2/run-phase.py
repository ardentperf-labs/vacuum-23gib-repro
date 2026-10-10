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
    parser.add_argument('phase', choices=['prepare', 'test'])
    args = parser.parse_args()
    stack = args.stack.resolve()
    root = Path(__file__).resolve().parent
    if stack.parent != root:
        raise RuntimeError('stack must be a direct child of this experiment workspace')
    configuration = json.loads((stack / 'run-config.json').read_text())
    state = stack / '.state'
    state.mkdir(exist_ok=True)
    receipt_path = state / (args.phase + '-process.json')
    if receipt_path.exists():
        raise RuntimeError(f'phase receipt already exists: {receipt_path}')
    environment = os.environ.copy()
    environment['TEST_HEAP_GIB'] = str(configuration['targetHeapGiB'])
    environment['SOURCE_MANIFEST'] = str(stack / 'source-manifest.json')
    if args.phase == 'prepare':
        command = ['bash', str(stack / '02-prepare.sh')]
    else:
        gate = json.loads((root / 'measurement-gate.json').read_text())
        if not gate.get('authorized') or stack.name not in gate.get('stacks', []):
            raise RuntimeError('measurement barrier has not been released for this stack')
        command = [str(root / '.venv/bin/python'), str(stack / '03-test.py'),
                   '--out', str(stack / 'results'), '--interval', '5',
                   '--safety-hours', '24', '--catchup-hours', '24']
    scripts = ['02-prepare.sh'] if args.phase == 'prepare' else ['03-test.py', 'plot.py']
    receipt = {'phase': args.phase, 'stack': stack.name, 'status': 'starting',
               'supervisorPid': os.getpid(), 'startedAtUtc': now(), 'command': command,
               'environment': {'TEST_HEAP_GIB': environment['TEST_HEAP_GIB'],
                               'SOURCE_MANIFEST': environment['SOURCE_MANIFEST']},
               'scriptSha256': {name: hashlib.sha256((stack / name).read_bytes()).hexdigest()
                                for name in scripts}}
    if args.phase == 'prepare' and 'STAGE_HELPER=' in (stack / '02-prepare.sh').read_text():
        helper = root / 'recovery/source-restore/stage-allowlisted-source.sh'
        receipt['sourceStageHelperSha256'] = hashlib.sha256(helper.read_bytes()).hexdigest()
    save(receipt_path, receipt)
    with (stack / (args.phase + '.log')).open('ab', buffering=0) as log:
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
