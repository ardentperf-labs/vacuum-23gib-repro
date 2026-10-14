#!/usr/bin/env python3
"""Coordinate two isolated runs; preserve evidence before removing owned compute."""
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parent
SHARED = ROOT / 'shared'
PYTHON = ROOT.parents[1] / '.venv/bin/python'
NAMES = ('sigterm', 'control')
EVENT_LOCK = threading.Lock()

def now(): return dt.datetime.now(dt.timezone.utc).isoformat()
def save(path, obj):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(obj, indent=2) + '\n'); temp.replace(path)
def event(phase, **data):
    obj = dict(atUtc=now(), phase=phase, **data)
    with EVENT_LOCK:
        with (ROOT / 'controller-events.jsonl').open('a') as f: f.write(json.dumps(obj) + '\n')
        save(ROOT / 'controller-status.json', obj)
        print(json.dumps(obj), flush=True)
def command(stack, argv, log, timeout=None):
    with (stack / log).open('ab', buffering=0) as out:
        subprocess.run(argv, cwd=stack, stdout=out, stderr=out, check=True, timeout=timeout)
def spawn(stack, argv, log):
    with (stack / log).open('ab', buffering=0) as out:
        return subprocess.Popen(argv, cwd=stack, stdout=out, stderr=out, stdin=subprocess.DEVNULL)
def parallel(fn):
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        tasks = {pool.submit(fn, ROOT / name): name for name in NAMES}
        errors = []
        for future in cf.as_completed(tasks):
            try: future.result()
            except BaseException as e: errors.append(f'{tasks[future]}: {e}')
        if errors: raise RuntimeError('; '.join(errors))
def wait_provision(stack):
    end = time.monotonic() + 7200
    while time.monotonic() < end:
        state = stack / '.state/state.json'
        if state.exists() and json.loads(state.read_text()).get('status') == 'ready':
            event('stack_provisioned', stack=stack.name); return
        launch = json.loads((stack / 'provision-launch.json').read_text())
        try: os.kill(launch['pid'], 0)
        except ProcessLookupError: raise RuntimeError('Provision process exited before ready; inspect provision.log')
        time.sleep(5)
    raise TimeoutError('Provisioning exceeded two hours')
def prepare(stack):
    event('stack_preparing', stack=stack.name)
    launch = stack / 'prepare-launch.json'
    finalizer_launch = stack / '.state/finalize-launch.json'
    if finalizer_launch.exists(): launch = finalizer_launch
    prepared = stack / '.state/prepare.json'
    if launch.exists():
        pid = json.loads(launch.read_text())['pid']
        end = time.monotonic() + 10800
        while not prepared.exists():
            try: os.kill(pid, 0)
            except ProcessLookupError: raise RuntimeError('Preparation process exited; inspect prepare.log')
            if time.monotonic() > end: raise TimeoutError('Preparation exceeded three hours')
            time.sleep(5)
        assert json.loads(prepared.read_text())['status'] == 'complete'
    elif not prepared.exists():
        command(stack, ['bash', str(stack / '02-prepare.sh'), '--apply'], 'prepare.log')
    event('configuring_and_proving_wal_archive', stack=stack.name)
    command(stack, [str(PYTHON), str(stack / 'setup-wal-archive.py'), '--stack', stack.name, '--apply'], 'wal-archive-setup.log')
    archive = json.loads((stack / '.state/wal-archive.json').read_text())
    smoke = json.loads((stack / '.state/wal-archive-smoke.json').read_text())
    assert archive.get('status') == 'ready' and smoke.get('status') == 'complete'
    assert smoke.get('restorePodRole') == 'replica' and smoke.get('checks', {}).get('allThreeHashesMatch') is True
    assert smoke.get('checks', {}).get('livePgWalTargeted') is False
    hashes = smoke['hashes']; assert len(set(hashes.values())) == 1
    preparation = stack / 'results/preparation'; preparation.mkdir(parents=True, exist_ok=True)
    for receipt in ('wal-archive.json', 'wal-archive-smoke.json'):
        shutil.copy2(stack / '.state' / receipt, preparation / receipt)
    event('wal_archive_upload_and_replica_restore_verified', stack=stack.name, walName=smoke['walName'])
    command(stack, [str(PYTHON), str(stack / 'verify-attachments.py')], 'attachment-verification.log')
    run = stack / 'results'; run.mkdir(exist_ok=True)
    assert not (run / 'measurement').exists(), 'Existing measurement must be preserved before another launch'
    roles = json.loads((stack / '.state/attachment-verification/verified.json').read_text())['roles']
    for role in ('primary', 'replica'):
        with (run / f'{role}-controller-queues.log').open('w') as out:
            subprocess.run(['kubectl', '--kubeconfig', str(stack / '.state/kubeconfig'), '-n', 'repro',
                            'exec', '-i', roles[role]['pod'], '-c', 'postgres', '--', 'sh', '-s'],
                           input=(stack / 'controller-probe.sh').read_text(), text=True,
                           stdout=out, stderr=subprocess.STDOUT, check=True, timeout=120)
    command(stack, [str(PYTHON), str(stack / 'clear-guest-cache.py'), '--kubeconfig',
                   str(stack / '.state/kubeconfig'), '--namespace', 'repro', '--out', str(run)], 'cache-clear.log')
    event('stack_prepared', stack=stack.name)
def verify_harvest(stack):
    receipt = json.loads((stack / '.state/downloads.json').read_text())
    assert receipt['remoteArtifactsDownloaded'] and receipt['files']
    for item in receipt['files']:
        p = stack / item['path']
        assert p.stat().st_size == item['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest() == item['sha256']
    run = stack / 'results'
    timing = json.loads((run / 'measurement/timing.json').read_text())
    coverage = json.loads((run / 'instrumentation/coverage.json').read_text())
    required = ['measurement/samples.jsonl', 'measurement/vacuum.log', 'measurement/client/sessions.jsonl',
                'instrumentation/primary-pgstat.jsonl', 'instrumentation/replica-pgstat.jsonl',
                'instrumentation/primary-diskstats.jsonl', 'instrumentation/replica-diskstats.jsonl',
                'instrumentation/azure/azure-metrics.jsonl']
    missing = [x for x in required if not (run / x).is_file() or not (run / x).stat().st_size]
    assert not missing, f'Required measurements missing: {missing}'
    save(run / 'download-verification.json', {'atUtc': now(), 'checksumsVerified': True,
         'remoteLogsVerified': True, 'testComplete': timing.get('testComplete'),
         'collectorFatalErrors': coverage.get('fatalErrors'), 'coverage': coverage})
    return timing.get('testComplete', False)

def main():
    collectors = {}; tests = {}; codes = {}; watchers = []
    try:
        event('waiting_for_provisioning'); parallel(wait_provision)
        for name in NAMES:
            stack = ROOT / name
            for source in SHARED.iterdir():
                if source.name.startswith('test_') or source.name == '__pycache__': continue
                if source.name == '01-provision.sh': continue
                if source.is_dir(): shutil.copytree(source, stack / source.name, dirs_exist_ok=True)
                else:
                    target = stack / source.name
                    if not target.exists() or hashlib.sha256(target.read_bytes()).digest() != hashlib.sha256(source.read_bytes()).digest():
                        shutil.copy2(source, target)
        event('preparing_both_stacks'); parallel(prepare)
        event('starting_collectors')
        for name in NAMES:
            stack = ROOT / name
            base = ['kubectl', '--kubeconfig', str(stack / '.state/kubeconfig')]
            private_logs = stack / 'results/diagnosis/private'
            private_logs.mkdir(parents=True, exist_ok=True, mode=0o700)
            for role, pod in (('primary', 'hdd-pg-1'), ('replica', 'hdd-pg-2')):
                watchers.append(spawn(stack, [str(PYTHON), str(stack / 'watch-recovery.py'),
                    '--kind', 'postgres', '--pod', pod,
                    '--kubeconfig', str(stack / '.state/kubeconfig'),
                    '--out', str(private_logs / f'live-{role}-postgres.log'),
                    '--gaps', str(private_logs / f'live-{role}-postgres-gaps.jsonl')],
                    f'{role}-postgres-watcher.log'))
            for kind, filename in (('cluster','cluster-watch.json'),('operator','cnpg-operator.log')):
                watchers.append(spawn(stack,[str(PYTHON),str(stack/'watch-recovery.py'),'--kind',kind,
                    '--kubeconfig',str(stack/'.state/kubeconfig'),'--out',str(stack/'results'/filename),
                    '--gaps',str(stack/'results'/f'{kind}-watcher-gaps.jsonl')],f'{kind}-watcher.log'))
            collectors[name] = spawn(stack, [str(PYTHON), str(stack / 'instrumentation/collect.py'),
                '--namespace', 'repro', '--database', 'repro', '--verified-attachments',
                str(stack / '.state/attachment-verification/verified.json'), '--out', str(stack / 'results/instrumentation'),
                '--kubeconfig', str(stack / '.state/kubeconfig'), '--client-app-name', 'vacuum-pgbench-',
                '--metric-drain-seconds', '300'], 'instrumentation.log')
        end = time.monotonic() + 1800
        while time.monotonic() < end:
            ready = []
            for name, process in collectors.items():
                if process.poll() is not None: raise RuntimeError(f'{name} collector exited before measurement')
                p = ROOT / name / 'results/instrumentation/collector-status.json'
                ready.append(p.exists() and json.loads(p.read_text()).get('phase') == 'collecting')
            if all(ready): break
            time.sleep(3)
        else: raise TimeoutError('Collectors not ready within 30 minutes')
        event('measurement_barrier_released')
        for name in NAMES:
            stack = ROOT / name
            tests[name] = spawn(stack, [str(PYTHON), str(stack / '03-test.py'), '--out', str(stack / 'results/measurement'),
                '--intervention-mode', name, '--minimum-observation-seconds', '8400'], 'test.log')
        while any(x.poll() is None for x in tests.values()):
            time.sleep(5)
        codes = {name: p.returncode for name, p in tests.items()}
        event('measurements_finished', exitCodes=codes)
    except BaseException as e:
        event('controller_error', error=str(e))
        # A detached VACUUM is never destroyed by controller failure.
        for p in tests.values():
            if p.poll() is None: p.wait()
        raise
    finally:
        for p in watchers:
            if p.poll() is None: p.terminate()
        for p in watchers:
            try: p.wait(timeout=30)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
        for p in collectors.values():
            if p.poll() is None: p.send_signal(signal.SIGTERM)
        for name, p in collectors.items():
            try: codes[name + '-collector'] = p.wait(timeout=1800)
            except subprocess.TimeoutExpired: event('collector_stop_timeout', stack=name)
        save(ROOT / 'process-exit-codes.json', codes)
    event('harvesting')
    for name in NAMES:
        stack = ROOT / name
        command(stack, [str(PYTHON), str(stack / 'harvest.py'), str(stack / 'results/downloaded')], 'harvest.log')
        verify_harvest(stack)
        command(stack, [str(PYTHON), str(stack / 'client-report.py'), str(stack / 'results/measurement')], 'client-report.log')
        command(stack, [str(PYTHON), str(stack / 'client-replication.py'), str(stack / 'results/measurement')], 'client-replication.log')
    for name in NAMES:
        stack = ROOT / name
        command(stack, [str(PYTHON), str(stack / 'capture-diagnosis.py'), '--namespace', 'repro',
            '--cluster', 'hdd-pg', '--kubeconfig', str(stack / '.state/kubeconfig'),
            '--out', str(stack / 'results/diagnosis/public'), '--since', '24h'], 'diagnosis-capture.log')
    for name in NAMES:
        stack = ROOT / name
        command(stack, [str(PYTHON), str(stack / 'analysis-restore-path-report.py'),
            '--postgres-log', str(stack / 'results/downloaded/hdd-pg-2-postgres.log'),
            '--postgres-log', str(stack / 'results/diagnosis/public/postgres-log-events-redacted.jsonl'),
            '--intervention', str(stack / 'results/measurement/walreceiver-intervention.json'),
            '--primary-pgstat', str(stack / 'results/instrumentation/primary-pgstat.jsonl'),
            '--replica-pgstat', str(stack / 'results/instrumentation/replica-pgstat.jsonl'),
            '--out', str(stack / 'results/diagnosis/public/restore-path-report.json')], 'restore-path-report.log')
    restore_path = json.loads((ROOT / 'sigterm/results/diagnosis/public/restore-path-report.json').read_text())
    if restore_path['archiveRestore']['successfulRestoreCountAfterSignal'] < 1:
        raise RuntimeError('No successful archive restore observed after SIGTERM; preserving resources for diagnosis')
    event('post_sigterm_archive_restore_verified', successfulRestores=restore_path['archiveRestore']['successfulRestoreCountAfterSignal'])
    event('results_downloaded_and_verified', exitCodes=codes)
    if any(codes.values()):
        raise RuntimeError('Measurement or collector failed; preserving resources for diagnosis')
    command(ROOT, [str(PYTHON), str(ROOT / 'build-comparison.py')], 'comparison-build.log')
    event('analysis_verified_before_teardown')
    event('exporting_owned_wal_archives')
    parallel(lambda stack: command(stack, [str(PYTHON), str(stack / 'export-wal-archive.py'),
        '--stack', stack.name, '--apply'], 'wal-archive-export.log'))
    event('wal_archives_preserved_locally_before_teardown')
    # The stack owning the regional watcher is removed last.
    cleanup = sorted(NAMES, key=lambda n: bool(json.loads((ROOT/n/'.state/state.json').read_text()).get('networkWatcher', {}).get('owned')))
    for name in cleanup:
        stack = ROOT / name
        command(stack, ['bash', str(stack / '04-teardown.sh')], 'teardown.log')
        event('stack_removed', stack=name)
    event('complete', exitCodes=codes)
    command(ROOT, [str(PYTHON), str(ROOT / 'publish-local.py')], 'publication-local.log')
    return 0 if not any(codes.values()) else 2

if __name__ == '__main__': raise SystemExit(main())
