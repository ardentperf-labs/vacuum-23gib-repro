#!/usr/bin/env python3
"""Run instrumentation and VACUUM, harvest locally, then remove owned infrastructure."""
import datetime, json, os, signal, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'
PYTHON = Path(sys.executable)
run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
run_dir = ROOT / 'results' / run_id
run_dir.mkdir(parents=True, exist_ok=False)

def status(phase, **more):
    obj = {'phase': phase, 'atUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'runId': run_id, **more}
    (SD / 'controller.json').write_text(json.dumps(obj, indent=2) + '\n')
    print(json.dumps(obj), flush=True)

def run(argv, log):
    with (run_dir / log).open('w') as f:
        subprocess.run(argv, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT, check=True)

collector = None
test = None
try:
    status('verifying_attachments')
    run([sys.executable, str(ROOT / 'verify-attachments.py')], 'attachment-verification.log')
    roles = json.loads((SD / 'attachment-verification/verified.json').read_text())['roles']
    status('verifying_guest_controllers_and_queues')
    for role in ('primary', 'replica'):
        with (run_dir / f'{role}-controller-queues.log').open('w') as logfile:
            subprocess.run(['kubectl', '--kubeconfig', str(SD / 'kubeconfig'),
                            '-n', 'repro', 'exec', '-i', roles[role]['pod'],
                            '-c', 'postgres', '--', 'sh', '-s'],
                           input=(ROOT / 'controller-probe.sh').read_text(), text=True,
                           stdout=logfile, stderr=subprocess.STDOUT, check=True)
    status('clearing_verified_guest_caches', preparationChange='guest Linux drop_caches=3; Azure ReadOnly host cache remains enabled')
    run([sys.executable, str(ROOT / 'clear-guest-cache.py'),
         '--kubeconfig', str(SD / 'kubeconfig'), '--namespace', 'repro', '--out', str(run_dir)],
        'guest-cache-clear.log')
    cache_clear = json.loads((run_dir / 'guest-cache-clearing.json').read_text())
    if cache_clear.get('phase') != 'complete' or set(cache_clear.get('roles', {})) != {'primary', 'replica'}:
        raise RuntimeError('Guest cache clearing did not complete on both verified nodes')
    status('guest_caches_cleared', preparationChange=cache_clear['preparationChange'],
           azureReadOnlyHostCachePurged=cache_clear['azureReadOnlyHostCachePurged'])
    replica = roles['replica']
    instr = run_dir / 'replica-instrumentation'
    with (run_dir / 'instrumentation.log').open('w') as logfile:
        collector = subprocess.Popen([sys.executable, str(ROOT / 'instrumentation/collect.py'),
             '--kubeconfig', str(SD / 'kubeconfig'), '--namespace', 'repro',
             '--replica-pod', replica['pod'], '--node', replica['node'],
             '--vm-resource-id', replica['vmResourceId'], '--require-verified-attachments',
             '--out', str(instr)], stdout=logfile, stderr=subprocess.STDOUT, cwd=ROOT)
        status('instrumentation_preflight')
        deadline = time.monotonic() + 900
        bcc_started = None
        readiness_checkpoint = False
        while True:
            p = instr / 'collector-status.json'
            if p.exists() and json.loads(p.read_text()).get('phase') == 'collecting': break
            if collector.poll() is not None: raise RuntimeError('Required instrumentation failed before VACUUM')
            if time.monotonic() > deadline: raise RuntimeError('Instrumentation readiness timeout')
            raw = instr / 'biolatency.raw.log'
            if bcc_started is None and raw.exists() and '__BCC_TOOL_START__' in raw.read_text(errors='replace'):
                bcc_started = time.monotonic()
            if bcc_started is not None and not readiness_checkpoint and time.monotonic() - bcc_started >= 5:
                # An idle replica may produce no disk histogram. A preparation
                # checkpoint gives the receiver real WAL I/O without scanning
                # or warming the fixture. 03-test performs its own checkpoint.
                run(['kubectl', '--kubeconfig', str(SD / 'kubeconfig'), '-n', 'repro',
                     'exec', roles['primary']['pod'], '-c', 'postgres', '--',
                     'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
                     '-d', 'repro', '-c', 'CHECKPOINT;'], 'instrumentation-readiness-checkpoint.log')
                readiness_checkpoint = True
                status('instrumentation_preflight', readinessCheckpointPerformed=True)
            time.sleep(2)
        if collector.poll() is not None: raise RuntimeError('Instrumentation exited before VACUUM')
        status('measuring')
        with (run_dir / 'test.log').open('w') as f:
            test = subprocess.Popen([str(PYTHON), str(ROOT / '03-test.py'), '--out', str(run_dir / 'measurement')],
                                    stdout=f, stderr=subprocess.STDOUT, cwd=ROOT)
            while test.poll() is None:
                time.sleep(5)
            test_rc = test.returncode
        status('harvesting', testExitCode=test_rc)
        if collector.poll() is None:
            collector.send_signal(signal.SIGTERM)
        collector_rc = collector.wait(timeout=600)
        final_collector_status = json.loads((instr / 'collector-status.json').read_text())
        if final_collector_status.get('phase') != 'stopped': collector_rc = collector_rc or 2
    run([sys.executable, str(ROOT / 'harvest.py'), str(run_dir / 'downloaded')], 'harvest.log')
    run([sys.executable, str(ROOT / 'verify-harvest.py'), str(run_dir)], 'download-verification.log')
    status('tearing_down', testExitCode=test_rc, instrumentationExitCode=collector_rc)
    run([str(ROOT / '04-teardown.sh')], 'teardown.log')
    status('complete' if test_rc == 0 and collector_rc == 0 else 'completed_with_errors',
           testExitCode=test_rc, instrumentationExitCode=collector_rc, infraDeleted=True)
except BaseException as e:
    # A detached VACUUM must finish; do not tear down while it may still be active.
    if test is not None and test.poll() is None:
        status('waiting_for_vacuum_after_controller_error', error=str(e))
        test.wait()
    if collector is not None and collector.poll() is None:
        collector.send_signal(signal.SIGTERM)
        try: collector.wait(timeout=600)
        except subprocess.TimeoutExpired: status('collector_stop_timeout', error=str(e)); raise
    status('recovering_after_error', error=str(e), infraDeleted=False)
    try:
        run([sys.executable, str(ROOT / 'harvest.py'), str(run_dir / 'downloaded')], 'harvest-after-error.log')
        run([sys.executable, str(ROOT / 'verify-harvest.py'), str(run_dir), '--allow-incomplete'], 'download-verification-after-error.log')
        run([str(ROOT / '04-teardown.sh')], 'teardown-after-error.log')
        status('failed', error=str(e), infraDeleted=True, evidenceDownloaded=True)
    except Exception as cleanup_error:
        # Keep harvest failures reviewable; remote evidence must not be destroyed.
        status('failed', error=str(e), cleanupError=str(cleanup_error), infraDeleted=False)
    raise
