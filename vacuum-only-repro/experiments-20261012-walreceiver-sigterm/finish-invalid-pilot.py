#!/usr/bin/env python3
"""Honor the user's stop instruction, draining clients and preserving invalid-run evidence."""
import concurrent.futures as cf
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REQUESTED = dt.datetime.now(dt.timezone.utc).isoformat()
REASON = 'missing archive configuration; user requested finishing only if less than one hour remained'

def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')

def finish(lane):
    stack = ROOT / lane
    sys.path.insert(0, str(stack))
    spec = importlib.util.spec_from_file_location('pilot_' + lane, stack / '03-test.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    out = stack / 'results/measurement'
    client = json.loads((out / 'client-process.json').read_text())
    vacuum = json.loads((out / 'vacuum-process.json').read_text())
    base = ['kubectl', '--kubeconfig', str(stack / '.state/kubeconfig')]
    ns = 'repro'; primary = 'hdd-pg-1'
    # Verify the PID file, executable, UID and run directory before requesting
    # pgbench's duration-handler shutdown. No writer transaction is canceled.
    script = '''set -eu
pid="$1"; work="$2"
[ "$(cat "$work/pgbench.pid")" = "$pid" ]
tr '\\000' '\\n' <"/proc/$pid/cmdline" | grep -Fx -- "--file=$work/transaction.sql" >/dev/null
tr '\\000' '\\n' <"/proc/$pid/cmdline" | grep -Fx -- "--log-prefix=$work/pgbench-log" >/dev/null
case "$(readlink -f "/proc/$pid/exe")" in */pgbench) :;; *) exit 41;; esac
[ "$(awk '$1=="Uid:" {print $2}' "/proc/$pid/status")" = "$(id -u)" ]
printf 'verified pgbench pid=%s\n' "$pid"
'''
    verified = m.cmd(base + ['exec', '-n', ns, primary, '-c', 'postgres', '--',
        'sh', '-c', script, 'verify-client-stop', str(client['pgbenchPid']), client['workDirectory']])
    before = m.sql(base, ns, primary, 'repro', m.client_activity_query(client['applicationName']))
    stop = m.stop_pgbench(base, ns, primary, client, timeout=180)
    stop['identityVerification'] = verified.strip()
    stop['serverActivityBeforeStopRequest'] = before
    drained = m.sql(base, ns, primary, 'repro', m.client_activity_query(client['applicationName']))
    if drained.get('benchSessions') != 0:
        raise RuntimeError('Clients did not drain; retaining infrastructure')
    stop['serverActivityAfterDrain'] = drained
    save(out / 'client-stop.json', stop)
    m.append(out / 'client/sessions.jsonl', dict(drained, sampleStatus='ok', manualUserStop=True))
    summary = m.capture_pgbench_results({'client': client, 'base': base, 'namespace': ns,
        'pod': primary, 'outputDirectory': str(out), 'stopResult': stop, 'drainedSample': drained})
    if summary.get('validationErrors'):
        raise RuntimeError('Native client totals did not reconcile; retaining infrastructure')
    rc = m.remote_status(base, ns, primary, vacuum['workDirectory'])
    if rc != '0':
        raise RuntimeError('VACUUM is not complete with exit zero; retaining infrastructure')
    m.fetch_log(base, ns, primary, vacuum['workDirectory'], out / 'vacuum.log')
    marks = {}
    for kind in ('start', 'complete'):
        raw = m.cmd(base + ['exec', '-n', ns, primary, '-c', 'postgres', '--',
            'cat', vacuum['workDirectory'] + '/vacuum-' + kind + '.marker']).strip()
        f = raw.split('|')
        if len(f) != 6 or f[0] != 'VACUUM_' + kind.upper():
            raise RuntimeError('Invalid real VACUUM marker')
        marks[kind] = dict(time=f[1], flushLsn=f[2], walBytes=int(f[3]), walRecords=int(f[4]), walFpi=int(f[5]))
    pods = {role: json.loads(m.cmd(base + ['get', 'pod', name, '-n', ns, '-o', 'json']))
            for role, name in [('primary', primary), ('replica', 'hdd-pg-2')]}
    preflight = json.loads((out / 'preflight.json').read_text())
    final = m.both(base, ns, 'repro', vacuum['applicationName'], client['applicationName'],
        pods, preflight['storage'], expected_bench_sessions=0)
    rows = [json.loads(line) for line in (out / 'samples.jsonl').read_text().splitlines() if line.strip()]
    last_seq = max(r.get('seq', 0) for r in rows)
    m.append(out / 'samples.jsonl', dict(final, seq=last_seq + 1, phase='user_stopped_invalid_pilot'))
    rows.append(final)
    replay = final['replica']['replayLsn']; flush = final['primary']['flushLsn']
    intervention = json.loads((out / 'walreceiver-intervention.json').read_text())
    user_stop = dict(stopped=True, reason=REASON, requestedAtUtc=REQUESTED,
        finishedAtUtc=stop['finishedAtUtc'], testComplete=False,
        estimatedReplayMinutesAtDecision=72, nativeClientTotalsReconciled=True,
        replayCatchupSkipped=True, vacuumCompletedBeforeStop=True,
        userInstruction='If less than an hour from completion let them finish; rerun with correctly configured backups')
    save(out / 'user-stop.json', user_stop)
    timing = dict(vacuumStart=marks['start']['time'], vacuumStartMarkerObserved=marks['start'],
        vacuumComplete=marks['complete']['time'], observationEnd=final['primary']['ts'],
        remoteVacuumExitCode=0, testComplete=False, intervention=intervention,
        interventionMode=lane, interventionAfterSeconds=600,
        minimumObservationSeconds=8400, minimumObservationSatisfied=False,
        minimumObservationEndEpoch=dt.datetime.fromisoformat(marks['start']['time'].replace('Z','+00:00')).timestamp()+8400,
        replayCaughtUp=False, replayEnd=None, replayTargetLsn=marks['complete']['flushLsn'],
        replicaReplayLsnAtObservationEnd=replay, primaryFlushLsnAtObservationEnd=flush,
        replicaLagBytesAtObservationEnd=max(0,m.lsn(flush)-m.lsn(replay)),
        finalReplayCaughtUp=False, clientStop=stop, clientSummary=summary,
        clientWarmup=json.loads((out/'client-warmup.json').read_text()),
        clientPostRecoveryHealthy=False, clientTargetCaughtWithSessions=False,
        walAtVacuumStart=marks['start'], walAtVacuumComplete=marks['complete'],
        vacuumOptions=vacuum.get('options'), vacuumSql=vacuum.get('sql'),
        errors=[REASON, 'No archive backend; experiment objective not tested',
                'Minimum observation and final replay drain intentionally not completed'],
        userStop=user_stop, postVacuumObservationError=REASON)
    save(out / 'timing.json', timing)
    save(out / 'vacuum-process.json', dict(vacuum, status='0', remoteExitCode=0))
    m.event(out / 'events.jsonl', 'user_stopped_invalid_pilot', **user_stop)
    save(stack/'.state/test-state.json', dict(phase='user_stopped_invalid_pilot', updatedAt=m.stamp(),
        testComplete=False, error=REASON, vacuumLeftRunning=False))
    report = m.plot.analyze(rows, timing, 5)
    report['status'] = dict(testComplete=False, error=REASON, vacuumExitCode=0,
        vacuumComplete=True, replayCaughtUp=False, clientComplete=True,
        minimumObservationSatisfied=False, userStopped=True)
    m.plot.render(out, rows, timing, report)
    report.pop('_series', None)
    save(out/'summary.json', report)
    return dict(lane=lane, completedTransactions=summary['completedTransactions'],
        loggedCompletedTransactions=summary['loggedCompletedTransactions'],
        committedTransactions=summary['committedTransactions'], failedTransactions=summary['failedTransactions'])

def main():
    controller = json.loads((ROOT/'controller-launch.json').read_text())['pid']
    if os.getpgid(controller) != controller:
        raise RuntimeError('Controller process group ownership not proven')
    members = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal(): continue
        pid = int(proc.name)
        try:
            if os.getpgid(pid) != controller: continue
            raw = (proc/'cmdline').read_bytes()
        except (OSError, ProcessLookupError): continue
        if raw and str(ROOT).encode() not in raw:
            raise RuntimeError('Unexpected member of experiment controller process group')
        members.append(pid)
    save(ROOT/'user-stop-controller.json', dict(requestedAtUtc=REQUESTED, reason=REASON,
        controllerPid=controller, verifiedProcessGroupMembers=members))
    os.killpg(controller, signal.SIGSTOP)
    # Keep independent collectors running while the clients drain and logs copy.
    with cf.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(finish, ['sigterm','control']))
    os.killpg(controller, signal.SIGTERM)
    os.killpg(controller, signal.SIGCONT)
    save(ROOT/'user-stop-finalization.json', dict(atUtc=dt.datetime.now(dt.timezone.utc).isoformat(),
        results=results, controllerStopped=True, reason=REASON))
    print(json.dumps(results))

if __name__ == '__main__': main()
