#!/usr/bin/env python3
"""Measure completion gaps and validate continuous all-writer waits from raw evidence."""
import datetime as dt
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('stable_writer_evidence',ROOT/'shared/stable-writer-evidence.py')
stable_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(stable_module)
def epoch(value):
    if isinstance(value, bool) or value is None:
        raise ValueError('timestamp is missing or invalid')
    if isinstance(value, (int, float)):
        return float(value)
    return dt.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def utc(value): return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat()
def jsonl(path):
    with path.open() as f:
        for line in f:
            if line.strip(): yield json.loads(line)

def client_offer_boundary(timing, sessions, observation_start, observation_end):
    """Return a conservative server-time end for the client's offered-load window."""
    client_stop = timing.get('clientStop') or {}
    activity = client_stop.get('serverActivityBeforeStopRequest') or {}
    observed = activity.get('observedAtUtc')
    if observed:
        offer_end = epoch(observed)
        source = 'clientStop.serverActivityBeforeStopRequest.observedAtUtc'
        reason = ('last server-side writer observation before the pgbench stop request; '
                  'later SIGALRM drain and telemetry tail are excluded from client-window gaps')
    else:
        requested = client_stop.get('requestedAtUtc')
        if not requested:
            raise ValueError('timing.json lacks a client stop offer boundary')
        controller_end = epoch(requested)
        offsets = []
        for row in sessions:
            if row.get('sampleStatus') != 'ok':
                continue
            try:
                controller_sample = epoch(row.get('sampleEpoch'))
                server_sample = epoch(row.get('observedAtUtc'))
            except (TypeError, ValueError, OverflowError):
                continue
            offsets.append((controller_sample, server_sample-controller_sample))
        if not offsets:
            raise ValueError('cannot map client stop request to server time without successful client samples')
        nearest = min(offsets, key=lambda item: abs(item[0]-controller_end))
        offer_end = controller_end+nearest[1]
        source = 'clientStop.requestedAtUtc mapped through nearest successful client sample'
        reason = ('controller stop-request time mapped to server clock from the nearest successful client sample; '
                  'later SIGALRM drain and telemetry tail are excluded from client-window gaps')
    if not observation_start <= offer_end <= observation_end:
        raise ValueError('client offer boundary is outside the measured VACUUM/observation window')
    return {'epoch': offer_end, 'utc': utc(offer_end), 'source': source, 'reason': reason,
            'overallObservationEndUtc': utc(observation_end),
            'postOfferTelemetryTailSeconds': observation_end-offer_end}

def longest_pending(samples, low, high):
    """Require all ten writers active and waiting with no >2.5s sample gaps."""
    best = None; start = None; previous = None; last = None
    for row in samples:
        if row.get('sampleStatus') != 'ok':
            if start is not None and (best is None or last-start > best[1]-best[0]): best=(start,last)
            start=previous=last=None; continue
        t = epoch(row['observedAtUtc'])
        if not low <= t <= high: continue
        backends = row.get('backends', [])
        all_waiting = len(backends)==10 and all(b.get('state')=='active' and b.get('waitEvent') not in (None,'ClientRead') for b in backends)
        if not all_waiting or previous is not None and t-previous > 2.5:
            if start is not None and (best is None or last-start > best[1]-best[0]): best=(start,last)
            start=None
        if all_waiting:
            if start is None: start=t
            last=t
        previous=t
    if start is not None and (best is None or last-start > best[1]-best[0]): best=(start,last)
    return {'seconds': best[1]-best[0], 'startUtc':utc(best[0]),'endUtc':utc(best[1])} if best else {'seconds':0}
def summarize(name):
    run=ROOT/name/'results/measurement'
    timing=json.loads((run/'timing.json').read_text())
    impact=json.loads((run/'client-impact.json').read_text())
    low=epoch(timing['vacuumStart']); observation_end=epoch(timing['observationEnd'])
    sessions=list(jsonl(run/'client/sessions.jsonl'))
    offer=client_offer_boundary(timing,sessions,low,observation_end)
    high=offer['epoch']
    completions=[]; observation_completions=[]; latency=[]
    for path in sorted((run/'client/raw').glob('pgbench-log.*')):
        for line in path.read_text().splitlines():
            f=line.split()
            if len(f)<7: continue
            t=float(f[4])+int(f[5])/1e6
            if low<=t<=observation_end:
                observation_completions.append(t)
                latency.append((float(f[2])-float(f[6]))/1e6)
                if t<=high: completions.append(t)
    completions.sort(); observation_completions.sort()
    boundaries=[low,*completions,high]
    gaps=[(b-a,a,b) for a,b in zip(boundaries,boundaries[1:])]
    longest=max(gaps,default=(high-low,low,high))
    pending=longest_pending(sessions,low,high)
    client_summary=json.loads((run/'client-summary.json').read_text())
    native_clean=not impact.get('validationErrors') and not client_summary.get('validationErrors') and client_summary.get('failedTransactions')==0
    reconciled=(client_summary.get('completedTransactions') is not None and client_summary.get('completedTransactions')==client_summary.get('loggedCompletedTransactions') and client_summary.get('commitLogReconciliation') is not None and not client_summary.get('validationErrors'))
    stable=stable_module.analyze_stable_writers(sessions,completions,low,high,native_log_clean=native_clean,native_counts_reconciled=reconciled)
    (run/'stable-writer-evidence.json').write_text(json.dumps(stable,indent=2)+'\n')
    overlap=max((longest_pending(sessions,a,b)['seconds'] for seconds,a,b in gaps if seconds>7200),default=0)
    coverage=json.loads((ROOT/name/'results/instrumentation/coverage.json').read_text())
    signal_file=run/'walreceiver-intervention.json'
    intervention=json.loads(signal_file.read_text()) if signal_file.exists() else timing.get('intervention')
    return {'name':name,'timing':timing,'intervention':intervention,'clientImpactValidForClaims':impact.get('validForClaims'),
      'clientOfferWindowEndUtc':offer['utc'],'clientOfferWindowEndReason':offer['reason'],
      'clientOfferWindowBoundarySource':offer['source'],'clientOfferWindowSeconds':high-low,
      'postOfferTelemetryTailSeconds':offer['postOfferTelemetryTailSeconds'],
      'completedTransactionsDuringClientOfferWindow':len(completions),
      'completedTransactionsDuringObservation':len(observation_completions),
      'nativeClientRunTotals':{'loggedCompletedTransactions':client_summary.get('loggedCompletedTransactions'),
        'summaryCompletedTransactions':client_summary.get('completedTransactions'),
        'serverCommittedUpdates':(client_summary.get('commitLogReconciliation') or {}).get('serverCommittedUpdates'),
        'failedTransactions':client_summary.get('failedTransactions'),
        'reconciliationDifference':(client_summary.get('commitLogReconciliation') or {}).get('difference')},
      'maxExecutionSeconds':max(latency,default=None),
      'longestNoCompletionInterval':{'seconds':longest[0],'startUtc':utc(longest[1]),'endUtc':utc(longest[2])},
      'longestContinuouslySampledAllWriterWait':pending,'stableWriterEvidence':stable,'twoHourContinuouslyObservedAllWriterWait':
      bool(native_clean and reconciled and impact.get('validForClaims') and overlap>7200),
      'twoHourAllWriteHangReproduced':bool(stable.get('claimValid') or (native_clean and reconciled and impact.get('validForClaims') and overlap>7200)),
      'continuouslySampledAllWriterWaitInsideNoCompletionGapSeconds':overlap,
      'collectorFatalErrors':coverage.get('fatalErrors'),'azureMetricCoverage':coverage.get('azureMetricDrain'),
      'priorInstrumentationSegments':[str(z.relative_to(ROOT/name)) for z in (ROOT/name/'results/attempts').rglob('coverage.json')],
      'durationSeconds':observation_end-low,'observationDurationSeconds':observation_end-low,
      'vacuumSeconds':epoch(timing['vacuumComplete'])-low,
      'clientPhases':impact.get('phases')}
def main():
    subprocess.run([sys.executable, str(ROOT/'shared/plot_azure_metrics.py'), '--experiment-root', str(ROOT), '--output', str(ROOT/'results/azure-graphs')], check=True)
    subprocess.run([sys.executable, str(ROOT/'shared/plot_archive_metrics.py'), '--experiment-root', str(ROOT), '--output', str(ROOT/'results/archive-graphs')], check=True)
    plot_args=[sys.executable,str(ROOT/'shared/plot_signal_comparison.py'),'--experiment-root',str(ROOT)]
    subprocess.run(plot_args,check=True)
    comparison={'runs':[summarize(n) for n in ['sigterm','control']],
      'hangDefinition':'More than 7200 seconds with zero native client completions and the same ten unfinished writer statements, supported by unchanged query/transaction start identities at successful observations. Native logs and totals must validate. Sampler gaps are recorded. The separate strict continuously-observed wait metric requires no successful-sample gap >2.5 seconds.',
      'signalCaveat':'A single SIGTERM simulates receiver disconnection; it does not execute the primary wal_sender_timeout code path.',
      'stableStatementDefinition':'Separately validates the same ten active in-flight writer statements by PID, unchanged query/transaction start timestamps, and zero native completions; sampler gaps are listed, and wait states are only claimed at successful observations.',
      'clientScope':'Ten persistent pgbench writers at 5 TPS total; claims apply to these measured writers.',
      'azureGrain':'Native PT1M. Missing metric points and BCC gaps remain explicit.'}
    comparison['restorePathEvidence']={n:json.loads((ROOT/n/'results/diagnosis/public/restore-path-report.json').read_text()) for n in ['sigterm','control']}
    (ROOT/'comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
    lines=['# 23 GiB Standard HDD: walreceiver SIGTERM versus control','',
      'Both runs restored the same retained paired DATA/WAL backup, using two Standard_D2s_v4 workers per stack, four separate 512 GiB Standard_LRS disks, host caching disabled, PostgreSQL 16.15 system image and CNPG 1.30.1. Both lanes retained working Azure Blob WAL archiving, proved by independent upload/download and replica restore byte hashes before measurement. The tiny logged client offers 5 TPS over ten persistent connections with synchronous_commit=on and no statement timeout.','',
      '| Run | VACUUM seconds | Observation seconds | Client-offer seconds | Longest no-completion interval | Longest stable ten-writer statement span | >2 h all-write hang |',
      '|---|---:|---:|---:|---:|---:|---|']
    for r in comparison['runs']:
      lines.append(f"| {r['name']} | {r['vacuumSeconds']:.2f} | {r['observationDurationSeconds']:.2f} | {r['clientOfferWindowSeconds']:.2f} | {r['longestNoCompletionInterval']['seconds']:.2f} s | {r['stableWriterEvidence']['maxStableAllWritersInFlightWithoutCompletionSeconds']:.2f} s | {r['twoHourAllWriteHangReproduced']} |")
    lines+=['','![Client, replication state and backlog comparison](results/comparison-overview.png)','', 'This timing-correct pair uses the reviewed fixes from the earlier late-signal pilot, retained separately in experiments-20261012-walreceiver-sigterm. Both stacks directly copied the same protected source pair with caller CLI identity, without source grants or lock changes. Signal timing and metric coverage are measured and retained.','', 'The treatment sends one verified OS SIGTERM at the scheduled VACUUM-start plus 600-second boundary. The control records that boundary without a signal. This is a controlled receiver disconnect. It does not itself exercise the sender-timeout code path. Raw signal receipts and PostgreSQL logs establish what followed.','',
      'The report distinguishes a completion gap, individual transaction latency, and all ten writers waiting. A completion gap alone is insufficient evidence that every write hung. Coverage errors and timing bounds are retained in comparison.json and per-run client-impact.json. stable-writer-evidence.json separately follows the same ten query/transaction identities across recorded sampler gaps; it validates no native completions and does not infer wait events inside gaps.','',
      'Each run includes separate linear-axis await, IOPS, and throughput graphs for primary/replica DATA/WAL reads and writes, client throughput/latency/pending-session graphs, PostgreSQL IO/prefetch/progress/wait counters, Linux diskstats/controller/queue metadata, and Azure per-disk and exact-VM measurements with metric definitions and explicit gaps. BCC histograms are diagnostic and their coverage is recorded.','',
      'Full end-to-end UTC events are in controller-events.jsonl, per-run preparation receipts, measurement/events.jsonl and timing.json, signal receipts, and cleanup evidence. Private kubeconfig/credentials and source access metadata stay outside this checkout. No Git push was performed.','']
    restores=comparison['restorePathEvidence']['sigterm']['archiveRestore']
    lines += ['', f"Post-SIGTERM archive recovery was observed: {restores['successfulRestoreCountAfterSignal']} successful restore log events covering {restores['uniquePostSignalWalCount']} distinct WAL files. The first and last were {restores['firstPostSignalWal']} and {restores['lastPostSignalWal']}. Per-lane restore-path-report.json relates these events to the observed receiver disconnect, sender catchup/streaming/quorum, and recorded sample gaps.", '',
      'Azure Blob archive charts retain native one-minute GET/PUT response counts, upload/download throughput and successful-request latency, with exact dimensions and missing-point evidence under results/archive-graphs/. Full archived WAL was exported and hash-verified locally before the owned Blob accounts were deleted; payloads remain private outside Git.', '']
    (ROOT/'REPORT.md').write_text('\n'.join(lines))
if __name__=='__main__':main()
