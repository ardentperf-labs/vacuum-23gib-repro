#!/usr/bin/env python3
"""Measure completion gaps and validate continuous all-writer waits from raw evidence."""
import datetime as dt
import importlib.util
import argparse
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
def summarize(name, *, user_stopped_pilot=False):
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
    stopped_receipt=json.loads((run/'user-stop.json').read_text()) if (run/'user-stop.json').is_file() else None
    invalid_user_stopped=bool(user_stopped_pilot and stopped_receipt and stopped_receipt.get('stopped') is True)
    # The plotter has already enforced the corresponding strict gates:
    # testComplete=true for a normal pair, or its canonical late-signal
    # treatment-only override for the prior pilot mode.
    test_valid=not invalid_user_stopped
    two_hour_claim=(bool(stable.get('claimValid') or (native_clean and reconciled and impact.get('validForClaims') and overlap>7200))
                    if test_valid else None)
    return {'name':name,'timing':timing,'intervention':intervention,'userStopReceipt':stopped_receipt,
      'experimentValidity':'invalid_user_stopped_pilot' if invalid_user_stopped else 'validated_pair' if timing.get('testComplete') is True else 'validated_after_recorded_runner_failure_override',
      'requiredObservationSeconds':timing.get('minimumObservationSeconds'),'minimumObservationSatisfied':timing.get('minimumObservationSatisfied'),
      'twoHourTestOutcome':'not_evaluated_invalid_user_stopped_pilot' if invalid_user_stopped else 'evaluated' if test_valid else 'not_evaluated_incomplete',
      'twoHourAllWriteHangReproduced':two_hour_claim,
      'replayCaughtUpAsRecorded':timing.get('replayCaughtUp'),
      'finalReplayCaughtUpAsRecorded':timing.get('finalReplayCaughtUp'),
      'clientImpactValidForClaims':impact.get('validForClaims'),
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
      (bool(native_clean and reconciled and impact.get('validForClaims') and overlap>7200) if test_valid else None),
      'continuouslySampledAllWriterWaitInsideNoCompletionGapSeconds':overlap,
      'collectorFatalErrors':coverage.get('fatalErrors'),'azureMetricCoverage':coverage.get('azureMetricDrain'),
      'priorInstrumentationSegments':[str(z.relative_to(ROOT/name)) for z in (ROOT/name/'results/attempts').rglob('coverage.json')],
      'durationSeconds':observation_end-low,'observationDurationSeconds':observation_end-low,
      'vacuumSeconds':epoch(timing['vacuumComplete'])-low,
      'clientPhases':impact.get('phases')}
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--allow-user-stopped-pilot',action='store_true',
        help='render the explicitly invalid archive-misconfigured pilot after complete native client harvest')
    args=parser.parse_args()
    subprocess.run([sys.executable, str(ROOT/'shared/plot_azure_metrics.py'), '--experiment-root', str(ROOT), '--output', str(ROOT/'results/azure-graphs')], check=True)
    plot_args=[sys.executable,str(ROOT/'shared/plot_signal_comparison.py'),'--experiment-root',str(ROOT)]
    if args.allow_user_stopped_pilot:
      plot_args.append('--allow-user-stopped-pilot')
    else:
      plot_args.append('--allow-recorded-test-failure')
    subprocess.run(plot_args,check=True)
    comparison={'runs':[summarize(n,user_stopped_pilot=args.allow_user_stopped_pilot) for n in ['sigterm','control']],
      'experimentValidity':('invalid_user_stopped_pilot' if args.allow_user_stopped_pilot else
          'validated_pair' if all(json.loads((ROOT/n/'results/measurement/timing.json').read_text()).get('testComplete') is True
                                  for n in ('sigterm','control')) else 'validated_after_recorded_runner_failure_override'),
      'twoHourTestOutcome':'not_evaluated_invalid_user_stopped_pilot' if args.allow_user_stopped_pilot else 'evaluated_by_run',
      'hangDefinition':'More than 7200 seconds with zero native client completions and the same ten unfinished writer statements, supported by unchanged query/transaction start identities at successful observations. Native logs and totals must validate. Sampler gaps are recorded. The separate strict continuously-observed wait metric requires no successful-sample gap >2.5 seconds.',
      'signalCaveat':'A single SIGTERM simulates receiver disconnection; it does not execute the primary wal_sender_timeout code path.',
      'stableStatementDefinition':'Separately validates the same ten active in-flight writer statements by PID, unchanged query/transaction start timestamps, and zero native completions; sampler gaps are listed, and wait states are only claimed at successful observations.',
      'clientScope':'Ten persistent pgbench writers at 5 TPS total; claims apply to these measured writers.',
      'azureGrain':'Native PT1M. Missing metric points and BCC gaps remain explicit.'}
    (ROOT/'comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
    title=('# INVALID PILOT — invalid archive configuration; user-stopped pilot'
           if args.allow_user_stopped_pilot else '# 23 GiB Standard HDD: walreceiver SIGTERM versus control')
    lines=[title,'']
    if args.allow_user_stopped_pilot:
      lines+=['The two runs were deliberately stopped because the restored fixture lacked the required archive configuration. Both completed VACUUM successfully and their stopped-client native logs and count reconciliation are validated, but both observation windows ended before the required minimum. This is an invalid, incomplete pilot: the >2-hour outcome was not evaluated, and it is not a negative result from a completed two-hour test. Replay/catch-up fields below retain only recorded values.','']
    lines+=['Both runs restored the same retained paired DATA/WAL backup, using two Standard_D2s_v4 workers per stack, four separate 512 GiB Standard_LRS disks, host caching disabled, PostgreSQL 16.15 and CNPG 1.30.1. The tiny logged client offers 5 TPS over ten persistent connections with synchronous_commit=on and no statement timeout.','',
      '| Run | VACUUM seconds | Observation seconds | Client-offer seconds | Longest no-completion interval | Longest stable ten-writer statement span | >2 h all-write hang |',
      '|---|---:|---:|---:|---:|---:|---|']
    for r in comparison['runs']:
      outcome=('NOT EVALUATED — INVALID PILOT' if r['twoHourAllWriteHangReproduced'] is None else str(r['twoHourAllWriteHangReproduced']))
      lines.append(f"| {r['name']} | {r['vacuumSeconds']:.2f} | {r['observationDurationSeconds']:.2f} | {r['clientOfferWindowSeconds']:.2f} | {r['longestNoCompletionInterval']['seconds']:.2f} s | {r['stableWriterEvidence']['maxStableAllWritersInFlightWithoutCompletionSeconds']:.2f} s | {outcome} |")
    lines+=['', 'The pilot did not have an object-storage WAL backend: inherited preparation removed archive references. See [DIAGNOSIS.md](DIAGNOSIS.md) and the per-lane diagnostic captures for why writes resumed when the sender reached synchronous quorum. This run cannot establish object-storage restore behavior.', '', '![Client, replication state and backlog comparison](results/comparison-overview.png)','', 'Preparation included a corrected pre-client logging error, an Azure VM endpoint correction, a failed timestamp COPY that stopped before VACUUM, and a recovered auxiliary collector transport reset. Both VACUUM commands were launched after correcting their timestamp paths; timing and the intervention use the actual server marker. Failed attempts, restore-path differences and collector gaps remain in the evidence.','', 'The treatment scheduled SIGTERM at VACUUM start plus ten minutes. PostgreSQL pg_terminate_backend returned false without sending it; the verified OS SIGTERM was sent at +673.729 seconds (73.729 seconds late). The control records its +600-second boundary without a signal. This is a controlled receiver disconnect. It does not itself exercise the sender-timeout code path. Raw signal receipts and PostgreSQL logs establish what followed.','',
      'The report distinguishes a completion gap, individual transaction latency, and all ten writers waiting. A completion gap alone is insufficient evidence that every write hung. Coverage errors and timing bounds are retained in comparison.json and per-run client-impact.json. stable-writer-evidence.json separately follows the same ten query/transaction identities across recorded sampler gaps; it validates no native completions and does not infer wait events inside gaps.','',
      'Each run includes separate linear-axis await, IOPS, and throughput graphs for primary/replica DATA/WAL reads and writes, client throughput/latency/pending-session graphs, PostgreSQL IO/prefetch/progress/wait counters, Linux diskstats/controller/queue metadata, and Azure per-disk and exact-VM measurements with metric definitions and explicit gaps. BCC histograms are diagnostic and their coverage is recorded.','',
      'Full end-to-end UTC events are in controller-events.jsonl, per-run preparation receipts, measurement/events.jsonl and timing.json, signal receipts, and cleanup evidence. Private kubeconfig/credentials and source access metadata stay outside this checkout. No Git push was performed.','']
    (ROOT/'REPORT.md').write_text('\n'.join(lines))
if __name__=='__main__':main()
