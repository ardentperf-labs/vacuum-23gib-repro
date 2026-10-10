#!/usr/bin/env python3
"""Build a measured report and full evidence-linked UTC timeline for this run."""
import datetime as dt
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT/'run/results'

def read(path):
    return json.loads(path.read_text())

def jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []

def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timeline timestamps must include a timezone')
    return parsed.astimezone(dt.timezone.utc)

def fmt(value, precision=2):
    return 'unavailable' if value is None else f'{value:,.{precision}f}'

def render_client_replication(results):
    helper_path = Path(__file__).resolve().with_name('client-replication.py')
    spec = importlib.util.spec_from_file_location('client_replication_report', helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Cannot load client-replication.py to build the replication graph')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.render(results)

def longest_full_zero_run(series, bucket_seconds=10.0, window_start=None, window_end=None):
    """Return the longest contiguous run of full-width, zero-completion buckets."""
    best = current = []
    previous_end = None
    epsilon = max(1e-6, abs(bucket_seconds) * 1e-9)
    for bucket in sorted(series, key=lambda row: float(row['startEpoch'])):
        low, high = float(bucket['startEpoch']), float(bucket['endEpoch'])
        full = abs((high-low)-bucket_seconds) <= epsilon
        inside = ((window_start is None or low >= window_start-epsilon) and
                  (window_end is None or high <= window_end+epsilon))
        zero = bucket.get('completedTransactions') == 0
        contiguous = previous_end is not None and abs(low-previous_end) <= epsilon
        if not (full and inside and zero):
            current = []
        elif not contiguous:
            current = [bucket]
        else:
            current.append(bucket)
        if len(current) > len(best):
            best = current
        previous_end = high
    if not best:
        return {'seconds': 0.0, 'fullBuckets': 0, 'startUtc': None, 'endUtc': None}
    return {'seconds': len(best)*bucket_seconds, 'fullBuckets': len(best),
            'startUtc': timestamp(dt.datetime.fromtimestamp(float(best[0]['startEpoch']), dt.timezone.utc).isoformat()).isoformat().replace('+00:00','Z'),
            'endUtc': timestamp(dt.datetime.fromtimestamp(float(best[-1]['endEpoch']), dt.timezone.utc).isoformat()).isoformat().replace('+00:00','Z')}

def main():
    summary,timing,client = (read(RESULTS/f) for f in ['summary.json','timing.json','client-impact.json'])
    native = read(RESULTS/'client-summary.json')
    sources = read(ROOT/'run/source-manifest.json')
    process = read(ROOT/'run/.state/test-process.json')
    if (not summary.get('status',{}).get('testComplete') or not timing.get('testComplete') or
        not summary.get('coverage',{}).get('valid') or process.get('status') != 'complete' or
        process.get('exitCode') != 0 or native.get('pgbenchExitCode') != 0 or
        native.get('failedTransactions') != 0 or native.get('validationErrors') or
        native.get('completedTransactions') != native.get('loggedCompletedTransactions') or
        not native.get('completedTransactions')):
        raise RuntimeError('Measurement/client validation failed or remains incomplete; no success report written')
    rows = jsonl(RESULTS/'samples.jsonl')
    if not rows:
        raise RuntimeError('No valid replica replay samples; backlog is unavailable')
    replication_path, replication_plot, _ = render_client_replication(RESULTS)
    replication = read(replication_path)
    replay = replication['series']['replicaReplayBacklogGiB']
    if replay.get('validSamples', 0) == 0 or not replication_plot.is_file():
        raise RuntimeError('No valid replica replay samples; backlog is unavailable')
    events = []
    for event in jsonl(ROOT/'timeline-events.jsonl'):
        events.append({'timeUtc':event.get('occurredAtUtc') or event['observedAtUtc'],
                       'event':event['event'],'precision':event.get('precision','local-observation'),
                       'evidence':event.get('source','timeline-events.jsonl'),'details':event})
    for name in ['provision','prepare','restore','seed','finalize','condition','test','teardown']:
        paths = [ROOT/f'run/.state/{name}-process.json']
        paths += sorted((ROOT/'run/.state').glob(f'{name}-process-attempt-*.json'))
        for path in paths:
            if not path.exists():
                continue
            receipt = read(path)
            label = name if receipt.get('attempt', 1) == 1 else f"{name}-attempt-{receipt['attempt']}"
            for field,suffix in [('startedAtUtc','started'),('finishedAtUtc','finished')]:
                if receipt.get(field):events.append({'timeUtc':receipt[field],'event':f'{label}-{suffix}',
                    'precision':'exact-process-receipt','evidence':str(path.relative_to(ROOT)),
                    'details':{'status':receipt['status'],'exitCode':receipt.get('exitCode'),'scriptSha256':receipt.get('scriptSha256')}})
    for path in sorted((ROOT/'run/.state/recovery').rglob('*events*.jsonl')):
        for event in jsonl(path):
            when = event.get('timeUtc') or event.get('time') or event.get('atUtc') or event.get('observedAtUtc')
            label = event.get('event') or event.get('phase') or path.stem
            if event.get('phase') and event.get('name'):
                label += ':' + Path(str(event['name'])).name
            if when:events.append({'timeUtc':when,'event':label,
                 'precision':event.get('precision','local-event-receipt'),'evidence':str(path.relative_to(ROOT)),'details':event})
    for event in jsonl(RESULTS/'events.jsonl'):
        events.append({'timeUtc':event['time'],'event':event['event'],'precision':'local-measurement-event',
                       'evidence':'run/results/events.jsonl','details':event})
    # Kubernetes and Azure metadata distinguish exact creation times from observed hydration completion.
    evidence_paths = list((ROOT/'run/.state/recovery').rglob('*.json'))
    evidence_paths += list((ROOT/'run/.state').glob('*.json'))
    evidence_paths += list((ROOT/'run/recovery/source-evidence').glob('*.json'))
    retained_source_ids = {sources['sourceSnapshotId'].casefold(),
                           sources['pairedWAL']['sourceSnapshotId'].casefold()}
    for path in sorted(evidence_paths):
        historical = 'source-evidence' in path.parts
        try:obj=read(path)
        except (ValueError,OSError):continue
        if isinstance(obj,dict) and str(obj.get('id','')).casefold() in retained_source_ids:
            historical = True
        if isinstance(obj,dict) and obj.get('type','').lower() in ['microsoft.compute/snapshots','microsoft.compute/disks']:
            properties = obj.get('properties',obj)
            if properties.get('timeCreated'):
                events.append({'timeUtc':properties['timeCreated'],
                    'event':f"{'Historical-' if historical else ''}Azure-{obj['type'].split('/')[-1]}-created:{obj.get('name')}",
                    'precision':'ARM-resource-creation-time','evidence':str(path.relative_to(ROOT)),
                    'details':{'id':obj.get('id'),'uniqueId':properties.get('uniqueId'),'historicalSource':historical}})
        objects = obj.get('items',[]) if isinstance(obj,dict) else []
        if isinstance(obj,dict) and obj.get('kind') in ['Backup','VolumeSnapshot','PersistentVolumeClaim']:objects=[obj]
        for item in objects:
            kind,meta=item.get('kind'),item.get('metadata',{})
            if kind in ['Backup','VolumeSnapshot','PersistentVolumeClaim'] and meta.get('creationTimestamp'):
                events.append({'timeUtc':meta['creationTimestamp'],'event':f'{"Historical-" if historical else ""}{kind}-created:{meta.get("name")}',
                               'precision':'Kubernetes-resource-time','evidence':str(path.relative_to(ROOT)),
                               'details':{'uid':meta.get('uid'),'historicalSource':historical}})
            if kind=='Backup':
                for field in ['startedAt','stoppedAt']:
                    if item.get('status',{}).get(field):events.append({'timeUtc':item['status'][field],
                        'event':f'{"Historical-" if historical else ""}Backup-{field}:{meta.get("name")}','precision':'CNPG-backup-status',
                        'evidence':str(path.relative_to(ROOT)),'details':{'phase':item['status'].get('phase')}})
    unique = {}
    for event in events:
        event['timeUtc'] = timestamp(event['timeUtc']).isoformat().replace('+00:00','Z')
        # Preserve the first captured evidence for duplicated resource timestamps.
        unique.setdefault((event['timeUtc'],event['event']),event)
    events = sorted(unique.values(),key=lambda event:timestamp(event['timeUtc']))
    for event in events:
        path = ROOT/event['evidence']
        if path.is_file():event['evidenceSha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    (RESULTS/'timeline.json').write_text(json.dumps({'generatedAtUtc':dt.datetime.now(dt.timezone.utc).isoformat(),'events':events},indent=2)+'\n')
    lag = replay.get('maximumBacklog')
    duration=(timestamp(timing['vacuumComplete'])-timestamp(timing['vacuumStart'])).total_seconds()
    measurement_events = jsonl(RESULTS/'events.jsonl')
    target_event = next(event for event in measurement_events
                        if event.get('event') == 'replica_reached_vacuum_target_with_client')
    vacuum_target_seconds = (timestamp(target_event['time'])-timestamp(timing['vacuumStart'])).total_seconds()
    final_observation_seconds = (timestamp(timing['observationEnd'])-timestamp(timing['vacuumStart'])).total_seconds()
    window=summary['windows']['vacuumAndReplay']
    primary,replica=window['primary_data'],window['replica_data']
    ops_ratio=replica['readIops']*replica['coveredSeconds']/(primary['readIops']*primary['coveredSeconds'])
    bytes_ratio=replica['readMBps']*replica['coveredSeconds']/(primary['readMBps']*primary['coveredSeconds'])
    phases = client['phases']
    warmup = client['healthyWarmup']
    pre = phases.get('pre_vacuum', {})
    during = phases.get('vacuum', {})
    vac_end_to_end = during.get('latencyMs', {})
    vac_execution = during.get('executionMs', {})
    bucket_seconds = float(client.get('completionBucketSeconds', 10.0))
    zero_any = longest_full_zero_run(client.get('series', []), bucket_seconds)
    vacuum_start = float(during.get('startEpoch', 0))
    vacuum_end = float(during.get('endEpoch', 0))
    zero_vacuum = longest_full_zero_run(client.get('series', []), bucket_seconds,
                                        vacuum_start, vacuum_end)
    out=['# Snapshot baseline with a tiny 5 TPS target-rate client','',
         f"During the {fmt(warmup.get('serverCounterDurationSeconds'),0)}-second healthy warm-up, the primary committed **{warmup.get('serverCommittedTransactions',0):,} transactions ({fmt(warmup.get('serverCommittedTps'),3)} TPS from the exact server counter in [client-warmup.json](client-warmup.json))**. Across the full pre-VACUUM interval (warm-up plus checkpoint and replica replay wait), native client logs show **{fmt(pre.get('completedTps'),3)} completed TPS**; during VACUUM they show **{fmt(during.get('completedTps'),3)} completed TPS**. VACUUM-period p95 end-to-end latency was **{fmt(vac_end_to_end.get('p95'),3)} ms** (execution p95 **{fmt(vac_execution.get('p95'),3)} ms**). The longest consecutive run of full 10-second zero-completion buckets was **{fmt(zero_vacuum['seconds'],0)} seconds during VACUUM** and **{fmt(zero_any['seconds'],0)} seconds across the measured client-active window**.",'',
         f"Measured run: VACUUM finished in **{fmt(duration)} seconds**. The replica reached its completion LSN in **{fmt(vacuum_target_seconds)} seconds from VACUUM start**, at **{target_event['time']}**, with the client still active. The full observation, including five healthy recovery minutes, graceful client drain and final replay verification, ended **{fmt(final_observation_seconds)} seconds from VACUUM start**; final lag was **{fmt(summary['status']['replicaLagBytesAtObservationEnd'],0)} bytes**.",'',
         'The client requested 5 TPS in total across ten persistent connections, updating one logged row per connection. pgbench ran inside the primary pod over localhost TCP with the app role. `synchronous_commit=on` was preserved: commits wait for standby WAL flush, while replica replay can fall behind. The run measures client transaction throughput and latency.','',
         '## Client impact','',
         '| Phase | Seconds | Completed TPS | p50 latency ms | p95 latency ms | p99 latency ms | Max latency ms | Max pending transaction age s |',
         '|---|---:|---:|---:|---:|---:|---:|---:|']
    if warmup.get('latencyMs'):
        d = warmup['latencyMs']
        out.append(f"| healthy warm-up (subset of pre_vacuum) | {fmt(warmup['seconds'])} | {fmt(warmup['completedTps'],3)} | {fmt(d['p50'],3)} | {fmt(d['p95'],3)} | {fmt(d['p99'],3)} | {fmt(d['max'],3)} | {fmt(warmup['maxPendingXactAgeSeconds'],3)} |")
    for name,phase in client['phases'].items():
        d=phase['latencyMs']
        label = 'pre_vacuum (full warm-up + checkpoint/replay interval)' if name == 'pre_vacuum' else name
        out.append(f"| {label} | {fmt(phase['seconds'])} | {fmt(phase['completedTps'],3)} | {fmt(d['p50'],3)} | {fmt(d['p95'],3)} | {fmt(d['p99'],3)} | {fmt(d['max'],3)} | {fmt(phase['maxPendingXactAgeSeconds'],3)} |")
    if all('completedTransactions' in phase for phase in phases.values()):
        phase_completions = sum(phase['completedTransactions'] for phase in phases.values())
        startup_completions = client['totalCompletedTransactions'] - phase_completions
        out += ['', f"The {len(phases)} disjoint phase rows contain {phase_completions:,} native completions. Another {startup_completions:,} startup completions preceded the healthy warm-up boundary; together they account for all {client['totalCompletedTransactions']:,} completed transactions."]
    out += ['',f"Native completion logs contain {client['totalCompletedTransactions']:,} transactions. Client failures: {native.get('failedTransactions')}; pgbench exit code: {native.get('pgbenchExitCode')}. Maximum observed replay backlog across the sampled post-warm-up client window: {fmt(lag,3)} GiB.",'',
            f"The opening healthy warm-up measurement uses the exact primary committed-transaction counter delta ({warmup.get('serverCommittedTransactions',0):,} over {fmt(warmup.get('serverCounterDurationSeconds'),3)} seconds). Table TPS uses native-log completions; it can differ slightly because the client log assigns transactions by completion timestamps. The pre-VACUUM phase row aggregates the entire interval from warm-up start through the checkpoint and replica replay wait up to VACUUM start, and includes the healthy warm-up subset.",'',
            'pgbench schedules Poisson arrivals around the target rate. Completed TPS and phase attribution use native completion timestamps, including buckets with zero completions while the client runs. A transaction can begin in an earlier phase. End-to-end latency includes schedule lag and commit wait; execution latency subtracts schedule lag but still includes commit wait. Commit-wait duration is not isolated. Latency distributions include only completed transactions, so the pending-age graph separately shows unfinished work. The zero-completion duration above counts consecutive full 10-second buckets only; partial edge buckets are excluded.','',
            f"Pending-session samples: {client['sessionSamples']:,}; errors: {client['sessionSampleErrors']:,}; maximum successful-sample gap: {fmt(client['sessionSampleMaxGapSeconds'],3)} seconds. Pending age is sampled and can miss short waits or work during missing intervals.",'',
            '![Client TPS](client-tps.png)','![Client latency](client-latency.png)','![Pending work](client-pending.png)','',
            '## Client throughput and replication','',
            f"The combined [client and replication graph](client-replication.png) uses the five-minute healthy warm-up as time zero. Client throughput and latency span that warm-up; replication traces begin at the first pre-VACUUM sample, leaving the earlier warm-up blank in those panels. Replay backlog had {replay.get('validSamples',0):,} valid samples over {fmt(replay.get('coveredSeconds'),1)} covered seconds ({fmt(100*(replay.get('coverageFraction') or 0),1)}% of the plotted window); first/last valid samples were {replay.get('firstValidSampleUtc') or 'unavailable'} / {replay.get('lastValidSampleUtc') or 'unavailable'}, with {fmt(replay.get('unobservedLeadingSeconds'),1)} seconds before the first valid sample and a maximum valid-sample gap of {fmt(replay.get('maximumValidSampleGapSeconds'),1)} seconds. The maximum observed primary-flush-to-replica-replay backlog was {fmt(lag,3)} GiB. See [client-replication.json](client-replication.json) for invalid fields, gaps, and coverage details.",'',
            'Primary flush LSN marks WAL flushed on the primary; the sole replication sender flush LSN reports WAL flushed on the standby, while replica replay LSN marks WAL applied by recovery. The flush backlog and replay backlog are different quantities: synchronous_commit=on waits for synchronous standby flush, not remote apply/replay. Replay can therefore lag while commits continue to wait only for flush.','',
            '## Storage','',
            f"Across the complete VACUUM and replay observation window, replica/primary DATA read operations were **{ops_ratio:.3f}×**, and DATA bytes read were **{bytes_ratio:.3f}×**. These guest block counters describe operations and bytes; they do not establish repeated reads of the same physical block. The tiny client contributes additional DATA/WAL traffic.",'',
            '| Device | Read IOPS | Write IOPS | Read MB/s | Write MB/s | Read await ms | Write await ms |',
            '|---|---:|---:|---:|---:|---:|---:|']
    for name,metrics in window.items():
        out.append('| '+name+' | '+' | '.join(fmt(metrics[k],3) for k in ['readIops','writeIops','readMBps','writeMBps','readAwaitMs','writeAwaitMs'])+' |')
    out += ['', f"Mean DATA read requests were {primary['readMBps']*1e6/primary['readIops']/1024:.2f} KiB on the primary and {replica['readMBps']*1e6/replica['readIops']/1024:.2f} KiB on the replica. Similar bytes split into much smaller requests explain the operation ratio; this does not identify how often any individual page was read.", '']
    out += ['', 'MB/s is decimal. Await is the operation-weighted guest block completion time. Linux read-ahead and merging, hypervisor, controller, firmware and hardware coalescing can change the operations visible at each layer; results can vary substantially by environment. All plots use linear y-axes.','',
            '![DATA and WAL await](await.png)','![DATA and WAL IOPS](iops.png)','![DATA and WAL throughput](throughput.png)','',
            f"The storage graphs mark the first verified arrival at the fixed VACUUM completion LSN ({target_event['time']}). Curves and storage totals retain the complete observation window, including the later healthy recovery interval and final post-client replay verification ({timing.get('replayEnd',timing['observationEnd'])}). The [plot-boundary receipt](storage-plot-boundaries.json) preserves both endpoints and original collector plots; raw timing and counters are unchanged.",'',
            '## Setup and source','',
            '23 GiB heap; separate 512 GiB DATA and WAL disks on both instances; Azure PremiumV2_LRS at 3000 IOPS and 125 MB/s per disk, caching None, logical sector size 512. East US zone 1; two Standard_D2s_v4 workers; AKS 1.35.8; CNPG 1.30.1; PostgreSQL 16.15. All four measurement disks completed hydration before client warm-up.','',
            'The existing 23 GiB paired online CNPG snapshot backup was restored to its verified backup-end LSN using preserved archive WAL. The large target fixture DML was not rerun. A fresh paired backup seeded the replica. The source has the same system identifier and physical fixture as the prior baseline. Live/dead tuple counts are inherited estimates; no full-table count warmed the test heap.','',
            f"DATA source: `{sources['sourceSnapshotId']}` (unique ID `{sources['uniqueId']}`).",'',
            f"WAL source: `{sources['pairedWAL']['sourceSnapshotId']}` (unique ID `{sources['pairedWAL']['uniqueId']}`).",'',
            'Baseline PostgreSQL settings and synchronous quorum were verified in [preflight.json](preflight.json). Raw samples, native client logs, measurement events and phase receipts are retained. This is one measured repeat with a client workload; it does not isolate prefetch against an off control.','',
            '## Complete end-to-end UTC timeline','',
            'Exact resource/process timestamps are distinguished from observation times. Original source snapshots are historical inputs; restore and hydration below belong to this run. The table shows phase changes and copy progress; [timeline.json](timeline.json) also retains routine metadata polls, full details and evidence hashes.','',
            '| UTC timestamp | Event | Time precision | Evidence |','|---|---|---|---|']
    conditioning_path = ROOT/'run/.state/recovery/cache-conditioning.json'
    conditioning = read(conditioning_path)
    if conditioning.get('status') != 'complete' or not conditioning.get('cacheDropSucceeded'):
        raise RuntimeError('Cold Linux page-cache conditioning did not complete')
    setup_note = ('Preparation initially selected a base-backup join; it was stopped and its replica volumes were removed. '
                  'The replica used for measurement was restored from the verified offline snapshot pair. '
                  'After all four copies reached 100%, both PostgreSQL pods restarted onto the pinned measurement image. '
                  'Before client setup, privileged one-shot pods ran sync and wrote 3 to Linux drop_caches on both isolated workers '
                  'to remove possible OS cache warming from the aborted join. This clears Linux page cache, dentries and inodes; '
                  'it does not clear PostgreSQL shared_buffers. The commands succeeded and the helper pods were removed. '
                  'Two pre-VACUUM client launcher failures are retained with exact executed script copies and process receipts: the first occurred before client launch; the second client was gracefully drained with 531 completed/logged/server-committed transactions and zero failures. Only its ten-row client table was reset before the successful measurement. '
                  'The complete preparation detour and conditioning are recorded below.')
    storage_review = ROOT/'verification/azure-d2sv4-storage-review.md'
    if storage_review.exists():
        setup_note += (' Azure Monitor reported 100% VM uncached bandwidth utilization on both workers and 100% VM uncached IOPS utilization on the replica during 08:43–08:52 UTC. '
                       'The live Standard_D2s_v4 SKU exposes aggregate limits of 3200 uncached IOPS and 48000000 bytes/s, in addition to each disk configuration. '
                       'Guest DATA/WAL counters exceed that bandwidth figure; this layer discrepancy remains unresolved, and this run does not isolate the cause of client latency. '
                       'The [storage review](../../verification/azure-d2sv4-storage-review.md) links Microsoft documentation, retained Azure responses and reproducible guest calculations. '
                       'These are guest-observed disk operations; they should not be interpreted as a count of separately billed Azure transactions.')
    position = out.index('## Complete end-to-end UTC timeline')
    out[position:position] = [setup_note, '']
    for event in events:
        if event.get('details',{}).get('phase') in {'live-check','disk-inspected',
                'node-resource-group-revalidated','source-locks-revalidated'}:
            continue
        evidence=event['evidence'];out.append(f"| {event['timeUtc']} | {event['event']} | {event['precision']} | [{Path(evidence).name}](../../{evidence}) |")
    audit_path = ROOT/'preflight/snapshot-audit-final.json'
    if audit_path.exists():
        audit = read(audit_path)
        if not audit.get('preservedPriorSnapshots') or not audit.get('allSnapshotsProtected'):
            raise RuntimeError('Final snapshot retention verification failed')
        out += ['', '## Cleanup and retention', '', f"The [final retention audit](../../preflight/snapshot-audit-final.json) verifies all {audit['priorSnapshotCount']} prior snapshot identities and protection of all {audit['currentSnapshotCount']} current snapshots.", '']
        cleanup_path = ROOT/'run/.state/cleanup-run.json'
        cleanup = read(cleanup_path) if cleanup_path.exists() else {}
        attempts = cleanup.get('attempts', [])
        if cleanup.get('status') == 'complete' and attempts and attempts[-1].get('status') == 'complete':
            attempt = attempts[-1]
            teardown = read(ROOT/'run/.state/teardown.json')
            if teardown.get('status') != 'complete':
                raise RuntimeError('Completed cleanup has no completed teardown receipt')
            residual = attempt['sourceReaderCleanup']['residualScopeLocked']
            snapshot_role = teardown.get('snapshotRoleAssignment') or {}
            out += [f"The [guarded cleanup receipt](../../run/.state/cleanup-run.json) completed at {attempt['finishedAtUtc']}. Owned AKS, worker/node resources, all four measurement disks and the temporary Blob account were removed with the exact owned test/node resource groups. Snapshot resource groups and all protection locks were preserved.", '',
                    f"Regional Network Watcher disposition: `{attempt['ownedWatcherCleanup']['status']}`. The root review found two other East US AKS stacks outside this run's ownership and a changed live watcher ETag; the watcher and its resource group remain preserved. No unrelated resources were adopted or deleted.", '',
                    f"Source Reader assignments retained because of snapshot protection locks: {len(residual)}. New snapshot-group role assignment status: `{snapshot_role.get('status', 'unavailable')}`. The retained Reader grants refer to the AKS principal removed with this stack; no locks were removed to delete grants. Their IDs and scopes are recorded in the cleanup/teardown receipts.", '']
        else:
            out += ['Snapshot retention was audited; cleanup is not recorded complete. Consult the linked process receipts for its disposition.', '']
    else:
        out += ['','Measurement is complete; final cleanup and snapshot retention verification remain pending.','']
    (RESULTS/'REPORT.md').write_text('\n'.join(out))
    print(RESULTS/'REPORT.md')

if __name__=='__main__':main()
