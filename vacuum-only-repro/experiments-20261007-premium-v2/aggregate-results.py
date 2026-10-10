#!/usr/bin/env python3
"""Write a comparison from completed measurements without assuming an outcome."""
import datetime as dt
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNS = ('baseline-23gib', '50gib', '100gib')


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def elapsed(start, end):
    if not start or not end:
        return None
    return (dt.datetime.fromisoformat(end.replace('Z', '+00:00')) -
            dt.datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()


def format_number(value, precision=2):
    return '—' if value is None else f'{value:,.{precision}f}'


def duration(seconds):
    if seconds is None:
        return '—'
    minutes, seconds = divmod(round(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f'{hours}h {minutes:02d}m {seconds:02d}s'


def observation_totals(samples):
    if len(samples) < 2:
        return {}
    first, last = samples[0], samples[-1]
    totals = {}
    for role in ('primary', 'replica'):
        for device in ('data', 'wal'):
            before, after = first[role], last[role]
            before_identity = before['devices'][device]
            after_identity = after['devices'][device]
            if any(before_identity[k] != after_identity[k] for k in ('claim', 'pv', 'majorMinor', 'source')):
                raise RuntimeError('device identity changed across observation')
            a = before['disks'][before_identity['majorMinor']]
            b = after['disks'][after_identity['majorMinor']]
            delta = {k: b[k] - a[k] for k in ('r', 'rs', 'rm', 'w', 'ws', 'wm')}
            valid = min(delta.values()) >= 0
            totals[f'{role}_{device}'] = {
                'valid': valid, 'readOps': delta['r'], 'writeOps': delta['w'],
                'readBytes': delta['rs'] * 512, 'writeBytes': delta['ws'] * 512,
                'readAwaitMs': delta['rm'] / delta['r'] if valid and delta['r'] else None,
                'writeAwaitMs': delta['wm'] / delta['w'] if valid and delta['w'] else None,
                'meanReadKiB': delta['rs'] / delta['r'] / 2 if valid and delta['r'] else None,
                'observationSeconds': after['epoch'] - before['epoch']}
    return totals


def main():
    runs = []
    lines = ['# Premium SSD v2 VACUUM results', '',
             'Three separate stacks use the baseline fixture construction and PostgreSQL settings, '
             'with 23, 50, and 100 GiB heap targets. Each DATA/WAL disk is a 512-GiB '
             'Premium SSD v2 disk provisioned at the minimum 3,000 IOPS and 125 MB/s, '
             'with host caching disabled and 512-byte logical sectors.', '',
             'The 23- and 100-GiB stacks are in East US zone 1. The 50-GiB stack is in '
             'West US 3 zone 1 because Azure denied the East US DSv4 quota increase. '
             'Region therefore limits comparisons involving the 50-GiB result. All workers '
             'are Standard_D2s_v4. Azure SKU metadata advertises 3,200 uncached IOPS and '
             '48 MB/s for this size, below the combined configured disk limits. The '
             '[Dsv4 table](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series) '
             'labels these columns Premium SSD and has no separate Premium SSD v2 row. '
             'Treat the advertised host values as context; this experiment measures the '
             'effective path rather than independently proving a Premium-v2-specific VM ceiling. '
             'The older Premium SSD burst figures are not assumed to apply here. '
             'Supplementary [full-run VM telemetry and guest I/O](recovery/vm-burst-review/MEASUREMENT-SUPPLEMENT.md) '
             'is aggregated across each two-node scale set and cannot resolve node-specific peaks.', '',
             'Measurements require all three preparations to finish, every restored measured '
             'Premium SSD v2 disk to report 100% copy completion, and the two temporary '
             'Standard staging disks to be retired with all snapshot identities and locks '
             'preserved. Azure does not expose reliable copy progress for Standard disks '
             'or Standard-origin snapshots; availability alone cannot prove those transfers '
             'finished. The gate therefore establishes measured-disk hydration and staging-disk '
             'absence, rather than global Azure backend quiescence. See '
             '[Azure disk restore monitoring](https://learn.microsoft.com/en-us/azure/virtual-machines/scripts/create-managed-disk-from-snapshot) '
             'and [snapshot access semantics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-instant-access-snapshots).', '',
             'These are one measured VACUUM run per fixture size. OS page caches are not explicitly '
             'dropped; preparation and the pre-observation checkpoint can leave cached pages. '
             'The checkpoint and initial replay catch-up finish before the first counter sample. '
             'Cache history and the regional difference therefore limit size-only causal conclusions.', '',
             '| Run | Region | Actual heap GiB | VACUUM | Start through catch-up | Status |',
             '|---|---|---:|---:|---:|---|']
    for name in RUNS:
        stack = ROOT / name
        config = read(stack / 'run-config.json')
        prepare = read(stack / '.state/prepare.json')
        summary = read(stack / 'results/summary.json')
        timing = read(stack / 'results/timing.json')
        test = read(stack / '.state/test-state.json')
        prepare_process = read(stack / '.state/prepare-process.json')
        test_process = read(stack / '.state/test-process.json')
        phase = test.get('phase') or ('prepare failed' if prepare_process.get('status') == 'failed'
                else test_process.get('status') or
                ('prepared; waiting for measurement' if prepare_process.get('status') == 'complete'
                 else 'preparing'))
        complete = (summary.get('status', {}).get('testComplete') is True and
                    timing.get('testComplete') is True and
                    summary.get('coverage', {}).get('valid') is True)
        run = {'name': name, 'configuration': config, 'prepare': prepare,
               'complete': complete, 'phase': phase,
               'vacuumSeconds': elapsed(timing.get('vacuumStart'), timing.get('vacuumComplete')),
               'throughCatchupSeconds': elapsed(timing.get('vacuumStart'), timing.get('replayEnd')),
               'coverage': summary.get('coverage'),
               'phaseWindows': summary.get('windows', {})}
        if summary:
            samples = [json.loads(line) for line in (stack / 'results/samples.jsonl').read_text().splitlines() if line]
            run['observationTotals'] = observation_totals(samples)
            run['observedMaxIndexVacuumCount'] = max(
                (row['primary'].get('vacuumProgress') or {}).get('indexVacuumCount', 0) or 0
                for row in samples)
            a = run['observationTotals'].get('primary_data', {})
            b = run['observationTotals'].get('replica_data', {})
            run['dataReadOperationRatio'] = b['readOps'] / a['readOps'] if a.get('valid') and b.get('valid') and a.get('readOps') else None
            run['dataReadByteRatio'] = b['readBytes'] / a['readBytes'] if a.get('valid') and b.get('valid') and a.get('readBytes') else None
            heap_bytes = prepare.get('heapBytes')
            run['dataReadsInHeapEquivalents'] = {
                role: totals['readBytes'] / heap_bytes if totals.get('valid') and heap_bytes else None
                for role, totals in (('primary', a), ('replica', b))}
            run['prefetchBoundaryStats'] = {role: {'before': samples[0][role].get('recoveryPrefetchStats'),
                                                  'after': samples[-1][role].get('recoveryPrefetchStats')}
                                            for role in ('primary', 'replica')}
        runs.append(run)
        heap = prepare.get('heapBytes')
        lines.append(f'| [{name}]({name}/results/) | {config.get("location", "—")} | '
                     f'{format_number(heap / 1024**3 if heap else None)} | '
                     f'{duration(run["vacuumSeconds"])} | {duration(run["throughCatchupSeconds"])} | '
                     f'{"complete with valid coverage" if complete else run["phase"]} |')
    lines += ['', 'Full observation totals include the baseline sample through the final catch-up sample. '
              'They come directly from guest block-device counters. These are guest operation counts; '
              'backend merging and the cloud provider’s billing or throttling operation counts may differ. '
              'Read await is operation-weighted guest block-device time, including queueing; '
              'it does not measure Azure service time or application latency.', '',
              '| Run | Primary DATA reads | Replica DATA reads | Operation ratio | Byte ratio | Primary mean read KiB | Replica mean read KiB | Replica read await ms |',
              '|---|---:|---:|---:|---:|---:|---:|---:|']
    for run in runs:
        a = run.get('observationTotals', {}).get('primary_data', {})
        b = run.get('observationTotals', {}).get('replica_data', {})
        if not a.get('valid'):
            a = {}
        if not b.get('valid'):
            b = {}
        values = [format_number(a.get('readOps'), 0), format_number(b.get('readOps'), 0),
                  format_number(run.get('dataReadOperationRatio')), format_number(run.get('dataReadByteRatio')),
                  format_number(a.get('meanReadKiB')), format_number(b.get('meanReadKiB')),
                  format_number(b.get('readAwaitMs'))]
        lines.append('| ' + run['name'] + ' | ' + ' | '.join(values) + ' |')
    lines += ['', '| Run | Primary DATA read GiB | Replica DATA read GiB | Primary heap equivalents | Replica heap equivalents |',
              '|---|---:|---:|---:|---:|']
    for run in runs:
        totals = run.get('observationTotals', {})
        equivalents = run.get('dataReadsInHeapEquivalents', {})
        values = []
        for role in ('primary', 'replica'):
            disk = totals.get(role + '_data', {})
            values.append(format_number(disk['readBytes'] / 1024**3 if disk.get('valid') else None))
        values += [format_number(equivalents.get(role)) for role in ('primary', 'replica')]
        lines.append('| ' + run['name'] + ' | ' + ' | '.join(values) + ' |')
    lines += ['', 'Heap equivalents divide whole DATA-device read bytes by the prepared heap size. '
              'They include index and other PostgreSQL reads and do not count repeated reads of individual pages. '
              'An operation ratio alone cannot establish cache thrashing: smaller requests can produce '
              'many more operations while reading similar total bytes. All three runs retain '
              'recovery_prefetch=try; they do not include a matched prefetch-off control and '
              'therefore do not isolate prefetch as a causal factor.', '',
              'For context, the earlier HDD baseline in the '
              '[prefetch read investigation](../../PREFETCH-READS-INVESTIGATION.md) '
              'had approximately 15.8 times as many replica DATA read operations with nearly equal '
              'read bytes: 48.757 versus 49.231 decimal GB. Its approximately 8-KiB replica requests '
              'versus 128-KiB primary requests explain that operation ratio without requiring '
              'fifteen reads of each page. The replica read volume was about two heap equivalents; '
              'aggregate counters cannot establish individual-page read histories. These new runs '
              'report direct whole-observation deltas, so exact window boundaries and units must '
              'be considered when comparing them with historical boundary-prorated estimates.']
    lines += ['', 'Replica replay can continue long after VACUUM exits. The whole-observation '
              'operation ratio above therefore includes that replay drain. The table below '
              'separates the two periods using the stored phase analysis; counter intervals '
              'crossing a boundary are prorated by their overlap duration. Rates and await '
              'remain guest block-device measurements.', '',
              '| Run | Phase | Primary DATA read IOPS | Replica DATA read IOPS | Primary / replica read MB/s | Operation ratio | Byte-rate ratio | Replica read await ms |',
              '|---|---|---:|---:|---:|---:|---:|---:|']
    for run in runs:
        for key, label in (('vacuum', 'VACUUM'), ('replayDrain', 'Replay after VACUUM')):
            window = run.get('phaseWindows', {}).get(key, {})
            a, b = window.get('primary_data', {}), window.get('replica_data', {})
            if not window:
                continue
            op_ratio = b.get('readIops', 0) / a['readIops'] if a.get('readIops') else None
            byte_ratio = b.get('readMBps', 0) / a['readMBps'] if a.get('readMBps') else None
            lines.append('| ' + run['name'] + ' | ' + label + ' | ' + ' | '.join([
                format_number(a.get('readIops')), format_number(b.get('readIops')),
                format_number(a.get('readMBps')) + ' / ' + format_number(b.get('readMBps')),
                format_number(op_ratio), format_number(byte_ratio),
                format_number(b.get('readAwaitMs'))]) + ' |')
    cleanup = read(ROOT / 'cleanup-state.json')
    cleanup_sentence = ('Owned experiment compute, ordinary disks, and shared watchers were removed. '
                        f'All {cleanup["snapshotCount"]} snapshots remain protected; identities '
                        'were checked before and after cleanup. Required online-backup WAL and '
                        'history files are preserved in the private backup-retention exports; '
                        'future recovery requires republishing that archive.' if cleanup.get('phase') == 'complete' else
                        'Cleanup of experiment compute and ordinary disks awaits completed '
                        'measurements, snapshot-retention verification, and required backup WAL exports.')
    lines += ['', 'Each run provides raw samples in its own `results/samples.jsonl`, '
              'effective configuration and storage identity in `preflight.json`, VACUUM output, '
              'continuous PostgreSQL logs, replay boundaries, and separate linear-axis '
              '`await.png`, `iops.png`, and `throughput.png` figures. The plot captions '
              'identify any clipped peaks; the raw samples preserve those values.', '',
              'Each standalone report embeds its complete evidence-linked end-to-end UTC timeline: '
              '[baseline](baseline-23gib/results/REPORT.md), '
              '[50 GiB](50gib/results/REPORT.md), and [100 GiB](100gib/results/REPORT.md). '
              'Snapshot creation, preparation retries, disk hydration, VACUUM, replica catch-up, '
              'required WAL exports, and cleanup are included. Exact events and first-observed '
              'copy states are labeled separately.', '',
              'No application workload runs in these tests. VACUUM exit 0, healthy replica catch-up '
              'through both the completion LSN and current primary flush LSN, and valid sample coverage '
              'are required for a completed result. Client latency is not measured.', '',
              'All runs keep `maintenance_work_mem=64MB`. The available final sample streams '
              'record maximum completed index-vacuum counts of ' + ', '.join(
                  f'{run["name"]}: {run.get("observedMaxIndexVacuumCount", "pending")}' for run in runs) + '. '
              'PostgreSQL can interrupt its heap scan for index and heap cleanup when its '
              'dead-tuple memory fills, then resume the scan. These cycles divide heap work '
              'into sections; they do not mean every cycle scans the entire heap. See '
              '[PostgreSQL 16 VACUUM progress phases](https://www.postgresql.org/docs/16/progress-reporting.html#VACUUM-PROGRESS-REPORTING). '
              'The observed cycle counts and the time-varying storage rates should be considered '
              'alongside fixture size when interpreting duration differences.', '',
              'All snapshots must remain in protected resource groups. ' + cleanup_sentence, '']
    result = {'generatedAtUtc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'complete': all(run['complete'] for run in runs), 'runs': runs}
    (ROOT / 'comparison.json').write_text(json.dumps(result, indent=2) + '\n')
    (ROOT / 'REPORT.md').write_text('\n'.join(lines))
    print('Comparison complete' if result['complete'] else 'Comparison pending completed measurements')


if __name__ == '__main__':
    main()
