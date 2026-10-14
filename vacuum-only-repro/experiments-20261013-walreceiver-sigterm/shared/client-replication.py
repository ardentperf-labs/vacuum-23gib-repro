#!/usr/bin/env python3
"""Plot client throughput/latency alongside sampled PostgreSQL replication lag."""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


NOMINAL_SAMPLE_SECONDS = 5.0
GAP_LIMIT_SECONDS = 2 * NOMINAL_SAMPLE_SECONDS
MIB = 1024 ** 2
GIB = 1024 ** 3
LSN_RE = re.compile(r'^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$')
PLOT_TITLE = 'Client response and replication backlog across warm-up, VACUUM, and recovery'
TIME_AXIS_LABEL = 'Minutes since healthy warm-up began (primary DB clock)'


def parse_epoch(value, label):
    """Parse a finite Unix epoch or timezone-aware ISO timestamp."""
    if isinstance(value, bool) or value is None:
        raise ValueError(f'{label} is missing or invalid')
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f'{label} is empty')
        try:
            result = float(text)
        except ValueError:
            stamp = dt.datetime.fromisoformat(text.replace('Z', '+00:00'))
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise ValueError(f'{label} must include a timezone')
            result = stamp.astimezone(dt.timezone.utc).timestamp()
    else:
        raise ValueError(f'{label} has unsupported type {type(value).__name__}')
    if not math.isfinite(result):
        raise ValueError(f'{label} is not finite')
    return result


def parse_lsn(value):
    """Convert PostgreSQL X/Y LSN text to its byte position, or raise ValueError."""
    if not isinstance(value, str) or not LSN_RE.fullmatch(value.strip()):
        raise ValueError('missing or invalid PostgreSQL LSN')
    high, low = value.strip().split('/')
    return (int(high, 16) << 32) + int(low, 16)


def lsn_backlog_bytes(primary_lsn, downstream_lsn):
    """Return a nonnegative concurrent LSN delta and whether it was clamped."""
    delta = primary_lsn - downstream_lsn
    return (0, True) if delta < 0 else (delta, False)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read_jsonl(path):
    rows, malformed = [], []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            rows.append((line_number, json.loads(line)))
        except json.JSONDecodeError as exc:
            malformed.append({'line': line_number, 'error': str(exc)})
    return rows, malformed


def _lsn_field(value, key, errors, prefix):
    try:
        return parse_lsn((value or {}).get(key))
    except (ValueError, AttributeError, TypeError):
        errors.append(f'{prefix}.{key}')
        return None


def parse_sample_rows(rows, start, stop):
    """Parse source samples; preserve timestamped invalid LSNs as explicit gaps."""
    parsed, unplaceable, ordering = [], [], {'reorderedRows': 0, 'duplicateTimestamps': 0}
    previous_source_time = None
    timestamped_count = 0
    for line_number, raw in rows:
        primary = raw.get('primary') if isinstance(raw, dict) else None
        replica = raw.get('replica') if isinstance(raw, dict) else None
        if not isinstance(primary, dict):
            unplaceable.append({'line': line_number, 'reason': 'missing primary sample'})
            continue
        try:
            primary_time = primary.get('epoch')
            if primary_time is None:
                primary_time = primary.get('ts')
            timestamp = parse_epoch(primary_time, f'line {line_number} primary time')
        except (ValueError, TypeError, OverflowError) as exc:
            unplaceable.append({'line': line_number, 'reason': str(exc)})
            continue
        timestamped_count += 1
        if previous_source_time is not None:
            if timestamp < previous_source_time:
                ordering['reorderedRows'] += 1
            elif timestamp == previous_source_time:
                ordering['duplicateTimestamps'] += 1
        previous_source_time = timestamp
        if timestamp < start or timestamp > stop:
            continue

        reasons = {'standbyFlush': [], 'replicaReplay': []}
        primary_lsn = _lsn_field(primary, 'flushLsn', reasons['standbyFlush'], 'primary')
        senders = primary.get('senders')
        sender_lsn = None
        if not isinstance(senders, list) or len(senders) != 1 or not isinstance(senders[0], dict):
            reasons['standbyFlush'].append('primary.senders (expected exactly one sender)')
        else:
            sender_lsn = _lsn_field(senders[0], 'flushLsn', reasons['standbyFlush'], 'primary.senders[0]')
        replay_lsn = _lsn_field(replica, 'replayLsn', reasons['replicaReplay'], 'replica')
        # Replay backlog is primary flush minus replica replay, so a missing
        # primary flush LSN invalidates this metric too (not only standby flush).
        if primary_lsn is None and 'primary.flushLsn' not in reasons['replicaReplay']:
            reasons['replicaReplay'].append('primary.flushLsn')

        flush_mib = replay_gib = None
        flush_clamped = replay_clamped = False
        if primary_lsn is not None and sender_lsn is not None:
            delta, flush_clamped = lsn_backlog_bytes(primary_lsn, sender_lsn)
            flush_mib = delta / MIB
        if primary_lsn is not None and replay_lsn is not None:
            delta, replay_clamped = lsn_backlog_bytes(primary_lsn, replay_lsn)
            replay_gib = delta / GIB
        parsed.append({
            'line': line_number, 'epoch': timestamp,
            'standbyFlushMiB': flush_mib, 'replicaReplayGiB': replay_gib,
            'flushNegativeClamped': flush_clamped, 'replayNegativeClamped': replay_clamped,
            'standbyFlushInvalidFields': reasons['standbyFlush'],
            'replicaReplayInvalidFields': reasons['replicaReplay'],
        })
    parsed.sort(key=lambda row: (row['epoch'], row['line']))
    return parsed, {
        'inputRows': len(rows), 'timestampedRows': timestamped_count,
        'inWindowRows': len(parsed), 'unplaceableRows': unplaceable,
        'ordering': ordering,
    }


def metric_coverage(rows, key, start, stop):
    """Count actual represented intervals; invalid samples and long gaps break coverage."""
    valid = [row for row in rows if row[key] is not None]
    covered = 0.0
    long_gaps = []
    invalid = [row for row in rows if row[key] is None]
    for left, right in zip(rows, rows[1:]):
        gap = right['epoch'] - left['epoch']
        if gap > GAP_LIMIT_SECONDS:
            long_gaps.append({'startEpoch': left['epoch'], 'endEpoch': right['epoch'], 'seconds': gap})
        if left[key] is not None and right[key] is not None and gap <= GAP_LIMIT_SECONDS:
            covered += gap
    duration = max(0.0, stop - start)
    first_valid = valid[0]['epoch'] if valid else None
    last_valid = valid[-1]['epoch'] if valid else None
    valid_intervals = [b['epoch']-a['epoch'] for a, b in zip(valid, valid[1:])]
    return {
        'validSamples': len(valid), 'invalidSamples': len(invalid),
        'invalidFieldCounts': _field_counts(invalid, key),
        'maximumBacklog': max((row[key] for row in valid), default=None),
        'firstValidSampleEpoch': first_valid,
        'firstValidSampleUtc': _utc(first_valid) if first_valid is not None else None,
        'lastValidSampleEpoch': last_valid,
        'lastValidSampleUtc': _utc(last_valid) if last_valid is not None else None,
        'unobservedLeadingSeconds': (max(0.0, first_valid-start) if first_valid is not None else duration),
        'unobservedTrailingSeconds': (max(0.0, stop-last_valid) if last_valid is not None else duration),
        'maximumValidSampleGapSeconds': max(valid_intervals, default=None),
        'negativeConcurrentLagClampedSamples': sum(
            row['flushNegativeClamped' if key == 'standbyFlushMiB' else 'replayNegativeClamped'] for row in rows),
        'longSamplingGapCount': len(long_gaps), 'longSamplingGaps': long_gaps,
        'maximumTimestampGapSeconds': max((b['epoch']-a['epoch'] for a, b in zip(rows, rows[1:])), default=None),
        'coveredSeconds': min(duration, covered),
        'coverageFraction': min(1.0, covered / duration) if duration else None,
        'gapRule': 'A line is broken at missing/invalid LSNs and between samples more than 10 seconds apart.',
    }


def _field_counts(rows, key):
    counts = {}
    field_key = 'standbyFlushInvalidFields' if key == 'standbyFlushMiB' else 'replicaReplayInvalidFields'
    for row in rows:
        for field in row[field_key]:
            counts[field] = counts.get(field, 0) + 1
    return counts


def line_points(rows, key, minute_zero):
    """Create plotted coordinates with explicit NaNs for invalid values and long gaps."""
    xs, ys = [], []
    previous = None
    for row in rows:
        if previous is not None and row['epoch'] - previous['epoch'] > GAP_LIMIT_SECONDS:
            xs.append(math.nan)
            ys.append(math.nan)
        xs.append((row['epoch'] - minute_zero) / 60.0)
        ys.append(row[key] if row[key] is not None else math.nan)
        previous = row
    return xs, ys


def recovery_markers(phases, minute_zero, stop):
    markers = []
    def add(label, epoch, kind):
        if epoch is not None and minute_zero <= epoch <= stop:
            markers.append({'label': label, 'epoch': epoch, 'minutesFromZero': (epoch-minute_zero)/60,
                            'kind': kind})
    vacuum = phases.get('vacuum') or {}
    add('VACUUM started', _optional_epoch(vacuum.get('startEpoch')), 'vacuum-start')
    add('VACUUM completed', _optional_epoch(vacuum.get('endEpoch')), 'vacuum-end')
    replay = phases.get('replay_catchup') or {}
    add('Replica reached replay target', _optional_epoch(replay.get('endEpoch')), 'replay-target')
    healthy = phases.get('recovery_verification') or {}
    add('Post-recovery healthy interval', _optional_epoch(healthy.get('endEpoch')), 'recovery-healthy')
    return markers


def _optional_epoch(value):
    if value is None:
        return None
    try:
        return parse_epoch(value, 'phase epoch')
    except (ValueError, TypeError, OverflowError):
        return None


def _series_summary(series, key):
    values = [item.get(key) for item in series]
    valid = [value for value in values if isinstance(value, (int, float)) and math.isfinite(value)]
    return {'bucketCount': len(series), 'validBuckets': len(valid), 'emptyBuckets': len(series)-len(valid)}


def build_evidence(results_dir, impact_path, samples_path, impact, raw_rows, malformed):
    phases = impact.get('phases') or {}
    try:
        minute_zero = parse_epoch(phases['pre_vacuum']['startEpoch'], 'phases.pre_vacuum.startEpoch')
        stop = parse_epoch(impact['clockAlignment']['clientStopRequest']['dbEpoch'],
                           'clockAlignment.clientStopRequest.dbEpoch')
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError(f'client-impact lacks required common-window clock boundaries: {exc}') from exc
    if stop <= minute_zero:
        raise ValueError('client stop request must be later than the pre-vacuum minute-zero boundary')
    parsed, raw_summary = parse_sample_rows(raw_rows, minute_zero, stop)
    series = impact.get('series')
    if not isinstance(series, list) or not series:
        raise ValueError('client-impact.series must contain the native pgbench 10-second buckets')
    series = [row for row in series if _bucket_overlaps(row, minute_zero, stop)]
    if not series:
        raise ValueError('no client-impact series buckets overlap the active client window')
    markers = recovery_markers(phases, minute_zero, stop)
    if not any(row['kind'] == 'vacuum-start' for row in markers) or not any(row['kind'] == 'vacuum-end' for row in markers):
        raise ValueError('client-impact.phases.vacuum must provide startEpoch and endEpoch')
    return {
        'schemaVersion': 1,
        'sources': {
            'clientImpact': {'path': str(impact_path.relative_to(results_dir)), 'sha256': sha256(impact_path)},
            'rawSamples': {'path': str(samples_path.relative_to(results_dir)), 'sha256': sha256(samples_path)},
        },
        'window': {
            'minuteZeroEpoch': minute_zero,
            'minuteZeroUtc': _utc(minute_zero),
            'minuteZeroSource': 'client-impact.phases.pre_vacuum.startEpoch (healthy warm-up start)',
            'clientStopRequestEpoch': stop,
            'clientStopRequestUtc': _utc(stop),
            'stopSource': 'client-impact.clockAlignment.clientStopRequest.dbEpoch',
            'activeWindowSeconds': stop-minute_zero,
            'sampleNominalPeriodSeconds': NOMINAL_SAMPLE_SECONDS,
            'sampleLineBreakThresholdSeconds': GAP_LIMIT_SECONDS,
            'rawSampleBoundaryRule': 'Only samples with primary DB epoch from minute zero through the mapped client stop request are plotted.',
        },
        'series': {
            'completedTps': {
                'source': 'client-impact.series[].completedTps',
                'targetTps': impact.get('targetTps'),
                'bucketSeconds': impact.get('completionBucketSeconds', 10.0),
                **_series_summary(series, 'completedTps'),
            },
            'executionLatencyP95Ms': {
                'source': 'client-impact.series[].executionMs.p95',
                'definition': 'Native pgbench execution latency after subtracting schedule lag; includes commit wait.',
                'bucketCount': len(series),
                'validBuckets': sum(isinstance((row.get('executionMs') or {}).get('p95'), (int, float)) and
                                    math.isfinite((row.get('executionMs') or {}).get('p95')) for row in series),
                'emptyBuckets': sum((row.get('executionMs') or {}).get('p95') is None for row in series),
            },
            'standbyFlushBacklogMiB': {
                'definition': 'max(0, primary.flushLsn - sole primary.senders[0].flushLsn), converted to MiB.',
                **metric_coverage(parsed, 'standbyFlushMiB', minute_zero, stop),
            },
            'replicaReplayBacklogGiB': {
                'definition': 'max(0, primary.flushLsn - replica.replayLsn), converted to GiB.',
                **metric_coverage(parsed, 'replicaReplayGiB', minute_zero, stop),
            },
        },
        'sampling': {
            **raw_summary,
            'outsideWindowRows': raw_summary['timestampedRows'] - raw_summary['inWindowRows'],
            'malformedJsonRows': malformed,
            'malformedJsonRowCount': len(malformed),
            'lineBreakCounts': {
                'standbyFlushLongGaps': len(metric_coverage(parsed, 'standbyFlushMiB', minute_zero, stop)['longSamplingGaps']),
                'replicaReplayLongGaps': len(metric_coverage(parsed, 'replicaReplayGiB', minute_zero, stop)['longSamplingGaps']),
            },
        },
        'markers': markers,
        'interpretation': [
            'The execution-latency p95 series includes commit wait; the graph does not isolate or directly measure commit-wait duration.',
            'The standby FLUSH backlog uses the flush LSN reported by the sole primary replication sender and is sampled concurrently with primary flush.',
            'The replica REPLAY backlog compares concurrently sampled primary flush and replica replay positions; negative deltas are clamped to zero and counted because of sampling concurrency.',
            'Missing/invalid LSN inputs produce gaps. Lines also break where adjacent timestamped samples are more than 10 seconds apart.',
            'All four panels share elapsed minutes from the healthy client warm-up start and use linear y axes.',
        ],
    }


def _bucket_overlaps(row, start, stop):
    try:
        low = parse_epoch(row.get('startEpoch'), 'series.startEpoch')
        high = parse_epoch(row.get('endEpoch'), 'series.endEpoch')
    except (ValueError, TypeError):
        return False
    return high > start and low < stop


def _utc(value):
    return dt.datetime.fromtimestamp(value, dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def render(results_dir, output_dir=None):
    results_dir = results_dir.resolve()
    output_dir = (output_dir or results_dir).resolve()
    impact_path = results_dir / 'client-impact.json'
    if not impact_path.is_file():
        raise FileNotFoundError(f'missing {impact_path}')
    samples_candidates = [results_dir / 'rawsamples.jsonl', results_dir / 'samples.jsonl']
    samples_path = next((path for path in samples_candidates if path.is_file()), None)
    if samples_path is None:
        raise FileNotFoundError(f'missing rawsamples.jsonl or samples.jsonl in {results_dir}')
    impact = json.loads(impact_path.read_text())
    raw_rows, malformed = read_jsonl(samples_path)
    evidence = build_evidence(results_dir, impact_path, samples_path, impact, raw_rows, malformed)
    start, stop = evidence['window']['minuteZeroEpoch'], evidence['window']['clientStopRequestEpoch']
    phases = impact['phases']
    series = [row for row in impact['series'] if _bucket_overlaps(row, start, stop)]
    parsed, _ = parse_sample_rows(raw_rows, start, stop)
    markers = evidence['markers']
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(4, 1, figsize=(14, 12), sharex=True)
    titles = [
        ('Completed transaction rate', 'Completed transactions / second'),
        ('Execution latency p95 (includes commit wait)', 'Milliseconds'),
        ('Standby FLUSH backlog (last flush LSN reported to primary)', 'MiB'),
        ('Replica REPLAY backlog', 'GiB'),
    ]
    vacuum = phases['vacuum']
    vx0 = (parse_epoch(vacuum['startEpoch'], 'VACUUM start') - start) / 60
    vx1 = (parse_epoch(vacuum['endEpoch'], 'VACUUM end') - start) / 60
    for ax, (title, ylabel) in zip(axes, titles):
        ax.axvspan(vx0, vx1, color='#e8b053', alpha=.20, label='VACUUM')
        ax.set_title(title, loc='left')
        ax.set_ylabel(ylabel)
        ax.set_yscale('linear')
        ax.grid(alpha=.22)
        for marker in markers:
            if marker['kind'] in ('vacuum-start', 'vacuum-end'):
                continue
            ax.axvline(marker['minutesFromZero'], color='#8055a3', linestyle='--', alpha=.72,
                       label=marker['label'])
        ax.legend(loc='upper right', fontsize=7, framealpha=.8)

    tps_x, tps_y, latency_y = [], [], []
    for bucket in series:
        low = max(start, parse_epoch(bucket['startEpoch'], 'series.startEpoch'))
        high = min(stop, parse_epoch(bucket['endEpoch'], 'series.endEpoch'))
        tps_x.append((low + high - 2 * start) / 120)
        tps_y.append(bucket.get('completedTps') if bucket.get('completedTps') is not None else math.nan)
        p95 = (bucket.get('executionMs') or {}).get('p95')
        latency_y.append(p95 if p95 is not None else math.nan)
    axes[0].plot(tps_x, tps_y, drawstyle='steps-mid', marker='.', markersize=3,
                 color='#1766ac', label='Native pgbench completions (10 s buckets)')
    target = impact.get('targetTps')
    if isinstance(target, (int, float)) and math.isfinite(target):
        axes[0].axhline(target, color='#444444', linestyle=':', label=f'Expected offer: {target:g} TPS')
    axes[0].legend(loc='upper right', fontsize=7, framealpha=.8)
    axes[1].plot(tps_x, latency_y, marker='.', markersize=3, color='#cf5c24', label='Execution p95')
    axes[1].legend(loc='upper right', fontsize=7, framealpha=.8)

    for ax, key, color, label in (
        (axes[2], 'standbyFlushMiB', '#16806b', 'Primary minus sender flush'),
        (axes[3], 'replicaReplayGiB', '#8055a3', 'Primary flush minus replica replay'),
    ):
        x, y = line_points(parsed, key, start)
        evidence_key = ('standbyFlushBacklogMiB' if key == 'standbyFlushMiB'
                        else 'replicaReplayBacklogGiB')
        clamped = evidence['series'][evidence_key]['negativeConcurrentLagClampedSamples']
        ax.plot(x, y, color=color, marker='.', markersize=3, linewidth=1,
                label=f'{label} (negative clamps: {clamped})')
        ax.legend(loc='upper right', fontsize=7, framealpha=.8)
    axes[-1].set_xlabel(TIME_AXIS_LABEL)
    axes[-1].set_xlim(0, (stop-start)/60)
    for ax in axes:
        ax.relim()
        ax.autoscale_view(scalex=False, scaley=True)
        _, upper = ax.get_ylim()
        ax.set_ylim(0, upper if upper > 0 else 1.0)
    fig.suptitle(PLOT_TITLE, y=.995, fontsize=14)
    fig.tight_layout(rect=(0, .025, 1, .985))
    fig.text(.01, .004,
             'Execution p95 includes commit wait but does not isolate its duration; LSN backlogs compare concurrent samples.',
             fontsize=8, color='#444444')
    png_path = output_dir / 'client-replication.png'
    fig.savefig(png_path, dpi=170)
    plt.close(fig)
    evidence['output'] = {'plot': png_path.name, 'plotSha256': sha256(png_path),
                          'plotTitle': PLOT_TITLE, 'timeAxisLabel': TIME_AXIS_LABEL,
                          'allAxesLinear': True, 'sharedTimeAxis': True}
    evidence_path = output_dir / 'client-replication.json'
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + '\n')
    return evidence_path, png_path, evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path, help='run/results directory with client-impact.json and samples')
    parser.add_argument('--output-dir', type=Path, help='output directory (defaults to results)')
    args = parser.parse_args(argv)
    try:
        evidence_path, png_path, evidence = render(args.results, args.output_dir)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f'client-replication: {exc}', file=sys.stderr)
        return 2
    print(json.dumps({
        'evidence': str(evidence_path), 'plot': str(png_path),
        'windowSeconds': evidence['window']['activeWindowSeconds'],
        'sampleRows': evidence['sampling']['inWindowRows'],
        'flushValidSamples': evidence['series']['standbyFlushBacklogMiB']['validSamples'],
        'replayValidSamples': evidence['series']['replicaReplayBacklogGiB']['validSamples'],
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
