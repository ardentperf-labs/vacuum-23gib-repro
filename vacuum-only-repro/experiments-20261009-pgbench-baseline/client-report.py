#!/usr/bin/env python3
"""Render linear client-impact graphs from native pgbench completion logs."""
import argparse
import datetime as dt
import json
import math
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


GAP_LIMIT_SECONDS = 2.5


def aware_utc(value, label='timestamp'):
    """Parse an ISO timestamp, rejecting naive values and normalizing to UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{label} must be a timezone-aware ISO timestamp')
    parsed = dt.datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f'{label} must include a timezone offset')
    return parsed.astimezone(dt.timezone.utc)


def epoch(value, label='timestamp'):
    if isinstance(value, bool):
        raise ValueError(f'{label} is not a valid epoch')
    if isinstance(value, (float, int)):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f'{label} is not a finite epoch')
        return number
    return aware_utc(value, label).timestamp()


def utc_text(value):
    return dt.datetime.fromtimestamp(float(value), dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def distribution(values):
    """Use the same floor-index percentile estimator as run/03-test.py."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {'count': 0, 'mean': None, 'p50': None, 'p95': None, 'p99': None, 'max': None}
    last = len(ordered) - 1
    return {'count': len(ordered), 'mean': statistics.mean(ordered),
            **{f'p{p}': ordered[int(last*p/100)] for p in [50, 95, 99]}, 'max': ordered[-1]}


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def finite_number(value, label):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f'{label} is not finite')
    return number


def read_warmup_record(root, timing):
    """Validate the persisted, server-counter measurement for the healthy warm-up."""
    path = root/'client-warmup.json'
    if not path.is_file():
        raise RuntimeError('missing client-warmup.json; cannot report the exact healthy warm-up TPS')
    measured = json.loads(path.read_text())
    embedded = timing.get('clientWarmup')
    if not isinstance(embedded, dict):
        raise RuntimeError('timing.json is missing its clientWarmup record')
    for field in ['startEpoch', 'endEpoch', 'seconds', 'transactions', 'averageTps']:
        if field not in measured or field not in embedded:
            raise RuntimeError(f'client warm-up record is missing {field}')
        if measured[field] != embedded[field]:
            raise RuntimeError(f'client-warmup.json and timing.json disagree on {field}')
    start = finite_number(measured['startEpoch'], 'client-warmup.startEpoch')
    end = finite_number(measured['endEpoch'], 'client-warmup.endEpoch')
    seconds = finite_number(measured['seconds'], 'client-warmup.seconds')
    average = finite_number(measured['averageTps'], 'client-warmup.averageTps')
    count = measured['transactions']
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise RuntimeError('client-warmup.transactions must be a nonnegative integer')
    if seconds <= 0 or end < start or not math.isclose(count/seconds, average, rel_tol=1e-9, abs_tol=1e-9):
        raise RuntimeError('client-warmup counter, duration, and TPS are inconsistent')
    return measured


def parse_transactions(root):
    transactions, malformed, errors = [], [], []
    sources = sorted((root/'client/raw').glob('pgbench-log.*'))
    if not sources:
        errors.append('No native pgbench completion log files were found.')
    for source in sources:
        for line_number, line in enumerate(source.read_text().splitlines(), 1):
            fields = line.split()
            try:
                if len(fields) < 7:
                    raise ValueError('missing schedule lag')
                latency = finite_number(fields[2], 'latency') / 1000.0
                completion = finite_number(fields[4], 'completion seconds') + int(fields[5]) / 1e6
                lag = finite_number(fields[6], 'schedule lag') / 1000.0
                if latency < 0 or lag < 0:
                    raise ValueError('negative latency or schedule lag')
                execution = latency - lag
                if execution < 0:
                    raise ValueError(f'schedule lag ({lag:g} ms) exceeds latency ({latency:g} ms)')
                transactions.append({'clientId': int(fields[0]), 'transactionNumber': int(fields[1]),
                                     'completedAtEpoch': completion, 'latencyMs': latency,
                                     'scheduleLagMs': lag, 'executionMs': execution})
            except (ValueError, OverflowError) as exc:
                malformed.append({'file': source.name, 'line': line_number, 'reason': str(exc)})
    if malformed:
        errors.append(f'{len(malformed)} malformed or internally inconsistent native completion row(s) were excluded.')
    transactions.sort(key=lambda row: row['completedAtEpoch'])
    return transactions, malformed, errors


def prepare_samples(sessions):
    good = []
    errors = []
    for index, row in enumerate(sessions, 1):
        try:
            controller_epoch = finite_number(row.get('sampleEpoch'), f'session sample {index} sampleEpoch')
            if row.get('sampleStatus') != 'ok':
                errors.append({'sourceIndex': index, 'controllerEpoch': controller_epoch,
                               'error': str(row.get('error', 'sampleStatus is not ok'))})
                continue
            db_time = aware_utc(row.get('observedAtUtc'), f'session sample {index} observedAtUtc')
            db_epoch = db_time.timestamp()
            good.append({**row, '_controllerEpoch': controller_epoch, '_dbEpoch': db_epoch,
                         '_observedAtUtc': utc_text(db_epoch)})
        except (ValueError, TypeError) as exc:
            try:
                controller_epoch = finite_number(row.get('sampleEpoch'), f'session sample {index} sampleEpoch')
            except (ValueError, TypeError):
                controller_epoch = None
            errors.append({'sourceIndex': index, 'controllerEpoch': controller_epoch, 'error': str(exc)})
    for before, after in zip(good, good[1:]):
        if after['_dbEpoch'] < before['_dbEpoch']:
            raise RuntimeError('successful DB observedAtUtc samples are not monotonic')
        if after['_controllerEpoch'] < before['_controllerEpoch']:
            raise RuntimeError('controller sampleEpoch values are not monotonic')
    return good, errors


def map_controller_epoch(controller_epoch, samples, snap=False, label='controller boundary'):
    """Map controller time to DB time using its nearest paired monitor sample."""
    if not samples:
        raise RuntimeError(f'cannot map {label}: no successful paired monitor samples')
    nearest = min(samples, key=lambda row: abs(row['_controllerEpoch']-controller_epoch))
    offset = nearest['_dbEpoch'] - nearest['_controllerEpoch']
    mapped = nearest['_dbEpoch'] if snap else controller_epoch + offset
    return {'controllerEpoch': controller_epoch, 'dbEpoch': mapped, 'dbTimeUtc': utc_text(mapped),
            'nearestSampleControllerEpoch': nearest['_controllerEpoch'],
            'nearestSampleObservedAtUtc': nearest['_observedAtUtc'],
            'nearestSampleDistanceSeconds': abs(nearest['_controllerEpoch']-controller_epoch),
            'appliedOffsetSeconds': offset,
            'mapping': 'nearest sample observedAtUtc' if snap else 'controller time plus nearest paired DB/controller offset'}


def event_time(events, name):
    rows = [row for row in events if row.get('event') == name]
    if not rows:
        return None
    row = rows[-1]
    if row.get('time') is None:
        raise RuntimeError(f'event {name} is present without its pod-clock time')
    return epoch(row['time'], f'event {name} time')


def stop_drain_time(timing, events):
    stop = timing.get('clientStop') or {}
    activity = stop.get('serverActivityAfterDrain') or {}
    value = activity.get('observedAtUtc')
    source = 'clientStop.serverActivityAfterDrain.observedAtUtc'
    if value is None:
        value = event_time(events, 'client_sessions_drained')
        source = 'client_sessions_drained event time'
    if value is None:
        raise RuntimeError('missing DB-clock client-session drain boundary')
    return (epoch(value, source) if isinstance(value, str) else float(value)), source


def time_intervals(start, vacuum_start, vacuum_end, recovered, healthy, stop, drain):
    ordered = [('warmup start', start), ('VACUUM start', vacuum_start), ('VACUUM complete', vacuum_end)]
    if recovered is not None:
        ordered.append(('replica reached target', recovered))
    if healthy is not None:
        ordered.append(('post-recovery healthy interval complete', healthy))
    ordered += [('client stop request', stop), ('client sessions drained', drain)]
    for (left_name, left), (right_name, right) in zip(ordered, ordered[1:]):
        if right < left:
            raise RuntimeError(f'non-monotonic client timeline: {right_name} precedes {left_name}')
    intervals = [('pre_vacuum', start, vacuum_start)]
    intervals.append(('vacuum', vacuum_start, vacuum_end))
    if recovered is None:
        intervals.append(('after_vacuum_client', vacuum_end, stop))
    else:
        intervals.append(('replay_catchup', vacuum_end, recovered))
        if healthy is None:
            intervals.append(('post_recovery_client', recovered, stop))
        else:
            intervals.extend([('recovery_verification', recovered, healthy),
                              ('post_recovery_client', healthy, stop)])
    intervals.append(('stop_drain', stop, drain))
    return intervals


def metrics_for_interval(name, low, high, transactions, samples):
    rows = [row for row in transactions if low <= row['completedAtEpoch'] < high]
    live = [row for row in samples if low <= row['_dbEpoch'] < high]
    seconds = max(0.0, high-low)
    return {'startEpoch': low, 'endEpoch': high, 'startUtc': utc_text(low), 'endUtc': utc_text(high),
            'seconds': seconds, 'completedTransactions': len(rows),
            'completedTps': len(rows)/seconds if seconds > 0 and name != 'stop_drain' else None,
            'clientActive': name != 'stop_drain',
            'transactionPhaseBasis': 'completion timestamp; a transaction may have started in an earlier phase',
            **{key: distribution([row[key] for row in rows]) for key in ['latencyMs', 'scheduleLagMs', 'executionMs']},
            'maxPendingXactAgeSeconds': max((row.get('maxActiveXactAgeSeconds') or 0 for row in live), default=None),
            'maxPendingQueryAgeSeconds': max((row.get('maxActiveQueryAgeSeconds') or 0 for row in live), default=None)}


def build_series(start, stop, width, transactions):
    duration = max(0.0, stop-start)
    count = math.ceil(duration/width) if duration > 0 else 0
    buckets = [[] for _ in range(count)]
    for row in transactions:
        completion = row['completedAtEpoch']
        if start <= completion < stop:
            index = int((completion-start)//width)
            if 0 <= index < count:
                buckets[index].append(row)
    series = []
    for index, rows in enumerate(buckets):
        low, high = start+index*width, min(start+(index+1)*width, stop)
        seconds = high-low
        series.append({'startEpoch': low, 'endEpoch': high, 'startUtc': utc_text(low), 'endUtc': utc_text(high),
                       'seconds': seconds, 'completedTransactions': len(rows),
                       'completedTps': len(rows)/seconds if seconds > 0 else None,
                       **{key: distribution([row[key] for row in rows]) for key in ['latencyMs', 'scheduleLagMs', 'executionMs']}})
    return series


def sampling_coverage(samples, sample_errors, start, end):
    duration = max(0.0, end-start)
    in_window = [row for row in samples if start <= row['_dbEpoch'] <= end]
    in_window.sort(key=lambda row: row['_dbEpoch'])
    covered = 0.0
    gaps = []
    intervals = []
    error_controller_epochs = [row['controllerEpoch'] for row in sample_errors
                               if row.get('controllerEpoch') is not None]
    if duration > 0:
        for left, right in zip(in_window, in_window[1:]):
            gap = right['_dbEpoch']-left['_dbEpoch']
            clipped_low, clipped_high = max(start, left['_dbEpoch']), min(end, right['_dbEpoch'])
            error_between = any(left['_controllerEpoch'] < value < right['_controllerEpoch']
                                for value in error_controller_epochs)
            if gap <= GAP_LIMIT_SECONDS and not error_between:
                covered += max(0.0, clipped_high-clipped_low)
                intervals.append((left, right))
            else:
                gaps.append({'startUtc': utc_text(left['_dbEpoch']), 'endUtc': utc_text(right['_dbEpoch']),
                             'seconds': gap,
                             'kind': 'sample error between successful observations' if error_between else 'successful-sample gap exceeds limit'})
        if in_window and in_window[0]['_dbEpoch'] > start:
            gaps.insert(0, {'startUtc': utc_text(start), 'endUtc': in_window[0]['_observedAtUtc'],
                            'seconds': in_window[0]['_dbEpoch']-start, 'kind': 'leading uncovered interval'})
        if in_window and in_window[-1]['_dbEpoch'] < end:
            gaps.append({'startUtc': in_window[-1]['_observedAtUtc'], 'endUtc': utc_text(end),
                         'seconds': end-in_window[-1]['_dbEpoch'], 'kind': 'trailing uncovered interval'})
        if not in_window:
            gaps.append({'startUtc': utc_text(start), 'endUtc': utc_text(end), 'seconds': duration,
                         'kind': 'no successful samples'})
    # Failed controller samples are retained as explicit gaps; the successful-point spacing also
    # breaks lines whenever those failures leave a gap wider than the accepted sampling interval.
    error_epochs = []
    for error in sample_errors:
        if error.get('controllerEpoch') is None:
            continue
        nearest = min(samples, key=lambda sample: abs(sample['_controllerEpoch']-error['controllerEpoch']))
        error_epochs.append(error['controllerEpoch'] + nearest['_dbEpoch']-nearest['_controllerEpoch'])
    report = {'windowStartUtc': utc_text(start), 'windowEndUtc': utc_text(end), 'windowSeconds': duration,
              'successfulSamples': len(in_window), 'sampleErrorsInWindow': sum(start <= value <= end for value in error_epochs),
              'gapLimitSeconds': GAP_LIMIT_SECONDS, 'coveredSeconds': min(duration, covered),
              'coverageFraction': min(1.0, covered/duration) if duration > 0 else None,
              'maximumSuccessfulSampleGapSeconds': max((b['_dbEpoch']-a['_dbEpoch'] for a,b in zip(in_window,in_window[1:])), default=None),
              'gaps': gaps,
              'coverageDefinition': 'Only intervals between consecutive successful DB observations no more than 2.5 seconds apart count as covered; leading and trailing intervals are uncovered.'}
    return report, intervals


def series_with_breaks(samples, x, key, sample_errors=()):
    px, py = [], []
    for index, (sample, x_value) in enumerate(zip(samples, x)):
        if index:
            previous = samples[index-1]
            gap = sample['_dbEpoch']-previous['_dbEpoch']
            controller_left, controller_right = previous['_controllerEpoch'], sample['_controllerEpoch']
            has_error = any(controller_left < row['_controllerEpoch'] < controller_right
                            for row in sample_errors if row.get('_controllerEpoch') is not None)
            if gap > GAP_LIMIT_SECONDS or has_error:
                px.append(math.nan); py.append(math.nan)
        px.append(x_value)
        value = sample.get(key)
        py.append(0 if value is None else value)
    return px, py


def plot_with_breaks(ax, x, samples, key, sample_errors=(), **kwargs):
    px, py = series_with_breaks(samples, x, key, sample_errors)
    ax.plot(px, py, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    args = parser.parse_args()
    root = args.results.resolve()
    timing = json.loads((root/'timing.json').read_text())
    warmup = read_warmup_record(root, timing)
    events = read_jsonl(root/'events.jsonl')
    config = json.loads((root/'client-config.json').read_text())
    sessions = read_jsonl(root/'client/sessions.jsonl')
    transactions, malformed, validation_errors = parse_transactions(root)
    samples, sample_errors = prepare_samples(sessions)
    untimed_sample_errors = sum(row.get('controllerEpoch') is None for row in sample_errors)
    if untimed_sample_errors:
        validation_errors.append(f'{untimed_sample_errors} session sample row(s) have no usable controller timestamp and cannot be placed on the report timeline.')
    if not samples:
        raise RuntimeError('no successful pending-session samples with DB observedAtUtc timestamps')

    warmup_epoch = epoch(timing['clientWarmup']['startEpoch'], 'clientWarmup.startEpoch')
    warmup_mapping = map_controller_epoch(warmup_epoch, samples, snap=True, label='warm-up start')
    warmup_end_mapping = map_controller_epoch(float(warmup['endEpoch']), samples, snap=True,
                                              label='warm-up end')
    start = warmup_mapping['dbEpoch']
    warmup_end = warmup_end_mapping['dbEpoch']
    vacuum_start = epoch(timing['vacuumStart'], 'vacuumStart')
    vacuum_end = epoch(timing['vacuumComplete'], 'vacuumComplete')
    recovered = event_time(events, 'replica_reached_vacuum_target_with_client')
    healthy = event_time(events, 'client_post_recovery_healthy_complete')
    stop = timing.get('clientStop') or {}
    stop_text = stop.get('requestedAtUtc')
    if stop_text is None:
        raise RuntimeError('missing actual client stop requestedAtUtc; refusing to infer the active-client boundary')
    stop_controller_epoch = epoch(stop_text, 'clientStop.requestedAtUtc')
    stop_mapping = map_controller_epoch(stop_controller_epoch, samples, label='client stop request')
    stop_db = stop_mapping['dbEpoch']
    drain, drain_source = stop_drain_time(timing, events)
    if warmup_end <= start or warmup_end > vacuum_start:
        raise RuntimeError('mapped healthy warm-up must end after it starts and no later than VACUUM start')
    intervals = time_intervals(start, vacuum_start, vacuum_end, recovered, healthy, stop_db, drain)

    if any(not math.isfinite(value) for row in transactions for value in row.values() if isinstance(value, (float, int))):
        raise RuntimeError('non-finite native transaction values are present')
    measured_rows = [row for row in transactions if start <= row['completedAtEpoch'] < drain]
    outside_before = sum(row['completedAtEpoch'] < start for row in transactions)
    outside_after = sum(row['completedAtEpoch'] >= drain for row in transactions)
    phase_metrics = {name: metrics_for_interval(name, low, high, measured_rows, samples)
                     for name, low, high in intervals}
    warmup_metrics = metrics_for_interval('healthy_warmup', start, warmup_end, measured_rows, samples)
    warmup_metrics.update({'serverCommittedTransactions': warmup['transactions'],
                           'serverCommittedTps': warmup['averageTps'],
                           'serverCounterDurationSeconds': warmup['seconds'],
                           'serverCounterStartEpoch': warmup['startEpoch'],
                           'serverCounterEndEpoch': warmup['endEpoch'],
                           'healthyWindows': warmup.get('healthyWindows', []),
                           'sourceFile': 'client-warmup.json',
                           'serverCounterBasis': 'committed transaction counter delta over the confirmed healthy warm-up window'})
    width = 10.0
    series = build_series(start, stop_db, width, measured_rows)
    coverage, coverage_intervals = sampling_coverage(samples, sample_errors, start, drain)
    offsets = [row['_dbEpoch']-row['_controllerEpoch'] for row in samples]
    target_tps = finite_number(config['targetTps'], 'client-config.targetTps')
    client_count = int(config['clients'])

    report = {'targetTps': target_tps, 'clients': client_count,
              'synchronousCommit': config['synchronousCommit'],
              'targetTpsMeaning': 'Expected Poisson offer rate; not a fixed transaction count or backlog.',
              'completionBucketSeconds': width, 'healthyWarmup': warmup_metrics,
              'phases': phase_metrics, 'series': series,
              'transactionPhaseBasis': 'Transactions are assigned by pod-clock completion timestamp; latency can include work begun in an earlier phase.',
              'totalCompletedTransactions': len(transactions),
              'measurementCompletedTransactions': len(measured_rows),
              'transactionsBeforeMeasurementStart': outside_before, 'transactionsAfterDrainBoundary': outside_after,
              'malformedRows': malformed, 'validationErrors': validation_errors,
              'validForClaims': not validation_errors and coverage['sampleErrorsInWindow'] == 0,
              'sessionSamples': coverage['successfulSamples'],
              'sessionSampleErrors': coverage['sampleErrorsInWindow'],
              'sessionSamplesInFullFile': len(sessions), 'sessionSampleErrorsInFullFile': len(sample_errors),
              'sessionSampleErrorDetails': sample_errors,
              'sessionSampleMaxGapSeconds': coverage['maximumSuccessfulSampleGapSeconds'],
              'samplingCoverage': coverage,
              'clockAlignment': {'sourceClock': 'PostgreSQL primary pod UTC observedAtUtc for samples, transaction completions, VACUUM and recovery boundaries.',
                                 'controllerToDbOffsetSeconds': {'min': min(offsets), 'max': max(offsets),
                                     'median': statistics.median(offsets), 'sampleCount': len(offsets),
                                     'definition': 'DB observedAtUtc epoch minus controller sampleEpoch for paired successful monitor rows.'},
                                 'warmupStart': warmup_mapping, 'warmupEnd': warmup_end_mapping,
                                 'clientStopRequest': stop_mapping,
                                 'drainBoundaryUtc': utc_text(drain), 'drainBoundarySource': drain_source},
              'notes': ['Target TPS is the expected Poisson offer rate; no synthetic offered-count backlog is inferred.',
                        'Latency includes schedule lag; execution latency subtracts schedule lag and includes commit wait.',
                        'Latency statistics contain completed transactions only. Pending ages include sampled unfinished work.',
                        'Transactions are assigned to phases by completion time, so phase latency can include work started earlier.',
                        'TPS buckets include zero-completion intervals only while pgbench is active and stop at the mapped stop request.',
                        'The healthy warm-up TPS is the exact primary committed-transaction counter delta in client-warmup.json; phase TPS uses native client-log completion timestamps.',
                        'The pre_vacuum phase is the full interval from warm-up start to VACUUM start, including the post-warm-up checkpoint and replica replay wait.',
                        'The stop_drain phase includes completions from transactions still in flight at the stop request; no active-client TPS is reported for it.',
                        'Successful pending-work samples are graphed at DB observedAtUtc; lines break across sample errors and gaps over 2.5 seconds.',
                        'Sampling coverage counts only intervals between consecutive successful DB observations no more than 2.5 seconds apart.',
                        'All graph axes are linear.']}
    (root/'client-impact.json').write_text(json.dumps(report, indent=2)+'\n')

    def decorate(ax, title, ylabel):
        ax.axvspan((vacuum_start-start)/60, (vacuum_end-start)/60,
                   color='#e8b053', alpha=.18, label='VACUUM')
        ax.set(title=title, xlabel='Minutes since healthy client warm-up began', ylabel=ylabel)
        ax.set_yscale('linear')
        ax.set_ylim(bottom=0)
        ax.grid(alpha=.2)
        ax.legend(fontsize=8)

    x = [(row['startEpoch']+row['endEpoch']-2*start)/120 for row in series]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(x, [row['completedTps'] for row in series], label='Completed TPS (10 s buckets)', color='#1766ac')
    ax.axhline(target_tps, label=f'Expected Poisson offer: {target_tps:g} TPS', color='#444', linestyle='--')
    decorate(ax, 'Client transaction completions', 'Transactions / second')
    fig.tight_layout(); fig.savefig(root/'client-tps.png', dpi=160); plt.close(fig)

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    for ax, key, title in zip(axes, ['latencyMs', 'executionMs', 'scheduleLagMs'],
                              ['End-to-end latency (includes schedule lag)',
                               'Execution latency (includes commit wait)', 'Schedule lag']):
        for pct, color in [('p50', '#1766ac'), ('p95', '#cf5c24'), ('max', '#7d509a')]:
            ax.plot(x, [row[key][pct] if row[key][pct] is not None else math.nan for row in series],
                    label=pct, color=color)
        decorate(ax, title, 'Milliseconds')
    fig.tight_layout(); fig.savefig(root/'client-latency.png', dpi=160); plt.close(fig)

    pending_samples = [row for row in samples if start <= row['_dbEpoch'] <= drain]
    pending_x = [(row['_dbEpoch']-start)/60 for row in pending_samples]
    plotted_errors = []
    for row in sample_errors:
        if row.get('controllerEpoch') is None:
            continue
        plotted_errors.append({**row, '_controllerEpoch': row['controllerEpoch']})
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    plot_with_breaks(axes[0], pending_x, pending_samples, 'maxActiveXactAgeSeconds', plotted_errors,
                     label='Oldest active transaction', color='#1766ac')
    plot_with_breaks(axes[0], pending_x, pending_samples, 'maxActiveQueryAgeSeconds', plotted_errors,
                     label='Oldest active query', color='#cf5c24')
    decorate(axes[0], 'Pending client work (includes unfinished transactions)', 'Seconds')
    px, active, sync = [], [], []
    for index, row in enumerate(pending_samples):
        if index:
            previous = pending_samples[index-1]
            gap = row['_dbEpoch']-previous['_dbEpoch']
            errors_between = any(previous['_controllerEpoch'] < item['_controllerEpoch'] < row['_controllerEpoch']
                                 for item in plotted_errors)
            if gap > GAP_LIMIT_SECONDS or errors_between:
                px.append(math.nan); active.append(math.nan); sync.append(math.nan)
        px.append(pending_x[index])
        backends = row.get('backends', [])
        active.append(sum(item.get('state') == 'active' for item in backends))
        sync.append(sum(item.get('waitEvent') == 'SyncRep' for item in backends))
    axes[1].plot(px, active, label='Active client sessions')
    axes[1].plot(px, sync, label='Sessions waiting for SyncRep')
    decorate(axes[1], 'Client concurrency and synchronous commit waits', 'Sessions')
    axes[1].set_yticks(range(0, client_count+1)); axes[1].set_ylim(0, client_count+.5)
    fig.tight_layout(); fig.savefig(root/'client-pending.png', dpi=160); plt.close(fig)
    print(json.dumps({key: value for key, value in report.items() if key not in ('series', 'sessionSampleErrorDetails')}, indent=2))


if __name__ == '__main__':
    main()
