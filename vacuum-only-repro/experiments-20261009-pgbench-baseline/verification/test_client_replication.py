#!/usr/bin/env python3
"""Local clock, LSN, gap and synthetic-render checks for client-replication.py."""
import importlib.util
import json
import math
import datetime as dt
from pathlib import Path
import unittest


HERE = Path(__file__).resolve().parents[1]
SCRIPT = HERE / 'client-replication.py'
SPEC = importlib.util.spec_from_file_location('client_replication', SCRIPT)
cr = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cr)
SYNTHETIC = HERE / 'run' / '.state' / 'report-review' / 'synthetic' / 'client-replication-fixture'
RESULTS = SYNTHETIC / 'run' / 'results'


def sample(epoch, flush, sender, replay):
    stamp = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
    return {
        'primary': {'epoch': epoch, 'ts': stamp,
                    'flushLsn': flush,
                    'senders': [{'flushLsn': sender, 'state': 'streaming'}]},
        'replica': {'epoch': epoch + 0.05, 'replayLsn': replay},
    }


def fixture():
    RESULTS.mkdir(parents=True, exist_ok=True)
    start, stop = 1000.0, 1030.0
    phases = {
        'pre_vacuum': {'startEpoch': start, 'endEpoch': 1005.0},
        'vacuum': {'startEpoch': 1005.0, 'endEpoch': 1015.0},
        'replay_catchup': {'startEpoch': 1015.0, 'endEpoch': 1020.0},
        'recovery_verification': {'startEpoch': 1020.0, 'endEpoch': 1025.0},
        'post_recovery_client': {'startEpoch': 1025.0, 'endEpoch': stop},
    }
    buckets = []
    for index, low in enumerate(range(1000, 1030, 10)):
        buckets.append({
            'startEpoch': float(low), 'endEpoch': float(low + 10),
            'completedTps': [5.0, 3.0, 5.0][index],
            'executionMs': {'p95': [12.0, 25.0, 13.0][index]},
        })
    impact = {
        'targetTps': 5, 'completionBucketSeconds': 10.0,
        'phases': phases, 'series': buckets,
        'clockAlignment': {'clientStopRequest': {'dbEpoch': stop}},
    }
    (RESULTS / 'client-impact.json').write_text(json.dumps(impact, indent=2) + '\n')
    raw = [
        {'seq': 1, 'phase': 'vacuum', **sample(1000.0, '1/00000000', '0/00000000', '0/80000000')},
        {'seq': 2, 'phase': 'vacuum', **sample(1005.0, '2/00000000', '1/00000000', '0/00000000')},
        {'seq': 3, 'phase': 'vacuum', **sample(1010.0, '3/00000000', None, '1/80000000')},
        {'seq': 4, 'phase': 'replay_drain', **sample(1015.0, '4/00000000', '5/00000000', '4/00000001')},
        {'seq': 5, 'phase': 'replay_drain', **sample(1030.0, '6/00000000', '5/00000000', '5/00000000')},
        # Excluded because it is after the mapped client stop request.
        {'seq': 6, 'phase': 'replay_drain', **sample(1035.0, '7/00000000', '6/00000000', '6/00000000')},
    ]
    (RESULTS / 'rawsamples.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in raw))
    return RESULTS


class ClientReplicationTests(unittest.TestCase):
    def test_clock_parsing_requires_aware_iso_or_finite_epoch(self):
        self.assertEqual(cr.parse_epoch('1970-01-01T00:16:40Z', 't'), 1000.0)
        self.assertEqual(cr.parse_epoch(1000, 't'), 1000.0)
        for bad in ('1970-01-01T00:16:40', float('nan'), True, None):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                cr.parse_epoch(bad, 't')

    def test_lsn_conversion_and_concurrent_negative_clamp(self):
        self.assertEqual(cr.parse_lsn('1/00000010'), (1 << 32) + 16)
        self.assertEqual(cr.parse_lsn('0/00000010'), 16)
        self.assertEqual(cr.lsn_backlog_bytes(9, 10), (0, True))
        self.assertEqual(cr.lsn_backlog_bytes(10, 9), (1, False))
        for bad in (None, '', 'invalid', '100000000/0', '0/100000000'):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                cr.parse_lsn(bad)

    def test_invalid_lsn_and_long_sample_gap_are_visible_breaks(self):
        rows = [
            (1, sample(1000.0, '0/1000000', '0/0FFFFF0', '0/0FFFFF0')),
            (2, sample(1005.0, '0/2000000', None, '0/1FFFFF0')),
            (3, sample(1010.0, '0/3000000', '0/2FFFFF0', '0/2FFFFF0')),
            (4, sample(1025.1, '0/4000000', '0/3FFFFF0', '0/3FFFFF0')),
        ]
        parsed, summary = cr.parse_sample_rows(rows, 1000.0, 1030.0)
        self.assertEqual(summary['inWindowRows'], 4)
        self.assertIsNone(parsed[1]['standbyFlushMiB'])
        self.assertEqual(parsed[0]['replicaReplayGiB'], 16 / cr.GIB)
        coverage = cr.metric_coverage(parsed, 'standbyFlushMiB', 1000.0, 1030.0)
        self.assertEqual(coverage['invalidSamples'], 1)
        self.assertEqual(coverage['longSamplingGapCount'], 1)
        x, y = cr.line_points(parsed, 'standbyFlushMiB', 1000.0)
        self.assertTrue(math.isnan(y[1]))
        self.assertTrue(math.isnan(x[3]))

    def test_primary_flush_lsn_invalidates_replay_metric_and_reports_leading_coverage(self):
        rows = [
            (1, sample(1005.0, None, '0/0', '0/0')),
            (2, sample(1010.0, '0/2000000', '0/1FFFFF0', '0/1FFFFFE')),
            (3, sample(1015.0, '0/3000000', '0/2FFFFF0', '0/2FFFFFE')),
        ]
        parsed, _ = cr.parse_sample_rows(rows, 1000.0, 1020.0)
        coverage = cr.metric_coverage(parsed, 'replicaReplayGiB', 1000.0, 1020.0)
        self.assertEqual(parsed[0]['replicaReplayInvalidFields'], ['primary.flushLsn'])
        self.assertEqual(coverage['invalidFieldCounts'], {'primary.flushLsn': 1})
        self.assertEqual(coverage['firstValidSampleEpoch'], 1010.0)
        self.assertEqual(coverage['lastValidSampleEpoch'], 1015.0)
        self.assertEqual(coverage['unobservedLeadingSeconds'], 10.0)
        self.assertEqual(coverage['maximumValidSampleGapSeconds'], 5.0)

    def test_synthetic_render_and_evidence(self):
        results = fixture()
        evidence_path, png_path, evidence = cr.render(results)
        self.assertTrue(png_path.read_bytes().startswith(b'\x89PNG\r\n\x1a\n'))
        self.assertTrue(evidence['output']['allAxesLinear'])
        self.assertEqual(evidence['window']['minuteZeroEpoch'], 1000.0)
        self.assertIn('healthy warm-up start', evidence['window']['minuteZeroSource'])
        self.assertEqual(evidence['window']['clientStopRequestEpoch'], 1030.0)
        self.assertEqual(evidence['sampling']['inWindowRows'], 5)
        self.assertEqual(evidence['series']['standbyFlushBacklogMiB']['negativeConcurrentLagClampedSamples'], 1)
        self.assertEqual(evidence['series']['replicaReplayBacklogGiB']['negativeConcurrentLagClampedSamples'], 1)
        self.assertEqual(evidence['series']['standbyFlushBacklogMiB']['invalidSamples'], 1)
        self.assertEqual(evidence['series']['replicaReplayBacklogGiB']['validSamples'], 5)
        self.assertTrue(evidence_path.is_file())
        self.assertIn('does not isolate or directly measure commit-wait duration',
                      evidence['interpretation'][0])
        self.assertIn('healthy client warm-up start', evidence['interpretation'][-1])
        self.assertEqual(evidence['output']['plotTitle'], cr.PLOT_TITLE)
        self.assertIn('across warm-up, VACUUM, and recovery', evidence['output']['plotTitle'])
        self.assertEqual(evidence['output']['timeAxisLabel'], cr.TIME_AXIS_LABEL)
        self.assertIn('healthy warm-up began', evidence['output']['timeAxisLabel'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
