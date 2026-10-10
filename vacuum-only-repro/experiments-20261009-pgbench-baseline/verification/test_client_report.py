#!/usr/bin/env python3
"""Offline checks for client-report timing, validation, and output boundaries."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('client_report', ROOT/'client-report.py')
client_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(client_report)


class ClientReportTests(unittest.TestCase):
    def test_utc_requires_aware_input_and_normalizes(self):
        with self.assertRaisesRegex(ValueError, 'timezone offset'):
            client_report.epoch('2026-10-09T12:00:00')
        self.assertEqual(client_report.utc_text(client_report.epoch('2026-10-09T14:00:00+02:00')),
                         '2026-10-09T12:00:00.000000Z')

    def test_percentiles_match_native_floor_estimator(self):
        result = client_report.distribution([1, 2, 3, 4])
        self.assertEqual((result['p50'], result['p95'], result['p99']), (2, 3, 3))
        self.assertIsNone(client_report.distribution([])['p50'])

    def test_clock_alignment_uses_nearest_paired_sample(self):
        good, errors = client_report.prepare_samples([
            {'sampleStatus': 'ok', 'sampleEpoch': 100, 'observedAtUtc': '1970-01-01T00:01:42Z'},
            {'sampleStatus': 'ok', 'sampleEpoch': 104, 'observedAtUtc': '1970-01-01T00:01:46Z'}])
        self.assertFalse(errors)
        snapped = client_report.map_controller_epoch(101, good, snap=True)
        shifted = client_report.map_controller_epoch(101, good)
        self.assertEqual(snapped['dbEpoch'], 102)
        self.assertEqual(shifted['dbEpoch'], 103)
        self.assertEqual(shifted['appliedOffsetSeconds'], 2)

    def test_bad_rows_and_negative_execution_are_flagged(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            raw = root/'client/raw'
            raw.mkdir(parents=True)
            (raw/'pgbench-log.0').write_text('0 1 1000 0 100 0 100\n0 2 100 0 101 0 200\nnot a row\n')
            transactions, malformed, errors = client_report.parse_transactions(root)
            self.assertEqual(len(transactions), 1)
            self.assertEqual(len(malformed), 2)
            self.assertIn('internally inconsistent', errors[0])
            self.assertIn('exceeds latency', malformed[0]['reason'])

    def test_sampling_coverage_and_lines_expose_gaps(self):
        rows = []
        for controller, database in [(0, 0), (1, 1), (5, 5)]:
            rows.append({'_controllerEpoch': controller, '_dbEpoch': database,
                         '_observedAtUtc': client_report.utc_text(database), 'maxAge': 3})
        coverage, _ = client_report.sampling_coverage(rows, [], 0, 5)
        self.assertEqual(coverage['coveredSeconds'], 1)
        self.assertEqual(coverage['coverageFraction'], .2)
        x, y = client_report.series_with_breaks(rows, [0, 1, 5], 'maxAge')
        self.assertTrue(any(value != value for value in x))
        self.assertTrue(any(value != value for value in y))
        errors = [{'_controllerEpoch': 2}]
        x, _ = client_report.series_with_breaks(rows, [0, 1, 5], 'maxAge', errors)
        self.assertTrue(any(value != value for value in x))
        short_pair = rows[:2]
        coverage, _ = client_report.sampling_coverage(short_pair, [{'controllerEpoch': .5}], 0, 1)
        self.assertEqual(coverage['coveredSeconds'], 0)

    def test_phases_split_healthy_stop_and_drain_with_empty_phase_support(self):
        intervals = client_report.time_intervals(10, 12, 14, 16, 19, 22, 24)
        self.assertEqual([item[0] for item in intervals], [
            'pre_vacuum', 'vacuum', 'replay_catchup', 'recovery_verification',
            'post_recovery_client', 'stop_drain'])
        empty = client_report.metrics_for_interval('empty', 2, 2, [], [])
        self.assertIsNone(empty['completedTps'])
        self.assertIsNone(empty['latencyMs']['p50'])
        drain = client_report.metrics_for_interval('stop_drain', 2, 3, [], [])
        self.assertFalse(drain['clientActive'])
        self.assertIsNone(drain['completedTps'])
        self.assertEqual(client_report.build_series(4, 4, 10, []), [])

    def test_synthetic_end_to_end_respects_stop_and_drain_boundaries(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root/'client/raw').mkdir(parents=True)
            (root/'client').mkdir(exist_ok=True)
            (root/'client-config.json').write_text(json.dumps({
                'targetTps': 7.5, 'clients': 2, 'synchronousCommit': 'on'}))
            (root/'timing.json').write_text(json.dumps({
                'clientWarmup': {'startEpoch': 1000, 'endEpoch': 1005, 'seconds': 5,
                                 'transactions': 22, 'averageTps': 4.4,
                                 'healthyWindows': [{'transactions': 22, 'seconds': 5}]},
                'vacuumStart': '1970-01-01T00:16:49Z',
                'vacuumComplete': '1970-01-01T00:16:52Z',
                'clientStop': {'requestedAtUtc': '1970-01-01T00:17:08Z',
                               'serverActivityAfterDrain': {'observedAtUtc': '1970-01-01T00:17:12Z'}}}))
            (root/'client-warmup.json').write_text(json.dumps({
                'startEpoch': 1000, 'endEpoch': 1005, 'seconds': 5,
                'transactions': 22, 'averageTps': 4.4,
                'healthyWindows': [{'transactions': 22, 'seconds': 5}]}))
            events = [
                {'event': 'replica_reached_vacuum_target_with_client', 'time': '1970-01-01T00:16:54Z'},
                {'event': 'client_post_recovery_healthy_complete', 'time': '1970-01-01T00:16:57Z'}]
            (root/'events.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in events))
            samples = []
            for controller in range(1000, 1031):
                samples.append({'sampleEpoch': controller, 'observedAtUtc': client_report.utc_text(controller+2),
                                'sampleStatus': 'ok', 'maxActiveXactAgeSeconds': 0,
                                'maxActiveQueryAgeSeconds': 0, 'backends': []})
            (root/'client/sessions.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in samples))
            rows = [1006, 1008, 1011, 1013, 1016, 1018, 1031, 1033]
            (root/'client/raw/pgbench-log.0').write_text(''.join(
                f'0 {index} 1000 0 {second} 0 100\n' for index, second in enumerate(rows, 1)))
            result = subprocess.run([sys.executable, str(ROOT/'client-report.py'), str(root)],
                                    capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads((root/'client-impact.json').read_text())
            self.assertEqual(report['clockAlignment']['warmupStart']['dbEpoch'], 1002)
            self.assertEqual(report['clockAlignment']['warmupEnd']['dbEpoch'], 1007)
            self.assertEqual(report['clockAlignment']['controllerToDbOffsetSeconds']['min'], 2)
            self.assertEqual(report['healthyWarmup']['serverCommittedTransactions'], 22)
            self.assertEqual(report['healthyWarmup']['serverCommittedTps'], 4.4)
            self.assertEqual(report['healthyWarmup']['sourceFile'], 'client-warmup.json')
            self.assertEqual(report['healthyWarmup']['completedTransactions'], 1)
            self.assertEqual(report['phases']['pre_vacuum']['completedTransactions'], 2)
            self.assertAlmostEqual(report['phases']['pre_vacuum']['completedTps'], 2/7)
            self.assertEqual(report['phases']['recovery_verification']['completedTransactions'], 1)
            self.assertEqual(report['phases']['post_recovery_client']['completedTransactions'], 1)
            self.assertEqual(report['phases']['stop_drain']['completedTransactions'], 1)
            self.assertIsNone(report['phases']['stop_drain']['completedTps'])
            self.assertEqual(report['transactionsAfterDrainBoundary'], 1)
            self.assertEqual(len(report['series']), 3)
            self.assertAlmostEqual(report['series'][-1]['seconds'], 8)
            self.assertEqual(report['series'][-1]['completedTransactions'], 0)
            self.assertEqual(report['series'][-1]['completedTps'], 0)
            self.assertEqual(report['targetTps'], 7.5)
            self.assertTrue((root/'client-pending.png').is_file())


if __name__ == '__main__':
    unittest.main()
