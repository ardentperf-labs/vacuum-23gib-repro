#!/usr/bin/env python3
"""Focused offline checks for the optional tracepoint BCC fallback."""
from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
INSTRUMENTATION = HERE / "instrumentation"
sys.path.insert(0, str(INSTRUMENTATION))
spec = importlib.util.spec_from_file_location("wal_repro_collect", INSTRUMENTATION / "collect.py")
assert spec and spec.loader
collect = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = collect
spec.loader.exec_module(collect)


class TracepointFallbackTests(unittest.TestCase):
    def test_log_resume_uses_overlap_and_exact_line_dedup_key(self) -> None:
        raw, timestamp = collect.parse_kubectl_log_timestamp(
            "2026-10-13T02:22:48.123456789Z __BCC_RW_TP__{\"event\": \"interval\"}")
        self.assertEqual(raw, "2026-10-13T02:22:48.123456789Z")
        self.assertIsNotNone(timestamp)
        since = collect.bcc_stream_resume_since(timestamp)
        self.assertEqual(since, "2026-10-13T02:22:46.123456Z")
        one = collect.bcc_log_line_fingerprint(raw, "same histogram row")
        replay = collect.bcc_log_line_fingerprint(raw, "same histogram row")
        next_period = collect.bcc_log_line_fingerprint("2026-10-13T02:22:49Z", "same histogram row")
        self.assertEqual(one, replay)
        self.assertNotEqual(one, next_period)
        self.assertIsNone(collect.bcc_stream_resume_since(None))

    def test_pod_embeds_optional_helper_and_shell_is_valid(self) -> None:
        pod = collect.build_bcc_pod("bcc-test", "repro", "node-a", "ubuntu:24.04", "sdc", "sdb")
        command = pod["spec"]["containers"][0]["args"][0]
        encoded = command.split("printf '%s' '", 1)[1].split("' | base64 -d", 1)[0]
        embedded = base64.b64decode(encoded).decode("utf-8")
        self.assertIn("block_io_start", embedded)
        self.assertIn("keyCollisions", embedded)
        self.assertIn("__BCC_RW_TP_UNAVAILABLE__", command)
        self.assertIn("continuing with tracepoint fallback", command)
        self.assertIn("delayed -F process exit; starting tracepoint fallback", command)
        self.assertIn("while :; do", command)
        self.assertIn("proc_running", command)
        self.assertIn("flock -n -F /tmp/bcc-rw-tracepoint-live.lock", command)
        self.assertIn("/tmp/bcc-rw-tracepoint-live.pid", command)
        import subprocess
        checked = subprocess.run(["/bin/sh", "-n"], input=command, text=True,
                                 capture_output=True, check=False)
        self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_ready_and_interval_are_saved_with_pair_counters(self) -> None:
        target = collect.Target("primary", {
            "pod": "pg-primary", "node": "node-a", "vmResourceId": "/subscriptions/x/vm/a"
        })
        with tempfile.TemporaryDirectory() as out:
            collector = collect.Collector.__new__(collect.Collector)
            collector.out = Path(out)
            collector.handle_tracepoint_bcc_line(target,
                '__BCC_RW_TP__{"event":"ready","pairingKey":["dev","sector"]}')
            collector.handle_tracepoint_bcc_line(target,
                '__BCC_RW_TP__{"event":"interval","capturedAtUtc":"2026-10-13T02:22:46Z",'
                '"intervalSeconds":1,"unit":"microseconds",'
                '"diagnosticPairCounters":{"starts":7,"completions":6,"matchedCompletions":5,'
                '"keyCollisions":1,"unmatchedCompletions":1,"mapUpdateFailures":0},'
                '"histograms":[{"diskKind":"data","device":"sdc","operation":"read",'
                '"log2Microseconds":12,"count":5}]}')

            self.assertTrue(target.bcc_tracepoint_ready)
            self.assertEqual(target.bcc_samples["rwTracepointIntervals"], 1)
            self.assertEqual(target.bcc_samples["rwTracepointCompletions"], 5)
            self.assertEqual(target.bcc_tracepoint_pair_counters["keyCollisions"], 1)
            intervals = (Path(out) / "bcc-rw-tracepoint-intervals.jsonl").read_text().splitlines()
            rows = (Path(out) / "bcc-read-write-histograms.jsonl").read_text().splitlines()
            self.assertEqual(json.loads(intervals[0])["diagnosticPairCounters"]["unmatchedCompletions"], 1)
            bucket = json.loads(rows[0])
            self.assertEqual(bucket["source"], "block-tracepoint-fallback")
            self.assertEqual(bucket["histogram"], {"log2Microseconds": 12, "count": 5})
            self.assertEqual(bucket["flags"], 0)

    def test_helper_failure_is_a_gap_not_a_collector_fatal(self) -> None:
        target = collect.Target("replica", {
            "pod": "pg-replica", "node": "node-b", "vmResourceId": "/subscriptions/x/vm/b"
        })
        with tempfile.TemporaryDirectory() as out:
            collector = collect.Collector.__new__(collect.Collector)
            collector.out = Path(out)
            collector.handle_tracepoint_bcc_line(
                target, "__BCC_RW_TP____BCC_RW_TP_UNAVAILABLE__ kernel tracepoint unavailable")
            self.assertEqual(target.bcc_tracepoint_error, "kernel tracepoint unavailable")
            self.assertTrue(target.bcc_gaps)
            self.assertEqual(target.bcc_samples["rwTracepointIntervals"], 0)
            self.assertTrue((Path(out) / "replica-bcc-errors.jsonl").is_file())


if __name__ == "__main__":
    unittest.main()
