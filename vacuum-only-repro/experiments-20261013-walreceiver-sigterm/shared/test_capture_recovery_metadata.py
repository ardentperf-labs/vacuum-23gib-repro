#!/usr/bin/env python3
"""Offline safety checks for recovery metadata collection."""

from __future__ import annotations

import importlib.util
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("capture-recovery-metadata.py")
SPEC = importlib.util.spec_from_file_location("capture_recovery_metadata", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CaptureRecoveryMetadataTests(unittest.TestCase):
    def test_standby_filename_boundaries_match_postgresql(self) -> None:
        cases = [("0/1000000", "16MB", "000000020000000000000000"),
                 ("0/1000001", "16MB", "000000020000000000000001"),
                 ("1/0", "16MB", "0000000200000000000000FF"),
                 ("1/1", "16MB", "000000020000000100000000"),
                 ("1/0", "64MB", "00000002000000000000003F")]
        for lsn, size, expected in cases:
            with self.subTest(lsn=lsn, size=size):
                self.assertEqual(MODULE.wal_filename(2, lsn, size), expected)

    def test_standby_uses_received_timeline_or_paired_replay_fallback(self) -> None:
        import json
        import subprocess
        value = {"systemIdentifier": "123", "dataDirectory": "/pgdata", "isInRecovery": True,
                 "appClientSessions": 0, "vacuumClientSessions": 0, "checkpointTimeline": 2,
                 "walReceiverTimeline": 3, "receiveLsn": "1/1", "replayLsn": "0/1000001",
                 "walSegmentSize": "16MB"}
        def snapshot():
            result = subprocess.CompletedProcess([], 0, json.dumps(value).encode())
            with patch.object(MODULE, "kubectl", return_value=result):
                return MODULE.sql_snapshot(["kubectl"], "hdd-pg-2")
        self.assertEqual(snapshot()["currentWalFile"], "000000030000000100000000")
        value["walReceiverTimeline"] = None
        self.assertEqual(snapshot()["currentWalFile"], "000000020000000000000001")

    def test_private_paths_reject_traversal_and_empty_components(self) -> None:
        self.assertEqual(MODULE.safe_relative("history/00000002.history"), Path("history/00000002.history"))
        for path in ("../outside", "/absolute", "history//file", "history/./file", "history\\file"):
            with self.subTest(path=path), self.assertRaises(MODULE.Refusal):
                MODULE.safe_relative(path)

    def test_wal_size_has_a_strict_upper_bound(self) -> None:
        self.assertEqual(MODULE.wal_segment_size_bytes("16MB"), 16 * 1024 * 1024)
        self.assertEqual(MODULE.wal_segment_size_bytes("32MB"), 32 * 1024 * 1024)
        with self.assertRaises(MODULE.Refusal):
            MODULE.wal_segment_size_bytes("1GB")

    def test_large_wal_read_retries_short_chunk_and_verifies_full_segment_hash(self) -> None:
        chunk_size = MODULE.WAL_COPY_CHUNK_BYTES
        chunk = b"\0" * chunk_size
        segment = chunk * 8
        digest = hashlib.sha256(segment).hexdigest()
        calls = []

        def fake_kubectl(base, args, *, timeout=180, input_bytes=None, check=True):
            index = int(args[-1])
            calls.append(index)
            if index == 0 and calls.count(0) == 1:
                return SimpleNamespace(returncode=0, stdout=b"short")
            start = index * chunk_size
            return SimpleNamespace(returncode=0, stdout=segment[start:start + chunk_size])

        with patch.object(MODULE, "remote_wal_fingerprint", side_effect=[(len(segment), digest),
                                                                          (len(segment), digest)]) as fingerprint, \
                patch.object(MODULE, "kubectl", side_effect=fake_kubectl):
            captured = MODULE.remote_read_wal_segment(
                ["kubectl"], "hdd-pg-2", "/pgdata", "000000020000005700000009", len(segment))

        self.assertEqual(captured, segment)
        self.assertEqual(calls, [0, 0, 1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(fingerprint.call_count, 2)

    def test_large_wal_read_stops_after_bounded_chunk_failures(self) -> None:
        expected_size = 16 * 1024 * 1024
        digest = hashlib.sha256(b"\0" * expected_size).hexdigest()
        with patch.object(MODULE, "remote_wal_fingerprint", return_value=(expected_size, digest)), \
                patch.object(MODULE, "kubectl", return_value=SimpleNamespace(returncode=0, stdout=b"short")) as kubectl:
            with self.assertRaisesRegex(MODULE.Refusal, "chunk 1/8 after bounded retries"):
                MODULE.remote_read_wal_segment(
                    ["kubectl"], "hdd-pg-2", "/pgdata", "000000020000005700000009", expected_size)
        self.assertEqual(kubectl.call_count, MODULE.WAL_COPY_CHUNK_ATTEMPTS)

    def test_large_wal_read_rejects_remote_or_local_hash_change(self) -> None:
        expected_size = 16 * 1024 * 1024
        chunk = b"\0" * MODULE.WAL_COPY_CHUNK_BYTES
        segment = chunk * 8
        digest = hashlib.sha256(segment).hexdigest()
        changed_digest = "f" * 64

        def fake_kubectl(base, args, *, timeout=180, input_bytes=None, check=True):
            index = int(args[-1])
            start = index * MODULE.WAL_COPY_CHUNK_BYTES
            return SimpleNamespace(returncode=0, stdout=segment[start:start + MODULE.WAL_COPY_CHUNK_BYTES])

        with patch.object(MODULE, "remote_wal_fingerprint", side_effect=[(expected_size, digest),
                                                                          (expected_size, changed_digest)]), \
                patch.object(MODULE, "kubectl", side_effect=fake_kubectl):
            with self.assertRaisesRegex(MODULE.Refusal, "changed during chunked capture"):
                MODULE.remote_read_wal_segment(
                    ["kubectl"], "hdd-pg-2", "/pgdata", "000000020000005700000009", expected_size)

    def test_unstable_or_missing_wal_segment_is_bounded_and_never_written(self) -> None:
        before = {"systemIdentifier": "123", "currentFlushLsn": "0/100",
                  "receiveLsn": None, "replayLsn": None, "currentWalFile": "000000020000000000000001",
                  "lastWalSegment": "000000020000000000000000", "dataDirectory": "/pgdata",
                  "walSegmentSize": "16MB"}
        with tempfile.TemporaryDirectory() as temporary:
            role_root = Path(temporary) / "primary"
            role_root.mkdir()
            with patch.object(MODULE, "sql_snapshot", return_value=before.copy()) as sql, \
                 patch.object(MODULE, "remote_read_wal_segment", return_value=None) as read:
                result = MODULE.capture_stable_wal(["kubectl"], "hdd-pg-1", role_root, "primary", before,
                                                   max_attempts=3)
            self.assertEqual(result["status"], "not-captured")
            self.assertEqual(len(result["attempts"]), 3)
            self.assertEqual(sql.call_count, 6)
            self.assertEqual(read.call_count, 3)
            self.assertEqual(list(role_root.iterdir()), [])

    def test_public_manifest_excludes_owner_and_snapshot_ids(self) -> None:
        token = "0123456789abcdef0123456789abcdef"
        role = {"role": "primary", "systemIdentifier": "123456789", "checkpointTimeline": 2,
                "timelineHex": "00000002", "isInRecovery": False, "currentFlushLsn": "0/200",
                "appClientSessions": 0, "vacuumClientSessions": 0,
                "receiveLsn": None, "replayLsn": None, "checkpointLsn": "0/100",
                "pgControlBinary": {"path": "global/pg_control", "status": "present", "bytes": 8192,
                                    "sha256": "d" * 64},
                "files": [{"path": "history/00000002.history", "status": "present", "bytes": 4,
                           "sha256": "a" * 64}],
                "optionalRecoveryFiles": {"backup_label": {"status": "absent", "bytes": 0, "sha256": None}},
                "stableWalCapture": {"status": "captured", "file": {"path": "wal/segment", "status": "present",
                                     "bytes": 16, "sha256": "b" * 64}}}
        public = MODULE.safe_public({"stack": "sigterm", "pairSha256": "c" * 64, "token": token}, [role], 2)
        serialized = str(public)
        self.assertNotIn(token, serialized)
        self.assertNotIn("/subscriptions/", serialized)
        self.assertEqual(public["timeline"], 2)
        self.assertEqual(public["roles"][0]["files"][0]["sha256"], "a" * 64)
        self.assertTrue(public["redactions"]["sourceSnapshotIds"])


if __name__ == "__main__":
    unittest.main()
