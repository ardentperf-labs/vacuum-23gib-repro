#!/usr/bin/env python3
"""Local safety tests for the pending-PVC restore fallback."""
from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("restore-pending-disks.py")
SPEC = importlib.util.spec_from_file_location("restore_pending_disks", SCRIPT)
restore = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(restore)


class RestoreFallbackSafetyTests(unittest.TestCase):
    def setUp(self):
        self.specs = [
            {"role": "data", "claim": "hdd-pg-1", "pvName": "restore-data-test",
             "diskId": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/data",
             "vss": "source-data-test"},
            {"role": "wal", "claim": "hdd-pg-1-wal", "pvName": "restore-wal-test",
             "diskId": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/wal",
             "vss": "source-wal-test"},
        ]
        self.pvcs = [
            {"metadata": {"uid": "pvc-data", "resourceVersion": "10"},
             "spec": {"dataSource": {"name": "source-data-test"}},
             "status": {"phase": "Pending"}},
            {"metadata": {"uid": "pvc-wal", "resourceVersion": "11"},
             "spec": {"dataSource": {"name": "source-wal-test"}},
             "status": {"phase": "Pending"}},
        ]

    def test_apply_hydrates_both_disks_before_creating_or_binding_pvs(self):
        events = []
        runner = object.__new__(restore.ApplyRun)
        runner.plan = {"specs": self.specs}
        runner.receipt = {
            "ownerToken": "a" * 32,
            "cluster": {"uid": None},
            "restoreDisks": [
                {"role": x["role"], "id": x["diskId"], "pvcUid": None, "status": "intent"}
                for x in self.specs
            ],
            "staticPVs": [
                {"role": x["role"], "name": x["pvName"], "diskId": x["diskId"],
                 "pvc": f"repro/{x['claim']}", "pvcUid": None,
                 "uid": None, "status": "intent", "preexistingAbsent": None}
                for x in self.specs
            ],
        }
        runner.cluster_live = lambda: {"metadata": {"uid": "cluster-uid"}}
        runner.live_snapshots_and_pvcs = lambda cluster: self.pvcs
        runner.get_kube_object = lambda args, label: None
        runner.write_receipt = lambda: None
        runner.kubernetes_preflight = lambda: (events.append("strict-preflight") or self.pvcs)
        runner.create_or_reuse_disk = lambda spec: (events.append(f"disk:{spec['role']}") or {"id": spec["diskId"]})
        runner.kubectl_json = lambda args, label: self.pvcs[0 if args[2] == "hdd-pg-1" else 1]
        runner.create_or_validate_pv = lambda spec, pvc, disk: (
            events.append(f"pv:{spec['role']}") or {"metadata": {"name": spec["pvName"]}})
        runner.bind_pvc = lambda spec, pvc, pv: events.append(f"bind:{spec['role']}")

        with patch.object(restore, "record_event"):
            runner.apply()

        self.assertLess(max(events.index("disk:data"), events.index("disk:wal")),
                        min(events.index("pv:data"), events.index("pv:wal")))
        self.assertLess(max(events.index("pv:data"), events.index("pv:wal")),
                        min(events.index("bind:data"), events.index("bind:wal")))

    def _binding_runner(self, pvc):
        runner = object.__new__(restore.ApplyRun)
        runner.plan = {"token": "a" * 32}
        runner.env = {}
        runner.receipt = {"restoreDisks": [
            {"role": "data", "id": self.specs[0]["diskId"], "pvcUid": "pvc-data"}
        ]}
        runner.kubernetes_bind_preflight = lambda: None
        runner.kubectl_json = lambda args, label: pvc
        runner.wait_for_bound_pvc = lambda spec, uid, pv: {"waited": True, "uid": uid}
        return runner

    def test_json_patch_omits_volume_name_test_when_field_is_absent(self):
        pvc = {"metadata": {"uid": "pvc-data", "resourceVersion": "20"},
               "spec": {"dataSource": {"kind": "VolumeSnapshot", "name": "source-data-test"}},
               "status": {"phase": "Pending"}}
        runner = self._binding_runner(pvc)
        captured = {}

        def fake_command(args, **kwargs):
            captured["patch"] = json.loads(args[args.index("-p") + 1])
            return json.dumps({**pvc, "spec": {**pvc["spec"], "volumeName": "restore-data-test"}})

        pv = {"metadata": {"uid": "pv-data"}}
        with patch.object(restore, "run_command", side_effect=fake_command), \
             patch.object(restore, "save_evidence"), patch.object(restore, "record_event"):
            result = restore.ApplyRun.bind_pvc(runner, self.specs[0], pvc, pv)

        tests = [item["path"] for item in captured["patch"] if item["op"] == "test"]
        self.assertNotIn("/spec/volumeName", tests)
        self.assertIn("/metadata/uid", tests)
        self.assertIn("/metadata/resourceVersion", tests)
        self.assertIn("/status/phase", tests)
        self.assertEqual(result["uid"], "pvc-data")

    def test_existing_wrong_binding_is_rejected_without_patch(self):
        pvc = {"metadata": {"uid": "pvc-data", "resourceVersion": "20"},
               "spec": {"volumeName": "unrelated-pv",
                        "dataSource": {"kind": "VolumeSnapshot", "name": "source-data-test"}},
               "status": {"phase": "Bound"}}
        runner = self._binding_runner(pvc)
        with patch.object(restore, "run_command") as command:
            with self.assertRaises(restore.GuardError):
                restore.ApplyRun.bind_pvc(runner, self.specs[0], pvc, {"metadata": {"uid": "pv-data"}})
        command.assert_not_called()

    def test_expected_controller_bound_pv_is_accepted(self):
        pvc = {"metadata": {"uid": "pvc-data", "resourceVersion": "20"},
               "spec": {"volumeName": "restore-data-test",
                        "dataSource": {"kind": "VolumeSnapshot", "name": "source-data-test"}},
               "status": {"phase": "Bound"}}
        runner = self._binding_runner(pvc)
        with patch.object(restore, "run_command") as command:
            result = restore.ApplyRun.bind_pvc(runner, self.specs[0], pvc, {"metadata": {"uid": "pv-data"}})
        command.assert_not_called()
        self.assertTrue(result["waited"])

    def test_auto_bound_pv_uid_must_match_saved_create_intent(self):
        runner = object.__new__(restore.ApplyRun)
        spec = {**self.specs[0], "sourceSnapshotUniqueId": "snapshot-uid"}
        pvc = {"metadata": {"uid": "pvc-data"}}
        runner.receipt = {
            "staticPVs": [{"role": "data", "name": spec["pvName"], "diskId": spec["diskId"],
                           "pvc": "repro/hdd-pg-1", "pvcUid": "pvc-data", "uid": "saved-pv-uid",
                           "diskUniqueId": "disk-uid", "preexistingAbsent": True}],
            "restoreDisks": [{"role": "data", "id": spec["diskId"], "diskUniqueId": "disk-uid"}],
        }
        runner.kubectl_json = lambda args, label: {"metadata": {"uid": "different-pv-uid"}}
        runner.validate_pv = lambda *args: self.fail("A mismatched PV UID must fail before PV adoption.")
        runner.write_receipt = lambda: None

        with self.assertRaises(restore.GuardError):
            restore.ApplyRun.validate_receipted_pv(runner, spec, pvc)

    def test_empty_no_wait_output_is_saved_as_valid_json(self):
        result = json.loads(restore.command_result_evidence(["az", "disk", "create"], ""))
        self.assertEqual(result, {"command": ["az", "disk", "create"], "exitCode": 0, "stdout": ""})

    def test_evidence_digest_matches_exact_bytes_written(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(restore, "RECOVERY_DIR", Path(tmp)), \
             patch.object(restore, "record_event"):
            path = restore.save_evidence("phase", "kind", "name", "")
            contents = path.read_bytes()
        self.assertEqual(contents, b"\n")
        self.assertIn(hashlib.sha256(contents).hexdigest(), path.name)


if __name__ == "__main__":
    unittest.main()
