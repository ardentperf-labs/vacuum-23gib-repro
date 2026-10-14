#!/usr/bin/env python3
"""Offline tests for direct-restore source-lineage checks."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("direct-restore-disks.py")
PREPARE_SCRIPT = Path(__file__).with_name("02-prepare-direct.sh")
spec = importlib.util.spec_from_file_location("direct_restore_disks", SCRIPT)
assert spec is not None and spec.loader is not None
direct_restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(direct_restore)


TOKEN = "1" * 32
SNAPSHOT_ID = "/subscriptions/sub/resourceGroups/retained/providers/Microsoft.Compute/snapshots/data"
SNAPSHOT_NAME = "source-data-11111111"
CONTENT_NAME = "source-data-content-11111111"
SNAPSHOT_UID = "volume-snapshot-uid"


def objects(*, ready: bool) -> tuple[dict, dict]:
    snapshot = {
        "metadata": {"labels": {"repro.stack.token": TOKEN}, "uid": SNAPSHOT_UID},
        "spec": {"source": {"volumeSnapshotContentName": CONTENT_NAME}},
        "status": {
            "readyToUse": ready,
            "boundVolumeSnapshotContentName": CONTENT_NAME if ready else None,
        },
    }
    content = {
        "metadata": {"labels": {"repro.stack.token": TOKEN}, "uid": "content-uid"},
        "spec": {
            "driver": "disk.csi.azure.com",
            "deletionPolicy": "Retain",
            "source": {"snapshotHandle": SNAPSHOT_ID},
            "volumeSnapshotRef": {
                "name": SNAPSHOT_NAME,
                "namespace": "repro",
                "uid": SNAPSHOT_UID,
            },
        },
        "status": {"readyToUse": ready, "snapshotHandle": SNAPSHOT_ID if ready else None},
    }
    return snapshot, content


class SnapshotLineageTests(unittest.TestCase):
    def check(self, snapshot: dict, content: dict, *, static: bool) -> dict:
        return direct_restore.verify_snapshot_lineage(
            token=TOKEN,
            kind="data",
            source_snapshot_id=SNAPSHOT_ID,
            snapshot_name=SNAPSHOT_NAME,
            content_name=CONTENT_NAME,
            snapshot=snapshot,
            content=content,
            static_direct_restore=static,
        )

    def test_static_copy_accepts_unready_status_when_spec_lineage_matches(self) -> None:
        snapshot, content = objects(ready=False)
        result = self.check(snapshot, content, static=True)
        self.assertEqual(result["snapshotUid"], SNAPSHOT_UID)
        self.assertEqual(result["snapshotHandle"], SNAPSHOT_ID)

    def test_normal_csi_path_still_requires_ready_status(self) -> None:
        snapshot, content = objects(ready=False)
        with self.assertRaises(direct_restore.Refusal):
            self.check(snapshot, content, static=False)

    def test_static_copy_rejects_wrong_snapshot_id(self) -> None:
        snapshot, content = objects(ready=False)
        content["spec"]["source"]["snapshotHandle"] = SNAPSHOT_ID + "-other"
        with self.assertRaises(direct_restore.Refusal):
            self.check(snapshot, content, static=True)

    def test_static_copy_rejects_wrong_snapshot_uid(self) -> None:
        snapshot, content = objects(ready=False)
        content["spec"]["volumeSnapshotRef"]["uid"] = "another-snapshot-uid"
        with self.assertRaises(direct_restore.Refusal):
            self.check(snapshot, content, static=True)

    def test_zoneless_direct_disk_does_not_need_a_scheduled_pod(self) -> None:
        ctx = {
            "token": TOKEN,
            "pairSha": "pair-hash",
            "pair": {"data": {"id": SNAPSHOT_ID, "uniqueId": "snapshot-uid"}},
        }
        k8s = {
            "pvcs": {"data": {"name": "hdd-pg-1", "uid": "pvc-uid", "selectedNode": None}},
            "nodeByName": {},
        }
        disk = {
            "id": "/subscriptions/sub/resourceGroups/owned/providers/Microsoft.Compute/disks/restore-data",
            "zones": [],
        }
        pv = direct_restore.pv_object(ctx, "data", disk, k8s)
        self.assertNotIn("nodeAffinity", pv["spec"])
        self.assertEqual(pv["spec"]["csi"]["volumeAttributes"]["cachingMode"], "None")

    def test_direct_prepare_has_no_source_permission_lock_or_snapshot_writes(self) -> None:
        script = PREPARE_SCRIPT.read_text()
        for forbidden in (
            "az role assignment create",
            "az role assignment delete",
            "az lock create",
            "az lock delete",
            "az snapshot create",
            "az snapshot delete",
        ):
            self.assertNotIn(forbidden, script)
        self.assertIn("--create-only --apply", script)
        self.assertIn("--bind-only --static-direct-restore --apply", script)


if __name__ == "__main__":
    unittest.main()
