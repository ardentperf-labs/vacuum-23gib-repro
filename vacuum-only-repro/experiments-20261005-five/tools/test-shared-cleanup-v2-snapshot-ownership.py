#!/usr/bin/env python3
"""Focused offline tests for shared-cleanup snapshot ownership verification."""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("finish-shared-cleanup-v2.py")
SPEC = importlib.util.spec_from_file_location("shared_cleanup_v2", SCRIPT)
assert SPEC and SPEC.loader
shared = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shared)


SUB = "00000000-0000-0000-0000-000000000000"
TOKEN = "a" * 32
RG = f"rg-vacuum-nodes-{TOKEN}"
SID = f"/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.Compute/snapshots/csi-1"
DISK = f"/subscriptions/{SUB}/resourceGroups/{RG}/providers/Microsoft.Compute/disks/pvc-1"
UID = "3a9c85c7-1688-4215-964f-731649064d6f"
GROUP = {"name": RG, "ownerToken": TOKEN}


def ledger_row(**overrides):
    row = {
        "id": SID,
        "uniqueId": UID,
        "resourceGroup": RG,
        "provisioningState": "Succeeded",
        "creationData": {"sourceResourceId": DISK},
        "tags": {"k8s-azure-created-by": "kubernetes-azure-dd", "source_volume_id": DISK},
    }
    row.update(overrides)
    return row


def live_snapshot(**overrides):
    snapshot = ledger_row()
    snapshot.update(overrides)
    return snapshot


class SnapshotOwnershipTests(unittest.TestCase):
    def expectations(self, row=None):
        with tempfile.TemporaryDirectory() as temporary:
            stack = Path(temporary)
            ledger = stack / "cleanup/snapshots-after.json"
            ledger.parent.mkdir(parents=True)
            ledger.write_text(json.dumps([row or ledger_row()]))
            states = [{"_stackPath": str(stack)}]
            groups = {RG.lower(): {"name": RG, "ownerToken": TOKEN}}
            return shared.snapshot_ledger_expectations(states, groups)

    def test_accepts_tokenless_native_csi_snapshot_with_exact_ledger_identity(self):
        proof = self.expectations()[SID.lower()]
        shared.verify_snapshot_in_owned_group(live_snapshot(), SID, GROUP, proof)

    def test_keeps_tokened_snapshot_path_as_control(self):
        snapshot = live_snapshot(tags={"reprotoken": TOKEN})
        shared.verify_snapshot_in_owned_group(snapshot, SID, GROUP, None)

    def test_rejects_unknown_tokenless_snapshot(self):
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(live_snapshot(), SID, GROUP, None)

    def test_rejects_unique_id_change(self):
        proof = self.expectations()[SID.lower()]
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(live_snapshot(uniqueId="changed"), SID, GROUP, proof)

    def test_rejects_source_disk_mismatch(self):
        proof = self.expectations()[SID.lower()]
        other_disk = DISK + "-other"
        snapshot = live_snapshot(
            creationData={"sourceResourceId": other_disk},
            tags={"k8s-azure-created-by": "kubernetes-azure-dd", "source_volume_id": other_disk},
        )
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(snapshot, SID, GROUP, proof)

    def test_rejects_wrong_csi_creator(self):
        proof = self.expectations()[SID.lower()]
        snapshot = live_snapshot(tags={"k8s-azure-created-by": "someone-else", "source_volume_id": DISK})
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(snapshot, SID, GROUP, proof)

    def test_rejects_conflicting_present_token_instead_of_using_ledger_fallback(self):
        proof = self.expectations()[SID.lower()]
        snapshot = live_snapshot(tags={"ReproToken": "b" * 32,
                                       "k8s-azure-created-by": "kubernetes-azure-dd",
                                       "source_volume_id": DISK})
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(snapshot, SID, GROUP, proof)

    def test_rejects_wrong_resource_group(self):
        proof = self.expectations()[SID.lower()]
        snapshot = live_snapshot(resourceGroup="rg-other")
        with self.assertRaises(shared.GateError):
            shared.verify_snapshot_in_owned_group(snapshot, SID, GROUP, proof)

    def test_rejects_ledger_id_resource_group_disagreement(self):
        with self.assertRaises(shared.GateError):
            self.expectations(ledger_row(resourceGroup="rg-other"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
