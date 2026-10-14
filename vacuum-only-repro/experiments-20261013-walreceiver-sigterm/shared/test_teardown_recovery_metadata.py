#!/usr/bin/env python3
"""Teardown must retain every manifested recovery file and timeline history."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("teardown.py")
SPEC = importlib.util.spec_from_file_location("recovery_metadata_teardown", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TeardownRecoveryMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.stack = self.base / "sigterm"
        self.stack.mkdir()
        self.sd = self.stack / ".state"
        self.sd.mkdir()
        MODULE.ROOT, MODULE.SD = self.stack, self.sd
        self.token = "0123456789abcdef0123456789abcdef"
        self.sub = "00000000-0000-0000-0000-000000000001"
        self.rg = f"rg-vacuum-only-{self.token}"
        pair_path = self.stack / "source-pair.json"
        pair_path.write_bytes((SCRIPT.parent / "source-pair.json").read_bytes())
        (self.stack / "source-pair-proof.json").write_bytes((SCRIPT.parent / "source-pair-proof.json").read_bytes())
        self.pair = json.loads(pair_path.read_text())
        self.pair_hash = hashlib.sha256(pair_path.read_bytes()).hexdigest()
        self.make_receipts(include_history=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_receipts(self, include_history: bool) -> None:
        root = self.sd / "recovery-metadata"
        if root.exists():
            shutil.rmtree(root)
        root.mkdir()
        system_id = self.pair["fixture"]["systemIdentifier"]
        timeline = 2
        roles = []
        for role in ("primary", "replica"):
            role_root = root / role
            role_root.mkdir()
            entries = []
            contents = {"pg_controldata.txt": f"role={role}\n".encode(), "metadata.json": b"{}\n"}
            if role == "primary" and include_history:
                contents["history/00000002.history"] = b"1\t0/100\tpromotion\n"
            for relative, raw in contents.items():
                path = role_root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(raw)
                entries.append({"path": relative, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
            roles.append({"role": role, "systemIdentifier": system_id, "timelineHex": "00000002",
                          "isInRecovery": role == "replica", "appClientSessions": 0,
                          "vacuumClientSessions": 0, "files": entries})
        private = {"version": 1, "status": "complete", "ownerToken": self.token, "stack": "sigterm",
                   "subscriptionId": self.sub, "resourceGroup": self.rg,
                   "sourcePairProofSha256": self.pair_hash,
                   "sourceSnapshotIds": {"data": self.pair["data"]["id"], "wal": self.pair["wal"]["id"]},
                   "systemIdentifier": system_id, "timeline": timeline, "historyFileRequired": True,
                   "checks": {"primaryReplicaSystemIdentifiersMatch": True,
                              "sourcePairSystemIdentifierMatches": True, "primaryNotInRecovery": True,
                              "replicaInRecovery": True,
                              "clientAndVacuumStopped": True,
                              "matchingTimelineHistoryRetained": include_history,
                              "allRawFilesPrivate": True}, "roles": roles}
        private_path = root / "receipt.json"
        private_path.write_text(json.dumps(private))
        public = {"version": 1, "status": "complete", "stack": "sigterm",
                  "systemIdentifier": system_id, "timeline": timeline,
                  "sourcePairProofSha256": self.pair_hash,
                  "redactions": {name: True for name in
                    ("ownerToken", "subscriptionId", "resourceGroup", "sourceSnapshotIds", "podUids", "privatePaths")}}
        public_path = self.base / "results/diagnosis/public/recovery-metadata-sigterm.json"
        public_path.parent.mkdir(parents=True, exist_ok=True)
        public_path.write_text(json.dumps(public))
        top = {"version": 1, "status": "complete", "ownerToken": self.token, "stack": "sigterm",
               "subscriptionId": self.sub, "resourceGroup": self.rg,
               "sourcePairProofSha256": self.pair_hash,
               "privateDirectory": ".state/recovery-metadata", "privateReceipt": ".state/recovery-metadata/receipt.json",
               "privateReceiptSha256": hashlib.sha256(private_path.read_bytes()).hexdigest(),
               "publicReceiptPath": "results/diagnosis/public/recovery-metadata-sigterm.json",
               "publicReceiptSha256": hashlib.sha256(public_path.read_bytes()).hexdigest(),
               "systemIdentifier": system_id, "timeline": timeline, "historyFileRequired": True,
               "historyFilePresent": include_history}
        (self.sd / "recovery-metadata.json").write_text(json.dumps(top))

    def test_valid_timeline_history_and_file_hashes_pass(self) -> None:
        MODULE.verify_local_recovery_metadata(self.token, "sigterm", self.sub, self.rg)

    def test_missing_timeline_history_blocks_teardown(self) -> None:
        self.make_receipts(include_history=False)
        with self.assertRaises(AssertionError):
            MODULE.verify_local_recovery_metadata(self.token, "sigterm", self.sub, self.rg)

    def test_writer_shaped_top_receipt_uses_hashed_private_scope(self) -> None:
        self.make_receipts(include_history=True)
        top_path = self.sd / "recovery-metadata.json"
        top = json.loads(top_path.read_text())
        top.pop("subscriptionId")
        top.pop("resourceGroup")
        top_path.write_text(json.dumps(top))
        MODULE.verify_local_recovery_metadata(self.token, "sigterm", self.sub, self.rg)

    def test_private_scope_mismatch_and_private_hash_tampering_still_fail(self) -> None:
        self.make_receipts(include_history=True)
        top_path = self.sd / "recovery-metadata.json"
        private_path = self.sd / "recovery-metadata" / "receipt.json"
        top = json.loads(top_path.read_text())
        top.pop("subscriptionId")
        top.pop("resourceGroup")
        private = json.loads(private_path.read_text())
        private["subscriptionId"] = "foreign-subscription"
        private_path.write_text(json.dumps(private))
        top["privateReceiptSha256"] = hashlib.sha256(private_path.read_bytes()).hexdigest()
        top_path.write_text(json.dumps(top))
        with self.assertRaises(AssertionError):
            MODULE.verify_local_recovery_metadata(self.token, "sigterm", self.sub, self.rg)

        self.make_receipts(include_history=True)
        top_path = self.sd / "recovery-metadata.json"
        private_path = self.sd / "recovery-metadata" / "receipt.json"
        top = json.loads(top_path.read_text())
        top.pop("subscriptionId")
        top.pop("resourceGroup")
        private = json.loads(private_path.read_text())
        private["resourceGroup"] = "foreign-resource-group"
        private_path.write_text(json.dumps(private))
        top_path.write_text(json.dumps(top))
        with self.assertRaises(AssertionError):
            MODULE.verify_local_recovery_metadata(self.token, "sigterm", self.sub, self.rg)


if __name__ == "__main__":
    unittest.main()
