#!/usr/bin/env python3
"""Ensure teardown checks local WAL retention before deleting Blob storage."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("teardown.py")
SPEC = importlib.util.spec_from_file_location("wal_export_teardown", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TeardownWalExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.stack = self.base / "sigterm"
        self.stack.mkdir()
        self.state = self.stack / ".state"
        self.state.mkdir()
        self.experiment = self.base
        MODULE.ROOT = self.stack
        MODULE.SD = self.state
        self.token = "0123456789abcdef0123456789abcdef"
        self.subscription = "00000000-0000-0000-0000-000000000001"
        self.resource_group = f"rg-vacuum-only-{self.token}"
        self.account = "cnpgwal" + self.token[:17]
        self.container = "wal-" + self.token[:12]
        self.account_id = (f"/subscriptions/{self.subscription}/resourceGroups/{self.resource_group}"
                           f"/providers/Microsoft.Storage/storageAccounts/{self.account}")
        shared = SCRIPT.parent
        pair_path = self.stack / "source-pair.json"
        proof_path = self.stack / "source-pair-proof.json"
        pair_path.write_bytes((shared / "source-pair.json").read_bytes())
        proof_path.write_bytes((shared / "source-pair-proof.json").read_bytes())
        self.pair = json.loads(pair_path.read_text())
        self.pair_hash = hashlib.sha256(pair_path.read_bytes()).hexdigest()
        self.storage = {"accountId": self.account_id, "accountName": self.account,
                        "containerName": self.container}
        self.prepare_fixture()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def prepare_fixture(self) -> None:
        payload = b"wal segment payload"
        relative = "wals/0000000200000057/000000020000005700000009"
        export_root = self.state / "archived-wal" / "run"
        local = export_root / relative
        local.parent.mkdir(parents=True)
        local.write_bytes(payload)
        sha = hashlib.sha256(payload).hexdigest()
        token = self.token
        name = f"cnpg-wal-archive/{token}/hdd-pg/{relative}"
        prefix = f"cnpg-wal-archive/{token}/hdd-pg/"
        (self.state / "wal-archive-export-owner.json").write_text(json.dumps({
            "ownerToken": token, "stack": "sigterm", "purpose": "cnpg-wal-archive-export"}))
        smoke = {
            "status": "complete", "ownerToken": token, "stack": "sigterm", "restorePodRole": "replica",
            "archiveObject": name, "walName": "000000020000005700000009", "bytes": len(payload),
            "retainedLocalWALFile": "archived-wal/000000020000005700000009",
            "retainedLocalWALSha256": sha,
            "hashes": {"independentAzureDownloadSha256": sha},
            "checks": {"allThreeHashesMatch": True, "sourceSnapshotsReadOnly": True,
                       "snapshotLineagePreserved": True},
            "snapshotLineage": {"data": "verified", "wal": "verified"},
        }
        retained = self.state / smoke["retainedLocalWALFile"]
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_bytes(payload)
        (self.state / "wal-archive-smoke.json").write_text(json.dumps(smoke))
        (self.state / "wal-archive.json").write_text(json.dumps({
            "status": "ready", "ownerToken": token, "accountId": self.account_id,
            "sourceSnapshotIds": {"data": self.pair["data"]["id"], "wal": self.pair["wal"]["id"]},
            "destinationPath": f"https://{self.account}.blob.core.windows.net/{self.container}/cnpg-wal-archive/{token}/",
            "snapshotLineage": smoke["snapshotLineage"],
            "sourceLockVerification": {
                "data": {"snapshotId": self.pair["data"]["id"], "level": "CanNotDelete"},
                "wal": {"snapshotId": self.pair["wal"]["id"], "level": "CanNotDelete"},
                "resourceGroup": {"level": "CanNotDelete"},
            },
        }))
        item = {"name": name, "relativeName": relative, "localRelativePath": f"archived-wal/run/{relative}",
                "etag": '"etag"', "bytes": len(payload), "sha256": sha,
                "lastModified": "2026-10-13T00:00:00+00:00", "contentMd5Base64": None}
        public_item = {key: item[source] for key, source in (
            ("path", "relativeName"), ("bytes", "bytes"), ("sha256", "sha256"),
            ("etag", "etag"), ("lastModified", "lastModified"), ("contentMd5Base64", "contentMd5Base64"))}
        public = {"version": 1, "status": "complete", "stack": "sigterm", "serverName": "hdd-pg",
                  "sourcePairProofSha256": self.pair_hash, "objectCount": 1, "totalBytes": len(payload),
                  "objects": [public_item], "redactions": {key: True for key in
                    ("ownerToken", "accountId", "containerName", "connectionString", "accountKey")}}
        public_path = self.experiment / "results/diagnosis/public/wal-archive-export-sigterm.json"
        public_path.parent.mkdir(parents=True)
        public_path.write_text(json.dumps(public))
        private = {
            "version": 1, "status": "complete", "ownerToken": token, "stack": "sigterm",
            "subscriptionId": self.subscription, "resourceGroup": self.resource_group,
            "accountName": self.account, "accountId": self.account_id, "containerName": self.container,
            "destinationPath": f"https://{self.account}.blob.core.windows.net/{self.container}/cnpg-wal-archive/{token}/",
            "prefix": prefix, "serverName": "hdd-pg", "sourcePairSha256": self.pair_hash,
            "sourceSnapshotIds": {"data": self.pair["data"]["id"], "wal": self.pair["wal"]["id"]},
            "sourceLocksPreserved": True, "sourcePairLineage": smoke["snapshotLineage"],
            "restoreSmoke": {"walName": smoke["walName"], "archiveObject": name, "bytes": len(payload),
                             "sha256": sha, "restorePodRole": "replica"},
            "publicReceiptPath": "results/diagnosis/public/wal-archive-export-sigterm.json",
            "publicReceiptSha256": hashlib.sha256(public_path.read_bytes()).hexdigest(),
            "objectCount": 1, "totalBytes": len(payload), "objects": [item],
            "checks": {"exactTokenServerPrefixOnly": True, "sourcePairLineageVerified": True,
                       "sourceLocksVerifiedUnmodified": True, "allDownloadsEtagPinned": True,
                       "everyObjectSizeVerified": True, "everyLocalSha256Verified": True,
                       "azureListingStableAfterDownload": True, "azureMutations": 0},
        }
        (self.state / "wal-archive-export.json").write_text(json.dumps(private))

    def test_complete_export_is_accepted_and_payload_tampering_blocks_teardown(self) -> None:
        MODULE.verify_local_archive_export(self.token, "sigterm", self.subscription,
                                           self.resource_group, self.storage)
        payload = self.state / "archived-wal/run/wals/0000000200000057/000000020000005700000009"
        payload.write_bytes(b"tampered")
        with self.assertRaises(AssertionError):
            MODULE.verify_local_archive_export(self.token, "sigterm", self.subscription,
                                               self.resource_group, self.storage)

    def make_decline_receipt(self) -> None:
        smoke_path = self.state / "wal-archive-smoke.json"
        smoke = json.loads(smoke_path.read_text())
        wal_bytes = b"W" * (16 * 1024 * 1024)
        smoke["bytes"] = len(wal_bytes)
        smoke["retainedLocalWALSha256"] = hashlib.sha256(wal_bytes).hexdigest()
        smoke["hashes"]["independentAzureDownloadSha256"] = hashlib.sha256(wal_bytes).hexdigest()
        smoke_path.write_text(json.dumps(smoke))
        (self.state / smoke["retainedLocalWALFile"]).write_bytes(wal_bytes)
        receipt = {
            "version": 1,
            "status": "user-declined-full-export",
            "userInstructions": ["i dont think we need to download the WAL", "please finish cleanup"],
            "ownerToken": self.token,
            "stack": "sigterm",
            "subscriptionId": self.subscription,
            "resourceGroup": self.resource_group,
            "accountId": self.account_id,
            "accountName": self.account,
            "containerName": self.container,
            "prefix": f"cnpg-wal-archive/{self.token}/hdd-pg/",
            "sourcePairSha256": self.pair_hash,
            "localExporterStopped": True,
            "kubectlChildrenStopped": [12345],
            "postgresqlSignals": 0,
            "existingVerifiedPayloadsRetained": True,
            "fullExportComplete": False,
            "recoveryMetadataRetained": True,
        }
        (self.state / "wal-archive-export.json").unlink()
        (self.state / "wal-export-declined.json").write_text(json.dumps(receipt))

    def test_explicit_owned_decline_accepts_only_full_export_waiver(self) -> None:
        self.make_decline_receipt()
        result = MODULE.verify_declined_wal_archive_export(
            self.token, "sigterm", self.subscription, self.resource_group, self.storage)
        self.assertEqual(result["status"], "user-declined-full-export")
        self.assertFalse(result["fullArchiveDownloadRequired"])

    def test_missing_decline_receipt_is_rejected(self) -> None:
        (self.state / "wal-archive-export.json").unlink()
        with self.assertRaises(AssertionError):
            MODULE.verify_local_archive_export(self.token, "sigterm", self.subscription,
                                               self.resource_group, self.storage)

    def test_foreign_or_mismatched_decline_receipt_is_rejected(self) -> None:
        self.make_decline_receipt()
        path = self.state / "wal-export-declined.json"
        original = json.loads(path.read_text())
        for field, bad_value in (("ownerToken", "f" * 32), ("subscriptionId", "foreign-sub"),
                                 ("accountId", "foreign-account"), ("sourcePairSha256", "0" * 64),
                                 ("status", "complete"), ("userInstructions", ["unrelated text"])):
            with self.subTest(field=field):
                changed = dict(original)
                changed[field] = bad_value
                path.write_text(json.dumps(changed))
                with self.assertRaises(AssertionError):
                    MODULE.verify_local_archive_export(self.token, "sigterm", self.subscription,
                                                       self.resource_group, self.storage)
        path.write_text(json.dumps(original))


if __name__ == "__main__":
    unittest.main()
