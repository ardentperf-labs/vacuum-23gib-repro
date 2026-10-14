#!/usr/bin/env python3
"""Offline ownership and manifest checks for staged WAL recovery."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("recover-staged-wal-bundles.py")
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("recover_staged_wal_bundles", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
EXPORTER = MODULE.EXPORTER


class RecoverStagedWALBundlesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.token = "0123456789abcdef0123456789abcdef"
        self.relative = "wals/0000000200000057/000000020000005700000009"
        self.name = f"cnpg-wal-archive/{self.token}/hdd-pg/{self.relative}"
        self.payload = b"verified WAL bytes"
        self.etag = '"0x8DF123456789"'
        self.row = {"name": self.name, "relativeName": self.relative,
                    "etag": self.etag, "bytes": len(self.payload),
                    "sha256": hashlib.sha256(self.payload).hexdigest()}
        self.ctx = {"ownerToken": self.token, "stack": "sigterm"}
        self.inventory = {self.name: {"name": self.name, "relativeName": self.relative,
            "etag": "0x8DF123456789", "bytes": len(self.payload),
            "lastModified": "2026-10-13T00:00:00+00:00"}}
        self.manifest = {"version": 1, "ownerToken": self.token,
                         "stack": "sigterm", "files": [self.row]}

    def test_fresh_inventory_and_semantically_equal_etag_are_accepted(self) -> None:
        items = MODULE.validate_manifest_rows(self.manifest, self.ctx, self.inventory)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["etag"], self.etag)
        self.assertEqual(items[0]["lastModified"], self.inventory[self.name]["lastModified"])

    def test_changed_etag_or_size_is_rejected_against_fresh_inventory(self) -> None:
        for field, value in (("etag", '"changed"'), ("bytes", len(self.payload) + 1)):
            with self.subTest(field=field):
                changed = dict(self.row)
                changed[field] = value
                manifest = {**self.manifest, "files": [changed]}
                with self.assertRaises(EXPORTER.Refusal):
                    MODULE.validate_manifest_rows(manifest, self.ctx, self.inventory)

    def test_foreign_stack_token_and_unowned_stage_ids_are_rejected(self) -> None:
        foreign = {**self.manifest, "ownerToken": "f" * 32}
        with self.assertRaises(EXPORTER.Refusal):
            MODULE.validate_manifest_rows(foreign, self.ctx, self.inventory)
        with self.assertRaises(EXPORTER.Refusal):
            MODULE.validate_stage_ids({"stageIds": ["../not-owned"]})
        with self.assertRaises(EXPORTER.Refusal):
            MODULE.validate_stage_ids({"stageIds": ["a" * 32, "a" * 32]})

    def test_skip_verified_requires_same_original_hash_and_preserves_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_dir = root / "run"
            progress_dir = root / "progress"
            state_dir = root / "state"
            target = EXPORTER.local_file_for(run_dir, self.relative, create_parents=True)
            target.write_bytes(self.payload)
            ctx = {**self.ctx, "runDir": run_dir, "progressDir": progress_dir,
                   "stateDir": state_dir}
            item = {**self.inventory[self.name], "etag": self.etag}
            progress = {"version": 1, "ownerToken": self.token, "stack": "sigterm",
                "name": self.name, "etag": self.etag, "bytes": len(self.payload),
                "sha256": hashlib.sha256(self.payload).hexdigest(),
                "lastModified": item["lastModified"]}
            EXPORTER.atomic_json(EXPORTER.progress_path(ctx, item), progress)
            bundle_path = root / "bundle.tar.gz"
            manifest_bytes = json.dumps(self.manifest, sort_keys=True, separators=(",", ":")).encode()
            with tarfile.open(bundle_path, "w:gz") as archive:
                data_info = tarfile.TarInfo(self.relative)
                data_info.size = len(self.payload)
                archive.addfile(data_info, io.BytesIO(self.payload))
                manifest_info = tarfile.TarInfo("_wal-export-manifest.json")
                manifest_info.size = len(manifest_bytes)
                archive.addfile(manifest_info, io.BytesIO(manifest_bytes))
            records = EXPORTER.extract_verified_remote_bundle(
                bundle_path, ctx, [item],
                {"ownerToken": self.token, "stack": "sigterm", "files": [self.row]},
                run_dir, save_progress=True, skip_verified=True,
            )
            self.assertEqual(records, [progress])
            self.assertEqual(target.read_bytes(), self.payload)


if __name__ == "__main__":
    unittest.main()
