from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
LINEAGE_DIR = ROOT / "recovery/fixture-watch/100-backup-lineage-20261007T071940Z"
LINEAGE_MANIFEST_SHA256 = "52d35a2ce77cbc03f4e6b6ac4aadd781e015d3f70e43bd99324fe7aa5ab9b0d2"
FROZEN_RESUMER = HERE / "resume_after_backup.py"
V2_RESUMER = HERE / "resume_after_backup_v2.py"
FROZEN_ADAPTER = HERE / "finalize_continuation.py"
V2_ADAPTER = HERE / "finalize_continuation_v2.py"
FROZEN_RESUMER_SHA256 = "9278b938a3adb8cb5f18c4d2a65f8aff2792f5c4029e77902d3af6b170de56a2"
FROZEN_ADAPTER_SHA256 = "873da9c7c32ef6980d4bf2856109fbd7ef53706d624fff634bffd735615af280"
STACK_TOKEN = "f1b90aec9e24f6a2008d6991738742cf"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path.name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_captured_lineage():
    manifest_bytes = (LINEAGE_DIR / "manifest.json").read_bytes()
    if sha(manifest_bytes) != LINEAGE_MANIFEST_SHA256:
        raise AssertionError("captured public-lineage manifest changed")
    manifest = json.loads(manifest_bytes)
    for name, expected_hash in manifest["filesSha256"].items():
        if sha((LINEAGE_DIR / name).read_bytes()) != expected_hash:
            raise AssertionError(f"captured public-lineage file changed: {name}")
    lineage = json.loads((LINEAGE_DIR / "public-kubernetes-lineage.json").read_text())
    arms = {
        "PG_DATA": json.loads((LINEAGE_DIR / "arm-data-snapshot.json").read_text()),
        "PG_WAL": json.loads((LINEAGE_DIR / "arm-wal-snapshot.json").read_text()),
    }
    disks = {
        "PG_DATA": json.loads((LINEAGE_DIR / "arm-data-source-disk.json").read_text()),
        "PG_WAL": json.loads((LINEAGE_DIR / "arm-wal-source-disk.json").read_text()),
    }
    return manifest, lineage, arms, disks


def arm_cli_shape(raw: dict, *, source_disk: bool = False) -> dict:
    props = raw["properties"]
    if source_disk:
        return {
            "id": raw["id"], "uniqueId": props["uniqueId"], "location": raw["location"],
            "sku": raw["sku"], "diskSizeGB": props["diskSizeGB"],
            "diskIOPSReadWrite": props["diskIOPSReadWrite"],
            "diskMBpsReadWrite": props["diskMBpsReadWrite"],
            "managedBy": raw["managedBy"], "zones": raw["zones"],
        }
    return {
        "id": raw["id"], "uniqueId": props["uniqueId"], "location": raw["location"],
        "provisioningState": props["provisioningState"], "diskSizeGB": props["diskSizeGB"],
        "sku": raw["sku"], "incremental": props["incremental"], "tags": raw["tags"],
        "creationData": props["creationData"],
        "snapshotAccessState": props["snapshotAccessState"],
        "completionPercent": props["completionPercent"],
    }


class CapturedCnpgPairV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module(V2_RESUMER, "resume_after_backup_v2_test")
        cls.manifest, cls.lineage, cls.raw_arms, cls.raw_disks = load_captured_lineage()

    def make_runner(self, lineage=None, raw_arms=None, raw_disks=None):
        module = self.module
        lineage = copy.deepcopy(lineage if lineage is not None else self.lineage)
        raw_arms = copy.deepcopy(raw_arms if raw_arms is not None else self.raw_arms)
        raw_disks = copy.deepcopy(raw_disks if raw_disks is not None else self.raw_disks)
        runner = module.Resume.__new__(module.Resume)
        backup = lineage["backup"]
        owner = backup["metadata"]["labels"]["repro.stack.token"]
        runner.token = owner
        runner.short = owner[:8]
        runner.backup_name = backup["metadata"]["name"]
        runner.backup_uid = self.manifest["backupUID"]
        runner.sub = self.manifest["pairIdentity"][0]["snapshotId"].split("/")[2]
        runner.snap_rg = self.manifest["snapshotResourceGroup"]
        runner.node_rg = self.manifest["nodeResourceGroup"]
        runner.location = raw_arms["PG_DATA"]["location"]
        runner.zone = "1"
        runner.continuation_mode = True
        runner.pair = {}
        runner.snapshot_evidence = {}
        runner.snapshot_identity_pins = {}
        runner.pv_pvc = {}

        vs_by_name = {row["metadata"]["name"]: row for row in lineage["volumeSnapshots"].values()}
        content_by_name = {
            row["metadata"]["name"]: row for row in lineage["volumeSnapshotContents"].values()
        }

        def fake_kjson(*args):
            if len(args) >= 2 and args[0:2] == ("get", "volumesnapshot"):
                return copy.deepcopy(vs_by_name[args[2]])
            if len(args) >= 2 and args[0:2] == ("get", "volumesnapshotcontent"):
                return copy.deepcopy(content_by_name[args[2]])
            if len(args) >= 2 and args[0:2] == ("get", "volumesnapshotclass"):
                return copy.deepcopy(lineage["backupClass"])
            if len(args) >= 2 and args[0:2] == ("get", "pvc"):
                return {"items": [copy.deepcopy(x) for x in lineage["primaryPVCs"].values()]}
            if len(args) >= 2 and args[0:2] == ("get", "pv"):
                by_name = {x["metadata"]["name"]: x for x in lineage["primaryPVs"].values()}
                return copy.deepcopy(by_name[args[2]])
            raise AssertionError(f"unexpected mocked kubectl request: {args!r}")

        arm_snapshot_rows = {
            raw_arms[kind]["id"]: arm_cli_shape(raw_arms[kind]) for kind in ("PG_DATA", "PG_WAL")
        }
        arm_disk_rows = {
            raw_disks[kind]["id"]: arm_cli_shape(raw_disks[kind], source_disk=True)
            for kind in ("PG_DATA", "PG_WAL")
        }

        def fake_azjson(*args):
            if args[:3] == ("snapshot", "show", "--ids"):
                return copy.deepcopy(arm_snapshot_rows[args[3]])
            if args[:3] == ("disk", "show", "--ids"):
                return copy.deepcopy(arm_disk_rows[args[3]])
            raise AssertionError(f"unexpected mocked az request: {args!r}")

        runner.kjson = fake_kjson
        runner.azjson = fake_azjson
        # Avoid exercising the continuation lock writer. This regression tests
        # the real lineage validator with all external reads supplied locally.
        runner.protect_snapshot_promptly = lambda *_args: None
        return runner, backup

    def snapshot_pair(self, lineage=None, raw_arms=None):
        runner, backup = self.make_runner(lineage, raw_arms)
        module = self.module
        owner = backup["metadata"]["labels"]["repro.stack.token"]
        module.validate_backup_object(
            backup, backup["metadata"]["name"], owner, self.manifest["backupUID"]
        )
        runner.source_pv_disk()
        return runner.snapshot_pair(backup)

    def assert_pair_rejected(self, mutate):
        lineage = copy.deepcopy(self.lineage)
        arms = copy.deepcopy(self.raw_arms)
        mutate(lineage, arms)
        runner, backup = self.make_runner(lineage, arms)
        owner = self.lineage["backup"]["metadata"]["labels"]["repro.stack.token"]
        self.module.validate_backup_object(
            backup, "vacuum-backup-f1b90aec", owner, self.manifest["backupUID"]
        )
        runner.source_pv_disk()
        with self.assertRaises(self.module.ResumeError):
            runner.snapshot_pair(backup)

    def test_captured_cnpg_pair_without_custom_token_passes_real_snapshot_pair(self):
        for snapshot in self.lineage["volumeSnapshots"].values():
            labels = snapshot["metadata"]["labels"]
            self.assertNotIn("repro.stack.token", labels)
            self.assertEqual(snapshot["metadata"]["namespace"], "repro")
            self.assertIn("cnpg.io/cluster", labels)
            self.assertIn("cnpg.io/backupName", labels)
        result = self.snapshot_pair()
        self.assertEqual(result["pair"], {
            "data": "vacuum-backup-f1b90aec", "wal": "vacuum-backup-f1b90aec-wal"
        })
        self.assertEqual(len(result["snapshots"]), 2)
        for kind, name in (("PG_DATA", "vacuum-backup-f1b90aec"),
                           ("PG_WAL", "vacuum-backup-f1b90aec-wal")):
            row = result["snapshots"][name]
            self.assertEqual(row["volumeSnapshotUID"], self.lineage["volumeSnapshots"][kind]["metadata"]["uid"])
            self.assertEqual(row["volumeSnapshotContentUID"],
                             self.lineage["volumeSnapshotContents"][kind]["metadata"]["uid"])
            self.assertEqual(row["uniqueId"], self.raw_arms[kind]["properties"]["uniqueId"])
            self.assertEqual(row["completionPercent"], 100.0)

    def test_matching_custom_token_when_present_remains_valid(self):
        lineage = copy.deepcopy(self.lineage)
        for snapshot in lineage["volumeSnapshots"].values():
            snapshot["metadata"]["labels"]["repro.stack.token"] = STACK_TOKEN
        result = self.snapshot_pair(lineage)
        self.assertEqual(len(result["snapshots"]), 2)

    def test_wrong_namespace_source_class_backup_cnpg_or_present_token_fails(self):
        mutations = [
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["metadata"].update(namespace="default"),
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["spec"]["source"].update(
                persistentVolumeClaimName="wrong-pvc"),
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["spec"].update(
                volumeSnapshotClassName="wrong-class"),
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["metadata"]["labels"].update(
                {"cnpg.io/backupName": "other-backup"}),
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["metadata"]["labels"].update(
                {"cnpg.io/cluster": "other-cluster"}),
            lambda d, a: d["volumeSnapshots"]["PG_DATA"]["metadata"]["labels"].update(
                {"repro.stack.token": "different-owner-token"}),
            lambda d, a: d["volumeSnapshotContents"]["PG_DATA"]["spec"]["source"].update(
                {"volumeHandle": "/wrong/disk"}),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                self.assert_pair_rejected(mutate)

    def test_wrong_arm_source_or_owner_still_fails_lineage(self):
        wrong_source = lambda d, a: a["PG_DATA"]["properties"]["creationData"].update(
            sourceResourceId="/wrong/disk")
        wrong_owner = lambda d, a: a["PG_DATA"]["tags"].update(reprotoken="other-owner")
        for mutate in (wrong_source, wrong_owner):
            with self.subTest(mutation=mutate):
                self.assert_pair_rejected(mutate)

    def test_pinned_backup_uid_still_rejects_recreated_backup(self):
        runner, backup = self.make_runner()
        changed = copy.deepcopy(backup)
        changed["metadata"]["uid"] = "recreated-backup-uid"
        with self.assertRaises(self.module.ResumeError):
            self.module.validate_backup_object(
                changed, "vacuum-backup-f1b90aec", STACK_TOKEN, self.manifest["backupUID"]
            )

    def test_backup_snapshot_class_remains_stack_owned_and_retained(self):
        runner, _backup = self.make_runner()
        runner.source_pv_disk()
        lineage = copy.deepcopy(self.lineage)
        lineage["backupClass"]["metadata"]["labels"]["repro.stack.token"] = "wrong-owner"
        runner, _backup = self.make_runner(lineage)
        with self.assertRaises(self.module.ResumeError):
            runner.source_pv_disk()

    def test_v2_adapter_is_identical_except_resumer_path_and_sha_pin(self):
        frozen_resumer_sha = sha(FROZEN_RESUMER.read_bytes())
        frozen_adapter_sha = sha(FROZEN_ADAPTER.read_bytes())
        self.assertEqual(frozen_resumer_sha, FROZEN_RESUMER_SHA256)
        self.assertEqual(frozen_adapter_sha, FROZEN_ADAPTER_SHA256)
        resumer_sha = sha(V2_RESUMER.read_bytes())
        old = FROZEN_ADAPTER.read_text()
        expected = old.replace(
            'RESUMER_PATH = RECOVERY / "resume_after_backup.py"',
            'RESUMER_PATH = RECOVERY / "resume_after_backup_v2.py"',
        ).replace(
            f'RESUMER_SHA = "{FROZEN_RESUMER_SHA256}"',
            f'RESUMER_SHA = "{resumer_sha}"',
        )
        self.assertEqual(V2_ADAPTER.read_text(), expected)
        adapter = load_module(V2_ADAPTER, "finalize_continuation_v2_test")
        self.assertEqual(adapter.RESUMER_PATH.resolve(), V2_RESUMER.resolve())
        self.assertEqual(adapter.RESUMER_SHA, resumer_sha)
        loaded = adapter.load_resume_module()
        self.assertEqual(Path(loaded.__file__).resolve(), V2_RESUMER.resolve())


if __name__ == "__main__":
    unittest.main()
