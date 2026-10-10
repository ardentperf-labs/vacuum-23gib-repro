import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("hydration_barrier", ROOT / "hydration-barrier.py")
barrier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(barrier)


SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"


def resource_id(group, provider, name):
    return (f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{group}/providers/"
            f"Microsoft.Compute/{provider}/{name}")


def disk(owner, name, sku, option, percent, *, stage=False):
    tags = {"reprotoken": owner}
    if stage:
        tags["reproPurpose"] = "premiumv2-source-stage"
    props = {
        "provisioningState": "Succeeded",
        "completionPercent": percent,
        "copyCompletionError": None,
        "diskSizeGB": 512,
        "diskIOPSReadWrite": 3000,
        "diskMBpsReadWrite": 125,
        "logicalSectorSize": 512,
        "creationData": {"createOption": option},
    }
    return {
        "id": resource_id(f"rg-node-{owner}", "disks", name),
        "name": name,
        "sku": {"name": sku},
        "zones": ["1"],
        "tags": tags,
        "properties": props,
    }


def snapshot(owner, name, option, percent, *, state="Succeeded", access="Available",
             purpose="premiumv2-vacuum-backup", source_id=None, copy_error=None):
    tags = {"reprotoken": owner, "reproPurpose": purpose}
    if source_id:
        tags["sourceSnapshotId"] = source_id
    return {
        "id": resource_id(f"rg-snap-{owner}", "snapshots", name),
        "name": name,
        "properties": {
            "uniqueId": "unique-" + name,
            "provisioningState": state,
            "snapshotAccessState": access,
            "completionPercent": percent,
            "copyCompletionError": copy_error,
            "creationData": {"createOption": option},
        },
        "sku": {"name": "Standard_ZRS" if purpose.endswith("backup") else "Standard_LRS"},
        "tags": tags,
    }


class HydrationBarrierTests(unittest.TestCase):
    def fixture(self, root):
        root = Path(root)
        responses = {}
        owners = {}
        for name in barrier.RUNS:
            owner = "a" * 30 + str(len(name) % 10) + "b"
            owners[name] = owner
            node_rg = f"rg-node-{owner}"
            snap_rg = f"rg-snap-{owner}"
            (root / name / ".state").mkdir(parents=True)
            state = {
                "subscriptionId": SUBSCRIPTION,
                "nodeResourceGroup": node_rg,
                "snapshotResourceGroupId": f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{snap_rg}",
                "ownerToken": owner,
                "availabilityZone": "1",
            }
            source_id = resource_id("rg-source", "snapshots", "pinned-source")
            (root / name / ".state/state.json").write_text(json.dumps(state))
            (root / name / "source-manifest.json").write_text(json.dumps({"sourceSnapshotId": source_id}))
            for group in (node_rg, snap_rg):
                group_path = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{group}"
                responses[group_path] = {"tags": {"reprotoken": owner}}

            disks = [
                disk(owner, f"{name}-premium-{index}", "PremiumV2_LRS", option, percent)
                for index, (option, percent) in enumerate((
                    ("CopyStart", 100), ("CopyStart", 100), ("Empty", None), ("Empty", 100)))
            ]
            disks.append(disk(owner, f"{name}-source-stage", "Standard_LRS", "Copy", None, stage=True))
            snapshots = [
                snapshot(owner, f"{name}-keeper", "Copy", None, purpose="premiumv2-keeper-copy"),
                snapshot(owner, f"{name}-backup", "CopyStart", 100),
            ]
            pins = []
            if name == "100gib":
                failed = snapshot(owner, f"{name}-failed-copy", "CopyStart", 0.0, state="Failed",
                                  access="Unknown", purpose="premiumv2-keeper-copy", source_id=source_id,
                                  copy_error={"code": "CopyFailed", "message": "terminal source copy failure"})
                snapshots.append(failed)
                pins.append({
                    "id": failed["id"], "uniqueId": failed["properties"]["uniqueId"],
                    "ownerToken": owner, "reprotoken": owner, "sourceSnapshotId": source_id,
                    "provisioningState": "Failed", "completionPercent": 0.0,
                    "reproPurpose": "premiumv2-keeper-copy",
                })
            (root / name / ".state/known-failed-source-copies.json").write_text(
                json.dumps({"version": 1, "snapshots": pins}))
            node_path = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{node_rg}"
            snap_path = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{snap_rg}"
            responses[f"{node_path}/providers/Microsoft.Compute/disks"] = {"value": disks}
            responses[f"{snap_path}/providers/Microsoft.Compute/snapshots"] = {"value": snapshots}
        (root / "preflight").mkdir()
        return root, responses, owners

    def run_fixture(self, mutate=None):
        with tempfile.TemporaryDirectory() as temp_dir:
            root, responses, owners = self.fixture(temp_dir)
            if mutate:
                mutate(responses, owners)

            def open_response(request, timeout):
                path = urlsplit(request.full_url).path
                if path not in responses:
                    raise AssertionError(f"Unexpected ARM GET: {path}")
                return io.BytesIO(json.dumps(responses[path]).encode())

            with (
                mock.patch.object(barrier, "ROOT", root),
                mock.patch.object(barrier.subprocess, "check_output", return_value='{"accessToken":"test-token"}'),
                mock.patch.object(barrier.urllib.request, "urlopen", side_effect=open_response),
            ):
                return barrier.verify(), responses, owners

    def test_complete_stack_accepts_empty_disks_stage_and_retained_snapshot_null_percent(self):
        receipt, _, _ = self.run_fixture()
        self.assertTrue(receipt["complete"])
        self.assertEqual(len(receipt["stacks"]), 3)
        self.assertTrue(all(stack["complete"] for stack in receipt["stacks"]))

    def test_partial_premium_disk_copy_holds_measurement_gate(self):
        def make_partial(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-node-{owner}/providers/Microsoft.Compute/disks"
            responses[group]["value"][0]["properties"]["completionPercent"] = 0

        receipt, _, _ = self.run_fixture(make_partial)
        self.assertFalse(receipt["complete"])
        self.assertFalse(receipt["stacks"][0]["complete"])

    def test_premium_disk_uses_null_and_alias_metadata_fallbacks(self):
        def apply_fallbacks(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-node-{owner}/providers/Microsoft.Compute/disks"
            props = responses[group]["value"][0]["properties"]
            props["diskSizeGB"] = None
            props["diskSizeGb"] = 512
            props["logicalSectorSize"] = None
            props["creationData"]["logicalSectorSize"] = 512

        receipt, _, _ = self.run_fixture(apply_fallbacks)
        self.assertTrue(receipt["complete"])

    def test_partial_retained_snapshot_copy_holds_measurement_gate(self):
        def make_partial(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-snap-{owner}/providers/Microsoft.Compute/snapshots"
            responses[group]["value"][1]["properties"]["completionPercent"] = 0

        receipt, _, _ = self.run_fixture(make_partial)
        self.assertFalse(receipt["complete"])
        self.assertFalse(receipt["stacks"][0]["complete"])

    def test_owner_source_bound_terminal_failed_copy_is_preserved_without_blocking(self):
        receipt, _, _ = self.run_fixture()
        self.assertTrue(receipt["complete"])
        stack_100 = next(stack for stack in receipt["stacks"] if stack["stack"] == "100gib")
        failed = [r for r in stack_100["resources"] if r["kind"] == "snapshot" and r["terminalFailedCopyPreserved"]]
        self.assertEqual(len(failed), 1)
        self.assertTrue(failed[0]["complete"])
        self.assertEqual(failed[0]["createOption"], "CopyStart")
        self.assertEqual(failed[0]["completionPercent"], 0.0)

    def test_unpinned_or_wrong_source_failed_copy_fails_closed(self):
        def change_source(responses, owners):
            name = "100gib"
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-snap-{owner}/providers/Microsoft.Compute/snapshots"
            responses[group]["value"][-1]["tags"]["sourceSnapshotId"] = resource_id(
                "rg-other", "snapshots", "different-source")

        with self.assertRaisesRegex(RuntimeError, "unexpected failed retained snapshot"):
            self.run_fixture(change_source)

    def test_wrong_snapshot_owner_fails_closed(self):
        def change_owner(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-snap-{owner}/providers/Microsoft.Compute/snapshots"
            responses[group]["value"][0]["tags"]["reprotoken"] = "wrong-owner"

        with self.assertRaisesRegex(RuntimeError, "snapshot ownership changed"):
            self.run_fixture(change_owner)

    def test_owned_nonstage_standard_disk_with_active_copy_holds_gate(self):
        def add_active_copy(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-node-{owner}/providers/Microsoft.Compute/disks"
            responses[group]["value"].append(disk(owner, f"{name}-other-standard-copy", "Standard_LRS", "Copy", 0))

        receipt, _, _ = self.run_fixture(add_active_copy)
        self.assertFalse(receipt["complete"])

    def test_successful_snapshot_with_copy_error_does_not_release_gate(self):
        def add_error(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-snap-{owner}/providers/Microsoft.Compute/snapshots"
            responses[group]["value"][0]["properties"]["copyCompletionError"] = {}

        receipt, _, _ = self.run_fixture(add_error)
        self.assertFalse(receipt["complete"])

    def test_successful_snapshot_with_unknown_create_option_holds_gate(self):
        def unknown_option(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-snap-{owner}/providers/Microsoft.Compute/snapshots"
            responses[group]["value"][0]["properties"]["creationData"]["createOption"] = "FutureOption"

        receipt, _, _ = self.run_fixture(unknown_option)
        self.assertFalse(receipt["complete"])

    def test_stage_must_be_standard_lrs(self):
        def change_sku(responses, owners):
            name = barrier.RUNS[0]
            owner = owners[name]
            group = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/rg-node-{owner}/providers/Microsoft.Compute/disks"
            responses[group]["value"][-1]["sku"]["name"] = "PremiumV2_LRS"

        with self.assertRaisesRegex(RuntimeError, "source staging disk is not Standard_LRS"):
            self.run_fixture(change_sku)


if __name__ == "__main__":
    unittest.main()
