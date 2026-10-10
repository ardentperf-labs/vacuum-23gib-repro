import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[1]
TEARDOWN = WORKSPACE / "baseline-23gib" / "04-teardown.sh"
SUBSCRIPTION = "12345678-1234-1234-1234-123456789abc"
TOKEN = "a" * 32
LOCATION = "eastus"
RG = f"rg-vacuum-only-{TOKEN}"
NODE_RG = f"rg-vacuum-nodes-{TOKEN}"
SNAP_RG = f"rg-vacuum-pv2-snap-{TOKEN}"
SNAP_RG_ID = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{SNAP_RG}"
SOURCE_ID = (f"/subscriptions/{SUBSCRIPTION}/resourceGroups/keeper/"
             "providers/Microsoft.Compute/snapshots/pinned-source")
RA_ROLE = "7efff54f-a5b4-42b5-a1c5-5411624893ce"
RA_PRINCIPAL = "fcb3e0ab-82e7-470e-a99d-0b12f3da4767"
RA_ID = SNAP_RG_ID + "/providers/Microsoft.Authorization/roleAssignments/11111111-2222-4333-8444-555555555555"


FAKE_AZ = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path

path = Path(os.environ["MOCK_AZ_STATE"])
state = json.loads(path.read_text())
args = sys.argv[1:]
while "--subscription" in args:
    index = args.index("--subscription")
    del args[index:index + 2]
def value(flag, default=""):
    return args[args.index(flag) + 1] if flag in args and args.index(flag) + 1 < len(args) else default

def save():
    path.write_text(json.dumps(state))

state.setdefault("calls", []).append(args)
save()

def output(value):
    print(json.dumps(value))

command = args[:2]
if command == ["account", "show"]:
    print(state["subscription"])
elif command == ["group", "exists"]:
    print("true" if state["groups"].get(value("-n"), False) else "false")
elif command == ["group", "show"]:
    name = value("-n")
    if name not in state["groupObjects"]:
        print("ResourceNotFound", file=sys.stderr); sys.exit(1)
    output(state["groupObjects"][name])
elif command == ["group", "delete"]:
    state["groups"][value("-n")] = False
    save()
elif command == ["snapshot", "list"]:
    output(state["snapshots"] if value("-g", value("--resource-group")) == state["snapshotResourceGroup"] else [])
elif command == ["snapshot", "show"]:
    sid = value("--ids")
    snap = next((item for item in state["snapshots"] if item["id"].lower() == sid.lower()), None)
    if snap is None:
        print("ResourceNotFound", file=sys.stderr); sys.exit(1)
    output(snap)
elif command == ["resource", "list"]:
    group = value("-g")
    output([{"id": item["id"]} for item in state["snapshots"]] if group == state["snapshotResourceGroup"] else [])
elif command == ["lock", "list"]:
    output(state["locks"])
elif command == ["lock", "create"]:
    name = value("--name")
    snap_name = value("--resource-name")
    scope = state["snapshotResourceGroupId"] + "/providers/Microsoft.Compute/snapshots/" + snap_name
    state["locks"].append({"id": scope + "/providers/Microsoft.Authorization/locks/" + name,
                           "scope": scope, "level": "CanNotDelete"})
    save()
elif command == ["aks", "show"]:
    print("ResourceNotFound: AKS cluster not found", file=sys.stderr); sys.exit(1)
elif command == ["role", "assignment"] and args[2:3] == ["list"]:
    if "--all" in args and ("--scope" in args or "-g" in args or "--resource-group" in args):
        print("group or scope are not required when --all is used", file=sys.stderr); sys.exit(1)
    if value("--scope").lower() != state["snapshotResourceGroupId"].lower():
        print("Unexpected mock role-assignment list scope", file=sys.stderr); sys.exit(64)
    output(state.get("roleAssignments", []))
elif command == ["role", "assignment"] and args[2:3] == ["delete"]:
    assignment_id = value("--ids")
    if assignment_id.lower() != state.get("expectedRoleAssignmentId", "").lower():
        print("Unexpected role-assignment deletion ID", file=sys.stderr); sys.exit(64)
    if state.get("roleAssignmentDeleteBehavior") == "ScopeLocked":
        print("ScopeLocked: role assignment is protected by a resource lock", file=sys.stderr); sys.exit(1)
    state["roleAssignments"] = [item for item in state.get("roleAssignments", [])
                                if item["id"].lower() != assignment_id.lower()]
    save()
else:
    print("Unexpected mock az command: " + " ".join(args), file=sys.stderr); sys.exit(64)
'''


def make_snapshot(name, token=TOKEN, purpose="premiumv2-vacuum-backup", sku="Standard_LRS",
                  state="Succeeded", size=512, option="Copy", completion=None,
                  source=SOURCE_ID, access="Available"):
    snapshot = {
        "id": SNAP_RG_ID + "/providers/Microsoft.Compute/snapshots/" + name,
        "name": name,
        "location": LOCATION,
        "provisioningState": state,
        "snapshotAccessState": access,
        "incremental": True,
        "tags": {"reprotoken": token, "reproPurpose": purpose},
        "creationData": {"createOption": option,
                         "sourceResourceId": f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_RG}/providers/Microsoft.Compute/disks/stage"},
    }
    if purpose == "premiumv2-keeper-copy":
        snapshot["tags"]["sourceSnapshotId"] = source
    if size is not None:
        snapshot["diskSizeGB"] = size
    if sku is not None:
        snapshot["sku"] = {"name": sku}
    if completion is not None:
        snapshot["completionPercent"] = completion
    if state == "Failed":
        snapshot["snapshotAccessState"] = access
    return snapshot


class TeardownPolicyTests(unittest.TestCase):
    def run_teardown(self, snapshots, role_assignments=None, role_assignment_delete_behavior=None):
        temporary = tempfile.TemporaryDirectory()
        stack = Path(temporary.name) / "baseline-23gib"
        state_dir = stack / ".state"
        bindir = Path(temporary.name) / "bin"
        state_dir.mkdir(parents=True)
        bindir.mkdir()
        script = stack / "04-teardown.sh"
        shutil.copy2(TEARDOWN, script)
        kubeconfig = state_dir / "kubeconfig"
        kubeconfig.write_text("mock kubeconfig\n")
        (stack / "source-manifest.json").write_text(json.dumps({"sourceSnapshotId": SOURCE_ID}))

        groups = {RG: True, NODE_RG: True, SNAP_RG: True}
        group_objects = {
            RG: {"name": RG, "location": LOCATION,
                 "tags": {"reprotoken": TOKEN, "managed-by": "vacuum-only-repro"}},
            NODE_RG: {"name": NODE_RG, "tags": {"reprotoken": TOKEN}},
            SNAP_RG: {"name": SNAP_RG, "location": LOCATION,
                      "tags": {"reprotoken": TOKEN, "reproPurpose": "premiumv2-retained-snapshots",
                               "managed-by": "vacuum-only-repro"}},
        }
        state = {
            "subscription": SUBSCRIPTION,
            "resourceGroup": RG,
            "snapshotResourceGroup": SNAP_RG,
            "snapshotResourceGroupId": SNAP_RG_ID,
            "groups": groups,
            "groupObjects": group_objects,
            "snapshots": snapshots,
            "roleAssignments": role_assignments or [],
            "expectedRoleAssignmentId": RA_ID,
            "roleAssignmentDeleteBehavior": role_assignment_delete_behavior,
            "locks": [{"id": SNAP_RG_ID + "/providers/Microsoft.Authorization/locks/retain-all",
                       "scope": SNAP_RG_ID, "level": "CanNotDelete"}],
            "calls": [],
        }
        mock_state = Path(temporary.name) / "azure.json"
        mock_state.write_text(json.dumps(state))
        mock_az = bindir / "az"
        mock_az.write_text(FAKE_AZ)
        mock_az.chmod(0o755)
        owner_state = {
            "version": 1,
            "subscriptionId": SUBSCRIPTION,
            "resourceGroup": RG,
            "clusterName": f"aks-vacuum-only-{TOKEN[:12]}",
            "ownerToken": TOKEN,
            "nodeResourceGroup": NODE_RG,
            "requestedNodeResourceGroup": NODE_RG,
            "snapshotResourceGroup": SNAP_RG,
            "snapshotResourceGroupId": SNAP_RG_ID,
            "availabilityZone": "1",
            "location": LOCATION,
            "kubeconfigPath": str(kubeconfig),
        }
        if role_assignments is not None:
            owner_state["snapshotRoleAssignment"] = {"id": RA_ID, "principalId": RA_PRINCIPAL}
        (state_dir / "state.json").write_text(json.dumps(owner_state))
        env = os.environ.copy()
        env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
        env["MOCK_AZ_STATE"] = str(mock_state)
        env["SUB"] = SUBSCRIPTION
        process = subprocess.run(["bash", str(script)], cwd=stack, env=env,
                                 capture_output=True, text=True, timeout=30)
        result_state = json.loads(mock_state.read_text())
        receipt_path = state_dir / "teardown.json"
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        return temporary, process, result_state, receipt

    def test_successful_lrs_zrs_backups_and_copy_with_null_percent_are_retained(self):
        snapshots = [
            make_snapshot("keeper-copy", purpose="premiumv2-keeper-copy", sku="Standard_LRS",
                          option="Copy", completion=None),
            make_snapshot("backup-lrs", sku="Standard_LRS", option="Copy", completion=None),
            make_snapshot("backup-zrs", sku="Standard_ZRS", option="CopyStart", completion=100),
        ]
        temporary, process, state, receipt = self.run_teardown(snapshots)
        try:
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertIsNotNone(receipt)
            self.assertEqual(receipt["status"], "complete")
            records = {item["name"]: item for item in receipt["retainedSnapshots"]}
            self.assertEqual(set(records), {"keeper-copy", "backup-lrs", "backup-zrs"})
            self.assertEqual(records["keeper-copy"]["preservationStatus"], "ready")
            self.assertEqual(records["backup-lrs"]["sku"], "Standard_LRS")
            self.assertEqual(records["backup-zrs"]["sku"], "Standard_ZRS")
            direct_locks = {lock["scope"].rsplit("/", 1)[-1] for lock in state["locks"]
                            if lock.get("scope", "").lower().startswith(SNAP_RG_ID.lower() + "/providers/microsoft.compute/snapshots/")}
            self.assertEqual(direct_locks, set(records))
        finally:
            temporary.cleanup()

    def test_failed_owner_and_source_bound_copy_is_preserved_without_size(self):
        failed = make_snapshot("failed-copy", purpose="premiumv2-keeper-copy", sku=None,
                               state="Failed", size=None, option="CopyStart", completion=0,
                               access="Unknown")
        temporary, process, state, receipt = self.run_teardown([failed])
        try:
            self.assertEqual(process.returncode, 0, process.stderr)
            record, = receipt["retainedSnapshots"]
            self.assertEqual(record["provisioningState"], "Failed")
            self.assertEqual(record["preservationStatus"], "terminal_failed_preserved")
            self.assertIsNone(record["diskSizeGiB"])
            self.assertEqual(record["sourceSnapshotId"], SOURCE_ID)
            self.assertTrue(any(lock.get("scope", "").lower() == failed["id"].lower()
                                and lock.get("level") == "CanNotDelete" for lock in state["locks"]))
        finally:
            temporary.cleanup()

    def test_failed_copy_with_wrong_source_is_rejected_before_compute_deletion(self):
        failed = make_snapshot("wrong-source", purpose="premiumv2-keeper-copy", sku=None,
                               state="Failed", size=None, option="CopyStart", completion=0,
                               source=SOURCE_ID + "-other", access="Unknown")
        temporary, process, state, receipt = self.run_teardown([failed])
        try:
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("Failed snapshot ownership/source check failed", process.stderr)
            self.assertFalse(any(call[:2] == ["group", "delete"] or call[:2] == ["aks", "delete"]
                                 for call in state["calls"]))
            self.assertIsNone(receipt)
        finally:
            temporary.cleanup()

    def test_failed_copy_with_wrong_owner_is_rejected_before_compute_deletion(self):
        failed = make_snapshot("wrong-owner", token="b" * 32, purpose="premiumv2-keeper-copy",
                               sku=None, state="Failed", size=None, option="CopyStart", completion=0,
                               access="Unknown")
        temporary, process, state, receipt = self.run_teardown([failed])
        try:
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("Failed snapshot ownership/source check failed", process.stderr)
            self.assertFalse(any(call[:2] == ["group", "delete"] or call[:2] == ["aks", "delete"]
                                 for call in state["calls"]))
            self.assertIsNone(receipt)
        finally:
            temporary.cleanup()

    def test_nonterminal_copy_blocks_compute_deletion(self):
        active = make_snapshot("active-copy", purpose="premiumv2-keeper-copy", sku=None,
                               state="Creating", size=None, option="CopyStart", completion=35)
        temporary, process, state, receipt = self.run_teardown([active])
        try:
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("Snapshot operation is nonterminal or unknown", process.stderr)
            self.assertFalse(any(call[:2] == ["group", "delete"] or call[:2] == ["aks", "delete"]
                                 for call in state["calls"]))
            self.assertIsNone(receipt)
        finally:
            temporary.cleanup()

    def test_copy_start_requires_completion_percent_100(self):
        incomplete = make_snapshot("copy-start-incomplete", purpose="premiumv2-keeper-copy",
                                   sku="Standard_LRS", state="Succeeded", size=512,
                                   option="CopyStart", completion=None)
        temporary, process, state, receipt = self.run_teardown([incomplete])
        try:
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("Retained snapshot identity or readiness check failed", process.stderr)
            self.assertFalse(any(call[:2] == ["group", "delete"] or call[:2] == ["aks", "delete"]
                                 for call in state["calls"]))
            self.assertIsNone(receipt)
        finally:
            temporary.cleanup()

    def test_scoped_role_assignment_list_omits_all_and_deletes_only_pinned_id(self):
        target = {"id": RA_ID, "scope": SNAP_RG_ID, "principalId": RA_PRINCIPAL,
                  "roleDefinitionId": "/subscriptions/" + SUBSCRIPTION +
                  "/providers/Microsoft.Authorization/roleDefinitions/" + RA_ROLE}
        unrelated = {"id": SNAP_RG_ID + "/providers/Microsoft.Authorization/roleAssignments/"
                     "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                     "scope": SNAP_RG_ID, "principalId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                     "roleDefinitionId": "/subscriptions/" + SUBSCRIPTION +
                     "/providers/Microsoft.Authorization/roleDefinitions/" + RA_ROLE}
        temporary, process, state, receipt = self.run_teardown([], [target, unrelated])
        try:
            self.assertEqual(process.returncode, 0, process.stderr)
            list_calls = [call for call in state["calls"] if call[:3] == ["role", "assignment", "list"]]
            self.assertGreaterEqual(len(list_calls), 2)
            for call in list_calls:
                self.assertIn("--scope", call)
                self.assertEqual(call[call.index("--scope") + 1].lower(), SNAP_RG_ID.lower())
                self.assertNotIn("--all", call)
            delete_calls = [call for call in state["calls"] if call[:3] == ["role", "assignment", "delete"]]
            self.assertEqual(len(delete_calls), 1)
            self.assertEqual(delete_calls[0][delete_calls[0].index("--ids") + 1], RA_ID)
            self.assertEqual([item["id"] for item in state["roleAssignments"]], [unrelated["id"]])
            self.assertEqual(receipt["snapshotRoleAssignment"]["status"], "deleted")
        finally:
            temporary.cleanup()

    def test_scope_locked_role_assignment_is_recorded_as_preserved_residual(self):
        target = {"id": RA_ID, "scope": SNAP_RG_ID, "principalId": RA_PRINCIPAL,
                  "roleDefinitionId": "/subscriptions/" + SUBSCRIPTION +
                  "/providers/Microsoft.Authorization/roleDefinitions/" + RA_ROLE}
        temporary, process, state, receipt = self.run_teardown(
            [], [target], role_assignment_delete_behavior="ScopeLocked")
        try:
            self.assertEqual(process.returncode, 0, process.stderr)
            assignment = receipt["snapshotRoleAssignment"]
            self.assertEqual(assignment["status"], "retained_scope_locked")
            self.assertTrue(assignment["residual"])
            self.assertEqual(assignment["reason"], "ScopeLocked")
            self.assertTrue(assignment["protectedByCanNotDeleteLock"])
            self.assertTrue(assignment["principalDeletedWithAks"])
            self.assertEqual([item["id"] for item in state["roleAssignments"]], [RA_ID])
            self.assertFalse(any(call[:2] == ["role", "assignment"] and call[2:3] == ["delete"]
                                 and "--all" in call for call in state["calls"]))
        finally:
            temporary.cleanup()

    def test_wrong_scope_role_assignment_is_rejected_before_any_delete(self):
        target = {"id": RA_ID, "scope": "/subscriptions/" + SUBSCRIPTION + "/resourceGroups/other",
                  "principalId": RA_PRINCIPAL,
                  "roleDefinitionId": "/subscriptions/" + SUBSCRIPTION +
                  "/providers/Microsoft.Authorization/roleDefinitions/" + RA_ROLE}
        temporary, process, state, receipt = self.run_teardown([], [target])
        try:
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("role assignment scope, principal, or role did not verify", process.stderr)
            self.assertFalse(any(call[:2] in (["group", "delete"], ["aks", "delete"])
                                 or call[:3] == ["role", "assignment", "delete"]
                                 for call in state["calls"]))
            self.assertIsNone(receipt)
            self.assertEqual(state["roleAssignments"], [target])
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
