#!/usr/bin/env python3
"""Remove only recorded Reader grants on the exact paired source snapshots.

The restore workflow must persist ``sourceSnapshotReaderAssignments`` intent
records before creating any grant. It must allocate each role-assignment GUID
client-side and save the full assignment ID, principal, exact snapshot scope,
Reader role, source snapshot ID, and pinned unique ID. The helper preserves grants
for every other principal and refuses unrecorded grants to this stack's principal.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

TOKEN_RE = re.compile(r"^[a-f0-9]{32}$")
GUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")
READER_ROLE_ID = "acdd72a7-3385-48ef-bd42-f606fba81ae7"


class Refusal(RuntimeError):
    pass


class AzError(Refusal):
    def __init__(self, operation: str, code: str):
        self.code = code
        super().__init__(f"Azure CLI {operation} failed ({code or 'unclassified error'}).")


class AzureCLI:
    def __init__(self, subscription: str):
        self.subscription = subscription

    def run(self, *args: str, timeout: int = 180) -> str:
        cmd = ["az", *args, "--subscription", self.subscription, "--only-show-errors"]
        try:
            p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise Refusal(f"Azure CLI call failed or timed out ({args[0]}).") from e
        if p.returncode:
            m = re.search(r"\b(RoleAssignmentNotFound|ResourceNotFound|NotFound|404|ScopeLocked|AuthorizationFailed|InvalidAuthenticationToken)\b", p.stderr, re.I)
            raise AzError(args[0], m.group(1) if m else f"exit-{p.returncode}")
        return p.stdout

    def json(self, *args: str, timeout: int = 180) -> Any:
        try:
            return json.loads(self.run(*args, timeout=timeout))
        except json.JSONDecodeError as e:
            raise Refusal(f"Azure CLI returned invalid JSON for {args[0]}.") from e


def lc(x: str) -> str:
    return x.casefold()


def role_guid(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.rstrip("/").rsplit("/", 1)[-1].casefold()


def validate_state(s: dict[str, Any], manifest: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Return the exact grant contract; None means source-reader grants are absent."""
    source = s.get("sourceSnapshotReaderAssignments")
    if source is None:
        return None
    token = s.get("ownerToken")
    sub = s.get("subscriptionId")
    if not isinstance(source, dict):
        raise Refusal("Source-snapshot reader state is malformed.")
    principal = source.get("principalId")
    records = source.get("assignments")
    if not isinstance(token, str) or not TOKEN_RE.fullmatch(token):
        raise Refusal("Saved stack ownership token is invalid.")
    if source.get("ownerToken") != token:
        raise Refusal("Source-reader grant state does not match the stack ownership token.")
    if not isinstance(sub, str) or not GUID_RE.fullmatch(sub):
        raise Refusal("Saved subscription identity is invalid.")
    if not isinstance(principal, str) or not GUID_RE.fullmatch(principal):
        raise Refusal("Saved source-reader principal is invalid.")
    if not isinstance(records, list) or not records:
        raise Refusal("Source-reader assignment intent is empty or malformed.")
    if manifest is None:
        raise Refusal("Pinned source manifest is required to validate source snapshot unique IDs.")
    if str(manifest.get("subscriptionId", "")).casefold() != sub.casefold():
        raise Refusal("Pinned source manifest subscription differs from saved state.")
    paired_wal = manifest.get("pairedWAL")
    if not isinstance(paired_wal, dict):
        raise Refusal("Pinned paired-WAL snapshot evidence is missing.")
    allowed = [
        {"id": manifest.get("sourceSnapshotId"), "uniqueId": manifest.get("uniqueId")},
        {"id": paired_wal.get("sourceSnapshotId"), "uniqueId": paired_wal.get("uniqueId")},
    ]
    allowed_snapshots: dict[str, dict[str, str]] = {}
    for item in allowed:
        if not isinstance(item, dict) or not all(isinstance(item.get(k), str) and item[k]
                                                 for k in ("id", "uniqueId")):
            raise Refusal("Allowed source snapshot ID/unique ID record is malformed.")
        allowed_snapshots[lc(item["id"])] = {"id": item["id"], "uniqueId": item["uniqueId"]}
    allowed_lc = set(allowed_snapshots)
    if len(allowed_lc) != len(allowed):
        raise Refusal("Allowed source snapshot IDs contain duplicates.")
    snapshot_re = re.compile(
        rf"^/subscriptions/{re.escape(sub)}/resourcegroups/[^/]+/providers/microsoft\.compute/snapshots/[^/]+$", re.I)
    for snap in allowed_snapshots.values():
        sid = snap["id"]
        if not snapshot_re.fullmatch(sid):
            raise Refusal("An allowed source scope is not an exact snapshot resource ID in the saved subscription.")
    normalized = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise Refusal("Source-reader assignment intent contains a malformed record.")
        rid, scope, p, role, role_name, source_id, source_uid = (record.get(k) for k in
            ("id", "scope", "principalId", "roleDefinitionId", "role", "sourceSnapshotId", "sourceSnapshotUniqueId"))
        if not all(isinstance(v, str) and v for v in (rid, scope, p, role, role_name, source_id, source_uid)):
            raise Refusal("Source-reader assignment intent is missing a required identity field.")
        if lc(scope) not in allowed_lc:
            raise Refusal("A recorded source-reader scope is not in the exact allowed snapshot set.")
        if lc(p) != lc(principal):
            raise Refusal("A recorded source-reader principal differs from the saved AKS principal.")
        if role_name != "Reader" or role_guid(role) != READER_ROLE_ID:
            raise Refusal("A recorded source-reader role changed from the built-in Reader role.")
        pinned_snapshot = allowed_snapshots[lc(scope)]
        if (lc(source_id) != lc(scope) or lc(source_id) != lc(pinned_snapshot["id"]) or
            source_uid != pinned_snapshot["uniqueId"]):
            raise Refusal("Recorded source snapshot ID/unique ID differs from the pinned source manifest.")
        role_assignment_prefix = lc(scope.rstrip("/") + "/providers/microsoft.authorization/roleassignments/")
        if not lc(rid).startswith(role_assignment_prefix):
            raise Refusal("A recorded role-assignment ID is not a child of its exact source snapshot scope.")
        assignment_guid = rid.rstrip("/").rsplit("/", 1)[-1]
        if not GUID_RE.fullmatch(assignment_guid):
            raise Refusal("A recorded role-assignment ID has no valid client-generated GUID.")
        if lc(rid) in seen:
            raise Refusal("Duplicate source-reader role-assignment IDs are recorded.")
        seen.add(lc(rid))
        normalized.append({"id": rid, "scope": scope, "principalId": principal,
                           "roleDefinitionId": role, "role": role_name,
                           "sourceSnapshotId": source_id, "sourceSnapshotUniqueId": source_uid})
    if {lc(r["scope"]) for r in normalized} != allowed_lc:
        raise Refusal("Recorded source-reader grants do not cover exactly the allowed source snapshots.")
    return {"subscription": sub, "ownerToken": token, "principalId": principal,
            "allowedSnapshots": list(allowed_snapshots.values()),
            "allowedScopes": [snap["id"] for snap in allowed_snapshots.values()],
            "records": normalized}


def validate_inventory(contract: dict[str, Any], scope: str,
                       assignments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate one scope's direct grants before any delete is attempted."""
    if not isinstance(assignments, list):
        raise Refusal("Azure returned an invalid source-scope role-assignment inventory.")
    wanted = {lc(r["id"]): r for r in contract["records"] if lc(r["scope"]) == lc(scope)}
    direct: dict[str, dict[str, Any]] = {}
    for item in assignments:
        if not isinstance(item, dict):
            raise Refusal("Azure returned a malformed role assignment.")
        item_scope = item.get("scope")
        if not isinstance(item_scope, str) or lc(item_scope) != lc(scope):
            continue  # Inherited assignments are neither targets nor unknown direct grants.
        rid = item.get("id")
        if not isinstance(rid, str) or not rid:
            raise Refusal("An exact-scope assignment has no resource ID.")
        key = lc(rid)
        if key in direct:
            raise Refusal("Azure returned a duplicate exact-scope assignment ID.")
        direct[key] = item
        principal = item.get("principalId")
        if isinstance(principal, str) and lc(principal) == lc(contract["principalId"]) and key not in wanted:
            raise Refusal("An unrecorded exact-scope role assignment exists for this stack principal.")
    for key, item in direct.items():
        if key not in wanted:
            continue
        expected = wanted[key]
        if (lc(str(item.get("scope", ""))) != lc(expected["scope"]) or
            lc(str(item.get("principalId", ""))) != lc(expected["principalId"]) or
            role_guid(item.get("roleDefinitionId")) != READER_ROLE_ID):
            raise Refusal("A saved source-reader assignment changed scope, principal, or role.")
    return [wanted[key] for key in wanted if key in direct]


def cleanup(cli: AzureCLI, contract: dict[str, Any], apply: bool) -> list[dict[str, str]]:
    active = cli.run("account", "show", "--query", "id", "-o", "tsv").strip()
    if lc(active) != lc(contract["subscription"]):
        raise Refusal("Active subscription differs from the saved stack subscription.")
    for snapshot in contract["allowedSnapshots"]:
        live = cli.json("snapshot", "show", "--ids", snapshot["id"], "-o", "json")
        if (lc(str(live.get("id", ""))) != lc(snapshot["id"]) or
            str(live.get("uniqueId", "")) != snapshot["uniqueId"]):
            raise Refusal("Source snapshot ID/unique ID changed; preserving every source-reader grant.")
    # Inventory and validate every scope before deleting any grant. This prevents
    # a later unknown scope from producing a partially cleaned state.
    targets_by_scope: dict[str, list[dict[str, Any]]] = {}
    for scope in contract["allowedScopes"]:
        listing = cli.json("role", "assignment", "list", "--scope", scope, "-o", "json")
        targets_by_scope[scope] = validate_inventory(contract, scope, listing)
    results: list[dict[str, str]] = []
    for scope in contract["allowedScopes"]:
        for record in targets_by_scope[scope]:
            rid = record["id"]
            if not apply:
                results.append({"id": rid, "status": "verified-dry-run"})
                continue
            try:
                cli.run("role", "assignment", "delete", "--ids", rid, "-o", "none", timeout=240)
            except AzError as e:
                after = cli.json("role", "assignment", "list", "--scope", scope, "-o", "json")
                remaining = validate_inventory(contract, scope, after)
                still = [r for r in remaining if lc(r["id"]) == lc(rid)]
                if e.code.casefold() in {"roleassignmentnotfound", "notfound", "404"} and not still:
                    results.append({"id": rid, "status": "already-absent"})
                    continue
                if e.code.casefold() == "scopelocked" and len(still) == 1:
                    results.append({"id": rid, "status": "retained-scope-locked"})
                    continue
                raise
            after = cli.json("role", "assignment", "list", "--scope", scope, "-o", "json")
            remaining = validate_inventory(contract, scope, after)
            if any(lc(r["id"]) == lc(rid) for r in remaining):
                raise Refusal("Exact source-reader assignment remains after successful delete.")
            results.append({"id": rid, "status": "deleted"})
    return results


def _self_test() -> None:
    token = "a" * 32
    sub = "00000000-0000-0000-0000-000000000000"
    principal = "11111111-2222-3333-4444-555555555555"
    s1 = f"/subscriptions/{sub}/resourceGroups/source/providers/Microsoft.Compute/snapshots/data"
    s2 = f"/subscriptions/{sub}/resourceGroups/source/providers/Microsoft.Compute/snapshots/wal"
    id1 = s1 + "/providers/Microsoft.Authorization/roleAssignments/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    id2 = s2 + "/providers/Microsoft.Authorization/roleAssignments/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    role = "/subscriptions/" + sub + "/providers/Microsoft.Authorization/roleDefinitions/" + READER_ROLE_ID
    manifest = {"subscriptionId": sub, "sourceSnapshotId": s1, "uniqueId": "uid-data",
                "pairedWAL": {"sourceSnapshotId": s2, "uniqueId": "uid-wal"}}
    base = {"ownerToken": token, "subscriptionId": sub,
            "sourceSnapshotReaderAssignments": {"ownerToken": token, "principalId": principal, "assignments": [
                {"id": id1, "scope": s1, "principalId": principal, "roleDefinitionId": role,
                 "role": "Reader", "sourceSnapshotId": s1, "sourceSnapshotUniqueId": "uid-data"},
                {"id": id2, "scope": s2, "principalId": principal, "roleDefinitionId": role,
                 "role": "Reader", "sourceSnapshotId": s2, "sourceSnapshotUniqueId": "uid-wal"}]}}
    c = validate_state(base, manifest)
    assert c is not None
    assert validate_inventory(c, s1, [{"id": id1, "scope": s1, "principalId": principal,
                                       "roleDefinitionId": role}])
    def rejected(s: dict[str, Any], m: dict[str, Any] = manifest) -> bool:
        try:
            validate_state(s, m)
        except Refusal:
            return True
        return False
    bad = json.loads(json.dumps(base)); bad["sourceSnapshotReaderAssignments"]["ownerToken"] = "b" * 32
    assert rejected(bad), "wrong owner token accepted"
    bad = json.loads(json.dumps(base)); bad["sourceSnapshotReaderAssignments"]["assignments"][0]["scope"] = "/subscriptions/other/resourceGroups/x/providers/Microsoft.Compute/snapshots/foreign"
    assert rejected(bad), "unexpected snapshot scope accepted"
    bad = json.loads(json.dumps(base)); bad["sourceSnapshotReaderAssignments"]["assignments"][0]["roleDefinitionId"] = "00000000-0000-0000-0000-000000000000"
    assert rejected(bad), "changed role accepted"
    unknown = {"id": s1 + "/providers/Microsoft.Authorization/roleAssignments/cccccccc-cccc-cccc-cccc-cccccccccccc",
               "scope": s1, "principalId": principal, "roleDefinitionId": role}
    try:
        validate_inventory(c, s1, [{"id": id1, "scope": s1, "principalId": principal,
                                    "roleDefinitionId": role}, unknown])
    except Refusal:
        pass
    else:
        raise AssertionError("unknown exact-scope assignment accepted")
    changed = {"id": id1, "scope": s1, "principalId": principal,
               "roleDefinitionId": "00000000-0000-0000-0000-000000000000"}
    try:
        validate_inventory(c, s1, [changed])
    except Refusal:
        pass
    else:
        raise AssertionError("live role change accepted")
    bad_manifest = json.loads(json.dumps(manifest)); bad_manifest["uniqueId"] = "replaced-data-uid"
    assert rejected(base, bad_manifest), "source snapshot UID change accepted"
    class WrongSnapshotCLI:
        def __init__(self): self.role_list_called = False
        def run(self, *args: str, **kwargs: Any) -> str:
            if args[:2] == ("account", "show"): return sub
            raise AssertionError(f"unexpected call {args[:2]}")
        def json(self, *args: str, **kwargs: Any) -> Any:
            if args[:2] == ("snapshot", "show"):
                return {"id": args[3], "uniqueId": "replaced-snapshot"}
            if args[:3] == ("role", "assignment", "list"):
                self.role_list_called = True
                return []
            raise AssertionError(f"unexpected call {args[:2]}")
    wrong_snapshot_cli = WrongSnapshotCLI()
    try:
        cleanup(wrong_snapshot_cli, c, True)  # type: ignore[arg-type]
    except Refusal:
        pass
    else:
        raise AssertionError("source snapshot replacement accepted")
    assert not wrong_snapshot_cli.role_list_called, "role cleanup began before snapshot UID validation"
    print("cleanup-source-readers self-test: PASS")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", type=Path, default=Path(__file__).parents[1] / "run" / ".state" / "state.json")
    p.add_argument("--apply", action="store_true", help="delete only exact recorded role-assignment IDs")
    p.add_argument("--self-test", action="store_true", help="run offline safety tests; no Azure calls")
    a = p.parse_args()
    if a.self_test:
        _self_test(); return 0
    if not a.state.is_file():
        raise Refusal("Saved ownership state is missing; refusing to guess.")
    state = json.loads(a.state.read_text(encoding="utf-8"))
    manifest_path = Path(__file__).parents[1] / "run" / "source-manifest.json"
    if not manifest_path.is_file():
        raise Refusal("Pinned source manifest is missing; refusing source-reader cleanup.")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise Refusal("Pinned source manifest is invalid.") from e
    contract = validate_state(state, manifest)
    if contract is None:
        print(json.dumps({"status": "not-configured", "mutated": False}, sort_keys=True))
        return 0
    if a.apply:
        with (a.state.parent / "teardown.lock").open("a") as teardown_lock, \
             (a.state.parent / "02-prepare.lock").open("a") as prepare_lock:
            try:
                fcntl.flock(teardown_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(prepare_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as e:
                raise Refusal("Preparation or teardown is active; refusing role cleanup.") from e
            results = cleanup(AzureCLI(contract["subscription"]), contract, True)
    else:
        results = cleanup(AzureCLI(contract["subscription"]), contract, False)
    print(json.dumps({"status": "complete", "mutated": bool(a.apply),
                      "assignments": results}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        raise SystemExit(2)
