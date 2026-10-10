#!/usr/bin/env python3
"""Owner-checked final watcher cleanup and preservation audit for five runs.

Default mode performs local integration-gate checks only and prints a plan.  It
does not contact Azure or write final receipts.  --apply is reserved for root
after the fifth full run is complete and reviewed.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
GATE = ROOT / "integration-v2-gates.py"
FULL = {
    "baseline": "baseline",
    "prefetch-off": "prefetch-off/attempt-westus3",
    "100gib": "100gib",
    "prefetch-off-fpw-off": "prefetch-off-fpw-off",
}
PRESSURE = "read-pressure"
OLD = PROJECT / ".parallel/vacuum-23gib-20261004T205657Z"
PREFETCH_PARENT = ROOT / "prefetch-off"
EXPECTED_RESIDUAL_STACKS = {"baseline", "older-source"}
LOCK_NAME = "preserve-all-snapshots"
LOCK_NOTES = "Preserve every snapshot until explicit user deletion authorization."


class GateError(RuntimeError):
    pass


def load(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise GateError(f"Cannot read required JSON {path}: {exc}") from exc


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def run_integration_gate() -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, str(GATE)], text=True, capture_output=True, check=False,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    try:
        result = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise GateError(f"Integration gate did not emit JSON: {exc}; {proc.stderr[-1000:]}") from exc
    if proc.returncode or result.get("readyForFinalIntegration") is not True:
        blockers = []
        for run in result.get("fullRuns", []):
            if not run.get("ready"):
                blockers.extend(f"{run.get('name')}: {x}" for x in run.get("blockers", []))
                blockers.extend(f"{run.get('name')} cleanup: {x}" for x in run.get("cleanupBlockers", []))
        pressure = result.get("pressurePartial", {})
        if not pressure.get("ready"):
            blockers.extend(f"pressure partial: {x}" for x in pressure.get("blockers", []))
        raise GateError("Five-run integration gate is not ready: " + "; ".join(blockers or ["unknown blocker"]))
    return result


def selected_states() -> list[dict[str, Any]]:
    paths = [(name, ROOT / rel) for name, rel in FULL.items()]
    paths += [(PRESSURE, ROOT / PRESSURE), ("older-source", OLD), ("prefetch-parent", PREFETCH_PARENT)]
    states = []
    for label, stack in paths:
        state = load(stack / ".state/state.json")
        state["_stack"] = label
        state["_stackPath"] = str(stack)
        if not state.get("subscriptionId") or not state.get("ownerToken") or not state.get("resourceGroup"):
            raise GateError(f"{label}: incomplete ownership state")
        states.append(state)
    subscriptions = {s["subscriptionId"].lower() for s in states}
    if len(subscriptions) != 1:
        raise GateError("Selected states do not share one explicit subscription")
    tokens = [s["ownerToken"].lower() for s in states]
    if len(tokens) != len(set(tokens)):
        raise GateError("Selected run states reuse an owner token")
    return states


def local_cleanup_gates() -> dict[str, Any]:
    integration = run_integration_gate()
    full_results = []
    for name, rel in FULL.items():
        stack = ROOT / rel
        final = load(stack / ".state/finalization.json")
        cleanup = load(stack / "cleanup/result.json")
        if final.get("status") != "finished":
            raise GateError(f"{name}: full-run finalization is not finished")
        if cleanup.get("allSnapshotsPreserved") is not True:
            raise GateError(f"{name}: cleanup does not prove snapshot preservation")
        if name != "baseline" and cleanup.get("cleanupComplete") is not True:
            raise GateError(f"{name}: cleanup is incomplete")
        full_results.append({"stack": name, "finalization": final, "cleanup": cleanup})
    partial_final = load(ROOT / PRESSURE / ".state/finalization.json")
    partial_cleanup = load(ROOT / PRESSURE / "cleanup/result.json")
    if not (
        partial_final.get("phase") == "finished"
        and partial_final.get("kind") == "user-terminated-partial"
        and partial_final.get("jobCompleteAsRequested") is True
        and partial_final.get("measurementComplete") is False
        and partial_final.get("replayCaughtUp") is False
        and partial_final.get("cleanupComplete") is True
        and partial_final.get("allSnapshotsPreserved") is True
        and partial_cleanup.get("allSnapshotsPreserved") is True
        and partial_cleanup.get("cleanupComplete") is True
    ):
        raise GateError("Pressure finalization does not prove accepted user-terminated partial plus completed cleanup")
    audit = load(ROOT / PRESSURE / ".state/pressure-three-hour-partial-audit.json")
    if not (
        audit.get("auditPassed") is True and audit.get("userTerminated") is True
        and audit.get("measurementComplete") is False and audit.get("replayCaughtUp") is False
        and audit.get("snapshotsPreserved") is True and audit.get("cleanupPassed") is True
    ):
        raise GateError("Independent pressure partial audit/cleanup proof is incomplete")
    return {
        "integration": integration,
        "fullRuns": full_results,
        "pressureFinalization": partial_final,
        "pressureCleanup": partial_cleanup,
        "pressureAudit": audit,
    }


def metadata_resource_groups(states: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Return only groups named by trusted per-stack state/cleanup metadata."""
    groups: dict[str, dict[str, Any]] = {}
    watchers: dict[str, dict[str, Any]] = {}

    def add_group(name: str, token: str, stack: str, role: str, purpose: str | None = None) -> None:
        if not name:
            return
        expected_name = f"rg-vacuum-only-{token}" if role == "main" else f"rg-vacuum-nodes-{token}" if role == "node" else None
        if expected_name and name.lower() != expected_name.lower():
            raise GateError(f"{stack}: {role} group name is not token-derived")
        if role == "retention" and name.lower() not in {
            f"rg-vacuum-snapshots-{token}".lower(), f"rg-vacuum-retained-{token}".lower()
        }:
            raise GateError(f"{stack}: retention group name is not token-derived")
        key = name.lower()
        value = {"name": name, "ownerToken": token, "stack": stack, "role": role,
                 "purpose": purpose, "expectedId": None}
        previous = groups.get(key)
        if previous and previous["ownerToken"].lower() != token.lower():
            raise GateError(f"Resource group {name} appears under conflicting owner tokens")
        groups[key] = previous or value

    for state in states:
        stack, token = state["_stack"], state["ownerToken"]
        add_group(state.get("resourceGroup"), token, stack, "main")
        node = state.get("nodeResourceGroup")
        if node:
            add_group(node, token, stack, "node")
        retention = state.get("snapshotRetentionResourceGroup")
        if not isinstance(retention, dict):
            retention = state.get("snapshotRetention")
        if isinstance(retention, dict):
            name = retention.get("name") or retention.get("resourceGroup")
            purpose = retention.get("purpose")
            declared_token = retention.get("ownerToken")
            if declared_token and declared_token.lower() != token.lower():
                raise GateError(f"{stack}: retention metadata owner token conflicts")
            if stack == "read-pressure":
                purpose = purpose or "retained-snapshots"
            elif stack == "100gib":
                # State records this RG's semantic purpose as
                # cnpg-pre-vacuum-snapshot-retention; the provisioner actually
                # tags the Azure RG retained-cnpg-volume-snapshots.
                purpose = "retained-cnpg-volume-snapshots"
            elif stack in ("prefetch-off", "prefetch-off-fpw-off"):
                purpose = purpose or "experiment-snapshots"
            add_group(name, token, stack, "retention", purpose)
        watcher = state.get("networkWatcher")
        if isinstance(watcher, dict) and watcher.get("owned") is True:
            wtoken = watcher.get("ownerToken")
            if not wtoken or wtoken.lower() != token.lower():
                raise GateError(f"{stack}: owned watcher token is absent or conflicts with stack")
            expected_group = f"rg-vacuum-watcher-{wtoken}"
            expected_name = f"vacuum-watcher-{wtoken[:12]}"
            if watcher.get("resourceGroup", "").lower() != expected_group.lower() or watcher.get("name", "").lower() != expected_name.lower():
                raise GateError(f"{stack}: watcher group/name does not match exact owner token")
            rid = str(watcher.get("id", ""))
            if not rid.lower().endswith(f"/resourcegroups/{expected_group.lower()}/providers/microsoft.network/networkwatchers/{expected_name.lower()}"):
                raise GateError(f"{stack}: watcher ID does not match its exact token group/name")
            watchers[rid.lower()] = {"id": rid, "name": expected_name, "resourceGroup": expected_group,
                                     "ownerToken": wtoken, "stack": stack,
                                     "location": watcher.get("location")}

    for record in (load(ROOT / rel / "cleanup/ownership.json") for rel in FULL.values() if (ROOT / rel / "cleanup/ownership.json").is_file()):
        retention = record.get("snapshotRetention")
        if isinstance(retention, dict):
            name = retention.get("resourceGroup")
            token = record.get("ownerToken")
            if name and token:
                purpose = retention.get("purpose")
                add_group(name, token, record.get("resourceGroup", "cleanup-ledger"), "retention", purpose)

    keeper_files = [ROOT / rel / "snapshot.json" for rel in FULL.values()]
    keeper_files += [ROOT / PRESSURE / "snapshot.json", OLD / "snapshot.json", PREFETCH_PARENT / "snapshot.json"]
    keeper_records = [load(p) for p in keeper_files if p.is_file()]
    if not keeper_records:
        raise GateError("No keeper snapshot metadata found")
    canonical = keeper_records[0]
    keys = ("resourceGroup", "snapshotId", "originalKeeperSnapshotId", "keeperOwnerToken", "sourceSnapshotId", "sourceOwnerToken")
    for record in keeper_records[1:]:
        if any(record.get(k) != canonical.get(k) for k in keys):
            raise GateError("Per-stack keeper snapshot metadata does not agree on the exact keeper/source identities")
    keeper = {
        "resourceGroup": canonical["resourceGroup"],
        "ownerToken": canonical["keeperOwnerToken"],
        # Only snapshotId is the current prepared keeper. The other two IDs
        # are historical provenance: the previous keeper was deliberately
        # replaced and deleted before the current all-snapshots preservation
        # phase, and its source copy disappeared with its old node RG.
        "snapshotIds": [canonical.get("snapshotId")],
        "historicalAbsentSources": [
            {"id": canonical.get("originalKeeperSnapshotId"), "kind": "replaced-previous-keeper",
             "evidencePath": "vacuum-only-repro/results/20261004T140103Z/proof/originaldeletion.json"},
            {"id": canonical.get("sourceSnapshotId"), "kind": "source-copy-from-previous-keeper-preparation",
             "evidencePath": ".parallel/scsi-vacuum-rerun-20261004/artifacts/cloudinventory-after.json"},
        ],
        "sourceOwnerToken": canonical.get("sourceOwnerToken"),
        "sourceResourceGroup": str(canonical.get("sourceSnapshotId", "")).split("/resourceGroups/")[-1].split("/")[0],
    }
    if not keeper["ownerToken"] or not keeper["snapshotIds"][0]:
        raise GateError("Keeper metadata lacks the exact current prepared snapshot ID/owner")
    if any(not x.get("id") for x in keeper["historicalAbsentSources"]):
        raise GateError("Keeper metadata lacks historical snapshot provenance IDs")
    if keeper["sourceResourceGroup"].lower() != f"rg-vacuum-nodes-{keeper['sourceOwnerToken']}".lower():
        raise GateError("Keeper source snapshot group does not match its recorded source-owner token")

    candidate_proofs = [
        load(PROJECT / "vacuum-only-repro/results/20261004T140103Z/proof/newAzuremetadata.json"),
        load(PROJECT / ".parallel/keeper-validation-20261004/artifacts/prepare-proof/candidate-after-validation-teardown.json"),
    ]
    keeper_uid = candidate_proofs[0].get("uniqueId")
    if (not keeper_uid or any(str(x.get("id", "")).lower() != keeper["snapshotIds"][0].lower()
                              or x.get("uniqueId") != keeper_uid for x in candidate_proofs)):
        raise GateError("Independent saved keeper records disagree on the current snapshot ID/uniqueId")
    keeper["expectedUniqueId"] = keeper_uid

    previous_delete = load(PROJECT / "vacuum-only-repro/results/20261004T140103Z/proof/originaldeletion.json")
    old_id, current_id = keeper["historicalAbsentSources"][0]["id"], keeper["snapshotIds"][0]
    if not (
        previous_delete.get("deletedSnapshotId", "").lower() == old_id.lower()
        and previous_delete.get("deleteIssuedByThisAttempt") is True
        and previous_delete.get("deleteVerifiedAbsent") is True
        and previous_delete.get("candidatePreserved") is True
        and previous_delete.get("candidateSnapshotId", "").lower() == current_id.lower()
    ):
        raise GateError("Historical previous-keeper deletion proof does not bind exact old/current IDs")
    old_source = keeper["historicalAbsentSources"][1]
    source_inventory = load(PROJECT / ".parallel/scsi-vacuum-rerun-20261004/artifacts/cloudinventory-after.json")
    absent_groups = source_inventory.get("experimentGroupsAbsent") or {}
    keeper_rows = source_inventory.get("keeperResourceGroupInventory") or []
    if not (
        absent_groups.get(keeper["sourceResourceGroup"]) is False
        and all(str(x.get("id", "")).lower() != old_source["id"].lower() for x in keeper_rows)
        and all(str(x.get("id", "")).lower() != old_id.lower() for x in keeper_rows)
        and any(str(x.get("id", "")).lower() == current_id.lower() for x in keeper_rows)
    ):
        raise GateError("Saved cloud inventory does not prove historical source/previous keeper absence and current keeper presence")
    exact_external_ids = {str(x).lower() for x in keeper["snapshotIds"]}
    for state in states:
        stack = Path(state["_stackPath"])
        ledger = stack / "cleanup/snapshots-after.json"
        if not ledger.is_file():
            continue
        rows = load(ledger)
        if not isinstance(rows, list):
            raise GateError(f"Cleanup snapshot ledger is not a list: {ledger}")
        token = state["ownerToken"]
        retention = state.get("snapshotRetentionResourceGroup")
        if not isinstance(retention, dict):
            retention = state.get("snapshotRetention")
        retention_name = ((retention or {}).get("name") or (retention or {}).get("resourceGroup")) if isinstance(retention, dict) else None
        for item in rows:
            sid = str(item.get("id", ""))
            if sid.lower() in exact_external_ids:
                continue
            rg = str(item.get("resourceGroup", ""))
            if not rg:
                raise GateError(f"Cleanup ledger snapshot lacks resourceGroup: {sid}")
            if state.get("resourceGroup") and rg.lower() == state["resourceGroup"].lower():
                add_group(rg, token, state["_stack"], "main")
            elif state.get("nodeResourceGroup") and rg.lower() == state["nodeResourceGroup"].lower():
                add_group(rg, token, state["_stack"], "node")
            elif retention_name and rg.lower() == str(retention_name).lower():
                purpose = ((retention or {}).get("purpose") if isinstance(retention, dict) else None)
                if not purpose:
                    purpose = {"read-pressure": "retained-snapshots", "100gib": "retained-cnpg-volume-snapshots",
                               "prefetch-off": "experiment-snapshots", "prefetch-off-fpw-off": "experiment-snapshots"}.get(state["_stack"])
                add_group(rg, token, state["_stack"], "retention", purpose)
            else:
                raise GateError(f"Snapshot ledger refers to an RG not named by stack state or keeper metadata: {rg}")
    return groups, {"watchers": watchers, "keeper": keeper}


def azure_json(subscription: str, *args: str, timeout_s: int = 300) -> Any:
    proc = subprocess.run(
        ["az", *args, "--subscription", subscription, "-o", "json"],
        capture_output=True, text=True, check=False, timeout=timeout_s,
    )
    if proc.returncode:
        raise GateError(f"Azure CLI failed for {args!r}: {proc.stderr[-1500:]}")
    try:
        return json.loads(proc.stdout) if proc.stdout.strip() else None
    except json.JSONDecodeError as exc:
        raise GateError(f"Azure CLI returned invalid JSON for {args!r}: {exc}") from exc


def azure_none(subscription: str, *args: str, timeout_s: int = 300) -> None:
    proc = subprocess.run(["az", *args, "--subscription", subscription, "-o", "none"],
                          capture_output=True, text=True, check=False, timeout=timeout_s)
    if proc.returncode:
        raise GateError(f"Azure mutation failed for {args!r}: {proc.stderr[-1500:]}")


def group_exists(subscription: str, name: str) -> bool:
    proc = subprocess.run(["az", "group", "exists", "-n", name, "--subscription", subscription, "-o", "tsv"],
                          capture_output=True, text=True, check=False, timeout=300)
    if proc.returncode:
        raise GateError(f"Cannot determine whether resource group exists: {name}: {proc.stderr[-1000:]}")
    value = proc.stdout.strip().lower()
    if value not in ("true", "false"):
        raise GateError(f"Unexpected az group exists response for {name}: {value!r}")
    return value == "true"


def normalize_group_metadata(subscription: str, groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    verified = []
    for key, spec in groups.items():
        if not group_exists(subscription, spec["name"]):
            verified.append({**spec, "exists": False, "resources": []})
            continue
        group = azure_json(subscription, "group", "show", "-n", spec["name"])
        tags = group.get("tags") or {}
        if str(tags.get("reprotoken", "")).lower() != spec["ownerToken"].lower():
            raise GateError(f"Refusing to inspect resource group without exact matching reprotoken: {spec['name']}")
        if (group.get("name", "").lower() != spec["name"].lower()
                or f"/resourcegroups/{spec['name']}".lower() not in group.get("id", "").lower()):
            raise GateError(f"Azure group identity mismatch for {spec['name']}")
        if spec["role"] == "main" and tags.get("managed-by") != "vacuum-only-repro":
            raise GateError(f"Main resource group lacks exact managed-by ownership tag: {spec['name']}")
        if spec["role"] == "retention":
            if tags.get("managed-by") != "vacuum-only-repro":
                raise GateError(f"Retention resource group lacks managed-by ownership tag: {spec['name']}")
            if spec.get("purpose") and tags.get("reproPurpose") != spec["purpose"]:
                raise GateError(f"Retention resource group purpose differs from local ownership record: {spec['name']}")
        resources = azure_json(subscription, "resource", "list", "-g", spec["name"])
        verified.append({**spec, "exists": True, "group": group, "resources": resources})
    return verified


def verify_watcher(subscription: str, watcher: dict[str, Any]) -> dict[str, Any]:
    group = watcher["resourceGroup"]
    if not group_exists(subscription, group):
        return {**watcher, "exists": False, "status": "already-absent"}
    rg = azure_json(subscription, "group", "show", "-n", group)
    tags = rg.get("tags") or {}
    if (rg.get("name", "").lower() != group.lower()
            or str(tags.get("reprotoken", "")).lower() != watcher["ownerToken"].lower()
            or tags.get("reproPurpose") != "owned-network-watcher"
            or tags.get("managed-by") != "vacuum-only-repro"):
        raise GateError(f"Watcher group ownership/purpose does not verify: {group}")
    resources = azure_json(subscription, "resource", "list", "-g", group)
    exact = [r for r in resources if str(r.get("id", "")).lower() == watcher["id"].lower()]
    if len(resources) != 1 or len(exact) != 1:
        raise GateError(f"Watcher group contains unexpected resources: {group}")
    resource = azure_json(subscription, "resource", "show", "--ids", watcher["id"])
    rtags = resource.get("tags") or {}
    if (str(resource.get("id", "")).lower() != watcher["id"].lower()
            or str(rtags.get("reprotoken", "")).lower() != watcher["ownerToken"].lower()
            or rtags.get("reproPurpose") != "owned-vacuum-repro-watcher"
            or str(resource.get("type", "")).lower() != "microsoft.network/networkwatchers"):
        raise GateError(f"Watcher resource ownership does not verify: {watcher['id']}")
    return {**watcher, "exists": True, "status": "verified", "resource": resource, "groupTags": tags}


def keeper_snapshot_refs(subscription: str, keeper: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    expectations = [(keeper["snapshotIds"][0], keeper["resourceGroup"], keeper["ownerToken"], "prepared-keeper")]
    for sid, expected_rg, token, label in expectations:
        expected_prefix = f"/subscriptions/{subscription}/resourcegroups/{expected_rg}/providers/microsoft.compute/snapshots/".lower()
        if not sid.lower().startswith(expected_prefix) or not expected_rg or not token:
            raise GateError(f"Keeper metadata does not bind an exact resource ID: {label}")
        group = azure_json(subscription, "group", "show", "-n", expected_rg)
        if (group.get("name", "").lower() != expected_rg.lower()
                or str((group.get("tags") or {}).get("reprotoken", "")).lower() != token.lower()):
            raise GateError(f"Exact {label} snapshot group ownership does not match recorded token")
        snap = azure_json(subscription, "snapshot", "show", "--ids", sid)
        actual_rg = str(snap.get("resourceGroup", ""))
        if (str(snap.get("id", "")).lower() != sid.lower()
                or actual_rg.lower() != expected_rg.lower()
                or str((snap.get("tags") or {}).get("reprotoken", "")).lower() != token.lower()
                or snap.get("provisioningState") != "Succeeded"
                or snap.get("uniqueId") != keeper.get("expectedUniqueId")):
            raise GateError(f"Exact {label} snapshot identity/owner/state did not match snapshot.json")
        result.append(snap)
    return result


def _arm_snapshot_group(snapshot_id: str) -> str:
    lower_id = snapshot_id.lower()
    marker = "/providers/microsoft.compute/snapshots/"
    marker_at = lower_id.find(marker)
    rg_marker = "/resourcegroups/"
    rg_at = lower_id.find(rg_marker)
    if marker_at <= 0 or rg_at <= 0 or rg_at >= marker_at:
        raise GateError(f"Snapshot ID is not an unambiguous ARM snapshot resource ID: {snapshot_id}")
    return snapshot_id[rg_at + len(rg_marker):marker_at].rstrip("/")


def _snapshot_tag(tags: dict[str, Any], key: str) -> Any:
    matches = [value for name, value in tags.items() if str(name).lower() == key.lower()]
    if len(matches) > 1 and any(value != matches[0] for value in matches[1:]):
        raise GateError(f"Snapshot has conflicting {key} tag spellings")
    return matches[0] if matches else None


def _snapshot_source_disk(snapshot: dict[str, Any]) -> str:
    tags = snapshot.get("tags") or {}
    tag_source = str(_snapshot_tag(tags, "source_volume_id") or "")
    creation_source = str((snapshot.get("creationData") or {}).get("sourceResourceId", ""))
    explicit_source = str(snapshot.get("sourceDiskId") or "")
    present = [x.lower() for x in (tag_source, creation_source, explicit_source) if x]
    if present and any(x != present[0] for x in present[1:]):
        raise GateError(f"Snapshot has conflicting source-disk metadata: {snapshot.get('id')}")
    return tag_source or creation_source or explicit_source


def snapshot_ledger_expectations(states: list[dict[str, Any]], group_specs: dict[str, dict[str, Any]],
                                separately_verified_ids: set[str] | None = None) -> dict[str, dict[str, Any]]:
    """Index exact earlier cleanup-ledger identities for the tokenless CSI case.

    Azure's CSI driver does not consistently copy the reprotoken onto native
    snapshots.  A missing snapshot token is acceptable only when the earlier
    per-stack ledger binds the exact snapshot ID, uniqueId, RG, source disk and
    CSI creator tag.  Resource-group token ownership is verified separately
    against Azure immediately before use.
    """
    expected: dict[str, dict[str, Any]] = {}
    separately_verified = {x.lower() for x in (separately_verified_ids or set())}
    for state in states:
        ledger = Path(state["_stackPath"]) / "cleanup/snapshots-after.json"
        if not ledger.is_file():
            continue
        rows = load(ledger)
        if not isinstance(rows, list):
            raise GateError(f"Snapshot cleanup ledger is not a list: {ledger}")
        for row in rows:
            if not isinstance(row, dict):
                raise GateError(f"Malformed snapshot cleanup ledger row: {ledger}")
            sid = str(row.get("id", ""))
            if sid.lower() in separately_verified:
                # The prepared keeper is not discovered through an owned RG
                # resource listing; keeper_snapshot_refs verifies its exact
                # ID, token, RG, state and uniqueId independently.
                continue
            uid = str(row.get("uniqueId", ""))
            rg = str(row.get("resourceGroup", ""))
            if not sid or not uid or not rg:
                raise GateError(f"Snapshot cleanup ledger row lacks ID/uniqueId/resourceGroup: {ledger}")
            if row.get("provisioningState") != "Succeeded":
                raise GateError(f"Snapshot cleanup ledger row was not captured Succeeded: {sid}")
            id_group = _arm_snapshot_group(sid)
            if id_group.lower() != rg.lower():
                raise GateError(f"Snapshot cleanup ledger RG disagrees with resource ID: {sid}")
            group_spec = group_specs.get(rg.lower())
            if not group_spec or not group_spec.get("ownerToken"):
                raise GateError(f"Snapshot cleanup ledger RG is not bound to an owned group: {rg}")
            tags = row.get("tags") or {}
            source_disk = _snapshot_source_disk(row)
            proof = {
                "id": sid,
                "uniqueId": uid,
                "resourceGroup": rg,
                "ownerToken": group_spec["ownerToken"],
                "sourceDiskId": source_disk,
                "csiCreator": str(_snapshot_tag(tags, "k8s-azure-created-by") or ""),
                "ledgerPath": str(ledger),
            }
            key = sid.lower()
            previous = expected.get(key)
            if previous and any(previous.get(k) != proof.get(k) for k in
                                ("uniqueId", "resourceGroup", "ownerToken", "sourceDiskId", "csiCreator")):
                raise GateError(f"Conflicting per-stack ledger proofs for snapshot: {sid}")
            expected[key] = previous or proof
    return expected


def verify_snapshot_in_owned_group(snapshot: dict[str, Any], sid: str, group: dict[str, Any],
                                   ledger_proof: dict[str, Any] | None) -> None:
    tags = snapshot.get("tags") or {}
    actual_id = str(snapshot.get("id", ""))
    actual_rg = str(snapshot.get("resourceGroup", ""))
    owner_token = str(group["ownerToken"])
    if (actual_id.lower() != sid.lower()
            or actual_rg.lower() != group["name"].lower()
            or _arm_snapshot_group(actual_id).lower() != group["name"].lower()
            or snapshot.get("provisioningState") != "Succeeded"):
        raise GateError(f"Owned snapshot identity/group/state mismatch: {sid}")

    # A present reprotoken is authoritative and must match.  Never treat a
    # conflicting value as if the tag were merely absent.
    token_present = any(str(key).lower() == "reprotoken" for key in tags)
    if token_present:
        if str(_snapshot_tag(tags, "reprotoken") or "").lower() != owner_token.lower():
            raise GateError(f"Owned snapshot has a conflicting reprotoken: {sid}")
        return

    if not ledger_proof:
        raise GateError(f"Tokenless snapshot has no exact prior cleanup-ledger proof: {sid}")
    if (ledger_proof.get("id", "").lower() != actual_id.lower()
            or ledger_proof.get("uniqueId") != snapshot.get("uniqueId")
            or ledger_proof.get("resourceGroup", "").lower() != actual_rg.lower()
            or ledger_proof.get("ownerToken", "").lower() != owner_token.lower()
            or not ledger_proof.get("sourceDiskId")
            or ledger_proof.get("csiCreator") != "kubernetes-azure-dd"
            or str(_snapshot_tag(tags, "k8s-azure-created-by") or "") != "kubernetes-azure-dd"
            or _snapshot_source_disk(snapshot).lower() != ledger_proof["sourceDiskId"].lower()):
        raise GateError(f"Tokenless CSI snapshot does not match its exact prior ledger proof: {sid}")


def snapshots_in_owned_groups(subscription: str, verified_groups: list[dict[str, Any]],
                              ledger_expectations: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    items = []
    for group in verified_groups:
        if not group["exists"]:
            continue
        for resource in group["resources"]:
            if str(resource.get("type", "")).lower() == "microsoft.compute/snapshots":
                sid = resource.get("id")
                snapshot = azure_json(subscription, "snapshot", "show", "--ids", sid)
                verify_snapshot_in_owned_group(snapshot, sid, group,
                                               ledger_expectations.get(str(sid).lower()))
                items.append(snapshot)
    return items


def snapshot_scope_from_lock(lock: dict[str, Any]) -> str:
    """Derive a lock's protected scope from its ARM resource ID.

    ``az lock list`` can omit the convenience ``scope`` property.  The lock
    ID still carries the scope as the prefix before the Authorization/locks
    child path.  Derive that value and, when Azure does return ``scope``,
    require it to agree so an inherited parent lock can never masquerade as a
    direct snapshot lock.
    """
    lock_id = str(lock.get("id", ""))
    marker = "/providers/microsoft.authorization/locks/"
    lower_id = lock_id.lower()
    marker_at = lower_id.find(marker)
    if not lock_id or marker_at <= 0 or lower_id.find(marker, marker_at + len(marker)) >= 0:
        raise GateError(f"Azure lock row has no unambiguous ARM lock ID: {lock!r}")
    scope = lock_id[:marker_at].rstrip("/")
    if not scope.lower().startswith("/subscriptions/"):
        raise GateError(f"Azure lock ID does not contain a subscription-scoped resource: {lock_id}")
    explicit_scope = lock.get("scope")
    if explicit_scope and str(explicit_scope).rstrip("/").lower() != scope.lower():
        raise GateError(f"Azure lock scope conflicts with its ARM lock ID: {lock_id}")
    return scope


def exact_snapshot_locks(locks: list[dict[str, Any]], snapshot_id: str) -> list[dict[str, Any]]:
    direct = []
    for lock in locks:
        derived_scope = snapshot_scope_from_lock(lock)
        if derived_scope.lower() == snapshot_id.rstrip("/").lower():
            direct.append({**lock, "scope": derived_scope})
    return direct


def ensure_snapshot_lock(subscription: str, snapshot: dict[str, Any], create_missing: bool) -> dict[str, Any]:
    rg = snapshot.get("resourceGroup")
    name = snapshot.get("name")
    sid = snapshot.get("id", "")
    if not (rg and name and sid):
        raise GateError("Snapshot object lacks ID, resource group, or name")
    locks = azure_json(subscription, "lock", "list", "--resource-group", rg,
                       "--resource-type", "Microsoft.Compute/snapshots", "--resource-name", name)
    direct = exact_snapshot_locks(locks, sid)
    protected = [x for x in direct if x.get("level") == "CanNotDelete"]
    if not protected:
        if not create_missing:
            raise GateError(f"Snapshot lacks an exact-scope CanNotDelete lock: {sid}")
        azure_none(subscription, "lock", "create", "--name", LOCK_NAME, "--lock-type", "CanNotDelete",
                   "--resource-group", rg, "--resource-type", "Microsoft.Compute/snapshots",
                   "--resource-name", name, "--notes", LOCK_NOTES)
        locks = azure_json(subscription, "lock", "list", "--resource-group", rg,
                           "--resource-type", "Microsoft.Compute/snapshots", "--resource-name", name)
        direct = exact_snapshot_locks(locks, sid)
        protected = [x for x in direct if x.get("level") == "CanNotDelete"]
    if not protected:
        raise GateError(f"Unable to verify exact-scope snapshot lock after create: {sid}")
    lock = protected[0]
    return {"name": lock.get("name"), "level": lock.get("level"), "scope": lock.get("scope"),
            "id": lock.get("id")}


def attach_and_verify_locks(subscription: str, snapshots: list[dict[str, Any]], create_missing: bool) -> list[dict[str, Any]]:
    output = []
    seen = set()
    for snapshot in snapshots:
        sid = str(snapshot.get("id", "")).lower()
        if sid in seen:
            continue
        seen.add(sid)
        lock = ensure_snapshot_lock(subscription, snapshot, create_missing)
        output.append({**snapshot, "lock": lock})
    return output


def audit_resources(verified_groups: list[dict[str, Any]], watcher_ids: set[str], snapshots: list[dict[str, Any]], states: list[dict[str, Any]], subscription: str) -> dict[str, Any]:
    residual_aks = []
    resource_records = []
    residual = []
    cluster_state_by_name = {}
    for state in states:
        if state["_stack"] in ("baseline", "older-source"):
            cluster_state_by_name[state["clusterName"].lower()] = state
    by_group = {g["name"].lower(): g for g in verified_groups}
    snap_by_group: dict[str, list[dict[str, Any]]] = {}
    for s in snapshots:
        snap_by_group.setdefault(str(s.get("resourceGroup", "")).lower(), []).append(s)

    for group in verified_groups:
        if not group["exists"]:
            continue
        for resource in group["resources"]:
            rid = str(resource.get("id", ""))
            rtype = str(resource.get("type", "")).lower()
            rname = str(resource.get("name", ""))
            resource_records.append({"resourceGroup": group["name"], "ownerStack": group["stack"],
                                     "role": group["role"], "id": rid, "name": rname,
                                     "type": resource.get("type"), "tags": resource.get("tags") or {}})
            if rtype == "microsoft.compute/snapshots":
                continue
            if rtype == "microsoft.containerservice/managedclusters":
                state = cluster_state_by_name.get(rname.lower())
                if (not state or group["role"] != "main"
                        or group["name"].lower() != state["resourceGroup"].lower()):
                    raise GateError(f"Unexpected/owned AKS resource outside the two allowed stopped stacks: {rid}")
                actual = azure_json(subscription, "aks", "show", "-g", state["resourceGroup"], "-n", state["clusterName"])
                if (str((actual.get("tags") or {}).get("reprotoken", "")).lower() != state["ownerToken"].lower()
                        or actual.get("powerState", {}).get("code") != "Stopped"
                        or actual.get("provisioningState") != "Succeeded"):
                    raise GateError(f"Allowed AKS residual is not exact-token stopped/succeeded: {rid}")
                node_group = str(state.get("nodeResourceGroup", "")).lower()
                protected = bool(snap_by_group.get(node_group)) and all(
                    s.get("provisioningState") == "Succeeded"
                    and s.get("lock", {}).get("level") == "CanNotDelete"
                    for s in snap_by_group.get(node_group, [])
                )
                if not protected:
                    raise GateError(f"Stopped AKS residual lacks the protected snapshots that require preservation: {rid}")
                residual_aks.append({"stack": state["_stack"], "name": rname, "resourceId": rid,
                                     "powerState": "Stopped", "provisioningState": "Succeeded",
                                     "snapshotsProtected": True})
                residual.append(rid)
                continue
            raise GateError(f"Unexpected non-snapshot Azure resource remains in owned RG {group['name']}: {rtype} {rid}")

    for watcher_id in watcher_ids:
        raise GateError(f"Owned watcher still remains after shared watcher cleanup: {watcher_id}")
    got = {x["stack"] for x in residual_aks}
    if got != EXPECTED_RESIDUAL_STACKS or len(residual_aks) != 2:
        raise GateError(f"Expected exactly baseline and older-source stopped AKS residuals; found {sorted(got)}")
    return {
        "schemaVersion": 1,
        "auditedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "allWorkerVmssDisksStorageAndNetworkRemoved": True,
        "residualAks": residual_aks,
        "resources": resource_records,
        "remainingNonSnapshotResources": residual,
        "ownedResourceGroups": [{"name": g["name"], "ownerToken": g["ownerToken"], "role": g["role"], "exists": g["exists"]} for g in verified_groups],
    }


def assert_no_final_receipts() -> None:
    for name in (
        "all-snapshots-before-shared-cleanup.json", "all-snapshots-after-shared-cleanup.json",
        "shared-cleanup.json", "final-owned-snapshot-inventory.json",
        "snapshot-locks-after-shared-cleanup.json", "final-owned-resource-audit.json",
    ):
        if (ROOT / name).exists():
            raise GateError(f"Refusing to overwrite existing final receipt: {ROOT / name}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="after all gates, lock owned snapshots and delete only exact owned watcher groups")
    args = parser.parse_args()
    try:
        local = local_cleanup_gates()
        states = selected_states()
        groups, extras = metadata_resource_groups(states)
        watcher_plan = sorted(extras["watchers"].values(), key=lambda x: x["id"].lower())
        if not args.apply:
            print(json.dumps({
                "localGatesReadyForSharedCleanupPlan": True,
                "finalCleanupComplete": False,
                "ownershipReadbackPerformed": False,
                "mode": "read-only local gate; no Azure calls and no output files written",
                "fullRuns": [x["stack"] for x in local["fullRuns"]],
                "pressurePartialAcceptedAsIncomplete": True,
                "ownedResourceGroupCount": len(groups),
                "exactOwnedWatchersToCheckAndRemove": watcher_plan,
                "preservedKeeperSnapshotIds": extras["keeper"]["snapshotIds"],
                "preservedKeeperSnapshotUniqueId": extras["keeper"]["expectedUniqueId"],
                "historicalAbsentSources": extras["keeper"]["historicalAbsentSources"],
                "finalReceiptPaths": [
                    "all-snapshots-before-shared-cleanup.json", "all-snapshots-after-shared-cleanup.json",
                    "shared-cleanup.json", "final-owned-snapshot-inventory.json",
                    "snapshot-locks-after-shared-cleanup.json", "final-owned-resource-audit.json",
                ],
            }, indent=2))
            return 0

        assert_no_final_receipts()
        subscription = states[0]["subscriptionId"]
        lock_path = ROOT / ".state/shared-cleanup-v2.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+") as lock_stream:
            try:
                fcntl.flock(lock_stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise GateError("Another shared cleanup v2 process holds the lock") from exc
            # Re-check the dynamic result gate and all ownership tags immediately
            # before the only authorized mutation (owned watcher group removal).
            local = local_cleanup_gates()
            verified_groups = normalize_group_metadata(subscription, groups)
            verified_watchers = [verify_watcher(subscription, w) for w in watcher_plan]
            global_before = azure_json(subscription, "snapshot", "list")
            before_ids = {str(x.get("uniqueId")) for x in global_before if x.get("uniqueId")}
            if len(before_ids) != len(global_before):
                raise GateError("Subscription snapshot inventory has missing/duplicate uniqueId values")

            ledger_expectations = snapshot_ledger_expectations(
                states, groups, set(extras["keeper"]["snapshotIds"])
            )
            owned = snapshots_in_owned_groups(subscription, verified_groups, ledger_expectations)
            owned.extend(keeper_snapshot_refs(subscription, extras["keeper"]))
            owned = attach_and_verify_locks(subscription, owned, create_missing=True)
            owned_by_id = {str(x["id"]).lower(): x for x in owned}
            if len(owned_by_id) != len(owned):
                # Duplicate discovery is acceptable only when same Azure object
                # and same uniqueId; normalize the exact duplicates here.
                dedup = {}
                for item in owned:
                    key = item["id"].lower()
                    if key in dedup and dedup[key].get("uniqueId") != item.get("uniqueId"):
                        raise GateError(f"Duplicate owned snapshot ID has conflicting uniqueId: {key}")
                    dedup[key] = item
                owned = list(dedup.values())
                owned_by_id = {str(x["id"]).lower(): x for x in owned}
            missing_before = [x["id"] for x in owned if str(x.get("uniqueId")) not in before_ids]
            if missing_before:
                raise GateError("Owned snapshots were not present before cleanup: " + ", ".join(missing_before))

            # Cross-check every stack cleanup ledger and the three exact keeper
            # IDs against the bounded owner inventory before touching watchers.
            ledger_paths = [ROOT / rel / "cleanup/snapshots-after.json" for rel in FULL.values()]
            ledger_paths += [ROOT / PRESSURE / "cleanup/snapshots-after.json", OLD / "cleanup/snapshots-after.json"]
            ledger_snapshots = []
            for path in ledger_paths:
                rows = load(path)
                if not isinstance(rows, list):
                    raise GateError(f"Snapshot cleanup ledger is not a list: {path}")
                ledger_snapshots.extend(rows)
            for row in ledger_snapshots:
                item = owned_by_id.get(str(row.get("id", "")).lower())
                if not item or item.get("uniqueId") != row.get("uniqueId"):
                    raise GateError(f"A per-stack retained snapshot is absent/changed from the bounded inventory: {row.get('id')}")

            # No group deletion occurs before ownership+resource enumeration.
            resource_audit_before = audit_resources(verified_groups, set(), owned, states, subscription)
            attempt = {
                "schemaVersion": 1, "status": "in-progress", "startedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "actions": [], "snapshotUniqueIdsBefore": sorted(before_ids),
                "ownedSnapshotUniqueIds": sorted(str(x.get("uniqueId")) for x in owned),
            }
            attempt_path = ROOT / ".state/shared-cleanup-v2-attempt.json"
            write_json(attempt_path, attempt)
            watcher_actions = []
            for watcher in verified_watchers:
                if not watcher["exists"]:
                    watcher_actions.append({"id": watcher["id"], "status": "already-absent"})
                    continue
                if group_exists(subscription, watcher["resourceGroup"]):
                    azure_none(subscription, "group", "delete", "-n", watcher["resourceGroup"], "--yes", "--no-wait")
                    deadline = time.monotonic() + 1800
                    while group_exists(subscription, watcher["resourceGroup"]) and time.monotonic() < deadline:
                        time.sleep(10)
                    if group_exists(subscription, watcher["resourceGroup"]):
                        raise GateError(f"Owned watcher group deletion timed out: {watcher['resourceGroup']}")
                watcher_actions.append({"id": watcher["id"], "status": "deleted"})
                attempt["actions"] = watcher_actions
                write_json(attempt_path, attempt)

            global_after = azure_json(subscription, "snapshot", "list")
            after_ids = {str(x.get("uniqueId")) for x in global_after if x.get("uniqueId")}
            if len(after_ids) != len(global_after):
                raise GateError("Post-cleanup snapshot inventory has missing/duplicate uniqueId values")
            if not before_ids.issubset(after_ids):
                raise GateError("Shared cleanup caused a previously listed snapshot uniqueId to disappear")
            global_after_by_id = {str(x.get("id", "")).lower(): x for x in global_after}
            for sid, owned_item in owned_by_id.items():
                current = global_after_by_id.get(sid)
                if not current or current.get("uniqueId") != owned_item.get("uniqueId") or current.get("provisioningState") != "Succeeded":
                    raise GateError(f"Owned snapshot identity/state changed during shared cleanup: {sid}")
            verified_groups_after = normalize_group_metadata(subscription, groups)
            remaining_watchers = []
            for watcher in watcher_plan:
                if group_exists(subscription, watcher["resourceGroup"]):
                    remaining_watchers.append(watcher["id"])
            if remaining_watchers:
                raise GateError("Owned watcher groups remain after cleanup: " + ", ".join(remaining_watchers))
            resource_audit = audit_resources(verified_groups_after, set(), owned, states, subscription)
            raw_locks = azure_json(subscription, "lock", "list")
            # Refresh each direct snapshot lock after the shared cleanup action.
            final_owned = attach_and_verify_locks(subscription, owned, create_missing=False)
            final_owned_by_id = {str(x["id"]).lower(): x for x in final_owned}
            for row in ledger_snapshots:
                current = final_owned_by_id.get(str(row.get("id", "")).lower())
                if not current or current.get("uniqueId") != row.get("uniqueId"):
                    raise GateError(f"Final lock inventory no longer matches retained stack ledger: {row.get('id')}")

            now = dt.datetime.now(dt.timezone.utc).isoformat()
            attempt.update(status="complete", finishedAtUtc=now, actions=watcher_actions)
            write_json(attempt_path, attempt)
            # Final receipts are emitted only after every independent fresh
            # identity, lock, resource and global-subset check has succeeded.
            write_json(ROOT / "all-snapshots-before-shared-cleanup.json", global_before)
            write_json(ROOT / "all-snapshots-after-shared-cleanup.json", global_after)
            write_json(ROOT / "final-owned-snapshot-inventory.json", {
                "schemaVersion": 1, "verifiedAtUtc": now, "snapshots": final_owned,
                "ownedResourceGroups": [{"name": g["name"], "ownerToken": g["ownerToken"], "stack": g["stack"], "role": g["role"]} for g in verified_groups_after],
                "protectedKeeperSnapshotIds": extras["keeper"]["snapshotIds"],
                "protectedKeeperSnapshotUniqueId": extras["keeper"]["expectedUniqueId"],
                "historicalAbsentSources": extras["keeper"]["historicalAbsentSources"],
            })
            write_json(ROOT / "snapshot-locks-after-shared-cleanup.json", raw_locks)
            write_json(ROOT / "final-owned-resource-audit.json", resource_audit)
            write_json(ROOT / "shared-cleanup.json", {
                "schemaVersion": 2, "apply": True, "complete": True,
                "allSnapshotsPreserved": True, "verifiedAtUtc": now,
                "snapshotCountBefore": len(global_before), "snapshotCountAfter": len(global_after),
                "beforeUniqueIds": sorted(before_ids), "afterUniqueIds": sorted(after_ids),
                "actions": watcher_actions,
                "resourceAuditPath": "final-owned-resource-audit.json",
                "snapshotInventoryPath": "final-owned-snapshot-inventory.json",
                "snapshotLockInventoryPath": "snapshot-locks-after-shared-cleanup.json",
            })
            print(json.dumps({"ready": True, "applied": True, "verifiedAtUtc": now,
                              "snapshotCountBefore": len(global_before), "snapshotCountAfter": len(global_after),
                              "ownedSnapshotCount": len(final_owned), "watcherActions": watcher_actions,
                              "residualAks": resource_audit["residualAks"]}, indent=2))
            return 0
    except GateError as exc:
        print(json.dumps({"ready": False, "applied": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
