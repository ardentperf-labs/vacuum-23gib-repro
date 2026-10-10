#!/usr/bin/env python3
"""Fail-closed, stack-scoped Azure copy hydration barrier.

This is the staged refinement. It is intentionally separate from the production
entry point until root integration and review.
"""

import concurrent.futures
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import subprocess
import urllib.request


# STAGED PATH NOTE: this file lives at recovery/hydration-refinement/. Its
# repository root is parents[2]. When integrated into the top-level production
# hydration-barrier.py, change this to Path(__file__).resolve().parent.
ROOT = Path(__file__).resolve().parents[2]
RUNS = ("baseline-23gib", "50gib", "100gib")
TARGET_HEAP_GIB = {"baseline-23gib": 23, "50gib": 50, "100gib": 100}
STAGED_RUNS = ("50gib", "100gib")
PREMIUM_SKU = "PremiumV2_LRS"
STAGE_PURPOSE = "premiumv2-source-stage"
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
SOURCE_STAGE_HELPER = Path("recovery/source-restore/stage-allowlisted-source.sh")
RETIREMENT_SCRIPT = Path("recovery/staging-retirement/retire_staging_disks.py")

STANDARD_COPY_LIMITATION = (
    "Standard-origin Copy snapshots can be Succeeded and Available while Azure "
    "does not expose background-copy completion. They are restore-available; "
    "their copy hydration is unproven. This gate makes no completion claim for "
    "those snapshots. CopyStart snapshots and snapshots sourced from measured "
    "PremiumV2 disks require an explicit 100 percent completion value."
)


def _canonical_id(value):
    return value.rstrip("/").lower() if isinstance(value, str) else ""


def _digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _digest_file(path):
    try:
        return _digest_bytes(path.read_bytes())
    except OSError as error:
        raise RuntimeError("Cannot read hashed evidence file: " + str(path)) from error


def _check_hash(value, label):
    if not isinstance(value, str) or not HEX_SHA256.fullmatch(value):
        raise RuntimeError(label + ": malformed SHA-256")


def _read_json(path, label):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(label + ": cannot read trusted JSON evidence") from error


def _repo_file(relative_path, label):
    if (not isinstance(relative_path, str) or not relative_path or
            Path(relative_path).is_absolute() or ".." in Path(relative_path).parts):
        raise RuntimeError(label + ": path must be repository-relative")
    candidate = ROOT / relative_path
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(ROOT.resolve(strict=True))
    except (OSError, ValueError) as error:
        raise RuntimeError(label + ": path escapes the repository or is missing") from error
    if candidate.is_symlink() or not resolved.is_file():
        raise RuntimeError(label + ": expected a regular non-symlink evidence file")
    return resolved


def _timestamp(value, label):
    if not isinstance(value, str) or not value:
        raise RuntimeError(label + ": missing UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RuntimeError(label + ": invalid timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError(label + ": timestamp must include a timezone")
    return parsed


def _check_digest(path, expected, label):
    _check_hash(expected, label)
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(label + ": expected a regular non-symlink file")
    actual = _digest_file(path)
    if actual != expected:
        raise RuntimeError(label + ": content hash changed")
    return actual


def _same_id(left, right):
    return _canonical_id(left) == _canonical_id(right) and bool(_canonical_id(left))


def _id_under(resource_id, group_id, provider_type):
    prefix = _canonical_id(group_id) + "/providers/microsoft.compute/" + provider_type + "/"
    return _canonical_id(resource_id).startswith(prefix)


def _copy_hash_rows(document, label):
    if not isinstance(document, dict) or not isinstance(document.get("snapshots"), list):
        raise RuntimeError(label + ": expected a snapshot audit object")
    rows = []
    identities = {}
    for item in document["snapshots"]:
        if not isinstance(item, dict):
            raise RuntimeError(label + ": malformed snapshot audit row")
        resource_id = item.get("id")
        unique_id = item.get("uniqueId")
        direct = item.get("directCanNotDelete")
        group = item.get("groupCanNotDelete")
        canonical_id = _canonical_id(resource_id)
        if not canonical_id or not isinstance(unique_id, str) or not unique_id:
            raise RuntimeError(label + ": snapshot identity is incomplete")
        if not isinstance(direct, bool) or not isinstance(group, bool):
            raise RuntimeError(label + ": snapshot protection evidence is incomplete")
        if not (direct or group):
            raise RuntimeError(label + ": an audited snapshot lacks CanNotDelete protection")
        if canonical_id in identities:
            raise RuntimeError(label + ": duplicate snapshot identity")
        identities[canonical_id] = unique_id
        rows.append([canonical_id, unique_id, bool(direct), bool(group)])
    rows.sort()
    fingerprint = _digest_bytes(json.dumps(rows, separators=(",", ":")).encode("utf-8"))
    return identities, fingerprint


def _load_snapshot_audit(retirement):
    audit = retirement.get("snapshotAudit")
    if not isinstance(audit, dict):
        raise RuntimeError("Staging retirement receipt lacks snapshot identity audits")
    for field in ("preservedPriorSnapshots", "allSnapshotsProtected", "unchangedIdentities"):
        if audit.get(field) is not True:
            raise RuntimeError("Snapshot audit did not confirm " + field)

    before_path = _repo_file(audit.get("beforePath"), "Snapshot audit beforePath")
    after_path = _repo_file(audit.get("afterPath"), "Snapshot audit afterPath")
    before_hash = _check_digest(before_path, audit.get("beforeSha256"), "Snapshot audit before")
    after_hash = _check_digest(after_path, audit.get("afterSha256"), "Snapshot audit after")
    before = _read_json(before_path, "Snapshot audit before")
    after = _read_json(after_path, "Snapshot audit after")
    before_ids, before_fingerprint = _copy_hash_rows(before, "Snapshot audit before")
    after_ids, after_fingerprint = _copy_hash_rows(after, "Snapshot audit after")

    if audit.get("beforeIdentityProtectionSha256") != before_fingerprint:
        raise RuntimeError("Snapshot audit before identity/protection fingerprint changed")
    if audit.get("afterIdentityProtectionSha256") != after_fingerprint:
        raise RuntimeError("Snapshot audit after identity/protection fingerprint changed")
    if before_fingerprint != after_fingerprint:
        raise RuntimeError("Snapshot IDs or CanNotDelete protection changed during staging-disk retirement")
    if before_ids != after_ids:
        raise RuntimeError("Snapshot identities changed during staging-disk retirement")

    return {
        "beforePath": audit["beforePath"],
        "afterPath": audit["afterPath"],
        "beforeSha256": before_hash,
        "afterSha256": after_hash,
        "beforeIdentityProtectionSha256": before_fingerprint,
        "afterIdentityProtectionSha256": after_fingerprint,
        "snapshotCount": len(after_ids),
        "beforeIdentities": before_ids,
        "identities": after_ids,
    }


def _state_context(name, preparation):
    stack_root = ROOT / name
    state_path = stack_root / ".state/state.json"
    manifest_path = stack_root / "source-manifest.json"
    process_path = stack_root / ".state/prepare-process.json"
    prepare_path = stack_root / ".state/prepare.json"
    script_path = stack_root / "02-prepare.sh"

    _check_digest(state_path, preparation.get("stateSha256"), name + " state")
    _check_digest(manifest_path, preparation.get("sourceManifestSha256"), name + " source manifest")
    _check_digest(process_path, preparation.get("prepareProcessSha256"), name + " prepare process")
    _check_digest(prepare_path, preparation.get("prepareMetadataSha256"), name + " prepare metadata")
    script_hash = _check_digest(script_path, preparation.get("prepareScriptSha256"), name + " prepare script")

    state = _read_json(state_path, name + " state")
    manifest = _read_json(manifest_path, name + " source manifest")
    process = _read_json(process_path, name + " prepare process")
    prepared = _read_json(prepare_path, name + " prepare metadata")

    owner = state.get("ownerToken")
    subscription = state.get("subscriptionId")
    node_rg = state.get("nodeResourceGroup")
    snapshot_rg_id = state.get("snapshotResourceGroupId")
    if (not isinstance(owner, str) or not re.fullmatch(r"[a-f0-9]{32}", owner) or
            not isinstance(subscription, str) or not subscription or
            state.get("status") != "ready" or
            not isinstance(node_rg, str) or not node_rg or
            not isinstance(state.get("snapshotResourceGroup"), str) or
            not state.get("snapshotResourceGroup") or
            not isinstance(snapshot_rg_id, str) or not snapshot_rg_id):
        raise RuntimeError(name + ": incomplete or malformed owned state")
    subscription_prefix = "/subscriptions/" + subscription + "/resourceGroups/"
    expected_snapshot_rg_id = subscription_prefix + state["snapshotResourceGroup"]
    manifest_snapshot_id = manifest.get("sourceSnapshotId", "")
    manifest_prefix = "/subscriptions/" + subscription + "/resourceGroups/"
    if (not _same_id(snapshot_rg_id, expected_snapshot_rg_id) or
            not isinstance(manifest_snapshot_id, str) or
            not _canonical_id(manifest_snapshot_id).startswith(
                _canonical_id(manifest_prefix) + "/") or
            "/providers/microsoft.compute/snapshots/" not in _canonical_id(manifest_snapshot_id)):
        raise RuntimeError(name + ": state or source manifest escaped its subscription")
    if manifest.get("subscriptionId") != subscription:
        raise RuntimeError(name + ": source manifest subscription changed")
    if not isinstance(manifest.get("ownerToken"), str) or not re.fullmatch(
            r"[a-f0-9]{32}", manifest["ownerToken"]):
        raise RuntimeError(name + ": source manifest owner token is malformed")
    if not isinstance(manifest.get("uniqueId"), str) or not manifest["uniqueId"]:
        raise RuntimeError(name + ": source manifest lacks snapshot uniqueId")
    if manifest.get("sku", {}).get("name") not in ("Standard_LRS", "Standard_ZRS"):
        raise RuntimeError(name + ": source manifest is not a Standard snapshot")

    if (process.get("stack") != name or process.get("phase") != "prepare" or
            process.get("status") != "complete" or process.get("exitCode") != 0):
        raise RuntimeError(name + ": preparation process is not complete with exit code zero")
    _timestamp(process.get("finishedAtUtc"), name + " prepare process")
    process_scripts = process.get("scriptSha256")
    if not isinstance(process_scripts, dict) or process_scripts.get("02-prepare.sh") != script_hash:
        raise RuntimeError(name + ": preparation script hash does not match its process receipt")

    expected_heap = TARGET_HEAP_GIB[name]
    if (prepared.get("status") != "complete" or prepared.get("fixtureTargetHeapGiB") != expected_heap or
            prepared.get("instances") != 2 or prepared.get("clientCreated") is not False or
            prepared.get("retainedSnapshotResourceGroup") != state.get("snapshotResourceGroup") or
            not _same_id(prepared.get("sourceSnapshotId"), manifest_snapshot_id)):
        raise RuntimeError(name + ": prepared metadata does not match its completed stack state")

    helper_hash = process.get("sourceStageHelperSha256")
    entry_helper_hash = preparation.get("sourceStageHelperSha256")
    if name in STAGED_RUNS:
        helper_path = ROOT / SOURCE_STAGE_HELPER
        actual_helper_hash = _digest_file(helper_path)
        if (not isinstance(helper_hash, str) or helper_hash != actual_helper_hash or
                entry_helper_hash != actual_helper_hash):
            raise RuntimeError(name + ": source-stage helper hash changed")
    elif helper_hash is not None or entry_helper_hash is not None:
        raise RuntimeError(name + ": baseline unexpectedly records a source-stage helper")

    return {
        "name": name,
        "state": state,
        "manifest": manifest,
        "process": process,
        "prepared": prepared,
        "statePath": state_path,
        "manifestPath": manifest_path,
        "prepareScriptSha256": script_hash,
        "sourceStageHelperSha256": helper_hash,
    }


def _stage_receipt_files(context):
    directory = ROOT / context["name"] / ".state/source-stage"
    if not directory.exists():
        return []
    return sorted(directory.glob("receipt-*.json"))


def _validate_stage_receipt(context, preparation, retired_entry):
    name = context["name"]
    path_value = preparation.get("sourceStageReceiptPath")
    receipt_hash = preparation.get("sourceStageReceiptSha256")
    if name not in STAGED_RUNS:
        if path_value is not None or receipt_hash is not None or retired_entry is not None:
            raise RuntimeError("Baseline stack must not claim a retired source stage")
        state = context["state"]
        if (state.get("snapshotSourceStagingDiskId") is not None or
                state.get("snapshotSourceStageReceiptPath") is not None or
                not _same_id(state.get("snapshotSourceCopyId"),
                             context["prepared"].get("restoreCopySnapshotId"))):
            raise RuntimeError("Baseline state unexpectedly claims a source stage or changed keeper copy")
        if _stage_receipt_files(context):
            raise RuntimeError("Baseline stack unexpectedly contains source-stage receipts")
        return None

    if not isinstance(path_value, str) or not isinstance(receipt_hash, str):
        raise RuntimeError(name + ": missing source-stage receipt path or hash")
    path = _repo_file(path_value, name + " source-stage receipt")
    expected_parent = (ROOT / name / ".state/source-stage").resolve(strict=True)
    if path.parent != expected_parent or not path.name.startswith("receipt-") or not path.name.endswith(".json"):
        raise RuntimeError(name + ": source-stage receipt path is outside the expected stack directory")
    matches = _stage_receipt_files(context)
    if len(matches) != 1 or matches[0].resolve(strict=True) != path:
        raise RuntimeError(name + ": source-stage receipt is missing or ambiguous")
    actual_hash = _check_digest(path, receipt_hash, name + " source-stage receipt")
    stage = _read_json(path, name + " source-stage receipt")
    state = context["state"]
    manifest = context["manifest"]
    owner = state["ownerToken"]
    subscription = state["subscriptionId"]

    if stage.get("version") != 1 or stage.get("status") != "ready":
        raise RuntimeError(name + ": source-stage receipt is not a ready v1 receipt")
    if stage.get("ownerToken") != owner or stage.get("subscriptionId") != subscription:
        raise RuntimeError(name + ": source-stage receipt owner or subscription changed")
    if (not _same_id(stage.get("sourceSnapshotId"), manifest.get("sourceSnapshotId")) or
            stage.get("sourceSnapshotUniqueId") != manifest.get("uniqueId")):
        raise RuntimeError(name + ": source-stage receipt source snapshot identity changed")
    inline_manifest = stage.get("sourceManifest")
    if (not isinstance(inline_manifest, dict) or
            not _same_id(inline_manifest.get("sourceSnapshotId"), manifest.get("sourceSnapshotId")) or
            inline_manifest.get("uniqueId") != manifest.get("uniqueId") or
            inline_manifest.get("ownerToken") != manifest.get("ownerToken") or
            inline_manifest.get("subscriptionId") != subscription):
        raise RuntimeError(name + ": source-stage receipt no longer matches the source manifest")

    stage_disk_id = stage.get("stagingDiskId")
    stage_disk_uid = stage.get("stagingDiskUniqueId")
    disk_metadata = stage.get("stagingDiskMetadata")
    if (not _id_under(stage_disk_id, "/subscriptions/" + subscription + "/resourceGroups/" +
                      state["nodeResourceGroup"], "disks") or
            not isinstance(stage_disk_uid, str) or not stage_disk_uid or
            not isinstance(disk_metadata, dict) or
            not _same_id(disk_metadata.get("id"), stage_disk_id) or
            disk_metadata.get("uniqueId") != stage_disk_uid or
            disk_metadata.get("sku", {}).get("name") != "Standard_LRS" or
            disk_metadata.get("provisioningState") != "Succeeded" or
            disk_metadata.get("diskState") != "Unattached" or
            disk_metadata.get("managedBy") is not None or
            disk_metadata.get("creationData", {}).get("createOption") != "Copy" or
            not _same_id(disk_metadata.get("creationData", {}).get("sourceResourceId"),
                         manifest.get("sourceSnapshotId")) or
            disk_metadata.get("creationData", {}).get("sourceUniqueId") != manifest.get("uniqueId") or
            disk_metadata.get("tags", {}).get("reprotoken") != owner or
            disk_metadata.get("tags", {}).get("reproPurpose") != STAGE_PURPOSE or
            not _same_id(disk_metadata.get("tags", {}).get("sourceSnapshotId"),
                         manifest.get("sourceSnapshotId"))):
        raise RuntimeError(name + ": immutable source-stage disk identity or lineage is invalid")

    source_metadata = stage.get("sourceSnapshotMetadata")
    if (not isinstance(source_metadata, dict) or
            not _same_id(source_metadata.get("id"), manifest.get("sourceSnapshotId")) or
            source_metadata.get("uniqueId") != manifest.get("uniqueId") or
            source_metadata.get("sku", {}).get("name") not in ("Standard_LRS", "Standard_ZRS") or
            source_metadata.get("provisioningState") != "Succeeded" or
            source_metadata.get("snapshotAccessState") != "Available" or
            source_metadata.get("tags", {}).get("reprotoken") != manifest.get("ownerToken")):
        raise RuntimeError(name + ": source-stage receipt lacks matching ready source snapshot identity")

    child_id = stage.get("restoreSnapshotId")
    child_uid = stage.get("restoreSnapshotUniqueId")
    child_metadata = stage.get("restoreSnapshotMetadata")
    if (not _id_under(child_id, state.get("snapshotResourceGroupId"), "snapshots") or
            not isinstance(child_uid, str) or not child_uid or
            not isinstance(child_metadata, dict) or
            not _same_id(child_metadata.get("id"), child_id) or
            child_metadata.get("uniqueId") != child_uid or
            child_metadata.get("sku", {}).get("name") != "Standard_LRS" or
            child_metadata.get("provisioningState") != "Succeeded" or
            child_metadata.get("snapshotAccessState") != "Available" or
            child_metadata.get("creationData", {}).get("createOption") != "Copy" or
            not _same_id(child_metadata.get("creationData", {}).get("sourceResourceId"), stage_disk_id) or
            child_metadata.get("creationData", {}).get("sourceUniqueId") != stage_disk_uid or
            child_metadata.get("tags", {}).get("reprotoken") != owner or
            child_metadata.get("tags", {}).get("reproPurpose") != "premiumv2-keeper-copy" or
            not _same_id(child_metadata.get("tags", {}).get("sourceSnapshotId"),
                         manifest.get("sourceSnapshotId")) or
            not _same_id(child_metadata.get("tags", {}).get("sourceDiskId"), stage_disk_id)):
        raise RuntimeError(name + ": source-stage child snapshot identity or lineage is invalid")

    suffix = stage.get("resourceSuffix")
    if not isinstance(suffix, str) or not re.fullmatch(r"[a-f0-9]{8}", suffix):
        raise RuntimeError(name + ": source-stage resource suffix is invalid")
    plan_path = ROOT / name / ".state/source-stage" / ("plan-" + suffix + ".json")
    plan = _read_json(plan_path, name + " source-stage plan")
    if (not _same_id(plan.get("sourceSnapshotId"), manifest.get("sourceSnapshotId")) or
            plan.get("sourceSnapshotUniqueId") != manifest.get("uniqueId") or
            not _same_id(plan.get("stagingDiskId"), stage_disk_id) or
            not _same_id(plan.get("ownedIncrementalSnapshotId"), child_id)):
        raise RuntimeError(name + ": source-stage plan no longer matches its immutable receipt")

    if not isinstance(retired_entry, dict):
        raise RuntimeError(name + ": missing retired staging disk receipt entry")
    expected_fields = {
        "id": stage_disk_id,
        "uniqueId": stage_disk_uid,
        "sourceSnapshotId": manifest["sourceSnapshotId"],
        "sourceSnapshotUniqueId": manifest["uniqueId"],
        "restoreSnapshotId": child_id,
        "restoreSnapshotUniqueId": child_uid,
    }
    for field, expected in expected_fields.items():
        actual = retired_entry.get(field)
        if field in {"id", "sourceSnapshotId", "restoreSnapshotId"}:
            equal = _same_id(actual, expected)
        else:
            equal = actual == expected
        if not equal:
            raise RuntimeError(name + ": retired staging disk " + field + " differs from its source receipt")
    if (retired_entry.get("stack") != name or retired_entry.get("absentVerified") is not True or
            retired_entry.get("sourceStageReceiptPath") != path_value or
            retired_entry.get("sourceStageReceiptSha256") != actual_hash):
        raise RuntimeError(name + ": retired staging disk absence or source receipt binding is invalid")
    state_stage_path = state.get("snapshotSourceStageReceiptPath")
    try:
        state_stage_path = Path(state_stage_path).resolve(strict=True)
    except (TypeError, OSError) as error:
        raise RuntimeError(name + ": state source-stage receipt path is missing") from error
    if (not _same_id(state.get("snapshotSourceStagingDiskId"), stage_disk_id) or
            not _same_id(state.get("snapshotSourceCopyId"), child_id) or
            state_stage_path != path):
        raise RuntimeError(name + ": state stage/source identities differ from the immutable stage receipt")
    if (not _same_id(context["prepared"].get("restoreCopySnapshotId"), child_id) or
            context["prepared"].get("sourceKind") != "incremental_copy"):
        raise RuntimeError(name + ": prepared metadata is not bound to the source-stage snapshot")

    return {
        "stack": name,
        "path": path_value,
        "sha256": actual_hash,
        "stagingDiskId": stage_disk_id,
        "stagingDiskUniqueId": stage_disk_uid,
        "sourceSnapshotId": manifest["sourceSnapshotId"],
        "sourceSnapshotUniqueId": manifest["uniqueId"],
        "restoreSnapshotId": child_id,
        "restoreSnapshotUniqueId": child_uid,
        "sourceSnapshotSku": source_metadata["sku"]["name"],
        "stageDiskSku": disk_metadata["sku"]["name"],
        "restoreSnapshotSku": child_metadata["sku"]["name"],
    }


def _load_retirement(ROOT_DIR):
    relative = "preflight/staging-retirement.json"
    receipt_path = ROOT_DIR / relative
    if receipt_path.is_symlink():
        raise RuntimeError("Staging retirement receipt must not be a symlink")
    if not receipt_path.exists():
        return None
    retirement = _read_json(receipt_path, "Staging retirement receipt")
    if not isinstance(retirement, dict):
        raise RuntimeError("Staging retirement receipt must be a JSON object")
    if retirement.get("phase") != "complete":
        return None
    if retirement.get("version") != 1:
        raise RuntimeError("Completed staging retirement receipt has an unsupported version")
    _timestamp(retirement.get("completedAtUtc"), "Staging retirement receipt")

    retirement_script_path = ROOT_DIR / RETIREMENT_SCRIPT
    retirement_script_hash = _check_digest(
        retirement_script_path, retirement.get("retirementScriptSha256"), "Staging retirement script")
    limitation = retirement.get("standardCopyLimitation")
    limitation_lower = limitation.lower() if isinstance(limitation, str) else ""
    if ("unobservable" not in limitation_lower or
            not any(phrase in limitation_lower for phrase in ("do not prove", "not proven", "not claimed"))):
        raise RuntimeError("Staging retirement receipt must state the Standard-copy limitation")

    preparation = retirement.get("preparation")
    if not isinstance(preparation, dict) or set(preparation) != set(RUNS):
        raise RuntimeError("Staging retirement preparation map must cover exactly the three stacks")
    contexts = {}
    for name in RUNS:
        item = preparation[name]
        if not isinstance(item, dict):
            raise RuntimeError(name + ": malformed preparation hash entry")
        for field in ("prepareProcessSha256", "prepareMetadataSha256", "prepareScriptSha256",
                      "stateSha256", "sourceManifestSha256"):
            _check_hash(item.get(field), name + " " + field)
        contexts[name] = _state_context(name, item)

    retired = retirement.get("retiredStagingDisks")
    if not isinstance(retired, list) or len(retired) != len(STAGED_RUNS):
        raise RuntimeError("Retirement receipt must bind exactly the 50-GiB and 100-GiB staging disks")
    retired_by_stack = {}
    for item in retired:
        if not isinstance(item, dict) or item.get("stack") not in STAGED_RUNS:
            raise RuntimeError("Retirement receipt contains an unexpected staging-disk entry")
        if item["stack"] in retired_by_stack:
            raise RuntimeError("Retirement receipt has duplicate staging-disk entries")
        retired_by_stack[item["stack"]] = item
    if set(retired_by_stack) != set(STAGED_RUNS):
        raise RuntimeError("Retirement receipt is missing a staged stack")

    snapshot_audit = _load_snapshot_audit(retirement)
    stage_contexts = {}
    for name in RUNS:
        prep = preparation[name]
        entry = retired_by_stack.get(name)
        stage_contexts[name] = _validate_stage_receipt(contexts[name], prep, entry)

    for name in STAGED_RUNS:
        stage = stage_contexts[name]
        for resource_field, uid_field in (("restoreSnapshotId", "restoreSnapshotUniqueId"),):
            canonical_id = _canonical_id(stage[resource_field])
            expected_uid = stage[uid_field]
            if (snapshot_audit["beforeIdentities"].get(canonical_id) != expected_uid or
                    snapshot_audit["identities"].get(canonical_id) != expected_uid):
                raise RuntimeError(name + ": source-stage child snapshot is absent or changed in preservation audit")

    measured = retirement.get("measuredPremiumV2Disks")
    if not isinstance(measured, list) or len(measured) != len(RUNS) * 4:
        raise RuntimeError("Retirement receipt must bind exactly four measured PremiumV2 disks per stack")
    measured_by_stack = {name: [] for name in RUNS}
    for item in measured:
        if not isinstance(item, dict) or item.get("stack") not in measured_by_stack:
            raise RuntimeError("Measured PremiumV2 disk receipt contains an unexpected stack")
        measured_by_stack[item["stack"]].append(item)
    if any(len(measured_by_stack[name]) != 4 for name in RUNS):
        raise RuntimeError("Measured PremiumV2 disk receipt must contain four entries per stack")
    return {
        "retirement": retirement,
        "retirementReceiptSha256": _digest_file(receipt_path),
        "retirementScriptSha256": retirement_script_hash,
        "preparation": preparation,
        "contexts": contexts,
        "stageContexts": stage_contexts,
        "retiredByStack": retired_by_stack,
        "snapshotAudit": snapshot_audit,
        "measuredByStack": measured_by_stack,
    }


def _write_barrier_receipt(receipt):
    destination = ROOT / "preflight/hydration-barrier.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return receipt


def _failed_copy_pins(name, owner, source_snapshot_id, snapshot_group_id):
    path = ROOT / name / ".state/known-failed-source-copies.json"
    data = _read_json(path, name + " known failed-copy pin inventory")
    if data.get("version") != 1 or not isinstance(data.get("snapshots"), list):
        raise RuntimeError(name + ": malformed known failed-copy pin inventory")
    pins = {}
    expected_group = _canonical_id(snapshot_group_id) + "/providers/microsoft.compute/snapshots/"
    for item in data["snapshots"]:
        resource_id = item.get("id", "")
        unique_id = item.get("uniqueId")
        if (not _canonical_id(resource_id).startswith(expected_group) or not unique_id or
                item.get("ownerToken") != owner or item.get("reprotoken") != owner or
                _canonical_id(item.get("sourceSnapshotId")) != _canonical_id(source_snapshot_id) or
                item.get("reproPurpose") != "premiumv2-keeper-copy" or
                item.get("provisioningState") != "Failed" or item.get("completionPercent") != 0.0):
            raise RuntimeError(name + ": invalid known failed-copy pin")
        key = _canonical_id(resource_id)
        if key in pins:
            raise RuntimeError(name + ": duplicate known failed-copy pin")
        pins[key] = item
    return pins


def _standard_origin_ids(contexts, stage_contexts):
    sources = set()
    standard_snapshots = set()
    for context in contexts.values():
        manifest = context["manifest"]
        source_id = _canonical_id(manifest.get("sourceSnapshotId"))
        if manifest.get("sku", {}).get("name") in ("Standard_LRS", "Standard_ZRS"):
            sources.add(source_id)
            standard_snapshots.add(source_id)
    for stage in stage_contexts.values():
        if stage is None:
            continue
        sources.add(_canonical_id(stage["stagingDiskId"]))
        standard_snapshots.add(_canonical_id(stage["restoreSnapshotId"]))
    return sources, standard_snapshots


def verify():
    retirement_context = _load_retirement(ROOT)
    if retirement_context is None:
        receipt_path = ROOT / "preflight/staging-retirement.json"
        try:
            state = _read_json(receipt_path, "Staging retirement receipt")
            phase = state.get("phase") if isinstance(state, dict) else None
        except RuntimeError:
            phase = "missing"
        reason = ("staging retirement receipt is missing" if phase == "missing" else
                  "staging retirement is not complete (phase=" + str(phase) + ")")
        return _write_barrier_receipt({
            "checkedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "phase": "waiting-for-staging-retirement",
            "complete": False,
            "pending": [{"reason": reason}],
            "stacks": [],
            "retirementReceiptSha256": None,
            "standardCopyLimitation": STANDARD_COPY_LIMITATION,
            "basis": "Hydration verification is held until the staging-retirement receipt reaches phase complete.",
        })
    contexts = retirement_context["contexts"]
    stage_contexts = retirement_context["stageContexts"]
    snapshot_audit = retirement_context["snapshotAudit"]
    measured_receipt = retirement_context["measuredByStack"]
    standard_source_ids, standard_source_snapshot_ids = _standard_origin_ids(contexts, stage_contexts)

    token = json.loads(subprocess.check_output([
        "az", "account", "get-access-token", "--resource", "https://management.azure.com/", "-o", "json"
    ], text=True, timeout=900))["accessToken"]

    def get(path):
        request = urllib.request.Request(
            "https://management.azure.com" + path,
            headers={"Authorization": "Bearer " + token},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def collection(path):
        body = get(path)
        if body.get("nextLink"):
            raise RuntimeError("Unexpected pagination in isolated experiment resource group")
        if not isinstance(body.get("value"), list):
            raise RuntimeError("ARM collection response is malformed")
        return body["value"]

    def inspect(name):
        context = contexts[name]
        state = context["state"]
        manifest = context["manifest"]
        owner = state["ownerToken"]
        subscription = state["subscriptionId"]
        node_rg = "/subscriptions/" + subscription + "/resourceGroups/" + state["nodeResourceGroup"]
        snapshot_rg = state["snapshotResourceGroupId"]
        source_snapshot_id = manifest["sourceSnapshotId"]
        stage = stage_contexts[name]

        if (not snapshot_rg.lower().startswith(
                ("/subscriptions/" + subscription + "/resourcegroups/").lower()) or
                len(snapshot_rg.rstrip("/").split("/")) != 5):
            raise RuntimeError(name + ": snapshot-group scope changed")
        for group in (node_rg, snapshot_rg):
            metadata = get(group + "?api-version=2024-03-01")
            if metadata.get("tags", {}).get("reprotoken") != owner:
                raise RuntimeError(name + ": live resource-group ownership changed")

        disks = collection(node_rg + "/providers/Microsoft.Compute/disks?api-version=2025-01-02")
        snapshots = collection(snapshot_rg + "/providers/Microsoft.Compute/snapshots?api-version=2025-01-02")
        pins = _failed_copy_pins(name, owner, source_snapshot_id, snapshot_rg)

        retired_entry = retirement_context["retiredByStack"].get(name)
        retired_stage_id = _canonical_id(retired_entry.get("id")) if retired_entry else ""
        premium_disks = []
        evidence = []
        pending = []
        seen_pins = set()
        seen_measured_receipt_ids = set()
        measured_source_uids = {}

        for disk in disks:
            props = disk.get("properties") or {}
            tags = disk.get("tags") or {}
            sku = (disk.get("sku") or {}).get("name")
            resource_id = disk.get("id", "")
            unique_id = props.get("uniqueId") or disk.get("uniqueId")
            option = (props.get("creationData") or {}).get("createOption")
            percent = props.get("completionPercent")
            provision = props.get("provisioningState")
            is_stage = tags.get("reproPurpose") == STAGE_PURPOSE
            disk_name = disk.get("name") or resource_id.rstrip("/").rsplit("/", 1)[-1]

            if not _id_under(resource_id, node_rg, "disks"):
                raise RuntimeError(name + ": disk escaped its owned node resource group")
            if is_stage and tags.get("reprotoken") != owner:
                raise RuntimeError(name + ": staging disk ownership changed")
            if _canonical_id(resource_id) == retired_stage_id and retired_stage_id:
                raise RuntimeError(name + ": retired source-stage disk reappeared in the live node group")
            if is_stage or disk_name.lower().startswith("restore-stage-"):
                raise RuntimeError(name + ": an unretired source-stage disk remains in the live node group")

            if provision == "Failed" or props.get("copyCompletionError") is not None:
                raise RuntimeError(name + ": disk copy failed: " + str(resource_id))

            if sku == PREMIUM_SKU:
                premium_disks.append(disk)
                sector = props.get("logicalSectorSize")
                if sector is None:
                    sector = (props.get("creationData") or {}).get("logicalSectorSize")
                size_gib = props.get("diskSizeGB")
                if size_gib is None:
                    size_gib = props.get("diskSizeGb")
                if (size_gib != 512 or props.get("diskIOPSReadWrite") != 3000 or
                        props.get("diskMBpsReadWrite") != 125 or str(sector) != "512" or
                        str(state["availabilityZone"]) not in [str(zone) for zone in disk.get("zones", [])]):
                    raise RuntimeError(name + ": Premium disk settings changed")
                if not isinstance(unique_id, str) or not unique_id:
                    raise RuntimeError(name + ": measured Premium disk lacks uniqueId")
                if option in ("Copy", "CopyStart"):
                    complete = provision == "Succeeded" and percent == 100
                elif option == "Empty":
                    complete = provision == "Succeeded" and percent in (None, 100)
                else:
                    complete = False
                if not complete:
                    pending.append({"id": resource_id, "reason": "measured PremiumV2 disk copy is not proven complete"})
                evidence.append({
                    "kind": "disk", "id": resource_id, "uniqueId": unique_id,
                    "sku": sku, "createOption": option, "state": provision,
                    "completionPercent": percent, "complete": complete,
                    "hydrationProven": complete and option in ("Copy", "CopyStart"),
                    "measuredPremiumV2Target": True, "sourceStage": False,
                })
            elif option in ("Copy", "CopyStart"):
                # Standard managed-disk copy progress is not a safe release signal,
                # even if Azure reports Succeeded and 100 percent.
                pending.append({"id": resource_id, "reason": "non-PremiumV2 copied auxiliary disk is not supported by this hydration monitor"})
                evidence.append({
                    "kind": "disk", "id": resource_id, "uniqueId": unique_id,
                    "sku": sku, "createOption": option, "state": provision,
                    "completionPercent": percent, "complete": False,
                    "hydrationProven": False,
                    "hydrationBasis": "unsupported-non-PremiumV2-copy-progress",
                    "measuredPremiumV2Target": False, "sourceStage": False,
                })
            else:
                ordinary_complete = provision == "Succeeded" and percent in (None, 100)
                if not ordinary_complete:
                    pending.append({"id": resource_id, "reason": "ordinary auxiliary disk is not provisioned"})
                evidence.append({
                    "kind": "disk", "id": resource_id, "uniqueId": unique_id,
                    "sku": sku, "createOption": option, "state": provision,
                    "completionPercent": percent, "complete": ordinary_complete,
                    "hydrationProven": None,
                    "hydrationBasis": "no-copy-option",
                    "measuredPremiumV2Target": False, "sourceStage": False,
                })

        if len(premium_disks) != 4:
            raise RuntimeError(name + ": expected exactly four Premium SSD v2 disks")

        receipt_disks = measured_receipt[name]
        receipt_ids = {}
        for row in receipt_disks:
            if not isinstance(row, dict) or not _id_under(row.get("id"), node_rg, "disks"):
                raise RuntimeError(name + ": malformed measured PremiumV2 disk receipt entry")
            key = _canonical_id(row["id"])
            if key in receipt_ids:
                raise RuntimeError(name + ": duplicate measured PremiumV2 disk receipt ID")
            receipt_ids[key] = row
        live_measured_ids = set()
        for disk in premium_disks:
            props = disk.get("properties") or {}
            resource_id = disk["id"]
            key = _canonical_id(resource_id)
            live_measured_ids.add(key)
            row = receipt_ids.get(key)
            unique_id = props.get("uniqueId") or disk.get("uniqueId")
            measured_source_uids[key] = unique_id
            option = (props.get("creationData") or {}).get("createOption")
            percent = props.get("completionPercent")
            if (row is None or row.get("uniqueId") != unique_id or
                    row.get("createOption") != option or row.get("completionPercent") != percent):
                raise RuntimeError(name + ": measured PremiumV2 disk changed since the retirement receipt")
            seen_measured_receipt_ids.add(key)
        if live_measured_ids != set(receipt_ids) or seen_measured_receipt_ids != set(receipt_ids):
            raise RuntimeError(name + ": measured PremiumV2 disk identity set changed")

        for disk in disks:
            resource_id = disk.get("id", "")
            sku = (disk.get("sku") or {}).get("name")
            if sku == PREMIUM_SKU:
                continue
            if retired_stage_id and _canonical_id(resource_id) == retired_stage_id:
                raise RuntimeError(name + ": retired source-stage disk reappeared")

        current_snapshot_ids = set()
        group_after_ids = {
            key for key in snapshot_audit["identities"]
            if key.startswith(_canonical_id(snapshot_rg) + "/providers/microsoft.compute/snapshots/")
        }
        for snapshot in snapshots:
            props = snapshot.get("properties") or {}
            tags = snapshot.get("tags") or {}
            snapshot_id = snapshot.get("id", "")
            canonical_snapshot_id = _canonical_id(snapshot_id)
            unique_id = props.get("uniqueId") or snapshot.get("uniqueId")
            if not _id_under(snapshot_id, snapshot_rg, "snapshots"):
                raise RuntimeError(name + ": retained snapshot escaped its owned snapshot group")
            if tags.get("reprotoken") != owner:
                raise RuntimeError(name + ": retained snapshot ownership changed")
            if not isinstance(unique_id, str) or not unique_id:
                raise RuntimeError(name + ": retained snapshot lacks uniqueId")
            if snapshot_audit["identities"].get(canonical_snapshot_id) != unique_id:
                raise RuntimeError(name + ": live snapshot identity differs from the preservation audit")
            current_snapshot_ids.add(canonical_snapshot_id)

        if current_snapshot_ids != group_after_ids:
            raise RuntimeError(name + ": live snapshot identity set differs from the after-retirement audit")

        for snapshot in snapshots:
            props = snapshot.get("properties") or {}
            tags = snapshot.get("tags") or {}
            option = (props.get("creationData") or {}).get("createOption")
            percent = props.get("completionPercent")
            provision = props.get("provisioningState")
            access = props.get("snapshotAccessState")
            snapshot_id = snapshot["id"]
            canonical_snapshot_id = _canonical_id(snapshot_id)
            unique_id = props.get("uniqueId") or snapshot.get("uniqueId")
            creation = props.get("creationData") or {}
            tags = snapshot.get("tags") or {}
            raw_parent_id = (creation.get("sourceResourceId") or
                             tags.get("source_volume_id") or
                             tags.get("sourceVolumeId") or
                             tags.get("sourceDiskId"))
            parent_id = _canonical_id(raw_parent_id)
            parent_uid = creation.get("sourceUniqueId")

            pin = pins.get(canonical_snapshot_id)
            failed_known_copy = (
                pin is not None and unique_id == pin.get("uniqueId") and
                pin.get("ownerToken") == owner and pin.get("reprotoken") == owner and
                _canonical_id(pin.get("sourceSnapshotId")) == _canonical_id(source_snapshot_id) and
                provision == "Failed" and access == "Unknown" and option == "CopyStart" and
                percent == pin.get("completionPercent") == 0.0 and
                tags.get("reproPurpose") == pin.get("reproPurpose") == "premiumv2-keeper-copy" and
                _canonical_id(tags.get("sourceSnapshotId")) == _canonical_id(source_snapshot_id)
            )
            if provision == "Failed" and not failed_known_copy:
                raise RuntimeError(name + ": unexpected failed retained snapshot")
            if failed_known_copy:
                seen_pins.add(canonical_snapshot_id)

            copy_error = props.get("copyCompletionError")
            if copy_error is not None and not failed_known_copy:
                raise RuntimeError(name + ": retained snapshot reports a copy error")

            source_is_measured_premium = parent_id in live_measured_ids
            source_is_standard = (parent_id in standard_source_ids or
                                  parent_id in standard_source_snapshot_ids)
            expected_parent_uid = measured_source_uids.get(parent_id)
            if expected_parent_uid is None:
                for source_context in contexts.values():
                    if _canonical_id(source_context["manifest"].get("sourceSnapshotId")) == parent_id:
                        expected_parent_uid = source_context["manifest"].get("uniqueId")
                        break
            if expected_parent_uid is None:
                for stage_context in stage_contexts.values():
                    if stage_context:
                        if _canonical_id(stage_context["stagingDiskId"]) == parent_id:
                            expected_parent_uid = stage_context["stagingDiskUniqueId"]
                            break
                        if _canonical_id(stage_context["restoreSnapshotId"]) == parent_id:
                            expected_parent_uid = stage_context["restoreSnapshotUniqueId"]
                            break
            lineage_known = expected_parent_uid is not None and parent_uid == expected_parent_uid
            if (parent_id and expected_parent_uid is not None and parent_uid != expected_parent_uid and
                    not failed_known_copy):
                raise RuntimeError(name + ": retained snapshot source uniqueId differs from the bound source")
            if option in ("Copy", "CopyStart") and not failed_known_copy and not lineage_known:
                complete = False
                hydration_basis = "copy-source-identity-unresolved"
                hydration_proven = False
                pending.append({"id": snapshot_id, "reason": "snapshot copy source ID/uniqueId is not bound to trusted source evidence"})
                evidence.append({
                    "kind": "snapshot", "id": snapshot_id, "uniqueId": unique_id,
                    "sku": (snapshot.get("sku") or {}).get("name"), "createOption": option,
                    "state": provision, "snapshotAccessState": access,
                    "completionPercent": percent, "complete": False,
                    "hydrationProven": False, "hydrationBasis": hydration_basis,
                    "sourceResourceId": raw_parent_id,
                    "sourceResourceUniqueId": parent_uid,
                    "sourceIsMeasuredPremiumV2": source_is_measured_premium,
                    "sourceIsKnownStandardOrigin": source_is_standard,
                    "copyCompletionError": copy_error,
                    "terminalFailedCopyPreserved": failed_known_copy,
                })
                continue
            hydration_proven = False
            hydration_basis = "unresolved-copy-origin"
            if failed_known_copy:
                complete = True
                hydration_basis = "known-terminal-failed-source-copy-preserved-by-exact-pin"
            elif (lineage_known and provision == "Succeeded" and access == "Available" and
                  option == "CopyStart" and percent == 100):
                complete = True
                hydration_proven = True
                hydration_basis = "managed-copy-start-explicit-100-percent"
            elif source_is_measured_premium:
                complete = (provision == "Succeeded" and access == "Available" and
                            option in ("Copy", "CopyStart") and percent == 100)
                hydration_proven = complete
                hydration_basis = "measured-PremiumV2-source-explicit-100-percent"
            elif (provision == "Succeeded" and access == "Available" and option == "Copy" and
                    copy_error is None and source_is_standard):
                complete = True
                hydration_proven = False
                hydration_basis = "standard-origin-Copy-progress-unobservable; Available-is-restore-readiness-only"
            else:
                complete = False

            if not complete:
                pending.append({"id": snapshot_id, "reason": "snapshot copy state is not proven by a supported source-aware rule"})
            evidence.append({
                "kind": "snapshot", "id": snapshot_id, "uniqueId": unique_id,
                "sku": (snapshot.get("sku") or {}).get("name"), "createOption": option,
                "state": provision, "snapshotAccessState": access,
                "completionPercent": percent, "complete": complete,
                "hydrationProven": hydration_proven,
                "hydrationBasis": hydration_basis,
                "sourceResourceId": raw_parent_id,
                "sourceResourceUniqueId": parent_uid,
                "sourceIsMeasuredPremiumV2": source_is_measured_premium,
                "sourceIsKnownStandardOrigin": source_is_standard,
                "copyCompletionError": copy_error,
                "terminalFailedCopyPreserved": failed_known_copy,
            })

        missing_pins = set(pins) - seen_pins
        if missing_pins:
            raise RuntimeError(name + ": pinned failed source-copy snapshot is missing or changed")

        stage_child = stage["restoreSnapshotId"] if stage else None
        if stage_child and _canonical_id(stage_child) not in current_snapshot_ids:
            raise RuntimeError(name + ": retired source-stage child snapshot is absent from the live snapshot group")

        return {
            "stack": name,
            "complete": not pending,
            "pending": pending,
            "resources": evidence,
            "retiredSourceStage": stage,
            "snapshotAuditAfterSha256": retirement_context["snapshotAudit"]["afterSha256"],
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        stacks = list(pool.map(inspect, RUNS))

    receipt = {
        "checkedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "complete": all(stack["complete"] for stack in stacks),
        "stacks": stacks,
        "retirementReceiptSha256": retirement_context["retirementReceiptSha256"],
        "retirementScriptSha256": retirement_context["retirementScriptSha256"],
        "snapshotAudit": {
            key: value for key, value in snapshot_audit.items() if key != "identities"
        },
        "standardCopyLimitation": STANDARD_COPY_LIMITATION,
        "basis": (
            "Scope is the three owned experiment stacks' node and retained-snapshot resource groups. "
            "Exactly four measured PremiumV2 disks per stack require the configured performance settings; "
            "Copy and CopyStart targets require explicit 100 percent completion. Standard-origin Copy "
            "snapshots may be Succeeded and Available for restore, but their copy hydration is unobservable "
            "and is not claimed. CopyStart snapshots require explicit 100 percent. Known terminal failed "
            "source copies are preserved only by their exact pinned identity exception."
        ),
    }
    return _write_barrier_receipt(receipt)


if __name__ == "__main__":
    result = verify()
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["complete"] else 1)
