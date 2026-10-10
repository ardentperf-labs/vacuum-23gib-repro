#!/usr/bin/env python3
"""Preserve the exact WAL archive objects required by an owned CNPG snapshot Backup.

All Azure reads go through the ordinary `az` executable (the workspace broker
wrapper). Account keys are captured from ARM into process memory and passed to
Azure CLI only through its environment, never argv or a file. Blob bytes and
the non-secret receipt are written beneath this script's private export root.
"""

from __future__ import annotations

import argparse
import base64
import bz2
import datetime as dt
import gzip
import hashlib
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[2]
REVIEW_DIR = Path(__file__).resolve().parent
EXPORT_ROOT = REVIEW_DIR / "exports"
EVIDENCE_ROOT = REVIEW_DIR / "evidence"
STACKS = ("baseline-23gib", "50gib", "100gib")
PREPARATION_STACKS = ("50gib", "100gib")
WAL_SEGMENT_SIZE = 16 * 1024 * 1024
MAX_SEGMENTS = 8192
MAX_EVIDENCE_FILE_BYTES = 8 * 1024 * 1024
MAX_EVIDENCE_TOTAL_BYTES = 40 * 1024 * 1024
SEGMENT_RE = re.compile(r"^(?P<tli>[0-9A-Fa-f]{8})(?P<log>[0-9A-Fa-f]{8})(?P<seg>[0-9A-Fa-f]{8})$")
COMPRESSION_SUFFIXES = ("", ".gz", ".bz2", ".xz", ".zst", ".snappy", ".lz4")


class ExportError(RuntimeError):
    """A sanitized validation or service error. Never includes credential text."""


def fail(message: str) -> None:
    raise ExportError(message)


def evidence_label(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        fail(f"Cannot read required JSON evidence: {evidence_label(path)}")


def canonical_id(value: str) -> str:
    return value.rstrip("/").lower()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while True:
                block = source.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
    except OSError:
        fail(f"Cannot hash required evidence file: {evidence_label(path)}")
    return digest.hexdigest()


def require_sha256(path: Path, expected: str | None, description: str) -> str:
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
        fail(f"{description} has no valid SHA-256 receipt.")
    actual = sha256_file(path)
    if actual != expected:
        fail(f"{description} no longer matches its recorded SHA-256.")
    return actual


def is_complete_percent(value) -> bool:
    try:
        return float(value) == 100.0
    except (TypeError, ValueError):
        return False


def az_json(args: list[str], *, env: dict[str, str] | None = None):
    """Run broker-wrapped az and suppress stderr, which might contain credentials."""
    result = subprocess.run(
        ["az", *args], text=True, capture_output=True, env=env, timeout=300
    )
    if result.returncode:
        fail(f"Azure read command failed at {args[0]} {args[1]} (exit {result.returncode}).")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        fail(f"Azure read command returned invalid JSON at {args[0]} {args[1]}.")


def resolve_owned_storage(stack: str):
    state_dir = ROOT / stack / ".state"
    state = read_json(state_dir / "state.json")
    storage = read_json(state_dir / "storage.json")
    container_record = read_json(state_dir / "storage-container.json")

    subscription = state.get("subscriptionId")
    resource_group = state.get("resourceGroup")
    account = storage.get("accountName")
    container = storage.get("containerName")
    owner = state.get("ownerToken")
    expected_account_id = (
        f"/subscriptions/{subscription}/resourceGroups/{resource_group}/"
        f"providers/Microsoft.Storage/storageAccounts/{account}"
    )
    if state.get("status") not in ("ready", "prepared", "complete"):
        fail(f"{stack}: local stack state is not ready for retention export.")
    if storage.get("status") != "ready":
        fail(f"{stack}: local storage state is not ready.")
    if not all(isinstance(v, str) and v for v in (subscription, resource_group, account, container, owner)):
        fail(f"{stack}: incomplete local stack/storage identity.")
    if storage.get("subscriptionId") != subscription or storage.get("resourceGroup") != resource_group:
        fail(f"{stack}: local storage subscription/resource group differs from stack state.")
    if storage.get("accountName") != account or storage.get("containerName") != container:
        fail(f"{stack}: incomplete local account/container identity.")
    if canonical_id(storage.get("accountId") or "") != canonical_id(expected_account_id):
        fail(f"{stack}: local storage account ID does not match stack state.")
    if storage.get("ownerToken") != owner:
        fail(f"{stack}: local storage ownership token differs from stack state.")
    if container_record.get("name") != container:
        fail(f"{stack}: local container record differs from storage state.")
    metadata = container_record.get("metadata") or {}
    if metadata.get("reprotoken") != owner or metadata.get("reprostack") != "vacuum-only-repro":
        fail(f"{stack}: local container ownership markers do not match.")
    if container_record.get("deleted") is True:
        fail(f"{stack}: local container record says the container was deleted.")

    # Verify the live Azure resource group and account using ordinary brokered ARM reads.
    rg = az_json(["group", "show", "--name", resource_group, "--subscription", subscription,
                  "--only-show-errors", "--output", "json"])
    if canonical_id(rg.get("id", "")) != canonical_id(f"/subscriptions/{subscription}/resourceGroups/{resource_group}"):
        fail(f"{stack}: live resource group ID differs from local ownership metadata.")
    if (rg.get("tags") or {}).get("reprotoken") != owner:
        fail(f"{stack}: live resource group ownership tag does not match.")

    arm_account = az_json(["storage", "account", "show", "--resource-group", resource_group,
                           "--name", account, "--subscription", subscription,
                           "--only-show-errors", "--output", "json"])
    if canonical_id(arm_account.get("id", "")) != canonical_id(expected_account_id):
        fail(f"{stack}: live storage account ID differs from local state.")
    if arm_account.get("name", "").lower() != account.lower():
        fail(f"{stack}: live storage account name differs from local state.")
    if arm_account.get("resourceGroup", "").lower() != resource_group.lower():
        fail(f"{stack}: live storage account resource group differs from local state.")
    if (arm_account.get("tags") or {}).get("reprotoken") != owner:
        fail(f"{stack}: live storage account ownership tag does not match.")
    if (arm_account.get("tags") or {}).get("reproStack") != "vacuum-only-repro":
        fail(f"{stack}: live storage account stack tag does not match.")
    if arm_account.get("allowBlobPublicAccess") is not False or arm_account.get("enableHttpsTrafficOnly") is not True:
        fail(f"{stack}: live storage account privacy/transport settings are not the expected values.")
    endpoint = (arm_account.get("primaryEndpoints") or {}).get("blob")
    if not endpoint:
        fail(f"{stack}: live storage account has no primary Blob endpoint.")
    endpoint_url = urlsplit(endpoint)
    if endpoint_url.scheme != "https" or not endpoint_url.hostname:
        fail(f"{stack}: live Blob endpoint is not HTTPS.")

    return {
        "state_dir": state_dir,
        "state": state,
        "storage": storage,
        "container_record": container_record,
        "subscription": subscription,
        "resource_group": resource_group,
        "account": account,
        "account_id": expected_account_id,
        "container": container,
        "owner": owner,
        "blob_endpoint": endpoint.rstrip("/"),
        "blob_host": endpoint_url.hostname,
        "region": arm_account.get("location"),
    }


def verify_preparation_capture_inputs(stack: str, root: Path = ROOT) -> dict:
    """Bind a 50/100 live evidence capture to frozen successful preparation receipts."""
    if stack not in PREPARATION_STACKS:
        fail("Live preparation evidence capture is limited to the 50gib and 100gib stacks.")
    base = root / stack
    state_path = base / ".state/state.json"
    process_path = base / ".state/prepare-process.json"
    prepare_path = base / ".state/prepare.json"
    script_path = base / "02-prepare.sh"
    manifest_path = base / "source-manifest.json"
    config_path = base / "run-config.json"
    state = read_json(state_path)
    process = read_json(process_path)
    if (process.get("stack") != stack or process.get("phase") != "prepare" or
            process.get("status") != "complete" or process.get("exitCode") != 0 or
            not isinstance(process.get("finishedAtUtc"), str)):
        fail(f"{stack}: preparation process is not complete with exit code zero.")
    prepared = read_json(prepare_path)
    manifest = read_json(manifest_path)
    config = read_json(config_path)

    retirement_path = root / "preflight/staging-retirement.json"
    retirement = read_json(retirement_path)
    if retirement.get("phase") != "complete":
        fail(f"{stack}: completed staging-retirement receipt is required before evidence capture.")
    receipt_preparation = (retirement.get("preparation") or {}).get(stack)
    if not isinstance(receipt_preparation, dict):
        fail(f"{stack}: staging-retirement receipt has no preparation hash entry.")
    expected_hashes = {
        "stateSha256": state_path,
        "sourceManifestSha256": manifest_path,
        "prepareProcessSha256": process_path,
        "prepareMetadataSha256": prepare_path,
        "prepareScriptSha256": script_path,
    }
    for field, path in expected_hashes.items():
        require_sha256(path, receipt_preparation.get(field), f"{stack} {field}")

    owner = state.get("ownerToken")
    subscription = state.get("subscriptionId")
    if (state.get("status") != "ready" or not isinstance(owner, str) or
            not re.fullmatch(r"[a-f0-9]{32}", owner) or not subscription or
            not state.get("resourceGroup") or not state.get("nodeResourceGroup") or
            not state.get("snapshotResourceGroup")):
        fail(f"{stack}: owned infrastructure state is incomplete.")
    script_hash = sha256_file(script_path)
    script_map = process.get("scriptSha256") or {}
    if script_map.get("02-prepare.sh") != script_hash:
        fail(f"{stack}: preparation process does not bind the current 02-prepare.sh bytes.")
    helper_hash = process.get("sourceStageHelperSha256")
    expected_helper_hash = receipt_preparation.get("sourceStageHelperSha256")
    helper_path = root / "recovery/source-restore/stage-allowlisted-source.sh"
    if (not isinstance(helper_hash, str) or helper_hash != expected_helper_hash or
            not helper_path.is_file() or sha256_file(helper_path) != helper_hash):
        fail(f"{stack}: source-stage helper hash differs from the completed preparation receipt.")

    expected_heap = 50 if stack == "50gib" else 100
    if (config.get("targetHeapGiB") != expected_heap or
            prepared.get("status") != "complete" or
            prepared.get("fixtureTargetHeapGiB") != expected_heap or
            prepared.get("instances") != 2 or prepared.get("clientCreated") is not False or
            prepared.get("retainedSnapshotResourceGroup") != state.get("snapshotResourceGroup") or
            prepared.get("cluster") != "repro/hdd-pg" or
            not prepared.get("replicaCaughtUpToLsn")):
        fail(f"{stack}: completed preparation metadata does not match its assigned stack fixture.")
    backup_name = prepared.get("backup")
    pair = prepared.get("replicaSnapshots")
    if (not isinstance(backup_name, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,62}", backup_name) or
            not isinstance(pair, dict) or set(pair) != {"data", "wal"} or
            not all(isinstance(pair.get(k), str) and
                    re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,62}", pair[k]) for k in ("data", "wal")) or
            pair.get("data") == pair.get("wal")):
        fail(f"{stack}: original prepare.json backup and replicaSnapshots fields are incomplete.")
    if canonical_id(prepared.get("sourceSnapshotId", "")) != canonical_id(manifest.get("sourceSnapshotId", "")):
        fail(f"{stack}: prepared source snapshot differs from the pinned source manifest.")
    if manifest.get("subscriptionId") != subscription:
        fail(f"{stack}: pinned source manifest belongs to a different subscription.")
    if canonical_id(state.get("snapshotSourceCopyId", "")) != canonical_id(prepared.get("restoreCopySnapshotId", "")):
        fail(f"{stack}: prepared restore-copy snapshot differs from owned state.")

    stage_receipt_rel = receipt_preparation.get("sourceStageReceiptPath")
    stage_receipt_hash = receipt_preparation.get("sourceStageReceiptSha256")
    if not isinstance(stage_receipt_rel, str) or not stage_receipt_rel:
        fail(f"{stack}: source-stage receipt path is missing from the completion receipt.")
    stage_receipt_path = Path(stage_receipt_rel)
    if not stage_receipt_path.is_absolute():
        stage_receipt_path = root / stage_receipt_path
    try:
        stage_receipt_real = stage_receipt_path.resolve(strict=True)
        expected_stage_dir = (base / ".state/source-stage").resolve(strict=True)
    except OSError:
        fail(f"{stack}: source-stage receipt is unavailable.")
    if stage_receipt_real.parent != expected_stage_dir:
        fail(f"{stack}: source-stage receipt path is outside the owned stack state directory.")
    require_sha256(stage_receipt_real, stage_receipt_hash, f"{stack} source-stage receipt")
    state_stage_path = state.get("snapshotSourceStageReceiptPath")
    if not isinstance(state_stage_path, str) or not state_stage_path:
        fail(f"{stack}: state.json does not bind the owned source-stage receipt.")
    normalized_state_stage = Path(state_stage_path)
    if not normalized_state_stage.is_absolute():
        normalized_state_stage = root / normalized_state_stage
    if normalized_state_stage.resolve() != stage_receipt_real:
        fail(f"{stack}: source-stage receipt path differs from state.json.")
    stage_receipt = read_json(stage_receipt_real)
    if (stage_receipt.get("status") != "ready" or stage_receipt.get("ownerToken") != owner or
            canonical_id(stage_receipt.get("sourceSnapshotId", "")) != canonical_id(manifest.get("sourceSnapshotId", "")) or
            canonical_id(stage_receipt.get("restoreSnapshotId", "")) != canonical_id(state.get("snapshotSourceCopyId", ""))):
        fail(f"{stack}: source-stage receipt no longer matches the owned prepared source.")
    if (stage_receipt.get("sourceSnapshotUniqueId") != manifest.get("uniqueId") or
            not stage_receipt.get("stagingDiskUniqueId") or
            canonical_id(stage_receipt.get("stagingDiskId", "")) != canonical_id(state.get("snapshotSourceStagingDiskId", ""))):
        fail(f"{stack}: source-stage receipt source/staging disk identities changed.")
    retired = [item for item in retirement.get("retiredStagingDisks", [])
               if item.get("stack") == stack]
    if len(retired) != 1:
        fail(f"{stack}: completed retirement receipt must contain exactly one retired source stage.")
    retired_item = retired[0]
    if (retired_item.get("absentVerified") is not True or
            canonical_id(retired_item.get("id", "")) != canonical_id(stage_receipt.get("stagingDiskId", "")) or
            retired_item.get("uniqueId") != stage_receipt.get("stagingDiskUniqueId") or
            canonical_id(retired_item.get("sourceSnapshotId", "")) != canonical_id(manifest.get("sourceSnapshotId", "")) or
            retired_item.get("sourceSnapshotUniqueId") != manifest.get("uniqueId") or
            canonical_id(retired_item.get("restoreSnapshotId", "")) != canonical_id(stage_receipt.get("restoreSnapshotId", "")) or
            retired_item.get("restoreSnapshotUniqueId") != stage_receipt.get("restoreSnapshotUniqueId") or
            retired_item.get("sourceStageReceiptSha256") != stage_receipt_hash or
            retired_item.get("sourceStageReceiptPath") != receipt_preparation.get("sourceStageReceiptPath")):
        fail(f"{stack}: retired source-stage record differs from the owned source-stage receipt.")

    return {
        "base": base,
        "state": state,
        "process": process,
        "prepared": prepared,
        "manifest": manifest,
        "config": config,
        "receiptPreparation": receipt_preparation,
        "stageReceipt": stage_receipt,
        "sourceHashes": {field: receipt_preparation[field] for field in expected_hashes},
        "sourceStageReceiptPath": stage_receipt_real,
        "sourceStageReceiptSha256": stage_receipt_hash,
        "sourceStageHelperSha256": helper_hash,
        "retiredStage": retired_item,
    }


def backup_label(status: dict) -> str:
    raw = status.get("backupLabelFile")
    if not isinstance(raw, str) or not raw:
        fail("Completed Backup has no encoded backup label.")
    try:
        return base64.b64decode(raw, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        fail("Completed Backup label is not valid base64 UTF-8.")


def label_fields(label: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in label.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            fields[key.strip().upper()] = value.strip()
    start = fields.get("START WAL LOCATION", "")
    match = re.search(r"\(file\s+([0-9A-Fa-f]{24})\)", start)
    if match:
        fields["START WAL FILE"] = match.group(1).upper()
    return fields


def wal_sequence(begin: str, end: str, segment_size: int) -> list[str]:
    start = SEGMENT_RE.fullmatch(begin or "")
    stop = SEGMENT_RE.fullmatch(end or "")
    if not start or not stop:
        fail("Backup begin/end WAL fields are not PostgreSQL WAL segment names.")
    if start.group("tli").upper() != stop.group("tli").upper():
        fail("Backup crosses timelines; exporter intentionally fails closed for this case.")
    tli = int(start.group("tli"), 16)
    if not 1 <= tli <= 0xFFFFFFFF:
        fail("Backup WAL timeline ID is invalid.")
    if segment_size <= 0 or (1 << 32) % segment_size:
        fail("PostgreSQL WAL segment size is invalid for WAL filename arithmetic.")
    segments_per_log = (1 << 32) // segment_size
    if int(start.group("seg"), 16) >= segments_per_log or int(stop.group("seg"), 16) >= segments_per_log:
        fail("Backup WAL segment index is outside the configured segment range.")
    first = int(start.group("log"), 16) * segments_per_log + int(start.group("seg"), 16)
    last = int(stop.group("log"), 16) * segments_per_log + int(stop.group("seg"), 16)
    if last < first:
        fail("Backup end WAL segment precedes its start segment.")
    count = last - first + 1
    if count > MAX_SEGMENTS:
        fail(f"Backup requires {count} WAL segments, above the conservative {MAX_SEGMENTS}-segment export cap.")
    out = []
    for absolute in range(first, last + 1):
        log, seg = divmod(absolute, segments_per_log)
        out.append(f"{tli:08X}{log:08X}{seg:08X}")
    return out


def backup_history_name(begin_lsn: str, begin_wal: str, segment_size: int) -> str:
    try:
        high, low = begin_lsn.split("/", 1)
        high_value, low_value = int(high, 16), int(low, 16)
        offset = low_value % segment_size
        start = SEGMENT_RE.fullmatch(begin_wal)
        if not start:
            fail("Backup start WAL name is invalid.")
        segment_index = low_value // segment_size
        if (int(start.group("log"), 16) != high_value or
                int(start.group("seg"), 16) != segment_index):
            fail("Backup start LSN does not map to its begin WAL file.")
        return f"{begin_wal.upper()}.{offset:08X}.backup"
    except (ValueError, AttributeError):
        fail("Backup start LSN cannot be mapped to a backup-history filename.")


def verify_backup_metadata(stack: str, storage: dict, wal_segment_size: int,
                           backup_document: dict | None = None) -> dict:
    state_dir = storage["state_dir"]
    state = storage["state"]
    if backup_document is None:
        backup_file = state_dir / "recovery" / "backup.json"
        backups = read_json(backup_file).get("items", [])
        owned = [b for b in backups if (b.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == storage["owner"]]
        if len(owned) != 1:
            fail(f"{stack}: expected exactly one completed Backup carrying this stack's owner label.")
        backup = owned[0]
    else:
        backup = backup_document
    meta, spec, status = backup.get("metadata", {}), backup.get("spec", {}), backup.get("status", {})
    if ((meta.get("labels") or {}).get("repro.stack.token") != storage["owner"] or
            meta.get("namespace") != "repro"):
        fail(f"{stack}: Backup namespace or owner label does not match the stack.")
    if status.get("phase") != "completed" or spec.get("method") != "volumeSnapshot":
        fail(f"{stack}: owned Backup is not a completed volume-snapshot Backup.")
    if spec.get("online") is not True or spec.get("onlineConfiguration", {}).get("waitForArchive") is not True:
        fail(f"{stack}: owned Backup is not a completed online Backup with waitForArchive=true.")
    cluster = spec.get("cluster", {}).get("name")
    if not cluster:
        fail(f"{stack}: Backup has no cluster identity.")
    begin_lsn, end_lsn = status.get("beginLSN"), status.get("endLSN")
    begin_wal, end_wal = status.get("beginWal"), status.get("endWal")
    segments = wal_sequence(begin_wal, end_wal, wal_segment_size)
    label = backup_label(status)
    parsed_label = label_fields(label)
    if parsed_label.get("START WAL FILE", "").upper() != begin_wal.upper():
        fail(f"{stack}: Backup label and Backup status disagree about the start WAL segment.")
    if parsed_label.get("START WAL LOCATION", "").split(" ", 1)[0] != begin_lsn:
        fail(f"{stack}: Backup label and Backup status disagree about the start LSN.")
    try:
        timeline = int(parsed_label["START TIMELINE"], 10)
    except (KeyError, ValueError):
        fail(f"{stack}: Backup label has no valid starting timeline.")
    if timeline != int(SEGMENT_RE.fullmatch(begin_wal).group("tli"), 16):
        fail(f"{stack}: Backup label timeline does not match the WAL segment timeline.")
    end_file = SEGMENT_RE.fullmatch(end_wal)
    if not end_file or timeline != int(end_file.group("tli"), 16):
        fail(f"{stack}: Backup end WAL segment has a different timeline than its backup label.")
    if status.get("beginWal").upper() != status.get("endWal").upper() and len(segments) == 1:
        fail(f"{stack}: Backup WAL range calculation is inconsistent.")

    snapshot_elements = status.get("snapshotBackupStatus", {}).get("elements", [])
    data_names = [e.get("name") for e in snapshot_elements if e.get("type") == "PG_DATA"]
    wal_names = [e.get("name") for e in snapshot_elements if e.get("type") == "PG_WAL"]
    if len(data_names) != 1 or len(wal_names) != 1 or data_names[0] == wal_names[0]:
        fail(f"{stack}: Backup does not identify one distinct PG_DATA/PG_WAL snapshot pair.")
    return {
        "backup": backup,
        "backup_uid": meta.get("uid"),
        "backup_name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "cluster": cluster,
        "status": status,
        "label": label,
        "label_fields": parsed_label,
        "timeline": timeline,
        "segments": segments,
        "backup_history_name": backup_history_name(begin_lsn, begin_wal, wal_segment_size),
        "data_snapshot_name": data_names[0],
        "wal_snapshot_name": wal_names[0],
        "begin_lsn": begin_lsn,
        "end_lsn": end_lsn,
        "begin_wal": begin_wal.upper(),
        "end_wal": end_wal.upper(),
    }


def verify_archive_configuration(stack: str, storage: dict, backup: dict,
                                 cluster_list: list | None = None) -> dict:
    state_dir = storage["state_dir"]
    if cluster_list is None:
        cluster_list = read_json(state_dir / "recovery" / "cluster.json").get("items", [])
    matches = [c for c in cluster_list if c.get("kind") == "Cluster" and c.get("metadata", {}).get("name") == backup["cluster"]]
    if len(matches) != 1:
        fail(f"{stack}: no unique saved CNPG Cluster manifest matches the Backup.")
    cluster = matches[0]
    labels = cluster.get("metadata", {}).get("labels", {})
    applied = cluster.get("metadata", {}).get("annotations", {}).get("kubectl.kubernetes.io/last-applied-configuration")
    config = cluster.get("spec", {}).get("backup", {}).get("barmanObjectStore")
    if not config and applied:
        try:
            config = json.loads(applied).get("spec", {}).get("backup", {}).get("barmanObjectStore")
        except json.JSONDecodeError:
            config = None
    if not config:
        fail(f"{stack}: saved cluster manifest has no Barman WAL archive configuration.")
    if labels.get("repro.stack.token") != storage["owner"]:
        fail(f"{stack}: saved Cluster owner label differs from local stack state.")
    destination = config.get("destinationPath")
    endpoint = urlsplit(destination or "")
    if endpoint.scheme != "https" or endpoint.hostname != storage["blob_host"]:
        fail(f"{stack}: Barman destination host does not exactly match the owned storage account.")
    parts = endpoint.path.strip("/").split("/")
    if len(parts) < 2 or parts[0] != storage["container"]:
        fail(f"{stack}: Barman destination container differs from the owned container.")
    prefix = "/".join(parts[1:])
    if not prefix.endswith("/"):
        prefix += "/"
    expected_prefix = f"cnpg-vacuum-only/{storage['owner']}/"
    if prefix != expected_prefix:
        fail(f"{stack}: Barman destination prefix does not exactly match this stack's owned prefix.")
    server = config.get("serverName")
    if server != backup["cluster"]:
        fail(f"{stack}: Barman serverName does not match the Backup cluster.")
    wal_settings = config.get("wal") or {}
    return {
        "host": endpoint.hostname,
        "container": parts[0],
        "prefix": prefix,
        "server_name": server,
        "archive_prefix": f"{prefix}{server}/wals/",
        "wal_settings": wal_settings,
    }


def _verify_snapshot_object_set(stack: str, storage: dict, backup: dict,
                                vss_list: list, vsc_list: list, vsc_classes: list,
                                audit: dict) -> list[dict]:
    if audit.get("status") != "complete" or audit.get("ownerToken") != storage["owner"]:
        fail(f"{stack}: snapshot evidence ownership does not verify.")
    if audit.get("backup") != backup["backup_name"]:
        fail(f"{stack}: snapshot evidence refers to a different Backup.")
    pair = audit.get("replicaSnapshots", {})
    if pair.get("data") != backup["data_snapshot_name"] or pair.get("wal") != backup["wal_snapshot_name"]:
        fail(f"{stack}: snapshot evidence DATA/WAL names differ from Backup status.")
    retained = audit.get("retainedSnapshots", [])
    if not isinstance(retained, list) or len(retained) < 2:
        fail(f"{stack}: both retained paired Azure snapshots are required.")
    result = []
    for expected_name in (backup["data_snapshot_name"], backup["wal_snapshot_name"]):
        vss_matches = [x for x in vss_list if x.get("kind") == "VolumeSnapshot" and x.get("metadata", {}).get("name") == expected_name]
        if len(vss_matches) != 1:
            fail(f"{stack}: expected exactly one VolumeSnapshot {expected_name}.")
        vss = vss_matches[0]
        vm, vs, vstatus = vss.get("metadata", {}), vss.get("spec", {}), vss.get("status", {})
        if vstatus.get("readyToUse") is not True:
            fail(f"{stack}: VolumeSnapshot {expected_name} is not ready.")
        labels = vm.get("labels", {})
        if (vm.get("namespace") != backup.get("namespace", "repro") or
                labels.get("cnpg.io/cluster") != backup["cluster"] or
                labels.get("cnpg.io/backupName") != backup["backup_name"]):
            fail(f"{stack}: VolumeSnapshot {expected_name} is not labeled for this Backup.")
        if (vm.get("uid") is None or
                vss["spec"].get("volumeSnapshotClassName") is None):
            fail(f"{stack}: VolumeSnapshot {expected_name} is missing its UID or class identity.")
        classes = [x for x in vsc_classes if x.get("kind") == "VolumeSnapshotClass" and
                   x.get("metadata", {}).get("name") == vss["spec"].get("volumeSnapshotClassName")]
        if len(classes) != 1 or classes[0].get("deletionPolicy") != "Retain" or classes[0].get("driver") != "disk.csi.azure.com":
            fail(f"{stack}: VolumeSnapshotClass is not uniquely identified with Azure CSI Retain policy.")
        instance_value = backup["status"].get("instanceID")
        instance_id = instance_value.get("podName") if isinstance(instance_value, dict) else instance_value
        if not isinstance(instance_id, str) or not instance_id:
            fail(f"{stack}: Backup status has no source instance pod identity.")
        expected_pvc = instance_id if expected_name == backup["data_snapshot_name"] else f"{instance_id}-wal"
        if vss["spec"].get("source", {}).get("persistentVolumeClaimName") != expected_pvc:
            fail(f"{stack}: VolumeSnapshot {expected_name} is not sourced from the Backup instance's expected PVC.")
        content_name = vstatus.get("boundVolumeSnapshotContentName")
        contents = [x for x in vsc_list if x.get("kind") == "VolumeSnapshotContent" and x.get("metadata", {}).get("name") == content_name]
        if len(contents) != 1:
            fail(f"{stack}: VolumeSnapshot {expected_name} has no unique bound content.")
        content = contents[0]
        cm, cs, cstatus = content.get("metadata", {}), content.get("spec", {}), content.get("status", {})
        ref = cs.get("volumeSnapshotRef", {})
        if not cm.get("uid") or cs.get("deletionPolicy") != "Retain" or cstatus.get("readyToUse") is not True:
            fail(f"{stack}: VolumeSnapshotContent {content_name} is not retained and ready.")
        if ref.get("name") != expected_name or ref.get("namespace") != vm.get("namespace") or ref.get("uid") != vm.get("uid"):
            fail(f"{stack}: VolumeSnapshotContent {content_name} is bound to a different snapshot UID.")
        snapshot_id = cstatus.get("snapshotHandle")
        if not snapshot_id:
            fail(f"{stack}: VolumeSnapshotContent {content_name} has no Azure snapshot handle.")
        expected_snapshot_rg = storage["state"].get("snapshotResourceGroup")
        if not expected_snapshot_rg or f"/resourcegroups/{expected_snapshot_rg.lower()}/providers/microsoft.compute/snapshots/" not in snapshot_id.lower():
            fail(f"{stack}: Azure snapshot handle is outside the stack's recorded retained snapshot group.")
        arm_matches = [x for x in retained if canonical_id(x.get("id", "")) == canonical_id(snapshot_id)]
        if len(arm_matches) != 1:
            fail(f"{stack}: Azure finalization audit does not uniquely include {expected_name}'s snapshot handle.")
        arm = arm_matches[0]
        creation = arm.get("creationData", {})
        if (arm.get("provisioningState") != "Succeeded" or arm.get("snapshotAccessState") != "Available" or
                not is_complete_percent(arm.get("completionPercent")) or creation.get("createOption") != "Copy" or
                (arm.get("tags") or {}).get("reprotoken") != storage["owner"]):
            fail(f"{stack}: Azure backup snapshot ownership/readiness metadata does not verify.")
        purpose = (arm.get("tags") or {}).get("reproPurpose")
        if purpose != "premiumv2-vacuum-backup":
            fail(f"{stack}: Azure backup snapshot purpose tag does not verify.")
        if canonical_id(creation.get("sourceResourceId", "")) != canonical_id(cs.get("source", {}).get("volumeHandle", "")):
            fail(f"{stack}: Azure backup snapshot source disk differs from its VolumeSnapshotContent.")
        result.append({
            "snapshot_name": expected_name,
            "snapshot_uid": vm.get("uid"),
            "snapshot_content_name": content_name,
            "snapshot_content_uid": cm.get("uid"),
            "azure_snapshot_id": snapshot_id,
            "azure_snapshot_unique_id": arm.get("uniqueId"),
            "source_volume_handle": cs.get("source", {}).get("volumeHandle"),
            "source_pvc": expected_pvc,
        })
    return result


def verify_snapshot_lineage(stack: str, storage: dict, backup: dict,
                            evidence: dict | None = None) -> list[dict]:
    if evidence is not None:
        vss_list = evidence["volumeSnapshots"]
        vsc_list = evidence["volumeSnapshotContents"]
        vsc_classes = evidence["volumeSnapshotClasses"]
        audit = evidence["derivedAudit"]
    else:
        state_dir = storage["state_dir"]
        vss_list = read_json(state_dir / "recovery" / "volumesnapshot.json").get("items", [])
        vsc_list = read_json(state_dir / "recovery" / "volumesnapshotcontent.json").get("items", [])
        audit_path = state_dir / "recovery" / "baseline-finalize-audit.json"
        if not audit_path.exists():
            fail(f"{stack}: baseline finalization audit is missing.")
        audit = read_json(audit_path)
        vsc_classes = read_json(state_dir / "recovery" / "volumesnapshotclass.json").get("items", [])
    return _verify_snapshot_object_set(stack, storage, backup, vss_list, vsc_list,
                                       vsc_classes, audit)


def get_account_key(storage: dict) -> str:
    # This Azure ARM listKeys response is captured only in Python memory. It is
    # never printed, written, used as an argument, or included in an exception.
    payload = az_json(["storage", "account", "keys", "list", "--resource-group", storage["resource_group"],
                       "--account-name", storage["account"], "--subscription", storage["subscription"],
                       "--only-show-errors", "--output", "json"])
    keys = [k.get("value") for k in payload if k.get("keyName") == "key1" and isinstance(k.get("value"), str)]
    if len(keys) != 1 or not keys[0]:
        fail("ARM did not return exactly one usable key1 value for the verified owned storage account.")
    return keys[0]


def storage_env(storage: dict, account_key: str) -> dict[str, str]:
    env = os.environ.copy()
    for name in ("AZURE_STORAGE_CONNECTION_STRING", "AZURE_STORAGE_SAS_TOKEN", "AZURE_STORAGE_KEY"):
        env.pop(name, None)
    env["AZURE_STORAGE_ACCOUNT"] = storage["account"]
    env["AZURE_STORAGE_KEY"] = account_key
    env["AZURE_STORAGE_AUTH_MODE"] = "key"
    env["AZURE_CORE_COLLECT_TELEMETRY"] = "false"
    return env


def verify_live_container(storage: dict, account_key: str) -> None:
    result = subprocess.run(
        ["az", "storage", "container", "show", "--name", storage["container"],
         "--account-name", storage["account"], "--auth-mode", "key", "--subscription", storage["subscription"],
         "--only-show-errors", "--output", "json"],
        text=True, capture_output=True, env=storage_env(storage, account_key), timeout=180,
    )
    if result.returncode:
        fail("Live Blob container read failed for the verified owned account/container.")
    try:
        live = json.loads(result.stdout)
    except json.JSONDecodeError:
        fail("Live Blob container read returned invalid JSON.")
    md = live.get("metadata") or {}
    if live.get("name") != storage["container"] or md.get("reprotoken") != storage["owner"] or md.get("reprostack") != "vacuum-only-repro":
        fail("Live Blob container identity/ownership metadata does not match local state.")
    if live.get("publicAccess") not in (None, "off"):
        fail("Live Blob container is not private.")


def normalized_object_name(base: str, name: str) -> tuple[str, str]:
    for suffix in ("", ".gz", ".bz2", ".xz", ".zst", ".snappy", ".lz4"):
        if name == base + suffix:
            return base, suffix
    return "", ""


def compression_for_object(settings: dict, object_name: str) -> str:
    # Do not guess a codec: accept only what the saved Cluster config identifies.
    configured = settings.get("compression")
    leaf = object_name.rsplit("/", 1)[-1]
    _, suffix = next(((leaf[:-len(s)], s) for s in COMPRESSION_SUFFIXES if s and leaf.endswith(s)), (leaf, ""))
    aliases = {None: "", "none": "", "gzip": ".gz", "bzip2": ".bz2", "xz": ".xz", "zstd": ".zst", "snappy": ".snappy", "lz4": ".lz4"}
    if configured not in aliases:
        fail("Saved Barman configuration uses an unsupported WAL compression mode.")
    expected = aliases[configured]
    if suffix != expected:
        fail("Archived WAL object's suffix does not match the saved Barman compression setting.")
    return suffix


def download_blob(storage: dict, blob_name: str, target: Path, account_key: str) -> dict:
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    result = subprocess.run(
        ["az", "storage", "blob", "download", "--container-name", storage["container"],
         "--account-name", storage["account"], "--auth-mode", "key", "--name", blob_name,
         "--file", str(target), "--no-progress", "--only-show-errors", "--output", "none"],
        text=True, capture_output=True, env=storage_env(storage, account_key), timeout=3600,
    )
    if result.returncode:
        target.unlink(missing_ok=True)
        fail(f"Blob download failed for {Path(blob_name).name} (exit {result.returncode}).")
    if not target.is_file():
        fail(f"Blob download did not create the expected local file for {Path(blob_name).name}.")
    os.chmod(target, 0o600)
    digest = hashlib.sha256()
    size = 0
    with target.open("rb") as source:
        while True:
            block = source.read(4 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
            size += len(block)
    return {"objectName": blob_name, "bytes": size, "sha256": digest.hexdigest()}


def decompressed_size(path: Path, suffix: str) -> int:
    if not suffix:
        return path.stat().st_size
    if suffix == ".gz":
        stream = gzip.open(path, "rb")
    elif suffix == ".bz2":
        stream = bz2.open(path, "rb")
    elif suffix == ".xz":
        stream = lzma.open(path, "rb")
    else:
        fail(f"Cannot validate decompression suffix {suffix} with the installed standard library.")
    total = 0
    try:
        with stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                total += len(block)
    except (OSError, EOFError, lzma.LZMAError):
        fail(f"Compressed WAL object failed integrity/decompression validation: {path.name}.")
    return total


def decompressed_small_file(path: Path, suffix: str, *, limit: int = 4 * 1024 * 1024) -> bytes:
    try:
        if not suffix:
            if path.stat().st_size > limit:
                fail(f"Archived metadata object exceeds the {limit}-byte size limit.")
            data = path.read_bytes()
        elif suffix == ".gz":
            with gzip.open(path, "rb") as source:
                data = source.read(limit + 1)
                if len(data) <= limit and source.read(1):
                    data += b"x"
        elif suffix == ".bz2":
            with bz2.open(path, "rb") as source:
                data = source.read(limit + 1)
                if len(data) <= limit and source.read(1):
                    data += b"x"
        elif suffix == ".xz":
            with lzma.open(path, "rb") as source:
                data = source.read(limit + 1)
                if len(data) <= limit and source.read(1):
                    data += b"x"
        else:
            fail(f"Cannot validate small archived object compression {suffix}.")
    except (OSError, EOFError, lzma.LZMAError):
        fail(f"Compressed metadata object failed integrity/decompression validation: {path.name}.")
    if len(data) > limit:
        fail(f"Archived metadata object exceeds the {limit}-byte decompression limit.")
    return data


def parse_history(data: bytes, expected: dict) -> None:
    try:
        history = data.decode("utf-8")
    except UnicodeDecodeError:
        fail("Backup-history object is not UTF-8 text.")
    fields = label_fields(history)
    expected_start = expected["begin_lsn"]
    expected_end = expected["end_lsn"]
    if fields.get("START WAL LOCATION", "").split(" ", 1)[0] != expected_start:
        fail("Backup-history object has a different start LSN than the Backup resource.")
    if fields.get("STOP WAL LOCATION", "").split(" ", 1)[0] != expected_end:
        fail("Backup-history object has a different stop LSN than the Backup resource.")
    start_match = re.search(r"\(file\s+([0-9A-Fa-f]{24})\)", fields.get("START WAL LOCATION", ""))
    stop_match = re.search(r"\(file\s+([0-9A-Fa-f]{24})\)", fields.get("STOP WAL LOCATION", ""))
    if not start_match or start_match.group(1).upper() != expected["begin_wal"]:
        fail("Backup-history object start WAL segment differs from Backup status.")
    if not stop_match or stop_match.group(1).upper() != expected["end_wal"]:
        fail("Backup-history object stop WAL segment differs from Backup status.")
    if fields.get("LABEL") != expected["backup_name"]:
        fail("Backup-history object label does not match the Backup resource.")
    if fields.get("START TIMELINE") != str(expected["timeline"]):
        fail("Backup-history start timeline does not match the Backup resource.")
    stop_timeline = fields.get("STOP TIMELINE")
    if stop_timeline and stop_timeline != str(expected["timeline"]):
        fail("Backup-history stop timeline does not match the Backup resource.")


def parse_timeline_history(data: bytes, expected_timeline: int) -> None:
    try:
        content = data.decode("ascii")
    except UnicodeDecodeError:
        fail("Timeline history object is not ASCII text.")
    parents = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.match(r"^(\d+)\s+[0-9A-Fa-f]+/[0-9A-Fa-f]+\s+", stripped)
        if not match:
            fail("Timeline history object contains an unrecognized record.")
        parents.append(int(match.group(1)))
    if not parents or any(parent <= 0 or parent >= expected_timeline for parent in parents):
        fail("Timeline history object does not describe an ancestor of the Backup timeline.")


def find_owned_backup_for_live_probe(stack: str, storage: dict,
                                     backup_document: dict | None = None) -> dict:
    if backup_document is None:
        backups = read_json(storage["state_dir"] / "recovery" / "backup.json").get("items", [])
        owned = [b for b in backups if (b.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == storage["owner"]]
        if len(owned) != 1:
            fail(f"{stack}: expected exactly one owned Backup before probing WAL segment size.")
        backup = owned[0]
    else:
        backup = backup_document
        if ((backup.get("metadata", {}).get("labels") or {}).get("repro.stack.token") != storage["owner"] or
                backup.get("metadata", {}).get("namespace") != "repro"):
            fail(f"{stack}: captured Backup owner or namespace does not verify.")
    if backup.get("status", {}).get("phase") != "completed":
        fail(f"{stack}: owned Backup is not complete.")
    return backup


def read_live_wal_segment_size(stack: str, storage: dict, backup: dict) -> int:
    state = storage["state"]
    kubeconfig = isolated_kubeconfig(stack, state)
    instance = backup.get("status", {}).get("instanceID")
    pod = instance.get("podName") if isinstance(instance, dict) else instance
    if not isinstance(pod, str) or not pod:
        fail(f"{stack}: completed Backup does not identify its source pod.")
    result = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "-n", "repro", "exec", pod,
         "-c", "postgres", "--", "psql", "-AtX", "-c", "SHOW wal_segment_size"],
        text=True, capture_output=True, timeout=90,
    )
    if result.returncode:
        fail(f"{stack}: read-only PostgreSQL WAL segment size query failed (exit {result.returncode}).")
    value = result.stdout.strip()
    match = re.fullmatch(r"([0-9]+)(B|kB|MB|GB)", value)
    if not match:
        fail(f"{stack}: PostgreSQL returned an unrecognized WAL segment size.")
    factor = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}[match.group(2)]
    size = int(match.group(1)) * factor
    if size <= 0 or (1 << 32) % size:
        fail(f"{stack}: PostgreSQL WAL segment size is unsupported for filename arithmetic.")
    return size


def isolated_kubeconfig(stack: str, state: dict) -> Path:
    configured = state.get("kubeconfigPath") or ".state/kubeconfig"
    kubeconfig = Path(configured)
    if not kubeconfig.is_absolute():
        root_candidate = ROOT / kubeconfig
        stack_candidate = ROOT / stack / kubeconfig
        kubeconfig = root_candidate if root_candidate.is_file() else stack_candidate
    try:
        resolved = kubeconfig.resolve(strict=True)
        expected_state_dir = (ROOT / stack / ".state").resolve(strict=True)
    except OSError:
        fail(f"{stack}: isolated kubeconfig is unavailable for snapshot evidence capture.")
    if not resolved.is_file() or not resolved.is_relative_to(expected_state_dir):
        fail(f"{stack}: kubeconfig path is not isolated to this stack's state directory.")
    return resolved


def kubectl_json(stack: str, kubeconfig: Path, *args: str) -> dict:
    result = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), *args, "-o", "json"],
        text=True, capture_output=True, timeout=180,
    )
    if result.returncode:
        fail(f"{stack}: read-only Kubernetes query failed for {args[0]} (exit {result.returncode}).")
    if len(result.stdout.encode("utf-8")) > MAX_EVIDENCE_FILE_BYTES:
        fail(f"{stack}: Kubernetes evidence object exceeds the per-file size limit.")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        fail(f"{stack}: Kubernetes returned invalid JSON for {args[0]}.")
    if not isinstance(payload, dict):
        fail(f"{stack}: Kubernetes evidence object has an unexpected shape.")
    return payload


def kubectl_context(stack: str, kubeconfig: Path) -> str:
    result = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "config", "current-context"],
        text=True, capture_output=True, timeout=30,
    )
    if result.returncode or not result.stdout.strip():
        fail(f"{stack}: cannot verify the isolated Kubernetes context.")
    return result.stdout.strip()


def _snapshot_elements(backup_document: dict) -> dict[str, str]:
    elements = (backup_document.get("status", {}).get("snapshotBackupStatus", {})
                .get("elements", []))
    result = {}
    for element in elements:
        if element.get("type") in ("PG_DATA", "PG_WAL"):
            result[element["type"]] = element.get("name")
    if (set(result) != {"PG_DATA", "PG_WAL"} or
            not all(isinstance(v, str) and v for v in result.values()) or
            result["PG_DATA"] == result["PG_WAL"]):
        fail("Completed Backup does not identify one distinct DATA/WAL VolumeSnapshot pair.")
    return {"data": result["PG_DATA"], "wal": result["PG_WAL"]}


def _write_private_json(path: Path, value: dict) -> tuple[int, str]:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(data) > MAX_EVIDENCE_FILE_BYTES:
        fail(f"Evidence object {path.name} exceeds the per-file size limit.")
    path.write_bytes(data)
    os.chmod(path, 0o600)
    return len(data), hashlib.sha256(data).hexdigest()


def capture_snapshot_evidence(stack: str) -> Path:
    """Capture the original prepare.json Backup pair and current live snapshot lineage."""
    context = verify_preparation_capture_inputs(stack)
    final_dir = EVIDENCE_ROOT / stack / context["prepared"]["backup"]
    if final_dir.exists():
        fail(f"{stack}: snapshot evidence destination already exists; refusing to overwrite it.")
    storage = resolve_owned_storage(stack)
    state, prepared = context["state"], context["prepared"]
    namespace, cluster_name = prepared["cluster"].split("/", 1)
    if namespace != "repro" or cluster_name != "hdd-pg":
        fail(f"{stack}: preparation Cluster identity is outside the fixed repro namespace/cluster.")
    kubeconfig = isolated_kubeconfig(stack, state)
    current_context = kubectl_context(stack, kubeconfig)
    if state.get("clusterName") not in current_context:
        fail(f"{stack}: isolated Kubernetes context does not identify this owned AKS cluster.")

    ns = kubectl_json(stack, kubeconfig, "get", "namespace", namespace)
    if (ns.get("metadata", {}).get("name") != namespace or
            not ns.get("metadata", {}).get("uid") or
            (ns.get("metadata", {}).get("labels") or {}).get("repro.stack.token") != storage["owner"]):
        fail(f"{stack}: live Kubernetes namespace is not owned by this stack.")
    cluster = kubectl_json(stack, kubeconfig, "get", "cluster", cluster_name, "-n", namespace)
    cm = cluster.get("metadata", {})
    if (cluster.get("kind") != "Cluster" or cm.get("name") != cluster_name or not cm.get("uid") or
            cm.get("namespace") != namespace or
            (cm.get("labels") or {}).get("repro.stack.token") != storage["owner"]):
        fail(f"{stack}: live CNPG Cluster identity or owner label does not match prepared metadata.")

    backup_name = prepared["backup"]
    backup_document = kubectl_json(stack, kubeconfig, "get", "backup", backup_name, "-n", namespace)
    bm = backup_document.get("metadata", {})
    bs, bstatus = backup_document.get("spec", {}), backup_document.get("status", {})
    if (backup_document.get("kind") != "Backup" or bm.get("name") != backup_name or not bm.get("uid") or
            bm.get("namespace") != namespace or
            (bm.get("labels") or {}).get("repro.stack.token") != storage["owner"] or
            bs.get("cluster", {}).get("name") != cluster_name or
            bs.get("method") != "volumeSnapshot" or bs.get("online") is not True or
            bs.get("onlineConfiguration", {}).get("waitForArchive") is not True or
            bstatus.get("phase") != "completed"):
        fail(f"{stack}: live Backup name, owner, Cluster, or completed online snapshot settings do not match.")
    pair = _snapshot_elements(backup_document)
    if pair != prepared["replicaSnapshots"]:
        fail(f"{stack}: live Backup DATA/WAL names differ from original prepare.json replicaSnapshots.")

    live_cluster_archive = verify_archive_configuration(
        stack, storage, {"cluster": cluster_name}, [cluster]
    )
    vss_documents, vsc_documents, class_documents, arm_snapshots = [], [], [], []
    class_cache = {}
    for role, snapshot_name in (("data", pair["data"]), ("wal", pair["wal"])):
        vss = kubectl_json(stack, kubeconfig, "get", "volumesnapshot", snapshot_name, "-n", namespace)
        vsc_name = vss.get("status", {}).get("boundVolumeSnapshotContentName")
        if not isinstance(vsc_name, str) or not vsc_name:
            fail(f"{stack}: live VolumeSnapshot {snapshot_name} has no bound content name.")
        vsc = kubectl_json(stack, kubeconfig, "get", "volumesnapshotcontent", vsc_name)
        class_name = vss.get("spec", {}).get("volumeSnapshotClassName")
        if not isinstance(class_name, str) or not class_name:
            fail(f"{stack}: live VolumeSnapshot {snapshot_name} has no snapshot class.")
        if class_name not in class_cache:
            class_cache[class_name] = kubectl_json(
                stack, kubeconfig, "get", "volumesnapshotclass", class_name
            )
        handle = vsc.get("status", {}).get("snapshotHandle")
        if not isinstance(handle, str) or not handle:
            fail(f"{stack}: live VolumeSnapshotContent {vsc_name} has no Azure snapshot handle.")
        if f"/resourcegroups/{state['snapshotResourceGroup'].lower()}/providers/microsoft.compute/snapshots/" not in handle.lower():
            fail(f"{stack}: live VolumeSnapshotContent handle is outside the owned retained snapshot group.")
        arm = az_json(["snapshot", "show", "--ids", handle,
                       "--subscription", storage["subscription"],
                       "--only-show-errors", "--output", "json"])
        arm_id = arm.get("id", "")
        creation = arm.get("creationData") or {}
        tags = arm.get("tags") or {}
        if (canonical_id(arm_id) != canonical_id(handle) or not arm.get("uniqueId") or
                arm.get("provisioningState") != "Succeeded" or
                arm.get("snapshotAccessState") != "Available" or
                not is_complete_percent(arm.get("completionPercent")) or
                creation.get("createOption") != "Copy" or
                tags.get("reprotoken") != storage["owner"] or
                tags.get("reproPurpose") != "premiumv2-vacuum-backup" or
                canonical_id(creation.get("sourceResourceId", "")) != canonical_id(
                    vsc.get("spec", {}).get("source", {}).get("volumeHandle", ""))):
            fail(f"{stack}: live Azure snapshot {snapshot_name} owner, source, or readiness does not verify.")
        vss_documents.append(vss)
        vsc_documents.append(vsc)
        arm_snapshots.append(arm)
    class_documents.extend(class_cache.values())
    derived_audit = {
        "status": "complete",
        "ownerToken": storage["owner"],
        "backup": backup_name,
        "replicaSnapshots": pair,
        "retainedSnapshots": arm_snapshots,
    }
    backup_identity = {
        "backup_name": backup_name,
        "namespace": namespace,
        "cluster": cluster_name,
        "status": bstatus,
        "data_snapshot_name": pair["data"],
        "wal_snapshot_name": pair["wal"],
    }
    lineage = _verify_snapshot_object_set(
        stack, storage, backup_identity, vss_documents, vsc_documents,
        class_documents, derived_audit,
    )

    evidence_dir = EVIDENCE_ROOT / stack
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(EVIDENCE_ROOT, 0o700)
    os.chmod(evidence_dir, 0o700)
    final_dir = evidence_dir / backup_name
    if final_dir.exists():
        fail(f"{stack}: snapshot evidence destination already exists; refusing to overwrite it.")
    stage_dir = Path(tempfile.mkdtemp(prefix=f".{backup_name}-", suffix=".partial", dir=evidence_dir))
    os.chmod(stage_dir, 0o700)
    try:
        documents = {
            "namespace.json": ns,
            "cluster.json": cluster,
            "backup.json": backup_document,
            "volumesnapshot-data.json": vss_documents[0],
            "volumesnapshot-wal.json": vss_documents[1],
            "volumesnapshotcontent-data.json": vsc_documents[0],
            "volumesnapshotcontent-wal.json": vsc_documents[1],
            "volumesnapshotclasses.json": {"items": class_documents},
            "azure-snapshot-data.json": arm_snapshots[0],
            "azure-snapshot-wal.json": arm_snapshots[1],
        }
        file_records, total_bytes = {}, 0
        for filename, document in documents.items():
            size, digest = _write_private_json(stage_dir / filename, document)
            total_bytes += size
            file_records[filename] = {"bytes": size, "sha256": digest}
        if total_bytes > MAX_EVIDENCE_TOTAL_BYTES:
            fail(f"{stack}: total captured snapshot evidence exceeds the size limit.")
        receipt = {
            "version": 1,
            "status": "complete",
            "capturedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "stack": stack,
            "namespace": {"name": namespace, "uid": ns["metadata"].get("uid"), "ownerVerified": True},
            "cluster": {"name": cluster_name, "namespace": namespace,
                        "uid": cm.get("uid"), "ownerVerified": True},
            "backup": {"name": backup_name, "uid": bm.get("uid"),
                       "namespace": namespace, "phase": "completed"},
            "replicaSnapshots": pair,
            "sourceReceipts": {
                **context["sourceHashes"],
                "sourceStageReceiptPath": context["receiptPreparation"]["sourceStageReceiptPath"],
                "sourceStageReceiptSha256": context["sourceStageReceiptSha256"],
                "sourceStageHelperSha256": context["sourceStageHelperSha256"],
            },
            "archiveDestination": {
                "host": live_cluster_archive["host"],
                "container": live_cluster_archive["container"],
                "prefix": live_cluster_archive["prefix"],
                "serverName": live_cluster_archive["server_name"],
            },
            "snapshotLineage": lineage,
            "derivedAuditBasis": "original completed prepare.json backup/replicaSnapshots plus live owned Backup, VolumeSnapshots, VolumeSnapshotContents, Retain classes, and exact live Azure snapshot records",
            "files": file_records,
            "credentialsPersisted": False,
        }
        _write_private_json(stage_dir / "capture.json", receipt)
        stage_dir.rename(final_dir)
        print(json.dumps({"status": "complete", "stack": stack, "backup": backup_name,
                          "localPath": str(final_dir), "credentialsPersisted": False}))
        return final_dir
    except BaseException as error:
        marker = stage_dir / "INCOMPLETE.txt"
        if not marker.exists():
            marker.write_text("Snapshot evidence capture did not complete; review as partial evidence only.\n")
            os.chmod(marker, 0o600)
        if isinstance(error, ExportError):
            raise
        fail(f"{stack}: snapshot evidence capture stopped unexpectedly ({type(error).__name__}).")


def load_captured_snapshot_evidence(stack: str, storage: dict) -> dict:
    context = verify_preparation_capture_inputs(stack)
    prepared = context["prepared"]
    evidence_dir = EVIDENCE_ROOT / stack / prepared["backup"]
    receipt_path = evidence_dir / "capture.json"
    receipt = read_json(receipt_path)
    if (receipt.get("version") != 1 or receipt.get("status") != "complete" or
            receipt.get("stack") != stack or receipt.get("backup", {}).get("name") != prepared["backup"] or
            receipt.get("replicaSnapshots") != prepared.get("replicaSnapshots")):
        fail(f"{stack}: completed live snapshot evidence does not match original prepare.json.")
    if receipt.get("sourceReceipts", {}).get("sourceStageReceiptSha256") != context["sourceStageReceiptSha256"]:
        fail(f"{stack}: captured evidence is bound to a different source-stage receipt.")
    for key, value in context["sourceHashes"].items():
        if receipt.get("sourceReceipts", {}).get(key) != value:
            fail(f"{stack}: captured evidence preparation hash changed ({key}).")
    files = receipt.get("files")
    if not isinstance(files, dict) or set(files) != {
            "namespace.json", "cluster.json", "backup.json",
            "volumesnapshot-data.json", "volumesnapshot-wal.json",
            "volumesnapshotcontent-data.json", "volumesnapshotcontent-wal.json",
            "volumesnapshotclasses.json", "azure-snapshot-data.json", "azure-snapshot-wal.json"}:
        fail(f"{stack}: captured evidence file inventory is incomplete or unexpected.")
    total_bytes = 0
    documents = {}
    for filename, record in files.items():
        path = evidence_dir / filename
        if path.parent != evidence_dir or not path.is_file():
            fail(f"{stack}: captured evidence file is missing or outside its evidence directory.")
        if path.stat().st_size > MAX_EVIDENCE_FILE_BYTES:
            fail(f"{stack}: captured evidence file exceeds its per-file size limit.")
        require_sha256(path, record.get("sha256"), f"{stack} captured {filename}")
        if path.stat().st_size != record.get("bytes"):
            fail(f"{stack}: captured evidence byte length changed for {filename}.")
        total_bytes += path.stat().st_size
        documents[filename] = read_json(path)
    if total_bytes > MAX_EVIDENCE_TOTAL_BYTES:
        fail(f"{stack}: captured evidence exceeds its total size limit.")
    state = context["state"]
    namespace_name = "repro"
    cluster_name = "hdd-pg"
    ns, cluster, backup_doc = documents["namespace.json"], documents["cluster.json"], documents["backup.json"]
    ns_meta = ns.get("metadata", {})
    cluster_meta = cluster.get("metadata", {})
    backup_meta = backup_doc.get("metadata", {})
    if (ns_meta.get("name") != "repro" or
            ns_meta.get("uid") != receipt.get("namespace", {}).get("uid") or
            (ns_meta.get("labels") or {}).get("repro.stack.token") != storage["owner"] or
            cluster.get("metadata", {}).get("name") != cluster_name or
            cluster.get("metadata", {}).get("namespace") != namespace_name or
            cluster_meta.get("uid") != receipt.get("cluster", {}).get("uid") or
            (cluster_meta.get("labels") or {}).get("repro.stack.token") != storage["owner"] or
            backup_meta.get("name") != prepared["backup"] or
            backup_meta.get("namespace") != namespace_name or
            backup_meta.get("uid") != receipt.get("backup", {}).get("uid") or
            (backup_meta.get("labels") or {}).get("repro.stack.token") != storage["owner"]):
        fail(f"{stack}: saved live Kubernetes objects no longer match this stack's owner/name identities.")
    pair = prepared["replicaSnapshots"]
    vss_documents = [documents["volumesnapshot-data.json"], documents["volumesnapshot-wal.json"]]
    vsc_documents = [documents["volumesnapshotcontent-data.json"], documents["volumesnapshotcontent-wal.json"]]
    class_documents = documents["volumesnapshotclasses.json"].get("items", [])
    arm_snapshots = [documents["azure-snapshot-data.json"], documents["azure-snapshot-wal.json"]]
    derived_audit = {
        "status": "complete", "ownerToken": storage["owner"],
        "backup": prepared["backup"], "replicaSnapshots": pair,
        "retainedSnapshots": arm_snapshots,
    }
    return {
        "prepare": context["prepared"],
        "backupDocument": backup_doc,
        "clusterList": [cluster],
        "volumeSnapshots": vss_documents,
        "volumeSnapshotContents": vsc_documents,
        "volumeSnapshotClasses": class_documents,
        "derivedAudit": derived_audit,
        "namespace": ns,
        "receipt": receipt,
    }


def build_plan(stack: str) -> tuple[dict, dict, list[dict]]:
    preparation_context = None
    snapshot_evidence = None
    if stack in PREPARATION_STACKS:
        preparation_context = verify_preparation_capture_inputs(stack)
        # This fails locally before any Blob credential read unless bounded live
        # snapshot capture has already been completed and hash-verified.
        storage_for_evidence = {
            "state_dir": ROOT / stack / ".state",
            "owner": preparation_context["state"]["ownerToken"],
        }
        snapshot_evidence = load_captured_snapshot_evidence(stack, storage_for_evidence)
    storage = resolve_owned_storage(stack)
    captured_backup = snapshot_evidence["backupDocument"] if snapshot_evidence else None
    backup_for_probe = find_owned_backup_for_live_probe(stack, storage, captured_backup)
    wal_segment_size = read_live_wal_segment_size(stack, storage, backup_for_probe)
    backup = verify_backup_metadata(stack, storage, wal_segment_size, captured_backup)
    archive = verify_archive_configuration(
        stack, storage, backup,
        snapshot_evidence["clusterList"] if snapshot_evidence else None,
    )
    snapshots = verify_snapshot_lineage(stack, storage, backup, snapshot_evidence)

    # The Blob prefix includes only this stack's owned CNPG namespace. The server
    # name follows that prefix in Barman's standard object layout.
    wal_prefix = archive["archive_prefix"]
    object_jobs = []
    for segment in backup["segments"]:
        # Barman groups WAL by the first 16 hex characters of the WAL segment.
        object_jobs.append({"kind": "wal", "base": segment,
                            "prefix": f"{wal_prefix}{segment[:16]}/"})
    history_name = backup["backup_history_name"]
    history_start = backup["begin_wal"][:16]
    object_jobs.append({"kind": "backup-history", "base": history_name,
                        "prefix": f"{wal_prefix}{history_start}/"})
    if backup["timeline"] > 1:
        tli_name = f"{backup['timeline']:08X}.history"
        object_jobs.append({"kind": "timeline-history", "base": tli_name,
                            "prefix": wal_prefix})
    return storage, {**backup, **archive, "wal_segment_size_bytes": wal_segment_size}, snapshots + object_jobs


def run_export(stack: str, *, plan_only: bool) -> Path | None:
    storage, plan, mixed = build_plan(stack)
    snapshots = mixed[:2]
    jobs = mixed[2:]
    if len(snapshots) != 2 or len(jobs) < 2:
        fail(f"{stack}: incomplete snapshot/archive export plan.")
    if plan_only:
        safe_prefix = plan["prefix"].replace(storage["owner"], "<owner-scope>")
        safe_archive_prefix = plan["archive_prefix"].replace(storage["owner"], "<owner-scope>")
        print(json.dumps({
            "stack": stack,
            "backupName": plan["backup_name"],
            "backupUid": plan["backup_uid"],
            "walRange": {"beginLsn": plan["begin_lsn"], "endLsn": plan["end_lsn"],
                         "beginWal": plan["begin_wal"], "endWal": plan["end_wal"],
                         "segments": plan["segments"]},
            "archive": {"host": plan["host"], "container": plan["container"],
                        "prefix": safe_prefix, "serverName": plan["server_name"]},
            "objectsToExport": [{"kind": j["kind"], "basename": j["base"],
                                 "prefix": j["prefix"].replace(storage["owner"], "<owner-scope>")} for j in jobs],
            "snapshotUids": [s["snapshot_uid"] for s in snapshots],
            "ownershipVerifiedLive": True,
            "liveContainerVerified": False,
            "archivePrefix": safe_archive_prefix,
            "planOnly": True,
        }, indent=2))
        return None

    account_key = get_account_key(storage)
    verify_live_container(storage, account_key)
    # Limit listing to the exact stack/server WAL archive path. Fetch all pages
    # because the CLI's '*' requests a complete enumeration for that prefix.
    list_result = subprocess.run(
        ["az", "storage", "blob", "list", "--container-name", storage["container"],
         "--account-name", storage["account"], "--auth-mode", "key", "--prefix", plan["archive_prefix"],
         "--num-results", "*", "--only-show-errors", "--output", "json"],
        text=True, capture_output=True, env=storage_env(storage, account_key), timeout=900,
    )
    if list_result.returncode:
        fail("Read-only Blob listing failed under the verified owned archive prefix.")
    try:
        blobs = json.loads(list_result.stdout)
    except json.JSONDecodeError:
        fail("Blob listing returned invalid JSON.")
    if not isinstance(blobs, list):
        fail("Blob listing did not return an object list.")

    resolved = []
    for job in jobs:
        matches = []
        for blob in blobs:
            name = blob.get("name", "")
            leaf = name.rsplit("/", 1)[-1]
            base, suffix = normalized_object_name(job["base"], leaf)
            if base and name.startswith(job["prefix"]):
                matches.append((blob, suffix))
        if len(matches) != 1:
            fail(f"Expected exactly one archived object for {job['base']}; found {len(matches)}.")
        blob, suffix = matches[0]
        suffix = compression_for_object(plan["wal_settings"] or {}, blob["name"])
        if suffix in (".zst", ".snappy", ".lz4"):
            fail(f"Archived object compression {suffix} is not supported for integrity validation.")
        length = (blob.get("properties") or {}).get("contentLength")
        if not isinstance(length, int) or length <= 0:
            fail(f"Archived object {job['base']} has no positive content length.")
        resolved.append({**job, "blob": blob, "suffix": suffix, "listed_bytes": length})

    total = sum(x["listed_bytes"] for x in resolved)
    if shutil.disk_usage(REVIEW_DIR).free < total + 32 * 1024 * 1024:
        fail("Insufficient local free space for the verified exact WAL/archive object set.")

    owner_label = plan["backup_name"].replace("/", "_")
    EXPORT_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(EXPORT_ROOT, 0o700)
    final_dir = EXPORT_ROOT / stack / owner_label
    staging_parent = EXPORT_ROOT / ".staging"
    staging_parent.mkdir(mode=0o700, exist_ok=True)
    os.chmod(staging_parent, 0o700)
    if final_dir.exists():
        fail(f"{stack}: export destination already exists; refusing to overwrite preserved bytes.")
    stage_dir = Path(tempfile.mkdtemp(prefix=f"{stack}-", dir=staging_parent))
    os.chmod(stage_dir, 0o700)

    try:
        exported = []
        for item in resolved:
            suffix = item["suffix"]
            leaf = item["base"] + suffix
            target = stage_dir / leaf
            downloaded = download_blob(storage, item["blob"]["name"], target, account_key)
            if downloaded["bytes"] != item["listed_bytes"]:
                fail(f"Downloaded byte count differs from Blob metadata for {item['base']}.")
            if item["kind"] == "wal":
                expanded = decompressed_size(target, suffix)
                if expanded != plan["wal_segment_size_bytes"]:
                    fail(f"WAL object {item['base']} expands to {expanded} bytes, expected {plan['wal_segment_size_bytes']}.")
                downloaded["uncompressedBytes"] = expanded
                downloaded["compression"] = suffix.lstrip(".") or "none"
            elif item["kind"] == "backup-history":
                history_bytes = decompressed_small_file(target, suffix)
                parse_history(history_bytes, plan)
                downloaded["uncompressedBytes"] = len(history_bytes)
            elif item["kind"] == "timeline-history":
                timeline_bytes = decompressed_small_file(target, suffix, limit=1024 * 1024)
                parse_timeline_history(timeline_bytes, plan["timeline"])
                downloaded["uncompressedBytes"] = len(timeline_bytes)
            downloaded["kind"] = item["kind"]
            exported.append(downloaded)

        # Persist the backup label returned by pg_backup_stop, alongside the
        # archived PostgreSQL backup-history object. Both are non-secret metadata.
        label_target = stage_dir / "backup_label.txt"
        label_target.write_text(plan["label"])
        os.chmod(label_target, 0o600)
        label_hash = hashlib.sha256(label_target.read_bytes()).hexdigest()

        now = dt.datetime.now(dt.timezone.utc).isoformat()
        receipt = {
            "version": 1,
            "status": "complete",
            "exportedAtUtc": now,
            "stack": stack,
            "ownerVerification": {
                "localStateMatches": True,
                "liveResourceGroupTagMatches": True,
                "liveStorageAccountIdAndTagsMatch": True,
                "liveContainerMetadataMatches": True,
                "accountKeySource": "ARM listKeys through brokered Azure CLI; retained in process memory only",
            },
            "source": {
                "subscriptionId": storage["subscription"],
                "resourceGroup": storage["resource_group"],
                "storageAccountId": storage["account_id"],
                "storageAccountName": storage["account"],
                "blobHost": plan["host"],
                "container": plan["container"],
                "destinationPrefix": plan["prefix"],
                "serverName": plan["server_name"],
                "archivePrefix": plan["archive_prefix"],
            },
            "backup": {
                "name": plan["backup_name"],
                "uid": plan["backup_uid"],
                "cluster": plan["cluster"],
                "phase": "completed",
                "method": "volumeSnapshot",
                "online": True,
                "waitForArchive": True,
                "startedAt": plan["status"].get("startedAt"),
                "stoppedAt": plan["status"].get("stoppedAt"),
                "beginLSN": plan["begin_lsn"],
                "endLSN": plan["end_lsn"],
                "beginWal": plan["begin_wal"],
                "endWal": plan["end_wal"],
                "timeline": plan["timeline"],
                "walSegmentSizeBytes": plan["wal_segment_size_bytes"],
                "backupLabelFile": {"path": "backup_label.txt", "bytes": label_target.stat().st_size,
                                     "sha256": label_hash},
            },
            "snapshots": snapshots,
            "exportedObjects": exported,
            "verification": {
                "allStartThroughEndSegmentsPresent": True,
                "rangeContiguous": True,
                "allWALSegmentsExpandToExpectedSize": True,
                "backupHistoryMatchesBackupStatus": True,
                "credentialsWritten": False,
                "credentialsIncludedInReceipt": False,
            },
            "restoreNotes": [
                "Create retained Kubernetes VolumeSnapshot definitions from the recorded Azure snapshot handles and preserve their Backup labels/UID lineage.",
                "For CNPG recovery from this online VolumeSnapshot Backup, configure the matching external WAL archive and provide both storage and walStorage snapshots.",
                "Restore archived files under the recorded destination prefix/server archive layout; use the WAL segment list and hashes to verify the imported archive.",
                "For replay beyond this Backup's endLSN, additional contiguous timeline WAL through the desired target is required and is not included unless its segments appear in exportedObjects.",
            ],
        }
        receipt_path = stage_dir / "receipt.json"
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
        os.chmod(receipt_path, 0o600)
        final_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(final_dir.parent, 0o700)
        stage_dir.rename(final_dir)
        print(json.dumps({"status": "complete", "stack": stack,
                          "backup": plan["backup_name"], "segmentCount": len(plan["segments"]),
                          "objectCount": len(exported), "localPath": str(final_dir),
                          "credentialsPersisted": False}))
        return final_dir
    except BaseException as error:
        # Preserve any downloaded bytes for review; never claim success without
        # a complete receipt, and never delete or overwrite partial evidence.
        marker = stage_dir / "INCOMPLETE.txt"
        if not marker.exists():
            marker.write_text("Export did not complete. Files are partial evidence only; do not use as a restore archive.\n")
            os.chmod(marker, 0o600)
        if isinstance(error, ExportError):
            raise
        fail(f"{stack}: export stopped unexpectedly ({type(error).__name__}); partial bytes preserved privately.")
    finally:
        # Drop our only direct reference to the key promptly. Python does not
        # guarantee zeroization, but no persistent copy was written.
        account_key = ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stack", choices=STACKS)
    parser.add_argument("--plan-only", action="store_true", help="Verify ownership and print the exact WAL export plan without reading Blob credentials/data.")
    parser.add_argument("--capture-evidence", action="store_true", help="Capture bounded live paired-snapshot lineage for a completed 50/100-GiB preparation; no Blob credentials or writes.")
    args = parser.parse_args()
    if args.plan_only and args.capture_evidence:
        parser.error("--plan-only and --capture-evidence cannot be combined")
    os.umask(0o077)
    try:
        if args.capture_evidence:
            capture_snapshot_evidence(args.stack)
        else:
            run_export(args.stack, plan_only=args.plan_only)
    except ExportError as error:
        print(f"export blocked: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
