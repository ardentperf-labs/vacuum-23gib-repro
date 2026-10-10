#!/usr/bin/env python3
"""Verify local WAL exports before cleanup can remove their source Blob accounts.

This is a local-only guard: it reads preparation, export, and current retention
audit receipts plus the exported files. It performs no Azure/Kubernetes calls
and writes no receipts or credentials.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import stat
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
STACKS = ("baseline-23gib", "50gib", "100gib")
RETENTION_AUDIT = Path("preflight/snapshot-audit-before-compute-cleanup.json")
EXPORT_ROOT = Path("recovery/backup-retention-review/exports")
EVIDENCE_ROOT = Path("recovery/backup-retention-review/evidence")
MAX_AUDIT_AGE = dt.timedelta(minutes=30)
EXPECTED_WAL_SEGMENT_BYTES = 16 * 1024 * 1024
EXPECTED_EXPORTER_SHA256 = "915b5972c845597865b51d10e97604ecc9e21dfcd662be5c998f936fe77f0a85"
HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9.-]{0,62}$")


class WalRetentionError(RuntimeError):
    """Missing, changed, unsafe, or incomplete local WAL retention evidence."""


def _load_exporter_helpers():
    """Reuse the exporter's PostgreSQL WAL parsing/decompression validators."""
    helper_path = ROOT / "recovery/backup-retention-review/export_cnpg_wal.py"
    try:
        actual = hashlib.sha256(helper_path.read_bytes()).hexdigest()
    except OSError:
        raise WalRetentionError("WAL exporter source is unavailable.") from None
    if actual != EXPECTED_EXPORTER_SHA256:
        raise WalRetentionError("WAL exporter source differs from the reviewed SHA-256.")
    spec = importlib.util.spec_from_file_location("wal_exporter_for_cleanup_guard", helper_path)
    if spec is None or spec.loader is None:
        raise WalRetentionError("WAL exporter validation helpers are unavailable.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPORTER = _load_exporter_helpers()


def fail(message: str) -> None:
    raise WalRetentionError(message)


def _root_path(root: Path) -> Path:
    try:
        return root.resolve(strict=True)
    except OSError:
        fail("Experiment evidence root is unavailable.")


def _safe_file(root: Path, relative: Path) -> Path:
    if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
        fail("Evidence path is not a safe relative path.")
    current = root
    for index, part in enumerate(relative.parts):
        current = current / part
        try:
            mode = current.lstat().st_mode
        except OSError:
            fail(f"Required local evidence is missing: {relative.as_posix()}")
        if stat.S_ISLNK(mode):
            fail(f"Symlink is not allowed in local evidence path: {relative.as_posix()}")
        final = index == len(relative.parts) - 1
        if final:
            if not stat.S_ISREG(mode):
                fail(f"Local evidence is not a regular file: {relative.as_posix()}")
        elif not stat.S_ISDIR(mode):
            fail(f"Local evidence parent is not a directory: {relative.as_posix()}")
    return current


def _safe_directory(root: Path, relative: Path) -> Path:
    if relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts):
        fail("Evidence directory is not a safe relative path.")
    current = root
    for part in relative.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except OSError:
            fail(f"Required local evidence directory is missing: {relative.as_posix()}")
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            fail(f"Unsafe local evidence directory: {relative.as_posix()}")
    return current


def _read_json(root: Path, relative: Path) -> dict[str, Any]:
    path = _safe_file(root, relative)
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        fail(f"Required local JSON evidence is invalid: {relative.as_posix()}")
    if not isinstance(data, dict):
        fail(f"Required local JSON evidence is not an object: {relative.as_posix()}")
    return data


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
    except OSError:
        fail(f"Cannot read exported file: {path.name}")
    return digest.hexdigest()


def _canonical_id(value: str) -> str:
    return value.rstrip("/").lower()


def _utc(value: Any, description: str) -> dt.datetime:
    if not isinstance(value, str):
        fail(f"{description} timestamp is missing.")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail(f"{description} timestamp is invalid.")
    if parsed.tzinfo is None:
        fail(f"{description} timestamp has no timezone.")
    return parsed.astimezone(dt.timezone.utc)


def _verify_current_retention_audit(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    audit = _read_json(root, RETENTION_AUDIT)
    checked = _utc(audit.get("checkedAtUtc"), "retention audit")
    age = dt.datetime.now(dt.timezone.utc) - checked
    if age < -dt.timedelta(minutes=2) or age > MAX_AUDIT_AGE:
        fail("Current pre-cleanup retention audit is stale or future-dated.")
    if (audit.get("preservedPriorSnapshots") is not True or
            audit.get("allSnapshotsProtected") is not True or
            audit.get("missing") != [] or audit.get("changedIdentities") != [] or
            audit.get("unprotected") != []):
        fail("Current pre-cleanup retention audit does not prove every snapshot is preserved and protected.")
    snapshots = audit.get("snapshots")
    if not isinstance(snapshots, list) or audit.get("currentSnapshotCount") != len(snapshots):
        fail("Current pre-cleanup retention audit has an incomplete snapshot inventory.")
    by_id: dict[str, dict[str, Any]] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            fail("Current pre-cleanup retention audit contains an invalid snapshot row.")
        snapshot_id = snapshot.get("id")
        unique_id = snapshot.get("uniqueId")
        if not isinstance(snapshot_id, str) or not snapshot_id or not isinstance(unique_id, str) or not unique_id:
            fail("Current pre-cleanup retention audit has a snapshot without ID/uniqueId.")
        key = _canonical_id(snapshot_id)
        if key in by_id:
            fail("Current pre-cleanup retention audit repeats a snapshot ID.")
        by_id[key] = snapshot
    return audit, by_id


def _verify_storage(stack: str, state: dict[str, Any], storage: dict[str, Any]) -> dict[str, str]:
    subscription = state.get("subscriptionId")
    owner = state.get("ownerToken")
    resource_group = state.get("resourceGroup")
    account = storage.get("accountName")
    container = storage.get("containerName")
    account_id = storage.get("accountId")
    if (state.get("status") != "ready" or storage.get("status") != "ready" or
            not isinstance(owner, str) or not re.fullmatch(r"[a-f0-9]{32}", owner) or
            not all(isinstance(x, str) and x for x in (subscription, resource_group, account, container, account_id))):
        fail(f"{stack}: owned local state/storage identity is incomplete.")
    expected_id = (f"/subscriptions/{subscription}/resourceGroups/{resource_group}/"
                   f"providers/Microsoft.Storage/storageAccounts/{account}")
    if (_canonical_id(account_id) != _canonical_id(expected_id) or
            storage.get("subscriptionId") != subscription or
            storage.get("resourceGroup") != resource_group or
            storage.get("ownerToken") != owner):
        fail(f"{stack}: local storage account differs from the owned stack state.")
    return {"subscriptionId": subscription, "resourceGroup": resource_group,
            "ownerToken": owner, "accountName": account, "accountId": account_id,
            "container": container}


def _expected_suffix(record: dict[str, Any], base: str, kind: str) -> str:
    object_name = record.get("objectName")
    if not isinstance(object_name, str) or "\\" in object_name:
        fail("Export receipt has an invalid Blob object name.")
    suffixes = ("", ".gz", ".bz2", ".xz")
    suffix = next((item for item in suffixes if object_name.endswith("/" + base + item)), None)
    if suffix is None:
        fail(f"Export receipt does not name the expected {kind} object {base}.")
    # The exporter stores suffix.lstrip(".") in receipt.compression, so its
    # values are "gz", "bz2", and "xz" rather than codec display names.
    compression = {"": "none", ".gz": "gz", ".bz2": "bz2", ".xz": "xz"}[suffix]
    recorded = record.get("compression")
    if kind == "wal" and recorded != compression:
        fail(f"WAL compression metadata disagrees with object name for {base}.")
    if recorded is not None and recorded != compression:
        fail(f"Compression metadata disagrees with object name for {base}.")
    return suffix


def _read_export_object(root: Path, export_dir: Path, prefix: str,
                        record: dict[str, Any], base: str, kind: str,
                        expected_uncompressed: int | None) -> tuple[Path, bytes | None]:
    suffix = _expected_suffix(record, base, kind)
    object_name = record["objectName"]
    if kind == "timeline-history":
        expected_object = prefix + base + suffix
    else:
        expected_object = prefix + base[:16] + "/" + base + suffix
    if object_name != expected_object:
        fail(f"Export receipt object path is outside the exact owned archive prefix for {base}.")
    relative_name = object_name[len(prefix):]
    relative_parts = Path(relative_name).parts
    expected_parts = (base + suffix,) if kind == "timeline-history" else (base[:16], base + suffix)
    if tuple(relative_parts) != expected_parts:
        fail(f"Export receipt object path is not a safe archive-relative WAL path for {base}.")
    # The exporter preserves Azure object names in the receipt but writes every
    # downloaded object as a flat local basename inside this backup directory.
    path = _safe_file(root, export_dir.relative_to(root) / (base + suffix))
    expected_hash = record.get("sha256")
    expected_bytes = record.get("bytes")
    expected_expanded = record.get("uncompressedBytes")
    if (not isinstance(expected_hash, str) or not HEX_SHA256.fullmatch(expected_hash) or
            not isinstance(expected_bytes, int) or expected_bytes <= 0 or
            not isinstance(expected_expanded, int) or expected_expanded <= 0):
        fail(f"Export receipt has invalid integrity metadata for {base}.")
    if path.stat().st_size != expected_bytes or _sha256(path) != expected_hash:
        fail(f"Exported file size or SHA-256 changed for {base}.")
    try:
        if kind == "wal":
            expanded = EXPORTER.decompressed_size(path, suffix)
            data = None
        else:
            data = EXPORTER.decompressed_small_file(path, suffix)
            expanded = len(data)
    except Exception as exc:
        fail(f"Exported {kind} object failed decompression/integrity validation ({type(exc).__name__}).")
    if expanded != expected_expanded or (expected_uncompressed is not None and expanded != expected_uncompressed):
        fail(f"Exported {kind} object expands to an unexpected byte length for {base}.")
    return path, data


def _verify_backup_label(root: Path, export_dir: Path, backup: dict[str, Any],
                         frozen_label: str) -> None:
    metadata = backup.get("backupLabelFile")
    if not isinstance(metadata, dict) or metadata.get("path") != "backup_label.txt":
        fail("Export receipt does not bind the expected local backup_label.txt.")
    relative = export_dir.relative_to(root) / "backup_label.txt"
    path = _safe_file(root, relative)
    expected_bytes, expected_hash = metadata.get("bytes"), metadata.get("sha256")
    if (not isinstance(expected_bytes, int) or expected_bytes <= 0 or
            not isinstance(expected_hash, str) or not HEX_SHA256.fullmatch(expected_hash) or
            path.stat().st_size != expected_bytes or _sha256(path) != expected_hash):
        fail("backup_label.txt no longer matches its recorded length/SHA-256.")
    try:
        label_text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        fail("backup_label.txt is missing or invalid UTF-8.")
    if label_text != frozen_label:
        fail("backup_label.txt differs from the frozen completed Backup status.")
    fields = EXPORTER.label_fields(label_text)
    start = backup["beginLSN"]
    if (fields.get("START WAL FILE", "").upper() != backup["beginWal"].upper() or
            fields.get("START WAL LOCATION", "").split(" ", 1)[0] != start or
            fields.get("LABEL") != backup["name"] or
            fields.get("START TIMELINE") != str(backup["timeline"])):
        fail("backup_label.txt does not match the Backup start LSN/WAL, label, and timeline.")


def _verify_export_files(root: Path, export_dir: Path, receipt: dict[str, Any],
                         backup: dict[str, Any], owner: str,
                         frozen_label: str) -> int:
    try:
        segment_size = int(backup.get("walSegmentSizeBytes"))
    except (TypeError, ValueError):
        fail("Export receipt has no valid WAL segment size.")
    if segment_size != EXPECTED_WAL_SEGMENT_BYTES:
        fail("Exported PostgreSQL WAL segment size differs from the prepared 16-MiB default.")
    try:
        segments = EXPORTER.wal_sequence(backup.get("beginWal"), backup.get("endWal"), segment_size)
        history_name = EXPORTER.backup_history_name(backup.get("beginLSN"), backup.get("beginWal"), segment_size)
    except Exception as exc:
        fail(f"Export receipt WAL range is invalid ({type(exc).__name__}).")
    timeline = backup.get("timeline")
    if not isinstance(timeline, int) or timeline < 1:
        fail("Export receipt has an invalid WAL timeline.")
    if int(segments[0][:8], 16) != timeline or int(segments[-1][:8], 16) != timeline:
        fail("Exported WAL range and declared timeline disagree.")
    if receipt.get("verification", {}).get("allStartThroughEndSegmentsPresent") is not True or receipt.get("verification", {}).get("rangeContiguous") is not True or receipt.get("verification", {}).get("allWALSegmentsExpandToExpectedSize") is not True or receipt.get("verification", {}).get("backupHistoryMatchesBackupStatus") is not True:
        fail("Export receipt does not claim all required WAL/history checks passed.")
    source = receipt.get("source") or {}
    prefix = source.get("archivePrefix")
    expected_prefix = f"cnpg-vacuum-only/{owner}/{backup['cluster']}/wals/"
    if prefix != expected_prefix:
        fail("Export receipt archive prefix is not the exact owned stack/Cluster prefix.")

    records = receipt.get("exportedObjects")
    if not isinstance(records, list):
        fail("Export receipt has no exported-object list.")
    expected: list[tuple[str, str]] = [(name, "wal") for name in segments]
    expected.append((history_name, "backup-history"))
    if timeline > 1:
        expected.append((f"{timeline:08X}.history", "timeline-history"))
    if len(records) != len(expected):
        fail("Export receipt object count differs from the exact Backup WAL/history range.")
    record_map: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            fail("Export receipt contains an invalid exported-object record.")
        kind = record.get("kind")
        object_name = record.get("objectName")
        leaf = object_name.rsplit("/", 1)[-1] if isinstance(object_name, str) else ""
        match = next(((base, expected_kind) for base, expected_kind in expected
                      if expected_kind == kind and any(leaf == base + suffix for suffix in ("", ".gz", ".bz2", ".xz"))), None)
        if match is None or match in record_map:
            fail("Exported object set is missing, duplicated, or outside the exact expected range.")
        record_map[match] = record
    if set(record_map) != set(expected):
        fail("Exported object set does not exactly cover the begin/end WAL range and history metadata.")

    for base, kind in expected:
        if kind == "wal":
            _read_export_object(root, export_dir, prefix, record_map[(base, kind)], base, kind, segment_size)
        else:
            _, content = _read_export_object(root, export_dir, prefix, record_map[(base, kind)], base, kind, None)
            if content is None:
                fail(f"Exported {kind} metadata was not decoded: {base}.")
            if kind == "backup-history":
                expected_history = {
                    "begin_lsn": backup["beginLSN"], "end_lsn": backup["endLSN"],
                    "begin_wal": backup["beginWal"].upper(), "end_wal": backup["endWal"].upper(),
                    "backup_name": backup["name"], "timeline": timeline,
                }
                try:
                    EXPORTER.parse_history(content, expected_history)
                except Exception as exc:
                    fail(f"Exported backup-history metadata does not match Backup status ({type(exc).__name__}).")
            else:
                try:
                    EXPORTER.parse_timeline_history(content, timeline)
                except Exception as exc:
                    fail(f"Exported timeline-history metadata is invalid ({type(exc).__name__}).")
    _verify_backup_label(root, export_dir, backup, frozen_label)
    return len(segments)


def _verify_capture_binding(root: Path, stack: str, backup_name: str,
                            pair: dict[str, str], prepared_path: Path,
                            process_path: Path, owner: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """For 50/100, require export-time capture to bind the frozen prepare bytes."""
    rel = EVIDENCE_ROOT / stack / backup_name / "capture.json"
    capture = _read_json(root, rel)
    if (capture.get("status") != "complete" or capture.get("stack") != stack or
            capture.get("backup", {}).get("name") != backup_name or
            capture.get("replicaSnapshots") != pair):
        fail(f"{stack}: captured Backup evidence does not match frozen prepare metadata.")
    source_hashes = capture.get("sourceReceipts") or {}
    if (_sha256(prepared_path) != source_hashes.get("prepareMetadataSha256") or
            _sha256(process_path) != source_hashes.get("prepareProcessSha256")):
        fail(f"{stack}: captured Backup evidence is bound to different preparation receipts.")
    lineage = capture.get("snapshotLineage")
    if not isinstance(lineage, list) or len(lineage) != 2:
        fail(f"{stack}: captured Backup evidence has no exact paired snapshot lineage.")
    evidence_dir = EVIDENCE_ROOT / stack / backup_name
    files = capture.get("files")
    backup_record = files.get("backup.json") if isinstance(files, dict) else None
    if (not isinstance(backup_record, dict) or
            not isinstance(backup_record.get("bytes"), int) or backup_record["bytes"] <= 0 or
            not isinstance(backup_record.get("sha256"), str) or
            not HEX_SHA256.fullmatch(backup_record["sha256"])):
        fail(f"{stack}: captured Backup source document has no valid byte/hash record.")
    backup_path = _safe_file(root, evidence_dir / "backup.json")
    if (backup_path.stat().st_size != backup_record["bytes"] or
            _sha256(backup_path) != backup_record["sha256"]):
        fail(f"{stack}: captured Backup source document differs from its capture receipt.")
    backup_document = _read_json(root, evidence_dir / "backup.json")
    if (backup_document.get("metadata", {}).get("uid") != capture.get("backup", {}).get("uid") or
            (backup_document.get("metadata", {}).get("labels") or {}).get("repro.stack.token") != owner):
        fail(f"{stack}: captured Backup source document differs from its owned capture identity.")
    return capture, backup_document


def _verify_frozen_backup_document(backup_document: dict[str, Any], stack: str,
                                   owner: str, backup_name: str,
                                   pair: dict[str, str]) -> tuple[dict[str, Any], str]:
    metadata = backup_document.get("metadata") or {}
    spec = backup_document.get("spec") or {}
    status = backup_document.get("status") or {}
    if (backup_document.get("kind") != "Backup" or metadata.get("name") != backup_name or
            metadata.get("namespace") != "repro" or
            (metadata.get("labels") or {}).get("repro.stack.token") != owner or
            spec.get("cluster", {}).get("name") != "hdd-pg" or
            spec.get("method") != "volumeSnapshot" or spec.get("online") is not True or
            spec.get("onlineConfiguration", {}).get("waitForArchive") is not True or
            status.get("phase") != "completed" or status.get("method") != "volumeSnapshot" or
            status.get("online") is not True or status.get("backupName") != backup_name):
        fail(f"{stack}: frozen source Backup document is not the completed owned online VolumeSnapshot Backup.")
    status_pair = status.get("snapshotBackupStatus", {}).get("elements")
    if not isinstance(status_pair, list) or len(status_pair) != 2:
        fail(f"{stack}: frozen Backup status has no exact DATA/WAL snapshot pair.")
    by_role: dict[str, str] = {}
    for element in status_pair:
        if not isinstance(element, dict) or element.get("type") not in ("PG_DATA", "PG_WAL"):
            fail(f"{stack}: frozen Backup status contains an invalid snapshot role.")
        role = "data" if element["type"] == "PG_DATA" else "wal"
        if role in by_role or not isinstance(element.get("name"), str):
            fail(f"{stack}: frozen Backup status repeats or omits a snapshot role.")
        by_role[role] = element["name"]
    if by_role != pair:
        fail(f"{stack}: frozen Backup status snapshot pair differs from prepare.json.")

    for field in ("beginLSN", "endLSN", "beginWal", "endWal"):
        if not isinstance(status.get(field), str) or not status[field]:
            fail(f"{stack}: frozen Backup status is missing {field}.")
    try:
        label = EXPORTER.backup_label(status)
        label_fields = EXPORTER.label_fields(label)
        timeline = int(label_fields.get("START TIMELINE", ""))
    except Exception as exc:
        fail(f"{stack}: frozen Backup status has no valid label/timeline ({type(exc).__name__}).")
    if (timeline < 1 or label_fields.get("LABEL") != backup_name or
            label_fields.get("START WAL LOCATION", "").split(" ", 1)[0] != status["beginLSN"] or
            label_fields.get("START WAL FILE", "").upper() != status["beginWal"].upper()):
        fail(f"{stack}: frozen Backup label does not match its name/start LSN/WAL/timeline.")
    return {
        "name": backup_name,
        "uid": metadata.get("uid"),
        "cluster": "hdd-pg",
        "beginLSN": status["beginLSN"],
        "endLSN": status["endLSN"],
        "beginWal": status["beginWal"],
        "endWal": status["endWal"],
        "timeline": timeline,
    }, label


def _compare_export_range_to_frozen(backup_receipt: dict[str, Any],
                                    frozen: dict[str, Any], stack: str) -> None:
    if (backup_receipt.get("name") != frozen["name"] or
            backup_receipt.get("uid") != frozen["uid"] or
            backup_receipt.get("cluster") != frozen["cluster"] or
            backup_receipt.get("beginLSN") != frozen["beginLSN"] or
            backup_receipt.get("endLSN") != frozen["endLSN"] or
            not isinstance(backup_receipt.get("beginWal"), str) or
            backup_receipt["beginWal"].upper() != frozen["beginWal"].upper() or
            not isinstance(backup_receipt.get("endWal"), str) or
            backup_receipt["endWal"].upper() != frozen["endWal"].upper() or
            backup_receipt.get("timeline") != frozen["timeline"]):
        fail(f"{stack}: WAL export range/timeline differs from the frozen Backup status.")


def _verify_baseline_binding(root: Path, prepared: dict[str, Any], state: dict[str, Any],
                             backup_name: str, pair: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    audit_rel = Path("baseline-23gib/.state/recovery/baseline-finalize-audit.json")
    finalizer = _read_json(root, audit_rel)
    expected_receipt_path = str(root / audit_rel)
    final = prepared.get("finalAudit") or {}
    if (final.get("receipt") != expected_receipt_path or
            final.get("ownedRetentionSnapshotsReady") is not True or
            finalizer.get("status") != "complete" or
            finalizer.get("ownerToken") != state.get("ownerToken") or
            finalizer.get("backup") != backup_name or
            finalizer.get("replicaSnapshots") != pair):
        fail("baseline: completed finalization receipt does not bind the frozen Backup and snapshot pair.")
    backup_inventory_rel = Path("baseline-23gib/.state/recovery/backup.json")
    backup_inventory = _read_json(root, backup_inventory_rel).get("items")
    if not isinstance(backup_inventory, list):
        fail("baseline: saved Backup inventory is missing.")
    matches = [item for item in backup_inventory
               if item.get("kind") == "Backup" and
               item.get("metadata", {}).get("name") == backup_name and
               item.get("metadata", {}).get("namespace") == "repro" and
               (item.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == state.get("ownerToken")]
    if len(matches) != 1 or not isinstance(matches[0].get("metadata", {}).get("uid"), str):
        fail("baseline: saved Backup object does not uniquely bind the owned preparation.")
    return finalizer, matches[0]


def _verify_stack(root: Path, stack: str, audit_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    state_rel = Path(stack) / ".state/state.json"
    storage_rel = Path(stack) / ".state/storage.json"
    process_rel = Path(stack) / ".state/prepare-process.json"
    prepare_rel = Path(stack) / ".state/prepare.json"
    state = _read_json(root, state_rel)
    storage = _read_json(root, storage_rel)
    process = _read_json(root, process_rel)
    prepared = _read_json(root, prepare_rel)
    identity = _verify_storage(stack, state, storage)
    if (process.get("phase") != "prepare" or process.get("stack") != stack or
            process.get("status") != "complete" or process.get("exitCode") != 0 or
            prepared.get("status") != "complete"):
        fail(f"{stack}: preparation receipt is not complete with exit code zero.")
    backup_name = prepared.get("backup")
    pair = prepared.get("replicaSnapshots")
    if (not isinstance(backup_name, str) or not SAFE_NAME.fullmatch(backup_name) or
            not isinstance(pair, dict) or set(pair) != {"data", "wal"} or
            not all(isinstance(pair.get(k), str) and SAFE_NAME.fullmatch(pair[k]) for k in ("data", "wal")) or
            pair.get("data") == pair.get("wal")):
        fail(f"{stack}: frozen prepare Backup name or DATA/WAL snapshot pair is invalid.")
    capture = None
    source_backup_document = None
    baseline_finalizer = None
    baseline_backup_object = None
    if stack in ("50gib", "100gib"):
        capture, source_backup_document = _verify_capture_binding(
            root, stack, backup_name, pair, _safe_file(root, prepare_rel),
            _safe_file(root, process_rel), identity["ownerToken"])
    else:
        baseline_finalizer, baseline_backup_object = _verify_baseline_binding(
            root, prepared, state, backup_name, pair)
        source_backup_document = baseline_backup_object
    frozen_backup, frozen_label = _verify_frozen_backup_document(
        source_backup_document, stack, identity["ownerToken"], backup_name, pair)

    export_dir_rel = EXPORT_ROOT / stack / backup_name
    export_dir = _safe_directory(root, export_dir_rel)
    receipt_rel = export_dir_rel / "receipt.json"
    receipt = _read_json(root, receipt_rel)
    if (receipt.get("version") != 1 or receipt.get("status") != "complete" or
            receipt.get("stack") != stack):
        fail(f"{stack}: WAL export receipt is missing or incomplete.")
    owner_verification = receipt.get("ownerVerification") or {}
    if any(owner_verification.get(key) is not True for key in (
            "localStateMatches", "liveResourceGroupTagMatches",
            "liveStorageAccountIdAndTagsMatch", "liveContainerMetadataMatches")):
        fail(f"{stack}: WAL export receipt lacks complete owned-account verification.")
    source = receipt.get("source") or {}
    expected_prefix = f"cnpg-vacuum-only/{identity['ownerToken']}/"
    if (source.get("subscriptionId") != identity["subscriptionId"] or
            source.get("resourceGroup") != identity["resourceGroup"] or
            not isinstance(source.get("storageAccountId"), str) or
            _canonical_id(source["storageAccountId"]) != _canonical_id(identity["accountId"]) or
            source.get("storageAccountName") != identity["accountName"] or
            source.get("container") != identity["container"] or
            source.get("blobHost") != identity["accountName"] + ".blob.core.windows.net" or
            source.get("destinationPrefix") != expected_prefix):
        fail(f"{stack}: WAL export source does not match the owned local storage account/container.")

    backup = receipt.get("backup") or {}
    cluster = prepared.get("cluster")
    if not isinstance(cluster, str) or cluster != "repro/hdd-pg":
        fail(f"{stack}: frozen preparation Cluster identity is unexpected.")
    if (backup.get("name") != backup_name or backup.get("phase") != "completed" or
            backup.get("method") != "volumeSnapshot" or backup.get("online") is not True or
            backup.get("waitForArchive") is not True or
            backup.get("cluster") != cluster.split("/", 1)[-1] or
            not isinstance(backup.get("uid"), str) or not backup.get("uid")):
        fail(f"{stack}: WAL export receipt does not match the frozen completed online Backup.")
    if capture is not None and backup.get("uid") != capture.get("backup", {}).get("uid"):
        fail(f"{stack}: exported Backup UID differs from frozen captured Backup evidence.")
    if baseline_backup_object is not None and backup.get("uid") != baseline_backup_object.get("metadata", {}).get("uid"):
        fail("baseline: exported Backup UID differs from the saved owned Backup object.")
    _compare_export_range_to_frozen(backup, frozen_backup, stack)
    if (source.get("serverName") != backup.get("cluster") or
            source.get("archivePrefix") != f"{expected_prefix}{backup['cluster']}/wals/"):
        fail(f"{stack}: WAL export server/archive prefix does not match the owned Backup.")
    if receipt.get("verification", {}).get("credentialsWritten") is not False or receipt.get("verification", {}).get("credentialsIncludedInReceipt") is not False:
        fail(f"{stack}: export receipt does not confirm credentials were excluded.")
    snapshots = receipt.get("snapshots")
    if not isinstance(snapshots, list) or len(snapshots) != 2:
        fail(f"{stack}: WAL export receipt does not contain the paired DATA/WAL snapshots.")
    snapshot_ids = set()
    for role, snapshot in zip(("data", "wal"), snapshots):
        if not isinstance(snapshot, dict) or snapshot.get("snapshot_name") != pair[role]:
            fail(f"{stack}: exported snapshot pair differs from prepare.json ({role}).")
        snapshot_id = snapshot.get("azure_snapshot_id")
        unique_id = snapshot.get("azure_snapshot_unique_id")
        snapshot_uid = snapshot.get("snapshot_uid")
        content_uid = snapshot.get("snapshot_content_uid")
        if not all(isinstance(x, str) and x for x in (snapshot_id, unique_id, snapshot_uid, content_uid)):
            fail(f"{stack}: exported {role} snapshot lineage is missing an ID/UID.")
        if snapshot_id.lower() in snapshot_ids:
            fail(f"{stack}: exported DATA/WAL snapshot IDs are not distinct.")
        snapshot_ids.add(snapshot_id.lower())
        snapshot_group = state.get("snapshotResourceGroup")
        if not isinstance(snapshot_group, str) or f"/resourcegroups/{snapshot_group.lower()}/providers/microsoft.compute/snapshots/" not in snapshot_id.lower():
            fail(f"{stack}: exported {role} snapshot ID is outside the retained snapshot resource group.")
        audit = audit_by_id.get(_canonical_id(snapshot_id))
        if (audit is None or audit.get("uniqueId") != unique_id or
                (audit.get("tags") or {}).get("reprotoken") != identity["ownerToken"] or
                (audit.get("tags") or {}).get("reproPurpose") != "premiumv2-vacuum-backup" or
                audit.get("state") != "Succeeded" or audit.get("accessState") != "Available" or
                not (audit.get("directCanNotDelete") or audit.get("groupCanNotDelete"))):
            fail(f"{stack}: exported {role} snapshot ID/UID is missing, changed, unowned, or unprotected in the current audit.")
        if capture is not None:
            captured = capture["snapshotLineage"][0 if role == "data" else 1]
            if any(captured.get(field) != snapshot.get(field) for field in (
                    "snapshot_name", "snapshot_uid", "snapshot_content_name", "snapshot_content_uid",
                    "azure_snapshot_id", "azure_snapshot_unique_id", "source_volume_handle", "source_pvc")):
                fail(f"{stack}: exported {role} snapshot identity differs from frozen captured lineage.")
        if baseline_finalizer is not None:
            retained = baseline_finalizer.get("retainedSnapshots")
            if not isinstance(retained, list):
                fail("baseline: completed finalization audit has no retained snapshot inventory.")
            final_matches = [item for item in retained if isinstance(item, dict) and
                             _canonical_id(item.get("id", "")) == _canonical_id(snapshot_id)]
            if (len(final_matches) != 1 or final_matches[0].get("uniqueId") != unique_id):
                fail(f"baseline: exported {role} snapshot differs from its completed finalization ID/UID.")

    segment_count = _verify_export_files(
        root, export_dir, receipt, backup, identity["ownerToken"], frozen_label)
    return {"stack": stack, "backup": backup_name, "walSegmentCount": segment_count,
            "exportedObjectCount": len(receipt["exportedObjects"]), "pairedSnapshotsVerified": 2}


def verify(root: Path = ROOT) -> dict[str, Any]:
    """Return a complete retention summary or raise without changing any resources."""
    root = _root_path(Path(root))
    audit, audit_by_id = _verify_current_retention_audit(root)
    stacks = [_verify_stack(root, name, audit_by_id) for name in STACKS]
    return {
        "status": "complete",
        "verifiedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "retentionAuditCheckedAtUtc": audit["checkedAtUtc"],
        "stacks": stacks,
        "cloudCalls": 0,
        "credentialsRead": False,
        "resourcesChanged": False,
    }


def main() -> int:
    try:
        print(json.dumps(verify(), indent=2))
        return 0
    except WalRetentionError as exc:
        print(f"WAL retention guard failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
