#!/usr/bin/env python3
"""Draft adapter for the explicitly reviewed 100 GiB Backup continuation.

It verifies the local continuation receipt chain before invoking the frozen,
read-only hydration finalizer. It writes external receipts only; it never
installs metadata in .state or changes Azure/Kubernetes/PostgreSQL.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
RECOVERY = ROOT / "recovery/backup-wait-resume"
STACK = ROOT / "100gib"
FINALIZER_PATH = ROOT / "recovery/numeric-hydration-finalizer/finalize_hydration.py"
FINALIZER_SHA = "78fc66344fd105ef96ac4299a9ddbf52ec5f74cff081b2b74898b099b0c6d2e1"
RESUMER_PATH = RECOVERY / "resume_after_backup_v2.py"
RESUMER_SHA = "9af2aa9ee75bafe1e888bcce12b471f1ade14239ff6e1992ef77870719016f1b"
ORIGINAL_02_SHA = "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"
STAGE_HELPER_SHA = "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"
CAPTURE_HELPER_SHA = "1a1437306ed4bfdf8edb872b7f5e1ade475eb54fa305a67b1048eb986dad214e"
BINDING_HELPER_SHA = "1a7e1b48ffe91a64ec7cdd90f8c520bb4ee5eb14edf60aa1e13e02e3e260dc3d"
SYSIMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
CAPTURE_FILES = ("pods-final.json", "pvcs-final.json", "hdd-pg-1-config.json",
                 "hdd-pg-2-config.json", "hdd-pg-1-pv.json", "hdd-pg-1-disk.json",
                 "backup.yaml", "cluster.yaml", "seed-path")
SNAPSHOT_IDENTITY_FIELDS = (
    "id", "uniqueId", "volumeSnapshotUID", "volumeSnapshotContent",
    "volumeSnapshotContentUID", "sourcePVC", "sourcePV", "sourcePVCUID",
    "sourceDiskId", "sourceDiskUniqueId", "sourceUniqueId", "createOption",
)


class AdapterError(RuntimeError):
    pass


class AdapterPending(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AdapterError(message)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(block)
    except OSError as exc:
        raise AdapterError(f"Cannot hash {path.name} ({exc.__class__.__name__})") from None
    return h.hexdigest()


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise AdapterError(f"Invalid local evidence {path.name} ({exc.__class__.__name__})") from None


def tree_manifest(directory: Path) -> dict[str, dict[str, Any]]:
    require(directory.is_dir() and not directory.is_symlink(), "Captured original TMP is missing or unsafe")
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.rglob("*")):
        rel = path.relative_to(directory).as_posix()
        require(not path.is_symlink(), f"Captured original TMP contains a symlink ({rel})")
        if path.is_dir():
            continue
        require(path.is_file(), f"Captured original TMP contains a non-file ({rel})")
        result[rel] = {"sha256": sha256(path), "sizeBytes": path.stat().st_size}
    return result


def repo_file(value: Any, label: str, *, under_recovery: bool = True,
              must_file: bool = True) -> Path:
    require(isinstance(value, str) and value and not Path(value).is_absolute(),
            f"{label} must be a repository-relative path")
    rel = Path(value)
    require(".." not in rel.parts, f"{label} cannot traverse outside the repository")
    path = ROOT / rel
    require(not path.is_symlink() and (path.is_file() if must_file else path.exists()),
            f"{label} is absent or a symlink")
    resolved = path.resolve()
    try:
        resolved.relative_to((RECOVERY if under_recovery else ROOT).resolve())
    except ValueError:
        raise AdapterError(f"{label} escapes its allowed evidence directory") from None
    return resolved


def private(path: Path, label: str) -> None:
    require(stat.S_IMODE(path.stat().st_mode) & 0o077 == 0,
            f"{label} is not private to the owner")


def load_resume_module():
    spec = importlib.util.spec_from_file_location("resume_after_backup_adapter", RESUMER_PATH)
    require(spec is not None and spec.loader is not None, "Cannot load the pinned resumer helpers")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_continuation(evidence_path: Path) -> dict[str, Any]:
    require(sha256(FINALIZER_PATH) == FINALIZER_SHA,
            "Frozen numeric hydration finalizer hash changed")
    require(sha256(RESUMER_PATH) == RESUMER_SHA,
            "Pinned resumer source is not frozen at the adapter's reviewed SHA")
    require(not evidence_path.is_symlink(), "Continuation evidence cannot be a symlink")
    evidence_path = evidence_path.resolve()
    try:
        evidence_path.relative_to(RECOVERY.resolve())
    except ValueError:
        raise AdapterError("Continuation evidence must be under recovery/backup-wait-resume") from None
    private(evidence_path, "Continuation evidence")
    evidence = load_json(evidence_path)
    evidence_sha = sha256(evidence_path)
    require(evidence.get("version") == 1
            and evidence.get("phase") == "tail-complete-awaiting-frozen-finalizer"
            and evidence.get("stackOwnerToken")
            and evidence.get("keeperSnapshotOwnerToken") != evidence.get("stackOwnerToken")
            and "Original 02 reached only the captured pre-Backup" in evidence.get("executionAttribution", "")
            and "pinned resumer continuation" in evidence.get("executionAttribution", ""),
            "Continuation receipt phase, owners, or attribution is incomplete")
    require(evidence.get("resumerScriptSha256") == RESUMER_SHA
            and evidence.get("originalScriptSha256") == ORIGINAL_02_SHA,
            "Continuation receipt does not bind the reviewed resumer and original frozen script")

    progress_path = evidence_path.parent / "resume-progress.json"
    require(progress_path.is_file() and not progress_path.is_symlink(),
            "Final continuation progress receipt is missing")
    private(progress_path, "Continuation progress receipt")
    progress = load_json(progress_path)
    require(progress.get("phase") == evidence.get("phase")
            and progress.get("resumerScriptSha256") == RESUMER_SHA
            and progress.get("originalScriptSha256") == ORIGINAL_02_SHA
            and progress.get("backup", {}).get("uid") == evidence.get("backup", {}).get("uid")
            and progress.get("pairedSnapshots") == evidence.get("pairedSnapshots")
            and progress.get("pinnedSnapshotIdentity") == evidence.get("pinnedSnapshotIdentity")
            and progress.get("replicaCatchup") == evidence.get("replicaCatchup"),
            "Continuation progress receipt disagrees with the final evidence")
    progress_sha = sha256(progress_path)

    capture_path = repo_file(evidence.get("preBackupCaptureReceiptPath"), "Capture receipt")
    running_path = repo_file(evidence.get("originalRunningProcessReceiptPath"), "Running process receipt")
    terminal_path = repo_file(evidence.get("originalProcessReceiptPath"), "Terminal process receipt")
    binding_path = repo_file(evidence.get("terminalProcessBindingPath"), "Terminal binding receipt")
    original_path = repo_file(evidence.get("originalScriptPath"), "Archived original 02 script")
    require(sha256(capture_path) == evidence.get("preBackupCaptureReceiptSha256")
            and sha256(running_path) == evidence.get("originalRunningProcessReceiptSha256")
            and sha256(terminal_path) == evidence.get("originalProcessReceiptSha256")
            and sha256(binding_path) == evidence.get("terminalProcessBindingSha256")
            and sha256(original_path) == ORIGINAL_02_SHA,
            "Original capture/process/script/binding receipt hash chain changed")
    private(capture_path, "Capture receipt")
    private(running_path, "Running process receipt")
    private(terminal_path, "Terminal process receipt")
    private(binding_path, "Terminal binding receipt")
    capture = load_json(capture_path)
    running = load_json(running_path)
    terminal = load_json(terminal_path)
    binding = load_json(binding_path)
    require(capture.get("captureHelperSha256") == CAPTURE_HELPER_SHA
            and evidence.get("captureHelperSha256") == CAPTURE_HELPER_SHA
            and evidence.get("captureHelperPath") == "recovery/backup-wait-resume/capture_prebackup_tmp.py"
            and sha256(RECOVERY / "capture_prebackup_tmp.py") == CAPTURE_HELPER_SHA
            and binding.get("bindingHelperSha256") == BINDING_HELPER_SHA
            and evidence.get("terminalBindingHelperSha256") == BINDING_HELPER_SHA
            and evidence.get("terminalBindingHelperPath") == "recovery/backup-wait-resume/bind_terminal_process.py"
            and sha256(RECOVERY / "bind_terminal_process.py") == BINDING_HELPER_SHA,
            "Capture/binding helper provenance changed")
    require(capture.get("runningProcessReceiptPath") == str(running_path.relative_to(ROOT))
            and binding.get("captureReceiptPath") == str(capture_path.relative_to(ROOT))
            and binding.get("runningProcessReceiptPath") == str(running_path.relative_to(ROOT))
            and binding.get("terminalProcessReceiptPath") == str(terminal_path.relative_to(ROOT)),
            "Capture and terminal-binding receipt paths do not identify the archived process chain")
    declared_inputs = evidence.get("inputHashes") or {}
    require(declared_inputs.get("captureReceiptSha256") == sha256(capture_path)
            and declared_inputs.get("terminalProcessBindingSha256") == sha256(binding_path)
            and progress.get("controlFlowInputHashes") == declared_inputs,
            "Continuation input hashes do not bind the capture/binding receipts")
    hashes = {"running": sha256(running_path), "terminal": sha256(terminal_path),
              "capture": sha256(capture_path)}
    resume_module = load_resume_module()
    resume_module.validate_process_identity_chain(running, terminal, binding,
        hashes["running"], hashes["terminal"], hashes["capture"], capture)
    require(capture.get("originalScriptSha256") == ORIGINAL_02_SHA
            and capture.get("sourceStageHelperSha256") == STAGE_HELPER_SHA
            and running.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and terminal.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and running.get("sourceStageHelperSha256") == STAGE_HELPER_SHA
            and terminal.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
            "Process chain is not bound to the frozen prepare script/helper")
    archived_script = repo_file(capture.get("originalScriptArchivePath"), "Captured original script")
    archived_helper = repo_file(capture.get("sourceStageHelperArchivePath"), "Captured source helper")
    require(sha256(archived_script) == ORIGINAL_02_SHA and sha256(archived_helper) == STAGE_HELPER_SHA,
            "Archived original source script/helper bytes changed")

    original_tmp = repo_file(evidence.get("preBackupCapturePath"), "Captured original TMP",
                             must_file=False)
    require(original_tmp.is_dir() and not original_tmp.is_symlink(), "Captured original TMP is not a directory")
    require(capture.get("archivedTempPath") == str(original_tmp.relative_to(ROOT))
            and capture.get("fullTmpManifest") == tree_manifest(original_tmp),
            "Pre-Backup TMP bytes do not match the original capture manifest")
    required_capture = {name: capture.get("filesSha256", {}).get(name)
                        for name in ("backup.yaml", "cluster.yaml", "seed-path")}
    stated_capture = evidence.get("preBackupCaptureFileSha256") or {}
    require(all(required_capture[name] == stated_capture.get(name) for name in required_capture),
            "Pre-Backup control file hashes differ from the capture receipt")

    local_hashes = evidence.get("localEvidenceHashes")
    require(isinstance(local_hashes, dict)
            and set(local_hashes) == {"state", "runConfig", "sourceManifest", "sourceMetadata",
                                      "sourceSnapshotRecord", "stageReceipt"},
            "Continuation lacks the complete pinned local preparation evidence hash set")
    state_path = STACK / ".state/state.json"
    state = load_json(state_path)
    stage_path = Path(str(state.get("snapshotSourceStageReceiptPath", "")))
    if not stage_path.is_absolute():
        stage_path = ROOT / stage_path
    require(stage_path.parent.resolve() == (STACK / ".state/source-stage").resolve()
            and stage_path.is_file() and not stage_path.is_symlink(),
            "Stack state no longer points to its exact owned source-stage receipt")
    manifest = load_json(STACK / "source-manifest.json")
    stage = load_json(stage_path)
    stack_token = str(state.get("ownerToken", ""))
    source_token = str(manifest.get("ownerToken", ""))
    require(state.get("status") == "ready"
            and re.fullmatch(r"[a-f0-9]{32}", stack_token) is not None
            and stack_token == evidence.get("stackOwnerToken")
            and source_token == evidence.get("keeperSnapshotOwnerToken")
            and source_token != stack_token and stage.get("ownerToken") == stack_token
            and stage.get("sourceManifest") == manifest,
            "Stack/keeper/stage owner identities differ from the continuation receipt")
    actual_local = {
        "state": sha256(state_path), "runConfig": sha256(STACK / "run-config.json"),
        "sourceManifest": sha256(STACK / "source-manifest.json"),
        "sourceMetadata": sha256(STACK / "snapshot.json"),
        "sourceSnapshotRecord": sha256(STACK / ".state/source-snapshot.json"),
        "stageReceipt": sha256(stage_path),
    }
    require(local_hashes == actual_local and progress.get("localEvidenceHashes") == local_hashes,
            "Pinned local source/owner metadata changed after continuation")

    pair = evidence.get("backup", {}).get("pair") or {}
    snapshots = evidence.get("pairedSnapshots") or {}
    identity_pins = evidence.get("pinnedSnapshotIdentity") or {}
    backup = evidence.get("backup") or {}
    require(backup.get("phase") == "completed" and isinstance(backup.get("uid"), str)
            and len(backup["uid"]) >= 8 and set(pair) == {"data", "wal"}
            and isinstance(pair.get("data"), str) and isinstance(pair.get("wal"), str)
            and pair["data"] != pair["wal"] and set(snapshots) == {pair["data"], pair["wal"]}
            and set(identity_pins) == set(snapshots),
            "Continuation does not identify one exact completed paired Backup")
    ids, uids = set(), set()
    for name, kind in ((pair["data"], "data"), (pair["wal"], "wal")):
        row = snapshots[name]
        pin = identity_pins[name]
        require(row.get("type") == kind and isinstance(row.get("id"), str)
                and isinstance(row.get("uniqueId"), str) and row.get("uniqueId")
                and row.get("provisioningState") == "Succeeded"
                and row.get("snapshotAccessState") == "Available"
                and row.get("readyToUse") is True and row.get("completionPercent") == 100.0
                and row.get("createOption") in ("Copy", "CopyStart")
                and all(pin.get(field) not in (None, "") and pin.get(field) == row.get(field)
                        for field in SNAPSHOT_IDENTITY_FIELDS),
                f"Paired Backup {kind} snapshot lacks exact Copy100 identity evidence")
        ids.add(row["id"].rstrip("/").lower())
        uids.add(row["uniqueId"])
    require(len(ids) == len(uids) == 2, "Paired Backup snapshots are not distinct ARM resources")

    journal_path = repo_file(evidence.get("snapshotJournalPath"), "External snapshot journal")
    journal_sha = sha256(journal_path)
    require(journal_sha == evidence.get("snapshotJournalSha256")
            and progress.get("externalSnapshotJournal", {}).get("sha256") == journal_sha,
            "Final paired snapshot journal hash changed")
    finalizer = load_frozen_finalizer()
    records = finalizer.load_output_snapshot_records(journal_path)
    require(set(records) == set(pair.values()), "External snapshot journal names differ from Backup outputs")
    for key, name in pair.items():
        row = snapshots[name]
        record = records[name]
        require(record.get("id", "").rstrip("/").lower() == row["id"].rstrip("/").lower()
                and record.get("sourcePVC") == row.get("sourcePVC")
                and row.get("sourcePVC") == ("hdd-pg-1" if key == "data" else "hdd-pg-1-wal"),
                f"External journal does not bind the exact {key} snapshot source")
    state_journal = STACK / ".state/output-snapshots.jsonl"
    require(state_journal.is_file() and not state_journal.is_symlink()
            and sha256(state_journal) == journal_sha,
            "Installed snapshot journal is not byte-identical to the continuation's external journal")

    seed = evidence.get("seedValidation") or {}
    events_path = repo_file(evidence.get("seedEventsPath"), "Seed Event evidence")
    require(sha256(events_path) == evidence.get("seedEventsSha256"), "Seed Event evidence hash changed")
    events_doc = load_json(events_path)
    require(seed.get("jobUID") and seed.get("jobUID") == evidence.get("seedValidation", {}).get("jobUID"),
            "Continuation lacks exact seed Job UID")
    pvs = seed.get("replicaPVCs") or {}
    pod = seed.get("replicaPod") or {}
    require(set(pvs) == {"hdd-pg-2", "hdd-pg-2-wal"}
            and all(isinstance(v.get("uid"), str) and v.get("uid")
                    and v.get("dataSource", {}).get("kind") == "VolumeSnapshot" for v in pvs.values())
            and isinstance(pod.get("uid"), str) and pod.get("name") == "hdd-pg-2",
            "Seed trace lacks exact replica PVC/pod identities")
    expected_source_names = {"hdd-pg-2": pair["data"], "hdd-pg-2-wal": pair["wal"]}
    for pvc_name, snapshot_name in expected_source_names.items():
        require(pvs[pvc_name]["dataSource"].get("name") == snapshot_name,
                f"Replica PVC {pvc_name} did not seed from its exact Backup snapshot")
    selected = resume_module.select_completed_seed_event(events_doc.get("items", []), seed["jobUID"],
        max(pvs[name]["creationTimestamp"] for name in expected_source_names),
        pod.get("creationTimestamp"))
    require(selected == seed.get("completedEvent")
            and resume_module.seed_event_timestamp(selected) == seed.get("completedEventTimestamp"),
            "Completed seed Event is not bound to the exact Job UID and replica chronology")
    seed_job_path = repo_file(evidence.get("seedJobReceiptPath"), "Seed Job receipt")
    require(sha256(seed_job_path) == evidence.get("seedJobReceiptSha256"), "Seed Job receipt hash changed")
    seed_job = load_json(seed_job_path)
    require(seed_job.get("uid") == seed["jobUID"] and seed_job.get("completedEvent") == selected
            and seed_job.get("systemImage") == SYSIMAGE and evidence.get("seedSystemImage") == SYSIMAGE
            and seed_job.get("sourceBackupSnapshots") == pair,
            "Seed Job receipt does not bind the completed Event, system image, and Backup pair")
    catchup = evidence.get("replicaCatchup") or {}
    require(catchup.get("streaming") is True and catchup.get("replicaCaughtUp") is True
            and progress.get("replicaCatchup") == catchup,
            "Continuation lacks replica streaming/replay catch-up evidence")

    control_path = repo_file(evidence.get("finalControlPath"), "Continuation final-control directory",
                             must_file=False)
    require(control_path.is_dir() and not control_path.is_symlink(), "Final-control path is not a directory")
    private(control_path, "Continuation final-control directory")
    final_control_hashes = evidence.get("finalControlFileSha256") or {}
    control_entries = list(control_path.iterdir())
    require(set(final_control_hashes) == set(CAPTURE_FILES)
            and {p.name for p in control_entries} == set(CAPTURE_FILES)
            and all(p.is_file() and not p.is_symlink() for p in control_entries),
            "Continuation final-control capture is incomplete or contains unexpected files")
    for name, expected in final_control_hashes.items():
        path = control_path / name
        require(not path.is_symlink() and path.is_file() and sha256(path) == expected,
                f"Continuation final-control file hash changed ({name})")
        private(path, f"Final-control file {name}")
    require(progress.get("finalControlCapture", {}).get("sha256") == final_control_hashes
            and evidence.get("finalControlPath") == progress.get("finalControlCapture", {}).get("path"),
            "Final-control capture differs from its progress receipt")

    return {"evidence": evidence, "evidencePath": evidence_path,
            "continuationEvidenceSha256": evidence_sha, "progress": progress,
            "progressPath": progress_path, "progressSha256": progress_sha,
            "capture": capture, "running": running, "terminal": terminal,
            "binding": binding, "originalScript": original_path,
            "terminalPath": terminal_path, "eventsPath": events_path,
            "seedJobPath": seed_job_path, "controlPath": control_path,
            "journalPath": journal_path, "journalSha256": journal_sha,
            "finalizerModule": finalizer}


def load_frozen_finalizer():
    require(sha256(FINALIZER_PATH) == FINALIZER_SHA, "Frozen finalizer hash changed")
    spec = importlib.util.spec_from_file_location("frozen_numeric_hydration_finalizer", FINALIZER_PATH)
    require(spec is not None and spec.loader is not None, "Cannot load frozen hydration finalizer")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def corrected_attribution(audit: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    result = dict(audit)
    checks = dict(result.get("checks") or {})
    continuation_capture = dict(checks.pop("originalFinalizationCapture", {}) or {})
    continuation_capture["meaning"] = (
        "This final control capture was generated by the pinned Backup-timeout continuation. The original 02 process "
        "did not execute the replica, image, default, or final hydration gates."
    )
    continuation_capture["producer"] = "backup-timeout-resumer-continuation"
    continuation_capture["resumerScriptSha256"] = RESUMER_SHA
    continuation_capture["continuationEvidencePath"] = str(provenance["evidencePath"].relative_to(ROOT))
    continuation_capture["continuationEvidenceSha256"] = sha256(provenance["evidencePath"])
    continuation_capture["progressReceiptPath"] = str(provenance["progressPath"].relative_to(ROOT))
    continuation_capture["progressReceiptSha256"] = provenance["progressSha256"]
    checks["continuationFinalizationCapture"] = continuation_capture
    evidence = provenance["evidence"]
    checks["originalPreBackupCapture"] = {
        "path": evidence["preBackupCapturePath"],
        "fileSha256": {key: evidence["preBackupCaptureFileSha256"][key]
                       for key in ("backup.yaml", "cluster.yaml", "seed-path")},
        "captureReceiptPath": evidence["preBackupCaptureReceiptPath"],
        "captureReceiptSha256": evidence["preBackupCaptureReceiptSha256"],
        "meaning": ("The original attempt reached this captured pre-Backup request boundary. The existing owner-bound "
                    "Backup matched that request; this local receipt chain is corroborating evidence, not a signed "
                    "attestation that a particular process issued the apply call."),
    }
    checks["continuationSeedEventEvidence"] = {
        "path": str(provenance["eventsPath"].relative_to(ROOT)),
        "sha256": evidence["seedEventsSha256"],
        "jobUID": evidence["seedValidation"]["jobUID"],
        "completedEvent": evidence["seedValidation"]["completedEvent"],
    }
    checks["continuationSnapshotJournal"] = {
        "path": str(provenance["journalPath"].relative_to(ROOT)),
        "sha256": provenance["journalSha256"], "records": 2,
    }
    seed_job = ((checks.get("backupAndReplicaSeed") or {}).get("seedJob") or {})
    archived_event = seed_job.get("archivedEventEvidence")
    if isinstance(archived_event, dict):
        archived_event = dict(archived_event)
        if "controlFlowCapturePath" in archived_event:
            archived_event["continuationFinalizationCapturePath"] = archived_event.pop("controlFlowCapturePath")
        if "controlFlowCaptureFileSha256" in archived_event:
            archived_event["continuationFinalizationCaptureFileSha256"] = archived_event.pop("controlFlowCaptureFileSha256")
        seed_job["archivedEventEvidence"] = archived_event
        checks["backupAndReplicaSeed"]["seedJob"] = seed_job
    result["checks"] = checks
    result["executionAttribution"] = (
        "The original preparation attempt reached the captured pre-Backup request boundary; the existing owner-bound "
        "Backup matched that request. The pinned continuation completed the Backup wait, replica seed, image/default "
        "tail, and final captures."
    )
    limits = [item for item in (result.get("limitations") or [])
              if "finalization capture prove control flow passed the original equality guards" not in item]
    limits.append("The frozen original script and captured pre-Backup TMP indicate the attempt reached the request boundary after its fixture equality guards; raw INSERT/DELETE output is unavailable, and this is corroborating control-flow evidence.")
    limits.append("Original 02 did not reach the final four-disk hydration loop; later tail/final-capture evidence is attributed to the pinned continuation.")
    limits.append("The local receipt chain is recorded corroborating evidence, not a signed execution attestation.")
    result["limitations"] = limits
    result["globalCloudQuiescenceClaim"] = False
    result["backupTimeoutContinuation"] = {
        "continuationEvidencePath": str(provenance["evidencePath"].relative_to(ROOT)),
        "continuationEvidenceSha256": sha256(provenance["evidencePath"]),
        "resumeProgressPath": str(provenance["progressPath"].relative_to(ROOT)),
        "resumeProgressSha256": provenance["progressSha256"],
        "backupUID": provenance["evidence"]["backup"]["uid"],
        "pairedSnapshotJournalPath": str(provenance["journalPath"].relative_to(ROOT)),
        "pairedSnapshotJournalSha256": provenance["journalSha256"],
        "originalProcessTerminalStatus": provenance["terminal"].get("status"),
    }
    return result


def validate_frozen_audit(audit: dict[str, Any], provenance: dict[str, Any]) -> None:
    """Cross-check the frozen live audit against the verified continuation chain."""
    evidence = provenance["evidence"]
    checks = audit.get("checks") or {}
    preparation = checks.get("frozenPreparation") or {}
    require(preparation.get("originalProcessReceiptSha256") == evidence.get("originalProcessReceiptSha256")
            and preparation.get("originalPrepareScriptSha256") == ORIGINAL_02_SHA
            and preparation.get("originalProcessStatus") in ("failed", "stopped"),
            "Frozen audit did not validate the exact stopped original preparation receipt/script")
    seed_check = checks.get("backupAndReplicaSeed") or {}
    backup = evidence.get("backup") or {}
    pair = backup.get("pair") or {}
    seed = evidence.get("seedValidation") or {}
    seed_job = seed_check.get("seedJob") or {}
    require(seed_check.get("backupUID") == backup.get("uid")
            and seed_check.get("backup") == backup.get("name")
            and seed_check.get("dataVolumeSnapshot") == pair.get("data")
            and seed_check.get("walVolumeSnapshot") == pair.get("wal")
            and seed_job.get("uid") == seed.get("jobUID"),
            "Frozen audit Backup/seed identities differ from continuation evidence")
    paired = checks.get("pairedBackupSnapshots") or {}
    snapshots = evidence.get("pairedSnapshots") or {}
    require(set(paired) == set(pair.values()) == set(snapshots),
            "Frozen audit paired snapshot names differ from the continuation")
    for name, row in snapshots.items():
        checked = paired[name]
        require(checked.get("id", "").rstrip("/").lower() == row.get("id", "").rstrip("/").lower()
                and checked.get("uniqueId") == row.get("uniqueId")
                and checked.get("sourcePVC") == row.get("sourcePVC")
                and checked.get("volumeSnapshotUID") == row.get("volumeSnapshotUID")
                and checked.get("sourceDiskId", "").rstrip("/").lower()
                    == row.get("sourceDiskId", "").rstrip("/").lower()
                and checked.get("sourceDiskUniqueId") == row.get("sourceDiskUniqueId")
                and checked.get("createOption") == row.get("createOption")
                and checked.get("content") == row.get("volumeSnapshotContent")
                and checked.get("completionPercent") == 100.0,
                f"Frozen audit paired snapshot identity/progress differs for {name}")
    capture = checks.get("originalFinalizationCapture") or {}
    require(capture.get("path") == evidence.get("finalControlPath")
            and capture.get("fileSha256") == evidence.get("finalControlFileSha256"),
            "Frozen audit final-control capture does not match the continuation's hashes")
    require(audit.get("globalCloudQuiescenceClaim") is False,
            "Frozen audit unexpectedly makes a global cloud quiescence claim")


def validate_unchanged_final_artifacts(provenance: dict[str, Any]) -> None:
    """Re-hash all continuation evidence after the live finalizer has returned."""
    evidence = provenance["evidence"]
    require(sha256(provenance["evidencePath"]) == provenance["continuationEvidenceSha256"]
            and sha256(provenance["progressPath"]) == provenance["progressSha256"]
            and sha256(provenance["eventsPath"]) == evidence.get("seedEventsSha256")
            and sha256(provenance["seedJobPath"]) == evidence.get("seedJobReceiptSha256")
            and sha256(provenance["journalPath"]) == provenance["journalSha256"]
            and sha256(STACK / ".state/output-snapshots.jsonl") == provenance["journalSha256"],
            "Continuation Event, Job, journal, or process evidence changed during frozen finalization")
    expected = evidence.get("finalControlFileSha256") or {}
    require(all(sha256(provenance["controlPath"] / name) == digest
                for name, digest in expected.items()),
            "Continuation final-control capture changed during frozen finalization")


def write_private(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o600)


def finalize(evidence_path: Path, output: Path) -> dict[str, Any]:
    provenance = validate_continuation(evidence_path)
    require(not output.is_symlink(), "Adapter output directory cannot be a symlink")
    output = output.resolve()
    try:
        output.relative_to(RECOVERY.resolve())
    except ValueError:
        raise AdapterError("Adapter receipts must stay under recovery/backup-wait-resume") from None
    require(not output.exists(), "Adapter output already exists; preserve it and choose a new path")
    module = provenance["finalizerModule"]
    finalizer = module.Finalizer(STACK, provenance["controlPath"], output,
        provenance["terminalPath"], provenance["originalScript"],
        provenance["evidence"]["seedValidation"]["jobUID"], provenance["eventsPath"],
        provenance["evidence"]["seedEventsSha256"])
    try:
        prepare, audit = finalizer.run()
    except module.HydrationPending:
        raise AdapterPending("validated PremiumV2 Copy/CopyStart hydration remains below 100 percent") from None
    validate_frozen_audit(audit, provenance)
    validate_unchanged_final_artifacts(provenance)
    audit = corrected_attribution(audit, provenance)
    audit["prepareReceipt"] = str((output / "prepare.json").relative_to(ROOT))
    audit["continuationReceiptChain"] = {
        "adapterScriptSha256": sha256(Path(__file__)),
        "frozenFinalizerSha256": FINALIZER_SHA,
        "continuationEvidencePath": str(provenance["evidencePath"].relative_to(ROOT)),
        "continuationEvidenceSha256": sha256(provenance["evidencePath"]),
        "resumeProgressPath": str(provenance["progressPath"].relative_to(ROOT)),
        "resumeProgressSha256": provenance["progressSha256"],
    }
    write_private(output / "audit.json", audit)
    write_private(output / "prepare.json", prepare)
    return {"output": output, "prepare": prepare, "audit": audit}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--continuation-evidence", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="new private external receipt directory under recovery/backup-wait-resume")
    args = parser.parse_args(argv)
    try:
        result = finalize(args.continuation_evidence, args.output_dir)
        print(f"Continuation hydration audit passed; external receipts are under {result['output'].relative_to(ROOT)}")
        return 0
    except AdapterPending as exc:
        print(f"HYDRATION PENDING: {exc}", file=sys.stderr)
        return 3
    except AdapterError as exc:
        print(f"CONTINUATION FINALIZATION BLOCKED: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        # Do not expose command output, credential material, or resource payloads.
        print(f"CONTINUATION FINALIZATION BLOCKED: frozen audit failed ({exc.__class__.__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
