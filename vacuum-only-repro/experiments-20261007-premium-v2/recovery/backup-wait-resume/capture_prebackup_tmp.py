#!/usr/bin/env python3
"""Read-only capture of the 100 GiB active preparation TMP and running receipt.

Writes only a private archive under recovery/backup-wait-resume. It never edits
.state, the source TMP, Azure, or Kubernetes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "100gib"
RECOVERY = ROOT / "recovery/backup-wait-resume"
ORIGINAL_02_SHA = "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"
STAGE_HELPER_SHA = "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"
REQUIRED_TMP_FILES = ("cluster.yaml", "backup.yaml", "seed-path")


class CaptureError(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise CaptureError(message)


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError(f"Invalid local metadata ({path.name}: {exc.__class__.__name__})") from None
    require(isinstance(value, dict), f"Local metadata {path.name} is not a JSON object")
    return value


def file_manifest(directory: Path) -> dict[str, dict[str, Any]]:
    require(directory.is_dir() and not directory.is_symlink(), "Original TMP is not a regular directory")
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.rglob("*")):
        rel = path.relative_to(directory).as_posix()
        require(not path.is_symlink(), f"Original TMP contains a symlink ({rel})")
        if path.is_dir():
            continue
        require(path.is_file(), f"Original TMP contains a non-regular entry ({rel})")
        result[rel] = {"sha256": digest_file(path), "sizeBytes": path.stat().st_size}
    require(all(name in result for name in REQUIRED_TMP_FILES),
            "Original TMP has not reached the Backup request boundary (required files missing)")
    return result


def proc_cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError as exc:
        raise CaptureError(f"Cannot verify live preparation PID ({exc.__class__.__name__})") from None
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def command_has_path(argv: list[str], expected: Path) -> bool:
    for arg in argv:
        try:
            if Path(arg).resolve() == expected.resolve():
                return True
        except OSError:
            continue
    return False


def validate_running_receipt(process: dict[str, Any], state: dict[str, Any],
                             script_path: Path, script_sha: str, helper_sha: str,
                             child_cmdline: list[str], supervisor_cmdline: list[str]) -> tuple[int, int]:
    token = str(state.get("ownerToken", ""))
    require(state.get("version") == 1 and state.get("status") == "ready"
            and re.fullmatch(r"[a-f0-9]{32}", token) is not None,
            "100 GiB stack owner state is not ready")
    require(process.get("stack") == "100gib" and process.get("phase") == "prepare"
            and process.get("status") == "running"
            and process.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and process.get("sourceStageHelperSha256") == STAGE_HELPER_SHA
            and script_sha == ORIGINAL_02_SHA and helper_sha == STAGE_HELPER_SHA,
            "Current preparation receipt/script/helper is not the frozen running 100 GiB attempt")
    command = process.get("command")
    require(isinstance(command, list) and any("02-prepare.sh" in str(x) for x in command),
            "Running process receipt command does not identify original 02-prepare.sh")
    child, supervisor = process.get("childPid"), process.get("supervisorPid")
    require(isinstance(child, int) and child > 1 and isinstance(supervisor, int)
            and supervisor > 1 and supervisor != child,
            "Running process receipt lacks distinct supervisor/child PIDs")
    require(any(Path(arg).name == "02-prepare.sh" and Path(arg).resolve() == script_path
                for arg in child_cmdline),
            "Live child PID command line does not identify the frozen 100 GiB preparation script")
    require(command_has_path(supervisor_cmdline, ROOT / "run-phase.py")
            and command_has_path(supervisor_cmdline, STACK)
            and "prepare" in supervisor_cmdline,
            "Live supervisor PID command line does not identify the 100 GiB run-phase controller")
    return child, supervisor


def write_private(path: Path, data: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o600)


def ensure_under_recovery(path: Path) -> Path:
    require(not path.is_symlink(), "Capture output path cannot be a symlink")
    resolved = path.resolve()
    try:
        resolved.relative_to(RECOVERY.resolve())
    except ValueError:
        raise CaptureError("Capture output must be under recovery/backup-wait-resume") from None
    require(resolved != RECOVERY.resolve(), "Capture output must be a new child directory")
    require(not resolved.exists(), "Capture output directory already exists; preserve it and choose a new path")
    return resolved


def capture(source_tmp: Path, output: Path) -> dict[str, Any]:
    require(not source_tmp.is_symlink(), "Source TMP path cannot be a symlink")
    source_tmp = source_tmp.resolve()
    require(source_tmp.parent == (STACK / ".state").resolve()
            and re.fullmatch(r"\.prepare\.[A-Za-z0-9]+", source_tmp.name) is not None,
            "Source must be the original private 100 GiB .state/.prepare.* TMP")
    receipt_path = STACK / ".state/prepare-process.json"
    state_path = STACK / ".state/state.json"
    original_script = STACK / "02-prepare.sh"
    helper = ROOT / "recovery/source-restore/stage-allowlisted-source.sh"
    require(not original_script.is_symlink() and not helper.is_symlink(),
            "Pinned original script/helper path cannot be a symlink")
    script_sha, helper_sha = digest_file(original_script), digest_file(helper)
    require(script_sha == ORIGINAL_02_SHA and helper_sha == STAGE_HELPER_SHA,
            "Frozen original script or source-stage helper hash changed")
    output = ensure_under_recovery(output)
    process_bytes_before = receipt_path.read_bytes()
    process = load_json(receipt_path)
    state = load_json(state_path)
    child, supervisor = validate_running_receipt(
        process, state, original_script.resolve(), script_sha, helper_sha,
        proc_cmdline(int(process.get("childPid", 0))),
        proc_cmdline(int(process.get("supervisorPid", 0))))
    source_manifest_before = file_manifest(source_tmp)
    run_started = process.get("startedAtUtc")
    require(isinstance(run_started, str) and run_started, "Running process receipt lacks start timestamp")

    output.mkdir(mode=0o700, parents=True)
    os.chmod(output, 0o700)
    archive_tmp = output / "prebackup-tmp"
    archive_tmp.mkdir(mode=0o700)
    for rel in source_manifest_before:
        src = source_tmp / rel
        dst = archive_tmp / rel
        dst.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        data = src.read_bytes()
        write_private(dst, data)
    running_receipt_path = output / "original-process-running.json"
    write_private(running_receipt_path, process_bytes_before)
    archived_script_path = output / "original-02-prepare.sh"
    write_private(archived_script_path, original_script.read_bytes())
    archived_helper_path = output / "stage-allowlisted-source.sh"
    write_private(archived_helper_path, helper.read_bytes())

    require(digest_file(original_script) == script_sha
            and digest_file(helper) == helper_sha
            and digest_file(archived_script_path) == script_sha
            and digest_file(archived_helper_path) == helper_sha,
            "Pinned script/helper bytes changed or failed verification after archive copy")

    source_manifest_after = file_manifest(source_tmp)
    process_bytes_after = receipt_path.read_bytes()
    require(source_manifest_before == source_manifest_after
            and process_bytes_before == process_bytes_after,
            "Original TMP/process receipt changed during capture; partial archive is left for review")
    validate_running_receipt(
        process, state, original_script.resolve(), script_sha, helper_sha,
        proc_cmdline(child), proc_cmdline(supervisor))
    output_manifest = file_manifest(archive_tmp)
    require(output_manifest == source_manifest_before,
            "Archived TMP bytes differ from the original source directory")
    required_hashes = {name: output_manifest[name]["sha256"] for name in REQUIRED_TMP_FILES}
    capture_receipt = {
        "version": 1, "stack": "100gib", "processStatusAtCapture": "running",
        "capturedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "runStartedAtUtc": run_started,
        "sourceTempPath": str(source_tmp),
        "archivedTempPath": str(archive_tmp.relative_to(ROOT)),
        "childPidAtCapture": child, "supervisorPidAtCapture": supervisor,
        "originalScriptSha256": script_sha, "sourceStageHelperSha256": helper_sha,
        "captureHelperSha256": digest_file(Path(__file__)),
        "captureHelperPath": str(Path(__file__).resolve().relative_to(ROOT)),
        "originalScriptArchivePath": str(archived_script_path.relative_to(ROOT)),
        "sourceStageHelperArchivePath": str(archived_helper_path.relative_to(ROOT)),
        "runningProcessReceiptPath": str(running_receipt_path.relative_to(ROOT)),
        "runningProcessReceiptSha256": digest_bytes(process_bytes_before),
        "filesSha256": required_hashes,
        "fullTmpManifest": output_manifest,
        "captureMethod": "read-only byte copy with source and destination hashes before/after",
    }
    receipt_path_out = output / "prebackup-capture.json"
    write_private(receipt_path_out, (json.dumps(capture_receipt, indent=2, sort_keys=True) + "\n").encode())
    return {"receipt": receipt_path_out, "receiptSha256": digest_file(receipt_path_out),
            "fileCount": len(output_manifest), "scriptSha256": script_sha,
            "helperSha256": helper_sha}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-tmp", type=Path, required=True,
                        help="the exact active 100gib/.state/.prepare.* directory")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="new private archive directory under recovery/backup-wait-resume")
    args = parser.parse_args(argv)
    try:
        result = capture(args.source_tmp, args.output_dir)
        print(f"Captured {result['fileCount']} private TMP files; receipt SHA-256 {result['receiptSha256']}")
        print(f"Receipt: {result['receipt'].relative_to(ROOT)}")
        return 0
    except CaptureError as exc:
        print(f"CAPTURE BLOCKED: {exc}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, ValueError) as exc:
        print(f"CAPTURE BLOCKED: malformed local metadata ({exc.__class__.__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
