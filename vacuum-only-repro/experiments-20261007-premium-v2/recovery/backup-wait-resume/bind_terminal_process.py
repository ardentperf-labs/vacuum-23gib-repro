#!/usr/bin/env python3
"""Bind the later terminal process receipt to a prior read-only TMP capture."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
RECOVERY = ROOT / "recovery/backup-wait-resume"
ORIGINAL_02_SHA = "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"
STAGE_HELPER_SHA = "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"


class BindError(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise BindError(message)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise BindError(f"Invalid process evidence ({path.name}: {exc.__class__.__name__})") from None
    require(isinstance(obj, dict), f"Process evidence {path.name} is not a JSON object")
    return obj


def parse_time(value: Any, label: str) -> dt.datetime:
    require(isinstance(value, str) and value, f"{label} timestamp is missing")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise BindError(f"{label} timestamp is malformed") from None
    require(parsed.tzinfo is not None and parsed.utcoffset() == dt.timedelta(0),
            f"{label} timestamp is not UTC")
    return parsed


def repo_path(value: Path) -> str:
    try:
        return str(value.resolve().relative_to(ROOT))
    except ValueError:
        raise BindError("All archived process/capture receipts must be under the repository recovery folder") from None


def bind(capture_receipt_path: Path, terminal_receipt_path: Path,
         output_path: Path) -> dict[str, Any]:
    for candidate in (capture_receipt_path, terminal_receipt_path, output_path):
        require(not candidate.is_symlink(), "Binding input/output paths cannot be symlinks")
    capture_receipt_path = capture_receipt_path.resolve()
    terminal_receipt_path = terminal_receipt_path.resolve()
    output_path = output_path.resolve()
    for candidate in (capture_receipt_path, terminal_receipt_path, output_path):
        try:
            candidate.relative_to(RECOVERY.resolve())
        except ValueError:
            raise BindError("Binding inputs and output must remain under recovery/backup-wait-resume") from None
    require(not output_path.exists(), "Terminal binding output already exists; preserve it and choose another path")
    capture = load(capture_receipt_path)
    running_path = ROOT / str(capture.get("runningProcessReceiptPath", ""))
    require(running_path.is_file() and not running_path.is_symlink(),
            "Captured running process receipt is absent or a symlink")
    running = load(running_path)
    terminal = load(terminal_receipt_path)
    running_hash, terminal_hash = sha256(running_path), sha256(terminal_receipt_path)
    capture_hash = sha256(capture_receipt_path)
    require(capture.get("version") == 1 and capture.get("stack") == "100gib"
            and capture.get("processStatusAtCapture") == "running"
            and capture.get("runningProcessReceiptSha256") == running_hash,
            "TMP capture receipt does not match its preserved running process receipt")
    require(running.get("stack") == "100gib" and running.get("phase") == "prepare"
            and running.get("status") == "running"
            and running.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and running.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
            "Captured process snapshot is not the frozen running 100 GiB prepare attempt")
    require(terminal.get("stack") == running.get("stack") == "100gib"
            and terminal.get("phase") == running.get("phase") == "prepare"
            and terminal.get("status") in ("failed", "stopped")
            and terminal.get("supervisorPid") == running.get("supervisorPid")
            and terminal.get("childPid", terminal.get("childProcessId"))
                == running.get("childPid", running.get("childProcessId"))
            and terminal.get("startedAtUtc") == running.get("startedAtUtc")
            and terminal.get("command") == running.get("command")
            and terminal.get("scriptSha256") == running.get("scriptSha256")
            and terminal.get("sourceStageHelperSha256") == running.get("sourceStageHelperSha256"),
            "Terminal process receipt does not continue the exact captured run identity")
    require(terminal.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and terminal.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
            "Terminal process receipt lost the frozen script/helper pins")
    child_pid = int(terminal.get("childPid", terminal.get("childProcessId")))
    try:
        os.kill(child_pid, 0)
    except ProcessLookupError:
        pass
    except PermissionError:
        raise BindError("Original preparation child PID is still present") from None
    else:
        raise BindError("Original preparation child PID is still present")

    captured_at = parse_time(capture.get("capturedAtUtc"), "TMP capture")
    started_at = parse_time(running.get("startedAtUtc"), "Original process start")
    observed_at = dt.datetime.now(dt.timezone.utc)
    finished = terminal.get("finishedAtUtc") or terminal.get("endedAtUtc")
    if finished:
        require(parse_time(finished, "Original process finish") >= captured_at,
                "Terminal process finish predates the active TMP capture")
    require(captured_at >= started_at and observed_at >= captured_at,
            "Capture/terminal observation timestamps are out of order")

    output_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    obj = {
        "version": 1, "phase": "terminal-bound",
        "captureReceiptPath": repo_path(capture_receipt_path), "captureReceiptSha256": capture_hash,
        "runningProcessReceiptPath": repo_path(running_path), "runningProcessReceiptSha256": running_hash,
        "terminalProcessReceiptPath": repo_path(terminal_receipt_path),
        "terminalProcessReceiptSha256": terminal_hash,
        "terminalObservedAtUtc": observed_at.isoformat(),
        "bindingHelperSha256": sha256(Path(__file__)),
        "bindingHelperPath": str(Path(__file__).resolve().relative_to(ROOT)),
        "runIdentity": {"stack": "100gib", "phase": "prepare",
            "startedAtUtc": running.get("startedAtUtc"),
            "supervisorPid": running.get("supervisorPid"),
            "childPid": running.get("childPid", running.get("childProcessId")),
            "command": running.get("command"),
            "scriptSha256": running.get("scriptSha256"),
            "sourceStageHelperSha256": running.get("sourceStageHelperSha256")},
        "captureAtUtc": capture.get("capturedAtUtc"),
        "terminalStatus": terminal.get("status"),
        "evidenceScope": "recorded local receipt chain; corroborating provenance, not a signed execution attestation",
    }
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(obj, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(output_path, 0o600)
    return {"path": output_path, "sha256": sha256(output_path)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-receipt", type=Path, required=True)
    parser.add_argument("--terminal-process-receipt", type=Path, required=True,
                        help="separately archived failed/stopped receipt for the same original run")
    parser.add_argument("--output", type=Path, required=True,
                        help="new terminal-process-binding.json under recovery/backup-wait-resume")
    args = parser.parse_args(argv)
    try:
        result = bind(args.capture_receipt, args.terminal_process_receipt, args.output)
        print(f"Bound terminal receipt; SHA-256 {result['sha256']}")
        print(f"Binding: {result['path'].relative_to(ROOT)}")
        return 0
    except BindError as exc:
        print(f"BIND BLOCKED: {exc}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, ValueError) as exc:
        print(f"BIND BLOCKED: malformed local evidence ({exc.__class__.__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
