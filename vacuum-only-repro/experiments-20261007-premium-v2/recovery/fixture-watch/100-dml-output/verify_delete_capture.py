#!/usr/bin/env python3
"""Verify and attach the preserved original 100 GiB DELETE transcript.

This module is local-only: it reads pinned evidence files and makes no Azure,
Kubernetes, database, or stack-state calls.  The capture constants deliberately
bind the verifier to one immutable evidence pair.
"""

from __future__ import annotations

import copy
import datetime as _datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


class EvidenceError(ValueError):
    """The local capture is missing, changed, malformed, or inconsistent."""


_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[2]
_INITIAL_DIR = "initial-20261007T051136Z"
_FINAL_DIR = "final-20261007T051507Z"

_INITIAL_MANIFEST = {
    "sha256": "143259e3b9900e3ce6e2a19b5d19700a44098997a9b124f78d9412a05c7567d0",
    "bytes": 2144,
}
_FINAL_MANIFEST = {
    "sha256": "5aa531c946803b3936b068fc2a48fac5f83a3828291487997b6c27e9a24448b4",
    "bytes": 974,
}

_INITIAL_FILES = {
    "q.sql": (140, "01d89421727f9a76a3de0e6cfa4ea4ef3d1d6afa1651e7f5f2528a5e056f3584"),
    "out": (13, "073bf29e5e659431aa78d08858d230cc8626d6dd05a13fe935d22d29d7931c2a"),
    "prepare-process.json": (626, "68472626da0721b22247773d4782d1940e5b992b1d3c5af3ba422249aeb4205e"),
    "02-prepare.sh": (45550, "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"),
    "stage-allowlisted-source.sh": (20120, "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"),
    "seed-path": (77, "b0fdb21b8b59c8965adaf224a243d816093d95ef2fd968035eecc5de33ce3391"),
}
_FINAL_FILES = {
    "out": (40, "a20fccfbfdeaa7de3929eaebb554b42aec408acdf7da3965354d9717372849cb"),
    "status": (2, "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa"),
}

_QUERY = (
    b"CREATE INDEX vacuum_target_stage_id_idx ON public.vacuum_target_stage(id); "
    b"DELETE FROM public.vacuum_target_stage WHERE id%2=1; CHECKPOINT;\n"
)
_INITIAL_OUTPUT = b"CREATE INDEX\n"
_FINAL_OUTPUT = b"CREATE INDEX\nDELETE 32768000\nCHECKPOINT\n"
_FINAL_STATUS = b"0\n"
_PROCESS_RECEIPT_SHA256 = _INITIAL_FILES["prepare-process.json"][1]
_SCRIPT_SHA256 = _INITIAL_FILES["02-prepare.sh"][1]
_HELPER_SHA256 = _INITIAL_FILES["stage-allowlisted-source.sh"][1]
_POD_UID = "405550c1-241d-4bb2-81ea-30ec649ea373"
_SEED_PATH = "/controller/tmp/seed-f1b90aec9e24f6a2008d6991738742cf-20261006T205332Z-24491"
_GENERIC_LIMITATION = (
    "Original INSERT/DELETE psql_long output was command-substituted and then removed; "
    "raw DML transcripts are unavailable."
)
_PRECISE_LIMITATION = (
    "Original INSERT psql_long output was command-substituted and then removed, so its "
    "raw transcript is unavailable. Original 100-GiB DELETE output and CHECKPOINT were "
    "captured with psql exit status 0; DELETE removed 32,768,000 rows."
)
_CONTINUATION_LIMITATION = (
    "The frozen original script and captured pre-Backup TMP indicate the attempt reached the request boundary "
    "after its fixture equality guards; raw INSERT/DELETE output is unavailable, and this is corroborating "
    "control-flow evidence."
)
_PRECISE_CONTINUATION_LIMITATION = (
    "The frozen original script and captured pre-Backup TMP indicate the attempt reached the request boundary "
    "after its fixture equality guards; raw INSERT output remains unavailable, while the original 100-GiB "
    "DELETE output and CHECKPOINT were captured with psql exit status 0 (DELETE 32768000); this is corroborating "
    "control-flow evidence. The live pg_stat_all_tables estimates are required to equal ceil(targetBytes * "
    "sourceLiveEstimate / sourceHeapBytes)."
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise EvidenceError(f"cannot read evidence file {path.name}: {exc}") from exc


def _json_no_duplicate_keys(data: bytes, name: str) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        for key, value in pairs:
            if key in obj:
                raise EvidenceError(f"duplicate key {key!r} in {name}")
            obj[key] = value
        return obj

    try:
        value = json.loads(data, object_pairs_hook=reject_duplicates)
    except EvidenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"invalid JSON in {name}: {exc}") from exc
    if not isinstance(value, dict):
        raise EvidenceError(f"{name} must contain a JSON object")
    return value


def _check_private(path: Path, *, directory: bool = False) -> None:
    try:
        mode = path.stat().st_mode & 0o777
    except OSError as exc:
        raise EvidenceError(f"cannot stat evidence path {path.name}: {exc}") from exc
    if directory and not path.is_dir():
        raise EvidenceError(f"expected evidence directory: {path.name}")
    if not directory and not path.is_file():
        raise EvidenceError(f"expected evidence file: {path.name}")
    if mode & 0o077:
        raise EvidenceError(f"evidence path is accessible by group/other: {path.name}")


def _verify_file(directory: Path, name: str, expected: tuple[int, str]) -> bytes:
    path = directory / name
    _check_private(path)
    data = _read(path)
    expected_bytes, expected_hash = expected
    if len(data) != expected_bytes or _sha256(data) != expected_hash:
        raise EvidenceError(f"captured {name} size/hash mismatch")
    return data


def _time(value: Any, field: str) -> _datetime.datetime:
    if not isinstance(value, str):
        raise EvidenceError(f"{field} is missing or not a string")
    try:
        parsed = _datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceError(f"{field} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise EvidenceError(f"{field} has no timezone")
    return parsed.astimezone(_datetime.timezone.utc)


def verify_original_100gib_delete(evidence_root: str | Path | None = None) -> dict[str, Any]:
    """Return a structured proof after validating the immutable local capture pair.

    ``evidence_root`` is an offline-test seam. Production callers should omit it,
    which selects the directory containing this module.
    """
    base = Path(evidence_root).resolve() if evidence_root is not None else _HERE
    initial = base / _INITIAL_DIR
    final = base / _FINAL_DIR
    _check_private(base, directory=True)
    _check_private(initial, directory=True)
    _check_private(final, directory=True)

    initial_manifest_bytes = _read(initial / "capture-manifest.json")
    final_manifest_bytes = _read(final / "capture-manifest.json")
    for path, data, expected in (
        (initial / "capture-manifest.json", initial_manifest_bytes, _INITIAL_MANIFEST),
        (final / "capture-manifest.json", final_manifest_bytes, _FINAL_MANIFEST),
    ):
        _check_private(path)
        if len(data) != expected["bytes"] or _sha256(data) != expected["sha256"]:
            raise EvidenceError(f"{path.parent.name} manifest size/hash mismatch")

    im = _json_no_duplicate_keys(initial_manifest_bytes, "initial capture manifest")
    fm = _json_no_duplicate_keys(final_manifest_bytes, "final capture manifest")

    if set(p.name for p in initial.iterdir()) != set(_INITIAL_FILES) | {"capture-manifest.json"}:
        raise EvidenceError("initial capture contains missing or unexpected files")
    if set(p.name for p in final.iterdir()) != set(_FINAL_FILES) | {"capture-manifest.json"}:
        raise EvidenceError("final capture contains missing or unexpected files")

    initial_files: dict[str, bytes] = {}
    for name, expected in _INITIAL_FILES.items():
        data = _verify_file(initial, name, expected)
        initial_files[name] = data
    final_files = {name: _verify_file(final, name, expected)
                   for name, expected in _FINAL_FILES.items()}

    im_remote = im.get("remoteFiles")
    im_local = im.get("localProvenanceFiles")
    if not isinstance(im_remote, dict) or not isinstance(im_local, dict):
        raise EvidenceError("initial manifest file inventories are malformed")
    if set(im_remote) != {"q.sql", "out"} or set(im_local) != {
        "prepare-process.json", "02-prepare.sh", "stage-allowlisted-source.sh", "seed-path"
    }:
        raise EvidenceError("initial manifest file inventory has unexpected members")
    for name in ("q.sql", "out"):
        row = im_remote[name]
        if row != {"path": name, "sha256": _sha256(initial_files[name]), "bytes": len(initial_files[name])}:
            raise EvidenceError(f"initial manifest does not bind remote {name}")
    for name in im_local:
        row = im_local[name]
        if row != {"path": name, "sha256": _sha256(initial_files[name]), "bytes": len(initial_files[name])}:
            raise EvidenceError(f"initial manifest does not bind local provenance file {name}")

    process = _json_no_duplicate_keys(initial_files["prepare-process.json"], "running process receipt")
    expected_command = ["bash", str(_ROOT / "100gib/02-prepare.sh")]
    if (process.get("stack") != "100gib" or process.get("phase") != "prepare"
            or process.get("status") != "running" or process.get("supervisorPid") != 126517
            or process.get("childPid") != 126518 or process.get("command") != expected_command):
        raise EvidenceError("captured original running process receipt identity mismatch")
    if process.get("scriptSha256") != {"02-prepare.sh": _SCRIPT_SHA256}:
        raise EvidenceError("running receipt does not bind the frozen 02 script")
    if process.get("sourceStageHelperSha256") != _HELPER_SHA256:
        raise EvidenceError("running receipt does not bind the frozen source helper")

    if initial_files["q.sql"] != _QUERY or initial_files["out"] != _INITIAL_OUTPUT:
        raise EvidenceError("initial remote SQL/output does not match the frozen DML boundary")
    if initial_files["seed-path"].decode("utf-8") != _SEED_PATH + "\n":
        raise EvidenceError("captured seed-path bytes do not match the pinned remote path")

    initial_at = _time(im.get("capturedAtUtc"), "initial capturedAtUtc")
    final_at = _time(fm.get("capturedAtUtc"), "final capturedAtUtc")
    if not final_at > initial_at:
        raise EvidenceError("final capture does not postdate the initial running capture")
    if (im.get("version") != 1 or im.get("captureKind") != "initial-read-only-remote-psql-long-capture"
            or im.get("stack") != "100gib" or im.get("namespace") != "repro"
            or im.get("clusterName") != "aks-vacuum-only-f1b90aec9e24"
            or im.get("podName") != "hdd-pg-1" or im.get("podUid") != _POD_UID
            or im.get("remoteSeedPath") != _SEED_PATH
            or im.get("processReceiptStatus") != "running"
            or im.get("supervisorPid") != 126517 or im.get("childPid") != 126518
            or im.get("originalPrepareScriptSha256") != _SCRIPT_SHA256
            or im.get("sourceHelperSha256") != _HELPER_SHA256
            or im.get("processReceiptSha256") != _PROCESS_RECEIPT_SHA256
            or im.get("seedPathFileSha256") != _INITIAL_FILES["seed-path"][1]
            or im.get("expectedSqlExactIgnoringOneTerminalNewline") is not True
            or im.get("remoteStatusPresent") is not False or im.get("remoteStatusValue") is not None):
        raise EvidenceError("initial manifest identity/state binding mismatch")
    if (fm.get("version") != 1 or fm.get("captureKind") != "final-read-only-remote-psql-long-capture"
            or fm.get("stack") != "100gib" or fm.get("namespace") != "repro"
            or fm.get("podName") != "hdd-pg-1" or fm.get("podUid") != _POD_UID
            or fm.get("remoteSeedPath") != _SEED_PATH
            or fm.get("processReceiptSha256") != _PROCESS_RECEIPT_SHA256
            or fm.get("initialCaptureManifestSha256") != _INITIAL_MANIFEST["sha256"]
            or fm.get("initialQSqlSha256") != _INITIAL_FILES["q.sql"][1]
            or fm.get("statusValue") != "0" or fm.get("statusSha256") != _FINAL_FILES["status"][1]
            or fm.get("outputSha256") != _FINAL_FILES["out"][1] or fm.get("outputBytes") != 40
            or fm.get("expectedFinalDeleteLine") != "DELETE 32768000"
            or fm.get("containsExpectedDeleteLine") is not True
            or fm.get("containsCheckpointLine") is not True):
        raise EvidenceError("final manifest identity, linkage, or completion binding mismatch")
    if final_files["out"] != _FINAL_OUTPUT or final_files["status"] != _FINAL_STATUS:
        raise EvidenceError("final raw output/status do not match successful DELETE and CHECKPOINT")

    return {
        "version": 1,
        "verification": "complete",
        "stack": "100gib",
        "namespace": "repro",
        "pod": {"name": "hdd-pg-1", "uid": _POD_UID},
        "remoteSeedPath": _SEED_PATH,
        "originalProcess": {
            "statusAtInitialCapture": "running",
            "supervisorPid": 126517,
            "childPid": 126518,
            "receiptSha256": _PROCESS_RECEIPT_SHA256,
            "prepareScriptSha256": _SCRIPT_SHA256,
            "sourceHelperSha256": _HELPER_SHA256,
        },
        "initialCapture": {
            "capturedAtUtc": im["capturedAtUtc"],
            "directory": _INITIAL_DIR,
            "manifestSha256": _INITIAL_MANIFEST["sha256"],
            "querySha256": _INITIAL_FILES["q.sql"][1],
            "initialOutputSha256": _INITIAL_FILES["out"][1],
        },
        "finalCapture": {
            "capturedAtUtc": fm["capturedAtUtc"],
            "directory": _FINAL_DIR,
            "manifestSha256": _FINAL_MANIFEST["sha256"],
            "outputSha256": _FINAL_FILES["out"][1],
            "statusSha256": _FINAL_FILES["status"][1],
            "psqlExitStatus": 0,
        },
        "delete": {
            "captured": True,
            "statement": "DELETE FROM public.vacuum_target_stage WHERE id%2=1",
            "rows": 32768000,
            "outputLine": "DELETE 32768000",
            "checkpointCaptured": True,
            "outputLines": ["CREATE INDEX", "DELETE 32768000", "CHECKPOINT"],
        },
        "insert": {
            "captured": False,
            "reason": "The original INSERT psql_long output is unavailable; no raw INSERT transcript was preserved.",
        },
        "evidenceScope": "Local hash-checked capture of the original psql_long output and exit status; no cloud/DB calls are made by this verifier.",
    }


def supplement_audit(audit: Mapping[str, Any],
                    evidence_root: str | Path | None = None) -> dict[str, Any]:
    """Return a copy of a 100 GiB audit with the verified DELETE proof attached.

    Existing receipts and factual limitations are retained. It recognizes the
    frozen finalizer's base limitation or the continuation adapter's exact
    limitation, makes only that raw INSERT/DELETE claim precise, and preserves
    the surrounding estimate/control-flow caveats.
    """
    proof = verify_original_100gib_delete(evidence_root)
    if not isinstance(audit, Mapping) or audit.get("stack") != "100gib":
        raise EvidenceError("audit must be the 100gib audit object")
    out = copy.deepcopy(dict(audit))
    limitations = out.get("limitations")
    if not isinstance(limitations, list) or not all(isinstance(v, str) for v in limitations):
        raise EvidenceError("audit limitations must be a list of strings")
    matches = [
        (i, phrase, replacement)
        for i, value in enumerate(limitations)
        for phrase, replacement in (
            (_GENERIC_LIMITATION, _PRECISE_LIMITATION),
            (_CONTINUATION_LIMITATION, _PRECISE_CONTINUATION_LIMITATION),
        )
        if phrase in value
    ]
    if len(matches) != 1:
        raise EvidenceError("expected exactly one recognized raw INSERT/DELETE limitation to refine")
    i, phrase, replacement = matches[0]
    limitations[i] = limitations[i].replace(phrase, replacement, 1)

    checks = out.get("checks")
    if not isinstance(checks, dict):
        raise EvidenceError("audit checks must be an object")
    postgres = checks.get("postgres")
    if not isinstance(postgres, dict):
        raise EvidenceError("audit checks.postgres must be an object")
    postgres["originalInsertOutputCaptured"] = False
    postgres["originalDeleteCheckpointOutputCaptured"] = True
    postgres["originalDeleteRowsFromCapturedOutput"] = 32768000
    checks["original100GiBFixtureDmlCapture"] = proof
    return out
