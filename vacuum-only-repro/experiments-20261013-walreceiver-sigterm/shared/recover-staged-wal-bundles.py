#!/usr/bin/env python3
"""Recover complete token-owned WAL bundles left by an interrupted exporter.

This is an explicit, post-measurement recovery command. It reads the live Blob
inventory, accepts only token-owned stage directories with an exact embedded
manifest, verifies the compressed bundle before extraction, and writes normal
per-object export progress only after each original WAL SHA-256 is verified.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location("export_wal_archive", HERE / "export-wal-archive.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load the shared WAL exporter.")
EXPORTER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = EXPORTER
SPEC.loader.exec_module(EXPORTER)
from remote_chunk_download import download_verified_file

STAGE_ID_RE = re.compile(r"^[a-f0-9]{32}$")
HASH_RE = re.compile(r"^[a-f0-9]{64}$")
MAX_BUNDLE_BYTES = EXPORTER.REMOTE_BATCH_OBJECTS * 16 * 1024 * 1024 + 64 * 1024 * 1024

# Inventory only directories whose marker and payload are regular files under
# the exact token/stack prefix. No connection string or Azure operation occurs.
REMOTE_DISCOVERY_SCRIPT = r'''import json, os, re, stat, sys
request = json.load(sys.stdin)
token, stack = request.get("ownerToken", ""), request.get("stack", "")
if not re.fullmatch(r"[a-f0-9]{32}", token) or stack not in ("sigterm", "control"):
    sys.exit(2)
scratch = "/controller/tmp"
if os.path.realpath(scratch) != scratch or not os.path.isdir(scratch):
    sys.exit(2)
prefix = f"wal-export-{token}-{stack}-"
found = []
for name in os.listdir(scratch):
    if not name.startswith(prefix):
        continue
    stage_id = name[len(prefix):]
    if not re.fullmatch(r"[a-f0-9]{32}", stage_id):
        continue
    stage = os.path.join(scratch, name)
    try:
        st = os.lstat(stage)
        if not stat.S_ISDIR(st.st_mode):
            continue
        marker_path = os.path.join(stage, ".owner.json")
        bundle_path = os.path.join(stage, "batch.tar.gz")
        marker_stat, bundle_stat = os.lstat(marker_path), os.lstat(bundle_path)
        if not stat.S_ISREG(marker_stat.st_mode) or not stat.S_ISREG(bundle_stat.st_mode):
            continue
        with open(marker_path, "r", encoding="utf-8") as stream:
            marker = json.load(stream)
        expected = {"version": 1, "ownerToken": token, "stack": stack,
                    "purpose": "post-measurement-wal-export", "stageId": stage_id}
        if marker != expected:
            continue
        found.append(stage_id)
    except (OSError, ValueError, TypeError):
        continue
print(json.dumps({"stageIds": sorted(found)}, separators=(",", ":")))
'''


def validate_stage_ids(response: Any) -> list[str]:
    EXPORTER.require(isinstance(response, dict) and set(response) == {"stageIds"}
                     and isinstance(response["stageIds"], list),
                     "Remote stage discovery response is malformed.")
    values = response["stageIds"]
    EXPORTER.require(all(isinstance(value, str) and STAGE_ID_RE.fullmatch(value) for value in values)
                     and len(values) == len(set(values)),
                     "Remote stage discovery included an invalid or duplicate stage ID.")
    return values


def read_embedded_manifest(bundle_path: Path) -> dict[str, Any]:
    try:
        with tarfile.open(bundle_path, mode="r:gz") as archive:
            candidates = [member for member in archive.getmembers()
                          if member.name == "_wal-export-manifest.json"]
            EXPORTER.require(len(candidates) == 1 and candidates[0].isfile()
                             and not candidates[0].issym() and not candidates[0].islnk()
                             and candidates[0].size <= 2 * 1024 * 1024,
                             "Staged bundle does not contain one bounded regular manifest.")
            stream = archive.extractfile(candidates[0])
            EXPORTER.require(stream is not None, "Staged bundle manifest could not be read.")
            try:
                value = json.loads(stream.read().decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise EXPORTER.Refusal("Staged bundle manifest is invalid.") from exc
    except (OSError, tarfile.TarError) as exc:
        raise EXPORTER.Refusal("Staged compressed WAL bundle is unreadable.") from exc
    EXPORTER.require(isinstance(value, dict), "Staged bundle manifest is not an object.")
    return value


def validate_manifest_rows(manifest: dict[str, Any], ctx: dict[str, Any],
                           current_by_name: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    EXPORTER.require(manifest.get("version") == 1
                     and manifest.get("ownerToken") == ctx["ownerToken"]
                     and manifest.get("stack") == ctx["stack"],
                     "Staged bundle manifest belongs to a different owner or stack.")
    rows = manifest.get("files")
    EXPORTER.require(isinstance(rows, list) and 1 <= len(rows) <= EXPORTER.REMOTE_BATCH_OBJECTS,
                     "Staged bundle manifest object count is outside the allowed batch bound.")
    expected_fields = {"name", "relativeName", "etag", "bytes", "sha256"}
    seen_names: set[str] = set()
    seen_relative: set[str] = set()
    current_items: list[dict[str, Any]] = []
    for row in rows:
        EXPORTER.require(isinstance(row, dict) and set(row) == expected_fields,
                         "Staged bundle manifest row has unexpected fields.")
        name, relative, etag = row.get("name"), row.get("relativeName"), row.get("etag")
        EXPORTER.require(isinstance(name, str) and isinstance(relative, str)
                         and EXPORTER.safe_wal_relative(relative)
                         and isinstance(etag, str) and bool(etag)
                         and type(row.get("bytes")) is int and row["bytes"] > 0
                         and isinstance(row.get("sha256"), str) and HASH_RE.fullmatch(row["sha256"]),
                         "Staged bundle manifest has invalid WAL identity or content metadata.")
        EXPORTER.require(name not in seen_names and relative not in seen_relative,
                         "Staged bundle manifest contains duplicate WAL identities.")
        seen_names.add(name)
        seen_relative.add(relative)
        fresh = current_by_name.get(name)
        EXPORTER.require(fresh is not None
                         and fresh["relativeName"] == relative
                         and fresh["bytes"] == row["bytes"]
                         and EXPORTER.etags_match(fresh["etag"], etag),
                         "Staged bundle manifest differs from the fresh exact ETag inventory.")
        # The extractor compares exact ETag text; retain the manifest spelling
        # after the semantic Azure ETag equality above has been established.
        item = dict(fresh)
        item["etag"] = etag
        current_items.append(item)
    EXPORTER.require(rows == sorted(rows, key=lambda row: row["name"]),
                     "Staged bundle manifest rows are not in canonical name order.")
    return current_items


def append_recovery_receipt(ctx: dict[str, Any], record: dict[str, Any]) -> None:
    path = ctx["stateDir"] / "wal-archive-export-recovered-stages.jsonl"
    EXPORTER.require(not path.is_symlink(), "Refusing a symlinked staged-recovery receipt.")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as stream:
            fd = -1
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if fd >= 0:
            os.close(fd)


def recover(stack: str) -> dict[str, int]:
    initial = EXPORTER.validate_local(stack)
    locks = EXPORTER.acquire_export_locks(initial["stateDir"])
    try:
        ctx = EXPORTER.validate_local(stack)
        EXPORTER.ensure_export_roots(ctx)
        container, connection = EXPORTER.verify_live_owner(ctx)
        del connection
        inventory = EXPORTER.list_inventory(container, ctx)
        previous, _ = EXPORTER.load_previous(ctx)
        EXPORTER.verify_previous_inventory(previous, inventory)
        current_by_name = EXPORTER.inventory_map(inventory)
        EXPORTER.require(any(item["name"] == ctx["smokeObject"] and item["bytes"] == ctx["smokeBytes"]
                             for item in inventory),
                         "Fresh Azure inventory no longer contains the exact independently verified smoke WAL.")
        EXPORTER.check_no_unexpected_local_files(ctx, inventory)
        pod = EXPORTER.resolve_primary_pod(ctx)
        request = {"ownerToken": ctx["ownerToken"], "stack": stack}
        result = EXPORTER.kubectl_exec(ctx, pod,
            ["env", "PYTHONDONTWRITEBYTECODE=1", "python3", "-c", REMOTE_DISCOVERY_SCRIPT],
            input_bytes=json.dumps(request, separators=(",", ":")).encode(), timeout=120)
        if result.returncode != 0:
            raise EXPORTER.Refusal("Token-owned remote WAL stage discovery failed; details suppressed.")
        try:
            stage_ids = validate_stage_ids(json.loads(result.stdout))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise EXPORTER.Refusal("Token-owned remote WAL stage discovery returned invalid JSON.") from exc

        totals = {"stagesFound": len(stage_ids), "stagesRecovered": 0,
                  "objectsRecovered": 0, "objectsAlreadyVerified": 0}
        for stage_id in stage_ids:
            size, digest = EXPORTER.remote_bundle_fingerprint(ctx, pod, stage_id)
            EXPORTER.require(0 < size <= MAX_BUNDLE_BYTES,
                             "Owned staged bundle exceeds the bounded recovery size.")
            temp_dir = Path(tempfile.mkdtemp(prefix="wal-stage-recovery-", dir=ctx["stateDir"]))
            os.chmod(temp_dir, 0o700)
            bundle_path = temp_dir / "batch.tar.gz"
            fingerprints: list[tuple[int, str]] = []
            try:
                def fingerprint() -> tuple[int, str]:
                    value = EXPORTER.remote_bundle_fingerprint(ctx, pod, stage_id)
                    fingerprints.append(value)
                    return value

                def read_chunk(offset: int, expected: int) -> bytes:
                    return EXPORTER.remote_bundle_chunk(ctx, pod, stage_id, offset, expected)

                transfer = download_verified_file(
                    bundle_path, fingerprint, read_chunk,
                    chunk_bytes=EXPORTER.REMOTE_CHUNK_BYTES,
                    attempts_per_chunk=4,
                    retry_backoff_seconds=0.5,
                    max_retry_backoff_seconds=2.0,
                )
                EXPORTER.require(transfer["bytes"] == size and transfer["sha256"] == digest
                                 and len(fingerprints) >= 2 and fingerprints[0] == fingerprints[-1],
                                 "Staged bundle changed during bounded recovery transfer.")
                manifest = read_embedded_manifest(bundle_path)
                items = validate_manifest_rows(manifest, ctx, current_by_name)
                EXPORTER.require(size <= sum(item["bytes"] for item in items) + 16 * 1024 * 1024,
                                 "Staged bundle size is implausible for its exact WAL manifest.")
                response = {"ownerToken": ctx["ownerToken"], "stack": stack,
                            "files": manifest["files"]}
                before_verified = sum(EXPORTER.verified_progress(ctx, item) is not None for item in items)
                records = EXPORTER.extract_verified_remote_bundle(
                    bundle_path, ctx, items, response, ctx["runDir"],
                    save_progress=True, skip_verified=True,
                )
                after_verified = sum(EXPORTER.verified_progress(ctx, item) is not None for item in items)
                EXPORTER.require(len(records) == len(items) and after_verified == len(items),
                                 "Recovered stage did not produce a complete verified local batch.")
            except Exception:
                # A failed extraction deliberately leaves the owner stage in place.
                raise
            finally:
                for child in temp_dir.iterdir():
                    if child.is_file() and not child.is_symlink():
                        child.unlink()
                temp_dir.rmdir()

            cleanup = EXPORTER.remote_json(ctx, pod, {
                "operation": "cleanup", "ownerToken": ctx["ownerToken"],
                "stack": stack, "stageId": stage_id,
            }, timeout=120)
            EXPORTER.require(cleanup.get("status") in ("removed", "absent")
                             and cleanup.get("stageId") == stage_id,
                             "Verified token-owned stage cleanup did not return its exact receipt.")
            already = before_verified
            recovered = len(items) - already
            append_recovery_receipt(ctx, {
                "version": 1,
                "atUtc": datetime.now(timezone.utc).isoformat(),
                "stack": stack,
                "stageId": stage_id,
                "bundleBytes": size,
                "bundleSha256": digest,
                "objectCount": len(items),
                "objectsRecovered": recovered,
                "objectsAlreadyVerified": already,
                "chunkBytes": EXPORTER.REMOTE_CHUNK_BYTES,
                "chunkRetries": transfer["retries"],
                "remoteFingerprintStable": True,
                "remoteOwnerCleanup": cleanup["status"],
                "secretsPersisted": False,
            })
            totals["stagesRecovered"] += 1
            totals["objectsRecovered"] += recovered
            totals["objectsAlreadyVerified"] += already
        return totals
    finally:
        for fd in reversed(locks):
            fd.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", choices=EXPORTER.STACK_NAMES, required=True)
    parser.add_argument("--apply", action="store_true",
                        help="recover exact token-owned bundles into ignored local WAL export state")
    args = parser.parse_args()
    if not args.apply:
        print(json.dumps({"mode": "plan-only", "stack": args.stack,
                          "azureMutations": 0, "sourceSnapshotMutations": 0,
                          "remoteObjectsStaged": 0, "secretsPrintedOrPersisted": False}, sort_keys=True))
        return 0
    summary = recover(args.stack)
    print(json.dumps({"mode": "verified-staged-recovery", "stack": args.stack,
                      **summary, "secretsPrintedOrPersisted": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except EXPORTER.Refusal as exc:
        print(f"refused: {exc}", file=sys.stderr)
        raise SystemExit(2)
