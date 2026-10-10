#!/usr/bin/env python3
"""Read-only final audit for a Standard-stage 50/100 GiB preparation.

The only writes are private receipts below recovery/numeric-hydration-finalizer.
This deliberately does not update .state, Kubernetes, Azure, or PostgreSQL.
"""
from __future__ import annotations

import argparse
import datetime as dt
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
IMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
SYSIMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
PREPARE_SHA = "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"
TEST_SCRIPT_SHA = "1b6b25fdd0c49f1ec9c1b116f28c72ddb5d63202c858ffa34ec12815756e004a"
STAGE_HELPER_SHA = "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"
GIB = 1024**3
NS, CLUSTER, SC = "repro", "hdd-pg", "azure-premiumv2"
CONTROL_CAPTURE_FILES = (
    "pods-final.json", "pvcs-final.json", "hdd-pg-1-config.json", "hdd-pg-2-config.json",
    "hdd-pg-1-pv.json", "hdd-pg-1-disk.json", "backup.yaml", "cluster.yaml", "seed-path",
)


class AuditError(RuntimeError):
    pass


class HydrationPending(AuditError):
    """Only a valid succeeded Copy/CopyStart resource below 100% is retryable."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise AuditError(message)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditError(f"Cannot read valid JSON evidence at {path}: {exc.__class__.__name__}") from None


def load_json_document_sequence(path: Path) -> list[dict[str, Any]]:
    """Read whitespace-separated JSON documents, as emitted by jq -n."""
    try:
        text = path.read_text()
    except OSError as exc:
        raise AuditError(f"Cannot read JSON document sequence at {path}: {exc.__class__.__name__}") from None

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError("duplicate JSON object key")
            obj[key] = value
        return obj

    decoder = json.JSONDecoder(object_pairs_hook=unique_object)
    documents: list[dict[str, Any]] = []
    offset = 0
    while True:
        while offset < len(text) and text[offset].isspace():
            offset += 1
        if offset == len(text):
            break
        start = offset
        try:
            document, offset = decoder.raw_decode(text, offset)
        except (json.JSONDecodeError, ValueError):
            raise AuditError(f"Malformed JSON document sequence at {path}") from None
        require(isinstance(document, dict), "JSON document sequence contains a non-object")
        require(offset > start, "JSON document sequence contains an empty document")
        if offset < len(text) and not text[offset].isspace():
            raise AuditError("JSON documents in a sequence must be separated by whitespace")
        documents.append(document)
    require(bool(documents), f"JSON document sequence is empty: {path}")
    return documents


def load_output_snapshot_records(path: Path) -> dict[str, dict[str, Any]]:
    """Validate the exact paired Backup snapshot journal and index by name."""
    documents = load_json_document_sequence(path)
    require(len(documents) == 2, "output-snapshots journal must contain exactly two JSON objects")
    records: dict[str, dict[str, Any]] = {}
    for document in documents:
        name, resource_id = document.get("volumeSnapshot"), document.get("id")
        require(isinstance(name, str) and name and isinstance(resource_id, str) and resource_id,
                "output-snapshots journal entry lacks its VolumeSnapshot name or ARM ID")
        require(name not in records, "output-snapshots journal contains a duplicate VolumeSnapshot name")
        records[name] = document
    return records


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(chunk)
    except OSError as exc:
        raise AuditError(f"Cannot hash required file {path}: {exc.__class__.__name__}") from None
    return h.hexdigest()


def canonical_id(value: Any) -> str:
    return str(value or "").rstrip("/").lower()


def completion_percent(value: Any) -> decimal.Decimal | None:
    """Normalize ARM numeric JSON scalars without confusing 100 and 100.0."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise AuditError("completionPercent must be a number or null")
    try:
        out = decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        raise AuditError("completionPercent is not numeric") from None
    require(out.is_finite() and decimal.Decimal(0) <= out <= decimal.Decimal(100),
            "completionPercent is outside 0..100")
    return out


def observe_hydration(disk: dict[str, Any], pv: dict[str, Any], disk_id: str) -> tuple[dict[str, Any], bool]:
    """Validate identity/status/sector and report a safely retryable copy state."""
    require(canonical_id(disk.get("id")) == canonical_id(disk_id), "disk ID differs from PV handle")
    require(disk.get("provisioningState") == "Succeeded", "disk provisioning is not Succeeded")
    option = (disk.get("creationData") or {}).get("createOption")
    progress = completion_percent(disk.get("completionPercent"))
    if option not in ("Empty", "Copy", "CopyStart"):
        raise AuditError(f"unsupported PremiumV2 createOption {option!r}")
    attrs = ((pv.get("spec") or {}).get("csi") or {}).get("volumeAttributes") or {}
    sector = disk.get("logicalSectorSize")
    if sector is None:
        sector = (disk.get("creationData") or {}).get("logicalSectorSize")
    if sector is None:
        sector = attrs.get("LogicalSectorSize") or attrs.get("logicalSectorSize")
    require(str(sector) == "512", "disk sector size is not 512 in ARM or validated PV context")
    pending = False
    if option == "Empty":
        require(progress is None or progress == 100, "Empty disk has incomplete hydration")
    else:
        require(progress is not None, "Copy/CopyStart disk has no numeric completionPercent")
        pending = progress < 100
    return ({
        "id": disk["id"], "uniqueId": disk.get("uniqueId"),
        "sku": (disk.get("sku") or {}).get("name"),
        "sizeGiB": disk.get("diskSizeGB", disk.get("diskSizeGb")),
        "iops": disk.get("diskIOPSReadWrite"), "throughputMBps": disk.get("diskMBpsReadWrite"),
        "createOption": option,
        "completionPercent": None if progress is None else float(progress),
        "sector": str(sector), "provisioningState": disk.get("provisioningState"),
        "source": "ARM", "sectorEvidence": "ARM" if disk.get("logicalSectorSize") is not None or (disk.get("creationData") or {}).get("logicalSectorSize") is not None else "PV CSI LogicalSectorSize",
    }, pending)


def validate_hydration(disk: dict[str, Any], pv: dict[str, Any], disk_id: str) -> dict[str, Any]:
    """Validate a PremiumV2 disk or signal only a retryable valid partial copy."""
    details, pending = observe_hydration(disk, pv, disk_id)
    if pending:
        raise HydrationPending(f"validated Copy/CopyStart disk is Succeeded but below 100%: {details['id']}")
    return details


def validate_tuple_estimates(live: int, dead: int, min_id: int, max_id: int,
                             target_gib: int, source_heap: int,
                             source_live_estimate: int) -> int:
    """Bind tuple estimates to the source-prefix formula and indexed extrema."""
    target = target_gib * GIB
    require(live > 0 and dead > 0, "pg_stat_all_tables lacks live/dead tuple estimates")
    require(min_id > 0 and max_id > min_id and min_id % 2 == 0 and max_id % 2 == 0,
            "indexed fixture ID extrema are not positive even IDs")
    # The frozen 02 script's COPY_ROWS formula scales its 50/100-GiB target by
    # the pinned source live-tuple estimate and source heap bytes. Raw INSERT /
    # DELETE output was not persisted, so this expected value is only a bound
    # against live statistics, never presented as a recovered transcript.
    expected = (target * source_live_estimate + source_heap - 1) // source_heap
    require(expected > 0 and live == expected and dead == expected,
            "live pg_stat_all_tables estimates differ from the exact pinned source-prefix estimate")
    return expected


def validate_fixture_sql(row: dict[str, Any], target_gib: int, source_heap: int,
                         source_live_estimate: int) -> dict[str, int]:
    """Check the fixture SQL result and preserve tuple counts as estimates."""
    target = target_gib * GIB
    heap = int(row["heapBytes"])
    require(abs(heap - target) <= target // 100, "live fixture heap is outside the original 1% target tolerance")
    require(int(row["totalBytes"]) >= heap, "pg_total_relation_size is smaller than heap size")
    require(row.get("systemIdentifier") and not row.get("inRecovery"), "primary SQL fixture identity/recovery state mismatch")
    require(row.get("payloadBytes") == 1536 and row.get("payloadStoragePlain") is True,
            "fixture payload is not the pinned plain 1536-byte payload")
    require(row.get("tableAutovacuumDisabled") is True and row.get("hasIndex") is True,
            "fixture table/index attributes differ from the VACUUM fixture")
    require(row.get("stageTableAbsent") is True, "temporary fixture stage table still exists")
    live, dead = int(row["liveEstimate"]), int(row["deadEstimate"])
    min_id, max_id = int(row["minId"]), int(row["maxId"])
    expected = validate_tuple_estimates(live, dead, min_id, max_id, target_gib,
                                        source_heap, source_live_estimate)
    return {"heapBytes": heap, "totalBytes": int(row["totalBytes"]),
            "preparedLiveTuplesEstimate": live, "preparedDeadTuplesEstimate": dead,
            "scriptDerivedTupleEstimate": expected, "minId": min_id, "maxId": max_id}


def validate_image_pin(prepare_script: str, test_script_sha: str, config: dict[str, Any]) -> None:
    match = re.search(r"^IMAGE='([^']+)'$", prepare_script, re.MULTILINE)
    require(match is not None and match.group(1) == IMAGE,
            "frozen prepare script IMAGE does not equal the pinned measurement image")
    require(test_script_sha == TEST_SCRIPT_SHA, "immutable 03-test.py hash changed")
    require(config.get("diskSku") == "PremiumV2_LRS" and config.get("diskIops") == 3000
            and config.get("diskMBps") == 125 and config.get("diskSizeGiB") == 512,
            "run configuration no longer pins the four measured disk settings")


def validate_stack_config(config: dict[str, Any], state: dict[str, Any], target: int) -> None:
    require(config.get("targetHeapGiB") == target and config.get("diskSku") == "PremiumV2_LRS"
            and config.get("diskIops") == 3000 and config.get("diskMBps") == 125
            and config.get("diskSizeGiB") == 512, "run-config no longer matches the pinned stack")
    require(config.get("location") == state.get("location")
            and str(config.get("zone")) == str(state.get("availabilityZone"))
            and str(config.get("zone")) == "1",
            "run-config location/zone differs from owner state or assigned zone 1")


def select_gc_safe_seed_event(events: list[dict[str, Any]], pvc_time: str, pod_time: str,
                              expected_job_uid: str) -> dict[str, Any]:
    def parse_utc(value: Any, label: str) -> dt.datetime:
        require(isinstance(value, str) and value, f"{label} timestamp is missing")
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise AuditError(f"{label} timestamp is malformed") from None
        require(parsed.tzinfo is not None and parsed.utcoffset() == dt.timedelta(0),
                f"{label} timestamp is not UTC")
        return parsed

    pvc_dt, pod_dt = parse_utc(pvc_time, "replica PVC"), parse_utc(pod_time, "replica pod")
    matches = [e for e in events
               if (e.get("involvedObject") or {}).get("kind") == "Job"
               and (e.get("involvedObject") or {}).get("name") == f"{CLUSTER}-2-snapshot-recovery"
               and (e.get("involvedObject") or {}).get("uid") == expected_job_uid
               and e.get("reason") == "Completed" and e.get("message") == "Job completed"
               and _event_time(e) is not None
               and pvc_dt <= _event_time(e) < pod_dt]
    require(len(matches) == 1,
            "GC'd seed Job has no unique pinned Completed Event after replica PVCs and before current pod")
    return matches[0]


def _event_time(event: dict[str, Any]) -> dt.datetime | None:
    value = event.get("lastTimestamp") or event.get("eventTime") or event.get("firstTimestamp")
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != dt.timedelta(0):
        return None
    return parsed


def read_pinned_archived_seed_event(path: Path, expected_sha256: str, pvc_time: str,
                                    pod_time: str, expected_job_uid: str) -> tuple[dict[str, Any], str]:
    require(re.fullmatch(r"[a-f0-9]{64}", expected_sha256) is not None,
            "archived seed Event requires a lowercase SHA-256 pin")
    actual = sha256(path)
    require(actual == expected_sha256, "archived seed Event file SHA-256 differs from its pin")
    document = load_json(path)
    require(isinstance(document, dict) and isinstance(document.get("items"), list),
            "archived seed Event capture has an unexpected Kubernetes list shape")
    event = select_gc_safe_seed_event(document["items"], pvc_time, pod_time, expected_job_uid)
    return event, actual


def unchanged_replica_pvc_identities(live_pvcs: dict[str, Any],
                                     captured_pvcs: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    names = {f"{CLUSTER}-2", f"{CLUSTER}-2-wal"}

    def index(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
        rows = [p for p in document.get("items", [])
                if p.get("metadata", {}).get("name") in names]
        by_name = {p.get("metadata", {}).get("name"): p for p in rows}
        require(set(by_name) == names and len(rows) == len(names),
                "seed evidence must identify exactly the replica DATA/WAL PVCs")
        return by_name

    live, captured = index(live_pvcs), index(captured_pvcs)
    identities: list[dict[str, str]] = []
    for name in sorted(names):
        lm, cm = live[name].get("metadata", {}), captured[name].get("metadata", {})
        require(lm.get("uid") and lm.get("creationTimestamp")
                and lm.get("uid") == cm.get("uid")
                and lm.get("creationTimestamp") == cm.get("creationTimestamp"),
                f"live replica PVC {name} UID/creation time differs from captured finalization state")
        identities.append({"name": name, "uid": lm["uid"],
                           "creationTimestamp": lm["creationTimestamp"]})
    return identities, max(x["creationTimestamp"] for x in identities)


def unchanged_replica_pod_identity(live_pods: dict[str, Any],
                                   captured_pods: dict[str, Any]) -> dict[str, str]:
    name = f"{CLUSTER}-2"

    def find(document: dict[str, Any]) -> dict[str, Any]:
        rows = [p for p in document.get("items", []) if p.get("metadata", {}).get("name") == name]
        require(len(rows) == 1, "seed evidence must identify exactly one replica pod")
        return rows[0].get("metadata", {})

    live, captured = find(live_pods), find(captured_pods)
    require(live.get("uid") and live.get("creationTimestamp")
            and live.get("uid") == captured.get("uid")
            and live.get("creationTimestamp") == captured.get("creationTimestamp"),
            "live replica pod UID/creation time differs from captured finalization state")
    return {"name": name, "uid": live["uid"], "creationTimestamp": live["creationTimestamp"]}


def validate_stage_lineage(manifest: dict[str, Any], state: dict[str, Any],
                           record: dict[str, Any], stage: dict[str, Any]) -> None:
    """Bind keeper -> exact Standard staging disk UID -> restore-copy snapshot UID."""
    source_id = canonical_id(manifest.get("sourceSnapshotId"))
    stage_disk_id = canonical_id(state.get("snapshotSourceStagingDiskId"))
    copy_id = canonical_id(state.get("snapshotSourceCopyId"))
    disk = stage.get("stagingDiskMetadata") or {}
    restore = stage.get("restoreSnapshotMetadata") or {}
    require(stage.get("status") == "ready"
            and canonical_id(stage.get("sourceSnapshotId")) == source_id
            and stage.get("sourceSnapshotUniqueId") == manifest.get("uniqueId"),
            "stage receipt source snapshot ID/UID differs from pinned manifest")
    require(canonical_id(record.get("sourceSnapshotId")) == source_id
            and canonical_id(record.get("ownedCopySnapshotId")) == copy_id
            and canonical_id(record.get("stagingDiskId")) == stage_disk_id,
            "source-snapshot record and stack state do not bind the same source/copy/stage")
    require(canonical_id(stage.get("stagingDiskId")) == stage_disk_id
            and stage.get("stagingDiskUniqueId") == disk.get("uniqueId")
            and canonical_id(stage.get("restoreSnapshotId")) == copy_id
            and stage.get("restoreSnapshotUniqueId") == restore.get("uniqueId"),
            "stage/copy unique IDs differ from exact archived stage metadata")
    require(canonical_id((disk.get("creationData") or {}).get("sourceResourceId")) == source_id
            and (disk.get("creationData") or {}).get("sourceUniqueId") == manifest.get("uniqueId"),
            "staging disk source ID/UID differs from the pinned Standard source")
    require(canonical_id((restore.get("creationData") or {}).get("sourceResourceId")) == stage_disk_id
            and (restore.get("creationData") or {}).get("sourceUniqueId") == stage.get("stagingDiskUniqueId"),
            "restore-copy snapshot source ID/UID differs from the exact Standard staging disk")
    require(record.get("stageReceipt") == stage and record.get("manifest") == manifest,
            "source-snapshot.json embeds a changed stage receipt or manifest")


def make_prepare(stack: str, source_id: str, source_kind: str, system_id: str,
                 source_heap: int, source_total: int, target_gib: int, fixture: dict[str, int],
                 replay_lsn: str, copy_id: str, short: str, zone: str, snapshot_rg: str,
                 backup: str, data: str, wal: str) -> dict[str, Any]:
    return {
        "version": 1, "status": "complete", "sourceSnapshotId": source_id,
        "sourceKind": source_kind, "systemIdentifier": str(system_id),
        "sourceSnapshotHeapBytes": source_heap, "sourceSnapshotTableBytes": source_total,
        "fixtureTargetHeapGiB": target_gib, "fixtureTargetHeapBytes": target_gib * GIB,
        "heapBytes": fixture["heapBytes"], "tableBytes": fixture["totalBytes"],
        "estimatedVacuumMinutes": round(target_gib * 260.583 / 110, 1),
        "preparedLiveTuplesEstimate": fixture["preparedLiveTuplesEstimate"],
        "preparedDeadTuplesEstimate": fixture["preparedDeadTuplesEstimate"],
        "replicaCaughtUpToLsn": replay_lsn, "restoreCopySnapshotId": copy_id,
        "cluster": "repro/hdd-pg", "instances": 2, "availabilityZone": zone,
        "retainedSnapshotResourceGroup": snapshot_rg,
        "storage": "four separate 512-GiB PremiumV2_LRS data/WAL disks at 3000 IOPS / 125 MB/s",
        "backup": backup, "replicaSnapshots": {"data": data, "wal": wal},
        "measurementImage": IMAGE, "clientCreated": False,
    }


def run(argv: list[str], env: dict[str, str] | None = None, check: bool = True) -> str:
    try:
        p = subprocess.run(argv, text=True, capture_output=True, env=env, timeout=900)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AuditError(f"Read-only command failed to run ({Path(argv[0]).name}): {exc.__class__.__name__}") from None
    if check and p.returncode:
        # Avoid relaying full stderr or command arguments, which can contain
        # owner metadata and cluster details.
        raise AuditError(f"Read-only command failed ({Path(argv[0]).name}, exit {p.returncode})")
    return p.stdout if check else json.dumps({"stdout": p.stdout, "stderr": p.stderr, "returncode": p.returncode})


class Finalizer:
    def __init__(self, stack: Path, control: Path, output: Path,
                 original_process_receipt: Path | None = None,
                 original_prepare_script: Path | None = None,
                 seed_job_uid: str | None = None,
                 seed_event_evidence: Path | None = None,
                 seed_event_sha256: str | None = None):
        self.stack = stack.resolve()
        self.control = control.resolve()
        self.output = output.resolve()
        self.sd = self.stack / ".state"
        self.original_process_path = (original_process_receipt or self.sd / "prepare-process.json").resolve()
        self.original_script_path = (original_prepare_script or self.stack / "02-prepare.sh").resolve()
        self.seed_job_uid = seed_job_uid
        self.seed_event_evidence_path = seed_event_evidence.resolve() if seed_event_evidence else None
        self.seed_event_sha256 = seed_event_sha256
        require((self.seed_event_evidence_path is None) == (self.seed_event_sha256 is None),
                "archived seed Event evidence path and SHA-256 pin must be supplied together")
        self.state = load_json(self.sd / "state.json")
        self.token = str(self.state.get("ownerToken", ""))
        self.sub = str(self.state.get("subscriptionId", ""))
        self.rg = str(self.state.get("resourceGroup", ""))
        self.node_rg = str(self.state.get("nodeResourceGroup", ""))
        self.aks = str(self.state.get("clusterName", ""))
        self.snapshot_rg = str(self.state.get("snapshotResourceGroup", ""))
        self.loc = str(self.state.get("location", ""))
        self.zone = str(self.state.get("availabilityZone", ""))
        self.short = self.token[:8]
        self.env = os.environ.copy()
        kcfg = self.state.get("kubeconfigPath") or str(self.sd / "kubeconfig")
        if not Path(kcfg).is_absolute():
            kcfg = str(self.stack / kcfg)
        require(Path(kcfg).is_file(), "stack kubeconfig is missing")
        self.env["KUBECONFIG"] = kcfg
        self.evidence: dict[str, Any] = {"version": 1, "mode": "read-only numeric hydration finalizer",
                                         "stack": self.stack.name, "checks": {}, "limitations": []}

    def az(self, *args: str, check: bool = True) -> str:
        argv = ["az", *args, "--subscription", self.sub]
        if "-o" not in args and "--output" not in args:
            argv.extend(("-o", "json"))
        return run(argv, env=self.env, check=check)

    def kubectl(self, *args: str, check: bool = True) -> str:
        return run(["kubectl", *args], env=self.env, check=check)

    def json_cmd(self, argv: list[str]) -> Any:
        return json.loads(run(argv, env=self.env))

    def psql_json(self, pod: str, sql: str) -> dict[str, Any]:
        raw = run(["kubectl", "exec", "-n", NS, "-c", "postgres", pod, "--", "psql", "-XAtq",
                   "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", sql], env=self.env)
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError:
            raise AuditError(f"PostgreSQL returned no valid JSON evidence for {pod}") from None

    def local_provenance(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], int]:
        require(self.stack.name in ("50gib", "100gib"), "only the 50gib and 100gib stacks are supported")
        target = int(self.stack.name[:-3])
        config = load_json(self.stack / "run-config.json")
        validate_stack_config(config, self.state, target)
        # The current stack script may be a small retry wrapper installed after
        # the frozen preparation process was stopped. Validate the externally
        # archived original script, whose hash is independently pinned below.
        validate_image_pin(self.original_script_path.read_text(),
                           sha256(self.stack / "03-test.py"), config)
        require(sha256(ROOT / "recovery/audit/03-test.py") == TEST_SCRIPT_SHA,
                "canonical immutable 03-test.py differs from the pinned audit script")
        require(self.state.get("version") == 1 and self.state.get("status") == "ready", "stack state is not ready")
        require(re.fullmatch(r"[a-f0-9]{32}", self.token) is not None, "stack owner token is invalid")
        require(self.rg == f"rg-vacuum-only-{self.token}" and self.node_rg == f"rg-vacuum-nodes-{self.token}"
                and self.snapshot_rg == f"rg-vacuum-pv2-snap-{self.token}", "resource group names do not bind to exact owner")
        require(canonical_id(self.state.get("snapshotResourceGroupId")) == canonical_id(
            f"/subscriptions/{self.sub}/resourceGroups/{self.snapshot_rg}"), "snapshot resource group ID mismatch")
        require(self.loc and self.zone in ("1", "2", "3"), "region or zone metadata is invalid")
        process = load_json(self.original_process_path)
        require(process.get("stack") == self.stack.name and process.get("phase") == "prepare",
                "archived original prepare process receipt is not for this stack")
        require(process.get("status") in ("failed", "stopped"),
                "archived original preparation must have an explicit failed/stopped terminal receipt")
        old_child = process.get("childPid", process.get("childProcessId"))
        child_absent = old_child is None
        if old_child is not None:
            try:
                os.kill(int(old_child), 0)
            except ProcessLookupError:
                child_absent = True
            except PermissionError:
                child_absent = False
        require(child_absent, "archived original preparation child PID is still present")
        require(process.get("scriptSha256", {}).get("02-prepare.sh") == PREPARE_SHA
                and sha256(self.original_script_path) == PREPARE_SHA,
                "archived frozen prepare script provenance/hash mismatch")
        validate_image_pin(self.original_script_path.read_text(),
                           sha256(self.stack / "03-test.py"), load_json(self.stack / "run-config.json"))
        helper = ROOT / "recovery/source-restore/stage-allowlisted-source.sh"
        require(sha256(helper) == STAGE_HELPER_SHA
                and process.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
                "pinned source-stage helper provenance/hash mismatch")
        manifest_path = self.stack / "source-manifest.json"
        manifest = load_json(manifest_path)
        require(manifest.get("version") == 1 and manifest.get("systemIdentifier"), "pinned source manifest is invalid")
        meta = load_json(self.stack / "snapshot.json")
        require(str(meta.get("systemIdentifier")) == str(manifest.get("systemIdentifier"))
                and int(meta.get("heapBytes", 0)) > 0 and int(meta.get("tableBytes", 0)) >= int(meta.get("heapBytes", 0)),
                "source snapshot fixture metadata differs from the manifest")
        source_record = load_json(self.sd / "source-snapshot.json")
        copy_id = str(self.state.get("snapshotSourceCopyId", ""))
        stage_id = str(self.state.get("snapshotSourceStagingDiskId", ""))
        require(canonical_id(source_record.get("ownedCopySnapshotId")) == canonical_id(copy_id)
                and canonical_id(source_record.get("stagingDiskId")) == canonical_id(stage_id),
                "stack state and archived Standard-stage source lineage differ")
        require(canonical_id(source_record.get("sourceSnapshotId")) == canonical_id(manifest.get("sourceSnapshotId")),
                "source snapshot ID differs from the immutable manifest")
        stage_path_s = str(self.state.get("snapshotSourceStageReceiptPath", ""))
        stage_path = Path(stage_path_s).resolve()
        expected_dir = (self.sd / "source-stage").resolve()
        require(stage_path.parent == expected_dir and stage_path.is_file(), "state does not point to exact owned stage receipt")
        stage = load_json(stage_path)
        stage_hash = sha256(stage_path)
        manifest_hash = sha256(manifest_path)
        require(stage.get("sourceManifest") == manifest,
                "stage receipt embeds a changed source manifest")
        validate_stage_lineage(manifest, self.state, source_record, stage)
        self.evidence["checks"]["frozenPreparation"] = {
            "originalProcessReceiptPath": str(self.original_process_path.relative_to(ROOT)),
            "originalProcessReceiptSha256": sha256(self.original_process_path),
            "originalProcessStatus": process.get("status"), "originalChildPidAbsent": child_absent,
            "originalPrepareScriptPath": str(self.original_script_path.relative_to(ROOT)),
            "originalPrepareScriptSha256": sha256(self.original_script_path),
            "scriptSha256": PREPARE_SHA, "stageHelperSha256": STAGE_HELPER_SHA,
            "sourceManifestPath": str(manifest_path.relative_to(ROOT)), "sourceManifestSha256": manifest_hash,
            "stageReceiptPath": str(stage_path.relative_to(ROOT)), "stageReceiptSha256": stage_hash,
            "stageSnapshotId": copy_id, "stageSnapshotUniqueId": stage.get("restoreSnapshotUniqueId"),
            "stagingDiskId": stage_id, "stagingDiskUniqueId": stage.get("stagingDiskUniqueId"),
            "stageHydrationScope": "Standard-origin stage/copy readiness is recorded; this receipt does not claim Standard copy progress is observable.",
        }
        self.evidence["limitations"].append("Original INSERT/DELETE psql_long output was command-substituted and then removed; raw DML transcripts are unavailable. The frozen script and finalization capture prove control flow passed the original equality guards; live pg_stat_all_tables estimates are required to equal ceil(targetBytes * sourceLiveEstimate / sourceHeapBytes).")
        return manifest, meta, stage, target

    def owner_preflight(self) -> None:
        require(self.az("account", "show", "--query", "id", "-o", "tsv").strip().strip('"') == self.sub,
                "active Azure subscription does not match this stack")
        for rg in (self.rg, self.node_rg, self.snapshot_rg):
            obj = json.loads(self.az("group", "show", "--name", rg))
            require((obj.get("tags") or {}).get("reprotoken") == self.token,
                    f"Azure resource group ownership mismatch: {rg}")
        aks = json.loads(self.az("aks", "show", "--resource-group", self.rg, "--name", self.aks))
        require((aks.get("tags") or {}).get("reprotoken") == self.token
                and aks.get("nodeResourceGroup") == self.node_rg, "AKS owner/node resource group mismatch")
        current = self.kubectl("config", "current-context").strip()
        require(self.aks in current, "Kubernetes context does not identify this AKS cluster")
        namespace = json.loads(self.kubectl("get", "namespace", NS, "-o", "json"))
        require((namespace.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token,
                "namespace owner token mismatch")
        storage = load_json(self.sd / "storage.json")
        require(storage.get("status") == "ready" and storage.get("ownerToken") == self.token
                and storage.get("subscriptionId") == self.sub and storage.get("resourceGroup") == self.rg
                and storage.get("publicAccess") is None, "Blob account local owner metadata mismatch")
        account, aid = str(storage.get("accountName", "")), str(storage.get("accountId", ""))
        expected_aid = f"/subscriptions/{self.sub}/resourceGroups/{self.rg}/providers/Microsoft.Storage/storageAccounts/{account}"
        require(canonical_id(aid) == canonical_id(expected_aid), "Blob account ID escapes the owned stack")
        arm_storage = json.loads(self.az("storage", "account", "show", "--resource-group", self.rg, "--name", account))
        require(canonical_id(arm_storage.get("id")) == canonical_id(aid)
                and (arm_storage.get("tags") or {}).get("reprotoken") == self.token
                and arm_storage.get("provisioningState") == "Succeeded"
                and arm_storage.get("allowBlobPublicAccess") is False
                and arm_storage.get("enableHttpsTrafficOnly") is True,
                "live Blob account ID/privacy/owner mismatch")
        sc = json.loads(self.kubectl("get", "storageclass", SC, "-o", "json"))
        sp = sc.get("parameters") or {}
        require(sc.get("provisioner") == "disk.csi.azure.com" and sp.get("skuName") == "PremiumV2_LRS"
                and sp.get("cachingMode") == "None" and sp.get("DiskIOPSReadWrite") == "3000"
                and sp.get("DiskMBpsReadWrite") == "125" and sp.get("LogicalSectorSize") == "512",
                "PremiumV2 StorageClass settings mismatch")
        for crd in ("volumesnapshots.snapshot.storage.k8s.io", "volumesnapshotcontents.snapshot.storage.k8s.io"):
            self.kubectl("get", "crd", crd)
        secret = json.loads(run(["kubectl", "get", "secret", "azure-barman-credentials", "-n", NS, "-o", "name"], env=self.env, check=False))
        require(secret["returncode"] != 0 and re.search(r"NotFound|not found", secret["stderr"], re.I) is not None,
                "archive Secret is present or its absence cannot be proven")
        self.evidence["checks"]["ownerPreflight"] = {"subscriptionId": self.sub, "resourceGroup": self.rg,
            "nodeResourceGroup": self.node_rg, "snapshotResourceGroup": self.snapshot_rg,
            "clusterName": self.aks, "namespace": NS, "storageClass": SC, "archiveSecretAbsent": True}

    def control_flow_capture(self) -> None:
        hashes = {}
        for name in CONTROL_CAPTURE_FILES:
            path = self.control / name
            require(path.is_file(), f"captured original final-audit TMP is missing {name}")
            hashes[name] = sha256(path)
        self.evidence["checks"]["originalFinalizationCapture"] = {
            "path": str(self.control.relative_to(ROOT)), "fileSha256": hashes,
            "meaning": "The unchanged original prepare script reached its final four-disk hydration loop after fixture SQL equality guards, backup, replica seeding, image/default checks, and PVC binding checks.",
        }

    def live_source_snapshot(self, manifest: dict[str, Any], stage: dict[str, Any]) -> str:
        copy_id = str(stage["restoreSnapshotId"])
        src_id = str(manifest["sourceSnapshotId"])
        source = json.loads(self.az("snapshot", "show", "--ids", src_id))
        require(canonical_id(source.get("id")) == canonical_id(src_id)
                and source.get("uniqueId") == manifest.get("uniqueId")
                and source.get("provisioningState") == "Succeeded"
                and source.get("snapshotAccessState") == "Available"
                and (source.get("diskSizeGB", source.get("diskSizeGb")) == 512)
                and source.get("incremental") is True
                and (source.get("sku") or {}).get("name") == "Standard_LRS"
                and (source.get("tags") or {}).get("reprotoken") == manifest.get("ownerToken")
                and (source.get("tags") or {}).get("reproPurpose") == manifest.get("purpose"),
                "live pinned source snapshot identity/readiness mismatch")
        copy = json.loads(self.az("snapshot", "show", "--ids", copy_id))
        restore = stage.get("restoreSnapshotMetadata") or {}
        require(canonical_id(copy.get("id")) == canonical_id(copy_id)
                and copy.get("uniqueId") == stage.get("restoreSnapshotUniqueId")
                and copy.get("provisioningState") == "Succeeded"
                and copy.get("snapshotAccessState") == "Available"
                and canonical_id(copy.get("location")) == canonical_id(self.loc)
                and (copy.get("diskSizeGB", copy.get("diskSizeGb")) == 512)
                and (copy.get("sku") or {}).get("name") == "Standard_LRS"
                and copy.get("incremental") is True
                and (copy.get("creationData") or {}).get("createOption") == "Copy"
                and canonical_id((copy.get("creationData") or {}).get("sourceResourceId")) == canonical_id(stage.get("stagingDiskId"))
                and (copy.get("creationData") or {}).get("sourceUniqueId") == stage.get("stagingDiskUniqueId")
                and (copy.get("incrementalSnapshotFamilyId") or "")
                    != (manifest.get("incrementalSnapshotFamilyId") or "")
                and (copy.get("tags") or {}).get("reprotoken") == self.token
                and (copy.get("tags") or {}).get("reproPurpose") == "premiumv2-keeper-copy"
                and canonical_id((copy.get("tags") or {}).get("sourceSnapshotId")) == canonical_id(src_id)
                and canonical_id((copy.get("tags") or {}).get("sourceDiskId")) == canonical_id(stage.get("stagingDiskId")),
                "live restore-copy snapshot does not match archived Standard-stage source UID lineage")
        require(canonical_id(restore.get("id")) == canonical_id(copy.get("id"))
                and restore.get("uniqueId") == copy.get("uniqueId"), "live restore-copy snapshot UID differs from archived stage receipt")
        self.evidence["checks"]["sourceSnapshots"] = {
            "source": {"id": src_id, "uniqueId": source.get("uniqueId"), "accessState": source.get("snapshotAccessState")},
            "restoreCopy": {"id": copy_id, "uniqueId": copy.get("uniqueId"), "createOption": "Copy",
                            "sourceStagingDiskId": stage.get("stagingDiskId"),
                            "sourceStagingDiskUniqueId": stage.get("stagingDiskUniqueId"),
                            "accessState": copy.get("snapshotAccessState"),
                            "completionPercent": copy.get("completionPercent"),
                            "hydrationNote": "Standard-origin copy progress is recorded as returned; Available/Succeeded is not described as measured full hydration."},
        }
        return copy_id

    def read_k8s(self, kind: str, *args: str) -> Any:
        return json.loads(self.kubectl("get", kind, *args, "-o", "json"))

    def check_cluster_and_seed(self, copy_id: str, source_snapshot_id: str,
                               system_identifier: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str, str, dict[str, Any]]:
        c = self.read_k8s("cluster", CLUSTER, "-n", NS)
        require((c.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and canonical_id(c.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/source-snapshot-id")) == canonical_id(source_snapshot_id)
                and canonical_id(c.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/restore-copy-id")) == canonical_id(copy_id),
                "Cluster owner/source/copy annotations mismatch")
        spec, status = c.get("spec", {}), c.get("status", {})
        require(spec.get("instances") == 2 and status.get("readyInstances") == 2
                and status.get("currentPrimary") == f"{CLUSTER}-1"
                and str(status.get("systemID")) == str(system_identifier),
                "Cluster is not the ready two-instance source system")
        require(spec.get("imageName") == IMAGE and spec.get("backup", {}).get("barmanObjectStore") is None
                and spec.get("storage", {}).get("storageClass") == SC and spec.get("storage", {}).get("size") == "512Gi"
                and spec.get("walStorage", {}).get("storageClass") == SC and spec.get("walStorage", {}).get("size") == "512Gi",
                "Cluster image, archive removal, or separate DATA/WAL storage mismatch")
        sync = spec.get("postgresql", {}).get("synchronous", {})
        require(sync.get("method") == "any" and sync.get("number") == 1 and sync.get("dataDurability") == "preferred",
                "Cluster synchronous replication defaults/settings mismatch")
        pods = self.read_k8s("pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        require(len(pods.get("items", [])) == 2, "Cluster has unexpected pod count")
        by_name = {p.get("metadata", {}).get("name"): p for p in pods["items"]}
        require(set(by_name) == {f"{CLUSTER}-1", f"{CLUSTER}-2"}, "Cluster pod names do not match primary/replica")
        digest = IMAGE.split("@", 1)[1]
        for name, pod in by_name.items():
            containers = pod.get("spec", {}).get("containers", [])
            statuses = pod.get("status", {}).get("containerStatuses", [])
            require(pod.get("status", {}).get("phase") == "Running"
                    and any(x.get("name") == "postgres" and x.get("image") == IMAGE for x in containers)
                    and any(x.get("name") == "postgres" and x.get("ready") is True and str(x.get("imageID", "")).endswith(digest) for x in statuses),
                    f"{name} is not ready on the exact measurement image")
        vsc_name = f"vacuum-snap-{self.short}"
        vsc = self.read_k8s("volumesnapshotclass", vsc_name)
        require((vsc.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and vsc.get("driver") == "disk.csi.azure.com" and vsc.get("deletionPolicy") == "Retain",
                "source VolumeSnapshotClass is not owned/retained")
        vss_name = f"vacuum-data-{self.short}"
        vss = self.read_k8s("volumesnapshot", vss_name, "-n", NS)
        require((vss.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and vss.get("status", {}).get("readyToUse") is True
                and vss.get("spec", {}).get("volumeSnapshotClassName") == vsc_name,
                "primary source VolumeSnapshot is not owned/ready")
        vscnt_name = vss.get("status", {}).get("boundVolumeSnapshotContentName")
        content = self.read_k8s("volumesnapshotcontent", str(vscnt_name))
        require((content.get("spec", {}).get("volumeSnapshotRef") or {}).get("uid") == vss.get("metadata", {}).get("uid")
                and canonical_id((content.get("spec", {}).get("source") or {}).get("snapshotHandle")) == canonical_id(copy_id)
                and canonical_id((content.get("status", {}).get("snapshotHandle"))) == canonical_id(copy_id)
                and content.get("spec", {}).get("deletionPolicy") == "Retain"
                and content.get("status", {}).get("readyToUse") is True,
                "source VolumeSnapshotContent does not retain the exact restore-copy ID and VolumeSnapshot UID")
        # The successful script's final TMP is captured only after its Backup,
        # replica seed, image, default-GUC and initial PVC guards completed.
        self.evidence["checks"]["cluster"] = {"readyInstances": 2, "primary": f"{CLUSTER}-1",
            "image": IMAGE, "synchronous": sync, "backupWriterRemoved": True,
            "sourceVolumeSnapshot": vss_name, "sourceVolumeSnapshotUID": vss["metadata"]["uid"],
            "sourceVolumeSnapshotContent": vscnt_name, "restoreCopySnapshotId": copy_id}
        return c, pods, vss, vss_name, vscnt_name, content

    def check_backup_and_pvcs(self) -> tuple[dict[str, Any], str, str, dict[str, Any], dict[str, Any]]:
        bk_name = f"vacuum-backup-{self.short}"
        backup_list = self.read_k8s("backup", "-n", NS)
        owned = [b for b in backup_list.get("items", []) if b.get("metadata", {}).get("name") == bk_name
                 and (b.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token]
        require(len(owned) == 1 and all(b.get("status", {}).get("phase") == "completed" for b in backup_list.get("items", [])
                                         if (b.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token),
                "expected one completed owned CNPG Backup and no active owned backups")
        backup = owned[0]
        require(backup.get("spec", {}).get("cluster", {}).get("name") == CLUSTER
                and backup.get("spec", {}).get("method") == "volumeSnapshot"
                and sorted(e.get("type") for e in backup.get("status", {}).get("snapshotBackupStatus", {}).get("elements", [])) == ["PG_DATA", "PG_WAL"],
                "paired CNPG snapshot Backup metadata is incomplete")
        els = backup["status"]["snapshotBackupStatus"]["elements"]
        data, wal = next(e["name"] for e in els if e["type"] == "PG_DATA"), next(e["name"] for e in els if e["type"] == "PG_WAL")
        records = load_output_snapshot_records(self.sd / "output-snapshots.jsonl")
        require(set(records) == {data, wal}
                and {r.get("sourcePVC") for r in records.values()} == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
                "output-snapshots journal does not match exact paired Backup outputs")
        pvcs = self.read_k8s("pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        expected = {f"{CLUSTER}-1", f"{CLUSTER}-1-wal", f"{CLUSTER}-2", f"{CLUSTER}-2-wal"}
        pby = {p.get("metadata", {}).get("name"): p for p in pvcs.get("items", [])}
        require(set(pby) == expected, "expected exactly four Cluster DATA/WAL PVCs")
        base_vss = f"vacuum-data-{self.short}"
        mapping = {f"{CLUSTER}-1": base_vss, f"{CLUSTER}-1-wal": None,
                   f"{CLUSTER}-2": data, f"{CLUSTER}-2-wal": wal}
        for name, snapshot in mapping.items():
            pvc = pby[name]
            require(pvc.get("status", {}).get("phase") == "Bound"
                    and pvc.get("spec", {}).get("storageClassName") == SC
                    and pvc.get("spec", {}).get("resources", {}).get("requests", {}).get("storage") == "512Gi",
                    f"{name} is not a bound 512-GiB PremiumV2 PVC")
            ds = pvc.get("spec", {}).get("dataSource")
            if snapshot is None:
                require(ds is None, f"{name} WAL primary unexpectedly has snapshot dataSource")
            else:
                require(ds and ds.get("kind") == "VolumeSnapshot" and ds.get("name") == snapshot,
                        f"{name} is not bound from its exact VolumeSnapshot source")
        jobs = self.read_k8s("jobs", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        require(all((j.get("status", {}).get("active") or 0) == 0 for j in jobs.get("items", [])),
                "a Cluster job is still active")
        seed_jobs = [j for j in jobs.get("items", []) if (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/instanceName") == f"{CLUSTER}-2"
                     and (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/jobRole") == "snapshot-recovery"]
        if seed_jobs:
            require(len(seed_jobs) == 1, "duplicate replica snapshot-recovery Jobs")
            job = seed_jobs[0]
            containers = [x for x in job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                          if x.get("name") == "snapshot-recovery"]
            require(job.get("status", {}).get("succeeded", 0) >= 1 and (job.get("status", {}).get("failed") or 0) == 0
                    and len(containers) == 1 and containers[0].get("image") == SYSIMAGE
                    and "restoresnapshot" in " ".join(containers[0].get("command", [])),
                    "live replica snapshot-recovery Job is not completed on the pinned CNPG system image")
            require(self.seed_job_uid is None or job.get("metadata", {}).get("uid") == self.seed_job_uid,
                    "live replica snapshot-recovery Job UID differs from the captured pin")
            seed_evidence = {"source": "live-job", "uid": job.get("metadata", {}).get("uid"), "completed": True,
                             "systemImage": SYSIMAGE, "restoreSnapshotCommand": True}
        else:
            seed_evidence = self._gc_safe_seed_event(pvcs, self.read_k8s("pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}"))
        self.evidence["checks"]["backupAndReplicaSeed"] = {
            "backup": bk_name, "backupUID": backup.get("metadata", {}).get("uid"), "phase": "completed",
            "dataVolumeSnapshot": data, "walVolumeSnapshot": wal, "seedJob": seed_evidence,
            "pvcNames": sorted(expected),
        }
        return backup, data, wal, pvcs, pby

    def _gc_safe_seed_event(self, pvcs: dict[str, Any], pods: dict[str, Any]) -> dict[str, Any]:
        archived_script = self.original_script_path
        require(archived_script.is_file() and sha256(archived_script) == PREPARE_SHA,
                "GC'd seed Job lacks the exact externally archived frozen preparation script")
        archived = archived_script.read_text()
        require(f"SYSIMAGE='{SYSIMAGE}'" in archived and 'contains("restoresnapshot")' in archived,
                "archived preparation script does not validate the exact system image/restore command")
        captured_pvcs = load_json(self.control / "pvcs-final.json")
        captured_pods = load_json(self.control / "pods-final.json")
        pvc_identities, ptime = unchanged_replica_pvc_identities(pvcs, captured_pvcs)
        replica_identity = unchanged_replica_pod_identity(pods, captured_pods)
        podtime = replica_identity["creationTimestamp"]
        events = self.read_k8s("events", "-n", NS)
        require(self.seed_job_uid is not None and re.fullmatch(r"[a-f0-9-]{36}", self.seed_job_uid) is not None,
                "GC'd seed audit requires the captured exact snapshot-recovery Job UID pin")
        live_candidates = [e for e in events.get("items", [])
                           if (e.get("involvedObject") or {}).get("kind") == "Job"
                           and (e.get("involvedObject") or {}).get("name") == f"{CLUSTER}-2-snapshot-recovery"
                           and (e.get("involvedObject") or {}).get("uid") == self.seed_job_uid
                           and e.get("reason") == "Completed" and e.get("message") == "Job completed"]
        archived_evidence: dict[str, Any] = {}
        if live_candidates:
            e = select_gc_safe_seed_event(live_candidates, ptime, podtime, self.seed_job_uid)
            evidence_source = "live-completed-event-after-job-gc"
        else:
            require(self.seed_event_evidence_path is not None and self.seed_event_sha256 is not None,
                    "exact live Completed seed Event is absent and no hash-pinned archived Event was supplied")
            try:
                self.seed_event_evidence_path.relative_to(ROOT)
            except ValueError:
                raise AuditError("archived seed Event evidence must be inside the repository") from None
            e, archived_hash = read_pinned_archived_seed_event(
                self.seed_event_evidence_path, self.seed_event_sha256, ptime, podtime, self.seed_job_uid)
            evidence_source = "archived-completed-event"
            archived_evidence = {"path": str(self.seed_event_evidence_path.relative_to(ROOT)),
                                 "sha256": archived_hash, "expectedSha256": self.seed_event_sha256,
                                 "originalProcessReceiptSha256": self.evidence["checks"]["frozenPreparation"]["originalProcessReceiptSha256"],
                                 "controlFlowCapturePath": self.evidence["checks"]["originalFinalizationCapture"]["path"],
                                 "controlFlowCaptureFileSha256": self.evidence["checks"]["originalFinalizationCapture"]["fileSha256"]}
        uid = (e.get("involvedObject") or {}).get("uid")
        require(uid, "Completed seed Event has no Job UID")
        return {"source": evidence_source, "uid": uid, "reason": "Completed",
                "message": "Job completed", "eventTimestamp": e.get("lastTimestamp") or e.get("eventTime") or e.get("firstTimestamp"),
                "replicaPVCs": pvc_identities, "replicaPod": replica_identity,
                "archivedEventEvidence": archived_evidence,
                "archivedAttemptScriptSha256": PREPARE_SHA, "archivedScriptPinnedSystemImage": True,
                "archivedScriptCheckedRestoreSnapshotCommand": True}

    def check_backup_snapshot_lineage(self, backup: dict[str, Any], data: str, wal: str,
                                      pby: dict[str, Any]) -> dict[str, Any]:
        vsc_name = f"vacuum-backup-snap-{self.short}"
        vsc = self.read_k8s("volumesnapshotclass", vsc_name)
        require((vsc.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and vsc.get("driver") == "disk.csi.azure.com" and vsc.get("deletionPolicy") == "Retain",
                "paired backup VolumeSnapshotClass is not owner-scoped/retained")
        records = load_output_snapshot_records(self.sd / "output-snapshots.jsonl")
        require(set(records) == {data, wal},
                "output-snapshots journal differs from exact paired Backup names")
        found: dict[str, Any] = {}
        pending_backups: list[str] = []
        pv_disk: dict[str, str] = {}
        for pvc_name in (f"{CLUSTER}-1", f"{CLUSTER}-1-wal"):
            pvname = pby[pvc_name]["spec"]["volumeName"]
            pv = self.read_k8s("pv", pvname)
            handle = pv.get("spec", {}).get("csi", {}).get("volumeHandle")
            require(handle, f"{pvc_name} PV lacks Azure CSI volumeHandle")
            pv_disk[pvc_name] = handle
        for snap_name, pvc_name in ((data, f"{CLUSTER}-1"), (wal, f"{CLUSTER}-1-wal")):
            vs = self.read_k8s("volumesnapshot", snap_name, "-n", NS)
            require((vs.get("metadata", {}).get("labels") or {}).get("cnpg.io/cluster") == CLUSTER
                    and (vs.get("metadata", {}).get("labels") or {}).get("cnpg.io/backupName") == backup.get("metadata", {}).get("name")
                    and vs.get("status", {}).get("readyToUse") is True
                    and vs.get("spec", {}).get("source", {}).get("persistentVolumeClaimName") == pvc_name
                    and vs.get("spec", {}).get("volumeSnapshotClassName") == vsc_name,
                    f"paired VolumeSnapshot {snap_name} metadata/source PVC mismatch")
            content_name = vs.get("status", {}).get("boundVolumeSnapshotContentName")
            content = self.read_k8s("volumesnapshotcontent", str(content_name))
            src = content.get("spec", {}).get("source", {}).get("volumeHandle")
            arm_id = content.get("status", {}).get("snapshotHandle")
            require(content.get("spec", {}).get("deletionPolicy") == "Retain"
                    and content.get("spec", {}).get("driver") == "disk.csi.azure.com"
                    and (content.get("spec", {}).get("volumeSnapshotRef") or {}).get("uid") == vs.get("metadata", {}).get("uid")
                    and canonical_id(src) == canonical_id(pv_disk[pvc_name])
                    and canonical_id(arm_id) == canonical_id(records[snap_name]["id"]),
                    f"paired VolumeSnapshotContent {content_name} UID/source/ARM lineage mismatch")
            source_disk = json.loads(self.az("disk", "show", "--ids", pv_disk[pvc_name]))
            require(canonical_id(source_disk.get("id")) == canonical_id(pv_disk[pvc_name])
                    and source_disk.get("uniqueId")
                    and (source_disk.get("sku") or {}).get("name") == "PremiumV2_LRS",
                    f"measured source disk identity is unavailable for {pvc_name}")
            live = json.loads(self.az("snapshot", "show", "--ids", arm_id))
            require(canonical_id(live.get("id")) == canonical_id(arm_id)
                    and live.get("provisioningState") == "Succeeded" and live.get("snapshotAccessState") == "Available"
                    and live.get("uniqueId") and live.get("incremental") is True
                    and live.get("diskSizeGB", live.get("diskSizeGb")) == 512
                    and (live.get("sku") or {}).get("name") in ("Standard_LRS", "Standard_ZRS")
                    and (live.get("tags") or {}).get("reprotoken") == self.token
                    and (live.get("tags") or {}).get("reproPurpose") == "premiumv2-vacuum-backup"
                    and canonical_id((live.get("creationData") or {}).get("sourceResourceId")) == canonical_id(pv_disk[pvc_name])
                    and (live.get("creationData") or {}).get("sourceUniqueId") == source_disk.get("uniqueId"),
                    f"paired Backup ARM snapshot {snap_name} ownership/readiness mismatch")
            opt = (live.get("creationData") or {}).get("createOption")
            progress = completion_percent(live.get("completionPercent"))
            require(opt in ("Copy", "CopyStart") and progress is not None,
                    f"PremiumV2-origin Backup snapshot {snap_name} lacks numeric Copy/CopyStart progress")
            if progress < 100:
                pending_backups.append(snap_name)
            found[snap_name] = {"id": arm_id, "uniqueId": live["uniqueId"], "sourceDiskId": src,
                                "sourceDiskUniqueId": source_disk["uniqueId"], "createOption": opt,
                                "completionPercent": float(progress), "sourcePVC": pvc_name,
                                "volumeSnapshotUID": vs["metadata"]["uid"], "content": content_name}
        if pending_backups:
            raise HydrationPending("validated PremiumV2-origin Backup snapshot copy below 100%: "
                                   + ", ".join(pending_backups))
        self.evidence["checks"]["pairedBackupSnapshots"] = found
        return found

    def check_premium_disks(self, pby: dict[str, Any]) -> list[dict[str, Any]]:
        disks = []
        pending_disks: list[dict[str, Any]] = []
        for claim in (f"{CLUSTER}-1", f"{CLUSTER}-1-wal", f"{CLUSTER}-2", f"{CLUSTER}-2-wal"):
            pvname = pby[claim].get("spec", {}).get("volumeName")
            pv = self.read_k8s("pv", str(pvname))
            claim_ref = pv.get("spec", {}).get("claimRef", {})
            require(claim_ref.get("name") == claim and claim_ref.get("namespace") == NS
                    and claim_ref.get("uid") == pby[claim].get("metadata", {}).get("uid"),
                    f"{claim} PV claimRef does not bind the exact PVC UID")
            csi = pv.get("spec", {}).get("csi", {})
            disk_id = csi.get("volumeHandle")
            require(disk_id and csi.get("driver") == "disk.csi.azure.com", f"{claim} is not Azure managed disk CSI")
            live = json.loads(self.az("disk", "show", "--ids", disk_id))
            require(live.get("uniqueId"), f"{claim} disk has no Azure uniqueId")
            require(canonical_id(disk_id).startswith(canonical_id(
                f"/subscriptions/{self.sub}/resourceGroups/{self.node_rg}/providers/Microsoft.Compute/disks/")),
                f"{claim} disk ID is outside the owned node resource group")
            zones = [str(x) for x in live.get("zones", [])]
            require(canonical_id(live.get("location")) == canonical_id(self.loc)
                    and (live.get("sku") or {}).get("name") == "PremiumV2_LRS"
                    and int(live.get("diskSizeGB", live.get("diskSizeGb", 0))) == 512
                    and live.get("diskIOPSReadWrite") == 3000 and live.get("diskMBpsReadWrite") == 125
                    and live.get("managedBy") and self.zone in zones,
                    f"{claim} PremiumV2 size/performance/zone/owner mismatch")
            d, pending = observe_hydration(live, pv, disk_id)
            if pending:
                pending_disks.append({"claim": claim, "completionPercent": d["completionPercent"]})
            d.update({"claim": claim, "pv": pvname, "zone": zones, "managedBy": live.get("managedBy")})
            disks.append(d)
        require(len({canonical_id(d["id"]) for d in disks}) == 4, "expected four distinct PremiumV2 DATA/WAL disks")
        require(len({d.get("uniqueId") for d in disks}) == 4, "expected four distinct PremiumV2 Azure uniqueIds")
        if pending_disks:
            raise HydrationPending("validated PremiumV2 disk Copy/CopyStart progress below 100%: "
                                   + ", ".join(f"{d['claim']}={d['completionPercent']}" for d in pending_disks))
        self.evidence["checks"]["hydratedPremiumV2Disks"] = disks
        return disks

    def check_retention_locks(self, ids: list[str]) -> None:
        locks = json.loads(self.az("lock", "list", "--resource-group", self.snapshot_rg))
        for target in ids:
            def lock_scope(item: dict[str, Any]) -> str:
                if item.get("scope"):
                    return canonical_id(item["scope"])
                raw = canonical_id(item.get("id"))
                marker = "/providers/microsoft.authorization/locks/"
                return raw.split(marker, 1)[0] if marker in raw else raw
            matches = [x for x in locks if x.get("level") == "CanNotDelete" and lock_scope(x) == canonical_id(target)]
            require(bool(matches), f"retained snapshot lacks a direct CanNotDelete lock: {target.rsplit('/', 1)[-1]}")
        self.evidence["checks"]["retentionLocks"] = {"directCanNotDeleteSnapshotIds": ids}

    def check_pg_and_replication(self, pods: dict[str, Any], pby: dict[str, Any], meta: dict[str, Any], target: int) -> tuple[dict[str, int], str]:
        cfg_sql = """SELECT json_build_object(
          'systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),
          'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),
          'totalBytes',pg_total_relation_size('public.vacuum_target'),
          'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id%2=0 ORDER BY id LIMIT 1),
          'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),
          'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),
          'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,
          'stageTableAbsent',to_regclass('public.vacuum_target_stage') IS NULL,
          'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'minId',(SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1),
          'maxId',(SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1),
          'autovacuum',current_setting('autovacuum'),'walLevel',current_setting('wal_level'),
          'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),
          'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),
          'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),
          'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),
          'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),
          'trackWalIoTiming',current_setting('track_wal_io_timing'),
          'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'),
          'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"""
        configs: dict[str, dict[str, Any]] = {}
        for name in (f"{CLUSTER}-1", f"{CLUSTER}-2"):
            row = self.psql_json(name, cfg_sql)
            recovery = name.endswith("-2")
            require(str(row.get("systemIdentifier")) == str(meta.get("systemIdentifier"))
                    and row.get("inRecovery") is recovery, f"{name} source system/recovery state mismatch")
            expected = {"autovacuum": "on", "walLevel": "logical", "fullPageWrites": "on",
                        "recoveryPrefetch": "try", "vacuumCostDelay": "0", "sharedBuffers": "256MB",
                        "maintenanceIoConcurrency": "10", "checkpointTimeout": "5min", "maxWalSize": "1GB",
                        "archiveMode": "on", "trackIoTiming": "on", "trackWalIoTiming": "on"}
            require(all(row.get(k) == v for k, v in expected.items()) and row.get("tablespaceOverride") is False
                    and row.get("appSessions") == 0, f"{name} GUC defaults/workload check mismatch")
            # Read-only filesystem/mount check. No pod or DB mutation.
            mount = run(["kubectl", "exec", "-n", NS, "-c", "postgres", name, "--", "sh", "-eu", "-c",
                         'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"'], env=self.env)
            configs[name] = {"systemIdentifier": row["systemIdentifier"], "inRecovery": row["inRecovery"],
                             "heapBytes": row["heapBytes"], "selectedDefaults": expected,
                             "separateDataWalMounts": bool(mount is not None)}
        primary_live = self.psql_json(f"{CLUSTER}-1", cfg_sql)
        replica_live = self.psql_json(f"{CLUSTER}-2", cfg_sql)
        fixture = validate_fixture_sql(primary_live, target, int(meta["heapBytes"]),
                                       int(meta.get("preparedLiveTuplesEstimate", 0)))
        require(int(replica_live.get("heapBytes", 0)) == fixture["heapBytes"]
                and int(replica_live.get("totalBytes", 0)) == fixture["totalBytes"]
                and replica_live.get("payloadBytes") == 1536 and replica_live.get("payloadStoragePlain") is True
                and replica_live.get("tableAutovacuumDisabled") is True and replica_live.get("hasIndex") is True
                and replica_live.get("stageTableAbsent") is True,
                "replica fixture contents/heap do not match the primary")
        # Check each SQL min/max probe is index-backed; the SELECTs themselves
        # are LIMIT 1 and never count/scan the full heap.
        for direction in ("ASC", "DESC"):
            plan = self.psql_json(f"{CLUSTER}-1", f"EXPLAIN (FORMAT JSON) SELECT id FROM public.vacuum_target ORDER BY id {direction} LIMIT 1;")
            plan_text = json.dumps(plan)
            require("Index" in plan_text and "Seq Scan" not in plan_text,
                    f"{direction.lower()} ID probe is not index-backed; refusing a full table scan")
        primary_lsn = run(["kubectl", "exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-1", "--", "psql", "-XAtq",
                           "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", "SELECT pg_current_wal_flush_lsn();"], env=self.env).strip()
        deadline = time.monotonic() + 3600
        caught = False
        while time.monotonic() < deadline:
            streaming = run(["kubectl", "exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-1", "--", "psql", "-XAtq",
                             "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c",
                             "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;"], env=self.env).strip()
            replay = run(["kubectl", "exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-2", "--", "psql", "-XAtq",
                          "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c",
                          f"SELECT pg_last_wal_replay_lsn()>='{primary_lsn}'::pg_lsn;"], env=self.env).strip()
            if streaming == "1" and replay == "t":
                caught = True
                break
            time.sleep(5)
        require(caught, "replica did not catch up through the sampled primary flush LSN within one hour")
        self.evidence["checks"]["postgres"] = {"configs": configs, "fixtureLiveSQL": fixture,
            "tupleCountsAreEstimates": True, "rawInsertDeleteOutputCaptured": False,
            "indexBackedMinMaxProbes": True, "temporaryStageTableAbsent": True,
            "primaryFlushLsn": primary_lsn, "replicaCaughtUp": True}
        return fixture, primary_lsn

    def run(self) -> tuple[dict[str, Any], dict[str, Any]]:
        manifest, meta, stage, target = self.local_provenance()
        self.control_flow_capture()
        self.owner_preflight()
        copy_id = self.live_source_snapshot(manifest, stage)
        c, pods, vss, vss_name, vscnt, content = self.check_cluster_and_seed(
            copy_id, str(manifest["sourceSnapshotId"]), str(meta["systemIdentifier"]))
        backup, data, wal, pvcs, pby = self.check_backup_and_pvcs()
        backup_snaps = self.check_backup_snapshot_lineage(backup, data, wal, pby)
        self.check_retention_locks([copy_id, backup_snaps[data]["id"], backup_snaps[wal]["id"]])
        disks = self.check_premium_disks(pby)
        fixture, replay_lsn = self.check_pg_and_replication(pods, pby, meta, target)
        # Bind the target to the immutable source prefix estimate and live max-id.
        expected_target_bytes = target * GIB
        require(abs(fixture["heapBytes"] - expected_target_bytes) <= expected_target_bytes // 100,
                "fixture target heap failed final size bound")
        self.evidence.update({"status": "complete", "verifiedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
                              "sourceKind": "incremental_copy", "systemIdentifier": str(meta["systemIdentifier"]),
                              "targetHeapGiB": target, "restoreCopySnapshotId": copy_id,
                              "backupSnapshotIdentities": backup_snaps,
                              "fourHydratedPremiumV2Disks": len(disks),
                              "globalCloudQuiescenceClaim": False})
        prepare = make_prepare(self.stack.name, str(manifest["sourceSnapshotId"]), "incremental_copy",
                               str(meta["systemIdentifier"]), int(meta["heapBytes"]), int(meta["tableBytes"]),
                               target, fixture, replay_lsn, copy_id, self.short, self.zone, self.snapshot_rg,
                               str(backup["metadata"]["name"]), data, wal)
        return prepare, self.evidence


def write_private(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    tmp = path.with_name(path.name + f".tmp.{os.getpid()}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(obj, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stack", required=True, choices=("50gib", "100gib"))
    ap.add_argument("--control-flow-dir", required=True, type=Path,
                    help="privately captured original prepare TMP directory; no .state writes")
    ap.add_argument("--output-dir", type=Path,
                    help="private external output directory (default recovery/numeric-hydration-finalizer/results/<stack>)")
    ap.add_argument("--original-process-receipt", type=Path,
                    help="archived failed/stopped original 02 process receipt (default stack .state receipt)")
    ap.add_argument("--original-prepare-script", type=Path,
                    help="externally preserved frozen original 02 script (default current stack script)")
    ap.add_argument("--seed-job-uid", help="captured exact replica snapshot-recovery Job UID")
    ap.add_argument("--seed-event-evidence", type=Path,
                    help="optional archived Kubernetes Event list for a GC'd seed Job")
    ap.add_argument("--seed-event-sha256",
                    help="required SHA-256 pin paired with --seed-event-evidence")
    args = ap.parse_args(argv)
    stack = ROOT / args.stack
    out = args.output_dir or ROOT / "recovery/numeric-hydration-finalizer/results" / args.stack
    try:
        finalizer = Finalizer(stack, args.control_flow_dir, out, args.original_process_receipt,
                              args.original_prepare_script, args.seed_job_uid,
                              args.seed_event_evidence, args.seed_event_sha256)
        prepare, audit = finalizer.run()
        audit["prepareReceipt"] = str((out / "prepare.json").relative_to(ROOT))
        write_private(out / "audit.json", audit)
        write_private(out / "prepare.json", prepare)
        print(f"Read-only hydration finalization passed for {args.stack}; private external receipts are under {out.relative_to(ROOT)}")
        return 0
    except HydrationPending as exc:
        print(f"HYDRATION PENDING: {exc}", file=sys.stderr)
        return 3
    except AuditError as exc:
        print(f"FINALIZATION BLOCKED: {exc}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, ValueError, StopIteration) as exc:
        print(f"FINALIZATION BLOCKED: malformed evidence ({exc.__class__.__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
