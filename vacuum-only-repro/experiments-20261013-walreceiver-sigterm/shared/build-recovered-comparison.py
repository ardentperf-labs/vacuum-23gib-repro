#!/usr/bin/env python3
"""Build a separate, proof-gated report for the recovered WALreceiver run.

This reads retained artifacts only. It never repairs or edits the original
measurement/timing/client summary files and deliberately leaves the original
runner outcome incomplete.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

UTC = dt.timezone.utc
TEN = 10
MAX_SAMPLE_GAP = 2.5
MIN_PROOF_SECONDS = 7200.0


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                value = json.loads(line)
                if isinstance(value, dict):
                    yield number, value


def parse_time(value: Any) -> dt.datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("missing timestamp")
    stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC)


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def qualifying(row: dict[str, Any]) -> tuple[bool, tuple[tuple[Any, ...], ...], int | None]:
    if row.get("sampleStatus") != "ok":
        return False, (), None
    backends = row.get("backends") or []
    if len(backends) != TEN:
        return False, (), None
    identities = []
    for backend in backends:
        if (backend.get("state") != "active" or backend.get("waitEventType") != "IPC"
                or backend.get("waitEvent") != "SyncRep" or not backend.get("queryStartUtc")
                or not backend.get("xactStartUtc")):
            return False, (), None
        identities.append((backend.get("pid"), backend.get("backendStartUtc"),
                           backend.get("queryStartUtc"), backend.get("xactStartUtc")))
    return True, tuple(sorted(identities)), row.get("committedTransactions")


def measure_writer_streak(path: Path, start: dt.datetime, cutoff: dt.datetime) -> dict[str, Any]:
    active_start = previous = last = None
    stable_ids = None
    samples = 0
    max_gap = 0.0
    commits: set[int] = set()
    parse_errors = 0
    for _, row in read_jsonl(path):
        try:
            observed = parse_time(row.get("observedAtUtc"))
        except (ValueError, TypeError, OverflowError):
            parse_errors += 1
            continue
        if observed < start or observed > cutoff:
            continue
        okay, identities, committed = qualifying(row)
        if not okay:
            active_start = previous = last = None
            stable_ids = None
            continue
        if stable_ids is not None and identities != stable_ids:
            active_start = previous = last = None
            stable_ids = None
        if previous is not None:
            gap = (observed - previous).total_seconds()
            max_gap = max(max_gap, gap)
            if gap > MAX_SAMPLE_GAP:
                active_start = last = None
                stable_ids = None
        if active_start is None:
            active_start = observed
            stable_ids = identities
        previous = last = observed
        samples += 1
        if isinstance(committed, int):
            commits.add(committed)
    span = (last - active_start).total_seconds() if active_start and last else 0.0
    if samples == 0 or span <= 0:
        raise ValueError("no continuously sampled ten-writer SyncRep interval before cutoff")
    if len(commits) != 1:
        raise ValueError("blocked writer sample commit counter changed during the proof interval")
    return {
        "startUtc": iso(active_start), "endUtc": iso(last), "spanSeconds": round(span, 3),
        "qualifyingSamples": samples, "maximumGapSeconds": round(max_gap, 3),
        "maxAllowedGapSeconds": MAX_SAMPLE_GAP, "sameTenBackendQueryAndTransactionStarts": True,
        "allTenActiveIpcSyncRepAtEverySample": True, "committedTransactionsAtEverySample": next(iter(commits)),
        "parseErrorsOutsideProofRows": parse_errors,
    }


def ensure_hash(path: Path, expected: str, label: str) -> str:
    actual = sha256(path)
    if not expected or actual != expected:
        raise ValueError(f"{label} SHA-256 does not match its retained receipt")
    return actual


def measure_native_progress(path: Path, start: dt.datetime, cutoff: dt.datetime) -> dict[str, Any]:
    pattern = re.compile(r"^progress: ([0-9]+(?:\.[0-9]+)?) s, ([0-9]+(?:\.[0-9]+)?) tps,.*? ([0-9]+) failed,")
    rows = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        stamp = dt.datetime.fromtimestamp(float(match.group(1)), UTC)
        if start <= stamp <= cutoff:
            rows.append((stamp, float(match.group(2)), int(match.group(3))))
    if len(rows) < 2:
        raise ValueError("native pgbench output lacks progress coverage after SIGTERM")
    zero = [row for row in rows if row[1] == 0 and row[2] == 0]
    if not zero or (zero[-1][0] - zero[0][0]).total_seconds() < MIN_PROOF_SECONDS:
        raise ValueError("native progress does not show at least two hours of zero successful/failed TPS")
    if any(row[2] != 0 for row in rows):
        raise ValueError("native progress records a failed pgbench transaction during proof window")
    return {
        "progressBucketsAfterSignalThroughCutoff": len(rows),
        "zeroTpsZeroFailureBuckets": len(zero), "nonzeroTpsBuckets": len(rows) - len(zero),
        "firstZeroBucketUtc": iso(zero[0][0]), "lastZeroBucketUtc": iso(zero[-1][0]),
        "zeroTpsSpanSeconds": round((zero[-1][0] - zero[0][0]).total_seconds(), 3),
        "allFailureCountsZero": True,
    }


def evidence_file(path: Path, root: Path) -> dict[str, Any]:
    return {"path": path.relative_to(root).as_posix(), "sha256": sha256(path), "bytes": path.stat().st_size}


def read_marker(path: Path, expected: str) -> dict[str, Any]:
    fields = path.read_text(encoding="utf-8").strip().split("|")
    if len(fields) != 6 or fields[0] != expected:
        raise ValueError(f"invalid {expected} marker")
    return {"kind": fields[0], "time": fields[1], "lsn": fields[2],
            "walBytes": int(fields[3]), "walRecords": int(fields[4]), "walFpi": int(fields[5])}


def build(root: Path) -> dict[str, Any]:
    proof_path = root / "results/preview/ongoing-stall-proof.json"
    progress_path = root / "results/diagnosis/public/native-progress-snapshots/sigterm-pgbench.out"
    if not progress_path.is_file():
        progress_path = root / ".state/ongoing-monitor/native-progress-full/sigterm-pgbench.out"
    control_progress_path = root / "results/diagnosis/public/native-progress-snapshots/control-pgbench.out"
    sessions_path = root / "sigterm/results/measurement/client/sessions.jsonl"
    treatment_timing_path = root / "sigterm/results/measurement/timing.json"
    control_timing_path = root / "control/results/measurement/timing.json"
    intervention_path = root / "sigterm/results/measurement/walreceiver-intervention.json"
    vacuum_receipts = {lane: root / f"results/preview/{lane}-vacuum-completion/receipt.json"
                       for lane in ("control", "sigterm")}
    recovery_path = root / "sigterm/results/diagnosis/recovered-client/recovery-receipt.json"
    archive_path = root / "results/diagnosis/public/archive-restore-azure-client-correlation.json"
    if not archive_path.is_file():
        archive_path = root / "results/diagnosis/private/supplemental/postgres-archive-restore-azure-correlation-through-20261013T102800Z.json"
    control_summary_path = root / "control/results/measurement/client-summary.json"
    control_timing = read_json(control_timing_path)
    treatment_timing = read_json(treatment_timing_path)
    proof = read_json(proof_path)
    intervention = read_json(intervention_path)
    recovery = read_json(recovery_path)
    archive = read_json(archive_path)
    control_summary = read_json(control_summary_path)
    if "archiveRecovery" in archive:
        public_archive = archive["archiveRecovery"]
        treatment_archive = public_archive["treatment"]
        control_archive = public_archive["control"]
        azure_archive = public_archive["azureBlobTransactions"]
        archive_facts = {
            "treatmentCount": treatment_archive["restoreEventCount"],
            "treatmentUnique": treatment_archive["uniqueWalSegmentCount"],
            "treatmentContiguous": treatment_archive["walSequenceContiguous"],
            "treatmentErrors": treatment_archive["restoreWarningOrErrorEvents"],
            "controlCount": control_archive["restoreEventCount"],
            "controlErrors": control_archive["restoreWarningOrErrorEvents"],
            "treatmentBlobCount": azure_archive["treatmentPostInterventionCount"],
            "controlBlobCount": azure_archive["controlPostInterventionCount"],
            "countsMatch": azure_archive["treatmentCountEqualsRestoreLogEvents"],
            "lastRestoreUtc": public_archive["archiveToStreamingBoundary"]["lastSuccessfulRestoreUtc"],
            "streamStartUtc": public_archive["archiveToStreamingBoundary"]["newStreamingStartedUtc"],
            "secondsToStream": public_archive["archiveToStreamingBoundary"]["secondsFromLastRestoreToStreamStart"],
        }
    else:
        treatment_archive = archive["treatment"]
        control_archive = archive["control"]
        azure = archive["azureBlobNativeMetrics"]
        checks = archive["crossChecks"]
        archive_facts = {
            "treatmentCount": treatment_archive["restoreEventCount"],
            "treatmentUnique": treatment_archive["uniqueWalSegmentNames"],
            "treatmentContiguous": treatment_archive["walSegmentSequenceContiguous"],
            "treatmentErrors": treatment_archive["archiveRelatedWarningOrErrorCount"],
            "controlCount": control_archive["restoreEventCount"],
            "controlErrors": control_archive["archiveRelatedWarningOrErrorCount"],
            "treatmentBlobCount": azure["sigterm"]["getBlobSuccessPostMarkerTotal"],
            "controlBlobCount": azure["control"]["getBlobSuccessPostMarkerTotal"],
            "countsMatch": checks["treatmentGetBlobSuccessCountMatchesRestoreLogEvents"],
            "lastRestoreUtc": checks["treatmentLastArchiveRestoreUtc"],
            "streamStartUtc": checks["treatmentNewWalStreamStart"]["logTimeUtc"],
            "secondsToStream": checks["secondsFromLastRestoreEventToNewStream"],
        }

    cutoff = parse_time(recovery["originalRunState"]["runnerObservationEndUtc"])
    start = parse_time(intervention["executedAtUtc"])
    signal_accepted = parse_time(intervention["signalResult"]["signalAcceptedAtUtc"])
    if intervention.get("status") != "sent" or intervention.get("signalResult", {}).get("signalSent") is not True:
        raise ValueError("treatment does not have a verified SIGTERM receipt")
    if abs((signal_accepted - start).total_seconds()) > 5:
        raise ValueError("SIGTERM requested and accepted timestamps differ unexpectedly")
    if treatment_timing.get("testComplete") is not False or recovery["originalRunState"].get("testComplete") is not False:
        raise ValueError("original treatment harness state must remain explicitly incomplete")
    if treatment_timing.get("remoteVacuumExitCode") != 0 or recovery["originalRunState"].get("remoteVacuumExitCode") != 0:
        raise ValueError("treatment VACUUM exit status is not successful")
    if not recovery["originalRunState"].get("minimumObservationSatisfied"):
        raise ValueError("minimum observation window was not satisfied")
    if parse_time(recovery["originalRunState"]["vacuumStartMarkerUtc"]) >= cutoff:
        raise ValueError("observation cutoff precedes VACUUM start")

    proof_native = proof["nativeFullProgressSnapshot"]["sigterm"]
    native_sha = ensure_hash(progress_path, proof_native["captureSha256"], "native pgbench progress")
    if native_sha != recovery["nativeAggregateProgress"]["sha256"]:
        raise ValueError("native progress digest differs between the two evidence receipts")
    sessions_sha = ensure_hash(sessions_path, recovery["clientSessionTelemetry"]["sha256"], "writer session telemetry")
    # The in-progress checkpoint was captured later than this recovered,
    # immutable native session file; their byte hashes are expected to differ.
    # The checkpoint supplies an earlier independent summary, while the
    # recovery receipt pins the final retained sessions file above.
    streak = measure_writer_streak(sessions_path, start, cutoff)
    if streak["spanSeconds"] < MIN_PROOF_SECONDS:
        raise ValueError("recovered proof does not establish at least two hours before client stop")
    if streak["committedTransactionsAtEverySample"] != 4610:
        raise ValueError("treatment blocked-writer transaction counter differs from reviewed checkpoint")
    native_progress = measure_native_progress(progress_path, start, cutoff)

    vacuum = {}
    for lane, receipt_path in vacuum_receipts.items():
        receipt = read_json(receipt_path)
        if receipt.get("remoteExitCode") != 0:
            raise ValueError(f"{lane} has no successful native VACUUM completion receipt")
        marker_start = read_marker(receipt_path.parent / "vacuum-start.marker", "VACUUM_START")
        marker_end = read_marker(receipt_path.parent / "vacuum-complete.marker", "VACUUM_COMPLETE")
        start_at, end_at = parse_time(marker_start["time"]), parse_time(marker_end["time"])
        if end_at <= start_at:
            raise ValueError(f"{lane} VACUUM markers are not ordered")
        vacuum[lane] = {
            "startUtc": iso(start_at), "completeUtc": iso(end_at),
            "elapsedSeconds": round((end_at - start_at).total_seconds(), 3),
            "exitCode": 0, "markerEvidence": evidence_file(receipt_path.parent / "vacuum-complete.marker", root),
        }
    if parse_time(treatment_timing["vacuumStartMarkerObserved"]["time"]) != parse_time(vacuum["sigterm"]["startUtc"]):
        raise ValueError("treatment VACUUM marker disagrees with retained timing evidence")

    reconcile = control_summary.get("commitLogReconciliation") or {}
    if (control_summary.get("pgbenchExitCode") != 0 or control_summary.get("failedTransactions") != 0
            or reconcile.get("difference") != 0 or reconcile.get("serverCommittedUpdates") != 43670):
        raise ValueError("control client native transactions do not reconcile cleanly")

    if not (archive_facts["countsMatch"] is True
            and archive_facts["treatmentCount"] == archive_facts["treatmentBlobCount"]
            and archive_facts["controlCount"] == archive_facts["controlBlobCount"]):
        raise ValueError("archive restore and native Azure request evidence does not reconcile")
    if archive_facts["treatmentContiguous"] is not True or archive_facts["treatmentErrors"] != 0:
        raise ValueError("treatment archive restore stream has gaps or reported archive errors")

    return {
        "schemaVersion": 1,
        "analysisType": "recovered-evidence-review; separate from original runner result",
        "generatedAtUtc": dt.datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "setup": {"dataset": "23 GiB PostgreSQL fixture restored from the same retained paired DATA/WAL source",
                  "nodeVmSize": "Standard_D2s_v4", "dataAndWalDiskSku": "Standard_LRS",
                  "dataAndWalDiskSizeGiB": 512, "hostCaching": "None",
                  "postgresImageVersion": "16.15", "cnpgOperatorVersion": "1.30.1",
                  "clientWriters": TEN, "targetAggregateRateTransactionsPerSecond": 5,
                  "synchronousCommit": "on", "synchronousStandbyNames": "ANY 1"},
        "incidentClaim": {
            "status": "verified_over_two_hours_before_stop_cutoff",
            "durationSeconds": streak["spanSeconds"], "startUtc": streak["startUtc"], "lastSampleUtc": streak["endUtc"],
            "stopCutoffUtc": iso(cutoff), "cutoffDefinition": "last runner observation before the recorded client stop/capture failure; no post-cutoff samples count toward the offered-load proof",
            "writers": TEN, "wait": "IPC/SyncRep", "nativeProgress": native_progress,
            "nativeZeroTPSNoFailuresAfterFirstPostSignalBucket": (native_progress["nonzeroTpsBuckets"] <= 1
                                                                    and native_progress["allFailureCountsZero"]),
            "committedTransactionsStayedAt": streak["committedTransactionsAtEverySample"],
            "sampleEvidence": streak,
        },
        "treatment": {
            "mode": "verified replica walreceiver SIGTERM", "signalRequestedUtc": iso(start),
            "signalAcceptedUtc": iso(signal_accepted),
            "secondsAfterVacuumStart": round((signal_accepted - parse_time(vacuum["sigterm"]["startUtc"])).total_seconds(), 3),
            "schedulerLatenessSeconds": intervention.get("schedulerDispatchLatenessSeconds"),
            "vacuum": vacuum["sigterm"], "nativeProgressEvidence": evidence_file(progress_path, root),
            "writerTelemetryEvidence": evidence_file(sessions_path, root),
            "eventualFixtureUpdateCount": recovery["currentReadOnlyDatabaseState"]["fixtureUpdateCountSum"],
            "lastBlockedSampleCommittedTransactions": recovery["clientSessionTelemetry"]["lastSampleCommittedTransactions"],
            "eventualCounterDifference": recovery["currentReadOnlyDatabaseState"]["fixtureUpdateCountSum"] - recovery["clientSessionTelemetry"]["lastSampleCommittedTransactions"],
            "firstCompletionTime": None,
            "clientRawTransactionLogsAvailable": False,
        },
        "control": {
            "mode": "no walreceiver signal", "vacuum": vacuum["control"],
            "clientTransactions": control_summary["completedTransactions"],
            "failedTransactions": control_summary["failedTransactions"],
            "latencyMs": {key: control_summary["latencyMs"].get(key) for key in ("p50", "p95", "p99", "max")},
            "clientServerCommitDifference": reconcile["difference"],
        },
        "archiveRecovery": {
            "treatmentRestoreEventsAfterSignal": archive_facts["treatmentCount"],
            "uniqueContiguousWalSegments": archive_facts["treatmentUnique"],
            "treatmentAzureGetBlobSuccessTotal": archive_facts["treatmentBlobCount"],
            "controlRestoreEventsAfterMarker": archive_facts["controlCount"],
            "controlAzureGetBlobSuccessTotal": archive_facts["controlBlobCount"],
            "getBlobCountEqualsRestoreLogCount": archive_facts["countsMatch"],
            "lastRestoreUtc": archive_facts["lastRestoreUtc"],
            "streamingResumedUtc": archive_facts["streamStartUtc"],
            "secondsFromLastRestoreToStreaming": archive_facts["secondsToStream"],
            "archiveErrorCount": archive_facts["treatmentErrors"],
            "interpretation": "Archive restores and Azure GetBlob totals agree; this supports archive-backed recovery but does not map individual API calls to WAL filenames or identify the first resumed client transaction.",
        },
        "originalHarness": {
            "treatmentTestComplete": False, "treatmentVacuumCompleteField": treatment_timing.get("vacuumComplete"),
            "treatmentRemoteVacuumExitCode": treatment_timing.get("remoteVacuumExitCode"),
            "treatmentRecordedFailure": recovery["originalRunState"].get("error"),
            "failureUtc": recovery["originalRunState"].get("clientStopOrCaptureFailedUtc"),
            "controlTestComplete": control_timing.get("testComplete"),
            "limitation": "The original timing marker parser and client drain path failed to finalize the treatment run. This recovered analysis verifies the event independently; it does not alter timing.json, fabricate transaction logs, or mark the original run complete.",
        },
        "evidence": {
            "checkpointProof": evidence_file(proof_path, root),
            "recoveryReceipt": evidence_file(recovery_path, root),
            "archiveCorrelation": evidence_file(archive_path, root),
            "controlClientSummary": evidence_file(control_summary_path, root),
            "controlNativeProgress": evidence_file(control_progress_path, root) if control_progress_path.is_file() else None,
        },
    }


def report(data: dict[str, Any]) -> str:
    incident, treatment, control = data["incidentClaim"], data["treatment"], data["control"]
    ar = data["archiveRecovery"]
    return f"""# Recovered analysis: walreceiver SIGTERM and control

This report keeps the original failed harness result intact and evaluates retained native evidence separately. Both stacks used the same 23 GiB PostgreSQL fixture from the retained paired DATA/WAL restore source, 512 GiB `Standard_LRS` DATA and WAL disks with host caching disabled, `Standard_D2s_v4` nodes, PostgreSQL 16.15, and CNPG 1.30.1. The ten persistent pgbench writers targeted 5 aggregate transactions per second with synchronous commit enabled. The treatment reproduced a greater-than-two-hour write stall for these measured writers. It does not establish that every write in the whole database was blocked, and it does not prove the first client completion time after the stall.

The intervention SIGTERM was accepted at {treatment['signalAcceptedUtc']}, {treatment['secondsAfterVacuumStart']} seconds after VACUUM start. From {incident['startUtc']} through the last sample at {incident['lastSampleUtc']} (the conservative pre-stop cutoff {incident['stopCutoffUtc']}), all ten same backend/query/transaction identities remained active on `IPC/SyncRep`. The independently counted sample streak spans {incident['durationSeconds']:.1f} seconds, with a maximum sample gap of {incident['sampleEvidence']['maximumGapSeconds']:.3f} seconds. Their committed transaction counter remained {incident['committedTransactionsStayedAt']}.

The native `pgbench -P` capture independently shows only zero-TPS, zero-failure buckets after the initial post-SIGTERM bucket through the stop cutoff. These are aggregate progress buckets rather than per-transaction latency records. The two-hour claim ends at the runner’s final observation before its failed client stop/capture; later samples are not counted as offered-load proof.

| Measurement | SIGTERM treatment | Control |
|---|---:|---:|
| VACUUM duration | {treatment['vacuum']['elapsedSeconds']:.1f} s | {control['vacuum']['elapsedSeconds']:.1f} s |
| Completed client transactions | Not recoverable from deleted treatment logs | {control['clientTransactions']} |
| Control failures / reconciliation difference | — | {control['failedTransactions']} / {control['clientServerCommitDifference']} |
| Control client latency p50 / p95 / p99 / max | — | {control['latencyMs']['p50']} / {control['latencyMs']['p95']} / {control['latencyMs']['p99']} / {control['latencyMs']['max']} ms |

Treatment archive evidence records {ar['treatmentRestoreEventsAfterSignal']} contiguous WAL restore events after SIGTERM, matching the same count of Azure Blob `GetBlob` successes. The control recorded {ar['controlRestoreEventsAfterMarker']} archive restores and {ar['controlAzureGetBlobSuccessTotal']:.0f} post-marker successful `GetBlob` requests. The last observed treatment archive restore was {ar['lastRestoreUtc']}; streaming resumed at {ar['streamingResumedUtc']}, about {ar['secondsFromLastRestoreToStreaming']:.2f} seconds later. This confirms the archive path was active and Azure requests accompanied restore logs; it does not identify the first completed client write or prove a specific failed-object restore.

The primary’s later read-only fixture counter was {treatment['eventualFixtureUpdateCount']}, ten above the last blocked-sample counter. That establishes eventual additional commits by the later capture, but the exact first completion time is unknown. No treatment per-transaction completion logs survived, so treatment latency percentiles, completion rate after recovery, and a finalized native client summary are unavailable.

## Original harness status

Both original lane timing records remain `testComplete: false`. The treatment native VACUUM process exited zero and marker files independently confirm completion, but its timing record has no parsed completion marker and client drain/capture failed at {data['originalHarness']['failureUtc']}. The control client stopped cleanly and its transactions reconcile, though the same missing-marker issue kept the original control harness from finalizing. The run source subsequently removed the treatment remote client work directory; this is an artifact-loss limitation, not evidence of zero transactions. The separate recovered JSON keeps these facts explicit and does not rewrite original state.

## Figures

![Client stall and writer wait evidence](graphs/recovered-writer-stall.png)

![Client wait, transaction age and replica backlog](graphs/comparison-overview.png)

![Guest disk IOPS](graphs/guest-iops.png)

![Guest disk await](graphs/guest-await.png)

![Guest disk throughput](graphs/guest-throughput.png)

![BCC block I/O latency](graphs/bcc-read-write-latency.png)

![Azure disk IOPS](graphs/azure-disk-iops.png)

![Azure disk throughput](graphs/azure-disk-throughput.png)

![Azure disk latency](graphs/azure-disk-latency.png)

![Azure disk queue depth](graphs/azure-disk-queue-depth.png)

![Azure VM uncached consumption](graphs/azure-vm-uncached-consumed.png)

![Azure Blob requests](graphs/azure-blob-requests.png)

![Azure Blob traffic](graphs/azure-blob-traffic.png)

![Azure Blob successful-request latency](graphs/azure-blob-successful-latency.png)

All plotted axes are linear. The guest diskstats streams end around 08:25 UTC. Azure Monitor figures are clipped to 05:13–08:36 UTC, covering the client start through five minutes after replica streaming resumed. The offered-load proof ends at 07:38 UTC. Native metric gaps remain gaps; the figures do not extend zero activity across unobserved intervals. `graph-coverage.json` records clipped windows and chart coverage without Azure resource identifiers. These are diagnostic views and do not change original run completion state.

## Machine-readable evidence

`comparison.json` contains the verified cutoffs, proof counts, reconciliation, archive correlation, and safe evidence hashes. It excludes backend PIDs, Azure resource identifiers, credentials, owner tokens, and raw private receipts.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, help="separate output directory; defaults to recovered-analysis/")
    args = parser.parse_args()
    root = args.experiment_root.expanduser().resolve()
    out = (args.output or root / "recovered-analysis").expanduser().resolve()
    data = build(root)
    out.mkdir(parents=True, exist_ok=True)
    (out / "comparison.json").write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    (out / "REPORT.md").write_text(report(data), encoding="utf-8")
    print(json.dumps({"output": str(out), "claim": data["incidentClaim"]["status"],
                      "durationSeconds": data["incidentClaim"]["durationSeconds"],
                      "treatmentTestComplete": data["originalHarness"]["treatmentTestComplete"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
