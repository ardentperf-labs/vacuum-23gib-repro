from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("finalize_continuation.py")
spec = importlib.util.spec_from_file_location("finalize_continuation", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


def example_provenance():
    backup = {"name": "vacuum-backup-ab12cd34", "uid": "backup-uid",
              "pair": {"data": "data-snapshot", "wal": "wal-snapshot"}}
    snapshots = {
        "data-snapshot": {"id": "/snapshots/data", "uniqueId": "data-uid",
                           "sourcePVC": "hdd-pg-1", "volumeSnapshotUID": "vss-data",
                           "sourceDiskId": "/disks/data", "sourceDiskUniqueId": "disk-data",
                           "createOption": "Copy", "volumeSnapshotContent": "content-data",
                           "completionPercent": 100.0},
        "wal-snapshot": {"id": "/snapshots/wal", "uniqueId": "wal-uid",
                         "sourcePVC": "hdd-pg-1-wal", "volumeSnapshotUID": "vss-wal",
                         "sourceDiskId": "/disks/wal", "sourceDiskUniqueId": "disk-wal",
                         "createOption": "CopyStart", "volumeSnapshotContent": "content-wal",
                         "completionPercent": 100.0},
    }
    evidence = {
        "originalProcessReceiptSha256": "terminal-sha", "backup": backup,
        "pairedSnapshots": snapshots, "seedValidation": {"jobUID": "seed-job-uid"},
        "finalControlPath": "recovery/backup-wait-resume/result/control",
                "finalControlFileSha256": {"cluster.yaml": "cluster-hash"},
    }
    evidence["seedValidation"]["completedEvent"] = {"reason": "Completed", "message": "Job completed"}
    provenance = {"evidence": evidence}
    audit = {"globalCloudQuiescenceClaim": False,
        "checks": {
            "frozenPreparation": {"originalProcessReceiptSha256": "terminal-sha",
                                  "originalPrepareScriptSha256": module.ORIGINAL_02_SHA,
                                  "originalProcessStatus": "failed"},
            "backupAndReplicaSeed": {"backupUID": "backup-uid", "backup": "vacuum-backup-ab12cd34",
                "dataVolumeSnapshot": "data-snapshot", "walVolumeSnapshot": "wal-snapshot",
                "seedJob": {"uid": "seed-job-uid"}},
            "pairedBackupSnapshots": {
                name: {"id": row["id"], "uniqueId": row["uniqueId"], "sourcePVC": row["sourcePVC"],
                       "volumeSnapshotUID": row["volumeSnapshotUID"], "completionPercent": 100.0}
                for name, row in snapshots.items()},
            "originalFinalizationCapture": {"path": evidence["finalControlPath"],
                                              "fileSha256": evidence["finalControlFileSha256"]},
        }}
    for name, row in provenance["evidence"]["pairedSnapshots"].items():
        audit["checks"]["pairedBackupSnapshots"][name].update({
            "sourceDiskId": row["sourceDiskId"],
            "sourceDiskUniqueId": row["sourceDiskUniqueId"],
            "createOption": row["createOption"], "content": row["volumeSnapshotContent"],
        })
    return audit, provenance


class ContinuationAdapterTests(unittest.TestCase):
    def test_frozen_live_audit_must_match_backup_seed_and_snapshot_uids(self):
        audit, provenance = example_provenance()
        module.validate_frozen_audit(audit, provenance)

        changed = {**audit, "checks": {**audit["checks"],
            "backupAndReplicaSeed": {**audit["checks"]["backupAndReplicaSeed"], "backupUID": "new-uid"}}}
        with self.assertRaises(module.AdapterError):
            module.validate_frozen_audit(changed, provenance)

        paired = dict(audit["checks"]["pairedBackupSnapshots"])
        paired["wal-snapshot"] = {**paired["wal-snapshot"], "uniqueId": "changed"}
        changed = {**audit, "checks": {**audit["checks"], "pairedBackupSnapshots": paired}}
        with self.assertRaises(module.AdapterError):
            module.validate_frozen_audit(changed, provenance)

        capture = {**audit["checks"]["originalFinalizationCapture"],
                   "fileSha256": {"cluster.yaml": "different"}}
        changed = {**audit, "checks": {**audit["checks"], "originalFinalizationCapture": capture}}
        with self.assertRaises(module.AdapterError):
            module.validate_frozen_audit(changed, provenance)

    def test_attribution_replaces_false_original_tail_claim_with_continuation_scope(self):
        audit, provenance = example_provenance()
        audit["checks"]["originalFinalizationCapture"]["meaning"] = "Original script reached final tail"
        audit["checks"]["backupAndReplicaSeed"]["seedJob"] = {"archivedEventEvidence": {
            "controlFlowCapturePath": "recovery/backup-wait-resume/result/control",
            "controlFlowCaptureFileSha256": {"cluster.yaml": "cluster-hash"}}}
        audit["limitations"] = ["The frozen script and finalization capture prove control flow passed the original equality guards."]
        with tempfile.TemporaryDirectory(dir=module.RECOVERY) as td:
            root = Path(td)
            evidence_path = root / "continuation-evidence.json"
            evidence_path.write_text("{}")
            progress_path = root / "resume-progress.json"
            progress_path.write_text("{}")
            journal_path = root / "output-snapshots.jsonl"
            journal_path.write_text("{}\n")
            evidence = provenance["evidence"]
            evidence.update({"preBackupCapturePath": "recovery/backup-wait-resume/original/prebackup-tmp",
                "preBackupCaptureFileSha256": {"backup.yaml": "backup-hash", "cluster.yaml": "cluster-hash",
                                                "seed-path": "seed-path-hash"},
                "preBackupCaptureReceiptPath": "recovery/backup-wait-resume/original/prebackup-capture.json",
                "preBackupCaptureReceiptSha256": "capture-sha",
                "seedEventsSha256": "event-sha"})
            provenance.update({"evidencePath": evidence_path, "progressPath": progress_path,
                               "progressSha256": module.sha256(progress_path),
                               "journalPath": journal_path, "journalSha256": module.sha256(journal_path),
                               "eventsPath": evidence_path,
                               "terminal": {"status": "failed"}})
            rewritten = module.corrected_attribution(audit, provenance)
        meaning = rewritten["checks"]["continuationFinalizationCapture"]["meaning"]
        original_boundary = rewritten["checks"]["originalPreBackupCapture"]["meaning"]
        self.assertIn("generated by the pinned Backup-timeout continuation", meaning)
        self.assertIn("request boundary", original_boundary)
        self.assertIn("not a signed", original_boundary)
        self.assertNotIn("originalFinalizationCapture", rewritten["checks"])
        self.assertIn("continuationFinalizationCapturePath",
                      rewritten["checks"]["backupAndReplicaSeed"]["seedJob"]["archivedEventEvidence"])
        self.assertNotIn("controlFlowCapturePath",
                         rewritten["checks"]["backupAndReplicaSeed"]["seedJob"]["archivedEventEvidence"])
        self.assertTrue(all("finalization capture prove control flow passed the original equality guards" not in x
                            for x in rewritten["limitations"]))
        self.assertIn("attempt reached the captured pre-Backup request boundary", rewritten["executionAttribution"])
        self.assertFalse(rewritten["globalCloudQuiescenceClaim"])

    def test_frozen_audit_compares_copy_source_and_content_lineage(self):
        audit, provenance = example_provenance()
        module.validate_frozen_audit(audit, provenance)
        paired = dict(audit["checks"]["pairedBackupSnapshots"])
        paired["data-snapshot"] = {**paired["data-snapshot"], "sourceDiskUniqueId": "changed"}
        changed = {**audit, "checks": {**audit["checks"], "pairedBackupSnapshots": paired}}
        with self.assertRaises(module.AdapterError):
            module.validate_frozen_audit(changed, provenance)

    def test_event_journal_progress_and_final_controls_are_rehashed_after_audit(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stack = root / "100gib"
            (stack / ".state").mkdir(parents=True)
            module.STACK = stack
            evidence_path = root / "continuation-evidence.json"
            evidence_path.write_text("continuation")
            progress_path = root / "resume-progress.json"
            progress_path.write_text("progress")
            events_path = root / "seed-events.json"
            events_path.write_text("events")
            seed_job_path = root / "seed-job.json"
            seed_job_path.write_text("seed-job")
            journal_path = root / "external-journal.jsonl"
            journal_path.write_text("journal")
            (stack / ".state/output-snapshots.jsonl").write_text("journal")
            controls = root / "control"
            controls.mkdir()
            (controls / "config.json").write_text("config")
            provenance = {"evidence": {"seedEventsSha256": module.sha256(events_path),
                                       "seedJobReceiptSha256": module.sha256(seed_job_path),
                                       "finalControlFileSha256": {"config.json": module.sha256(controls / "config.json")}},
                "evidencePath": evidence_path, "continuationEvidenceSha256": module.sha256(evidence_path),
                "progressPath": progress_path, "progressSha256": module.sha256(progress_path),
                "eventsPath": events_path, "seedJobPath": seed_job_path,
                "journalPath": journal_path, "journalSha256": module.sha256(journal_path),
                "controlPath": controls}
            module.validate_unchanged_final_artifacts(provenance)
            events_path.write_text("changed")
            with self.assertRaises(module.AdapterError):
                module.validate_unchanged_final_artifacts(provenance)


if __name__ == "__main__":
    unittest.main()
