from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.dont_write_bytecode = True
from verify_delete_capture import (
    EvidenceError,
    supplement_audit,
    verify_original_100gib_delete,
)


EVIDENCE = Path(__file__).resolve().parent
GENERIC = (
    "Original INSERT/DELETE psql_long output was command-substituted and then removed; "
    "raw DML transcripts are unavailable."
)
ADAPTER_PATH = EVIDENCE.parents[1] / "backup-wait-resume" / "finalize_continuation.py"


def copy_evidence(root: Path) -> Path:
    target = root / "capture"
    target.mkdir(mode=0o700)
    for name in ("initial-20261007T051136Z", "final-20261007T051507Z"):
        shutil.copytree(EVIDENCE / name, target / name, copy_function=shutil.copy2)
    return target


def load_continuation_adapter():
    spec = importlib.util.spec_from_file_location("continuation_adapter_for_capture_test", ADAPTER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load continuation adapter")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class VerifyCapturedDeleteTests(unittest.TestCase):
    def test_valid_capture_proves_delete_but_not_insert(self) -> None:
        proof = verify_original_100gib_delete()
        self.assertEqual(proof["verification"], "complete")
        self.assertEqual(proof["delete"]["rows"], 32768000)
        self.assertTrue(proof["delete"]["captured"])
        self.assertEqual(proof["finalCapture"]["psqlExitStatus"], 0)
        self.assertFalse(proof["insert"]["captured"])
        self.assertEqual(proof["pod"]["uid"], "405550c1-241d-4bb2-81ea-30ec649ea373")

    def test_corrupt_raw_output_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = copy_evidence(Path(temporary))
            output = root / "final-20261007T051507Z" / "out"
            output.write_bytes(output.read_bytes().replace(b"32768000", b"32768001"))
            with self.assertRaises(EvidenceError):
                verify_original_100gib_delete(root)

    def test_corrupt_status_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = copy_evidence(Path(temporary))
            (root / "final-20261007T051507Z" / "status").write_bytes(b"1\n")
            with self.assertRaises(EvidenceError):
                verify_original_100gib_delete(root)

    def test_changed_manifest_linkage_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = copy_evidence(Path(temporary))
            manifest = root / "final-20261007T051507Z" / "capture-manifest.json"
            value = json.loads(manifest.read_text())
            value["initialCaptureManifestSha256"] = "0" * 64
            manifest.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
            with self.assertRaises(EvidenceError):
                verify_original_100gib_delete(root)

    def test_supplement_only_refines_generic_dml_limitation(self) -> None:
        suffix = (
            " The frozen script and finalization capture prove control flow passed the original "
            "equality guards; live tuple estimates remain estimates."
        )
        audit = {
            "stack": "100gib",
            "prepareReceipt": "external/prepare.json",
            "limitations": [GENERIC + suffix, "another factual limitation"],
            "checks": {"postgres": {"rawInsertDeleteOutputCaptured": False, "replicaCaughtUp": True}},
        }
        supplemented = supplement_audit(audit)
        self.assertEqual(audit["limitations"][0], GENERIC + suffix)
        self.assertEqual(supplemented["prepareReceipt"], "external/prepare.json")
        self.assertEqual(supplemented["limitations"][1], "another factual limitation")
        self.assertTrue(supplemented["checks"]["postgres"]["originalDeleteCheckpointOutputCaptured"])
        self.assertFalse(supplemented["checks"]["postgres"]["originalInsertOutputCaptured"])
        self.assertFalse(supplemented["checks"]["postgres"]["rawInsertDeleteOutputCaptured"])
        self.assertEqual(supplemented["checks"]["postgres"]["replicaCaughtUp"], True)
        self.assertEqual(supplemented["checks"]["original100GiBFixtureDmlCapture"]["delete"]["rows"], 32768000)
        self.assertIn("its raw transcript is unavailable", supplemented["limitations"][0])
        self.assertIn("32,768,000 rows", supplemented["limitations"][0])
        self.assertIn("live tuple estimates remain estimates", supplemented["limitations"][0])

    def test_supplement_refines_actual_continuation_adapter_output(self) -> None:
        adapter = load_continuation_adapter()
        with tempfile.TemporaryDirectory(dir=adapter.RECOVERY) as temporary:
            root = Path(temporary)
            evidence_path = root / "continuation-evidence.json"
            evidence_path.write_text("{}\n")
            progress_path = root / "resume-progress.json"
            progress_path.write_text("{}\n")
            journal_path = root / "output-snapshots.jsonl"
            journal_path.write_text("{}\n")
            events_path = root / "seed-events.json"
            events_path.write_text("{}\n")
            provenance = {
                "evidencePath": evidence_path,
                "progressPath": progress_path,
                "progressSha256": hashlib.sha256(progress_path.read_bytes()).hexdigest(),
                "journalPath": journal_path,
                "journalSha256": hashlib.sha256(journal_path.read_bytes()).hexdigest(),
                "eventsPath": events_path,
                "terminal": {"status": "failed"},
                "evidence": {
                    "preBackupCapturePath": "recovery/backup-wait-resume/original/prebackup-tmp",
                    "preBackupCaptureFileSha256": {
                        "backup.yaml": "backup-hash", "cluster.yaml": "cluster-hash", "seed-path": "seed-hash"
                    },
                    "preBackupCaptureReceiptPath": "recovery/backup-wait-resume/original/prebackup-capture.json",
                    "preBackupCaptureReceiptSha256": "capture-receipt-hash",
                    "seedEventsSha256": "events-hash",
                    "seedValidation": {"jobUID": "seed-job-uid", "completedEvent": {"reason": "Completed"}},
                    "backup": {"uid": "backup-uid"},
                },
            }
            audit = {
                "stack": "100gib",
                "prepareReceipt": "external/prepare.json",
                "limitations": [
                    GENERIC + " The frozen script and finalization capture prove control flow passed the original "
                    "equality guards; live pg_stat_all_tables estimates are required to equal ceil(targetBytes * "
                    "sourceLiveEstimate / sourceHeapBytes)."
                ],
                "checks": {
                    "originalFinalizationCapture": {"path": "captured-control"},
                    "backupAndReplicaSeed": {"seedJob": {"uid": "seed-job-uid"}},
                    "postgres": {"rawInsertDeleteOutputCaptured": False, "tupleCountsAreEstimates": True},
                },
            }
            continuation_audit = adapter.corrected_attribution(audit, provenance)

        self.assertTrue(any("The frozen original script and captured pre-Backup TMP indicate" in item
                            for item in continuation_audit["limitations"]))
        estimate_caveat = (
            "live pg_stat_all_tables estimates are required to equal ceil(targetBytes * "
            "sourceLiveEstimate / sourceHeapBytes)."
        )
        self.assertFalse(any(estimate_caveat in item for item in continuation_audit["limitations"]))
        supplemented = supplement_audit(continuation_audit)
        limitations = supplemented["limitations"]
        self.assertFalse(any("raw INSERT/DELETE output is unavailable" in item for item in limitations))
        self.assertTrue(any("raw INSERT output remains unavailable" in item for item in limitations))
        self.assertTrue(any("DELETE output and CHECKPOINT were captured" in item for item in limitations))
        self.assertTrue(any("corroborating control-flow evidence" in item for item in limitations))
        self.assertIn("Original 02 did not reach the final four-disk hydration loop; later tail/final-capture evidence is attributed to the pinned continuation.", limitations)
        self.assertIn("The local receipt chain is recorded corroborating evidence, not a signed execution attestation.", limitations)
        self.assertTrue(any(estimate_caveat in item for item in limitations))
        self.assertEqual(supplemented["prepareReceipt"], "external/prepare.json")
        self.assertIn("originalPreBackupCapture", supplemented["checks"])
        self.assertIn("continuationFinalizationCapture", supplemented["checks"])
        self.assertEqual(supplemented["checks"]["original100GiBFixtureDmlCapture"]["delete"]["rows"], 32768000)


if __name__ == "__main__":
    unittest.main()
