import importlib.util
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name("finalize_hydration.py")
spec = importlib.util.spec_from_file_location("finalize_hydration", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


class NumericHydrationTests(unittest.TestCase):
    def setUp(self):
        self.pv = {"spec": {"csi": {"volumeAttributes": {"LogicalSectorSize": "512"}}}}
        self.disk = {
            "id": "/subscriptions/s/resourceGroups/n/providers/Microsoft.Compute/disks/d1",
            "uniqueId": "disk-uid", "provisioningState": "Succeeded",
            "creationData": {"createOption": "Copy"}, "completionPercent": 100.0,
            "logicalSectorSize": None,
        }

    def test_copy_100_point_zero_is_complete(self):
        got = module.validate_hydration(self.disk, self.pv, self.disk["id"])
        self.assertEqual(got["completionPercent"], 100.0)
        self.assertEqual(got["sectorEvidence"], "PV CSI LogicalSectorSize")

    def test_copy_below_100_fails(self):
        self.disk["completionPercent"] = 99.9
        with self.assertRaisesRegex(module.HydrationPending, "below 100%"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])

    def test_only_succeeded_numeric_partial_copy_is_pending(self):
        self.disk["completionPercent"] = 99
        details, pending = module.observe_hydration(self.disk, self.pv, self.disk["id"])
        self.assertTrue(pending)
        self.assertEqual(details["completionPercent"], 99.0)
        self.disk["completionPercent"] = "not-a-number"
        with self.assertRaisesRegex(module.AuditError, "not numeric"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])
        self.disk["completionPercent"] = 99
        self.disk["provisioningState"] = "Updating"
        with self.assertRaisesRegex(module.AuditError, "not Succeeded"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])

    def test_missing_copy_progress_is_audit_error_not_pending(self):
        self.disk["completionPercent"] = None
        with self.assertRaisesRegex(module.AuditError, "no numeric completionPercent"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])

    def test_cli_reserves_exit_three_for_validated_hydration_pending(self):
        args = ["--stack", "50gib", "--control-flow-dir", "unused"]
        with patch.object(module, "Finalizer", side_effect=module.HydrationPending("partial copy")):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(module.main(args), 3)
        with patch.object(module, "Finalizer", side_effect=module.AuditError("bad identity")):
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(module.main(args), 2)

    def test_empty_allows_null_or_100_only(self):
        self.disk["creationData"]["createOption"] = "Empty"
        self.disk["completionPercent"] = None
        self.assertIsNone(module.validate_hydration(self.disk, self.pv, self.disk["id"])["completionPercent"])
        self.disk["completionPercent"] = 100.0
        self.assertEqual(module.validate_hydration(self.disk, self.pv, self.disk["id"])["completionPercent"], 100.0)
        self.disk["completionPercent"] = 99
        with self.assertRaisesRegex(module.AuditError, "Empty disk"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])

    def test_sector_fallback_requires_validated_pv_value(self):
        self.pv["spec"]["csi"]["volumeAttributes"]["LogicalSectorSize"] = "4096"
        with self.assertRaisesRegex(module.AuditError, "sector size"):
            module.validate_hydration(self.disk, self.pv, self.disk["id"])


class StageLineageTests(unittest.TestCase):
    def fixture(self):
        src = "/subscriptions/s/resourceGroups/src/providers/Microsoft.Compute/snapshots/keeper"
        disk = "/subscriptions/s/resourceGroups/n/providers/Microsoft.Compute/disks/stage"
        copy = "/subscriptions/s/resourceGroups/snap/providers/Microsoft.Compute/snapshots/copy"
        manifest = {"sourceSnapshotId": src, "uniqueId": "src-uid"}
        state = {"snapshotSourceStagingDiskId": disk, "snapshotSourceCopyId": copy}
        stage_disk = {"uniqueId": "disk-uid", "creationData": {"sourceResourceId": src, "sourceUniqueId": "src-uid"}}
        restore = {"uniqueId": "copy-uid", "creationData": {"sourceResourceId": disk, "sourceUniqueId": "disk-uid"}}
        stage = {"status": "ready", "sourceSnapshotId": src, "sourceSnapshotUniqueId": "src-uid",
                 "stagingDiskId": disk, "stagingDiskUniqueId": "disk-uid", "restoreSnapshotId": copy,
                 "restoreSnapshotUniqueId": "copy-uid", "stagingDiskMetadata": stage_disk,
                 "restoreSnapshotMetadata": restore}
        record = {"sourceSnapshotId": src, "ownedCopySnapshotId": copy, "stagingDiskId": disk,
                  "stageReceipt": stage, "manifest": manifest}
        return manifest, state, record, stage

    def test_exact_stage_source_and_uids_pass(self):
        module.validate_stage_lineage(*self.fixture())

    def test_changed_staging_uid_fails_closed(self):
        args = list(self.fixture())
        args[3]["restoreSnapshotMetadata"]["creationData"]["sourceUniqueId"] = "other-uid"
        with self.assertRaisesRegex(module.AuditError, "exact Standard staging disk"):
            module.validate_stage_lineage(*args)


class FixtureEvidenceTests(unittest.TestCase):
    SOURCE_HEAP = 118_128_640_000
    SOURCE_LIVE = 36_050_000

    def test_actual_50_gib_capture_is_bound_exactly(self):
        path = module.ROOT / "recovery/fixture-watch/50-finalization/live/db-hdd-pg-1.json"
        captured = module.load_json(path)
        self.assertEqual(captured["heapBytes"], 53_687_091_200)
        self.assertEqual(captured["totalBytes"], 54_436_372_480)
        expected = module.validate_tuple_estimates(
            captured["liveTuplesEstimate"], captured["deadTuplesEstimate"],
            captured["minId"], captured["maxId"], 50,
            self.SOURCE_HEAP, self.SOURCE_LIVE)
        self.assertEqual(expected, 16_384_000)
        self.assertEqual((captured["liveTuplesEstimate"], captured["deadTuplesEstimate"]),
                         (16_384_000, 16_384_000))
        self.assertEqual((captured["minId"], captured["maxId"]), (4, 65_535_998))

    def test_100_gib_source_prefix_estimates_are_exact(self):
        expected = module.validate_tuple_estimates(32_768_000, 32_768_000, 4, 131_071_998,
            100, self.SOURCE_HEAP, self.SOURCE_LIVE)
        self.assertEqual(expected, 32_768_000)

    def test_changed_stats_or_invalid_extrema_fail_closed(self):
        with self.assertRaisesRegex(module.AuditError, "exact pinned source-prefix"):
            module.validate_tuple_estimates(16_384_001, 16_384_000, 4, 65_535_998,
                50, self.SOURCE_HEAP, self.SOURCE_LIVE)
        with self.assertRaisesRegex(module.AuditError, "positive even IDs"):
            module.validate_tuple_estimates(16_384_000, 16_384_000, 3, 65_535_998,
                50, self.SOURCE_HEAP, self.SOURCE_LIVE)

    def test_prepare_receipt_uses_live_exact_estimates(self):
        fixture = {"heapBytes": 50 * module.GIB, "totalBytes": 50 * module.GIB + 1,
                   "preparedLiveTuplesEstimate": 16_384_000, "preparedDeadTuplesEstimate": 16_384_000}
        receipt = module.make_prepare("50gib", "/source", "incremental_copy", "sys", self.SOURCE_HEAP,
            self.SOURCE_HEAP + 1, 50, fixture, "0/1", "/copy", "abcd1234", "1", "/snaprg",
            "backup", "data", "wal")
        self.assertEqual(receipt["status"], "complete")
        self.assertEqual(receipt["fixtureTargetHeapGiB"], 50)
        self.assertEqual(receipt["preparedLiveTuplesEstimate"], 16_384_000)

    def test_run_config_binds_owner_location_and_assigned_zone(self):
        stack = module.ROOT / "50gib"
        config = module.load_json(stack / "run-config.json")
        state = module.load_json(stack / ".state/state.json")
        module.validate_stack_config(config, state, 50)
        changed = dict(state, location="eastus")
        with self.assertRaisesRegex(module.AuditError, "location/zone"):
            module.validate_stack_config(config, changed, 50)


class ImagePinTests(unittest.TestCase):
    def test_exact_image_matches_frozen_prepare_and_immutable_03(self):
        stack = module.ROOT / "50gib"
        config = module.load_json(stack / "run-config.json")
        script = (stack / "02-prepare.sh").read_text()
        module.validate_image_pin(script, module.sha256(stack / "03-test.py"), config)
        self.assertEqual(module.IMAGE,
            "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd")

    def test_image_or_immutable_test_script_change_fails(self):
        stack = module.ROOT / "50gib"
        config = module.load_json(stack / "run-config.json")
        script = (stack / "02-prepare.sh").read_text().replace(module.IMAGE, module.IMAGE + "x")
        with self.assertRaisesRegex(module.AuditError, "prepare script IMAGE"):
            module.validate_image_pin(script, module.TEST_SCRIPT_SHA, config)
        with self.assertRaisesRegex(module.AuditError, "03-test.py hash"):
            module.validate_image_pin((stack / "02-prepare.sh").read_text(), "changed", config)

    def test_captured_final_audit_tmp_has_required_files(self):
        capture = module.ROOT / "recovery/fixture-watch/50-finalization/active-prepare-tmp.a7vQvw"
        self.assertEqual(set(module.CONTROL_CAPTURE_FILES),
                         {name for name in module.CONTROL_CAPTURE_FILES if (capture / name).is_file()})


class SnapshotJournalTests(unittest.TestCase):
    def parse_text(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output-snapshots.jsonl"
            path.write_text(text)
            return module.load_output_snapshot_records(path)

    def test_original_jq_pretty_documents_and_compact_lines_parse_as_exact_pair(self):
        data = {"volumeSnapshot": "data-snapshot", "id": "/snapshots/data", "sourcePVC": "hdd-pg-1"}
        wal = {"volumeSnapshot": "wal-snapshot", "id": "/snapshots/wal", "sourcePVC": "hdd-pg-1-wal"}
        pretty_documents = json.dumps(data, indent=2) + "\n" + json.dumps(wal, indent=2) + "\n"
        compact_lines = json.dumps(data, separators=(",", ":")) + "\n" + json.dumps(wal, separators=(",", ":")) + "\n"
        self.assertEqual(set(self.parse_text(pretty_documents)), {"data-snapshot", "wal-snapshot"})
        self.assertEqual(set(self.parse_text(compact_lines)), {"data-snapshot", "wal-snapshot"})

    def test_snapshot_journal_rejects_trailing_junk_nonobjects_and_duplicate_names(self):
        first = {"volumeSnapshot": "data-snapshot", "id": "/snapshots/data"}
        second = {"volumeSnapshot": "wal-snapshot", "id": "/snapshots/wal"}
        with self.assertRaisesRegex(module.AuditError, "separated by whitespace|Malformed JSON"):
            self.parse_text(json.dumps(first, indent=2) + "\n" + json.dumps(second, indent=2) + " junk\n")
        with self.assertRaisesRegex(module.AuditError, "non-object"):
            self.parse_text("[1, 2]\n")
        with self.assertRaisesRegex(module.AuditError, "duplicate VolumeSnapshot"):
            self.parse_text(json.dumps(first) + "\n" + json.dumps(first) + "\n")
        with self.assertRaisesRegex(module.AuditError, "exactly two"):
            self.parse_text(json.dumps(first) + "\n")


class SeedEventTests(unittest.TestCase):
    def test_exact_pinned_completed_event_has_gc_safe_chronology(self):
        uid = "bc0de226-e948-4186-90c8-573c8b0be2ed"
        event = {"involvedObject": {"kind": "Job", "name": "hdd-pg-2-snapshot-recovery", "uid": uid},
                 "reason": "Completed", "message": "Job completed", "eventTime": "2026-10-07T02:24:47Z"}
        got = module.select_gc_safe_seed_event([event], "2026-10-07T02:13:41Z", "2026-10-07T02:25:14Z", uid)
        self.assertEqual(got["involvedObject"]["uid"], uid)

    def test_changed_uid_or_out_of_order_event_fails(self):
        uid = "bc0de226-e948-4186-90c8-573c8b0be2ed"
        event = {"involvedObject": {"kind": "Job", "name": "hdd-pg-2-snapshot-recovery", "uid": "changed"},
                 "reason": "Completed", "message": "Job completed", "eventTime": "2026-10-07T02:24:47Z"}
        with self.assertRaisesRegex(module.AuditError, "pinned Completed Event"):
            module.select_gc_safe_seed_event([event], "2026-10-07T02:13:41Z", "2026-10-07T02:25:14Z", uid)
        event["involvedObject"]["uid"] = uid
        with self.assertRaisesRegex(module.AuditError, "pinned Completed Event"):
            module.select_gc_safe_seed_event([event], "2026-10-07T02:25:00Z", "2026-10-07T02:25:14Z", uid)

    def test_hash_pinned_archived_event_capture_passes_and_tampering_fails(self):
        path = module.ROOT / "recovery/fixture-watch/50-finalization/live/events.json"
        expected_sha = "4e96d8a122c0f8adc2308d76f45e457d4ad8fd2681d78accd81c5f8957c0aa4f"
        uid = "bc0de226-e948-4186-90c8-573c8b0be2ed"
        event, actual_sha = module.read_pinned_archived_seed_event(
            path, expected_sha, "2026-10-07T02:13:41Z", "2026-10-07T02:25:14Z", uid)
        self.assertEqual(actual_sha, expected_sha)
        self.assertEqual(event["involvedObject"]["uid"], uid)
        with self.assertRaisesRegex(module.AuditError, "SHA-256 differs"):
            module.read_pinned_archived_seed_event(
                path, "0" * 64, "2026-10-07T02:13:41Z", "2026-10-07T02:25:14Z", uid)
        with self.assertRaisesRegex(module.AuditError, "pinned Completed Event"):
            module.read_pinned_archived_seed_event(
                path, expected_sha, "2026-10-07T02:13:41Z", "2026-10-07T02:25:14Z",
                "00000000-0000-0000-0000-000000000000")

    def test_replica_pvcs_and_pod_must_match_captured_uid_and_time(self):
        pvc_items = [{"metadata": {"name": "hdd-pg-2", "uid": "data-uid",
                                   "creationTimestamp": "2026-10-07T02:13:41Z"}},
                     {"metadata": {"name": "hdd-pg-2-wal", "uid": "wal-uid",
                                   "creationTimestamp": "2026-10-07T02:13:41Z"}}]
        captured_pvcs = {"items": [dict(x) for x in pvc_items]}
        identities, latest = module.unchanged_replica_pvc_identities(
            {"items": pvc_items}, captured_pvcs)
        self.assertEqual(latest, "2026-10-07T02:13:41Z")
        self.assertEqual({x["uid"] for x in identities}, {"data-uid", "wal-uid"})
        changed = {"items": [dict(x) for x in pvc_items]}
        changed["items"][0] = {"metadata": dict(changed["items"][0]["metadata"], uid="replacement")}
        with self.assertRaisesRegex(module.AuditError, "PVC hdd-pg-2 UID/creation time"):
            module.unchanged_replica_pvc_identities(changed, captured_pvcs)
        pod = {"items": [{"metadata": {"name": "hdd-pg-2", "uid": "pod-uid",
                                        "creationTimestamp": "2026-10-07T02:25:14Z"}}]}
        identity = module.unchanged_replica_pod_identity(pod, pod)
        self.assertEqual(identity["uid"], "pod-uid")
        changed_pod = {"items": [{"metadata": dict(pod["items"][0]["metadata"], uid="replacement")}]}
        with self.assertRaisesRegex(module.AuditError, "pod UID/creation time"):
            module.unchanged_replica_pod_identity(changed_pod, pod)


if __name__ == "__main__":
    unittest.main()
