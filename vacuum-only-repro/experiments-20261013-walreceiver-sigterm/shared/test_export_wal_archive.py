#!/usr/bin/env python3
"""Offline path, ETag, checksum, and redaction checks for WAL export."""

from __future__ import annotations

import importlib.util
import hashlib
import io
import json
import subprocess
import tarfile
import sys
import tempfile
import types
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).with_name("export-wal-archive.py")
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("export_wal_archive", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExportWalArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.token = "0123456789abcdef0123456789abcdef"
        self.prefix = MODULE.archive_prefix(self.token, "hdd-pg")

    def test_only_owned_server_wal_paths_are_accepted(self) -> None:
        allowed = self.prefix + "wals/0000000200000057/000000020000005700000009"
        self.assertEqual(MODULE.relative_blob_name(allowed, self.prefix),
                         "wals/0000000200000057/000000020000005700000009")
        for bad in (
            "cnpg-wal-archive/ffffffffffffffffffffffffffffffff/hdd-pg/wals/seg",
            self.prefix + "base/backup/backup.info",
            self.prefix + "wals/../other",
            self.prefix + "wals/./other",
            self.prefix + "wals//other",
            self.prefix + "wals/..\\other",
            self.prefix + "/absolute",
        ):
            with self.subTest(bad=bad), self.assertRaises(MODULE.Refusal):
                MODULE.relative_blob_name(bad, self.prefix)

    def test_inventory_must_be_monotonic_by_name_etag_size_and_time(self) -> None:
        old = [{"name": "a", "etag": '"1"', "bytes": 4,
                "lastModified": "2026-10-13T00:00:00+00:00"}]
        same_and_new = old + [{"name": "b", "etag": '"2"', "bytes": 9,
                              "lastModified": "2026-10-13T00:01:00+00:00"}]
        MODULE.compare_monotonic(old, same_and_new)
        for changed in (
            [{**old[0], "etag": '"changed"'}],
            [{**old[0], "bytes": 5}],
            [{**old[0], "lastModified": "2026-10-13T00:02:00+00:00"}],
            [],
        ):
            with self.subTest(changed=changed), self.assertRaises(MODULE.Refusal):
                MODULE.compare_monotonic(old, changed)

    @unittest.skipIf(MODULE.BlobServiceClient is None, "Azure Blob SDK is optional for offline-only tests")
    def test_installed_blob_sdk_accepts_page_size_on_list_call(self) -> None:
        # Construct the real SDK client without making a request. This checks
        # the SDK's ItemPaged.by_page contract instead of a permissive fake.
        service = MODULE.BlobServiceClient(
            account_url="https://example.blob.core.windows.net", credential="offline-test"
        )
        container = service.get_container_client("wal-test-container")
        paged = container.list_blobs(name_starts_with=self.prefix, results_per_page=500)
        pages = paged.by_page()
        self.assertEqual(pages.results_per_page, 500)
        self.assertEqual(pages.prefix, self.prefix)

    @unittest.skipIf(MODULE.BlobServiceClient is None, "Azure Blob SDK is optional for offline-only tests")
    def test_installed_container_properties_expose_attributes(self) -> None:
        from azure.storage.blob._models import ContainerProperties

        properties = ContainerProperties()
        properties.name = "wal-test-container"
        properties.metadata = {"reprotoken": self.token}
        properties.public_access = None
        self.assertEqual(properties.name, "wal-test-container")
        self.assertEqual(properties.metadata["reprotoken"], self.token)
        self.assertIsNone(properties.public_access)

    def test_local_path_rejects_traversal_and_symlink_components(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            root.mkdir()
            with self.assertRaises(MODULE.Refusal):
                MODULE.local_file_for(root, "wals/../../outside")
            with self.assertRaises(MODULE.Refusal):
                MODULE.local_file_for(root, "wals//segment")
            (root / "wals").symlink_to(Path(temporary), target_is_directory=True)
            with self.assertRaises(MODULE.Refusal):
                MODULE.local_file_for(root, "wals/segment")

    def test_local_inventory_checks_exact_bytes_hashes_and_no_extra_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            path = MODULE.local_file_for(root, "wals/0000000200000057/segment", create_parents=True)
            path.write_bytes(b"verified WAL")
            item = {"localRelativePath": "wals/0000000200000057/segment", "bytes": 12,
                    "sha256": MODULE.sha256_file(path)}
            MODULE.verify_local_inventory(root, [item], 12)
            path.write_bytes(b"tampered WAL")
            with self.assertRaises(MODULE.Refusal):
                MODULE.verify_local_inventory(root, [item], 12)
            path.write_bytes(b"verified WAL")
            (root / "extra").write_bytes(b"unmanifested")
            with self.assertRaises(MODULE.Refusal):
                MODULE.verify_local_inventory(root, [item], 12)

    @unittest.skipIf(MODULE.MatchConditions is None, "Azure Blob SDK is optional for offline-only tests")
    def test_download_pins_etag_and_writes_hash_receipt(self) -> None:
        class Downloader:
            def chunks(self):
                yield b"wal"

        class Properties:
            etag = '"tag"'
            size = 3

        class Blob:
            def __init__(self):
                self.download_args = None

            def download_blob(self, **kwargs):
                self.download_args = kwargs
                return Downloader()

            def get_blob_properties(self):
                return Properties()

        class Container:
            def __init__(self):
                self.blob = Blob()

            def get_blob_client(self, name):
                self.name = name
                return self.blob

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ctx = {"runDir": root / "run", "progressDir": root / "progress",
                   "ownerToken": self.token, "stack": "sigterm"}
            ctx["progressDir"].mkdir()
            container = Container()
            name = self.prefix + "wals/0000000200000057/segment"
            item = {"name": name, "relativeName": "wals/0000000200000057/segment",
                    "etag": "tag", "bytes": 3, "lastModified": "2026-10-13T00:00:00+00:00"}
            record = MODULE.download_one(container, ctx, item, 2)
            self.assertEqual(record["sha256"], MODULE.hashlib.sha256(b"wal").hexdigest())
            self.assertEqual(container.blob.download_args["etag"], '"tag"')
            self.assertEqual(container.blob.download_args["match_condition"], MODULE.MatchConditions.IfNotModified)
            self.assertEqual(MODULE.verified_progress(ctx, item), record)

    @unittest.skipIf(MODULE.BlobServiceClient is None, "Azure Blob SDK is optional for offline-only tests")
    def test_etag_comparison_normalizes_sdk_quote_representation_only(self) -> None:
        from azure.storage.blob._models import BlobProperties

        sdk = BlobProperties()
        sdk.etag = '"0x8DF0123456789AB"'
        self.assertTrue(MODULE.etags_match(sdk.etag, "0x8DF0123456789AB"))
        self.assertFalse(MODULE.etags_match(sdk.etag, "0x8DF0123456789AC"))
        self.assertEqual(MODULE.azure_etag("0x8DF0123456789AB"), sdk.etag)
        self.assertEqual(MODULE.azure_etag(sdk.etag), sdk.etag)

    def test_public_inventory_omits_secret_and_owner_identifiers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            token = "0123456789abcdef0123456789abcdef"
            account = "cnpgwal0123456789abcdef0"
            container = "wal-0123456789ab"
            public_path = Path(temporary) / "public.json"
            ctx = {"publicReceiptPath": public_path, "stack": "sigterm", "serverName": "hdd-pg",
                   "pairSha256": "a" * 64}
            objects = [{"relativeName": "wals/seg", "bytes": 3, "sha256": "b" * 64,
                        "etag": '"etag"', "lastModified": datetime.now(timezone.utc).isoformat(),
                        "contentMd5Base64": None}]
            MODULE.save_public_receipt(ctx, objects, 3, 1)
            raw = public_path.read_text()
            self.assertNotIn(token, raw)
            self.assertNotIn(account, raw)
            self.assertNotIn(container, raw)
            self.assertNotIn("AccountKey", raw)
            public = json.loads(raw)
        self.assertEqual(public["objects"][0]["sha256"], "b" * 64)
        self.assertEqual(public["objects"][0]["path"], "wals/seg")

    def test_remote_bundle_round_trip_verifies_manifest_and_original_hashes(self) -> None:
        payloads = {
            "wals/0000000200000057/000000020000005700000001": b"wal-one" * 100,
            "wals/0000000200000057/000000020000005700000002": b"wal-two" * 80,
        }
        items = []
        records = []
        for index, (relative, payload) in enumerate(payloads.items(), 1):
            name = self.prefix + relative
            item = {"name": name, "relativeName": relative, "etag": f'"etag-{index}"',
                    "bytes": len(payload), "lastModified": "2026-10-13T10:00:00+00:00"}
            items.append(item)
            records.append({"name": name, "relativeName": relative, "etag": item["etag"],
                            "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()})
        response = {"ownerToken": self.token, "stack": "sigterm", "files": records}
        manifest = {"version": 1, "ownerToken": self.token, "stack": "sigterm", "files": records}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = root / "batch.tar.gz"
            with tarfile.open(bundle, "w:gz", compresslevel=1) as archive:
                for relative, payload in payloads.items():
                    info = tarfile.TarInfo(relative); info.size = len(payload)
                    archive.addfile(info, io.BytesIO(payload))
                raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                info = tarfile.TarInfo("_wal-export-manifest.json"); info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))
            destination = root / "unpacked"
            destination.mkdir()
            saved = MODULE.extract_verified_remote_bundle(bundle, None, items, response,
                destination, save_progress=False)
            saved_by_name = {row["name"]: row["sha256"] for row in saved}
            self.assertEqual({item["relativeName"]: saved_by_name[item["name"]] for item in items},
                {relative: hashlib.sha256(payload).hexdigest() for relative, payload in payloads.items()})
            for relative, payload in payloads.items():
                self.assertEqual((destination / relative).read_bytes(), payload)

    def test_remote_bundle_rejects_traversal_and_manifest_etag_mismatch(self) -> None:
        relative = "wals/0000000200000057/000000020000005700000001"
        payload = b"original wal"
        item = {"name": self.prefix + relative, "relativeName": relative, "etag": '"e1"',
                "bytes": len(payload), "lastModified": "2026-10-13T10:00:00+00:00"}
        record = {"name": item["name"], "relativeName": relative, "etag": item["etag"],
                  "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
        response = {"ownerToken": self.token, "stack": "control", "files": [record]}
        manifest = {"version": 1, "ownerToken": self.token, "stack": "control", "files": [record]}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); bundle = root / "bad.tar.gz"
            with tarfile.open(bundle, "w:gz") as archive:
                info = tarfile.TarInfo("../../outside"); info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
                raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                info = tarfile.TarInfo("_wal-export-manifest.json"); info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))
            destination = root / "unpacked"; destination.mkdir()
            with self.assertRaises(MODULE.Refusal):
                MODULE.extract_verified_remote_bundle(bundle, None, [item], response, destination, save_progress=False)
            self.assertFalse((root / "outside").exists())

            with tarfile.open(bundle, "w:gz") as archive:
                info = tarfile.TarInfo(relative); info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
                raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                info = tarfile.TarInfo("_wal-export-manifest.json"); info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))
            changed_item = {**item, "etag": '"changed"'}
            with self.assertRaises(MODULE.Refusal):
                MODULE.extract_verified_remote_bundle(bundle, None, [changed_item], response, destination, save_progress=False)

    def test_remote_connection_string_is_stdin_only(self) -> None:
        secret = "AccountName=cnpgwal0123456789abcdef0;AccountKey=example-private-key"
        ctx = {"stateDir": Path("/owned/state")}
        completed = __import__("subprocess").CompletedProcess([], 0, b'{"status":"ready"}', b"")
        with mock.patch.object(MODULE, "kubectl_exec", return_value=completed) as remote:
            MODULE.remote_json(ctx, "hdd-pg-1", {"connectionString": secret, "operation": "stage"})
        argv = remote.call_args.args[2]
        stdin = remote.call_args.kwargs["input_bytes"]
        self.assertNotIn(secret, " ".join(argv))
        self.assertNotIn(secret, MODULE.REMOTE_STAGE_SCRIPT)
        self.assertIn(secret.encode(), stdin)

    def test_kubectl_exec_enables_stdin_tty_only_when_bytes_are_forwarded(self) -> None:
        completed = __import__("subprocess").CompletedProcess([], 0, b"", b"")
        ctx = {"stateDir": Path("/owned/state")}
        with mock.patch.object(MODULE.subprocess, "run", return_value=completed) as run:
            MODULE.kubectl_exec(ctx, "hdd-pg-1", ["python3", "-c", "read"], input_bytes=b"payload")
            interactive = run.call_args
            MODULE.kubectl_exec(ctx, "hdd-pg-1", ["true"])
            noninteractive = run.call_args
        self.assertIn("-i", interactive.args[0])
        self.assertEqual(interactive.kwargs["input"], b"payload")
        self.assertNotIn("-i", noninteractive.args[0])
        self.assertIsNone(noninteractive.kwargs["input"])

    def test_remote_stage_program_is_syntax_valid_and_pins_exact_inventory_blobs(self) -> None:
        compile(MODULE.REMOTE_STAGE_SCRIPT, "remote-wal-stage.py", "exec")
        self.assertIn("importlib.util, io, json", MODULE.REMOTE_STAGE_SCRIPT)
        self.assertIn("name != prefix + relative", MODULE.REMOTE_STAGE_SCRIPT)
        self.assertIn("download_blob(etag=azure_etag(etag), match_condition=MatchConditions.IfNotModified", MODULE.REMOTE_STAGE_SCRIPT)
        self.assertIn("canonical_etag(before.etag) != canonical_etag(etag)", MODULE.REMOTE_STAGE_SCRIPT)
        self.assertGreaterEqual(MODULE.REMOTE_STAGE_SCRIPT.count("get_blob_properties()"), 2)

    def test_remote_guard_errors_report_only_a_source_line(self) -> None:
        request = json.dumps({"operation": "preflight", "ownerToken": "invalid-secret-marker",
                              "stack": "sigterm"}).encode()
        result = subprocess.run([sys.executable, "-c", MODULE.REMOTE_STAGE_SCRIPT], input=request,
                                capture_output=True, check=False)
        self.assertEqual(result.returncode, 2)
        error = result.stderr.decode("utf-8")
        self.assertRegex(error, r"remote WAL staging failed \(GuardFailure, guard line [0-9]+\)")
        self.assertNotIn("invalid-secret-marker", error)

    def test_remote_stage_lifecycle_keeps_success_until_cleanup_and_removes_failed_stage(self) -> None:
        token = self.token
        stage_id = "a" * 32
        relative = "wals/0000000200000057/000000020000005700000001"
        prefix = self.prefix
        payload = b"offline-fake-wal-payload"
        account_name = "cnpgwal" + token[:17]
        container_name = "wal-" + token[:12]

        class FakeDownloader:
            def chunks(self):
                yield payload

        class FakeBlob:
            def get_blob_properties(self):
                return types.SimpleNamespace(etag='"etag-one"', size=len(payload))

            def download_blob(self, *, etag, match_condition, max_concurrency):
                if etag != '"etag-one"' or match_condition != "IfNotModified" or max_concurrency != 1:
                    raise AssertionError("remote stage did not use the pinned conditional download")
                return FakeDownloader()

        class FakeContainer:
            def get_blob_client(self, name):
                if name != prefix + relative:
                    raise AssertionError("remote stage requested an unexpected blob")
                return FakeBlob()

        class FakeService:
            def __init__(self):
                self.account_name = account_name

            def get_container_client(self, name):
                if name != container_name:
                    raise AssertionError("remote stage requested an unexpected container")
                return FakeContainer()

        class FakeBlobServiceClient:
            @staticmethod
            def from_connection_string(connection, **kwargs):
                if "AccountName=" not in connection or not kwargs:
                    raise AssertionError("remote stage SDK was not initialized from stdin credentials")
                return FakeService()

        azure = types.ModuleType("azure"); azure.__path__ = []
        core = types.ModuleType("azure.core"); core.MatchConditions = types.SimpleNamespace(IfNotModified="IfNotModified")
        storage = types.ModuleType("azure.storage"); storage.__path__ = []
        blob_module = types.ModuleType("azure.storage.blob"); blob_module.BlobServiceClient = FakeBlobServiceClient
        azure.core = core; azure.storage = storage; storage.blob = blob_module
        fake_sdk = {"azure": azure, "azure.core": core, "azure.storage": storage,
                    "azure.storage.blob": blob_module}

        def invoke(request: dict[str, object], scratch: Path) -> tuple[int, str, str]:
            source = MODULE.REMOTE_STAGE_SCRIPT.replace('"/controller/tmp"', repr(str(scratch)))
            stdout = io.StringIO(); stderr = io.StringIO()
            namespace = {"__name__": "__main__"}
            with mock.patch.object(sys, "stdin", io.StringIO(json.dumps(request))), \
                    mock.patch.object(sys, "stdout", stdout), mock.patch.object(sys, "stderr", stderr), \
                    mock.patch.dict(sys.modules, fake_sdk):
                try:
                    exec(compile(source, "remote-wal-stage.py", "exec"), namespace)
                    code = 0
                except SystemExit as exc:
                    code = int(exc.code or 0)
            return code, stdout.getvalue(), stderr.getvalue()

        with tempfile.TemporaryDirectory() as temporary:
            scratch = Path(temporary)
            stage_request = {"operation": "stage", "ownerToken": token, "stack": "sigterm",
                "stageId": stage_id, "accountName": account_name, "containerName": container_name,
                "connectionString": f"AccountName={account_name};AccountKey=fake-test-key",
                "items": [{"name": prefix + relative, "relativeName": relative,
                           "etag": "etag-one", "bytes": len(payload)}]}
            code, stdout, stderr = invoke(stage_request, scratch)
            self.assertEqual((code, stderr), (0, ""))
            response = json.loads(stdout)
            self.assertEqual(response["status"], "staged")
            stage = scratch / f"wal-export-{token}-sigterm-{stage_id}"
            self.assertTrue((stage / ".owner.json").is_file())
            bundle = stage / "batch.tar.gz"
            self.assertTrue(bundle.is_file())
            self.assertFalse((stage / "raw").exists())
            self.assertEqual(hashlib.sha256(bundle.read_bytes()).hexdigest(), response["bundleSha256"])

            cleanup = {key: stage_request[key] for key in ("ownerToken", "stack", "stageId")}
            cleanup["operation"] = "cleanup"
            code, stdout, stderr = invoke(cleanup, scratch)
            self.assertEqual((code, stderr), (0, ""))
            self.assertEqual(json.loads(stdout)["status"], "removed")
            self.assertFalse(stage.exists())

            failed_id = "b" * 32
            failed = {**cleanup, "operation": "stage", "stageId": failed_id, "items": []}
            code, stdout, stderr = invoke(failed, scratch)
            self.assertEqual(code, 2)
            self.assertEqual(stdout, "")
            self.assertRegex(stderr, r"GuardFailure, guard line [0-9]+")
            self.assertFalse((scratch / f"wal-export-{token}-sigterm-{failed_id}").exists())

    def test_remote_batch_failure_writes_redacted_private_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            ctx = {"stack": "sigterm", "stateDir": state}
            item = {"name": "private-wal-name", "etag": "private-etag", "bytes": 16}
            problem = MODULE.Refusal("Remote transfer failed AccountKey=private-key https://private.example/path")
            with mock.patch.object(MODULE, "_remote_batch_transfer", side_effect=problem), \
                    self.assertRaises(MODULE.Refusal):
                MODULE.remote_batch_transfer(ctx, "primary-pod", "AccountKey=not-persisted", [item], 1)
            path = state / "wal-archive-export-remote-batch-failures.jsonl"
            receipt = json.loads(path.read_text().splitlines()[0])
            self.assertEqual(receipt["errorType"], "Refusal")
            self.assertIn("<redacted>", receipt["error"])
            self.assertIn("<url-redacted>", receipt["error"])
            serialized = path.read_text()
            for secret in ("private-key", "private.example", "not-persisted", "private-wal-name", "private-etag"):
                self.assertNotIn(secret, serialized)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_remote_short_chunk_writes_sanitized_range_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ctx = {"ownerToken": self.token, "stack": "sigterm", "stateDir": Path(temporary)}
            stage_id = "c" * 32
            completed = subprocess.CompletedProcess([], 0, b"", b"AccountKey=secret-data")
            with mock.patch.object(MODULE, "kubectl_exec", return_value=completed), \
                    self.assertRaises(RuntimeError):
                MODULE.remote_bundle_chunk(ctx, "primary-pod", stage_id, 0, 2 * 1024 * 1024)
            path = Path(temporary) / "wal-archive-export-chunk-failures.jsonl"
            receipt = json.loads(path.read_text().splitlines()[0])
            self.assertEqual(receipt["stageId"], stage_id)
            self.assertEqual(receipt["offsetBytes"], 0)
            self.assertEqual(receipt["expectedBytes"], 2 * 1024 * 1024)
            self.assertEqual(receipt["actualBytes"], 0)
            self.assertEqual(receipt["returnCode"], 0)
            self.assertEqual(receipt["stderrDisposition"], "present-redacted")
            self.assertNotIn("secret-data", path.read_text())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_fail_fast_window_stops_later_submissions_and_preserves_completed_progress(self) -> None:
        progress = []
        submitted = []

        def task(job):
            if job == "failed":
                raise MODULE.Refusal("simulated safe batch refusal")
            return {"name": job}

        with MODULE.ThreadPoolExecutor(max_workers=1) as pool:
            def submit(job):
                submitted.append(job)
                return pool.submit(task, job)

            def consume(record):
                progress.append(record["name"])

            with mock.patch("sys.stderr", io.StringIO()) as stderr:
                with self.assertRaises(MODULE.Refusal):
                    MODULE.run_bounded_futures(pool, ["already-verified", "failed", "never-submitted"],
                        1, submit, consume, "sigterm", "remote WAL batch")
        self.assertEqual(submitted, ["already-verified", "failed"])
        self.assertEqual(progress, ["already-verified"])
        self.assertIn("cancelled 0 queued tasks", stderr.getvalue())

    def test_only_owned_run_directory_partial_downloads_are_pruned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            parent = root / "wals" / "0000000200000057"
            parent.mkdir(parents=True)
            partial = parent / ".wal-export-ab12.partial"
            partial.write_bytes(b"incomplete")
            item = {"relativeName": "wals/0000000200000057/000000020000005700000001"}
            ctx = {"runDir": root}
            MODULE.check_no_unexpected_local_files(ctx, [item])
            self.assertFalse(partial.exists())
            unrelated = root / ".wal-export-ab12.partial"
            unrelated.write_bytes(b"leave me")
            with self.assertRaises(MODULE.Refusal):
                MODULE.check_no_unexpected_local_files(ctx, [item])
            self.assertTrue(unrelated.exists())


if __name__ == "__main__":
    unittest.main()
