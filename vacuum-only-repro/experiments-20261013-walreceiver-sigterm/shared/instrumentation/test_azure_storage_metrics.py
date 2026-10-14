from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from azure_metrics import AzureMetrics, STORAGE_BLOB_METRICS  # noqa: E402
from collect import archive_receipt_path_for  # noqa: E402


ACCOUNT = ("/subscriptions/11111111-2222-3333-4444-555555555555/resourceGroups/"
           "rg-owned/providers/Microsoft.Storage/storageAccounts/cnpgwalabc123")


class FakeBlobAzure(AzureMetrics):
    def __init__(self, *args, omit_response_type: bool = False, **kwargs):
        self.commands = []
        self.omit_response_type = omit_response_type
        super().__init__(*args, **kwargs)

    def _call_json(self, argv):
        self.commands.append(argv)
        if "list-definitions" in argv:
            rows = []
            for metric in STORAGE_BLOB_METRICS:
                dimensions = ([{"value": "ApiName"}, {"value": "ResponseType"}]
                              if metric == "Transactions" else [{"value": "GeoType"}])
                rows.append({"name": {"value": metric}, "namespace": "Microsoft.Storage/storageAccounts/blobServices",
                             "supportedAggregationTypes": ["Average", "Total"],
                             "primaryAggregationType": "Average", "dimensions": dimensions})
            return rows, 0, "", json.dumps(rows)
        metric = argv[argv.index("--metric") + 1]
        values = []
        for metric_name in metric.split(","):
            if metric_name == "Transactions":
                dims = [{"name": {"value": "ApiName"}, "value": "PutBlob"}]
                if not self.omit_response_type:
                    dims.append({"name": {"value": "ResponseType"}, "value": "Success"})
            else:
                dims = [{"name": {"value": "GeoType"}, "value": "Primary"}]
            values.append({"name": {"value": metric_name}, "timeseries": [{"metadatavalues": dims,
                "data": [{"timeStamp": "2026-10-13T01:00:00Z", "total": 7, "average": 7.0}]}]})
        response = {"interval": "PT1M", "value": values}
        return response, 0, "", json.dumps(response)


class AzureStorageMetricTests(unittest.TestCase):
    def make_collector(self, directory: Path, omit_response_type: bool = False):
        return FakeBlobAzure({}, directory, subscription="11111111-2222-3333-4444-555555555555",
                             archive_receipt={"status": "ready", "accountId": ACCOUNT},
                             omit_response_type=omit_response_type)

    def test_targets_exact_blob_resource_and_preserves_transactions_dimensions(self):
        with tempfile.TemporaryDirectory() as temporary:
            collector = self.make_collector(Path(temporary))
            inventory = collector.discover()
            self.assertEqual(len(inventory["targets"]), 1)
            target_id = ACCOUNT + "/blobServices/default"
            self.assertEqual(inventory["targets"][0]["resourceId"], target_id)
            self.assertEqual(inventory["targets"][0]["availableMetrics"], list(STORAGE_BLOB_METRICS))
            poll = collector.poll()
            self.assertEqual(poll["targets"][0]["kind"], "blob-service")
            self.assertEqual(len(collector.commands), 1 + len(STORAGE_BLOB_METRICS))
            rows = [json.loads(line) for line in (Path(temporary) / "azure-metrics.jsonl").read_text().splitlines()]
            transactions = next(row for row in rows if row["metric"] == "Transactions")
            self.assertEqual(transactions["dimensions"], {"apiname": "PutBlob", "responsetype": "Success"})
            self.assertEqual(transactions["nativeDimensions"], [
                {"name": "ApiName", "value": "PutBlob"},
                {"name": "ResponseType", "value": "Success"},
            ])
            self.assertEqual(transactions["aggregation"], "Total")
            self.assertEqual(transactions["resourceId"], target_id)
            command = next(argv for argv in collector.commands if "--metric" in argv and
                           argv[argv.index("--metric") + 1] == "Transactions")
            dimension_index = command.index("--dimension")
            self.assertEqual(command[dimension_index + 1:dimension_index + 3], ["ApiName", "ResponseType"])
            self.assertEqual(next(row for row in rows if row["metric"] == "Egress")["aggregation"], "Total")

    def test_missing_transactions_identity_dimensions_are_a_coverage_gap(self):
        with tempfile.TemporaryDirectory() as temporary:
            collector = self.make_collector(Path(temporary), omit_response_type=True)
            collector.discover()
            poll = collector.poll()
            transaction_result = next(row for row in poll["targets"][0]["metrics"]
                                      if row["metric"] == "Transactions")
            self.assertEqual(transaction_result["pointCount"], 0)
            self.assertTrue(any(gap.get("metric") == "Transactions" and
                                "required point identity dimensions" in gap.get("gap", "")
                                for gap in collector.gaps))

    def test_unready_or_malformed_receipt_never_creates_resource_target(self):
        for receipt in ({"status": "configuring", "accountId": ACCOUNT},
                        {"status": "ready", "accountId": "not-a-resource-id"}):
            with tempfile.TemporaryDirectory() as temporary:
                collector = FakeBlobAzure({}, Path(temporary), archive_receipt=receipt)
                self.assertFalse(any(row["kind"] == "blob-service" for row in collector.targets))
                self.assertTrue(collector.gaps)

    def test_receipt_is_loaded_from_state_dir_above_attachment_verification(self):
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary) / ".state"
            attachment_path = state_dir / "attachment-verification" / "verified.json"
            self.assertEqual(archive_receipt_path_for(attachment_path), state_dir / "wal-archive.json")


if __name__ == "__main__":
    unittest.main()
