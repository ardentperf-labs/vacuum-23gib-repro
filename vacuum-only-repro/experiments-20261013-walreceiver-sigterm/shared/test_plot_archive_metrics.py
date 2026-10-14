import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("plot_archive_metrics", HERE / "plot_archive_metrics.py")
plotter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = plotter
spec.loader.exec_module(plotter)

BASE = "2026-10-13T00:00:00Z"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def row(metric, offset, value, *, lane="sigterm", dims=None, field="total", captured=1,
        unit=None, poll="poll-1", resource="/sub/storageAccounts/archive/blobServices/default"):
    timestamp = f"2026-10-13T00:{offset:02d}:00Z"
    point = {"timeStamp": timestamp, field: value}
    record = {"capturedAtUtc": f"2026-10-13T01:00:{captured:02d}Z", "pollId": poll,
              "role": "archive", "targetKind": "blob-service", "resourceId": resource,
              "accountResourceId": resource.rsplit("/blobServices", 1)[0], "metric": metric,
              "aggregation": "Total" if field == "total" else "Average", "nativeInterval": "PT1M",
              "dimensionNames": sorted((dims or {}).keys()), "dimensions": dims or {},
              "nativeDimensions": [{"name": key, "value": value} for key, value in (dims or {}).items()],
              "point": point}
    return record


def make_lane(root, lane, rows, *, signal=True, include_definitions=True):
    results = root / lane / "results"
    azure = results / "instrumentation/azure"
    write_json(results / "measurement/timing.json", {"vacuumStart": BASE})
    write_json(results / "measurement/walreceiver-intervention.json", {
        "executedAtUtc": "2026-10-13T00:10:00Z",
        "signalResult": {"signalSent": signal}})
    write_jsonl(azure / "azure-metrics.jsonl", rows)
    write_jsonl(azure / "azure-metric-polls.jsonl", [{
        "resourceId": "/sub/storageAccounts/archive/blobServices/default",
        "rawResponse": {"value": [
            {"name": {"value": "SuccessE2ELatency"}, "unit": "Milliseconds"},
            {"name": {"value": "SuccessServerLatency"}, "unit": "Milliseconds"},
        ]}}])
    if include_definitions:
        write_json(azure / "definitions-inventory.json", {"targets": [{"kind": "blob-service",
            "availableMetrics": ["Transactions", "Ingress", "Egress", "SuccessE2ELatency",
                                 "SuccessServerLatency"]}]})
    return azure / "azure-metrics.jsonl"


class ArchiveMetricsPlotTests(unittest.TestCase):
    def test_native_series_dedup_aggregation_units_and_verified_signal_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = [
                row("Transactions", 0, 9, dims={"apiname": "GetBlob", "responsetype": "Success"}),
                row("Transactions", 0, 1, dims={"apiname": "GetBlob", "responsetype": "ServerBusyError"}),
                row("Transactions", 0, 5, dims={"apiname": "PutBlob", "responsetype": "Success"}),
                row("Transactions", 0, 9, dims={"apiname": "GetBlob", "responsetype": "Success"},
                    captured=2, poll="poll-2"),
                row("Ingress", 0, 64 * plotter.MiB, dims={"geotype": "Primary", "apiname": "PutBlob"}),
                row("Egress", 0, 32 * plotter.MiB, dims={"geotype": "Primary", "apiname": "GetBlob"}),
                row("SuccessE2ELatency", 0, 12.5, field="average", dims={"apiname": "PutBlob"}),
                row("SuccessServerLatency", 0, 4.5, field="average", dims={"apiname": "PutBlob"}),
            ]
            path = make_lane(root, "sigterm", rows)
            points, metadata = plotter.load_series([("sigterm", path)])
            requests = plotter.grouped_transactions(points, metadata)
            self.assertEqual(requests[("sigterm", "GET", "success")][0], 9)
            self.assertEqual(requests[("sigterm", "GET", "failure")][0], 1)
            self.assertEqual(requests[("sigterm", "PUT", "success")][0], 5)
            traffic = plotter.grouped_traffic(points, metadata)
            self.assertAlmostEqual(traffic[("sigterm", "Ingress")][0], 64 / 60)
            self.assertAlmostEqual(traffic[("sigterm", "Egress")][0], 32 / 60)
            latency = plotter.grouped_latency(points, metadata)
            self.assertEqual(latency[("sigterm", "SuccessE2ELatency", "PutBlob", "", "archive")][0], 12.5)
            duplicated = next(item for item in points if item.get("metric") == "Transactions"
                              and item.get("dimensions", {}).get("apiname") == "GetBlob"
                              and item.get("dimensions", {}).get("responsetype") == "Success")
            self.assertEqual(duplicated["_duplicateCount"], 1)
            self.assertEqual(metadata["sources"]["sigterm"]["signalTimeUtc"].isoformat(),
                             "2026-10-13T00:10:00+00:00")

    def test_unverified_signal_is_not_plotted_and_missing_metrics_are_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = make_lane(root, "control", [], signal=False, include_definitions=False)
            out = root / "plots"
            summary = plotter.build(root, out, [f"control={path}"])
            coverage = json.loads((out / "azure-blob-archive-coverage.json").read_text())
            self.assertEqual(summary["nativePointCount"], 0)
            self.assertIsNone(coverage["sourcePoints"]["control"]["signalTimeUtc"])
            self.assertIn("signalSent is not true", coverage["sourcePoints"]["control"]["signalMarkerSource"])
            self.assertEqual(len(coverage["missingMetrics"]), len(plotter.METRICS))
            self.assertTrue(all(chart["missingPanel"] for chart in coverage["charts"].values()))
            self.assertEqual(len(list(out.glob("*.png"))), 3)

    def test_default_run_with_no_azure_artifacts_still_emits_missing_panels(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "results" / "azure-archive-graphs"
            summary = plotter.build(root, out, [])
            coverage = json.loads((out / "azure-blob-archive-coverage.json").read_text())
            self.assertEqual(summary["nativePointCount"], 0)
            self.assertEqual(set(coverage["sourcePoints"]), {"control", "sigterm"})
            self.assertTrue(all(chart["missingPanel"] for chart in coverage["charts"].values()))
            self.assertEqual(len(list(out.glob("*.png"))), 3)

    def test_pt1m_holes_break_lines_instead_of_becoming_zeroes(self):
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        count = plotter.plot_series_with_gaps(ax, {0.0: 2.0, 3.0: 7.0}, label="test")
        self.assertEqual(count, 2)
        self.assertTrue(any(float(value) != float(value) for value in ax.lines[0].get_ydata()))
        self.assertEqual(list(ax.lines[0].get_ydata())[:1], [2.0])
        plt.close(fig)


if __name__ == "__main__":
    unittest.main()
