import datetime as dt
import unittest

import plot


def sample(epoch, step, changed=False):
    rows = {}
    for role in plot.ROLES:
        devices, disks = {}, {}
        for dev, mm in (("data", "259:5"), ("wal", "259:6")):
            source = f"/dev/{dev}"
            path = f"/mnt/{dev}"
            if changed and role == "replica" and dev == "data":
                path = "/mnt/changed"
            devices[dev] = {"majorMinor": mm, "path": path, "claim": f"{role}-{dev}", "source": source}
            disks[mm] = {"r": step * 100, "rs": step * 200, "rm": step * 500,
                         "w": step * 50, "ws": step * 100, "wm": step * 1000}
        rows[role] = {"epoch": epoch, "ts": dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(),
                      "devices": devices, "disks": disks}
    return rows


def timing(vac_start, vac_end, replay_end):
    def ts(v):
        return dt.datetime.fromtimestamp(v, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return {"vacuumStart": ts(vac_start), "vacuumComplete": ts(vac_end), "replayEnd": ts(replay_end)}


def interval(t0, t1, read_ops=0, read_bytes=0, read_ms=0, write_ops=0,
             write_bytes=0, write_ms=0, valid=True):
    return {"t0": t0, "t1": t1, "seconds": t1 - t0, "valid": valid,
            "readOps": read_ops, "readBytes": read_bytes, "readMs": read_ms,
            "writeOps": write_ops, "writeBytes": write_bytes, "writeMs": write_ms}


class PlotMathTests(unittest.TestCase):
    def test_prorates_interval_edges_and_operation_weights(self):
        rows = [sample(100, 0), sample(110, 1), sample(120, 2), sample(130, 3)]
        result = plot.analyze(rows, timing(102, 118, 130), 10)
        stat = result["windows"]["vacuum"]["primary_data"]
        self.assertAlmostEqual(stat["coverage"], 1.0)
        self.assertAlmostEqual(stat["readIops"], 10.0)
        self.assertAlmostEqual(stat["readAwaitMs"], 5.0)
        self.assertAlmostEqual(stat["writeAwaitMs"], 20.0)
        self.assertAlmostEqual(stat["operationWeightedAwaitMs"], 10.0)
        self.assertAlmostEqual(stat["readMBps"], 200 * 512 / 1e6 / 10)

    def test_complete_short_replay_window_keeps_edge_coverage(self):
        rows = [sample(100, 0), sample(110, 1), sample(120, 2), sample(130, 3)]
        result = plot.analyze(rows, timing(102, 118, 130), 10)
        self.assertAlmostEqual(result["windows"]["replayDrain"]["replica_wal"]["coverage"], 1.0)
        self.assertTrue(result["coverage"]["valid"])

    def test_observation_end_does_not_claim_replica_catchup(self):
        rows = [sample(100, 0), sample(110, 1), sample(120, 2), sample(130, 3)]
        bounds = timing(102, 118, 130)
        bounds["observationEnd"] = bounds.pop("replayEnd")
        result = plot.analyze(rows, bounds, 10)
        self.assertTrue(result["coverage"]["valid"])
        self.assertAlmostEqual(result["windows"]["replayDrain"]["replica_wal"]["coverage"], 1)
        self.assertNotIn("replayEnd", bounds)

    def test_counter_gap_fails_closed(self):
        rows = [sample(100, 0), sample(110, 1), sample(140, 2), sample(150, 3)]
        result = plot.analyze(rows, timing(101, 149, 150), 10)
        self.assertFalse(result["coverage"]["valid"])
        self.assertGreater(result["windows"]["vacuum"]["replica_data"]["rejectedIntervals"], 0)

    def test_no_final_wait_does_not_fail_coverage(self):
        rows = [sample(100, 0), sample(110, 1), sample(120, 2), sample(130, 3)]
        bounds = timing(102, 118, 130)
        bounds.update(heapWorkStart=bounds["vacuumStart"], heapWorkComplete=bounds["vacuumComplete"])
        result = plot.analyze(rows, bounds, 10)
        self.assertTrue(result["coverage"]["valid"])
        self.assertIn("heapWork", result["windows"])
        self.assertNotIn("vacuumFinalWait", result["windows"])

    def test_device_remap_from_first_sample_fails_closed(self):
        rows = [sample(100, 0), sample(110, 1), sample(120, 2, changed=True), sample(130, 3)]
        result = plot.analyze(rows, timing(100, 120, 130), 10)
        self.assertFalse(result["coverage"]["valid"])
        self.assertGreater(result["windows"]["vacuum"]["replica_data"]["rejectedIntervals"], 0)

    def test_minute_bucket_uses_time_and_operation_weighting(self):
        points = [interval(0, 30, read_ops=300, read_bytes=3_000_000, read_ms=3000,
                           write_ops=60, write_bytes=600_000, write_ms=1200),
                  interval(30, 60, read_ops=30, read_bytes=300_000, read_ms=600)]
        bucket, = plot.minute_buckets(points)
        self.assertTrue(bucket["valid"])
        self.assertEqual(bucket["t0"], 0)
        self.assertEqual(bucket["t1"], 60)
        self.assertAlmostEqual(bucket["coverage"], 1.0)
        self.assertAlmostEqual(bucket["readIops"], 5.5)
        self.assertAlmostEqual(bucket["readMBps"], 3_300_000 / 60 / 1e6)
        self.assertAlmostEqual(bucket["readAwaitMs"], 3600 / 330)
        self.assertAlmostEqual(bucket["writeAwaitMs"], 20)
        self.assertAlmostEqual(bucket["operationWeightedAwaitMs"], 4800 / 390)

    def test_minute_bucket_splits_intervals_at_utc_boundary(self):
        points = [interval(0, 55), interval(55, 65, read_ops=100, read_bytes=10_000,
                                             read_ms=1000), interval(65, 120)]
        left, right = plot.minute_buckets(points)
        self.assertTrue(left["valid"] and right["valid"])
        self.assertEqual((left["t0"], left["t1"]), (0, 60))
        self.assertEqual((right["t0"], right["t1"]), (60, 120))
        self.assertAlmostEqual(left["readIops"], 50 / 60)
        self.assertAlmostEqual(right["readIops"], 50 / 60)

    def test_minute_buckets_leave_a_gap_instead_of_bridging(self):
        points = [interval(0, 60, read_ops=60), interval(60, 120, valid=False),
                  interval(120, 180, read_ops=60)]
        buckets = plot.minute_buckets(points)
        self.assertEqual([p["valid"] for p in buckets], [True, False, True])
        self.assertNotIn("readIops", buckets[1])

    def test_small_invalid_overlap_invalidates_high_coverage_bucket(self):
        bucket, = plot.minute_buckets([interval(0, 56, read_ops=56),
                                       interval(56, 60, valid=False)])
        self.assertAlmostEqual(bucket["coverage"], 56 / 60)
        self.assertEqual(bucket["invalidOverlapSeconds"], 4)
        self.assertFalse(bucket["valid"])

    def test_minute_bucket_requires_ninety_percent_coverage(self):
        just_enough, = plot.minute_buckets([interval(0, 54, read_ops=54)])
        too_short, = plot.minute_buckets([interval(0, 53.9, read_ops=53.9)])
        self.assertAlmostEqual(just_enough["coverage"], .9)
        self.assertTrue(just_enough["valid"])
        self.assertFalse(too_short["valid"])

    def test_display_limit_is_chart_only_and_handles_sparse_peaks(self):
        display = plot.display_limit([1] * 99 + [1000])
        self.assertAlmostEqual(display["p99"], 10.99)
        self.assertAlmostEqual(display["limit"], 10.99 * 1.1)
        self.assertEqual(display["maxValue"], 1000)
        self.assertEqual(plot.display_limit([0, 0])["limit"], 1.0)
        self.assertEqual(plot.display_limit([])["limit"], 1.0)


if __name__ == "__main__":
    unittest.main()
