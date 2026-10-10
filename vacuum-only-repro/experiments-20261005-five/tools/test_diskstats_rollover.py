"""Focused offline tests for diskstats_rollover.normalize_rows."""
import copy
import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).with_name("diskstats_rollover.py")
spec = importlib.util.spec_from_file_location("diskstats_rollover", MODULE)
rollover = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rollover)

M = rollover.UINT32


def sample(epoch, *, rm=1000, wm=2000, reads=10, writes=20,
           read_sectors=100, write_sectors=200, major_minor="8:16",
           pv="pv-data", nr_requests=316):
    device = {
        "majorMinor": major_minor,
        "path": "/var/lib/postgresql/data/pgdata",
        "claim": "hdd-pg-2",
        "source": "/dev/sdb" if major_minor == "8:16" else "/dev/sdc",
        "node": "node-1", "pv": pv, "lun": 0,
        "mount": "/var/lib/postgresql/data",
        "mountMajorMinor": major_minor,
        "sysfsDevicePath": "/sys/mock/target0",
        "queue": {"nrRequests": nr_requests, "scsiDeviceQueueDepth": 316},
    }
    disk = {"name": device["source"].split("/")[-1], "r": reads, "rs": read_sectors,
            "rm": rm, "w": writes, "ws": write_sectors, "wm": wm}
    return {
        "phase": "vacuum", "role": "replica", "epoch": epoch,
        "ts": f"2026-10-06T00:00:{int(epoch):02d}Z",
        "devices": {"data": device}, "disks": {major_minor: disk},
    }


def wrap_pair(field, before_value, after_value, *, ops_before=100, ops_after=3100,
              sectors_before=1000, sectors_after=5000, second_field_value=2000):
    first = sample(1.0, **{field: before_value})
    second_kwargs = {field: after_value}
    if field == "rm":
        first.update(sample(1.0, rm=before_value, reads=ops_before, read_sectors=sectors_before))
        second = sample(6.0, rm=after_value, reads=ops_after, read_sectors=sectors_after)
    elif field == "wm":
        first = sample(1.0, wm=before_value, writes=ops_before, write_sectors=sectors_before)
        second = sample(6.0, wm=after_value, writes=ops_after, write_sectors=sectors_after)
    else:
        raise ValueError(field)
    return [first, second]


class DiskstatsRolloverTests(unittest.TestCase):
    def test_read_time_wrap_is_unwrapped_without_mutating_raw(self):
        rows = wrap_pair("rm", M - 700_000, 50_000)
        original = copy.deepcopy(rows)
        normalized, audit = rollover.normalize_rows(rows)
        self.assertEqual(len(audit["events"]), 1)
        self.assertEqual(audit["events"][0]["counter"], "rm")
        self.assertEqual(audit["events"][0]["correctedDelta"], 750_000)
        self.assertEqual(normalized[1]["disks"]["8:16"]["rm"], 50_000 + M)
        self.assertEqual(rows, original)

    def test_write_time_wrap_is_unwrapped(self):
        rows = wrap_pair("wm", M - 700_000, 50_000, ops_before=50, ops_after=1550,
                         sectors_before=100, sectors_after=2100)
        normalized, audit = rollover.normalize_rows(rows)
        self.assertEqual(len(audit["events"]), 1)
        self.assertEqual(audit["events"][0]["counter"], "wm")
        self.assertEqual(audit["events"][0]["correctedDelta"], 750_000)
        self.assertEqual(normalized[1]["disks"]["8:16"]["wm"], 50_000 + M)

    def test_simultaneous_read_and_write_wraps_are_both_proven(self):
        first = sample(1.0, rm=M - 700_000, wm=M - 800_000,
                       reads=100, writes=50, read_sectors=1000, write_sectors=500)
        second = sample(6.0, rm=50_000, wm=200_000,
                        reads=3100, writes=2050, read_sectors=5000, write_sectors=4500)
        normalized, audit = rollover.normalize_rows([first, second])
        self.assertEqual({event["counter"] for event in audit["events"]}, {"rm", "wm"})
        self.assertEqual({event["correctedDelta"] for event in audit["events"]}, {750_000, 1_000_000})
        self.assertEqual(normalized[1]["disks"]["8:16"]["rm"], 50_000 + M)
        self.assertEqual(normalized[1]["disks"]["8:16"]["wm"], 200_000 + M)

    def test_ordinary_reset_is_rejected_and_stream_is_unchanged(self):
        rows = [sample(1.0, rm=20_000), sample(6.0, rm=5_000, reads=20, read_sectors=200)]
        normalized, audit = rollover.normalize_rows(rows)
        self.assertEqual(audit["status"], "rejected-ambiguous-streams")
        self.assertFalse(audit["events"])
        self.assertEqual(normalized, rows)
        self.assertTrue(any("not_upper_to_lower_uint32_transition" in r.get("reason", [])
                            for r in audit["rejections"]))

    def test_changed_device_identity_rejects_candidate(self):
        first = sample(1.0, rm=M - 700_000)
        second = sample(6.0, rm=50_000, reads=3100, read_sectors=5000,
                        major_minor="8:32", pv="pv-other")
        normalized, audit = rollover.normalize_rows([first, second])
        self.assertEqual(audit["status"], "rejected-ambiguous-streams")
        self.assertFalse(audit["events"])
        self.assertTrue(any(r["reason"] == "device_identity_changed" for r in audit["rejections"]))
        self.assertEqual(normalized, [first, second])

    def test_long_gap_rejects_candidate(self):
        first = sample(1.0, rm=M - 700_000)
        second = sample(30.0, rm=50_000, reads=3100, read_sectors=5000)
        normalized, audit = rollover.normalize_rows([first, second])
        self.assertEqual(audit["status"], "rejected-ambiguous-streams")
        self.assertFalse(audit["events"])
        self.assertEqual(normalized, [first, second])
        self.assertTrue(any(r["reason"] == "invalid_or_long_sampling_gap" for r in audit["rejections"]))

    def test_implausible_modulo_delta_rejects_candidate(self):
        first = sample(1.0, rm=M - 2_000_000)
        second = sample(6.0, rm=2_000_000, reads=3100, read_sectors=5000)
        normalized, audit = rollover.normalize_rows([first, second])
        self.assertEqual(audit["status"], "rejected-ambiguous-streams")
        self.assertFalse(audit["events"])
        self.assertEqual(normalized, [first, second])
        self.assertTrue(any("modulo_delta_not_plausible_for_gap_and_queue_depth" in r.get("reason", [])
                            for r in audit["rejections"]))

    def test_operation_counter_reset_rejects_wrap(self):
        first = sample(1.0, rm=M - 700_000, reads=3100, read_sectors=5000)
        second = sample(6.0, rm=50_000, reads=5, read_sectors=2)
        normalized, audit = rollover.normalize_rows([first, second])
        self.assertEqual(audit["status"], "rejected-ambiguous-streams")
        self.assertFalse(audit["events"])
        self.assertEqual(normalized, [first, second])
        self.assertTrue(any(r["reason"] == "operation_or_sector_counter_reset" for r in audit["rejections"]))


if __name__ == "__main__":
    unittest.main()
