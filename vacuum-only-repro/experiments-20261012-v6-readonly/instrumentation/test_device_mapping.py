import importlib.util
import os
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COLLECT_SPEC = importlib.util.spec_from_file_location("device_collector", HERE / "collect.py")
assert COLLECT_SPEC and COLLECT_SPEC.loader
collect = importlib.util.module_from_spec(COLLECT_SPEC)
COLLECT_SPEC.loader.exec_module(collect)

TEST_SPEC = importlib.util.spec_from_file_location("measurement_test", ROOT / "03-test.py")
assert TEST_SPEC and TEST_SPEC.loader
measurement = importlib.util.module_from_spec(TEST_SPEC)
TEST_SPEC.loader.exec_module(measurement)


def dev_row(kind, *, mount_mm, disk_mm, name, lun):
    major, minor = (int(x) for x in mount_mm.split(":"))
    controller = name[:name.rfind("n")]
    sysdev = f"/sys/devices/pci0000:00/0000:00:06.0/nvme/{controller}/{name}"
    fields = [
        "/var/lib/postgresql/data/pgdata" if kind == "DATA" else "/var/lib/postgresql/data/pg_wal",
        mount_mm, disk_mm, f"/dev/{name}",
        "/var/lib/postgresql/data" if kind == "DATA" else "/var/lib/postgresql/wal",
        "ext4", str(lun), f"/dev/disk/azure/data/by-lun/{lun}", name,
        "128", "128", "128", str(os.makedev(major, minor)), mount_mm, sysdev,
        "nvme", controller, "nvme", sysdev.rsplit("/", 1)[0],
        f"{name}:nvme,{controller}:nvme", "", "", "[none] mq-deadline", "128",
        "0", "0", "2000", "1", "0", "0", "2",
        "0~0,8-15~1023~0;1~8-15~1023~0",
    ]
    return fields


class NvmeDeviceMappingTests(unittest.TestCase):
    def test_collector_resolves_nvme_and_storvsc_mount_targets(self):
        rows = "\n".join([
            "data|259:1|nvme0n1|/var/lib/postgresql/data/pgdata|nvme|nvme0|nvme|/sys/class/nvme/nvme0|nvme0n1:nvme",
            "wal|259:2|sdc|/var/lib/postgresql/wal|storvsc|hv_storvsc|hv_storvsc|/sys/devices/vmbus/host2|host2:hv_storvsc",
        ])
        devices = collect.parse_device_rows(rows)
        self.assertEqual(devices["data"]["device"], "nvme0n1")
        self.assertEqual(devices["data"]["controller"]["type"], "nvme")
        self.assertEqual(devices["wal"]["controller"]["type"], "storvsc")
        self.assertEqual(devices["wal"]["majorMinor"], "259:2")

    def test_collector_requires_distinct_data_and_wal_mounts(self):
        row = "data|259:1|nvme0n1|/data|nvme|nvme0|nvme|/sys/class/nvme/nvme0|nvme"
        with self.assertRaisesRegex(RuntimeError, "both DATA/WAL"):
            collect.parse_device_rows(row)
        duplicate = row + "\n" + row.replace("data|", "wal|")
        with self.assertRaisesRegex(RuntimeError, "distinct mounted"):
            collect.parse_device_rows(duplicate)

    def test_diskstats_matches_exact_nvme_major_minor_and_name(self):
        stats = collect.parse_diskstats_rows("8 0 sda 1 0 2 3\n259 1 nvme0n1 4 0 8 9\n259 2 nvme0n2 5 0 10 11")
        self.assertIn(("259", "1", "nvme0n1"), stats)
        self.assertNotIn(("259", "1", "nvme0n2"), stats)
        targets = {
            "data": {"majorMinor": "259:1", "device": "nvme0n1"},
            "wal": {"majorMinor": "259:2", "device": "nvme0n2"},
        }
        self.assertEqual(collect.missing_diskstats_targets(targets, stats), [])
        self.assertEqual(collect.missing_diskstats_targets(targets, {("259", "1", "nvme0n1")}), ["wal"])

    def test_bcc_readiness_needs_one_exact_data_or_wal_histogram(self):
        targets = {"nvme0n1", "nvme0n2"}
        self.assertFalse(collect.bcc_preflight_ready(targets, {"sda"}))
        self.assertTrue(collect.bcc_preflight_ready(targets, {"nvme0n1"}))
        self.assertTrue(collect.bcc_preflight_ready(targets, {"nvme0n1", "nvme0n2"}))

    def test_measurement_sample_preserves_nvme_controller_and_blk_mq_without_scsi_depth(self):
        data = dev_row("DATA", mount_mm="259:1", disk_mm="259:0", name="nvme0n1", lun=10)
        wal = dev_row("WAL", mount_mm="259:3", disk_mm="259:2", name="nvme0n2", lun=11)
        # The root disk is unrelated to the exact DATA/WAL block major:minor rows.
        disks = "8 0 sda 1 0 2 3 4 0 8 9 10 0 11 12 0 13 14 0\n"
        disks += "259 0 nvme0n1 1 0 2 3 4 0 8 9 10 0 11 12 0 13 14 0\n"
        disks += "259 2 nvme0n2 1 0 2 3 4 0 8 9 10 0 11 12 0 13 14 0"
        sql = 'primary|||0||[]|{"status":"streaming"}|{}'
        out = "\n".join([
            "__TIME__", "1791806400.0", "2026-10-12T12:00:00.000Z",
            "__DEV_DATA__", "|".join(data),
            "__DEV_WAL__", "|".join(wal),
            "__DISKSTATS__", disks,
            "__SQL__", sql,
        ])
        pod = {"metadata": {"name": "pg-2"}}
        mapping = {
            "data": {"lun": "10", "path": "/var/lib/postgresql/data", "claim": "data-pvc", "pv": "pv-data", "node": "node-1", "attachmentDevicePath": "/dev/disk/azure/data/by-lun/10"},
            "wal": {"lun": "11", "path": "/var/lib/postgresql/wal", "claim": "wal-pvc", "pv": "pv-wal", "node": "node-1", "attachmentDevicePath": "/dev/disk/azure/data/by-lun/11"},
        }
        sample = measurement.parse_sample(out, pod, mapping)
        self.assertEqual(sample["devices"]["data"]["controller"]["type"], "nvme")
        self.assertEqual(sample["devices"]["wal"]["device"], "nvme0n2")
        self.assertIsNone(sample["devices"]["data"]["scsi"]["queueDepth"])
        self.assertEqual(sample["devices"]["data"]["queue"]["blkMq"][0]["cpuList"], "0,8-15")
        self.assertEqual(sample["devices"]["data"]["queue"]["blkMq"][0]["nrTags"], 1023)
        self.assertEqual(sample["devices"]["data"]["queue"]["blkMq"][0]["nrReservedTags"], 0)
        self.assertEqual(set(sample["disks"]), {"8:0", "259:0", "259:2"})
        self.assertNotIn("sda", {x["device"] for x in sample["devices"].values()})

    def test_nvme_controller_suffix_handles_multi_digit_controller(self):
        for device, controller in (("nvme0n2", "nvme0"), ("nvme12n34", "nvme12")):
            result = __import__("subprocess").run(
                ["sh", "-c", 'disk="$1"; printf "%s" "${disk%n[0-9]*}"', "sh", device],
                text=True, capture_output=True, check=True)
            self.assertEqual(result.stdout, controller)


if __name__ == "__main__":
    unittest.main()
