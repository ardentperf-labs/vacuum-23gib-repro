import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("collect.py")
SPEC = importlib.util.spec_from_file_location("replica_collector", MODULE_PATH)
assert SPEC and SPEC.loader
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


class CollectorOutputParsingTests(unittest.TestCase):
    def test_bcc_python_repr_keeps_each_device_histogram(self):
        # BCC's published -jD sample uses a Python dict repr and bytes device
        # bucket, so strict json.loads cannot parse it.
        first = (
            "2026-10-12T12:00:00.000000000Z "
            "{'ts': '12:00:00', 'val_type': 'msecs', 'data': "
            "[{'interval-start': 0, 'interval-end': 1, 'count': 4}], "
            "'Bucket ptr': b'sda'}"
        )
        second = (
            "2026-10-12T12:00:01.000000000Z "
            "{'ts': '12:00:01', 'val_type': 'msecs', 'data': "
            "[{'interval-start': 0, 'interval-end': 1, 'count': 2}], "
            "'Bucket ptr': b'sdb'}"
        )

        parsed = [collector.parse_bcc_histogram_line(x) for x in (first, second)]

        self.assertEqual([x[0] for x in parsed if x], ["sda", "sdb"])
        self.assertEqual(parsed[0][1]["Bucket ptr"], "sda")
        self.assertEqual(parsed[1][1]["data"][0]["count"], 2)

    def test_bcc_json_histogram_and_non_histogram(self):
        line = (
            "{'ts': '12:00:02', 'val_type': 'msecs', 'data': [], 'disk': 'nvme0n2'}"
        )
        parsed = collector.parse_bcc_histogram_line(line)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed[0], "nvme0n2")
        self.assertIsNone(collector.parse_bcc_histogram_line("12:00:02 no device histogram"))
        path_label = "{'ts': '12:00:03', 'data': [], 'disk': '/dev/nvme0n2'}"
        self.assertEqual(collector.parse_bcc_histogram_line(path_label)[0], "nvme0n2")

    def test_psql_watch_row_keeps_receiver_wait_and_stat_fields(self):
        line = (
            '{"sampledAt":"2026-10-12T12:00:03Z","replayLsn":"0/1A2B",'
            '"walReceiverWait":[{"pid":31,"backendType":"walreceiver",'
            '"waitEventType":"Activity","waitEvent":"WalReceiverMain"}],'
            '"walReceiver":{"status":"streaming","latestEndLsn":"0/1A2B"}}'
        )
        row = collector.parse_psql_json_line(line)
        self.assertEqual(row["walReceiverWait"][0]["waitEvent"], "WalReceiverMain")
        self.assertEqual(row["walReceiver"]["status"], "streaming")
        self.assertIsNone(collector.parse_psql_json_line("psql watch header"))
        self.assertIn("pg_stat_activity", collector.SQL)
        self.assertIn("wait_event_type", collector.SQL)
        self.assertIn("pg_stat_wal_receiver", collector.SQL)
        self.assertIn("\\watch 1", collector.SQL_PRETTY)


if __name__ == "__main__":
    unittest.main()
