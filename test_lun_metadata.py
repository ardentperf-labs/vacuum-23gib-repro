import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("vacuum_test", Path(__file__).with_name("03-test.py"))
vacuum_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vacuum_test)


class AttachmentLunTests(unittest.TestCase):
    def test_azure_csi_publish_context_uses_uppercase_lun_without_device_path(self):
        # Azure Disk CSI's ControllerPublishVolume returns publish_context{"LUN": ...};
        # the node-side devicePath may be empty in AKS.
        self.assertEqual(vacuum_test.attachment_lun({"LUN": "2", "devicePath": ""}), "2")

    def test_lowercase_key_and_device_path_fallback(self):
        self.assertEqual(vacuum_test.attachment_lun({"lun": 3}), "3")
        self.assertEqual(vacuum_test.attachment_lun({}, "/dev/disk/azure/scsi1/lun4"), "4")

    def test_missing_or_invalid_lun_fails_closed(self):
        for metadata in ({}, {"LUN": "unknown"}):
            with self.assertRaises(RuntimeError):
                vacuum_test.attachment_lun(metadata)


if __name__ == "__main__":
    unittest.main()
