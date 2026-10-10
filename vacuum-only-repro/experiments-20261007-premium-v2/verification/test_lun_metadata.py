import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

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


class AzureDiskMetadataTests(unittest.TestCase):
    def fixture(self, omit_size=False):
        claims = {}
        pvcs = []
        pvs = []
        attachments = []
        pods = {}
        disks = {}
        for role, node in (("primary", "node-a"), ("replica", "node-b")):
            data_claim, wal_claim = role + "-data", role + "-wal"
            volumes = []
            mounts = []
            for kind, claim in (("data", data_claim), ("wal", wal_claim)):
                pv_name = "pv-" + claim
                disk_id = "/subscriptions/test/resourceGroups/nodes/providers/Microsoft.Compute/disks/" + claim
                pvcs.append({"metadata": {"name": claim}, "spec": {
                    "storageClassName": "azure-premiumv2",
                    "resources": {"requests": {"storage": "512Gi"}}, "volumeName": pv_name},
                    "status": {"phase": "Bound"}})
                pvs.append({"metadata": {"name": pv_name}, "spec": {
                    "csi": {"driver": "disk.csi.azure.com", "volumeHandle": disk_id,
                            "volumeAttributes": {"skuName": "PremiumV2_LRS", "cachingMode": "None"}},
                    "capacity": {"storage": "512Gi"}}})
                volumes.append({"name": kind, "persistentVolumeClaim": {"claimName": claim}})
                mount_path = "/var/lib/postgresql/data" if kind == "data" else "/var/lib/postgresql/wal"
                mounts.append({"name": kind, "mountPath": mount_path})
                attachments.append({"spec": {"source": {"persistentVolumeName": pv_name}, "nodeName": node},
                                    "status": {"attached": True, "attachmentMetadata": {"LUN": "2"}}})
                disk = {
                    "id": disk_id, "sku": {"name": "PremiumV2_LRS"}, "diskSizeGB": None,
                    "diskSizeGb": None if omit_size else 512, "diskIOPSReadWrite": 3000,
                    "diskMBpsReadWrite": 125, "provisioningState": "Succeeded", "managedBy": "mock-vm",
                    "zones": ["1"], "logicalSectorSize": None,
                    "creationData": {"createOption": "Empty", "logicalSectorSize": 512},
                    "completionPercent": None,
                }
                disks[disk_id] = disk
            pods[role] = {"metadata": {"name": role + "-pod"}, "spec": {
                "nodeName": node, "containers": [{"name": "postgres", "volumeMounts": mounts}],
                "volumes": volumes}}
        storage_class = {"provisioner": "disk.csi.azure.com", "parameters": {
            "skuName": "PremiumV2_LRS", "cachingMode": "None", "kind": "Managed",
            "DiskIOPSReadWrite": "3000", "DiskMBpsReadWrite": "125", "LogicalSectorSize": "512"}}
        responses = {
            ("pvc",): {"items": pvcs},
            ("pv",): {"items": pvs},
            ("volumeattachments",): {"items": attachments},
            ("storageclass",): storage_class,
        }

        def fake_cmd(argv, timeout=30, input=None):
            if argv[0] == "az":
                return json.dumps(disks[argv[4]])
            if argv[0] == "kubectl":
                for key, value in responses.items():
                    if key[0] in argv:
                        return json.dumps(value)
            raise AssertionError("Unexpected command: " + " ".join(argv))

        return pods, fake_cmd

    def test_disk_size_alias_and_null_logical_sector_fall_back_to_creation_data(self):
        pods, fake_cmd = self.fixture()
        with patch.object(vacuum_test, "cmd", fake_cmd):
            result = vacuum_test.storage(["kubectl"], "repro", pods, {
                "subscriptionId": "test", "availabilityZone": "1"})
        for claim in result["claims"].values():
            self.assertEqual(claim["azureDisk"]["sizeGiB"], 512)
            self.assertEqual(claim["azureDisk"]["logicalSectorSize"], 512)

    def test_missing_size_alias_fails_closed(self):
        pods, fake_cmd = self.fixture(omit_size=True)
        with patch.object(vacuum_test, "cmd", fake_cmd):
            with self.assertRaisesRegex(RuntimeError, "Azure disk SKU, size"):
                vacuum_test.storage(["kubectl"], "repro", pods, {
                    "subscriptionId": "test", "availabilityZone": "1"})


if __name__ == "__main__":
    unittest.main()
