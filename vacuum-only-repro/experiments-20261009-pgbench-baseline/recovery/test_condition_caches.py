import runpy
from pathlib import Path
import unittest


MODULE = runpy.run_path(str(Path(__file__).with_name("condition-caches.py")))


class CacheDropCommandTests(unittest.TestCase):
    def test_sysctl_receives_numeric_three_not_the_log_label(self):
        pod = MODULE["pod_object"](
            "cache-drop-01234567-primary",
            "0123456789abcdef0123456789abcdef",
            "primary",
            "aks-nodepool1-000000-vmss000000",
        )
        command = pod["spec"]["containers"][0]["command"][2]
        self.assertIn("printf '3\\n' > /host-vm/drop_caches", command)
        self.assertIn("printf 'drop_caches=3\\n';", command)
        self.assertNotIn("printf 'drop_caches=3\\n' > /host-vm/drop_caches", command)


if __name__ == "__main__":
    unittest.main()
