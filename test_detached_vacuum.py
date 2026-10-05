import importlib.util
import os
import tempfile
import time
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location("vacuum_runner", Path(__file__).with_name("03-test.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class DetachedVacuumTests(unittest.TestCase):
    def test_rebuilt_fixture_freezes_while_legacy_source_keeps_default(self):
        rebuilt = runner.vacuum_spec({"fixtureTargetHeapBytes": 23 * runner.GIB})
        legacy = runner.vacuum_spec({"sourceKind": "keeper"})
        self.assertEqual(rebuilt, {"options": ["VERBOSE", "FREEZE"],
                                   "sql": "VACUUM (VERBOSE, FREEZE) public.vacuum_target;"})
        self.assertEqual(legacy, {"options": ["VERBOSE"],
                                  "sql": "VACUUM (VERBOSE) public.vacuum_target;"})

    def test_status_and_log_survive_short_exec_return(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bindir = root / "bin"
            bindir.mkdir()
            fake_kubectl = bindir / "kubectl"
            fake_kubectl.write_text("#!/usr/bin/env python3\nimport os,sys,subprocess\ni=sys.argv.index('--')\nsys.exit(subprocess.run(sys.argv[i+1:],env=os.environ.copy()).returncode)\n")
            fake_psql = bindir / "psql"
            fake_psql.write_text("""#!/usr/bin/env python3
import os,sys
print('VACUUM_START|2026-10-04T00:00:00.000000Z|0/10|100|10|1', flush=True)
if int(os.environ['FAKE_PSQL_EXIT']) == 0:
 print('VACUUM_COMPLETE|2026-10-04T00:00:02.000000Z|0/20|200|20|2', flush=True)
sys.exit(int(os.environ['FAKE_PSQL_EXIT']))
""")
            fake_kubectl.chmod(0o755); fake_psql.chmod(0o755)
            old = {k: os.environ.get(k) for k in ("TMPDIR", "PATH", "FAKE_PSQL_EXIT")}
            os.environ.update(TMPDIR=tmp, PATH=f"{bindir}:{old['PATH']}", FAKE_PSQL_EXIT="0")
            try:
                base = [str(fake_kubectl), "--kubeconfig", "unused-by-shim"]
                workdir, pid = runner.launch_vacuum(base, "repro", "hdd-pg-1", "repro", "vacuum-only-test")
                self.assertGreater(pid, 0)
                deadline, status = time.monotonic() + 5, "RUNNING"
                while status == "RUNNING" and time.monotonic() < deadline:
                    status = runner.remote_status(base, "repro", "hdd-pg-1", workdir)
                    if status == "RUNNING": time.sleep(.05)
                self.assertEqual(status, "0")
                marks = runner.fetch_log(base, "repro", "hdd-pg-1", workdir, root / "vacuum.log")
                self.assertEqual(set(marks), {"start", "complete"})

                os.environ["FAKE_PSQL_EXIT"] = "7"
                workdir, _ = runner.launch_vacuum(base, "repro", "hdd-pg-1", "repro", "vacuum-only-failed")
                deadline, status = time.monotonic() + 5, "RUNNING"
                while status == "RUNNING" and time.monotonic() < deadline:
                    status = runner.remote_status(base, "repro", "hdd-pg-1", workdir)
                    if status == "RUNNING": time.sleep(.05)
                self.assertEqual(status, "7")
            finally:
                for key, value in old.items():
                    if value is None: os.environ.pop(key, None)
                    else: os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
