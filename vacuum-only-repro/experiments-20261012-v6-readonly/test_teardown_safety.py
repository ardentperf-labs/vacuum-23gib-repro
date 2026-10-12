import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cleanup', Path(__file__).with_name('teardown.py'))
cleanup = importlib.util.module_from_spec(spec); spec.loader.exec_module(cleanup)

class TeardownSafety(unittest.TestCase):
    def fixture(self, directory):
        token = 'a' * 32
        state = {'subscriptionId': 'test-sub', 'ownerToken': token,
                 'resourceGroup': 'rg-vacuum-only-' + token,
                 'requestedNodeResourceGroup': 'rg-vacuum-nodes-' + token,
                 'nodeResourceGroup': 'rg-vacuum-nodes-' + token, 'clusterName': 'test-aks'}
        (directory / 'state.json').write_text(json.dumps(state))
        return state

    def test_snapshot_in_node_group_prevents_aks_cascade(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp); state = self.fixture(directory); calls = []
            def fake(argv, **kwargs):
                calls.append(argv)
                command = argv[1:3]
                if command == ['account', 'show']: obj = {'id': 'test-sub'}
                elif command == ['group', 'exists']: obj = True
                elif command == ['group', 'show']: obj = {'tags': {'reprotoken': state['ownerToken']}}
                elif command == ['snapshot', 'list']:
                    obj = [{'id': '/keeper'}] if '-g' not in argv or argv[argv.index('-g') + 1] == state['nodeResourceGroup'] else []
                elif command == ['resource', 'list']: obj = []
                else: raise AssertionError('Unexpected mutation: ' + repr(argv))
                return subprocess.CompletedProcess(argv, 0, json.dumps(obj), '')
            with patch.object(cleanup, 'SD', directory), patch.object(cleanup.subprocess, 'run', fake):
                with self.assertRaisesRegex(AssertionError, 'containing snapshots'): cleanup.main()
            self.assertFalse(any('delete' in x for x in calls))

    def test_wrong_owner_prevents_any_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp); self.fixture(directory); calls = []
            def fake(argv, **kwargs):
                calls.append(argv); command = argv[1:3]
                if command == ['account', 'show']: obj = {'id': 'test-sub'}
                elif command == ['snapshot', 'list']: obj = []
                elif command == ['group', 'exists']: obj = True
                elif command == ['group', 'show']: obj = {'tags': {'reprotoken': 'unrelated'}}
                else: raise AssertionError('Unexpected mutation: ' + repr(argv))
                return subprocess.CompletedProcess(argv, 0, json.dumps(obj), '')
            with patch.object(cleanup, 'SD', directory), patch.object(cleanup.subprocess, 'run', fake):
                with self.assertRaisesRegex(AssertionError, 'ownership mismatch'): cleanup.main()
            self.assertFalse(any('delete' in x for x in calls))

if __name__ == '__main__': unittest.main()
