import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("vacuum_runner", Path(__file__).with_name("03-test.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def sample(primary_role="primary", replica_role="replica", replay="2/00000030", app_sessions=0):
    return ({"role": primary_role, "flushLsn": "2/00000030", "appSessions": 0},
            {"role": replica_role, "replayLsn": replay, "appSessions": app_sessions})


class ReplayCompletionTests(unittest.TestCase):
    def test_fixture_heap_gate_accepts_metadata_sized_23_gib_table(self):
        expected = 23 * runner.GIB
        self.assertEqual(runner.minimum_fixture_heap(expected), int(expected * .97))
        self.assertEqual(runner.minimum_fixture_heap(None), 100 * runner.GIB)

    def test_requires_replica_to_replay_exact_vacuum_completion_lsn(self):
        primary, replica = sample()
        self.assertTrue(runner.replica_reached_target(primary, replica, "2/00000020"))
        self.assertTrue(runner.replica_reached_target(primary, replica, "2/00000030"))
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000031"))

    def test_replica_at_marker_but_behind_current_primary_flush_is_not_caught_up(self):
        primary, replica = sample(replay="2/00000020")
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))

    def test_catchup_requires_three_consecutive_qualifying_samples(self):
        streak = 0
        finished_at = None
        for i, qualifies in enumerate((True, True, False, True, True, True)):
            streak = runner.next_catchup_streak(streak, qualifies)
            if streak >= 3:
                finished_at = i
                break
        self.assertEqual(finished_at, 5)
        self.assertEqual(streak, 3)

    def test_catchup_timeout_cannot_mark_measurement_complete(self):
        self.assertTrue(runner.measurement_complete(0, {"flushLsn": "2/20"}, True, None))
        self.assertFalse(runner.measurement_complete(0, {"flushLsn": "2/20"}, False,
                                                      "replica catch-up timed out"))
        self.assertFalse(runner.measurement_complete(0, {"flushLsn": "2/20"}, False, None))

    def test_requires_both_expected_postgres_roles_and_no_app_sessions(self):
        primary, replica = sample(primary_role="replica")
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))
        primary, replica = sample(replica_role="primary")
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))
        primary, replica = sample(app_sessions=1)
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))

    def test_missing_or_invalid_replay_state_cannot_complete(self):
        primary, replica = sample(replay="")
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))
        primary, replica = sample(replay="invalid")
        self.assertFalse(runner.replica_reached_target(primary, replica, "2/00000020"))
        self.assertFalse(runner.replica_reached_target(primary, replica, ""))

    def test_pod_health_is_required_during_replay_sampling(self):
        def pod(name, role, ready=True):
            return {"metadata": {"name": name, "uid": name + "-uid",
                                 "labels": {"cnpg.io/instanceRole": role}},
                    "status": {"phase": "Running", "conditions": [
                        {"type": "Ready", "status": "True" if ready else "False"}]}}

        expected = {"primary": pod("db-1", "primary"), "replica": pod("db-2", "replica")}
        with patch.object(runner, "cmd", return_value=json.dumps({"items": list(expected.values())})):
            runner.check_pods(["kubectl"], "repro", "db", expected)

        not_ready = {"primary": expected["primary"], "replica": pod("db-2", "replica", False)}
        with patch.object(runner, "cmd", return_value=json.dumps({"items": list(not_ready.values())})):
            with self.assertRaisesRegex(RuntimeError, "replica pod is not Running/Ready"):
                runner.check_pods(["kubectl"], "repro", "db", expected)


if __name__ == "__main__":
    unittest.main()
