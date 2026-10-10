#!/usr/bin/env python3
"""Calibration entry point with a bounded retry for the Kubernetes log startup race."""
import importlib.util
import datetime as dt
import json
from pathlib import Path
import re
import sys
import time

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("read_pressure_frozen", HERE / "read_pressure.py")
rp = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(rp)

_logs_snapshot = rp.ReaderPressure.logs_snapshot
_start_log_capture = rp.ReaderPressure.start_log_capture

def record_wait(self, reason, attempt, phase, container_state):
    path = self.outdir.parent / "reader-startup-retries.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"time": dt.datetime.now(dt.timezone.utc).isoformat(),
           "operation": "read_only_reader_startup_wait", "pod": self.pod,
           "attempt": attempt, "reason": reason, "podPhase": phase,
           "containerState": container_state}
    with path.open("a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")

def bounded_call(self, fn, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
    original_run = rp.run
    def deadline_run(argv, timeout=60, input=None, retryable=False):
        budget = deadline - time.monotonic()
        reserve = 11.0 if retryable else 0.0
        attempts = 6 if retryable else 1
        per_attempt = (budget - reserve) / attempts
        if per_attempt <= 0:
            raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
        result = original_run(argv, min(timeout, per_attempt), input=input, retryable=retryable)
        if time.monotonic() >= deadline:
            raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
        return result
    rp.run = deadline_run
    try:
        return fn(self)
    finally:
        rp.run = original_run

def read_pod_state(self, deadline):
    def get_status(obj):
        raw = rp.run(obj.base + ["get", "pod", obj.pod, "-n", obj.ns, "-o", "json"],
                     30, retryable=True)
        return json.loads(raw)
    item = bounded_call(self, get_status, deadline)
    if time.monotonic() >= deadline:
        raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
    phase = item.get("status", {}).get("phase")
    status = next((x for x in item.get("status", {}).get("containerStatuses", [])
                   if x.get("name") == "readers"), {})
    state = status.get("state") or {}
    if phase == "Running" and state.get("running") is not None:
        return phase, "running"
    waiting = state.get("waiting") or {}
    reason = waiting.get("reason")
    if phase == "Pending" and reason == "ContainerCreating":
        return phase, reason
    raise RuntimeError(f"reader pod did not reach Running; phase={phase}, containerState={state}")

def startup_ready_log_capture(self):
    deadline = time.monotonic() + 120
    attempt = 0
    while True:
        attempt += 1
        phase, state = read_pod_state(self, deadline)
        if time.monotonic() >= deadline:
            raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
        if phase == "Running" and state == "running":
            result = _start_log_capture(self)
            if time.monotonic() >= deadline:
                raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
            return result
        record_wait(self, "ContainerCreating", attempt, phase, state)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("reader startup exceeded the 120-second ContainerCreating bound")
        time.sleep(min(2, remaining))

def startup_race_safe_logs(self):
    deadline = time.monotonic() + 120
    attempt = 0
    while True:
        try:
            result = bounded_call(self, _logs_snapshot, deadline)
            if time.monotonic() >= deadline:
                raise RuntimeError("reader log startup exceeded the 120-second ContainerCreating bound")
            return result
        except RuntimeError as exc:
            message = str(exc)
            if (" logs " not in message or "waiting to start: ContainerCreating" not in message):
                raise
            phase, state = read_pod_state(self, deadline)
            if not ((phase == "Pending" and state == "ContainerCreating") or
                    (phase == "Running" and state == "running")):
                raise
            attempt += 1
            record_wait(self, "logs endpoint reported ContainerCreating", attempt, phase, state)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("reader log startup exceeded the 120-second ContainerCreating bound") from exc
            time.sleep(min(2 if phase == "Pending" else 1, remaining))

rp.ReaderPressure.start_log_capture = startup_ready_log_capture
rp.ReaderPressure.logs_snapshot = startup_race_safe_logs

if __name__ == "__main__":
    raise SystemExit(rp.main())
