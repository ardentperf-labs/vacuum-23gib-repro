#!/usr/bin/env python3
"""Reconnect a long-running kubectl watch/log stream after API resets."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

stop = False
child: subprocess.Popen[str] | None = None


def stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def stop_handler(*_: object) -> None:
    global stop
    stop = True
    if child and child.poll() is None:
        child.terminate()


def main() -> int:
    global child
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("cluster", "operator", "postgres"), required=True)
    parser.add_argument("--pod", choices=("hdd-pg-1", "hdd-pg-2"),
                        help="Required for --kind postgres; only owned CNPG pods are accepted.")
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--gaps", type=Path, required=True)
    args = parser.parse_args()
    if args.kind == "postgres" and not args.pod:
        parser.error("--pod is required with --kind postgres")
    if args.kind != "postgres" and args.pod:
        parser.error("--pod is only valid with --kind postgres")
    base = ["kubectl", "--kubeconfig", args.kubeconfig]
    if args.kind == "cluster":
        command = base + ["get", "cluster", "hdd-pg", "-n", "repro", "--watch",
                          "--output-watch-events", "-o", "json"]
    elif args.kind == "operator":
        command = base + ["logs", "-n", "cnpg-system", "deployment/cnpg-controller-manager",
                          "--follow", "--timestamps", "--since=2m"]
    else:
        command = base + ["logs", "-n", "repro", args.pod, "-c", "postgres",
                          "--follow", "--since=5m"]
    args.out.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.gaps.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    restarts = 0
    with args.out.open("a", encoding="utf-8", buffering=1) as log:
        if args.kind == "postgres":
            os.chmod(args.out, 0o600)
        while not stop:
            started = stamp()
            log.write(json.dumps({"watcher": args.kind, "event": "stream_start", "time": started,
                                  "command": command}) + "\n") if args.kind == "cluster" else None
            try:
                child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         text=True, bufsize=1)
                assert child.stdout
                for line in child.stdout:
                    if args.kind == "cluster":
                        log.write(line)
                    else:
                        log.write(line if line.endswith("\n") else line + "\n")
                    log.flush()
                code = child.wait()
            except Exception as exc:
                code = 255
                with args.gaps.open("a", encoding="utf-8") as gaps:
                    if args.kind == "postgres":
                        os.chmod(args.gaps, 0o600)
                    gaps.write(json.dumps({"watcher": args.kind, "startedAtUtc": started,
                                           "failedAtUtc": stamp(), "error": str(exc)}) + "\n")
            finally:
                child = None
            if stop:
                break
            restarts += 1
            ended = stamp()
            with args.gaps.open("a", encoding="utf-8") as gaps:
                if args.kind == "postgres":
                    os.chmod(args.gaps, 0o600)
                gaps.write(json.dumps({"watcher": args.kind, "startedAtUtc": started,
                                       "endedAtUtc": ended, "exitCode": code,
                                       "reconnectAttempt": restarts}) + "\n")
            if args.kind == "cluster":
                log.write(json.dumps({"watcher": args.kind, "event": "reconnect", "time": ended,
                                      "exitCode": code, "attempt": restarts}) + "\n")
            log.flush()
            time.sleep(min(10, 1 + restarts))
    if child and child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
