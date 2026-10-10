#!/usr/bin/env python3
"""Remove only this stack's generated Network Watcher after all stacks are gone.

This is intentionally a separate recovery step. It uses the configured Azure CLI
executable (including the credential broker in PATH), never reads access tokens,
defaults to a read-only dry run, and requires both --apply and --all-stacks-gone.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

API_VERSION = "2025-09-01"
TOKEN_RE = re.compile(r"^[a-f0-9]{32}$")
GUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


class Refusal(RuntimeError):
    pass


class AzError(Refusal):
    def __init__(self, operation: str, code: str):
        self.code = code
        super().__init__(f"Azure CLI {operation} failed ({code or 'unclassified error'}).")


class AzureCLI:
    def __init__(self, subscription: str):
        self.subscription = subscription

    def run(self, *args: str, timeout: int = 180) -> str:
        cmd = ["az", *args, "--subscription", self.subscription, "--only-show-errors"]
        try:
            p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            raise Refusal(f"Azure CLI call failed or timed out ({args[0]}).") from e
        if p.returncode:
            # Do not echo the CLI's stderr: it can contain account or request data.
            m = re.search(r"\b(ResourceNotFound|NotFound|404|ScopeLocked|AuthorizationFailed|InvalidAuthenticationToken|OperationNotFound)\b", p.stderr, re.I)
            raise AzError(args[0], m.group(1) if m else f"exit-{p.returncode}")
        return p.stdout

    def json(self, *args: str, timeout: int = 180) -> Any:
        try:
            return json.loads(self.run(*args, timeout=timeout))
        except json.JSONDecodeError as e:
            raise Refusal(f"Azure CLI returned invalid JSON for {args[0]}.") from e


def _lc(value: str) -> str:
    return value.casefold()


def validate_state(s: dict[str, Any]) -> dict[str, Any]:
    token = s.get("ownerToken")
    sub = s.get("subscriptionId")
    loc = s.get("location")
    nw = s.get("networkWatcher")
    if not isinstance(token, str) or not TOKEN_RE.fullmatch(token):
        raise Refusal("Saved stack ownership token is invalid.")
    if not isinstance(sub, str) or not GUID_RE.fullmatch(sub):
        raise Refusal("Saved subscription identity is invalid.")
    if not isinstance(loc, str) or not loc:
        raise Refusal("Saved location is missing.")
    if not isinstance(nw, dict) or nw.get("owned") is not True:
        raise Refusal("State does not prove that this stack owns a Network Watcher.")
    rg = f"rg-vacuum-watcher-{token}"
    name = f"vacuum-watcher-{token[:12]}"
    rid = f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkWatchers/{name}"
    checks = {
        "ownerToken": token,
        "resourceGroup": rg,
        "name": name,
        "location": loc,
        "id": rid,
    }
    for key, expected in checks.items():
        actual = nw.get(key)
        if not isinstance(actual, str) or _lc(actual) != _lc(expected):
            raise Refusal(f"Saved owned-watcher {key} does not match this stack's generated identity.")
    baseline = s.get("networkWatcherBaseline")
    if not isinstance(baseline, dict) or not isinstance(baseline.get("ids"), list):
        raise Refusal("Network Watcher baseline is missing; cannot prove the target was created by this stack.")
    baseline_ids = baseline["ids"]
    if any(not isinstance(x, str) for x in baseline_ids):
        raise Refusal("Network Watcher baseline contains an invalid resource ID.")
    if _lc(rid) in {_lc(x) for x in baseline_ids}:
        raise Refusal("Generated watcher appears in the pre-provision baseline; refusing cleanup.")
    if _lc(baseline.get("location", "")) != _lc(loc):
        raise Refusal("Network Watcher baseline location differs from the saved stack location.")
    if nw.get("status") not in {"creating", "ready"}:
        raise Refusal("Owned-watcher state is not a recognized creation state.")
    etag = nw.get("etag")
    if etag is not None and (not isinstance(etag, str) or not etag):
        raise Refusal("Saved watcher ETag is malformed.")
    if nw.get("status") == "ready" and not etag:
        raise Refusal("Ready watcher state has no saved ETag.")
    return {"subscription": sub, "location": loc, "token": token,
            "resourceGroup": rg, "name": name, "id": rid,
            "baselineIds": baseline_ids, "savedEtag": etag}


def _as_json(cli: AzureCLI, *args: str) -> Any:
    return cli.json(*args)


def check_live(cli: AzureCLI, s: dict[str, Any]) -> tuple[bool, str | None]:
    """Prove exact ownership and scope; return (group_exists, live_etag)."""
    x = validate_state(s)
    active = cli.run("account", "show", "--query", "id", "-o", "tsv").strip()
    if _lc(active) != _lc(x["subscription"]):
        raise Refusal("Active subscription differs from the saved stack subscription.")

    group_exists = cli.run("group", "exists", "-n", x["resourceGroup"], "-o", "tsv").strip()
    if group_exists not in {"true", "false"}:
        raise Refusal("Azure returned an unexpected watcher resource-group existence result.")
    if group_exists == "false":
        return False, None

    group = _as_json(cli, "group", "show", "-n", x["resourceGroup"], "-o", "json")
    tags = group.get("tags") or {}
    if (_lc(str(group.get("name", ""))) != _lc(x["resourceGroup"]) or
        _lc(str(group.get("location", ""))) != _lc(x["location"]) or
        tags.get("reprotoken") != x["token"] or
        tags.get("reproPurpose") != "owned-network-watcher" or
        tags.get("managed-by") != "vacuum-only-repro"):
        raise Refusal("Watcher resource-group ownership or location changed; preserving it.")

    resources = _as_json(cli, "resource", "list", "-g", x["resourceGroup"], "-o", "json")
    if not isinstance(resources, list):
        raise Refusal("Watcher resource-group inventory is invalid.")
    for resource in resources:
        if _lc(str(resource.get("id", ""))) != _lc(x["id"]):
            raise Refusal("Watcher resource group contains an unexpected resource; preserving the group.")

    locks = _as_json(cli, "lock", "list", "--resource-group", x["resourceGroup"], "-o", "json")
    if not isinstance(locks, list) or locks:
        raise Refusal("Watcher resource group has unknown locks; preserving it.")
    rg_id = f"/subscriptions/{x['subscription']}/resourceGroups/{x['resourceGroup']}"
    assignments = _as_json(cli, "role", "assignment", "list", "--scope", rg_id, "-o", "json")
    if not isinstance(assignments, list):
        raise Refusal("Watcher resource-group role-assignment inventory is invalid.")
    if any(_lc(str(a.get("scope", ""))) == _lc(rg_id) for a in assignments):
        raise Refusal("Watcher resource group has an unexpected direct role assignment; preserving it.")

    # Keep the initial regional set stable. This refuses cleanup if an unowned
    # watcher appeared during provisioning, while never targeting it for deletion.
    watchers = _as_json(cli, "network", "watcher", "list", "-o", "json")
    if not isinstance(watchers, list):
        raise Refusal("Regional Network Watcher inventory is invalid.")
    regional = {_lc(str(w.get("id", ""))) for w in watchers
                if _lc(str(w.get("location", ""))) == _lc(x["location"])}
    expected = {_lc(w) for w in x["baselineIds"]} | {_lc(x["id"])}
    if regional != expected:
        raise Refusal("Regional Network Watcher set changed; preserving every watcher for review.")

    try:
        live = _as_json(cli, "resource", "show", "--ids", x["id"], "-o", "json")
    except AzError as e:
        # An already-removed exact watcher is safe only if the owned group is empty.
        if e.code.casefold() not in {"resourcenotfound", "notfound", "404"} or resources:
            raise
        return True, None
    live_tags = live.get("tags") or {}
    if (_lc(str(live.get("id", ""))) != _lc(x["id"]) or
        _lc(str(live.get("type", ""))) != "microsoft.network/networkwatchers" or
        _lc(str(live.get("location", ""))) != _lc(x["location"]) or
        live.get("properties", {}).get("provisioningState") != "Succeeded" or
        live_tags.get("reprotoken") != x["token"] or
        live_tags.get("reproPurpose") != "owned-vacuum-repro-watcher"):
        raise Refusal("Live watcher identity, readiness, or ownership tag changed.")
    live_etag = live.get("etag")
    if not isinstance(live_etag, str) or not live_etag:
        raise Refusal("Live watcher has no ETag; preserving it.")
    if x["savedEtag"] and live_etag != x["savedEtag"]:
        raise Refusal("Live watcher ETag changed since ownership state was saved.")
    return True, live_etag


def remove(cli: AzureCLI, state: dict[str, Any], apply: bool) -> str:
    x = validate_state(state)
    group_exists, etag = check_live(cli, state)
    if not group_exists:
        return "already-absent"
    if not apply:
        return "verified-dry-run"

    if etag:
        url = f"https://management.azure.com{x['id']}?api-version={API_VERSION}"
        cli.run("rest", "--method", "delete", "--url", url,
                "--headers", f"If-Match={etag}", "-o", "none", timeout=240)
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            try:
                cli.json("resource", "show", "--ids", x["id"], "-o", "json")
            except AzError as e:
                if e.code.casefold() not in {"resourcenotfound", "notfound", "404"}:
                    raise
                break
            time.sleep(10)
        else:
            raise Refusal("Owned watcher deletion did not reach 404 before timeout.")

    # Re-inventory immediately before group deletion. Any child resource blocks it.
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        resources = _as_json(cli, "resource", "list", "-g", x["resourceGroup"], "-o", "json")
        if not isinstance(resources, list):
            raise Refusal("Watcher resource-group inventory is invalid after watcher deletion.")
        if not resources:
            break
        if any(_lc(str(r.get("id", ""))) != _lc(x["id"]) for r in resources):
            raise Refusal("Watcher resource group contains an unexpected resource; preserving it.")
        time.sleep(10)
    else:
        raise Refusal("Watcher resource group still lists the exact watcher after deletion; preserving it.")
    locks = _as_json(cli, "lock", "list", "--resource-group", x["resourceGroup"], "-o", "json")
    if locks:
        raise Refusal("Watcher resource group gained a lock; preserving it.")
    assignments = _as_json(cli, "role", "assignment", "list", "--scope",
        f"/subscriptions/{x['subscription']}/resourceGroups/{x['resourceGroup']}", "-o", "json")
    if not isinstance(assignments, list):
        raise Refusal("Watcher resource-group role-assignment inventory is invalid after watcher deletion.")
    if any(_lc(str(a.get("scope", ""))) == _lc(f"/subscriptions/{x['subscription']}/resourceGroups/{x['resourceGroup']}")
           for a in assignments):
        raise Refusal("Watcher resource group gained a direct role assignment; preserving it.")
    group = _as_json(cli, "group", "show", "-n", x["resourceGroup"], "-o", "json")
    tags = group.get("tags") or {}
    if (_lc(str(group.get("name", ""))) != _lc(x["resourceGroup"]) or
        _lc(str(group.get("location", ""))) != _lc(x["location"]) or
        tags.get("reprotoken") != x["token"] or
        tags.get("reproPurpose") != "owned-network-watcher" or
        tags.get("managed-by") != "vacuum-only-repro"):
        raise Refusal("Watcher resource-group ownership changed before deletion.")
    cli.run("group", "delete", "-n", x["resourceGroup"], "--yes", "--no-wait", "-o", "none", timeout=240)
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        exists = cli.run("group", "exists", "-n", x["resourceGroup"], "-o", "tsv").strip()
        if exists == "false":
            return "deleted"
        if exists != "true":
            raise Refusal("Azure returned an unexpected watcher-group deletion state.")
        time.sleep(10)
    raise Refusal("Owned empty watcher resource-group deletion timed out.")


def _self_test() -> None:
    token = "a" * 32
    sub = "00000000-0000-0000-0000-000000000000"
    rg = f"rg-vacuum-watcher-{token}"
    name = f"vacuum-watcher-{token[:12]}"
    rid = f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkWatchers/{name}"
    base = {"ownerToken": token, "subscriptionId": sub, "location": "eastus",
            "networkWatcherBaseline": {"location": "eastus", "ids": []},
            "networkWatcher": {"owned": True, "status": "ready", "id": rid,
                "resourceGroup": rg, "name": name, "location": "eastus",
                "ownerToken": token, "etag": "etag-1"}}
    validate_state(base)
    def rejected(s: dict[str, Any]) -> bool:
        try:
            validate_state(s)
        except Refusal:
            return True
        return False
    bad = json.loads(json.dumps(base)); bad["networkWatcher"]["ownerToken"] = "b" * 32
    assert rejected(bad), "wrong ownership token accepted"
    bad = json.loads(json.dumps(base)); bad["networkWatcherBaseline"]["ids"] = [rid]
    assert rejected(bad), "pre-existing watcher accepted as stack-owned"
    bad = json.loads(json.dumps(base)); bad["networkWatcher"]["etag"] = ""
    assert rejected(bad), "changed/missing ETag accepted for ready watcher"
    # A foreign resource in the dedicated group is rejected by a fake CLI before
    # any mutation. Keep the behavioral assertion offline and credential-free.
    class FakeCLI:
        def __init__(self, foreign: bool, live_etag: str = "etag-1"):
            self.foreign = foreign; self.live_etag = live_etag
        def run(self, *args: str, **kwargs: Any) -> str:
            if args[:2] == ("account", "show"): return sub
            if args[:2] == ("group", "exists"): return "true"
            raise AssertionError(f"unexpected call {args[:2]}")
        def json(self, *args: str, **kwargs: Any) -> Any:
            if args[:2] == ("group", "show"):
                return {"name": rg, "location": "eastus", "tags": {
                    "reprotoken": token, "reproPurpose": "owned-network-watcher",
                    "managed-by": "vacuum-only-repro"}}
            if args[:2] == ("resource", "list"):
                return [{"id": "/subscriptions/other/resourceGroups/rg/providers/Microsoft.Compute/disks/foreign"}] if self.foreign else []
            if args[:2] == ("lock", "list"):
                return []
            if args[:3] == ("role", "assignment", "list"):
                return []
            if args[:3] == ("network", "watcher", "list"):
                return [{"id": rid, "location": "eastus"}]
            if args[:2] == ("resource", "show"):
                return {"id": rid, "type": "Microsoft.Network/networkWatchers",
                    "location": "eastus", "etag": self.live_etag,
                    "properties": {"provisioningState": "Succeeded"},
                    "tags": {"reprotoken": token, "reproPurpose": "owned-vacuum-repro-watcher"}}
            raise AssertionError(f"unexpected call {args[:2]}")
    try:
        check_live(FakeCLI(True), base)  # type: ignore[arg-type]
    except Refusal:
        pass
    else:
        raise AssertionError("unexpected resource accepted")
    assert check_live(FakeCLI(False), base) == (True, "etag-1")  # type: ignore[arg-type]
    try:
        check_live(FakeCLI(False, "etag-2"), base)  # type: ignore[arg-type]
    except Refusal:
        pass
    else:
        raise AssertionError("changed live ETag accepted")
    print("cleanup-owned-watcher self-test: PASS")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", type=Path, default=Path(__file__).parents[1] / "run" / ".state" / "state.json")
    p.add_argument("--apply", action="store_true", help="perform exact owned watcher and empty owned RG cleanup")
    p.add_argument("--all-stacks-gone", action="store_true", help="assert every experiment stack sharing this watcher is gone")
    p.add_argument("--self-test", action="store_true", help="run offline safety tests; no Azure calls")
    a = p.parse_args()
    if a.self_test:
        _self_test(); return 0
    if a.apply and not a.all_stacks_gone:
        raise Refusal("--apply also requires --all-stacks-gone.")
    if not a.state.is_file():
        raise Refusal("Saved ownership state is missing; refusing to guess.")
    state = json.loads(a.state.read_text(encoding="utf-8"))
    x = validate_state(state)
    if a.apply:
        lock_path = a.state.parent / "teardown.lock"
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as e:
                raise Refusal("Another teardown or watcher cleanup is active.") from e
            status = remove(AzureCLI(x["subscription"]), state, True)
    else:
        status = remove(AzureCLI(x["subscription"]), state, False)
    print(json.dumps({"status": status, "watcherId": x["id"],
                      "resourceGroup": x["resourceGroup"], "mutated": bool(a.apply)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        raise SystemExit(2)
