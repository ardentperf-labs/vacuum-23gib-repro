#!/usr/bin/env python3
"""Create the two explicitly owned shared watchers before parallel AKS creates."""
import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "preflight/shared-network-watchers.json"
state = json.loads(MANIFEST.read_text())
assert state["status"] == "intent", "Shared watcher intent already processed"
token = json.loads(subprocess.check_output([
    "az", "account", "get-access-token", "--resource",
    "https://management.azure.com/", "-o", "json"
], text=True, timeout=30))["accessToken"]

def request(method, resource_id, version, body=None, missing_ok=False):
    request_body = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        "https://management.azure.com" + resource_id + "?api-version=" + version,
        data=request_body, method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            data = response.read()
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        if missing_ok and error.code == 404:
            return None
        raise RuntimeError(f"ARM {method} failed: HTTP {error.code}: {error.read().decode()[:1000]}") from None

def save():
    MANIFEST.write_text(json.dumps(state, indent=2) + "\n")

for region in state["regions"]:
    group_id = "/subscriptions/" + state["subscriptionId"] + "/resourceGroups/" + region["resourceGroup"]
    assert request("GET", group_id, "2022-09-01", missing_ok=True) is None, "Refusing group adoption"
    tags = {"reprotoken": state["ownerToken"], "managed-by": "premium-v2-vacuum-20261006", "reproPurpose": "shared-network-watcher"}
    group = request("PUT", group_id, "2022-09-01", {"location": region["location"], "tags": tags})
    assert group["tags"]["reprotoken"] == state["ownerToken"]
    request("PUT", region["id"], "2025-09-01", {"location": region["location"], "properties": {}, "tags": tags})
    watcher = request("GET", region["id"], "2025-09-01")
    assert watcher["tags"]["reprotoken"] == state["ownerToken"]
    assert watcher["properties"]["provisioningState"] == "Succeeded"
    region["status"] = "ready"
    save()
    print(region["location"] + ": owned shared Network Watcher ready", flush=True)
state["status"] = "ready"
save()
