#!/usr/bin/env python3
"""Validate the pinned retained DATA/WAL backup and render retained CSI imports."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent.parent
RECOVERY = HERE / "recovery"
MANIFEST = HERE / "source-manifest.json"
RECEIPT = RECOVERY / "archive-input" / "receipt.json"
EVIDENCE = RECOVERY / "source-evidence"


def fail(msg):
    raise SystemExit(f"ERROR: {msg}")


def read_json(path):
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        fail(f"cannot read valid JSON {path}: {exc}")


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--short", required=True)
    ap.add_argument("--namespace", default="repro")
    ap.add_argument("--cluster", default="hdd-pg")
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    if not (len(args.token) == 32 and all(c in "0123456789abcdef" for c in args.token)):
        fail("stack owner token must be 32 lowercase hex characters")
    if args.short != args.token[:8]:
        fail("short token does not match the stack owner token")

    manifest = read_json(MANIFEST)
    receipt = read_json(RECEIPT)
    evidence_manifest = read_json(RECOVERY / "source-evidence-manifest.json")
    original_receipt = read_json(EVIDENCE / "archive-receipt.json")
    if receipt != original_receipt:
        fail("private archive-input receipt differs from the preserved original source receipt")
    for item in evidence_manifest.get("files", []):
        path = HERE / item["path"]
        if not path.is_file() or path.stat().st_size != item["bytes"] or sha(path) != item["sha256"]:
            fail(f"preserved source evidence changed: {item['path']}")

    backup_list = read_json(EVIDENCE / "backup.json")
    vss_list = read_json(EVIDENCE / "volumesnapshot.json")
    vsc_list = read_json(EVIDENCE / "volumesnapshotcontent.json")
    backup_items = [x for x in backup_list.get("items", []) if x.get("metadata", {}).get("uid") == manifest.get("backup", {}).get("uid")]
    if len(backup_items) != 1:
        fail("source Backup UID is absent or duplicated in raw evidence")
    backup = backup_items[0]
    bs = backup.get("status", {})
    if (bs.get("phase") != "completed" or bs.get("method") != "volumeSnapshot" or
        bs.get("beginLSN") != manifest["backup"].get("beginLSN") or
        bs.get("endLSN") != manifest["backup"].get("endLSN") or
        bs.get("beginWal") != manifest["backup"].get("beginWal") or
        bs.get("endWal") != manifest["backup"].get("endWal") or
        bs.get("backupLabelFile") != manifest["backup"].get("backupLabelFileBase64")):
        fail("Backup LSN/WAL/label binding differs from the pinned manifest")
    try:
        label = base64.b64decode(bs["backupLabelFile"], validate=True)
    except Exception as exc:
        fail(f"source Backup label is not valid base64: {exc}")
    label_path = RECOVERY / "archive-input" / "backup_label.txt"
    if label_path.read_bytes() != label:
        fail("exported backup_label.txt differs from the source Backup label")
    if sha(label_path) != receipt["backup"]["backupLabelFile"]["sha256"]:
        fail("backup label SHA-256 differs from the export receipt")

    objects = receipt.get("exportedObjects", [])
    if len(objects) != 2:
        fail("expected exactly the verified WAL segment and backup-history object")
    for obj in objects:
        name = Path(obj["objectName"]).name
        path = RECOVERY / "archive-input" / name
        if not path.is_file() or path.stat().st_size != obj["bytes"] or sha(path) != obj["sha256"]:
            fail(f"archived WAL input does not match its export receipt: {name}")
    if not receipt.get("verification", {}).get("allStartThroughEndSegmentsPresent") or not receipt["verification"].get("rangeContiguous"):
        fail("WAL export receipt does not establish a contiguous source backup range")

    snapshot_map = {x.get("snapshot_name"): x for x in receipt.get("snapshots", [])}
    resources = [("data", manifest["sourceData"], "hdd-pg-1", "storage"),
                 ("wal", manifest["pairedWAL"], "hdd-pg-1-wal", "walStorage")]
    items = []
    class_name = f"source-snapshot-{args.short}"
    items.append({
        "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotClass",
        "metadata": {"name": class_name, "labels": {"repro.stack.token": args.token}},
        "driver": "disk.csi.azure.com", "deletionPolicy": "Retain",
    })
    bindings = {}
    for role, source, source_pvc, volume_role in resources:
        source_id = source["sourceSnapshotId"]
        source_uid = source["uniqueId"]
        source_name = source["volumeSnapshotName"]
        source_item = snapshot_map.get(source_name)
        if not source_item or source_item.get("azure_snapshot_id", "").lower() != source_id.lower() or source_item.get("azure_snapshot_unique_id") != source_uid:
            fail(f"{role} snapshot ID/unique ID differs from the archive receipt")
        vss_candidates = [x for x in vss_list.get("items", [])
                          if x.get("kind") == "VolumeSnapshot" and x.get("metadata", {}).get("uid") == source["volumeSnapshotUid"]]
        vsc_candidates = [x for x in vsc_list.get("items", [])
                          if x.get("kind") == "VolumeSnapshotContent" and x.get("metadata", {}).get("uid") == source["volumeSnapshotContentUid"]]
        if len(vss_candidates) != 1 or len(vsc_candidates) != 1:
            fail(f"{role} source VSS/VSC UID is absent or duplicated")
        old_vss, old_vsc = vss_candidates[0], vsc_candidates[0]
        if (old_vss.get("spec", {}).get("source", {}).get("persistentVolumeClaimName") != source_pvc or
            old_vss.get("metadata", {}).get("labels", {}).get("cnpg.io/backupName") != manifest["backup"]["name"] or
            old_vss.get("metadata", {}).get("annotations", {}).get("cnpg.io/backupLabelFile") != bs["backupLabelFile"] or
            old_vsc.get("spec", {}).get("source", {}).get("volumeHandle", "").lower() != source["sourceDiskId"].lower() or
            old_vsc.get("status", {}).get("snapshotHandle", "").lower() != source_id.lower()):
            fail(f"{role} VSS/VSC metadata does not bind to the recorded Backup and snapshot ID")
        if old_vss.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") != "hdd-pg":
            fail(f"{role} source VSS is not labeled for the verified source cluster")
        vss_name = f"source-{role}-{args.short}"
        vsc_name = f"source-content-{role}-{args.short}"
        old_labels = dict(old_vss.get("metadata", {}).get("labels", {}))
        old_labels["repro.stack.token"] = args.token
        annotations = {k: v for k, v in old_vss.get("metadata", {}).get("annotations", {}).items() if k.startswith("cnpg.io/") and k != "cnpg.io/clusterManifest"}
        annotations.update({
            "vacuum-only-repro/source-backup-uid": manifest["backup"]["uid"],
            "vacuum-only-repro/source-volume-snapshot-uid": source["volumeSnapshotUid"],
            "vacuum-only-repro/source-volume-snapshot-content-uid": source["volumeSnapshotContentUid"],
            "vacuum-only-repro/source-snapshot-id": source_id,
            "vacuum-only-repro/source-snapshot-unique-id": source_uid,
            "vacuum-only-repro/source-begin-lsn": manifest["backup"]["beginLSN"],
            "vacuum-only-repro/source-end-lsn": manifest["backup"]["endLSN"],
            "vacuum-only-repro/source-backup-label-sha256": receipt["backup"]["backupLabelFile"]["sha256"],
            "vacuum-only-repro/source-volume-role": volume_role,
        })
        items.append({
            "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent",
            "metadata": {"name": vsc_name, "labels": {"repro.stack.token": args.token}, "annotations": {
                "vacuum-only-repro/source-backup-uid": manifest["backup"]["uid"],
                "vacuum-only-repro/source-volume-snapshot-content-uid": source["volumeSnapshotContentUid"],
                "vacuum-only-repro/source-snapshot-id": source_id,
                "vacuum-only-repro/source-snapshot-unique-id": source_uid,
            }},
            "spec": {"deletionPolicy": "Retain", "driver": "disk.csi.azure.com",
                     "source": {"snapshotHandle": source_id}, "volumeSnapshotClassName": class_name,
                     "volumeSnapshotRef": {"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
                                           "name": vss_name, "namespace": args.namespace}},
        })
        items.append({
            "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
            "metadata": {"name": vss_name, "namespace": args.namespace, "labels": old_labels, "annotations": annotations},
            "spec": {"volumeSnapshotClassName": class_name,
                     "source": {"volumeSnapshotContentName": vsc_name}},
        })
        bindings[role] = {"volumeSnapshot": vss_name, "volumeSnapshotContent": vsc_name,
                          "sourceSnapshotId": source_id, "sourceSnapshotUniqueId": source_uid,
                          "sourceVolumeSnapshotUid": source["volumeSnapshotUid"],
                          "sourceVolumeSnapshotContentUid": source["volumeSnapshotContentUid"],
                          "volumeRole": volume_role}
    result = {"apiVersion": "v1", "kind": "List", "items": items}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    args.output.chmod(0o600)
    bindings_path = args.output.with_name("source-bindings.json")
    bindings_doc = {"version": 1, "sourceBackupName": manifest["backup"]["name"],
                    "sourceBackupUid": manifest["backup"]["uid"],
                    "beginLSN": manifest["backup"]["beginLSN"], "endLSN": manifest["backup"]["endLSN"],
                    "beginWal": manifest["backup"]["beginWal"], "endWal": manifest["backup"]["endWal"],
                    "backupLabelFileSha256": receipt["backup"]["backupLabelFile"]["sha256"],
                    "republishedArchiveObjects": objects, "bindings": bindings}
    bindings_path.write_text(json.dumps(bindings_doc, indent=2, sort_keys=True) + "\n")
    bindings_path.chmod(0o600)
    print(f"Validated paired retained source: Backup UID {manifest['backup']['uid']}; DATA and WAL IDs, labels, LSNs, backup label, and archive SHA-256s match.")
    print(f"Rendered retained source VolumeSnapshot imports to {args.output}")


if __name__ == "__main__":
    main()
