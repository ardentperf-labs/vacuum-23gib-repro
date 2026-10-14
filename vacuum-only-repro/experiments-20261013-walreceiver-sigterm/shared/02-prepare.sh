#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Direct, read-only-source restore path for a fresh stack. Copy this file to a
# stack as 02-prepare.sh. Unlike the legacy CSI-import path, this copies the
# pinned snapshots to owned Standard_LRS disks with the Azure CLI caller's
# identity, then statically binds CNPG's pending primary PVCs to those disks.
# It creates no source grants, edits no locks, and creates/deletes no snapshots.

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SHARED="$ROOT/../shared"
PROJECT="$(cd -- "$ROOT/../../.." && pwd)"
STACK="$(basename -- "$ROOT")"
SD="$ROOT/.state"
STATE="$SD/state.json"
PAIR="$SHARED/source-pair.json"
PROOF="$SHARED/source-pair-proof.json"
BACKUP="$SHARED/source-backup-status.json"
AUDIT="$SHARED/source-restore-audit.json"
GLOBAL_SOURCE_LOCK="$PROJECT/.state/walreceiver-source-restore.lock"

NS=repro
C=hdd-pg
SYSTEM_IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
MEASUREMENT_IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
ROLE_ID=7efff54f-a5b4-42b5-a1c5-5411624893ce

APPLY=false
TMP=''
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
prepare_exit_trap(){
  local rc=$?
  trap - EXIT
  if [[ -n "$TMP" && -d "$TMP" ]]; then
    if ((rc==0)); then rm -rf -- "$TMP"; else printf 'Preparation intermediates retained at %s\n' "$TMP" >&2; fi
  fi
  exit "$rc"
}
azj(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB" -o json; }
k(){ timeout --foreground --kill-after=15s 180s kubectl --kubeconfig "$KC" "$@"; }
sql(){ k exec -i -c postgres -n "$NS" "$1" -- psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
lower(){ tr '[:upper:]' '[:lower:]' <<<"$1"; }

if [[ "${1:-}" == --apply && $# == 1 ]]; then APPLY=true
elif [[ $# == 0 ]]; then :
else die 'Usage: 02-prepare.sh [--apply]'; fi

[[ "$STACK" == sigterm || "$STACK" == control ]] || die 'Run from an isolated sigterm/ or control/ stack directory.'
for x in az jq python3 sha256sum timeout kubectl flock; do command -v "$x" >/dev/null || die "Missing $x."; done
for f in "$PAIR" "$PROOF" "$BACKUP" "$AUDIT"; do [[ -s "$f" ]] || die "Pinned EastUS source evidence is missing: $f"; done
[[ -s "$STATE" ]] || die 'Run 01-provision.sh first.'

SUB="$(jq -er '.subscriptionId' "$PAIR")"
SOURCE_RG="$(jq -er '.resourceGroup' "$PAIR")"
SOURCE_RG_TOKEN="$(jq -er '.resourceGroupOwnerToken' "$PAIR")"
SOURCE_DATA="$(jq -er '.data.id' "$PAIR")"
SOURCE_WAL="$(jq -er '.wal.id' "$PAIR")"
SOURCE_DATA_UID="$(jq -er '.data.uniqueId' "$PAIR")"
SOURCE_WAL_UID="$(jq -er '.wal.uniqueId' "$PAIR")"
SOURCE_DATA_DISK="$(jq -er '.data.sourceDiskId' "$PAIR")"
SOURCE_WAL_DISK="$(jq -er '.wal.sourceDiskId' "$PAIR")"
BACKUP_NAME="$(jq -er '.backup.name' "$PAIR")"
BACKUP_UID="$(jq -er '.backup.uid' "$PAIR")"
SYSTEM_ID="$(jq -er '.fixture.systemIdentifier' "$PAIR")"
TARGET_HEAP="$(jq -er '.fixture.heapBytes' "$PAIR")"
TARGET_TABLE="$(jq -er '.fixture.tableBytes' "$PAIR")"
TOKEN="$(jq -er '.ownerToken' "$STATE")"
RG="$(jq -er '.resourceGroup' "$STATE")"
AKS="$(jq -er '.clusterName' "$STATE")"
LOC="$(jq -er '.location' "$STATE")"
NODE_RG="$(jq -er '.nodeResourceGroup' "$STATE")"
KC="$(jq -er '.kubeconfigPath' "$STATE")"
[[ "$SUB" == "$(jq -er '.subscriptionId' "$STATE")" && "${LOC,,}" == eastus ]] || die 'Stack subscription/location differs from the pinned EastUS pair.'
[[ "$KC" == "$SD/kubeconfig" && -s "$KC" ]] || die 'Kubeconfig is not the isolated per-stack file.'
[[ "$(jq -r '.status' "$STATE")" == ready ]] || die 'AKS stack is not ready.'
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$NODE_RG" == "rg-vacuum-nodes-$TOKEN" ]] || die 'Stack resource groups do not match its exact owner-token names.'
[[ "$(az account show --query id -o tsv)" == "$SUB" ]] || die 'Active Azure subscription does not match the pinned source pair.'

[[ "$(sha256sum "$PAIR" | awk '{print $1}')" == "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" ]] || die 'Pinned source-pair metadata hash changed.'
[[ "$(sha256sum "$BACKUP" | awk '{print $1}')" == "$(jq -er '.provenance.copiedBackupStatusSha256' "$PROOF")" ]] || die 'Pinned backup-status hash changed.'
[[ "$(sha256sum "$AUDIT" | awk '{print $1}')" == "$(jq -er '.provenance.copiedRestoreAuditSha256' "$PROOF")" ]] || die 'Pinned restore-audit hash changed.'
jq -e --arg sub "$SUB" --arg d "$(lower "$SOURCE_DATA")" --arg w "$(lower "$SOURCE_WAL")" \
  --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" --arg sys "$SYSTEM_ID" --arg rg "$(lower "$SOURCE_RG")" \
  '.version==1 and .subscriptionId==$sub and .region=="eastus" and .diskSizeGiB==512 and .kind=="offline-postgresql-pair" and (.resourceGroup|ascii_downcase)==$rg and (.data.id|ascii_downcase)==$d and (.wal.id|ascii_downcase)==$w and .data.uniqueId==$du and .wal.uniqueId==$wu and .fixture.systemIdentifier==$sys and .fixture.heapBytes==24696061952 and .fixture.tableBytes==25040748544 and .fixture.sourceSnapshotBeforeVacuum==true and .restoreEvidence.status=="complete" and .restoreEvidence.sourceSnapshotsUntouched==true and .restoreEvidence.replicaRestoreCompleted==true and .restoreEvidence.replicaInRecovery==true and .restoreEvidence.replicaSystemIdentifier==$sys and .restoreEvidence.replicaHeapBytes==24696061952 and .restoreEvidence.externalWalArchiveRequired==false' "$PAIR" >/dev/null || die 'Pinned pair does not prove the selected pre-VACUUM 23-GiB fixture.'
jq -e --arg name "$BACKUP_NAME" --arg uid "$BACKUP_UID" \
  '.kind=="Backup" and .spec.method=="volumeSnapshot" and .spec.online==false and .status.phase=="completed" and .status.backupName==$name and .metadata.uid==$uid and ([.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA" and .name==$name)]|length)==1 and ([.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL" and .name==($name+"-wal"))]|length)==1 and .status.beginLSN==.status.endLSN' "$BACKUP" >/dev/null || die 'Pinned offline CNPG backup metadata is incomplete or not a matched pair.'
jq -e '.status=="complete" and .sourceSnapshotsUntouched==true and (.primaryAndReplicaSnapshots.records|length)==2' "$AUDIT" >/dev/null || die 'Pinned snapshot restore audit is incomplete.'
for kind in data wal; do
  if [[ "$kind" == data ]]; then expected="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; disk_uid="$(jq -er '.data.sourceDiskUniqueId' "$PAIR")"; role=PG_DATA
  else expected="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; disk_uid="$(jq -er '.wal.sourceDiskUniqueId' "$PAIR")"; role=PG_WAL; fi
  jq -e --arg id "$(lower "$expected")" --arg uid "$uid" --arg role "$role" --arg backup "$BACKUP_UID" \
    'any(.primaryAndReplicaSnapshots.records[];(.id|ascii_downcase)==$id and .uniqueId==$uid and .type==$role and .backupUid==$backup)' "$AUDIT" >/dev/null || die "Restore audit does not bind the exact $kind snapshot to the offline backup."
done

[[ ! -e "$SD/prepare.json" ]] || die 'prepare.json already exists; refusing to reuse a completed stack.'
for p in "$SD/source-snapshot-role-assignments.json" "$SD/source-snapshot-lock-cycle.json" "$SD/source-snapshot-lock-cycles.jsonl" "$SD/direct-restore/source-preflight.json" "$SD/direct-restore/binding.json"; do [[ ! -e "$p" ]] || die "Prior grant/lock/binding evidence exists at $p; use a fresh owned stack."; done

NSOBJ="$(k get namespace "$NS" -o json)" || die 'The repro namespace is missing.'
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' <<<"$NSOBJ" >/dev/null || die 'Namespace ownership label mismatch.'
SC="$(k get storageclass azure-standard-hdd -o json)" || die 'Standard HDD StorageClass is missing.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .reclaimPolicy=="Delete" and .allowVolumeExpansion==true and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$SC" >/dev/null || die 'StorageClass must be the owned uncached Standard_LRS StorageClass.'
if [[ -e "$ROOT/verify-attachments.py" ]]; then
  cmp -s "$SHARED/verify-attachments.py" "$ROOT/verify-attachments.py" || die 'Stack-local attachment verifier differs from the pinned shared verifier.'
else
  install -m 700 "$SHARED/verify-attachments.py" "$ROOT/verify-attachments.py"
fi

SHORT="${TOKEN:0:8}"
VSC="source-pair-$SHORT"; VCD="source-data-content-$SHORT"; VCW="source-wal-content-$SHORT"
VSD="source-data-$SHORT"; VSW="source-wal-$SHORT"
for item in "volumesnapshotclass:$VSC" "volumesnapshotcontent:$VCD" "volumesnapshotcontent:$VCW" "volumesnapshot:$VSD" "volumesnapshot:$VSW" "cluster:$C" "pvc:hdd-pg-1" "pvc:hdd-pg-1-wal"; do
  kind="${item%%:*}"; name="${item#*:}"
  if [[ "$kind" == volumesnapshotcontent || "$kind" == volumesnapshotclass ]]; then
    if k get "$kind" "$name" -o json >/dev/null 2>&1; then die "$kind/$name already exists; refusing to adopt it."; fi
  elif k get "$kind" "$name" -n "$NS" -o json >/dev/null 2>&1; then
    die "$kind/$name already exists; refusing to adopt it."
  fi
done

# Read-only identity, lock, and permission check. Shared flock blocks any
# legacy source-lock maintenance cycle, while allowing the two direct-copy
# stacks to validate the retained source concurrently.
mkdir -p "$PROJECT/.state"; chmod 700 "$PROJECT/.state"
exec 8>"$GLOBAL_SOURCE_LOCK"; flock -s 8
rg_live="$(azj group show -n "$SOURCE_RG")" || die 'Cannot inspect retained source resource group.'
jq -e --arg name "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and (.location|ascii_downcase)=="eastus" and .tags.reprotoken==$token' <<<"$rg_live" >/dev/null || die 'Retained source resource group identity or ownership changed.'
rg_locks="$(azj lock list --resource-group "$SOURCE_RG")" || die 'Cannot inspect retained source resource-group lock.'
jq -e --arg id "$(lower "$(jq -er '.resourceGroupLock.id' "$PAIR")")" --arg name "$(jq -er '.resourceGroupLock.name' "$PAIR")" --arg notes "$(jq -er '.resourceGroupLock.notes' "$PAIR")" \
  '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$rg_locks" >/dev/null || die 'Retained source resource-group CanNotDelete lock differs from the pinned definition.'

aks_obj="$(azj aks show -g "$RG" -n "$AKS")" || die 'Cannot inspect this stack AKS identity.'
PRINCIPAL="$(jq -er '.identity.principalId|select(type=="string" and length>0)' <<<"$aks_obj")" || die 'AKS control-plane principal is missing.'
source_verification='[]'
for kind in data wal; do
  if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; disk_uid="$(jq -er '.data.sourceDiskUniqueId' "$PAIR")"; else sid="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; disk_uid="$(jq -er '.wal.sourceDiskUniqueId' "$PAIR")"; fi
  live="$(azj snapshot show --ids "$sid")" || die "Cannot inspect retained source $kind snapshot."
  jq -e --arg id "$(lower "$sid")" --arg uid "$uid" --arg disk "$(lower "$disk")" --arg diskUid "$disk_uid" --arg rg "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" \
    '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)=="eastus" and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .completionPercent==100 and .diskSizeGB==512 and .incremental==true and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk and .creationData.sourceUniqueId==$diskUid and .tags.reprotoken==$token and (.tags.source_volume_id|ascii_downcase)==$disk' <<<"$live" >/dev/null || die "Retained $kind snapshot identity, availability, or pair metadata mismatch."
  locks="$(azj lock list --resource "$sid")" || die "Cannot inspect retained $kind snapshot lock."
  jq -e --arg id "$(lower "$(jq -er --arg k "$kind" '.[$k].lock.id' "$PAIR")")" --arg name "$(jq -er --arg k "$kind" '.[$k].lock.name' "$PAIR")" --arg notes "$(jq -er --arg k "$kind" '.[$k].lock.notes' "$PAIR")" \
    '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$locks" >/dev/null || die "Retained $kind snapshot lock differs from the pinned definition."
  # This mode never creates a source role grant. Refuse if the exact grant is
  # already present for this AKS identity rather than silently adopting it.
  roles="$(azj role assignment list --scope "$sid" --fill-principal-name false)" || die "Cannot inspect $kind snapshot permissions."
  jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$sid")" \
    'all(.[];.principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$roles" >/dev/null || die "An exact AKS Disk Snapshot Contributor grant already exists at the $kind snapshot scope."
  record="$(jq -cn --arg kind "$kind" --argjson snapshot "$live" --argjson lock "$(jq -c --arg k "$kind" '.[$k].lock' "$PAIR")" '{kind:$kind,snapshotId:$snapshot.id,uniqueId:$snapshot.uniqueId,sourceDiskId:$snapshot.creationData.sourceResourceId,provisioningState:$snapshot.provisioningState,snapshotAccessState:$snapshot.snapshotAccessState,completionPercent:$snapshot.completionPercent,diskSizeGB:$snapshot.diskSizeGB,sku:$snapshot.sku.name,directLock:$lock,aksExactSourceGrantAbsent:true}')"
  source_verification="$(jq -cn --argjson all "$source_verification" --argjson item "$record" '$all+[$item]')"
done

if [[ "$APPLY" != true ]]; then
  flock -u 8
  python3 "$SHARED/direct-restore-disks.py" --stack "$STACK" --owner-token "$TOKEN" --create-only
  jq -n --arg stack "$STACK" --arg token "$TOKEN" --arg pair "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" --argjson sources "$source_verification" \
    '{mode:"dry-run",stack:$stack,ownerToken:$token,sourcePairSha256:$pair,sourceGrantMode:"not-created",sourceLocksModified:false,sources:$sources,planned:"Copy the pinned DATA/WAL snapshots to owned Standard_LRS disks; create static VolumeSnapshot lineage; bootstrap one primary; bind its exact CNPG PVC UIDs directly; verify the 23-GiB fixture; switch image; finalize streaming replica."}'
  exit 0
fi

mkdir -p "$SD"; chmod 700 "$SD"
for name in source-pair.json source-pair-proof.json source-backup-status.json source-restore-audit.json; do
  case "$name" in
    source-pair.json) from="$PAIR";; source-pair-proof.json) from="$PROOF";;
    source-backup-status.json) from="$BACKUP";; source-restore-audit.json) from="$AUDIT";;
  esac
  [[ ! -e "$SD/$name" ]] || { cmp -s "$from" "$SD/$name" || die "Private source evidence already differs: $SD/$name"; }
  [[ -e "$SD/$name" ]] || install -m 600 "$from" "$SD/$name"
done
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg pair "$(sha256sum "$SD/source-pair.json"|awk '{print $1}')" \
  --arg proof "$(sha256sum "$SD/source-pair-proof.json"|awk '{print $1}')" --arg backup "$(sha256sum "$SD/source-backup-status.json"|awk '{print $1}')" --arg audit "$(sha256sum "$SD/source-restore-audit.json"|awk '{print $1}')" \
  '{version:1,copiedAtUtc:$at,files:{pair:$pair,proof:$proof,backupStatus:$backup,restoreAudit:$audit}}' >"$SD/source-evidence.json"
chmod 600 "$SD/source-evidence.json"
mkdir -p "$SD/direct-restore"; chmod 700 "$SD/direct-restore"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg pairSha "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" --arg principal "$PRINCIPAL" --argjson sources "$source_verification" \
  '{version:1,status:"verified",verifiedAtUtc:$at,sourcePairSha256:$pairSha,aksPrincipalId:$principal,roleAssignmentsCreated:false,exactAssignmentsAbsent:true,sourceLocksModified:false,sources:$sources}' >"$SD/direct-restore/source-preflight.json"
chmod 600 "$SD/direct-restore/source-preflight.json"

# The Azure CLI identity copies the exact protected source snapshots into the
# stack-owned node resource group; the AKS identity receives no new permission.
python3 "$SHARED/direct-restore-disks.py" --stack "$STACK" --owner-token "$TOKEN" --create-only --apply
flock -u 8

TMP="$(mktemp -d "$SD/direct-prepare.XXXXXX")"
trap prepare_exit_trap EXIT
cat >"$TMP/source-pair.yaml" <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: $VSC
  labels: {repro.stack.token: "$TOKEN"}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters: {resourceGroup: $SOURCE_RG}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata: {name: $VCD, labels: {repro.stack.token: "$TOKEN"}}
spec:
  deletionPolicy: Retain
  driver: disk.csi.azure.com
  source: {snapshotHandle: "$SOURCE_DATA"}
  volumeSnapshotClassName: $VSC
  volumeSnapshotRef: {kind: VolumeSnapshot, name: $VSD, namespace: $NS}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $VSD, namespace: $NS, labels: {repro.stack.token: "$TOKEN", cnpg.io/pvcRole: PG_DATA, cnpg.io/backupName: "$BACKUP_NAME"}}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: $VCD}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata: {name: $VCW, labels: {repro.stack.token: "$TOKEN"}}
spec:
  deletionPolicy: Retain
  driver: disk.csi.azure.com
  source: {snapshotHandle: "$SOURCE_WAL"}
  volumeSnapshotClassName: $VSC
  volumeSnapshotRef: {kind: VolumeSnapshot, name: $VSW, namespace: $NS}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $VSW, namespace: $NS, labels: {repro.stack.token: "$TOKEN", cnpg.io/pvcRole: PG_WAL, cnpg.io/backupName: "$BACKUP_NAME"}}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: $VCW}}
EOF
k apply -f "$TMP/source-pair.yaml" >/dev/null || die 'Cannot create this stack-owned static source-lineage pair.'

# Static VolumeSnapshotContent binding is Kubernetes metadata only. Do not wait
# for readyToUse: Azure CSI may be denied source access, but the existing
# populated disks are mounted via statically bound PVs below.
end=$((SECONDS+180))
while ((SECONDS<end)); do
  vss_data="$(k get volumesnapshot "$VSD" -n "$NS" -o json)" || die 'Cannot read the static DATA VolumeSnapshot.'
  vsc_data="$(k get volumesnapshotcontent "$VCD" -o json)" || die 'Cannot read the static DATA VolumeSnapshotContent.'
  vss_wal="$(k get volumesnapshot "$VSW" -n "$NS" -o json)" || die 'Cannot read the static WAL VolumeSnapshot.'
  vsc_wal="$(k get volumesnapshotcontent "$VCW" -o json)" || die 'Cannot read the static WAL VolumeSnapshotContent.'
  jq -e --arg t "$TOKEN" --arg c "$VCD" '.metadata.labels["repro.stack.token"]==$t and .spec.source.volumeSnapshotContentName==$c' <<<"$vss_data" >/dev/null || die 'DATA VolumeSnapshot ownership/source spec changed.'
  jq -e --arg t "$TOKEN" --arg c "$VCW" '.metadata.labels["repro.stack.token"]==$t and .spec.source.volumeSnapshotContentName==$c' <<<"$vss_wal" >/dev/null || die 'WAL VolumeSnapshot ownership/source spec changed.'
  jq -e --arg t "$TOKEN" --arg id "$(lower "$SOURCE_DATA")" --arg n "$VSD" --arg ns "$NS" --arg uid "$(jq -r '.metadata.uid' <<<"$vss_data")" \
    '.metadata.labels["repro.stack.token"]==$t and .spec.driver=="disk.csi.azure.com" and .spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$id and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and ((.spec.volumeSnapshotRef.uid//"")=="" or .spec.volumeSnapshotRef.uid==$uid)' <<<"$vsc_data" >/dev/null || die 'DATA static VolumeSnapshotContent lineage changed.'
  jq -e --arg t "$TOKEN" --arg id "$(lower "$SOURCE_WAL")" --arg n "$VSW" --arg ns "$NS" --arg uid "$(jq -r '.metadata.uid' <<<"$vss_wal")" \
    '.metadata.labels["repro.stack.token"]==$t and .spec.driver=="disk.csi.azure.com" and .spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$id and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and ((.spec.volumeSnapshotRef.uid//"")=="" or .spec.volumeSnapshotRef.uid==$uid)' <<<"$vsc_wal" >/dev/null || die 'WAL static VolumeSnapshotContent lineage changed.'
  break
done
((SECONDS<end)) || die 'Static VolumeSnapshot objects did not become visible.'

cat >"$TMP/cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $C
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
  annotations:
    vacuum-only-repro/source-data-snapshot-id: "$SOURCE_DATA"
    vacuum-only-repro/source-wal-snapshot-id: "$SOURCE_WAL"
    vacuum-only-repro/source-backup-uid: "$BACKUP_UID"
spec:
  instances: 1
  imageName: $SYSTEM_IMAGE
  inheritedMetadata:
    labels: {repro.stack.token: "$TOKEN"}
  primaryUpdateMethod: restart
  primaryUpdateStrategy: unsupervised
  storage: {storageClass: azure-standard-hdd, size: 512Gi}
  walStorage: {storageClass: azure-standard-hdd, size: 512Gi}
  bootstrap:
    recovery:
      database: repro
      owner: app
      volumeSnapshots:
        storage: {name: $VSD, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
        walStorage: {name: $VSW, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
  affinity: {enablePodAntiAffinity: true, podAntiAffinityType: required, topologyKey: kubernetes.io/hostname}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "2", memory: 3Gi}}
  postgresql:
    parameters:
      wal_level: logical
      full_page_writes: "on"
      recovery_prefetch: try
      vacuum_cost_delay: "0"
      maintenance_io_concurrency: "10"
      maintenance_work_mem: 64MB
      shared_buffers: 256MB
      max_wal_size: 1GB
      checkpoint_timeout: 5min
      track_io_timing: "on"
      track_wal_io_timing: "on"
      wal_sender_timeout: 5s
      wal_receiver_timeout: 5s
EOF
install -m 600 "$TMP/cluster.yaml" "$SD/direct-restore/initial-cluster.yaml"
sha256sum "$SD/direct-restore/initial-cluster.yaml" >"$SD/direct-restore/initial-cluster.sha256"
k apply -f "$TMP/cluster.yaml" >/dev/null || die 'Cannot submit primary recovery from the retained paired snapshots.'

# Bind immediately after CNPG creates the pending, UID-owned primary claims.
end=$((SECONDS+900))
while ((SECONDS<end)); do
  pvc_data="$(k get pvc hdd-pg-1 -n "$NS" -o json 2>/dev/null || true)"
  pvc_wal="$(k get pvc hdd-pg-1-wal -n "$NS" -o json 2>/dev/null || true)"
  if [[ -n "$pvc_data" && -n "$pvc_wal" ]] && jq -e '.metadata.uid and .status.phase=="Pending" and (.spec.volumeName//"")==""' <<<"$pvc_data" >/dev/null && jq -e '.metadata.uid and .status.phase=="Pending" and (.spec.volumeName//"")==""' <<<"$pvc_wal" >/dev/null; then break; fi
  sleep 2
done
((SECONDS<end)) || die 'CNPG did not create both exact pending primary PVCs in time.'
python3 "$SHARED/direct-restore-disks.py" --stack "$STACK" --owner-token "$TOKEN" --bind-only --static-direct-restore --apply

end=$((SECONDS+3600)); primary=''
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)" || die 'Cannot inspect primary recovery.'
  primary_ready="$(jq -r '.status.readyInstances // 0' <<<"$cluster")"
  current_primary="$(jq -r '.status.currentPrimary // empty' <<<"$cluster")"
  if [[ "$primary_ready" == 1 && "$current_primary" == "$C-1" ]]; then primary="$C-1"; break; fi
  sleep 10
done
[[ "$primary" == "$C-1" ]] || die 'CNPG did not recover one primary from the statically bound DATA/WAL disks.'
pod="$(k get pod "$primary" -n "$NS" -o json)"
jq -e --arg token "$TOKEN" --arg c "$C" '.metadata.labels["repro.stack.token"]==$token and .metadata.labels["cnpg.io/cluster"]==$c and .metadata.labels["cnpg.io/instanceRole"]=="primary" and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True")' <<<"$pod" >/dev/null || die 'Restored primary pod ownership/readiness failed.'

SQL="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id=4),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=5),'indexName',(SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='public.vacuum_target'::regclass AND indexrelid='public.vacuum_target_id_idx'::regclass),'tableAutovacuumDisabled',EXISTS(SELECT 1 FROM pg_class WHERE oid='public.vacuum_target'::regclass AND 'autovacuum_enabled=false'=ANY(reloptions)),'settings',(SELECT json_object_agg(name,current_setting(name)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency','maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','track_io_timing','track_wal_io_timing','autovacuum','wal_sender_timeout','wal_receiver_timeout'])))::text;"
primary_meta="$(sql "$primary" "$SQL")" || die 'Cannot query the restored pre-VACUUM fixture.'
jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$TARGET_TABLE" \
  '.systemIdentifier==$sys and .inRecovery==false and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.wal_level=="logical" and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0" and .settings.maintenance_io_concurrency=="10" and .settings.maintenance_work_mem=="64MB" and .settings.shared_buffers=="256MB" and .settings.max_wal_size=="1GB" and .settings.checkpoint_timeout=="5min" and .settings.track_io_timing=="on" and .settings.track_wal_io_timing=="on" and .settings.autovacuum=="on" and .settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s"' <<<"$primary_meta" >/dev/null || die 'Restored primary differs from the pinned pre-VACUUM fixture/default PostgreSQL settings.'
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson meta "$primary_meta" '{version:1,status:"fixture-verified",verifiedAtUtc:$at,primary:"hdd-pg-1",fixture:$meta,dmlRepeated:false}' >"$SD/direct-restore/fixture-verification.json"
chmod 600 "$SD/direct-restore/fixture-verification.json"

# Recovery metadata/archive references are removed only after the primary
# passes system-ID, tuple-size, row, index, and default-setting checks.
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"bootstrap":null,"backup":null,"externalClusters":null}}' >/dev/null || die 'Cannot clear completed recovery/archive references.'
k patch cluster "$C" -n "$NS" --type=merge -p "{\"spec\":{\"imageName\":\"$MEASUREMENT_IMAGE\",\"primaryUpdateMethod\":\"restart\"}}" >/dev/null || die 'Cannot switch the restored primary to the measurement image.'
end=$((SECONDS+7200)); image_ready=false
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)"
  pod="$(k get pod "$C-1" -n "$NS" -o json 2>/dev/null || true)"
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 1 && "$(jq -r '.status.currentPrimary // empty' <<<"$cluster")" == "$C-1" && "$(jq -r '.status.image // empty' <<<"$cluster")" == "$MEASUREMENT_IMAGE" ]] && jq -e --arg token "$TOKEN" --arg image "$MEASUREMENT_IMAGE" '.metadata.labels["repro.stack.token"]==$token and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True") and any(.spec.containers[];.name=="postgres" and .image==$image)' <<<"$pod" >/dev/null; then image_ready=true; break; fi
  sleep 10
done
[[ "$image_ready" == true ]] || die 'Restored primary did not restart ready on the measurement image.'
cluster="$(k get cluster "$C" -n "$NS" -o json)"
jq -e '.spec.bootstrap.recovery==null and .spec.bootstrap.pg_basebackup==null and .spec.backup==null and .spec.externalClusters==null' <<<"$cluster" >/dev/null || die 'Source recovery/archive references remain on the measurement Cluster.'

# Replica bootstrap uses ordinary CNPG streaming/basebackup from the restored
# primary. Finalizer verifies the replica, catch-up, no client sessions, and
# VM/PV caching=none before writing prepare.json.
python3 "$SHARED/finalize-restored-primary.py" --stack "$STACK" --owner-token "$TOKEN" --direct-source-copy --apply
printf 'Prepared the retained EastUS pre-VACUUM 23-GiB pair through caller-identity direct disk copies; no source RBAC, locks, or snapshots were changed.\n'
