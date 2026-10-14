#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Copy this file into sigterm/ or control/ before running. All source-pair
# identity and restore evidence lives beside the copied script in ../shared.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SHARED="$ROOT/../shared"
PROJECT="$(cd -- "$ROOT/../../.." && pwd)"
SD="$ROOT/.state"
STATE="$SD/state.json"
PAIR="$SHARED/source-pair.json"
PROOF="$SHARED/source-pair-proof.json"
BACKUP="$SHARED/source-backup-status.json"
AUDIT="$SHARED/source-restore-audit.json"
FIXTURE="$SD/prepare.json"
GRANTS="$SD/source-snapshot-role-assignments.json"
LOCK_STATE="$SD/source-snapshot-lock-cycle.json"
LOCK_CYCLES="$SD/source-snapshot-lock-cycles.jsonl"
NS=repro
C=hdd-pg
SYSTEM_IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
MEASUREMENT_IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
ROLE_ID=7efff54f-a5b4-42b5-a1c5-5411624893ce
GLOBAL_SOURCE_LOCK="$PROJECT/.state/walreceiver-source-restore.lock"
TMP=''
GLOBAL_LOCK_HELD=false
GRANTS_ACTIVE=false
LOCK_CYCLE_ACTIVE=false
PRINCIPAL=''

die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
azj(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB" -o json; }
k(){ timeout --foreground --kill-after=15s 180s kubectl --kubeconfig "$KC" "$@"; }
sql(){ k exec -i -c postgres -n "$NS" "$1" -- psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
lower(){ tr '[:upper:]' '[:lower:]' <<<"$1"; }

for f in "$PAIR" "$PROOF" "$BACKUP" "$AUDIT"; do [[ -s "$f" ]] || die "Pinned EastUS source evidence is missing: $f"; done
for x in az jq python3 sha256sum timeout; do command -v "$x" >/dev/null || die "Missing $x."; done
SUB="$(jq -er '.subscriptionId' "$PAIR")"
LOCATION="$(jq -er '.region' "$PAIR")"
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

[[ "$(sha256sum "$PAIR" | awk '{print $1}')" == "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" ]] || die 'Pinned source-pair metadata hash changed.'
[[ "$(sha256sum "$BACKUP" | awk '{print $1}')" == "$(jq -er '.provenance.copiedBackupStatusSha256' "$PROOF")" ]] || die 'Pinned CNPG backup-status hash changed.'
[[ "$(sha256sum "$AUDIT" | awk '{print $1}')" == "$(jq -er '.provenance.copiedRestoreAuditSha256' "$PROOF")" ]] || die 'Pinned restore-audit hash changed.'
jq -e --arg sub "$SUB" --arg d "$(lower "$SOURCE_DATA")" --arg w "$(lower "$SOURCE_WAL")" \
  --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" --arg sys "$SYSTEM_ID" --arg rg "$(lower "$SOURCE_RG")" \
  '.version==1 and .subscriptionId==$sub and .region=="eastus" and .diskSizeGiB==512 and .kind=="offline-postgresql-pair" and (.resourceGroup|ascii_downcase)==$rg and (.data.id|ascii_downcase)==$d and (.wal.id|ascii_downcase)==$w and .data.uniqueId==$du and .wal.uniqueId==$wu and .fixture.systemIdentifier==$sys and .fixture.heapBytes==24696061952 and .fixture.tableBytes==25040748544 and .fixture.sourceSnapshotBeforeVacuum==true and .restoreEvidence.status=="complete" and .restoreEvidence.sourceSnapshotsUntouched==true and .restoreEvidence.replicaRestoreCompleted==true and .restoreEvidence.replicaInRecovery==true and .restoreEvidence.replicaSystemIdentifier==$sys and .restoreEvidence.replicaHeapBytes==24696061952 and .restoreEvidence.externalWalArchiveRequired==false' "$PAIR" >/dev/null || die 'Pinned pair metadata does not prove the selected 23-GiB offline restore set.'
jq -e --arg name "$BACKUP_NAME" --arg uid "$BACKUP_UID" --arg d "$(lower "$SOURCE_DATA")" --arg w "$(lower "$SOURCE_WAL")" \
  '.kind=="Backup" and .spec.method=="volumeSnapshot" and .spec.online==false and .status.phase=="completed" and .status.backupName==$name and .metadata.uid==$uid and ([.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA" and .name==$name)]|length)==1 and ([.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL" and .name==($name+"-wal"))]|length)==1 and .status.beginLSN==.status.endLSN' "$BACKUP" >/dev/null || die 'Pinned CNPG offline-backup status is incomplete or is not a matched DATA/WAL pair.'
jq -e --arg d "$(lower "$SOURCE_DATA")" --arg w "$(lower "$SOURCE_WAL")" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" \
  '.status=="complete" and .sourceSnapshotsUntouched==true and (.primaryAndReplicaSnapshots.records|length)==2' "$AUDIT" >/dev/null || die 'Pinned restore audit is incomplete.'
for kind in data wal; do
  if [[ "$kind" == data ]]; then expected="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; role=PG_DATA; else expected="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; role=PG_WAL; fi
  jq -e --arg id "$(lower "$expected")" --arg uid "$uid" --arg role "$role" --arg backup "$BACKUP_UID" \
    'any(.primaryAndReplicaSnapshots.records[];(.id|ascii_downcase)==$id and .uniqueId==$uid and .type==$role and .backupUid==$backup)' "$AUDIT" >/dev/null || die "Restore audit does not bind the exact $kind snapshot to the offline backup."
done

if [[ "${1:-}" == --validate-source-only ]]; then
  [[ $# == 1 ]] || die 'Usage: 02-prepare.sh [--validate-source-only]'
  [[ "$(az account show --query id -o tsv)" == "$SUB" ]] || die 'Active Azure subscription does not match the retained source pair.'
  rg_live="$(azj group show -n "$SOURCE_RG")" || die 'Cannot inspect retained source snapshot resource group.'
  jq -e --arg name "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and (.location|ascii_downcase)=="eastus" and .tags.reprotoken==$token' <<<"$rg_live" >/dev/null || die 'Retained source resource-group identity/owner token changed.'
  rg_locks="$(azj lock list --resource-group "$SOURCE_RG")" || die 'Cannot inspect retained source resource-group lock.'
  jq -e --arg id "$(lower "$(jq -er '.resourceGroupLock.id' "$PAIR")")" --arg name "$(jq -er '.resourceGroupLock.name' "$PAIR")" --arg notes "$(jq -er '.resourceGroupLock.notes' "$PAIR")" \
    '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$rg_locks" >/dev/null || die 'Retained source resource-group CanNotDelete lock differs from the pinned audit.'
  for kind in data wal; do
    if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; disk_uid="$(jq -er '.data.sourceDiskUniqueId' "$PAIR")"; else sid="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; disk_uid="$(jq -er '.wal.sourceDiskUniqueId' "$PAIR")"; fi
    snap="$(azj snapshot show --ids "$sid")" || die "Cannot inspect pinned $kind snapshot."
    jq -e --arg id "$(lower "$sid")" --arg uid "$uid" --arg disk "$(lower "$disk")" --arg diskUid "$disk_uid" --arg rg "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" \
      '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.location|ascii_downcase)=="eastus" and (.resourceGroup|ascii_downcase)==$rg and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .completionPercent==100 and .diskSizeGB==512 and .incremental==true and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk and .creationData.sourceUniqueId==$diskUid and .tags.reprotoken==$token and (.tags.source_volume_id|ascii_downcase)==$disk' <<<"$snap" >/dev/null || die "Pinned EastUS $kind snapshot identity, copy, ownership, or readiness check failed."
    locks="$(azj lock list --resource "$sid")" || die "Cannot inspect retained $kind snapshot lock."
    jq -e --arg id "$(lower "$(jq -er --arg k "$kind" '.[$k].lock.id' "$PAIR")")" --arg name "$(jq -er --arg k "$kind" '.[$k].lock.name' "$PAIR")" --arg notes "$(jq -er --arg k "$kind" '.[$k].lock.notes' "$PAIR")" \
      '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$locks" >/dev/null || die "Pinned EastUS $kind CanNotDelete lock differs from the restore audit."
  done
  printf 'EastUS offline DATA/WAL pair, completed backup, prior replica restore, live Azure identities, and retention locks verify read-only.\n'
  exit 0
fi
[[ $# == 0 ]] || die 'Usage: 02-prepare.sh [--validate-source-only]'
for x in kubectl flock; do command -v "$x" >/dev/null || die "Missing $x."; done
[[ -s "$STATE" ]] || die 'Need the provisioned per-stack .state/state.json.'
SUB_STATE="$(jq -er '.subscriptionId' "$STATE")"
TOKEN="$(jq -er '.ownerToken' "$STATE")"
RG="$(jq -er '.resourceGroup' "$STATE")"
AKS="$(jq -er '.clusterName' "$STATE")"
LOC="$(jq -er '.location' "$STATE")"
KC="$(jq -er '.kubeconfigPath' "$STATE")"
[[ "$SUB_STATE" == "$SUB" && "${LOC,,}" == eastus ]] || die 'Stack subscription/location differs from the pinned EastUS pair.'
[[ "$KC" == "$SD/kubeconfig" && -s "$KC" ]] || die 'Kubeconfig is not the isolated per-stack file.'
[[ "$(jq -r '.status' "$STATE")" == ready ]] || die 'AKS stack is not ready.'
[[ ! -e "$FIXTURE" && ! -e "$GRANTS" && ! -e "$LOCK_STATE" && ! -e "$LOCK_CYCLES" ]] || die 'Preparation evidence already exists; refusing to overwrite/adopt a prior run.'
mkdir -p "$SD"; chmod 700 "$SD"
exec 9>"$SD/02-prepare.lock"; flock -n 9 || die 'This stack preparation is already running.'
TMP="$(mktemp -d "$SD/prepare-work.XXXXXX")"
for source_file in source-pair.json source-pair-proof.json source-backup-status.json source-restore-audit.json; do
  case "$source_file" in
    source-pair.json) from="$PAIR";; source-pair-proof.json) from="$PROOF";;
    source-backup-status.json) from="$BACKUP";; source-restore-audit.json) from="$AUDIT";;
  esac
  to="$SD/$source_file"
  if [[ -e "$to" ]]; then
    cmp -s "$from" "$to" || die "Existing private source evidence differs from the pinned shared file: $to"
  else
    install -m 600 "$from" "$to"
  fi
done
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg pair "$(sha256sum "$SD/source-pair.json" | awk '{print $1}')" \
  --arg proof "$(sha256sum "$SD/source-pair-proof.json" | awk '{print $1}')" \
  --arg backup "$(sha256sum "$SD/source-backup-status.json" | awk '{print $1}')" \
  --arg audit "$(sha256sum "$SD/source-restore-audit.json" | awk '{print $1}')" \
  '{version:1,copiedAtUtc:$at,files:{pair:$pair,proof:$proof,backupStatus:$backup,restoreAudit:$audit}}' >"$SD/source-evidence.json"
chmod 600 "$SD/source-evidence.json"
trap 'prepare_exit_trap' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

active="$(az account show --query id -o tsv)" || die 'Cannot inspect active Azure subscription.'
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active Azure subscription does not match source/stack metadata.'
ns="$(k get namespace "$NS" -o json)" || die 'The repro namespace is missing.'
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' <<<"$ns" >/dev/null || die 'Namespace ownership label mismatch.'
sc="$(k get storageclass azure-standard-hdd -o json)" || die 'Standard HDD StorageClass is missing.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="None" and .reclaimPolicy=="Delete" and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$sc" >/dev/null || die 'StorageClass must be uncached Standard_LRS.'
SHORT="${TOKEN:0:8}"
VSC="source-pair-$SHORT"; VCD="source-data-content-$SHORT"; VCW="source-wal-content-$SHORT"
VSD="source-data-$SHORT"; VSW="source-wal-$SHORT"
for item in "volumesnapshotclass:$VSC" "volumesnapshotcontent:$VCD" "volumesnapshotcontent:$VCW" "volumesnapshot:$VSD" "volumesnapshot:$VSW" "cluster:$C"; do
  kind="${item%%:*}"; name="${item#*:}"
  if k get "$kind" "$name" -n "$NS" -o json >/dev/null 2>&1; then die "$kind/$name already exists; refusing to adopt it."; fi
done

# The two experiment stacks share one retained source pair. Serialize the full
# snapshot grant/import/clone/cleanup window so neither stack can alter source
# access or lock state while the other is using it.
mkdir -p "$PROJECT/.state"; chmod 700 "$PROJECT/.state"
exec 8>"$GLOBAL_SOURCE_LOCK"; flock -x 8; GLOBAL_LOCK_HELD=true
[[ "$(jq -r '.status' "$STATE")" == ready ]] || die 'Stack state changed while waiting for shared source access.'
rg_live="$(azj group show -n "$SOURCE_RG")" || die 'Cannot inspect retained source resource group.'
jq -e --arg name "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and (.location|ascii_downcase)=="eastus" and .tags.reprotoken==$token' <<<"$rg_live" >/dev/null || die 'Retained source group identity or ownership changed.'
rg_locks="$(azj lock list --resource-group "$SOURCE_RG")" || die 'Cannot inspect inherited source retention lock.'
jq -e --arg id "$(lower "$(jq -er '.resourceGroupLock.id' "$PAIR")")" --arg name "$(jq -er '.resourceGroupLock.name' "$PAIR")" --arg notes "$(jq -er '.resourceGroupLock.notes' "$PAIR")" \
  '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$rg_locks" >/dev/null || die 'Retained source resource-group lock changed.'
for kind in data wal; do
  if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; disk_uid="$(jq -er '.data.sourceDiskUniqueId' "$PAIR")"; else sid="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; disk_uid="$(jq -er '.wal.sourceDiskUniqueId' "$PAIR")"; fi
  live="$(azj snapshot show --ids "$sid")" || die "Cannot inspect retained source $kind snapshot."
  jq -e --arg id "$(lower "$sid")" --arg uid "$uid" --arg disk "$(lower "$disk")" --arg diskUid "$disk_uid" --arg rg "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" \
    '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)=="eastus" and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .completionPercent==100 and .diskSizeGB==512 and .incremental==true and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk and .creationData.sourceUniqueId==$diskUid and .tags.reprotoken==$token and (.tags.source_volume_id|ascii_downcase)==$disk' <<<"$live" >/dev/null || die "Retained source $kind snapshot live identity/readiness mismatch."
  locks="$(azj lock list --resource "$sid")" || die "Cannot inspect direct source $kind lock."
  jq -e --arg id "$(lower "$(jq -er --arg k "$kind" '.[$k].lock.id' "$PAIR")")" --arg name "$(jq -er --arg k "$kind" '.[$k].lock.name' "$PAIR")" --arg notes "$(jq -er --arg k "$kind" '.[$k].lock.notes' "$PAIR")" \
    '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$locks" >/dev/null || die "Retained source $kind direct lock changed."
  jq -n --arg kind "$kind" --argjson snap "$live" --argjson directLock "$(jq -c --arg k "$kind" '.[$k].lock' "$PAIR")" \
    '{kind:$kind,snapshotId:$snap.id,uniqueId:$snap.uniqueId,sourceDiskId:$snap.creationData.sourceResourceId,provisioningState:$snap.provisioningState,snapshotAccessState:$snap.snapshotAccessState,completionPercent:$snap.completionPercent,diskSizeGB:$snap.diskSizeGB,sku:$snap.sku.name,directLock:$directLock}' >>"$TMP/source-verification.jsonl"
done

aks_obj="$(azj aks show -g "$RG" -n "$AKS")" || die 'Cannot inspect the owned AKS control-plane identity.'
PRINCIPAL="$(jq -er '.identity.principalId|select(type=="string" and length>0)' <<<"$aks_obj")" || die 'AKS control-plane principal is missing.'
role_name="$(az role definition list --name "$ROLE_ID" --subscription "$SUB" --query '[0].roleName' -o tsv)"
[[ "$role_name" == 'Disk Snapshot Contributor' ]] || die 'Pinned role ID is not Disk Snapshot Contributor.'
jq -n --arg token "$TOKEN" --arg p "$PRINCIPAL" --arg role "$ROLE_ID" --arg d "$SOURCE_DATA" --arg w "$SOURCE_WAL" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" \
  --arg pairSha "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" --arg globalLock "$GLOBAL_SOURCE_LOCK" \
  '{version:1,status:"creating",sourceKind:"retained-eastus-offline-postgresql-pair",sourcePairSha256:$pairSha,ownerToken:$token,principalId:$p,roleDefinitionId:$role,globalSourceLock:$globalLock,locksModified:false,assignments:[{kind:"data",snapshotId:$d,uniqueId:$du,scope:$d,assignmentId:null,createAttempted:false,createdByThisStack:false,removed:false},{kind:"wal",snapshotId:$w,uniqueId:$wu,scope:$w,assignmentId:null,createAttempted:false,createdByThisStack:false,removed:false}]}' >"$GRANTS"
chmod 600 "$GRANTS"

set_grant(){
  local kind="$1" patch="$2" tmpfile
  tmpfile="$(mktemp "$SD/.grant-update.XXXXXX")"
  jq --arg k "$kind" --argjson p "$patch" '.assignments|=map(if .kind==$k then . * $p else . end)' "$GRANTS" >"$tmpfile"
  mv -f "$tmpfile" "$GRANTS"; chmod 600 "$GRANTS"
}
save_grants_status(){
  local status="$1" tmpfile
  tmpfile="$(mktemp "$SD/.grant-status.XXXXXX")"
  jq --arg s "$status" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.status=$s|.updatedAtUtc=$at|if $s=="removed" then .postRemovalConfirmed=true else . end' "$GRANTS" >"$tmpfile"
  mv -f "$tmpfile" "$GRANTS"; chmod 600 "$GRANTS"
}
check_source_locks(){
  local rg_rows direct_rows kind sid
  rg_rows="$(azj lock list --resource-group "$SOURCE_RG")" || return 1
  jq -e --arg id "$(lower "$(jq -er '.resourceGroupLock.id' "$PAIR")")" --arg name "$(jq -er '.resourceGroupLock.name' "$PAIR")" --arg notes "$(jq -er '.resourceGroupLock.notes' "$PAIR")" \
    '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$rg_rows" >/dev/null || return 1
  for kind in data wal; do
    [[ "$kind" == data ]] && sid="$SOURCE_DATA" || sid="$SOURCE_WAL"
    direct_rows="$(azj lock list --resource "$sid")" || return 1
    jq -e --arg id "$(lower "$(jq -er --arg k "$kind" '.[$k].lock.id' "$PAIR")")" --arg name "$(jq -er --arg k "$kind" '.[$k].lock.name' "$PAIR")" --arg notes "$(jq -er --arg k "$kind" '.[$k].lock.notes' "$PAIR")" \
      '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level=="CanNotDelete" and .notes==$notes)]|length==1' <<<"$direct_rows" >/dev/null || return 1
  done
}
check_source_pair_live(){
  local group kind sid uid disk disk_uid live
  group="$(azj group show -n "$SOURCE_RG")" || return 1
  jq -e --arg name "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and (.location|ascii_downcase)=="eastus" and .tags.reprotoken==$token' <<<"$group" >/dev/null || return 1
  for kind in data wal; do
    if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; disk_uid="$(jq -er '.data.sourceDiskUniqueId' "$PAIR")"; else sid="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; disk_uid="$(jq -er '.wal.sourceDiskUniqueId' "$PAIR")"; fi
    live="$(azj snapshot show --ids "$sid")" || return 1
    jq -e --arg id "$(lower "$sid")" --arg uid "$uid" --arg disk "$(lower "$disk")" --arg diskUid "$disk_uid" --arg rg "$(lower "$SOURCE_RG")" --arg token "$SOURCE_RG_TOKEN" \
      '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)=="eastus" and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .completionPercent==100 and .diskSizeGB==512 and .incremental==true and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk and .creationData.sourceUniqueId==$diskUid and .tags.reprotoken==$token and (.tags.source_volume_id|ascii_downcase)==$disk' <<<"$live" >/dev/null || return 1
  done
}
lock_spec_json(){
  case "$1" in group) jq -c '.resourceGroupLock' "$PAIR";; data|wal) jq -c --arg k "$1" '.[$k].lock' "$PAIR";; *) return 2;; esac
}
lock_scope_id(){
  case "$1" in
    group) jq -er '.resourceGroupLock.id|split("/providers/Microsoft.Authorization/locks/")[0]' "$PAIR";;
    data|wal) jq -er --arg k "$1" '.[$k].lock.id|split("/providers/Microsoft.Authorization/locks/")[0]' "$PAIR";;
    *) return 2;;
  esac
}
lock_resource_id(){
  case "$1" in group) printf '%s\n' "$SOURCE_RG";; data) printf '%s\n' "$SOURCE_DATA";; wal) printf '%s\n' "$SOURCE_WAL";; *) return 2;; esac
}
list_lock_scope(){
  case "$1" in group) azj lock list --resource-group "$SOURCE_RG";; data|wal) azj lock list --resource "$(lock_resource_id "$1")";; *) return 2;; esac
}
verify_exact_lock(){
  local kind="$1" rows spec id name level notes
  rows="$(list_lock_scope "$kind")" || return 1
  spec="$(lock_spec_json "$kind")" || return 1
  id="$(jq -er '.id' <<<"$spec")"; name="$(jq -er '.name' <<<"$spec")"; level="$(jq -er '.level' <<<"$spec")"; notes="$(jq -er '.notes' <<<"$spec")"
  jq -e --arg id "$(lower "$id")" --arg name "$name" --arg level "$level" --arg notes "$notes" \
    '[.[]|select((.id|ascii_downcase)==$id and .name==$name and .level==$level and .notes==$notes)]|length==1' <<<"$rows" >/dev/null
}
verify_exact_lock_absent(){
  local kind="$1" rows id
  rows="$(list_lock_scope "$kind")" || return 1
  id="$(lock_spec_json "$kind" | jq -er '.id')"
  jq -e --arg id "$(lower "$id")" 'all(.[];(.id|ascii_downcase)!=$id)' <<<"$rows" >/dev/null
}
delete_exact_lock(){
  local kind="$1" spec id rc
  verify_exact_lock "$kind" || { echo "Pinned $kind CanNotDelete lock is missing or changed; refusing lock cycle." >&2; return 1; }
  spec="$(lock_spec_json "$kind")"; id="$(jq -er '.id' <<<"$spec")"
  set +e; az lock delete --ids "$id" --subscription "$SUB" -o none >/dev/null; rc=$?; set -e
  if ! verify_exact_lock_absent "$kind"; then
    echo "Exact $kind lock deletion did not leave the pinned lock absent (CLI exit $rc)." >&2
    return 1
  fi
  [[ "$rc" -eq 0 ]] || printf 'Azure returned exit %s after removing the exact %s lock; absence was confirmed and restoration is armed.\n' "$rc" "$kind" >&2
  case "$kind" in group) jq '.removed.resourceGroup=true' "$LOCK_STATE" >"$LOCK_STATE.tmp";; data) jq '.removed.data=true' "$LOCK_STATE" >"$LOCK_STATE.tmp";; wal) jq '.removed.wal=true' "$LOCK_STATE" >"$LOCK_STATE.tmp";; esac
  mv -f "$LOCK_STATE.tmp" "$LOCK_STATE"; chmod 600 "$LOCK_STATE"
}
restore_exact_lock(){
  local kind="$1" spec id name level notes resource created rc current
  spec="$(lock_spec_json "$kind")" || return 1
  id="$(jq -er '.id' <<<"$spec")"; name="$(jq -er '.name' <<<"$spec")"; level="$(jq -er '.level' <<<"$spec")"; notes="$(jq -er '.notes' <<<"$spec")"
  if verify_exact_lock "$kind"; then return 0; fi
  current="$(list_lock_scope "$kind")" || return 1
  jq -e --arg id "$(lower "$id")" --arg name "$name" 'all(.[];(.id|ascii_downcase)!=$id or .name==$name)' <<<"$current" >/dev/null || { echo "Unexpected same-name $kind lock exists; refusing to replace it." >&2; return 1; }
  if [[ "$kind" == group ]]; then
    set +e; created="$(azj lock create --name "$name" --lock-type "$level" --resource-group "$SOURCE_RG" --notes "$notes")"; rc=$?; set -e
  else
    resource="$(lock_resource_id "$kind")"
    set +e; created="$(azj lock create --name "$name" --lock-type "$level" --resource "$resource" --notes "$notes")"; rc=$?; set -e
  fi
  if [[ "$rc" -ne 0 ]] && ! verify_exact_lock "$kind"; then echo "Failed to restore the exact pinned $kind CanNotDelete lock." >&2; return 1; fi
  verify_exact_lock "$kind" || { echo "Restored $kind lock does not match its pinned identity/name/level/notes." >&2; return 1; }
  printf '%s\n' "$created" >"$SD/restored-source-lock-$kind.json" 2>/dev/null || true
  chmod 600 "$SD/restored-source-lock-$kind.json" 2>/dev/null || true
}
begin_source_lock_cycle(){
  local rg_rows all_rows rg_scope subscription_scope group_lock data_lock wal_lock
  [[ "$GLOBAL_LOCK_HELD" == true ]] || { echo 'Refusing source lock changes without the workspace-wide source lock.' >&2; return 1; }
  [[ ! -e "$LOCK_STATE" ]] || { echo 'A prior source lock-cycle state file exists; refusing to overwrite it.' >&2; return 1; }
  check_source_locks && check_source_pair_live || { echo 'Source pair/retention locks failed pre-cycle verification.' >&2; return 1; }
  rg_scope="$(lock_scope_id group)"; subscription_scope="/subscriptions/$SUB"
  rg_rows="$(azj lock list --resource-group "$SOURCE_RG")" || return 1
  all_rows="$(azj lock list)" || return 1
  jq -e --arg rg "$(lower "$rg_scope")" --arg sub "$(lower "$subscription_scope")" \
    '[.[]|(.id|ascii_downcase|split("/providers/microsoft.authorization/locks/")[0]) as $scope|select($scope==$rg or $scope==$sub)] as $locks | ([ $locks[]|select((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/")[0])==$sub)]|length)==0' <<<"$all_rows" >/dev/null || { echo 'A subscription-level lock may also protect this scope; refusing to alter it.' >&2; return 1; }
  jq -e --arg gid "$(lower "$(lock_spec_json group | jq -er .id)")" --arg did "$(lower "$(lock_spec_json data | jq -er .id)")" --arg wid "$(lower "$(lock_spec_json wal | jq -er .id)")" \
    '([.[]|select((.id|ascii_downcase)==$gid and .level=="CanNotDelete")]|length)==1 and ([.[]|select((.id|ascii_downcase)==$did and .level=="CanNotDelete")]|length)==1 and ([.[]|select((.id|ascii_downcase)==$wid and .level=="CanNotDelete")]|length)==1' <<<"$rg_rows" >/dev/null || { echo 'Resource-group lock inventory does not contain the exact pinned resource-group/DATA/WAL locks.' >&2; return 1; }
  group_lock="$(lock_spec_json group)"; data_lock="$(lock_spec_json data)"; wal_lock="$(lock_spec_json wal)"
  jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg pairSha "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" --arg group "$SOURCE_RG" --arg sub "$SUB" \
    --argjson rgLock "$group_lock" --argjson dataLock "$data_lock" --argjson walLock "$wal_lock" \
    '{version:1,status:"in-progress",startedAtUtc:$at,sourcePairSha256:$pairSha,subscriptionId:$sub,resourceGroup:$group,snapshotDeletionAttempted:false,originalLocks:{resourceGroup:$rgLock,data:$dataLock,wal:$walLock},removed:{resourceGroup:false,data:false,wal:false}}' >"$LOCK_STATE"
  chmod 600 "$LOCK_STATE"; : >"$LOCK_CYCLES"; chmod 600 "$LOCK_CYCLES"
  LOCK_CYCLE_ACTIVE=true
  delete_exact_lock group || return 1
  verify_exact_lock data && verify_exact_lock wal || { echo 'Direct snapshot locks changed while removing the source resource-group lock.' >&2; return 1; }
}
restore_source_locks(){
  [[ "$LOCK_CYCLE_ACTIVE" == true || -s "$LOCK_STATE" ]] || return 0
  [[ -s "$LOCK_STATE" ]] || return 1
  [[ "$(jq -r '.status // empty' "$LOCK_STATE")" != restored ]] || { LOCK_CYCLE_ACTIVE=false; return 0; }
  [[ "$GLOBAL_LOCK_HELD" == true ]] || { echo 'Cannot restore source locks outside the workspace-wide source lock.' >&2; return 1; }
  local kind tmp
  for kind in data wal group; do restore_exact_lock "$kind" || return 1; done
  check_source_locks && check_source_pair_live || { echo 'Restored source lock or snapshot identity verification failed.' >&2; return 1; }
  tmp="$(mktemp "$SD/.lock-cycle-state.XXXXXX")"
  jq --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.status="restored"|.restoredAtUtc=$at|.removed={resourceGroup:false,data:false,wal:false}' "$LOCK_STATE" >"$tmp"
  mv -f "$tmp" "$LOCK_STATE"; chmod 600 "$LOCK_STATE"
  LOCK_CYCLE_ACTIVE=false
}
prepare_exit_trap(){
  local rc=$? cleanup_rc
  trap - EXIT
  if [[ "$GLOBAL_LOCK_HELD" == true && "$GRANTS_ACTIVE" == true ]] && declare -F remove_grants >/dev/null; then
    set +e; remove_grants; cleanup_rc=$?; set -e
    [[ "$cleanup_rc" -eq 0 ]] || printf 'Temporary source grants need review: %s\n' "$GRANTS" >&2
  fi
  if [[ "$GLOBAL_LOCK_HELD" == true ]] && { [[ "$LOCK_CYCLE_ACTIVE" == true ]] || { [[ -s "$LOCK_STATE" ]] && [[ "$(jq -r '.status // empty' "$LOCK_STATE")" != restored ]]; }; }; then
    set +e
    until restore_source_locks; do
      printf 'Source CanNotDelete locks are not yet restored; retaining the shared source lock and retrying.\n' >&2
      sleep 10
    done
    set -e
  fi
  if [[ -n "$TMP" && -d "$TMP" ]]; then
    if ((rc==0)); then rm -rf -- "$TMP"; else printf 'Preparation evidence retained at %s\n' "$TMP" >&2; fi
  fi
  exit "$rc"
}
remove_grants(){
  [[ -s "$GRANTS" ]] || return 0
  [[ "$GLOBAL_LOCK_HELD" == true ]] || { echo 'Cannot remove snapshot grants outside the shared source lock.' >&2; return 1; }
  local kind scope aid created removed rows matches rc role_operation_started=false group_lock_before
  group_lock_before="$(lock_spec_json group)"
  for kind in data wal; do
    created="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.createdByThisStack // false' "$GRANTS")"
    aid="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.assignmentId // empty' "$GRANTS")"
    removed="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.removed // false' "$GRANTS")"
    [[ "$removed" == true || ( "$created" != true && -z "$aid" ) ]] && continue
    scope="$(jq -er --arg k "$kind" '.assignments[]|select(.kind==$k)|.scope' "$GRANTS")"
    rows="$(azj role assignment list --scope "$scope")" || return 1
    matches="$(jq -c --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" '[.[]|select(.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)]' <<<"$rows")"
    case "$(jq 'length' <<<"$matches")" in
      0) set_grant "$kind" '{"removed":true,"postRemovalRoleListAbsent":true,"createdByThisStack":false}';;
      1)
        matches_id="$(jq -er '.[0].id' <<<"$matches")"
        if [[ -n "$aid" && "$(lower "$aid")" != "$(lower "$matches_id")" ]]; then echo "Live $kind grant id differs from its ledger." >&2; return 1; fi
        set_grant "$kind" "$(jq -cn --arg id "$matches_id" '{assignmentId:$id}')"
        role_operation_started=true
        ;;
      *) echo "Ambiguous exact $kind role grants; preserving ledger." >&2; return 1;;
    esac
  done
  if [[ "$role_operation_started" == true && "$LOCK_CYCLE_ACTIVE" != true ]]; then
    begin_source_lock_cycle || return 1
  fi
  for kind in data wal; do
    scope="$(jq -er --arg k "$kind" '.assignments[]|select(.kind==$k)|.scope' "$GRANTS")"
    aid="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.assignmentId // empty' "$GRANTS")"
    created="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.createdByThisStack // false' "$GRANTS")"
    removed="$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.removed // false' "$GRANTS")"
    [[ "$removed" == true ]] && continue
    if [[ "$created" != true && -z "$aid" ]]; then continue; fi
    if [[ "$removed" != true ]]; then
      rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg id "$(lower "$aid")" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" \
        'any(.[];.principalId==$p and (.id|ascii_downcase)==$id and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$rows" >/dev/null || return 1
      if verify_exact_lock "$kind"; then delete_exact_lock "$kind" || return 1; else verify_exact_lock_absent "$kind" || return 1; fi
      set +e; az role assignment delete --ids "$aid" --subscription "$SUB" -o none >/dev/null; rc=$?; set -e
      [[ "$rc" -eq 0 ]] || { echo "Role grant removal failed for $kind (exit $rc); direct lock restoration is armed." >&2; return 1; }
      rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg id "$(lower "$aid")" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" \
        'all(.[];.principalId!=$p or (.id|ascii_downcase)!=$id or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$rows" >/dev/null || return 1
      restore_exact_lock "$kind" || return 1
      rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg id "$(lower "$aid")" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" \
        'all(.[];.principalId!=$p or (.id|ascii_downcase)!=$id or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$rows" >/dev/null || return 1
      set_grant "$kind" "$(jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson rgBefore "$group_lock_before" --argjson directBefore "$(lock_spec_json "$kind")" --argjson directAfter "$(lock_spec_json "$kind")" --argjson deleteExit "$rc" '{removed:true,postRemovalRoleListAbsent:true,removedAtUtc:$at,groupLockBefore:$rgBefore,directLockBefore:$directBefore,directLockAfter:$directAfter,roleAssignmentDeleteExitCode:$deleteExit}')"
      jq -cn --arg kind "$kind" --arg scope "$scope" --arg aid "$aid" --arg uid "$(jq -er --arg k "$kind" '.assignments[]|select(.kind==$k)|.uniqueId' "$GRANTS")" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson groupBefore "$group_lock_before" --argjson directBefore "$(lock_spec_json "$kind")" --argjson directAfter "$(lock_spec_json "$kind")" \
        --argjson roleExit "$rc" '{kind:$kind,snapshotId:$scope,uniqueId:$uid,roleAssignmentId:$aid,resourceGroupLockTemporarilyRemoved:true,groupLockBefore:$groupBefore,directLockBefore:$directBefore,directLockAfter:$directAfter,roleAssignmentDeleteExitCode:$roleExit,lockRestored:true,snapshotDeletionAttempted:false,verifiedAtUtc:$at}' >>"$LOCK_CYCLES"
    fi
  done
  if [[ "$LOCK_CYCLE_ACTIVE" == true ]]; then
    restore_source_locks || return 1
    local rg_after
    rg_after="$(lock_spec_json group)"
    jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson groupBefore "$group_lock_before" --argjson groupAfter "$rg_after" \
      '{kind:"resourceGroup",groupLockBefore:$groupBefore,groupLockAfter:$groupAfter,resourceGroupLockRestored:true,snapshotDeletionAttempted:false,verifiedAtUtc:$at}' >>"$LOCK_CYCLES"
  else
    check_source_locks && check_source_pair_live || return 1
  fi
  save_grants_status removed
  GRANTS_ACTIVE=false
}

for kind in data wal; do
  if [[ "$kind" == data ]]; then scope="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; else scope="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; fi
  before="$(azj role assignment list --scope "$scope")" || die "Cannot inspect existing $kind snapshot permissions."
  jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" \
    'all(.[];.principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$before" >/dev/null || die "Unexpected pre-existing exact $kind snapshot grant; refusing to adopt it."
  set_grant "$kind" '{"createAttempted":true,"createdByThisStack":true}'
  GRANTS_ACTIVE=true
  grant="$(az role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role "$ROLE_ID" --scope "$scope" --subscription "$SUB" -o json)" || die "Cannot create exact temporary $kind snapshot grant."
  aid="$(jq -er '.id' <<<"$grant")" || die "The $kind snapshot grant returned no assignment ID."
  set_grant "$kind" "$(jq -cn --arg id "$aid" --arg uid "$uid" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{assignmentId:$id,uniqueId:$uid,createdAtUtc:$at}')"
  rows="$(azj role assignment list --scope "$scope")" || die "Cannot verify temporary $kind snapshot grant."
  jq -e --arg id "$(lower "$aid")" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "$(lower "$scope")" \
    '[.[]|select(.principalId==$p and (.id|ascii_downcase)==$id and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)]|length==1' <<<"$rows" >/dev/null || die "Temporary $kind snapshot grant does not match the exact role/scope."
done
save_grants_status active

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
metadata:
  name: $VSD
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
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
metadata:
  name: $VSW
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: $VCW}}
EOF
k apply -f "$TMP/source-pair.yaml" >/dev/null || die 'Cannot create this stack-owned static DATA/WAL VolumeSnapshots.'
end=$((SECONDS+3600))
while ((SECONDS<end)); do
  d="$(k get volumesnapshot "$VSD" -n "$NS" -o json)" || die 'Cannot inspect static DATA VolumeSnapshot.'
  w="$(k get volumesnapshot "$VSW" -n "$NS" -o json)" || die 'Cannot inspect static WAL VolumeSnapshot.'
  if jq -e '.status.readyToUse==true' <<<"$d" >/dev/null && jq -e '.status.readyToUse==true' <<<"$w" >/dev/null; then break; fi
  sleep 10
done
((SECONDS<end)) || die 'Static paired VolumeSnapshots did not become ready.'
for pair in "$VSD:$VCD:$SOURCE_DATA" "$VSW:$VCW:$SOURCE_WAL"; do
  IFS=: read -r vss vsc sid <<<"$pair"
  v="$(k get volumesnapshot "$vss" -n "$NS" -o json)"; c="$(k get volumesnapshotcontent "$vsc" -o json)"
  jq -e --arg t "$TOKEN" --arg c "$vsc" '.metadata.labels["repro.stack.token"]==$t and .status.readyToUse==true and .spec.source.volumeSnapshotContentName==$c' <<<"$v" >/dev/null || die "$vss binding/readiness check failed."
  jq -e --arg t "$TOKEN" --arg id "$(lower "$sid")" --arg n "$vss" --arg ns "$NS" '.metadata.labels["repro.stack.token"]==$t and .spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$id and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and .status.readyToUse==true and (.status.snapshotHandle|ascii_downcase)==$id' <<<"$c" >/dev/null || die "$vsc does not retain the exact source snapshot handle."
done

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
cp "$TMP/cluster.yaml" "$SD/source-pair-cluster-manifest.yaml"; chmod 600 "$SD/source-pair-cluster-manifest.yaml"
sha256sum "$SD/source-pair-cluster-manifest.yaml" >"$SD/source-pair-cluster-manifest.sha256"
k apply -f "$TMP/cluster.yaml" >/dev/null || die 'Cannot submit primary recovery from the retained paired snapshots.'
end=$((SECONDS+3600)); primary=''
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)" || die 'Cannot inspect CNPG recovery.'
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 1 && "$(jq -r '.status.currentPrimary // empty' <<<"$cluster")" == "$C-1" ]]; then primary="$C-1"; break; fi
  sleep 10
done
[[ "$primary" == "$C-1" ]] || die 'Primary did not finish paired-snapshot recovery.'
pod="$(k get pod "$primary" -n "$NS" -o json)"
jq -e --arg token "$TOKEN" --arg cluster "$C" '.metadata.labels["repro.stack.token"]==$token and .metadata.labels["cnpg.io/cluster"]==$cluster and .metadata.labels["cnpg.io/instanceRole"]=="primary" and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True")' <<<"$pod" >/dev/null || die 'Recovered primary pod is not owned/ready.'
pvc_json="$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)" || die 'Cannot inspect recovered DATA/WAL claims.'
jq -e --arg d "$C-1" --arg w "$C-1-wal" --arg token "$TOKEN" --arg vssd "$VSD" --arg vssw "$VSW" '[.items[]|select(.metadata.name==$d or .metadata.name==$w)] as $p | ($p|length)==2 and all($p[];.metadata.labels["repro.stack.token"]==$token and .status.phase=="Bound" and .spec.storageClassName=="azure-standard-hdd" and .spec.resources.requests.storage=="512Gi") and any($p[];.metadata.name==$d and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vssd) and any($p[];.metadata.name==$w and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vssw)' <<<"$pvc_json" >/dev/null || die 'Recovered PVCs do not prove separate source DATA/WAL VolumeSnapshot claims.'
data_pv="$(jq -er --arg n "$C-1" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
wal_pv="$(jq -er --arg n "$C-1-wal" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
[[ -n "$data_pv" && -n "$wal_pv" && "$data_pv" != "$wal_pv" ]] || die 'Recovered primary DATA and WAL did not land on distinct PVs.'

SQL="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id=4),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=5),'indexName',(SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='public.vacuum_target'::regclass AND indexrelid='public.vacuum_target_id_idx'::regclass),'tableAutovacuumDisabled',EXISTS(SELECT 1 FROM pg_class WHERE oid='public.vacuum_target'::regclass AND 'autovacuum_enabled=false'=ANY(reloptions)),'settings',(SELECT json_object_agg(name,current_setting(name)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency','maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','track_io_timing','track_wal_io_timing','autovacuum','wal_sender_timeout','wal_receiver_timeout'])))::text;"
primary_meta="$(sql "$primary" "$SQL")" || die 'Cannot query the recovered source fixture.'
jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$TARGET_TABLE" \
  '.systemIdentifier==$sys and .inRecovery==false and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.wal_level=="logical" and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0" and .settings.maintenance_io_concurrency=="10" and .settings.maintenance_work_mem=="64MB" and .settings.shared_buffers=="256MB" and .settings.max_wal_size=="1GB" and .settings.checkpoint_timeout=="5min" and .settings.track_io_timing=="on" and .settings.track_wal_io_timing=="on" and .settings.autovacuum=="on" and .settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s"' <<<"$primary_meta" >/dev/null || die 'Restored primary does not contain the pinned 23-GiB pre-VACUUM fixture/settings.'

# Snapshot recovery metadata and any optional archiver configuration are no
# longer needed once CNPG has made independent DATA/WAL managed disks.
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"bootstrap":null,"backup":null,"externalClusters":null}}' >/dev/null || die 'Cannot clear completed source bootstrap/archive references.'
k patch cluster "$C" -n "$NS" --type=merge -p "{\"spec\":{\"imageName\":\"$MEASUREMENT_IMAGE\",\"primaryUpdateMethod\":\"restart\"}}" >/dev/null || die 'Cannot switch restored primary to the measurement image.'
end=$((SECONDS+7200)); image_ready=false
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)"
  pod="$(k get pod "$C-1" -n "$NS" -o json 2>/dev/null || true)"
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 1 && "$(jq -r '.status.currentPrimary // empty' <<<"$cluster")" == "$C-1" && "$(jq -r '.status.image // empty' <<<"$cluster")" == "$MEASUREMENT_IMAGE" ]] && jq -e --arg t "$TOKEN" --arg image "$MEASUREMENT_IMAGE" '.metadata.labels["repro.stack.token"]==$t and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True") and any(.spec.containers[];.name=="postgres" and .image==$image)' <<<"$pod" >/dev/null; then image_ready=true; break; fi
  sleep 10
done
[[ "$image_ready" == true ]] || die 'Primary did not restart ready on the measurement image.'
cluster="$(k get cluster "$C" -n "$NS" -o json)"
jq -e '.spec.bootstrap.recovery==null and .spec.bootstrap.pg_basebackup==null and .spec.backup==null and .spec.externalClusters==null' <<<"$cluster" >/dev/null || die 'Source recovery or archive references remain on the measurement Cluster.'

# The source snapshot grants are removed only after both static claims have
# become separate bound Azure disks and the recovery/bootstrap references are
# cleared. The pinned resource-group and snapshot locks remain present throughout.
remove_grants || die 'Could not verify removal of temporary source grants and unchanged retention locks.'
flock -u 8; GLOBAL_LOCK_HELD=false

k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null || die 'Cannot configure the original ANY 1 preferred synchronous replica.'
end=$((SECONDS+21600)); replica=''; observed_jobs='[]'
while ((SECONDS<end)); do
  jobs="$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)"
  observed_jobs="$(jq -c --arg c "$C" --arg t "$TOKEN" '[.items[]|select(.metadata.labels["cnpg.io/cluster"]==$c and .metadata.labels["repro.stack.token"]==$t and any(.metadata.ownerReferences[]?;.kind=="Cluster" and .name==$c))|{name:.metadata.name,uid:.metadata.uid,role:.metadata.labels["cnpg.io/jobRole"],instance:.metadata.labels["cnpg.io/instanceName"],succeeded:(.status.succeeded//0),active:(.status.active//0)}]' <<<"$jobs")"
  cluster="$(k get cluster "$C" -n "$NS" -o json)"
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 2 ]]; then
    pods="$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)"
    replica="$(jq -r --arg c "$C" '[.items[]|select(.metadata.labels["cnpg.io/instanceRole"]=="replica" and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True"))|.metadata.name]|first//empty' <<<"$pods")"
    [[ -n "$replica" ]] && break
  fi
  sleep 10
done
[[ -n "$replica" ]] || die 'CNPG streaming replica did not become ready.'
pods="$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)"
jq -e --arg t "$TOKEN" --arg p "$primary" --arg r "$replica" '[.items[]|select(.metadata.labels["repro.stack.token"]==$t and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True"))] as $all | ($all|length)==2 and any($all[];.metadata.name==$p and .metadata.labels["cnpg.io/instanceRole"]=="primary") and any($all[];.metadata.name==$r and .metadata.labels["cnpg.io/instanceRole"]=="replica") and ($all[0].spec.nodeName!=$all[1].spec.nodeName)' <<<"$pods" >/dev/null || die 'Expected exactly two owned ready instances on separate nodes.'
primary_meta="$(sql "$primary" "$SQL")"; replica_meta="$(sql "$replica" "$SQL")"
jq -e --arg sync "ANY 1 (\"$C-2\")" '.settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s" and .settings.synchronous_standby_names==$sync' <<<"$primary_meta" >/dev/null || die 'Primary sender/receiver timeout or synchronous ANY 1 setting differs.'
jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$TARGET_TABLE" \
  '.systemIdentifier==$sys and .inRecovery==true and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.wal_level=="logical" and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0" and .settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s" and .settings.autovacuum=="on"' <<<"$replica_meta" >/dev/null || die 'Replica does not match the restored PostgreSQL fixture/default settings.'
lsn_caught_up(){ python3 - "$1" "$2" <<'PY'
import sys
def n(x):
 a,b=x.split('/'); return (int(a,16)<<32)+int(b,16)
try: print('true' if n(sys.argv[2])>=n(sys.argv[1]) else 'false')
except Exception: print('false')
PY
}
catchup_deadline=$((SECONDS+3600)); caught=false; target_lsn=''; replay_lsn=''
while (( SECONDS < catchup_deadline )); do
  target_lsn="$(sql "$primary" 'SELECT pg_current_wal_flush_lsn();' | tr -d '\r\n')" || die 'Could not read primary flush LSN.'
  replay_lsn="$(sql "$replica" "SELECT coalesce(pg_last_wal_replay_lsn()::text,'');" | tr -d '\r\n')" || die 'Could not read replica replay LSN.'
  caught="$(lsn_caught_up "$target_lsn" "$replay_lsn")"
  [[ "$caught" == true ]] && break
  sleep 5
done
[[ "$caught" == true ]] || die 'Replica did not replay through the primary flush LSN within one hour.'
app_count="$(sql "$primary" "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';" | tr -d '\r\n')"
replica_app_count="$(sql "$replica" "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';" | tr -d '\r\n')"
[[ "$app_count" == 0 && "$replica_app_count" == 0 ]] || die 'Unexpected application client sessions exist before the test.'
receiver="$(sql "$replica" "SELECT coalesce((SELECT status FROM pg_stat_wal_receiver LIMIT 1),'');" | tr -d '\r\n')"
[[ "$receiver" == streaming ]] || die 'Replica WAL receiver is not streaming.'

python3 "$ROOT/verify-attachments.py" >"$SD/attachment-verification.stdout.json" || die 'Live VM, PV, disk-SKU, or caching verification failed.'
for pair in "$VSD:$VCD:$SOURCE_DATA:$C-1" "$VSW:$VCW:$SOURCE_WAL:$C-1-wal"; do
  IFS=: read -r vss vsc sid claim <<<"$pair"
  content="$(k get volumesnapshotcontent "$vsc" -o json)"; pvc="$(k get pvc "$claim" -n "$NS" -o json)"
  jq -e --arg sid "$(lower "$sid")" '.spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$sid and (.status.snapshotHandle|ascii_downcase)==$sid' <<<"$content" >/dev/null || die "Final $claim static content handle changed."
  jq -e --arg t "$TOKEN" --arg vss "$vss" '.metadata.labels["repro.stack.token"]==$t and .status.phase=="Bound" and .spec.storageClassName=="azure-standard-hdd" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vss' <<<"$pvc" >/dev/null || die "Final $claim PVC no longer proves the exact retained snapshot source."
done

k get cluster "$C" -n "$NS" -o json >"$SD/measurement-cluster.json"; chmod 600 "$SD/measurement-cluster.json"
sha256sum "$SD/measurement-cluster.json" >"$SD/measurement-cluster.sha256"
snapshot_verification="$(jq -s '.' "$TMP/source-verification.jsonl")"
[[ -s "$LOCK_STATE" && -s "$LOCK_CYCLES" ]] || die 'Exact source lock-cycle receipts are missing.'
jq -e '.status=="restored" and .snapshotDeletionAttempted==false and .removed.resourceGroup==false and .removed.data==false and .removed.wal==false' "$LOCK_STATE" >/dev/null || die 'The source lock-cycle ledger does not prove all original locks restored.'
lock_cycle_events="$(jq -s '.' "$LOCK_CYCLES")"
[[ "$(jq 'length' <<<"$lock_cycle_events")" -ge 2 ]] || die 'The source lock-cycle receipt does not prove the exact role-assignment cleanup.'
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg token "$TOKEN" --arg loc "$LOC" --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg cluster "$NS/$C" --arg image "$MEASUREMENT_IMAGE" \
  --arg sourceData "$SOURCE_DATA" --arg sourceWal "$SOURCE_WAL" --arg sourceRg "$SOURCE_RG" --arg backup "$BACKUP_NAME" --arg backupUid "$BACKUP_UID" --arg sys "$SYSTEM_ID" --arg principal "$PRINCIPAL" \
  --arg dataUid "$SOURCE_DATA_UID" --arg walUid "$SOURCE_WAL_UID" --arg primaryPod "$primary" --arg replicaPod "$replica" --arg target "$target_lsn" --arg replay "$replay_lsn" --arg receiver "$receiver" \
  --argjson heap "$TARGET_HEAP" --argjson total "$TARGET_TABLE" --argjson snapshots "$snapshot_verification" --argjson primaryMeta "$primary_meta" --argjson replicaMeta "$replica_meta" --argjson jobs "$observed_jobs" --argjson lockState "$(cat "$LOCK_STATE")" --argjson lockCycles "$lock_cycle_events" --arg pairSha "$(jq -er '.provenance.copiedPairSha256' "$PROOF")" \
  '{version:1,status:"complete",verifiedAtUtc:$at,ownerToken:$token,location:$loc,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,cluster:$cluster,instances:2,measurementImage:$image,sourceKind:"reused-prepared-paired-snapshot",fixtureTargetHeapGiB:23,fixtureTargetHeapBytes:$heap,systemIdentifier:$sys,heapBytes:$heap,tableBytes:$total,preparedLiveTuplesEstimate:0,preparedDeadTuplesEstimate:0,fixture:{sourceKind:"reused-prepared-paired-snapshot",heapBytes:$heap,tableBytes:$total,payloadBytes:1536,firstLiveId:4,adjacentOddIdAbsent:true,indexName:"vacuum_target_id_idx",tableAutovacuumDisabled:true,snapshotBeforeVacuum:true},sourcePair:{manifest:"../shared/source-pair.json",manifestSha256:$pairSha,backupName:$backup,backupUid:$backupUid,resourceGroup:$sourceRg,dataSnapshotId:$sourceData,walSnapshotId:$sourceWal,dataUniqueId:$dataUid,walUniqueId:$walUid,liveAzureVerification:$snapshots,bootstrapMode:"CNPG offline recovery from both retained static DATA/WAL VolumeSnapshots",sourceSnapshotsCreatedByTestStack:false,requiredExternalWalArchive:false,archiveReferencesRemoved:true},sourceSnapshotRoleAssignments:{status:"removed",principalId:$principal,evidence:".state/source-snapshot-role-assignments.json",globalLock:".state/walreceiver-source-restore.lock",locksModified:true,locksRestoredVerified:true,originalLockDefinitionsRestored:true,lockStateEvidence:".state/source-snapshot-lock-cycle.json",lockCycleEvidence:".state/source-snapshot-lock-cycles.jsonl",lockState:$lockState,lockCycles:$lockCycles},replicaSeed:{method:"CNPG streaming join/basebackup from restored primary",completedJobs:$jobs,receiverStatus:$receiver,primaryFlushLsn:$target,replicaReplayLsn:$replay,caughtUp:true},finalRoles:{primary:$primaryPod,replica:$replicaPod},clientCreated:false,snapshotDeletionPolicy:"Retain",noBackupCRCreated:true,settings:{primary:$primaryMeta.settings,replica:$replicaMeta.settings}}' >"$TMP/prepare.json"
chmod 600 "$TMP/prepare.json"; mv "$TMP/prepare.json" "$FIXTURE"
printf 'Prepared the EastUS 23-GiB fixture from the retained offline DATA/WAL pair; no source snapshots were created or deleted, and the original retention locks were restored and verified after temporary exact role-grant cleanup.\n'
