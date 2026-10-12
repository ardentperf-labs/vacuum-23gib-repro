#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SD="$ROOT/.state"
STATE="$SD/state.json"
META="$ROOT/westus3-source-pair.json"
PAIR_PROOF="$ROOT/westus3-source-pair-proof.json"
RESTORE_PROOF="$ROOT/westus3-restore-proof.json"
FIXTURE="$SD/prepare.json"
NS=repro
C=hdd-pg
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
MEASUREMENT_IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
TARGET_HEAP=24696061952
ROLE_ID=7efff54f-a5b4-42b5-a1c5-5411624893ce
TMP=''
GRANTS="$SD/source-snapshot-role-assignments.json"
LOCK_CYCLES="$SD/source-snapshot-lock-cycles.jsonl"
LOCK_ARMED=false
ACTIVE_ID=''
ACTIVE_LOCK_NAME=''
ACTIVE_LOCK_NOTES=''
ACTIVE_LOCK_KIND=''
ACTIVE_SNAPSHOT_UID=''
ACTIVE_LOCK_BEFORE_JSON='{}'
ACTIVE_ROLE_OPERATION=''
ACTIVE_ROLE_ASSIGNMENT_ID=''
ACTIVE_UNLOCK_RC=125
ACTIVE_ROLE_OPERATION_RC=125
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
azj(){ az "$@" --subscription "$SUB" -o json; }
k(){ kubectl --kubeconfig "$KC" "$@"; }
sql(){ k exec -i -c postgres -n "$NS" "$1" -- psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }

[[ -s "$META" && -s "$PAIR_PROOF" && -s "$RESTORE_PROOF" ]] || die 'Selected WestUS3 source manifest or exact-pair proof is missing.'
command -v az >/dev/null || die 'Missing az.'
command -v jq >/dev/null || die 'Missing jq.'
command -v python3 >/dev/null || die 'Missing python3.'
SUB="$(jq -er '.subscriptionId' "$META")"
SOURCE_DATA="$(jq -er '.data.id' "$META")"
SOURCE_WAL="$(jq -er '.wal.id' "$META")"
SOURCE_DATA_UID="$(jq -er '.data.uniqueId' "$META")"
SOURCE_WAL_UID="$(jq -er '.wal.uniqueId' "$META")"
SOURCE_DATA_DISK="$(jq -er '.data.sourceDiskId' "$META")"
SOURCE_WAL_DISK="$(jq -er '.wal.sourceDiskId' "$META")"
SOURCE_RG="$(jq -er '.sourceResourceGroup' "$META")"
SOURCE_RG_TOKEN="$(jq -er '.sourceResourceGroupOwnerToken' "$META")"
DATA_ID="$SOURCE_DATA"
WAL_ID="$SOURCE_WAL"
ORIGINAL_SOURCE_RG="$SOURCE_RG"
BACKUP_NAME="$(jq -er '.backupName' "$META")"
SYSTEM_ID="$(jq -er '.systemIdentifier' "$META")"
[[ "$(jq -r '.selection.selected // false' "$META")" == true ]] || die 'The WestUS3 manifest is not explicitly selected.'
[[ "$SOURCE_DATA" != "$SOURCE_WAL" && "${SOURCE_DATA,,}" != "${SOURCE_WAL,,}" ]] || die 'Selected DATA/WAL source IDs are not distinct.'
[[ "$(jq -r '.location|ascii_downcase' "$META")" == westus3 && "$(jq -r '.fixture.heapBytes' "$META")" == "$TARGET_HEAP" ]] || die 'Selected source is not the proven WestUS3 23-GiB fixture.'
[[ "$(jq -r '.selection.localProofs.sourcePair.file' "$META")" == "${PAIR_PROOF##*/}" && "$(sha256sum "$PAIR_PROOF" | awk '{print $1}')" == "$(jq -er '.selection.localProofs.sourcePair.sha256' "$META")" ]] || die 'Selected source-pair proof is missing or its pinned SHA-256 changed.'
[[ "$(jq -r '.selection.localProofs.priorRestore.file' "$META")" == "${RESTORE_PROOF##*/}" && "$(sha256sum "$RESTORE_PROOF" | awk '{print $1}')" == "$(jq -er '.selection.localProofs.priorRestore.sha256' "$META")" ]] || die 'Exact-pair restore proof is missing or its pinned SHA-256 changed.'
jq -e --arg d "${SOURCE_DATA,,}" --arg w "${SOURCE_WAL,,}" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" --arg sys "$SYSTEM_ID" \
  '(.data.id|ascii_downcase)==$d and (.wal.id|ascii_downcase)==$w and .data.uniqueId==$du and .wal.uniqueId==$wu and .systemIdentifier==$sys and .location=="westus3" and .fixture.heapBytes==24696061952' "$PAIR_PROOF" >/dev/null || die 'Selected manifest fields differ from its hash-pinned local source-pair proof.'
jq -e --arg d "${SOURCE_DATA,,}" --arg w "${SOURCE_WAL,,}" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" \
  '.verifiedBeforeTuning==true and .sourcePairRestore.fixture.verifiedBeforeTuning==true and (.sourcePairRestore.data.snapshotId|ascii_downcase)==$d and (.sourcePairRestore.wal.snapshotId|ascii_downcase)==$w and .sourcePairRestore.data.uniqueId==$du and .sourcePairRestore.wal.uniqueId==$wu and .sourcePairRestore.fixture.systemIdentifier==$sys and .sourcePairRestore.fixture.heapBytes==$heap and .sourcePairRestore.fixture.sourceKind=="reused-prepared-paired-snapshot"' "$RESTORE_PROOF" >/dev/null || die 'Local prior restore proof does not match the exact selected WestUS3 pair.'
if [[ "${1:-}" == --validate-source-only ]]; then
  [[ "$(az account show --query id -o tsv)" == "$SUB" ]] || die 'Active Azure subscription does not match source manifest.'
  rg_live="$(az group show -n "$SOURCE_RG" --subscription "$SUB" -o json)" || die 'Cannot inspect selected source snapshot group ownership.'
  jq -e --arg name "${SOURCE_RG,,}" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and .tags.reprotoken==$token and .tags.reproPurpose=="experiment-snapshots"' <<<"$rg_live" >/dev/null || die 'Selected source snapshot group ownership does not match the pinned source pair.'
  for kind in data wal; do
    if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; uid="$SOURCE_DATA_UID"; disk="$SOURCE_DATA_DISK"; else sid="$SOURCE_WAL"; uid="$SOURCE_WAL_UID"; disk="$SOURCE_WAL_DISK"; fi
    snap="$(az snapshot show --ids "$sid" --subscription "$SUB" -o json)" || die "Cannot inspect pinned $kind snapshot."
    jq -e --arg id "${sid,,}" --arg uid "$uid" --arg disk "${disk,,}" \
      '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.location|ascii_downcase)=="westus3" and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .diskSizeGB==512 and .incremental==true and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk' <<<"$snap" >/dev/null || die "Pinned WestUS3 $kind snapshot identity/state check failed."
    locks="$(az lock list --resource "$sid" --subscription "$SUB" -o json)" || die "Cannot inspect pinned $kind snapshot retention lock."
    jq -e --arg id "${sid,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and .[0].notes=="Preserve every snapshot until explicit user deletion authorization." and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$locks" >/dev/null || die "Pinned WestUS3 $kind snapshot retention lock check failed."
  done
  printf 'Selected WestUS3 snapshot IDs, unique IDs, source disks, retention locks, restore proof, and fixture verify read-only; no WAL archive is required.\n'
  exit 0
fi
RESUME=false
if [[ $# == 1 && "${1:-}" == --resume-restored-primary ]]; then
  RESUME=true
elif [[ $# != 0 ]]; then
  die 'Usage: 02-prepare.sh [--validate-source-only|--resume-restored-primary]'
fi
command -v kubectl >/dev/null || die 'Missing kubectl.'
command -v flock >/dev/null || die 'Missing flock.'
[[ -s "$STATE" ]] || die 'Need provisioned .state/state.json.'
SUB="$(jq -er '.subscriptionId' "$STATE")"
TOKEN="$(jq -er '.ownerToken' "$STATE")"
RG="$(jq -er '.resourceGroup' "$STATE")"
AKS="$(jq -er '.clusterName' "$STATE")"
LOC="$(jq -er '.location' "$STATE")"
KC="$(jq -er '.kubeconfigPath' "$STATE")"
[[ "$KC" == "$SD/kubeconfig" ]] || die 'Kubeconfig path is not the isolated stack kubeconfig.'
[[ "$(jq -r '.status' "$STATE")" == ready ]] || die 'AKS stack is not ready.'
[[ "$SUB" == "$(jq -er '.subscriptionId' "$META")" ]] || die 'Selected source subscription mismatch.'
[[ "${LOC,,}" == westus3 && "$(jq -r '.location|ascii_downcase' "$META")" == "${LOC,,}" ]] || die 'AKS and selected original snapshot pair must both be in WestUS3.'
[[ -f "$KC" ]] || die 'Isolated kubeconfig is missing.'
[[ ! -e "$FIXTURE" ]] || die 'prepare.json already exists; refusing to overwrite a completed or partial preparation.'
if [[ "$RESUME" == true ]]; then
  [[ -s "$GRANTS" && -s "$LOCK_CYCLES" ]] || die 'Resume requires the existing removed-grants ledger and lock-cycle receipts.'
else
  [[ ! -e "$GRANTS" && ! -e "$LOCK_CYCLES" ]] || die 'Source permission/lock lifecycle evidence already exists; refusing to adopt it.'
fi
exec 9>"$SD/02-prepare.lock"; flock -n 9 || die 'Preparation is already running.'
TMP="$(mktemp -d "$SD/prepare-work.XXXXXX")"
mkdir -p "$SD"
[[ "$(jq -r '.status' "$STATE")" == ready ]] || die 'Stack state changed during preparation.'
[[ "$SOURCE_DATA" != "$SOURCE_WAL" && "$DATA_ID" != "$WAL_ID" ]] || die 'DATA/WAL source pair is not distinct.'
# Recheck only the exact selected WestUS3 originals; never mix another pair or its WAL archive.
rg_live="$(azj group show -n "$SOURCE_RG")" || die 'Cannot inspect selected source snapshot group ownership.'
jq -e --arg name "${SOURCE_RG,,}" --arg token "$SOURCE_RG_TOKEN" '(.name|ascii_downcase)==$name and .tags.reprotoken==$token and .tags.reproPurpose=="experiment-snapshots"' <<<"$rg_live" >/dev/null || die 'Selected source snapshot group ownership does not match the pinned source pair.'
for kind in data wal; do
  if [[ "$kind" == data ]]; then sid="$SOURCE_DATA"; expected_uid="$SOURCE_DATA_UID"; expected_disk="$SOURCE_DATA_DISK"; else sid="$SOURCE_WAL"; expected_uid="$SOURCE_WAL_UID"; expected_disk="$SOURCE_WAL_DISK"; fi
  live="$(azj snapshot show --ids "$sid")" || die "Cannot inspect selected original $kind snapshot."
  jq -e --arg id "${sid,,}" --arg uid "$expected_uid" --arg disk "${expected_disk,,}" --arg rg "${SOURCE_RG,,}" \
    '(.id|ascii_downcase)==$id and .uniqueId==$uid and (.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)=="westus3" and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .incremental==true and .diskSizeGB==512 and .sku.name=="Standard_ZRS" and (.creationData.sourceResourceId|ascii_downcase)==$disk and (.tags.source_volume_id|ascii_downcase)==$disk' <<<"$live" >/dev/null || die "Selected original $kind snapshot live identity/state mismatch."
  locks="$(azj lock list --resource "$sid")" || die "Cannot inspect selected $kind snapshot retention lock."
  jq -e --arg id "${sid,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and .[0].notes=="Preserve every snapshot until explicit user deletion authorization." and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$locks" >/dev/null || die "Selected $kind snapshot does not have its exact expected CanNotDelete lock."
  jq --arg kind "$kind" --arg id "$sid" --arg uid "$expected_uid" --arg disk "$expected_disk" --argjson snapshot "$live" --argjson lock "$(jq -c '.[0]' <<<"$locks")" \
    '{kind:$kind,snapshotId:$id,uniqueId:$uid,sourceDiskId:$disk,liveLocation:$snapshot.location,provisioningState:$snapshot.provisioningState,snapshotAccessState:$snapshot.snapshotAccessState,diskSizeGB:$snapshot.diskSizeGB,sku:$snapshot.sku.name,lock:$lock}' >>"$TMP/snapshot-verification.jsonl"
done

active="$(azj account show | jq -r '.id')"
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active Azure subscription does not match stack state.'
ns="$(k get namespace "$NS" -o json)" || die 'The repro namespace is missing.'
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' <<<"$ns" >/dev/null || die 'Namespace ownership label mismatch.'
sc="$(k get storageclass azure-standard-hdd -o json)" || die 'Standard HDD StorageClass is missing.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="ReadOnly" and .reclaimPolicy=="Delete" and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$sc" >/dev/null || die 'StorageClass is not Standard_LRS with ReadOnly caching.'
SHORT="${TOKEN:0:8}"
VSC="source-pair-$SHORT"
VCD="source-data-content-$SHORT"; VCW="source-wal-content-$SHORT"
VSD="source-data-$SHORT"; VSW="source-wal-$SHORT"

if [[ "$RESUME" == true ]]; then
  PRINCIPAL="$(jq -er '.principalId' "$GRANTS")" || die 'Resume grants ledger has no principal ID.'
  DATA_ASSIGNMENT_ID="$(jq -er '.assignments[]|select(.kind=="data")|.assignmentId' "$GRANTS")" || die 'Resume grants ledger has no DATA assignment ID.'
  WAL_ASSIGNMENT_ID="$(jq -er '.assignments[]|select(.kind=="wal")|.assignmentId' "$GRANTS")" || die 'Resume grants ledger has no WAL assignment ID.'
  jq -e --arg token "$TOKEN" --arg principal "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg d "${DATA_ID,,}" --arg w "${WAL_ID,,}" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" \
    '.version==2 and .status=="removed" and .sourceKind=="selected-original-westus3-pair" and .postRemovalConfirmed==true and .ownerToken==$token and .principalId==$principal and (.roleDefinitionId|ascii_downcase)==$role and (.assignments|length)==2 and ([.assignments[]|select(.kind=="data" and (.snapshotId|ascii_downcase)==$d and .uniqueId==$du and (.scope|ascii_downcase)==$d and .createdByThisStack==true and .removed==true and .postRemovalRoleListAbsent==true and .lockAfter.level=="CanNotDelete" and .lockAfter.name=="preserve-all-snapshots" and .lockAfter.notes=="Preserve every snapshot until explicit user deletion authorization.")]|length)==1 and ([.assignments[]|select(.kind=="wal" and (.snapshotId|ascii_downcase)==$w and .uniqueId==$wu and (.scope|ascii_downcase)==$w and .createdByThisStack==true and .removed==true and .postRemovalRoleListAbsent==true and .lockAfter.level=="CanNotDelete" and .lockAfter.name=="preserve-all-snapshots" and .lockAfter.notes=="Preserve every snapshot until explicit user deletion authorization.")]|length)==1' "$GRANTS" >/dev/null || die 'Resume grants ledger does not prove this stack removed only its exact pair grants and recorded both retained locks.'
  jq -s -e --arg d "${DATA_ID,,}" --arg w "${WAL_ID,,}" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" --arg daid "${DATA_ASSIGNMENT_ID,,}" --arg waid "${WAL_ASSIGNMENT_ID,,}" \
    'reduce .[] as $event ({}; .[($event.snapshotId|ascii_downcase)]=$event) | (keys|length)==2 and has($d) and has($w) and .[$d].uniqueId==$du and .[$w].uniqueId==$wu and (.[$d].roleAssignmentId|ascii_downcase)==$daid and (.[$w].roleAssignmentId|ascii_downcase)==$waid and all(.[];.roleOperation=="delete" and .temporaryUnlockExitCode==0 and .roleOperationExitCode==0 and .lockRestoreVerified==true and .snapshotDeletionAttempted==false and .lockAfter.name=="preserve-all-snapshots" and .lockAfter.level=="CanNotDelete" and .lockAfter.notes=="Preserve every snapshot until explicit user deletion authorization.")' "$LOCK_CYCLES" >/dev/null || die 'Latest exact-pair lock receipts do not prove both grant deletions restored CanNotDelete locks.'

  vsc_obj="$(k get volumesnapshotclass "$VSC" -o json)" || die 'Resume could not find the existing stack-owned VolumeSnapshotClass.'
  jq -e --arg token "$TOKEN" --arg rg "$SOURCE_RG" --arg name "$VSC" '.metadata.name==$name and .metadata.labels["repro.stack.token"]==$token and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain" and .parameters.resourceGroup==$rg' <<<"$vsc_obj" >/dev/null || die 'Existing VolumeSnapshotClass ownership/Retain/resource-group checks failed.'
  for kind in data wal; do
    if [[ "$kind" == data ]]; then vss="$VSD"; vsc="$VCD"; sid="$DATA_ID"; uid="$SOURCE_DATA_UID"; expected_ns="$NS"; else vss="$VSW"; vsc="$VCW"; sid="$WAL_ID"; uid="$SOURCE_WAL_UID"; expected_ns="$NS"; fi
    snapshot_obj="$(k get volumesnapshot "$vss" -n "$NS" -o json)" || die "Resume could not find existing $kind VolumeSnapshot."
    content_obj="$(k get volumesnapshotcontent "$vsc" -o json)" || die "Resume could not find existing $kind VolumeSnapshotContent."
    jq -e --arg token "$TOKEN" --arg class "$VSC" --arg content "$vsc" '.metadata.labels["repro.stack.token"]==$token and .status.readyToUse==true and .spec.volumeSnapshotClassName==$class and .spec.source.volumeSnapshotContentName==$content' <<<"$snapshot_obj" >/dev/null || die "Existing $kind VolumeSnapshot is not ready, stack-owned, and bound to the expected content."
    jq -e --arg token "$TOKEN" --arg class "$VSC" --arg id "${sid,,}" --arg name "$vss" --arg ns "$expected_ns" '(.metadata.labels["repro.stack.token"]==$token) and .spec.deletionPolicy=="Retain" and .spec.driver=="disk.csi.azure.com" and .spec.volumeSnapshotClassName==$class and (.spec.source.snapshotHandle|ascii_downcase)==$id and .spec.volumeSnapshotRef.kind=="VolumeSnapshot" and .spec.volumeSnapshotRef.name==$name and .spec.volumeSnapshotRef.namespace==$ns and .status.readyToUse==true and (.status.snapshotHandle|ascii_downcase)==$id' <<<"$content_obj" >/dev/null || die "Existing $kind VolumeSnapshotContent does not pin the exact retained original snapshot handle."
  done

  cluster="$(k get cluster "$C" -n "$NS" -o json)" || die 'Resume could not find the existing CNPG primary Cluster.'
  jq -e --arg token "$TOKEN" --arg image "$IMAGE" --arg d "${DATA_ID,,}" --arg w "${WAL_ID,,}" --arg vssd "$VSD" --arg vssw "$VSW" \
    '.metadata.labels["repro.stack.token"]==$token and (.metadata.annotations["vacuum-only-repro/source-data-snapshot-id"]|ascii_downcase)==$d and (.metadata.annotations["vacuum-only-repro/source-wal-snapshot-id"]|ascii_downcase)==$w and (.metadata.annotations["vacuum-only-repro/pinned-source-data-snapshot-id"]|ascii_downcase)==$d and (.metadata.annotations["vacuum-only-repro/pinned-source-wal-snapshot-id"]|ascii_downcase)==$w and .spec.imageName==$image and .spec.instances==1 and .spec.backup==null and .spec.externalClusters==null and .spec.bootstrap.recovery.source==null and .spec.bootstrap.recovery.volumeSnapshots.storage.name==$vssd and .spec.bootstrap.recovery.volumeSnapshots.walStorage.name==$vssw and .status.readyInstances==1 and .status.currentPrimary=="hdd-pg-1" and .status.image==$image' <<<"$cluster" >/dev/null || die 'Resume primary Cluster is not the exact ready one-instance system-image snapshot restore.'
  primary="$C-1"
  pod="$(k get pod "$primary" -n "$NS" -o json)" || die 'Resume could not find the restored primary pod.'
  jq -e --arg token "$TOKEN" --arg cluster "$C" --arg image "$IMAGE" '.metadata.labels["repro.stack.token"]==$token and .metadata.labels["cnpg.io/cluster"]==$cluster and .metadata.labels["cnpg.io/instanceRole"]=="primary" and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True") and any(.spec.containers[];.name=="postgres" and .image==$image)' <<<"$pod" >/dev/null || die 'Resume primary pod runtime/image/ownership checks failed.'
  pvc_json="$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)" || die 'Resume could not inspect primary DATA/WAL PVCs.'
  jq -e --arg d "$C-1" --arg w "$C-1-wal" --arg token "$TOKEN" --arg vssd "$VSD" --arg vssw "$VSW" '[.items[]|select(.metadata.name==$d or .metadata.name==$w)] as $p | ($p|length)==2 and all($p[];.metadata.labels["repro.stack.token"]==$token and .status.phase=="Bound" and .spec.storageClassName=="azure-standard-hdd" and .spec.resources.requests.storage=="512Gi") and any($p[];.metadata.name==$d and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vssd) and any($p[];.metadata.name==$w and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vssw)' <<<"$pvc_json" >/dev/null || die 'Resume DATA/WAL PVCs do not prove the exact owned static snapshot sources.'
  data_pv="$(jq -er --arg n "$C-1" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
  wal_pv="$(jq -er --arg n "$C-1-wal" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
  [[ -n "$data_pv" && -n "$wal_pv" && "$data_pv" != "$wal_pv" ]] || die 'Resume primary DATA/WAL PVs are missing or not separate.'
  resume_sql="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id=4),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=5),'indexName',(SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='public.vacuum_target'::regclass AND indexrelid='public.vacuum_target_id_idx'::regclass),'tableAutovacuumDisabled',EXISTS(SELECT 1 FROM pg_class WHERE oid='public.vacuum_target'::regclass AND 'autovacuum_enabled=false'=ANY(reloptions)),'settings',(SELECT json_object_agg(name,current_setting(name)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay'])))::text;"
  resume_primary_meta="$(sql "$primary" "$resume_sql")" || die 'Resume could not query the existing primary fixture read-only.'
  jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$(jq -er '.fixture.tableBytes' "$META")" \
    '.systemIdentifier==$sys and .inRecovery==false and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.wal_level=="logical" and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0"' <<<"$resume_primary_meta" >/dev/null || die 'Resume primary runtime fixture/system identity/default GUC checks failed.'
  recovery_source_removed=true
  trap 'rc=$?; if [[ -n "$TMP" && -d "$TMP" ]]; then if ((rc==0)); then rm -rf -- "$TMP"; else printf "Resume evidence retained at %s\n" "$TMP" >&2; fi; fi' EXIT
else
for kind_name in "volumesnapshotclass:$VSC" "volumesnapshotcontent:$VCD" "volumesnapshotcontent:$VCW" "volumesnapshot:$VSD" "volumesnapshot:$VSW" "cluster:$C"; do
  kind="${kind_name%%:*}"; name="${kind_name#*:}"
  if k get "$kind" "$name" -n "$NS" -o json >/dev/null 2>&1; then die "$kind/$name already exists; refusing to adopt it."; fi
done

# Grant only the AKS control-plane identity exact temporary access to these two pinned snapshots.
# Role grants are created at exact snapshot scopes. Each grant is removed inside a narrow
# direct-lock cycle; the CanNotDelete lock is reverified immediately after every role operation.
aks_obj="$(azj aks show -g "$RG" -n "$AKS")" || die 'Cannot inspect owned AKS identity.'
PRINCIPAL="$(jq -er '.identity.principalId|select(type=="string" and length>0)' <<<"$aks_obj")" || die 'AKS control-plane principal is missing.'
role_name="$(az role definition list --name "$ROLE_ID" --subscription "$SUB" --query '[0].roleName' -o tsv)"
[[ "$role_name" == 'Disk Snapshot Contributor' ]] || die 'Pinned role ID is not Disk Snapshot Contributor.'
jq -n --arg token "$TOKEN" --arg p "$PRINCIPAL" --arg role "$ROLE_ID" --arg d "$DATA_ID" --arg w "$WAL_ID" --arg du "$SOURCE_DATA_UID" --arg wu "$SOURCE_WAL_UID" \
  '{version:2,status:"creating",sourceKind:"selected-original-westus3-pair",ownerToken:$token,principalId:$p,roleDefinitionId:$role,assignments:[{kind:"data",snapshotId:$d,uniqueId:$du,scope:$d,assignmentId:null,createdByThisStack:false,removed:false},{kind:"wal",snapshotId:$w,uniqueId:$wu,scope:$w,assignmentId:null,createdByThisStack:false,removed:false}]}' >"$GRANTS"
chmod 600 "$GRANTS"
set_grant(){
  local gkind="$1" patch="$2" tmpfile
  tmpfile="$(mktemp "$SD/.grants.XXXXXX")"
  jq --arg k "$gkind" --argjson patch "$patch" '.assignments|=map(if .kind==$k then . * $patch else . end)' "$GRANTS" >"$tmpfile"
  mv -f "$tmpfile" "$GRANTS"; chmod 600 "$GRANTS"
}
restore_active_lock(){
  [[ "$LOCK_ARMED" == true ]] || return 0
  local current rc created lock_after='null'
  set +e; current="$(azj lock list --resource "$ACTIVE_ID" 2>/dev/null)"; rc=$?; set -e
  if [[ "$rc" -ne 0 ]]; then
    echo "FATAL: cannot inspect lock state for $ACTIVE_ID" >&2
    record_lock_cycle false null 'lock query failed'
    return 1
  fi
  if ! jq -e --arg id "${ACTIVE_ID,,}" --arg name "$ACTIVE_LOCK_NAME" --arg notes "$ACTIVE_LOCK_NOTES" 'length==1 and .[0].name==$name and .[0].level=="CanNotDelete" and .[0].notes==$notes and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$current" >/dev/null; then
    if [[ "$(jq 'length' <<<"$current")" != 0 ]]; then
      echo "FATAL: unexpected direct lock state for $ACTIVE_ID; refusing to alter it" >&2
      record_lock_cycle false "$current" 'unexpected direct lock state'
      return 1
    fi
    set +e; created="$(azj lock create --name "$ACTIVE_LOCK_NAME" --lock-type CanNotDelete --resource "$ACTIVE_ID" --notes "$ACTIVE_LOCK_NOTES" 2>/dev/null)"; rc=$?; set -e
    if [[ "$rc" -ne 0 ]]; then
      echo "FATAL: failed restoring CanNotDelete lock for $ACTIVE_ID" >&2
      record_lock_cycle false null 'lock create failed'
      return 1
    fi
    printf '%s\n' "$created" >"$SD/restored-source-lock-$(basename "${ACTIVE_ID,,}").json"; chmod 600 "$SD/restored-source-lock-$(basename "${ACTIVE_ID,,}").json"
  fi
  current="$(azj lock list --resource "$ACTIVE_ID")" || { record_lock_cycle false null 'post-restore lock query failed'; return 1; }
  if ! jq -e --arg id "${ACTIVE_ID,,}" --arg name "$ACTIVE_LOCK_NAME" --arg notes "$ACTIVE_LOCK_NOTES" 'length==1 and .[0].name==$name and .[0].level=="CanNotDelete" and .[0].notes==$notes and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$current" >/dev/null; then
    echo "FATAL: CanNotDelete lock restore is not verified for $ACTIVE_ID" >&2
    record_lock_cycle false "$current" 'post-restore lock mismatch'
    return 1
  fi
  lock_after="$(jq -c '.[0]' <<<"$current")"
  record_lock_cycle true "$lock_after" ''
  LOCK_ARMED=false; ACTIVE_ID=''; ACTIVE_LOCK_NAME=''; ACTIVE_LOCK_NOTES=''
}
record_lock_cycle(){
  local restored="$1" lock_after="$2" error="$3" at
  [[ -n "$ACTIVE_ID" ]] || return 0
  at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  jq -cn --arg kind "$ACTIVE_LOCK_KIND" --arg id "$ACTIVE_ID" --arg uid "$ACTIVE_SNAPSHOT_UID" --arg operation "$ACTIVE_ROLE_OPERATION" --arg aid "$ACTIVE_ROLE_ASSIGNMENT_ID" --arg at "$at" --arg err "$error" --argjson lockBefore "$ACTIVE_LOCK_BEFORE_JSON" --argjson unlock "$ACTIVE_UNLOCK_RC" --argjson roleRc "$ACTIVE_ROLE_OPERATION_RC" --argjson restored "$restored" --argjson lockAfter "$lock_after" '{kind:$kind,snapshotId:$id,uniqueId:$uid,roleOperation:$operation,roleAssignmentId:$aid,lockBefore:$lockBefore,temporaryUnlockExitCode:$unlock,roleOperationExitCode:$roleRc,lockRestoreVerified:$restored,lockAfter:$lockAfter,error:(if $err=="" then null else $err end),snapshotDeletionAttempted:false,verifiedAtUtc:$at}' >>"$LOCK_CYCLES" 2>/dev/null || printf 'Could not append source lock lifecycle receipt for %s\n' "$ACTIVE_ID" >&2
  chmod 600 "$LOCK_CYCLES" 2>/dev/null || true
}
begin_lock_cycle(){
  local scope="$1" locks all_locks sub_scope rg_scope
  locks="$(azj lock list --resource "$scope")" || { echo "Cannot inspect direct retention lock on $scope." >&2; return 1; }
  jq -e --arg id "${scope,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and .[0].notes=="Preserve every snapshot until explicit user deletion authorization." and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$locks" >/dev/null || { echo "Expected direct CanNotDelete retention lock is absent or changed on $scope." >&2; return 1; }
  ACTIVE_LOCK_NAME="$(jq -er '.[0].name' <<<"$locks")"
  ACTIVE_LOCK_NOTES="$(jq -er '.[0].notes' <<<"$locks")"
  ACTIVE_ID="$scope"
  ACTIVE_LOCK_BEFORE_JSON="$(jq -c '.[0]' <<<"$locks")"
  ACTIVE_UNLOCK_RC=125
  ACTIVE_ROLE_OPERATION_RC=125
  sub_scope="/subscriptions/$SUB"; rg_scope="/subscriptions/$SUB/resourceGroups/$SOURCE_RG"
  all_locks="$(azj lock list)" || { echo 'Cannot inspect inherited lock inventory before source role operation.' >&2; return 1; }
  jq -e --arg rg "${rg_scope,,}" --arg sub "${sub_scope,,}" '[.[]|(.id|ascii_downcase|split("/providers/microsoft.authorization/locks/")[0])]|any(.[];.==$rg or .==$sub)' <<<"$all_locks" >/dev/null && { echo 'Inherited resource-group/subscription lock blocks the narrow source lock cycle.' >&2; return 1; }
  LOCK_ARMED=true
  set +e; az lock delete --ids "$(jq -er '.[0].id' <<<"$locks")" --subscription "$SUB" -o none >/dev/null; ACTIVE_UNLOCK_RC=$?; set -e
  [[ "$ACTIVE_UNLOCK_RC" -eq 0 ]] || { echo "Cannot temporarily unlock exact selected source snapshot $scope." >&2; return 1; }
  locks="$(azj lock list --resource "$scope")" || { echo "Cannot verify temporary unlock for $scope." >&2; return 1; }
  [[ "$(jq 'length' <<<"$locks")" == 0 ]] || { echo "Direct lock remains on $scope after unlock request." >&2; return 1; }
}
remove_grants(){
  [[ -s "$GRANTS" ]] || return 0
  local gkind scope aid role_rows lock_before lock_after live uid removed created matches
  for gkind in data wal; do
    scope="$(jq -r --arg k "$gkind" '.assignments[]|select(.kind==$k)|.scope' "$GRANTS")"
    aid="$(jq -r --arg k "$gkind" '.assignments[]|select(.kind==$k)|.assignmentId // empty' "$GRANTS")"
    removed="$(jq -r --arg k "$gkind" '.assignments[]|select(.kind==$k)|.removed // false' "$GRANTS")"
    created="$(jq -r --arg k "$gkind" '.assignments[]|select(.kind==$k)|.createdByThisStack // false' "$GRANTS")"
    if [[ "$removed" != true && -z "$aid" && "$created" == true ]]; then
      role_rows="$(azj role assignment list --scope "$scope")" || return 1
      matches="$(jq -c --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" '[.[]|select(.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)]' <<<"$role_rows")"
      if [[ "$(jq 'length' <<<"$matches")" == 0 ]]; then
        set_grant "$gkind" '{"createdByThisStack":false,"creationFailed":true,"removed":true,"postRemovalRoleListAbsent":true}'
        continue
      fi
      [[ "$(jq 'length' <<<"$matches")" == 1 ]] || return 1
      aid="$(jq -er '.[0].id' <<<"$matches")"
      set_grant "$gkind" "$(jq -cn --arg aid "$aid" '{assignmentId:$aid}')"
    fi
    [[ "$removed" == true || -n "$aid" ]] || continue
    uid="$(jq -er --arg k "$gkind" '.assignments[]|select(.kind==$k)|.uniqueId' "$GRANTS")"
    lock_before="$(azj lock list --resource "$scope")" || return 1
    if [[ "$removed" != true ]]; then
      role_rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg aid "${aid,,}" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" 'any(.[];.principalId==$p and (.id|ascii_downcase)==$aid and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$role_rows" >/dev/null || return 1
      ACTIVE_LOCK_KIND="$gkind"; ACTIVE_SNAPSHOT_UID="$uid"; ACTIVE_ROLE_OPERATION='delete'; ACTIVE_ROLE_ASSIGNMENT_ID="$aid"
      begin_lock_cycle "$scope" || return 1
      set +e; az role assignment delete --ids "$aid" --subscription "$SUB" -o none >/dev/null; ACTIVE_ROLE_OPERATION_RC=$?; set -e
      [[ "$ACTIVE_ROLE_OPERATION_RC" -eq 0 ]] || return 1
      role_rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" 'all(.[];.principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$role_rows" >/dev/null || return 1
      restore_active_lock || return 1
    else
      role_rows="$(azj role assignment list --scope "$scope")" || return 1
      jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" 'all(.[];.principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$role_rows" >/dev/null || return 1
    fi
    lock_after="$(azj lock list --resource "$scope")" || return 1
    jq -e --arg id "${scope,,}" --arg name "preserve-all-snapshots" 'length==1 and .[0].name==$name and .[0].level=="CanNotDelete" and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$lock_after" >/dev/null || return 1
    set_grant "$gkind" "$(jq -cn --argjson lockBefore "$(jq -c '.[0]' <<<"$lock_before")" --argjson lockAfter "$(jq -c '.[0]' <<<"$lock_after")" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{removed:true,postRemovalRoleListAbsent:true,lockBefore:$lockBefore,lockAfter:$lockAfter,removedAtUtc:$at}')"
    live="$(azj snapshot show --ids "$scope")" || return 1
    jq -e --arg id "${scope,,}" --arg uid "$uid" '(.id|ascii_downcase)==$id and .uniqueId==$uid and .provisioningState=="Succeeded" and .snapshotAccessState=="Available"' <<<"$live" >/dev/null || return 1
  done
  jq '.status="removed"|.postRemovalConfirmed=true' "$GRANTS" >"$GRANTS.tmp"; mv -f "$GRANTS.tmp" "$GRANTS"; chmod 600 "$GRANTS"
}
trap 'rc=$?; if ((rc!=0)); then remove_grants || printf "Temporary snapshot grants/lock cycle needs review: %s\n" "$GRANTS" >&2; fi; restore_active_lock || printf "CanNotDelete lock restoration needs immediate review for %s\n" "$ACTIVE_ID" >&2; if [[ -n "$TMP" && -d "$TMP" ]]; then if ((rc==0)); then rm -rf -- "$TMP"; else printf "Preparation evidence retained at %s\n" "$TMP" >&2; fi; fi' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for kind in data wal; do
  scope="$DATA_ID"; [[ "$kind" == wal ]] && scope="$WAL_ID"
  before="$(azj role assignment list --scope "$scope")" || die "Cannot inspect $kind snapshot permissions."
  if jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" 'any(.[]; .principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$before" >/dev/null; then die "Unexpected pre-existing exact $kind snapshot permission; refusing to adopt it."; fi
  lock_before="$(azj lock list --resource "$scope")" || die "Cannot capture the $kind snapshot retention lock before grant creation."
  jq -e --arg id "${scope,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$lock_before" >/dev/null || die "The $kind snapshot retention lock changed before grant creation."
  set_grant "$kind" "$(jq -cn --argjson lock "$(jq -c '.[0]' <<<"$lock_before")" '{lockBefore:$lock}')"
  set_grant "$kind" '{"createdByThisStack":true,"createAttempted":true}'
  grant_result="$(az role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role "$ROLE_ID" --scope "$scope" --subscription "$SUB" -o json)" || die "Cannot create exact $kind snapshot access grant."
  assignment_id="$(jq -er '.id' <<<"$grant_result")" || die "$kind role creation returned no assignment ID."
  set_grant "$kind" "$(jq -cn --arg aid "$assignment_id" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{assignmentId:$aid,createdByThisStack:true,createdAtUtc:$at}')"
  after="$(azj role assignment list --scope "$scope")" || die "Cannot verify $kind snapshot access grant."
  jq -e --arg aid "${assignment_id,,}" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${scope,,}" '[.[]|select(.principalId==$p and (.id|ascii_downcase)==$aid and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)]|length==1' <<<"$after" >/dev/null || die "$kind snapshot permission is not unique or differs from the exact grant."
  lock_after="$(azj lock list --resource "$scope")" || die "Cannot verify restored $kind retention lock."
  jq -e --arg id "${scope,,}" --arg name "preserve-all-snapshots" 'length==1 and .[0].name==$name and .[0].level=="CanNotDelete" and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$lock_after" >/dev/null || die "The $kind snapshot retention lock changed during role grant creation."
  set_grant "$kind" "$(jq -cn --argjson after "$(jq -c '.[0]' <<<"$lock_after")" '{lockAfter:$after}')"
done
jq '.status="active"|.activatedAtUtc=(now|todateiso8601)' "$GRANTS" >"$GRANTS.tmp"; mv -f "$GRANTS.tmp" "$GRANTS"; chmod 600 "$GRANTS"

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
  source: {snapshotHandle: "$DATA_ID"}
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
  source: {snapshotHandle: "$WAL_ID"}
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
k apply -f "$TMP/source-pair.yaml" >/dev/null || die 'Cannot create owned static DATA/WAL VolumeSnapshot resources.'
end=$((SECONDS+3600))
while ((SECONDS<end)); do
  d="$(k get volumesnapshot "$VSD" -n "$NS" -o json)" || die 'Cannot inspect static DATA VolumeSnapshot.'
  w="$(k get volumesnapshot "$VSW" -n "$NS" -o json)" || die 'Cannot inspect static WAL VolumeSnapshot.'
  if jq -e '.status.readyToUse==true' <<<"$d" >/dev/null && jq -e '.status.readyToUse==true' <<<"$w" >/dev/null; then break; fi
  sleep 10
done
((SECONDS<end)) || die 'Static VolumeSnapshots did not become ready.'
for entry in "$VSD:$VCD:$DATA_ID" "$VSW:$VCW:$WAL_ID"; do
  IFS=: read -r vss vsc sid <<<"$entry"
  v="$(k get volumesnapshot "$vss" -n "$NS" -o json)"; c="$(k get volumesnapshotcontent "$vsc" -o json)"
  jq -e --arg c "$vsc" --arg t "$TOKEN" '.status.readyToUse==true and .metadata.labels["repro.stack.token"]==$t and .spec.source.volumeSnapshotContentName==$c' <<<"$v" >/dev/null || die "$vss binding/owner check failed."
  jq -e --arg id "${sid,,}" --arg n "$vss" --arg ns "$NS" --arg t "$TOKEN" '(.metadata.labels["repro.stack.token"]==$t) and .spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$id and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and (.status.snapshotHandle|ascii_downcase)==$id' <<<"$c" >/dev/null || die "$vsc source ID/deletion policy check failed."
done

cat >"$TMP/cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $C
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
  annotations:
    vacuum-only-repro/source-data-snapshot-id: "$DATA_ID"
    vacuum-only-repro/source-wal-snapshot-id: "$WAL_ID"
    vacuum-only-repro/pinned-source-data-snapshot-id: "$SOURCE_DATA"
    vacuum-only-repro/pinned-source-wal-snapshot-id: "$SOURCE_WAL"
spec:
  instances: 1
  imageName: $IMAGE
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
      shared_buffers: 256MB
      track_io_timing: "on"
      track_wal_io_timing: "on"
      wal_sender_timeout: 5s
      wal_receiver_timeout: 5s
EOF
cp "$TMP/cluster.yaml" "$SD/source-pair-cluster-manifest.yaml"
chmod 600 "$SD/source-pair-cluster-manifest.yaml"
sha256sum "$SD/source-pair-cluster-manifest.yaml" >"$SD/source-pair-cluster-manifest.sha256"
k apply -f "$TMP/cluster.yaml" >/dev/null || die 'Cannot submit CNPG paired-snapshot primary recovery.'

end=$((SECONDS+3600)); primary=''; last_recovery_check=$SECONDS
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)" || die 'Cannot inspect CNPG recovery state.'
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 1 ]]; then
    primary="$(jq -r '.status.currentPrimary // empty' <<<"$cluster")"
    [[ "$primary" == "$C-1" ]] && break
  fi
  if ((SECONDS-last_recovery_check>=60)); then
    last_recovery_check=$SECONDS
    k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$TMP/recovery-pods.json"
    while IFS= read -r recovery_pod; do
      k logs "$recovery_pod" -n "$NS" --all-containers --timestamps --tail=100 >"$TMP/$recovery_pod-recovery.log" 2>&1 || true
    done < <(jq -r '.items[].metadata.name' "$TMP/recovery-pods.json")
    if rg -iq 'WAL ends before end of online backup|recovery ended before consistent state|could not find a valid checkpoint record|requested WAL segment .* has already been removed|could not open file .*pg_wal.*No such file' "$TMP"/*-recovery.log; then
      die 'Snapshot recovery reported a missing WAL/checkpoint error; diagnostic logs are preserved.'
    fi
  fi
  sleep 10
done
[[ "$primary" == "$C-1" ]] || die 'Primary did not finish snapshot recovery before timeout.'
pod="$(k get pod "$primary" -n "$NS" -o json)"
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True")' <<<"$pod" >/dev/null || die 'Recovered primary pod is not ready/owned.'
pvc_json="$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)"
jq -e --arg d "$C-1" --arg w "$C-1-wal" --arg t "$TOKEN" '[.items[]|select((.metadata.name==$d or .metadata.name==$w) and .metadata.labels["repro.stack.token"]==$t and .status.phase=="Bound" and .spec.storageClassName=="azure-standard-hdd" and .spec.resources.requests.storage=="512Gi")]|length==2' <<<"$pvc_json" >/dev/null || die 'Primary DATA/WAL PVCs are not two separate 512Gi Standard HDD claims.'
data_pv="$(jq -er --arg n "$C-1" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
wal_pv="$(jq -er --arg n "$C-1-wal" '.items[]|select(.metadata.name==$n)|.spec.volumeName' <<<"$pvc_json")"
[[ "$data_pv" != "$wal_pv" ]] || die 'Primary DATA and WAL resolved to the same PV.'
# Source access is no longer needed once the two primary managed disks are provisioned from the snapshots.
remove_grants || die 'Could not prove temporary source snapshot grants were removed.'
jq -s -e --arg d "${DATA_ID,,}" --arg w "${WAL_ID,,}" 'reduce .[] as $event ({}; .[($event.snapshotId|ascii_downcase)]=$event) | (keys|length)==2 and has($d) and has($w) and all(.[];.lockRestoreVerified==true and .roleOperation=="delete" and .roleOperationExitCode==0 and .snapshotDeletionAttempted==false)' "$LOCK_CYCLES" >/dev/null || die 'Source snapshot role grant removal lacks successful lock-restoration receipts for the exact pair.'
fi

# The exact paired DATA/WAL snapshot is self-contained for this proven restore path. No archive
# source or external cluster is configured; switch only from the system image to measurement image.
recovery_source_removed=true
k patch cluster "$C" -n "$NS" --type=merge -p "{\"spec\":{\"imageName\":\"$MEASUREMENT_IMAGE\",\"primaryUpdateMethod\":\"restart\"}}" >/dev/null || die 'Cannot switch the recovered primary to the original measurement image.'
end=$((SECONDS+7200)); primary=''
while ((SECONDS<end)); do
  cluster="$(k get cluster "$C" -n "$NS" -o json)"
  if [[ "$(jq -r '.status.readyInstances // 0' <<<"$cluster")" == 1 && "$(jq -r '.status.currentPrimary // empty' <<<"$cluster")" == "$C-1" && "$(jq -r '.status.image // empty' <<<"$cluster")" == "$MEASUREMENT_IMAGE" ]]; then
    pod="$(k get pod "$C-1" -n "$NS" -o json)"
    if jq -e --arg t "$TOKEN" --arg image "$MEASUREMENT_IMAGE" '.metadata.labels["repro.stack.token"]==$t and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True") and any(.spec.containers[];.name=="postgres" and .image==$image)' <<<"$pod" >/dev/null; then primary="$C-1"; break; fi
  fi
  sleep 10
done
[[ "$primary" == "$C-1" ]] || die 'Recovered primary did not restart ready on the original measurement image.'
cluster="$(k get cluster "$C" -n "$NS" -o json)"
jq -e '.spec.backup==null' <<<"$cluster" >/dev/null || die 'A backup archiver is configured on the measurement cluster.'
jq -e '.spec.externalClusters==null and .spec.bootstrap.recovery.source==null' <<<"$cluster" >/dev/null || die 'An archive recovery source is configured on the measurement cluster.'

SQL="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'liveTuplesEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'deadTuplesEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id=4),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=5),'indexName',(SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='public.vacuum_target'::regclass AND indexrelid='public.vacuum_target_id_idx'::regclass),'tableAutovacuumDisabled',EXISTS(SELECT 1 FROM pg_class WHERE oid='public.vacuum_target'::regclass AND 'autovacuum_enabled=false'=ANY(reloptions)),'settings',(SELECT json_object_agg(name,current_setting(name)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency','shared_buffers','max_wal_size','checkpoint_timeout','track_io_timing','track_wal_io_timing','autovacuum','wal_sender_timeout','wal_receiver_timeout','synchronous_commit','synchronous_standby_names'])))::text;"
primary_meta="$(sql "$primary" "$SQL")"
jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$(jq -er '.fixture.tableBytes' "$META")" \
  '.systemIdentifier==$sys and .inRecovery==false and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.wal_level=="logical" and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0" and .settings.maintenance_io_concurrency=="10" and .settings.shared_buffers=="256MB" and .settings.max_wal_size=="1GB" and .settings.checkpoint_timeout=="5min" and .settings.track_io_timing=="on" and .settings.track_wal_io_timing=="on" and .settings.autovacuum=="on"' <<<"$primary_meta" >/dev/null || die 'Recovered primary fixture, source identity, or default PostgreSQL settings differ from the pinned pre-VACUUM fixture.'

# CNPG seeds the replica from the restored primary by its native join/basebackup path.
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null || die 'Cannot request the original synchronous replica configuration and streaming replica seed.'
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
[[ -n "$replica" ]] || die 'CNPG streaming replica did not become ready before timeout.'
pods="$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)"
jq -e --arg t "$TOKEN" --arg p "$primary" --arg r "$replica" '[.items[]|select(.metadata.labels["repro.stack.token"]==$t and .status.phase=="Running" and any(.status.conditions[]?;.type=="Ready" and .status=="True"))] as $all | ($all|length)==2 and any($all[];.metadata.name==$p and .metadata.labels["cnpg.io/instanceRole"]=="primary") and any($all[];.metadata.name==$r and .metadata.labels["cnpg.io/instanceRole"]=="replica") and ($all[0].spec.nodeName!=$all[1].spec.nodeName)' <<<"$pods" >/dev/null || die 'Expected exactly two owned ready instances on separate nodes.'

primary_meta="$(sql "$primary" "$SQL")"
jq -e --arg sync "ANY 1 (\"$C-2\")" '.settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s" and .settings.synchronous_commit=="on" and .settings.synchronous_standby_names==$sync' <<<"$primary_meta" >/dev/null || die 'Primary sender/receiver timeout or ANY 1 synchronous replication settings differ from the original test.'
replica_meta="$(sql "$replica" "$SQL")"
jq -e --arg sys "$SYSTEM_ID" --argjson heap "$TARGET_HEAP" --argjson total "$(jq -er '.fixture.tableBytes' "$META")" \
  '.systemIdentifier==$sys and .inRecovery==true and .heapBytes==$heap and .tableBytes==$total and .payloadBytes==1536 and .firstLiveId==4 and .adjacentOddIdAbsent==true and .indexName=="vacuum_target_id_idx" and .tableAutovacuumDisabled==true and .settings.full_page_writes=="on" and .settings.recovery_prefetch=="try" and .settings.vacuum_cost_delay=="0" and .settings.wal_sender_timeout=="5s" and .settings.wal_receiver_timeout=="5s" and .settings.autovacuum=="on"' <<<"$replica_meta" >/dev/null || die 'Replica data/default PostgreSQL settings do not match the recovered primary fixture.'

end=$((SECONDS+7200)); caught=false; target_lsn=''; replay_lsn=''
while ((SECONDS<end)); do
  target_lsn="$(sql "$primary" 'SELECT pg_current_wal_flush_lsn();' | tr -d '\r\n')"
  replay_lsn="$(sql "$replica" 'SELECT pg_last_wal_replay_lsn();' | tr -d '\r\n')"
  caught="$(python3 - "$target_lsn" "$replay_lsn" <<'PY'
import sys
def n(x):
 a,b=x.split('/'); return (int(a,16)<<32)+int(b,16)
try: print('true' if n(sys.argv[2])>=n(sys.argv[1]) else 'false')
except Exception: print('false')
PY
)"
  if [[ "$caught" == true ]]; then break; fi
  sleep 5
done
[[ "$caught" == true ]] || die 'Replica did not replay through the observed primary flush LSN.'
app_count="$(sql "$primary" "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';" | tr -d '\r\n')"
replica_app_count="$(sql "$replica" "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';" | tr -d '\r\n')"
[[ "$app_count" == 0 && "$replica_app_count" == 0 ]] || die 'Application workload exists on a PostgreSQL instance.'
receiver="$(sql "$replica" "SELECT coalesce((SELECT status FROM pg_stat_wal_receiver LIMIT 1),'');" | tr -d '\r\n')"
[[ "$receiver" == streaming ]] || die 'Replica WAL receiver is not streaming.'

# This verifier checks the live selected v6 VMs and actual Azure attachments, including ReadOnly cache mode.
python3 "$ROOT/verify-attachments.py" >"$SD/attachment-verification.stdout.json" || die 'Live VM/disk/caching verification failed.'

for kind in data wal; do
  if [[ "$kind" == data ]]; then vss="$VSD"; vsc="$VCD"; sid="$DATA_ID"; claim="$C-1"; else vss="$VSW"; vsc="$VCW"; sid="$WAL_ID"; claim="$C-1-wal"; fi
  snapshot_obj="$(k get volumesnapshot "$vss" -n "$NS" -o json)"; content_obj="$(k get volumesnapshotcontent "$vsc" -o json)"
  pvc="$(k get pvc "$claim" -n "$NS" -o json)"
  jq -e --arg t "$TOKEN" --arg vsc "$vsc" '.metadata.labels["repro.stack.token"]==$t and .status.readyToUse and .spec.source.volumeSnapshotContentName==$vsc' <<<"$snapshot_obj" >/dev/null || die "Final $kind VolumeSnapshot ownership/binding check failed."
  jq -e --arg sid "${sid,,}" '.spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$sid and (.status.snapshotHandle|ascii_downcase)==$sid' <<<"$content_obj" >/dev/null || die "Final $kind VolumeSnapshotContent handle changed."
  jq -e --arg t "$TOKEN" --arg vss "$vss" '.metadata.labels["repro.stack.token"]==$t and .status.phase=="Bound" and .spec.storageClassName=="azure-standard-hdd" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$vss' <<<"$pvc" >/dev/null || die "Primary $kind PVC does not point to the exact retained source snapshot."
done

snapshot_verification="$(jq -s '.' "$TMP/snapshot-verification.jsonl")"
k get cluster "$C" -n "$NS" -o json >"$SD/measurement-cluster.json"
chmod 600 "$SD/measurement-cluster.json"
sha256sum "$SD/measurement-cluster.json" >"$SD/measurement-cluster.sha256"
primary_lsn="$target_lsn"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg token "$TOKEN" --arg loc "$LOC" --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg cluster "$NS/$C" --arg image "$MEASUREMENT_IMAGE" --argjson sourceRemoved "$recovery_source_removed" \
  --arg sourceData "$SOURCE_DATA" --arg sourceWal "$SOURCE_WAL" --arg originalSourceRg "$ORIGINAL_SOURCE_RG" --arg data "$DATA_ID" --arg wal "$WAL_ID" --arg sourceRg "$SOURCE_RG" \
  --arg backup "$BACKUP_NAME" --arg sys "$SYSTEM_ID" \
  --arg dataUid "$SOURCE_DATA_UID" --arg walUid "$SOURCE_WAL_UID" --arg primary "$primary" --arg replica "$replica" \
  --argjson heap "$TARGET_HEAP" --argjson total "$(jq -er '.fixture.tableBytes' "$META")" --argjson live "$(jq -er '.fixture.preparedLiveTuplesEstimate' "$META")" --argjson dead "$(jq -er '.fixture.preparedDeadTuplesEstimate' "$META")" \
  --argjson snapshots "$snapshot_verification" --argjson primary "$primary_meta" --argjson replica "$replica_meta" --argjson jobs "$observed_jobs" --arg target "$primary_lsn" --arg replay "$replay_lsn" --arg receiver "$receiver" --arg manifest "$META" \
  '{version:1,status:"complete",verifiedAtUtc:$at,ownerToken:$token,location:$loc,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,cluster:$cluster,instances:2,measurementImage:$image,sourceKind:"reused-prepared-paired-snapshot",fixtureTargetHeapGiB:23,fixtureTargetHeapBytes:$heap,systemIdentifier:$sys,heapBytes:$heap,tableBytes:$total,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$dead,fixture:{sourceKind:"reused-prepared-paired-snapshot",heapBytes:$heap,tableBytes:$total,liveTuplesEstimate:$live,deadTuplesEstimate:$dead,payloadBytes:1536,indexName:"vacuum_target_id_idx",tableAutovacuumDisabled:true,snapshotBeforeVacuum:true},sourcePair:{manifest:$manifest,backupName:$backup,originalEastUs:null,originalWestUs3:{resourceGroup:$originalSourceRg,dataSnapshotId:$sourceData,walSnapshotId:$sourceWal,dataUniqueId:$dataUid,walUniqueId:$walUid},regionalRetained:{resourceGroup:$sourceRg,dataSnapshotId:$data,walSnapshotId:$wal,liveAzureVerification:$snapshots},reuseOriginal:true,bootstrapMode:"CNPG recovery from both static DATA and WAL VolumeSnapshots",recoveryArchiveSourceRemoved:$sourceRemoved,regionalSnapshotsCreatedByTestStack:false},walArchive:{importedAndDownloadedHashVerified:false,objects:[],receiptSha256:null,serverName:null,notRequiredForProvenPairedSnapshotRestore:true},sourceSnapshotRoleAssignments:{status:"removed",evidence:".state/source-snapshot-role-assignments.json"},replicaSeed:{method:"CNPG streaming join/basebackup from the restored primary",completedJobs:$jobs,receiverStatus:$receiver,primaryFlushLsn:$target,replicaReplayLsn:$replay,caughtUp:true},finalRoles:{primary:$primary,replica:$replica},clientCreated:false,snapshotDeletionPolicy:"Retain",noBackupCRCreated:true}' >"$TMP/prepare.json"
chmod 600 "$TMP/prepare.json"
mv "$TMP/prepare.json" "$FIXTURE"
printf 'Prepared the 23-GiB fixture from the verified retained pair; source snapshots were not modified.\n'
