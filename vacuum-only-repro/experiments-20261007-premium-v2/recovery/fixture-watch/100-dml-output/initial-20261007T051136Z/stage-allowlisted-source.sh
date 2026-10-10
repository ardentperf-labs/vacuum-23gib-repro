#!/usr/bin/env bash
# Convert an allowlisted incremental snapshot into a stack-owned snapshot family.
# This preserves the normal VolumeSnapshot/CSI/CNPG restore path while avoiding
# another CopyStart from a source snapshot that already has a newer target copy.
set -Eeuo pipefail
umask 077

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'EOF'
Usage: stage-allowlisted-source.sh STACK_STATE SOURCE_MANIFEST [--plan-only|--apply]

The default is --plan-only. It validates stack and source ownership, source
readiness, and retained-snapshot group protection, then prints the exact
resource IDs it would create. --apply creates an unattached Standard_LRS
staging disk in the owned AKS node resource group, takes a new incremental
Copy snapshot of that disk into the stack-owned retained-snapshot resource
group, and verifies a direct CanNotDelete lock on the new snapshot.

Set RESOURCE_SUFFIX to eight lowercase hex digits to make a plan reproducible
or resume it. Before any create request, --apply saves the full source metadata
and planned IDs below STACK_STATE's .state/source-stage directory. A retry with
the same suffix validates and reuses only resources whose owner tags, IDs,
source lineage, unique IDs, size, SKU, sector evidence, and state match. This
helper never changes or deletes an existing resource or the source snapshot.
EOF
}

[[ $# -ge 2 && $# -le 3 ]] || { usage >&2; exit 2; }
STATE_FILE=$1
SOURCE_MANIFEST=$2
MODE=${3:---plan-only}
case "$MODE" in --plan-only|--apply) ;; *) usage >&2; exit 2 ;; esac
[[ -r "$STATE_FILE" && -r "$SOURCE_MANIFEST" ]] || die 'State and source manifest must be readable files.'
for tool in az jq timeout od tr date sleep mktemp mkdir chmod mv grep; do
  command -v "$tool" >/dev/null || die "Missing $tool."
done

jq -e '.version == 1 and .status == "ready"' "$STATE_FILE" >/dev/null || die 'Stack state is not ready.'
jq -e '.version == 1 and .externalResource == true and (.sourceSnapshotId|type == "string" and startswith("/subscriptions/")) and (.ownerToken|type == "string" and test("^[a-f0-9]{32}$")) and (.resourceGroup|type == "string" and length > 0) and (.location|type == "string" and length > 0) and (.uniqueId|type == "string" and length > 0) and .incremental == true and .diskSizeGiB == 512 and .logicalSectorSize == 512 and .sku.name == "Standard_LRS"' "$SOURCE_MANIFEST" >/dev/null || die 'Source manifest is not an allowlisted 512-GiB incremental Standard_LRS snapshot with 512-byte sectors.'

SUB=$(jq -er .subscriptionId "$STATE_FILE")
TEST_RG=$(jq -er .resourceGroup "$STATE_FILE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE_FILE")
SNAP_RG=$(jq -er .snapshotResourceGroup "$STATE_FILE")
SNAP_RG_ID=$(jq -er .snapshotResourceGroupId "$STATE_FILE")
LOC=$(jq -er .location "$STATE_FILE")
ZONE=$(jq -er .availabilityZone "$STATE_FILE")
TOKEN=$(jq -er .ownerToken "$STATE_FILE")
SOURCE_ID=$(jq -er .sourceSnapshotId "$SOURCE_MANIFEST")
SOURCE_RG=$(jq -er .resourceGroup "$SOURCE_MANIFEST")
SOURCE_LOC=$(jq -er .location "$SOURCE_MANIFEST")
SOURCE_NAME=$(jq -er .name "$SOURCE_MANIFEST")
SOURCE_OWNER=$(jq -er .ownerToken "$SOURCE_MANIFEST")
SOURCE_PURPOSE=$(jq -er .purpose "$SOURCE_MANIFEST")
SOURCE_UID=$(jq -er .uniqueId "$SOURCE_MANIFEST")
SOURCE_FAMILY=$(jq -er .incrementalSnapshotFamilyId "$SOURCE_MANIFEST")
SHORT=${TOKEN:0:8}

[[ "$SUB" =~ ^[0-9a-fA-F-]{36}$ && "$TOKEN" =~ ^[a-f0-9]{32}$ && "$ZONE" == 1 ]] || die 'Invalid stack subscription, owner token, or availability zone.'
[[ "${TEST_RG,,}" == "rg-vacuum-only-$TOKEN" ]] || die 'Test resource group does not match stack ownership.'
[[ "${NODE_RG,,}" == "rg-vacuum-nodes-$TOKEN" ]] || die 'Node resource group does not match stack ownership.'
[[ "${LOC,,}" == "${SOURCE_LOC,,}" ]] || die 'Source snapshot is not in the stack region.'
[[ "${SNAP_RG,,}" == "rg-vacuum-pv2-snap-$TOKEN" ]] || die 'Snapshot resource group does not match stack ownership.'
EXPECTED_SNAP_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAP_RG"
[[ "${SNAP_RG_ID,,}" == "${EXPECTED_SNAP_RG_ID,,}" ]] || die 'Snapshot resource group ID does not match stack ownership.'
EXPECTED_SOURCE_ID="/subscriptions/$(jq -er .subscriptionId "$SOURCE_MANIFEST")/resourceGroups/$SOURCE_RG/providers/Microsoft.Compute/snapshots/$SOURCE_NAME"
[[ "${SOURCE_ID,,}" == "${EXPECTED_SOURCE_ID,,}" ]] || die 'Source snapshot ID does not match its manifest identity.'
[[ "$(jq -er .subscriptionId "$SOURCE_MANIFEST")" == "$SUB" ]] || die 'Source and stack subscriptions differ.'

azc() { timeout --foreground --kill-after=10s 900s az "$@" --subscription "$SUB"; }

TMP=$(mktemp -d)
cleanup_tmp() {
  local status=$?
  trap - EXIT
  rm -rf -- "$TMP"
  exit "$status"
}
trap cleanup_tmp EXIT

active_sub=$(azc account show --query id -o tsv) || die 'Cannot verify the active Azure subscription.'
[[ "${active_sub,,}" == "${SUB,,}" ]] || die 'Active Azure subscription differs from the stack state.'

test_group=$(azc group show --name "$TEST_RG" -o json) || die 'Cannot inspect the owned test resource group.'
jq -e --arg n "${TEST_RG,,}" --arg l "${LOC,,}" --arg t "$TOKEN" '(.name|ascii_downcase)==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t' <<<"$test_group" >/dev/null || die 'Test resource group ownership or location did not verify.'

node_group=$(azc group show --name "$NODE_RG" -o json) || die 'Cannot inspect the AKS node resource group.'
jq -e --arg n "${NODE_RG,,}" --arg l "${LOC,,}" --arg t "$TOKEN" '(.name|ascii_downcase)==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t' <<<"$node_group" >/dev/null || die 'Node resource group ownership or location did not verify.'

snapshot_group=$(azc group show --name "$SNAP_RG" -o json) || die 'Cannot inspect the retained-snapshot resource group.'
jq -e --arg id "${SNAP_RG_ID,,}" --arg n "${SNAP_RG,,}" --arg l "${LOC,,}" --arg t "$TOKEN" '((.id|ascii_downcase)==$id) and ((.name|ascii_downcase)==$n) and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-retained-snapshots"' <<<"$snapshot_group" >/dev/null || die 'Retained-snapshot resource group ownership, purpose, or location did not verify.'

group_locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die 'Cannot inspect retained-snapshot group protection.'
jq -e --arg id "${SNAP_RG_ID,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$group_locks" >/dev/null || die 'Retained-snapshot resource group lacks its CanNotDelete lock.'

source=$(azc snapshot show --ids "$SOURCE_ID" -o json) || die 'Cannot inspect the allowlisted source snapshot.'
jq -e --arg id "${SOURCE_ID,,}" --arg uid "$SOURCE_UID" --arg family "$SOURCE_FAMILY" --arg t "$SOURCE_OWNER" --arg purpose "$SOURCE_PURPOSE" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and .uniqueId==$uid and .incrementalSnapshotFamilyId==$family and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (((.completionPercent|tonumber?) // 100)==100) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose==$purpose' <<<"$source" >/dev/null || die 'Live source snapshot identity, ownership, readiness, size, or incremental state differs from its manifest.'

SUFFIX=${RESOURCE_SUFFIX:-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')}
[[ "$SUFFIX" =~ ^[a-f0-9]{8}$ ]] || die 'RESOURCE_SUFFIX must be eight lowercase hex digits.'
STAGE_DISK_NAME="restore-stage-$SHORT-$SUFFIX"
STAGE_SNAPSHOT_NAME="keeper-pv2-$SHORT-$SUFFIX"
STAGE_DISK_ID="/subscriptions/$SUB/resourceGroups/$NODE_RG/providers/Microsoft.Compute/disks/$STAGE_DISK_NAME"
STAGE_SNAPSHOT_ID="$SNAP_RG_ID/providers/Microsoft.Compute/snapshots/$STAGE_SNAPSHOT_NAME"
LOCK_NAME="preserve-$STAGE_SNAPSHOT_NAME"
STATE_DIR=$(cd -- "$(dirname -- "$STATE_FILE")" && pwd)
STAGE_DIR="$STATE_DIR/source-stage"
PLAN_FILE="$STAGE_DIR/plan-$SUFFIX.json"
RECEIPT_FILE="$STAGE_DIR/receipt-$SUFFIX.json"

read_existing() {
  local type=$1 id=$2 output_name=$3
  local err="$TMP/show-$type.err" output
  if output=$(azc "$type" show --ids "$id" -o json 2>"$err"); then
    printf -v "$output_name" '%s' "$output"
    return 0
  fi
  grep -Eiq 'ResourceNotFound|DiskNotFound|SnapshotNotFound|NotFound|not found|could not be found' "$err" || {
    cat "$err" >&2
    die "Cannot prove $type resource state for $id."
  }
  printf -v "$output_name" '%s' ''
  return 1
}

disk=''
snapshot=''
disk_present=false
snapshot_present=false
if read_existing disk "$STAGE_DISK_ID" disk; then disk_present=true; fi
if read_existing snapshot "$STAGE_SNAPSHOT_ID" snapshot; then snapshot_present=true; fi
[[ "$snapshot_present" == false || "$disk_present" == true ]] || die 'A matching-suffix snapshot exists without its staging disk; preserve it and use a newly reviewed suffix.'

plan=$(jq -n --arg mode "$MODE" --arg sub "$SUB" --arg source "$SOURCE_ID" --arg sourceUid "$SOURCE_UID" --arg disk "$STAGE_DISK_ID" --arg snapshot "$STAGE_SNAPSHOT_ID" --arg loc "$LOC" --arg zone "$ZONE" --arg token "$TOKEN" --arg suffix "$SUFFIX" --argjson sourceMetadata "$source" '{mode:$mode,subscriptionId:$sub,sourceSnapshotId:$source,sourceSnapshotUniqueId:$sourceUid,stagingDiskId:$disk,ownedIncrementalSnapshotId:$snapshot,location:$loc,availabilityZone:$zone,ownerToken:$token,resourceSuffix:$suffix,sourceSnapshotMetadata:$sourceMetadata,mutationPlan:["Create a Standard_LRS 512-GiB disk from the allowlisted incremental snapshot in the owned node resource group","Create an incremental Copy snapshot of that disk in the owned retained-snapshot resource group","Apply and verify a CanNotDelete lock directly on the new snapshot"],existingResourcesChanged:[] }')
if [[ "$MODE" == --plan-only ]]; then printf '%s\n' "$plan"; exit 0; fi

mkdir -p "$STAGE_DIR"
chmod 700 "$STAGE_DIR"
if [[ -e "$PLAN_FILE" ]]; then
  jq -e --arg source "${SOURCE_ID,,}" --arg disk "${STAGE_DISK_ID,,}" --arg snapshot "${STAGE_SNAPSHOT_ID,,}" --arg uid "$SOURCE_UID" '((.sourceSnapshotId|ascii_downcase)==$source) and ((.stagingDiskId|ascii_downcase)==$disk) and ((.ownedIncrementalSnapshotId|ascii_downcase)==$snapshot) and .sourceSnapshotUniqueId==$uid' "$PLAN_FILE" >/dev/null || die 'Existing source-stage plan does not match this source and resource suffix.'
else
  plan_tmp=$(mktemp "$STAGE_DIR/.plan-$SUFFIX.XXXXXX")
  printf '%s\n' "$plan" >"$plan_tmp"
  chmod 600 "$plan_tmp"
  mv -- "$plan_tmp" "$PLAN_FILE"
fi

verify_disk() {
  local d=$1 state sector
  state=$(jq -r '.provisioningState // ""' <<<"$d")
  [[ "$state" == Succeeded ]] || die "Staging disk is not Succeeded (state=$state)."
  jq -e --arg id "${STAGE_DISK_ID,,}" --arg src "${SOURCE_ID,,}" --arg srcUid "$SOURCE_UID" --arg t "$TOKEN" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .sku.name=="Standard_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .provisioningState=="Succeeded" and .diskState=="Unattached" and .managedBy==null and ((.creationData.createOption // "")=="Copy") and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .creationData.sourceUniqueId==$srcUid and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-source-stage" and ((.tags.sourceSnapshotId|ascii_downcase)==$src)' <<<"$d" >/dev/null || die 'Staging disk identity, source lineage, ownership, size, SKU, or attachment state failed verification.'
  sector=$(jq -r '.logicalSectorSize // .creationData.logicalSectorSize // empty | tostring' <<<"$d")
  if [[ -n "$sector" && "$sector" != 512 ]]; then die "Staging disk reports unsupported logical sector size $sector."; fi
  SECTOR_EVIDENCE='explicitly reported by Azure disk metadata'
  [[ -n "$sector" ]] || SECTOR_EVIDENCE='inferred from the allowlisted 512-byte-sector source snapshot and verified Copy lineage; Azure omitted the live disk sector field'
  completion=$(jq -r '((.completionPercent|tonumber?) // 0)' <<<"$d")
}

verify_snapshot_identity() {
  local s=$1 state
  state=$(jq -r '.provisioningState // ""' <<<"$s")
  [[ "$state" == Succeeded ]] || die "Owned snapshot is not Succeeded (state=$state)."
  jq -e --arg id "${STAGE_SNAPSHOT_ID,,}" --arg src "${STAGE_DISK_ID,,}" --arg srcUid "$(jq -r .uniqueId <<<"$disk")" --arg original "${SOURCE_ID,,}" --arg originalFamily "$SOURCE_FAMILY" --arg t "$TOKEN" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (((.completionPercent|tonumber?) // 100)==100) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and ((.creationData.createOption // "")=="Copy") and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .creationData.sourceUniqueId==$srcUid and (.uniqueId|type=="string" and length>0) and (.incrementalSnapshotFamilyId|type=="string" and length>0) and .incrementalSnapshotFamilyId!=$originalFamily and ((.logicalSectorSize // .creationData.logicalSectorSize // 512)|tostring)=="512" and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-keeper-copy" and ((.tags.sourceSnapshotId|ascii_downcase)==$original) and ((.tags.sourceDiskId|ascii_downcase)==$src)' <<<"$s" >/dev/null || die 'Owned snapshot identity, source lineage, ownership, sector evidence, or readiness check failed.'
}

if [[ "$disk_present" == true ]]; then
  printf 'Validating existing owned staging disk %s for safe resume.\n' "$STAGE_DISK_NAME" >&2
  jq -e --arg id "${STAGE_DISK_ID,,}" --arg src "${SOURCE_ID,,}" --arg srcUid "$SOURCE_UID" --arg t "$TOKEN" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .creationData.sourceUniqueId==$srcUid and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-source-stage" and ((.tags.sourceSnapshotId|ascii_downcase)==$src)' <<<"$disk" >/dev/null || die 'Existing staging disk is not an exact owned match; it will be preserved.'
else
  printf 'Creating owned Standard_LRS staging disk %s from the allowlisted source.\n' "$STAGE_DISK_NAME" >&2
  azc disk create --resource-group "$NODE_RG" --name "$STAGE_DISK_NAME" --location "$LOC" --sku Standard_LRS --size-gb 512 --source "$SOURCE_ID" --tags "reprotoken=$TOKEN" reproPurpose=premiumv2-source-stage "sourceSnapshotId=$SOURCE_ID" --no-wait -o none || die 'Staging-disk creation submission failed; preserve resources and inspect Azure state.'
fi

deadline=$((SECONDS+7200))
while ((SECONDS < deadline)); do
  if disk=$(azc disk show --ids "$STAGE_DISK_ID" -o json 2>/dev/null); then
    disk_state=$(jq -r '.provisioningState // ""' <<<"$disk")
    case "$disk_state" in
      Failed) die 'Staging disk entered Failed state; preserve it and inspect Azure Activity Log.' ;;
      Succeeded)
        verify_disk "$disk"
        completion=$(jq -r '((.completionPercent|tonumber?) // 100)' <<<"$disk")
        [[ "$completion" == 100 ]] && break
        ;;
      Updating|Creating|Accepted|Provisioning) ;;
      *) die "Staging disk has unexpected provisioning state: $disk_state" ;;
    esac
  fi
  sleep 20
done
((SECONDS < deadline)) || die 'Staging disk did not finish provisioning and hydration within two hours; preserve all resources.'

if [[ "$snapshot_present" == true ]]; then
  printf 'Validating existing owned snapshot %s for safe resume.\n' "$STAGE_SNAPSHOT_NAME" >&2
  jq -e --arg id "${STAGE_SNAPSHOT_ID,,}" --arg src "${STAGE_DISK_ID,,}" --arg t "$TOKEN" --arg original "${SOURCE_ID,,}" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-keeper-copy" and ((.tags.sourceSnapshotId|ascii_downcase)==$original)' <<<"$snapshot" >/dev/null || die 'Existing snapshot is not an exact owned match; it will be preserved.'
else
  printf 'Creating stack-owned incremental Copy snapshot %s.\n' "$STAGE_SNAPSHOT_NAME" >&2
  azc snapshot create --resource-group "$SNAP_RG" --name "$STAGE_SNAPSHOT_NAME" --location "$LOC" --source "$STAGE_DISK_ID" --sku Standard_LRS --incremental true --tags "reprotoken=$TOKEN" reproPurpose=premiumv2-keeper-copy "sourceSnapshotId=$SOURCE_ID" "sourceDiskId=$STAGE_DISK_ID" --no-wait -o none || die 'Owned incremental snapshot creation submission failed; preserve resources and inspect Azure state.'
fi

deadline=$((SECONDS+7200))
while ((SECONDS < deadline)); do
  if snapshot=$(azc snapshot show --ids "$STAGE_SNAPSHOT_ID" -o json 2>/dev/null); then
    snapshot_state=$(jq -r '.provisioningState // ""' <<<"$snapshot")
    case "$snapshot_state" in
      Failed) die 'Owned snapshot entered Failed state; preserve it and inspect Azure Activity Log.' ;;
      Succeeded) verify_snapshot_identity "$snapshot"; break ;;
      Updating|Creating|Accepted|Provisioning) ;;
      *) die "Owned snapshot has unexpected provisioning state: $snapshot_state" ;;
    esac
  fi
  sleep 20
done
((SECONDS < deadline)) || die 'Owned incremental snapshot did not complete within two hours; preserve all resources.'

snapshot_locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die 'Cannot inspect snapshot locks.'
direct_lock=$(jq -c --arg id "${STAGE_SNAPSHOT_ID,,}" '[.[]|select(.level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))]|first // empty' <<<"$snapshot_locks")
if [[ -z "$direct_lock" ]]; then
  azc lock create --name "$LOCK_NAME" --lock-type CanNotDelete --resource-group "$SNAP_RG" --resource-name "$STAGE_SNAPSHOT_NAME" --resource-type Microsoft.Compute/snapshots --notes 'Preserve every experiment snapshot until explicit user deletion authorization.' -o none || die 'Cannot place a direct CanNotDelete lock on the new snapshot.'
  snapshot_locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die 'Cannot verify the new snapshot protection.'
  direct_lock=$(jq -c --arg id "${STAGE_SNAPSHOT_ID,,}" '[.[]|select(.level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))]|first // empty' <<<"$snapshot_locks")
fi
[[ -n "$direct_lock" ]] || die 'Direct CanNotDelete snapshot lock did not verify.'

finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)
receipt=$(jq -n --arg sub "$SUB" --arg suffix "$SUFFIX" --arg sourceId "$SOURCE_ID" --arg sourceUid "$SOURCE_UID" --arg diskId "$STAGE_DISK_ID" --arg snapshotId "$STAGE_SNAPSHOT_ID" --arg loc "$LOC" --arg zone "$ZONE" --arg token "$TOKEN" --arg created "$finished" --arg evidence "$SECTOR_EVIDENCE" --arg lockName "$LOCK_NAME" --argjson sourceManifest "$(cat "$SOURCE_MANIFEST")" --argjson sourceSnapshot "$source" --argjson stagingDisk "$disk" --argjson restoreSnapshot "$snapshot" --argjson lock "$direct_lock" '{version:1,status:"ready",subscriptionId:$sub,resourceSuffix:$suffix,sourceSnapshotId:$sourceId,sourceSnapshotUniqueId:$sourceUid,sourceManifest:$sourceManifest,sourceSnapshotMetadata:$sourceSnapshot,stagingDiskId:$diskId,stagingDiskUniqueId:$stagingDisk.uniqueId,stagingDiskMetadata:$stagingDisk,restoreSnapshotId:$snapshotId,restoreSnapshotUniqueId:$restoreSnapshot.uniqueId,incrementalSnapshotFamilyId:$restoreSnapshot.incrementalSnapshotFamilyId,restoreSnapshotMetadata:$restoreSnapshot,location:$loc,availabilityZone:$zone,ownerToken:$token,logicalSectorSizeBytes:512,logicalSectorSizeEvidence:$evidence,retention:{locked:true,lockName:$lockName,lockMetadata:$lock},verifiedAtUtc:$created,restorePath:"Owned Standard_LRS staging disk -> owned incremental Copy snapshot -> existing VolumeSnapshot/CSI/CNPG recovery"}')

if [[ -e "$RECEIPT_FILE" ]]; then
  jq -e --arg disk "${STAGE_DISK_ID,,}" --arg snapshot "${STAGE_SNAPSHOT_ID,,}" '((.stagingDiskId|ascii_downcase)==$disk) and ((.restoreSnapshotId|ascii_downcase)==$snapshot)' "$RECEIPT_FILE" >/dev/null || die 'Existing receipt does not match the selected resource suffix.'
fi
receipt_tmp=$(mktemp "$STAGE_DIR/.receipt-$SUFFIX.XXXXXX")
printf '%s\n' "$receipt" >"$receipt_tmp"
chmod 600 "$receipt_tmp"
mv -f -- "$receipt_tmp" "$RECEIPT_FILE"
printf '%s\n' "$receipt"
