#!/usr/bin/env bash
# Restore keeper -> separate data/WAL -> CNPG snapshot-seeded replica.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")" && pwd)"; SD="$ROOT/.state"; STATE="$SD/state.json"
NS=repro; C=hdd-pg; SC=azure-premiumv2; SECRET=azure-barman-credentials
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
TEST_HEAP_GIB=${TEST_HEAP_GIB:-23}
die(){ echo "ERROR: $*" >&2; exit 1; }
if [[ ! "$TEST_HEAP_GIB" =~ ^[1-9][0-9]{0,2}$ ]] || ((TEST_HEAP_GIB>110)); then die "TEST_HEAP_GIB must be 1..110 whole GiB."; fi
TARGET_HEAP_BYTES=$((TEST_HEAP_GIB*1024*1024*1024))
# 23 GiB estimates ~54.5m from 110 GiB / 4h20m35s on Standard_D2s_v4
# (run 20261004T140103Z); prior Standard_D2as_v6 run 20261004T065423Z took 1h54m45s.
VACUUM_ESTIMATE_MIN=$(awk -v g="$TEST_HEAP_GIB" 'BEGIN{printf "%.1f",g*260.583/110}')
while (($#)); do case "$1" in -h|--help) echo "$0"; exit 0;; *) die "Unknown option $1";; esac; done
for x in az kubectl jq timeout flock curl; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die "Run 01-provision.sh first."
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE"); LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SNAP_RG=$(jq -er .snapshotResourceGroup "$STATE"); SNAP_RG_ID=$(jq -er .snapshotResourceGroupId "$STATE"); AZURE_ZONE=$(jq -er .availabilityZone "$STATE")
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die "Bad owner token."
[[ "$SNAP_RG" == "rg-vacuum-pv2-snap-$TOKEN" && "${SNAP_RG_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SNAP_RG,,}" && "$AZURE_ZONE" =~ ^[1-3]$ ]] || die "Snapshot RG or zone metadata does not match stack ownership."
ST="$SD/storage.json"; jq -e --arg t "$TOKEN" --arg s "$SUB" --arg rg "$RG" '.status=="ready" and .ownerToken==$t and .subscriptionId==$s and .resourceGroup==$rg and .publicAccess==null' "$ST" >/dev/null || die "Storage metadata ownership mismatch."
ACCOUNT=$(jq -er .accountName "$ST"); AID=$(jq -er .accountId "$ST"); CONTAINER=$(jq -er .containerName "$ST")
EXPECTED_AID="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$ACCOUNT"
[[ "${AID,,}" == "${EXPECTED_AID,,}" ]] || die "Storage account metadata ID is outside this stack."
SOURCE_MANIFEST="${SOURCE_MANIFEST:-$ROOT/source-manifest.json}"
[[ -f "$SOURCE_MANIFEST" ]] || die "Pinned PremiumV2 source manifest is missing: $SOURCE_MANIFEST"
jq -e '.version==1 and (.sourceSnapshotId|type=="string" and startswith("/subscriptions/")) and (.uniqueId|type=="string" and length>0) and (.ownerToken|type=="string" and test("^[a-f0-9]{32}$")) and (.resourceGroup|type=="string" and length>0) and (.location|type=="string" and length>0) and .incremental==true and .diskSizeGiB==512 and .logicalSectorSize==512 and (.systemIdentifier|type=="string" and length>0)' "$SOURCE_MANIFEST" >/dev/null || die "Invalid or incomplete PremiumV2 source manifest."
SOURCE_SNAPSHOT_ID="${SOURCE_SNAPSHOT_ID:-$(jq -er .sourceSnapshotId "$SOURCE_MANIFEST")}"
jq -e --arg id "${SOURCE_SNAPSHOT_ID,,}" '(.sourceSnapshotId|ascii_downcase)==$id' "$SOURCE_MANIFEST" >/dev/null || die "SOURCE_SNAPSHOT_ID must match the immutable source manifest."
SHORT=$(cut -c1-8 <<<"$TOKEN"); RUN=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/02-prepare.lock"; flock -n 9 || die "Prepare already active."
TMP=$(mktemp -d "$SD/.prepare.XXXXXX"); chmod 700 "$TMP"
trap 'rc=$?; rm -rf -- "$TMP"; ((rc==0)) || echo "Preparation stopped; inspect stack state and retry 04-teardown.sh after review." >&2' EXIT
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" ]] || KCFG="$SD/kubeconfig"
[[ "$KCFG" == /* ]] || KCFG="$ROOT/$KCFG"; [[ -f "$KCFG" ]] || die "Kubeconfig missing."
export KUBECONFIG="$KCFG"
azc(){ timeout 900s az "$@" --subscription "$SUB"; }; azlong(){ timeout 3600s az "$@" --subscription "$SUB"; }
k(){ timeout 120s kubectl "$@"; }; klong(){ timeout 1800s kubectl "$@"; }
psql(){ k exec -n "$NS" -c postgres "$1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
psql_long(){
 local remote rc end
 remote="/controller/tmp/seed-$TOKEN-$RUN-$RANDOM"
 printf '%s\n' "$2" | k exec -i -n "$NS" -c postgres "$1" -- sh -c 'd="$1"; mkdir -m 700 "$d"; cat >"$d/q.sql"; nohup sh -c '\''psql -XAt -v ON_ERROR_STOP=1 -U postgres -d repro -f "$1/q.sql" >"$1/out" 2>&1; echo $? >"$1/status"'\'' worker "$d" </dev/null >/dev/null 2>&1 & echo "$d"' sh "$remote" >"$TMP/seed-path"
 remote=$(cat "$TMP/seed-path"); end=$((SECONDS+36*3600))
 while ((SECONDS<end)); do
  rc=$(k exec -n "$NS" -c postgres "$1" -- sh -c 'cat "$1/status" 2>/dev/null || true' sh "$remote")
  if [[ "$rc" =~ ^[0-9]+$ ]]; then k exec -n "$NS" -c postgres "$1" -- cat "$remote/out"; k exec -n "$NS" -c postgres "$1" -- rm -rf "$remote" >/dev/null; [[ "$rc" == 0 ]] || die "Long SQL failed."; return; fi
  sleep 30
 done
 die "Long SQL timed out."
}
wait_ready(){
 local n="$1" end=$((SECONDS+10800)) c p
 while ((SECONDS<end)); do
  c=$(k get cluster "$C" -n "$NS" -o json 2>/dev/null || true)
  if [[ -n "$c" && $(jq -r '.status.readyInstances // 0' <<<"$c") == "$n" ]]; then
   p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
   jq -e --argjson n "$n" '[.items[]|select(.status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==$n' <<<"$p" >/dev/null && { echo "$c"; return; }
  fi
  sleep 10
 done
 die "Timed out waiting for $n ready instances."
}
wait_streaming(){
 local end=$((SECONDS+1800)) n
 while ((SECONDS<end)); do
  n=$(psql "$C-1" "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;" 2>/dev/null || true)
  [[ "$n" == 1 ]] && return; sleep 5
 done
 die "Replica did not catch up."
}
wait_replay_lsn(){
 local target="$1" end=$((SECONDS+3600)) caught
 while ((SECONDS<end)); do
  caught=$(psql "$C-2" "SELECT pg_last_wal_replay_lsn()>='$target'::pg_lsn;" 2>/dev/null || true)
  [[ "$caught" == t ]] && return
  sleep 5
 done
 die "Replica did not replay through $target."
}
wait_premiumv2_hydrated(){
 local claim="$1" disk="$2" outfile="$3" end=$((SECONDS+86400)) now last_log=0 state option percent
 echo "Waiting for $claim Premium SSD v2 disk copy hydration before preparation is declared complete."
 while ((SECONDS<end)); do
  azc disk show --ids "$disk" -o json >"$outfile" || die "Cannot inspect $claim disk hydration."
  state=$(jq -r '.provisioningState // ""' "$outfile")
  [[ "$state" != Failed ]] || die "$claim disk reached terminal provisioningState Failed."
  option=$(jq -r '.creationData.createOption // ""' "$outfile")
  percent=$(jq -r 'if .completionPercent==null then "null" else ((.completionPercent|tonumber?) // "invalid") end' "$outfile")
  case "$option" in
   Empty)
    [[ "$percent" == null || "$percent" == 100 ]] || die "$claim Empty disk has unexpected completionPercent=$percent."
    [[ "$state" == Succeeded ]] && return 0
    ;;
   Copy|CopyStart)
    [[ "$percent" == 100 && "$state" == Succeeded ]] && return 0
    ;;
   *) die "$claim disk has unsupported createOption=$option during hydration gate." ;;
  esac
  now=$SECONDS
  if ((now-last_log>=900)); then echo "$claim disk hydration pending (state=$state, createOption=$option, completionPercent=$percent)."; last_log=$now; fi
  sleep 20
 done
 die "$claim disk did not finish Copy/CopyStart hydration within 24 hours."
}
absent(){ local what="$1"; shift; if "$@" >/dev/null 2>"$TMP/absent.err"; then die "$what already exists."; fi; grep -Eiq 'ResourceNotFound|SnapshotNotFound|DiskNotFound|NotFound|not found|could not be found' "$TMP/absent.err" || { cat "$TMP/absent.err" >&2; die "Cannot prove $what absent."; }; }
lock_snapshot(){
 local id="$1" name="$2" locks lockname="retain-$2"
 azc lock create --name "$lockname" --lock-type CanNotDelete --resource-group "$SNAP_RG" --resource-name "$name" --resource-type Microsoft.Compute/snapshots --notes 'Snapshot retained by the Premium SSD v2 VACUUM experiment until explicit user authorization.' -o none || die "Cannot protect snapshot $name."
 locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die "Cannot inspect lock for snapshot $name."
 jq -e --arg id "${id,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die "CanNotDelete lock did not verify for snapshot $name."
}
[[ $(azc account show --query id -o tsv) == "$SUB" ]] || die "Azure subscription mismatch."
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$RG" -o json) >/dev/null || die "Stack RG token mismatch."
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$NODE_RG" -o json) >/dev/null || die "Node RG token mismatch."
jq -e --arg t "$TOKEN" --arg n "$NODE_RG" '.tags.reprotoken==$t and .nodeResourceGroup==$n' < <(azc aks show -g "$RG" -n "$AKS" -o json) >/dev/null || die "AKS identity mismatch."
jq -e --arg id "$(tr '[:upper:]' '[:lower:]' <<<"$AID")" --arg t "$TOKEN" --arg n "$ACCOUNT" '((.id|ascii_downcase)==$id) and (.name|ascii_downcase)==($n|ascii_downcase) and .provisioningState=="Succeeded" and .kind=="StorageV2" and .sku.name=="Standard_LRS" and .allowBlobPublicAccess==false and .enableHttpsTrafficOnly==true and .tags.reprotoken==$t' < <(azc storage account show -g "$RG" -n "$ACCOUNT" -o json) >/dev/null || die "Storage account identity/privacy mismatch."
CID="$AID/blobServices/default/containers/$CONTAINER"
container=$(azc rest --method get --url "https://management.azure.com$CID?api-version=2023-05-01" -o json)
jq -e --arg id "${CID,,}" --arg t "$TOKEN" --arg n "$CONTAINER" '((.id|ascii_downcase)==$id) and (.name|ascii_downcase)==($n|ascii_downcase) and .properties.metadata.reprotoken==$t and (.properties.publicAccess==null or .properties.publicAccess=="None")' <<<"$container" >/dev/null || die "Container owner/privacy mismatch."
[[ $(k config current-context) == *"$AKS"* ]] || die "Wrong Kubernetes context."
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' < <(k get namespace "$NS" -o json) >/dev/null || die "Namespace token mismatch."
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and (.data["connection-string"]|length>0)' < <(k get secret "$SECRET" -n "$NS" -o json) >/dev/null || die "Archive Secret check failed."
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="PremiumV2_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .parameters.DiskIOPSReadWrite=="3000" and .parameters.DiskMBpsReadWrite=="125" and .parameters.LogicalSectorSize=="512"' < <(k get storageclass "$SC" -o json) >/dev/null || die "StorageClass must be uncached PremiumV2_LRS at 3000 IOPS / 125 MB/s and 512-byte sectors."
for crd in volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io; do k get crd "$crd" >/dev/null || die "Missing $crd"; done

META="$ROOT/snapshot.json"
SYSID=$(jq -er '.systemIdentifier|tostring' "$META"); HEAP=$(jq -er .heapBytes "$META"); TOTAL=$(jq -er .tableBytes "$META")
SNAP=$(jq -er .sourceSnapshotId "$SOURCE_MANIFEST"); SOURCE_RG=$(jq -er .resourceGroup "$SOURCE_MANIFEST")
[[ "${SNAP,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SOURCE_RG,,}/providers/microsoft.compute/snapshots/"* ]] || die "Pinned source ID and manifest resource group differ."
jq -e --arg sys "$SYSID" --argjson heap "$HEAP" --argjson total "$TOTAL" '.systemIdentifier==$sys and .heapBytes==$heap and .tableBytes==$total and .sourceTopology=="combined PGDATA/pg_wal"' "$META" >/dev/null || die "Pinned keeper fixture metadata mismatch."
jq -e --arg sys "$SYSID" '.systemIdentifier==$sys' "$SOURCE_MANIFEST" >/dev/null || die "Allowlisted source manifest does not match the pinned keeper system identifier."
STAGE_HELPER="$ROOT/../recovery/source-restore/stage-allowlisted-source.sh"
[[ -x "$STAGE_HELPER" ]] || die "Owned source-staging helper is missing: $STAGE_HELPER"
STAGE_RECEIPT=''
STAGE_SUFFIX=''
shopt -s nullglob
receipts=("$SD"/source-stage/receipt-*.json)
shopt -u nullglob
for candidate in "${receipts[@]}"; do
  if jq -e --arg id "${SNAP,,}" --arg t "$TOKEN" '.status=="ready" and ((.sourceSnapshotId|ascii_downcase)==$id) and .ownerToken==$t and (.resourceSuffix|test("^[a-f0-9]{8}$"))' "$candidate" >/dev/null 2>&1; then
    STAGE_SUFFIX=$(jq -er .resourceSuffix "$candidate")
  fi
done
if [[ -z "$STAGE_SUFFIX" ]]; then
  shopt -s nullglob
  plans=("$SD"/source-stage/plan-*.json)
  shopt -u nullglob
  for candidate in "${plans[@]}"; do
    if jq -e --arg id "${SNAP,,}" --arg t "$TOKEN" '((.sourceSnapshotId|ascii_downcase)==$id) and .ownerToken==$t and (.resourceSuffix|test("^[a-f0-9]{8}$"))' "$candidate" >/dev/null 2>&1; then
      STAGE_SUFFIX=$(jq -er .resourceSuffix "$candidate")
    fi
  done
fi
if [[ -n "$STAGE_SUFFIX" ]]; then
  STAGE_RECEIPT=$(RESOURCE_SUFFIX="$STAGE_SUFFIX" "$STAGE_HELPER" "$STATE" "$SOURCE_MANIFEST" --apply) || die "Cannot revalidate the previous owned source-stage receipt."
else
  STAGE_RECEIPT=$("$STAGE_HELPER" "$STATE" "$SOURCE_MANIFEST" --apply) || die "Owned source staging did not complete."
fi
COPY=$(jq -er .restoreSnapshotId <<<"$STAGE_RECEIPT")
STAGE_DISK=$(jq -er .stagingDiskId <<<"$STAGE_RECEIPT")
STAGE_RECEIPT_PATH="$SD/source-stage/receipt-$(jq -er .resourceSuffix <<<"$STAGE_RECEIPT").json"
[[ -f "$STAGE_RECEIPT_PATH" ]] || die "Staging helper receipt file is missing: $STAGE_RECEIPT_PATH"
jq -e --arg id "${COPY,,}" --arg disk "${STAGE_DISK,,}" --arg token "$TOKEN" '.status=="ready" and ((.restoreSnapshotId|ascii_downcase)==$id) and ((.stagingDiskId|ascii_downcase)==$disk) and .ownerToken==$token' "$STAGE_RECEIPT_PATH" >/dev/null || die "Saved staging receipt does not match helper output."
jq -e --arg id "${SNAP,,}" --arg srcuid "$(jq -er .uniqueId "$SOURCE_MANIFEST")" --arg copy "${COPY,,}" --arg disk "${STAGE_DISK,,}" --arg diskuid "$(jq -er .stagingDiskUniqueId <<<"$STAGE_RECEIPT")" --arg token "$TOKEN" --arg loc "${LOC,,}" --arg snaprg "${SNAP_RG,,}" --arg noderg "${NODE_RG,,}" --arg purpose "$(jq -er .purpose "$SOURCE_MANIFEST")" --arg originalFamily "$(jq -er .incrementalSnapshotFamilyId "$SOURCE_MANIFEST")" '
  .status=="ready"
  and ((.sourceSnapshotId|ascii_downcase)==$id)
  and .sourceSnapshotUniqueId==$srcuid
  and .ownerToken==$token
  and ((.restoreSnapshotId|ascii_downcase)==$copy)
  and ((.stagingDiskId|ascii_downcase)==$disk)
  and ((.sourceSnapshotMetadata.id|ascii_downcase)==$id)
  and .sourceSnapshotMetadata.uniqueId==$srcuid
  and .sourceSnapshotMetadata.tags.reprotoken==.sourceManifest.ownerToken
  and .sourceSnapshotMetadata.tags.reproPurpose==$purpose
  and .sourceSnapshotMetadata.provisioningState=="Succeeded"
  and .sourceSnapshotMetadata.snapshotAccessState=="Available"
  and .sourceSnapshotMetadata.incremental==true
  and (.sourceSnapshotMetadata.diskSizeGB // .sourceSnapshotMetadata.diskSizeGb)==512
  and .sourceSnapshotMetadata.sku.name=="Standard_LRS"
  and .sourceSnapshotMetadata.incrementalSnapshotFamilyId==$originalFamily
  and ((.location|ascii_downcase)==$loc)
  and ((.stagingDiskMetadata.id|ascii_downcase)==$disk)
  and .stagingDiskMetadata.uniqueId==$diskuid
  and (.stagingDiskMetadata.uniqueId|length>0)
  and ((.stagingDiskMetadata.id|ascii_downcase|contains("/resourcegroups/"+$noderg+"/providers/microsoft.compute/disks/")))
  and .stagingDiskMetadata.provisioningState=="Succeeded"
  and .stagingDiskMetadata.sku.name=="Standard_LRS"
  and (.stagingDiskMetadata.diskSizeGB // .stagingDiskMetadata.diskSizeGb)==512
  and .stagingDiskMetadata.diskState=="Unattached"
  and .stagingDiskMetadata.managedBy==null
  and ((.stagingDiskMetadata.creationData.sourceResourceId|ascii_downcase)==$id)
  and .stagingDiskMetadata.creationData.sourceUniqueId==$srcuid
  and ((.stagingDiskMetadata.logicalSectorSize // .stagingDiskMetadata.creationData.logicalSectorSize // .logicalSectorSizeBytes)|tostring)=="512"
  and ((.restoreSnapshotMetadata.id|ascii_downcase)==$copy)
  and .restoreSnapshotMetadata.uniqueId==.restoreSnapshotUniqueId
  and (.restoreSnapshotUniqueId|length>0)
  and ((.restoreSnapshotMetadata.id|ascii_downcase|contains("/resourcegroups/"+$snaprg+"/providers/microsoft.compute/snapshots/")))
  and .restoreSnapshotMetadata.provisioningState=="Succeeded"
  and .restoreSnapshotMetadata.snapshotAccessState=="Available"
  and (((.restoreSnapshotMetadata.completionPercent|tonumber?) // 100)==100)
  and (.restoreSnapshotMetadata.diskSizeGB // .restoreSnapshotMetadata.diskSizeGb)==512
  and .restoreSnapshotMetadata.sku.name=="Standard_LRS"
  and .restoreSnapshotMetadata.incremental==true
  and .restoreSnapshotMetadata.incrementalSnapshotFamilyId!=$originalFamily
  and ((.restoreSnapshotMetadata.creationData.createOption // "")=="Copy")
  and ((.restoreSnapshotMetadata.creationData.sourceResourceId|ascii_downcase)==$disk)
  and .restoreSnapshotMetadata.creationData.sourceUniqueId==$diskuid
  and .restoreSnapshotMetadata.incrementalSnapshotFamilyId!=$originalFamily
  and .restoreSnapshotMetadata.tags.reprotoken==$token
  and .restoreSnapshotMetadata.tags.reproPurpose=="premiumv2-keeper-copy"
  and ((.restoreSnapshotMetadata.tags.sourceSnapshotId|ascii_downcase)==$id)
  and ((.restoreSnapshotMetadata.tags.sourceDiskId|ascii_downcase)==$disk)
  and .retention.locked
  and .retention.lockMetadata.level=="CanNotDelete"
  and (((.retention.lockMetadata.scope // ((.retention.lockMetadata.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$copy)
  and .logicalSectorSizeBytes==512
  and (.logicalSectorSizeEvidence|type=="string" and length>0)
' <<<"$STAGE_RECEIPT" >/dev/null || die "Owned restore snapshot/staging-disk receipt failed provenance and readiness checks."
COPY=$(jq -er '.restoreSnapshotMetadata.id' <<<"$STAGE_RECEIPT")
source_copy_live=$(azc snapshot show --ids "$COPY" -o json) || die "Cannot reread the owned source-copy snapshot."
jq -e --arg id "${COPY,,}" --arg copyuid "$(jq -er .restoreSnapshotUniqueId <<<"$STAGE_RECEIPT")" --arg disk "${STAGE_DISK,,}" --arg diskuid "$(jq -er .stagingDiskUniqueId <<<"$STAGE_RECEIPT")" --arg original "${SNAP,,}" --arg family "$(jq -er .incrementalSnapshotFamilyId "$SOURCE_MANIFEST")" --arg stagedFamily "$(jq -er .incrementalSnapshotFamilyId <<<"$STAGE_RECEIPT")" --arg t "$TOKEN" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and .uniqueId==$copyuid and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (((.completionPercent|tonumber?) // 100)==100) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and ((.creationData.createOption // "")=="Copy") and ((.creationData.sourceResourceId|ascii_downcase)==$disk) and .creationData.sourceUniqueId==$diskuid and .incrementalSnapshotFamilyId==$stagedFamily and .incrementalSnapshotFamilyId!=$family and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-keeper-copy" and ((.tags.sourceSnapshotId|ascii_downcase)==$original)' <<<"$source_copy_live" >/dev/null || die "Owned incremental source copy changed after helper verification."
SOURCE_LOCKS=$(azc lock list --resource-group "$SNAP_RG" -o json) || die "Cannot verify the owned restore snapshot lock."
jq -e --arg id "${COPY,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$SOURCE_LOCKS" >/dev/null || die "Owned restore snapshot lacks a direct CanNotDelete lock."
state_tmp=$(mktemp "$SD/.state.XXXXXX"); jq --arg id "$COPY" --arg disk "$STAGE_DISK" --arg receipt "$STAGE_RECEIPT_PATH" '.snapshotSourceCopyId=$id|.snapshotSourceStagingDiskId=$disk|.snapshotSourceStageReceiptPath=$receipt' "$STATE" >"$state_tmp"; chmod 600 "$state_tmp"; mv -f "$state_tmp" "$STATE"
KIND=incremental_copy
source_record=$(jq -n --arg id "$SNAP" --arg copy "$COPY" --arg disk "$STAGE_DISK" --arg rg "$SNAP_RG" --arg sourceRg "$SOURCE_RG" --arg receiptPath "$STAGE_RECEIPT_PATH" --argjson metadata "$(cat "$SOURCE_MANIFEST")" --argjson stage "$STAGE_RECEIPT" '{version:2,sourceSnapshotId:$id,sourceResourceGroup:$sourceRg,ownedCopySnapshotId:$copy,ownedCopyResourceGroup:$rg,stagingDiskId:$disk,stageReceiptPath:$receiptPath,stageReceipt:$stage,manifest:$metadata}')
printf '%s\n' "$source_record" >"$SD/source-snapshot.json"; chmod 600 "$SD/source-snapshot.json"
VSC="vacuum-snap-$SHORT"; VSS="vacuum-data-$SHORT"; BK="vacuum-backup-$SHORT"; BSC="vacuum-backup-snap-$SHORT"
for item in "volumesnapshotclass:$VSC" "volumesnapshotclass:$BSC" "volumesnapshotcontent:vacuum-content-$SHORT" "volumesnapshot:$VSS" "cluster:$C" "backup:$BK"; do
 kind=$(cut -d: -f1 <<<"$item"); name=$(cut -d: -f2 <<<"$item")
 if k get "$kind" "$name" -n "$NS" -o json >/dev/null 2>"$TMP/absent-$kind-$name.err"; then die "$kind/$name already exists."; fi
 grep -Eiq 'NotFound|not found' "$TMP/absent-$kind-$name.err" || die "Cannot prove $kind/$name absent."
done
k apply -f - <<EOF >/dev/null
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: {name: $VSC, labels: {repro.stack.token: "$TOKEN"}}
driver: disk.csi.azure.com
deletionPolicy: Retain
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata: {name: vacuum-content-$SHORT, labels: {repro.stack.token: "$TOKEN"}}
spec:
  deletionPolicy: Retain
  driver: disk.csi.azure.com
  source: {snapshotHandle: "$COPY"}
  volumeSnapshotClassName: $VSC
  volumeSnapshotRef: {kind: VolumeSnapshot, name: $VSS, namespace: $NS}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $VSS, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: vacuum-content-$SHORT}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: {name: $BSC, labels: {repro.stack.token: "$TOKEN"}}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters:
  resourceGroup: $SNAP_RG
  incremental: "true"
  tags: "reprotoken=$TOKEN,reproPurpose=premiumv2-vacuum-backup"
EOF
end=$((SECONDS+1800)); until [[ $(k get volumesnapshot "$VSS" -n "$NS" -o json|jq -r '.status.readyToUse // false') == true ]]; do ((SECONDS<end)) || die "Source VolumeSnapshot timed out."; sleep 10; done
cat >"$TMP/cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: $C, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}, annotations: {vacuum-only-repro/source-snapshot-id: "$SNAP", vacuum-only-repro/restore-copy-id: "$COPY"}}
spec:
  instances: 1
  imageName: $SYSIMAGE
  storage: {storageClass: $SC, size: 512Gi}
  bootstrap:
    recovery:
      database: repro
      owner: app
      volumeSnapshots: {storage: {name: $VSS, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}}
  affinity: {enablePodAntiAffinity: true, podAntiAffinityType: required, topologyKey: kubernetes.io/hostname}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "2", memory: 3Gi}}
  postgresql: {parameters: {shared_buffers: 256MB, track_io_timing: "on", track_wal_io_timing: "on"}}
  backup:
    barmanObjectStore:
      destinationPath: https://$ACCOUNT.blob.core.windows.net/$CONTAINER/cnpg-vacuum-only/$TOKEN/
      serverName: $C
      azureCredentials: {connectionString: {name: $SECRET, key: connection-string}}
    volumeSnapshot: {className: $BSC, walClassName: $BSC, online: true, onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}
EOF
k apply -f "$TMP/cluster.yaml" >/dev/null
c=$(wait_ready 1); POD=$(jq -r .status.currentPrimary <<<"$c")
auto=$(k exec -n "$NS" -c postgres "$POD" -- sh -eu -c 'd=$(printenv PGDATA); [ -n "$d" ] || d=/var/lib/postgresql/data/pgdata; f=$d/postgresql.auto.conf; test -f "$f" && test ! -L "$f"; grep -Eq "^[[:space:]]*[^#[:space:]]" "$f" && echo dirty || echo clean')
if [[ "$auto" == dirty ]]; then
 k exec -n "$NS" -c postgres "$POD" -- sh -eu -c 'd=$(printenv PGDATA); [ -n "$d" ] || d=/var/lib/postgresql/data/pgdata; f=$d/postgresql.auto.conf; chmod u+w "$f"; trap '\''chmod 400 "$f"'\'' EXIT; psql -X -v ON_ERROR_STOP=1 -U postgres -d repro -c "ALTER SYSTEM RESET ALL"' >/dev/null
 klong delete pod "$POD" -n "$NS" --wait=true --timeout=10m >/dev/null; c=$(wait_ready 1); POD=$(jq -r .status.currentPrimary <<<"$c")
fi
SOURCE_HEAP=$HEAP; SOURCE_TOTAL=$TOTAL
psql "$POD" 'ALTER TABLESPACE pg_default RESET (maintenance_io_concurrency); ALTER TABLE public.vacuum_target SET (autovacuum_enabled=false);' >/dev/null
CFG="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'autovacuum',current_setting('autovacuum'),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'))::text;"
primary=$(psql "$POD" "$CFG")
jq -e --arg s "$SYSID" --argjson h "$SOURCE_HEAP" --argjson t "$SOURCE_TOTAL" '.systemIdentifier==$s and .inRecovery==false and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled and .autovacuum=="on" and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and .sharedBuffers=="256MB" and .maintenanceIoConcurrency=="10" and .checkpointTimeout=="5min" and .maxWalSize=="1GB" and .trackIoTiming=="on" and .trackWalIoTiming=="on" and .tablespaceOverride==false' <<<"$primary" >/dev/null || die "Primary source/default config check failed."

# Copy a bounded indexed prefix of incompressible source rows, then replace only
# the restored clone's large relation with a half-dead fixture at the target size.
source_info=$(psql "$POD" "SELECT json_build_object('maxId',(SELECT max(id) FROM public.vacuum_target),'heapBytes',pg_relation_size('public.vacuum_target'),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'samplePayloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target ORDER BY id LIMIT 1))::text;")
jq -e --argjson h "$SOURCE_HEAP" '.maxId>0 and .heapBytes==$h and .hasIndex and .payloadStoragePlain and .samplePayloadBytes==1536' <<<"$source_info" >/dev/null || die "Pinned source fixture is not the expected indexed 1536-byte incompressible table."
SOURCE_MAX=$(jq -r .maxId <<<"$source_info")
COPY_ROWS=$(((TARGET_HEAP_BYTES*SOURCE_MAX+SOURCE_HEAP-1)/(2*SOURCE_HEAP)))
((COPY_ROWS>0 && COPY_ROWS<=SOURCE_MAX/2)) || die "Requested fixture exceeds the verified live source prefix."
[[ $(psql "$POD" "SELECT to_regclass('public.vacuum_target_stage') IS NULL;") == t ]] || die "Fixture staging table already exists."
psql_long "$POD" 'CREATE TABLE public.vacuum_target_stage(id bigint,payload bytea) WITH (autovacuum_enabled=false); ALTER TABLE public.vacuum_target_stage ALTER COLUMN payload SET STORAGE PLAIN;'
fixture_sql="INSERT INTO public.vacuum_target_stage SELECT copies.id,copies.payload FROM (SELECT id,payload FROM public.vacuum_target ORDER BY id LIMIT $COPY_ROWS) s CROSS JOIN LATERAL (VALUES (s.id*2,s.payload),(s.id*2+1,s.payload)) AS copies(id,payload);"
plan=$(psql "$POD" "BEGIN; SET LOCAL enable_seqscan=off; EXPLAIN (FORMAT JSON) $fixture_sql COMMIT;")
jq -e '([..|objects|select((."Node Type"?=="Index Scan" or ."Node Type"?=="Index Only Scan") and ."Index Name"?=="vacuum_target_id_idx")]|length)>0 and ([..|objects|select(."Node Type"?=="Nested Loop")]|length)>0 and ([..|objects|select(."Node Type"?=="Values Scan")]|length)>0 and ([..|objects|select(."Node Type"?=="Sort")]|length)==0' <<<"$plan" >/dev/null || die "Bounded fixture copy plan must use the source ID index and interleave with a Nested Loop/Values Scan, without sorting."
duplicated=$(psql_long "$POD" "SET enable_seqscan=off; $fixture_sql")
[[ $(awk '/^INSERT 0 /{n=$3} END{print n}' <<<"$duplicated") == "$((2*COPY_ROWS))" ]] || die "The interleaved source payload duplication count did not match the requested fixture."
deleted=$(psql_long "$POD" 'CREATE INDEX vacuum_target_stage_id_idx ON public.vacuum_target_stage(id); DELETE FROM public.vacuum_target_stage WHERE id%2=1; CHECKPOINT;')
FIXTURE_DEAD=$(awk '/^DELETE /{n=$2} END{print n}' <<<"$deleted")
[[ "$FIXTURE_DEAD" == "$COPY_ROWS" ]] || die "The fixture did not retain exactly half its rows as dead tuples."
fixture=$(psql "$POD" "SELECT json_build_object('heapBytes',pg_relation_size('public.vacuum_target_stage'),'totalBytes',pg_total_relation_size('public.vacuum_target_stage'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target_stage WHERE id%2=0 ORDER BY id LIMIT 1),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target_stage'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target_stage'::regclass))::text;")
FIXTURE_HEAP=$(jq -r .heapBytes <<<"$fixture"); FIXTURE_TOTAL=$(jq -r .totalBytes <<<"$fixture")
TOLERANCE=$((TARGET_HEAP_BYTES/100))
jq -e --argjson lo "$((TARGET_HEAP_BYTES-TOLERANCE))" --argjson hi "$((TARGET_HEAP_BYTES+TOLERANCE))" '.heapBytes>=$lo and .heapBytes<=$hi and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled' <<<"$fixture" >/dev/null || die "Restored fixture did not verify at the requested size and row format."
psql_long "$POD" 'DROP TABLE public.vacuum_target; ALTER TABLE public.vacuum_target_stage RENAME TO vacuum_target; ALTER INDEX public.vacuum_target_stage_id_idx RENAME TO vacuum_target_id_idx;'
fixture=$(psql "$POD" "SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id%2=0 ORDER BY id LIMIT 1),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL)::text;")
FIXTURE_HEAP=$(jq -r .heapBytes <<<"$fixture"); FIXTURE_TOTAL=$(jq -r .totalBytes <<<"$fixture")
jq -e --arg s "$SYSID" --argjson lo "$((TARGET_HEAP_BYTES-TOLERANCE))" --argjson hi "$((TARGET_HEAP_BYTES+TOLERANCE))" '.systemIdentifier==$s and .inRecovery==false and .heapBytes>=$lo and .heapBytes<=$hi and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex' <<<"$fixture" >/dev/null || die "Restored fixture did not verify at the requested size and row format."
HEAP=$FIXTURE_HEAP; TOTAL=$FIXTURE_TOTAL
FIXTURE_LIVE=$COPY_ROWS

OLDUID=$(k get pod "$C-1" -n "$NS" -o json|jq -r .metadata.uid)
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"primaryUpdateMethod":"restart","walStorage":{"storageClass":"azure-premiumv2","size":"512Gi","resizeInUseVolumes":false}}}' >/dev/null
end=$((SECONDS+10800))
while ((SECONDS<end)); do
 c=$(k get cluster "$C" -n "$NS" -o json); p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); v=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 uid=$(jq -r --arg n "$C-1" '.items[]|select(.metadata.name==$n)|.metadata.uid' <<<"$p")
 ready=$(jq -r '.status.readyInstances // 0' <<<"$c")
 bound=$(jq -r --arg n "$C-1-wal" '[.items[]|select(.metadata.name==$n and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi")]|length' <<<"$v")
 mounted=$(jq -r --arg n "$C-1" --arg w "$C-1-wal" '.items[]|select(.metadata.name==$n)|any(.spec.volumes[]?;.persistentVolumeClaim.claimName==$w)' <<<"$p")
 [[ "$uid" != "$OLDUID" && "$ready" == 1 && "$bound" == 1 && "$mounted" == true ]] && break; sleep 10
done
((SECONDS<end)) || die "WAL migration timed out."
k exec -n "$NS" -c postgres "$C-1" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"'
before=$(psql "$C-1" 'SELECT archived_count FROM pg_stat_archiver;'); psql "$C-1" 'SELECT pg_switch_wal();' >/dev/null
end=$((SECONDS+900)); status='{}'
while ((SECONDS<end)); do
 status=$(psql "$C-1" "SELECT json_build_object('archived',archived_count,'failed',failed_count,'lastWal',last_archived_wal)::text FROM pg_stat_archiver;")
 (( $(jq -r .archived <<<"$status") > before )) && break; sleep 10
done
(( $(jq -r .archived <<<"$status") > before )) && jq -e '.failed==0 and (.lastWal|length)>0' <<<"$status" >/dev/null || die "WAL was not archived."
cat >"$TMP/backup.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: {name: $BK, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {cluster: {name: $C}, method: volumeSnapshot, online: true, onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}
EOF
k apply -f "$TMP/backup.yaml" >/dev/null
end=$((SECONDS+3600))
while ((SECONDS<end)); do
 backup=$(k get backup "$BK" -n "$NS" -o json); phase=$(jq -r '.status.phase // empty' <<<"$backup"); [[ "$phase" == completed ]] && break
 [[ "$phase" != failed && "$phase" != walArchivingFailing ]] || die "CNPG Backup failed: $phase"; sleep 10
done
jq -e '.status.phase=="completed" and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die "Backup lacks paired snapshots."
DATA=$(jq -r '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup"); WAL=$(jq -r '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
for spec in "$DATA:$C-1" "$WAL:$C-1-wal"; do
 IFS=: read -r snap pvc <<<"$spec"; vs=$(k get volumesnapshot "$snap" -n "$NS" -o json)
 jq -e --arg p "$pvc" '.status.readyToUse and .spec.source.persistentVolumeClaimName==$p' <<<"$vs" >/dev/null || die "Backup snapshot source validation failed."
 content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$vs"); content=$(k get volumesnapshotcontent "$content_name" -o json)
 snap_id=$(jq -er '.status.snapshotHandle' <<<"$content"); snap_name=${snap_id##*/}
 [[ "${snap_id,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SNAP_RG,,}/providers/microsoft.compute/snapshots/"* ]] || die "CNPG output snapshot escaped the owned retained RG."
 snapshot=$(azc snapshot show --ids "$snap_id" -o json) || die "Cannot read CNPG output snapshot $snap_name."
 jq -e --arg id "${snap_id,,}" --arg t "$TOKEN" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and (.diskSizeGB // .diskSizeGb)==512 and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-vacuum-backup"' <<<"$snapshot" >/dev/null || die "CNPG output snapshot ownership/performance metadata failed."
 lock_snapshot "$snap_id" "$snap_name"
 jq -n --arg id "$snap_id" --arg vs "$snap" --arg pvc "$pvc" '{id:$id,volumeSnapshot:$vs,sourcePVC:$pvc}' >>"$SD/output-snapshots.jsonl"
done
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null
end=$((SECONDS+10800)); JOB_UID=''
while ((SECONDS<end)); do
 jobs=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 job=$(jq -c --arg i "$C-2" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery")]|first // empty' <<<"$jobs")
 if [[ -n "$job" ]]; then
  jq -e --arg i "$SYSIMAGE" '(.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.image)==$i and (([.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.command[]]|join(" "))|contains("restoresnapshot"))' <<<"$job" >/dev/null || die "Replica job is not CNPG restoresnapshot on the pinned system image."
  JOB_UID=$(jq -r .metadata.uid <<<"$job")
 fi
 c=$(k get cluster "$C" -n "$NS" -o json)
 [[ -n "$JOB_UID" && $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 ]] && break; sleep 5
done
[[ -n "$JOB_UID" ]] || die "Snapshot recovery job not observed."
c=$(wait_ready 2)
event=$(k get events -n "$NS" -o json|jq --arg u "$JOB_UID" '[.items[]|select(.involvedObject.uid==$u and .reason=="Completed" and .message=="Job completed")]|first // empty')
[[ -n "$event" ]] || die "Snapshot recovery completion event missing."
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json); pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg n "$C-2" --arg d "$DATA" --arg w "$WAL" '[.items[]|select((.metadata.name==$n and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d) or (.metadata.name==($n+"-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w))]|length==2' <<<"$pvcs" >/dev/null || die "Replica snapshot PVC mismatch."
uids=$(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$pods")
k patch cluster "$C" -n "$NS" --type=json -p='[{"op":"remove","path":"/spec/backup/barmanObjectStore"}]' >/dev/null
end=$((SECONDS+180))
while ((SECONDS<end)); do
 c=$(k get cluster "$C" -n "$NS" -o json); p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 [[ $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 && $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$p") == "$uids" ]] && break
 sleep 5
done
[[ $(jq -r '.spec.backup.barmanObjectStore // null' <<<"$c") == null && $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$p") == "$uids" ]] || die "Removing archive writer restarted pods."
cache(){
 local pod="$1" port=$((40000+RANDOM%20000)) pid code=000 i
 k port-forward -n "$NS" "pod/$pod" "$port:8010" >"$TMP/cache-$pod.log" 2>&1 & pid=$!
 for ((i=0;i<20;i++)); do code=$(timeout 5s curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/cache/wal-archive" 2>/dev/null || true); [[ "$code" != 000 ]] && break; sleep 1; done
 kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; echo "$code"
}
end=$((SECONDS+180))
while ((SECONDS<end)); do
 cp=$(cache "$C-1"); cr=$(cache "$C-2"); [[ "$cp" == 404 && "$cr" == 404 ]] && break
 [[ "$cp" == 200 || "$cp" == 404 ]] && [[ "$cr" == 200 || "$cr" == 404 ]] || die "Unexpected archive cache status."; sleep 5
done
[[ "$cp" == 404 && "$cr" == 404 ]] || die "Archive cache did not clear."
k delete secret "$SECRET" -n "$NS" --wait=true >/dev/null
k patch cluster "$C" -n "$NS" --type=merge -p "$(jq -cn --arg i "$IMAGE" '{spec:{imageName:$i,primaryUpdateMethod:"restart"}}')" >/dev/null
digest=$(sed 's/.*@//' <<<"$IMAGE"); end=$((SECONDS+10800))
while ((SECONDS<end)); do
 c=$(k get cluster "$C" -n "$NS" -o json); p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 if [[ $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 ]] && jq -e --arg i "$IMAGE" --arg d "$digest" '[.items[]|select(.status.phase=="Running")|select(any(.spec.containers[];.name=="postgres" and .image==$i) and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($d))))]|length==2' <<<"$p" >/dev/null; then break; fi
 sleep 10
done
((SECONDS<end)) || die "Instances did not reach measurement image."
jq -e --arg i "$IMAGE" --arg c "$C" '.spec.imageName==$i and .status.readyInstances==2 and .status.currentPrimary==($c+"-1") and .spec.backup.barmanObjectStore==null and .spec.walStorage.size=="512Gi" and .spec.postgresql.synchronous.method=="any" and .spec.postgresql.synchronous.number==1 and .spec.postgresql.synchronous.dataDurability=="preferred"' <<<"$c" >/dev/null || die "Final Cluster check failed."
wait_streaming
k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$TMP/pods-final.json"; k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$TMP/pvcs-final.json"
for pod in "$C-1" "$C-2"; do
 k exec -n "$NS" -c postgres "$pod" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"'
 psql "$pod" "$CFG" >"$TMP/$pod-config.json"; rec=false; [[ "$pod" == "$C-2" ]] && rec=true
 jq -e --arg s "$SYSID" --argjson h "$HEAP" --argjson t "$TOTAL" --argjson r "$rec" '.systemIdentifier==$s and .inRecovery==$r and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled and .autovacuum=="on" and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and .sharedBuffers=="256MB" and .maintenanceIoConcurrency=="10" and .checkpointTimeout=="5min" and .maxWalSize=="1GB" and .archiveMode=="on" and .trackIoTiming=="on" and .trackWalIoTiming=="on" and .tablespaceOverride==false' "$TMP/$pod-config.json" >/dev/null || die "$pod settings/source mismatch."
done
disks=''
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do
 pv=$(jq -er --arg n "$claim" '.items[]|select(.metadata.name==$n and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-premiumv2")|.spec.volumeName' "$TMP/pvcs-final.json")
 k get pv "$pv" -o json >"$TMP/$claim-pv.json"; disk=$(jq -er .spec.csi.volumeHandle "$TMP/$claim-pv.json"); wait_premiumv2_hydrated "$claim" "$disk" "$TMP/$claim-disk.json"
 jq -e --arg rg "$(tr '[:upper:]' '[:lower:]' <<<"$NODE_RG")" --arg l "$(tr '[:upper:]' '[:lower:]' <<<"$LOC")" --arg z "$AZURE_ZONE" --arg id "$(tr '[:upper:]' '[:lower:]' <<<"$disk")" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$l) and .sku.name=="PremiumV2_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .diskIOPSReadWrite==3000 and .diskMBpsReadWrite==125 and .provisioningState=="Succeeded" and .managedBy!=null and ((.creationData.createOption // "") as $option | ((.completionPercent|tonumber?) // null) as $percent | if $option=="Empty" then ($percent==null or $percent==100) elif ($option=="Copy" or $option=="CopyStart") then $percent==100 else false end) and ((.zones // [])|map(tostring)|index($z))!=null and ((.logicalSectorSize // .creationData.logicalSectorSize // 0)|tostring)=="512"' "$TMP/$claim-disk.json" >/dev/null || die "$claim Premium SSD v2 disk SKU, performance, zone, sector, or hydration check failed."
 disks="$disks$disk"$'\n'
done
[[ $(sort -u <<<"$disks"|sed '/^$/d'|wc -l) == 4 ]] || die "Expected four distinct data/WAL disks."
REQUIRED_REPLAY_LSN=$(psql "$C-1" 'SELECT pg_current_wal_flush_lsn();')
wait_replay_lsn "$REQUIRED_REPLAY_LSN"
FINAL_KIND="$KIND"; [[ "$KIND" != fresh ]] || FINAL_KIND="fresh-initdb-seed"
jq -n --arg id "$SNAP" --arg kind "$FINAL_KIND" --arg s "$SYSID" --arg replay "$REQUIRED_REPLAY_LSN" --argjson estimate "$VACUUM_ESTIMATE_MIN" --argjson h "$HEAP" --argjson t "$TOTAL" --argjson sourceHeap "$SOURCE_HEAP" --argjson sourceTotal "$SOURCE_TOTAL" --argjson targetGiB "$TEST_HEAP_GIB" --argjson targetBytes "$TARGET_HEAP_BYTES" --argjson live "$FIXTURE_LIVE" --argjson dead "$FIXTURE_DEAD" --arg copy "$COPY" --arg b "$BK" --arg d "$DATA" --arg w "$WAL" --arg i "$IMAGE" --arg zone "$AZURE_ZONE" --arg snaprg "$SNAP_RG" '{version:1,status:"complete",sourceSnapshotId:$id,sourceKind:$kind,systemIdentifier:$s,sourceSnapshotHeapBytes:$sourceHeap,sourceSnapshotTableBytes:$sourceTotal,fixtureTargetHeapGiB:$targetGiB,fixtureTargetHeapBytes:$targetBytes,heapBytes:$h,tableBytes:$t,estimatedVacuumMinutes:$estimate,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$dead,replicaCaughtUpToLsn:$replay,restoreCopySnapshotId:$copy,cluster:"repro/hdd-pg",instances:2,availabilityZone:$zone,retainedSnapshotResourceGroup:$snaprg,storage:"four separate 512-GiB PremiumV2_LRS data/WAL disks at 3000 IOPS / 125 MB/s",backup:$b,replicaSnapshots:{data:$d,wal:$w},measurementImage:$i,clientCreated:false}' >"$SD/prepare.json"
chmod 600 "$SD/prepare.json"
echo "Prepared $C from $KIND source with a ${TEST_HEAP_GIB}-GiB half-dead fixture, paired snapshot recovery, four separate Premium SSD v2 data/WAL disks, and replica catch-up through $REQUIRED_REPLAY_LSN (estimated VACUUM ${VACUUM_ESTIMATE_MIN} min); state: $SD/prepare.json"
