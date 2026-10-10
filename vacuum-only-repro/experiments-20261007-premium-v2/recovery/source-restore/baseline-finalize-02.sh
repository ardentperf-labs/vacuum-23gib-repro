#!/usr/bin/env bash
# Read-only final audit for the already bootstrapped baseline; writes local receipts only.
set -Eeuo pipefail
umask 077

HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
if [[ -f "$HERE/.state/state.json" && -f "$HERE/run-config.json" ]]; then
  STACK="$HERE"; ROOT="$(cd -- "$HERE/.." && pwd)"
else
  ROOT="$(cd -- "$HERE/../.." && pwd)"; STACK="$ROOT/baseline-23gib"
fi
SD="$STACK/.state"; STATE="$SD/state.json"; NS=repro; C=hdd-pg; SC=azure-premiumv2
SECRET=azure-barman-credentials
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
TEST_HEAP_GIB=${TEST_HEAP_GIB:-23}
PREFLIGHT_ONLY=false
die(){ echo "ERROR: $*" >&2; exit 1; }
while (($#)); do case "$1" in --preflight-only) PREFLIGHT_ONLY=true;; -h|--help) echo "$0 [--preflight-only]"; exit 0;; *) die "Unknown option $1";; esac; shift; done
[[ "$TEST_HEAP_GIB" == 23 ]] || die "Baseline finalizer is pinned to its 23-GiB fixture."
TARGET_HEAP_BYTES=$((TEST_HEAP_GIB*1024*1024*1024))
VACUUM_ESTIMATE_MIN=$(awk -v g="$TEST_HEAP_GIB" 'BEGIN{printf "%.1f",g*260.583/110}')
for x in az kubectl jq timeout flock grep rg awk sort; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die "Baseline provision state is not ready."
jq -e '.targetHeapGiB==23 and .location=="eastus" and .zone=="1" and .diskSku=="PremiumV2_LRS" and .diskIops==3000 and .diskMBps==125 and .diskSizeGiB==512' "$STACK/run-config.json" >/dev/null || die "Baseline run configuration changed."
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE"); LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SNAP_RG=$(jq -er .snapshotResourceGroup "$STATE"); SNAP_RG_ID=$(jq -er .snapshotResourceGroupId "$STATE"); AZURE_ZONE=$(jq -er .availabilityZone "$STATE")
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die "Bad owner token."
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$NODE_RG" == "rg-vacuum-nodes-$TOKEN" && "$SNAP_RG" == "rg-vacuum-pv2-snap-$TOKEN" ]] || die "Baseline resource-group names do not match the owner token."
[[ "${SNAP_RG_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SNAP_RG,,}" && "${LOC,,}" == eastus && "$AZURE_ZONE" == 1 ]] || die "Baseline snapshot group, region, or zone metadata changed."
SHORT=${TOKEN:0:8}
mkdir -p "$SD/recovery"; chmod 700 "$SD" "$SD/recovery"
exec 9>"$SD/baseline-finalize.lock"; flock -n 9 || die "Baseline final audit is already active."
TMP=$(mktemp -d "$SD/recovery/.baseline-finalize.XXXXXX"); chmod 700 "$TMP"
cleanup(){ local rc=$?; rm -rf -- "$TMP"; return "$rc"; }
trap cleanup EXIT
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" ]] || KCFG="$SD/kubeconfig"
[[ "$KCFG" == /* ]] || KCFG="$STACK/$KCFG"; [[ -f "$KCFG" ]] || die "Kubeconfig missing."
export KUBECONFIG="$KCFG"
azc(){ timeout 900s az "$@" --subscription "$SUB"; }
k(){ timeout 120s kubectl "$@"; }
psql(){ k exec -n "$NS" -c postgres "$1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }

[[ "$(azc account show --query id -o tsv)" == "$SUB" ]] || die "Azure subscription mismatch."
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$RG" -o json) >/dev/null || die "Baseline resource-group ownership mismatch."
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$NODE_RG" -o json) >/dev/null || die "Baseline node resource-group ownership mismatch."
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$SNAP_RG" -o json) >/dev/null || die "Baseline snapshot resource-group ownership mismatch."
jq -e --arg t "$TOKEN" --arg n "$NODE_RG" '.tags.reprotoken==$t and .nodeResourceGroup==$n' < <(azc aks show -g "$RG" -n "$AKS" -o json) >/dev/null || die "Baseline AKS identity mismatch."
[[ "$(k config current-context)" == *"$AKS"* ]] || die "Wrong Kubernetes context."
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' < <(k get namespace "$NS" -o json) >/dev/null || die "Namespace owner token mismatch."

# The archive Secret was intentionally removed by the completed restore path.
# Prove exact NotFound, rather than requiring the former Secret or treating an
# auth/network error as absence.
if k get secret "$SECRET" -n "$NS" -o json >"$TMP/archive-secret.json" 2>"$TMP/archive-secret.err"; then
  die "Archive Secret is still present after archive removal."
fi
grep -Eiq 'NotFound|not found' "$TMP/archive-secret.err" || { cat "$TMP/archive-secret.err" >&2; die "Cannot prove the removed Archive Secret absent."; }
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="PremiumV2_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .parameters.DiskIOPSReadWrite=="3000" and .parameters.DiskMBpsReadWrite=="125" and .parameters.LogicalSectorSize=="512"' < <(k get storageclass "$SC" -o json) >/dev/null || die "StorageClass is not uncached PremiumV2_LRS at 3000 IOPS / 125 MB/s and 512-byte sectors."
for crd in volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io; do k get crd "$crd" >/dev/null || die "Missing $crd"; done

SOURCE_MANIFEST="$STACK/source-manifest.json"; META="$STACK/snapshot.json"
SOURCE_RECORD="$SD/source-snapshot.json"; FROZEN_FIXTURE="$SD/recovery/fixture.json"
BACKUP_RECORD="$SD/recovery/backup.json"; SNAPSHOT_RECORDS="$SD/output-snapshots.jsonl"
for f in "$SOURCE_MANIFEST" "$META" "$SOURCE_RECORD" "$FROZEN_FIXTURE" "$BACKUP_RECORD" "$SNAPSHOT_RECORDS"; do [[ -s "$f" ]] || die "Required prior-stage metadata is missing or empty: $f"; done
jq -e '.version==1 and (.sourceSnapshotId|type=="string" and startswith("/subscriptions/")) and (.uniqueId|type=="string" and length>0) and (.ownerToken|type=="string" and test("^[a-f0-9]{32}$")) and (.resourceGroup|type=="string" and length>0) and (.location|type=="string" and length>0) and .incremental==true and .diskSizeGiB==512 and .logicalSectorSize==512 and (.systemIdentifier|type=="string" and length>0)' "$SOURCE_MANIFEST" >/dev/null || die "Pinned source manifest is invalid."
SYSID=$(jq -er '.systemIdentifier|tostring' "$META"); SOURCE_HEAP=$(jq -er .heapBytes "$META"); SOURCE_TOTAL=$(jq -er .tableBytes "$META")
SNAP=$(jq -er .sourceSnapshotId "$SOURCE_MANIFEST"); SOURCE_OWNER=$(jq -er .ownerToken "$SOURCE_MANIFEST"); SOURCE_RG=$(jq -er .resourceGroup "$SOURCE_MANIFEST")
[[ "${SNAP,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SOURCE_RG,,}/providers/microsoft.compute/snapshots/"* ]] || die "Pinned source ID and manifest resource group differ."
jq -e --arg s "$SYSID" --argjson h "$SOURCE_HEAP" --argjson total "$SOURCE_TOTAL" '.systemIdentifier==$s and .heapBytes==$h and .tableBytes==$total and .sourceTopology=="combined PGDATA/pg_wal"' "$META" >/dev/null || die "Pinned source fixture metadata mismatch."
jq -e --arg s "$SYSID" '.systemIdentifier==$s' "$SOURCE_MANIFEST" >/dev/null || die "Pinned source system identifier mismatch."
jq -e --arg id "${SNAP,,}" --arg t "$SOURCE_OWNER" --arg uid "$(jq -er .uniqueId "$SOURCE_MANIFEST")" '(.sourceSnapshotId|ascii_downcase)==$id and (.manifest.sourceSnapshotId|ascii_downcase)==$id and .manifest.ownerToken==$t and .manifest.uniqueId==$uid and .manifest.incremental==true and .manifest.diskSizeGiB==512 and .manifest.logicalSectorSize==512' "$SOURCE_RECORD" >/dev/null || die "source-snapshot.json does not match the pinned source."
COPY=$(jq -er .ownedCopySnapshotId "$SOURCE_RECORD")
[[ "${COPY,,}" == "${SNAP,,}" ]] && die "Restore copy unexpectedly points at the original source."
jq -e --arg id "${COPY,,}" '(.snapshotSourceCopyId|ascii_downcase)==$id' "$STATE" >/dev/null || die "Stack state and source-snapshot.json disagree about the restore copy."
jq -e --arg s "$SYSID" --argjson target "$TARGET_HEAP_BYTES" '.systemIdentifier==$s and .inRecovery==false and .heapBytes==$target and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex and .liveEstimate==.deadEstimate and .deadEstimate>0 and .imageSettings.full_page_writes=="on" and .imageSettings.recovery_prefetch=="try" and .imageSettings.wal_level=="logical" and .imageSettings.vacuum_cost_delay=="0"' "$FROZEN_FIXTURE" >/dev/null || die "Frozen baseline fixture record does not match 23 GiB and the source image."
HEAP=$(jq -er .heapBytes "$FROZEN_FIXTURE"); TOTAL=$(jq -er .totalBytes "$FROZEN_FIXTURE")
FIXTURE_LIVE=$(jq -er .liveEstimate "$FROZEN_FIXTURE"); FIXTURE_DEAD=$(jq -er .deadEstimate "$FROZEN_FIXTURE")

source=$(azc snapshot show --ids "$SNAP" -o json) || die "Cannot read pinned source snapshot."
jq -e --arg id "${SNAP,,}" --arg uid "$(jq -er .uniqueId "$SOURCE_MANIFEST")" --arg token "$SOURCE_OWNER" --arg loc "${LOC,,}" --arg purpose "$(jq -er .purpose "$SOURCE_MANIFEST")" '((.id|ascii_downcase)==$id) and .uniqueId==$uid and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (((.completionPercent|tonumber?) // 100)==100) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .tags.reprotoken==$token and .tags.reproPurpose==$purpose and ((.creationData.logicalSectorSize // 512)==512)' <<<"$source" >/dev/null || die "Allowlisted source snapshot identity/readiness changed."
copy_live=$(azc snapshot show --ids "$COPY" -o json) || die "Cannot read owned restore-copy snapshot."
jq -e --arg id "${COPY,,}" --arg src "${SNAP,,}" --arg uid "$(jq -er .uniqueId "$SOURCE_MANIFEST")" --arg family "$(jq -er .incrementalSnapshotFamilyId "$SOURCE_MANIFEST")" --arg token "$TOKEN" --arg loc "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .creationData.sourceUniqueId==$uid and (.incrementalSnapshotFamilyId|type=="string" and length>0) and .incrementalSnapshotFamilyId!=$family and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .tags.reprotoken==$token and .tags.reproPurpose=="premiumv2-keeper-copy" and ((.tags.sourceSnapshotId|ascii_downcase)==$src)' <<<"$copy_live" >/dev/null || die "Owned restore-copy snapshot identity/readiness changed."

BK="vacuum-backup-$SHORT"
backup_list=$(k get backup -n "$NS" -o json)
backup=$(jq -ce --arg n "$BK" --arg t "$TOKEN" '[.items[]|select(.metadata.name==$n and .metadata.labels["repro.stack.token"]==$t)]|if length==1 then .[0] else error("expected exactly one owned baseline Backup") end' <<<"$backup_list") || die "Cannot identify the completed owned backup."
jq -e --arg t "$TOKEN" 'all(.items[]|select(.metadata.labels["repro.stack.token"]==$t); .status.phase=="completed")' <<<"$backup_list" >/dev/null || die "An owned CNPG backup is still active or incomplete."
jq -e '.status.phase=="completed" and .spec.cluster.name=="hdd-pg" and .spec.method=="volumeSnapshot" and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die "Owned baseline paired data/WAL Backup is incomplete."
DATA=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup")
WAL=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
[[ "$BK" == "vacuum-backup-$SHORT" ]] || die "Unexpected baseline backup identity."
[[ "$DATA" != "$WAL" ]] || die "Backup DATA and WAL snapshots have the same name."
jq -s -e --arg d "$DATA" --arg w "$WAL" 'length==2 and any(.[];.volumeSnapshot==$d and .sourcePVC=="hdd-pg-1") and any(.[];.volumeSnapshot==$w and .sourcePVC=="hdd-pg-1-wal")' "$SNAPSHOT_RECORDS" >/dev/null || die "Recorded retained snapshots are not the exact paired backup outputs."

c=$(k get cluster "$C" -n "$NS" -o json) || die "Cannot read baseline Cluster."
jq -e --arg t "$TOKEN" --arg i "$IMAGE" --arg c "$C" --arg src "$SNAP" --arg copy "$COPY" --arg s "$SYSID" '.metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/source-snapshot-id"]==$src and .metadata.annotations["vacuum-only-repro/restore-copy-id"]==$copy and .spec.instances==2 and .status.readyInstances==2 and .status.currentPrimary==($c+"-1") and .status.systemID==$s and .spec.imageName==$i and .spec.backup.barmanObjectStore==null and .spec.walStorage.size=="512Gi" and .spec.storage.size=="512Gi" and .spec.storage.storageClass=="azure-premiumv2" and .spec.postgresql.synchronous.method=="any" and .spec.postgresql.synchronous.number==1 and .spec.postgresql.synchronous.dataDurability=="preferred"' <<<"$c" >/dev/null || die "Cluster is not at the completed two-instance measurement stage."
pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg i "$IMAGE" --arg d "${IMAGE##*@}" --arg p "$C-1" --arg r "$C-2" '((.items|length)==2) and ([.items[]|select(.status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($d))) and any(.spec.containers[];.name=="postgres" and .image==$i))] as $ready | ($ready|length)==2 and any($ready[];.metadata.name==$p) and any($ready[];.metadata.name==$r))' <<<"$pods" >/dev/null || die "Both baseline pods must be ready on the exact measurement image."
jq -e --arg p "$C-1" --arg r "$C-2" '[.items[]|select(.metadata.name==$p or .metadata.name==$r)]|length==2' <<<"$pods" >/dev/null || die "Unexpected baseline Cluster pod set."
BASE_VSS="vacuum-data-$SHORT"; BASE_VSC="vacuum-snap-$SHORT"
source_vsc=$(k get volumesnapshotclass "$BASE_VSC" -o json) || die "Cannot read original source VolumeSnapshotClass."
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain"' <<<"$source_vsc" >/dev/null || die "Original source VolumeSnapshotClass is not owned and retained."
source_vss=$(k get volumesnapshot "$BASE_VSS" -n "$NS" -o json) || die "Cannot read original source VolumeSnapshot."
jq -e --arg t "$TOKEN" --arg cls "$BASE_VSC" '.metadata.labels["repro.stack.token"]==$t and .status.readyToUse==true and .spec.volumeSnapshotClassName==$cls and .spec.source.volumeSnapshotContentName!=null' <<<"$source_vss" >/dev/null || die "Original source VolumeSnapshot is not ready/owned."
source_content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$source_vss")
source_content=$(k get volumesnapshotcontent "$source_content_name" -o json) || die "Cannot read original source VolumeSnapshotContent."
jq -e --arg t "$TOKEN" --arg n "$BASE_VSS" --arg ns "$NS" --arg uid "$(jq -er .metadata.uid <<<"$source_vss")" --arg copy "${COPY,,}" '.metadata.labels["repro.stack.token"]==$t and .spec.deletionPolicy=="Retain" and .spec.driver=="disk.csi.azure.com" and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and .spec.volumeSnapshotRef.uid==$uid and ((.spec.source.snapshotHandle|ascii_downcase)==$copy) and ((.status.snapshotHandle|ascii_downcase)==$copy) and .status.readyToUse==true' <<<"$source_content" >/dev/null || die "Primary source VolumeSnapshotContent is not retained from the exact restore-copy snapshot."
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg p "$C-1" --arg pw "$C-1-wal" --arg r "$C-2" --arg rw "$C-2-wal" --arg base "$BASE_VSS" --arg d "$DATA" --arg w "$WAL" '(.items|length)==4 and ([.items[]|select((.metadata.name==$p and .status.phase=="Bound" and .spec.storageClassName=="azure-premiumv2" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$base) or (.metadata.name==$pw and .status.phase=="Bound" and .spec.storageClassName=="azure-premiumv2" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource==null) or (.metadata.name==$r and .status.phase=="Bound" and .spec.storageClassName=="azure-premiumv2" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d) or (.metadata.name==$rw and .status.phase=="Bound" and .spec.storageClassName=="azure-premiumv2" and .spec.resources.requests.storage=="512Gi" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w))]|length==4)' <<<"$pvcs" >/dev/null || die "The four DATA/WAL PVCs are not bound to the expected source and paired backup snapshot inputs."

CFG="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id%2=0 ORDER BY id LIMIT 1),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'autovacuum',current_setting('autovacuum'),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'),'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
check_pod(){
  local pod="$1" recovery="$2" file="$TMP/$1-config.json"
  psql "$pod" "$CFG" >"$file" || die "Cannot read live PostgreSQL fixture/settings on $pod."
  jq -e --arg s "$SYSID" --argjson h "$HEAP" --argjson t "$TOTAL" --argjson r "$recovery" '.systemIdentifier==$s and .inRecovery==$r and .heapBytes==$h and .totalBytes==$t and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex and .autovacuum=="on" and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and .sharedBuffers=="256MB" and .maintenanceIoConcurrency=="10" and .checkpointTimeout=="5min" and .maxWalSize=="1GB" and .archiveMode=="on" and .trackIoTiming=="on" and .trackWalIoTiming=="on" and .tablespaceOverride==false and .appSessions==0' "$file" >/dev/null || die "$pod fixture, exact settings, or no-client-workload check failed."
  k exec -n "$NS" -c postgres "$pod" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"' || die "$pod DATA/WAL mount separation failed."
}
for spec in "$C-1:false" "$C-2:true"; do IFS=: read -r pod recovery <<<"$spec"; check_pod "$pod" "$recovery"; done

# Verify actual paired VolumeSnapshot -> Content -> owned ARM snapshot lineage.
vss_class="vacuum-backup-snap-$SHORT"
k get volumesnapshotclass "$vss_class" -o json >"$TMP/backup-vsc.json"
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain"' "$TMP/backup-vsc.json" >/dev/null || die "Baseline backup VolumeSnapshotClass is not owned/retained."
for spec in "$DATA:$C-1" "$WAL:$C-1-wal"; do
  IFS=: read -r snap pvc <<<"$spec"
  vs=$(k get volumesnapshot "$snap" -n "$NS" -o json) || die "Missing completed backup VolumeSnapshot $snap."
  jq -e --arg b "$BK" --arg p "$pvc" --arg cls "$vss_class" '.metadata.labels["cnpg.io/cluster"]=="hdd-pg" and .metadata.labels["cnpg.io/backupName"]==$b and .status.readyToUse==true and .spec.source.persistentVolumeClaimName==$p and .spec.volumeSnapshotClassName==$cls' <<<"$vs" >/dev/null || die "Backup VolumeSnapshot $snap does not match its source PVC/backup."
  content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$vs")
  content=$(k get volumesnapshotcontent "$content_name" -o json) || die "Cannot read bound backup VolumeSnapshotContent."
  primary_pv=$(jq -er --arg p "$pvc" '.items[]|select(.metadata.name==$p and .status.phase=="Bound")|.spec.volumeName' <<<"$pvcs")
  k get pv "$primary_pv" -o json >"$TMP/$pvc-source-pv.json"
  primary_disk_id=$(jq -er .spec.csi.volumeHandle "$TMP/$pvc-source-pv.json")
  jq -e --arg n "$snap" --arg p "$NS" --arg uid "$(jq -er .metadata.uid <<<"$vs")" --arg disk "${primary_disk_id,,}" '.spec.deletionPolicy=="Retain" and .spec.driver=="disk.csi.azure.com" and .spec.volumeSnapshotRef.kind=="VolumeSnapshot" and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$p and .spec.volumeSnapshotRef.uid==$uid and ((.spec.source.volumeHandle|ascii_downcase)==$disk) and .status.readyToUse==true' <<<"$content" >/dev/null || die "Backup snapshot content does not retain the exact primary source disk/VolumeSnapshot UID."
  snap_id=$(jq -er '.status.snapshotHandle' <<<"$content")
  expected_id=$(jq -s -er --arg n "$snap" '.[]|select(.volumeSnapshot==$n)|.id' "$SNAPSHOT_RECORDS")
  [[ "${snap_id,,}" == "${expected_id,,}" ]] || die "Backup VolumeSnapshot $snap no longer resolves to its recorded ARM snapshot."
  jq -e --arg id "${snap_id,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$l) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-vacuum-backup"' <<<"$(azc snapshot show --ids "$snap_id" -o json)" >/dev/null || die "Paired backup ARM snapshot metadata mismatch."
done

# Confirm completed snapshot-recovery job provenance and the replica's two
# restored PVCs. These are get/list operations only.
jobs=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e 'all(.items[]; (.status.active // 0)==0)' <<<"$jobs" >/dev/null || die "A CNPG job is still active and may contend with storage measurement."
job_count=$(jq -er --arg i "$C-2" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery")]|length' <<<"$jobs")
if [[ "$job_count" == 1 ]]; then
  job=$(jq -ce --arg i "$C-2" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery")]|.[0]' <<<"$jobs")
  jq -e --arg i "$SYSIMAGE" '.status.succeeded>=1 and (.status.active // 0)==0 and ((.status.failed // 0)==0) and (.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.image)==$i and (([.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.command[]]|join(" "))|contains("restoresnapshot"))' <<<"$job" >/dev/null || die "Replica snapshot-recovery job is not completed on the pinned system image."
  job_uid=$(jq -er .metadata.uid <<<"$job")
  job_evidence=$(jq -cn --arg uid "$job_uid" --arg image "$SYSIMAGE" '{source:"live-job",uid:$uid,image:$image,commandContainsRestoreSnapshot:true}')
elif [[ "$job_count" == 0 ]]; then
  # CNPG's TTL controller garbage-collected the successful Job. Accept only
  # this baseline Job's retained Kubernetes completion Event, pinned to the
  # UID captured in the earlier watch/failed prepare attempt. The archived
  # attempt script independently required the exact system image and command.
  RECOVERY_JOB_UID=b221c328-954a-4699-8424-bf177e8b9400
  archived_job_script="$SD/attempts/prepare-2/02-prepare.sh"
  [[ -s "$archived_job_script" ]] || die "Replica Job is absent and its validated prior attempt script is missing."
  rg -Fq "SYSIMAGE='$SYSIMAGE'" "$archived_job_script" || die "Archived baseline attempt did not pin the replica recovery system image."
  rg -Fq 'contains("restoresnapshot")' "$archived_job_script" || die "Archived baseline attempt did not validate the restoresnapshot command."
  replica_pod_created=$(jq -er --arg n "$C-2" '[.items[]|select(.metadata.name==$n)|.metadata.creationTimestamp]|if length==1 then .[0] else error("missing or duplicate replica pod") end' <<<"$pods") || die "Cannot determine current replica pod creation time."
  latest_replica_pvc_created=$(jq -er --arg d "$C-2" --arg w "$C-2-wal" '[.items[]|select(.metadata.name==$d or .metadata.name==$w)|.metadata.creationTimestamp]|if length==2 and all(.[];type=="string") then max else error("missing replica PVC creation times") end' <<<"$pvcs") || die "Cannot determine both replica PVC creation times."
  events=$(k get events -n "$NS" -o json) || die "Cannot inspect namespace Events for the completed replica recovery Job."
  job_event=$(jq -ce --arg n "$C-2-snapshot-recovery" --arg uid "$RECOVERY_JOB_UID" --arg after "$latest_replica_pvc_created" --arg before "$replica_pod_created" '[.items[]|select(.involvedObject.kind=="Job" and .involvedObject.name==$n and .involvedObject.uid==$uid and .reason=="Completed" and .message=="Job completed" and ((.lastTimestamp // .eventTime // .firstTimestamp // "") as $ts | ($ts >= $after and $ts < $before)))]|if length==1 then .[0] else error("expected one pinned completion Event after both replica PVCs and before the current replica pod") end' <<<"$events") || die "Replica recovery Job is absent and no matching retained completion Event proves its successful completion."
  job_uid=$RECOVERY_JOB_UID
  job_evidence=$(jq -cn --arg uid "$job_uid" --arg ts "$(jq -er '.lastTimestamp // .eventTime // .firstTimestamp' <<<"$job_event")" --arg pvc "$latest_replica_pvc_created" --arg pod "$replica_pod_created" --arg archived "$archived_job_script" '{source:"completed-event-after-job-gc",uid:$uid,reason:"Completed",message:"Job completed",eventTimestamp:$ts,replicaPVCsCreatedBy:$pvc,currentReplicaPodCreatedAt:$pod,archivedValidatedAttemptScript:$archived,archivedScriptPinnedSystemImage:true,archivedScriptCheckedRestoreSnapshotCommand:true}')
else
  die "Found $job_count replica snapshot-recovery Jobs; expected at most one."
fi
[[ -n "$job_uid" ]] || die "Replica snapshot-recovery job UID is missing."
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg n "$C-2" --arg d "$DATA" --arg w "$WAL" '[.items[]|select((.metadata.name==$n and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d) or (.metadata.name==($n+"-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w))]|length==2' <<<"$pvcs" >/dev/null || die "Replica PVCs are not bound to the exact backup data/WAL snapshots."

# Existing snapshot reads can saturate disks. Wait for every baseline-owned
# retained snapshot and all four actual disks before the parent releases the
# measurement barrier. Failed snapshots remain untouched and are never counted
# as active; they stop this finalizer with their failure state recorded.
locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die "Cannot inspect baseline snapshot retention locks."
snapshot_list=$(azc snapshot list --resource-group "$SNAP_RG" -o json) || die "Cannot inventory baseline retained snapshots."
mapfile -t OWNED_SNAPSHOT_IDS < <(jq -r --arg t "$TOKEN" '.[]|select(.tags.reprotoken==$t)|.id' <<<"$snapshot_list")
(( ${#OWNED_SNAPSHOT_IDS[@]} >= 3 )) || die "Expected source-copy plus paired backup snapshots in the owned snapshot group."
contains_id(){ local id="$1"; jq -e --arg id "${id,,}" 'any(.[]; ((.id|ascii_downcase)==$id))' <<<"$snapshot_list" >/dev/null; }
contains_id "$COPY" || die "Owned restore copy is missing from snapshot inventory."
DATA_SNAPSHOT_ID=$(jq -s -er --arg n "$DATA" '.[]|select(.volumeSnapshot==$n)|.id' "$SNAPSHOT_RECORDS")
WAL_SNAPSHOT_ID=$(jq -s -er --arg n "$WAL" '.[]|select(.volumeSnapshot==$n)|.id' "$SNAPSHOT_RECORDS")
[[ "${DATA_SNAPSHOT_ID,,}" != "${WAL_SNAPSHOT_ID,,}" ]] || die "Backup DATA and WAL snapshots have the same Azure ID."
contains_id "$DATA_SNAPSHOT_ID" || die "Recorded DATA backup snapshot is missing from retained inventory."
contains_id "$WAL_SNAPSHOT_ID" || die "Recorded WAL backup snapshot is missing from retained inventory."
if [[ "$PREFLIGHT_ONLY" == true ]]; then
  echo "Read-only baseline preflight passed through retained-snapshot inventory and replica recovery provenance ($job_uid via $(jq -r .source <<<"$job_evidence")); stopping before snapshot/disk hydration waits."
  exit 0
fi

wait_retained_snapshot(){
  local id="$1" outfile="$2" end=$((SECONDS+86400)) now last=0 state access option percent name
  name=${id##*/}
  while ((SECONDS<end)); do
    azc snapshot show --ids "$id" -o json >"$outfile" || die "Cannot inspect retained snapshot $name."
    state=$(jq -r '.provisioningState // ""' "$outfile"); access=$(jq -r '.snapshotAccessState // ""' "$outfile")
    option=$(jq -r '.creationData.createOption // ""' "$outfile")
    percent=$(jq -r 'if .completionPercent==null then "null" else ((.completionPercent|tonumber?) as $p | if $p==null then "invalid" elif $p==100 then "100" else ($p|tostring) end) end' "$outfile")
    [[ "$state" != Failed ]] || die "Retained snapshot $name reached terminal Failed; it remains retained and was not changed."
    case "$option" in
      CopyStart) [[ "$percent" == 100 && "$state" == Succeeded && "$access" == Available ]] && break ;;
      Copy) [[ ( "$percent" == null || "$percent" == 100 ) && "$state" == Succeeded && "$access" == Available ]] && break ;;
      *) die "Retained snapshot $name has unsupported createOption=$option." ;;
    esac
    now=$SECONDS
    if ((now-last>=900)); then echo "Waiting for owned snapshot $name (state=$state, access=$access, createOption=$option, completionPercent=$percent)."; last=$now; fi
    sleep 20
  done
  ((SECONDS<end)) || die "Retained snapshot $name did not hydrate within 24 hours."
  jq -e --arg id "${id,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$l) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and .incremental==true and .tags.reprotoken==$t and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS")' "$outfile" >/dev/null || die "Retained snapshot $name ownership/readiness/size metadata mismatch."
  purpose=$(jq -er .tags.reproPurpose "$outfile")
  case "$purpose" in
    premiumv2-keeper-copy)
      jq -e --arg src "${SNAP,,}" '((.creationData.sourceResourceId|ascii_downcase)==$src) and ((.tags.sourceSnapshotId|ascii_downcase)==$src)' "$outfile" >/dev/null || die "Keeper copy $name source lineage mismatch."
      ;;
    premiumv2-vacuum-backup) ;;
    *) die "Unexpected purpose for owned retained snapshot $name: $purpose" ;;
  esac
  jq -e --arg id "${id,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die "Retained snapshot $name lacks a direct CanNotDelete lock."
}
for ((i=0;i<${#OWNED_SNAPSHOT_IDS[@]};i++)); do wait_retained_snapshot "${OWNED_SNAPSHOT_IDS[$i]}" "$TMP/snapshot-$i.json"; done

wait_premiumv2_hydrated(){
  local claim="$1" disk="$2" outfile="$3" end=$((SECONDS+86400)) now last=0 state option percent
  echo "Waiting for $claim Premium SSD v2 disk copy hydration before baseline preparation completes."
  while ((SECONDS<end)); do
    azc disk show --ids "$disk" -o json >"$outfile" || die "Cannot inspect $claim disk hydration."
    state=$(jq -r '.provisioningState // ""' "$outfile")
    [[ "$state" != Failed ]] || die "$claim disk reached terminal provisioningState Failed."
    option=$(jq -r '.creationData.createOption // ""' "$outfile")
    percent=$(jq -r 'if .completionPercent==null then "null" else ((.completionPercent|tonumber?) as $p | if $p==null then "invalid" elif $p==100 then "100" else ($p|tostring) end) end' "$outfile")
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
    if ((now-last>=900)); then echo "$claim disk hydration pending (state=$state, createOption=$option, completionPercent=$percent)."; last=$now; fi
    sleep 20
  done
  die "$claim disk did not finish Copy/CopyStart hydration within 24 hours."
}

# Image and configuration checks intentionally precede disk waits, as the
# replica may be ready while the snapshot-backed PremiumV2 disk is still at 0%.
disks=''
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do
  pv=$(jq -er --arg n "$claim" '.items[]|select(.metadata.name==$n and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-premiumv2")|.spec.volumeName' <<<"$pvcs")
  k get pv "$pv" -o json >"$TMP/$claim-pv.json"
  disk=$(jq -er .spec.csi.volumeHandle "$TMP/$claim-pv.json")
  wait_premiumv2_hydrated "$claim" "$disk" "$TMP/$claim-disk.json"
  jq -e --arg rg "${NODE_RG,,}" --arg l "${LOC,,}" --arg z "$AZURE_ZONE" --arg id "${disk,,}" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$l) and .sku.name=="PremiumV2_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .diskIOPSReadWrite==3000 and .diskMBpsReadWrite==125 and .provisioningState=="Succeeded" and .managedBy!=null and ((.creationData.createOption // "") as $option | ((.completionPercent|tonumber?) // null) as $percent | if $option=="Empty" then ($percent==null or $percent==100) elif ($option=="Copy" or $option=="CopyStart") then $percent==100 else false end) and ((.zones // [])|map(tostring)|index($z))!=null and ((.logicalSectorSize // .creationData.logicalSectorSize // 0)|tostring)=="512"' "$TMP/$claim-disk.json" >/dev/null || die "$claim Premium SSD v2 SKU, performance, zone, sector, ownership, or hydration check failed."
  disks+="$disk"$'\n'
done
[[ $(sort -fu <<<"$disks" | sed '/^$/d' | wc -l) == 4 ]] || die "Expected four distinct hydrated data/WAL disks."

# Confirm the replica is streaming and fully replayed through the primary's
# current flush position, without changing either server.
flush_lsn=$(psql "$C-1" 'SELECT pg_current_wal_flush_lsn();')
end=$((SECONDS+3600)); caught=''
while ((SECONDS<end)); do
  streaming=$(psql "$C-1" "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;" 2>/dev/null || true)
  caught=$(psql "$C-2" "SELECT pg_last_wal_replay_lsn()>='$flush_lsn'::pg_lsn;" 2>/dev/null || true)
  [[ "$streaming" == 1 && "$caught" == t ]] && break
  sleep 5
done
[[ "$streaming" == 1 && "$caught" == t ]] || die "Replica did not catch up through primary flush LSN $flush_lsn."

FINAL_KIND=incremental_copy
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
AUDIT="$SD/recovery/baseline-finalize-audit.json"
PREPARE="$SD/prepare.json"
audit_tmp=$(mktemp "$SD/recovery/.baseline-audit.XXXXXX")
jq -n --arg at "$NOW" --arg script "$0" --arg sub "$SUB" --arg rg "$RG" --arg node "$NODE_RG" --arg snaprg "$SNAP_RG" --arg copy "$COPY" --arg src "$SNAP" --arg backup "$BK" --arg data "$DATA" --arg wal "$WAL" --arg jobuid "$job_uid" --argjson jobEvidence "$job_evidence" --arg flush "$flush_lsn" --arg token "$TOKEN" --arg i "$IMAGE" --argjson snapshots "$(jq -s '.' "$TMP"/snapshot-*.json)" --argjson disks "$(jq -s '[.[]|{id,sku,sizeGiB:(.diskSizeGB // .diskSizeGb),iops:.diskIOPSReadWrite,throughputMBps:.diskMBpsReadWrite,completionPercent,createOption:.creationData.createOption,sector:(.logicalSectorSize // .creationData.logicalSectorSize),zones,provisioningState}]' "$TMP"/*-disk.json)" '{version:1,status:"complete",verifiedAtUtc:$at,finalizerScript:$script,ownerToken:$token,subscriptionId:$sub,resourceGroup:$rg,nodeResourceGroup:$node,snapshotResourceGroup:$snaprg,sourceSnapshotId:$src,restoreCopySnapshotId:$copy,backup:$backup,replicaSnapshots:{data:$data,wal:$wal},snapshotRecoveryJobUid:$jobuid,snapshotRecoveryJobEvidence:$jobEvidence,replicaCaughtUpToLsn:$flush,retainedSnapshots:$snapshots,hydratedPremiumV2Disks:$disks,archiveSecretAbsent:true,cluster:"repro/hdd-pg",instances:2,measurementImage:$i}' >"$audit_tmp"
chmod 600 "$audit_tmp"; mv -f "$audit_tmp" "$AUDIT"
jq -n --arg id "$SNAP" --arg kind "$FINAL_KIND" --arg s "$SYSID" --arg replay "$flush_lsn" --argjson estimate "$VACUUM_ESTIMATE_MIN" --argjson h "$HEAP" --argjson t "$TOTAL" --argjson sourceHeap "$SOURCE_HEAP" --argjson sourceTotal "$SOURCE_TOTAL" --argjson targetGiB "$TEST_HEAP_GIB" --argjson targetBytes "$TARGET_HEAP_BYTES" --argjson live "$FIXTURE_LIVE" --argjson dead "$FIXTURE_DEAD" --arg copy "$COPY" --arg b "$BK" --arg d "$DATA" --arg w "$WAL" --arg i "$IMAGE" --arg zone "$AZURE_ZONE" --arg snaprg "$SNAP_RG" --arg audit "$AUDIT" --arg verified "$NOW" '{version:1,status:"complete",sourceSnapshotId:$id,sourceKind:$kind,systemIdentifier:$s,sourceSnapshotHeapBytes:$sourceHeap,sourceSnapshotTableBytes:$sourceTotal,fixtureTargetHeapGiB:$targetGiB,fixtureTargetHeapBytes:$targetBytes,heapBytes:$h,tableBytes:$t,estimatedVacuumMinutes:$estimate,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$dead,replicaCaughtUpToLsn:$replay,restoreCopySnapshotId:$copy,cluster:"repro/hdd-pg",instances:2,availabilityZone:$zone,retainedSnapshotResourceGroup:$snaprg,storage:"four separate 512-GiB PremiumV2_LRS data/WAL disks at 3000 IOPS / 125 MB/s",backup:$b,replicaSnapshots:{data:$d,wal:$w},measurementImage:$i,clientCreated:false,finalAudit:{verifiedAtUtc:$verified,receipt:$audit,archiveSecretAbsent:true,ownedRetentionSnapshotsReady:true,allFourPremiumV2DisksHydrated:true}}' >"$TMP/prepare.json"
chmod 600 "$TMP/prepare.json"
state_tmp=$(mktemp "$SD/.state.finalized.XXXXXX")
jq --arg at "$NOW" --arg audit "$AUDIT" --arg prep "$PREPARE" '.preparationFinalAudit={status:"complete",verifiedAtUtc:$at,auditReceipt:$audit,prepareReceipt:$prep,readOnlyRemoteAudit:true}' "$STATE" >"$state_tmp"
chmod 600 "$state_tmp"
mv -f "$TMP/prepare.json" "$PREPARE"
mv -f "$state_tmp" "$STATE"
echo "Baseline final audit passed: 23-GiB fixture, paired backup and replica seed, exact measurement image/settings, snapshot retention, and four fully hydrated Premium SSD v2 disks. Local receipts: $PREPARE and $AUDIT"
