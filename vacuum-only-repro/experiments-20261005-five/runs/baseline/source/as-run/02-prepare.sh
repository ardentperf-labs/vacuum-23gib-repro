#!/usr/bin/env bash
# Restore keeper -> separate data/WAL -> CNPG snapshot-seeded replica.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")" && pwd)"; SD="$ROOT/.state"; STATE="$SD/state.json"
NS=repro; C=hdd-pg; SC=azure-standard-hdd; SECRET=azure-barman-credentials
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
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die "Bad owner token."
ST="$SD/storage.json"; jq -e --arg t "$TOKEN" --arg s "$SUB" --arg rg "$RG" '.status=="ready" and .ownerToken==$t and .subscriptionId==$s and .resourceGroup==$rg and .publicAccess==null' "$ST" >/dev/null || die "Storage metadata ownership mismatch."
ACCOUNT=$(jq -er .accountName "$ST"); AID=$(jq -er .accountId "$ST"); CONTAINER=$(jq -er .containerName "$ST")
EXPECTED_AID="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$ACCOUNT"
[[ "${AID,,}" == "${EXPECTED_AID,,}" ]] || die "Storage account metadata ID is outside this stack."
SHORT=$(cut -c1-8 <<<"$TOKEN"); RUN=$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/02-prepare.lock"; flock -n 9 || die "Prepare already active."
TMP=$(mktemp -d "$SD/.prepare.XXXXXX"); chmod 700 "$TMP"
trap 'rc=$?; rm -rf -- "$TMP"; ((rc==0)) || echo "Preparation stopped; inspect stack state and retry 04-teardown.sh after review." >&2' EXIT
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" ]] || KCFG="$SD/kubeconfig"
[[ "$KCFG" == /* ]] || KCFG="$ROOT/$KCFG"; [[ -f "$KCFG" ]] || die "Kubeconfig missing."
export KUBECONFIG="$KCFG"
azc(){ timeout 240s az "$@" --subscription "$SUB"; }; azlong(){ timeout 1800s az "$@" --subscription "$SUB"; }
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
absent(){ local what="$1"; shift; if "$@" >/dev/null 2>"$TMP/absent.err"; then die "$what already exists."; fi; grep -Eiq 'ResourceNotFound|SnapshotNotFound|DiskNotFound|NotFound|not found|could not be found' "$TMP/absent.err" || { cat "$TMP/absent.err" >&2; die "Cannot prove $what absent."; }; }
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
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="None"' < <(k get storageclass "$SC" -o json) >/dev/null || die "HDD class must use uncached Standard_LRS."
for crd in volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io; do k get crd "$crd" >/dev/null || die "Missing $crd"; done

META="$ROOT/snapshot.json"; SNAP=$(jq -r '.snapshotId // empty' "$META" 2>/dev/null || true)
SYSID=$(jq -r '.systemIdentifier // empty' "$META" 2>/dev/null || true); HEAP=$(jq -r '.heapBytes // empty' "$META" 2>/dev/null || true); TOTAL=$(jq -r '.tableBytes // empty' "$META" 2>/dev/null || true)
KEEPER_LOC=$(jq -r '.location // empty' "$META" 2>/dev/null || true); KIND=keeper
if [[ -n "$SNAP" ]]; then
 jq -e --arg s "$SUB" --arg id "${SNAP,,}" --arg rg "$(jq -er .resourceGroup "$META")" --arg sys "$SYSID" --argjson heap "$HEAP" --argjson total "$TOTAL" '(.subscriptionId==$s) and ((.snapshotId|ascii_downcase)==$id) and (.resourceGroup|ascii_downcase)==($rg|ascii_downcase) and ((.systemIdentifier|tostring)==$sys) and .heapBytes==$heap and .tableBytes==$total and .sourceTopology=="combined PGDATA/pg_wal"' "$META" >/dev/null || die "Pinned keeper metadata mismatch."
 if source=$(azc snapshot show --ids "$SNAP" -o json 2>"$TMP/source.err"); then
  jq -e --arg id "${SNAP,,}" --arg t "$(jq -er .keeperOwnerToken "$META")" --arg l "$KEEPER_LOC" '((.id|ascii_downcase)==$id) and .provisioningState=="Succeeded" and (.diskSizeGB // .diskSizeGb)==512 and .location==$l and .sku.name=="Standard_LRS" and .incremental==false and .tags.reprotoken==$t and .tags.reproPurpose=="retained-primary-pg-snapshot"' <<<"$source" >/dev/null || die "Keeper snapshot owner/size check failed."
 else grep -Eiq 'SnapshotNotFound|ResourceNotFound|could not be found|not found' "$TMP/source.err" || { cat "$TMP/source.err" >&2; die "Snapshot read failed; no fallback on non-404."; }; KIND=fresh; SNAP=; fi
else KIND=fresh; fi

# If quota forces another region, derive a temporary incremental copy; 04 removes the whole stack RG.
if [[ "$KIND" == keeper && -n "${SOURCE_SNAPSHOT_ID:-}" ]]; then
 [[ "${SOURCE_SNAPSHOT_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${RG,,}/providers/microsoft.compute/snapshots/"* ]] || die "SOURCE_SNAPSHOT_ID must be in this subscription's owned stack RG."
 source=$(azc snapshot show --ids "$SOURCE_SNAPSHOT_ID" -o json) || die "Cannot read regional source snapshot."
 jq -e --arg id "${SOURCE_SNAPSHOT_ID,,}" --arg t "$TOKEN" --arg l "$LOC" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==($l|ascii_downcase)) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .completionPercent==100 and .tags.reprotoken==$t and .tags.reproPurpose=="regional-prepared-source"' <<<"$source" >/dev/null || die "Regional source identity or readiness check failed."
 SNAP="$SOURCE_SNAPSHOT_ID"; KIND=regional_copy
elif [[ "$KIND" == keeper && "${KEEPER_LOC,,}" != "${LOC,,}" ]]; then
 DISK_ID="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Compute/disks/keeper-stage-$SHORT"
 BASE_ID="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Compute/snapshots/keeper-base-$SHORT"
 REGIONAL="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Compute/snapshots/keeper-$SHORT"
 absent 'regional staging disk' azc disk show -g "$RG" -n "keeper-stage-$SHORT" -o json
 absent 'regional base snapshot' azc snapshot show --ids "$BASE_ID" -o json
 absent 'regional target snapshot' azc snapshot show --ids "$REGIONAL" -o json
 azlong disk create -g "$RG" -n "keeper-stage-$SHORT" -l "$KEEPER_LOC" --source "$SNAP" --sku Standard_LRS --size-gb 512 --tags "reprotoken=$TOKEN" reproPurpose=regional-snapshot-stage --no-wait -o none
 end=$((SECONDS+7200)); until disk=$(azc disk show -g "$RG" -n "keeper-stage-$SHORT" -o json 2>/dev/null) && [[ $(jq -r .provisioningState <<<"$disk") == Succeeded ]]; do ((SECONDS<end)) || die "Regional staging disk timed out."; sleep 15; done
 jq -e --arg id "${DISK_ID,,}" --arg src "${SNAP,,}" --arg t "$TOKEN" --arg l "${KEEPER_LOC,,}" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and ((.location|ascii_downcase)==$l) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .tags.reprotoken==$t and .tags.reproPurpose=="regional-snapshot-stage"' <<<"$disk" >/dev/null || die "Regional staging disk mismatch."
 azlong snapshot create -g "$RG" -n "keeper-base-$SHORT" -l "$KEEPER_LOC" --source "$DISK_ID" --sku Standard_LRS --incremental true --tags "reprotoken=$TOKEN" reproPurpose=regional-snapshot-base --no-wait -o none
 end=$((SECONDS+7200)); until base=$(azc snapshot show --ids "$BASE_ID" -o json 2>/dev/null) && jq -e '.provisioningState=="Succeeded" and (.completionPercent // 100)==100' <<<"$base" >/dev/null; do ((SECONDS<end)) || die "Regional base snapshot timed out."; sleep 15; done
 jq -e --arg id "${BASE_ID,,}" --arg disk "${DISK_ID,,}" --arg t "$TOKEN" --arg l "${KEEPER_LOC,,}" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$disk) and ((.location|ascii_downcase)==$l) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose=="regional-snapshot-base"' <<<"$base" >/dev/null || die "Regional base snapshot mismatch."
 azlong snapshot create -g "$RG" -n "keeper-$SHORT" -l "$LOC" --source "$BASE_ID" --sku Standard_LRS --incremental true --copy-start --tags "reprotoken=$TOKEN" reproPurpose=regional-prepared-source --no-wait -o none
 end=$((SECONDS+14400)); until regional=$(azc snapshot show --ids "$REGIONAL" -o json 2>/dev/null) && jq -e '.provisioningState=="Succeeded" and (.completionPercent // 0)==100' <<<"$regional" >/dev/null; do ((SECONDS<end)) || die "Regional snapshot copy timed out."; sleep 20; done
 jq -e --arg id "${REGIONAL,,}" --arg src "${BASE_ID,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and ((.location|ascii_downcase)==$l) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==true and .completionPercent==100 and .snapshotAccessState=="Available" and .tags.reprotoken==$t and .tags.reproPurpose=="regional-prepared-source"' <<<"$regional" >/dev/null || die "Regional snapshot copy mismatch."
 SNAP="$REGIONAL"; KIND=regional_copy
fi
# Missing-source fallback is slow and not live-validated: make a 110-GiB heap,
# delete half its rows, checkpoint, and retain the resulting owned disk snapshot.
if [[ "$KIND" == fresh ]]; then
 SEED="$C-seed"; SRG="rg-vacuum-source-$SHORT"; SNAME="prepared-$SHORT"
 [[ $(azc group exists -n "$SRG" -o tsv) == false ]] || die "Fallback RG exists."
 absent 'fresh seed Cluster' k get cluster "$SEED" -n "$NS" -o json
 SNAP="/subscriptions/$SUB/resourceGroups/$SRG/providers/Microsoft.Compute/snapshots/$SNAME"
 jq -n --arg id "$SNAP" --arg rg "$SRG" --arg t "$TOKEN" --arg s "$SUB" --arg l "$LOC" '{version:1,status:"creating",id:$id,resourceGroup:$rg,ownerToken:$t,subscriptionId:$s,location:$l,sourceDiskId:null,createdBy:"vacuum-only-repro/02-prepare.sh"}' >"$SD/ownedSnapshot.json"
 azc group create -n "$SRG" -l "$LOC" --tags managed-by=vacuum-only-repro reproPurpose=retained-source-snapshot "reprotoken=$TOKEN" -o none >/dev/null
 jq -e --arg t "$TOKEN" --arg n "$SRG" --arg l "$LOC" '.name==$n and .location==$l and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro" and .tags.reproPurpose=="retained-source-snapshot"' < <(azc group show -n "$SRG" -o json) >/dev/null || die "Fallback snapshot RG ownership mismatch."
 cat >"$TMP/seed.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: $SEED, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec:
  instances: 1
  imageName: $SYSIMAGE
  storage: {storageClass: $SC, size: 512Gi}
  bootstrap: {initdb: {database: repro, owner: app}}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "2", memory: 3Gi}}
EOF
 k apply -f "$TMP/seed.yaml" >/dev/null
 end=$((SECONDS+10800)); while :; do c=$(k get cluster "$SEED" -n "$NS" -o json 2>/dev/null || true); [[ -n "$c" && $(jq -r '.status.readyInstances // 0' <<<"$c") == 1 ]] && break; ((SECONDS<end)) || die "Seed primary timed out."; sleep 10; done
 POD=$(jq -r .status.currentPrimary <<<"$c")
 psql_long "$POD" "CREATE EXTENSION pgcrypto; CREATE TABLE public.vacuum_target(id bigint,payload bytea); ALTER TABLE public.vacuum_target ALTER COLUMN payload SET STORAGE PLAIN; ALTER TABLE public.vacuum_target SET (autovacuum_enabled=false); CREATE PROCEDURE public.seed_target(n bigint) LANGUAGE plpgsql AS \$\$ DECLARE i bigint:=1; BEGIN LOOP INSERT INTO public.vacuum_target SELECT g,gen_random_bytes(1024)||gen_random_bytes(512) FROM generate_series(i,i+99999) g; i:=i+100000; COMMIT; EXIT WHEN pg_relation_size('public.vacuum_target')>=n; END LOOP; END \$\$;"
 psql_long "$POD" 'CALL public.seed_target(110::bigint*1024*1024*1024); CREATE INDEX vacuum_target_id_idx ON public.vacuum_target(id);'
 deleted=$(psql_long "$POD" 'DELETE FROM public.vacuum_target WHERE id%2=0;'); dead=$(awk '/DELETE/{print $2}' <<<"$deleted")
 psql_long "$POD" 'CHECKPOINT;'
 SQL="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'maxId',(SELECT max(id) FROM public.vacuum_target),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass))::text;"
 fresh=$(psql "$POD" "$SQL")
 SYSID=$(jq -r .systemIdentifier <<<"$fresh"); HEAP=$(jq -r .heapBytes <<<"$fresh"); TOTAL=$(jq -r .tableBytes <<<"$fresh"); maxid=$(jq -r .maxId <<<"$fresh")
 [[ "$HEAP" -ge 118111600640 && "$dead" -gt 0 && $(jq -r .tableAutovacuumDisabled <<<"$fresh") == true ]] && ((dead*100/maxid>=40)) || die "Seed heap/dead-row check failed."
 pv=$(k get pvc "$SEED-1" -n "$NS" -o json|jq -er '.spec.volumeName'); disk=$(k get pv "$pv" -o json|jq -er .spec.csi.volumeHandle)
 d=$(azc disk show --ids "$disk" -o json) || die "Cannot inspect fresh seed disk."
 jq -e --arg id "${disk,,}" --arg rg "${NODE_RG,,}" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$l) and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .managedBy!=null' <<<"$d" >/dev/null || die "Fresh seed disk mismatch."
 jq --arg d "$disk" '.sourceDiskId=$d' "$SD/ownedSnapshot.json" >"$SD/ownedSnapshot.tmp"; mv "$SD/ownedSnapshot.tmp" "$SD/ownedSnapshot.json"
 azlong snapshot create -g "$SRG" -n "$SNAME" -l "$LOC" --source "$disk" --sku Standard_LRS --incremental false --tags "reprotoken=$TOKEN" reproPurpose=retained-primary-pg-snapshot --no-wait -o none
 end=$((SECONDS+14400)); until source=$(azc snapshot show --ids "$SNAP" -o json 2>/dev/null) && [[ $(jq -r .provisioningState <<<"$source") == Succeeded && $(jq -r '.snapshotAccessState // "Available"' <<<"$source") == Available ]]; do ((SECONDS<end)) || die "Source snapshot timed out."; sleep 15; done
 jq -e --arg id "${SNAP,,}" --arg disk "${disk,,}" --arg t "$TOKEN" --arg l "$LOC" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$disk) and ((.location|ascii_downcase)==($l|ascii_downcase)) and .provisioningState=="Succeeded" and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==false and .snapshotAccessState=="Available" and .tags.reprotoken==$t and .tags.reproPurpose=="retained-primary-pg-snapshot"' <<<"$source" >/dev/null || die "Prepared source snapshot did not verify."
 jq '.status="retained"' "$SD/ownedSnapshot.json" >"$SD/ownedSnapshot.tmp"; mv "$SD/ownedSnapshot.tmp" "$SD/ownedSnapshot.json"
 live=$((maxid-dead))
 jq -n --arg id "$SNAP" --arg s "$SYSID" --argjson h "$HEAP" --argjson b "$TOTAL" --argjson l "$live" --argjson d "$dead" '{version:1,status:"complete",sourceSnapshotId:$id,sourceKind:"fresh-initdb-seed",systemIdentifier:$s,heapBytes:$h,tableBytes:$b,preparedLiveTuplesEstimate:$l,preparedDeadTuplesEstimate:$d}' >"$SD/prepared.json"
 [[ ! -f "$META" ]] || cp -p "$META" "$SD/replacedSnapshot.json"
 jq -n --arg id "$SNAP" --arg s "$SUB" --arg rg "$SRG" --arg owner "$TOKEN" --arg l "$LOC" --arg sys "$SYSID" --argjson h "$HEAP" --argjson b "$TOTAL" --argjson live "$live" --argjson d "$dead" '{version:1,snapshotId:$id,subscriptionId:$s,resourceGroup:$rg,keeperOwnerToken:$owner,location:$l,diskSizeGiB:512,systemIdentifier:$sys,heapBytes:$h,tableBytes:$b,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$d,sourceTopology:"combined PGDATA/pg_wal",consistency:"online primary disk snapshot after checkpoint; crash-consistent",createdAtUtc:(now|todateiso8601)}' >"$META.tmp"
 chmod 600 "$META.tmp"; mv -f "$META.tmp" "$META"
 klong delete cluster "$SEED" -n "$NS" --wait=true --timeout=10m >/dev/null
fi

# Copy to the AKS node RG for CSI access; the keeper is always read-only.
COPY="/subscriptions/$SUB/resourceGroups/$NODE_RG/providers/Microsoft.Compute/snapshots/vacuum-copy-$SHORT"
absent 'restore-copy snapshot' azc snapshot show --ids "$COPY" -o json
azlong snapshot create -g "$NODE_RG" -n "vacuum-copy-$SHORT" -l "$LOC" --source "$SNAP" --sku Standard_LRS --incremental false --tags "reprotoken=$TOKEN" reproPurpose=restore-source-copy "sourceSnapshotId=$SNAP" --no-wait -o none
end=$((SECONDS+14400)); until copy=$(azc snapshot show --ids "$COPY" -o json 2>/dev/null) && [[ $(jq -r .provisioningState <<<"$copy") == Succeeded && $(jq -r '.snapshotAccessState // "Available"' <<<"$copy") == Available ]]; do ((SECONDS<end)) || die "Restore copy timed out."; sleep 15; done
jq -e --arg id "${COPY,,}" --arg t "$TOKEN" --arg src "${SNAP,,}" --arg l "$LOC" '((.id|ascii_downcase)==$id) and ((.creationData.sourceResourceId|ascii_downcase)==$src) and .location==$l and .incremental==false and .sku.name=="Standard_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .tags.reprotoken==$t and .tags.reproPurpose=="restore-source-copy"' <<<"$copy" >/dev/null || die "Restore copy mismatch."
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
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"primaryUpdateMethod":"restart","walStorage":{"storageClass":"azure-standard-hdd","size":"512Gi","resizeInUseVolumes":false}}}' >/dev/null
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
 pv=$(jq -er --arg n "$claim" '.items[]|select(.metadata.name==$n and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-standard-hdd")|.spec.volumeName' "$TMP/pvcs-final.json")
 k get pv "$pv" -o json >"$TMP/$claim-pv.json"; disk=$(jq -er .spec.csi.volumeHandle "$TMP/$claim-pv.json"); azc disk show --ids "$disk" -o json >"$TMP/$claim-disk.json"
 jq -e --arg rg "$(tr '[:upper:]' '[:lower:]' <<<"$NODE_RG")" --arg l "$(tr '[:upper:]' '[:lower:]' <<<"$LOC")" --arg id "$(tr '[:upper:]' '[:lower:]' <<<"$disk")" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$l) and .sku.name=="Standard_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .managedBy!=null' "$TMP/$claim-disk.json" >/dev/null || die "$claim is not attached Standard HDD."
 disks="$disks$disk"$'\n'
done
[[ $(sort -u <<<"$disks"|sed '/^$/d'|wc -l) == 4 ]] || die "Expected four distinct data/WAL disks."
REQUIRED_REPLAY_LSN=$(psql "$C-1" 'SELECT pg_current_wal_flush_lsn();')
wait_replay_lsn "$REQUIRED_REPLAY_LSN"
FINAL_KIND="$KIND"; [[ "$KIND" != fresh ]] || FINAL_KIND="fresh-initdb-seed"
jq -n --arg id "$SNAP" --arg keeper "$(jq -r '.snapshotId // empty' "$META")" --arg kind "$FINAL_KIND" --arg s "$SYSID" --arg replay "$REQUIRED_REPLAY_LSN" --argjson estimate "$VACUUM_ESTIMATE_MIN" --argjson h "$HEAP" --argjson t "$TOTAL" --argjson sourceHeap "$SOURCE_HEAP" --argjson sourceTotal "$SOURCE_TOTAL" --argjson targetGiB "$TEST_HEAP_GIB" --argjson targetBytes "$TARGET_HEAP_BYTES" --argjson live "$FIXTURE_LIVE" --argjson dead "$FIXTURE_DEAD" --arg copy "$COPY" --arg b "$BK" --arg d "$DATA" --arg w "$WAL" --arg i "$IMAGE" '{version:1,status:"complete",sourceSnapshotId:$id,retainedKeeperSnapshotId:$keeper,sourceKind:$kind,systemIdentifier:$s,sourceSnapshotHeapBytes:$sourceHeap,sourceSnapshotTableBytes:$sourceTotal,fixtureTargetHeapGiB:$targetGiB,fixtureTargetHeapBytes:$targetBytes,heapBytes:$h,tableBytes:$t,estimatedVacuumMinutes:$estimate,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$dead,replicaCaughtUpToLsn:$replay,restoreCopySnapshotId:$copy,cluster:"repro/hdd-pg",instances:2,storage:"four separate 512-GiB Standard_LRS data/WAL disks",backup:$b,replicaSnapshots:{data:$d,wal:$w},measurementImage:$i,clientCreated:false}' >"$SD/prepare.json"
chmod 600 "$SD/prepare.json"
echo "Prepared $C from $KIND source with a ${TEST_HEAP_GIB}-GiB half-dead fixture, paired snapshot recovery, four separate data/WAL disks, and replica catch-up through $REQUIRED_REPLAY_LSN (estimated VACUUM ${VACUUM_ESTIMATE_MIN} min); state: $SD/prepare.json"
