#!/usr/bin/env bash
# Recover the retained paired CNPG DATA/WAL source, seed a paired replica, and
# block client creation until all four Premium SSD v2 copies report 100 percent.
set -Eeuo pipefail
umask 077
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
SD="$HERE/.state"; STATE="$SD/state.json"; NS=repro; C=hdd-pg; SC=azure-premiumv2; SECRET=azure-barman-credentials
SOURCE_MANIFEST="$HERE/source-manifest.json"; META="$HERE/snapshot.json"; RUNCFG="$HERE/run-config.json"
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
READER_ROLE='acdd72a7-3385-48ef-bd42-f606fba81ae7'
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
lower(){ tr '[:upper:]' '[:lower:]' <<<"$1" | tr -d '\n'; }
for x in az kubectl jq python3 timeout flock sha256sum base64 awk sort; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die 'Run 01-provision.sh first.'
jq -e '.targetHeapGiB==23 and .location=="eastus" and .zone=="1" and .diskSku=="PremiumV2_LRS" and .diskIops==3000 and .diskMBps==125 and .diskSizeGiB==512 and .nodeVmSize=="Standard_D2s_v4" and .nodeCount==2 and .client.targetTps==5 and .client.connections==10 and .client.synchronousCommit=="on"' "$RUNCFG" >/dev/null || die 'Pinned run configuration changed.'
jq -e '.version==1 and (.sourceSnapshotId|type=="string" and startswith("/subscriptions/")) and (.uniqueId|type=="string" and length>0) and (.ownerToken|type=="string" and test("^[a-f0-9]{32}$")) and (.resourceGroup|type=="string" and length>0) and (.location|type=="string" and length>0) and .incremental==true and .diskSizeGiB==512 and .logicalSectorSize==512 and (.systemIdentifier|tostring)=="7692518806614212629" and (.pairedWAL.sourceSnapshotId|type=="string" and startswith("/subscriptions/")) and (.pairedWAL.uniqueId|type=="string" and length>0) and .backup.phase=="completed" and .backup.method=="volumeSnapshot" and .backup.endLSN=="4E/50000138"' "$SOURCE_MANIFEST" >/dev/null || die 'Pinned retained paired source manifest is invalid.'
jq -e --slurpfile m "$SOURCE_MANIFEST" '.systemIdentifier=="7692518806614212629" and .heapBytes==24696061952 and .tableBytes==25040748544 and .preparedLiveTuplesEstimate==7536640 and .preparedDeadTuplesEstimate==7536640 and .sourceSnapshotId==$m[0].sourceSnapshotId and .pairedWALSnapshotId==$m[0].pairedWAL.sourceSnapshotId' "$META" >/dev/null || die 'Pinned fixture metadata is not the paired 23-GiB source.'
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE"); LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SNAP_RG=$(jq -er .snapshotResourceGroup "$STATE"); AZURE_ZONE=$(jq -er .availabilityZone "$STATE")
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die 'Bad stack token.'
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$NODE_RG" == "rg-vacuum-nodes-$TOKEN" && "$SNAP_RG" == "rg-vacuum-pv2-snap-$TOKEN" && "$(lower "$LOC")" == eastus && "$AZURE_ZONE" == 1 ]] || die 'Saved resource ownership, region, or zone does not match this run.'
SOURCE_ID=$(jq -er .sourceSnapshotId "$SOURCE_MANIFEST"); SOURCE_UID=$(jq -er .uniqueId "$SOURCE_MANIFEST"); SOURCE_RG=$(jq -er .resourceGroup "$SOURCE_MANIFEST"); SOURCE_OWNER=$(jq -er .ownerToken "$SOURCE_MANIFEST")
WAL_ID=$(jq -er .pairedWAL.sourceSnapshotId "$SOURCE_MANIFEST"); WAL_UID=$(jq -er .pairedWAL.uniqueId "$SOURCE_MANIFEST")
SOURCE_BACKUP=$(jq -er .backup.name "$SOURCE_MANIFEST"); SOURCE_BACKUP_UID=$(jq -er .backup.uid "$SOURCE_MANIFEST"); SOURCE_END_LSN=$(jq -er .backup.endLSN "$SOURCE_MANIFEST")
SHORT=$(cut -c1-8 <<<"$TOKEN")
mkdir -p "$SD/recovery"; chmod 700 "$SD" "$SD/recovery"; exec 9>"$SD/02-prepare.lock"; flock -n 9 || die 'Preparation is already active.'
RESP="$SD/recovery/evidence"; EVENTS="$SD/recovery/phase-events.jsonl"
mkdir -p "$RESP"; chmod 700 "$RESP"
TMP=$(mktemp -d "$SD/.prepare.XXXXXX"); chmod 700 "$TMP"; CONNECTION_FILE=''
on_exit(){ local rc=$?; trap - EXIT; [[ -z "$CONNECTION_FILE" ]] || python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$CONNECTION_FILE" 2>/dev/null || true; python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1],ignore_errors=True)' "$TMP" 2>/dev/null || true; if ((rc)); then printf 'Preparation stopped; preserve all snapshots and inspect %s before retrying.\n' "$SD" >&2; fi; exit "$rc"; }
trap on_exit EXIT
event(){ local phase="$1" resource="$2" name="$3" details="$4" at; at=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ); jq -cn --arg at "$at" --arg phase "$phase" --arg resource "$resource" --arg name "$name" --arg details "$details" '{atUtc:$at,phase:$phase,resource:$resource,name:$name,details:$details}' >>"$EVENTS"; chmod 600 "$EVENTS"; }
save_response(){ local phase="$1" resource="$2" name="$3" path="$4" digest stamp out; [[ -s "$path" ]] || die "Cannot preserve empty $resource response for $name."; digest=$(sha256sum "$path" | awk '{print $1}'); stamp=$(date -u +%Y%m%dT%H%M%S.%NZ); out="$RESP/$stamp-$phase-$resource-$name-$digest.json"; cp -- "$path" "$out"; chmod 600 "$out"; event "$phase" "$resource" "$name" "sha256=$digest file=$out"; }
event 'prepare-started' 'run' "$C" "sourceData=$SOURCE_ID sourceWAL=$WAL_ID sourceBackupUid=$SOURCE_BACKUP_UID targetLSN=$SOURCE_END_LSN"
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" ]] || KCFG="$SD/kubeconfig"; [[ "$KCFG" == /* ]] || KCFG="$HERE/$KCFG"; [[ -f "$KCFG" ]] || die 'Kubeconfig missing.'; export KUBECONFIG="$KCFG"
azc(){ timeout --foreground --kill-after=15s 900s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 3600s az "$@" --subscription "$SUB"; }
azdata(){ timeout --foreground --kill-after=15s 900s az storage "$@"; }
k(){ timeout --foreground --kill-after=10s 120s kubectl "$@"; }
klong(){ timeout --foreground --kill-after=10s 1800s kubectl "$@"; }
psql(){ k exec -n "$NS" -c postgres "$1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
wait_ready(){
 local target="$1" end=$((SECONDS+10800)) c pods
 while ((SECONDS<end)); do
  c=$(k get cluster "$C" -n "$NS" -o json 2>/dev/null || true)
  if [[ -n "$c" && $(jq -r '.status.readyInstances // 0' <<<"$c") == "$target" ]]; then
   pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
   jq -e --argjson n "$target" '[.items[]|select(.status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==$n' <<<"$pods" >/dev/null && { printf '%s' "$c"; return 0; }
  fi
  sleep 10
 done
 die "Timed out waiting for $target ready CNPG instance(s)."
}
wait_streaming(){ local end=$((SECONDS+1800)) n; while ((SECONDS<end)); do n=$(psql "$C-1" "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;" 2>/dev/null || true); [[ "$n" == 1 ]] && return; sleep 5; done; die 'Replica did not reach streaming/caught-up state.'; }
wait_replay_lsn(){ local target="$1" end=$((SECONDS+3600)) caught; while ((SECONDS<end)); do caught=$(psql "$C-2" "SELECT pg_last_wal_replay_lsn()>='$target'::pg_lsn;" 2>/dev/null || true); [[ "$caught" == t ]] && return; sleep 5; done; die "Replica did not replay through $target."; }
wait_disk(){
 local claim="$1" disk="$2" output="$3" end=$((SECONDS+86400)) now last=0 option state percent signature last_signature=''
 printf 'Waiting for %s PremiumV2 copy hydration.\n' "$claim"
 while ((SECONDS<end)); do
  azc disk show --ids "$disk" -o json >"$output" || die "Cannot inspect $claim disk."
  state=$(jq -r '.provisioningState // ""' "$output"); [[ "$state" != Failed ]] || die "$claim PremiumV2 disk reached Failed."
  option=$(jq -r '.creationData.createOption // ""' "$output")
  case "$option" in
   Copy|CopyStart)
    signature=$(jq -c '[.provisioningState,.completionPercent,.diskIOPSReadWrite,.diskMBpsReadWrite,.logicalSectorSize]' "$output")
    if [[ "$signature" != "$last_signature" ]]; then
     now=$SECONDS
     if ((now-last>=900)) || jq -e '(.provisioningState=="Succeeded") and (.completionPercent|tonumber?)==100' "$output" >/dev/null; then
      save_response 'disk-hydration-progress' 'azureDisk' "$claim" "$output"; last=$now; last_signature="$signature"
     fi
    fi
    if jq -e '(.provisioningState=="Succeeded") and (.completionPercent|tonumber?)==100' "$output" >/dev/null; then return 0; fi
    percent=$(jq -r 'if .completionPercent==null then "null" else (.completionPercent|tostring) end' "$output")
    ;;
   Empty) die "$claim is Empty; the baseline requires Copy/CopyStart numeric completionPercent 100." ;;
   *) die "$claim has unsupported PremiumV2 createOption=$option." ;;
  esac
  now=$SECONDS; if ((now-last>=900)); then printf '%s copy pending (state=%s, completionPercent=%s).\n' "$claim" "$state" "$percent"; event 'disk-hydration-pending' 'azureDisk' "$claim" "state=$state completionPercent=$percent"; last=$now; fi
  sleep 20
 done
 die "$claim copy did not reach numeric completionPercent 100 within 24 hours."
}
lock_snapshot(){
 local id="$1" name="$2" locks
 azc lock create --name "retain-$name" --lock-type CanNotDelete --resource-group "$SNAP_RG" --resource-name "$name" --resource-type Microsoft.Compute/snapshots --notes 'Retained by the 2026-10-09 pgbench/VACUUM reproduction until explicit user authorization.' -o none || die "Cannot protect new retained snapshot $name."
 locks=$(azc lock list --resource-group "$SNAP_RG" -o json) || die "Cannot inspect retention lock for $name."
 jq -e --arg id "$(lower "$id")" 'any(.[];.level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die "CanNotDelete lock failed to verify for $name."
}
source_lock_check(){
 local id="$1" locks
 locks=$(azc lock list --resource-group "$SOURCE_RG" -o json) || die 'Cannot inspect read-only source snapshot locks.'
 jq -e --arg id "$(lower "$id")" 'any(.[];.level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die "Source snapshot lacks its expected direct CanNotDelete lock: $id"
}
verify_source(){
 local id="$1" uid="$2" path="$3" live
 live=$(azc snapshot show --ids "$id" -o json) || die "Cannot read allowlisted retained source snapshot $id."
 jq -e --arg id "$(lower "$id")" --arg uid "$uid" --arg owner "$SOURCE_OWNER" --arg rg "$(lower "$SOURCE_RG")" '((.id|ascii_downcase)==$id) and .uniqueId==$uid and ((.resourceGroup|ascii_downcase)==$rg) and ((.location|ascii_downcase)=="eastus") and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.completionPercent|tonumber?)==100 and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_ZRS" and .incremental==true and .tags.reprotoken==$owner and .tags.reproPurpose=="premiumv2-vacuum-backup"' <<<"$live" >/dev/null || die "Source snapshot identity/readiness/ownership changed: $id"
 printf '%s\n' "$live" >"$path"
 save_response 'source-snapshot-verified' 'azureSnapshot' "$(basename "$id")" "$path"
 source_lock_check "$id"
}
[[ "$(azc account show --query id -o tsv)" == "$SUB" ]] || die 'Azure subscription mismatch.'
[[ "$(k config current-context)" == *"$AKS"* ]] || die 'Wrong Kubernetes context.'
jq -e --arg token "$TOKEN" '.metadata.labels["repro.stack.token"]==$token' < <(k get namespace "$NS" -o json) >/dev/null || die 'Namespace ownership token mismatch.'
jq -e --arg token "$TOKEN" '.metadata.labels["repro.stack.token"]==$token and (.data["connection-string"]|length>0)' < <(k get secret "$SECRET" -n "$NS" -o json) >/dev/null || die 'WAL archive Secret identity/key check failed.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="PremiumV2_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .parameters.DiskIOPSReadWrite=="3000" and .parameters.DiskMBpsReadWrite=="125" and .parameters.LogicalSectorSize=="512"' < <(k get storageclass "$SC" -o json) >/dev/null || die 'StorageClass differs from the uncached PremiumV2 baseline.'
for crd in volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io; do k get crd "$crd" >/dev/null || die "Missing snapshot CRD $crd"; done
verify_source "$SOURCE_ID" "$SOURCE_UID" "$TMP/source-data-snapshot.json"
verify_source "$WAL_ID" "$WAL_UID" "$TMP/source-wal-snapshot.json"
event 'retained-source-verified' 'snapshotPair' 'data+wal' "systemIdentifier=$(jq -r .systemIdentifier "$META") sourceBackupUid=$SOURCE_BACKUP_UID endLSN=$SOURCE_END_LSN; source locks remain untouched"

# Scope Reader to these two old snapshot IDs only. Never grant access to the old RG.
# Persist exact role-assignment resource IDs as write-ahead intents before any
# create call so teardown can safely distinguish ours from unrelated IAM.
AKS_JSON=$(azc aks show -g "$RG" -n "$AKS" -o json) || die 'Cannot read the new AKS control-plane principal.'
PRINCIPAL=$(jq -er '.identity.principalId' <<<"$AKS_JSON")
[[ "$PRINCIPAL" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'New AKS control-plane principal is invalid.'
ASSIGNMENTS='[]'
for role in data wal; do
 if [[ "$role" == data ]]; then source_id="$SOURCE_ID"; source_uid="$SOURCE_UID"; else source_id="$WAL_ID"; source_uid="$WAL_UID"; fi
 listing=$(azc role assignment list --scope "$source_id" -o json) || die "Cannot inspect exact-scope Reader grants for $source_id."
 exact=$(jq -c --arg p "$(lower "$PRINCIPAL")" --arg s "$(lower "$source_id")" '[.[]|select(((.scope // "")|ascii_downcase)==$s and ((.principalId // "")|ascii_downcase)==$p)]' <<<"$listing")
 old=$(jq -c --arg id "$source_id" '[.sourceSnapshotReaderAssignments.assignments[]? | select(.sourceSnapshotId==$id)]' "$STATE")
 [[ $(jq 'length' <<<"$old") -le 1 ]] || die "Multiple saved role-assignment intents exist for $source_id."
 if [[ $(jq 'length' <<<"$old") == 1 ]]; then
  intent=$(jq -c '.[0]' <<<"$old")
  jq -e --arg t "$TOKEN" '.sourceSnapshotReaderAssignments.ownerToken==$t' "$STATE" >/dev/null || die 'Saved Reader intent owner token does not match this run.'
  jq -e --arg p "$(lower "$PRINCIPAL")" --arg s "$(lower "$source_id")" --arg uid "$source_uid" --arg role "$READER_ROLE" '((.principalId|ascii_downcase)==$p) and ((.scope|ascii_downcase)==$s) and ((.sourceSnapshotId|ascii_downcase)==$s) and .sourceSnapshotUniqueId==$uid and ((.id|ascii_downcase)|startswith(($s+"/providers/microsoft.authorization/roleassignments/"))) and ((.roleDefinitionId|ascii_downcase)|endswith($role)) and (.status=="intent" or .status=="created")' <<<"$intent" >/dev/null || die "Saved Reader intent does not exactly match new stack/source $source_id."
  assignment_id=$(jq -er .id <<<"$intent")
  matches=$(jq -c --arg id "$(lower "$assignment_id")" '[.[]|select((.id|ascii_downcase)==$id)]' <<<"$exact")
  unmatched=$(jq -c --arg id "$(lower "$assignment_id")" '[.[]|select((.id|ascii_downcase)!=$id)]' <<<"$exact")
  [[ $(jq 'length' <<<"$unmatched") == 0 ]] || die "Unrecorded direct Reader grant exists for the new principal on $source_id."
  if [[ $(jq 'length' <<<"$matches") == 1 ]]; then
   assignment=$(jq -c '.[0]' <<<"$matches")
   jq -e --arg p "$(lower "$PRINCIPAL")" --arg s "$(lower "$source_id")" --arg role "$READER_ROLE" '((.principalId|ascii_downcase)==$p) and ((.scope|ascii_downcase)==$s) and ((.roleDefinitionId|ascii_downcase)|endswith($role)) and ((.id|ascii_downcase)|contains("/providers/microsoft.authorization/roleassignments/"))' <<<"$assignment" >/dev/null || die "Recorded Reader assignment identity changed for $source_id."
   assignment=$(jq -c --arg uid "$source_uid" '. + {sourceSnapshotUniqueId:$uid,role:"Reader",status:"created"}' <<<"$assignment")
  else
   [[ $(jq -r '.status' <<<"$intent") == intent ]] || die "Previously created Reader assignment disappeared from $source_id; refusing replacement."
   assignment=$(jq -c '. + {role:"Reader",status:"intent"}' <<<"$intent")
  fi
 else
  [[ $(jq 'length' <<<"$exact") == 0 ]] || die "Refusing to adopt an unrecorded direct assignment for the new principal at $source_id."
  guid=$(python3 -c 'import uuid; print(uuid.uuid4())')
  assignment_id="$source_id/providers/Microsoft.Authorization/roleAssignments/$guid"
  assignment=$(jq -cn --arg id "$assignment_id" --arg p "$PRINCIPAL" --arg s "$source_id" --arg sub "$SUB" --arg role "$READER_ROLE" --arg uid "$source_uid" '{id:$id,principalId:$p,scope:$s,roleDefinitionId:("/subscriptions/"+$sub+"/providers/Microsoft.Authorization/roleDefinitions/"+$role),role:"Reader",sourceSnapshotId:$s,sourceSnapshotUniqueId:$uid,status:"intent"}')
 fi
 ASSIGNMENTS=$(jq -cn --argjson old "$ASSIGNMENTS" --argjson item "$assignment" '$old+[$item]')
done
state_tmp=$(mktemp "$SD/.state.XXXXXX")
jq --arg token "$TOKEN" --arg principal "$PRINCIPAL" --argjson assignments "$ASSIGNMENTS" '.sourceSnapshotReaderAssignments={ownerToken:$token,principalId:$principal,assignments:$assignments,scope:"exact source DATA/WAL snapshot resource IDs only"}' "$STATE" >"$state_tmp"
chmod 600 "$state_tmp"; mv -f "$state_tmp" "$STATE"
for assignment in $(jq -r '.[]|@base64' <<<"$ASSIGNMENTS"); do
 item=$(base64 -d <<<"$assignment"); [[ $(jq -r .status <<<"$item") == intent ]] || continue
 source_id=$(jq -er .sourceSnapshotId <<<"$item"); source_uid=$(jq -er .sourceSnapshotUniqueId <<<"$item"); assignment_id=$(jq -er .id <<<"$item")
 event 'reader-assignment-intent' 'roleAssignment' "$assignment_id" "sourceSnapshotId=$source_id sourceSnapshotUniqueId=$source_uid principalId=$PRINCIPAL"
 created=$(azlong role assignment create --name "$(basename "$assignment_id")" --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role Reader --scope "$source_id" -o json) || die "Cannot grant Reader on exact retained snapshot scope $source_id."
 jq -e --arg id "$(lower "$assignment_id")" --arg p "$(lower "$PRINCIPAL")" --arg s "$(lower "$source_id")" --arg role "$READER_ROLE" '((.id|ascii_downcase)==$id) and ((.principalId|ascii_downcase)==$p) and ((.scope|ascii_downcase)==$s) and ((.roleDefinitionId|ascii_downcase)|endswith($role))' <<<"$created" >/dev/null || die "Created Reader assignment does not match its persisted intent for $source_id."
 source_uid=$(jq -er --arg id "$source_id" '.[]|select(.sourceSnapshotId==$id)|.sourceSnapshotUniqueId' <<<"$ASSIGNMENTS")
 updated=$(jq -c --arg id "$assignment_id" --argjson created "$created" --arg uid "$source_uid" '[.[]|if .id==$id then ($created+{role:"Reader",sourceSnapshotId:.sourceSnapshotId,sourceSnapshotUniqueId:$uid,status:"created"}) else . end]' <<<"$ASSIGNMENTS")
 ASSIGNMENTS="$updated"
 state_tmp=$(mktemp "$SD/.state.XXXXXX"); jq --arg token "$TOKEN" --arg principal "$PRINCIPAL" --argjson assignments "$ASSIGNMENTS" '.sourceSnapshotReaderAssignments={ownerToken:$token,principalId:$principal,assignments:$assignments,scope:"exact source DATA/WAL snapshot resource IDs only"}' "$STATE" >"$state_tmp"; chmod 600 "$state_tmp"; mv -f "$state_tmp" "$STATE"
 printf '%s\n' "$created" >"$TMP/role-assignment-created.json"; save_response 'reader-assignment-created' 'roleAssignment' "$(basename "$assignment_id")" "$TMP/role-assignment-created.json"
done

# Validate raw source evidence and render the retained static CSI imports.
python3 "$HERE/recovery/prepare-source.py" --token "$TOKEN" --short "$SHORT" --output "$TMP/source-snapshots.json"
jq -e --arg data "$SOURCE_ID" --arg wal "$WAL_ID" --arg buid "$SOURCE_BACKUP_UID" --arg bname "$SOURCE_BACKUP" --arg end "$SOURCE_END_LSN" '.bindings.data.sourceSnapshotId==$data and .bindings.wal.sourceSnapshotId==$wal and .sourceBackupUid==$buid and .sourceBackupName==$bname and .endLSN==$end' "$TMP/source-bindings.json" >/dev/null || die 'Rendered snapshot import bindings lost source UID/LSN metadata.'
cp "$TMP/source-bindings.json" "$SD/recovery/source-bindings.json"; chmod 600 "$SD/recovery/source-bindings.json"
save_response 'source-bindings-validated' 'sourceBindings' 'paired-source' "$SD/recovery/source-bindings.json"
if k get cluster "$C" -n "$NS" -o json >"$TMP/existing-cluster.json" 2>"$TMP/cluster-absent.err"; then die 'Cluster already exists; refusing adoption.'; fi
grep -Eiq 'NotFound|not found' "$TMP/cluster-absent.err" || die 'Cannot prove target Cluster absent.'
k apply --dry-run=server -f "$TMP/source-snapshots.json" >/dev/null || die 'Kubernetes rejected the pinned retained VolumeSnapshot source bindings.'
k apply -f "$TMP/source-snapshots.json" >/dev/null
for role in data wal; do
 vss="source-$role-$SHORT"; expected="$SOURCE_ID"; [[ "$role" == wal ]] && expected="$WAL_ID"
 end=$((SECONDS+1800)); ready=false
 while ((SECONDS<end)); do
  obj=$(k get volumesnapshot "$vss" -n "$NS" -o json 2>/dev/null || true)
 if [[ -n "$obj" && $(jq -r '.status.readyToUse // false' <<<"$obj") == true ]]; then
   printf '%s\n' "$obj" >"$TMP/$vss.json"; save_response 'source-vss-ready' 'volumeSnapshot' "$vss" "$TMP/$vss.json"
   content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$obj"); content=$(k get volumesnapshotcontent "$content_name" -o json)
   printf '%s\n' "$content" >"$TMP/$content_name.json"; save_response 'source-vsc-ready' 'volumeSnapshotContent' "$content_name" "$TMP/$content_name.json"
   jq -e --arg id "$(lower "$expected")" --arg t "$TOKEN" --arg buid "$SOURCE_BACKUP_UID" '((.status.snapshotHandle|ascii_downcase)==$id) and ((.spec.source.snapshotHandle|ascii_downcase)==$id) and .spec.deletionPolicy=="Retain" and .metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/source-backup-uid"]==$buid' <<<"$content" >/dev/null || die "Imported $role VolumeSnapshotContent did not bind the exact retained source."
   jq -e --arg buid "$SOURCE_BACKUP_UID" --arg data "$SOURCE_ID" --arg wal "$WAL_ID" --arg role "$role" '.metadata.annotations["vacuum-only-repro/source-backup-uid"]==$buid and .metadata.annotations["vacuum-only-repro/source-snapshot-id"]==(if $role=="data" then $data else $wal end) and .metadata.annotations["cnpg.io/backupLabelFile"]!=null and .metadata.labels["cnpg.io/backupName"]!=null and .metadata.labels["cnpg.io/cluster"]=="hdd-pg"' <<<"$obj" >/dev/null || die "Imported $role VolumeSnapshot lost source Backup labels/backup-label binding."
   ready=true; break
  fi
  sleep 10
 done
 [[ "$ready" == true ]] || die "Imported $role VolumeSnapshot did not become ready."
done

# Republish only the source WAL segment and its history file into this run's
# private archive path. Existing objects are accepted only when their hash
# metadata, owner token, source Backup UID, and content length match.
STORAGE=$(cat "$SD/storage.json")
jq -e --arg token "$TOKEN" --arg sub "$SUB" --arg rg "$RG" '.status=="ready" and .ownerToken==$token and .subscriptionId==$sub and .resourceGroup==$rg and .publicAccess==null' <<<"$STORAGE" >/dev/null || die 'Private WAL storage ownership metadata mismatch.'
ACCOUNT=$(jq -er .accountName <<<"$STORAGE"); CONTAINER=$(jq -er .containerName <<<"$STORAGE")
CONNECTION_FILE=$(mktemp "$SD/.archive-connection.XXXXXX"); chmod 600 "$CONNECTION_FILE"
secret_b64=$(k get secret "$SECRET" -n "$NS" -o json | jq -er '.data["connection-string"]')
printf '%s' "$secret_b64" | base64 -d >"$CONNECTION_FILE"; unset secret_b64
[[ -s "$CONNECTION_FILE" ]] || die 'Private archive credential Secret is empty.'
blob_exists(){ AZURE_STORAGE_CONNECTION_STRING="$(<"$CONNECTION_FILE")" azdata blob exists --auth-mode key --container-name "$CONTAINER" --name "$1" --query exists -o tsv; }
blob_show(){ AZURE_STORAGE_CONNECTION_STRING="$(<"$CONNECTION_FILE")" azdata blob show --auth-mode key --container-name "$CONTAINER" --name "$1" -o json; }
blob_upload(){ AZURE_STORAGE_CONNECTION_STRING="$(<"$CONNECTION_FILE")" azdata blob upload --auth-mode key --container-name "$CONTAINER" --name "$1" --file "$2" --overwrite false --metadata "sha256=$3" "reprotoken=$TOKEN" "sourcebackupuid=$SOURCE_BACKUP_UID" --only-show-errors -o none; }
ARCHIVE_PREFIX="cnpg-vacuum-only/$TOKEN/hdd-pg/wals"
for file in 000000010000004E00000050 000000010000004E00000050.00000028.backup; do
 local_path="$HERE/recovery/archive-input/$file"; [[ -s "$local_path" ]] || die "Verified WAL object input missing: $file"
 if [[ "$file" == *.backup ]]; then blob_name="$ARCHIVE_PREFIX/000000010000004E/$file"; else blob_name="$ARCHIVE_PREFIX/000000010000004E/$file"; fi
 digest=$(sha256sum "$local_path" | awk '{print $1}')
 exists=$(blob_exists "$blob_name") || die "Cannot inspect target archive object $blob_name."
 if [[ "$exists" == true ]]; then
  live_blob=$(blob_show "$blob_name") || die "Cannot inspect pre-existing archive object $blob_name."
  jq -e --arg hash "$digest" --arg token "$TOKEN" --arg uid "$SOURCE_BACKUP_UID" '.metadata.sha256==$hash and .metadata.reprotoken==$token and .metadata.sourcebackupuid==$uid' <<<"$live_blob" >/dev/null || die "Existing private archive object differs from the verified input: $blob_name."
 else
  blob_upload "$blob_name" "$local_path" "$digest" || die "Cannot publish verified source WAL object $blob_name."
 fi
 live_blob=$(blob_show "$blob_name") || die "Cannot verify published archive object $blob_name."
 jq -e --arg hash "$digest" --arg token "$TOKEN" --arg uid "$SOURCE_BACKUP_UID" --argjson bytes "$(wc -c <"$local_path")" '.metadata.sha256==$hash and .metadata.reprotoken==$token and .metadata.sourcebackupuid==$uid and .properties.contentLength==$bytes' <<<"$live_blob" >/dev/null || die "Private archive object hash/size verification failed: $blob_name."
 event 'source-wal-object-ready' 'blob' "$blob_name" "sha256=$digest bytes=$(wc -c <"$local_path") sourceBackupUid=$SOURCE_BACKUP_UID"
done
python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$CONNECTION_FILE"; CONNECTION_FILE=''
DESTINATION="https://$ACCOUNT.blob.core.windows.net/$CONTAINER/cnpg-vacuum-only/$TOKEN/"

DATA="source-data-$SHORT"; WAL="source-wal-$SHORT"; BK="vacuum-seed-$SHORT"; BSC="vacuum-backup-snap-$SHORT"
cat >"$TMP/cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $C
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
  annotations:
    vacuum-only-repro/source-data-snapshot-id: "$SOURCE_ID"
    vacuum-only-repro/source-wal-snapshot-id: "$WAL_ID"
    vacuum-only-repro/source-backup-uid: "$SOURCE_BACKUP_UID"
    vacuum-only-repro/source-backup-end-lsn: "$SOURCE_END_LSN"
spec:
  instances: 1
  imageName: "$SYSIMAGE"
  storage: {storageClass: "$SC", size: "512Gi", resizeInUseVolumes: false}
  walStorage: {storageClass: "$SC", size: "512Gi", resizeInUseVolumes: false}
  bootstrap:
    recovery:
      source: retained-source
      database: repro
      owner: app
      volumeSnapshots:
        storage: {name: "$DATA", kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
        walStorage: {name: "$WAL", kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
      recoveryTarget: {targetLSN: "$SOURCE_END_LSN", targetTLI: "1"}
  externalClusters:
  - name: retained-source
    barmanObjectStore:
      destinationPath: "$DESTINATION"
      serverName: hdd-pg
      azureCredentials:
        connectionString: {name: "$SECRET", key: connection-string}
  backup:
    volumeSnapshot:
      className: "$BSC"
      walClassName: "$BSC"
      snapshotOwnerReference: none
      online: false
  affinity: {enablePodAntiAffinity: true, podAntiAffinityType: required, topologyKey: kubernetes.io/hostname}
  resources: {requests: {cpu: "1", memory: "2Gi"}, limits: {cpu: "2", memory: "3Gi"}}
  postgresql:
    parameters:
      shared_buffers: "256MB"
      track_io_timing: "on"
      track_wal_io_timing: "on"
      synchronous_commit: "on"
EOF
cat >"$TMP/backup-vsc.yaml" <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: $BSC
  labels: {repro.stack.token: "$TOKEN"}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters:
  resourceGroup: $SNAP_RG
  incremental: "true"
  tags: "reprotoken=$TOKEN,reproPurpose=premiumv2-vacuum-backup"
EOF
k apply --dry-run=server -f "$TMP/backup-vsc.yaml" >/dev/null || die 'Kubernetes rejected the retained backup VolumeSnapshotClass.'
k apply -f "$TMP/backup-vsc.yaml" >/dev/null
jq -e --arg t "$TOKEN" --arg rg "$SNAP_RG" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain" and .parameters.resourceGroup==$rg and .parameters.incremental=="true" and (.parameters.tags|contains($t))' < <(k get volumesnapshotclass "$BSC" -o json) >/dev/null || die 'New retained backup VolumeSnapshotClass ownership/retention failed.'
k apply --dry-run=server -f "$TMP/cluster.yaml" >/dev/null || die 'CNPG 1.30.1 rejected the paired snapshot/recovery manifest.'
event 'cluster-recovery-manifest-validated' 'cluster' "$C" 'database=repro owner=app; paired DATA/WAL retained snapshots and pinned end LSN'
k apply -f "$TMP/cluster.yaml" >/dev/null
printf 'Waiting for paired retained-source recovery through its pinned end LSN %s.\n' "$SOURCE_END_LSN"
c=$(wait_ready 1)
printf '%s\n' "$c" >"$TMP/primary-recovered-cluster.json"; save_response 'primary-recovery-ready' 'cluster' "$C" "$TMP/primary-recovered-cluster.json"
jq -e --arg t "$TOKEN" --arg sys "$(jq -er .systemIdentifier "$META")" --arg s "$SOURCE_ID" --arg w "$WAL_ID" --arg image "$SYSIMAGE" '.metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/source-data-snapshot-id"]==$s and .metadata.annotations["vacuum-only-repro/source-wal-snapshot-id"]==$w and .spec.instances==1 and .spec.imageName==$image and .status.readyInstances==1 and .status.systemID==$sys and .spec.bootstrap.recovery.source=="retained-source" and .spec.bootstrap.recovery.database=="repro" and .spec.bootstrap.recovery.owner=="app" and .spec.bootstrap.recovery.volumeSnapshots.storage.name!=null and .spec.bootstrap.recovery.volumeSnapshots.walStorage.name!=null' <<<"$c" >/dev/null || die 'Primary did not recover from the pinned paired retained snapshots with the app database contract.'
POD=$(jq -er .status.currentPrimary <<<"$c"); [[ "$POD" == "$C-1" ]] || die 'Unexpected first primary name.'
event 'paired-source-recovery-complete' 'cluster' "$C" "primary=$POD systemIdentifier=$(jq -r .systemIdentifier "$META") targetLSN=$SOURCE_END_LSN"

# Only bounded pg_control/catalog/statistics reads: no table rowcount or full scan.
FIXTURE_SQL="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'relationFileNode',pg_relation_filenode('public.vacuum_target'),'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'synchronousCommit',current_setting('synchronous_commit'),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'maintenanceWorkMem',current_setting('maintenance_work_mem'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'autovacuum',current_setting('autovacuum'),'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'),'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
fixture=$(psql "$POD" "$FIXTURE_SQL") || die 'Cannot read source fixture identity/catalog metadata.'
jq -e --arg sys "$(jq -er .systemIdentifier "$META")" --argjson heap "$(jq -er .heapBytes "$META")" --argjson total "$(jq -er .tableBytes "$META")" '.systemIdentifier==$sys and .inRecovery==false and .heapBytes==$heap and .tableBytes==$total and (.relationFileNode|tonumber)>0 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex and .synchronousCommit=="on" and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and .sharedBuffers=="256MB" and .maintenanceIoConcurrency=="10" and .maintenanceWorkMem=="64MB" and .checkpointTimeout=="5min" and .maxWalSize=="1GB" and .trackIoTiming=="on" and .trackWalIoTiming=="on" and .autovacuum=="on" and .tablespaceOverride==false and .appSessions==0' <<<"$fixture" >/dev/null || die 'Restored 23-GiB fixture/settings/client-free check failed.'
printf '%s\n' "$fixture" >"$TMP/restored-primary-fixture.json"; save_response 'primary-fixture-verified' 'postgresFixture' "$C-1" "$TMP/restored-primary-fixture.json"
k exec -n "$NS" -c postgres "$POD" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"' || die 'Restored DATA and WAL paths are not on separate filesystems.'

# Do not snapshot a primary while the source DATA/WAL disks are still copying.
event 'primary-copy-hydration-started' 'cluster' "$C" 'require both DATA and WAL copy disks at numeric completionPercent 100 before offline seed Backup'
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json); printf '%s\n' "$pvcs" >"$TMP/primary-pvcs.json"; save_response 'primary-pvcs-before-seed' 'persistentVolumeClaims' "$C" "$TMP/primary-pvcs.json"
for claim in "$C-1" "$C-1-wal"; do
 pvc_json=$(k get pvc "$claim" -n "$NS" -o json); printf '%s\n' "$pvc_json" >"$TMP/$claim-pvc.json"; save_response 'primary-pvc-source' 'persistentVolumeClaim' "$claim" "$TMP/$claim-pvc.json"
 pvc_role=PG_DATA; [[ "$claim" == *-wal ]] && pvc_role=PG_WAL
 jq -e --arg c "$C" --arg instance "$C-1" --arg role "$pvc_role" '.metadata.labels["cnpg.io/cluster"]==$c and .metadata.labels["cnpg.io/instanceName"]==$instance and .metadata.labels["cnpg.io/instanceRole"]=="primary" and .metadata.labels["cnpg.io/pvcRole"]==$role and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-premiumv2"' <<<"$pvc_json" >/dev/null || die "Primary PVC $claim ownership/size/storage class failed."
 pv=$(jq -er .spec.volumeName <<<"$pvc_json"); pv_json=$(k get pv "$pv" -o json); printf '%s\n' "$pv_json" >"$TMP/$claim-pv.json"; save_response 'primary-pv-source' 'persistentVolume' "$claim" "$TMP/$claim-pv.json"
 disk=$(jq -er .spec.csi.volumeHandle <<<"$pv_json"); wait_disk "$claim" "$disk" "$TMP/$claim-disk.json"
 jq -e --arg rg "$(lower "$NODE_RG")" --arg loc "$(lower "$LOC")" --arg zone "$AZURE_ZONE" --arg id "$(lower "$disk")" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$loc) and .sku.name=="PremiumV2_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .diskIOPSReadWrite==3000 and .diskMBpsReadWrite==125 and .provisioningState=="Succeeded" and .managedBy!=null and (.completionPercent|tonumber?)==100 and (.creationData.createOption=="Copy" or .creationData.createOption=="CopyStart") and ((.zones // [])|map(tostring)|index($zone))!=null and ((.logicalSectorSize // .creationData.logicalSectorSize // 0)|tostring)=="512"' "$TMP/$claim-disk.json" >/dev/null || die "$claim primary disk failed the PremiumV2 hydrated-copy contract."
 event 'primary-copy-hydration-complete' 'azureDisk' "$claim" "id=$disk completionPercent=100"
done

# Take a cold paired Backup from the unchanged recovered primary. CNPG 1.30.1
# recommends snapshotting this single instance before scaling to preserve seed use.
# Its v1.30.1 API marks BackupLabelFile as online-backup-only and LSN fields
# optional, so verify offline mode + both snapshot elements rather than requiring
# hot-backup-only metadata from this cold Backup.
cat >"$TMP/backup.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: $BK
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
spec:
  cluster: {name: $C}
  method: volumeSnapshot
  online: false
EOF
k get backup "$BK" -n "$NS" -o json >"$TMP/existing-backup.json" 2>"$TMP/backup-absent.err" && die 'Seed Backup already exists; refusing adoption.'
grep -Eiq 'NotFound|not found' "$TMP/backup-absent.err" || die 'Cannot prove the new seed Backup absent.'
event 'offline-seed-backup-requested' 'backup' "$BK" 'online=false; source DATA and WAL copy disks were fully hydrated first'
k apply -f "$TMP/backup.yaml" >/dev/null
end=$((SECONDS+7200)); backup='{}'; last_backup_signature=''
while ((SECONDS<end)); do
 backup=$(k get backup "$BK" -n "$NS" -o json)
 printf '%s\n' "$backup" >"$TMP/new-backup.json"
 jq -e --arg t "$TOKEN" --arg c "$C" '.metadata.labels["repro.stack.token"]==$t and .spec.cluster.name==$c and .spec.method=="volumeSnapshot" and .spec.online==false' "$TMP/new-backup.json" >/dev/null || die 'New Backup identity/method/offline request failed.'
 phase=$(jq -r '.status.phase // empty' <<<"$backup")
 backup_signature=$(jq -c '[.metadata.uid,.status.phase,.status.online,.status.snapshotBackupStatus.elements,.status.error]' <<<"$backup")
 if [[ "$backup_signature" != "$last_backup_signature" ]]; then save_response "seed-backup-${phase:-pending}" 'backup' "$BK" "$TMP/new-backup.json"; last_backup_signature="$backup_signature"; fi
 [[ "$phase" == completed ]] && break
 [[ "$phase" != failed && "$phase" != walArchivingFailing ]] || die "Paired primary Backup failed: $phase"
 sleep 10
done
jq -e '.status.phase=="completed" and .status.method=="volumeSnapshot" and .spec.online==false and (.status.online==null or .status.online==false) and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die 'New offline Backup did not produce a completed paired DATA/WAL snapshot set.'
BACKUP_UID=$(jq -er .metadata.uid <<<"$backup"); BACKUP_CREATED=$(jq -er .metadata.creationTimestamp <<<"$backup")
event 'offline-seed-backup-complete' 'backup' "$BK" "uid=$BACKUP_UID spec.online=false; offline status LSN/backupLabel fields are optional in CNPG v1.30.1"
DATA=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup")
WAL=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
DATA_SNAPSHOT_ID=''; WAL_SNAPSHOT_ID=''; SNAPSHOT_RECORDS='[]'
for spec in "$DATA:$C-1" "$WAL:$C-1-wal"; do
 IFS=: read -r snap pvc <<<"$spec"
 vss=$(k get volumesnapshot "$snap" -n "$NS" -o json) || die "Cannot read new paired VolumeSnapshot $snap."
 printf '%s\n' "$vss" >"$TMP/$snap-vss-initial.json"; save_response 'seed-vss-observed' 'volumeSnapshot' "$snap" "$TMP/$snap-vss-initial.json"
 pvc_now=$(k get pvc "$pvc" -n "$NS" -o json); printf '%s\n' "$pvc_now" >"$TMP/$pvc-pvc-current.json"; save_response 'seed-primary-pvc-source' 'persistentVolumeClaim' "$pvc" "$TMP/$pvc-pvc-current.json"
 expected_pvc_uid=$(jq -er --arg p "$pvc" '.items[]|select(.metadata.name==$p)|.metadata.uid' <<<"$pvcs")
 pvc_role=PG_DATA; [[ "$pvc" == *-wal ]] && pvc_role=PG_WAL
 jq -e --arg c "$C" --arg instance "$C-1" --arg role "$pvc_role" --arg uid "$expected_pvc_uid" '.metadata.labels["cnpg.io/cluster"]==$c and .metadata.labels["cnpg.io/instanceName"]==$instance and .metadata.labels["cnpg.io/instanceRole"]=="primary" and .metadata.labels["cnpg.io/pvcRole"]==$role and .status.phase=="Bound" and .metadata.uid==$uid' <<<"$pvc_now" >/dev/null || die "Primary PVC $pvc ownership/UID changed after copy hydration."
 jq -e --arg b "$BK" --arg p "$pvc" --arg cls "$BSC" --arg ns "$NS" --arg backupUid "$BACKUP_UID" --arg created "$BACKUP_CREATED" '.metadata.labels["cnpg.io/backupName"]==$b and .metadata.labels["cnpg.io/cluster"]=="hdd-pg" and .metadata.namespace==$ns and .status.readyToUse==true and .spec.source.persistentVolumeClaimName==$p and .spec.volumeSnapshotClassName==$cls and .metadata.creationTimestamp>=$created and all(.metadata.ownerReferences[]?;.uid==$backupUid)' <<<"$vss" >/dev/null || die "New retained snapshot $snap does not bind to the just-created Backup and primary PVC."
 content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$vss"); content=$(k get volumesnapshotcontent "$content_name" -o json)
 printf '%s\n' "$content" >"$TMP/$content_name-vsc-initial.json"; save_response 'seed-vsc-observed' 'volumeSnapshotContent' "$content_name" "$TMP/$content_name-vsc-initial.json"
 jq -e --arg snap "$snap" --arg ns "$NS" --arg uid "$(jq -er .metadata.uid <<<"$vss")" '.spec.deletionPolicy=="Retain" and .spec.driver=="disk.csi.azure.com" and .spec.volumeSnapshotRef.name==$snap and .spec.volumeSnapshotRef.namespace==$ns and .spec.volumeSnapshotRef.uid==$uid and .status.readyToUse==true' <<<"$content" >/dev/null || die "New $snap content is not retained/bound to its exact VSS UID."
 label_value="repro.stack.token=$TOKEN"
 k label volumesnapshot "$snap" -n "$NS" "$label_value" --overwrite >/dev/null
 k annotate volumesnapshot "$snap" -n "$NS" "vacuum-only-repro/seed-backup-uid=$BACKUP_UID" "vacuum-only-repro/source-pvc-uid=$expected_pvc_uid" --overwrite >/dev/null
 k label volumesnapshotcontent "$content_name" "$label_value" --overwrite >/dev/null
 k annotate volumesnapshotcontent "$content_name" "vacuum-only-repro/seed-backup-uid=$BACKUP_UID" "vacuum-only-repro/source-pvc-uid=$expected_pvc_uid" --overwrite >/dev/null
 vss=$(k get volumesnapshot "$snap" -n "$NS" -o json); printf '%s\n' "$vss" >"$TMP/$snap-vss-owned.json"; save_response 'seed-vss-owned' 'volumeSnapshot' "$snap" "$TMP/$snap-vss-owned.json"
 content=$(k get volumesnapshotcontent "$content_name" -o json); printf '%s\n' "$content" >"$TMP/$content_name-vsc-owned.json"; save_response 'seed-vsc-owned' 'volumeSnapshotContent' "$content_name" "$TMP/$content_name-vsc-owned.json"
 jq -e --arg t "$TOKEN" --arg buid "$BACKUP_UID" --arg puid "$expected_pvc_uid" '.metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/seed-backup-uid"]==$buid and .metadata.annotations["vacuum-only-repro/source-pvc-uid"]==$puid' <<<"$vss" >/dev/null || die "Seed VolumeSnapshot $snap owner binding failed."
 jq -e --arg t "$TOKEN" --arg buid "$BACKUP_UID" --arg puid "$expected_pvc_uid" '.metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/seed-backup-uid"]==$buid and .metadata.annotations["vacuum-only-repro/source-pvc-uid"]==$puid' <<<"$content" >/dev/null || die "Seed VolumeSnapshotContent $content_name owner binding failed."
 snap_id=$(jq -er '.status.snapshotHandle' <<<"$content")
 [[ "$(lower "$snap_id")" == "/subscriptions/$(lower "$SUB")/resourcegroups/$(lower "$SNAP_RG")/providers/microsoft.compute/snapshots/"* ]] || die "New CNPG snapshot escaped the run-owned retained snapshot group: $snap_id"
 live=$(azc snapshot show --ids "$snap_id" -o json) || die "Cannot inspect newly created retained snapshot $snap_id."
 name=$(basename "$snap_id")
 printf '%s\n' "$live" >"$TMP/seed-$name-arm-snapshot.json"; save_response 'seed-arm-snapshot-observed' 'azureSnapshot' "$name" "$TMP/seed-$name-arm-snapshot.json"
 jq -e --arg id "$(lower "$snap_id")" --arg t "$TOKEN" --arg loc "$(lower "$LOC")" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-vacuum-backup"' <<<"$live" >/dev/null || die "New paired snapshot $name ownership/readiness metadata failed."
 lock_snapshot "$snap_id" "$name"
 record=$(jq -cn --arg id "$snap_id" --arg name "$snap" --arg pvc "$pvc" --arg uid "$(jq -er .metadata.uid <<<"$vss")" --arg contentUid "$(jq -er .metadata.uid <<<"$content")" --arg backupUid "$(jq -er .metadata.uid <<<"$backup")" '{id:$id,volumeSnapshot:$name,sourcePVC:$pvc,volumeSnapshotUid:$uid,volumeSnapshotContentUid:$contentUid,backupUid:$backupUid,retentionLock:"CanNotDelete"}')
 SNAPSHOT_RECORDS=$(jq -cn --argjson old "$SNAPSHOT_RECORDS" --argjson rec "$record" '$old+[$rec]')
 [[ "$pvc" == "$C-1" ]] && DATA_SNAPSHOT_ID="$snap_id" || WAL_SNAPSHOT_ID="$snap_id"
done
jq -c '.[]' <<<"$SNAPSHOT_RECORDS" >"$SD/output-snapshots.jsonl"; chmod 600 "$SD/output-snapshots.jsonl"

# Scale only after paired Backup; fail unless the new replica uses restoresnapshot.
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null
end=$((SECONDS+10800)); JOB_UID=''; JOB_NAME="$C-2-snapshot-recovery"
while ((SECONDS<end)); do
 jobs=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 job=$(jq -c --arg i "$C-2" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery")]|first // empty' <<<"$jobs")
 if [[ -n "$job" ]]; then
  jq -e --arg image "$SYSIMAGE" '(.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.image)==$image and (([.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.command[]]|join(" "))|contains("restoresnapshot"))' <<<"$job" >/dev/null || die 'Replica recovery job did not use CNPG restoresnapshot on the pinned recovery image.'
  JOB_UID=$(jq -er .metadata.uid <<<"$job")
 fi
 c=$(k get cluster "$C" -n "$NS" -o json)
 [[ -n "$JOB_UID" && $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 ]] && break
 sleep 5
done
[[ -n "$JOB_UID" ]] || die 'Did not observe the replica snapshot-recovery job.'
c=$(wait_ready 2)
printf '%s\n' "$c" >"$TMP/cluster-with-replica.json"; save_response 'paired-replica-ready' 'cluster' "$C" "$TMP/cluster-with-replica.json"
jq -e --arg image "$SYSIMAGE" --arg sys "$(jq -er .systemIdentifier "$META")" '.status.readyInstances==2 and .status.systemID==$sys and .spec.imageName==$image and .spec.bootstrap.recovery.source=="retained-source" and .spec.bootstrap.recovery.volumeSnapshots.storage.name!=null and .spec.bootstrap.recovery.volumeSnapshots.walStorage.name!=null' <<<"$c" >/dev/null || die 'Two-instance Cluster lost the paired recovery lineage.'
job=$(k get job "$JOB_NAME" -n "$NS" -o json 2>/dev/null || true)
if [[ -n "$job" ]]; then printf '%s\n' "$job" >"$TMP/replica-recovery-job.json"; save_response 'replica-recovery-job-complete' 'job' "$JOB_NAME" "$TMP/replica-recovery-job.json"; jq -e --arg uid "$JOB_UID" --arg image "$SYSIMAGE" '.metadata.uid==$uid and .status.succeeded>=1 and (.status.active // 0)==0 and ((.status.failed // 0)==0) and (.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.image)==$image' <<<"$job" >/dev/null || die 'Replica snapshot-recovery Job did not complete successfully.'; fi
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
printf '%s\n' "$pvcs" >"$TMP/replica-snapshotted-pvcs.json"; save_response 'replica-snapshot-pvcs-verified' 'persistentVolumeClaims' "$C" "$TMP/replica-snapshotted-pvcs.json"
jq -e --arg data "$DATA" --arg wal "$WAL" --arg c "$C" --arg short "$SHORT" '(.items|length)==4 and ([.items[]|select((.metadata.name==($c+"-1") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==("source-data-"+$short)) or (.metadata.name==($c+"-1-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==("source-wal-"+$short)) or (.metadata.name==($c+"-2") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$data) or (.metadata.name==($c+"-2-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$wal))]|length==4)' <<<"$pvcs" >/dev/null || die 'Four DATA/WAL PVCs are not bound to pinned source/seed snapshots.'

# The archive is only a recovery input. Prove the webhook permits removing the
# source reference, then remove it and the Secret before measurement.
k patch cluster "$C" -n "$NS" --type=json --dry-run=server -o yaml -p='[{"op":"remove","path":"/spec/externalClusters"}]' >"$TMP/external-clusters-removal-dryrun.yaml" || die 'CNPG webhook rejected removing the recovery-only external cluster.'
save_response 'external-cluster-removal-dryrun' 'clusterPatch' "$C" "$TMP/external-clusters-removal-dryrun.yaml"
k patch cluster "$C" -n "$NS" --type=json -p='[{"op":"remove","path":"/spec/externalClusters"}]' >/dev/null
event 'recovery-archive-reference-removed' 'cluster' "$C" 'externalClusters removed after successful paired restore and seed Backup'
k delete secret "$SECRET" -n "$NS" --wait=true >/dev/null
k patch cluster "$C" -n "$NS" --type=merge -p "$(jq -cn --arg image "$IMAGE" '{spec:{imageName:$image,primaryUpdateMethod:"restart"}}')" >/dev/null
digest=$(cut -d@ -f2 <<<"$IMAGE"); end=$((SECONDS+10800)); image_ready=false
while ((SECONDS<end)); do
 c=$(k get cluster "$C" -n "$NS" -o json); pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 if [[ $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 ]] && jq -e --arg i "$IMAGE" --arg d "$digest" '[.items[]|select(.status.phase=="Running" and any(.spec.containers[];.name=="postgres" and .image==$i) and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($d))))]|length==2' <<<"$pods" >/dev/null; then image_ready=true; break; fi
 sleep 10
done
[[ "$image_ready" == true ]] || die 'Instances did not reach the pinned measurement image.'
wait_streaming
flush_lsn=$(psql "$C-1" 'SELECT pg_current_wal_flush_lsn();'); wait_replay_lsn "$flush_lsn"

CFG="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'tableBytes',pg_total_relation_size('public.vacuum_target'),'relationFileNode',pg_relation_filenode('public.vacuum_target'),'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'synchronousCommit',current_setting('synchronous_commit'),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'walLogHints',current_setting('wal_log_hints'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'maintenanceWorkMem',current_setting('maintenance_work_mem'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'autovacuum',current_setting('autovacuum'),'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'),'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
check_pod(){
 local pod="$1" recovery="$2" result="$TMP/$1-config.json"
 psql "$pod" "$CFG" >"$result" || die "Cannot query final baseline settings on $pod."
 save_response 'final-fixture-check' 'postgresFixture' "$pod" "$result"
 jq -e --arg sys "$(jq -er .systemIdentifier "$META")" --argjson heap "$(jq -er .heapBytes "$META")" --argjson table "$(jq -er .tableBytes "$META")" --argjson recovery "$recovery" '.systemIdentifier==$sys and .inRecovery==$recovery and .heapBytes==$heap and .tableBytes==$table and (.relationFileNode|tonumber)>0 and .tableAutovacuumDisabled and .hasIndex and .synchronousCommit=="on" and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and .walLogHints=="on" and .sharedBuffers=="256MB" and .maintenanceIoConcurrency=="10" and .maintenanceWorkMem=="64MB" and .checkpointTimeout=="5min" and .maxWalSize=="1GB" and .trackIoTiming=="on" and .trackWalIoTiming=="on" and .autovacuum=="on" and .tablespaceOverride==false and .appSessions==0' "$result" >/dev/null || die "$pod fixture/settings/client-free check failed."
 k exec -n "$NS" -c postgres "$pod" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"' || die "$pod DATA/WAL mount separation failed."
}
check_pod "$C-1" false; check_pod "$C-2" true
jq -e --slurpfile p "$TMP/$C-1-config.json" --slurpfile r "$TMP/$C-2-config.json" '$p[0].relationFileNode==$r[0].relationFileNode' -n >/dev/null || die 'Primary and replica relation filenodes differ after snapshot recovery.'
event 'primary-replica-lineage-verified' 'postgresFixture' 'vacuum_target' "systemIdentifier=$(jq -r .systemIdentifier "$META") primaryAndReplicaRelationFileNode=$(jq -r .relationFileNode "$TMP/$C-1-config.json") heapBytes=$(jq -r .heapBytes "$TMP/$C-1-config.json") tableBytes=$(jq -r .tableBytes "$TMP/$C-1-config.json")"
c=$(k get cluster "$C" -n "$NS" -o json)
jq -e --arg t "$TOKEN" --arg i "$IMAGE" --arg sys "$(jq -er .systemIdentifier "$META")" --arg s "$SOURCE_ID" --arg w "$WAL_ID" '.metadata.labels["repro.stack.token"]==$t and .metadata.annotations["vacuum-only-repro/source-data-snapshot-id"]==$s and .metadata.annotations["vacuum-only-repro/source-wal-snapshot-id"]==$w and .spec.imageName==$i and .spec.instances==2 and .status.readyInstances==2 and .status.currentPrimary=="hdd-pg-1" and .status.systemID==$sys and .spec.backup.barmanObjectStore==null and .spec.externalClusters==null and .spec.walStorage.size=="512Gi" and .spec.postgresql.parameters.synchronous_commit=="on" and .spec.postgresql.synchronous.method=="any" and .spec.postgresql.synchronous.number==1 and .spec.postgresql.synchronous.dataDurability=="preferred"' <<<"$c" >/dev/null || die 'Final measurement Cluster configuration check failed.'
if k get secret "$SECRET" -n "$NS" -o json >"$TMP/archive-secret.json" 2>"$TMP/archive-secret.err"; then die 'Archive Secret remains after source recovery; measurement must have no archive writer/reader.'; fi
grep -Eiq 'NotFound|not found' "$TMP/archive-secret.err" || die 'Cannot prove the source archive Secret absent.'

# Check all new snapshot locks and await a numeric 100 percent for every disk.
locklist=$(azc lock list --resource-group "$SNAP_RG" -o json) || die 'Cannot inspect owned snapshot locks.'
snapshot_list=$(azc snapshot list --resource-group "$SNAP_RG" -o json) || die 'Cannot inventory run-owned retained snapshots.'
owned_ids=$(jq -r --arg t "$TOKEN" '.[]|select(.tags.reprotoken==$t)|.id' <<<"$snapshot_list")
[[ -n "$owned_ids" ]] || die 'No run-owned retained snapshots were found.'
for id in "$DATA_SNAPSHOT_ID" "$WAL_SNAPSHOT_ID"; do jq -e --arg id "$(lower "$id")" 'any(.[]; ((.id|ascii_downcase)==$id))' <<<"$snapshot_list" >/dev/null || die "Paired snapshot missing from retained inventory: $id"; done
while IFS= read -r id; do
 [[ -n "$id" ]] || continue
 name=$(basename "$id"); outfile="$TMP/owned-$name.json"; azc snapshot show --ids "$id" -o json >"$outfile" || die "Cannot verify retained output snapshot $name."
 jq -e --arg id "$(lower "$id")" --arg t "$TOKEN" --arg loc "$(lower "$LOC")" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-vacuum-backup"' "$outfile" >/dev/null || die "Retained snapshot readiness/ownership mismatch: $name"
 jq -e --arg id "$(lower "$id")" --argjson locks "$locklist" -n 'any($locks[];.level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' >/dev/null || die "Retained snapshot lacks a direct CanNotDelete lock: $name"
done <<<"$owned_ids"
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json); printf '%s\n' "$pvcs" >"$TMP/final-pvcs.json"; save_response 'final-pvcs-hydration-check' 'persistentVolumeClaims' "$C" "$TMP/final-pvcs.json"; disks=''; DISK_RECORDS='[]'
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do
 pv=$(jq -er --arg n "$claim" '.items[]|select(.metadata.name==$n and .status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-premiumv2")|.spec.volumeName' <<<"$pvcs")
 printf '%s\n' "$(jq -c --arg n "$claim" '.items[]|select(.metadata.name==$n)' <<<"$pvcs")" >"$TMP/$claim-pvc.json"; save_response 'final-pvc-hydration-check' 'persistentVolumeClaim' "$claim" "$TMP/$claim-pvc.json"
 k get pv "$pv" -o json >"$TMP/$claim-pv.json"; save_response 'final-pv-hydration-check' 'persistentVolume' "$claim" "$TMP/$claim-pv.json"; disk=$(jq -er '.spec.csi.volumeHandle' "$TMP/$claim-pv.json"); wait_disk "$claim" "$disk" "$TMP/$claim-disk.json"
 jq -e --arg rg "$(lower "$NODE_RG")" --arg loc "$(lower "$LOC")" --arg zone "$AZURE_ZONE" --arg id "$(lower "$disk")" '((.id|ascii_downcase)==$id) and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and ((.location|ascii_downcase)==$loc) and .sku.name=="PremiumV2_LRS" and (.diskSizeGB // .diskSizeGb)==512 and .diskIOPSReadWrite==3000 and .diskMBpsReadWrite==125 and .provisioningState=="Succeeded" and .managedBy!=null and (.completionPercent|tonumber?)==100 and (.creationData.createOption=="Copy" or .creationData.createOption=="CopyStart") and ((.zones // [])|map(tostring)|index($zone))!=null and ((.logicalSectorSize // .creationData.logicalSectorSize // 0)|tostring)=="512"' "$TMP/$claim-disk.json" >/dev/null || die "$claim PremiumV2 SKU/performance/zone/sector/hydration verification failed."
 disks+="$disk"$'\n'; rec=$(jq -c --arg claim "$claim" '{claim:$claim,id,sku:.sku.name,diskSizeGiB:(.diskSizeGB // .diskSizeGb),iops:.diskIOPSReadWrite,throughputMBps:.diskMBpsReadWrite,completionPercent:(.completionPercent|tonumber),createOption:.creationData.createOption,sector:(.logicalSectorSize // .creationData.logicalSectorSize),zones,provisioningState}' "$TMP/$claim-disk.json"); DISK_RECORDS=$(jq -cn --argjson old "$DISK_RECORDS" --argjson rec "$rec" '$old+[$rec]')
done
[[ $(sort -u <<<"$disks"|sed '/^$/d'|wc -l) == 4 ]] || die 'Expected four distinct primary/replica DATA/WAL PremiumV2 disks.'
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ); AUDIT="$SD/recovery/prepare-audit.json"
jq -n --arg at "$NOW" --arg sub "$SUB" --arg rg "$RG" --arg node "$NODE_RG" --arg snaprg "$SNAP_RG" --arg token "$TOKEN" --arg source "$SOURCE_ID" --arg sourceWal "$WAL_ID" --arg sourceBackup "$SOURCE_BACKUP" --arg sourceBackupUid "$SOURCE_BACKUP_UID" --arg endLsn "$SOURCE_END_LSN" --arg seedBackup "$BK" --arg seedBackupUid "$BACKUP_UID" --arg data "$DATA_SNAPSHOT_ID" --arg wal "$WAL_SNAPSHOT_ID" --arg jobUid "$JOB_UID" --arg flush "$flush_lsn" --arg image "$IMAGE" --argjson inheritedLive "$(jq -er .preparedLiveTuplesEstimate "$META")" --argjson inheritedDead "$(jq -er .preparedDeadTuplesEstimate "$META")" --argjson assignments "$ASSIGNMENTS" --argjson sourceBindings "$(cat "$SD/recovery/source-bindings.json")" --argjson outputSnapshots "$SNAPSHOT_RECORDS" --argjson disks "$DISK_RECORDS" --argjson primaryFixture "$fixture" '{version:1,status:"complete",verifiedAtUtc:$at,ownerToken:$token,subscriptionId:$sub,resourceGroup:$rg,nodeResourceGroup:$node,retainedSnapshotResourceGroup:$snaprg,source:{dataSnapshotId:$source,walSnapshotId:$sourceWal,backupName:$sourceBackup,backupUid:$sourceBackupUid,backupEndLSN:$endLsn,bindings:$sourceBindings,readerAssignments:$assignments,preparedTupleEstimates:{live:$inheritedLive,dead:$inheritedDead}},offlineSeedBackup:{name:$seedBackup,uid:$seedBackupUid,online:false,metadataFieldsRequired:["phase","method","paired DATA/WAL elements"]},primaryAndReplicaSnapshots:{data:$data,wal:$wal,records:$outputSnapshots},snapshotRecoveryJobUid:$jobUid,replicaCaughtUpToLsn:$flush,fixture:$primaryFixture,observedTupleEstimates:{primaryLive:$primaryFixture.liveEstimate,primaryDead:$primaryFixture.deadEstimate},hydratedPremiumV2Disks:$disks,archiveSecretAbsent:true,archiveWriterAbsent:true,clientCreated:false,cluster:"repro/hdd-pg",instances:2,measurementImage:$image,allSnapshotsRetained:true,sourceSnapshotsUntouched:true}' >"$AUDIT"
chmod 600 "$AUDIT"
jq -n --arg source "$SOURCE_ID" --arg sourceKind paired-retained-cnp-snapshot --arg sys "$(jq -er .systemIdentifier "$META")" --arg replay "$flush_lsn" --argjson estimate "$(awk 'BEGIN{printf "%.1f",23*260.583/110}')" --argjson heap "$(jq -er .heapBytes "$META")" --argjson total "$(jq -er .tableBytes "$META")" --argjson live "$(jq -er .preparedLiveTuplesEstimate "$META")" --argjson dead "$(jq -er .preparedDeadTuplesEstimate "$META")" --arg backup "$BK" --arg data "$DATA_SNAPSHOT_ID" --arg wal "$WAL_SNAPSHOT_ID" --arg image "$IMAGE" --arg zone "$AZURE_ZONE" --arg snaprg "$SNAP_RG" --arg audit "$AUDIT" --arg verified "$NOW" '{version:1,status:"complete",sourceSnapshotId:$source,sourceKind:$sourceKind,systemIdentifier:$sys,sourceSnapshotHeapBytes:$heap,sourceSnapshotTableBytes:$total,fixtureTargetHeapGiB:23,fixtureTargetHeapBytes:$heap,heapBytes:$heap,tableBytes:$total,estimatedVacuumMinutes:$estimate,preparedLiveTuplesEstimate:$live,preparedDeadTuplesEstimate:$dead,replicaCaughtUpToLsn:$replay,cluster:"repro/hdd-pg",instances:2,availabilityZone:$zone,retainedSnapshotResourceGroup:$snaprg,storage:"four separate 512-GiB PremiumV2_LRS DATA/WAL disks at 3000 IOPS / 125 MB/s",backup:$backup,replicaSnapshots:{data:$data,wal:$wal},measurementImage:$image,clientCreated:false,finalAudit:{verifiedAtUtc:$verified,receipt:$audit,archiveSecretAbsent:true,archiveWriterAbsent:true,ownedRetentionSnapshotsReady:true,allFourPremiumV2DisksHydrated:true,sourceLineageVerified:true}}' >"$TMP/prepare.json"
chmod 600 "$TMP/prepare.json"; mv -f "$TMP/prepare.json" "$SD/prepare.json"
cp "$HERE/snapshot.json" "$SD/snapshot.json"; chmod 600 "$SD/snapshot.json"
printf 'Prepared paired retained DATA/WAL source %s at %s; both instances have four hydrated PremiumV2 disks and no client has been created. Receipt: %s\n' "$SOURCE_ID" "$flush_lsn" "$SD/prepare.json"
