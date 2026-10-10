#!/usr/bin/env bash
# Remove owned compute/test resources while retaining every snapshot.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"; SD="$ROOT/.state"; SF="$SD/state.json"
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
for x in az jq timeout flock sleep python3; do command -v "$x" >/dev/null || die "Missing $x"; done
[[ -f "$SF" ]] || die 'No saved ownership state; refusing to guess resources.'
jq -e '(.version==1) and (.subscriptionId|type=="string" and length>0) and (.resourceGroup|type=="string" and length>0) and (.clusterName|type=="string" and length>0) and (.ownerToken|type=="string" and test("^[a-f0-9]{32}$")) and (.nodeResourceGroup|type=="string") and (.requestedNodeResourceGroup|type=="string" and length>0) and (.snapshotResourceGroup|type=="string" and length>0) and (.snapshotResourceGroupId|type=="string" and length>0) and (.availabilityZone|type=="string" and test("^[1-3]$")) and (.kubeconfigPath|type=="string" and length>0)' "$SF" >/dev/null || die 'Invalid ownership state.'
SUB_ENV="${SUB:-}"; SUB="$(jq -r .subscriptionId "$SF")"; RG="$(jq -r .resourceGroup "$SF")"; AKS="$(jq -r .clusterName "$SF")"
NODE_RG="$(jq -r .nodeResourceGroup "$SF")"; REQUESTED="$(jq -r .requestedNodeResourceGroup "$SF")"; SNAP_RG="$(jq -r .snapshotResourceGroup "$SF")"; SNAP_RG_ID="$(jq -r .snapshotResourceGroupId "$SF")"; TOKEN="$(jq -r .ownerToken "$SF")"; LOC="$(jq -r .location "$SF")"; KC="$(jq -r .kubeconfigPath "$SF")"
SOURCE_SNAPSHOT_ID="$(jq -er .sourceSnapshotId "$ROOT/source-manifest.json")" || die 'Pinned source snapshot identity is missing.'
[[ -z "$SUB_ENV" || "${SUB_ENV,,}" == "${SUB,,}" ]] || die 'SUB differs from saved subscription.'
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$AKS" == "aks-vacuum-only-${TOKEN:0:12}" && "$REQUESTED" == "rg-vacuum-nodes-$TOKEN" && "$SNAP_RG" == "rg-vacuum-pv2-snap-$TOKEN" && "${SNAP_RG_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SNAP_RG,,}" && ( -z "$NODE_RG" || "${NODE_RG,,}" == "${REQUESTED,,}" ) && "${KC#"$ROOT"/}" == ".state/kubeconfig" ]] || die 'Resource names, snapshot group, or kubeconfig path do not match stack ownership.'
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/teardown.lock"; flock -n 9 || die 'Teardown already running.'
exec 7>"$SD/test.lock"; flock -n 7 || die 'Measurement is running; let VACUUM and replay observation finish first.'
exec 8>"$SD/02-prepare.lock"; flock -n 8 || die 'Snapshot preparation is still running.'
azc(){ timeout --foreground --kill-after=15s 900s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 3600s az "$@" --subscription "$SUB"; }
active="$(azc account show --query id -o tsv)" || die 'Cannot inspect active subscription.'
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active Azure subscription differs from saved state.'
report_tmp="$(mktemp "$SD/.retention.XXXXXX")"; chmod 600 "$report_tmp"
trap 'rc=$?; [[ ! -e "$report_tmp" ]] || python3 -c '\''import os,sys; os.unlink(sys.argv[1])'\'' "$report_tmp" 2>/dev/null || true; exit "$rc"' EXIT

group_exists(){ local v; v="$(azc group exists -n "$1" -o tsv)" || die "Cannot inspect resource group $1."; case "$v" in true)return 0;; false)return 1;; *) die 'Unexpected group-exists response.';; esac; }
show_group(){ azc group show -n "$1" -o json; }
show_aks(){ local out err; err="$(mktemp "$SD/.aks.XXXXXX")"; chmod 600 "$err"; if out="$(azc aks show -g "$RG" -n "$AKS" -o json 2>"$err")"; then python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$err"; printf '%s' "$out"; elif grep -Eiq 'ResourceNotFound|NotFound|could not be found|404' "$err"; then python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$err"; return 0; else python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$err"; die 'Cannot inspect AKS.'; fi; }
wait_aks_gone(){ local end=$((SECONDS+7200)) a; while ((SECONDS<end)); do a="$(show_aks)"; [[ -z "$a" ]] && return 0; jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.tags.reprotoken==$t and .nodeResourceGroup==$n' <<<"$a" >/dev/null || die 'AKS ownership or node RG changed during deletion.'; sleep 15; done; die 'AKS deletion timed out.'; }
wait_rg_gone(){ local g="$1" end=$((SECONDS+7200)); while group_exists "$g"; do ((SECONDS<end)) || die "Resource group deletion timed out: $g"; sleep 15; done; }

# This group is the durable home for every experiment snapshot. Never delete it.
group_exists "$SNAP_RG" || die 'Retained snapshot resource group is missing; refusing all teardown.'
snaprg="$(show_group "$SNAP_RG")" || die 'Cannot inspect retained snapshot resource group.'
jq -e --arg t "$TOKEN" --arg n "$SNAP_RG" --arg l "${LOC,,}" '.name==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-retained-snapshots" and .tags["managed-by"]=="vacuum-only-repro"' <<<"$snaprg" >/dev/null || die 'Retained snapshot resource group ownership did not verify.'
locks="$(azc lock list --resource-group "$SNAP_RG" -o json)" || die 'Cannot inspect retained snapshot group locks.'
jq -e --arg id "${SNAP_RG_ID,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die 'Retained snapshot resource group lacks its CanNotDelete lock.'
snapshots="$(azc snapshot list -g "$SNAP_RG" -o json)" || die 'Cannot inventory retained snapshots.'
resources="$(azc resource list -g "$SNAP_RG" -o json)" || die 'Cannot inspect retained snapshot group resources.'
jq -e --arg rg "${SNAP_RG_ID,,}" 'all(.[]; ((.id|ascii_downcase)|startswith($rg+"/providers/microsoft.compute/snapshots/")))' <<<"$resources" >/dev/null || die 'Retained snapshot group contains an unrecognized resource; preserving it for review.'
: >"$report_tmp"
while IFS=$'\t' read -r sid name purpose; do
  [[ -n "$sid" ]] || continue
  [[ "${sid,,}" == "${SNAP_RG_ID,,}/providers/microsoft.compute/snapshots/"* ]] || die 'Snapshot escaped its retained group.'
  snap="$(azc snapshot show --ids "$sid" -o json)" || die "Cannot inspect retained snapshot $name."
  snapshot_state="$(jq -r '.provisioningState // ""' <<<"$snap")"
  size_json="$(jq -c '.diskSizeGB // .diskSizeGb // null' <<<"$snap")"
  completion_json="$(jq -c '.completionPercent // null' <<<"$snap")"
  sku="$(jq -r '.sku.name // ""' <<<"$snap")"
  if [[ "$snapshot_state" == Succeeded ]]; then
    jq -e --arg id "${sid,,}" --arg t "$TOKEN" --arg loc "${LOC,,}" --arg purpose "$purpose" '((.completionPercent|tonumber?) // null) as $percent | ((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Succeeded" and (if $purpose=="premiumv2-keeper-copy" then (if .creationData.createOption=="CopyStart" then $percent==100 elif .creationData.createOption=="Copy" then ($percent==null or $percent==100) else false end) else ($percent==null or $percent==100) end) and .snapshotAccessState=="Available" and (.diskSizeGB // .diskSizeGb)==512 and (if $purpose=="premiumv2-keeper-copy" then .sku.name=="Standard_LRS" else (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") end) and .incremental==true and .tags.reprotoken==$t and .tags.reproPurpose==$purpose and ($purpose=="premiumv2-keeper-copy" or $purpose=="premiumv2-vacuum-backup")' <<<"$snap" >/dev/null || die "Retained snapshot identity or readiness check failed: $name."
    preservation_status=ready
  elif [[ "$snapshot_state" == Failed ]]; then
    # A failed, owner-tagged copy is still a snapshot and must be preserved.
    # It is terminal, so it no longer depends on the source disk being kept.
    jq -e --arg id "${sid,,}" --arg t "$TOKEN" --arg source "${SOURCE_SNAPSHOT_ID,,}" --arg sub "${SUB,,}" --arg loc "${LOC,,}" --arg purpose "$purpose" '((.tags.sourceSnapshotId // "")|ascii_downcase) as $tagSource | ((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$loc) and .provisioningState=="Failed" and .tags.reprotoken==$t and .tags.reproPurpose==$purpose and ($purpose=="premiumv2-keeper-copy" or $purpose=="premiumv2-vacuum-backup") and (if $purpose=="premiumv2-keeper-copy" then $tagSource==$source and ($tagSource|startswith("/subscriptions/"+$sub+"/resourcegroups/")) and ($tagSource|contains("/providers/microsoft.compute/snapshots/")) else true end)' <<<"$snap" >/dev/null || die "Failed snapshot ownership/source check failed: $name."
    preservation_status=terminal_failed_preserved
  else
    die "Snapshot operation is nonterminal or unknown; preserving compute until it settles: $name ($snapshot_state)."
  fi
  locks="$(azc lock list --resource-group "$SNAP_RG" -o json)" || die "Cannot inspect snapshot lock for $name."
  if ! jq -e --arg id "${sid,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null; then
    azc lock create --name "retain-$name" --lock-type CanNotDelete --resource-group "$SNAP_RG" --resource-name "$name" --resource-type Microsoft.Compute/snapshots --notes 'Snapshot retained until explicit user authorization.' -o none || die "Cannot protect retained snapshot $name."
    locks="$(azc lock list --resource-group "$SNAP_RG" -o json)" || die "Cannot recheck snapshot lock for $name."
    jq -e --arg id "${sid,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$id))' <<<"$locks" >/dev/null || die "CanNotDelete lock did not verify for snapshot $name."
  fi
  jq -n --arg id "$sid" --arg name "$name" --arg purpose "$purpose" --arg source "$(jq -r '.tags.sourceSnapshotId // ""' <<<"$snap")" --arg state "$snapshot_state" --arg access "$(jq -r '.snapshotAccessState // ""' <<<"$snap")" --arg sku "$sku" --arg uniqueId "$(jq -r '.uniqueId // empty' <<<"$snap")" --arg preservation "$preservation_status" --argjson size "$size_json" --argjson completion "$completion_json" --argjson incremental "$(jq -c '.incremental // null' <<<"$snap")" '{id:$id,name:$name,purpose:$purpose,sourceSnapshotId:(if $source=="" then null else $source end),provisioningState:$state,snapshotAccessState:$access,sku:$sku,uniqueId:$uniqueId,diskSizeGiB:$size,completionPercent:$completion,preservationStatus:$preservation,incremental:$incremental,retained:true,resourceLock:"CanNotDelete"}' >>"$report_tmp"
done < <(jq -r '.[] | [.id,.name,(.tags.reproPurpose // "")] | @tsv' <<<"$snapshots")
retained="$(jq -s '.' "$report_tmp")"
receipt_tmp="$(mktemp "$SD/.snapshot-retention.XXXXXX")"; jq -n --arg token "$TOKEN" --arg rg "$SNAP_RG_ID" --arg loc "$LOC" --argjson snapshots "$retained" '{version:1,ownerToken:$token,resourceGroupId:$rg,location:$loc,resourceGroupLock:"CanNotDelete",snapshots:$snapshots}' >"$receipt_tmp"; chmod 600 "$receipt_tmp"; mv -f "$receipt_tmp" "$SD/snapshot-retention.json"

# AKS deletion can remove snapshots in either its node RG or the test RG.
# Any snapshot there blocks all destructive cleanup; never guess ownership.
assert_no_snapshots(){ local rg="$1" s; group_exists "$rg" || return 0; s="$(azc snapshot list -g "$rg" -o json)" || die "Cannot prove no snapshots exist in $rg."; [[ "$(jq length <<<"$s")" == 0 ]] || die "Snapshots exist in $rg; preserving AKS and resource groups to prevent indirect deletion."; }
# Do not remove an AKS/node/test resource group while a copy operation in the
# retained group may still be reading one of that stack's managed disks.
assert_no_active_disk_copy(){
  local listing sid name snap state option source source_lc rg
  listing="$(azc snapshot list -g "$SNAP_RG" -o json)" || die 'Cannot recheck retained snapshots before compute deletion.'
  while IFS=$'\t' read -r sid name; do
    [[ -n "$sid" ]] || continue
    snap="$(azc snapshot show --ids "$sid" -o json)" || die "Cannot recheck copy state for $name."
    state="$(jq -r '.provisioningState // ""' <<<"$snap")"
    option="$(jq -r '.creationData.createOption // ""' <<<"$snap")"
    if [[ ( "$option" == Copy || "$option" == CopyStart ) &&
          "$state" != Succeeded && "$state" != Failed && "$state" != Canceled && "$state" != Cancelled ]]; then
      source="$(jq -r '.creationData.sourceResourceId // ""' <<<"$snap")"
      source_lc="${source,,}"
      for rg in "$NODE_RG" "$REQUESTED" "$RG"; do
        [[ -n "$rg" ]] || continue
        if [[ "$source_lc" == "/subscriptions/${SUB,,}/resourcegroups/${rg,,}/providers/microsoft.compute/disks/"* ]]; then
          die "Snapshot copy is nonterminal and still references owned compute disk(s); preserving compute: $name ($state)."
        fi
      done
      [[ -n "$source" ]] || die "Snapshot copy is nonterminal and its source is unknown; preserving compute: $name ($state)."
    fi
  done < <(jq -r '.[] | [.id,.name] | @tsv' <<<"$listing")
}
if group_exists "$RG"; then
  test_group="$(show_group "$RG")" || die 'Cannot inspect test resource group.'
  jq -e --arg t "$TOKEN" --arg n "$RG" --arg l "${LOC,,}" '.name==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro"' <<<"$test_group" >/dev/null || die 'Test resource group ownership did not verify.'
fi
if [[ -n "$NODE_RG" ]] && group_exists "$NODE_RG"; then
  node_group="$(show_group "$NODE_RG")" || die 'Cannot inspect node resource group.'
  jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.name==$n and .tags.reprotoken==$t' <<<"$node_group" >/dev/null || die 'Node resource group ownership did not verify.'
fi
assert_no_active_disk_copy
assert_no_snapshots "$REQUESTED"; assert_no_snapshots "$RG"
ra_id="$(jq -r '.snapshotRoleAssignment.id // empty' "$SF")"
ra_principal="$(jq -r '.snapshotRoleAssignment.principalId // empty' "$SF")"
ra_role='7efff54f-a5b4-42b5-a1c5-5411624893ce'
ra_status=not_configured; ra_reason=''; ra_residual=false
ra_record='{"status":"not_configured"}'
if [[ -n "$ra_id" ]]; then
  [[ "$ra_principal" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'Saved snapshot role assignment principal is invalid.'
  ra_list="$(azc role assignment list --scope "$SNAP_RG_ID" -o json)" || die 'Cannot inspect exact-scope snapshot role assignments.'
  ra_matches="$(jq -c --arg id "${ra_id,,}" '[.[]|select((.id|ascii_downcase)==$id)]' <<<"$ra_list")"
  ra_count="$(jq length <<<"$ra_matches")"
  if [[ "$ra_count" == 0 ]]; then
    ra_status=already_absent
  elif [[ "$ra_count" == 1 ]]; then
    jq -e --arg id "${ra_id,,}" --arg p "${ra_principal,,}" --arg s "${SNAP_RG_ID,,}" --arg role "$ra_role" '((.[0].id|ascii_downcase)==$id) and ((.[0].principalId|ascii_downcase)==$p) and ((.[0].scope|ascii_downcase)==$s) and ((.[0].roleDefinitionId|ascii_downcase)|endswith($role))' <<<"$ra_matches" >/dev/null || die 'Snapshot role assignment scope, principal, or role did not verify.'
    ra_err="$(mktemp "$SD/.role.XXXXXX")"; chmod 600 "$ra_err"
    if azc role assignment delete --ids "$ra_id" -o none >"$ra_err" 2>&1; then
      ra_after="$(azc role assignment list --scope "$SNAP_RG_ID" -o json)" || die 'Cannot verify snapshot role assignment deletion.'
      ra_matches="$(jq -c --arg id "${ra_id,,}" '[.[]|select((.id|ascii_downcase)==$id)]' <<<"$ra_after")"
      [[ "$(jq length <<<"$ra_matches")" == 0 ]] || die 'Snapshot role assignment remains after successful delete.'
      ra_status=deleted
    else
      if grep -Eiq 'ScopeLocked|Scope.?Lock|scope.*locked' "$ra_err"; then
        ra_reason=ScopeLocked
        ra_after="$(azc role assignment list --scope "$SNAP_RG_ID" -o json)" || die 'Cannot verify scope-locked snapshot role assignment.'
        ra_matches="$(jq -c --arg id "${ra_id,,}" '[.[]|select((.id|ascii_downcase)==$id)]' <<<"$ra_after")"
        case "$(jq length <<<"$ra_matches")" in
          0) ra_status=already_absent ;;
          1)
            jq -e --arg id "${ra_id,,}" --arg p "${ra_principal,,}" --arg s "${SNAP_RG_ID,,}" --arg role "$ra_role" '((.[0].id|ascii_downcase)==$id) and ((.[0].principalId|ascii_downcase)==$p) and ((.[0].scope|ascii_downcase)==$s) and ((.[0].roleDefinitionId|ascii_downcase)|endswith($role))' <<<"$ra_matches" >/dev/null || die 'Scope-locked snapshot role assignment changed identity.'
            ra_status=retained_scope_locked; ra_residual=true ;;
          *) die 'Multiple exact-ID snapshot role assignments were returned.' ;;
        esac
      elif grep -Eiq 'RoleAssignmentNotFound|NotFound|could not be found|404' "$ra_err"; then
        ra_after="$(azc role assignment list --scope "$SNAP_RG_ID" -o json)" || die 'Cannot verify missing snapshot role assignment.'
        ra_matches="$(jq -c --arg id "${ra_id,,}" '[.[]|select((.id|ascii_downcase)==$id)]' <<<"$ra_after")"
        [[ "$(jq length <<<"$ra_matches")" == 0 ]] || die 'Role assignment delete reported not found but the assignment still exists.'
        ra_status=already_absent
      else
        python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$ra_err"
        die 'Cannot remove the verified snapshot role assignment; compute cleanup has not started.'
      fi
    fi
    python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$ra_err"
  elif [[ "$ra_count" -gt 1 ]]; then
    die 'Multiple exact-ID snapshot role assignments were returned.'
  else
    die 'Invalid role assignment list result.'
  fi
  ra_record="$(jq -n --arg status "$ra_status" --arg id "$ra_id" --arg principal "$ra_principal" --arg scope "$SNAP_RG_ID" --arg role "$ra_role" --arg reason "$ra_reason" --argjson residual "$ra_residual" '{status:$status,id:$id,principalId:$principal,scope:$scope,roleDefinitionId:$role,residual:$residual} + (if $reason=="" then {} else {reason:$reason,protectedByCanNotDeleteLock:true} end)')"
fi
aks="$(show_aks)"
aks_gone_verified=false
if [[ -n "$aks" ]]; then
  jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.tags.reprotoken==$t and .nodeResourceGroup==$n' <<<"$aks" >/dev/null || die 'AKS owner or Azure-returned node resource group differs.'
  state="$(jq -r '.provisioningState // empty' <<<"$aks")"
  if [[ "$state" != Deleting ]]; then
    [[ "$state" == Succeeded || "$state" == Failed || "$state" == Canceled || "$state" == Cancelled || "$state" == Creating || "$state" == Updating ]] || die "Unknown AKS state '$state'."
    azlong aks delete -g "$RG" -n "$AKS" --yes --no-wait -o none || die 'AKS delete failed.'
  fi
  wait_aks_gone
  aks_gone_verified=true
else
  aks_gone_verified=true
fi
if [[ "$ra_residual" == true ]]; then
  [[ "$aks_gone_verified" == true ]] || die 'Cannot record the scope-locked assignment until its AKS principal is deleted.'
  ra_record="$(jq '.principalDeletedWithAks=true' <<<"$ra_record")"
fi
assert_no_active_disk_copy
assert_no_snapshots "$REQUESTED"; assert_no_snapshots "$RG"
if group_exists "$REQUESTED"; then
  node_group="$(show_group "$REQUESTED")" || die 'Cannot recheck node resource group.'
  jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.name==$n and .tags.reprotoken==$t' <<<"$node_group" >/dev/null || die 'Node resource group ownership changed.'
  azlong group delete -n "$REQUESTED" --yes --no-wait -o none || die 'Node resource group delete failed.'
  wait_rg_gone "$REQUESTED"
fi
assert_no_active_disk_copy
assert_no_snapshots "$RG"
if group_exists "$RG"; then
  test_group="$(show_group "$RG")" || die 'Cannot recheck test resource group.'
  jq -e --arg t "$TOKEN" --arg n "$RG" '.name==$n and .tags.reprotoken==$t' <<<"$test_group" >/dev/null || die 'Test resource group ownership changed.'
  azlong group delete -n "$RG" --yes --no-wait -o none || die 'Test resource group delete failed.'
  wait_rg_gone "$RG"
fi

[[ ! -e "$KC" ]] || python3 -c 'import os,sys; os.unlink(sys.argv[1])' "$KC"
jq -n --arg t "$TOKEN" --arg sub "$SUB" --arg rg "$RG" --arg node "$REQUESTED" --arg snaps "$SNAP_RG_ID" --arg time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson records "$retained" --argjson assignment "$ra_record" '{version:1,status:"complete",ownerToken:$t,subscriptionId:$sub,deletedTestResourceGroup:$rg,deletedNodeResourceGroup:$node,retainedSnapshotResourceGroupId:$snaps,retainedSnapshots:$records,snapshotRoleAssignment:$assignment,completedAtUtc:$time}' >"$SD/teardown.json"; chmod 600 "$SD/teardown.json"
printf 'Removed owned AKS, node, and test resources. Retained %s snapshots in the locked resource group %s.\n' "$(jq length <<<"$retained")" "$SNAP_RG"
