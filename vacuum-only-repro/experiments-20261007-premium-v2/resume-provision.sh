#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Resume only the post-AKS portion of an interrupted vacuum-only-repro stack.
# There is deliberately no teardown/delete path. Azure writes require --execute.

readonly STACKS_ROOT="/workspace/vacuum-only-repro/experiments-20261007-premium-v2"
readonly EXPECTED_SUB="00000000-0000-0000-0000-000000000000"
readonly CNPG_MANIFEST="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/releases/cnpg-1.30.1.yaml"
readonly SNAPSHOT_ROLE_ID="7efff54f-a5b4-42b5-a1c5-5411624893ce"
readonly FIELD_SEP=$'\x1f'

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  printf 'Usage: %s STACK_PATH [--execute]\n' "$0"
  printf 'Without --execute, only local stack metadata is checked; no Azure or Kubernetes command runs.\n'
}

if (( $# < 1 || $# > 2 )); then usage >&2; exit 2; fi
STACK_DIR="$(realpath -e -- "$1")" || die 'Stack path does not exist.'
case "$STACK_DIR" in
  "$STACKS_ROOT/50gib"|"$STACKS_ROOT/100gib") ;;
  *) die 'Stack path must resolve exactly to the 50gib or 100gib experiment directory.' ;;
esac
EXECUTE=false
if (( $# == 2 )); then
  [[ "$2" == "--execute" ]] || { usage >&2; exit 2; }
  EXECUTE=true
fi

SD="$STACK_DIR/.state"
SF="$SD/state.json"
STORAGE_FILE="$SD/storage.json"
LOCK_FILE="$SD/provision.lock"
[[ -f "$SF" && -f "$STORAGE_FILE" && -f "$LOCK_FILE" ]] || die 'Required stack state, storage metadata, or provision lock is missing.'
[[ "$(stat -c '%a' "$SF")" == "600" && "$(stat -c '%a' "$STORAGE_FILE")" == "600" ]] || die 'State metadata permissions are not restricted to the stack owner.'

jq -e 'has("version") and has("subscriptionId") and has("resourceGroup")
  and has("clusterName") and has("requestedNodeResourceGroup")
  and has("snapshotResourceGroup") and has("snapshotResourceGroupId")
  and has("availabilityZone") and has("location") and has("ownerToken")
  and has("kubeconfigPath") and has("status")' "$SF" >/dev/null || die 'Stack state has an unexpected schema.'
IFS="$FIELD_SEP" read -r VERSION SUB RG AKS RECORDED_NODE_RG REQUESTED_NODE_RG SNAP_RG SNAP_RG_ID ZONE LOCATION TOKEN KC STATUS \
  < <(jq -r '[.version, .subscriptionId, .resourceGroup, .clusterName, .nodeResourceGroup,
    .requestedNodeResourceGroup, .snapshotResourceGroup, .snapshotResourceGroupId,
    .availabilityZone, .location, .ownerToken, .kubeconfigPath, .status]
    | map(if . == null then "" else tostring end) | join("\u001f")' "$SF")
[[ "$VERSION" == "1" && "$SUB" == "$EXPECTED_SUB" ]] || die 'Stack version or subscription differs from this experiment.'
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die 'Stack owner token is missing or malformed.'
[[ "$LOCATION" =~ ^[A-Za-z0-9-]+$ && "$ZONE" =~ ^[1-3]$ ]] || die 'Stack location or availability zone is malformed.'
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$REQUESTED_NODE_RG" == "rg-vacuum-nodes-$TOKEN" ]] || die 'Stack resource group names do not match the local ownership token.'
TOKEN_PREFIX="$(cut -c1-12 <<<"$TOKEN")"
[[ "$AKS" == "aks-vacuum-only-$TOKEN_PREFIX" ]] || die 'AKS name does not match the local ownership token.'
[[ "$SNAP_RG" == "rg-vacuum-pv2-snap-$TOKEN" ]] || die 'Snapshot resource group name does not match the local ownership token.'
EXPECTED_SNAP_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAP_RG"
[[ "$(tr '[:upper:]' '[:lower:]' <<<"$SNAP_RG_ID")" == "$(tr '[:upper:]' '[:lower:]' <<<"$EXPECTED_SNAP_RG_ID")" ]] || die 'Snapshot resource group ID does not match local state.'
[[ "$KC" == "$SD/kubeconfig" ]] || die 'Kubeconfig path is not isolated to this stack.'
[[ -z "$RECORDED_NODE_RG" || "$RECORDED_NODE_RG" == "$REQUESTED_NODE_RG" ]] || die 'Recorded AKS node resource group conflicts with the requested name.'
[[ "$STATUS" == "provisioning" || "$STATUS" == "ready" ]] || die 'Stack state is not a resumable post-AKS state.'

jq -e 'has("status") and has("subscriptionId") and has("resourceGroup")
  and has("location") and has("ownerToken") and has("accountName")
  and has("accountId") and has("containerName") and has("secret")' "$STORAGE_FILE" >/dev/null || die 'Storage metadata has an unexpected schema.'
IFS="$FIELD_SEP" read -r STORAGE_STATUS STORAGE_SUB STORAGE_RG STORAGE_LOC STORAGE_TOKEN SA SA_ID CONTAINER SECRET_NAMESPACE SECRET_NAME SECRET_KEY \
  < <(jq -r '[.status, .subscriptionId, .resourceGroup, .location, .ownerToken,
    .accountName, .accountId, .containerName, .secret.namespace, .secret.name, .secret.key]
    | map(if . == null then "" else tostring end) | join("\u001f")' "$STORAGE_FILE")
EXPECTED_SA_ID="/subscriptions/$SUB/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/$SA"
[[ "$STORAGE_STATUS" == "ready" && "$STORAGE_SUB" == "$SUB" && "$STORAGE_RG" == "$RG" && "$STORAGE_LOC" == "$LOCATION" && "$STORAGE_TOKEN" == "$TOKEN" ]] || die 'Storage metadata does not match this stack.'
[[ "$SA" =~ ^cnpgwal[a-z0-9]{17}$ && "$CONTAINER" == "wal-$TOKEN_PREFIX" ]] || die 'Storage account or container name does not match this stack.'
[[ "$(tr '[:upper:]' '[:lower:]' <<<"$SA_ID")" == "$(tr '[:upper:]' '[:lower:]' <<<"$EXPECTED_SA_ID")" ]] || die 'Storage account ID does not match local state.'
[[ "$SECRET_NAMESPACE" == "repro" && "$SECRET_NAME" == "azure-barman-credentials" && "$SECRET_KEY" == "connection-string" ]] || die 'Archive Secret metadata differs from the expected stack contract.'

if [[ "$EXECUTE" != true ]]; then
  printf 'Local metadata is consistent for stack %s (%s, zone %s).\n' "$(basename "$STACK_DIR")" "$LOCATION" "$ZONE"
  printf 'No Azure or Kubernetes command was run. Review this helper, then rerun with --execute to resume.\n'
  exit 0
fi
[[ "$STATUS" != "ready" ]] || { printf 'Stack is already marked ready: %s\n' "$SF"; exit 0; }
for x in az kubectl jq timeout flock; do command -v "$x" >/dev/null || die "Missing required command: $x"; done
exec 9>"$LOCK_FILE"
flock -n 9 || die 'Another provision process holds this stack lock.'

# The CLI wrapper serializes credentials with the live provisioning watcher.
azc() { timeout --foreground --kill-after=15s 900s az "$@" --subscription "$SUB"; }
azlong() { timeout --foreground --kill-after=15s 3600s az "$@" --subscription "$SUB"; }
kube() { timeout --foreground --kill-after=15s 120s kubectl --kubeconfig "$KC" "$@"; }

SECRET_FILE=""
SECRET_CLEAN_FILE=""
cleanup_local() {
  [[ -z "$SECRET_FILE" ]] || rm -f -- "$SECRET_FILE"
  [[ -z "$SECRET_CLEAN_FILE" ]] || rm -f -- "$SECRET_CLEAN_FILE"
  unset AZURE_STORAGE_CONNECTION_STRING || true
}
trap cleanup_local EXIT

ACTIVE_SUB="$(azc account show --query id -o tsv)" || die 'Cannot inspect the active Azure subscription.'
[[ "$(tr '[:upper:]' '[:lower:]' <<<"$ACTIVE_SUB")" == "$(tr '[:upper:]' '[:lower:]' <<<"$SUB")" ]] || die 'Active Azure subscription differs from stack state.'

# Verify resource ownership and preservation before any Azure write. This helper
# never creates or deletes resource groups, AKS, storage accounts, disks, or snapshots.
RG_JSON="$(azc group show --name "$RG" -o json)" || die 'Cannot inspect the stack resource group.'
jq -e --arg n "$RG" --arg l "$LOCATION" --arg t "$TOKEN" \
  '(.name|ascii_downcase)==($n|ascii_downcase) and (.location|ascii_downcase)==($l|ascii_downcase) and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro"' \
  <<<"$RG_JSON" >/dev/null || die 'Stack resource group name, location, or owner tags do not verify.'

SNAP_RG_JSON="$(azc group show --name "$SNAP_RG" -o json)" || die 'Cannot inspect the stack snapshot resource group.'
jq -e --arg n "$SNAP_RG" --arg l "$LOCATION" --arg t "$TOKEN" \
  '(.name|ascii_downcase)==($n|ascii_downcase) and (.location|ascii_downcase)==($l|ascii_downcase) and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-retained-snapshots"' \
  <<<"$SNAP_RG_JSON" >/dev/null || die 'Snapshot resource group identity or owner tags do not verify.'
LOCKS="$(azc lock list --resource-group "$SNAP_RG" -o json)" || die 'Cannot inspect the snapshot preservation lock.'
jq -e --arg scope "$(tr '[:upper:]' '[:lower:]' <<<"$SNAP_RG_ID")" \
  'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$scope))' \
  <<<"$LOCKS" >/dev/null || die 'Snapshot resource group lacks its expected CanNotDelete lock.'

AKS_JSON="$(azc aks show --resource-group "$RG" --name "$AKS" -o json)" || die 'Cannot inspect the stack AKS cluster.'
jq -e --arg sub "$SUB" --arg rg "$RG" --arg n "$AKS" --arg node "$REQUESTED_NODE_RG" \
  --arg t "$TOKEN" --arg loc "$LOCATION" --arg zone "$ZONE" \
  '(.id|ascii_downcase)==(("/subscriptions/"+$sub+"/resourcegroups/"+$rg+"/providers/microsoft.containerservice/managedclusters/"+$n)|ascii_downcase)
   and (.resourceGroup|ascii_downcase)==($rg|ascii_downcase)
   and (.name|ascii_downcase)==($n|ascii_downcase)
   and .location==$loc and .provisioningState=="Succeeded"
   and .tags.reprotoken==$t and .nodeResourceGroup==$node
   and (.kubernetesVersion|startswith("1.35"))
   and (.agentPoolProfiles|length)==1
   and .agentPoolProfiles[0].count==2
   and .agentPoolProfiles[0].vmSize=="Standard_D2s_v4"
   and .agentPoolProfiles[0].mode=="System"
   and any(.agentPoolProfiles[0].availabilityZones[]?; tostring==$zone)
   and (.identity.principalId|type=="string" and length>0)' \
  <<<"$AKS_JSON" >/dev/null || die 'AKS identity, owner tag, readiness, node pool, VM size, or zone does not verify.'
NODE_RG="$(jq -r '.nodeResourceGroup' <<<"$AKS_JSON")"
PRINCIPAL="$(jq -r '.identity.principalId' <<<"$AKS_JSON")"
[[ "$NODE_RG" == "$REQUESTED_NODE_RG" && "$PRINCIPAL" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'AKS node resource group or control-plane identity is malformed.'

STORAGE_JSON="$(azc storage account show --resource-group "$RG" --name "$SA" -o json)" || die 'Cannot inspect the WAL archive storage account.'
jq -e --arg id "$(tr '[:upper:]' '[:lower:]' <<<"$SA_ID")" --arg rg "$RG" --arg loc "$LOCATION" --arg n "$SA" --arg t "$TOKEN" \
  '(.id|ascii_downcase)==$id and (.name|ascii_downcase)==($n|ascii_downcase)
   and (.resourceGroup|ascii_downcase)==($rg|ascii_downcase)
   and (.location|ascii_downcase)==($loc|ascii_downcase)
   and .provisioningState=="Succeeded" and .kind=="StorageV2" and .sku.name=="Standard_LRS"
   and .allowBlobPublicAccess==false and .enableHttpsTrafficOnly==true
   and .minimumTlsVersion=="TLS1_2" and .tags.reprotoken==$t
   and .tags.reproStack=="vacuum-only-repro"' \
  <<<"$STORAGE_JSON" >/dev/null || die 'WAL archive storage account identity, ownership, or privacy settings do not verify.'

# Retrieve the existing archive credential only to a mode-600 file in this
# stack's private state directory. It is never printed and is removed on exit.
SECRET_FILE="$(mktemp "$SD/.resume-connection.XXXXXX")"
chmod 600 "$SECRET_FILE"
azc storage account show-connection-string --resource-group "$RG" --name "$SA" --query connectionString -o tsv >"$SECRET_FILE" || die 'Cannot retrieve the existing WAL archive credentials.'
SECRET_CLEAN_FILE="$SECRET_FILE.clean"
tr -d '\r\n' <"$SECRET_FILE" >"$SECRET_CLEAN_FILE"
mv -f "$SECRET_CLEAN_FILE" "$SECRET_FILE"
SECRET_CLEAN_FILE=""
[[ -s "$SECRET_FILE" ]] || die 'Retrieved WAL archive credentials are empty.'
AZURE_STORAGE_CONNECTION_STRING="$(<"$SECRET_FILE")"
export AZURE_STORAGE_CONNECTION_STRING
CONTAINER_JSON="$(azc storage container show --name "$CONTAINER" -o json)" || die 'Cannot inspect the existing WAL archive container.'
jq -e --arg t "$TOKEN" '(.metadata.reprotoken==$t) and (.publicAccess==null or .publicAccess=="off")' \
  <<<"$CONTAINER_JSON" >/dev/null || die 'WAL archive container ownership or public-access settings do not verify.'

# The exact AKS identity above proves which node resource group is associated.
NODE_RG_JSON="$(azc group show --name "$NODE_RG" -o json)" || die 'Cannot inspect the AKS-associated node resource group.'
jq -e --arg n "$NODE_RG" --arg l "$LOCATION" --arg t "$TOKEN" \
  '(.name|ascii_downcase)==($n|ascii_downcase) and (.location|ascii_downcase)==($l|ascii_downcase)
   and ((.tags.reprotoken // "")=="" or .tags.reprotoken==$t)' \
  <<<"$NODE_RG_JSON" >/dev/null || die 'AKS-associated node resource group has an unexpected identity, location, or owner tag.'
if [[ "$(jq -r '.tags.reprotoken // empty' <<<"$NODE_RG_JSON")" != "$TOKEN" ]]; then
  azc group update --name "$NODE_RG" --set "tags.reprotoken=$TOKEN" -o none || die 'Cannot add this stack owner tag to its AKS-associated node resource group.'
  NODE_RG_JSON="$(azc group show --name "$NODE_RG" -o json)" || die 'Cannot verify the AKS node resource group tag.'
  jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$NODE_RG_JSON" >/dev/null || die 'AKS node resource group owner tag did not verify.'
fi

# Reuse an exact prior assignment, or grant only Disk Snapshot Contributor at
# this stack's own snapshot resource group.
ASSIGNMENTS="$(azc role assignment list --scope "$SNAP_RG_ID" -o json)" || die 'Cannot inspect snapshot resource group role assignments.'
ASSIGNMENT="$(jq -c --arg p "$(tr '[:upper:]' '[:lower:]' <<<"$PRINCIPAL")" \
  --arg s "$(tr '[:upper:]' '[:lower:]' <<<"$SNAP_RG_ID")" --arg role "$SNAPSHOT_ROLE_ID" \
  '[.[] | select((.principalId|ascii_downcase)==$p and (.scope|ascii_downcase)==$s and ((.roleDefinitionId|ascii_downcase)|endswith($role)))] | first // empty' \
  <<<"$ASSIGNMENTS")"
if [[ -z "$ASSIGNMENT" ]]; then
  ASSIGNMENT="$(azlong role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role 'Disk Snapshot Contributor' --scope "$SNAP_RG_ID" -o json)" || die 'Cannot grant the AKS identity scoped access to its snapshot resource group.'
fi
jq -e --arg p "$(tr '[:upper:]' '[:lower:]' <<<"$PRINCIPAL")" \
  --arg s "$(tr '[:upper:]' '[:lower:]' <<<"$SNAP_RG_ID")" --arg role "$SNAPSHOT_ROLE_ID" \
  '(.principalId|ascii_downcase)==$p and (.scope|ascii_downcase)==$s and ((.roleDefinitionId|ascii_downcase)|endswith($role))' \
  <<<"$ASSIGNMENT" >/dev/null || die 'Snapshot role assignment identity, role, or scope did not verify.'
ASSIGNMENT_ID="$(jq -r '.id' <<<"$ASSIGNMENT")"

# Use only the isolated kubeconfig path recorded in this stack's state.
azc aks get-credentials --resource-group "$RG" --name "$AKS" --file "$KC" --overwrite-existing -o none || die 'Cannot retrieve isolated AKS credentials.'
chmod 600 "$KC"
export KUBECONFIG="$KC"

NS_JSON="$(kube get namespace "$SECRET_NAMESPACE" --ignore-not-found -o json)"
if [[ -z "$NS_JSON" ]]; then
  kube create namespace "$SECRET_NAMESPACE" >/dev/null || die 'Cannot create the stack namespace.'
  NS_JSON="$(kube get namespace "$SECRET_NAMESPACE" -o json)" || die 'Cannot verify the stack namespace.'
fi
NS_OWNER="$(jq -r '.metadata.labels["repro.stack.token"] // empty' <<<"$NS_JSON")"
[[ -z "$NS_OWNER" || "$NS_OWNER" == "$TOKEN" ]] || die 'The stack namespace has a different owner label.'
if [[ "$NS_OWNER" != "$TOKEN" ]]; then
  kube label namespace "$SECRET_NAMESPACE" "repro.stack.token=$TOKEN" --overwrite=false >/dev/null || die 'Cannot label the stack namespace.'
fi

SECRET_JSON="$(kube -n "$SECRET_NAMESPACE" get secret "$SECRET_NAME" --ignore-not-found -o json)"
if [[ -z "$SECRET_JSON" ]]; then
  kube -n "$SECRET_NAMESPACE" create secret generic "$SECRET_NAME" --from-file="$SECRET_KEY=$SECRET_FILE" >/dev/null || die 'Cannot create the WAL archive Secret.'
  kube -n "$SECRET_NAMESPACE" label secret "$SECRET_NAME" "repro.stack.token=$TOKEN" --overwrite=false >/dev/null || die 'Cannot label the WAL archive Secret.'
  SECRET_JSON="$(kube -n "$SECRET_NAMESPACE" get secret "$SECRET_NAME" -o json)" || die 'Cannot verify the WAL archive Secret.'
fi
jq -e --arg t "$TOKEN" --arg key "$SECRET_KEY" \
  '.metadata.labels["repro.stack.token"]==$t and .type=="Opaque" and (.data|keys)==[$key]' \
  <<<"$SECRET_JSON" >/dev/null || die 'WAL archive Secret ownership or key did not verify.'
EXPECTED_SECRET_B64="$(base64 -w0 "$SECRET_FILE")"
ACTUAL_SECRET_B64="$(jq -r --arg key "$SECRET_KEY" '.data[$key]' <<<"$SECRET_JSON")"
[[ "$ACTUAL_SECRET_B64" == "$EXPECTED_SECRET_B64" ]] || die 'Existing WAL archive Secret does not match current stack storage credentials.'
unset AZURE_STORAGE_CONNECTION_STRING
rm -f -- "$SECRET_FILE"
SECRET_FILE=""

printf 'Installing CNPG 1.30.1 and verifying the Premium SSD v2 StorageClass.\n'
timeout --foreground --kill-after=15s 900s kubectl --kubeconfig "$KC" apply --server-side -f "$CNPG_MANIFEST" >/dev/null || die 'Cannot install CNPG 1.30.1.'
timeout --foreground --kill-after=15s 660s kubectl --kubeconfig "$KC" -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=10m >/dev/null || die 'CNPG operator did not become ready.'

SC_JSON="$(timeout --foreground --kill-after=15s 120s kubectl --kubeconfig "$KC" get storageclass azure-premiumv2 --ignore-not-found -o json)"
if [[ -z "$SC_JSON" ]]; then
  cat <<'YAML' | timeout --foreground --kill-after=15s 120s kubectl --kubeconfig "$KC" create -f - >/dev/null || die 'Cannot create the Premium SSD v2 StorageClass.'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: azure-premiumv2
  labels:
    app.kubernetes.io/part-of: vacuum-only-repro
provisioner: disk.csi.azure.com
parameters:
  skuName: PremiumV2_LRS
  cachingMode: None
  kind: Managed
  DiskIOPSReadWrite: "3000"
  DiskMBpsReadWrite: "125"
  LogicalSectorSize: "512"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
YAML
  SC_JSON="$(timeout --foreground --kill-after=15s 120s kubectl --kubeconfig "$KC" get storageclass azure-premiumv2 -o json)" || die 'Cannot inspect the created Premium SSD v2 StorageClass.'
fi
jq -e '.provisioner=="disk.csi.azure.com"
  and .metadata.labels["app.kubernetes.io/part-of"]=="vacuum-only-repro"
  and .parameters.skuName=="PremiumV2_LRS" and .parameters.cachingMode=="None"
  and .parameters.kind=="Managed" and .parameters.DiskIOPSReadWrite=="3000"
  and .parameters.DiskMBpsReadWrite=="125" and .parameters.LogicalSectorSize=="512"
  and .reclaimPolicy=="Delete" and .allowVolumeExpansion==true
  and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$SC_JSON" >/dev/null || die 'Premium SSD v2 StorageClass settings do not verify.'
CNPG_IMAGE="$(timeout --foreground --kill-after=15s 120s kubectl --kubeconfig "$KC" get deployment cnpg-controller-manager -n cnpg-system -o json | jq -r '[.spec.template.spec.containers[].image]|join(" ")')" || die 'Cannot verify the CNPG controller image.'
[[ "$CNPG_IMAGE" == *1.30.1* ]] || die 'CNPG controller image is not version 1.30.1.'

# Persist verified ownership metadata only; credentials never enter state.
TMP_STATE="$(mktemp "$SD/.state-resume.XXXXXX")"
jq --arg node "$NODE_RG" --arg assignmentId "$ASSIGNMENT_ID" --arg principal "$PRINCIPAL" \
  '.nodeResourceGroup=$node
   | .status="ready"
   | .snapshotRoleAssignment={id:$assignmentId,principalId:$principal,role:"Disk Snapshot Contributor",scope:.snapshotResourceGroupId}' \
  "$SF" >"$TMP_STATE"
chmod 600 "$TMP_STATE"
mv -f -- "$TMP_STATE" "$SF"
printf 'Resume complete for %s in %s zone %s; verified CNPG and Premium SSD v2 StorageClass.\n' "$(basename "$STACK_DIR")" "$LOCATION" "$ZONE"
printf 'All resources, snapshots, and failed operations were preserved; state: %s\n' "$SF"
