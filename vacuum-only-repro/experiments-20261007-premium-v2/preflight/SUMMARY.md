# Premium SSD v2 preflight (read-only)

Checked at `2026-10-06T15:25:15Z` against subscription `00000000-0000-0000-0000-000000000000`. Azure metadata calls used read-only ARM/CLI operations. This agent did not create or alter Azure resources and did not submit a quota request. The VM SKU command through Azure CLI timed out; direct ARM `Microsoft.Compute/skus` queries succeeded.

## Placement and capacity

EastUS is the preferred common region because the retained source snapshot and the established 23 GiB baseline are there. Choose **zone 1** for all zonal worker pools and Premium SSD v2 disks. The live Compute SKU response lists both `Standard_D2s_v4` and `PremiumV2_LRS` in EastUS and WestUS3, with zones 1, 2, and 3 and no SKU restrictions. It reports Premium SSD v2 minimum size **1 GiB**, minimum performance **3,000 IOPS and 125 MB/s**, and maximum size 65,536 GiB. The Dsv4 VM family supports Premium SSD v2; the official Dsv4 size table names it explicitly.

The quota result prevents placing all six 2-vCPU nodes in EastUS under the current Standard DSv4 family limit. The root's narrow `az vm list-usage` read returned EastUS DSv4 0/10 cores (regional cores 0/24), and WestUS3 DSv4 0/10 (regional cores 0/10). Raising EastUS to 12 cores was rejected with `QuotaNotAvailableForResource`. Root's current plan of two EastUS stacks (23 GiB and 100 GiB; 8 DSv4 cores) plus one WestUS3 stack (50 GiB; 4 DSv4 cores) fits the per-region limits, but region is a confound for comparisons. Use zone 1 in each region; the identical zone number does not imply identical physical placement.

Premium SSD v2 disk quota is not a blocker. Live `Microsoft.Quota` usage and quota reads report:

| Region | PremiumV2 capacity | PremiumV2 disk count | Public IPs | AKS managed clusters |
| --- | ---: | ---: | ---: | ---: |
| EastUS | 0 / 102,400 GiB | 0 / 1,000 | 0 / 20 | 2 / 10 |
| WestUS3 | 0 / 1,048,576 GiB | 0 / 5,000 | 0 / 20 | 0 / 10 |

A requested 12 × 512 GiB is 6,144 GiB, below either regional PremiumV2 capacity quota. The two existing EastUS AKS clusters are Succeeded but Stopped, each has two `Standard_D2s_v4` nodes and Kubernetes 1.35. Adding two EastUS clusters and one WestUS3 cluster gives 4/10 and 1/10 against the live AKS cluster quotas. The orchestrator API reports Kubernetes 1.35.8 as the default in both regions and 1.36.4 as latest stable; the `managed-csi-premium-v2` built-in StorageClass is available starting in Kubernetes 1.35.

## Snapshot restore path

The retained `azhr-keep-scsi-prepared-7a297601434285b1` is a **512 GiB, EastUS, Standard_LRS, full (non-incremental) snapshot**, Succeeded and created with `Copy` from the prior `vacuum-copy-804391dc` snapshot. Its old source resource group currently returns `ResourceGroupNotFound` (per the parallel read-only source review). The live resource does not expose `LogicalSectorSize`; the 512-byte logical-sector requirement must be proven from the selected incremental keeper metadata or restored disk before PremiumV2 use.

For the active three-stack placement, use the already retained region-local incremental bases: EastUS `keeper-base-16244c2c` and WestUS3 `keeper-16244c2c`, in their owned snapshot resource group. Each stack should make only its same-region stack-owned `CopyStart` snapshot from the matching regional base, wait for its copy to complete, then create the zonal PremiumV2 disk in zone 1. This avoids a new cross-region snapshot copy and keeps the restore source aligned with each region. Set `LogicalSectorSize: "512"` on the custom PremiumV2 StorageClass and verify the live disk reports `creationData.logicalSectorSize=512` before CNPG bootstrap.

The full Standard_LRS keeper is not itself a valid direct source for creating Premium SSD v2. Azure documents that snapshot-based migration from Standard HDD/SSD or Premium requires an incremental snapshot, and the source logical sector size must be 512. Azure also documents a direct type switch for an existing disk if its logical sector size is 512; its copy/conversion must finish before performance measurement. The CSI implementation at tag `v1.35.0` passes `LogicalSectorSize` from the custom StorageClass into PremiumV2 disk `CreationData` when supplied; with the field omitted it leaves it unset. The public driver parameter table labels this field Ultra-only, despite the v1.35.0 code accepting it for PremiumV2, so automatic 512-sector inheritance on PVC restore from a Standard snapshot is **unverified**. Explicitly set 512 and check the resulting disk. The current approved run uses the retained incremental regional sources above. All incremental and `CopyStart` snapshots must remain in owned snapshot resource groups under the current preserve-all-snapshots instruction; stack/resource-group teardown must not remove them.

The pre-existing baseline at 15:25Z contained 32 snapshot resources across 8 resource groups, two AKS clusters, and 10 total resource groups. The lock listing returned 32 `CanNotDelete` locks named `preserve-all-snapshots` with notes to preserve every snapshot. This inventory predates the root's current three stack provisions; refresh the snapshot/lock inventory after they settle, preserving all snapshots.

## Saved evidence

- `eastus-compute-skus.json`, `westus3-compute-skus.json`: direct Compute SKU metadata for D2s_v4 and PremiumV2, including zones and capabilities.
- `eastus-premiumv2-quota-summary.json`, `westus3-premiumv2-quota-summary.json`: PremiumV2 quota/current usage.
- `eastus-network-quotas.json`, `westus3-network-quotas.json`: public IP and NIC usage.
- `eastus-aks-usages-raw.json`, `westus3-aks-usages.json`, `eastus-aks-orchestrators.json`, `westus3-aks-orchestrators.json`, `current-aks-inventory.json`: AKS quotas, available Kubernetes versions, and existing stopped clusters.
- `keeper-snapshot.json`, `regional-keepers.json`: keeper and existing region-local source metadata without ownership tokens.
- `selected-resources.json`, `resource-groups.json`, `subscription-locks.json`: pre-provision inventory; ownership-token tag values were removed from the saved summaries.
- `az-version.json`, `target-locations.json`: CLI version and region metadata.

## Official references

- [Deploy Premium SSD v2: supported regions, zones, and VM placement](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-deploy-premium-v2)
- [Dsv4 VM sizes and supported disk types](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series)
- [Convert disks to Premium SSD v2; full snapshots are not supported for snapshot migration and logical sector size must be 512](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-convert-types)
- [Incremental snapshot restrictions, sector-size metadata, and background copy completion](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-incremental-snapshots)
- [Copy incremental snapshots across regions; full snapshots cannot be copied across regions](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-copy-incremental-snapshot-across-regions)
- [AKS Premium SSD v2 support](https://learn.microsoft.com/en-us/azure/aks/use-premium-v2-disks)
- [Azure Disk CSI v1.35.0 implementation of PV2 logical-sector-size handling](https://github.com/kubernetes-sigs/azuredisk-csi-driver/blob/v1.35.0/pkg/azureutils/azure_disk_utils.go)
- [Azure Disk CSI v1.35.0 CreateVolume parameter handoff](https://github.com/kubernetes-sigs/azuredisk-csi-driver/blob/v1.35.0/pkg/azuredisk/controllerserver.go)
- [AKS StorageClasses and version 1.35 availability](https://learn.microsoft.com/en-us/azure/aks/concepts-storage)
- [AKS cluster quotas and limits](https://learn.microsoft.com/en-us/azure/aks/quotas-skus-regions)
- [Managed disk quota and scalability limits](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-scalability-targets)
- [Network usage REST API](https://learn.microsoft.com/en-us/rest/api/virtualnetwork/usages/list?view=rest-virtualnetwork-2025-05-01)
