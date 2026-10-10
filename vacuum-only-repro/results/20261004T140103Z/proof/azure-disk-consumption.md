# Azure VMSS disk target and consumption (14:01–16:00 UTC)

Captured the VMSS per-LUN metrics for the complete observed window from 2026-10-04 14:01 UTC through the latest complete one-minute point at 16:00 UTC. The request covered 14:01–16:01:06 UTC at PT1M; Azure had 120 consumed-metric points per disk through 16:00. The target metrics were sparse (24 observations per series), but all observed target values were constant and the latest available values were at 15:59 UTC.

Azure reported a per-disk baseline target of **550 IOPS and 100,000,000 bytes/s (100 MB/s decimal)** on all four LUNs. These are Azure Monitor “Data Disk Target” metrics, whose definitions describe baseline without bursting; they differ from Standard_LRS disk SKU nominal values. Estimated consumed rates below are the consumed-percentage minute average multiplied by the observed target. All-window mean includes the run’s warm-up/baseline and later VACUUM. “Last 5m” is 15:56–16:00 inclusive; “latest” is the 16:00 one-minute average.

| Pod role / disk | VMSS metric dimensions | PVC / LUN | Mean consumed | Last 5m mean | Latest minute |
|---|---|---|---:|---:|---:|
| Primary DATA | `vmname=aks-nodepool1-37028789-vmss_0`, `lun=0` | `hdd-pg-1`, LUN 0 | 116 IOPS (21.0%); 26.4 MB/s (26.4%) | 92 IOPS (16.8%); 21.4 MB/s (21.4%) | 94 IOPS (17%); 22 MB/s (22%) |
| Primary WAL | `vmname=aks-nodepool1-37028789-vmss_0`, `lun=1` | `hdd-pg-1-wal`, LUN 1 | 54 IOPS (9.7%); 10.9 MB/s (10.9%) | 44 IOPS (8%); 11 MB/s (11%) | 44 IOPS (8%); 11 MB/s (11%) |
| Replica DATA | `vmname=aks-nodepool1-37028789-vmss_1`, `lun=0` | `hdd-pg-2`, LUN 0 | 522 IOPS (94.9%); 7.9 MB/s (7.9%) | 534 IOPS (97%); 8 MB/s (8%) | 534 IOPS (97%); 8 MB/s (8%) |
| Replica WAL | `vmname=aks-nodepool1-37028789-vmss_1`, `lun=1` | `hdd-pg-2-wal`, LUN 1 | 103 IOPS (18.8%); 25.0 MB/s (25.0%) | 98 IOPS (17.8%); 25.6 MB/s (25.6%) | 99 IOPS (18%); 26 MB/s (26%) |

The dimension-to-disk mapping was cross-checked against saved VMSS instance data: instance `_0` is node `aks-nodepool1-37028789-vmss000000` and instance `_1` is `...vmss000001`. Both nodes are `Standard_D2s_v4` with SCSI controllers; each LUN is a separate 512-GiB Standard_LRS disk with caching `None`. The latest local sampler row maps `hdd-pg-1` (primary) to instance 0 and `hdd-pg-2` (replica) to instance 1. Full managed-disk IDs and PVC/PV mapping are in `vmss-disk-target-consumption-summary-140100Z-to-160106Z.json` and `vmss-instances.json`.

The replica DATA LUN’s approximately 95% all-window and 97% latest consumed-IOPS averages corroborate approach to the observed 550-IOPS baseline target while using only about 8 MB/s of its 100-MB/s bandwidth target. Primary DATA remains well below the same IOPS and bandwidth baselines. These minute averages do not establish a hard cap, individual-I/O latency, or latency percentiles.

Raw query: `vmss-disk-metrics-140100Z-to-160106Z.json`. Metric descriptions/dimensions: `vmss-disk-metric-definitions-current.json`. Query metadata and per-series calculations: `vmss-disk-target-consumption-summary-140100Z-to-160106Z.json`.
