# Azure VMSS managed-disk metrics, full run through 18:21 UTC

Azure Monitor query window: 2026-10-04T14:01:00Z/2026-10-04T18:22:50Z; one-minute aggregation. Query completed 2026-10-04T18:22:51Z. Azure returned 261 non-null one-minute consumed-percentage points per LUN/instance through 18:21 UTC, plus 52 sparse target points per series (latest target point 18:18 or 18:19 UTC).

Metric dimensions are `lun` and `vmname`. Saved sampler/preflight mapping identifies VMSS instance `_0` as primary and `_1` as replica; LUN 0 is DATA and LUN 1 is WAL. Each disk below is a distinct 512-GiB `Standard_LRS` volume with caching `None`.

| Role / node | Disk / LUN / PV disk ID | Runtime target | Full-run mean consumed | Trailing five samples | Latest point |
|---|---|---:|---:|---:|---:|
| primary / `aks-nodepool1-37028789-vmss000000` | DATA LUN0<br>`/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg-vacuum-nodes-804391dc0a7b56b597915163951aed1a/providers/microsoft.compute/disks/pvc-b10487ad-93ff-4649-985d-7ee7a4488187` | 550 IOPS / 100 MB/s | 129 IOPS / 29.2 MB/s | 217 IOPS / 46.0 MB/s | 231 IOPS / 51.0 MB/s at 18:21 |
| replica / `aks-nodepool1-37028789-vmss000001` | DATA LUN0<br>`/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg-vacuum-nodes-804391dc0a7b56b597915163951aed1a/providers/microsoft.compute/disks/pvc-3b7742d3-59d4-4a51-b9cb-c1818ec7dbaf` | 550 IOPS / 100 MB/s | 528 IOPS / 8.1 MB/s | 534 IOPS / 8.2 MB/s | 534 IOPS / 9.0 MB/s at 18:21 |
| primary / `aks-nodepool1-37028789-vmss000000` | WAL LUN1<br>`/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg-vacuum-nodes-804391dc0a7b56b597915163951aed1a/providers/microsoft.compute/disks/pvc-a13e4b08-de7f-4b92-b504-85856421e5fe` | 550 IOPS / 100 MB/s | 59 IOPS / 11.1 MB/s | 82 IOPS / 11.4 MB/s | 104 IOPS / 14.0 MB/s at 18:21 |
| replica / `aks-nodepool1-37028789-vmss000001` | WAL LUN1<br>`/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg-vacuum-nodes-804391dc0a7b56b597915163951aed1a/providers/microsoft.compute/disks/pvc-620fb663-31c7-4cd8-a241-f60774c157b4` | 550 IOPS / 100 MB/s | 99 IOPS / 24.8 MB/s | 100 IOPS / 24.0 MB/s | 121 IOPS / 29.0 MB/s at 18:21 |

Consumed absolute rates are estimates computed as the Azure Monitor consumed percentage multiplied by the corresponding target metric. Target metrics remained 550 IOPS and 100,000,000 bytes/s per disk (100 MB/s decimal). These are minute aggregates; they do not establish hard caps, burst-credit state, per-I/O latency, or latency percentiles.
