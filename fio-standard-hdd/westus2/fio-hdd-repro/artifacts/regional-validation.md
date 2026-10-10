# West US 2 fio final validation

Read-only artifact check. No Azure CLI or guest commands were invoked.

## 4 KiB: PASS (15/15 exact points)
Controller complete: True; successful points: [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 768, 1024, 1536, 2048, 3072]; missing: [].
VM/disk: Standard_E2bs_v5, SCSI, LUN0 cache=None, Standard_LRS 512GiB.
Fixture files: 16; 16.0GiB logical / 16.000061GiB allocated; block=4096; iodepth=1; queue={'scheduler': '[none] mq-deadline', 'device_queue_depth': '316', 'nr_requests': '316'}.

| threads | status | read bytes | write | trim | errors | iostat/vmstat/common | LUN0 latency PT1M full/guarded |
|---:|---|---:|---:|---:|---|---:|---:|
| 1 | ok | 15351808 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 2 | ok | 35348480 | 0 | 0 | [0] | 179/180/180 | 1/2 (guard 1/1) |
| 4 | ok | 83251200 | 0 | 0 | [0] | 179/180/180 | 1/2 (guard 0/1) |
| 8 | ok | 217128960 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 16 | ok | 413155328 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 32 | ok | 413659136 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 64 | ok | 413753344 | 0 | 0 | [0] | 179/180/180 | 1/2 (guard 1/1) |
| 128 | ok | 413880320 | 0 | 0 | [0] | 179/180/180 | 1/2 (guard 0/1) |
| 256 | ok | 412925952 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 512 | ok | 414896128 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 768 | ok | 434929664 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 1024 | ok | 474042368 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 1536 | ok | 539643904 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 2048 | ok | 634523648 | 0 | 0 | [0] | 179/180/180 | 1/2 (guard 1/1) |
| 3072 | ok | 802201600 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |

Maximum Azure Average over complete PT1M buckets inside main measurement windows:
- data disk bandwidth consumed percentage: 2.0000 %, LUN 0 (30 points)
- data disk iops consumed percentage: 100.0000 %, LUN 0 (30 points)
- data disk latency: 456.3190 ms, LUN 0 (25 points)
- data disk queue depth: 256.0300 requests, LUN 0 (30 points)
- data disk read bytes/sec: 2.2989 MB/s, LUN 0 (30 points)
- data disk read operations/sec: 561.2500 IOPS, LUN 0 (23 points)
- data disk target bandwidth: 100.0000 MB/s, LUN 0 (7 points)
- data disk target iops: 550.0000 IOPS, LUN 0 (7 points)
- data disk write bytes/sec: 0.0000 MB/s, LUN 0 (23 points)
- data disk write operations/sec: 0.0000 IOPS, LUN 0 (29 points)
- percentage cpu: 4.3850 % (30 points)
- vm remote used burst bps credits percentage: 0.0000  (9 points)
- vm remote used burst io credits percentage: 0.0000  (9 points)
- vm uncached bandwidth consumed percentage: 1.0000 % (30 points)
- vm uncached iops consumed percentage: 9.0000 % (30 points)

Warnings: 4 KiB run-2: missing LUN 0 latency buckets ['2026-10-09T10:01:00Z']; 4 KiB run-4: missing LUN 0 latency buckets ['2026-10-09T10:06:00Z']; 4 KiB run-64: missing LUN 0 latency buckets ['2026-10-09T10:21:00Z']; 4 KiB run-128: missing LUN 0 latency buckets ['2026-10-09T10:26:00Z']; 4 KiB run-2048: missing LUN 0 latency buckets ['2026-10-09T10:51:00Z']

## 1 MiB: PASS (14/14 exact points)
Controller complete: True; successful points: [1, 2, 4, 8, 16, 32, 64, 128, 144, 160, 256, 512, 768, 1024]; missing: [].
VM/disk: Standard_E2bs_v5, SCSI, LUN0 cache=None, Standard_LRS 512GiB.
Fixture files: 16; 16.0GiB logical / 16.000061GiB allocated; block=1048576; iodepth=1; queue={'scheduler': '[none] mq-deadline', 'device_queue_depth': '316', 'nr_requests': '316'}.

| threads | status | read bytes | write | trim | errors | iostat/vmstat/common | LUN0 latency PT1M full/guarded |
|---:|---|---:|---:|---:|---|---:|---:|
| 1 | ok | 3057647616 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 2 | ok | 6349127680 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 4 | ok | 14260633600 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 8 | ok | 18363711488 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 16 | ok | 18368954368 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 32 | ok | 18375245824 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 64 | ok | 18397265920 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 128 | ok | 18415091712 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 144 | ok | 18349031424 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 160 | ok | 18453889024 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 256 | ok | 18522046464 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 512 | ok | 18653118464 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 768 | ok | 25221398528 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |
| 1024 | ok | 35877027840 | 0 | 0 | [0] | 179/180/180 | 2/2 (guard 1/1) |

Maximum Azure Average over complete PT1M buckets inside main measurement windows:
- data disk bandwidth consumed percentage: 100.0000 %, LUN 0 (28 points)
- data disk iops consumed percentage: 17.0000 %, LUN 0 (28 points)
- data disk latency: 2631.7224 ms, LUN 0 (28 points)
- data disk queue depth: 255.9500 requests, LUN 0 (27 points)
- data disk read bytes/sec: 102.0282 MB/s, LUN 0 (27 points)
- data disk read operations/sec: 97.3000 IOPS, LUN 0 (27 points)
- data disk target bandwidth: 100.0000 MB/s, LUN 0 (7 points)
- data disk target iops: 550.0000 IOPS, LUN 0 (7 points)
- data disk write bytes/sec: 0.0000 MB/s, LUN 0 (27 points)
- data disk write operations/sec: 0.0000 IOPS, LUN 0 (27 points)
- percentage cpu: 2.3200 % (28 points)
- vm remote used burst bps credits percentage: 0.0000  (3 points)
- vm remote used burst io credits percentage: 0.0000  (7 points)
- vm uncached bandwidth consumed percentage: 58.0000 % (28 points)
- vm uncached iops consumed percentage: 1.0000 % (28 points)

