#!/bin/sh
# Run in a PostgreSQL pod before measurement; read guest CPU/block metadata only.
set -eu

cpus=$(getconf _NPROCESSORS_ONLN)
printf 'online_vcpus=%s\nkernel=%s\n' "$cpus" "$(uname -r)"
printf 'cpu_online='; cat /sys/devices/system/cpu/online

data=$(printenv PGDATA 2>/dev/null || true)
[ -n "$data" ] || data=/var/lib/postgresql/data/pgdata
data=$(readlink -f "$data")
wal=$(readlink -f "$data/pg_wal")

for kind in DATA WAL; do
 if [ "$kind" = DATA ]; then path=$data; else path=$wal; fi
 mount_mm=$(findmnt -n -r -T "$path" -o MAJ:MIN | tr -d '[:space:]')
 [ -n "$mount_mm" ] || { echo "$kind mount has no MAJ:MIN from findmnt" >&2; exit 1; }
 sys=$(readlink -f "/sys/dev/block/$mount_mm")
 while [ -f "$sys/partition" ]; do sys=$(dirname "$sys"); done
 disk=$(basename "$sys")
 device=$(readlink -f "$sys/device")
 printf '%s_path=%s\n%s_device=%s\n%s_major_minor=' "$kind" "$path" "$kind" "$disk" "$kind"
 cat "$sys/dev"
 printf '%s_mount_major_minor=%s\n%s_sysfs_device_path=%s\n' "$kind" "$mount_mm" "$kind" "$device"

 driver_chain=''
 storvsc_path=''
 nvme_bound=0
 ancestor=$device
 while [ "$ancestor" != /sys ] && [ "$ancestor" != / ]; do
  if [ -L "$ancestor/driver" ]; then
   driver_path=$(readlink -f "$ancestor/driver")
   driver=$(basename "$driver_path")
   if [ -n "$driver_chain" ]; then driver_chain="$driver_chain,"; fi
   driver_chain="$driver_chain$(basename "$ancestor"):$driver"
   if [ "$driver" = hv_storvsc ]; then storvsc_path=$ancestor; fi
   if [ "$driver" = nvme ]; then nvme_bound=1; fi
  fi
  ancestor=$(dirname "$ancestor")
 done
 printf '%s_driver_chain=%s\n' "$kind" "${driver_chain:-none}"

 controller=''
 controller_path=''
 controller_driver=''
 case "$disk" in
  nvme[0-9]*n[0-9]*)
   controller=${disk%n[0-9]*}
   controller_path=$(readlink -f "/sys/class/nvme/$controller" 2>/dev/null || true)
   [ -n "$controller_path" ] || { echo "$kind NVMe controller $controller is missing from sysfs" >&2; exit 1; }
   [ "$nvme_bound" = 1 ] && controller_driver=nvme
   [ "$controller_driver" = nvme ] || { echo "$kind namespace $disk has no nvme driver binding" >&2; exit 1; }
   controller_type=nvme
   ;;
  *)
   if [ -n "$storvsc_path" ]; then
    controller_type=storvsc
    controller=hv_storvsc
    controller_path=$storvsc_path
    controller_driver=hv_storvsc
   else
    echo "$kind device $disk is neither NVMe nor bound through hv_storvsc" >&2
    exit 1
   fi
   ;;
 esac
 printf '%s_controller_type=%s\n%s_controller=%s\n%s_controller_driver=%s\n%s_controller_path=%s\n' \
  "$kind" "$controller_type" "$kind" "$controller" "$kind" "$controller_driver" "$kind" "$controller_path"

 if [ "$controller_type" = nvme ]; then
  for name in model firmware_rev state transport address queue_count sqsize; do
   file="/sys/class/nvme/$controller/$name"
   [ -r "$file" ] || continue
   printf '%s_controller_%s=' "$kind" "$name"; cat "$file"
  done
 fi
 if [ "$controller_type" = storvsc ]; then
  for parameter in /sys/module/hv_storvsc/parameters/*; do
   [ -f "$parameter" ] || continue
   printf '%s_storvsc_parameter_%s=' "$kind" "$(basename "$parameter")"
   cat "$parameter"
  done
 fi

 queue="/sys/class/block/$disk/queue"
 for name in nr_requests scheduler max_sectors_kb max_hw_sectors_kb read_ahead_kb rotational nomerges wbt_lat_usec rq_affinity io_poll io_poll_delay; do
  [ -r "$queue/$name" ] || continue
  printf '%s_queue_%s=' "$kind" "$name"; cat "$queue/$name"
 done
 scsi_device=$(readlink -f "$sys/device" 2>/dev/null || true)
 if [ -n "$scsi_device" ] && [ -r "$scsi_device/queue_depth" ]; then
  hctl=$(basename "$scsi_device")
  case "$hctl" in *:*:*:*)
   printf '%s_scsi_hctl=%s\n%s_scsi_queue_depth=' "$kind" "$hctl" "$kind"
   cat "$scsi_device/queue_depth"
   ;;
  esac
 fi

 mq_count=0
 for mq in "/sys/class/block/$disk"/mq/*; do
  [ -d "$mq" ] || continue
  mq_count=$((mq_count + 1))
  qid=$(basename "$mq")
  for name in cpu_list nr_tags nr_reserved_tags; do
   [ -r "$mq/$name" ] || continue
   printf '%s_mq_%s_%s=' "$kind" "$qid" "$name"; cat "$mq/$name"
  done
 done
 printf '%s_mq_count=%s\n' "$kind" "$mq_count"
 [ "$mq_count" -gt 0 ] || { echo "$kind device $disk exposes no blk-mq hardware queues" >&2; exit 1; }
done
