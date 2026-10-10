#!/bin/sh
set -eu
Q=/sys/class/block/sdc/queue
DEV=/sys/class/block/sdc
IOS="$Q/iosched"
dev=$(cat "$DEV/dev")
[ "$dev" = 8:32 ] || { echo "deviceMismatch=$dev"; exit 41; }
sys=$(readlink -f "$DEV/device")
case "$sys" in */1:0:0:0) ;; *) echo "scsiPathMismatch=$sys"; exit 42;; esac
[ "$(cat "$DEV/device/queue_depth")" = 316 ] || { echo 'hardwareQueueDepthMismatch'; exit 43; }
case "$(cat "$Q/scheduler")" in *'[mq-deadline]'*) ;; *) echo 'schedulerMismatch'; exit 44;; esac
[ "$(cat "$Q/nr_requests")" = 1024 ] || { echo "nrRequestsMismatch=$(cat "$Q/nr_requests")"; exit 45; }
[ "$(cat "$IOS/async_depth")" = 512 ] || { echo "asyncDepthMismatch=$(cat "$IOS/async_depth")"; exit 46; }
printf '256\n' > "$IOS/async_depth"
[ "$(cat "$IOS/async_depth")" = 256 ] || { echo 'asyncDepthRestoreReadbackMismatch'; exit 47; }
[ "$(cat "$Q/nr_requests")" = 1024 ] || { echo 'nrRequestsChangedDuringAsyncRestore'; exit 48; }
case "$(cat "$Q/scheduler")" in *'[mq-deadline]'*) ;; *) echo 'schedulerChangedDuringAsyncRestore'; exit 49;; esac
echo 'asyncDepthRestored=256 scheduler=mq-deadline nr_requests=1024 DATAonly'
