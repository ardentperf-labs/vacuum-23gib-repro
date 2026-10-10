#!/bin/sh
set -eu
dev=$(cat /sys/class/block/sdc/dev)
sys=$(readlink -f /sys/class/block/sdc/device)
[ "$dev" = "8:32" ] || { echo "deviceMismatch=$dev"; exit 41; }
case "$sys" in */1:0:0:0) ;; *) echo "lunPathMismatch=$sys"; exit 42;; esac
printf '316\n' > /sys/class/block/sdc/queue/nr_requests
printf 'none\n' > /sys/class/block/sdc/queue/scheduler
[ "$(cat /sys/class/block/sdc/queue/nr_requests)" = "316" ] || exit 43
case "$(cat /sys/class/block/sdc/queue/scheduler)" in '[none]'*) ;; *) exit 44;; esac
echo 'settingsRestored=standbyDATAonly scheduler=none nr_requests=316'
