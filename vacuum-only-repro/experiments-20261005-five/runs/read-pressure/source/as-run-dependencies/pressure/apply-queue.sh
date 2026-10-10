#!/bin/sh
set -eu
Q=/sys/class/block/sdc/queue
DEV=/sys/class/block/sdc
IOS="$Q/iosched"
identity() {
  dev=$(cat "$DEV/dev")
  sys=$(readlink -f "$DEV/device")
  [ "$dev" = 8:32 ] || { echo "deviceMismatch=$dev"; return 41; }
  case "$sys" in */1:0:0:0) ;; *) echo "scsiPathMismatch=$sys"; return 42;; esac
  depth=$(cat "$DEV/device/queue_depth")
  [ "$depth" = 316 ] || { echo "hardwareQueueDepthMismatch=$depth"; return 43; }
}
rollback() {
  if [ -w "$IOS/async_depth" ]; then printf '256\n' > "$IOS/async_depth" 2>/dev/null || true; fi
  printf '316\n' > "$Q/nr_requests" 2>/dev/null || true
  printf 'none\n' > "$Q/scheduler" 2>/dev/null || true
  printf '316\n' > "$Q/nr_requests" 2>/dev/null || true
}
state() {
  identity
  nr=$(cat "$Q/nr_requests")
  sched=$(cat "$Q/scheduler")
  async='unavailable'
  if [ -r "$IOS/async_depth" ]; then async=$(cat "$IOS/async_depth"); fi
  tunables=''
  for name in writes_starved fifo_batch read_expire write_expire front_merges; do
    if [ -r "$IOS/$name" ]; then
      value=$(cat "$IOS/$name")
      tunables="${tunables}${name}=${value},"
    fi
  done
  maxsec=$(cat "$Q/max_sectors_kb")
  maxhw=$(cat "$Q/max_hw_sectors_kb")
  hctx=''
  for d in "$DEV"/mq/*; do
    [ -d "$d" ] || continue
    tags=$(cat "$d/nr_tags" 2>/dev/null || echo missing)
    hctx="${hctx}${d##*/}:${tags},"
  done
  printf 'dev=%s|sys=%s|nr=%s|depth=%s|scheduler=%s|async_depth=%s|tunables=%s|max_sectors_kb=%s|max_hw_sectors_kb=%s|hctx_nr_tags=%s\n' \
    "$dev" "$sys" "$nr" "$depth" "$sched" "$async" "$tunables" "$maxsec" "$maxhw" "$hctx"
}
identity
[ "$(cat "$Q/nr_requests")" = 316 ] || { echo "unexpectedInitialNrRequests=$(cat "$Q/nr_requests")"; exit 44; }
case "$(cat "$Q/scheduler")" in '[none]'*) ;; *) echo "unexpectedInitialScheduler=$(cat "$Q/scheduler")"; exit 45;; esac
printf 'before|%s\n' "$(state)"
trap rollback EXIT
printf 'mq-deadline\n' > "$Q/scheduler"
[ -r "$IOS/async_depth" ] || { echo 'asyncDepthUnavailableAfterSchedulerSwitch'; exit 46; }
case "$(cat "$Q/scheduler")" in *'[mq-deadline]'*) ;; *) echo 'schedulerSwitchMismatch'; exit 55;; esac
[ "$(cat "$IOS/async_depth")" = 256 ] || { echo 'unexpectedDefaultAsyncDepth'; exit 56; }
printf 'afterSchedulerSwitch|%s\n' "$(state)"
# Keep queue admission at the previously bounded value; this arm changes only
# mq-deadline async depth after recording the scheduler's automatic default.
printf '1024\n' > "$Q/nr_requests"
[ "$(cat "$Q/nr_requests")" = 1024 ] || { echo "nrRequestsReadbackMismatch=$(cat "$Q/nr_requests")"; exit 47; }
printf 'afterNrRequests1024|%s\n' "$(state)"
printf '512\n' > "$IOS/async_depth"
[ "$(cat "$IOS/async_depth")" = 512 ] || { echo "asyncDepthReadbackMismatch=$(cat "$IOS/async_depth")"; exit 48; }
[ "$(cat "$Q/nr_requests")" = 1024 ] || { echo 'nrRequestsChangedUnexpectedly'; exit 49; }
case "$(cat "$Q/scheduler")" in *'[mq-deadline]'*) ;; *) echo 'schedulerChangedUnexpectedly'; exit 57;; esac
[ "$(cat "$DEV/device/queue_depth")" = 316 ] || { echo 'hardwareQueueDepthChangedUnexpectedly'; exit 58; }
[ "$(cat "$IOS/writes_starved")" = 2 ] || { echo 'writesStarvedChangedUnexpectedly'; exit 50; }
[ "$(cat "$IOS/fifo_batch")" = 16 ] || { echo 'fifoBatchChangedUnexpectedly'; exit 51; }
[ "$(cat "$IOS/read_expire")" = 500 ] || { echo 'readExpireChangedUnexpectedly'; exit 52; }
[ "$(cat "$IOS/write_expire")" = 5000 ] || { echo 'writeExpireChangedUnexpectedly'; exit 53; }
[ "$(cat "$IOS/front_merges")" = 1 ] || { echo 'frontMergesChangedUnexpectedly'; exit 54; }
printf 'afterAsyncDepth512|%s\n' "$(state)"
trap - EXIT
