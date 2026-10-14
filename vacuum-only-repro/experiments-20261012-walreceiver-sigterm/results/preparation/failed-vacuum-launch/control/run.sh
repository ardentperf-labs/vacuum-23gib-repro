#!/bin/sh
d=$1
PGAPPNAME=vacuum-only-0aae0525c9054b1090fc4251cffd7595 psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d repro -f "$d/vacuum.sql" >"$d/vacuum.log" 2>&1 &
client=$!
sh -c 'end=$(( $(date +%s) + 86400 )); while [ ! -r "$1/status" ] && [ "$(date +%s)" -lt "$end" ]; do sleep 5; done; if [ ! -r "$1/status" ]; then PGAPPNAME=vacuum-guard psql -XAt -U postgres -d repro -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='''vacuum-only-0aae0525c9054b1090fc4251cffd7595''' AND usename='''postgres''';" >/dev/null 2>&1; fi' sh "$d" >/dev/null 2>&1 &
guard=$!
if wait "$client"; then rc=0; else rc=$?; fi
printf '%s\n' "$rc" >"$d/status.tmp"; mv "$d/status.tmp" "$d/status"
kill "$guard" >/dev/null 2>&1 || true
