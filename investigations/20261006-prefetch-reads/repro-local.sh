#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
WORK_DIR="$(printenv PREFETCH_REPRO_DIR || mktemp -d -p /tmp pg16-prefetch-repro.XXXXXX)"
for owned_dir in primary replay_off replay_try; do
  if [[ -e "$WORK_DIR/$owned_dir" ]]; then
    printf 'Use a fresh work directory; %s already exists.\n' "$WORK_DIR/$owned_dir" >&2
    exit 1
  fi
done
mkdir -p "$WORK_DIR"/{logs,pkg,sockets,sockets_off,sockets_try}

if [[ -n "$PG16_BINDIR" ]]; then
  PG_BINDIR="$PG16_BINDIR"
elif [[ -x /usr/lib/postgresql/16/bin/postgres ]]; then
  PG_BINDIR=/usr/lib/postgresql/16/bin
else
  (cd "$WORK_DIR/pkg" && apt-get download postgresql-16)
  SERVER_DEB="$(find "$WORK_DIR/pkg" -maxdepth 1 -name 'postgresql-16_*.deb' -print -quit)"
  dpkg-deb -x "$SERVER_DEB" "$WORK_DIR/pkgroot"
  PG_BINDIR="$WORK_DIR/pkgroot/usr/lib/postgresql/16/bin"
fi

PG_LIBDIR="$(dirname "$PG_BINDIR")/lib"
if [[ -n "$PG16_LIBDIR" ]]; then
  PG_LIBDIR="$PG16_LIBDIR"
fi
if [[ ! -f "$PG_LIBDIR/pageinspect.so" ]]; then
  (cd "$WORK_DIR/pkg" && apt-get download postgresql-16)
  SERVER_DEB="$(find "$WORK_DIR/pkg" -maxdepth 1 -name 'postgresql-16_*.deb' -print -quit)"
  dpkg-deb -x "$SERVER_DEB" "$WORK_DIR/pkgroot"
  PG_LIBDIR="$WORK_DIR/pkgroot/usr/lib/postgresql/16/lib"
fi

PG_CTL="$PG_BINDIR/pg_ctl"
INITDB="$PG_BINDIR/initdb"
PG_CONTROLDATA="$PG_BINDIR/pg_controldata"
PG_WALDUMP="$PG_BINDIR/pg_waldump"
PSQL_BIN="$(command -v psql)"
DB_USER="$(id -un)"
PORT=55493

stop_owned_servers() {
  for data_dir in "$WORK_DIR/replay_off" "$WORK_DIR/replay_try" "$WORK_DIR/primary"; do
    if [[ -f "$data_dir/postmaster.pid" ]]; then
      "$PG_CTL" -D "$data_dir" stop -m fast >/dev/null 2>&1 || true
    fi
  done
}
trap stop_owned_servers EXIT INT TERM

psql_primary() {
  "$PSQL_BIN" -X -h "$WORK_DIR/sockets" -p "$PORT" -U "$DB_USER" -d postgres "$@"
}

psql_off() {
  "$PSQL_BIN" -X -h "$WORK_DIR/sockets_off" -p 55494 -U "$DB_USER" -d postgres "$@"
}

psql_try() {
  "$PSQL_BIN" -X -h "$WORK_DIR/sockets_try" -p 55495 -U "$DB_USER" -d postgres "$@"
}

"$INITDB" -D "$WORK_DIR/primary" --no-locale --encoding=UTF8 --data-checksums \
  --username="$DB_USER" --auth-local=trust --auth-host=reject --no-instructions \
  > "$WORK_DIR/logs/initdb.log"
cat >> "$WORK_DIR/primary/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$WORK_DIR/sockets'
port = $PORT
shared_buffers = 16MB
max_wal_size = 256MB
checkpoint_timeout = 1h
autovacuum = off
wal_level = replica
wal_log_hints = on
full_page_writes = on
wal_compression = off
log_checkpoints = on
log_min_messages = info
EOF

"$PG_CTL" -D "$WORK_DIR/primary" -l "$WORK_DIR/logs/primary.log" start
psql_primary -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/local-fixture.sql" \
  > "$WORK_DIR/logs/fixture-setup.txt"
sed "s|@PAGEINSPECT_SO@|$PG_LIBDIR/pageinspect|g" \
  "$SCRIPT_DIR/local-pageinspect-functions.sql.in" > "$WORK_DIR/pageinspect-functions.sql"
psql_primary -v ON_ERROR_STOP=1 -f "$WORK_DIR/pageinspect-functions.sql" \
  > "$WORK_DIR/logs/pageinspect-functions.txt"
psql_primary -P pager=off -f "$SCRIPT_DIR/local-inspect-page0.sql" \
  > "$WORK_DIR/logs/page0-before-vacuum.txt"
psql_primary -v ON_ERROR_STOP=1 -c "SELECT pg_stat_reset_shared('io')" \
  > "$WORK_DIR/logs/io-stats-reset.txt"
psql_primary -v ON_ERROR_STOP=1 -c 'CHECKPOINT' > "$WORK_DIR/logs/base-checkpoint.txt"
"$PG_CTL" -D "$WORK_DIR/primary" stop -m fast

cp -a "$WORK_DIR/primary" "$WORK_DIR/replay_off"
cp -a "$WORK_DIR/primary" "$WORK_DIR/replay_try"
REDO_LSN="$("$PG_CONTROLDATA" "$WORK_DIR/replay_off" |
  sed -n "s/^Latest checkpoint's REDO location:[[:space:]]*//p")"

cat >> "$WORK_DIR/replay_off/postgresql.conf" <<EOF
unix_socket_directories = '$WORK_DIR/sockets_off'
port = 55494
recovery_prefetch = off
recovery_target_name = 'vacuum_done'
recovery_target_action = pause
hot_standby = on
restore_command = '/bin/false'
EOF
cat >> "$WORK_DIR/replay_try/postgresql.conf" <<EOF
unix_socket_directories = '$WORK_DIR/sockets_try'
port = 55495
recovery_prefetch = try
recovery_target_name = 'vacuum_done'
recovery_target_action = pause
hot_standby = on
restore_command = '/bin/false'
EOF

"$PG_CTL" -D "$WORK_DIR/primary" -l "$WORK_DIR/logs/primary.log" start
psql_primary -v ON_ERROR_STOP=1 -c 'VACUUM (FREEZE, VERBOSE) vacuum_fixture' \
  > "$WORK_DIR/logs/vacuum-verbose.txt" 2>&1
psql_primary -P pager=off -f "$SCRIPT_DIR/local-inspect-page0.sql" \
  > "$WORK_DIR/logs/page0-after-vacuum.txt"
TARGET_LSN="$(psql_primary -At -v ON_ERROR_STOP=1 \
  -c "SELECT pg_create_restore_point('vacuum_done')")"
psql_primary -At -v ON_ERROR_STOP=1 -c 'SELECT pg_switch_wal()' \
  > "$WORK_DIR/logs/wal-switch-lsn.txt"
psql_primary -At -v ON_ERROR_STOP=1 \
  -c "SELECT current_setting('wal_level'), current_setting('wal_log_hints'), current_setting('full_page_writes'), current_setting('recovery_prefetch'), pg_current_wal_lsn()" \
  > "$WORK_DIR/logs/primary-settings.txt"
"$PG_CTL" -D "$WORK_DIR/primary" stop -m immediate

cp -a "$WORK_DIR/primary/pg_wal/." "$WORK_DIR/replay_off/pg_wal/"
cp -a "$WORK_DIR/primary/pg_wal/." "$WORK_DIR/replay_try/pg_wal/"
touch "$WORK_DIR/replay_off/recovery.signal" "$WORK_DIR/replay_try/recovery.signal"

"$PG_CTL" -D "$WORK_DIR/replay_off" -l "$WORK_DIR/logs/replay-off.log" start
"$PG_CTL" -D "$WORK_DIR/replay_try" -l "$WORK_DIR/logs/replay-try.log" start

for attempt in $(seq 1 100); do
  OFF_PAUSED="$(psql_off -Atc 'SELECT pg_is_wal_replay_paused()')"
  TRY_PAUSED="$(psql_try -Atc 'SELECT pg_is_wal_replay_paused()')"
  [[ "$OFF_PAUSED" == t && "$TRY_PAUSED" == t ]] && break
  sleep 0.1
done
[[ "$OFF_PAUSED" == t && "$TRY_PAUSED" == t ]]

capture_replay() {
  local name="$1"
  local query
  query="SELECT jsonb_build_object(
    'gucs', jsonb_build_object(
      'recovery_prefetch', current_setting('recovery_prefetch'),
      'wal_log_hints', current_setting('wal_log_hints'),
      'full_page_writes', current_setting('full_page_writes'),
      'wal_level', current_setting('wal_level')),
    'recovery', jsonb_build_object(
      'in_recovery', pg_is_in_recovery(),
      'paused', pg_is_wal_replay_paused(),
      'replay_lsn', pg_last_wal_replay_lsn()),
    'prefetch_stats', (SELECT to_jsonb(s) FROM pg_stat_recovery_prefetch AS s),
    'startup_relation_io', (SELECT COALESCE(jsonb_agg(to_jsonb(i) ORDER BY i.context), '[]'::jsonb)
                            FROM pg_stat_io AS i
                            WHERE i.backend_type = 'startup' AND i.object = 'relation')
  )::text"
  if [[ "$name" == off ]]; then
    psql_off -Atc "$query" > "$WORK_DIR/logs/replay-off-stats.json"
  else
    psql_try -Atc "$query" > "$WORK_DIR/logs/replay-try-stats.json"
  fi
}
capture_replay off
capture_replay try

psql_off -Atc 'SELECT pg_wal_replay_resume()' > "$WORK_DIR/logs/replay-off-resume.txt"
psql_try -Atc 'SELECT pg_wal_replay_resume()' > "$WORK_DIR/logs/replay-try-resume.txt"
for attempt in $(seq 1 100); do
  OFF_RECOVERY="$(psql_off -Atc 'SELECT pg_is_in_recovery()')"
  TRY_RECOVERY="$(psql_try -Atc 'SELECT pg_is_in_recovery()')"
  [[ "$OFF_RECOVERY" == f && "$TRY_RECOVERY" == f ]] && break
  sleep 0.1
done
[[ "$OFF_RECOVERY" == f && "$TRY_RECOVERY" == f ]]
psql_off -Atc "SELECT COALESCE(jsonb_agg(to_jsonb(i) ORDER BY i.context), '[]'::jsonb)
                FROM pg_stat_io AS i
                WHERE i.backend_type = 'startup' AND i.object = 'relation'" \
  > "$WORK_DIR/logs/replay-off-startup-io-after-promotion.json"
psql_try -Atc "SELECT COALESCE(jsonb_agg(to_jsonb(i) ORDER BY i.context), '[]'::jsonb)
                FROM pg_stat_io AS i
                WHERE i.backend_type = 'startup' AND i.object = 'relation'" \
  > "$WORK_DIR/logs/replay-try-startup-io-after-promotion.json"

HEAP_PATH="$(psql_off -Atc "SELECT pg_relation_filepath('vacuum_fixture')")"
RELFILE="$(basename "$HEAP_PATH")"
"$PG_WALDUMP" -p "$WORK_DIR/replay_off/pg_wal" -s "$REDO_LSN" -e "$TARGET_LSN" \
  -R "1663/5/$RELFILE" -F main -B 0 -b > "$WORK_DIR/logs/page0-main-wal.txt"
"$PG_WALDUMP" -p "$WORK_DIR/replay_off/pg_wal" -s "$REDO_LSN" -e "$TARGET_LSN" \
  -R "1663/5/$RELFILE" -z > "$WORK_DIR/logs/table-wal-summary.txt"

cat > "$WORK_DIR/logs/run-metadata.txt" <<EOF
PostgreSQL: $("$PG_BINDIR/postgres" --version)
work directory: $WORK_DIR
heap relation path: $HEAP_PATH
replay start (redo): $REDO_LSN
restore point target: $TARGET_LSN
data checksums: enabled by initdb --data-checksums
wal_log_hints: on
full_page_writes: on
prefetch settings: off vs try
EOF

printf 'Results are in %s/logs\n' "$WORK_DIR"
