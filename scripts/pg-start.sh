#!/usr/bin/env bash
# Start (and on first run create) a disposable local PostgreSQL cluster. No root needed.
# Test cluster only: trust auth, localhost only, fsync off.
set -euo pipefail
. "$(dirname "$0")/pg-common.sh"

mkdir -p "$PGRUN"
if [ ! -f "$PGDATA/PG_VERSION" ]; then
  echo "initialising cluster in $PGDATA"
  initdb -D "$PGDATA" --auth=trust --encoding=UTF8 --locale=C.UTF-8 --username=postgres >/dev/null
fi

if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
  echo "already running"
else
  pg_ctl -D "$PGDATA" -l "$PGLOG" -w \
    -o "-c listen_addresses=127.0.0.1 -c port=$PGPORT -c unix_socket_directories=$PGRUN -c fsync=off" start >/dev/null
fi

psql -h 127.0.0.1 -p "$PGPORT" -U postgres -Atc 'select 1' >/dev/null
echo "PostgreSQL ready. Use:"
echo "  export STORAGE4_TEST_PG_DSN='postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable'"
