#!/usr/bin/env bash
# Stop the local test PostgreSQL. Pass --purge to also delete its data directory.
set -euo pipefail
. "$(dirname "$0")/pg-common.sh"

if [ -f "$PGDATA/PG_VERSION" ] && pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
  pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null
  echo "stopped"
else
  echo "not running"
fi
if [ "${1:-}" = "--purge" ]; then
  rm -rf "$PGDATA" "$PGRUN"
  echo "purged $PGDATA"
fi
