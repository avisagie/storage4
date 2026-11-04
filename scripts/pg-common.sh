# Shared settings for the throwaway local PostgreSQL used by L2/contract tests.
# Sourced by pg-start.sh / pg-stop.sh. Override via environment.
PGDATA="${STORAGE4_PGDATA:-$HOME/pgdata}"
PGRUN="${STORAGE4_PGRUN:-$HOME/pgrun}"
PGPORT="${STORAGE4_PGPORT:-5432}"
PGLOG="$PGDATA/server.log"

for bin in initdb pg_ctl psql; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: '$bin' not found. Install PostgreSQL (Arch: sudo pacman -S postgresql)." >&2
    exit 1
  fi
done
