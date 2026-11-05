#!/usr/bin/env bash
# Print the binary version: major.minor.build+hash[.dirty], SemVer 2.0.
#   major.minor  from the latest git tag vMAJOR.MINOR
#   build        commits since that tag (0 on the tagged commit)
#   hash         first 8 hex digits of the commit
#   .dirty       appended if the working tree has uncommitted changes
# Without a tag: 0.0.<commit count>+hash. Needs full history and tags (no shallow clone).
# Informational only: nothing compares versions; compatibility uses integer compat/schema levels.
set -euo pipefail
cd "$(dirname "$0")/.."

if desc=$(git describe --tags --long --abbrev=8 --dirty=.dirty --match 'v[0-9]*.[0-9]*' 2>/dev/null) &&
   [[ $desc =~ ^v([0-9]+)\.([0-9]+)-([0-9]+)-g([0-9a-f]{8})(\.dirty)?$ ]]; then
  echo "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}+${BASH_REMATCH[4]}${BASH_REMATCH[5]:-}"
  exit 0
fi

if [ "${RELEASE:-}" = 1 ]; then
  echo "version.sh: RELEASE=1 but no vMAJOR.MINOR tag is reachable (shallow clone or missing tags?)" >&2
  exit 1
fi
hash=$(git rev-parse --short=8 HEAD 2>/dev/null || echo unknown)
count=$(git rev-list --count HEAD 2>/dev/null || echo 0)
dirty=""
if [ -n "$(git status --porcelain 2>/dev/null)" ]; then dirty=".dirty"; fi
echo "0.0.${count}+${hash}${dirty}"
