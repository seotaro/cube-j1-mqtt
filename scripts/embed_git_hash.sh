#!/usr/bin/env bash
# Rewrite BRIDGE_GIT_HASH in production_tool/mqtt_bridge.py with the commit
# that is actually published on the GitHub remote's `main` branch.
#
# The remote defaults to `origin` and can be overridden with
# `BRIDGE_REMOTE=<name>` (e.g. `BRIDGE_REMOTE=fork` for a jj setup where the
# publishing remote is not `origin`).
#
# Two gates run before the rewrite:
#   1. After `git fetch <remote> main`, production_tool/mqtt_bridge.py must
#      match `<remote>/main` — otherwise the deployed bridge would silently
#      disagree with the hash it reports.
#   2. The hash is taken from the remote-tracking ref `<remote>/main`, not
#      whichever stale commit the local git HEAD happens to point at (jj
#      colocated workspaces routinely rewrite local commits without moving
#      git HEAD).
#
# Escape hatches:
#   - `ALLOW_UNCOMMITTED=1` skips both gates and stamps "unknown". Useful
#     for offline builds during development.
#
# Exit codes:
#   0  success
#   1  gate failed or fetch failed
set -euo pipefail

cd "$(dirname "$0")/.."

TARGET="production_tool/mqtt_bridge.py"
REMOTE="${BRIDGE_REMOTE:-origin}"
REMOTE_REF="${REMOTE}/main"

if [ ! -f "$TARGET" ]; then
    echo "embed_git_hash.sh: $TARGET not found" >&2
    exit 1
fi

if [ "${ALLOW_UNCOMMITTED:-0}" = "1" ]; then
    sed -i.bak 's|^BRIDGE_GIT_HASH = .*|BRIDGE_GIT_HASH = "unknown"|' "$TARGET"
    rm -f "${TARGET}.bak"
    echo "embed_git_hash.sh: ALLOW_UNCOMMITTED=1 — stamped 'unknown' (gates skipped)"
    exit 0
fi

# Refresh the remote-tracking ref so both gates see what is on GitHub now.
if ! git fetch --quiet "$REMOTE" main; then
    cat >&2 <<EOF
embed_git_hash.sh: 'git fetch ${REMOTE} main' failed.

Check the remote name (BRIDGE_REMOTE, default 'origin') and network access,
or set ALLOW_UNCOMMITTED=1 to fall back to 'unknown' for offline builds.
EOF
    exit 1
fi

# Gate 1: production_tool/mqtt_bridge.py must match what is published on the
# remote's `main` branch. Comparing against local git HEAD is not enough under
# jj colocated workflows because jj routinely rewrites local commits without
# moving git HEAD. Comparing against the remote-tracking ref guarantees
# "what we are about to ship == what is on GitHub".
if ! git diff --quiet "${REMOTE_REF}" -- "$TARGET" 2>/dev/null; then
    cat >&2 <<EOF
embed_git_hash.sh: $TARGET differs from ${REMOTE_REF}.

The bridge would lie about its version if we deployed this build. Commit
& push the changes to ${REMOTE} main before deploying, or set
ALLOW_UNCOMMITTED=1 to bypass for local experiments.

Inspect with:
  git diff ${REMOTE_REF} -- $TARGET
EOF
    exit 1
fi

# Gate 2: derive the hash from the remote's main — the single source of truth
# for "what has been published".
HASH="$(git rev-parse --short=7 "${REMOTE_REF}")"

sed -i.bak "s|^BRIDGE_GIT_HASH = .*|BRIDGE_GIT_HASH = \"${HASH}\"|" "$TARGET"
rm -f "${TARGET}.bak"

echo "BRIDGE_GIT_HASH set to ${HASH} in ${TARGET} (from ${REMOTE_REF})"
