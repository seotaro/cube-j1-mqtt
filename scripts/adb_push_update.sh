#!/usr/bin/env bash
# Hot-reload production_tool/mqtt_bridge.py onto a Cube J1 over ADB-over-TCP
# and restart the mqtt_ha_bridge init service.
#
# Usage:
#   scripts/adb_push_update.sh [<cube_j1_ip>]
#
# Default IP: 192.168.1.103 (Cube J1 on LAN, ADB port 5555).
#
# Steps:
#   1. Verify `adb` is installed (else exit 2).
#   2. Embed git short hash into production_tool/mqtt_bridge.py.
#   3. adb connect <ip>:5555, verify get-state == "device".
#   4. adb push production_tool/mqtt_bridge.py /data/local/mqtt_bridge.py
#   5. Restore in-tree BRIDGE_GIT_HASH to "unknown" (keep working copy clean).
#   6. stop mqtt_ha_bridge → sleep 1 → start mqtt_ha_bridge
#   7. Wait 10s, then `pgrep -f mqtt_bridge.py` — fail if no PID.
#   8. adb disconnect on EXIT (success or failure).
#
# See specs/004-adb-update/spec.md for the full contract.
set -euo pipefail

DEFAULT_IP="192.168.1.103"
IP="${1:-$DEFAULT_IP}"
PORT="5555"
TARGET="${IP}:${PORT}"

cd "$(dirname "$0")/.."

# FR-002: adb guard.
if ! command -v adb >/dev/null 2>&1; then
    echo "adb not found (install android-platform-tools)" >&2
    exit 2
fi

cleanup() {
    adb disconnect "$TARGET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# FR-004: embed git hash before push.
echo "[1/6] Embedding git short hash"
./scripts/embed_git_hash.sh

# Report the hash that was actually embedded (= what the device will report),
# not local HEAD.
HASH="$(awk -F'"' '/^BRIDGE_GIT_HASH/ {print $2; exit}' production_tool/mqtt_bridge.py)"
HASH="${HASH:-unknown}"
SEMVER="$(awk -F'"' '/^BRIDGE_SEMVER/ {print $2; exit}' production_tool/mqtt_bridge.py)"
SEMVER="${SEMVER:-unknown}"

# FR-003: connect + verify.
echo "[2/6] Connecting to $TARGET"
CONNECT_OUT="$(adb connect "$TARGET" 2>&1)"
echo "       $CONNECT_OUT"
case "$CONNECT_OUT" in
    *"unable to connect"*|*"failed to connect"*|*"cannot connect"*)
        echo "failed to connect to $TARGET" >&2
        exit 1
        ;;
esac

STATE="$(adb -s "$TARGET" get-state 2>/dev/null || echo offline)"
if [ "$STATE" != "device" ]; then
    echo "failed to connect to $TARGET (state=$STATE)" >&2
    exit 1
fi

# FR-005: push.
echo "[3/6] Pushing production_tool/mqtt_bridge.py to /data/local/mqtt_bridge.py"
adb -s "$TARGET" push production_tool/mqtt_bridge.py /data/local/mqtt_bridge.py

# FR-004: restore working copy to clean state right after push.
echo "[4/6] Restoring in-tree BRIDGE_GIT_HASH to 'unknown'"
sed -i.bak 's|^BRIDGE_GIT_HASH = .*|BRIDGE_GIT_HASH = "unknown"|' production_tool/mqtt_bridge.py
rm -f production_tool/mqtt_bridge.py.bak

# FR-006: restart init service.
echo "[5/6] Restarting mqtt_ha_bridge service"
adb -s "$TARGET" shell stop mqtt_ha_bridge
sleep 1
adb -s "$TARGET" shell start mqtt_ha_bridge

# FR-007: liveness check.
echo "[6/6] Waiting 10s, then verifying bridge process is alive"
sleep 10
PID="$(adb -s "$TARGET" shell pgrep -f mqtt_bridge.py 2>/dev/null | tr -d '\r' | head -n1 || true)"
if [ -z "$PID" ]; then
    echo "mqtt_ha_bridge did not start within 10s (no pid for mqtt_bridge.py)" >&2
    exit 1
fi

# FR-008: success line.
echo "Updated bridge at ${TARGET} (version ${SEMVER}+${HASH})"
