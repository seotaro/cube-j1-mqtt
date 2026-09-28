#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
mqtt_bridge.py  -  Wi-SUN B-route -> ECHONET Lite -> Home Assistant MQTT
Python 2.7 stdlib only: termios, fcntl, select, socket, struct, json, os
"""

from __future__ import print_function

import os
import sys
import json
import time
import struct
import socket
import select
import binascii
import termios
import fcntl
import collections
import re
import threading
import logging
import logging.handlers
import base64
import hmac
import tempfile
import subprocess
import traceback

try:
    # Python 2.7
    import BaseHTTPServer
    import urlparse
except ImportError:
    # Python 3 (for host tests)
    from http import server as BaseHTTPServer
    import urllib.parse as urlparse

# Bridge self-version. SemVer is updated manually; git short hash is overwritten
# by scripts/embed_git_hash.sh before USB distribution. When the embed script
# is skipped the version reports "<semver>+unknown" (FR-001 fallback).
BRIDGE_SEMVER = "1.0.0"
BRIDGE_GIT_HASH = "unknown"

# Python 2/3 compatible string type tuple. json.loads returns `unicode` on
# Python 2, plain `str` on Python 3 — both must be accepted as "text".
try:
    _TEXT_TYPES = (str, unicode)  # noqa: F821 (unicode only exists on Py2)
except NameError:
    _TEXT_TYPES = (str,)

CONFIG_PATH = "/data/local/config.json"
LOG_PATH    = "/data/local/mqtt_bridge.log"
# spec 041 Phase 2b (FR-002): uncaught-exception tracebacks. Android init
# discards the service's stderr, so a process death like the 2026-06-27
# silent death leaves no clue. The excepthook appends here instead.
STDERR_LOG_PATH = "/data/local/mqtt_bridge.stderr.log"

LED_R = "/sys/class/leds/red/brightness"
LED_G = "/sys/class/leds/green/brightness"
LED_B = "/sys/class/leds/blue/brightness"

def led_rgb(r, g, b):
    for path, val in ((LED_R, r), (LED_G, g), (LED_B, b)):
        try:
            with open(path, 'w') as f:
                f.write(str(val) + '\n')
        except Exception:
            pass

def led_read():
    result = []
    for path in (LED_R, LED_G, LED_B):
        try:
            with open(path) as f:
                result.append(int(f.read().strip()))
        except Exception:
            result.append(0)
    return tuple(result)

LOGGER = None  # Set in main() once config is loaded.


def log(msg):
    """Legacy plain-text log function used by call sites that haven't been
    converted to named events yet. Routes through the JSON logger when
    available, falling back to stderr otherwise (preserves the old fallback
    behaviour from FR-009).
    """
    if LOGGER is not None:
        try:
            LOGGER.info(event="log", msg=msg)
            return
        except Exception:
            pass
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write("[{}] {}\n".format(ts, msg))
    sys.stderr.flush()

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def apply_defaults(cfg):
    """Return a copy of *cfg* with optional observability + admin keys filled in.

    Preserves every existing entry (FR-012 / 003 FR-018). Unknown keys are
    passed through unchanged so future config additions don't crash older
    bridge versions.
    """
    out = dict(cfg)
    out.setdefault("log_level", "info")
    out.setdefault("log_max_bytes", 1048576)
    out.setdefault("log_backup_count", 3)
    out.setdefault("admin_ui_enabled", False)
    out.setdefault("admin_ui_port", 8080)
    out.setdefault("admin_user", "")
    out.setdefault("admin_password", "")
    # Number of consecutive ERXUDP timeouts before the bridge forces a
    # Wi-SUN re-join. 0 = disabled (legacy behaviour).
    # spec 011 元 default 5、 spec 027 で 30 に変更。 実機 grafana 24h で
    # 78% 欠損 + reconnect 430 回 (= 1 回/3 分) の主因と判明、 5 連続 fail で
    # 発火する設計はメーター応答性悪化時に loop 化。 30 連続 (= 900 秒 ≒
    # 15 分) に緩和でメーター完全 dead 検知は遅れるが欠損率劇的改善見込み。
    out.setdefault("erxudp_timeout_force_reconnect_threshold", 1)  # spec 050: 6 → 2、 spec 053: 2 → 1 (単発 timeout ≈ session 死の確定信号。 spec 048 INF filter + spec 049 noise 除外が前提、 3h A/B で live +24% 実証)
    # MQTT keep-alive (seconds). Cube J1 のメインループは ECHONET Lite
    # の同期 poll で詰まることがあり、上流デフォルトの 60s では broker から
    # 切断される（実測 約 12 分間隔）。300s なら poll が一時的に滞っても
    # PINGREQ をギリギリ間に合わせられる。
    out.setdefault("mqtt_keepalive", 300)
    # MQTT クライアントを送信ワーカ + keepalive ワーカに分離 (spec 005)。
    # メインループの ECHONET poll が詰まっても broker session を維持する。
    # 切り戻し用の fallback として False で従来パスに戻せる。
    out.setdefault("mqtt_threading_enabled", True)
    out.setdefault("mqtt_send_queue_maxsize", 1000)
    # spec 010: periodic EEDSCAN to track 920MHz noise floor. 5 min default
    # is a balance between visibility and meter-poll interference (~12s
    # sweep stops meter traffic for that cycle).
    out.setdefault("eedscan_enabled", True)
    # spec 012: shorten EEDSCAN interval so noise judgements stay fresh.
    out.setdefault("eedscan_interval_sec", 120)
    # spec 011: ERXUDP resilience. timeout 30s + 2s backoff covers the p95
    # 5s tail. Retry default further cut from 1 to 0 after a second 24h
    # observation showed retry rate ~0.92/min (≒ every cycle) and recovery
    # stayed at 2%, confirming retries were not winning — they were just
    # piling up replies in the meter's ECHONET queue. With retries off,
    # the queue gets a chance to drain; retry can be opted back in via
    # config when the queue clears.
    # spec 032 aggressive polling default を tako 合議 (= 2026-07-01) で tuning:
    # - erxudp_latency p50=3.28s / p95=4.96s / max=5.58s = メーター応答 3-5s
    # - timeout=6s では p95 tail が timeout 側へ落ちる、 8s に延長で 1 cycle 内回収
    # - intra_cycle_retries=3 で 490 retries / 2 recovered = 0.4% 効率 = メーター
    #   キュー DDoS 状態、 0 に抑制で自然応答を待つ = latency 緩和 + spec 020 の
    #   late frame 救済 (= 63 件 / 5h) で次 cycle 回収に委ねる。
    out.setdefault("erxudp_timeout_sec", 8)  # spec 032→tako 合議: 6 → 8 (p95=5s に対応)
    out.setdefault("erxudp_intra_cycle_retries", 0)  # spec 032→tako 合議: 3 → 0 (DDoS 解消)
    out.setdefault("erxudp_retry_backoff_sec", 2)
    # spec 012: noise-adaptive poll skip — skip normal poll when the last
    # EEDSCAN shows the PAN channel is noisy (>= threshold). Avoids the
    # wisun_reconnect storm that follows clusters of receive failures.
    out.setdefault("noise_adaptive_skip_enabled", True)
    out.setdefault("noise_skip_threshold", 100)
    out.setdefault("noise_skip_max_consecutive", 3)
    # spec 049: noisy cycle を完全 skip でなく 0xE7 単独 (OPC=1) minimal
    # poll に降格。 False で spec 012 の完全 skip に戻る (= kill switch)。
    out.setdefault("noise_skip_tier1_minimal_poll_enabled", True)
    # spec 051: PANA session の計画的 refresh (= 失効前に先回り full
    # reconnect、 不意の 2.5 分 blackout を ~15s の計画停止に変換)。
    # 480s = spec 040 hotfix 実測 tuning (= session 寿命 10-12 分の手前)。
    out.setdefault("proactive_rejoin_enabled", True)
    out.setdefault("proactive_rejoin_after_sec", 480)
    # spec 051 FR-004: SKREJOIN tick (= spec 040 Phase 2b、 94-100% reject、
    # tako 合議 2026-07-01 で本番 OFF 推奨) を default OFF に。 proactive が
    # supersede、 config で復活可。
    out.setdefault("skrejoin_tick_enabled", False)
    # spec 013: ARIB STD-T108 floor 30s. spec 053: default 60 → 30
    # (= floor ちょうど)。 120 poll/h の TX airtime は duty cycle 360s/h
    # に対し 2 桁の余裕、 3h A/B で live 51.2→98.7/h + session death
    # rate 非加速を実証。 floor 未満は引き続き clamp。
    out.setdefault("poll_interval", 30)
    if int(out["poll_interval"]) < MIN_POLL_INTERVAL_SEC:
        log("WARN: poll_interval={} below floor, clamping to {}".format(
            out["poll_interval"], MIN_POLL_INTERVAL_SEC))
        out["poll_interval"] = MIN_POLL_INTERVAL_SEC
    # spec 019: persist Wi-Fi AP toggle state across reboots.
    # Setting ap_state_persist_enabled=False disables both restore (on
    # startup) and write (on toggle) — a clean kill switch.
    out.setdefault("ap_state_file_path", "/data/local/cube_j1_ap_state")
    out.setdefault("ap_state_persist_enabled", True)
    # spec 017: Wi-SUN rejoin exponential backoff + serial port reopen
    # after N consecutive wisun_connect failures, recovering from long
    # outages without hammering the meter every 30s.
    out.setdefault("wisun_rejoin_backoff_initial_sec", 5)  # spec 052: 30→5 (threshold=2 で 11 回/h の reconnect が毎回 30s 払う隠れ損失を回収、同帯 A/B で +21% 実証)
    out.setdefault("wisun_rejoin_backoff_max_sec", 300)
    out.setdefault("wisun_rejoin_backoff_multiplier", 2.0)
    out.setdefault("wisun_serial_reopen_after_rejoin_failures", 5)
    # spec 018: cumulative energy fixed (EPC 0xEA/0xEB) tier4. Meter
    # records 30-min-boundary values with internal timestamp (JST).
    # Default every=30 cycles ≈ 30 min at poll_interval=60s. Set 0 to
    # disable tier4 entirely (kill switch).
    out.setdefault("epc_tier4_every", 30)
    # spec 022: realtime burst mode defaults. Burst is triggered by Admin
    # API (off by default at boot, no auto-start). interval floor 5 sec
    # protects against pathological config; duration cap 3600 sec is a soft
    # bound (BP35CX physically gates ARIB STD-T108 duty cycle anyway).
    out.setdefault("realtime_burst_default_duration_sec",
                   REALTIME_BURST_DEFAULT_DURATION_SEC)
    out.setdefault("realtime_burst_default_interval_sec",
                   REALTIME_BURST_DEFAULT_INTERVAL_SEC)
    # spec 023: burst 中の ERXUDP timeout 短縮。 0 は kill switch (= base 採用、
    # spec 022 v1 互換)、 それ以外は 5s floor で clamp (= burst_interval_min と同価)。
    out.setdefault("realtime_burst_erxudp_timeout_sec",
                   REALTIME_BURST_ERXUDP_TIMEOUT_SEC)
    _rbet = out["realtime_burst_erxudp_timeout_sec"]
    if _rbet != 0 and _rbet < REALTIME_BURST_MIN_INTERVAL_SEC:
        out["realtime_burst_erxudp_timeout_sec"] = REALTIME_BURST_MIN_INTERVAL_SEC
    # spec 025: burst (and catch-up) 中の force_wisun_reconnect threshold 緩和。
    # 0 は kill switch (= base 5 採用、 spec 022 互換)。 floor なし (= ユーザ自由)。
    out.setdefault("realtime_burst_force_reconnect_threshold",
                   REALTIME_BURST_FORCE_RECONNECT_THRESHOLD)
    # spec 026: burst (and catch-up) 中の rejoin backoff initial 短縮 (30s → 5s)。
    # 0 は kill switch (= base 30s 採用)。 floor なし。
    out.setdefault("realtime_burst_rejoin_backoff_initial_sec",
                   REALTIME_BURST_REJOIN_BACKOFF_INITIAL_SEC)
    # spec 020: TID mismatch frame を 過去 send 時刻で late publish 救済。
    # メーター ECHONET 内部 queue 遅延応答 (= grafana 観察で p95=9 frame 蓄積)
    # を破棄せず累積系 EPC のみ過去 timestamp で publish、 grafana の穴補完。
    out.setdefault("tid_mismatch_recover_enabled", True)
    out.setdefault("tid_mismatch_history_maxlen", 240)  # spec 028: 10 → 240 で深い遅延吸収
    out.setdefault("power_w_recovery_backfill_enabled", True)  # spec 028: 瞬時電力救済 frame backfill 経路
    out.setdefault("cumulative_recovery_backfill_enabled", True)  # spec 029: 累積系救済 frame backfill 経路
    # spec 034: SKSCAN channel mask cache for faster reconnect (32s → 4s).
    # enabled=False で旧挙動 (= 毎回全 scan、 spec 011 系列互換)、
    # fallback_duration は単 ch 取り損ね時の全 scan duration (= SCAN_DURATION_BASE 既存値)。
    out.setdefault("wisun_reconnect_channel_mask_enabled", True)
    out.setdefault("wisun_reconnect_channel_mask_fallback_duration", 6)
    # spec 035: SKLL64 cached + SKJOIN 直行 (= SKSCAN 完全 skip で reconnect 35s → 7s).
    # enabled=False で旧挙動 (= 毎 reconnect で SKSCAN 全 scan、 spec 011 系列互換)。
    # threshold = 連続 SKJOIN 失敗で cache 全 invalidate する閾値、 2 = 一時ノイズ吸収。
    out.setdefault("wisun_reconnect_cached_skjoin_enabled", True)
    out.setdefault("wisun_reconnect_cached_skjoin_invalidate_threshold", 2)
    return out


def should_retry_in_cycle(attempt, max_retries):
    """True while `attempt` (0-indexed try number) is inside the retry
    budget. attempt=0 means the first SKSENDTO failed; we return True up to
    and including `max_retries - 1`. Negative attempt is treated as 0."""
    return int(attempt) < int(max_retries)


# ---------------------------------------------------------------------------
# ProbeState: high-frequency RTT sampling toggle (spec 009)
# ---------------------------------------------------------------------------

class ProbeState(object):
    """In-memory probe-mode flag. Time-limited; auto-disables once the
    `deadline_ts` is crossed. The main loop checks `is_active(now)` before
    every cycle to decide which EPC list / interval to use."""

    def __init__(self):
        self.active = False
        self.interval_sec = 0
        self.deadline_ts = 0.0

    def start(self, interval_sec, duration_sec, now):
        if int(interval_sec) <= 0:
            raise ValueError("interval_sec must be > 0")
        if int(duration_sec) <= 0:
            raise ValueError("duration_sec must be > 0")
        self.active = True
        self.interval_sec = int(interval_sec)
        self.deadline_ts = float(now) + float(duration_sec)

    def stop(self):
        self.active = False
        self.interval_sec = 0
        self.deadline_ts = 0.0

    def is_active(self, now):
        return bool(self.active and now < self.deadline_ts)

    def snapshot(self, now):
        active = self.is_active(now)
        remaining = max(0, int(self.deadline_ts - now)) if active else 0
        return {
            "active": active,
            "interval_sec": self.interval_sec if active else 0,
            "deadline_ts": self.deadline_ts,
            "remaining_sec": remaining,
        }


# ---------------------------------------------------------------------------
# EEDSCAN: 920MHz Energy Detection Scan (spec 010)
# ---------------------------------------------------------------------------

def parse_eedscan(payload):
    """Parse the EEDSCAN data line `0 <ch> <energy> <ch> <energy> ...` into a
    `{channel: energy}` dict. Both channel and energy are 1-byte hex. The
    leading 0 is a status byte; the first hex token is treated as that
    status and skipped. A trailing dangling channel token (no paired
    energy) is dropped. Non-hex garbage tokens are skipped silently."""
    if not payload:
        return {}
    tokens = payload.split()
    out = {}
    i = 0
    seen_status = False
    while i < len(tokens):
        t = tokens[i]
        try:
            int(t, 16)
        except ValueError:
            i += 1
            continue
        if not seen_status:
            seen_status = True
            i += 1
            continue
        if i + 1 >= len(tokens):
            break
        try:
            ch = int(t, 16)
            energy = int(tokens[i + 1], 16)
            out[ch] = energy
            i += 2
        except ValueError:
            i += 1
    return out


class EedScanState(object):
    """Last EEDSCAN result + a short rolling history. The main loop polls
    `should_run(now)` to decide when to fire another sweep; the diag
    publisher reads `snapshot()` to emit 920MHz noise-floor metrics."""

    HISTORY_MAX = 100

    def __init__(self, interval_sec=300):
        self.interval_sec = int(interval_sec)
        self.last_run_ts = 0.0
        self.last_result = {}
        self.recent = collections.deque(maxlen=self.HISTORY_MAX)

    def should_run(self, now):
        return (now - self.last_run_ts) >= self.interval_sec

    def record(self, result, ts):
        self.last_run_ts = float(ts)
        self.last_result = dict(result)
        self.recent.append((float(ts), dict(result)))

    def snapshot(self, pan_channel=None):
        if not self.last_result:
            return {}
        energies = list(self.last_result.values())
        out = {
            "eedscan_max_energy": max(energies),
            "eedscan_min_energy": min(energies),
        }
        if pan_channel is not None and pan_channel in self.last_result:
            out["eedscan_pan_channel_energy"] = self.last_result[pan_channel]
        return out

    def is_noisy(self, threshold, pan_channel):
        """Spec 012: True if the most recent sweep observed the PAN channel
        at energy >= threshold. Empty history or missing channel returns
        False (= caller runs the normal poll)."""
        if not self.last_result:
            return False
        if pan_channel not in self.last_result:
            return False
        return self.last_result[pan_channel] >= threshold


def decide_cycle_kind(probe_active, last_normal_start, now, poll_interval):
    """Pick "probe" or "normal" for the next poll cycle. In probe mode we
    interleave fast 0x80 probes with the usual 0xE7 measurements so HA
    still gets power values at the regular cadence.

    Spec 009: a normal cycle runs whenever the previous one was at least
    poll_interval seconds ago. The very first cycle of a probe session is
    also normal so the rolling window starts with a fresh measurement.
    """
    if not probe_active:
        return "normal"
    if last_normal_start <= 0:
        return "normal"
    if (now - last_normal_start) >= float(poll_interval):
        return "normal"
    return "probe"


def compute_next_poll_sleep(last_poll_start, now, poll_interval):
    """How long to sleep so the next poll begins `poll_interval` seconds after
    the *start* of the previous one (deadline-based pacing). Clamped to 0 so
    that an overrun (ERXUDP timeout) does not waste an extra cycle of silence.
    """
    deadline = last_poll_start + float(poll_interval)
    remaining = deadline - now
    if remaining <= 0:
        return 0.0
    return remaining


# ---------------------------------------------------------------------------
# Embedded admin UI: pure helpers
# ---------------------------------------------------------------------------

# Path to the live config the bridge reads on startup. The admin UI reads /
# atomically rewrites this file so the next bridge restart picks up the new
# values. Tests can override these constants.
ADMIN_CONFIG_PATH = "/data/local/config.json"
ADMIN_BRIDGE_PATH = "/data/local/mqtt_bridge.py"
ADMIN_WPA_PATH    = "/data/misc/wifi/wpa_supplicant.conf"
ADMIN_LOG_PATH    = LOG_PATH


class AdminConfig(object):
    """Immutable admin UI configuration loaded from config.json."""

    def __init__(self, enabled, port, user, password):
        self.enabled  = bool(enabled)
        self.port     = int(port)
        self.user     = user or ""
        self.password = password or ""

    def is_active(self):
        return bool(self.enabled and self.user and self.password)

    def match_basic_auth(self, header_value):
        """Constant-time compare an `Authorization: Basic ...` header value.

        Returns False for any malformed input rather than raising.
        """
        if not header_value or not isinstance(header_value, str):
            return False
        if not header_value.startswith("Basic "):
            return False
        encoded = header_value[6:].strip()
        try:
            # Constitution II — Python 2.7 stdlib only. The `validate` kwarg
            # only landed in CPython 2.7.6, but Cube J1 ships 2.7.13 *without*
            # it on this build, so stay on the lowest-common-denominator API.
            # An accidentally-junk decode just yields junk and fails the
            # subsequent string compare anyway.
            decoded = base64.b64decode(encoded.encode("ascii")).decode("utf-8")
        except Exception:
            return False
        if ":" not in decoded:
            return False
        sent_user, _, sent_pwd = decoded.partition(":")
        # Use compare_digest on bytes so the same-length check is constant.
        u_ok = hmac.compare_digest(
            sent_user.encode("utf-8"), self.user.encode("utf-8"))
        p_ok = hmac.compare_digest(
            sent_pwd.encode("utf-8"), self.password.encode("utf-8"))
        return bool(u_ok and p_ok)


class AtomicWriter(object):
    """File writes via temp + os.rename so the target is never half-written."""

    @staticmethod
    def write_bytes(path, data):
        dir_ = os.path.dirname(os.path.abspath(path)) or "."
        fd, tmp = tempfile.mkstemp(prefix=".tmp.", dir=dir_)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass  # fsync may fail on some FS — best-effort
            os.rename(tmp, path)
            tmp = None  # rename consumed it
        finally:
            if tmp is not None and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

    @staticmethod
    def write_json(path, obj):
        AtomicWriter.write_bytes(
            path, json.dumps(obj, indent=2, separators=(",", ": ")).encode("utf-8"))


_VALID_LOG_LEVELS = ("debug", "info", "warn", "error")

# spec 013: ARIB STD-T108 920MHz duty cycle (360s/hour) safety floor.
# Polling faster than 30s risks exceeding the transmit-time limit once
# retries/reconnects are factored in.
MIN_POLL_INTERVAL_SEC = 30

# spec 022: realtime burst mode bounds. 5 s floor matches Nature Remo E
# Lite cadence; below that the BP35CX physically gates ARIB STD-T108 duty
# cycle and the host-side risk becomes mooting BP35CX retries. Default
# duration 5 min, soft cap 60 min.
REALTIME_BURST_DEFAULT_DURATION_SEC = 300
REALTIME_BURST_DEFAULT_INTERVAL_SEC = 5
REALTIME_BURST_MIN_INTERVAL_SEC = 5
REALTIME_BURST_MAX_DURATION_SEC = 3600

# spec 023: burst mode 中の ERXUDP timeout 短縮 (30s base → 5s burst)。
# 0 は kill switch (= base 採用、 spec 022 v1 互換)、 それ以外は 5s floor で clamp。
REALTIME_BURST_ERXUDP_TIMEOUT_SEC = 5

# spec 025: burst (and catch-up) 中の force_wisun_reconnect threshold 緩和。
# base 5 × burst 5s = 25 秒で reconnect 発火を防ぐ。 30 × 5s = 150 秒 (= base
# mode の 5 × 30s と同水準)。 0 は kill switch (= base 採用)。 floor なし。
REALTIME_BURST_FORCE_RECONNECT_THRESHOLD = 30

# spec 026: burst (and catch-up) 中の rejoin backoff initial 短縮 (30s → 5s)。
# 1 回 reconnect 5s + SKJOIN 5-15s = 10-20 秒で復帰、 spec 025 までの 30-60 秒
# から大幅短縮。 multiplier / max は base のまま (= 連発時 exponential 保護維持)。
# 0 は kill switch (= base 30s 採用)。 floor なし。
REALTIME_BURST_REJOIN_BACKOFF_INITIAL_SEC = 5

_POSITIVE_INT_KEYS = (
    "mqtt_port", "log_max_bytes", "log_backup_count",
    "admin_ui_port",
)


def validate_config_patch(patch, current):
    """Merge *patch* into *current* with type/range checks (FR-010).

    Returns (merged_dict, None) on success, (None, error_message) on failure.
    Unknown keys are passed through.
    """
    merged = dict(current)
    for key, value in patch.items():
        if key in _POSITIVE_INT_KEYS:
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                return None, "{} must be a positive integer".format(key)
        elif key == "poll_interval":
            if (not isinstance(value, int) or isinstance(value, bool)
                    or value < MIN_POLL_INTERVAL_SEC):
                return None, ("poll_interval must be an integer >= {} seconds "
                              "(ARIB STD-T108 920MHz duty cycle)"
                              .format(MIN_POLL_INTERVAL_SEC))
        elif key == "log_level":
            if value not in _VALID_LOG_LEVELS:
                return None, ("log_level must be one of {}"
                              .format("/".join(_VALID_LOG_LEVELS)))
        elif key == "admin_ui_enabled":
            if not isinstance(value, bool):
                return None, "admin_ui_enabled must be true or false"
        elif key == "admin_password" and value == "***":
            value = current.get("admin_password", "")
        merged[key] = value
    return merged, None


def validate_wifi_patch(payload):
    """Verify SSID/PSK shape per WPA2 PSK spec.

    Returns (normalized_dict, None) on success, (None, error_message)
    on failure. PSK must be 8..63 characters per IEEE 802.11i.
    """
    ssid = payload.get("ssid")
    psk = payload.get("psk")
    # Accept both Python 2 `str`/`unicode` and Python 3 `str`. json.loads
    # on Py2 always returns `unicode` for string values, so a plain
    # isinstance(ssid, str) check incorrectly rejects every JSON body
    # arriving on the Cube J1 (Python 2.7.13).
    if not isinstance(ssid, _TEXT_TYPES) or not ssid.strip():
        return None, "ssid is required"
    if not isinstance(psk, _TEXT_TYPES):
        return None, "psk must be a string"
    if len(psk) < 8 or len(psk) > 63:
        return None, "psk must be 8-63 characters"
    return {"ssid": ssid, "psk": psk}, None


def read_config_masked(path):
    """Read config.json and replace admin_password with '***' if present."""
    with open(path) as f:
        cfg = json.load(f)
    if "admin_password" in cfg:
        cfg["admin_password"] = "***"
    return cfg


_TAIL_LOG_MAX = 1000
_TAIL_LOG_READ_CHUNK = 4096


def tail_log(path, n):
    """Return the last `n` non-empty lines of `path` (clamped to 1..1000).

    Reads the file backwards in CHUNK-sized blocks so it stays cheap even for
    multi-megabyte rotated logs. Missing file returns an empty list.
    """
    if not isinstance(n, int) or n < 1:
        n = 1
    if n > _TAIL_LOG_MAX:
        n = _TAIL_LOG_MAX
    try:
        f = open(path, "rb")
    except (IOError, OSError):
        return []
    try:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        buf = b""
        lines = []
        pos = end
        while pos > 0 and len(lines) <= n:
            read_size = min(_TAIL_LOG_READ_CHUNK, pos)
            pos -= read_size
            f.seek(pos)
            buf = f.read(read_size) + buf
            lines = buf.split(b"\n")
        decoded = [line.decode("utf-8", errors="replace") for line in lines]
        non_empty = [line for line in decoded if line.strip()]
        return non_empty[-n:]
    finally:
        f.close()


# ---------------------------------------------------------------------------
# Embedded admin UI: HTTP handler / server
# ---------------------------------------------------------------------------

ADMIN_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>Cube J1 Admin</title>
<style>
body { font-family: -apple-system, system-ui, sans-serif; margin: 2rem auto;
       max-width: 760px; color: #222; }
h1 { font-size: 1.4rem; }
fieldset { border: 1px solid #ccc; padding: 1rem; margin: 1rem 0; }
legend { padding: 0 .4rem; font-weight: 600; }
label { display: block; margin: .4rem 0; font-size: .9rem; }
input, select { width: 100%; box-sizing: border-box; padding: .4rem .6rem;
                 font: inherit; }
button { padding: .55rem 1.2rem; font: inherit; cursor: pointer; }
.ok { color: #186a3b; } .err { color: #b71c1c; }
pre { background: #f4f4f4; padding: .6rem; font-size: .8rem;
      max-height: 12rem; overflow: auto; }
.notice { background: #fff8e1; border-left: 3px solid #f57f17; padding: .5rem;
          font-size: .85rem; margin: .5rem 0; }
</style>
</head>
<body>
<h1>Cube J1 Admin <a href="/wisun" style="font-size:14px;font-weight:normal;color:#5af">[ Wi-SUN Quality (real-time) ]</a></h1>
<div id="status"></div>
<fieldset><legend>Config</legend>
  <form id="config-form"></form>
  <button id="config-save" type="button">Save Config</button>
</fieldset>
<fieldset><legend>Wi-Fi</legend>
  <p class="notice">&#9888; Wi-Fi 変更後に AP に再接続できないと Cube J1 は
     LAN から見えなくなります。USB 経由で復旧する覚悟がある場合のみ実行してください.</p>
  <form id="wifi-form">
    <label>SSID <input name="ssid" required></label>
    <label>PSK  <input name="psk" type="password" required minlength="8" maxlength="63"></label>
    <button type="submit">Save Wi-Fi</button>
  </form>
</fieldset>
<fieldset><legend>AP Mode (CubeJ-...)</legend>
  <p class="notice">&#9888; CubeJ-* AP はデフォルトで 12345678 で公開されている。使わない間は OFF にしておいた方が安全。自宅 Wi-Fi 経由でいつでも戻せる.</p>
  <p>state: <strong id="ap_state">...</strong>
     <span id="ap_iface" style="font-family:monospace;font-size:.8rem;color:#888"></span></p>
  <button id="ap_toggle" type="button" disabled>--</button>
  <span id="ap_msg" style="font-size:.85rem;color:#888;margin-left:.6rem"></span>
</fieldset>
<fieldset><legend>Realtime Power (Burst Mode)</legend>
  <p class="notice">瞬時電力 (0xE7) を 5 秒間隔で更新するモード。5 分経過で自動的に通常 (60 秒) に戻る。ARIB STD-T108 duty cycle は BP35CX が物理層で保護.</p>
  <p>mode: <strong id="rt_mode">...</strong>
     <span id="rt_remaining" style="font-family:monospace;font-size:.85rem;color:#888"></span></p>
  <button id="rt_start" type="button">5 分間 burst 開始</button>
  <button id="rt_stop" type="button">停止</button>
</fieldset>
<fieldset><legend>Bridge Update</legend>
  <form id="update-form" enctype="multipart/form-data">
    <label><input type="file" name="update_file" accept=".py" required></label>
    <button type="submit">Upload &amp; Restart</button>
  </form>
</fieldset>
<fieldset><legend>Diagnostics</legend>
  <button id="btn-diag" type="button">Refresh Diag</button>
  <button id="btn-restart" type="button">Restart Bridge</button>
  <pre id="diag-output">(press refresh)</pre>
</fieldset>
<script>
var status = document.getElementById('status');
function showOk(msg) { status.innerHTML = '<p class="ok">' + msg + '</p>'; }
function showErr(msg) { status.innerHTML = '<p class="err">' + msg + '</p>'; }
function renderConfig(cfg) {
  var form = document.getElementById('config-form');
  form.innerHTML = '';
  Object.keys(cfg).sort().forEach(function(k) {
    var v = cfg[k];
    var row = document.createElement('label');
    row.textContent = k + ' ';
    var inp = document.createElement('input');
    inp.name = k;
    if (typeof v === 'boolean') {
      inp.type = 'checkbox'; inp.checked = v;
    } else {
      inp.value = v == null ? '' : String(v);
    }
    row.appendChild(inp);
    form.appendChild(row);
  });
}
function fetchConfig() {
  fetch('/api/config').then(function(r){ return r.json(); }).then(renderConfig);
}
document.getElementById('config-save').addEventListener('click', function() {
  var form = document.getElementById('config-form');
  var patch = {};
  Array.from(form.elements).forEach(function(el) {
    if (!el.name) return;
    if (el.type === 'checkbox') {
      patch[el.name] = el.checked;
    } else if (/^(mqtt_port|poll_interval|log_max_bytes|log_backup_count|admin_ui_port)$/.test(el.name)) {
      patch[el.name] = parseInt(el.value, 10);
    } else {
      patch[el.name] = el.value;
    }
  });
  fetch('/api/config', { method: 'PUT', headers: {'Content-Type':'application/json'},
                          body: JSON.stringify(patch) })
    .then(function(r){ return r.json().then(function(j){ return [r.status, j]; }); })
    .then(function(pair){ if(pair[0]===200) showOk('Config saved'); else showErr(pair[1].error || 'Save failed'); });
});
document.getElementById('wifi-form').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var f = ev.target;
  var body = JSON.stringify({ ssid: f.ssid.value, psk: f.psk.value });
  fetch('/api/wifi', { method: 'PUT', headers:{'Content-Type':'application/json'}, body: body })
    .then(function(r){ return r.json().then(function(j){ return [r.status, j]; }); })
    .then(function(pair){ if(pair[0]===200) showOk('Wi-Fi updated'); else showErr(pair[1].error||'Failed'); });
});
document.getElementById('update-form').addEventListener('submit', function(ev) {
  ev.preventDefault();
  var fd = new FormData(ev.target);
  fetch('/api/update', { method:'POST', body: fd })
    .then(function(r){ return r.json().then(function(j){ return [r.status, j]; }); })
    .then(function(pair){ if(pair[0]===200) showOk('Bridge updated, restarting...'); else showErr(pair[1].error||'Upload failed'); });
});
document.getElementById('btn-diag').addEventListener('click', function() {
  fetch('/api/diag').then(function(r){return r.json();}).then(function(j){
    document.getElementById('diag-output').textContent = JSON.stringify(j, null, 2);
  });
});
document.getElementById('btn-restart').addEventListener('click', function() {
  if (!confirm('Restart bridge process?')) return;
  fetch('/api/restart', { method:'POST' })
    .then(function(r){ return r.json(); })
    .then(function(){ showOk('Restart requested'); });
});
fetchConfig();

// ---- AP toggle (spec 008) ----
function apRender(state) {
  var el = document.getElementById('ap_state');
  var btn = document.getElementById('ap_toggle');
  document.getElementById('ap_iface').textContent = state.interface || '';
  if (state.enabled === true) {
    el.textContent = 'ON'; el.style.color = '#186a3b';
    btn.textContent = 'Turn OFF'; btn.disabled = false; btn.dataset.next = 'false';
  } else if (state.enabled === false) {
    el.textContent = 'OFF'; el.style.color = '#555';
    btn.textContent = 'Turn ON'; btn.disabled = false; btn.dataset.next = 'true';
  } else {
    el.textContent = 'unknown'; el.style.color = '#b71c1c';
    btn.textContent = '--'; btn.disabled = true;
  }
}
function apRefresh() {
  fetch('/api/ap_state', {cache:'no-store'})
    .then(function(r){ return r.json(); })
    .then(apRender)
    .catch(function(){ document.getElementById('ap_msg').textContent = 'offline'; });
}
document.getElementById('ap_toggle').addEventListener('click', function() {
  var btn = this, next = btn.dataset.next === 'true';
  btn.disabled = true; btn.textContent = '...';
  document.getElementById('ap_msg').textContent = 'applying...';
  fetch('/api/ap_state', {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({enabled: next})
  })
    .then(function(r){ return r.json().then(function(j){ return [r.status, j]; }); })
    .then(function(pair){
      if (pair[0] !== 200) throw new Error(pair[1].error || ('http '+pair[0]));
      apRender(pair[1]);
      document.getElementById('ap_msg').textContent = 'ok';
    })
    .catch(function(e){
      document.getElementById('ap_msg').textContent = 'failed: ' + e.message;
      setTimeout(apRefresh, 500);
    });
});
apRefresh();
setInterval(apRefresh, 10000);

// spec 022: realtime burst mode toggle + status polling.
function rtRender(j) {
  document.getElementById('rt_mode').textContent = j.mode || '?';
  var rem = j.remaining_sec;
  var eff = j.effective_interval_seconds;
  var msg = '';
  if (rem != null && rem > 0) {
    msg = '(残 ' + Math.ceil(rem) + 's / ' + (eff || '?') + 's 周期)';
  } else if (eff != null) {
    msg = '(' + eff + 's 周期)';
  }
  document.getElementById('rt_remaining').textContent = msg;
}
function rtRefresh() {
  fetch('/api/realtime/status').then(function(r){return r.json();}).then(rtRender)
    .catch(function(){});
}
document.getElementById('rt_start').addEventListener('click', function(){
  fetch('/api/realtime/start',
    {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'})
    .then(function(r){return r.json();}).then(rtRender)
    .catch(function(){});
});
document.getElementById('rt_stop').addEventListener('click', function(){
  fetch('/api/realtime/stop', {method:'POST'})
    .then(function(r){return r.json();}).then(rtRender)
    .catch(function(){});
});
rtRefresh();
setInterval(rtRefresh, 3000);
</script>
</body>
</html>"""


def _restart_bridge_async():
    """Schedule a stop/start of mqtt_ha_bridge 200 ms in the future.

    Run from a timer thread so the HTTP response can be written first.
    """
    def _do():
        try:
            subprocess.Popen(["stop", "mqtt_ha_bridge"]).wait()
            time.sleep(1)
            subprocess.Popen(["start", "mqtt_ha_bridge"]).wait()
        except Exception:
            pass
    threading.Timer(0.2, _do).start()


class AdminHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """Handler injected with config / paths / lock by start_admin_server."""

    # Class attributes set by start_admin_server before HTTPServer construction.
    admin_config = None        # AdminConfig
    diag_state_provider = None # callable() -> DiagState-like
    config_path = ADMIN_CONFIG_PATH
    bridge_path = ADMIN_BRIDGE_PATH
    wpa_supplicant_path = ADMIN_WPA_PATH
    log_path = ADMIN_LOG_PATH
    lock = None                # threading.Lock

    server_version = "CubeJ1Admin/1.0"

    # ------------------------------------------------------------------
    # framework hooks
    # ------------------------------------------------------------------

    def log_message(self, fmt, *args):
        if LOGGER is not None:
            try:
                LOGGER.debug(event="admin_http",
                             msg=fmt % args,
                             context={"client": self.address_string()})
                return
            except Exception:
                pass
        # Fallback: suppress noisy stderr (default would print every request).

    # ------------------------------------------------------------------
    # auth + helpers
    # ------------------------------------------------------------------

    def _authenticate(self):
        header = self.headers.get("Authorization")
        if self.admin_config is None:
            return False
        if not self.admin_config.match_basic_auth(header):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="cubej"')
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error": "Authentication required"}')
            return False
        return True

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status, text, content_type="text/plain; charset=utf-8"):
        # Python 2: str IS bytes — calling .encode("utf-8") on str holding
        # raw UTF-8 bytes triggers an implicit str→unicode decode through
        # the ASCII codec and blows up on any non-ASCII byte. Detect bytes
        # via `isinstance(text, bytes)` which holds on both interpreters
        # (Py2: bytes == str, Py3: bytes != str).
        if isinstance(text, bytes):
            body = text
        else:
            body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            return None
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def _wrap(self, fn):
        try:
            return fn()
        except Exception as e:
            if LOGGER is not None:
                try:
                    LOGGER.error(event="admin_unhandled_error",
                                 context={"path": self.path,
                                          "error": str(e)})
                except Exception:
                    pass
            self._send_json(500, {"error": "internal server error"})

    # ------------------------------------------------------------------
    # GET dispatch
    # ------------------------------------------------------------------

    def do_GET(self):
        if not self._authenticate():
            return
        self._wrap(self._do_get_dispatch)

    def _do_get_dispatch(self):
        parsed = urlparse.urlparse(self.path)
        path = parsed.path
        if path == "/" or path == "":
            self._send_text(200, ADMIN_HTML,
                            content_type="text/html; charset=utf-8")
            return
        if path == "/api/config":
            cfg = read_config_masked(self.config_path)
            self._send_json(200, cfg)
            return
        if path == "/api/diag":
            try:
                ds = self.diag_state_provider()
                snap = ds.snapshot(time.time())
                # spec 010: merge EEDSCAN noise-floor metrics
                try:
                    eed = self.eedscan_state_provider()
                    snap.update(eed.snapshot(
                        pan_channel=getattr(ds, "pan_channel", None)))
                except Exception:
                    pass
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            self._send_json(200, snap)
            return
        if path == "/api/log":
            qs = urlparse.parse_qs(parsed.query)
            try:
                n = int(qs.get("lines", ["100"])[0])
            except ValueError:
                n = 100
            lines = tail_log(self.log_path, n)
            body = ("\n".join(lines) + "\n") if lines else ""
            self._send_text(200, body,
                            content_type="application/x-ndjson; charset=utf-8")
            return
        if path == "/api/erxudp_raw":
            try:
                ds = self.diag_state_provider()
                line = ds.last_erxudp_raw_line
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            tokens = line.split() if line else []
            self._send_json(200, {
                "raw": line,
                "token_count": len(tokens),
                "tokens": tokens,
            })
            return
        if path == "/api/realtime/status":
            # spec 022: burst mode current state. effective_interval_seconds
            # comes from DiagState gauge (set by main loop after tick) so
            # catch-up phase is reflected accurately (dig 決定 H).
            state = self.realtime_state_provider()
            ds = self.diag_state_provider()
            self._send_json(200, self._realtime_status_payload(state, ds))
            return
        if path == "/wisun":
            self._send_text(200, WISUN_HTML,
                            content_type="text/html; charset=utf-8")
            return
        if path == "/api/ap_state":
            try:
                state = self.ap_controller.get()
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            self._send_json(200, state)
            return
        if path == "/api/probe":
            try:
                ps = self.probe_state_provider()
                self._send_json(200, ps.snapshot(time.time()))
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return
        if path == "/api/wisun_quality":
            try:
                ds = self.diag_state_provider()
                samples = [float(v) for v in ds.erxudp_latency_ms_recent]
                snap = ds.snapshot(time.time()) if hasattr(ds, "snapshot") else {}
                last_raw = getattr(ds, "last_erxudp_raw_line", None)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            if samples:
                ordered = sorted(samples)
                p50 = round(_percentile(ordered, 50), 2)
                p95 = round(_percentile(ordered, 95), 2)
                mx = round(ordered[-1], 2)
            else:
                p50 = p95 = mx = None
            self._send_json(200, {
                "samples": samples,
                "sample_count": len(samples),
                "p50_ms": p50,
                "p95_ms": p95,
                "max_ms": mx,
                "uptime_seconds": snap.get("uptime_seconds") if isinstance(snap, dict) else None,
                "last_erxudp_raw": last_raw,
            })
            return
        self._send_json(404, {"error": "not found"})

    # ------------------------------------------------------------------
    # PUT dispatch
    # ------------------------------------------------------------------

    def do_PUT(self):
        if not self._authenticate():
            return
        self._wrap(self._do_put_dispatch)

    def _do_put_dispatch(self):
        path = urlparse.urlparse(self.path).path
        if path == "/api/config":
            patch = self._read_json_body()
            if patch is None:
                self._send_json(400, {"error": "invalid JSON body"})
                return
            with self.lock:
                with open(self.config_path) as f:
                    current = json.load(f)
                merged, err = validate_config_patch(patch, current)
                if err:
                    self._send_json(400, {"error": err})
                    return
                AtomicWriter.write_json(self.config_path, merged)
            self._send_json(200, {"status": "ok"})
            return
        if path == "/api/wifi":
            body = self._read_json_body()
            if body is None:
                self._send_json(400, {"error": "invalid JSON body"})
                return
            normalized, err = validate_wifi_patch(body)
            if err:
                self._send_json(400, {"error": err})
                return
            with self.lock:
                content = _rewrite_wpa_supplicant(
                    self.wpa_supplicant_path, normalized["ssid"],
                    normalized["psk"])
                AtomicWriter.write_bytes(
                    self.wpa_supplicant_path, content.encode("utf-8"))
            wpa_output = _run_wpa_reconfigure()
            self._send_json(200, {"status": "ok",
                                   "wpa_cli_output": wpa_output})
            return
        if path == "/api/ap_state":
            body = self._read_json_body()
            if body is None or not isinstance(body, dict) or "enabled" not in body:
                self._send_json(400, {"error": "expected { enabled: bool }"})
                return
            wants_enabled = bool(body["enabled"])
            try:
                state = (self.ap_controller.enable()
                         if wants_enabled else self.ap_controller.disable())
            except Exception as e:
                self._send_json(500, {"error": str(e)})
                return
            self._send_json(200, state)
            return
        if path == "/api/probe":
            body = self._read_json_body()
            if body is None or not isinstance(body, dict) or "enabled" not in body:
                self._send_json(400, {"error": "expected { enabled, interval_sec?, duration_sec? }"})
                return
            ps = self.probe_state_provider()
            now = time.time()
            if not body["enabled"]:
                ps.stop()
                # Keep the rolling latency window meaningful: on stop we
                # leave the deque alone (post-probe samples will refill it).
                self._send_json(200, ps.snapshot(now))
                return
            try:
                interval = int(body.get("interval_sec", 5))
                duration = int(body.get("duration_sec", 300))
                ps.start(interval_sec=interval, duration_sec=duration, now=now)
            except (ValueError, TypeError) as e:
                self._send_json(400, {"error": str(e)})
                return
            # Clear the deque so the sparkline shows only probe samples
            # from this run.
            try:
                ds = self.diag_state_provider()
                if hasattr(ds, "erxudp_latency_ms_recent"):
                    ds.erxudp_latency_ms_recent.clear()
            except Exception as e:
                log("probe deque clear failed: {}".format(e))
            self._send_json(200, ps.snapshot(now))
            return
        self._send_json(404, {"error": "not found"})

    # ------------------------------------------------------------------
    # POST dispatch (US2)
    # ------------------------------------------------------------------

    def do_POST(self):
        if not self._authenticate():
            return
        self._wrap(self._do_post_dispatch)

    def _do_post_dispatch(self):
        path = urlparse.urlparse(self.path).path
        if path == "/api/restart":
            self._send_json(200, {"status": "restarting"})
            try:
                self.wfile.flush()
            except Exception:
                pass
            _restart_bridge_async()
            return
        if path == "/api/update":
            self._handle_update()
            return
        if path == "/api/realtime/start":
            self._handle_realtime_start()
            return
        if path == "/api/realtime/stop":
            self._handle_realtime_stop()
            return
        self._send_json(404, {"error": "not found"})

    def _handle_update(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            self._send_json(400, {"error": "missing Content-Length"})
            return
        if length > 100 * 1024:
            self._send_json(413, {"error": "File too large (max 100KB)"})
            return
        ctype = self.headers.get("Content-Type", "")
        if not ctype.startswith("multipart/form-data"):
            self._send_json(400,
                {"error": "expected multipart/form-data"})
            return
        try:
            body = self.rfile.read(length)
        except Exception as e:
            self._send_json(400, {"error": "read failed: {}".format(e)})
            return
        filename, data, err = _parse_multipart_file(body, ctype, "update_file")
        if err:
            self._send_json(400, {"error": err})
            return
        if not filename.endswith(".py"):
            self._send_json(415, {"error": "Only .py files are accepted"})
            return
        # syntax check via py_compile (does NOT execute the code)
        import py_compile
        dir_ = os.path.dirname(self.bridge_path) or "."
        try:
            fd, tmp = tempfile.mkstemp(suffix=".py", dir=dir_, prefix=".upload.")
        except (IOError, OSError) as e:
            self._send_json(500, {"error": "cannot create temp file: {}".format(e)})
            return
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            try:
                py_compile.compile(tmp, doraise=True)
            except py_compile.PyCompileError as e:
                self._send_json(400, {"error": str(e).strip()})
                return
            with self.lock:
                os.rename(tmp, self.bridge_path)
                tmp = None
        finally:
            if tmp is not None and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        self._send_json(200, {"status": "ok", "restarting": True})

    # ------------------------------------------------------------------
    # spec 022: realtime burst mode endpoints
    # ------------------------------------------------------------------

    def _realtime_status_payload(self, state, ds):
        snap = state.snapshot()
        now = time.time()
        remaining = None
        if (snap.get("mode") == "burst"
                and snap.get("expires_at") is not None):
            remaining = max(0.0, snap["expires_at"] - now)
        return {
            "mode": snap.get("mode"),
            "expires_at": snap.get("expires_at"),
            "remaining_sec": remaining,
            "burst_interval": snap.get("burst_interval"),
            "effective_interval_seconds":
                ds.realtime_effective_interval_seconds,
        }

    def _handle_realtime_start(self):
        body = self._read_json_body() or {}
        duration = body.get("duration_sec",
                            REALTIME_BURST_DEFAULT_DURATION_SEC)
        interval = body.get("interval_sec",
                            REALTIME_BURST_DEFAULT_INTERVAL_SEC)
        try:
            duration = int(duration)
            interval = int(interval)
        except (TypeError, ValueError):
            self._send_json(400, {
                "error": "duration_sec / interval_sec must be int"})
            return
        if duration < 1:
            self._send_json(400, {"error": "duration_sec must be > 0"})
            return
        if interval < REALTIME_BURST_MIN_INTERVAL_SEC:
            self._send_json(400, {"error":
                "interval_sec must be >= {}".format(
                    REALTIME_BURST_MIN_INTERVAL_SEC)})
            return
        if duration > REALTIME_BURST_MAX_DURATION_SEC:
            self._send_json(400, {"error":
                "duration_sec must be <= {}".format(
                    REALTIME_BURST_MAX_DURATION_SEC)})
            return
        state = self.realtime_state_provider()
        state.start_burst(time.time(), duration, interval)
        ds = self.diag_state_provider()
        ds.on_realtime_burst_started()
        self._send_json(200, self._realtime_status_payload(state, ds))

    def _handle_realtime_stop(self):
        state = self.realtime_state_provider()
        was_active = state.stop_burst()
        ds = self.diag_state_provider()
        if was_active:
            ds.on_realtime_burst_aborted()
        self._send_json(200, self._realtime_status_payload(state, ds))
        try:
            self.wfile.flush()
        except Exception:
            pass
        _restart_bridge_async()


_MULTIPART_BOUNDARY_RE = re.compile(r'boundary=(?:"([^"]+)"|([^;\s]+))')
_MULTIPART_FILENAME_RE = re.compile(r'filename="([^"]*)"')
_MULTIPART_NAME_RE = re.compile(r'name="([^"]+)"')


def _parse_multipart_file(body, content_type, field_name):
    """Extract a single file upload from a multipart/form-data body.

    Returns (filename, data, None) on success or (None, None, error_message).
    Pure stdlib (re only) — works on both Python 2.7 (Cube J1 runtime) and
    Python 3 (host test runner) where `cgi.FieldStorage` is unavailable.
    """
    m = _MULTIPART_BOUNDARY_RE.search(content_type)
    if not m:
        return None, None, "missing multipart boundary"
    boundary = (m.group(1) or m.group(2)).encode("ascii")
    sep = b"--" + boundary
    parts = body.split(sep)
    for part in parts:
        if not part or part in (b"", b"--", b"--\r\n"):
            continue
        if part.startswith(b"\r\n"):
            part = part[2:]
        idx = part.find(b"\r\n\r\n")
        if idx == -1:
            continue
        headers_blob = part[:idx].decode("utf-8", errors="replace")
        payload = part[idx + 4:]
        # strip trailing CRLF that precedes the next boundary
        if payload.endswith(b"\r\n"):
            payload = payload[:-2]
        name_match = _MULTIPART_NAME_RE.search(headers_blob)
        if not name_match or name_match.group(1) != field_name:
            continue
        fn_match = _MULTIPART_FILENAME_RE.search(headers_blob)
        filename = fn_match.group(1) if fn_match else ""
        return filename, payload, None
    return None, None, "field '{}' not found".format(field_name)


def _rewrite_wpa_supplicant(path, ssid, psk):
    """Replace the ssid= and psk= lines in the existing template, preserving
    other directives (`freq_list`, `scan_ssid`, etc).

    Falls back to a minimal template if the file is missing.
    """
    try:
        with open(path) as f:
            existing = f.read()
    except (IOError, OSError):
        return ('ctrl_interface=/data/misc/wifi/sockets\n'
                'update_config=1\n\n'
                'network={{\n'
                '        ssid="{}"\n'
                '        psk="{}"\n'
                '        key_mgmt=WPA-PSK\n'
                '}}\n').format(ssid, psk)
    lines = existing.splitlines()
    out_lines = []
    in_network = False
    saw_ssid = False
    saw_psk = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("network={"):
            in_network = True
            out_lines.append(line)
            continue
        if in_network and stripped == "}":
            if not saw_ssid:
                out_lines.append('        ssid="{}"'.format(ssid))
            if not saw_psk:
                out_lines.append('        psk="{}"'.format(psk))
            out_lines.append(line)
            in_network = False
            continue
        if in_network and stripped.startswith("ssid="):
            out_lines.append('        ssid="{}"'.format(ssid))
            saw_ssid = True
            continue
        if in_network and stripped.startswith("psk="):
            out_lines.append('        psk="{}"'.format(psk))
            saw_psk = True
            continue
        out_lines.append(line)
    return "\n".join(out_lines) + "\n"


def _run_wpa_reconfigure():
    """Invoke wpa_cli reconfigure on wlan0. Returns the captured stdout."""
    try:
        proc = subprocess.Popen(
            ["wpa_cli", "-p", "/data/misc/wifi/sockets",
             "-i", "wlan0", "reconfigure"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, _ = proc.communicate()
        return (out or b"").decode("utf-8", errors="replace").strip()
    except Exception as e:
        return "wpa_cli failed: {}".format(e)


class _ReusingHTTPServer(BaseHTTPServer.HTTPServer):
    allow_reuse_address = True


class AdminServer(object):
    """HTTPServer + dedicated thread wrapper."""

    def __init__(self, httpd, port):
        self.httpd = httpd
        self.port = port
        self.thread = threading.Thread(
            target=httpd.serve_forever,
            name="cubej-admin-http",
        )
        self.thread.daemon = True

    def start(self):
        self.thread.start()

    def stop(self):
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        except Exception:
            pass
        if self.thread.is_alive():
            self.thread.join(timeout=2)


def start_admin_server(port, user, password, diag_state_provider,
                       config_path=ADMIN_CONFIG_PATH,
                       bridge_path=ADMIN_BRIDGE_PATH,
                       wpa_supplicant_path=ADMIN_WPA_PATH,
                       log_path=ADMIN_LOG_PATH,
                       ap_controller=None,
                       probe_state_provider=None,
                       eedscan_state_provider=None,
                       realtime_state_provider=None):
    """Construct and start an AdminServer with the given settings.

    Returns the running AdminServer.
    """
    admin_config = AdminConfig(enabled=True, port=port,
                                user=user, password=password)
    AdminHandler.admin_config = admin_config
    # staticmethod prevents the function from being bound to instances
    # (so `self.diag_state_provider()` doesn't pass a phantom self).
    AdminHandler.diag_state_provider = staticmethod(diag_state_provider)
    AdminHandler.config_path = config_path
    AdminHandler.bridge_path = bridge_path
    AdminHandler.wpa_supplicant_path = wpa_supplicant_path
    AdminHandler.log_path = log_path
    AdminHandler.ap_controller = ap_controller or ApController()
    AdminHandler.probe_state_provider = staticmethod(
        probe_state_provider or (lambda: ProbeState()))
    AdminHandler.eedscan_state_provider = staticmethod(
        eedscan_state_provider or (lambda: EedScanState()))
    # spec 022: realtime burst mode — admin endpoints mutate this state
    # while the main loop ticks it; lock lives inside RealtimeModeState.
    AdminHandler.realtime_state_provider = staticmethod(
        realtime_state_provider or (lambda: RealtimeModeState()))
    AdminHandler.lock = threading.Lock()

    httpd = _ReusingHTTPServer(("", port), AdminHandler)
    server = AdminServer(httpd, port)
    server.start()
    return server

# ---------------------------------------------------------------------------
# Pure helpers (no I/O) shared by observability features
# ---------------------------------------------------------------------------

def format_iso8601_utc(epoch):
    """Format epoch seconds as `YYYY-MM-DDTHH:MM:SSZ` in UTC.

    Drops fractional seconds (truncates via int()). Pure function so it can
    be unit-tested without a clock.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(epoch)))


def bridge_version():
    """Return the bridge self-version string `<semver>+<git_hash>`."""
    return "{}+{}".format(BRIDGE_SEMVER, BRIDGE_GIT_HASH)


def should_force_wisun_reconnect(consecutive, threshold, pending=False):
    """Decide whether to force a Wi-SUN re-join after consecutive ERXUDP timeouts.

    `threshold <= 0` opts out of the safety net (legacy behaviour where the
    bridge only reconnects on uncaught exceptions in the main loop).

    spec 017: when *pending* is True (set by EVENT 24/29 PANA failure
    detection in read_erxudp), this overrides the threshold check and
    returns True immediately so the bridge can rejoin without waiting
    for ERXUDP timeouts to accumulate.
    """
    if pending:
        return True
    if not isinstance(threshold, int) or threshold <= 0:
        return False
    return consecutive >= threshold

# ---------------------------------------------------------------------------
# Structured JSON Lines logger
# ---------------------------------------------------------------------------

_LEVEL_NAME_TO_LOGGING = {
    "debug": logging.DEBUG,
    "info":  logging.INFO,
    "warn":  logging.WARNING,
    "error": logging.ERROR,
}

_LOGGING_TO_LEVEL_NAME = {
    logging.DEBUG:   "debug",
    logging.INFO:    "info",
    logging.WARNING: "warn",
    logging.ERROR:   "error",
}


class _JsonFormatter(logging.Formatter):
    """logging.Formatter that emits one JSON object per line.

    The required record attributes (ts/level/event) and the optional
    `msg`/`context` payload are pulled from the LogRecord. Producers attach
    them via `extra={...}` when calling Logger.<level>(...).
    """

    def format(self, record):
        out = collections.OrderedDict()
        out["ts"] = format_iso8601_utc(record.created)
        out["level"] = _LOGGING_TO_LEVEL_NAME.get(record.levelno, "info")
        out["event"] = getattr(record, "event", "log")
        msg = getattr(record, "msg_text", None)
        if msg:
            out["msg"] = msg
        context = getattr(record, "context", None)
        if context:
            out["context"] = context
        return json.dumps(out, separators=(",", ":"))


class JsonLogger(object):
    """Structured JSON Lines logger backed by logging.handlers.RotatingFileHandler.

    Falls back to stderr if the file handler cannot be constructed (FR-009).
    Single-process / single-thread main loop, so no extra locking needed.
    """

    DEFAULT_MAX_BYTES = 1024 * 1024
    DEFAULT_BACKUP_COUNT = 3

    def __init__(self, path, level="info",
                 max_bytes=None, backup_count=None):
        if max_bytes is None:
            max_bytes = self.DEFAULT_MAX_BYTES
        if backup_count is None:
            backup_count = self.DEFAULT_BACKUP_COUNT

        level_num = _LEVEL_NAME_TO_LOGGING.get(level, logging.INFO)
        self._logger = logging.Logger(
            "cubej_bridge_{}".format(id(self)),
            level=level_num,
        )
        # Avoid bubbling up to root logger (and double-printing).
        self._logger.propagate = False

        try:
            handler = logging.handlers.RotatingFileHandler(
                path,
                maxBytes=max_bytes,
                backupCount=backup_count,
                delay=False,
            )
        except (IOError, OSError) as e:
            sys.stderr.write(
                "JsonLogger: file handler unavailable ({}), falling back to "
                "stderr\n".format(e))
            handler = logging.StreamHandler(sys.stderr)

        handler.setFormatter(_JsonFormatter())
        self._logger.addHandler(handler)
        self._handler = handler

    def _emit(self, level_num, event, msg=None, context=None):
        # logging.Logger expects an actual message string; pass an empty
        # placeholder and attach our payload via `extra` so the formatter
        # can render the JSON.
        extra = {"event": event}
        if msg is not None:
            extra["msg_text"] = msg
        if context is not None:
            extra["context"] = context
        self._logger.log(level_num, "", extra=extra)

    def debug(self, event="log", msg=None, context=None):
        self._emit(logging.DEBUG, event, msg=msg, context=context)

    def info(self, event="log", msg=None, context=None):
        self._emit(logging.INFO, event, msg=msg, context=context)

    def warn(self, event="log", msg=None, context=None):
        self._emit(logging.WARNING, event, msg=msg, context=context)

    def error(self, event="log", msg=None, context=None):
        self._emit(logging.ERROR, event, msg=msg, context=context)

    def close(self):
        try:
            self._logger.removeHandler(self._handler)
            self._handler.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# spec 041 Phase 2b: uncaught exception capture (= stderr redirect substitute)
# ---------------------------------------------------------------------------

_STDERR_LOG_MAX_BYTES = 256 * 1024


def _make_uncaught_exception_hook(logger=None, stderr_path=None, prev_hook=None):
    """Build the sys.excepthook callable for spec 041 Phase 2b (FR-002).

    The hook appends the full traceback of an uncaught main-thread exception
    to *stderr_path* with a timestamp header, mirrors a one-line summary into
    the JSON logger as ``uncaught_exception``, and finally chains to
    *prev_hook* so the default stderr behaviour is preserved. Every step is
    best-effort: the hook itself must never raise (Python would warn and the
    original traceback display could be lost).
    """
    def _hook(exc_type, exc_value, exc_tb):
        try:
            tb_text = "".join(traceback.format_exception(
                exc_type, exc_value, exc_tb))
        except Exception:
            tb_text = "{}: {}\n".format(
                getattr(exc_type, "__name__", "Exception"), exc_value)
        try:
            path = stderr_path or STDERR_LOG_PATH
            # Crash-loop guard: rewrite (truncate) instead of appending once
            # the file grows past the cap, so /data cannot fill unbounded.
            mode = "a"
            try:
                if (os.path.exists(path)
                        and os.path.getsize(path) > _STDERR_LOG_MAX_BYTES):
                    mode = "w"
            except OSError:
                pass
            with open(path, mode) as f:
                f.write("===== {} =====\n{}\n".format(
                    time.strftime("%Y-%m-%d %H:%M:%S"), tb_text))
        except Exception:
            pass
        if logger is not None:
            try:
                lines = tb_text.strip().splitlines()
                logger.error(event="uncaught_exception",
                             context={"summary": lines[-1] if lines else ""})
            except Exception:
                pass
        if prev_hook is not None:
            try:
                prev_hook(exc_type, exc_value, exc_tb)
            except Exception:
                pass
    return _hook


def install_excepthook(logger=None, stderr_path=None):
    """Install the spec 041 Phase 2b excepthook on sys.excepthook.

    Covers death scenario (A) of the 2026-06-27 silent death: a Python-level
    uncaught exception that init's stderr discard would otherwise lose. The
    previous hook is chained after recording. MQTT worker threads already
    guard their loops internally, so main-thread coverage plus the JSON log
    mirror is sufficient for Phase 2b scope.
    """
    prev_hook = sys.excepthook
    sys.excepthook = _make_uncaught_exception_hook(logger=logger,
                                                   stderr_path=stderr_path,
                                                   prev_hook=prev_hook)


# ---------------------------------------------------------------------------
# Named event emitters (FR-010)
# ---------------------------------------------------------------------------

def emit_bridge_start(logger, device_id, version):
    logger.info(event="bridge_start",
                context={"device_id": device_id, "version": version})


def emit_mqtt_connected(logger, host, port):
    logger.info(event="mqtt_connected",
                context={"host": host, "port": port})


def emit_mqtt_reconnect(logger):
    logger.warn(event="mqtt_reconnect")


def emit_wisun_joined(logger, pan, ipv6):
    logger.info(event="wisun_joined", context={
        "channel": pan.get("Channel"),
        "pan_id":  pan.get("Pan ID"),
        "mac":     pan.get("Addr"),
        "lqi":     pan.get("LQI"),
        "ipv6":    ipv6,
    })


def emit_wisun_join_failed(logger, reason):
    logger.error(event="wisun_join_failed", context={"reason": str(reason)})


def emit_scan_retry(logger, duration):
    logger.warn(event="scan_retry", context={"duration": duration})


def emit_poll_success(logger, measurements):
    # Keep only the publish-relevant keys to avoid bloating the log line.
    summary = {}
    for key in ("power_w", "energy_forward_kwh", "energy_reverse_kwh",
                "current_r_a", "current_t_a"):
        if key in measurements:
            summary[key] = measurements[key]
    logger.info(event="poll_success", context=summary)


def emit_poll_failure(logger, reason):
    logger.warn(event="poll_failure", context={"reason": str(reason)})

# ---------------------------------------------------------------------------
# DiagState: bridge self-diagnostics aggregated into MQTT publish payloads
# ---------------------------------------------------------------------------

# Order is fixed for deterministic publish/snapshot iteration.
_DIAG_SNAPSHOT_KEYS = (
    "last_poll_success_ts",
    "last_poll_failure_ts",
    "lqi",
    "pan_channel",
    "scan_retries_total",
    "wisun_reconnects_total",
    "mqtt_reconnects_total",
    "erxudp_timeouts_total",
    "erxudp_intra_cycle_retries_total",
    "erxudp_recovered_by_retry_total",
    "erxudp_tid_mismatch_total",
    "noise_adaptive_skips_total",
    "last_discovery_publish_ts",
    "discovery_republish_total",
    "serial_reopen_total",
    # spec 022: realtime burst mode counters + gauges.
    "realtime_burst_started_total",
    "realtime_burst_completed_total",
    "realtime_burst_aborted_total",
    "realtime_mode_current",
    "realtime_effective_interval_seconds",
    # spec 020: TID mismatch late publish recovery counter.
    "erxudp_recovered_from_mismatch_total",
    # spec 028: 瞬時電力 0xE7 救済 frame backfill counter (= 別 channel).
    "power_w_recovered_backfill_total",
    # spec 029: 累積系 (energy_*_kwh) 救済 frame backfill counter (= 別 channel).
    "cumulative_recovered_backfill_total",
    # spec 034: SKSCAN channel mask cache (reconnect 32s → 4s) effect metrics.
    "wisun_reconnect_short_scan_total",
    "wisun_reconnect_fallback_full_scan_total",
    # spec 035: SKLL64 cached + SKJOIN 直行 (reconnect 35s → 7s) effect metrics.
    "wisun_reconnect_cached_skjoin_total",
    "wisun_reconnect_cached_skjoin_fallback_total",
    "uptime_seconds",
    "version",
)

# Spec 006: Wi-SUN health observability. EVENT and FAIL ids that we expose
# as named counters. Anything outside this list is still counted in memory
# (sk_event_counts) but not published — keeps HA discovery noise-free.
_PUBLISHED_SK_EVENT_IDS = ("21", "22", "24", "25", "26", "27", "28", "29", "32", "33")
_PUBLISHED_SK_ERROR_CODES = ("05", "09", "10")


def _percentile(sorted_samples, pct):
    """Linear-interpolation percentile on a pre-sorted list. Spec 006."""
    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return float(sorted_samples[0])
    k = (len(sorted_samples) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_samples) - 1)
    frac = k - lo
    return sorted_samples[lo] + (sorted_samples[hi] - sorted_samples[lo]) * frac


# ---------------------------------------------------------------------------
# AP (Wi-Fi Direct GO mode) toggle — spec 008
#
# Cube J1 の `CubeJ-*` AP は wpa_supplicant 配下の P2P GO group。
# wpa_cli の p2p_group_remove / p2p_group_add で manual 制御できる。
# 状態は getprop net.wifi.ap.state で観測可能。
# ---------------------------------------------------------------------------

_WPA_CLI_SOCKET = "/data/misc/wifi/sockets"


def build_wpa_cli_cmd(interface, action):
    if action == "disable":
        return ["wpa_cli", "-p", _WPA_CLI_SOCKET, "-i", interface,
                "p2p_group_remove", interface]
    if action == "enable":
        return ["wpa_cli", "-p", _WPA_CLI_SOCKET, "-i", interface,
                "p2p_group_add", "persistent=0", "freq=2412"]
    raise ValueError("unknown ap action: {}".format(action))


def parse_ap_state(text):
    """Map `getprop net.wifi.ap.state` output to True / False / None."""
    if not text:
        return None
    s = text.strip().lower()
    if not s:
        return None
    if s in ("created", "enabled", "started", "running"):
        return True
    if s in ("disabled", "removed", "stopped", "uninitialized"):
        return False
    return None


def _default_subprocess_runner(cmd, timeout=5):
    import subprocess
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        out, _err = proc.communicate(timeout=timeout)
    except TypeError:
        # Py2 communicate has no timeout
        out, _err = proc.communicate()
    if isinstance(out, bytes):
        out = out.decode("utf-8", "replace")
    return out


class ApController(object):
    """Toggle the Wi-Fi Direct GO mode AP via wpa_cli + getprop.

    `runner(cmd, timeout=5) -> str` is injectable so unit tests don't spawn
    real subprocesses. Production callers pass nothing and pick up the
    default subprocess runner.
    """

    def __init__(self, interface=None, runner=None, state_store=None):
        self._interface = interface
        self._runner = runner or _default_subprocess_runner
        # spec 019: optional persistence. None = legacy behaviour
        # (toggle does not survive reboot).
        self._state_store = state_store

    def _resolve_interface(self):
        if self._interface:
            return self._interface
        out = self._runner(["getprop", "net.wifi.ap.interface"], timeout=2)
        iface = (out or "").strip()
        return iface or "p2p-wlan0-0"

    def _persist(self, desired):
        if self._state_store is None:
            return
        try:
            self._state_store.write(desired)
        except Exception as e:
            log("ERROR: ApStateStore write failed: {}".format(e))

    def get(self):
        iface = self._resolve_interface()
        out = self._runner(["getprop", "net.wifi.ap.state"], timeout=2)
        return {"enabled": parse_ap_state(out), "interface": iface}

    def disable(self):
        iface = self._resolve_interface()
        self._runner(build_wpa_cli_cmd(iface, "disable"), timeout=5)
        self._persist("disabled")
        return self.get()

    def enable(self):
        iface = self._resolve_interface()
        self._runner(build_wpa_cli_cmd(iface, "enable"), timeout=5)
        self._persist("enabled")
        return self.get()


class ApStateStore(object):
    """spec 019: persist desired AP toggle state across reboots.

    1-line plain text file containing "enabled" or "disabled". Missing /
    malformed / unreadable → read() returns None; caller leaves OS
    default alone. `opener` is injectable for tests (read path); write
    always goes through AtomicWriter.
    """
    VALID_STATES = ("enabled", "disabled")

    def __init__(self, path, opener=None):
        self._path = path
        self._opener = opener or open

    def read(self):
        try:
            with self._opener(self._path, "rb") as f:
                raw = f.read().decode("ascii", errors="replace").strip()
        except IOError:
            return None
        if raw in self.VALID_STATES:
            return raw
        if raw:
            log("WARN: ApStateStore invalid value {!r} in {}".format(
                raw, self._path))
        return None

    def write(self, state):
        if state not in self.VALID_STATES:
            raise ValueError("state must be one of {}".format(
                self.VALID_STATES))
        AtomicWriter.write_bytes(self._path, state.encode("ascii"))


def apply_ap_state_restore(stored_state, ap_controller):
    """spec 019: dispatch restore based on previously stored state.

    Returns the action taken ("enabled" / "disabled") or None when no
    persisted state was found and the OS default is left alone.
    """
    if stored_state == "enabled":
        ap_controller.enable()
        return "enabled"
    if stored_state == "disabled":
        ap_controller.disable()
        return "disabled"
    return None


def render_sparkline(samples, width, height):
    """Return an SVG path string sketching *samples* over a (width, height)
    box. Min sample sits near y=height (bottom), max near y=0 (top). Returns
    "" for empty input. For 1 sample returns 'M w/2 h/2' (a centered dot).

    Used by the /wisun real-time quality page (spec 007)."""
    if not samples:
        return ""
    if len(samples) == 1:
        return "M {:.1f} {:.1f}".format(width / 2.0, height / 2.0)
    lo = min(samples)
    hi = max(samples)
    rng = hi - lo if hi > lo else 1.0
    n = len(samples)
    step = width / float(n - 1)
    parts = []
    for i, v in enumerate(samples):
        x = i * step
        y = height - (v - lo) / rng * height
        cmd = "M" if i == 0 else "L"
        parts.append("{} {:.1f} {:.1f}".format(cmd, x, y))
    return " ".join(parts)


# spec 007: real-time Wi-SUN quality page. Lives on the admin UI so the
# existing Basic Auth covers it. Vanilla JS only — Constitution II forbids
# external assets.
WISUN_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Wi-SUN Quality</title>
<style>
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,sans-serif;
background:#111;color:#eee;padding:16px}
h1{font-size:18px;margin:0 0 12px;color:#aaa}
.row{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.stat{flex:1;min-width:140px;padding:16px;border-radius:10px;
text-align:center;background:#222}
.stat .label{font-size:11px;color:#888;text-transform:uppercase;letter-spacing:1px}
.stat .v{font-size:48px;font-weight:700;line-height:1.1;margin-top:4px}
.stat .unit{font-size:14px;color:#888;margin-left:4px}
.bg-ok{background:#0a5}.bg-warn{background:#a80}
.bg-bad{background:#a40}.bg-crit{background:#902}
.bg-na{background:#333}
.sparkline{background:#181818;border-radius:10px;padding:10px;margin-bottom:16px}
.meta{font-size:12px;color:#888}
.meta code{background:#222;padding:2px 6px;border-radius:4px;color:#bbb;
word-break:break-all;display:inline-block;max-width:100%}
.offline{color:#f55}
</style>
</head>
<body>
<h1>Wi-SUN Quality (real-time)</h1>
<div class="sparkline" style="padding:14px 16px;display:flex;align-items:center;
     gap:12px;flex-wrap:wrap;margin-bottom:16px">
  <strong style="color:#aaa">Probe Mode:</strong>
  <span id="probe_state" style="color:#888">--</span>
  <select id="probe_interval" style="background:#222;color:#eee;border:1px solid #333;
          border-radius:6px;padding:4px 8px">
    <option value="2">2s</option><option value="3">3s</option>
    <option value="5" selected>5s</option><option value="10">10s</option>
  </select>
  <select id="probe_duration" style="background:#222;color:#eee;border:1px solid #333;
          border-radius:6px;padding:4px 8px">
    <option value="60">1 min</option><option value="180">3 min</option>
    <option value="300" selected>5 min</option><option value="600">10 min</option>
  </select>
  <button id="probe_start" style="background:#5af;border:0;border-radius:6px;
          padding:6px 14px;cursor:pointer;font-weight:600">Start Probe</button>
  <button id="probe_stop" style="background:#a40;color:#fff;border:0;border-radius:6px;
          padding:6px 14px;cursor:pointer;font-weight:600" disabled>Stop</button>
  <span id="probe_msg" style="color:#888;font-size:11px"></span>
</div>
<div class="row">
  <div class="stat" id="card_p50"><div class="label">p50 RTT</div>
    <div class="v"><span id="p50">--</span><span class="unit">ms</span></div></div>
  <div class="stat" id="card_p95"><div class="label">p95 RTT</div>
    <div class="v"><span id="p95">--</span><span class="unit">ms</span></div></div>
  <div class="stat" id="card_max"><div class="label">max RTT</div>
    <div class="v"><span id="vmax">--</span><span class="unit">ms</span></div></div>
</div>
<div class="sparkline">
  <svg id="spark" width="100%" height="80" viewBox="0 0 600 80"
       preserveAspectRatio="none">
    <path d="" stroke="#5af" stroke-width="2" fill="none" id="path"/>
  </svg>
</div>
<div class="meta">
  samples: <span id="n">0</span> /
  uptime: <span id="up">--</span>s /
  status: <span id="status">loading...</span><br><br>
  last raw ERXUDP: <code id="raw">--</code>
</div>
<script>
function cls(v){
  if(v===null||v===undefined)return 'bg-na';
  if(v<200)return 'bg-ok';
  if(v<500)return 'bg-warn';
  if(v<1000)return 'bg-bad';
  return 'bg-crit';
}
function fmt(v){return v===null||v===undefined?'--':Math.round(v);}
function spark(samples){
  if(!samples.length)return '';
  if(samples.length===1)return 'M 300 40';
  var lo=Math.min.apply(null,samples), hi=Math.max.apply(null,samples);
  var rng=hi>lo?hi-lo:1;
  var step=600/(samples.length-1);
  var d='';
  for(var i=0;i<samples.length;i++){
    var x=i*step;
    var y=80-(samples[i]-lo)/rng*80;
    d+=(i?' L ':'M ')+x.toFixed(1)+' '+y.toFixed(1);
  }
  return d;
}
function setCard(id,v){
  var card=document.getElementById('card_'+id);
  card.className='stat '+cls(v);
}
async function tick(){
  try{
    var r=await fetch('/api/wisun_quality',{cache:'no-store'});
    if(!r.ok)throw new Error('http '+r.status);
    var j=await r.json();
    document.getElementById('p50').textContent=fmt(j.p50_ms);
    document.getElementById('p95').textContent=fmt(j.p95_ms);
    document.getElementById('vmax').textContent=fmt(j.max_ms);
    setCard('p50',j.p50_ms);
    setCard('p95',j.p95_ms);
    setCard('max',j.max_ms);
    document.getElementById('n').textContent=j.sample_count;
    document.getElementById('up').textContent=j.uptime_seconds;
    document.getElementById('raw').textContent=j.last_erxudp_raw||'--';
    document.getElementById('status').textContent='ok';
    document.getElementById('status').className='';
    document.getElementById('path').setAttribute('d',spark(j.samples));
  }catch(e){
    document.getElementById('status').textContent='offline ('+e.message+')';
    document.getElementById('status').className='offline';
  }
}
tick();
setInterval(tick,1500);

// ---- Probe mode (spec 009) ----
async function probeRefresh(){
  try{
    var r=await fetch('/api/probe',{cache:'no-store'});
    var j=await r.json();
    var el=document.getElementById('probe_state');
    var btnStart=document.getElementById('probe_start');
    var btnStop=document.getElementById('probe_stop');
    if(j.active){
      el.textContent='ON ('+j.interval_sec+'s, '+j.remaining_sec+'s left)';
      el.style.color='#0fc';
      btnStart.disabled=true;btnStop.disabled=false;
    }else{
      el.textContent='OFF';el.style.color='#888';
      btnStart.disabled=false;btnStop.disabled=true;
    }
  }catch(e){
    document.getElementById('probe_msg').textContent='offline';
  }
}
document.getElementById('probe_start').addEventListener('click',async function(){
  var iv=parseInt(document.getElementById('probe_interval').value,10);
  var du=parseInt(document.getElementById('probe_duration').value,10);
  document.getElementById('probe_msg').textContent='starting...';
  try{
    var r=await fetch('/api/probe',{method:'PUT',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({enabled:true,interval_sec:iv,duration_sec:du})});
    if(!r.ok){
      var j=await r.json().catch(function(){return{};});
      throw new Error(j.error||('http '+r.status));
    }
    document.getElementById('probe_msg').textContent='probe started';
    probeRefresh();
  }catch(e){
    document.getElementById('probe_msg').textContent='failed: '+e.message;
  }
});
document.getElementById('probe_stop').addEventListener('click',async function(){
  document.getElementById('probe_msg').textContent='stopping...';
  try{
    var r=await fetch('/api/probe',{method:'PUT',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({enabled:false})});
    if(!r.ok)throw new Error('http '+r.status);
    document.getElementById('probe_msg').textContent='stopped';
    probeRefresh();
  }catch(e){
    document.getElementById('probe_msg').textContent='failed: '+e.message;
  }
});
probeRefresh();
setInterval(probeRefresh,2000);
</script>
</body>
</html>
"""


def classify_sk_line(line):
    """Return ('erxudp', payload_hex) / ('event', id_hex_upper) /
    ('error', code_hex_upper) / None for a single SK output line."""
    if not line:
        return None
    if line.startswith("ERXUDP"):
        parts = line.split()
        if len(parts) >= 10:
            return ("erxudp", parts[-1].strip())
        return None
    if line.startswith("EVENT "):
        rest = line[6:].strip().split()
        if not rest:
            return None
        return ("event", rest[0].upper())
    if line.startswith("FAIL ER"):
        rest = line[7:].strip().split()
        if not rest:
            return None
        return ("error", rest[0].upper())
    return None


def compute_effective_poll_interval(now, base_interval, mode_state):
    """spec 022: burst active なら burst_interval、 expired/off なら base_interval.

    state mutation はしない (caller の RealtimeModeState.tick が担う)。
    burst かつ expires_at が未来 のみ burst_interval 採用、 それ以外は base。"""
    if mode_state.get("mode") == "burst":
        exp = mode_state.get("expires_at")
        if exp is not None and now < exp:
            return int(mode_state.get("burst_interval", base_interval))
    return int(base_interval)


def compute_erxudp_timeout(mode, base_timeout, burst_timeout):
    """spec 023: burst (or catch-up) なら burst_timeout、 それ以外 base.

    burst_timeout <= 0 は kill switch sentinel として base 採用 (dig 決定 B)。
    catch-up は caller 側で mode='burst' として渡す (= burst の余韻)。"""
    if mode == "burst" and burst_timeout > 0:
        return int(burst_timeout)
    return int(base_timeout)


def compute_intra_cycle_retries(mode, base_retries, burst_retries):
    """spec 023: burst (or catch-up) なら burst_retries、 それ以外 base.

    burst default = 0 (= 失敗即次 iter で 5s 体感維持、 dig 決定 A)。
    catch-up は caller が mode='burst' として渡す。"""
    if mode == "burst":
        return int(burst_retries)
    return int(base_retries)


def compute_force_reconnect_threshold(mode, base_threshold, burst_threshold):
    """spec 025: burst (or catch-up) なら burst_threshold、 それ以外 base.

    burst_threshold <= 0 は kill switch sentinel として base 採用 (spec 023 と同じ pattern)。
    caller (main loop) は spec 023 で計算済の `_effective_mode` を渡す。"""
    if mode == "burst" and burst_threshold > 0:
        return int(burst_threshold)
    return int(base_threshold)


def compute_burst_aware_backoff_initial(mode, base_initial, burst_initial):
    """spec 026: burst (or catch-up) なら burst_initial、 それ以外 base.

    burst_initial <= 0 は kill switch sentinel として base 採用 (spec 023/025 と同じ pattern)。
    multiplier / max は base のまま (= burst 中も exponential 維持で連発時の保護)。"""
    if mode == "burst" and burst_initial > 0:
        return int(burst_initial)
    return int(base_initial)


class SendHistoryRing(object):
    """spec 020: TID → (send_ts, epc_tuple) bounded FIFO ring buffer.

    spec 014 で TID mismatch frame を破棄するパスで、 mismatch TID が過去 send
    の TID と一致するなら「メーター ECHONET 内部 queue 遅延応答」 と判定して
    late publish 救済する用途。 main loop only (admin thread から触らない) →
    lock 不要、 plain OrderedDict で FIFO eviction。
    """

    def __init__(self, maxlen=10):
        self._maxlen = int(maxlen)
        self._entries = collections.OrderedDict()

    def record(self, tid, send_ts, epc_list):
        if tid in self._entries:
            del self._entries[tid]  # move to end (refresh)
        self._entries[tid] = (float(send_ts), tuple(epc_list))
        while len(self._entries) > self._maxlen:
            self._entries.popitem(last=False)  # evict oldest

    def lookup(self, tid):
        return self._entries.get(tid)

    def lookup_latest(self):
        """spec 020 v1.5: 直近 send entry を返す (= 空なら None).

        got_tid=0 (= メーター uninit response) を「直近 send への応答」 と仮定して
        late publish 救済する用途。 OrderedDict 末尾 = 最新 send (= record() で
        既存 key は del → 末尾挿入で move-to-end するため insertion order = 時系列)。
        """
        if not self._entries:
            return None
        latest_tid = next(reversed(self._entries))
        return self._entries[latest_tid]

    def __len__(self):
        return len(self._entries)


class RealtimeModeState(object):
    """spec 022: realtime polling mode の thread-safe holder.

    admin API thread と main loop thread の境界。 tick() 戻り値の transition で
    main loop が catch-up trigger / counter 更新を判断する。

    dig 決定 B: tick = (mode, transition) で transition ∈ {None, 'expired', 'aborted'}
    dig 決定 D: start_burst 重複 = expires_at 上書き + _pending_abort クリア
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._mode = "off"
        self._expires_at = None
        self._burst_interval = 5
        self._pending_abort = False

    def start_burst(self, now, duration_sec, interval_sec):
        with self._lock:
            self._mode = "burst"
            self._expires_at = float(now) + float(duration_sec)
            self._burst_interval = int(interval_sec)
            self._pending_abort = False

    def stop_burst(self):
        with self._lock:
            was_active = self._mode == "burst"
            if was_active:
                self._pending_abort = True
            return was_active

    def tick(self, now):
        with self._lock:
            if self._mode == "burst" and self._pending_abort:
                self._mode = "off"
                self._expires_at = None
                self._pending_abort = False
                return ("off", "aborted")
            if (self._mode == "burst" and self._expires_at is not None
                    and now >= self._expires_at):
                self._mode = "off"
                self._expires_at = None
                return ("off", "expired")
            return (self._mode, None)

    def snapshot(self):
        with self._lock:
            return {
                "mode": self._mode,
                "expires_at": self._expires_at,
                "burst_interval": self._burst_interval,
            }


class DiagState(object):
    """Single-thread mutable diagnostics aggregator (main loop only)."""

    def __init__(self, start_time, version):
        self.start_time = start_time
        self.version = version
        self.last_poll_success_ts = None  # epoch seconds, formatted on snapshot
        self.last_poll_failure_ts = None
        self.lqi = None                   # int (decimal)
        self.pan_channel = None           # int (decimal)
        self.scan_retries_total = 0
        self.wisun_reconnects_total = 0
        self.mqtt_reconnects_total = 0
        self.erxudp_timeouts_total = 0
        # Consecutive (not total) ERXUDP timeouts since the last successful
        # poll. Drives auto-reconnect when the smart meter stops answering.
        self.consecutive_erxudp_timeouts = 0
        # Spec 011: intra-cycle retry counters.
        self.erxudp_intra_cycle_retries_total = 0
        self.erxudp_recovered_by_retry_total = 0
        # Spec 012: noise-adaptive skip + ERXUDP TID mismatch observability.
        self.noise_adaptive_skips_total = 0
        self.erxudp_tid_mismatch_total = 0
        # Spec 011 follow-up 2: how many request frames each delayed reply
        # was behind by — i.e. the meter's ECHONET queue depth.
        self.erxudp_tid_mismatch_lags_recent = collections.deque(maxlen=100)
        # Spec 016: HA discovery auto-republish state.
        self.last_discovery_publish_ts = None
        self.pending_discovery_republish = False
        self.discovery_republish_total = 0
        # Spec 017: Wi-SUN rejoin observability + EVENT 24/29 trigger.
        self.serial_reopen_total = 0
        self.consecutive_wisun_connect_failures = 0
        self.pending_wisun_rejoin = False
        # Spec 022: realtime burst mode — counters track start/expire/abort,
        # mode_current is a string gauge published every snapshot, eff_int
        # is set by main loop after first tick (None at boot → omitted from
        # snapshot until first iteration).
        self.realtime_burst_started_total = 0
        self.realtime_burst_completed_total = 0
        self.realtime_burst_aborted_total = 0
        self.realtime_mode_current = "off"
        self.realtime_effective_interval_seconds = None
        # Spec 020: TID mismatch late publish recovery — counter + rolling
        # window of lag in seconds (= now - send_ts at mismatch capture)。
        # spec 048 FR-007: 旧 last_recovered_send_ts bus は撤去、 rescued
        # frame の受け渡しは pending_rescued_frames (下の spec 048 節) に一本化。
        self.erxudp_recovered_from_mismatch_total = 0
        self.erxudp_recovered_lag_seconds_recent = collections.deque(maxlen=100)
        # spec 028: 瞬時電力 0xE7 救済 frame の backfill JSON publish 回数.
        # publish_recovery_backfill 内で直接 += 1 (= on_* method なし、 spec 020
        # last_recovered_send_ts と同じく単純 counter)。
        self.power_w_recovered_backfill_total = 0
        # spec 029: 累積系 (energy_*_kwh) 救済 frame の backfill JSON publish 回数.
        self.cumulative_recovered_backfill_total = 0
        # spec 034: SKSCAN channel mask cache (reconnect 32s → 4s). 単 ch mask
        # scan で PAN 即 hit した回数 / 単 ch fail で fallback 全 scan に落ちた
        # 回数。 cache hit rate = short / (short + fallback) を grafana 観察。
        self.wisun_reconnect_short_scan_total = 0
        self.wisun_reconnect_fallback_full_scan_total = 0
        # spec 035: SKLL64 cached + SKJOIN 直行 (reconnect 35s → 7s). bridge
        # process 生存中、 メーター pan_id/mac/ipv6 を cache して SKSCAN/SKLL64
        # を完全 skip。 SKJOIN N 回連続失敗で cache 全 invalidate → 次回必ず
        # 全 scan path に fallback。
        self.pan_id = None              # str (hex), e.g. "D4E3"
        self.mac = None                 # str (16 文字 hex), e.g. "001C64000B03D4E3"
        self.ipv6 = None                # str (SKLL64 結果 link-local IPv6)
        self.consecutive_skjoin_failures = 0
        self.wisun_reconnect_cached_skjoin_total = 0
        self.wisun_reconnect_cached_skjoin_fallback_total = 0
        # Spec 006: Wi-SUN health observability. Rolling window of recent
        # SKSENDTO→ERXUDP latencies (ms) for percentile reporting.
        self.erxudp_latency_ms_recent = collections.deque(maxlen=200)
        # SK EVENT and FAIL counters keyed by 2-digit hex (upper). All
        # observed ids are counted; only the ids in _PUBLISHED_SK_EVENT_IDS
        # / _PUBLISHED_SK_ERROR_CODES are emitted by snapshot().
        self.sk_event_counts = {}
        self.sk_error_counts = {}
        # spec 037: WOPT FLASH 書込み寿命対策 (= ROPT 確認で WOPT skip)。
        self.wopt_skip_count = 0
        self.wopt_write_count = 0
        # spec 042: SKADDNBR (= IP 層ネイバーキャッシュ登録) 発行成功 / 失敗回数。
        self.skaddnbr_count = 0
        self.skaddnbr_fail_count = 0
        # spec 038 Phase 2: EVENT 21 (= TX Result Notification) PARAM 別 count。
        # PARAM=0 成功 / 1 失敗 (= CSMA/ARIB/Ack) / 2 自動再送、 公式 p.51。
        self.sk_event_21_param_counts = [0, 0, 0]
        # spec 040 Phase 2a: S17=0 (= PaC 自動再認証抑制) 設定回数 + 直近 EVENT 25
        # 受信 epoch ts (= 能動 SKREJOIN tick の base time、 None で未受信)。
        self.s17_off_count = 0
        self.last_event_25_ts = None
        # spec 040 Phase 2b: 能動 SKREJOIN 発火 / 失敗回数 + 発火中 flag
        # (= main loop poll skip 判定、 二重発火防止)。
        self.skrejoin_count = 0
        self.skrejoin_fail_count = 0
        self.skrejoin_active = False
        # spec 051: 計画的 session refresh 発火回数 (= SKREJOIN tick を
        # supersede、 full reconnect path 再利用)。
        self.proactive_rejoin_total = 0
        # spec 046: current_r_a (= 0xE8) 救済 frame backfill 件数 counter。
        # publish_recovery_backfill helper の counter_attr 引数で inc。
        self.current_r_a_recovered_backfill_count = 0
        # spec 047: rescued frame 内訳観測 — ESV / TID / lag bucket の直交
        # 3 軸 + measurement 空 counter。 H1 (= TID=0 誤分類) / H2 (= INF
        # 混入) / H3 (= chain 自走) の支配要因判定用、 観測のみで挙動不変。
        self.erxudp_rescued_esv_counts = {
            "get_res": 0, "get_sna": 0, "inf": 0, "other": 0}
        self.erxudp_rescued_tid_zero_total = 0
        self.erxudp_rescued_tid_ring_hit_total = 0
        self.erxudp_rescued_lag_bucket_counts = {
            "lt5s": 0, "5to60s": 0, "60to300s": 0, "gt300s": 0}
        self.erxudp_rescued_empty_measurement_total = 0
        # spec 048: INF (= ESV 0x73 等の非 Get 応答) を rescue 対象から排除
        # した回数 (= FR-001)。 spec 047 判定で rescued の 59% が INF と確定、
        # deploy 後は esv_inf ≈ 0 / inf_ignored ≈ 6/h への移行が verify 信号。
        self.erxudp_inf_ignored_total = 0
        # spec 048 FR-003: rescue した late frame の (payload, send_ts) を
        # stash して read を継続、 main loop が毎 cycle drain して backfill
        # publish する。 maxlen=8 = reconnect 直後 burst 対策 (通常 0-1 件)。
        self.pending_rescued_frames = collections.deque(maxlen=8)
        # spec 049: noisy cycle の 0xE7 単独 minimal poll 観測 counter。
        # timeout は erxudp_timeouts_total と分離 (= FR-004、 spec 012 の
        # reconnect storm 防止を維持しつつ noise 中の回収率を直接測る)。
        self.noise_minimal_polls_total = 0
        self.noise_minimal_poll_success_total = 0
        self.noise_minimal_poll_timeout_total = 0
        # spec 040 Phase 2b hotfix v4: _wait_skjoin_event25 の terminal reason
        # を bus pattern で共有 (= "event_25" 成功 / "event_24" PANA fail /
        # "timeout" 経過秒切れ)。 skrejoin_tick の失敗 log 精細化に使用、
        # SKREJOIN 失敗原因 (= メーター reject vs handshake 遅延) を data で判定。
        self.last_skjoin_terminal_reason = None
        # Most recent raw ERXUDP line as the SKSTACK printed it. Used by the
        # admin UI to inspect firmware-specific token layout (RSSI / LQI
        # placement varies across SKSTACK builds).
        self.last_erxudp_raw_line = None

    # --- counters (monotonically non-decreasing) ---

    def on_scan_retry(self):
        self.scan_retries_total += 1

    def on_wisun_reconnect(self):
        self.wisun_reconnects_total += 1

    def on_wisun_reconnect_short_scan(self):
        """spec 034: 単 ch mask scan で PAN 即 hit した時 (= 32s → 4s 短縮成功)."""
        self.wisun_reconnect_short_scan_total += 1

    def on_wisun_reconnect_fallback_full_scan(self):
        """spec 034: 単 ch fail で fallback 全 scan に落ちた時 (= chan 変動 / 雑音時)."""
        self.wisun_reconnect_fallback_full_scan_total += 1

    def on_skll64(self, ipv6):
        """spec 035: SKLL64 結果を cache。 wisun_connect の SKLL64 成功直後に呼ぶ。"""
        self.ipv6 = ipv6

    def on_skjoin_success(self):
        """spec 035: SKJOIN 成功で連続失敗 counter リセット。"""
        self.consecutive_skjoin_failures = 0

    def on_skjoin_failure(self, invalidate_threshold=2):
        """spec 035: SKJOIN 失敗で counter += 1、 threshold 以上で cache 全 invalidate
        (= 次回必ず SKSCAN 全 scan path に fallback)。"""
        self.consecutive_skjoin_failures += 1
        if self.consecutive_skjoin_failures >= invalidate_threshold:
            self.pan_channel = None
            self.pan_id = None
            self.mac = None
            self.ipv6 = None

    def on_wisun_reconnect_cached_skjoin(self):
        """spec 035: cached SKJOIN 直行で reconnect 成功した時 (= 35s → 7s 短縮)."""
        self.wisun_reconnect_cached_skjoin_total += 1

    def on_wisun_reconnect_cached_skjoin_fallback(self):
        """spec 035: cached SKJOIN 失敗で fallback 全 scan に落ちた時."""
        self.wisun_reconnect_cached_skjoin_fallback_total += 1

    def on_mqtt_reconnect(self):
        self.mqtt_reconnects_total += 1
        # spec 016: broker reconnect drops retained discovery (or worse, the
        # broker was rebuilt). Flag so the next main-loop tick re-publishes
        # HA discovery and sensors come back without a bridge restart.
        self.pending_discovery_republish = True

    def on_erxudp_timeout(self):
        self.erxudp_timeouts_total += 1
        self.consecutive_erxudp_timeouts += 1

    # Spec 011: intra-cycle retry counters.

    def on_erxudp_intra_cycle_retry(self):
        self.erxudp_intra_cycle_retries_total += 1

    def on_erxudp_recovered_by_retry(self):
        self.erxudp_recovered_by_retry_total += 1

    # Spec 012: noise-adaptive skip + TID mismatch counters.

    def on_noise_adaptive_skip(self):
        self.noise_adaptive_skips_total += 1

    # Spec 016: HA discovery auto-republish tracking.

    def mark_initial_discovery_publish(self, now):
        """Startup publish — seed last_ts so the periodic check doesn't
        fire on the first main-loop tick. counter is NOT touched (counter
        only counts actual re-publishes, so SC-002 stays meaningful)."""
        self.last_discovery_publish_ts = float(now)
        self.pending_discovery_republish = False

    def on_discovery_republish(self, now):
        """An actual re-publish ran (reconnect or interval)."""
        self.discovery_republish_total += 1
        self.last_discovery_publish_ts = float(now)
        self.pending_discovery_republish = False

    # Spec 017: Wi-SUN rejoin observability + EVENT 24/29 immediate trigger.

    def on_serial_reopen(self):
        self.serial_reopen_total += 1

    def on_wisun_pana_fail(self, event_id):
        """spec 017: PANA fail (EVENT 24/29) — signal force-rejoin to the
        main loop, also bump the SK event counter (existing observability
        path so the EVENT id still shows up in /api/diag)."""
        self.on_sk_event(event_id)
        self.pending_wisun_rejoin = True

    def on_erxudp_tid_mismatch(self, expected=None, got=None):
        self.erxudp_tid_mismatch_total += 1
        # spec 047 FR-006: got=0 (= メーター TID echo 不良、 実機観察で
        # mismatch の 100%) は lag が「送信 counter の値」を写すだけの
        # ノイズになるため記録しない。 got≠0 の正規 mismatch のみ残す。
        if got == 0:
            return
        lag = compute_tid_lag(expected, got)
        if lag is not None:
            self.erxudp_tid_mismatch_lags_recent.append(lag)

    # Spec 022: realtime burst mode counters + mode/interval gauges.

    def on_realtime_burst_started(self):
        self.realtime_burst_started_total += 1

    def on_realtime_burst_completed(self):
        self.realtime_burst_completed_total += 1

    def on_realtime_burst_aborted(self):
        self.realtime_burst_aborted_total += 1

    def set_realtime_state(self, mode, effective_interval_seconds):
        self.realtime_mode_current = mode
        self.realtime_effective_interval_seconds = effective_interval_seconds

    # Spec 020: TID mismatch late publish recovery counter.

    def on_erxudp_recovered_from_mismatch(self, lag_sec, send_ts=None):
        # spec 048 FR-007: send_ts 引数は互換のため残すが bus stash は撤去
        # (= rescued frame の受け渡しは pending_rescued_frames に一本化)。
        self.erxudp_recovered_from_mismatch_total += 1
        self.erxudp_recovered_lag_seconds_recent.append(float(lag_sec))

    # spec 047: rescued frame 内訳観測 — read_erxudp の rescue 確定箇所から
    # ESV 分類 / TID=0 か / lag 秒数を 1 call で受けて 3 軸 counter を inc。

    def on_erxudp_rescued(self, esv_kind, tid_zero, lag_sec):
        if esv_kind not in self.erxudp_rescued_esv_counts:
            esv_kind = "other"
        self.erxudp_rescued_esv_counts[esv_kind] += 1
        if tid_zero:
            self.erxudp_rescued_tid_zero_total += 1
        else:
            self.erxudp_rescued_tid_ring_hit_total += 1
        self.erxudp_rescued_lag_bucket_counts[
            classify_rescued_lag_bucket(lag_sec)] += 1

    def on_erxudp_rescued_empty_measurement(self):
        self.erxudp_rescued_empty_measurement_total += 1

    def on_erxudp_inf_ignored(self):
        self.erxudp_inf_ignored_total += 1

    # spec 049: noisy cycle minimal poll 観測。

    def on_noise_minimal_poll(self):
        self.noise_minimal_polls_total += 1

    def on_noise_minimal_poll_success(self):
        self.noise_minimal_poll_success_total += 1

    def on_noise_minimal_poll_timeout(self):
        self.noise_minimal_poll_timeout_total += 1

    # Spec 006: Wi-SUN health observability — rolling RTT + event/error tallies.

    def on_erxudp_latency(self, ms):
        self.erxudp_latency_ms_recent.append(float(ms))

    def on_sk_event(self, event_id):
        key = str(event_id).upper()
        self.sk_event_counts[key] = self.sk_event_counts.get(key, 0) + 1

    def on_sk_error(self, error_code):
        key = str(error_code).upper()
        self.sk_error_counts[key] = self.sk_error_counts.get(key, 0) + 1

    def on_wopt_skip(self):
        self.wopt_skip_count += 1

    def on_wopt_write(self):
        self.wopt_write_count += 1

    def on_skaddnbr_success(self):
        """spec 042: SKADDNBR (= IP 層ネイバーキャッシュ登録) 発行成功時に発火。"""
        self.skaddnbr_count += 1

    def on_skaddnbr_fail(self):
        """spec 042: SKADDNBR 発行失敗時に発火 (= 安全側 continue、 観測点)。"""
        self.skaddnbr_fail_count += 1

    def on_sk_event_21_param(self, param):
        """spec 038 Phase 2: EVENT 21 PARAM 別 (= 0 成功 / 1 失敗 / 2 自動再送)
        の counter を増加。 範囲外の PARAM は ignore (= 仕様外値で例外起こさない)。
        """
        if isinstance(param, int) and 0 <= param <= 2:
            self.sk_event_21_param_counts[param] += 1

    def on_s17_off_set(self):
        """spec 040 Phase 2a: SKSREG S17 0 発行 (= PaC 自動再認証抑制) 時に発火。"""
        self.s17_off_count += 1

    def on_skrejoin_success(self):
        """spec 040 Phase 2b: 能動 SKREJOIN 成功時 (= EVENT 25 受信) に発火。"""
        self.skrejoin_count += 1

    def on_skrejoin_fail(self):
        """spec 040 Phase 2b: 能動 SKREJOIN 失敗時 (= EVENT 24 / timeout / exception)
        に発火、 既存 reconnect path に fallback (= pending_wisun_rejoin=True)。"""
        self.skrejoin_fail_count += 1

    def on_proactive_rejoin(self):
        """spec 051: 計画的 session refresh 発火時 (= raise 直前) に inc。"""
        self.proactive_rejoin_total += 1

    def on_erxudp_raw(self, line):
        self.last_erxudp_raw_line = line

    # --- timestamps ---

    def on_poll_success(self, now):
        self.last_poll_success_ts = now
        self.consecutive_erxudp_timeouts = 0

    def on_poll_failure(self, now):
        self.last_poll_failure_ts = now

    # --- PAN info from SKSCAN ---

    def on_wisun_joined(self, pan_info):
        lqi_hex = pan_info.get("LQI")
        if lqi_hex is not None:
            self.lqi = int(lqi_hex, 16)
        chan_hex = pan_info.get("Channel")
        if chan_hex is not None:
            self.pan_channel = int(chan_hex, 16)
        # spec 035: cached SKJOIN 直行用に pan_id/mac も cache
        pan_id = pan_info.get("Pan ID")
        if pan_id is not None:
            self.pan_id = pan_id
        mac = pan_info.get("Addr")
        if mac is not None:
            self.mac = mac

    # --- snapshot for MQTT publish ---

    def snapshot(self, now):
        uptime = int(now - self.start_time)
        if uptime < 0:
            uptime = 0
        raw = {
            "last_poll_success_ts":
                format_iso8601_utc(self.last_poll_success_ts)
                if self.last_poll_success_ts is not None else None,
            "last_poll_failure_ts":
                format_iso8601_utc(self.last_poll_failure_ts)
                if self.last_poll_failure_ts is not None else None,
            "lqi": self.lqi,
            "pan_channel": self.pan_channel,
            "scan_retries_total": self.scan_retries_total,
            "wisun_reconnects_total": self.wisun_reconnects_total,
            "mqtt_reconnects_total": self.mqtt_reconnects_total,
            "erxudp_timeouts_total": self.erxudp_timeouts_total,
            "erxudp_intra_cycle_retries_total":
                self.erxudp_intra_cycle_retries_total,
            "erxudp_recovered_by_retry_total":
                self.erxudp_recovered_by_retry_total,
            "erxudp_tid_mismatch_total": self.erxudp_tid_mismatch_total,
            "noise_adaptive_skips_total": self.noise_adaptive_skips_total,
            "last_discovery_publish_ts":
                format_iso8601_utc(self.last_discovery_publish_ts)
                if self.last_discovery_publish_ts is not None else None,
            "discovery_republish_total": self.discovery_republish_total,
            "serial_reopen_total": self.serial_reopen_total,
            "realtime_burst_started_total":
                self.realtime_burst_started_total,
            "realtime_burst_completed_total":
                self.realtime_burst_completed_total,
            "realtime_burst_aborted_total":
                self.realtime_burst_aborted_total,
            "realtime_mode_current": self.realtime_mode_current,
            "realtime_effective_interval_seconds":
                self.realtime_effective_interval_seconds,
            # spec 020: TID mismatch late publish recovery counter.
            "erxudp_recovered_from_mismatch_total":
                self.erxudp_recovered_from_mismatch_total,
            # spec 028: 瞬時電力 0xE7 救済 frame backfill counter.
            "power_w_recovered_backfill_total":
                self.power_w_recovered_backfill_total,
            # spec 029: 累積系 (energy_*_kwh) 救済 frame backfill counter.
            "cumulative_recovered_backfill_total":
                self.cumulative_recovered_backfill_total,
            # spec 034: SKSCAN channel mask cache effect counters.
            "wisun_reconnect_short_scan_total":
                self.wisun_reconnect_short_scan_total,
            "wisun_reconnect_fallback_full_scan_total":
                self.wisun_reconnect_fallback_full_scan_total,
            # spec 035: SKLL64 cached + SKJOIN 直行 effect counters.
            "wisun_reconnect_cached_skjoin_total":
                self.wisun_reconnect_cached_skjoin_total,
            "wisun_reconnect_cached_skjoin_fallback_total":
                self.wisun_reconnect_cached_skjoin_fallback_total,
            "uptime_seconds": uptime,
            "version": self.version,
        }
        # Preserve declared order; drop None entries so HA keeps "unknown".
        out = collections.OrderedDict()
        for key in _DIAG_SNAPSHOT_KEYS:
            value = raw[key]
            if value is None:
                continue
            out[key] = value

        # Spec 006: latency percentiles. Omit when no samples so HA does not
        # see a misleading "0 ms" entity at boot.
        samples = list(self.erxudp_latency_ms_recent)
        if samples:
            ordered = sorted(samples)
            out["erxudp_latency_p50_ms"] = round(_percentile(ordered, 50), 2)
            out["erxudp_latency_p95_ms"] = round(_percentile(ordered, 95), 2)
            out["erxudp_latency_max_ms"] = round(ordered[-1], 2)

        # Spec 011 follow-up 2: TID mismatch lag percentiles. Omit when no
        # mismatch has ever been observed.
        lags = list(self.erxudp_tid_mismatch_lags_recent)
        if lags:
            ordered_lags = sorted(lags)
            out["erxudp_tid_mismatch_lag_p50"] = int(round(
                _percentile(ordered_lags, 50)))
            out["erxudp_tid_mismatch_lag_p95"] = int(round(
                _percentile(ordered_lags, 95)))
            out["erxudp_tid_mismatch_lag_max"] = ordered_lags[-1]

        # Spec 020: TID mismatch late publish recovery lag (seconds) percentiles.
        # Omit when no late publish has ever occurred so HA keeps "unknown".
        rec_lags = list(self.erxudp_recovered_lag_seconds_recent)
        if rec_lags:
            ordered_rec = sorted(rec_lags)
            out["erxudp_recovered_lag_p50"] = int(round(
                _percentile(ordered_rec, 50)))
            out["erxudp_recovered_lag_p95"] = int(round(
                _percentile(ordered_rec, 95)))
            out["erxudp_recovered_lag_max"] = int(round(ordered_rec[-1]))

        # spec 047: rescued frame 内訳 counter。 比率計算 (= SC-1 の分類漏れ
        # 検算) に 0 も必要なので zero-omit しない (= FR-005)。
        for _kind in ("get_res", "get_sna", "inf", "other"):
            out["erxudp_rescued_esv_{}_total".format(_kind)] = (
                self.erxudp_rescued_esv_counts[_kind])
        out["erxudp_rescued_tid_zero_total"] = (
            self.erxudp_rescued_tid_zero_total)
        out["erxudp_rescued_tid_ring_hit_total"] = (
            self.erxudp_rescued_tid_ring_hit_total)
        for _bucket in ("lt5s", "5to60s", "60to300s", "gt300s"):
            out["erxudp_rescued_lag_{}_total".format(_bucket)] = (
                self.erxudp_rescued_lag_bucket_counts[_bucket])
        out["erxudp_rescued_empty_measurement_total"] = (
            self.erxudp_rescued_empty_measurement_total)
        # spec 048: INF 排除 counter (= 0 でも publish、 SC-1 verify 用)。
        out["erxudp_inf_ignored_total"] = self.erxudp_inf_ignored_total
        # spec 049: minimal poll counter 3 本 (= 回収率計算に 0 も必要)。
        out["noise_minimal_polls_total"] = self.noise_minimal_polls_total
        out["noise_minimal_poll_success_total"] = (
            self.noise_minimal_poll_success_total)
        out["noise_minimal_poll_timeout_total"] = (
            self.noise_minimal_poll_timeout_total)

        # Spec 006: named EVENT / ER counters. Omit zero-count entries
        # so HA discovery does not advertise sensors that have never fired.
        for eid in _PUBLISHED_SK_EVENT_IDS:
            n = self.sk_event_counts.get(eid, 0)
            if n > 0:
                out["sk_event_{}_total".format(eid)] = n
        for code in _PUBLISHED_SK_ERROR_CODES:
            n = self.sk_error_counts.get(code, 0)
            if n > 0:
                out["sk_error_ER{}_total".format(code)] = n
        # spec 037: WOPT FLASH 書込み寿命対策。 0 でも常時 publish (= skip が
        # 機能しているかを観測可能にするため、 zero-omit pattern からは外す)。
        out["wopt_write_skipped_total"] = self.wopt_skip_count
        out["wopt_write_total"] = self.wopt_write_count
        # spec 042: SKADDNBR 発行 counter。 同じく 0 でも常時 publish (= deploy
        # 後すぐに発行件数 > 0 となることが期待される観測点)。
        out["skaddnbr_total"] = self.skaddnbr_count
        out["skaddnbr_fail_total"] = self.skaddnbr_fail_count
        # spec 038 Phase 2: EVENT 21 PARAM 別 counter。 0 でも常時 publish
        # (= PARAM=1 (TX 失敗) の割合が ROI 判定の鍵、 0 件でも publish が必要)。
        out["sk_event_21_param0_total"] = self.sk_event_21_param_counts[0]
        out["sk_event_21_param1_total"] = self.sk_event_21_param_counts[1]
        out["sk_event_21_param2_total"] = self.sk_event_21_param_counts[2]
        # spec 040 Phase 2a: S17=0 設定 counter (= 起動毎に 1 件以上) +
        # 直近 EVENT 25 経過秒 (= Phase 2b 能動 SKREJOIN tick の判定 base)。
        # last_event_25_ts が None (= 未受信 = bridge 起動直後) なら omit。
        out["s17_off_total"] = self.s17_off_count
        if self.last_event_25_ts is not None:
            out["last_event_25_seconds"] = int(now - self.last_event_25_ts)
        # spec 040 Phase 2b: 能動 SKREJOIN 発火 / 失敗 counter (= 常時 publish、
        # 0 でも reconnect 経由 reauth が支配的なケースの観測点)。
        out["skrejoin_total"] = self.skrejoin_count
        out["skrejoin_fail_total"] = self.skrejoin_fail_count
        # spec 051: 計画的 session refresh counter (= 0 でも publish)。
        out["proactive_rejoin_total"] = self.proactive_rejoin_total
        # spec 046: current_r_a (= 0xE8) 救済 frame backfill counter。
        out["current_r_a_recovered_backfill_total"] = (
            self.current_r_a_recovered_backfill_count)
        return out

# ---------------------------------------------------------------------------
# Serial port (termios, no pyserial)
# ---------------------------------------------------------------------------

def open_serial(port, baud=115200):
    fd = os.open(port, os.O_RDWR | os.O_NOCTTY)

    attrs = list(termios.tcgetattr(fd))
    iflag, oflag, cflag, lflag = attrs[0], attrs[1], attrs[2], attrs[3]

    # raw input
    iflag &= ~(termios.IGNBRK | termios.BRKINT | termios.PARMRK |
               termios.ISTRIP | termios.INLCR  | termios.IGNCR  |
               termios.ICRNL  | termios.IXON)
    oflag &= ~termios.OPOST
    cflag &= ~(termios.CSIZE | termios.PARENB)
    cflag |=  termios.CS8 | termios.CREAD | termios.CLOCAL
    lflag &= ~(termios.ECHO | termios.ECHONL | termios.ICANON |
               termios.ISIG | termios.IEXTEN)

    baud_map = {
        9600:   termios.B9600,
        19200:  termios.B19200,
        38400:  termios.B38400,
        57600:  termios.B57600,
        115200: termios.B115200,
    }
    baud_const = baud_map.get(baud, termios.B115200)

    cc = attrs[6]
    # attrs[6] must be returned in the same type tcgetattr gave us.
    # On this device Python 2.7 it is a list of 32 ints; tcsetattr rejects bytes.
    if isinstance(cc, list):
        cc_list = list(cc)
        cc_list[termios.VMIN]  = 1
        cc_list[termios.VTIME] = 0
        attrs[6] = cc_list
    else:
        # bytes/bytearray path
        cc_arr = bytearray(cc)
        cc_arr[termios.VMIN]  = 1
        cc_arr[termios.VTIME] = 0
        attrs[6] = bytes(cc_arr)

    attrs[0], attrs[1], attrs[2], attrs[3] = iflag, oflag, cflag, lflag
    attrs[4] = baud_const
    attrs[5] = baud_const

    termios.tcsetattr(fd, termios.TCSANOW, attrs)
    termios.tcflush(fd, termios.TCIOFLUSH)
    return fd

def serial_write(fd, data):
    if isinstance(data, bytes):
        os.write(fd, data)
    else:
        os.write(fd, data.encode("ascii"))

def serial_readline(fd, timeout=10):
    """Read one CRLF-terminated line; return decoded str or None on timeout."""
    buf = b""
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        r, _, _ = select.select([fd], [], [], min(remaining, 0.5))
        if not r:
            continue
        ch = os.read(fd, 1)
        if not ch:
            continue
        buf += ch
        if buf.endswith(b"\r\n"):
            return buf[:-2].decode("ascii", errors="replace")
    return buf.decode("ascii", errors="replace") if buf else None

def _led_blink(stop_event, colors, interval=0.2):
    i = 0
    while not stop_event.is_set():
        led_rgb(*colors[i % len(colors)])
        i += 1
        stop_event.wait(interval)

def skcommand(fd, cmd, timeout=10):
    """Send one SKSTACK command; return list of response lines (up to OK/FAIL)."""
    orig_led = led_read()
    stop_event = threading.Event()
    t = threading.Thread(target=_led_blink,
                         args=(stop_event, [(0, 255, 0), (0, 0, 255)]))
    t.daemon = True
    t.start()

    serial_write(fd, cmd + "\r\n")
    lines = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            line = serial_readline(fd, timeout=max(0.5, deadline - time.time()))
            if line is None:
                break
            lines.append(line)
            if line in ("OK", ) or line.startswith("FAIL"):
                break
    finally:
        stop_event.set()
        t.join(timeout=1)
        led_rgb(*orig_led)
    return lines


def _add_neighbor_cache(fd, ipv6, mac, diag_state=None):
    """spec 042: SKADDNBR で IP 層ネイバーキャッシュに Reachable 状態で登録。

    公式 BP35A1 Ver 1.3.2 p.29: 「指定した IPv6 アドレスと MAC アドレス情報を
    IP 層のネイバーキャッシュに Reachable 状態で登録、 アドレス要請を省略して
    直接 IP パケットを出力」。 SKJOIN 成功直後に呼ぶことで初回 SKSENDTO の
    Neighbor Solicitation (= 1-2s 追加) を省略する。 失敗時は安全側 continue
    (= 次の SKSENDTO は通常 Neighbor Solicitation で解決)、 throw しない。
    SKRESET でクリアされる L3 ネイバーキャッシュなので毎回 SKJOIN 後に再登録
    する揮発前提設計、 spec 039 SKSAVE のような構造的問題なし。
    """
    try:
        skcommand(fd, "SKADDNBR {} {}".format(ipv6, mac))
        log("SKADDNBR OK ({} {})".format(ipv6, mac))
        if diag_state is not None:
            try:
                diag_state.on_skaddnbr_success()
            except Exception as e:
                log("diag on_skaddnbr_success error: {}".format(e))
    except Exception as e:
        log("SKADDNBR failed ({}), continue".format(e))
        if diag_state is not None:
            try:
                diag_state.on_skaddnbr_fail()
            except Exception as e2:
                log("diag on_skaddnbr_fail error: {}".format(e2))


def ropt(fd, timeout=3):
    """Read current WOPT MODE via ROPT. Returns int 0-0xFF. Raises on failure.

    ROPT response format (BP35A1 Ver 1.3.2 p.42): "OK <MODE:2 桁 hex>".
    skcommand cannot parse this because its break condition is exact "OK"
    string match. spec 037 で WOPT FLASH 書込み寿命対策の判定に使用。
    """
    serial_write(fd, "ROPT\r\n")
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = serial_readline(fd, timeout=max(0.5, deadline - time.time()))
        if line is None:
            continue
        if line.startswith("OK"):
            parts = line.split()
            if len(parts) >= 2:
                return int(parts[1], 16)
            raise RuntimeError("ROPT malformed response: {!r}".format(line))
        if line.startswith("FAIL"):
            raise RuntimeError("ROPT failed: {}".format(line))
    raise RuntimeError("ROPT timeout")


# ---------------------------------------------------------------------------
# Scan settings
# ---------------------------------------------------------------------------

# ROHM SKSCAN duration: 1 ch あたり dwell time = 0.0096 * (2^duration + 1) 秒
# (= 960 symbols at 100 kbps GFSK = IEEE 802.15.4 aBaseSuperframeDuration、
#    BP35A1 公式リファレンス Ver 1.3.2 p.20)。
# 全 28 ch スキャンの理論換算: duration=4 → 約 4.6 秒、 5 → 8.9 秒、
# 6 → 17.5 秒、 7 → 34.7 秒。 実機 BP35C0 EVER 1.5.2 では beacon 応答待ち
# overhead 等で 1-2 倍の実時間が観測される (= duration=6 で実測 17-32 秒)。
# 4 は最小推奨だが弱信号環境では PAN を取りこぼし scan_retries が嵩む。
# 6 にすると初回 scan が 32 秒、 LQI 推定精度も上がる。
SCAN_DURATION_BASE = 6
SCAN_RETRY_LIMIT = 10
# spec 054: 全 ch scan の mask = BP35CX の ch33-60 (= EEDSCAN と同じ 28 bit)。
FULL_SCAN_CHANNEL_MASK = "0FFFFFFF"
# spec 054: 実機 (EVER 1.5.2) で d=6 全 ch scan = 17.7s (= 28ch で ~0.63s/ch)。
# 仕様書の 0.96ms * (2^d + 1) に加え ch ごとの固定 overhead があるので余裕を持つ。
SKSCAN_PER_CHANNEL_OVERHEAD_SEC = 1.0
SKSCAN_TIMEOUT_MARGIN_SEC = 10.0


def compute_skscan_timeout(channel_mask, duration):
    """spec 054: EVENT 22 (scan 完了) を待つ上限秒数。

    旧実装は duration code をそのまま秒数として使い、 scan 完了前に打ち切って
    いた。 mask の bit 数 (= scan 対象 ch 数) と duration から所要時間を見積もる。
    """
    channels = bin(int(channel_mask, 16)).count("1")
    per_channel = 0.00096 * (2 ** int(duration) + 1) + SKSCAN_PER_CHANNEL_OVERHEAD_SEC
    return channels * per_channel + SKSCAN_TIMEOUT_MARGIN_SEC


def channel_to_mask(ch):
    """spec 034: BP35CX channel 番号 (= ch33-60) を SKSCAN 2 用 32-bit
    channel mask に変換。 ch33 = bit 0, ch60 = bit 27.

    例: channel_to_mask(57) = 0x01000000 (= bit 24)
        channel_to_mask(33) = 0x00000001
    """
    return 1 << (int(ch) - 33)

# ---------------------------------------------------------------------------
# SKSTACK-IP / Wi-SUN B-route connection
# ---------------------------------------------------------------------------

def skscan(fd, channel_mask=FULL_SCAN_CHANNEL_MASK, duration=SCAN_DURATION_BASE,
           max_retries=None, diag_state=None):
    """Active scan with retries; returns best PAN info dict or empty dict.

    `channel_mask` / `duration` / `max_retries` are spec 034 additions:
    - `channel_mask` (= 32-bit hex string) で対象 channel 限定 (= 単 ch なら
      `channel_to_mask(ch)` を `{:08X}.format(...)` で渡す)
    - `duration` の初期値 (= 旧挙動は SCAN_DURATION_BASE 固定)
    - `max_retries` (= None で旧挙動 = duration += 1 で SCAN_RETRY_LIMIT まで
      retry、 1 なら 1 回試行で抜け = 単 ch path 用 fast fail)

    `diag_state` is optional. When given, its `on_scan_retry()` is invoked
    each time the scan widens its duration. Wrapped in try/except so a diag
    bug never blocks the measurement path (Constitution IV).
    """
    attempt = 0

    while duration <= SCAN_RETRY_LIMIT:
        # Clear stale lines from previous command/scan cycle.
        termios.tcflush(fd, termios.TCIFLUSH)

        log("SKSCAN try mask={} duration={}".format(channel_mask, duration))
        # BP35C0 style scan command: <mode> <channel_mask> <duration> <side>
        serial_write(fd, "SKSCAN 2 {} {} 0\r\n".format(channel_mask, duration))

        pan_list  = []
        current   = {}
        scan_done = False
        # spec 054: duration は秒数ではなく scan 時間の code。 EVENT 22 まで待つ。
        deadline  = time.time() + compute_skscan_timeout(channel_mask, duration)
        while time.time() < deadline:
            line = serial_readline(fd, timeout=2)
            if line is None:
                continue
            if line.startswith("EVENT 20"):
                if current:
                    pan_list.append(current)
                current = {}
            elif line.startswith("EVENT 22"):
                if current:
                    pan_list.append(current)
                scan_done = True
                break  # Exit loop once EVENT 22 received
            elif ":" in line and not line.startswith("EVENT"):
                key, _, val = line.strip().partition(":")
                current[key.strip()] = val.strip()

        if pan_list:
            log("SKSCAN found {} PAN(s), selecting best LQI".format(len(pan_list)))
            pan_list.sort(key=lambda p: int(p.get("LQI", "0"), 16), reverse=True)
            return pan_list[0]

        if LOGGER is not None:
            emit_scan_retry(LOGGER, duration=duration + 1)
        else:
            log("SKSCAN no PAN found, retrying with longer duration")
        if diag_state is not None:
            try:
                diag_state.on_scan_retry()
            except Exception as e:
                log("diag on_scan_retry error: {}".format(e))
        attempt += 1
        if max_retries is not None and attempt >= max_retries:
            break
        duration += 1

    return {}

def skll64(fd, mac):
    """Convert MAC address to IPv6 link-local address.

    Reads lines until an IPv6-like substring (hex digits + colons) is found
    and validated. Returns the candidate string or None on timeout.
    """
    serial_write(fd, "SKLL64 {}\r\n".format(mac))
    deadline = time.time() + 10
    while time.time() < deadline:
        line = serial_readline(fd, timeout=2)
        if not line:
            continue
        # skip echoes and obvious non-data lines
        if line.startswith("SKLL64") or line.strip() == "":
            continue
        # extract only hex+colon runs (length threshold to avoid short noise)
        m = re.search(r'([0-9A-Fa-f:]{15,})', line)
        if not m:
            continue
        candidate = m.group(1)
        # validate with inet_pton if available
        try:
            socket.inet_pton(socket.AF_INET6, candidate)
            return candidate
        except Exception:
            # not valid IPv6; continue waiting for a proper response
            log("skll64: received candidate but validation failed: {}".format(candidate))
            continue
    return None

def _wisun_init_sequence(fd, br_id, br_pwd, diag_state=None):
    """spec 035: SKRESET + identity + credential 設定を helper 化。

    既存 wisun_connect 先頭の冪等な init を抽出、 cached SKJOIN 失敗時の
    fallback 経路で 2 度目の clean state init にも再利用 (= EVENT 24 後の
    BP35CX internal state 残留 risk 回避)。

    spec 037: WOPT は FLASH 書込み 10,000 回制限 (BP35A1 Ver 1.3.2 p.41) の
    対策で、 ROPT で現在値を読み bit0=1 なら skip する。 diag_state 引数は
    skip/write のカウント記録用 (= None なら無視、 既存呼出互換)。
    """
    log("SKRESET")
    skcommand(fd, "SKRESET", timeout=5)
    time.sleep(1)

    # SKVER は起動時の identity log として残す (debug 用、 副作用なし)。
    try:
        ver = skcommand(fd, "SKVER", timeout=2)
        log("SKVER: {}".format(ver))
    except Exception as e:
        log("SKVER failed: {}".format(e))

    log("SKSETPWD")
    skcommand(fd, "SKSETPWD C {}".format(br_pwd))

    log("SKSETRBID")
    skcommand(fd, "SKSETRBID {}".format(br_id))

    # Force ASCII-hex ERXUDP payload format so parser stays stable.
    # spec 037: WOPT 1 を毎回発行すると FLASH 寿命 10,000 回を消費するため、
    # ROPT で bit0=1 なら skip。 ROPT 失敗時は安全側で WOPT 1 を発行。
    try:
        current_wopt = ropt(fd)
        if (current_wopt & 1) == 1:
            log("WOPT 1 skip (ROPT={:02X})".format(current_wopt))
            if diag_state is not None:
                diag_state.on_wopt_skip()
        else:
            skcommand(fd, "WOPT 1")
            if diag_state is not None:
                diag_state.on_wopt_write()
    except Exception as e:
        log("ROPT failed ({}), fallback to WOPT 1".format(e))
        skcommand(fd, "WOPT 1")
        if diag_state is not None:
            diag_state.on_wopt_write()

    # spec 040 Phase 2a: S17=0 (= PaC 自動再認証抑制) を毎回発行。 S17 は通常
    # SKSREG レジスタで SKRESET でクリアされる ([[feedback-bp35a1-skreset-clears-sksreg-non-product]])、
    # SKSAVE は不要 (= 揮発で OK、 FLASH 寿命影響 0)。 Phase 2b で main loop が
    # 能動 SKREJOIN を発火するための前提設定。
    try:
        skcommand(fd, "SKSREG S17 0")
        if diag_state is not None:
            diag_state.on_s17_off_set()
        log("S17=0 set (= PaC auto reauth disabled、 spec 040 Phase 2a)")
    except Exception as e:
        log("SKSREG S17 0 failed ({}), continue (= 自動再認証は default S17=1 で継続)".format(e))


# spec 040 Phase 2b: 能動 SKREJOIN 発火の threshold (= last EVENT 25 からの経過秒)。
# 公式 BP35A1 Ver 1.3.2 p.14 で「PaC は PAA が指定したセッションライフタイムの
# 80% (= 720s) で自動的に SKREJOIN を実行」 = PAA セッション終了要請発火前に
# 確実に再認証完了させる必要。
#
# 初回 deploy (= f5dd4fc / c8e869b、 threshold=600s) では 8h で skrejoin_total=0
# 判明 = 実測 reconnect 周期 11-12 分 (= wisun_joined 23:36→23:47→23:59→00:10)
# より tick=600s (10 分) が PAA 主導 termination + reconnect + EVENT 25 hook
# 更新に間に合わず should_fire_skrejoin 常に False に。 tick=480s (8 分) に下方
# 修正で reconnect 周期より 3-4 分早く発火、 hotfix 後の初回 deploy で確認。
SKREJOIN_TICK_SECONDS = 480


class ProactiveSessionRefresh(RuntimeError):
    """spec 051: 計画的 session refresh の signal 例外。

    既存 reconnect path (= main loop 外側 except) を再利用しつつ、
    isinstance 判定で backoff を 0 に short-circuit する (= 失敗由来では
    ないので spec 017 の exponential backoff は不要)。"""
    pass


def should_fire_proactive_rejoin(last_event_25_ts, now, after_sec,
                                 skrejoin_active):
    """spec 051 FR-001: 計画的 full reconnect の発火判定 (pure helper)。

    PANA session 失効 (~10-12 分周期) を待ち受けるのではなく、 実測寿命
    より手前 (default 480s) で先回りして cached SKJOIN reconnect する。
    spec 040 Phase 2b (= SKREJOIN in-place 再認証、 94-100% reject) と
    同じ発火点・同じ base time で、 行動だけ実績のある full reconnect に
    差し替えたもの。"""
    if last_event_25_ts is None:
        return False
    if skrejoin_active:
        return False
    return (now - last_event_25_ts) > after_sec


def should_fire_skrejoin(diag_state, now):
    """spec 040 Phase 2b: 能動 SKREJOIN 発火条件判定。

    条件: last_event_25_ts は記録済 (= 初回 SKJOIN 後) AND 経過秒が
    SKREJOIN_TICK_SECONDS を超過 AND 既存 skrejoin_active=False (= 二重発火防止)。
    main loop の poll_success 直後で判定 → True なら skrejoin_tick 発火。
    """
    if diag_state.last_event_25_ts is None:
        return False
    if diag_state.skrejoin_active:
        return False
    return (now - diag_state.last_event_25_ts) > SKREJOIN_TICK_SECONDS


def skrejoin_tick(fd, diag_state, pan, ipv6):
    """spec 040 Phase 2b: 能動 SKREJOIN 発火 + EVENT 25 待ち。

    SKREJOIN 公式仕様 (= BP35A1 Ver 1.3.2 p.14): SKJOIN 後の任意のタイミングで
    再認証を発行可能、 成功で暗号キー + セッションライフタイム更新。 失敗時は
    既存 reconnect path に fallback (= pending_wisun_rejoin=True、 main loop が
    次 cycle で wisun_connect 再実行)。

    diag_state.skrejoin_active を try/finally で必ず False 復帰 (= 例外でも
    flag が True で stuck しないよう保証)。 成功で last_event_25_ts は
    _wait_skjoin_event25 hook 経由で自動更新済。
    """
    diag_state.skrejoin_active = True
    try:
        elapsed = int(time.time() - diag_state.last_event_25_ts)
        log("能動 SKREJOIN 発火 (last_event_25_seconds={})".format(elapsed))
        skcommand(fd, "SKREJOIN", timeout=2)
        # hotfix v3 (= 61a1174 の実機観察): timeout=30s では 17 件連続失敗 (=
        # 31-33s で EVENT 25 timeout)、 SKREJOIN 再認証は cached SKJOIN より
        # 実測長く 30s 上限 では届かない = 60s に延長。 SKJOIN full path (= 90s)
        # と cached path (= 30s) の中間、 PANA 4-way handshake の実測余裕。
        if _wait_skjoin_event25(fd, pan, ipv6, timeout=60, diag_state=diag_state):
            diag_state.on_skrejoin_success()
            log("能動 SKREJOIN 成功")
            return True
        else:
            diag_state.on_skrejoin_fail()
            diag_state.pending_wisun_rejoin = True
            # hotfix v4: terminal reason を明示 log = メーター EVENT 24 reject
            # (= 仕様上の壁) と timeout (= handshake 遅延) を data で分離判定。
            reason = getattr(diag_state, "last_skjoin_terminal_reason", "unknown")
            log("能動 SKREJOIN 失敗 (reason={}) 、 既存 reconnect path に fallback"
                .format(reason))
            return False
    except Exception as e:
        diag_state.on_skrejoin_fail()
        diag_state.pending_wisun_rejoin = True
        log("能動 SKREJOIN 例外 ({}), 既存 reconnect path に fallback".format(e))
        return False
    finally:
        diag_state.skrejoin_active = False


def _wait_skjoin_event25(fd, pan, ipv6, timeout, diag_state=None):
    """spec 035: SKJOIN 発行後の EVENT 25/24 待ち loop を helper 化。

    戻り値: True (= EVENT 25 接続成功) / False (= EVENT 24 PANA fail or
    timeout = 呼出側で fallback or raise 判定)。 LED blink 副作用込み。
    cached path = timeout=30 (= 早諦め fallback)、 full path = timeout=90
    (= 既存挙動互換)。

    spec 044: diag_state を受け取り、 EVENT 25/24 受信時に on_sk_event を呼ぶ
    (= spec 006 metric 観測 hook を SKJOIN 文脈にも反映、 spec 035 抽出時に
    漏れていた hook の補填)。 diag_state=None で既存呼出互換。
    """
    orig_led = led_read()
    stop_event = threading.Event()
    t = threading.Thread(target=_led_blink,
                         args=(stop_event, [(0, 255, 0), (0, 0, 255)]))
    t.daemon = True
    t.start()
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = serial_readline(fd, timeout=2)
            if line is None:
                continue
            if "EVENT 25" in line:
                if LOGGER is not None:
                    emit_wisun_joined(LOGGER, pan=pan, ipv6=ipv6)
                else:
                    log("SKJOIN: connected")
                if diag_state is not None:
                    try:
                        diag_state.on_sk_event("25")
                        # spec 040 Phase 2a: 直近 EVENT 25 受信 ts を記録
                        # (= 能動 SKREJOIN tick の base time)。
                        diag_state.last_event_25_ts = time.time()
                        # spec 040 Phase 2b hotfix v4: terminal reason bus。
                        diag_state.last_skjoin_terminal_reason = "event_25"
                    except Exception as e:
                        log("diag on_sk_event(25) error: {}".format(e))
                return True
            if "EVENT 24" in line:
                if LOGGER is not None:
                    emit_wisun_join_failed(LOGGER,
                                           reason="PANA authentication failed (EVENT 24)")
                if diag_state is not None:
                    try:
                        diag_state.on_sk_event("24")
                        # spec 040 Phase 2b hotfix v4: terminal reason bus。
                        diag_state.last_skjoin_terminal_reason = "event_24"
                    except Exception as e:
                        log("diag on_sk_event(24) error: {}".format(e))
                return False
        if diag_state is not None:
            diag_state.last_skjoin_terminal_reason = "timeout"
        return False  # timeout
    finally:
        stop_event.set()
        t.join(timeout=1)
        led_rgb(*orig_led)


def wisun_connect(fd, br_id, br_pwd, prefer_known_channel=False,
                  fallback_duration=SCAN_DURATION_BASE,
                  prefer_cached_join=False, cached_invalidate_threshold=2,
                  diag_state=None):
    """Full SKSTACK-IP join sequence. Returns IPv6 address of meter.

    spec 034: `prefer_known_channel=True` で単 ch mask scan を先に試行
    (= disable 推奨、 [[spec-034]] cache hit rate 0% で実証失敗)。

    spec 035: `prefer_cached_join=True` で cache 4 件 (= pan_channel/pan_id/
    mac/ipv6) 全揃いなら SKSCAN/SKLL64 完全 skip + SKSREG S2/S3 + SKJOIN
    cached_ipv6 直行 (= reconnect 35s → 7s、 5 倍短縮見込み)。 SKJOIN 失敗
    (= EVENT 24 / 30s timeout) で `cached_invalidate_threshold` (= default 2)
    回連続で cache 全 invalidate + fallback で SKSCAN 全 scan path に進む。

    `fallback_duration` = main loop から
    `config.get("wisun_reconnect_channel_mask_fallback_duration", 6)` で渡される。

    `diag_state` is forwarded to skscan and the PAN info is recorded onto it
    once a usable PAN is selected.
    """
    _wisun_init_sequence(fd, br_id, br_pwd, diag_state=diag_state)

    # spec 035: cached SKJOIN 直行 path (= SKSCAN/SKLL64 完全 skip)
    if prefer_cached_join and diag_state is not None:
        cached_ch = diag_state.pan_channel
        cached_pid = diag_state.pan_id
        cached_mac = diag_state.mac
        cached_ipv6 = diag_state.ipv6
        if all(x is not None for x in (cached_ch, cached_pid, cached_mac, cached_ipv6)):
            log("SKJOIN cached direct (ch={} pan_id={} mac={} ipv6={})".format(
                cached_ch, cached_pid, cached_mac, cached_ipv6))
            cached_succeeded = False
            try:
                ch_hex = "{:02X}".format(int(cached_ch))
                skcommand(fd, "SKSREG S2 {}".format(ch_hex))
                skcommand(fd, "SKSREG S3 {}".format(cached_pid))
                serial_write(fd, "SKJOIN {}\r\n".format(cached_ipv6))
                cached_pan = {"Channel": ch_hex, "Pan ID": cached_pid,
                              "Addr": cached_mac}
                if _wait_skjoin_event25(fd, cached_pan, cached_ipv6,
                                        timeout=30, diag_state=diag_state):
                    cached_succeeded = True
            except Exception as e:
                log("SKJOIN cached path raised: {} - falling back".format(e))

            if cached_succeeded:
                if diag_state is not None:
                    try:
                        diag_state.on_skjoin_success()
                        diag_state.on_wisun_reconnect_cached_skjoin()
                    except Exception as e:
                        log("diag on_skjoin_success error: {}".format(e))
                # spec 042: SKJOIN 成功後 SKADDNBR で初回 SKSENDTO 1-2s 短縮。
                _add_neighbor_cache(fd, cached_ipv6, cached_mac, diag_state)
                return cached_ipv6

            # cached path 失敗 → counter + invalidate + re-init for clean state
            if diag_state is not None:
                try:
                    diag_state.on_skjoin_failure(cached_invalidate_threshold)
                    diag_state.on_wisun_reconnect_cached_skjoin_fallback()
                except Exception as e:
                    log("diag on_skjoin_failure error: {}".format(e))
            log("SKJOIN cached failed, re-init + SKSCAN fallback")
            _wisun_init_sequence(fd, br_id, br_pwd, diag_state=diag_state)

    pan = None
    known_ch = getattr(diag_state, "pan_channel", None) if diag_state else None
    if prefer_known_channel and known_ch is not None:
        mask = "{:08X}".format(channel_to_mask(known_ch))
        log("SKSCAN single-ch try (ch={} mask={})".format(known_ch, mask))
        pan = skscan(fd, channel_mask=mask, duration=3, max_retries=1,
                     diag_state=diag_state)
        if pan.get("Channel"):
            if diag_state is not None:
                try:
                    diag_state.on_wisun_reconnect_short_scan()
                except Exception as e:
                    log("diag on_wisun_reconnect_short_scan error: {}".format(e))
        else:
            log("SKSCAN single-ch missed, falling back to full scan")
            if diag_state is not None:
                try:
                    diag_state.on_wisun_reconnect_fallback_full_scan()
                except Exception as e:
                    log("diag on_wisun_reconnect_fallback_full_scan error: {}".format(e))
            pan = None  # fall through to full scan below

    if not pan:
        log("SKSCAN full scan (waits for EVENT 22, ~20s per try)")
        pan = skscan(fd, channel_mask=FULL_SCAN_CHANNEL_MASK,
                     duration=fallback_duration, diag_state=diag_state)

    if not pan.get("Channel") or not pan.get("Pan ID") or not pan.get("Addr"):
        raise RuntimeError("SKSCAN: no PAN found ({})".format(pan))

    channel = pan["Channel"]
    pan_id  = pan["Pan ID"]
    mac     = pan["Addr"]
    log("PAN found: ch={} panId={} mac={}".format(channel, pan_id, mac))
    if diag_state is not None:
        try:
            diag_state.on_wisun_joined(pan)
        except Exception as e:
            log("diag on_wisun_joined error: {}".format(e))

    ipv6 = skll64(fd, mac)
    if not ipv6:
        raise RuntimeError("SKLL64 failed")
    log("Meter IPv6: {}".format(ipv6))
    if diag_state is not None:
        try:
            diag_state.on_skll64(ipv6)  # spec 035: cache ipv6 for next reconnect
        except Exception as e:
            log("diag on_skll64 error: {}".format(e))

    skcommand(fd, "SKSREG S2 {}".format(channel))
    skcommand(fd, "SKSREG S3 {}".format(pan_id))

    log("SKJOIN {}".format(ipv6))
    serial_write(fd, "SKJOIN {}\r\n".format(ipv6))

    if _wait_skjoin_event25(fd, pan, ipv6, timeout=90, diag_state=diag_state):
        if diag_state is not None:
            try:
                diag_state.on_skjoin_success()  # spec 035: reset failure counter
            except Exception as e:
                log("diag on_skjoin_success error: {}".format(e))
        # spec 042: SKJOIN 成功後 SKADDNBR で初回 SKSENDTO 1-2s 短縮。
        _add_neighbor_cache(fd, ipv6, mac, diag_state)
        return ipv6
    # EVENT 24 PANA fail or timeout
    raise RuntimeError("SKJOIN: PANA authentication failed or timeout")

# ---------------------------------------------------------------------------
# ECHONET Lite frame builder / parser
# ---------------------------------------------------------------------------

EPCS = [0xD3, 0xE1, 0xE7, 0xE0, 0xE3, 0xE8]

# Spec 011 C: EPC tier rotation. Cycle-by-cycle EPC selection keeps each
# SKSENDTO payload small (= faster meter response) and lets HA receive
# what changes the most (power) at full rate while less-volatile data is
# refreshed at a lower cadence.
TIER1_EPCS = [0xE7, 0xE8]         # 瞬時電力、 瞬時電流 — real-time
# spec 049: noisy cycle 用の最小 frame (= OPC=1)。 noise 中の airtime を
# 最小化しつつ user 最優先軸 (= 瞬時電力) だけ回収する。
NOISE_MINIMAL_EPCS = [0xE7]
TIER2_EPCS = [0xE0, 0xE3]         # 積算電力量 (forward / reverse) — slow
TIER3_EPCS = [0xD3, 0xE1]         # 係数 / 単位 — near-static
TIER4_EPCS = [0xEA, 0xEB]         # spec 018: 定時積算電力量 fwd/rev — meter ts


def decide_epc_tier(cycle_number, tier2_every=5, tier3_every=60,
                    tier4_every=30):
    """Pick which EPC tier to query for cycle *cycle_number*.

    spec 018: tier4 (定時積算電力量) wins over tier3 wins over tier2 when
    intervals align — losing tier4 means missing a 30-min cumulative
    energy boundary, while tier3 (coefficient/unit) is near-static and
    can wait. tier4_every <= 0 disables tier4 entirely (kill switch).
    """
    if tier4_every > 0 and cycle_number % int(tier4_every) == 0:
        return "tier4"
    if cycle_number % int(tier3_every) == 0:
        return "tier3"
    if cycle_number % int(tier2_every) == 0:
        return "tier2"
    return "tier1"


def epcs_for_tier(tier):
    if tier == "tier2":
        return TIER2_EPCS
    if tier == "tier3":
        return TIER3_EPCS
    if tier == "tier4":
        return TIER4_EPCS
    return TIER1_EPCS


def cycle_epcs_with_tier1(tier):
    """spec 033 (= spec 011 E): 全 cycle で tier1 EPCs を含めて OPC batch.

    tier2/tier3/tier4 のみ tier1 EPCs を合成 (= OPC=4 batch)、 それ以外
    (= tier1 / 未知 tier fallback) は TIER1_EPCS のみ返す = 重複 EPC 送信
    回避 (= ECHONET Lite 重複 EPC 仕様グレー、 BP35CX/メーター動作未検証)。
    mismatch 発火時 100% spec 028 backfill 対象、 [[feedback-cycle-counter-reconnect-tier4]]
    構造的問題も解決。
    """
    if tier in ("tier2", "tier3", "tier4"):
        return list(TIER1_EPCS) + list(epcs_for_tier(tier))
    return list(TIER1_EPCS)


def should_republish_discovery(now, last_publish_ts, pending, interval_sec):
    """spec 016: True iff a reconnect is pending, the interval has elapsed,
    or no publish has ever been recorded.

    interval_sec <= 0 disables periodic republish (reconnect / first
    publish still trigger). last_publish_ts=None is treated as "never
    published" → True so callers without an initialised ts get a publish
    on the first tick (defensive; main loop seeds the ts at startup so
    this path normally does not fire).
    """
    if pending:
        return True
    if last_publish_ts is None:
        return True
    if interval_sec <= 0:
        return False
    return (now - last_publish_ts) >= interval_sec


def compute_rejoin_backoff(attempt, initial_sec, multiplier, max_sec):
    """spec 017: exponential backoff for wisun_connect retries.

    attempt 0 → initial, 1 → initial*mult, ..., clamped at max_sec.
    Negative attempt is treated as 0. multiplier <= 1 → linear at initial.
    initial >= max → always max.
    """
    a = max(0, int(attempt))
    backoff = float(initial_sec) * (float(multiplier) ** a)
    return int(min(backoff, float(max_sec)))


def build_el_get(tid, epcs):
    frame = bytearray()
    frame += b"\x10\x81"                     # EHD1, EHD2
    frame += struct.pack(">H", tid & 0xFFFF) # TID
    frame += b"\x05\xFF\x01"                 # SEOJ: controller
    frame += b"\x02\x88\x01"                 # DEOJ: smart meter
    frame += b"\x62"                         # ESV: Get
    frame += struct.pack("B", len(epcs))     # OPC
    for epc in epcs:
        frame += struct.pack("BB", epc, 0)   # EPC, PDC=0
    return bytes(frame)


def extract_el_tid(payload):
    """Return the big-endian TID from an ECHONET Lite payload, or None.

    spec 014: used to reject ERXUDP frames whose TID doesn't match the one
    we just sent. Payload bytes 2-3 are the TID (after EHD1/EHD2 at 0-1).
    Returns None for payloads shorter than 4 bytes; the caller treats that
    as a mismatch and discards the frame.
    """
    if len(payload) < 4:
        return None
    return struct.unpack(">H", bytes(payload[2:4]))[0]


def compute_tid_lag(expected, got, modulo=0x10000):
    """How many ECHONET Lite request frames the late reply is behind.

    spec 011 follow-up 2: when a delayed reply arrives, `(expected - got) %
    modulo` is the number of request cycles it lagged by — i.e. how deep
    the meter's ECHONET queue is. Returns None when either side is None
    (the read_erxudp caller did not have a real TID yet)."""
    if expected is None or got is None:
        return None
    return (int(expected) - int(got)) % int(modulo)

def decide_noise_cycle_action(noisy, streak, max_consecutive,
                              minimal_enabled):
    """spec 049 FR-005: noisy 判定 cycle の扱いを決める pure helper.

    Returns "normal" / "skip" / "minimal":
      normal  = 静穏 or streak 上限到達 (= spec 012 fail-safe、 通常 poll)
      skip    = noisy + minimal 無効 (= spec 012 の完全 skip、 kill switch)
      minimal = noisy + minimal 有効 (= 0xE7 単独 OPC=1 の reduced poll)
    """
    if not noisy or streak >= max_consecutive:
        return "normal"
    if minimal_enabled:
        return "minimal"
    return "skip"


def classify_rescued_esv(payload):
    """spec 047: rescued frame の ESV 分類 (pure helper).

    read_erxudp の TID mismatch 救済 path で、救済 frame の中身が
    Get 応答 (0x72/0x52) か自律通知 (0x73 INF) かを counter で分解する
    ための分類。 offset 10 = ESV は parse_el_response と同一。
    len < 11 (= ESV 位置に届かない短 frame) は "other"。
    """
    if len(payload) < 11:
        return "other"
    esv = payload[10] if isinstance(payload[10], int) else ord(payload[10])
    if esv == 0x72:
        return "get_res"
    if esv == 0x52:
        return "get_sna"
    if esv == 0x73:
        return "inf"
    return "other"


def classify_rescued_lag_bucket(lag_sec):
    """spec 047: rescued frame の遅延秒数を 4 bucket に分類 (pure helper).

    境界の意味 (spec 047 FR-003):
      lt5s     = 実質 live (H1: TID=0 quirk の即時応答疑い)
      5to60s   = 8s timeout 直後の真の遅延応答
      60to300s = 1 cycle 遅れ chain (H3) / メーター queue 深滞留
      gt300s   = stale (reconnect 跨ぎ等)
    """
    if lag_sec < 5:
        return "lt5s"
    if lag_sec < 60:
        return "5to60s"
    if lag_sec < 300:
        return "60to300s"
    return "gt300s"


def parse_el_response(data):
    """Returns dict {epc_int: bytearray}."""
    if len(data) < 12:
        return {}
    esv = data[10] if isinstance(data[10], int) else ord(data[10])
    opc = data[11] if isinstance(data[11], int) else ord(data[11])
    # Accept Get_Res (0x72) or Get_SNA (0x52)
    if esv not in (0x72, 0x52):
        return {}
    result = {}
    pos = 12
    for _ in range(opc):
        if pos + 2 > len(data):
            break
        epc = data[pos] if isinstance(data[pos], int) else ord(data[pos])
        pdc = data[pos+1] if isinstance(data[pos+1], int) else ord(data[pos+1])
        pos += 2
        if pos + pdc > len(data):
            break
        result[epc] = bytearray(data[pos:pos+pdc])
        pos += pdc
    return result

def decode_cumulative_energy_fixed(epc_bytes):
    """spec 018: parse ECHONET Lite EPC 0xEA / 0xEB (定時積算電力量).

    11-byte response:
      bytes 0-1: year (big-endian uint16)
      byte  2:   month
      byte  3:   day
      byte  4:   hour
      byte  5:   minute
      byte  6:   second
      bytes 7-10: raw cumulative value (uint32; × coefficient × unit → kWh)

    Returns (timestamp_iso, raw_value) or None for short / invalid input.
    Meter clock confirmed as JST on a real 2026-06-22 probe, so ISO8601
    output carries `+09:00`. year < 2000 is treated as "meter clock not
    set" → returns None so the caller skips publishing.
    """
    if len(epc_bytes) < 11:
        return None
    year = struct.unpack(">H", bytes(epc_bytes[0:2]))[0]
    if year < 2000:
        return None
    month, day, hour, minute, second = (
        int(epc_bytes[2]), int(epc_bytes[3]), int(epc_bytes[4]),
        int(epc_bytes[5]), int(epc_bytes[6]))
    raw = struct.unpack(">I", bytes(epc_bytes[7:11]))[0]
    ts = "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}+09:00".format(
        year, month, day, hour, minute, second)
    return (ts, raw)


def decode_measurements(props):
    result = {}

    # D3: coefficient (4-byte unsigned)
    if 0xD3 in props and len(props[0xD3]) >= 4:
        result["coefficient"] = struct.unpack(">I", bytes(props[0xD3][:4]))[0]

    # E1: unit exponent byte
    if 0xE1 in props and len(props[0xE1]) >= 1:
        unit_byte = props[0xE1][0]
        unit_map = {0x00: 1.0, 0x01: 0.1,  0x02: 0.01,   0x03: 0.001, 0x04: 0.0001,
                    0x0A: 10.0, 0x0B: 100.0, 0x0C: 1000.0, 0x0D: 10000.0}
        result["unit_kwh"] = unit_map.get(unit_byte, 1.0)

    # E7: instantaneous power W (4-byte signed)
    if 0xE7 in props and len(props[0xE7]) >= 4:
        result["power_w"] = struct.unpack(">i", bytes(props[0xE7][:4]))[0]

    # E0: cumulative forward kWh (4-byte unsigned × coeff × unit)
    if 0xE0 in props and len(props[0xE0]) >= 4:
        result["energy_forward_raw"] = struct.unpack(">I", bytes(props[0xE0][:4]))[0]

    # E3: cumulative reverse kWh (4-byte unsigned × coeff × unit)
    if 0xE3 in props and len(props[0xE3]) >= 4:
        result["energy_reverse_raw"] = struct.unpack(">I", bytes(props[0xE3][:4]))[0]

    # E8: instantaneous current R,T phase (2×signed short, 0.1A)
    #
    # ECHONET Lite low-voltage smart meter class defines:
    #   - valid range 0x8001..0x7FFD (-3276.7 .. +3276.5 A)
    #   - 0x7FFE = "not measured" / phase not connected
    #   - 0x7FFF = reserved
    # Drop both sentinels so a 単相2線式 contract or unwired T phase shows up
    # as "unknown" in HA rather than the literal 3276.6 A.
    if 0xE8 in props and len(props[0xE8]) >= 4:
        r_raw, t_raw = struct.unpack(">HH", bytes(props[0xE8][:4]))
        if r_raw not in (0x7FFE, 0x7FFF):
            r_signed = struct.unpack(">h", struct.pack(">H", r_raw))[0]
            result["current_r_a"] = r_signed / 10.0
        if t_raw not in (0x7FFE, 0x7FFF):
            t_signed = struct.unpack(">h", struct.pack(">H", t_raw))[0]
            result["current_t_a"] = t_signed / 10.0

    # spec 018: EA / EB cumulative energy at 30-min boundary with meter
    # timestamp. decode_cumulative_energy_fixed returns None for invalid
    # input (short payload or meter clock not set) — skip silently.
    if 0xEA in props:
        ea = decode_cumulative_energy_fixed(props[0xEA])
        if ea is not None:
            result["energy_forward_fixed_ts"] = ea[0]
            result["energy_forward_fixed_raw"] = ea[1]
    if 0xEB in props:
        eb = decode_cumulative_energy_fixed(props[0xEB])
        if eb is not None:
            result["energy_reverse_fixed_ts"] = eb[0]
            result["energy_reverse_fixed_raw"] = eb[1]

    return result

def apply_energy_scale(measurements, coeff, unit_kwh):
    c = measurements.get("coefficient", coeff)
    u = measurements.get("unit_kwh", unit_kwh)
    if "energy_forward_raw" in measurements:
        measurements["energy_forward_kwh"] = measurements["energy_forward_raw"] * c * u
    if "energy_reverse_raw" in measurements:
        measurements["energy_reverse_kwh"] = measurements["energy_reverse_raw"] * c * u
    # spec 018: same scale for the 30-min boundary cumulative values.
    if "energy_forward_fixed_raw" in measurements:
        measurements["energy_forward_fixed_kwh"] = (
            measurements["energy_forward_fixed_raw"] * c * u)
    if "energy_reverse_fixed_raw" in measurements:
        measurements["energy_reverse_fixed_kwh"] = (
            measurements["energy_reverse_fixed_raw"] * c * u)
    return measurements

# ---------------------------------------------------------------------------
# Send ECHONET Lite Get via SKSENDTO
# ---------------------------------------------------------------------------

def send_el_get(fd, ipv6, tid, epc_list=None):
    """Send an ECHONET Get request to *ipv6*. Defaults to the full
    measurement EPC set; pass `epc_list=[0x80]` for a lightweight probe
    (spec 009)."""
    epcs = EPCS if epc_list is None else list(epc_list)
    frame = build_el_get(tid, epcs)
    # SKSENDTO expects 4-hex-digit payload length and trailing CRLF after raw data.
    cmd = "SKSENDTO 1 {} 0E1A 1 0 {:04X} ".format(ipv6, len(frame))
    serial_write(fd, cmd)
    serial_write(fd, frame)
    serial_write(fd, b"\r\n")


def _run_eedscan_sweep(fd, eedscan_state, diag_state):
    """Fire one EEDSCAN (SKSCAN mode 0) sweep and parse the EEDSCAN line.

    `SKSCAN 0 0FFFFFFF 4 0` covers BP35CX channels 33-60 (ch33 = bit 0 of
    `0FFFFFFF`). Duration code 4 → ~12 s end-to-end. Records the result
    onto *eedscan_state* on success."""
    serial_write(fd, "SKSCAN 0 0FFFFFFF 4 0\r\n")
    deadline = time.time() + 30.0
    saw_ok = False
    payload = None
    while time.time() < deadline:
        line = serial_readline(fd, timeout=2)
        if line is None:
            continue
        # EVENT/FAIL go to the existing health counters where applicable.
        kind = classify_sk_line(line)
        if kind is not None and diag_state is not None:
            try:
                if kind[0] == "event":
                    diag_state.on_sk_event(kind[1])
                elif kind[0] == "error":
                    diag_state.on_sk_error(kind[1])
            except Exception:
                pass
        if line.startswith("OK"):
            saw_ok = True
            continue
        if line.startswith("FAIL"):
            log("EEDSCAN rejected: {}".format(line))
            return
        if line.startswith("EEDSCAN"):
            # The data line follows; next readline holds the pairs.
            next_line = serial_readline(fd, timeout=2)
            payload = (next_line or "").strip()
            break
    if not saw_ok or payload is None:
        log("EEDSCAN: no data line received")
        return
    result = parse_eedscan(payload)
    if not result:
        log("EEDSCAN: parse returned empty for {!r}".format(payload))
        return
    eedscan_state.record(result, time.time())
    log("EEDSCAN OK: {} channels, max={:02X} min={:02X}".format(
        len(result), max(result.values()), min(result.values())))


# Lightweight EPC set used during probe mode. 0x80 (operation status) is
# a static 1-byte property the meter returns from internal flags, so RTT
# is closer to the pure Wi-SUN round-trip than 0xE7 (instant power, which
# requires the meter to take a fresh measurement).
PROBE_EPCS = [0x80]

def read_erxudp(fd, timeout=15, diag_state=None, expected_tid=None,
                readline=None, send_history=None):
    """Wait for ERXUDP and return payload as bytearray, or None.

    When *diag_state* is supplied, EVENT/FAIL lines observed before the
    ERXUDP are dispatched into Wi-SUN health counters (spec 006).

    spec 014: when *expected_tid* is given, frames whose ECHONET Lite TID
    does not match are discarded (the bridge keeps waiting). This prevents
    a delayed reply from the previous cycle (or a stale frame after a
    Wi-SUN re-join) being mistaken for the response to the current
    request. *readline* is the line-reader callable, defaulting to the
    module-level `serial_readline`; tests inject a fake.

    spec 020: when *send_history* is supplied, a TID mismatch frame whose
    TID was previously recorded in the ring buffer is RESCUED instead of
    discarded.

    spec 048: rescue は即 return せず (payload, send_ts) を
    diag_state.pending_rescued_frames に stash して expected TID を deadline
    まで待ち続ける (= chain 断ち)。 main loop が毎 cycle drain して過去時刻
    backfill publish する。 非 Get 応答 (= ESV 0x72/0x52 以外、 INF 等) は
    TID check より前段で erxudp_inf_ignored_total に計上して読み飛ばす。
    got_tid=0 の lookup_latest fallback (spec 020 v1.5) は撤去済 (= 正体が
    100% INF と spec 047 で判明)。 Ring lookup miss は spec 014 discard path。
    """
    if readline is None:
        readline = serial_readline
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = readline(fd, timeout=max(0.5, deadline - time.time()))
        if line is None:
            continue
        classified = classify_sk_line(line)
        if classified is None:
            continue
        kind, value = classified
        if kind == "event":
            if diag_state is not None:
                try:
                    # spec 017: EVENT 24 (PANA authentication failure) and
                    # EVENT 29 (PANA expired) are routed to on_wisun_pana_fail
                    # which sets pending_wisun_rejoin so the main loop can
                    # trigger a rejoin without waiting for ERXUDP timeouts.
                    # on_wisun_pana_fail itself also bumps the SK event
                    # counter (it delegates to on_sk_event internally).
                    if value in ("24", "29"):
                        diag_state.on_wisun_pana_fail(value)
                    else:
                        diag_state.on_sk_event(value)
                    # spec 038 Phase 2: EVENT 21 (= TX Result Notification) は
                    # 公式 p.51 で "EVENT 21 <SENDER_IPV6> <PARAM>" 形式、
                    # PARAM は 0 成功 / 1 失敗 / 2 自動再送。 line を re-parse
                    # して PARAM 別 counter を inc (= classify_sk_line は id のみ
                    # 返す既存仕様を維持しつつ追加観測)。
                    if value == "21":
                        parts = line.split()
                        if len(parts) >= 4:
                            try:
                                diag_state.on_sk_event_21_param(int(parts[3]))
                            except (ValueError, IndexError):
                                pass
                    # spec 040 Phase 2a: ERXUDP wait 中の EVENT 25 (= SKREJOIN
                    # 成功通知 or PAA 側からの再認証完了通知) も last_event_25_ts
                    # を更新、 main loop の能動 SKREJOIN tick base time として使用。
                    if value == "25":
                        diag_state.last_event_25_ts = time.time()
                except Exception as e:
                    log("diag on_sk_event error: {}".format(e))
            continue
        if kind == "error":
            if diag_state is not None:
                try:
                    diag_state.on_sk_error(value)
                except Exception as e:
                    log("diag on_sk_error error: {}".format(e))
            continue
        if kind == "erxudp":
            if diag_state is not None:
                try:
                    diag_state.on_erxudp_raw(line)
                except Exception as e:
                    log("diag on_erxudp_raw error: {}".format(e))
            if not value.startswith("1081"):
                continue
            try:
                payload = bytearray(binascii.unhexlify(value))
            except Exception as e:
                log("ERXUDP hex decode error: {}".format(e))
                continue
            # spec 048 FR-001: ESV filter を TID check より前段に移動。
            # INF (= 0x73、 spec 047 判定で rescued の 59% を占め全件データ
            # ゼロ) 等の非 Get 応答は rescue 対象外 — counter だけ inc して
            # deadline まで expected TID を待ち続ける (= cycle を潰さない)。
            if len(payload) >= 11:
                _esv = (payload[10] if isinstance(payload[10], int)
                        else ord(payload[10]))
                if _esv not in (0x72, 0x52):
                    if diag_state is not None:
                        try:
                            diag_state.on_erxudp_inf_ignored()
                        except Exception as e:
                            log("diag on_erxudp_inf_ignored error: {}"
                                .format(e))
                    continue
            if expected_tid is not None:
                got_tid = extract_el_tid(payload)
                if got_tid != expected_tid:
                    if diag_state is not None:
                        try:
                            diag_state.on_erxudp_tid_mismatch(
                                expected=expected_tid, got=got_tid)
                        except Exception as e:
                            log("diag on_erxudp_tid_mismatch error: {}"
                                .format(e))
                    # spec 020: ring lookup で late frame 救済を試行。
                    # spec 048 FR-002/003: got_tid=0 の lookup_latest fallback
                    # は撤去 (= 正体が 100% INF と spec 047 で判明、 INF は
                    # 上の ESV filter で排除済)。 hit した late frame は
                    # pending_rescued_frames に stash して読み続け、 自 cycle
                    # の応答 (expected TID) を deadline まで待つ = chain 断ち。
                    # main loop が毎 cycle drain して過去時刻 backfill publish。
                    if (send_history is not None and got_tid is not None
                            and diag_state is not None):
                        hit = send_history.lookup(got_tid)
                        if hit is not None:
                            send_ts_a, _epcs = hit
                            try:
                                diag_state.on_erxudp_recovered_from_mismatch(
                                    time.time() - send_ts_a,
                                    send_ts=send_ts_a)
                                log("INFO: ERXUDP TID mismatch got={:04X} "
                                    "stashed as late frame (send_ts ago "
                                    "{:.1f}s), keep waiting".format(
                                        got_tid, time.time() - send_ts_a))
                            except Exception as e:
                                log("diag on_erxudp_recovered_from_mismatch "
                                    "error: {}".format(e))
                            # spec 047: rescued frame の ESV / TID / lag 内訳観測。
                            try:
                                diag_state.on_erxudp_rescued(
                                    classify_rescued_esv(payload),
                                    got_tid == 0,
                                    time.time() - send_ts_a)
                            except Exception as e:
                                log("diag on_erxudp_rescued error: {}"
                                    .format(e))
                            diag_state.pending_rescued_frames.append(
                                (payload, send_ts_a))
                            continue
                    if got_tid is None:
                        log("WARN: ERXUDP payload too short ({} bytes), "
                            "discarding".format(len(payload)))
                    else:
                        log("WARN: ERXUDP TID mismatch expected={:04X} "
                            "got={:04X}, discarding".format(
                                expected_tid, got_tid))
                    continue
            return payload
    return None

# ---------------------------------------------------------------------------
# Minimal MQTT 3.1.1 client (raw socket, no paho)
# ---------------------------------------------------------------------------

def _encode_remaining(n):
    buf = b""
    while True:
        byte = n % 128
        n //= 128
        if n > 0:
            byte |= 0x80
        buf += struct.pack("B", byte)
        if n == 0:
            break
    return buf

def _encode_str(s):
    b = s.encode("utf-8")
    return struct.pack(">H", len(b)) + b

try:
    import Queue as _queue_mod  # Py2
except ImportError:
    import queue as _queue_mod  # Py3


class MQTTClient(object):
    def __init__(self, host, port, client_id, username=None, password=None,
                 on_reconnect=None, keepalive=60,
                 threading_enabled=False, send_queue_maxsize=1000):
        self.host              = host
        self.port              = port
        self.client_id         = client_id
        self.username          = username
        self.password          = password
        self.keepalive         = int(keepalive)
        self.threading_enabled = bool(threading_enabled)
        self.sock              = None
        self._out_queue        = collections.deque()  # legacy fallback only
        self.on_reconnect      = on_reconnect  # called after a successful re-connect

        # Threaded-mode primitives (spec 005). Allocated even in legacy mode
        # so that diagnostic snapshots (publish_dropped_total など) never
        # raise AttributeError.
        self.send_queue              = _queue_mod.Queue(maxsize=int(send_queue_maxsize))
        self.publish_dropped_total   = 0
        self.ping_failures_total     = 0
        self._send_lock              = threading.Lock()
        self._stop_event             = threading.Event()
        self._reconnect_event        = threading.Event()
        self._sender_thread          = None
        self._keepalive_thread       = None

    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30)
        # Enable TCP keepalive where available
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # platform-specific options
            for opt_name, opt_val in (('TCP_KEEPIDLE', 60), ('TCP_KEEPINTVL', 10), ('TCP_KEEPCNT', 3)):
                if hasattr(socket, opt_name):
                    try:
                        s.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt_name), opt_val)
                    except Exception:
                        pass
        except Exception:
            pass

        s.connect((self.host, self.port))

        flags = 0x02  # clean session
        if self.username: flags |= 0x80
        if self.password: flags |= 0x40

        var_hdr = (b"\x00\x04MQTT"
                   + b"\x04"
                   + struct.pack("B", flags)
                   + struct.pack(">H", self.keepalive))

        payload = _encode_str(self.client_id)
        if self.username: payload += _encode_str(self.username)
        if self.password: payload += _encode_str(self.password)

        remaining = var_hdr + payload
        pkt = b"\x10" + _encode_remaining(len(remaining)) + remaining
        s.sendall(pkt)

        # read CONNACK
        s.settimeout(10)
        ack = b""
        while len(ack) < 4:
            chunk = s.recv(4 - len(ack))
            if not chunk:
                break
            ack += chunk
        s.settimeout(None)

        if len(ack) < 4 or (ack[0] if isinstance(ack[0], int) else ord(ack[0])) != 0x20:
            raise RuntimeError("MQTT: bad CONNACK ({})".format(binascii.hexlify(ack)))
        rc = ack[3] if isinstance(ack[3], int) else ord(ack[3])
        if rc != 0:
            raise RuntimeError("MQTT: connection refused code {}".format(rc))

        self.sock = s
        if LOGGER is not None:
            emit_mqtt_connected(LOGGER, host=self.host, port=self.port)
        else:
            log("MQTT connected to {}:{}".format(self.host, self.port))

        if self.threading_enabled:
            self._reconnect_event.clear()
            self._start_workers_if_needed()
        else:
            # legacy: flush in-memory deque
            try:
                self._flush_queue()
            except Exception as e:
                log("MQTT flush queue error: {}".format(e))

    def _make_pkt(self, topic, payload, retain=False):
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":"))
        topic_b = topic.encode("utf-8")
        payload_b = payload.encode("utf-8") if isinstance(payload, str) else payload
        fixed = 0x30 | (0x01 if retain else 0x00)
        var_hdr = struct.pack(">H", len(topic_b)) + topic_b
        remaining = var_hdr + payload_b
        return struct.pack("B", fixed) + _encode_remaining(len(remaining)) + remaining

    def publish(self, topic, payload, retain=False):
        if self.threading_enabled:
            self._enqueue(topic, payload, retain)
            return
        # ---- legacy synchronous path (fallback when threading is disabled) ----
        pkt = self._make_pkt(topic, payload, retain)
        try:
            if not self.sock:
                raise RuntimeError("No MQTT socket")
            self.sock.sendall(pkt)
            return
        except Exception as e:
            log("MQTT publish error: {}".format(e))
            # try reconnect and resend
            try:
                self._reconnect()
            except Exception as e2:
                log("MQTT reconnect failed after publish error: {}".format(e2))
                # queue the message for later delivery
                try:
                    self._out_queue.append((topic, payload, retain))
                except Exception:
                    pass
                return

            try:
                self.sock.sendall(pkt)
                return
            except Exception as e3:
                log("MQTT publish retry failed: {}".format(e3))
                try:
                    self._out_queue.append((topic, payload, retain))
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Threaded mode helpers (spec 005-mqtt-threading)
    # ------------------------------------------------------------------

    def _enqueue(self, topic, payload, retain):
        """Non-blocking enqueue. On saturation drops the OLDEST entry (FIFO)
        and bumps publish_dropped_total so the most-recent value survives —
        important for HA dashboards that display "now"."""
        item = (topic, payload, retain)
        try:
            self.send_queue.put_nowait(item)
        except _queue_mod.Full:
            try:
                self.send_queue.get_nowait()
                self.publish_dropped_total += 1
            except _queue_mod.Empty:
                pass
            try:
                self.send_queue.put_nowait(item)
            except _queue_mod.Full:
                # Worker is faster than the drop loop expected; bump anyway.
                self.publish_dropped_total += 1

    def _start_workers_if_needed(self):
        if self._sender_thread is None or not self._sender_thread.is_alive():
            self._sender_thread = threading.Thread(
                target=self._sender_loop, name="mqtt-sender")
            self._sender_thread.daemon = True
            self._sender_thread.start()
        if self._keepalive_thread is None or not self._keepalive_thread.is_alive():
            self._keepalive_thread = threading.Thread(
                target=self._keepalive_loop, name="mqtt-keepalive")
            self._keepalive_thread.daemon = True
            self._keepalive_thread.start()

    def _sender_loop(self):
        """Pull from send_queue, send under _send_lock. On send failure or
        on a reconnect_event from the keepalive worker, perform an in-thread
        reconnect (also under lock so PINGREQ can't race)."""
        while not self._stop_event.is_set():
            if self._reconnect_event.is_set():
                self._reconnect_event.clear()
                try:
                    self._reconnect_under_lock()
                except Exception as e:
                    log("MQTT sender reconnect failed: {}".format(e))
            try:
                item = self.send_queue.get(timeout=1.0)
            except _queue_mod.Empty:
                continue
            if item is None:
                # stop sentinel
                self.send_queue.task_done()
                break
            topic, payload, retain = item
            pkt = self._make_pkt(topic, payload, retain)
            ok = self._send_under_lock(pkt)
            self.send_queue.task_done()
            if not ok:
                # Reconnect synchronously inside this thread; then retry once.
                try:
                    self._reconnect_under_lock()
                    self._send_under_lock(pkt)  # best-effort retry, no further loop
                except Exception as e:
                    log("MQTT sender post-reconnect retry failed: {}".format(e))

    def _keepalive_loop(self):
        """Send PINGREQ every keepalive/2 seconds. On send failure signal the
        sender thread to reconnect; do not attempt the reconnect here to keep
        the connect serialization simple."""
        interval = max(1.0, self.keepalive / 2.0)
        while not self._stop_event.wait(interval):
            ok = self._send_under_lock(b"\xC0\x00")
            if not ok:
                self.ping_failures_total += 1
                self._reconnect_event.set()

    def _send_under_lock(self, pkt):
        with self._send_lock:
            sock = self.sock
            if sock is None:
                return False
            try:
                sock.sendall(pkt)
                return True
            except Exception as e:
                log("MQTT socket send failed: {}".format(e))
                return False

    def _reconnect_under_lock(self):
        with self._send_lock:
            self._reconnect_socket_unlocked()

    def _reconnect_socket_unlocked(self):
        if LOGGER is not None:
            emit_mqtt_reconnect(LOGGER)
        else:
            log("MQTT reconnecting …")
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None
        # Re-establish; connect() itself does not take _send_lock so it is
        # safe to call from here.  Keep retrying until success or shutdown.
        while not self._stop_event.is_set():
            try:
                self._do_connect_socket_only()
                if self.on_reconnect is not None:
                    try:
                        self.on_reconnect()
                    except Exception as e:
                        log("on_reconnect callback error: {}".format(e))
                return
            except Exception as e:
                log("MQTT reconnect failed: {} - retry in 15s".format(e))
                # Use stop_event so shutdown wakes us promptly.
                if self._stop_event.wait(15):
                    return

    def _do_connect_socket_only(self):
        """Re-execute the TCP+CONNECT handshake without starting new workers
        (we are *inside* the sender worker)."""
        prev_flag = self.threading_enabled
        self.threading_enabled = False
        try:
            self.connect()
        finally:
            self.threading_enabled = prev_flag

    def shutdown(self, timeout=5.0):
        self._stop_event.set()
        try:
            # nudge sender out of the blocking get()
            self.send_queue.put_nowait(None)
        except Exception:
            pass
        for t in (self._sender_thread, self._keepalive_thread):
            if t is not None and t.is_alive():
                try:
                    t.join(timeout)
                except Exception:
                    pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None

    def _flush_queue(self):
        while self._out_queue and self.sock:
            topic, payload, retain = self._out_queue[0]
            try:
                pkt = self._make_pkt(topic, payload, retain)
                self.sock.sendall(pkt)
                self._out_queue.popleft()
            except Exception as e:
                log("MQTT queued publish failed: {}".format(e))
                break

    def ping(self):
        if self.threading_enabled:
            # The keepalive worker owns PINGREQ in threaded mode.
            return
        try:
            self.sock.sendall(b"\xC0\x00")
        except Exception as e:
            log("MQTT ping error: {}".format(e))
            self._reconnect()
            return
        # wait for PINGRESP (should be 0xD0 0x00)
        try:
            r, _, _ = select.select([self.sock], [], [], 5)
            if r:
                resp = self.sock.recv(2)
                if not resp:
                    log("MQTT ping: no response (empty)")
                    self._reconnect()
                elif len(resp) < 2:
                    log("MQTT ping: incomplete response (len={})".format(len(resp)))
                    self._reconnect()
                else:
                    first_byte = resp[0] if isinstance(resp[0], int) else ord(resp[0])
                    if first_byte != 0xD0:
                        log("MQTT ping: unexpected response first_byte=0x{:02X}".format(first_byte))
                        self._reconnect()
            else:
                log("MQTT ping: timeout (no data within 5s)")
                self._reconnect()
        except Exception as e:
            log("MQTT ping recv error: {}".format(e))
            self._reconnect()

    def _reconnect(self):
        if LOGGER is not None:
            emit_mqtt_reconnect(LOGGER)
        else:
            log("MQTT reconnecting …")
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None
        while True:
            try:
                self.connect()
                if self.on_reconnect is not None:
                    try:
                        self.on_reconnect()
                    except Exception as e:
                        log("on_reconnect callback error: {}".format(e))
                return
            except Exception as e:
                log("MQTT reconnect failed: {} - retry in 15s".format(e))
                time.sleep(15)

# ---------------------------------------------------------------------------
# Home Assistant MQTT auto-discovery
# ---------------------------------------------------------------------------

SENSOR_DEFS = [
    ("power",          "Instantaneous Power",  "W",   "power",   "measurement"),
    ("energy_forward", "Cumulative Energy Fwd", "kWh", "energy",  "total_increasing"),
    ("energy_reverse", "Cumulative Energy Rev", "kWh", "energy",  "total_increasing"),
    ("current_r",      "Current R Phase",       "A",   "current", "measurement"),
    ("current_t",      "Current T Phase",       "A",   "current", "measurement"),
    # spec 018: 30-min boundary cumulative energy with meter-side timestamp.
    # Suited for HA Energy dashboard which expects 30-min aligned values.
    ("energy_forward_fixed", "Cumulative Energy Fwd (30min)", "kWh", "energy", "total_increasing"),
    ("energy_reverse_fixed", "Cumulative Energy Rev (30min)", "kWh", "energy", "total_increasing"),
]

def _device_dict(device_id):
    return {
        "identifiers": [device_id],
        "name":         "Cube J1 Smart Meter",
        "model":        "Cube J1",
        "manufacturer": "NextDrive",
    }


# (key, name, unit, device_class, state_class, entity_category)
# Mirrors spec data-model.md.
DIAG_SENSOR_DEFS = [
    ("last_poll_success_ts",   "Last Poll Success",   None, "timestamp", None,               "diagnostic"),
    ("last_poll_failure_ts",   "Last Poll Failure",   None, "timestamp", None,               "diagnostic"),
    ("lqi",                    "LQI",                 None, None,        "measurement",      "diagnostic"),
    ("pan_channel",            "PAN Channel",         None, None,        "measurement",      "diagnostic"),
    ("scan_retries_total",     "Scan Retries",        None, None,        "total_increasing", "diagnostic"),
    ("wisun_reconnects_total", "Wi-SUN Reconnects",   None, None,        "total_increasing", "diagnostic"),
    ("mqtt_reconnects_total",  "MQTT Reconnects",     None, None,        "total_increasing", "diagnostic"),
    ("erxudp_timeouts_total",  "ERXUDP Timeouts",     None, None,        "total_increasing", "diagnostic"),
    ("erxudp_intra_cycle_retries_total", "ERXUDP Intra-Cycle Retries", None, None, "total_increasing", "diagnostic"),
    ("erxudp_recovered_by_retry_total",  "ERXUDP Recovered by Retry",  None, None, "total_increasing", "diagnostic"),
    ("erxudp_tid_mismatch_total",        "ERXUDP TID Mismatch",        None, None, "total_increasing", "diagnostic"),
    ("erxudp_tid_mismatch_lag_p50",      "TID Mismatch Lag p50",       None, None, "measurement",      "diagnostic"),
    ("erxudp_tid_mismatch_lag_p95",      "TID Mismatch Lag p95",       None, None, "measurement",      "diagnostic"),
    ("erxudp_tid_mismatch_lag_max",      "TID Mismatch Lag Max",       None, None, "measurement",      "diagnostic"),
    # Spec 020: TID mismatch late publish recovery (= 過去 send TID の遅延応答を救済).
    ("erxudp_recovered_from_mismatch_total", "ERXUDP Recovered from Mismatch", None, None, "total_increasing", "diagnostic"),
    ("erxudp_recovered_lag_p50",         "Recovered Lag p50",          "s",  None, "measurement",      "diagnostic"),
    ("erxudp_recovered_lag_p95",         "Recovered Lag p95",          "s",  None, "measurement",      "diagnostic"),
    ("erxudp_recovered_lag_max",         "Recovered Lag Max",          "s",  None, "measurement",      "diagnostic"),
    # Spec 028: 瞬時電力 0xE7 救済 frame backfill (= 別 channel で本来時刻にプロット).
    ("power_w_recovered_backfill_total", "Power W Recovered Backfill", None, None, "total_increasing", "diagnostic"),
    # Spec 029: 累積系 (energy_*_kwh) 救済 frame backfill (= spec 020 v1.5 可視化完成).
    ("cumulative_recovered_backfill_total", "Cumulative Recovered Backfill", None, None, "total_increasing", "diagnostic"),
    # Spec 034: SKSCAN channel mask cache (= reconnect 32s → 4s 短縮効果率).
    ("wisun_reconnect_short_scan_total",    "Wi-SUN Reconnect Short Scan", None, None, "total_increasing", "diagnostic"),
    ("wisun_reconnect_fallback_full_scan_total", "Wi-SUN Reconnect Fallback Full Scan", None, None, "total_increasing", "diagnostic"),
    # Spec 035: SKLL64 cached + SKJOIN 直行 (= reconnect 35s → 7s 短縮効果率).
    ("wisun_reconnect_cached_skjoin_total", "Wi-SUN Reconnect Cached SKJOIN", None, None, "total_increasing", "diagnostic"),
    ("wisun_reconnect_cached_skjoin_fallback_total", "Wi-SUN Reconnect Cached SKJOIN Fallback", None, None, "total_increasing", "diagnostic"),
    # Spec 022: realtime burst mode (= Admin UI ボタンで 5 分間 5s polling).
    ("realtime_burst_started_total",     "Realtime Burst Started",     None, None, "total_increasing", "diagnostic"),
    ("realtime_burst_completed_total",   "Realtime Burst Completed",   None, None, "total_increasing", "diagnostic"),
    ("realtime_burst_aborted_total",     "Realtime Burst Aborted",     None, None, "total_increasing", "diagnostic"),
    ("realtime_mode_current",            "Realtime Mode",              None, None, None,               "diagnostic"),
    ("realtime_effective_interval_seconds", "Realtime Effective Interval", "s", None, "measurement",   "diagnostic"),
    ("noise_adaptive_skips_total",       "Noise-Adaptive Skips",       None, None, "total_increasing", "diagnostic"),
    ("uptime_seconds",         "Uptime",              "s",  None,        "measurement",      "diagnostic"),
    ("version",                "Bridge Version",      None, None,        None,               "diagnostic"),
    # Spec 006: Wi-SUN health observability — RTT distribution + EVENT/FAIL.
    ("erxudp_latency_p50_ms",  "ERXUDP Latency p50",  "ms", None,        "measurement",      "diagnostic"),
    ("erxudp_latency_p95_ms",  "ERXUDP Latency p95",  "ms", None,        "measurement",      "diagnostic"),
    ("erxudp_latency_max_ms",  "ERXUDP Latency Max",  "ms", None,        "measurement",      "diagnostic"),
    # spec 036: SK EVENT ラベルは BP35A1 公式リファレンス Ver 1.3.2 p.51 に整合。
    # spec 038/040: EVENT 21 (= TX Result Notification) + EVENT 27 (= Session Termination Done) Phase 1 観察追加。
    ("sk_event_21_total",      "SK EVENT 21 (TX Result Notification)",                 None, None, "total_increasing", "diagnostic"),
    ("sk_event_22_total",      "SK EVENT 22 (Active Scan Done)",                       None, None, "total_increasing", "diagnostic"),
    ("sk_event_24_total",      "SK EVENT 24 (PANA Failed)",                            None, None, "total_increasing", "diagnostic"),
    ("sk_event_25_total",      "SK EVENT 25 (PANA Done)",                              None, None, "total_increasing", "diagnostic"),
    ("sk_event_26_total",      "SK EVENT 26 (Session Termination Requested by Peer)",  None, None, "total_increasing", "diagnostic"),
    ("sk_event_27_total",      "SK EVENT 27 (Session Termination Done)",               None, None, "total_increasing", "diagnostic"),
    ("sk_event_28_total",      "SK EVENT 28 (Session Termination Timeout)",            None, None, "total_increasing", "diagnostic"),
    ("sk_event_29_total",      "SK EVENT 29 (Session Lifetime Expired)",               None, None, "total_increasing", "diagnostic"),
    ("sk_event_32_total",      "SK EVENT 32 (ARIB Transmit Limit Hit)",                None, None, "total_increasing", "diagnostic"),
    ("sk_event_33_total",      "SK EVENT 33 (ARIB Transmit Limit Released)",           None, None, "total_increasing", "diagnostic"),
    # spec 037: WOPT FLASH 書込み寿命 10,000 回制限対策の観測点 (= BP35A1 Ver 1.3.2 p.41)。
    ("wopt_write_skipped_total", "WOPT Write Skipped (= FLASH 寿命対策)",               None, None, "total_increasing", "diagnostic"),
    ("wopt_write_total",         "WOPT Write Count",                                    None, None, "total_increasing", "diagnostic"),
    # spec 042: SKADDNBR で IP 層ネイバーキャッシュ登録 (= BP35A1 Ver 1.3.2 p.29、 初回 SKSENDTO 1-2s 短縮)。
    ("skaddnbr_total",           "SKADDNBR Neighbor Cache Add",                         None, None, "total_increasing", "diagnostic"),
    ("skaddnbr_fail_total",      "SKADDNBR Fail Count",                                 None, None, "total_increasing", "diagnostic"),
    # spec 038 Phase 2: EVENT 21 PARAM 別 counter (= BP35A1 Ver 1.3.2 p.51、 TX Result 0/1/2)。
    ("sk_event_21_param0_total", "SK EVENT 21 PARAM=0 (TX Success)",                    None, None, "total_increasing", "diagnostic"),
    ("sk_event_21_param1_total", "SK EVENT 21 PARAM=1 (TX Fail = CSMA/ARIB/Ack)",       None, None, "total_increasing", "diagnostic"),
    ("sk_event_21_param2_total", "SK EVENT 21 PARAM=2 (TX Auto-Retry)",                 None, None, "total_increasing", "diagnostic"),
    # spec 040 Phase 2a: S17=0 設定 + 直近 EVENT 25 経過秒 (= 能動 SKREJOIN tick base、 BP35A1 Ver 1.3.2 p.9)。
    ("s17_off_total",            "S17 Off Set Count (= PaC auto reauth disabled)",      None, None, "total_increasing", "diagnostic"),
    ("last_event_25_seconds",    "Seconds Since Last EVENT 25",                         "s",  None, "measurement",      "diagnostic"),
    # spec 040 Phase 2b: 能動 SKREJOIN 発火 / 失敗 counter (= BP35A1 Ver 1.3.2 p.14、 720s reauth 衝突回避)。
    ("skrejoin_total",           "SKREJOIN Active Count",                               None, None, "total_increasing", "diagnostic"),
    ("skrejoin_fail_total",      "SKREJOIN Fail Count (= fallback to reconnect)",       None, None, "total_increasing", "diagnostic"),
    # spec 051: 計画的 session refresh (= SKREJOIN tick を supersede)。
    ("proactive_rejoin_total",   "Proactive Session Refresh",   None, None, "total_increasing", "diagnostic"),
    # spec 046: current_r_a (= 0xE8) 救済 frame backfill (= spec 028 と同 ECHONET フレーム、 別 counter)。
    ("current_r_a_recovered_backfill_total",
     "Current R-A Recovered Backfill (= 0xE8)",                                         None, None, "total_increasing", "diagnostic"),
    # spec 047: rescued frame 内訳観測 (= ESV / TID / lag bucket / empty measurement)。
    ("erxudp_rescued_esv_get_res_total",  "Rescued ESV Get_Res",         None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_esv_get_sna_total",  "Rescued ESV Get_SNA",         None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_esv_inf_total",      "Rescued ESV INF (= H2)",      None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_esv_other_total",    "Rescued ESV Other",           None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_tid_zero_total",     "Rescued TID=0 (= H1)",        None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_tid_ring_hit_total", "Rescued TID Ring Hit",        None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_lag_lt5s_total",     "Rescued Lag <5s",             None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_lag_5to60s_total",   "Rescued Lag 5-60s",           None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_lag_60to300s_total", "Rescued Lag 60-300s (= H3)",  None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_lag_gt300s_total",   "Rescued Lag >300s",           None, None, "total_increasing", "diagnostic"),
    ("erxudp_rescued_empty_measurement_total",
     "Rescued Empty Measurement (= H2)",                                                None, None, "total_increasing", "diagnostic"),
    # spec 048: INF (= 非 Get 応答) を rescue 対象から排除した回数。
    ("erxudp_inf_ignored_total",          "INF Ignored (= spec 048)",    None, None, "total_increasing", "diagnostic"),
    # spec 049: noisy cycle の 0xE7 単独 minimal poll 観測。
    ("noise_minimal_polls_total",         "Noise Minimal Polls",         None, None, "total_increasing", "diagnostic"),
    ("noise_minimal_poll_success_total",  "Noise Minimal Poll Success",  None, None, "total_increasing", "diagnostic"),
    ("noise_minimal_poll_timeout_total",  "Noise Minimal Poll Timeout",  None, None, "total_increasing", "diagnostic"),
    ("sk_error_ER05_total",    "SK FAIL ER05",                None, None, "total_increasing", "diagnostic"),
    ("sk_error_ER09_total",    "SK FAIL ER09",                None, None, "total_increasing", "diagnostic"),
    ("sk_error_ER10_total",    "SK FAIL ER10",                None, None, "total_increasing", "diagnostic"),
    # Spec 010: EEDSCAN 920MHz noise floor.
    ("eedscan_pan_channel_energy", "EEDSCAN energy (PAN ch)", None, None, "measurement", "diagnostic"),
    ("eedscan_max_energy",     "EEDSCAN max energy",          None, None, "measurement", "diagnostic"),
    ("eedscan_min_energy",     "EEDSCAN min energy",          None, None, "measurement", "diagnostic"),
]


def _build_discovery_config(device_id, sid, name, unit, dev_class, state_class,
                            state_topic, entity_category=None):
    """Build a Home Assistant Auto-Discovery sensor config dict.

    Optional fields (unit_of_measurement, device_class, state_class,
    entity_category) are omitted when None so the payload stays minimal and
    HA doesn't reject unknown values.
    """
    cfg = {
        "name":        name,
        "unique_id":   "{}_{}".format(device_id, sid),
        "state_topic": state_topic,
        "device":      _device_dict(device_id),
    }
    if unit is not None:
        cfg["unit_of_measurement"] = unit
    if dev_class is not None:
        cfg["device_class"] = dev_class
    if state_class is not None:
        cfg["state_class"] = state_class
    if entity_category is not None:
        cfg["entity_category"] = entity_category
    return cfg


def publish_ha_discovery_diag(mqtt, device_id):
    """Publish HA Auto-Discovery configs for the 10 diagnostic sensors."""
    base = "cubej/{}".format(device_id)
    for sid, name, unit, dev_class, state_class, entity_category in DIAG_SENSOR_DEFS:
        topic = "homeassistant/sensor/{}/{}/config".format(device_id, sid)
        state_topic = "{}/diag/{}".format(base, sid)
        cfg = _build_discovery_config(
            device_id, sid, name, unit, dev_class, state_class,
            state_topic, entity_category=entity_category,
        )
        mqtt.publish(topic, cfg, retain=True)
        log("HA discovery: {}".format(topic))


def publish_ha_discovery(mqtt, device_id):
    base = "cubej/{}".format(device_id)
    for sid, name, unit, dev_class, state_class in SENSOR_DEFS:
        topic = "homeassistant/sensor/{}/{}/config".format(device_id, sid)
        state_topic = "{}/{}".format(base, sid)
        cfg = _build_discovery_config(
            device_id, sid, name, unit, dev_class, state_class, state_topic,
        )
        mqtt.publish(topic, cfg, retain=True)
        log("HA discovery: {}".format(topic))
    # Publish diagnostic sensor configs as part of the single entry point.
    publish_ha_discovery_diag(mqtt, device_id)


def publish_diag(mqtt, device_id, snapshot):
    """Publish each non-None snapshot entry to cubej/<id>/diag/<key>.

    All diagnostic topics are sent with retain=True so HA / broker restarts
    do not blank out the last known value.
    """
    base = "cubej/{}".format(device_id)
    for sid, _name, _unit, _dc, _sc, _ec in DIAG_SENSOR_DEFS:
        if sid not in snapshot:
            continue
        value = snapshot[sid]
        if value is None:
            continue
        mqtt.publish("{}/diag/{}".format(base, sid), str(value), retain=True)

# spec 020: late publish 対象 = 累積系 EPC のみ (= HA グラフ歪みリスクの
# 瞬時系 0xE7/0xE8 は除外)。 main loop で m を filter してから
# publish_measurements に渡す。
CUMULATIVE_PUBLISH_KEYS = frozenset((
    "energy_forward_kwh", "energy_reverse_kwh",
    "energy_forward_fixed_kwh", "energy_reverse_fixed_kwh",
    "energy_forward_fixed_ts", "energy_reverse_fixed_ts",
))


def publish_measurements(mqtt, device_id, m, timestamp=None):
    base = "cubej/{}".format(device_id)
    if "power_w" in m:
        mqtt.publish("{}/power".format(base), str(m["power_w"]))
    if "energy_forward_kwh" in m:
        mqtt.publish("{}/energy_forward".format(base), "{:.3f}".format(m["energy_forward_kwh"]))
    if "energy_reverse_kwh" in m:
        mqtt.publish("{}/energy_reverse".format(base), "{:.3f}".format(m["energy_reverse_kwh"]))
    if "current_r_a" in m:
        mqtt.publish("{}/current_r".format(base), "{:.1f}".format(m["current_r_a"]))
    if "current_t_a" in m:
        mqtt.publish("{}/current_t".format(base), "{:.1f}".format(m["current_t_a"]))
    # spec 018: 30-min boundary cumulative energy + meter-side timestamp.
    # ts topics are retained=True so the last meter timestamp survives a
    # broker rebuild (works well with the spec 016 discovery republish).
    if "energy_forward_fixed_kwh" in m:
        mqtt.publish("{}/energy_forward_fixed".format(base),
                     "{:.3f}".format(m["energy_forward_fixed_kwh"]))
    if "energy_reverse_fixed_kwh" in m:
        mqtt.publish("{}/energy_reverse_fixed".format(base),
                     "{:.3f}".format(m["energy_reverse_fixed_kwh"]))
    if "energy_forward_fixed_ts" in m:
        mqtt.publish("{}/energy_forward_fixed_ts".format(base),
                     m["energy_forward_fixed_ts"], retain=True)
    if "energy_reverse_fixed_ts" in m:
        mqtt.publish("{}/energy_reverse_fixed_ts".format(base),
                     m["energy_reverse_fixed_ts"], retain=True)
    # spec 020: late frame (= TID mismatch 救済 frame) は timestamp 引数で
    # 識別、 累積系 key のみ `_late_publish_ts` topic で過去時刻を発行。
    # 既存 value topic も publish するが HA state は last_changed=now で
    # 上書きされる、 InfluxDB / Grafana 側で `_late_publish_ts` を join 可能。
    if timestamp is not None:
        ts_iso = format_iso8601_utc(timestamp)
        for key in m.keys():
            if key in CUMULATIVE_PUBLISH_KEYS:
                mqtt.publish("{}/{}_late_publish_ts".format(base, key),
                             ts_iso, retain=True)

# spec 028: 救済 frame の瞬時値を別 topic に過去時刻 backfill JSON で publish。
# telegraf JSON parser が `json_time_key="ts"` で metric.time に変換、
# prometheus remote_write が client-supplied timestamp として送信 (= 2h backfill)。
RECOVERY_BACKFILL_KEYS = frozenset(("power_w",))

# spec 029: 累積系 (energy_*_kwh) の救済 frame backfill。 spec 020 v1.5 で
# `_late_publish_ts` retain publish していたが telegraf 未 subscribe で grafana
# に来ない片手落ち状態を、 spec 028 と同 JSON 経路で完全可視化する。
# `_fixed_kwh` 系も bridge publish 対象 (= reconnect 直後 cycle 0 = tier4 で
# 必ず発火、 panel overlay は forward/reverse のみ)。
CUMULATIVE_BACKFILL_KEYS = frozenset((
    "energy_forward_kwh", "energy_reverse_kwh",
    "energy_forward_fixed_kwh", "energy_reverse_fixed_kwh",
))

# spec 046: 電流 (= 0xE8) の救済 frame backfill。 spec 028 (= power_w / 0xE7)
# と同 ECHONET フレームに含まれるため遅延応答救済時に同時 publish 可能、 別
# counter (= current_r_a_recovered_backfill_total) で観測解像度を保つ。
# current_t_a (= T 相、 単相 2 線式メーターでは null) は publish_recovery_backfill
# 内 None ガードで skip 済、 安全に含めて OK。
CURRENT_BACKFILL_KEYS = frozenset(("current_r_a", "current_t_a"))

# spec 047 FR-004: rescued cycle で publish 可能な実 data が 1 つも取れて
# いないかの判定対象 (= late publish + backfill 3 系統の合算)。
_RESCUED_PUBLISHABLE_KEYS = (CUMULATIVE_PUBLISH_KEYS
                             | RECOVERY_BACKFILL_KEYS
                             | CUMULATIVE_BACKFILL_KEYS
                             | CURRENT_BACKFILL_KEYS)


def rescued_measurement_is_empty(m):
    """spec 047 FR-004: rescued frame の decode 結果 *m* が publish 対象
    key を 1 つも含まない (= INF 等の非 Get 応答で cycle を浪費した) か。
    None 値は publish 側で skip されるため空扱い。"""
    for key, value in m.items():
        if key in _RESCUED_PUBLISHABLE_KEYS and value is not None:
            return False
    return True


def publish_late_frame(mqtt, device_id, m, send_ts, cfg, diag_state):
    """spec 048 FR-004: drain された rescued frame の measurement publish。

    旧 _late_ts 分岐 (= spec 020 late publish + spec 028/029/046 backfill
    3 系統 + spec 047 空判定) の関数抽出。 live topic (現在時刻) には
    一切流さない = data 誠実性維持。"""
    _m_late = dict((k, v) for k, v in m.items()
                   if k in CUMULATIVE_PUBLISH_KEYS)
    if _m_late:
        publish_measurements(mqtt, device_id, _m_late, timestamp=send_ts)
    if cfg.get("power_w_recovery_backfill_enabled", True):
        _m_bf = dict((k, v) for k, v in m.items()
                     if k in RECOVERY_BACKFILL_KEYS)
        if _m_bf:
            publish_recovery_backfill(mqtt, device_id, _m_bf, send_ts,
                                      diag_state)
    if cfg.get("cumulative_recovery_backfill_enabled", True):
        _m_cum_bf = dict((k, v) for k, v in m.items()
                         if k in CUMULATIVE_BACKFILL_KEYS)
        if _m_cum_bf:
            publish_recovery_backfill(
                mqtt, device_id, _m_cum_bf, send_ts, diag_state,
                counter_attr="cumulative_recovered_backfill_total")
    if cfg.get("current_r_a_recovery_backfill_enabled", True):
        _m_cur_bf = dict((k, v) for k, v in m.items()
                         if k in CURRENT_BACKFILL_KEYS)
        if _m_cur_bf:
            publish_recovery_backfill(
                mqtt, device_id, _m_cur_bf, send_ts, diag_state,
                counter_attr="current_r_a_recovered_backfill_count")
    if rescued_measurement_is_empty(m):
        try:
            diag_state.on_erxudp_rescued_empty_measurement()
        except Exception as e:
            log("diag on_erxudp_rescued_empty_measurement error: {}"
                .format(e))


def publish_recovery_backfill(mqtt, device_id, m, send_ts, diag_state=None,
                              counter_attr="power_w_recovered_backfill_total"):
    """spec 028/029: 救済 frame の値を `<key>_recovered_json` topic に
    `{"value": V, "ts": "<ISO8601 UTC>"}` 形式で publish (retain=False, qos=0).

    `diag_state` が渡されたら `counter_attr` で指定された attribute を increment。
    spec 028 default = `power_w_recovered_backfill_total` (= 互換)。
    spec 029 caller は `counter_attr="cumulative_recovered_backfill_total"` を渡す。
    """
    base = "cubej/{}".format(device_id)
    ts_iso = format_iso8601_utc(send_ts)
    for key, value in m.items():
        if value is None:
            continue
        topic = "{}/{}_recovered_json".format(base, key)
        payload = json.dumps({"value": value, "ts": ts_iso})
        mqtt.publish(topic, payload, retain=False)
        if diag_state is not None:
            setattr(diag_state, counter_attr,
                    getattr(diag_state, counter_attr, 0) + 1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    cfg           = apply_defaults(load_config())
    br_id         = cfg["br_id"]
    br_pwd        = cfg["br_pwd"]
    ha_host       = cfg["mqtt_host"]
    ha_port       = int(cfg.get("mqtt_port", 1883))
    ha_user       = cfg.get("mqtt_user", "")
    ha_pass       = cfg.get("mqtt_pass", "")
    device_id     = cfg.get("device_id", "cubej1")
    serial_port   = cfg.get("serial_port", "/dev/ttyS1")
    poll_interval = int(cfg.get("poll_interval", 60))

    global LOGGER
    LOGGER = JsonLogger(LOG_PATH, level=cfg["log_level"],
                        max_bytes=int(cfg["log_max_bytes"]),
                        backup_count=int(cfg["log_backup_count"]))
    emit_bridge_start(LOGGER, device_id=device_id, version=bridge_version())

    # spec 041 Phase 2b: record uncaught-exception tracebacks before init
    # discards the process (silent death root-cause capture).
    install_excepthook(logger=LOGGER)

    # Diagnostics aggregator declared early so the admin UI can read it.
    diag_state = DiagState(start_time=time.time(), version=bridge_version())

    # spec 009: high-frequency RTT probe mode. In-memory; resets at restart.
    probe_state = ProbeState()

    # spec 010: periodic EEDSCAN to track 920MHz noise floor.
    eedscan_state = EedScanState(
        interval_sec=int(cfg.get("eedscan_interval_sec", 300)))

    # spec 022: realtime burst mode holder. Admin UI / API mutates via
    # start_burst/stop_burst, main loop reads via tick()/snapshot().
    realtime_state = RealtimeModeState()

    # spec 020: TID mismatch late publish 用 ring buffer (= 過去 send の
    # TID → send_ts を bounded で保持、 メーター ECHONET 内部 queue 遅延応答
    # 救済)。 disabled 時は send_history=None を read_erxudp に渡す。
    send_history = SendHistoryRing(maxlen=int(cfg.get(
        "tid_mismatch_history_maxlen", 240))) if cfg.get(
        "tid_mismatch_recover_enabled", True) else None

    # spec 019: persist Wi-Fi AP toggle state across reboots. The store
    # is shared with the admin handler's ApController so toggles via
    # admin UI write to the same file the startup restore will read.
    # ap_state_persist_enabled=False yields state_store=None → both
    # restore and write are skipped (clean kill switch).
    _ap_store = (ApStateStore(cfg.get("ap_state_file_path",
                              "/data/local/cube_j1_ap_state"))
                 if cfg.get("ap_state_persist_enabled", True) else None)
    _ap_controller = ApController(state_store=_ap_store)

    # Embedded admin UI (Constitution VI). Off by default; opted in via
    # config. Failure to start is logged but never aborts the bridge.
    if cfg.get("admin_ui_enabled") and cfg.get("admin_user") and cfg.get("admin_password"):
        try:
            start_admin_server(
                port=int(cfg.get("admin_ui_port", 8080)),
                user=cfg["admin_user"],
                password=cfg["admin_password"],
                diag_state_provider=lambda: diag_state,
                ap_controller=_ap_controller,
                probe_state_provider=lambda: probe_state,
                eedscan_state_provider=lambda: eedscan_state,
                realtime_state_provider=lambda: realtime_state,
            )
            LOGGER.info(event="admin_ui_started",
                        context={"port": int(cfg.get("admin_ui_port", 8080))})
        except Exception as e:
            LOGGER.error(event="admin_ui_start_failed",
                         context={"error": str(e)})

    # Connect MQTT. The on_reconnect callback bumps the diag counter every
    # time the broker session is re-established.
    mqtt = MQTTClient(ha_host, ha_port, "cubej1_{}".format(device_id),
                      username=ha_user, password=ha_pass,
                      on_reconnect=diag_state.on_mqtt_reconnect,
                      keepalive=cfg["mqtt_keepalive"],
                      threading_enabled=cfg["mqtt_threading_enabled"],
                      send_queue_maxsize=cfg["mqtt_send_queue_maxsize"])
    while True:
        try:
            mqtt.connect()
            break
        except Exception as e:
            log("MQTT connect failed: {} - retry in 15s".format(e))
            time.sleep(15)

    publish_ha_discovery(mqtt, device_id)
    # spec 016: seed last-publish ts so the periodic re-publish check
    # doesn't fire on the first main-loop tick. counter stays at 0.
    diag_state.mark_initial_discovery_publish(time.time())

    # spec 019: restore Wi-Fi AP toggle state from the last session.
    # _ap_store is None when persist is disabled → skip the restore
    # entirely (kill switch).
    if _ap_store is not None:
        try:
            _restored = apply_ap_state_restore(_ap_store.read(), _ap_controller)
            if _restored is not None:
                log("AP restored: {}".format(_restored))
        except Exception as e:
            log("WARN: AP state restore failed: {}".format(e))

    # Open serial port
    log("Opening serial {}".format(serial_port))
    fd = None
    while True:
        try:
            fd = open_serial(serial_port)
            break
        except Exception as e:
            log("Serial open failed: {} - retry in 10s".format(e))
            time.sleep(10)

    # Wi-SUN join (initial)
    ipv6 = None
    while True:
        try:
            ipv6 = wisun_connect(fd, br_id, br_pwd, diag_state=diag_state)
            break
        except Exception as e:
            log("Wi-SUN join failed: {} - retry in 60s".format(e))
            time.sleep(60)

    log("Meter connected at {}".format(ipv6))

    tid       = 1
    coeff     = 1
    unit_kwh  = 1.0
    last_ping = time.time()
    # spec 009 mixed pattern: track the last normal-EPCS cycle so probe
    # mode can interleave fast probes without starving HA of power values.
    last_normal_poll_start = 0.0
    # spec 011 C: count normal cycles to rotate EPC tier across the
    # tier1 (per cycle), tier2 (5 cycles), tier3 (60 cycles) schedule.
    normal_cycle_count = 0
    # spec 012: track consecutive noise-skip count for the fail-safe.
    noise_skip_streak = 0
    # spec 022: catch-up state after burst expires/aborts. catchup_interval
    # captures the burst_interval that was active so the catch-up runs at
    # the same fast cadence (dig 決定 A: burst_interval 引き継ぎ).
    catchup_remaining = 0
    catchup_interval = poll_interval

    while True:
        try:
            last_poll_start = time.time()
            # spec 051: 計画的 session refresh — PANA 失効 (~10-12 分周期) の
            # 手前 (default 480s) で先回り full reconnect。 専用例外で外側
            # reconnect path を再利用 (= backoff 0、 cached SKJOIN ~15s)。
            # 発火点は spec 040 Phase 2b hotfix v2 と同じ cycle 開始時。
            if cfg.get("proactive_rejoin_enabled", True) and \
                    should_fire_proactive_rejoin(
                        diag_state.last_event_25_ts, last_poll_start,
                        int(cfg.get("proactive_rejoin_after_sec", 480)),
                        diag_state.skrejoin_active):
                _elapsed = int(last_poll_start - diag_state.last_event_25_ts)
                try:
                    diag_state.on_proactive_rejoin()
                except Exception as e:
                    log("diag on_proactive_rejoin error: {}".format(e))
                raise ProactiveSessionRefresh(
                    "planned session refresh (last_event_25_seconds={})"
                    .format(_elapsed))
            # spec 040 Phase 2b hotfix v2: cycle 開始時に能動 SKREJOIN 判定
            # (= poll_success 直後のみに置くと poll_failure 連続 60% baseline
            # で 480s 閾値超過しても発火機会なし、 その間に既存 reconnect path
            # が先に発火して 12 分周期 reconnect が継続する root cause 判明)。
            # cycle 開始時 = poll_success/failure に関係なく発火機会を確保。
            # spec 051 FR-004: default OFF (= proactive が supersede)。
            if cfg.get("skrejoin_tick_enabled", False) and \
                    should_fire_skrejoin(diag_state, last_poll_start):
                try:
                    _pan = {
                        "Pan ID": diag_state.pan_id,
                        "Channel": "{:02X}".format(diag_state.pan_channel)
                        if diag_state.pan_channel is not None else None,
                        "Addr": diag_state.mac,
                    }
                    skrejoin_tick(fd, diag_state, _pan, ipv6)
                except Exception as e:
                    log("skrejoin_tick exception ({}), continue main loop".format(e))
            # spec 016: HA discovery auto-republish on MQTT reconnect
            # (pending flag) or every discovery_republish_interval_sec.
            # Localized try/except so a republish failure can't kill the
            # main loop; pending stays True so the next tick retries.
            if should_republish_discovery(
                    last_poll_start, diag_state.last_discovery_publish_ts,
                    diag_state.pending_discovery_republish,
                    int(cfg.get("discovery_republish_interval_sec", 86400))):
                try:
                    publish_ha_discovery(mqtt, device_id)
                    diag_state.on_discovery_republish(last_poll_start)
                except Exception as e:
                    log("WARN: HA discovery republish failed: {}".format(e))
            probing = probe_state.is_active(last_poll_start)

            # spec 022: realtime burst mode tick. transition='expired'/'aborted'
            # triggers a 4-iter catch-up (tier4 → tier3 → tier2 → tier1) at
            # the previous burst_interval. Re-start during catch-up clears
            # it so the new burst takes over (dig 決定 D).
            _rt_mode, _rt_trans = realtime_state.tick(last_poll_start)
            if _rt_trans == "expired":
                diag_state.on_realtime_burst_completed()
                catchup_remaining = 4
                catchup_interval = int(realtime_state.snapshot().get(
                    "burst_interval", REALTIME_BURST_DEFAULT_INTERVAL_SEC))
            elif _rt_trans == "aborted":
                # diag.on_realtime_burst_aborted() was emitted in the
                # admin thread already; here we just trigger the catch-up.
                catchup_remaining = 4
                catchup_interval = int(realtime_state.snapshot().get(
                    "burst_interval", REALTIME_BURST_DEFAULT_INTERVAL_SEC))
            if _rt_mode == "burst" and catchup_remaining > 0:
                catchup_remaining = 0
            if catchup_remaining > 0:
                _eff_interval = catchup_interval
            else:
                _eff_interval = compute_effective_poll_interval(
                    last_poll_start, poll_interval,
                    realtime_state.snapshot())
            diag_state.set_realtime_state(_rt_mode, _eff_interval)
            kind = decide_cycle_kind(probing, last_normal_poll_start,
                                     last_poll_start, poll_interval)
            # spec 012: skip normal poll while the PAN channel is noisy,
            # with a fail-safe of N consecutive skips to prevent indefinite
            # silence if EEDSCAN never settles.
            # spec 049: 完全 skip の代わりに 0xE7 単独 minimal poll に降格
            # (= decide_noise_cycle_action、 kill switch で spec 012 挙動)。
            _noise_minimal = False
            if kind == "normal" and cfg.get("noise_adaptive_skip_enabled", True):
                _noise_action = decide_noise_cycle_action(
                    eedscan_state.is_noisy(
                        threshold=int(cfg.get("noise_skip_threshold", 100)),
                        pan_channel=diag_state.pan_channel),
                    noise_skip_streak,
                    int(cfg.get("noise_skip_max_consecutive", 3)),
                    cfg.get("noise_skip_tier1_minimal_poll_enabled", True))
                if _noise_action != "normal":
                    try:
                        diag_state.on_noise_adaptive_skip()
                    except Exception as e:
                        log("diag on_noise_adaptive_skip error: {}".format(e))
                    noise_skip_streak += 1
                    if _noise_action == "skip":
                        # spec 022: 起床周期も _eff_interval に統一して
                        # burst 中の noise skip でも 5s 周期を維持。
                        time.sleep(compute_next_poll_sleep(
                            last_poll_start, time.time(), _eff_interval))
                        continue
                    _noise_minimal = True
                    try:
                        diag_state.on_noise_minimal_poll()
                    except Exception as e:
                        log("diag on_noise_minimal_poll error: {}".format(e))
                else:
                    noise_skip_streak = 0
            if _noise_minimal:
                # spec 049: 0xE7 単独 OPC=1。 tier rotation / normal_cycle_count
                # / last_normal_poll_start は進めない (= 既存 skip と同じ扱い、
                # noise 明けの tier 消化を乱さない)。
                cycle_epcs = NOISE_MINIMAL_EPCS
            elif kind == "probe":
                cycle_epcs = PROBE_EPCS
            elif catchup_remaining > 0:
                # spec 022: catch-up sequence after burst (dig 決定 A) —
                # 4 iter で tier4 → tier3 → tier2 → tier1 を消費して
                # 累積値などの skip 分を補完。 cycle counter は進めない。
                _catchup_tiers = ["tier4", "tier3", "tier2", "tier1"]
                tier = _catchup_tiers[4 - catchup_remaining]
                # spec 033: 全 cycle で tier1 EPCs を batch (= mismatch 100% backfill 対象).
                cycle_epcs = cycle_epcs_with_tier1(tier)
                catchup_remaining -= 1
                last_normal_poll_start = last_poll_start
            elif _rt_mode == "burst":
                # spec 022: burst 中は tier1 (0xE7 瞬時電力) 固定で 5s 周期。
                # normal_cycle_count は進めず、 burst 終了時に既存 tier
                # rotation を妨げない (cycle counter 連続性保持)。
                cycle_epcs = TIER1_EPCS
                last_normal_poll_start = last_poll_start
            else:
                # spec 011 C: tier rotation. Smaller per-cycle payload =
                # faster meter response + less data starvation on the
                # rare-but-needed tiers.
                # spec 018: tier4 added (default every=30 = 30-min boundary
                # cumulative energy with meter-side timestamp).
                tier = decide_epc_tier(
                    normal_cycle_count,
                    tier4_every=int(cfg.get("epc_tier4_every", 30)))
                # spec 033: 全 cycle で tier1 EPCs を batch (= mismatch 100% backfill 対象、
                # [[feedback-cycle-counter-reconnect-tier4]] 構造的問題解消).
                cycle_epcs = cycle_epcs_with_tier1(tier)
                normal_cycle_count += 1
                last_normal_poll_start = last_poll_start
            # In probe mode, schedule the next cycle at the tight probe
            # interval; otherwise spec 022 _eff_interval drives the gap
            # (burst → burst_interval, catch-up → catchup_interval, off →
            # base poll_interval).
            cycle_interval = probe_state.interval_sec if probing else _eff_interval
            orig_led = led_read()
            led_rgb(0, 0, 255)
            try:
                # spec 011: ERXUDP resilience — extend timeout to handle
                # the p95 tail and add intra-cycle retries to mask single-
                # cycle drops from HA's perspective.
                # spec 023: burst (and catch-up) 中は base 30s だと 5s cycle
                # が成立しないので burst 専用の短 timeout を採用。
                _effective_mode = ("burst"
                                   if _rt_mode == "burst" or catchup_remaining > 0
                                   else "off")
                _erxudp_timeout = compute_erxudp_timeout(
                    _effective_mode,
                    int(cfg.get("erxudp_timeout_sec", 30)),
                    int(cfg.get("realtime_burst_erxudp_timeout_sec",
                                REALTIME_BURST_ERXUDP_TIMEOUT_SEC)))
                _max_retries = int(cfg.get("erxudp_intra_cycle_retries", 2))
                _backoff = float(cfg.get("erxudp_retry_backoff_sec", 2))
                # spec 014: capture the TID actually sent so read_erxudp can
                # discard frames whose TID doesn't match (delayed reply from
                # the previous cycle or a stale frame after Wi-SUN re-join).
                sent_tid = tid
                send_el_get(fd, ipv6, sent_tid, epc_list=cycle_epcs)
                # spec 020: send 直後に ring 記録 (= mismatch 救済の根拠)
                if send_history is not None:
                    send_history.record(sent_tid, time.time(), cycle_epcs)
                tid = (tid + 1) & 0xFFFF
                t_send = time.time()
                data = read_erxudp(fd, timeout=_erxudp_timeout,
                                   diag_state=diag_state,
                                   expected_tid=sent_tid,
                                   send_history=send_history)
                attempt = 0
                while data is None and should_retry_in_cycle(attempt, _max_retries):
                    try:
                        diag_state.on_erxudp_intra_cycle_retry()
                    except Exception as e:
                        log("diag on_erxudp_intra_cycle_retry error: {}".format(e))
                    time.sleep(_backoff)
                    sent_tid = tid
                    send_el_get(fd, ipv6, sent_tid, epc_list=cycle_epcs)
                    if send_history is not None:
                        send_history.record(sent_tid, time.time(), cycle_epcs)
                    tid = (tid + 1) & 0xFFFF
                    t_send = time.time()
                    data = read_erxudp(fd, timeout=_erxudp_timeout,
                                       diag_state=diag_state,
                                       expected_tid=sent_tid,
                                       send_history=send_history)
                    attempt += 1
                if data is not None and attempt > 0:
                    try:
                        diag_state.on_erxudp_recovered_by_retry()
                    except Exception as e:
                        log("diag on_erxudp_recovered_by_retry error: {}".format(e))
                now = time.time()
                # spec 048 FR-004: read_erxudp が stash した rescued frame を
                # 毎 cycle drain して過去時刻 backfill publish (= data 有無に
                # 関わらず実行、 rescue-only cycle でも実データは救う)。
                while diag_state.pending_rescued_frames:
                    _payload_r, _send_ts_r = (
                        diag_state.pending_rescued_frames.popleft())
                    try:
                        _m_r = apply_energy_scale(
                            decode_measurements(parse_el_response(_payload_r)),
                            coeff, unit_kwh)
                        publish_late_frame(mqtt, device_id, _m_r,
                                           _send_ts_r, cfg, diag_state)
                    except Exception as e:
                        log("late frame drain error: {}".format(e))
                if data:
                    try:
                        diag_state.on_erxudp_latency((now - t_send) * 1000.0)
                    except Exception as e:
                        log("diag on_erxudp_latency error: {}".format(e))
                    props = parse_el_response(data)
                    m     = decode_measurements(props)
                    m     = apply_energy_scale(m, coeff, unit_kwh)
                    if "coefficient" in m:
                        coeff = m["coefficient"]
                    if "unit_kwh" in m:
                        unit_kwh = m["unit_kwh"]
                    if kind == "normal":
                        # Probe responses (0x80 only) carry no power values;
                        # publish only on real measurement cycles.
                        # spec 048 FR-007: 旧 _late_ts bus 分岐 (= spec 020/
                        # 028/029/046/047) は pending_rescued_frames drain +
                        # publish_late_frame に一本化して撤去。 ここに来る
                        # data は expected TID 一致の live 応答のみ。
                        publish_measurements(mqtt, device_id, m)
                    emit_poll_success(LOGGER, measurements=m)
                    try:
                        diag_state.on_poll_success(now)
                    except Exception as e:
                        log("diag on_poll_success error: {}".format(e))
                    if _noise_minimal:
                        # spec 049 FR-003: noise 中でも OPC=1 なら通る仮説の
                        # 回収率観測 (= SC-1)。
                        try:
                            diag_state.on_noise_minimal_poll_success()
                        except Exception as e:
                            log("diag on_noise_minimal_poll_success error: {}"
                                .format(e))
                elif _noise_minimal:
                    # spec 049 FR-004: minimal poll の timeout は専用 counter
                    # のみ。 erxudp_timeouts / consecutive counter / force
                    # reconnect 判定に数えない (= spec 012 の noise 由来
                    # reconnect storm 防止を維持)。
                    emit_poll_failure(LOGGER,
                                      reason="noise_minimal_poll_timeout")
                    try:
                        diag_state.on_noise_minimal_poll_timeout()
                    except Exception as e:
                        log("diag on_noise_minimal_poll_timeout error: {}"
                            .format(e))
                else:
                    emit_poll_failure(LOGGER, reason="erxudp_timeout")
                    try:
                        diag_state.on_erxudp_timeout()
                        diag_state.on_poll_failure(now)
                    except Exception as e:
                        log("diag poll_failure error: {}".format(e))
                    # Safety net: if the meter has stopped replying while the
                    # Wi-SUN session still looks alive, bail out of the main
                    # loop so the outer except runs wisun_connect again.
                    # spec 017: pending_wisun_rejoin (set by EVENT 24/29)
                    # overrides the threshold for immediate rejoin. Signal
                    # is consumed (cleared) right before raise; if reconnect
                    # fails the next EVENT 24/29 will re-set the flag.
                    # spec 025: burst (and catch-up) 中は threshold を緩和し
                    # reconnect 連発で 5s 周期が乱れるのを防ぐ。 _effective_mode
                    # は spec 023 で iter 冒頭計算済 ("burst" or "off")。
                    _force_threshold = compute_force_reconnect_threshold(
                        _effective_mode,
                        int(cfg.get("erxudp_timeout_force_reconnect_threshold", 1)),
                        int(cfg.get("realtime_burst_force_reconnect_threshold",
                                    REALTIME_BURST_FORCE_RECONNECT_THRESHOLD)))
                    if should_force_wisun_reconnect(
                        diag_state.consecutive_erxudp_timeouts,
                        _force_threshold,
                        pending=diag_state.pending_wisun_rejoin,
                    ):
                        _was_pending = diag_state.pending_wisun_rejoin
                        diag_state.pending_wisun_rejoin = False
                        raise RuntimeError(
                            "wisun reconnect forced: consecutive_erxudp_timeouts={}, "
                            "pending={}".format(
                                diag_state.consecutive_erxudp_timeouts, _was_pending))
                # Publish diag snapshot once per poll cycle (FR-004).
                # best-effort: never let diag failure block the measurement
                # path (Constitution IV / FR-005).
                try:
                    snap = diag_state.snapshot(now)
                    # spec 010: merge EEDSCAN metrics in for the same publish
                    # cycle so Grafana plots line up.
                    try:
                        snap.update(eedscan_state.snapshot(
                            pan_channel=diag_state.pan_channel))
                    except Exception as e2:
                        log("eedscan snapshot error: {}".format(e2))
                    publish_diag(mqtt, device_id, snap)
                except Exception as e:
                    log("publish_diag error: {}".format(e))
            finally:
                led_rgb(*orig_led)

            if time.time() - last_ping > 50:
                mqtt.ping()
                last_ping = time.time()

            # spec 010: periodic EEDSCAN sweep. ~12 s blocking, so we only
            # fire it when its deadline has passed AND we're not in probe
            # mode (probe mode wants tight RTT samples and a 12 s gap
            # would distort the sparkline). Failure is non-fatal.
            # spec 022 (dig 決定 E): also skip while burst is active — the
            # 12 s scan would shatter the 5 s realtime cadence. catch-up
            # itself is short enough (~20 s) to tolerate one EEDSCAN.
            if (cfg.get("eedscan_enabled", True)
                    and not probing
                    and _rt_mode != "burst"
                    and eedscan_state.should_run(time.time())):
                try:
                    _run_eedscan_sweep(fd, eedscan_state, diag_state)
                except Exception as e:
                    log("EEDSCAN sweep error: {}".format(e))

            # Deadline-based pacing: keep ~cycle_interval between *poll starts*
            # rather than between cycle ends, so ERXUDP timeouts don't push
            # the next measurement out by (timeout + interval). In probe mode
            # cycle_interval is the tight probe interval (e.g. 5 s).
            time.sleep(compute_next_poll_sleep(last_poll_start, time.time(), cycle_interval))

        except Exception as e:
            # spec 017: exponential backoff for repeated wisun_connect failures
            # (30 → 60 → 120 → 240 → 300 clamp by default), plus serial port
            # reopen after N consecutive failures so a BP35CX UART hang can
            # also recover.
            attempt = diag_state.consecutive_wisun_connect_failures
            # spec 026: burst (and catch-up) 中は initial backoff を 5s に短縮、
            # reconnect 1 回あたりの時間ロスを 30s → 5s に減らす。 main loop
            # の `_effective_mode` は前 iter 値 / reconnect 中の未定義リスク
            # があるので、 realtime_state を直接読んで mode 再判定 (catch-up
            # 中 reconnect は稀のため非対応で実用上問題なし、 mode='burst' のみ
            # で burst 扱い)。
            _rt_mode_for_backoff = realtime_state.snapshot().get("mode", "off")
            _backoff_initial = compute_burst_aware_backoff_initial(
                _rt_mode_for_backoff,
                int(cfg.get("wisun_rejoin_backoff_initial_sec", 30)),
                int(cfg.get("realtime_burst_rejoin_backoff_initial_sec",
                            REALTIME_BURST_REJOIN_BACKOFF_INITIAL_SEC)))
            _backoff = compute_rejoin_backoff(
                attempt,
                _backoff_initial,
                float(cfg.get("wisun_rejoin_backoff_multiplier", 2.0)),
                int(cfg.get("wisun_rejoin_backoff_max_sec", 300)))
            # spec 051: 計画的 refresh は失敗由来ではないので backoff 不要
            # (= 停止を reconnect 実時間 ~15s だけに抑える)。 reconnect 自体
            # が失敗した場合は次周回で通常例外 → 既存 backoff に自然移行。
            if isinstance(e, ProactiveSessionRefresh):
                _backoff = 0
                log("Planned session refresh: {} - reconnecting now".format(e))
            else:
                log("Main loop error (attempt {}): {} - reconnecting Wi-SUN in {}s"
                    .format(attempt + 1, e, _backoff))
            time.sleep(_backoff)
            _reopen_after = int(cfg.get(
                "wisun_serial_reopen_after_rejoin_failures", 5))
            if _reopen_after > 0 and attempt + 1 >= _reopen_after:
                try:
                    os.close(fd)
                    fd = open_serial(serial_port)
                    diag_state.on_serial_reopen()
                    log("Serial port reopened after {} failures".format(
                        attempt + 1))
                except Exception as re:
                    log("WARN: serial reopen failed (continuing with original fd): {}"
                        .format(re))
            try:
                # spec 034: 単 ch mask scan path (= disable 推奨、 cache hit 0%)
                # spec 035: cached SKJOIN 直行 path (= SKSCAN/SKLL64 skip で 35s→7s)
                ipv6 = wisun_connect(
                    fd, br_id, br_pwd,
                    prefer_known_channel=bool(cfg.get(
                        "wisun_reconnect_channel_mask_enabled", True)),
                    fallback_duration=int(cfg.get(
                        "wisun_reconnect_channel_mask_fallback_duration", 6)),
                    prefer_cached_join=bool(cfg.get(
                        "wisun_reconnect_cached_skjoin_enabled", True)),
                    cached_invalidate_threshold=int(cfg.get(
                        "wisun_reconnect_cached_skjoin_invalidate_threshold", 2)),
                    diag_state=diag_state)
                log("Wi-SUN reconnected at {}".format(ipv6))
                diag_state.consecutive_wisun_connect_failures = 0
                try:
                    diag_state.on_wisun_reconnect()
                except Exception as e3:
                    log("diag on_wisun_reconnect error: {}".format(e3))
            except Exception as e2:
                diag_state.consecutive_wisun_connect_failures += 1
                log("Wi-SUN reconnect failed (will retry next cycle): {}"
                    .format(e2))


if __name__ == "__main__":
    main()
