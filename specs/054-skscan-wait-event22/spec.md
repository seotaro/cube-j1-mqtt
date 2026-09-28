# Feature Specification: SKSCAN を EVENT 22 (scan 完了) まで待つ

**Feature Branch**: `054-skscan-wait-event22`
**Created**: 2026-09-28
**Status**: Implemented (実機 verify 済み)
**Input**: 2026-09-28、 HEAD (dd3f846) の bridge を実機に deploy したところ Wi-SUN join が一度も成立しなくなった。 直前まで同じ config・同じメーターで poll_success が出ていたため、 設定や電波ではなく scan 処理の回帰として調査・修正した。

## 症状 (2026-09-28 02:49〜03:00 UTC)

- 再起動後の初回 join で `SKSCAN try mask=FFFFFFFF duration=6..10` が各 4〜10 秒で終わり、 毎回 `scan_retry` → `Wi-SUN join failed: SKSCAN: no PAN found ({}) - retry in 60s` を 4 周以上繰り返した
- MQTT 接続・HA discovery は正常。 計測だけが全停止
- 再起動前 (同 config) は process 内 cache による `SKJOIN cached direct` (spec 035) で join していたため、 全 ch scan を通らず表面化していなかった。 bridge 再起動で cache が消え、 全 ch scan 経路に入って初めて発覚
- 以前実機で動いていた bridge (出所は本 repo 履歴外) は `SKSCAN full scan (waits for EVENT 22, ~20s per try)` / `mask=0FFFFFFF` で 1 試行 17〜35 秒待ち、 d=7 で PAN を発見していた

## 原因

- `skscan()` が受信待ちの deadline を `now + duration` としていた。 `duration` は SKSCAN の scan 時間 code (1 ch あたり 0.96ms × (2^d + 1)) であって秒数ではない
- 実機 (BP35C0 EVER 1.5.2) の実測: `SKSCAN 2 0FFFFFFF 6 0` で EVENT 20 (beacon 受信) が **11.4s**、 EVENT 22 (scan 完了) が **17.7s** (≈ 0.63s/ch)。 6 秒で打ち切ると beacon を必ず取りこぼす
- 次試行の先頭で `tcflush` するため、 遅れて届いた EPANDESC も捨てられる
- 全 ch mask が `FFFFFFFF` で、 BP35CX に存在しない bit 28-31 (ch61-64) も含んでいた
- spec 034 の単 ch scan (d=3) も deadline 3 秒で同じ構造の取りこぼしを抱えていた

## Requirements

- **FR-001**: SKSCAN の受信待ちは EVENT 22 受信で即終了し、 EVENT 22 が来ない場合のみ上限時間で打ち切る。 上限は scan 対象 ch 数 (mask の立っている bit 数) と duration code から見積もる: `ch 数 × (0.00096 × (2^d + 1) + 1.0) + 10.0` 秒 (`compute_skscan_timeout`)
  - d=6 全 28ch → 39.7s (実測 17.7s の 2 倍強)、 d=10 全 28ch → 65.6s、 単 ch d=3 → 11.1s
- **FR-002**: 全 ch scan の mask を EEDSCAN と同じ `0FFFFFFF` (= ch33-60) にする (`FULL_SCAN_CHANNEL_MASK`)
- **FR-003**: 1 回の scan 試行につき SKSCAN コマンドは 1 回だけ送る (待ち中に重ねて送らない)
- **NFR-001**: duration の retry 段階 (6 → 10、 `SCAN_RETRY_LIMIT`)、 単 ch scan → 全 ch fallback の順序 (spec 034)、 cached SKJOIN 直行 (spec 035) は変更しない
- **NFR-002**: Python 2.7 stdlib のみ (constitution II)。 bit 数計算は `bin(...).count("1")` (`int.bit_count` は 3.10+ のため不可)

## Success Criteria

- **SC-001**: bridge 再起動 (cache なし) から 2 分以内に `wisun_joined` → `poll_success` に至る
- **SC-002**: EVENT 20 が duration 秒より後に届く実機 dump を再生したとき PAN を返す (ユニットテストで固定)
- **SC-003**: EVENT 22 が来ない場合も上限時間 + 2s 以内に空結果で抜け、 無限待ちしない

## Verify 結果 (2026-09-28)

- ユニットテスト `tests/unit/test_skscan_wait_event22.py` 5 件 pass、 `tests/unit` + `tests/integration` 全 610 件 pass
- 実機 deploy (03:01:09 bridge_start):
  - 03:01:14 `SKSCAN try mask=0FFFFFFF duration=6` → 18s で scan 完了、 PAN なし → `scan_retry`
  - 03:01:32 d=7 → 03:02:06 `SKSCAN found 1 PAN(s)` (34s)
  - 03:02:14 `wisun_joined` (PAN 0B41 / ch 0x33 / LQI 0x88)
  - 03:02:18 `poll_success` (1088 W) — 再起動から約 70 秒で **SC-001 達成**
  - `mqtt_bridge.stderr.log` 生成なし (uncaught 例外なし)
- d=6 では PAN を拾えず d=7 で拾う挙動は旧 bridge と同一。 d=6 の取りこぼしは電波条件側の話で本 spec の対象外

## Assumptions

- 1 ch あたり固定 overhead 1.0s は実測 0.63s/ch に対する余裕値。 別ファームや電波条件で 1 ch が 1s を超える場合は `SKSCAN_PER_CHANNEL_OVERHEAD_SEC` を見直す
- 上限時間が延びたことで、 メーター不在時の全 retry (d=6〜10) 1 周の最悪時間は約 4〜5 分になる (旧実装は約 40 秒)。 join 不能中はそもそも計測できないため許容する

## 関連参照

- spec 011 (SKSCAN retry)、 spec 034 (単 ch mask scan)、 spec 035 (cached SKJOIN 直行)、 spec 010 (EEDSCAN の `0FFFFFFF`)
- `docs/vendor/bp35a1-skstack-ip/bp35a1_command_reference_se_2014-12-25.pdf` (SKSCAN duration 定義)
