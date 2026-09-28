"""spec 054: SKSCAN は EVENT 22 (scan 完了) まで待つ。

旧実装は `deadline = now + duration` で duration code (6..10) を秒数として
扱っていたため、 実機 (EVER 1.5.2) で d=6 全 ch scan が ~17.7s かかり
EVENT 20 が 11.4s に届くケースで毎回取りこぼし `no PAN found` になっていた。
"""
import mqtt_bridge as mb


# 2026-09-28 実機 dump (`SKSCAN 2 0FFFFFFF 6 0`) の経過秒と行。
REAL_SCAN_DUMP = [
    (0.0, "SKSCAN 2 0FFFFFFF 6 0"),
    (0.0, "OK"),
    (11.4, "EVENT 20 FE80:0000:0000:0000:021D:1291:0000:ABB4 0"),
    (11.4, "EPANDESC"),
    (11.4, "  Channel:33"),
    (11.4, "  Channel Page:09"),
    (11.4, "  Pan ID:0B41"),
    (11.4, "  Addr:C0F9450040480B41"),
    (11.4, "  LQI:89"),
    (11.4, "  Side:0"),
    (11.4, "  PairID:010FE7D4"),
    (17.7, "EVENT 22 FE80:0000:0000:0000:021D:1291:0000:ABB4 0"),
]


class FakeSerial(object):
    """Replays (elapsed_sec, line) pairs against a fake clock."""

    def __init__(self, dump):
        self.dump = list(dump)
        self.now = 1000.0
        self.start = None
        self.written = []

    def time(self):
        return self.now

    def write(self, fd, data):
        self.written.append(data)
        self.start = self.now

    def readline(self, fd, timeout=10):
        if self.start is not None and self.dump:
            at, line = self.dump[0]
            if self.start + at <= self.now + timeout:
                self.now = max(self.now, self.start + at)
                self.dump.pop(0)
                return line
        self.now += timeout
        return None


def _install(monkeypatch, fake):
    monkeypatch.setattr(mb.time, "time", fake.time)
    monkeypatch.setattr(mb, "serial_write", fake.write)
    monkeypatch.setattr(mb, "serial_readline", fake.readline)
    monkeypatch.setattr(mb.termios, "tcflush", lambda fd, q: None)


def test_skscan_finds_pan_that_answers_after_duration_seconds(monkeypatch):
    """EVENT 20 が 11.4s (> duration=6) に届いても拾えること (実機回帰)."""
    fake = FakeSerial(REAL_SCAN_DUMP)
    _install(monkeypatch, fake)

    pan = mb.skscan(fd=None, duration=6, max_retries=1)

    assert pan["Channel"] == "33"
    assert pan["Pan ID"] == "0B41"
    assert pan["Addr"] == "C0F9450040480B41"
    assert len(fake.written) == 1  # 1 回の scan で完結、 重ねて SKSCAN を送らない


def test_skscan_default_mask_covers_only_bp35cx_channels(monkeypatch):
    """全 ch scan は EEDSCAN と同じ ch33-60 (= 0FFFFFFF) を指定する."""
    fake = FakeSerial(REAL_SCAN_DUMP)
    _install(monkeypatch, fake)

    mb.skscan(fd=None, duration=6, max_retries=1)

    assert mb.FULL_SCAN_CHANNEL_MASK == "0FFFFFFF"
    assert fake.written[0] == "SKSCAN 2 0FFFFFFF 6 0\r\n"


def test_skscan_gives_up_when_event22_never_arrives(monkeypatch):
    """EVENT 22 が来なくても timeout で抜けて空 dict を返す (無限待ちしない)."""
    fake = FakeSerial([(0.0, "OK")])
    _install(monkeypatch, fake)

    pan = mb.skscan(fd=None, duration=6, max_retries=1)

    assert pan == {}
    assert fake.now - 1000.0 <= mb.compute_skscan_timeout("0FFFFFFF", 6) + 2


def test_compute_skscan_timeout_covers_measured_full_scan():
    """実測 d=6 全 ch (28ch) = 17.7s を十分カバーする."""
    assert mb.compute_skscan_timeout("0FFFFFFF", 6) > 17.7 * 1.5


def test_compute_skscan_timeout_grows_with_duration_and_channels():
    full = [mb.compute_skscan_timeout("0FFFFFFF", d) for d in range(3, 11)]
    assert full == sorted(full)
    single = mb.compute_skscan_timeout("{:08X}".format(mb.channel_to_mask(33)), 3)
    assert single < mb.compute_skscan_timeout("0FFFFFFF", 3)
    assert single > 3  # 単 ch でも旧 deadline (= 3s) より余裕を持つ
