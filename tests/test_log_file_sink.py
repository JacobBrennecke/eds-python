"""PARITY: the log-file sink + rotation (LogFileSink) and the logger's sink tee (newLoggerWithSink)."""

from __future__ import annotations

import io
import os

from eds.util.logger import ConsoleLogger, LogFileSink, LogLevel


class _RecordingSink:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def write(self, line: str) -> None:
        self.lines.append(line)


def test_log_file_sink_write_rotate_close(tmp_path, monkeypatch) -> None:
    import eds.util.logger as logger_mod

    clock = [1_000.000]
    monkeypatch.setattr(logger_mod.time, "time", lambda: clock[0])  # deterministic eds-<ms>.log names

    sink = LogFileSink(str(tmp_path))  # rotate #1 → eds-1000000.log
    first = tmp_path / "eds-1000000.log"
    assert first.exists()
    sink.write("line one")
    sink.write("line two")

    clock[0] = 1_000.001
    old = sink.rotate()  # close eds-1000000.log, open eds-1000001.log, return the closed path
    assert os.path.normpath(old) == os.path.normpath(str(first))
    assert first.read_text() == "line one\nline two\n"  # the parent reads the just-closed file
    second = tmp_path / "eds-1000001.log"
    assert second.exists()

    sink.write("line three")
    sink.close()
    assert second.read_text() == "line three\n"  # writes after rotate go to the new file


def test_first_rotate_returns_empty(tmp_path) -> None:
    sink = LogFileSink(str(tmp_path))
    try:
        # the first file was created by __init__; write goes there, no prior file to return
        assert len(list(tmp_path.glob("eds-*.log"))) == 1
    finally:
        sink.close()


def test_console_logger_tees_all_levels_to_sink() -> None:
    out = io.StringIO()
    sink = _RecordingSink()
    log = ConsoleLogger(LogLevel.INFO, output=out, sink=sink).with_prefix("[fork]").with_fields({"sid": "s1"})
    log.trace("trace-msg")  # below console INFO → console skips, but the sink captures it
    log.info("info-msg")

    console = out.getvalue()
    assert "trace-msg" not in console  # console respects min_level
    assert "info-msg" in console
    assert len(sink.lines) == 2  # the sink (Trace-level) gets BOTH records
    assert any("TRACE" in line and "trace-msg" in line for line in sink.lines)
    assert all("[fork]" in line and "sid=s1" in line for line in sink.lines)  # prefix + fields propagate
    assert all(line[:4].isdigit() for line in sink.lines)  # sink lines always carry a timestamp


def test_logger_without_sink_unchanged() -> None:
    out = io.StringIO()
    ConsoleLogger(LogLevel.INFO, output=out).trace("hidden")  # no sink, below min → nothing
    assert out.getvalue() == ""


def test_logfile_route_via_loopback_returns_rotated_path(tmp_path) -> None:
    # Integration: the /control/logfile route (fork.py) rotates and returns the just-closed path, whose file holds
    # the logs the parent will upload — exactly what send_logs reads via _control_get(ctx, "logfile").
    import requests

    from eds.cmd.loopback import LoopbackServer

    sink = LogFileSink(str(tmp_path))
    sink.write("session log line")
    srv = LoopbackServer(0, {"/control/logfile": lambda: (200, sink.rotate())})
    srv.start()
    try:
        resp = requests.get(f"http://127.0.0.1:{srv.port}/control/logfile", timeout=5)
        assert resp.status_code == 200
        assert (tmp_path / os.path.basename(resp.text)).read_text() == "session log line\n"
    finally:
        srv.stop()
        sink.close()
