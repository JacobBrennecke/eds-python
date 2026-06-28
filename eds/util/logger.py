"""PARITY: go-common/logger — leveled console logger with chained prefixes + structured fields.

go-common's exact wire format (it uses fatih/color) is NOT in the repo, so — like the C# port — this is a
clean, readable equivalent: ``[ts ]LEVEL [prefix] message [k=v …]``. The behaviors that DO matter are
faithful: level ordering/filtering, prefix chaining, field merge, ``fatal`` logging then exiting, and
printf-style message formatting (Go's ``%s``/``%d``/``%v`` map to Python's ``%`` with ``%v``→``%s``).
DEVIATION: see DEVIATIONS.md#logger-format (clean equivalent; no ANSI colors; writes to stderr).
"""

from __future__ import annotations

import enum
import os
import sys
import threading
import time
from datetime import datetime
from typing import IO, Protocol, runtime_checkable


class LogLevel(enum.IntEnum):
    """PARITY: go-common logger levels (ordered)."""

    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4


_TAGS = {
    LogLevel.TRACE: "TRACE",
    LogLevel.DEBUG: "DEBUG",
    LogLevel.INFO: "INFO",
    LogLevel.WARN: "WARN",
    LogLevel.ERROR: "ERROR",
}


@runtime_checkable
class Logger(Protocol):
    """PARITY: logger.Logger interface. ``With`` is renamed ``with_fields`` (``with`` is a Python keyword)."""

    def trace(self, fmt: str, *args: object) -> None: ...
    def debug(self, fmt: str, *args: object) -> None: ...
    def info(self, fmt: str, *args: object) -> None: ...
    def warn(self, fmt: str, *args: object) -> None: ...
    def error(self, fmt: str, *args: object) -> None: ...
    def fatal(self, fmt: str, *args: object) -> None: ...
    def with_prefix(self, prefix: str) -> Logger: ...
    def with_fields(self, fields: dict[str, object]) -> Logger: ...


_WRITE_GATE = threading.Lock()


class ConsoleLogger:
    """PARITY: go-common console logger semantics (see module docstring)."""

    def __init__(
        self,
        min_level: LogLevel = LogLevel.INFO,
        *,
        prefix: str = "",
        fields: dict[str, object] | None = None,
        timestamps: bool = False,
        output: IO[str] | None = None,
        sink: LogFileSink | None = None,
    ) -> None:
        self._min = min_level
        self._prefix = prefix
        self._fields = fields
        self._timestamps = timestamps
        self._out: IO[str] = output if output is not None else sys.stderr
        self._sink = sink  # PARITY: the JSON-sink half of newLoggerWithSink's MultiLogger (captures all levels)

    def trace(self, fmt: str, *args: object) -> None:
        self._log(LogLevel.TRACE, fmt, args)

    def debug(self, fmt: str, *args: object) -> None:
        self._log(LogLevel.DEBUG, fmt, args)

    def info(self, fmt: str, *args: object) -> None:
        self._log(LogLevel.INFO, fmt, args)

    def warn(self, fmt: str, *args: object) -> None:
        self._log(LogLevel.WARN, fmt, args)

    def error(self, fmt: str, *args: object) -> None:
        self._log(LogLevel.ERROR, fmt, args)

    def fatal(self, fmt: str, *args: object) -> None:
        """PARITY: Fatal — log at error level, then exit(1)."""
        self._log(LogLevel.ERROR, fmt, args)
        sys.exit(1)

    def with_prefix(self, prefix: str) -> ConsoleLogger:
        """PARITY: WithPrefix — chain prefixes (e.g. "[fork]" then "[consumer]" -> "[fork][consumer]")."""
        combined = prefix if not self._prefix else self._prefix + prefix
        return ConsoleLogger(
            self._min, prefix=combined, fields=self._fields, timestamps=self._timestamps,
            output=self._out, sink=self._sink,
        )

    def with_fields(self, fields: dict[str, object]) -> ConsoleLogger:
        """PARITY: With — merge structured fields."""
        merged: dict[str, object] = dict(self._fields or {})
        merged.update(fields)
        return ConsoleLogger(
            self._min, prefix=self._prefix, fields=merged, timestamps=self._timestamps,
            output=self._out, sink=self._sink,
        )

    def with_sink(self, sink: LogFileSink | None) -> ConsoleLogger:
        """PARITY: newLoggerWithSink — tee every record (all levels) to a log-file sink alongside the console."""
        return ConsoleLogger(
            self._min, prefix=self._prefix, fields=self._fields, timestamps=self._timestamps,
            output=self._out, sink=sink,
        )

    def _build_line(self, level: LogLevel, message: str, with_ts: bool) -> str:
        parts: list[str] = []
        if with_ts:
            parts.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        parts.append(_TAGS[level])
        if self._prefix:
            parts.append(self._prefix)
        line = " ".join(parts) + " " + message
        if self._fields:
            line += "".join(f" {k}={v}" for k, v in self._fields.items())
        return line

    def _log(self, level: LogLevel, fmt: str, args: tuple[object, ...]) -> None:
        to_console = level >= self._min
        if not to_console and self._sink is None:
            return
        message = _format(fmt, args)
        if to_console:
            with _WRITE_GATE:
                print(self._build_line(level, message, self._timestamps), file=self._out, flush=True)
        if self._sink is not None:
            # PARITY: Go tees to a Trace-level sink (all records); the archived file always carries a timestamp.
            self._sink.write(self._build_line(level, message, True))


def _format(fmt: str, args: tuple[object, ...]) -> str:
    """printf-style formatting. Go ``%v`` → Python ``%s``; a malformed/literal-percent format with no
    matching args is emitted verbatim (mirrors the C# SafeFormat fallback)."""
    if not args:
        return fmt
    py_fmt = fmt.replace("%+v", "%s").replace("%v", "%s")
    try:
        return py_fmt % args
    except (TypeError, ValueError):
        return fmt


def new_console_logger(min_level: LogLevel = LogLevel.INFO, *, timestamps: bool = False) -> ConsoleLogger:
    return ConsoleLogger(min_level, timestamps=timestamps)


class LogFileSink:
    """PARITY: cmd/root.go logFileSink — append log lines to eds-<unixMilli>.log in a directory, with rotation.

    write() appends a line + "\n"; rotate() closes the current file, opens a fresh eds-<ms>.log, and returns the
    just-closed path (which the parent reads + uploads via /control/logfile); close() closes the current file.
    Thread-safe — the logger writes from multiple threads. DEVIATION: the file carries the clean-text logger format,
    not go-common JSON (see DEVIATIONS.md#logger-format)."""

    def __init__(self, log_dir: str) -> None:
        self._log_dir = log_dir
        self._lock = threading.Lock()
        self._f: IO[str] | None = None
        self.rotate()  # PARITY: newLogFileSink rotates once to create the first file

    def write(self, line: str) -> None:
        with self._lock:
            if self._f is None:
                return
            self._f.write(line + "\n")  # PARITY: Write appends the message then a newline
            self._f.flush()  # Go's os.File writes are unbuffered → survive a hard kill before close

    def close(self) -> None:
        with self._lock:
            if self._f is not None:
                self._f.close()
                self._f = None

    def rotate(self) -> str:
        """Close the current file and open a fresh eds-<ms>.log; return the just-closed path (empty on first call)."""
        with self._lock:
            old = ""
            if self._f is not None:
                old = self._f.name
                self._f.close()
                self._f = None
            os.makedirs(self._log_dir, exist_ok=True)  # PARITY: MkdirAll 0755
            path = os.path.join(self._log_dir, f"eds-{int(time.time() * 1000)}.log")
            self._f = open(path, "w", encoding="utf-8")  # noqa: SIM115 — held open for the sink's lifetime
            return old


def new_log_file_sink(log_dir: str) -> LogFileSink:
    """PARITY: newLogFileSink — create the sink and its first log file (raises OSError on failure)."""
    return LogFileSink(log_dir)
