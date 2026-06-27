"""PARITY: go-common/logger — leveled console logger with chained prefixes + structured fields.

go-common's exact wire format (it uses fatih/color) is NOT in the repo, so — like the C# port — this is a
clean, readable equivalent: ``[ts ]LEVEL [prefix] message [k=v …]``. The behaviors that DO matter are
faithful: level ordering/filtering, prefix chaining, field merge, ``fatal`` logging then exiting, and
printf-style message formatting (Go's ``%s``/``%d``/``%v`` map to Python's ``%`` with ``%v``→``%s``).
DEVIATION: see DEVIATIONS.md#logger-format (clean equivalent; no ANSI colors; writes to stderr).
"""

from __future__ import annotations

import enum
import sys
import threading
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
    ) -> None:
        self._min = min_level
        self._prefix = prefix
        self._fields = fields
        self._timestamps = timestamps
        self._out: IO[str] = output if output is not None else sys.stderr

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
            self._min, prefix=combined, fields=self._fields, timestamps=self._timestamps, output=self._out
        )

    def with_fields(self, fields: dict[str, object]) -> ConsoleLogger:
        """PARITY: With — merge structured fields."""
        merged: dict[str, object] = dict(self._fields or {})
        merged.update(fields)
        return ConsoleLogger(
            self._min, prefix=self._prefix, fields=merged, timestamps=self._timestamps, output=self._out
        )

    def _log(self, level: LogLevel, fmt: str, args: tuple[object, ...]) -> None:
        if level < self._min:
            return
        message = _format(fmt, args)
        parts: list[str] = []
        if self._timestamps:
            parts.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        parts.append(_TAGS[level])
        if self._prefix:
            parts.append(self._prefix)
        line = " ".join(parts) + " " + message
        if self._fields:
            line += "".join(f" {k}={v}" for k, v in self._fields.items())
        with _WRITE_GATE:
            print(line, file=self._out, flush=True)


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
