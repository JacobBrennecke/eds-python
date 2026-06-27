"""PARITY: go-common logger semantics (mirrors the C# LoggingTests)."""

from __future__ import annotations

import io

import pytest

from eds.util.logger import ConsoleLogger, LogLevel


def _logger(level: LogLevel = LogLevel.TRACE) -> tuple[ConsoleLogger, io.StringIO]:
    buf = io.StringIO()
    return ConsoleLogger(level, output=buf), buf


def test_filters_below_min_level() -> None:
    log, buf = _logger(LogLevel.INFO)
    log.debug("hidden")
    log.info("shown")
    out = buf.getvalue()
    assert "hidden" not in out
    assert "shown" in out


def test_applies_prefix_and_format() -> None:
    log, buf = _logger()
    log.with_prefix("[file]").info("processed %d rows", 5)
    line = buf.getvalue().strip()
    assert "[file]" in line
    assert "processed 5 rows" in line
    assert "INFO" in line


def test_chained_prefix() -> None:
    log, buf = _logger()
    log.with_prefix("[fork]").with_prefix("[consumer]").info("hi")
    assert "[fork][consumer]" in buf.getvalue()


def test_literal_percent_no_args_emitted_verbatim() -> None:
    log, buf = _logger()
    log.info("offending sql: SELECT 100%done")  # no args -> verbatim, no formatting error
    assert "SELECT 100%done" in buf.getvalue()


def test_v_verb_is_translated() -> None:
    log, buf = _logger()
    log.info("got %v", {"a": 1})
    assert "got {'a': 1}" in buf.getvalue()


def test_with_fields() -> None:
    log, buf = _logger()
    log.with_fields({"table": "user"}).info("done")
    out = buf.getvalue()
    assert "done" in out
    assert "table=user" in out


def test_fatal_logs_then_exits() -> None:
    log, buf = _logger()
    with pytest.raises(SystemExit) as ei:
        log.fatal("boom")
    assert ei.value.code == 1
    assert "boom" in buf.getvalue()
