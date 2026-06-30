"""Shared log-duration rendering (mirrors the C# port's ``ImportDuration.Dur``).

DEVIATION (duration-format): log-only, not byte-checked; Go renders ``time.Duration.String()`` (the COMPACT
form, e.g. ``100ms`` / ``1.5s``). This is the single source for every import log line's ``<dur>`` — keeping the
terminal ``👋 Loaded N tables in <dur>`` line consistent with the per-file/per-table lines, Go, and the C# twin.
"""

from __future__ import annotations


def format_duration(seconds: float) -> str:
    """Render an elapsed-seconds float as Go's compact ``time.Duration.String()`` shape."""
    if seconds < 1e-3:
        return f"{seconds * 1e6:.3f}µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:.3f}ms"
    return f"{seconds:.3f}s"
