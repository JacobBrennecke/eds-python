"""PARITY: cmd/server.go collectCommandArgs/serverIgnoreFlags + cmd/root.go getOSInt + Go duration parsing.

The flag-forwarding filter is deliberately string-level (operates on raw argv, not a parsed namespace), and the
skip-next quirk (an ignored flag always drops the following token, even for booleans / `--foo=bar`) is preserved
verbatim from Go.
"""

from __future__ import annotations

import os
import re

# PARITY: serverIgnoreFlags (server.go:273-287) — flags the wrapper/server consumes itself and must NOT forward
# to the fork child.
SERVER_IGNORE_FLAGS = frozenset(
    {
        "--api-url",
        "--api-key",
        "--eds-id",
        "--silent",
        "--port",
        "--health-port",
        "--renew-interval",
        "--wrapper",
        "--parent",
        "--url",
        "--server",
        "--keep-logs",
        "--no-restart",
        # FEATURE(audit-mode): the server resolves --mode (flag/config/default) and forwards the RESOLVED value
        # to the fork EXPLICITLY, so drop any user-supplied --mode here to avoid duplicating it on the fork args.
        "--mode",
    }
)


def collect_command_args(args: list[str]) -> list[str]:
    """PARITY: collectCommandArgs (server.go:289-306) — filter raw argv, dropping serverIgnoreFlags + the token
    that follows each (faithful skip-next quirk). ``args`` is os.Args[2:] (after the program + subcommand)."""
    out: list[str] = []
    skipping = False
    for arg in args:
        if skipping:
            skipping = False
            continue
        tok = arg.split("=")[0]
        if tok in SERVER_IGNORE_FLAGS:
            skipping = True
            continue
        out.append(arg)
    return out


def get_os_int(name: str, default: int) -> int:
    """PARITY: getOSInt (root.go:79-89) — read env var as int, fall back to default on missing/parse error."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


_DUR_RE = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
_DUR_UNITS = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(s: str) -> float:
    """PARITY: Go time.ParseDuration → seconds (float). Accepts e.g. '500ms', '2s', '1m', '24h', '1h30m'."""
    s = s.strip()
    if s in ("0", ""):
        return 0.0
    neg = False
    if s[0] in "+-":
        neg = s[0] == "-"
        s = s[1:]
    total = 0.0
    pos = 0
    for m in _DUR_RE.finditer(s):
        if m.start() != pos:
            raise ValueError(f"invalid duration {s!r}")
        total += float(m.group(1)) * _DUR_UNITS[m.group(2)]
        pos = m.end()
    if pos != len(s):
        raise ValueError(f"invalid duration {s!r}")
    return -total if neg else total
