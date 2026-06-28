"""PARITY: internal/util/util.go — CRDB changefeed export-file name parsing.

DEVIATION (crdb-time-nanos): Go returns a nanosecond-precise time.Time; Python's datetime is only
microsecond-precise, so parse_precise_date returns integer UNIX-NANOS directly (the only thing the importer
needs) — derive UnixMilli/UnixNano from it without a datetime round-trip, keeping mvccTimestamp byte-exact.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import NamedTuple

# PARITY: crdbExportFileRegex. DEVIATION: Go RE2 \d==[0-9], \w==[A-Za-z0-9_] (ASCII); Python \d/\w are Unicode,
# so use explicit ASCII classes. Groups: 1=33-digit precise date, 2=table, 3=schema/version id (unused).
_CRDB_EXPORT_FILE_RE = re.compile(
    r"^([0-9]{33})-[A-Za-z0-9_]+-[A-Za-z0-9_-]+-([a-z0-9_]+)-([A-Za-z0-9_]+)\.ndjson\.gz"
)


def parse_precise_date(date_str: str) -> tuple[int, bool]:
    """PARITY: parsePreciseDate — the 33-digit YYYYMMDDHHMMSS + 9-ns (+ 10 ignored) string → (unix_nanos, ok)."""
    if len(date_str) < 23:  # PARITY-guard: Go slices [:14]/[14:23] (would panic); Python would silently truncate
        return 0, False
    head = date_str[:14]  # YYYYMMDDHHMMSS
    nanos_str = date_str[14:23]  # 9-digit nanoseconds
    try:
        dt = datetime.strptime(head, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        whole_seconds = int(dt.timestamp())
        nanos = int(nanos_str)
    except ValueError:
        return 0, False
    return whole_seconds * 1_000_000_000 + nanos, True


class CrdbExportFile(NamedTuple):
    """PARITY: a parsed CRDB export filename. Unpacks positionally like the original 3-tuple."""

    table: str
    unix_nanos: int
    ok: bool


def parse_crdb_export_file(file: str) -> CrdbExportFile:
    """PARITY: ParseCRDBExportFile — (table, unix_nanos, ok). ok=False on no-match or a bad date."""
    filename = os.path.basename(file)  # PARITY: filepath.Base (OS-aware separators)
    m = _CRDB_EXPORT_FILE_RE.match(filename)
    if m is None:
        return CrdbExportFile("", 0, False)
    unix_nano, ok = parse_precise_date(m.group(1))
    if not ok:
        return CrdbExportFile("", 0, False)
    return CrdbExportFile(m.group(2), unix_nano, True)
