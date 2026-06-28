"""PARITY: cmd/import.go lines 31-357 — the export-job + download client.

POST /v3/export/bulk (create) and GET /v3/export/bulk/{id} (status) use set_http_header + HttpRetry; each export
file is fetched with a bare unauthenticated GET (presigned URL, no retry). The "table-export" tracker key holds a
JSON array of TableExportInfo ({Table, Timestamp} — capitalized, no json tags, RFC3339 timestamp) consumed by
fork.py and the importer's --dir path. DEVIATION (crdb-time-nanos): parse_crdb_export_file returns unix-nanos ints,
so timestamps are tracked as ints and formatted to RFC3339 only when building TableExportInfo.
"""

from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from eds.cmd.session import Transport, handle_api_error, set_http_header
from eds.util.crdb import parse_crdb_export_file
from eds.util.gojson import marshal
from eds.util.http import HttpRetry
from eds.util.logger import Logger

TRACKER_TABLE_EXPORT_KEY = "table-export"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _default_transport(method: str, url: str, headers: dict, data: Any = None) -> Any:
    import requests

    return requests.request(method, url, headers=headers, data=data, timeout=None)


def _default_download(url: str) -> Any:
    import requests

    return requests.get(url, timeout=None)


def _rfc3339(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        s += ("." + f"{dt.microsecond:06d}").rstrip("0")  # PARITY: Go RFC3339Nano trims trailing zeros
    return s + "Z"


_FRAC_RE = re.compile(r"^(.*T\d{2}:\d{2}:\d{2})\.(\d+)(.*)$")


def parse_rfc3339(s: str) -> datetime:
    """Parse an RFC3339 timestamp with ARBITRARY fractional precision (Go time.Parse accepts any; datetime.from
    isoformat on Python 3.10 needs 0/3/6 digits — normalize the fraction to 6 so trimmed values like ".5" parse)."""
    s = s.replace("Z", "+00:00")
    m = _FRAC_RE.match(s)
    if m:
        frac = (m.group(2) + "000000")[:6]
        s = f"{m.group(1)}.{frac}{m.group(3)}"
    return datetime.fromisoformat(s)


def _nanos_to_dt(unix_nanos: int) -> datetime:
    # via microseconds to stay exact at datetime's resolution (PARITY: Go keeps ns; Python is µs — crdb-time-nanos)
    return _EPOCH + timedelta(microseconds=unix_nanos // 1000)


def decode_api_response(resp: Any) -> dict:
    """PARITY: decodeAPIResponse[T] — the {success, message, data} envelope; data on success, else raise."""
    try:
        m = json.loads(resp.content)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"error decoding response: {e}") from e
    if not m.get("success", False):
        raise RuntimeError(f"api error: {m.get('message', '')}")
    return m.get("data") or {}


@dataclass
class ExportJobTableData:
    error: str = ""
    status: str = ""
    urls: list[str] = field(default_factory=list)
    cursor: str = ""


@dataclass
class ExportJobResponse:
    completed: bool = False
    tables: dict[str, ExportJobTableData] = field(default_factory=dict)

    @classmethod
    def from_data(cls, data: dict) -> ExportJobResponse:
        tables = {
            name: ExportJobTableData(
                error=td.get("error", ""), status=td.get("status", ""),
                urls=list(td.get("urls") or []), cursor=td.get("cursor", ""),
            )
            for name, td in (data.get("tables") or {}).items()
        }
        return cls(completed=bool(data.get("completed", False)), tables=tables)

    def get_progress(self) -> float:
        total = len(self.tables)
        if total == 0:
            return 0.0
        completed = sum(1 for td in self.tables.values() if td.status == "Completed")
        return completed / total

    def progress_string(self) -> str:
        completed = sum(1 for td in self.tables.values() if td.status == "Completed")
        total = len(self.tables)
        percent = 100 * completed / total if completed > 0 else 0.0
        return f"{completed}/{total} ({percent:.2f}%)"


@dataclass
class TableExportInfo:
    """PARITY: TableExportInfo — JSON keys are the capitalized Go field names; Timestamp is RFC3339."""

    table: str = ""
    timestamp: datetime = _EPOCH

    def __gojson__(self) -> str:
        return '{"Table":' + marshal(self.table) + ',"Timestamp":' + marshal(_rfc3339(self.timestamp)) + "}"


def marshal_table_export_info(infos: list[TableExportInfo]) -> str:
    """JSONStringify([]TableExportInfo) for the tracker (gojson can't marshal datetime, so each is pre-formatted)."""
    return "[" + ",".join(i.__gojson__() for i in infos) + "]"


def load_table_export_info(tracker: Any) -> list[TableExportInfo] | None:
    """PARITY: loadTableExportInfo (root.go:230) — the "table-export" key → [TableExportInfo] or None."""
    found, val = tracker.get_key(TRACKER_TABLE_EXPORT_KEY)
    if not found:
        return None
    out: list[TableExportInfo] = []
    for item in json.loads(val):
        ts = item.get("Timestamp")
        when = parse_rfc3339(str(ts)) if ts else _EPOCH
        out.append(TableExportInfo(table=item.get("Table", ""), timestamp=when))
    return out


def table_names(infos: list[TableExportInfo]) -> list[str]:
    return [i.table for i in infos]


def create_export_job(
    logger: Logger, api_url: str, api_key: str, *, tables: list[str] | None, company_ids: list[str] | None,
    location_ids: list[str] | None, time_offset_ms: int | None, version: str, transport: Transport | None = None,
) -> str:
    """PARITY: createExportJob — POST /v3/export/bulk → jobId."""
    transport = transport or _default_transport
    body_obj: dict[str, Any] = {}  # omitempty: drop empty/None fields
    if time_offset_ms is not None:
        body_obj["timeOffset"] = time_offset_ms
    if company_ids:
        body_obj["companyIds"] = company_ids
    if location_ids:
        body_obj["locationIds"] = location_ids
    if tables:
        body_obj["tables"] = tables
    data = marshal(body_obj).encode()
    url = f"{api_url}/v3/export/bulk"
    headers = set_http_header(api_key, version)
    resp: Any = HttpRetry(lambda: transport("POST", url, headers, data), method="POST", url=url, logger=logger).do()
    if resp.status_code != 200:
        raise handle_api_error(resp, "import")
    return decode_api_response(resp).get("jobId", "")


def check_export_job(
    logger: Logger, api_url: str, api_key: str, job_id: str, *, version: str, transport: Transport | None = None
) -> ExportJobResponse:
    """PARITY: checkExportJob — GET the job status; raises if any table failed."""
    transport = transport or _default_transport
    url = f"{api_url}/v3/export/bulk/{job_id}"
    headers = set_http_header(api_key, version)
    resp: Any = HttpRetry(lambda: transport("GET", url, headers, None), method="GET", url=url, logger=logger).do()
    if resp.status_code != 200:
        raise handle_api_error(resp, "import")
    job = ExportJobResponse.from_data(decode_api_response(resp))
    for table, td in job.tables.items():
        if td.status == "Failed":
            raise RuntimeError(f"error exporting table {table}: {td.error}")
    return job


def is_cancelled(cancel: Any) -> bool:
    return cancel is not None and cancel.is_set()


def poll_until_complete(
    logger: Logger, api_url: str, api_key: str, job_id: str, *, version: str, cancel: Any = None,
    transport: Transport | None = None, sleep: Any = None, now: Any = None,
) -> ExportJobResponse | None:
    """PARITY: pollUntilComplete — poll every 5s; log status at most once/minute. None on cancel."""
    sleep = sleep or time.sleep
    now = now or time.monotonic
    last_printed: float | None = None
    while True:
        show_progress = False
        if last_printed is None or now() - last_printed > 60:
            logger.info("Checking for Export Status (%s)", job_id)
            last_printed = now()
            show_progress = True
        job = check_export_job(logger, api_url, api_key, job_id, version=version, transport=transport)
        if job.completed:
            logger.info("Export Progress: %s", job.progress_string())
            return job
        logger.debug("Waiting for Export to Complete: %s", job.progress_string())
        if show_progress:
            logger.info("Export Progress: %s", job.progress_string())
        if is_cancelled(cancel):
            return None
        # PARITY: fixed 5s poll interval (cancellable)
        for _ in range(10):
            if is_cancelled(cancel):
                return None
            sleep(0.5)


def download_file(logger: Logger, directory: str, url: str, *, get: Any = None) -> int:
    """PARITY: downloadFile — bare GET (no auth/retry) of a presigned URL → write to dir/basename; return bytes."""
    get = get or _default_download
    base = os.path.basename(urlsplit(url).path)
    resp = get(url)
    if resp.status_code != 200:
        reason = getattr(resp, "reason", "") or ""
        logger.trace("error fetching data: %s, (url: %s)\n%s", f"{resp.status_code} {reason}", url, resp.content)
        raise RuntimeError(f"error fetching data: {resp.status_code} {reason}")
    filename = os.path.join(directory, base)
    with open(filename, "wb") as f:
        f.write(resp.content)
    n = len(resp.content)
    logger.debug("downloaded file %s (%d bytes)", filename, n)
    return n


def bulk_download_data(
    logger: Logger, data: dict[str, ExportJobTableData], directory: str, *, get: Any = None
) -> list[TableExportInfo]:
    """PARITY: bulkDownloadData — per-table final timestamp + 10-worker concurrent download."""
    started = time.monotonic()
    downloads: list[str] = []
    tables: list[TableExportInfo] = []
    for table, td in data.items():
        if not td.urls:
            logger.debug("no data for table %s, setting timestamp: %s", table, td.cursor)
            try:
                cursor_nanos = int(td.cursor)
            except ValueError as e:
                raise RuntimeError(f"error parsing timestamp value: {td.cursor}. {e}") from e
            tables.append(TableExportInfo(table=table, timestamp=_nanos_to_dt(cursor_nanos)))
            continue
        final_nanos = 0
        for full_url in td.urls:
            _, ts_nanos, ok = parse_crdb_export_file(urlsplit(full_url).path)
            if not ok:
                raise RuntimeError(f"unrecognized file path: {os.path.basename(urlsplit(full_url).path)}")
            final_nanos = max(final_nanos, ts_nanos)
            downloads.append(full_url)
        tables.append(TableExportInfo(table=table, timestamp=_nanos_to_dt(final_nanos)))

    if not downloads:
        logger.debug("no files to download")
        return []  # PARITY: Go discards the accumulated no-URL entries when there are zero files (import.go:288-291)

    total = len(downloads)
    total_bytes = 0
    completed = 0
    errors: list[Exception] = []

    def _worker(url: str) -> int:
        return download_file(logger, directory, url, get=get)

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_worker, u): u for u in downloads}
        for fut in futures:
            try:
                size = fut.result()
            except Exception as e:  # noqa: BLE001 — first error wins (PARITY)
                errors.append(e)
                continue
            total_bytes += size
            completed += 1
            logger.debug("download completed: %d/%d (%.2f%%)", completed, total, 100 * completed / total)
    if errors:
        raise RuntimeError(f"error downloading file: {errors[0]}")

    logger.info("Downloaded %d files (%d bytes) in %.1fs", total, total_bytes, time.monotonic() - started)
    return tables
