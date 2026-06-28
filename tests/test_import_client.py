"""PARITY: the export-job + download client (create/check/poll/download/bulk + DTOs)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from eds.cmd.import_client import (
    ExportJobResponse,
    ExportJobTableData,
    TableExportInfo,
    bulk_download_data,
    check_export_job,
    create_export_job,
    decode_api_response,
    download_file,
    load_table_export_info,
    marshal_table_export_info,
    poll_until_complete,
    table_names,
)


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _Resp:
    def __init__(self, status_code, body="", reason="") -> None:
        self.status_code = status_code
        self._body = body
        self.reason = reason

    @property
    def content(self):
        return self._body.encode() if isinstance(self._body, str) else self._body

    @property
    def text(self):
        return self._body if isinstance(self._body, str) else self._body.decode()


class _Transport:
    def __init__(self, *resps) -> None:
        self._resps = list(resps)
        self.calls: list = []

    def __call__(self, method, url, headers, data=None):
        self.calls.append((method, url, data))
        return self._resps.pop(0) if len(self._resps) > 1 else self._resps[0]


LOG = _QuietLogger()


def test_decode_api_response() -> None:
    assert decode_api_response(_Resp(200, '{"success":true,"data":{"x":1}}')) == {"x": 1}
    with pytest.raises(RuntimeError, match="api error: nope"):
        decode_api_response(_Resp(200, '{"success":false,"message":"nope"}'))


def test_create_export_job_omitempty_body() -> None:
    t = _Transport(_Resp(200, '{"success":true,"data":{"jobId":"job-1"}}'))
    jid = create_export_job(
        LOG, "https://api", "k", tables=["a"], company_ids=None, location_ids=[],
        time_offset_ms=1700, version="1", transport=t,
    )
    assert jid == "job-1"
    method, url, data = t.calls[0]
    assert method == "POST" and url == "https://api/v3/export/bulk"
    body = json.loads(data)
    assert body == {"tables": ["a"], "timeOffset": 1700}  # empty/None fields omitted


def test_check_export_job_failed_table_raises() -> None:
    body = '{"success":true,"data":{"completed":false,"tables":{"o":{"status":"Failed","error":"boom"}}}}'
    with pytest.raises(RuntimeError, match="error exporting table o: boom"):
        check_export_job(LOG, "https://api", "k", "job-1", version="1", transport=_Transport(_Resp(200, body)))


def test_poll_until_complete() -> None:
    pending = '{"success":true,"data":{"completed":false,"tables":{"u":{"status":"Pending"}}}}'
    done = '{"success":true,"data":{"completed":true,"tables":{"u":{"status":"Completed"}}}}'
    t = _Transport(_Resp(200, pending), _Resp(200, done))
    job = poll_until_complete(LOG, "https://api", "k", "job-1", version="1", transport=t,
                              sleep=lambda s: None, now=lambda: 0.0)
    assert job is not None and job.completed is True


def test_poll_until_complete_cancel() -> None:
    pending = '{"success":true,"data":{"completed":false,"tables":{"u":{"status":"Pending"}}}}'

    class _Cancel:
        def is_set(self):
            return True

    t = _Transport(_Resp(200, pending))
    job = poll_until_complete(LOG, "https://api", "k", "j", version="1", cancel=_Cancel(),
                              sleep=lambda s: None, now=lambda: 0.0, transport=t)
    assert job is None  # cancelled → None


def test_progress_string_and_get_progress() -> None:
    job = ExportJobResponse(tables={
        "a": ExportJobTableData(status="Completed"),
        "b": ExportJobTableData(status="Pending"),
    })
    assert job.get_progress() == 0.5
    assert job.progress_string() == "1/2 (50.00%)"
    assert ExportJobResponse().progress_string() == "0/0 (0.00%)"


def test_download_file(tmp_path) -> None:
    written = download_file(LOG, str(tmp_path), "https://s3/bucket/0001.ndjson.gz?sig=x",
                            get=lambda url: _Resp(200, b"\x1f\x8bdata"))
    assert written == len(b"\x1f\x8bdata")
    assert (tmp_path / "0001.ndjson.gz").read_bytes() == b"\x1f\x8bdata"  # name from path, query dropped


def test_download_file_error() -> None:
    with pytest.raises(RuntimeError, match="error fetching data: 403"):
        download_file(LOG, ".", "https://s3/x.gz", get=lambda url: _Resp(403, b"", reason="Forbidden"))


def test_bulk_download_data_no_urls_uses_cursor(tmp_path) -> None:
    from datetime import timedelta

    # a table with no URLs records a cutoff from the nanos cursor; one with URLs downloads + dates from the file
    cursor_nanos = 1_700_000_000_000_000_000  # 2023-11-14T...Z
    # a VALID 33-char CRDB precise date prefix (YYYYMMDDHHMMSS + 9 nanos + 10 ignored) for 2026-01-01 12:00:00
    fname = "20260101120000" + "0" * 19 + "-x-y-orders-public.ndjson.gz"
    data = {
        "empty": ExportJobTableData(status="Completed", cursor=str(cursor_nanos)),
        "orders": ExportJobTableData(status="Completed", urls=[f"https://s3/{fname}"]),
    }
    infos = bulk_download_data(LOG, data, str(tmp_path), get=lambda url: _Resp(200, b"gz"))
    by_table = {i.table: i for i in infos}
    expected = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=cursor_nanos // 1000)
    assert by_table["empty"].timestamp == expected
    assert (tmp_path / fname).exists()  # the orders file was downloaded
    assert by_table["orders"].timestamp.year == 2026


def test_table_export_info_round_trip() -> None:
    infos = [TableExportInfo(table="user", timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))]
    payload = marshal_table_export_info(infos)
    assert json.loads(payload) == [{"Table": "user", "Timestamp": "2026-01-02T03:04:05Z"}]

    class _Tracker:
        def get_key(self, key):
            return (True, payload)

    loaded = load_table_export_info(_Tracker())
    assert loaded[0].table == "user"
    assert loaded[0].timestamp == datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert table_names(loaded) == ["user"]
