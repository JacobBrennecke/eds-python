"""PARITY: runner session lifecycle + HTTP helpers (against an injected fake transport)."""

from __future__ import annotations

import base64

import pytest

from eds.cmd.session import (
    AlreadyRunningError,
    get_remaining_log,
    get_request_id,
    handle_api_error,
    send_end,
    send_end_and_upload,
    send_start,
    set_http_header,
    upload_log_file,
    write_creds_to_file,
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
    def __init__(self, status_code: int, body: str = "", headers: dict | None = None) -> None:
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    @property
    def text(self) -> str:
        return self._body

    @property
    def content(self) -> bytes:
        return self._body.encode()


class _Transport:
    def __init__(self, *resps: _Resp) -> None:
        self._resps = list(resps)
        self.calls: list = []

    def __call__(self, method, url, headers, data=None):
        self.calls.append((method, url, headers, data))
        return self._resps.pop(0) if len(self._resps) > 1 else self._resps[0]


def test_set_http_header() -> None:
    h = set_http_header("k", "1.2.3")
    assert h["Content-Type"] == "application/json"
    assert h["User-Agent"] == "Shopmonkey EDS Server/1.2.3"
    assert h["Authorization"] == "Bearer k"
    assert "Authorization" not in set_http_header("", "1.2.3")  # no key → no auth (used for presigned PUT)


def test_get_request_id_and_handle_api_error() -> None:
    resp = _Resp(500, '{"message":"boom"}', {"X-Request-Id": "req-9"})
    assert get_request_id(resp) == "req-9"
    err = handle_api_error(resp, "session start")
    assert "session start" in str(err) and "boom" in str(err) and "req-9" in str(err)
    # non-JSON body falls back to raw + status code
    err2 = handle_api_error(_Resp(503, "upstream down"), "session end")
    assert "upstream down" in str(err2) and "status code=503" in str(err2)


def test_write_creds_to_file(tmp_path) -> None:
    raw = b"-----BEGIN NATS USER JWT-----\nabc\n"
    path = tmp_path / "nats.creds"
    write_creds_to_file(base64.b64encode(raw).decode(), str(path))
    assert path.read_bytes() == raw
    with pytest.raises(RuntimeError, match="failed to decode base64"):
        write_creds_to_file("!!!not base64!!!", str(tmp_path / "bad.creds"))


def test_send_start_success() -> None:
    body = '{"success":true,"message":"","data":{"sessionId":"s1","credential":"Y3JlZHM="}}'
    t = _Transport(_Resp(200, body))
    s = send_start(_QuietLogger(), "https://api", "key", "", "srv1", None, version="1.0", transport=t)
    assert s.session_id == "s1" and s.credential == "Y3JlZHM="
    method, url, headers, data = t.calls[0]
    assert method == "POST" and url == "https://api/v3/eds/internal"
    assert headers["Authorization"] == "Bearer key"


def test_send_start_409_raises_already_running() -> None:
    t = _Transport(_Resp(409, ""))
    with pytest.raises(AlreadyRunningError):
        send_start(_QuietLogger(), "https://api", "key", "", "srv1", None, version="1.0", transport=t)


def test_send_start_error_status() -> None:
    t = _Transport(_Resp(500, '{"message":"nope"}'))
    with pytest.raises(RuntimeError, match="session start"):
        send_start(_QuietLogger(), "https://api", "key", "", "srv1", None, version="1.0", transport=t)


def test_send_end_returns_urls() -> None:
    body = '{"success":true,"message":"","data":{"url":"https://up/main","errorUrl":"https://up/err"}}'
    t = _Transport(_Resp(200, body))
    urls = send_end(_QuietLogger(), "https://api", "key", "s1", True, version="1.0", transport=t)
    assert urls.url == "https://up/main" and urls.error_url == "https://up/err"
    method, url, _, _ = t.calls[0]
    assert method == "POST" and url == "https://api/v3/eds/internal/s1"


def test_get_remaining_log(tmp_path) -> None:
    (tmp_path / "eds-100.log").write_text("a", encoding="utf-8")
    (tmp_path / "eds-200.log").write_text("b", encoding="utf-8")
    (tmp_path / "server_stderr.txt").write_text("x", encoding="utf-8")
    last = get_remaining_log(str(tmp_path))
    assert last.endswith("eds-200.log")  # last sorted *.log


def test_upload_log_file_gzips_and_puts(tmp_path) -> None:
    log = tmp_path / "eds-1.log"
    log.write_text("hello logs", encoding="utf-8")
    t = _Transport(_Resp(200, ""))
    path = upload_log_file(_QuietLogger(), "https://up/bucket/obj?sig=1", str(log), version="1.0", transport=t)
    assert path == "/bucket/obj"  # url path component
    method, _, headers, data = t.calls[0]
    assert method == "PUT" and headers["Content-Type"] == "application/x-tgz"
    assert "Authorization" not in headers  # presigned → no auth
    assert isinstance(data, bytes) and len(data) > 0
    assert not (tmp_path / "eds-1.log.gz").exists()  # temp .gz cleaned up


def test_send_end_and_upload_puts_main_and_stderr(tmp_path) -> None:
    log = tmp_path / "eds-1.log"
    log.write_text("main", encoding="utf-8")
    stderr = tmp_path / "server_stderr.txt"
    stderr.write_text("err", encoding="utf-8")
    end_body = '{"success":true,"message":"","data":{"url":"https://up/main","errorUrl":"https://up/err"}}'
    t = _Transport(_Resp(200, end_body), _Resp(200, ""), _Resp(200, ""))
    p = send_end_and_upload(
        _QuietLogger(), "https://api", "k", "s1", True, str(log), str(stderr), version="1.0", transport=t
    )
    assert p == "/main"
    # 1 POST (sendEnd) + 2 PUTs (main log + stderr) — Python follows Go and uploads the ErrorURL branch
    methods = [c[0] for c in t.calls]
    assert methods == ["POST", "PUT", "PUT"]
