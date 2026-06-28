"""PARITY: the enroll command (code → api key, write config.toml)."""

from __future__ import annotations

import argparse

from eds.cmd.config import load_config
from eds.cmd.enroll import run_enroll
from eds.cmd.exit_codes import EXIT_ERROR, EXIT_SUCCESS


class _Resp:
    def __init__(self, status_code: int, body: str = "") -> None:
        self.status_code = status_code
        self._body = body
        self.headers: dict = {}

    @property
    def content(self) -> bytes:
        return self._body.encode()

    @property
    def text(self) -> str:
        return self._body


def _ns(tmp_path, code: str, api_url: str = "https://api") -> argparse.Namespace:
    return argparse.Namespace(
        code=code, api_url=api_url, data_dir=str(tmp_path),
        silent=True, verbose=False, timestamp=False, log_label="",
    )


def test_enroll_success_writes_config(tmp_path) -> None:
    body = '{"success":true,"message":"","data":{"token":"tok-1","serverId":"srv-9"}}'
    transport = lambda m, u, h, d=None: _Resp(200, body)  # noqa: E731
    rc = run_enroll(_ns(tmp_path, "P12345"), transport=transport)
    assert rc == EXIT_SUCCESS
    c = load_config(str(tmp_path))
    assert c.get_string("token") == "tok-1" and c.get_string("server_id") == "srv-9"


def test_enroll_404_invalid_code(tmp_path) -> None:
    transport = lambda m, u, h, d=None: _Resp(404, "")  # noqa: E731
    assert run_enroll(_ns(tmp_path, "BADCODE"), transport=transport) == EXIT_ERROR


def test_enroll_derives_api_url_from_code_prefix(tmp_path) -> None:
    captured: dict = {}

    def transport(method, url, headers, data=None):
        captured["url"] = url
        return _Resp(200, '{"success":true,"data":{"token":"t","serverId":"s"}}')

    run_enroll(_ns(tmp_path, "L9code", api_url=""), transport=transport)  # "L" → http://localhost:3101
    assert captured["url"] == "http://localhost:3101/v3/eds/internal/enroll/L9code"


def test_enroll_unsuccessful_response(tmp_path) -> None:
    transport = lambda m, u, h, d=None: _Resp(200, '{"success":false,"message":"nope"}')  # noqa: E731
    assert run_enroll(_ns(tmp_path, "P1"), transport=transport) == EXIT_ERROR


def test_enroll_transport_error_returns_exit_error(tmp_path) -> None:
    # PARITY: Go logger.Fatal("failed to enroll server") → exit 1, NOT an uncaught panic → exit 2.
    def transport(method, url, headers, data=None):
        raise RuntimeError("dns failure")  # non-retryable → HttpRetry re-raises immediately

    assert run_enroll(_ns(tmp_path, "P1"), transport=transport) == EXIT_ERROR


def test_enroll_malformed_response_returns_exit_error(tmp_path) -> None:
    # PARITY: Go logger.Fatal("failed to decode response") → exit 1, NOT a panic → exit 2.
    transport = lambda m, u, h, d=None: _Resp(200, "not json at all")  # noqa: E731
    assert run_enroll(_ns(tmp_path, "P1"), transport=transport) == EXIT_ERROR
