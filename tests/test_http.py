"""PARITY: internal/util/http.go — HttpRetry. §8.8: unbounded retry on 408/429/502/503/504
(NOT 500), connection errors bounded by timeout."""

from __future__ import annotations

import pytest

from eds.util.http import HttpRetry


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _ZeroRng:
    def randrange(self, _n: int) -> int:
        return 0


def _retry(send, **kw) -> HttpRetry:
    return HttpRetry(send, sleep=lambda _s: None, rng=_ZeroRng(), **kw)


def test_retries_5xx_until_success() -> None:  # §8.8
    calls = {"n": 0}

    def send() -> _Resp:
        calls["n"] += 1
        return _Resp(503 if calls["n"] <= 4 else 200)

    resp = _retry(send).do()
    assert resp.status_code == 200
    assert calls["n"] == 5  # 4 retries past the first attempt -> no small cap


def test_no_retry_on_success() -> None:
    calls = {"n": 0}

    def send() -> _Resp:
        calls["n"] += 1
        return _Resp(200)

    assert _retry(send).do().status_code == 200
    assert calls["n"] == 1


@pytest.mark.parametrize("code", [408, 429, 502, 503, 504])
def test_retryable_status_codes(code: int) -> None:
    calls = {"n": 0}

    def send() -> _Resp:
        calls["n"] += 1
        return _Resp(code if calls["n"] == 1 else 200)

    assert _retry(send).do().status_code == 200
    assert calls["n"] == 2


@pytest.mark.parametrize("code", [200, 400, 404, 500, 501])
def test_non_retryable_status_codes(code: int) -> None:
    # PARITY: 500 (InternalServerError) is NOT in the retry set — only 408/429/502/503/504.
    calls = {"n": 0}

    def send() -> _Resp:
        calls["n"] += 1
        return _Resp(code)

    assert _retry(send).do().status_code == code
    assert calls["n"] == 1


def test_connection_error_retries_within_timeout() -> None:
    clock = {"t": 0.0}
    calls = {"n": 0}

    def now() -> float:
        return clock["t"]

    def send() -> _Resp:
        calls["n"] += 1
        clock["t"] += 5.0
        if calls["n"] <= 2:
            raise ConnectionError("connection refused")
        return _Resp(200)

    resp = _retry(send, timeout=30.0, now=now).do()
    assert resp.status_code == 200
    assert calls["n"] == 3


def test_connection_error_stops_after_timeout() -> None:
    clock = {"t": 0.0}
    calls = {"n": 0}

    def now() -> float:
        return clock["t"]

    def send() -> _Resp:
        calls["n"] += 1
        clock["t"] += 20.0  # exceeds the 30s window after the 2nd attempt
        raise ConnectionError("connection reset by peer")

    with pytest.raises(ConnectionError):
        _retry(send, timeout=30.0, now=now).do()
    assert calls["n"] == 2


def test_non_connection_error_not_retried() -> None:
    def send() -> _Resp:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _retry(send).do()
