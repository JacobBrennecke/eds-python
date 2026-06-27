"""PARITY: internal/util/http.go — retrying HTTP helper.

Retries are UNBOUNDED on 408/429/502/503/504 (SPEC §8.8) and bounded by ``timeout`` on connection
reset/refused; jittered backoff of ``100ms + rand[0, 500*attempts)ms``. The actual send is injectable
(``send``) so the retry logic is unit-testable and the real ``requests`` backend is only needed at M3.
DEVIATION: connection-error classification is by message substring on the Python/OS exception (not Go's
runtime strings) — see DEVIATIONS.md#http-conn-error-detection. The loop is iterative (Go recurses);
behavior-neutral, but avoids Python's recursion limit on long unbounded retries.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

# PARITY: http.StatusRequestTimeout / TooManyRequests / BadGateway / ServiceUnavailable / GatewayTimeout.
_RETRY_STATUS = frozenset({408, 429, 502, 503, 504})

DEFAULT_TIMEOUT = 30.0


class HttpRetry:
    def __init__(
        self,
        send: Callable[[], object],
        *,
        method: str = "GET",
        url: str = "",
        timeout: float = DEFAULT_TIMEOUT,
        logger: Any = None,  # duck-typed EDS logger (Logger protocol arrives at M2)
        sleep: Callable[[float], None] | None = None,
        now: Callable[[], float] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        # send() rebuilds + performs the request, returning a response with a ``status_code`` int.
        self._send = send
        self._method = method
        self._url = url
        self._timeout = timeout
        self._logger = logger
        self._sleep = sleep if sleep is not None else time.sleep
        self._now = now if now is not None else time.monotonic
        self._rng = rng if rng is not None else random
        self._attempts = 0

    def do(self) -> object:
        """PARITY: HTTPRetry.Do — send, retrying per shouldRetry with jittered backoff."""
        started = self._now()
        while True:
            self._attempts += 1
            resp: object = None
            err: Exception | None = None
            try:
                resp = self._send()
            except Exception as e:  # noqa: BLE001 - mirror Go's (resp, err) handling
                err = e
            if not self._should_retry(resp, err, started):
                if err is not None:
                    raise err
                return resp
            jitter = (100 + self._rng.randrange(500 * self._attempts)) / 1000.0
            if self._logger is not None:
                code = getattr(resp, "status_code", 0) if resp is not None else 0
                self._logger.trace(
                    "%s request failed (path: %s) (status: %d), retrying request in %ss",
                    self._method, self._url, code, jitter,
                )
            self._sleep(jitter)

    def _should_retry(self, resp: object, err: Exception | None, started: float) -> bool:
        if err is not None:
            msg = str(err).lower()
            if "connection reset" in msg or "connection refused" in msg:
                # PARITY: connection errors retry only within the timeout window.
                return started + self._timeout > self._now()
            return False
        if resp is not None and getattr(resp, "status_code", None) in _RETRY_STATUS:
            _close(resp)  # PARITY: io.Copy(io.Discard, body); body.Close() before retry.
            return True
        return False


def _close(resp: object) -> None:
    close = getattr(resp, "close", None)
    if callable(close):
        close()


def requests_send(
    method: str, url: str, *, headers: dict | None = None, data: object = None, session: Any = None
) -> Callable[[], object]:
    """Build a ``send`` callable backed by ``requests`` (lazy-imported; installed at M3)."""

    def _send() -> object:
        import requests

        caller = session if session is not None else requests
        return caller.request(method, url, headers=headers, data=data)

    return _send
