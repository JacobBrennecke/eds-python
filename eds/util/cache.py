"""PARITY: internal/util/cache.go — in-memory TTL cache (lazy eviction on Get + a background sweeper)."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Cache(Protocol):
    def get(self, key: str) -> tuple[bool, object]: ...
    def set(self, key: str, val: object, expires: float) -> None: ...  # expires in seconds
    def close(self) -> None: ...


class _Value:
    __slots__ = ("object", "expires")

    def __init__(self, obj: object, expires: float) -> None:
        self.object = obj
        self.expires = expires


class InMemoryCache:
    """PARITY: cache.go inMemoryCache. Go's sweeper goroutine becomes a daemon thread; the monotonic clock
    replaces Go's wall clock (more robust against system-clock changes; behaviorally equivalent for TTL
    durations — DEVIATION: see DEVIATIONS.md#cache-monotonic-clock)."""

    def __init__(self, expiry_check: float) -> None:
        self._cache: dict[str, _Value] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._expiry_check = expiry_check
        self._thread = threading.Thread(target=self._run, name="eds-cache-sweeper", daemon=True)
        self._thread.start()

    def get(self, key: str) -> tuple[bool, object]:
        """PARITY: Get — lazily evicts an expired entry on access."""
        with self._lock:
            val = self._cache.get(key)
            if val is None:
                return False, None
            if val.expires < time.monotonic():
                del self._cache[key]
                return False, None
            return True, val.object

    def set(self, key: str, val: object, expires: float) -> None:
        """PARITY: Set — store with deadline now + ``expires`` (seconds)."""
        with self._lock:
            self._cache[key] = _Value(val, time.monotonic() + expires)

    def close(self) -> None:
        """PARITY: Close — stop the sweeper (idempotent via the stop event)."""
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        # _stop.wait returns True when stopped, False on timeout -> sweep each interval until closed.
        while not self._stop.wait(self._expiry_check):
            now = time.monotonic()
            with self._lock:
                expired = [k for k, v in self._cache.items() if v.expires < now]
                for k in expired:
                    del self._cache[k]


def new_cache(expiry_check: float) -> InMemoryCache:
    """PARITY: util.NewCache — start a cache with a background sweep interval (seconds). (Go also takes a
    parent context for cancellation; here close() stops the sweeper.)"""
    return InMemoryCache(expiry_check)
