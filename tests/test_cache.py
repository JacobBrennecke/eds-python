"""PARITY: internal/util/cache_test.go (timings widened for coarse OS clocks)."""

from __future__ import annotations

import time

from eds.util.cache import new_cache


def test_create_and_close() -> None:
    c = new_cache(1.0)
    c.close()  # must not hang


def test_set_get_and_lazy_expire() -> None:
    c = new_cache(60.0)  # long sweep -> lazy eviction on get is what's exercised
    try:
        assert c.get("test") == (False, None)
        c.set("test", "value", 0.1)
        assert c.get("test") == (True, "value")
        time.sleep(0.2)
        assert c.get("test") == (False, None)
    finally:
        c.close()


def test_background_sweep_evicts_without_access() -> None:
    c = new_cache(0.05)  # sweep every 50ms
    try:
        c.set("test", "value", 0.1)
        assert c.get("test")[0]
        time.sleep(0.3)
        assert len(c._cache) == 0  # swept proactively, without a get
    finally:
        c.close()
