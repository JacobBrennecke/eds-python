"""Regression: NATS URLs must carry an explicit port for nats-py.

nats-py (unlike Go's nats.go) does NOT apply NATS's default port, so a port-less URL such as
`nats://connect.nats.shopmonkey.pub` makes asyncio connect to port 0 → `WinError 10049` on Windows, and the server
(notification consumer) and the forked data consumer can never reach NATS. `_ensure_nats_port` appends :4222 when
no explicit port is present.
"""

from __future__ import annotations

import pytest

from eds.consumer.connection import _ensure_nats_port


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("nats://connect.nats.shopmonkey.pub", "nats://connect.nats.shopmonkey.pub:4222"),
        ("nats://localhost", "nats://localhost:4222"),
        ("nats://host:1234", "nats://host:1234"),            # explicit port preserved
        ("nats://localhost:4222", "nats://localhost:4222"),  # already correct
        ("host-no-scheme", "nats://host-no-scheme:4222"),    # scheme added + default port
    ],
)
def test_ensure_nats_port(url: str, expected: str) -> None:
    assert _ensure_nats_port(url) == expected
