"""PARITY: the loopback control server (127.0.0.1 GET routes + "/" catch-all)."""

from __future__ import annotations

import urllib.error
import urllib.request

from eds.cmd.loopback import LoopbackServer


def _get(port: int, path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def test_routes_and_catch_all() -> None:
    events: list[str] = []

    def restart() -> tuple[int, str]:
        events.append("restart")
        return 200, ""

    routes = {
        "/": lambda: (200, "OK"),
        "/control/restart": restart,
        "/control/logfile": lambda: (200, "/logs/eds-2.log"),
        "/control/boom": lambda: (_ for _ in ()).throw(RuntimeError("kaboom")),
    }
    srv = LoopbackServer(0, routes)
    srv.start()
    try:
        port = srv.port
        assert port > 0
        assert _get(port, "/") == (200, "OK")
        assert _get(port, "/anything-else") == (200, "OK")  # "/" catch-all (Go mux behavior)
        assert _get(port, "/control/restart")[0] == 200
        assert events == ["restart"]
        assert _get(port, "/control/logfile") == (200, "/logs/eds-2.log")
        assert _get(port, "/control/boom")[0] == 500  # a raising route → 500
    finally:
        srv.stop()


def test_no_catch_all_returns_404() -> None:
    srv = LoopbackServer(0, {"/restart": lambda: (202, "")})  # wrapper-style: only /restart
    srv.start()
    try:
        assert _get(srv.port, "/restart")[0] == 202
        assert _get(srv.port, "/something")[0] == 404  # no "/" route → 404
    finally:
        srv.stop()
