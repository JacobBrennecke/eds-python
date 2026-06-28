"""PARITY: the loopback HTTP control servers (fork.go health/metrics/control + server.go wrapper /restart).

DEVIATION (loopback-http-raw-server): Go binds http.DefaultServeMux via ListenAndServe; the Python port uses a
ThreadingHTTPServer bound to 127.0.0.1 ONLY (the loopback bind is the security boundary — no auth, matching Go).
A registered "/" route acts as the catch-all fallback (mirroring Go's mux, where "/" matches any unmatched path).
"""

from __future__ import annotations

import http.server
import threading
from collections.abc import Callable, Mapping
from typing import Any

# A route returns (status_code, body) or None (→ 200, ""); it is invoked per GET request.
Route = Callable[[], "tuple[int, str] | None"]


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        routes: Mapping[str, Route] = self.server._routes  # type: ignore[attr-defined]
        route = routes.get(self.path) or routes.get("/")
        if route is None:
            self.send_response(404)
            self.end_headers()
            return
        try:
            result = route()
        except Exception as e:  # noqa: BLE001
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())
            return
        status, body = (200, "") if result is None else result
        self.send_response(status)
        self.end_headers()
        if body:
            self.wfile.write(body.encode())

    def log_message(self, *args: Any) -> None:  # silence the default stderr access log
        pass


class _Server(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class LoopbackServer:
    """A 127.0.0.1-only HTTP server dispatching GET paths to route callbacks."""

    def __init__(self, port: int, routes: Mapping[str, Route]) -> None:
        self._srv = _Server(("127.0.0.1", port), _Handler)
        self._srv._routes = routes  # type: ignore[attr-defined]
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._srv.server_address[1]

    def start(self) -> None:
        self._thread = threading.Thread(target=self._srv.serve_forever, daemon=True, name="loopback-http")
        self._thread.start()

    def stop(self) -> None:
        self._srv.shutdown()
        self._srv.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
