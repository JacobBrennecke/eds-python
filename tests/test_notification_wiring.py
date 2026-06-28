"""PARITY: the Layer-2 NotificationHandler closures (build_notification_handler)."""

from __future__ import annotations

from eds.cmd.loopback import LoopbackServer
from eds.cmd.notification_wiring import ControlPlaneContext, build_notification_handler
from eds.drivers import register_all
from eds.notification.dtos import ConfigureRequest, ImportRequest, InitBackfillRequest


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _ctx(port: int = 0) -> ControlPlaneContext:
    return ControlPlaneContext(
        logger=_QuietLogger(), port=port, api_url="https://api", api_key="k", version="1.0",
        keep_logs=False, session_id="sess",
    )


def test_driver_config_and_validate() -> None:
    register_all()
    h = build_notification_handler(_ctx())
    dc = h.driver_config()
    assert dc.session_id == "sess"
    assert "postgres" in dc.drivers  # a real driver configurator

    # empty postgres config → field errors, not a valid url
    v = h.validate("postgres", {})
    assert v.success is False and v.field_errors and v.session_id == "sess"

    # unknown driver → validate raises → Message set, Success false
    bad = h.validate("bogus", {})
    assert bad.success is False and bad.message


def test_backfill_init_and_deferred_stubs() -> None:
    h = build_notification_handler(_ctx())
    assert h.backfill_init(InitBackfillRequest(backfill=False)).success is True  # no-op success
    assert h.backfill_init(InitBackfillRequest(backfill=True)).success is False  # deferred (export job)
    assert h.upgrade("1.2.3").success is False
    assert h.configure(ConfigureRequest(url="postgres://x")).success is False
    assert h.import_action(ImportRequest(backfill=True)).success is False


def test_control_endpoints_hit_fork_loopback() -> None:
    hits: list[str] = []

    def _rec(name):
        def route():
            hits.append(name)
            return 200, ""
        return route

    srv = LoopbackServer(0, {f"/control/{n}": _rec(n) for n in ("pause", "unpause", "restart", "shutdown")})
    srv.start()
    try:
        ctx = _ctx(port=srv.port)
        ctx.fork_running = True
        h = build_notification_handler(ctx)
        assert h.pause() is None
        assert h.unpause() is None
        h.restart()
        h.shutdown("bye", False)
        assert hits == ["pause", "unpause", "restart", "shutdown"]

        # when no fork is running, the control endpoints are not hit (Go's `if configured` guard)
        hits.clear()
        ctx.fork_running = False
        h.pause()
        h.restart()
        assert hits == []
    finally:
        srv.stop()


def test_control_failure_handling() -> None:
    from eds.util.file import get_free_port

    ctx = _ctx(port=get_free_port())  # nothing listening → connection refused
    ctx.fork_running = True
    h = build_notification_handler(ctx)
    # pause/unpause RETURN the error (→ respond_generically publishes Success=false); they must not raise
    assert isinstance(h.pause(), Exception)
    assert isinstance(h.unpause(), Exception)
    # restart/shutdown swallow the error (must not raise)
    h.restart()
    h.shutdown("bye", False)
    # send_logs returns None on failure (never raises out of the log-sender ticker)
    assert h.send_logs() is None
