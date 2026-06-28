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


def test_backfill_init_noop_and_upgrade_deferred() -> None:
    h = build_notification_handler(_ctx())
    assert h.backfill_init(InitBackfillRequest(backfill=False)).success is True  # no-op success
    assert h.upgrade("1.2.3").success is False  # upgrade still deferred


def _forker(exit_code: int, last: str = ""):
    """A fake ForkArgs→ForkResult forker that records the import args it was given."""
    from eds.util.process import ForkResult

    calls: list[list[str]] = []

    def f(args):
        calls.append(args.args)
        return ForkResult(exit_code=exit_code, last_error_lines=last)

    return f, calls


def test_configure_validates_via_runimport() -> None:
    forker, calls = _forker(0)  # import --validate-only succeeded
    ctx = _ctx()
    ctx.forker = forker
    ctx.configured = True  # configured → no restart attempt
    resp = build_notification_handler(ctx).configure(ConfigureRequest(url="postgres://x"))
    assert resp.success is True  # validated
    assert "--validate-only" in calls[0] and "--url" in calls[0]
    assert ctx.driver_url == "postgres://x"  # in-memory update (persist deferred)

    forker2, _ = _forker(3, "boom\ninvalid connection string")  # exit 3 = bad url
    ctx2 = _ctx()
    ctx2.forker = forker2
    bad = build_notification_handler(ctx2).configure(ConfigureRequest(url="postgres://bad"))
    assert bad.success is False and bad.message == "invalid connection string"


def test_import_args_are_parseable_by_the_import_subparser() -> None:
    # Regression: the forked `eds import` args must actually parse (the fake-forker unit tests can't catch this).
    # A --verbose=<bool> would trip argparse store_true → exit 3 → a bogus "invalid url" import failure.
    from eds.cmd.root import build_parser
    from eds.util.process import ForkResult

    def parsing_forker(args):
        try:
            build_parser().parse_args(["import", *args.args])  # simulate `eds import <args>` parsing
        except SystemExit as e:
            return ForkResult(exit_code=int(e.code or 0), last_error_lines="usage error")
        return ForkResult(exit_code=0)

    for verbose in (True, False):
        ctx = _ctx()
        ctx.forker = parsing_forker
        ctx.configured = True
        ctx.driver_url = "postgres://x"
        ctx.verbose = verbose
        resp = build_notification_handler(ctx).import_action(ImportRequest(backfill=True))
        assert resp.success is True, f"import args unparseable with verbose={verbose}"


def test_import_action_runs_import() -> None:
    forker, calls = _forker(0)
    ctx = _ctx()
    ctx.forker = forker
    ctx.configured = True
    ctx.driver_url = "postgres://x"
    resp = build_notification_handler(ctx).import_action(ImportRequest(backfill=True))
    assert resp.success is True
    assert "--schema-only" not in calls[0]  # backfill=True → full import (not schema-only)

    forker2, calls2 = _forker(3, "bad url")
    ctx2 = _ctx()
    ctx2.forker = forker2
    ctx2.driver_url = "postgres://x"
    bad = build_notification_handler(ctx2).import_action(ImportRequest(backfill=False))
    assert bad.success is False and bad.message == "bad url"
    assert "--schema-only" in calls2[0]  # backfill=False → schema-only


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
