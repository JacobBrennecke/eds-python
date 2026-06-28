"""PARITY: cmd/server.go NotificationHandler closures (548-921) + the async consumer's sync lifecycle bridge.

The Layer-2 control plane is synchronous (it blocks on subprocess fork); the notification consumer is asyncio.
NotificationRunner drives the async NotificationConsumer on a background event loop and exposes sync
start/stop/publish_send_logs_response + the renew/log tickers. build_notification_handler wires the control-plane
closures: the feasible actions hit the Layer-3 fork's loopback /control/* or call a local module; the actions whose
dependencies are not yet ported (upgrade/configure/import/backfill-real) return a Success=false "not yet ported"
response so the wire contract stays intact.
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass

from eds.cmd.session import get_log_upload_url, upload_log_file
from eds.driver import get_driver_configurations
from eds.driver import validate as driver_validate
from eds.notification import NotificationConsumer, NotificationHandler
from eds.notification.dtos import (
    ConfigureRequest,
    ConfigureResponse,
    DriverConfigResponse,
    ImportRequest,
    ImportResponse,
    InitBackfillRequest,
    InitBackfillResponse,
    SendLogsResponse,
    UpgradeResponse,
    ValidateResponse,
)
from eds.util.logger import Logger

_NOT_PORTED = "not yet ported in the Python EDS port"


def is_nats_connection_error(e: BaseException) -> bool:
    """PARITY: server.go:996 — only a NATS-connection failure triggers the 5s retry; other Start errors proceed."""
    import nats.errors

    if isinstance(e, (nats.errors.NoServersError, nats.errors.ConnectionClosedError, ConnectionError, OSError)):
        return True
    return "connect" in str(e).lower()


@dataclass
class ControlPlaneContext:
    """Mutable Layer-2 state the handler closures capture (session_id + fork_running change per session)."""

    logger: Logger
    port: int
    api_url: str
    api_key: str
    version: str
    keep_logs: bool
    session_id: str = ""
    fork_running: bool = False


def _control_get(ctx: ControlPlaneContext, path: str) -> str:
    import requests

    resp = requests.get(f"http://127.0.0.1:{ctx.port}/control/{path}", timeout=30)
    return resp.text


def build_notification_handler(ctx: ControlPlaneContext) -> NotificationHandler:
    """PARITY: the 11 NotificationHandler closures (server.go:552-921)."""

    def restart() -> None:
        # PARITY: restart swallows a loopback error (logs + returns void → respond_generically(None) Success=true).
        try:
            if ctx.fork_running:
                _control_get(ctx, "restart")
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to restart: %s", e)

    def shutdown(message: str, deleted: bool) -> None:
        # DEVIATION: Go logger.Fatal on a loopback error; here we log and continue (no Fatal from a worker thread).
        try:
            if ctx.fork_running:
                _control_get(ctx, "shutdown")
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to shutdown: %s", e)
        if deleted:
            # DEFERRED: de-enroll (write server_id="" to config.toml) needs a TOML writer.
            ctx.logger.warn("de-enroll on shutdown is not yet ported (config.toml write)")

    def pause():  # PARITY: returns None on success or the error (→ respond_generically publishes Success=false)
        try:
            if ctx.fork_running:
                _control_get(ctx, "pause")
            return None
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to pause: %s", e)
            return e

    def unpause():
        try:
            if ctx.fork_running:
                _control_get(ctx, "unpause")
            return None
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to unpause: %s", e)
            return e

    def upgrade(version: str) -> UpgradeResponse:
        # DEFERRED: upgrade module (download + PGP verify + apply) + `eds download` not ported.
        return UpgradeResponse(success=False, message=_NOT_PORTED, session_id=ctx.session_id, version=version)

    def send_logs() -> SendLogsResponse | None:
        # PARITY: sendLogs treats any failure as "no logs" (returns None) — it must never raise out of the
        # 1h log-sender ticker.
        try:
            if not ctx.session_id:
                return None
            log_file = _control_get(ctx, "logfile")  # the rotated log path (currently a stub returning "")
            if not log_file:
                return None
            upload_url = get_log_upload_url(ctx.logger, ctx.api_url, ctx.api_key, ctx.session_id, version=ctx.version)
            storage = upload_log_file(ctx.logger, upload_url, log_file, version=ctx.version)
            if not ctx.keep_logs:
                try:
                    os.remove(log_file)
                except OSError:
                    pass
            return SendLogsResponse(path=storage, session_id=ctx.session_id)
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to send logs: %s", e)
            return None

    def configure(req: ConfigureRequest) -> ConfigureResponse:
        # DEFERRED: needs runImport (cmd/import.go) + config.toml WRITE.
        return ConfigureResponse(success=False, message=_NOT_PORTED, session_id=ctx.session_id, backfill=req.backfill)

    def backfill_init(req: InitBackfillRequest) -> InitBackfillResponse:
        if not req.backfill:  # PARITY: backfill=false → success no-op
            return InitBackfillResponse(success=True, session_id=ctx.session_id)
        # DEFERRED: createExportJob (POST /v3/export/bulk) not ported.
        return InitBackfillResponse(success=False, message=_NOT_PORTED, session_id=ctx.session_id)

    def import_action(req: ImportRequest) -> ImportResponse:
        # DEFERRED: needs runImport (cmd/import.go fork).
        return ImportResponse(success=False, message=_NOT_PORTED, session_id=ctx.session_id, job_id=req.job_id)

    def driver_config() -> DriverConfigResponse:
        return DriverConfigResponse(drivers=get_driver_configurations(), session_id=ctx.session_id)

    def validate(driver: str, values: dict) -> ValidateResponse:
        try:
            url, field_errors = driver_validate(driver, values)
            return ValidateResponse(
                success=url != "", field_errors=field_errors, session_id=ctx.session_id, url=url
            )
        except Exception as e:  # noqa: BLE001 — PARITY: Validate error → Message, Success=false (C# does the same)
            return ValidateResponse(success=False, message=str(e), session_id=ctx.session_id)

    return NotificationHandler(
        restart=restart, shutdown=shutdown, pause=pause, unpause=unpause, upgrade=upgrade,
        send_logs=send_logs, configure=configure, backfill_init=backfill_init, import_action=import_action,
        driver_config=driver_config, validate=validate,
    )


class NotificationRunner:
    """Drives the async NotificationConsumer on a background event loop, exposing a sync API to Layer 2."""

    def __init__(self, logger: Logger, natsurl: str, handler: NotificationHandler) -> None:
        self._logger = logger
        self._consumer = NotificationConsumer(logger, natsurl, handler)
        self._handler = handler
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._tickers: list[asyncio.Task] = []

    @property
    def session_id(self) -> str:
        return self._consumer.session_id

    def _run(self, coro, timeout: float = 30.0):
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=timeout)

    def start(self, creds_file: str, *, renew_interval: float = 0.0, log_sender_interval: float = 3600.0) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True, name="notification-loop")
        self._thread.start()
        self._run(self._consumer.start(creds_file))
        if renew_interval > 0:
            self._tickers.append(self._schedule(self._renew_loop(renew_interval)))
        if log_sender_interval > 0:
            self._tickers.append(self._schedule(self._log_sender_loop(log_sender_interval)))

    def _schedule(self, coro) -> asyncio.Task:
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(self._wrap_task(coro), self._loop).result(timeout=5)

    async def _wrap_task(self, coro) -> asyncio.Task:
        return asyncio.create_task(coro)

    async def _renew_loop(self, interval: float) -> None:
        while True:  # PARITY: renewTicker (24h) → restart(); resilient so one failure doesn't kill the ticker
            try:
                await asyncio.sleep(interval)
                await asyncio.to_thread(self._handler.restart)
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                self._logger.error("renew tick failed: %s", e)

    async def _log_sender_loop(self, interval: float) -> None:
        while True:  # PARITY: logSenderTicker (1h) → CallSendLogs()
            try:
                await asyncio.sleep(interval)
                await self._consumer.call_send_logs()
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                self._logger.error("log-sender tick failed: %s", e)

    def publish_send_logs_response(self, resp: SendLogsResponse) -> None:
        if self._loop is None:  # the consumer never started (e.g. a non-connection start error) → nothing to publish
            return
        self._run(self._consumer.publish_send_logs_response(resp))

    def stop(self) -> None:
        if self._loop is None:
            return
        for t in self._tickers:
            self._loop.call_soon_threadsafe(t.cancel)
        self._tickers = []
        try:
            self._run(self._consumer.stop())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=5)
            self._loop.close()
            self._loop = None
