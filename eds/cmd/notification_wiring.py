"""PARITY: cmd/server.go NotificationHandler closures (548-921) + the async consumer's sync lifecycle bridge.

The Layer-2 control plane is synchronous (it blocks on subprocess fork); the notification consumer is asyncio.
NotificationRunner drives the async NotificationConsumer on a background event loop and exposes sync
start/stop/publish_send_logs_response + the renew/log tickers. build_notification_handler wires the control-plane
closures: configure/import/backfill fork `eds import` (runImport); the remaining feasible actions hit the Layer-3
fork's loopback /control/* or call a local module; upgrade (and configure's config.toml persist) stay deferred.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from dataclasses import dataclass
from typing import Any

from eds.cmd.exit_codes import EXIT_INCORRECT_USAGE, EXIT_SUCCESS
from eds.cmd.import_client import create_export_job
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
from eds.util.mask import mask, mask_url
from eds.util.process import ForkArgs, fork

_NOT_PORTED = "not yet ported in the Python EDS port"


def _run_import(
    ctx: ControlPlaneContext, url: str, schema_only: bool, validate_only: bool, job_id: str
) -> tuple[bool, bool, str | None, str | None]:
    """PARITY: runImport (server.go:759-836) — fork `eds import`; returns (success, validated, message, logPath)."""
    importargs = ["--url", url, "--api-key", ctx.api_key, "--no-confirm", "--data-dir", ctx.data_dir]
    if schema_only:
        importargs.append("--schema-only")
    if job_id:
        importargs += ["--job-id", job_id]
    if validate_only:
        importargs += ["--validate-only", "--silent"]
        ctx.logger.info("configuring the driver, one moment please...")
    else:
        # PARITY: Go emits --verbose=<bool> (pflag accepts =value); argparse store_true rejects =value, so emit a
        # bare --verbose only when on (and nothing when off) — same effect, parseable by `eds import`.
        if ctx.verbose:
            importargs.append("--verbose")
        ctx.logger.info("running import process ... (duration will vary based on the amount of data being exported)")

    forker = ctx.forker or fork
    try:
        result = forker(
            ForkArgs(
                command="import", args=importargs, log_filename_label="import", save_logs=True,
                forward_interrupt=True, write_to_std=False, dir=ctx.session_dir, log=ctx.logger,
            )
        )
    except Exception:  # noqa: BLE001 — Go: err && result==nil
        result = None
    if result is None:
        return True, False, "Error importing data. Please contact support for assistance.", None

    ec = result.exit_code
    ctx.logger.debug("import exit code: %d, last log line: %s", ec, result.last_error_lines)
    if ctx.no_restart:
        sys.exit(ec)
    if ec == EXIT_SUCCESS:
        return True, True, None, None
    if ec == EXIT_INCORRECT_USAGE:  # the url is invalid
        lines = result.last_error_lines.rstrip("\n").split("\n")
        msg = lines[-1] if len(lines) > 1 else result.last_error_lines.strip()
        return False, False, msg, None
    # default failure: upload the import stdout/stderr logs
    ctx.logger.error("import failed with exit code %d: %s", ec, result.last_error_lines)
    upload_log_path = ""
    try:
        upload_url = get_log_upload_url(ctx.logger, ctx.api_url, ctx.api_key, ctx.session_id, version=ctx.version)
    except Exception as e:  # noqa: BLE001
        ctx.logger.error("failed to get upload URL: %s", e)
    else:
        stdout_file = os.path.join(ctx.session_dir, "import_stdout.txt")
        if os.path.exists(stdout_file) and os.path.getsize(stdout_file) > 0:
            try:
                upload_log_path = upload_log_file(ctx.logger, upload_url, stdout_file, version=ctx.version)
            except Exception as e:  # noqa: BLE001
                ctx.logger.error("failed to upload stdout logfile: %s", e)
        stderr_file = os.path.join(ctx.session_dir, "import_stderr.txt")
        if os.path.exists(stderr_file) and os.path.getsize(stderr_file) > 0:
            try:
                upload_log_file(ctx.logger, upload_url, stderr_file, version=ctx.version)
            except Exception as e:  # noqa: BLE001
                ctx.logger.error("failed to upload stderr logfile: %s", e)
    return (
        True, False,
        "Error importing data. See the error logs for more details or contact support for further assistance.",
        upload_log_path,
    )


def is_nats_connection_error(e: BaseException) -> bool:
    """PARITY: server.go:996 — only a NATS-connection failure triggers the 5s retry; other Start errors proceed."""
    import nats.errors

    if isinstance(e, (nats.errors.NoServersError, nats.errors.ConnectionClosedError, ConnectionError, OSError)):
        return True
    return "connect" in str(e).lower()


@dataclass
class ControlPlaneContext:
    """Mutable Layer-2 state the handler closures capture (session_id/session_dir/fork_running change per session)."""

    logger: Logger
    port: int
    api_url: str
    api_key: str
    version: str
    keep_logs: bool
    data_dir: str = ""
    verbose: bool = False
    no_restart: bool = False
    driver_url: str = ""
    configured: bool = False
    session_id: str = ""
    session_dir: str = ""
    fork_running: bool = False
    forker: Any = None  # injectable for tests; default eds.util.process.fork


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
        # PARITY: configure (server.go:838-868) — validate the url via `import --validate-only`.
        ctx.logger.trace("received driver configuration. url: %s", mask(req.url))  # PARITY: cstr.Mask for the trace
        success, validated, msg, upload_log_path = _run_import(ctx, req.url, False, True, "")
        masked_url = None
        if success and validated:
            # DEFERRED: persist the url to config.toml (no TOML writer yet); the change is in-memory only.
            ctx.logger.warn("persisting the driver url to config.toml is not yet ported")
            ctx.logger.info("driver configured successfully, waiting for import action...")
            ctx.driver_url = req.url
            try:
                masked_url = mask_url(req.url)
            except Exception as e:  # noqa: BLE001
                ctx.logger.warn("could not mask URL, will not display in app: %s", e)
            if not ctx.configured:
                restart()
        return ConfigureResponse(
            session_id=ctx.session_id, success=validated, log_path=upload_log_path, masked_url=masked_url,
            message=msg, backfill=req.backfill,
        )

    def backfill_init(req: InitBackfillRequest) -> InitBackfillResponse:
        ctx.logger.trace("received init backfill request")
        if not req.backfill:  # PARITY: backfill=false → success no-op
            return InitBackfillResponse(success=True, session_id=ctx.session_id)
        try:
            job_id = create_export_job(
                ctx.logger, ctx.api_url, ctx.api_key, tables=None, company_ids=None, location_ids=None,
                time_offset_ms=None, version=ctx.version,
            )
        except Exception as e:  # noqa: BLE001
            ctx.logger.error("failed to create export job: %s", e)
            return InitBackfillResponse(success=False, message=str(e), session_id=ctx.session_id)
        return InitBackfillResponse(success=True, job_id=job_id, session_id=ctx.session_id)

    def import_action(req: ImportRequest) -> ImportResponse:
        # PARITY: importaction (server.go:884-898) — pause, fork `eds import`, then restart (or signal first-consumer).
        ctx.logger.trace("received import action")
        pause()
        success, _validated, msg, upload_log_path = _run_import(
            ctx, ctx.driver_url, not req.backfill, False, req.job_id
        )
        if not success:
            return ImportResponse(success=False, message=msg, session_id=ctx.session_id, log_path=upload_log_path)
        if not ctx.configured:
            # DEFERRED: the configureChannel first-consumer gate (server.py requires --url so configured is True)
            ctx.logger.trace("driver configured")
        else:
            restart()
        return ImportResponse(success=success, message=msg, session_id=ctx.session_id, log_path=upload_log_path)

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
