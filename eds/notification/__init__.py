"""PARITY: internal/notification/notification.go — the control-plane CORE-NATS consumer + dispatch.

Subscribes to ``eds.notify.<sessionID>.>`` and dispatches 11 actions to a NotificationHandler. Two reply paths:
JSON request/reply (m.Respond) for configure/import-init/driverconfig/validate; msgpack publish to
``eds.client.<sessionID>.<action>-{response,status}`` for the rest; raw ``b"pong"`` for ping.

DEVIATION (async + task-tracking): Go uses nats.go sync subscribe + a sync.WaitGroup; this port is async (nats-py)
with an asyncio.Task set gathered in stop(). Sync handler closures (HTTP/module/subprocess calls) run via
asyncio.to_thread so the event loop is not blocked — only the import leg detaches (Go runs it on a goroutine).
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import msgpack

from eds.consumer.connection import new_nats_connection
from eds.notification.dtos import (
    ConfigureRequest,
    ConfigureResponse,
    DriverConfigResponse,
    GenericResponse,
    ImportRequest,
    ImportResponse,
    InitBackfillRequest,
    InitBackfillResponse,
    Notification,
    SendLogsResponse,
    UpgradeResponse,
    ValidateResponse,
)
from eds.util.logger import Logger
from eds.util.nats import decode_nats_msg


@dataclass
class NotificationHandler:
    """PARITY: NotificationHandler — the 11 control-plane callbacks (wired by the Layer-2 control plane)."""

    restart: Callable[[], None]
    shutdown: Callable[[str, bool], None]
    pause: Callable[[], Any]  # returns None on success, or an error/exception
    unpause: Callable[[], Any]
    upgrade: Callable[[str], UpgradeResponse]
    send_logs: Callable[[], SendLogsResponse | None]
    configure: Callable[[ConfigureRequest], ConfigureResponse]
    backfill_init: Callable[[InitBackfillRequest], InitBackfillResponse]
    import_action: Callable[[ImportRequest], ImportResponse]
    driver_config: Callable[[], DriverConfigResponse]
    validate: Callable[[str, dict], ValidateResponse]


def get_bool(val: Any) -> bool:
    """PARITY: getBool — a real bool, or the literal string "true"; else False."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val == "true"
    return False


class NotificationConsumer:
    """PARITY: NotificationConsumer."""

    def __init__(
        self, logger: Logger, natsurl: str, handler: NotificationHandler, *, nc: Any = None, session_id: str = ""
    ) -> None:
        self._logger = logger.with_prefix("[notification]")
        self._natsurl = natsurl
        self._handler = handler
        self._nc = nc
        self._sub: Any = None
        self._session_id = session_id
        self._tasks: set[asyncio.Task] = set()

    @property
    def session_id(self) -> str:
        return self._session_id

    # ---- lifecycle ----
    async def start(self, creds_file: str) -> None:
        self._nc, info = await new_nats_connection(self._logger, self._natsurl, creds_file)
        self._logger.debug("connected to nats: %s", info.session_id)
        self._session_id = info.session_id
        subject = f"eds.notify.{info.session_id}.>"
        self._sub = await self._nc.subscribe(subject, cb=self._callback)
        self._logger.debug("subscribed to: %s", subject)

    async def stop(self) -> None:
        if self._sub is not None:
            try:
                await self._sub.unsubscribe()
            except Exception as e:  # noqa: BLE001 — PARITY: notification.go:177
                self._logger.error("failed to unsubscribe from nats: %s", e)
            self._sub = None
        if self._nc is not None:
            with contextlib.suppress(Exception):
                await self._nc.close()
            self._nc = None
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)  # PARITY: wg.Wait()
        self._logger.debug("stopped")

    async def restart(self, creds_file: str) -> None:
        await self.stop()
        await self.start(creds_file)

    # ---- publish helpers ----
    async def _publish(self, sid: str, action: str, mod: str, v: Any) -> None:
        data = msgpack.packb(v.to_msgpack(), use_bin_type=True)
        subject = f"eds.client.{sid}.{action}-{mod}"
        self._logger.trace("sending response: %s", subject)
        await self._nc.publish(
            subject, data, headers={"Nats-Msg-Id": str(uuid.uuid4()), "content-encoding": "msgpack"}
        )

    async def _publish_response(self, sid: str, action: str, v: Any) -> None:
        await self._publish(sid, action, "response", v)

    async def _publish_status(self, sid: str, action: str, v: Any) -> None:
        await self._publish(sid, action, "status", v)

    async def _publish_simple_status(self, action: str, err_msg: str) -> None:
        try:
            await self._publish_status(
                self._session_id,
                action,
                GenericResponse(success=err_msg == "", message=err_msg, session_id=self._session_id, action=action),
            )
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send %s status: %s", action, e)

    async def publish_send_logs_response(self, resp: SendLogsResponse) -> None:
        await self._publish_response(resp.session_id, "sendlogs", resp)  # PARITY: uses resp.session_id

    async def call_send_logs(self) -> None:
        resp = await asyncio.to_thread(self._handler.send_logs)
        if resp is None:
            self._logger.warn("sendlogs handler returned nothing")
            return
        try:
            await self.publish_send_logs_response(resp)
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send sendlogs response: %s", e)

    # ---- callback + dispatch ----
    async def _callback(self, msg: Any) -> None:
        try:
            # PARITY: Go's Header.Get is case-insensitive — HQ sends the canonical "Content-Encoding".
            content_encoding = None
            for k, v in (msg.headers or {}).items():
                if k.lower() == "content-encoding":
                    content_encoding = v
                    break
            raw = decode_nats_msg(msg.data, content_encoding)
            if not isinstance(raw, dict):
                raise ValueError("notification is not an object")
            action = raw.get("action")
            data = raw.get("data")
            notification = Notification(
                action=action if isinstance(action, str) else "",
                data=data if isinstance(data, dict) else {},
            )
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to decode notification message: %s", e)
            return
        self._logger.trace("received message: %s", notification.__gojson__())
        # PARITY: the waitgroup tracks every callback (so stop() waits for in-flight dispatch, not just imports).
        t = asyncio.create_task(self.dispatch(notification, msg))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        await asyncio.shield(t)

    async def dispatch(self, notification: Notification, msg: Any) -> None:  # noqa: C901 — faithful to the Go switch
        action = notification.action
        data = notification.data or {}

        async def respond_generically(err: Any) -> None:
            errmsg = None
            if err is not None:
                self._logger.error("failed to %s: %s", action, err)
                errmsg = str(err)
            try:
                await self._publish_response(
                    self._session_id,
                    action,
                    GenericResponse(
                        success=errmsg is None, message=errmsg, session_id=self._session_id, action=action
                    ),
                )
            except Exception as e:  # noqa: BLE001 — PARITY: Go logs "pause" for every action (copy-paste bug)
                self._logger.error("failed to send pause response: %s", e)

        if action == "restart":
            await self._publish_simple_status("restart", "")
            await asyncio.to_thread(self._handler.restart)
            await respond_generically(None)
        elif action == "ping":
            subject = data.get("subject")
            if isinstance(subject, str):
                self._logger.trace("received ping notification, replying to: %s", subject)
                try:
                    await self._nc.publish(subject, b"pong")  # raw, no headers
                except Exception as e:  # noqa: BLE001
                    self._logger.error("error sending ping response: %s", e)
            else:
                self._logger.warn("invalid ping notification. missing subject for: %s", notification.__gojson__())
        elif action == "shutdown":
            deleted = get_bool(data.get("deleted"))
            message = data.get("message")
            if isinstance(message, str):
                await asyncio.to_thread(self._handler.shutdown, message, deleted)
            else:
                self._logger.warn("invalid shutdown notification. missing message for: %s", notification.__gojson__())
        elif action == "pause":
            await respond_generically(await asyncio.to_thread(self._handler.pause))
        elif action == "unpause":
            await respond_generically(await asyncio.to_thread(self._handler.unpause))
        elif action == "upgrade":
            version = data.get("version")
            if not isinstance(version, str):
                m = f"invalid upgrade notification. missing version for: {notification.__gojson__()}"
                self._logger.warn(m)
                await self._publish_simple_status("upgrade", m)
                return
            await self._publish_simple_status("upgrade", "")
            await self._upgrade(version)
        elif action == "sendlogs":
            await self.call_send_logs()
        elif action == "configure":
            url = data.get("url")
            req = ConfigureRequest(url=url if isinstance(url, str) else "", backfill=get_bool(data.get("backfill")))
            await self._configure(req, msg)
        elif action == "import":
            await self._importaction(ImportRequest(backfill=get_bool(data.get("backfill"))), msg)
        elif action == "driverconfig":
            await self._driverconfig(msg)
        elif action == "validate":
            driver = data.get("driver")
            config = data.get("config")
            if not isinstance(driver, str):
                self._logger.error("invalid validate notification. missing driver for: %s", notification.__gojson__())
                return
            if not isinstance(config, dict):
                self._logger.error("invalid validate notification. missing config for: %s", notification.__gojson__())
                return
            await self._validate(driver, config, msg)
        else:
            self._logger.warn("unknown action: %s", action)

    # ---- action methods ----
    async def _configure(self, config: ConfigureRequest, msg: Any) -> None:
        resp = await asyncio.to_thread(self._handler.configure, config)
        try:
            await msg.respond(resp.__gojson__().encode())
        except Exception as e:  # noqa: BLE001 — PARITY: Go logs "driverconfig" here (copy-paste)
            self._logger.error("failed to send driverconfig response: %s", e)
            return
        if resp.log_path is not None:
            try:
                await self.publish_send_logs_response(SendLogsResponse(path=resp.log_path, session_id=resp.session_id))
            except Exception as e:  # noqa: BLE001
                self._logger.error("failed to publish send logs response during configure: %s", e)

    async def _upgrade(self, version: str) -> None:
        resp = await asyncio.to_thread(self._handler.upgrade, version)
        try:
            await self._publish_response(resp.session_id, "upgrade", resp)
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send upgrade response: %s", e)
            return
        if resp.log_path is not None:
            try:
                await self.publish_send_logs_response(SendLogsResponse(path=resp.log_path, session_id=resp.session_id))
            except Exception as e:  # noqa: BLE001
                self._logger.error("failed to publish send logs response during upgrade: %s", e)

    async def _importaction(self, req: ImportRequest, msg: Any) -> None:
        init = await asyncio.to_thread(self._handler.backfill_init, InitBackfillRequest(backfill=req.backfill))
        try:
            await msg.respond(init.__gojson__().encode())
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send import response: %s", e)
            return
        if not init.success:
            return
        req.job_id = init.job_id
        await self._publish_simple_status("import", "")
        # PARITY: run the (long) import leg on a background task so other commands proceed meanwhile.
        t = asyncio.create_task(self._run_import(req))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _run_import(self, req: ImportRequest) -> None:
        resp = await asyncio.to_thread(self._handler.import_action, req)
        try:
            await self._publish_response(resp.session_id, "import", resp)
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send import response: %s", e)
            return
        if resp.log_path is not None:
            try:
                await self.publish_send_logs_response(SendLogsResponse(path=resp.log_path, session_id=resp.session_id))
            except Exception as e:  # noqa: BLE001
                self._logger.error("failed to publish send logs response during import: %s", e)

    async def _driverconfig(self, msg: Any) -> None:
        resp = await asyncio.to_thread(self._handler.driver_config)
        try:
            await msg.respond(resp.__gojson__().encode())
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send driverconfig response: %s", e)

    async def _validate(self, driver: str, vals: dict, msg: Any) -> None:
        resp = await asyncio.to_thread(self._handler.validate, driver, vals)
        try:
            await msg.respond(resp.__gojson__().encode())
        except Exception as e:  # noqa: BLE001
            self._logger.error("failed to send validate response: %s", e)
