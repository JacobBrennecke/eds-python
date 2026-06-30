"""Docker-gated e2e: drive the notification consumer over real core NATS (request/reply JSON + publish msgpack)."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess

import pytest

pytest.importorskip("testcontainers.core.container")


def _docker_up() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not _docker_up(), reason="Docker not available"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]

import msgpack  # noqa: E402
import nats  # noqa: E402

from eds.notification import NotificationConsumer, NotificationHandler  # noqa: E402
from eds.notification.dtos import DriverConfigResponse, SendLogsResponse  # noqa: E402


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _handler(state: dict) -> NotificationHandler:
    def driver_config():
        return DriverConfigResponse(session_id=state["sid"])

    def send_logs():
        return SendLogsResponse(path="/logs/eds-1.log", session_id=state["sid"])

    def _noop(*a):
        return None

    return NotificationHandler(
        restart=_noop, shutdown=_noop, pause=_noop, unpause=_noop, upgrade=_noop,
        send_logs=send_logs, configure=_noop, backfill_init=_noop, import_action=_noop,
        driver_config=driver_config, validate=_noop,
    )


async def _await_ready(url: str) -> None:
    for _ in range(40):
        try:
            nc = await nats.connect(url, connect_timeout=2)
            await nc.close()
            return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("NATS not ready")


async def test_notification_request_reply_and_publish() -> None:
    from testcontainers.core.container import DockerContainer

    with DockerContainer("nats:latest").with_exposed_ports(4222) as c:  # core NATS (no JetStream needed)
        url = f"nats://{c.get_container_host_ip()}:{c.get_exposed_port(4222)}"
        await _await_ready(url)

        state: dict = {"sid": ""}
        consumer = NotificationConsumer(_QuietLogger(), url, _handler(state))
        await consumer.start("")  # dev creds (empty) → generated session id
        state["sid"] = consumer.session_id
        sid = consumer.session_id
        assert sid

        client = await nats.connect(url)
        try:
            # 1) request/reply (driverconfig) → JSON reply on the request's reply subject
            reply = await client.request(
                f"eds.notify.{sid}.driverconfig", json.dumps({"action": "driverconfig"}).encode(), timeout=5
            )
            assert json.loads(reply.data)["sessionId"] == sid

            # 2) fire-and-forget (sendlogs) → msgpack publish to eds.client.<sid>.sendlogs-response
            sub = await client.subscribe(f"eds.client.{sid}.sendlogs-response")
            await client.publish(f"eds.notify.{sid}.sendlogs", json.dumps({"action": "sendlogs"}).encode())
            resp = await sub.next_msg(timeout=5)
            assert resp.headers.get("content-encoding") == "msgpack"
            assert msgpack.unpackb(resp.data, raw=False) == {"path": "/logs/eds-1.log", "sessionId": sid}
        finally:
            await client.close()
            await consumer.stop()
