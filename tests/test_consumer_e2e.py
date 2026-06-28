"""Docker-gated e2e: stream dbchange events through real NATS JetStream into the consumer + a fake driver.

Skipped when Docker is unavailable. Exercises the full data path: connect → create durable → pull-fetch →
queue → Bufferer → BatchProcessor → driver.process/flush → ack.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess

import pytest


def _docker_up() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not _docker_up(), reason="Docker not available"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),  # testcontainers internal deprecations
]

import nats  # noqa: E402

from eds.consumer.config import ConsumerConfig  # noqa: E402
from eds.consumer.consumer import Consumer  # noqa: E402


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _NoopMetrics:
    def pending_events_inc(self): ...
    def pending_events_dec(self): ...
    def total_events_inc(self): ...
    def observe_flush_duration(self, s): ...
    def observe_flush_count(self, c): ...
    def observe_processing_duration(self, s): ...


class _RecordingDriver:
    def __init__(self) -> None:
        self.events: list = []
        self.flushes = 0

    def max_batch_size(self) -> int:
        return 2  # flush after every 2 events

    def process(self, logger, evt) -> bool:
        self.events.append(evt)
        return False

    def flush(self, logger) -> None:
        self.flushes += 1


async def _await_ready(url: str) -> None:
    for _ in range(40):
        try:
            nc = await nats.connect(url, connect_timeout=2)
            await nc.close()
            return
        except Exception:
            await asyncio.sleep(0.5)
    raise RuntimeError("NATS not ready")


async def test_consumes_dbchange_events_into_driver() -> None:
    from testcontainers.core.container import DockerContainer

    with DockerContainer("nats:latest").with_command("-js").with_exposed_ports(4222) as c:
        url = f"nats://{c.get_container_host_ip()}:{c.get_exposed_port(4222)}"
        await _await_ready(url)

        # Set up the stream + publish two dbchange events.
        nc = await nats.connect(url)
        js = nc.jetstream()
        await js.add_stream(name="dbchange", subjects=["dbchange.>"])
        subject = "dbchange.a.b.comp1.c.PUBLIC.user"
        for i in range(2):
            evt = json.dumps(
                {"operation": "INSERT", "table": "user", "key": [f"u{i}"],
                 "after": {"id": f"u{i}"}, "modelVersion": "v1"}
            ).encode()
            await js.publish(subject, evt)
        await nc.close()

        driver = _RecordingDriver()
        # dev creds → company_ids ["*"]; its filter "dbchange.*.*.*.*.PUBLIC.>" matches the published subject.
        # (A specific override like ["comp1"] is rejected here: the strict validation requires it to be in creds.)
        config = ConsumerConfig(
            url=url, credentials="", driver=driver, deliver_all=True, registry=None,
        )
        consumer = Consumer(config, _QuietLogger(), _NoopMetrics())
        await consumer.create()
        await consumer.start()
        try:
            for _ in range(40):
                if len(driver.events) >= 2 and driver.flushes >= 1:
                    break
                await asyncio.sleep(0.25)
            assert len(driver.events) == 2
            assert driver.flushes >= 1
            assert [e.get_primary_key() for e in driver.events] == ["u0", "u1"]
            assert consumer.error() is None
        finally:
            await consumer.stop()
