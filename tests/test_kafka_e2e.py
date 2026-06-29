"""Docker-gated e2e: stream events into a real Kafka broker via testcontainers, then consume them back.

Exercises the REAL lazy-confluent-kafka connect path, the explicit-partition produce (partition resolved from
topic metadata + the FNV balancer), and Flush. Skipped when Docker, testcontainers, or confluent-kafka are
unavailable.
"""

from __future__ import annotations

import importlib.util
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


def _deps_available() -> bool:
    return (
        importlib.util.find_spec("confluent_kafka") is not None
        and importlib.util.find_spec("testcontainers.kafka") is not None
    )


pytestmark = [
    pytest.mark.skipif(not _docker_up(), reason="Docker not available"),
    pytest.mark.skipif(not _deps_available(), reason="confluent-kafka / testcontainers[kafka] not installed"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),  # testcontainers internal deprecations
]

from eds.dbchange import DBChangeEvent  # noqa: E402
from eds.driver import DriverConfig  # noqa: E402
from eds.drivers.kafka import KafkaDriver, message_key  # noqa: E402
from eds.util.gojson import RawJson, stringify  # noqa: E402


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def test_streams_events_into_kafka() -> None:
    import time

    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient, NewTopic
    from testcontainers.kafka import KafkaContainer

    topic = "eds-test"
    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()

        # pre-create the topic with multiple partitions so the explicit-partition balancer is exercised
        admin = AdminClient({"bootstrap.servers": bootstrap})
        fs = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        for f in fs.values():
            f.result(timeout=30)

        # host[:port] from the bootstrap (driver appends nothing; it uses the full host as bootstrap.servers)
        url = f"kafka://{bootstrap}/{topic}"
        log = _QuietLogger()
        driver = KafkaDriver()
        driver.start(DriverConfig(url=url, logger=log))
        events = [
            DBChangeEvent(operation="INSERT", id="e1", table="customer", key=["c1"], company_id="comp1",
                          after=RawJson('{"id":"c1"}')),
            DBChangeEvent(operation="UPDATE", id="e2", table="customer", key=["c2"],
                          after=RawJson('{"id":"c2"}')),
        ]
        try:
            for e in events:
                assert driver.process(log, e) is False
            driver.flush(log)
        finally:
            driver.stop()

        # consume the messages back
        consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": "eds-test-consumer",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([topic])
        got: dict[str, bytes] = {}
        deadline = time.time() + 30
        while len(got) < len(events) and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            got[msg.key().decode("utf-8")] = msg.value()
        consumer.close()

        assert message_key("customer", "INSERT", "comp1", None, "e1") in got
        assert message_key("customer", "UPDATE", None, None, "e2") in got
        assert got[message_key("customer", "INSERT", "comp1", None, "e1")] == stringify(events[0]).encode("utf-8")
