"""PARITY: internal/drivers/kafka/kafka_test.go — Validate golden vectors + the pure key / partition-key /
balancer logic and the process-buffering path. All pure (no confluent-kafka): the client is lazy-imported."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.driver import ImporterConfig
from eds.drivers.kafka import (
    EDS_PARTITION_KEY_HEADER,
    KafkaDriver,
    balance,
    is_leader_not_available,
    message_key,
    partition_key,
    str_with_def,
    validate_config,
)
from eds.util.gojson import RawJson, stringify
from eds.util.hash import hash as eds_hash
from eds.util.hash import modulo


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def test_str_with_def() -> None:
    assert str_with_def(None, "NONE") == "NONE"
    assert str_with_def("", "NONE") == "NONE"
    assert str_with_def("x", "NONE") == "x"


def test_message_key() -> None:
    assert message_key("table", "INSERT", None, None, "id1") == "dbchange.table.INSERT.NONE.NONE.id1"
    assert message_key("table", "UPDATE", "c1", "l1", "id1") == "dbchange.table.UPDATE.c1.l1.id1"


def test_partition_key() -> None:
    assert partition_key("table", None, None, "pk1") == "table.NONE.NONE.pk1"
    assert partition_key("table", "c1", "l1", "pk1") == "table.c1.l1.pk1"


def test_balance_single_partition() -> None:
    assert balance("anything", "key", 1) == 0


def test_balance_uses_header_then_key() -> None:
    pkey = "table.NONE.NONE.pk"
    assert balance(pkey, "msgkey", 5) == modulo(eds_hash(pkey), 5)
    # falls back to the message key when there's no header
    assert balance(None, "msgkey", 5) == modulo(eds_hash("msgkey"), 5)


# ---- Validate (PARITY: TestValidate) ----
@pytest.mark.parametrize(
    ("config", "expected_url", "expect_error"),
    [
        ({"Hostname": "hostname", "Topic": "topic"}, "kafka://hostname:9092/topic", False),
        ({"Hostname": "hostname", "Topic": "topic", "Port": 9999}, "kafka://hostname:9999/topic", False),
        ({"Hostname": "hostname"}, "", True),
    ],
)
def test_validate(config, expected_url, expect_error) -> None:
    url, errs = validate_config(config)
    if expect_error:
        assert len(errs) >= 1
        assert url == ""
    else:
        assert errs == []
        assert url == expected_url


# ---- metadata ----
def test_metadata() -> None:
    d = KafkaDriver()
    assert d.name() == "Kafka"
    assert d.description() == "Supports streaming EDS messages to a Kafka topic."
    assert d.example_url() == "kafka://kafka:9092/topic"
    assert d.max_batch_size() == -1
    assert d.supports_delete() is False
    assert [f.name for f in d.configuration()] == ["Hostname", "Port", "Topic"]
    # the Port field default is the decimal string form
    assert d.configuration()[1].default == "9092"


def test_help_has_sections() -> None:
    h = KafkaDriver().help()
    assert "Partitioning" in h
    assert "Message Key" in h
    assert "Message Value" in h


# ---- process buffering (no client needed) ----
def test_process_buffers_message() -> None:
    d = KafkaDriver()
    evt = DBChangeEvent(
        operation="INSERT", id="evt1", table="customer", key=["c1"], company_id="comp1",
        after=RawJson('{"id":"c1"}'),
    )
    assert d.process(_QuietLogger(), evt) is False
    assert len(d._pending) == 1
    key, value, pkey = d._pending[0]
    assert key == b"dbchange.customer.INSERT.comp1.NONE.evt1"
    assert value == stringify(evt).encode("utf-8")
    assert pkey == "customer.comp1.NONE.c1"
    assert EDS_PARTITION_KEY_HEADER == "eds-partitionkey"


def test_import_event_dry_run_does_not_buffer() -> None:
    d = KafkaDriver()
    d._logger = _QuietLogger()
    d._import_config = ImporterConfig(dry_run=True)
    d.import_event(DBChangeEvent(table="t", key=["k"], operation="INSERT", id="i"), None)
    assert d._pending == []


# ---- leader-not-available retry decision (kafka-01 regression) ----
def test_is_leader_not_available_substring_case_insensitive() -> None:
    # the e2e pre-creates the topic, so the retry path is only covered here. librdkafka emits lowercase text.
    assert is_leader_not_available(RuntimeError("Broker: Leader not available")) is True
    assert is_leader_not_available(RuntimeError("KafkaError: leader not available for topic")) is True
    # Go's title-cased form must also match (segmentio-style messages)
    assert is_leader_not_available(RuntimeError("Leader Not Available")) is True


def test_is_leader_not_available_false_for_other_errors() -> None:
    assert is_leader_not_available(RuntimeError("unknown topic or partition")) is False
    assert is_leader_not_available(ValueError("some other failure")) is False


def test_is_leader_not_available_matches_confluent_error_code() -> None:
    # the robust signal: a confluent KafkaError with code LEADER_NOT_AVAILABLE (even if the text changes)
    confluent_kafka = pytest.importorskip("confluent_kafka")
    err = confluent_kafka.KafkaError(confluent_kafka.KafkaError.LEADER_NOT_AVAILABLE)
    assert is_leader_not_available(confluent_kafka.KafkaException(err)) is True
    other = confluent_kafka.KafkaError(confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART)
    assert is_leader_not_available(confluent_kafka.KafkaException(other)) is False


def test_flush_retries_on_leader_not_available_then_succeeds(monkeypatch) -> None:
    # prove the flush loop RETRIES (does not immediately NAK) when _produce_all reports leader-not-available
    d = KafkaDriver()
    d._pending = [(b"k", b"v", "pk")]
    monkeypatch.setattr("eds.drivers.kafka.time.sleep", lambda _s: None)  # don't actually wait
    calls = {"n": 0}

    def _fake_produce_all() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("Broker: Leader not available")  # transient -> should retry
        # second attempt succeeds

    monkeypatch.setattr(d, "_produce_all", _fake_produce_all)
    d.flush(_QuietLogger())
    assert calls["n"] == 2  # retried exactly once, then succeeded
    assert d._pending == []  # cleared on success


def test_flush_naks_immediately_on_non_leader_error(monkeypatch) -> None:
    d = KafkaDriver()
    d._pending = [(b"k", b"v", "pk")]

    def _fake_produce_all() -> None:
        raise RuntimeError("some fatal broker error")

    monkeypatch.setattr(d, "_produce_all", _fake_produce_all)
    with pytest.raises(RuntimeError, match="error publishing message"):
        d.flush(_QuietLogger())
    assert d._pending == [(b"k", b"v", "pk")]  # preserved for NAK/redelivery
