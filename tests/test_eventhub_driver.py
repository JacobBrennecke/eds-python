"""PARITY: internal/drivers/eventhub/eventhub_test.go — Validate golden vectors + the pure connection-string /
key / partition-key / batch-coalescing logic. No e2e (no local emulator); the SDK send is the only untested
binding. The pure tests need no azure-eventhub (lazy import); the flush test is guarded by importorskip."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.driver import ImporterConfig
from eds.drivers.eventhub import (
    EventHubDriver,
    get_keys,
    new_partition_key,
    parse_connection_string,
    plan_batches,
    str_with_def,
    validate_config,
)
from eds.util.batcher import Batcher
from eds.util.gojson import RawJson


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


def test_parse_connection_string() -> None:
    # PARITY: ParseConnectionString — scheme → sb, "Endpoint=" prefix.
    assert (
        parse_connection_string("eventhub://h.servicebus.windows.net/;EntityPath=e")
        == "Endpoint=sb://h.servicebus.windows.net/;EntityPath=e"
    )


def test_new_partition_key() -> None:
    assert new_partition_key("t", None, None, "id1") == "t.NONE.NONE.id1"
    assert new_partition_key("t", "c1", "l1", "id1") == "t.c1.l1.id1"


def test_get_keys() -> None:
    key, pkey = get_keys("t", "INSERT", "c1", "l1", "id1")
    assert key == "dbchange.t.INSERT.c1.l1.id1"
    assert pkey == "t.c1.l1.id1"
    key, pkey = get_keys("t", "INSERT", "", "", "id1")
    assert key == "dbchange.t.INSERT.NONE.NONE.id1"
    assert pkey == "t.NONE.NONE.id1"


# ---- Validate (PARITY: TestValidate) ----
def test_validate_valid_connection_string() -> None:
    cs = ("Endpoint=sb://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;"
          "SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test")
    url, errs = validate_config({"Connection String": cs})
    assert errs == []
    assert url == ("eventhub://shopmonkey-xx-test.servicebus.windows.net/;SharedAccessKeyName=send;"
                   "SharedAccessKey=x/x+x+x+x=;EntityPath=shopmonkey-eds-test")


def test_validate_missing_field() -> None:
    url, errs = validate_config({"Format": "json"})
    assert url == ""
    assert len(errs) >= 1


def test_validate_no_endpoint_prefix() -> None:
    url, errs = validate_config({"Connection String": "sb://host/;EntityPath=e"})
    assert url == ""
    assert "Endpoint=" in errs[0].message


def test_validate_no_scheme() -> None:
    url, errs = validate_config({"Connection String": "Endpoint=host"})
    assert url == ""
    assert "url scheme" in errs[0].message


# ---- metadata ----
def test_metadata() -> None:
    d = EventHubDriver()
    assert d.name() == "Microsoft Azure EventHub"
    assert d.description() == "Supports streaming EDS messages to a Microsoft Azure EventHub."
    assert d.example_url().startswith("eventhub://my-eventhub.servicebus.windows.net/")
    assert d.max_batch_size() == -1
    assert d.supports_delete() is False
    assert [f.name for f in d.configuration()] == ["Connection String"]


def test_help_has_sections() -> None:
    h = EventHubDriver().help()
    assert "Partitioning" in h
    assert "Message Value" in h


# ---- batch coalescing (PARITY: Flush grouping) ----
def _event(pk: str, company: str | None) -> DBChangeEvent:
    obj = f'{{"id":"{pk}"'
    if company is not None:
        obj += f',"companyId":"{company}"'
    obj += "}"
    return DBChangeEvent(operation="INSERT", table="customer", key=[pk], after=RawJson(obj))


def test_plan_batches_coalesces_consecutive_only() -> None:
    b = Batcher()
    b.add(_event("c1", "comp1"))  # group A
    b.add(_event("c1", "comp1"))  # same pkey -> group A
    b.add(_event("c2", "comp1"))  # new pkey -> group B
    b.add(_event("c1", "comp1"))  # pkey reappears AFTER B -> new group C
    groups = plan_batches(b.records())
    assert [g.partition_key for g in groups] == [
        "customer.comp1.NONE.c1", "customer.comp1.NONE.c2", "customer.comp1.NONE.c1",
    ]
    assert [len(g.events) for g in groups] == [2, 1, 1]
    # each event carries its objectId key
    key0, _ = groups[0].events[0]
    _, object_id = groups[0].events[0]
    assert object_id == "dbchange.customer.INSERT.comp1.NONE.c1"


def test_plan_batches_reads_company_from_object_not_event_field() -> None:
    # PARITY: Flush reads companyId/locationId from the parsed Object, not the event's CompanyID field.
    b = Batcher()
    evt = DBChangeEvent(operation="INSERT", table="t", key=["k"], company_id="IGNORED",
                        after=RawJson('{"id":"k","companyId":"fromobj"}'))
    b.add(evt)
    groups = plan_batches(b.records())
    assert groups[0].partition_key == "t.fromobj.NONE.k"


# ---- streaming / importer state ----
def test_process_batches_event() -> None:
    d = EventHubDriver()
    assert d.process(_QuietLogger(), _event("c1", "comp1")) is False
    assert len(d._batcher) == 1


def test_import_event_flushes_at_threshold(monkeypatch) -> None:
    d = EventHubDriver()
    d._logger = _QuietLogger()
    flushed: list[int] = []
    monkeypatch.setattr(d, "flush", lambda logger: flushed.append(len(d._batcher)))
    for i in range(100):
        d.import_event(_event(f"c{i}", "comp1"), None)
    assert flushed == [100]  # flush triggered exactly at the 100-event threshold


def test_import_schema_only_returns_without_connect() -> None:
    d = EventHubDriver()
    d.run_import(ImporterConfig(
        url="eventhub://h.servicebus.windows.net/;EntityPath=e", logger=_QuietLogger(), schema_only=True
    ))
    assert d._producer is None


# ---- flush against a fake producer (needs azure EventData; guarded) ----
def test_flush_creates_one_batch_per_group() -> None:
    pytest.importorskip("azure.eventhub")

    class _FakeBatch:
        def __init__(self, partition_key):
            self.partition_key = partition_key
            self.events: list = []

        def add(self, data):
            self.events.append(data)

        def __len__(self):
            return len(self.events)

        @property
        def size_in_bytes(self):
            return 0

    class _FakeProducer:
        def __init__(self):
            self.created: list[_FakeBatch] = []
            self.sent: list[_FakeBatch] = []

        def create_batch(self, partition_key=None):
            b = _FakeBatch(partition_key)
            self.created.append(b)
            return b

        def send_batch(self, batch):
            self.sent.append(batch)

    d = EventHubDriver()
    d._logger = _QuietLogger()
    producer = _FakeProducer()
    d._producer = producer
    d.process(_QuietLogger(), _event("c1", "comp1"))
    d.process(_QuietLogger(), _event("c1", "comp1"))
    d.process(_QuietLogger(), _event("c2", "comp1"))
    d.flush(_QuietLogger())
    assert [b.partition_key for b in producer.created] == [
        "customer.comp1.NONE.c1", "customer.comp1.NONE.c2",
    ]
    assert [len(b) for b in producer.created] == [2, 1]
    assert producer.sent == producer.created
    # batcher drained
    assert len(d._batcher) == 0
