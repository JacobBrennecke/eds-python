"""PARITY: internal/importer/importer.go Run — synthetic-event build + the gz→files e2e (Go + C# vectors)."""

from __future__ import annotations

import gzip

from eds.driver import ImporterConfig
from eds.drivers.file import FileDriver
from eds.importer import run as importer_run
from eds.schema import Schema, SchemaProperty
from eds.util.hash import hash as eds_hash

_GZ = "202407242003015854988560000000000-abc-def-customer-2.ndjson.gz"
_ROWS = '{"id":"c1","companyId":"comp1"}\n{"id":"c2"}\n'


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _FakeRegistry:
    def __init__(self, schema_map) -> None:
        self._m = schema_map

    def get_latest_schema(self):
        return self._m


class _CapHandler:
    def __init__(self) -> None:
        self.created = None
        self.events: list = []
        self.completed = False

    def create_datasource(self, schema) -> None:
        self.created = schema

    def import_event(self, event, schema) -> None:
        self.events.append(event)

    def import_completed(self) -> None:
        self.completed = True


def _customer_schema() -> Schema:
    return Schema(
        table="customer", model_version="v1", primary_keys=["id"],
        properties={"id": SchemaProperty(type="string"), "companyId": SchemaProperty(type="string")},
    )


def _write_gz(indir) -> None:
    with gzip.open(indir / _GZ, "wt", encoding="utf-8") as f:
        f.write(_ROWS)


def test_run_builds_synthetic_events(tmp_path) -> None:
    indir = tmp_path / "in"
    indir.mkdir()
    _write_gz(indir)
    reg = _FakeRegistry({"customer": _customer_schema()})
    config = ImporterConfig(logger=_QuietLogger(), schema_registry=reg, data_dir=str(indir), tables=["customer"])
    h = _CapHandler()
    importer_run(_QuietLogger(), config, h)

    assert h.created is not None  # no_delete=False -> create_datasource ran
    assert h.completed is True
    assert len(h.events) == 2
    e1, e2 = h.events
    assert e1.operation == "INSERT"
    assert e1.table == "customer"
    assert e1.key == ["c1"]
    assert e1.location_id == "comp1"  # companyId -> locationId quirk
    assert e1.user_id is None
    assert e1.imported is True
    assert e1.timestamp == 1721851381585
    assert e1.mvcc_timestamp == "1721851381585498856"  # full nanosecond precision
    assert e1.id == eds_hash(_GZ)
    assert e2.key == ["c2"]
    assert e2.location_id is None  # no companyId in the row


def test_run_skips_table_not_in_config(tmp_path) -> None:
    indir = tmp_path / "in"
    indir.mkdir()
    _write_gz(indir)
    reg = _FakeRegistry({"customer": _customer_schema()})
    config = ImporterConfig(logger=_QuietLogger(), schema_registry=reg, data_dir=str(indir), tables=["other"])
    h = _CapHandler()
    importer_run(_QuietLogger(), config, h)
    assert h.events == []  # customer not in tables -> silently skipped


def test_run_schema_only_returns_after_create(tmp_path) -> None:
    indir = tmp_path / "in"
    indir.mkdir()
    _write_gz(indir)
    reg = _FakeRegistry({"customer": _customer_schema()})
    config = ImporterConfig(
        logger=_QuietLogger(), schema_registry=reg, data_dir=str(indir), tables=["customer"], schema_only=True
    )
    h = _CapHandler()
    importer_run(_QuietLogger(), config, h)
    assert h.created is not None  # create_datasource ran
    assert h.events == []  # but no events (schema_only)


def test_file_driver_import_e2e(tmp_path) -> None:
    indir = tmp_path / "in"
    indir.mkdir()
    outdir = tmp_path / "out"
    outdir.mkdir()
    _write_gz(indir)
    reg = _FakeRegistry({"customer": _customer_schema()})
    driver = FileDriver()
    driver.run_import(ImporterConfig(
        url="file://" + str(outdir).replace("\\", "/"), logger=_QuietLogger(),
        schema_registry=reg, data_dir=str(indir), tables=["customer"],
    ))
    cust = outdir / "customer"
    assert sorted(p.name for p in cust.iterdir()) == ["1721851381-c1.json", "1721851381-c2.json"]
    eid = eds_hash(_GZ)
    c1 = (cust / "1721851381-c1.json").read_text(encoding="utf-8")
    c2 = (cust / "1721851381-c2.json").read_text(encoding="utf-8")
    assert c1 == (
        '{"operation":"INSERT","id":"' + eid + '","table":"customer","key":["c1"],"modelVersion":"v1",'
        '"locationId":"comp1","after":{"id":"c1","companyId":"comp1"},'
        '"timestamp":1721851381585,"mvccTimestamp":"1721851381585498856","imported":true}'
    )
    assert c2 == (
        '{"operation":"INSERT","id":"' + eid + '","table":"customer","key":["c2"],"modelVersion":"v1",'
        '"after":{"id":"c2"},"timestamp":1721851381585,"mvccTimestamp":"1721851381585498856","imported":true}'
    )
