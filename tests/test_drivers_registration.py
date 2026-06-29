"""PARITY: driver registration — register_all wires every SQL driver + its aliases."""

from __future__ import annotations

from eds import driver as d
from eds.drivers import register_all


def _meta(url):
    m = d.get_driver_metadata_for_url(url)
    assert m is not None
    return m


def test_register_all_resolves_drivers_and_aliases() -> None:
    d.reset_registries()
    try:
        register_all()
        assert _meta("postgres://h/db").name == "PostgreSQL"
        assert _meta("postgresql://h/db").name == "PostgreSQL"  # alias
        assert _meta("mysql://h/db").name == "MySQL"
        assert _meta("sqlserver://h/db").name == "Microsoft SQL Server"
        assert _meta("mssql://h/db").name == "Microsoft SQL Server"  # alias
        assert _meta("snowflake://h/db").name == "Snowflake [DEPRECATED]"
        assert _meta("snowflake-keypair://h/db").name == "Snowflake Key Pair"
        assert _meta("file://folder").name == "File"
        assert _meta("file://folder").supports_migration is False  # File driver has no migration
        assert _meta("postgres://h/db").supports_migration is True  # SQL drivers support migration
        # resolves for import without connecting
        assert d.new_driver_for_import(None, None, "mysql://h/db", None, None, "/d") is not None
        # validate routes to the driver
        url, errs = d.validate("postgres", {"Database": "db", "Hostname": "h"})
        assert url == "postgres://h:5432/db"
        assert errs == []
    finally:
        d.reset_registries()


def test_register_all_includes_streaming_drivers() -> None:
    # PARITY: register_all wires all 9 Go schemes — the 3 streaming drivers (s3/kafka/eventhub) too.
    d.reset_registries()
    try:
        register_all()
        configs = d.get_driver_configurations()
        assert set(configs) == {
            "postgres", "mysql", "sqlserver", "snowflake", "snowflake-keypair",
            "file", "s3", "kafka", "eventhub",
        }
        assert _meta("s3://bucket").name == "AWS S3"
        assert _meta("kafka://h:9092/t").name == "Kafka"
        assert _meta("eventhub://h.servicebus.windows.net/;EntityPath=e").name == "Microsoft Azure EventHub"
        # all three are importers and none support migration
        for url in ("s3://bucket", "kafka://h:9092/t", "eventhub://h.servicebus.windows.net/;EntityPath=e"):
            m = _meta(url)
            assert m.supports_import is True
            assert m.supports_migration is False
        # streaming drivers register a SEPARATE instance per registry (driver vs importer)
        drv = d._resolve_driver("s3")
        imp = d.new_importer(None, None, "s3://bucket", None)
        assert drv is not imp
    finally:
        d.reset_registries()
