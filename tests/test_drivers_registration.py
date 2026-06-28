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
        assert _meta("postgres://h/db").supports_migration is True  # SQL drivers support migration
        # resolves for import without connecting
        assert d.new_driver_for_import(None, None, "mysql://h/db", None, None, "/d") is not None
        # validate routes to the driver
        url, errs = d.validate("postgres", {"Database": "db", "Hostname": "h"})
        assert url == "postgres://h:5432/db"
        assert errs == []
    finally:
        d.reset_registries()
