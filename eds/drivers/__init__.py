"""PARITY: internal/drivers — driver registration.

Go registers each driver/importer in its package init(); the Python port registers them explicitly via
register_all() (called at startup) to avoid import-time side effects. Aliases (postgresql, mssql) are wired
by register_driver via each driver's aliases().
"""

from __future__ import annotations


def register_all() -> None:
    """Register every built-in driver + importer (PostgreSQL, MySQL, SQL Server, Snowflake, Snowflake KeyPair)."""
    from eds.driver import register_driver, register_importer
    from eds.drivers.file import FileDriver
    from eds.drivers.mysql.driver import MysqlDriver
    from eds.drivers.postgresql.driver import PostgresqlDriver
    from eds.drivers.snowflake.snowflake import SnowflakeDriver
    from eds.drivers.snowflake.snowflake_keypair import SnowflakeKeyPairDriver
    from eds.drivers.sqlserver.driver import MssqlDriver

    for protocol, driver in (
        ("postgres", PostgresqlDriver()),
        ("mysql", MysqlDriver()),
        ("sqlserver", MssqlDriver()),
        ("snowflake", SnowflakeDriver()),
        ("snowflake-keypair", SnowflakeKeyPairDriver()),
        ("file", FileDriver()),
    ):
        register_driver(protocol, driver)
        register_importer(protocol, driver)
