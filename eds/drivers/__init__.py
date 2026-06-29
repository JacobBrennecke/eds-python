"""PARITY: internal/drivers — driver registration.

Go registers each driver/importer in its package init(); the Python port registers them explicitly via
register_all() (called at startup) to avoid import-time side effects. Aliases (postgresql, mssql) are wired
by register_driver via each driver's aliases().
"""

from __future__ import annotations


def register_all() -> None:
    """Register every built-in driver + importer (the 9 Go schemes: PostgreSQL, MySQL, SQL Server, Snowflake,
    Snowflake KeyPair, File, S3, Kafka, EventHub)."""
    from eds.driver import register_driver, register_importer
    from eds.drivers.eventhub import EventHubDriver
    from eds.drivers.file import FileDriver
    from eds.drivers.kafka import KafkaDriver
    from eds.drivers.mysql.driver import MysqlDriver
    from eds.drivers.postgresql.driver import PostgresqlDriver
    from eds.drivers.s3 import S3Driver
    from eds.drivers.snowflake.snowflake import SnowflakeDriver
    from eds.drivers.snowflake.snowflake_keypair import SnowflakeKeyPairDriver
    from eds.drivers.sqlserver.driver import MssqlDriver

    for protocol, cls in (
        ("postgres", PostgresqlDriver),
        ("mysql", MysqlDriver),
        ("sqlserver", MssqlDriver),
        ("snowflake", SnowflakeDriver),
        ("snowflake-keypair", SnowflakeKeyPairDriver),
        ("file", FileDriver),
        ("s3", S3Driver),
        ("kafka", KafkaDriver),
        ("eventhub", EventHubDriver),
    ):
        # PARITY: Go registers a SEPARATE instance in each registry (driver vs importer), so streaming and
        # import don't share mutable state on one object.
        register_driver(protocol, cls())
        register_importer(protocol, cls())
