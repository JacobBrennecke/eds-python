"""Docker-gated e2e: stream insert/update/delete into a real SQL Server via testcontainers (pymssql).

Skipped when Docker is unavailable. Exercises the pymssql adapter, the MERGE upsert, the transactional batch
flush, migration, and the resulting row state. The SQL Server image is large/slow.
"""

from __future__ import annotations

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
    # testcontainers' SqlServerContainer readiness wait uses a deprecated decorator; the project escalates
    # warnings to errors, so ignore this third-party DeprecationWarning for the e2e only.
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]

from eds.dbchange import DBChangeEvent  # noqa: E402
from eds.driver import DriverConfig  # noqa: E402
from eds.drivers.sqlserver.driver import MssqlDriver  # noqa: E402
from eds.schema import Schema, SchemaProperty  # noqa: E402
from eds.util.gojson import RawJson  # noqa: E402


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
    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    def get_table_version(self, table: str):
        return True, "v1"

    def get_schema(self, table: str, version: str) -> Schema:
        return self._schema

    def get_latest_schema(self):
        return {self._schema.table: self._schema}


def _user_schema() -> Schema:
    return Schema(
        table="user", model_version="v1", primary_keys=["id"], required=["id"],
        properties={
            "id": SchemaProperty(type="string"), "name": SchemaProperty(type="string"),
            "age": SchemaProperty(type="integer"),
        },
    )


def test_streams_insert_update_delete_into_sqlserver() -> None:
    from testcontainers.mssql import SqlServerContainer

    password = "Strong!Passw0rd"
    with SqlServerContainer(password=password) as ms:
        url = (
            f"sqlserver://{ms.username}:{password}@"
            f"{ms.get_container_host_ip()}:{ms.get_exposed_port(1433)}/{ms.dbname}"
        )
        log = _QuietLogger()
        schema = _user_schema()
        driver = MssqlDriver()
        driver.start(DriverConfig(url=url, logger=log, schema_registry=_FakeRegistry(schema), context=None))
        try:
            driver.migrate_new_table(None, log, schema)

            driver.process(log, DBChangeEvent(
                operation="INSERT", table="user", key=["u1"],
                after=RawJson('{"id":"u1","name":"Alice","age":30}')))
            driver.process(log, DBChangeEvent(
                operation="INSERT", table="user", key=["u2"],
                after=RawJson('{"id":"u2","name":"Bob","age":40}')))
            driver.flush(log)

            driver.process(log, DBChangeEvent(
                operation="UPDATE", table="user", key=["u1"], diff=["name"],
                after=RawJson('{"id":"u1","name":"Alice2","age":30}')))
            driver.flush(log)
            driver.process(log, DBChangeEvent(operation="DELETE", table="user", key=["u2"]))
            driver.flush(log)

            import pymssql

            conn = pymssql.connect(
                server=ms.get_container_host_ip(), port=int(ms.get_exposed_port(1433)),
                user=ms.username, password=password, database=ms.dbname,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT [id],[name],[age] FROM [user] ORDER BY [id]")
                    rows = cur.fetchall()
            finally:
                conn.close()
            assert rows == [("u1", "Alice2", 30)]
        finally:
            driver.stop()
