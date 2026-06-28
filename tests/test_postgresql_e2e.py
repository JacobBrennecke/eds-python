"""Docker-gated e2e: stream insert/update/delete into a real PostgreSQL via testcontainers.

Mirrors the C# PostgresqlDriverTests.Streams_insert_update_delete_into_postgres. Skipped when Docker is
unavailable. Exercises the real connect path (GoUrl connstring → psycopg), migration, the transactional
multi-statement flush, and the resulting row state.
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


pytestmark = pytest.mark.skipif(not _docker_up(), reason="Docker not available")

from eds.dbchange import DBChangeEvent  # noqa: E402
from eds.driver import DriverConfig, ImporterConfig  # noqa: E402
from eds.drivers.postgresql.driver import PostgresqlDriver  # noqa: E402
from eds.drivers.postgresql.sql import get_connection_string_from_url  # noqa: E402
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


def test_streams_insert_update_delete_into_postgres() -> None:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = (
            f"postgres://{pg.username}:{pg.password}@"
            f"{pg.get_container_host_ip()}:{pg.get_exposed_port(5432)}/{pg.dbname}"
        )
        log = _QuietLogger()
        schema = _user_schema()
        driver = PostgresqlDriver()
        driver.start(DriverConfig(url=url, logger=log, schema_registry=_FakeRegistry(schema), context=None))
        try:
            driver.migrate_new_table(None, log, schema)

            # Two inserts in one batch (multi-statement transactional flush).
            driver.process(log, DBChangeEvent(
                operation="INSERT", table="user", key=["u1"],
                after=RawJson('{"id":"u1","name":"Alice","age":30}')))
            driver.process(log, DBChangeEvent(
                operation="INSERT", table="user", key=["u2"],
                after=RawJson('{"id":"u2","name":"Bob","age":40}')))
            driver.flush(log)

            # Update u1 (diff-only upsert), then delete u2.
            driver.process(log, DBChangeEvent(
                operation="UPDATE", table="user", key=["u1"], diff=["name"],
                after=RawJson('{"id":"u1","name":"Alice2","age":30}')))
            driver.flush(log)
            driver.process(log, DBChangeEvent(operation="DELETE", table="user", key=["u2"]))
            driver.flush(log)

            import psycopg

            with psycopg.connect(get_connection_string_from_url(url)) as conn:
                rows = conn.execute('SELECT "id","name","age" FROM "user" ORDER BY "id"').fetchall()
            assert rows == [("u1", "Alice2", 30)]
        finally:
            driver.stop()


def _customer_schema() -> Schema:
    return Schema(
        table="customer", model_version="v1", primary_keys=["id"],
        properties={
            "id": SchemaProperty(type="string"), "companyId": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
        },
    )


def test_imports_gzipped_ndjson_into_postgres(tmp_path) -> None:
    # Exercises the SQL import path: run_import -> importer.run -> create_datasource / import_event
    # (byte-batched) / import_completed, against a real PostgreSQL.
    import gzip

    from testcontainers.postgres import PostgresContainer

    indir = tmp_path / "in"
    indir.mkdir()
    gzname = "202407242003015854988560000000000-abc-def-customer-2.ndjson.gz"
    with gzip.open(indir / gzname, "wt", encoding="utf-8") as f:
        f.write('{"id":"c1","companyId":"comp1"}\n{"id":"c2"}\n')

    with PostgresContainer("postgres:16-alpine") as pg:
        url = (
            f"postgres://{pg.username}:{pg.password}@"
            f"{pg.get_container_host_ip()}:{pg.get_exposed_port(5432)}/{pg.dbname}"
        )
        driver = PostgresqlDriver()
        driver.run_import(ImporterConfig(
            url=url, logger=_QuietLogger(), schema_registry=_FakeRegistry(_customer_schema()),
            data_dir=str(indir), tables=["customer"],
        ))
        import psycopg

        with psycopg.connect(get_connection_string_from_url(url)) as conn:
            rows = conn.execute('SELECT "id","companyId" FROM "customer" ORDER BY "id"').fetchall()
        assert rows == [("c1", "comp1"), ("c2", None)]
