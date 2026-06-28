"""PARITY: the --schema-validator directory loader (util.NewSchemaValidator + Validate).

Exercises cross-$ref resolution across base/ + models/ subdirs (file:/// URIs), validation pass/fail, and the
Go-template path rendering, using a fixture that mirrors the Go testdata layout."""

from __future__ import annotations

import os

import pytest

from eds.dbchange import DBChangeEvent
from eds.schema import SchemaValidationError
from eds.util.gojson import RawJson
from eds.util.schema import new_schema_validator

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "schema")


def _evt(**kw) -> DBChangeEvent:
    base = dict(operation="INSERT", id="lab1", table="labor", location_id="loc1", mvcc_timestamp="167.5")
    base.update(kw)
    return DBChangeEvent(**base)


def test_valid_event_passes_and_renders_path() -> None:
    validator = new_schema_validator(FIXTURE)
    found, valid, path = validator.validate(_evt(after=RawJson('{"id":"x1"}')))
    assert found is True and valid is True
    assert path == "labor/received/labor_167.5_lab1.json"  # Go-template {{.table}}/{{.mvccTimestamp}}/{{.id}}


def test_unknown_table_not_found() -> None:
    validator = new_schema_validator(FIXTURE)
    assert validator.validate(_evt(table="orders")) == (False, False, "")  # no rule → skipped upstream


def test_missing_required_field_raises() -> None:
    validator = new_schema_validator(FIXTURE)
    # locationId is required by the base message ($ref) — its absence is a schema miss
    with pytest.raises(SchemaValidationError):
        validator.validate(_evt(location_id=None))


def test_nested_ref_validates_after_model() -> None:
    validator = new_schema_validator(FIXTURE)
    # after must satisfy models/labor.json (requires id) — an empty after object fails
    with pytest.raises(SchemaValidationError):
        validator.validate(_evt(after=RawJson("{}")))


def test_missing_config_raises() -> None:
    with pytest.raises(RuntimeError, match="config.json"):
        new_schema_validator(os.path.join(FIXTURE, "base"))  # a dir with no config.json


def test_missing_schema_file_raises(tmp_path) -> None:
    (tmp_path / "config.json").write_text('{"labor": {"schema": "nope.json"}}')
    with pytest.raises(RuntimeError, match="schema file not found"):
        new_schema_validator(str(tmp_path))


def test_rejects_malformed_schema_at_load(tmp_path) -> None:
    # PARITY: Go's compiler.Compile validates the schema structure eagerly → fail fast at load, not per-event.
    (tmp_path / "config.json").write_text('{"t": {"schema": "bad.json"}}')
    (tmp_path / "bad.json").write_text('{"type": 12345}')  # invalid JSON Schema
    with pytest.raises(RuntimeError):
        new_schema_validator(str(tmp_path))


def test_rejects_unresolvable_ref_at_load(tmp_path) -> None:
    # PARITY: Go resolves every $ref at load; an unresolvable ref must fail at load (else the consumer would
    # silently skip+ack every event for that table — a data-loss risk).
    (tmp_path / "config.json").write_text('{"t": {"schema": "root.json"}}')
    (tmp_path / "root.json").write_text(
        '{"$schema":"http://json-schema.org/draft-07/schema#","$ref":"file:///models/MISSING.json"}'
    )
    with pytest.raises(RuntimeError):
        new_schema_validator(str(tmp_path))


def test_skips_ds_store(tmp_path) -> None:
    # PARITY: Go's ListDir skips .DS_Store; a Finder artifact must not be json.load'd (which would abort the loader).
    (tmp_path / "config.json").write_text('{"t": {"schema": "s.json"}}')
    (tmp_path / "s.json").write_text('{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}')
    (tmp_path / ".DS_Store").write_bytes(b"\x00\x01\x02 binary finder junk")
    new_schema_validator(str(tmp_path))  # must not raise
