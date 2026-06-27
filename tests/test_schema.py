"""PARITY: internal/schema.go — Schema.columns, SchemaProperty helpers, DatabaseSchema."""

from __future__ import annotations

from eds.schema import DatabaseSchema, Schema, SchemaProperty


def _user_schema() -> Schema:
    return Schema(
        table="user",
        model_version="v1",
        primary_keys=["id"],
        required=["id"],
        properties={
            "id": SchemaProperty(type="string"),
            "name": SchemaProperty(type="string"),
            "age": SchemaProperty(type="integer"),
            "meta": SchemaProperty(type="object"),
        },
    )


def test_columns_primary_keys_first_then_sorted() -> None:
    # PARITY: matches the Go postgres CreateSql column order (id, age, meta, name).
    assert _user_schema().columns() == ["id", "age", "meta", "name"]


def test_columns_cached() -> None:
    s = _user_schema()
    first = s.columns()
    assert s.columns() is first  # cached (same list object)


def test_is_not_null() -> None:
    assert SchemaProperty(type="string", nullable=False).is_not_null() is True
    assert SchemaProperty(type="string", nullable=True).is_not_null() is False
    # arrays are always not-null, even when nullable
    assert SchemaProperty(type="array", nullable=True).is_not_null() is True


def test_is_array_or_json() -> None:
    assert SchemaProperty(type="object").is_array_or_json() is True
    assert SchemaProperty(type="array").is_array_or_json() is True
    assert SchemaProperty(type="string").is_array_or_json() is False


def test_database_schema() -> None:
    db = DatabaseSchema({"user": {"id": "text", "name": "text"}})
    assert db.columns("user") == ["id", "name"]
    assert db.columns("missing") == []
    assert db.get_type("user", "id") == (True, "text")
    assert db.get_type("user", "nope") == (False, "")
    assert db.get_type("missing", "id") == (False, "")


def test_schema_from_dict() -> None:
    s = Schema.from_dict(
        {
            "table": "user",
            "modelVersion": "v2",
            "primaryKeys": ["id"],
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}, "nullable": True},
            },
        }
    )
    assert s.table == "user"
    assert s.model_version == "v2"
    assert s.primary_keys == ["id"]
    assert s.properties["tags"].type == "array"
    assert s.properties["tags"].items is not None
    assert s.properties["tags"].items.type == "string"
    assert s.columns() == ["id", "tags"]
