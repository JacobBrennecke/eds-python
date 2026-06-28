"""PARITY: internal/schema.go — schema metadata + the registry/validator protocols.

``UpdateDestinationSchema`` (Go schema.go) is startup-migration logic that needs a live driver +
registry; it is ported in ``eds.migration`` at M5.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from eds.util.gojson import marshal

if TYPE_CHECKING:
    from eds.dbchange import DBChangeEvent


@dataclass
class ItemsType:
    """PARITY: schema.go ItemsType."""

    type: str = ""
    enum: list[str] | None = None
    format: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> ItemsType:
        return cls(type=d.get("type", ""), enum=d.get("enum"), format=d.get("format", ""))

    def __gojson__(self) -> str:
        # PARITY: schema.go ItemsType struct marshaling — declaration order + omitempty.
        parts = ['"type":' + marshal(self.type)]
        if self.enum:
            parts.append('"enum":' + marshal(self.enum))
        if self.format:
            parts.append('"format":' + marshal(self.format))
        return "{" + ",".join(parts) + "}"


@dataclass
class SchemaProperty:
    """PARITY: schema.go SchemaProperty."""

    type: str = ""
    format: str = ""
    nullable: bool = False
    items: ItemsType | None = None
    additional_properties: bool | None = None  # json: additionalProperties
    comment: str | None = None  # json: $comment
    deprecated: bool | None = None

    def is_not_null(self) -> bool:
        """PARITY: SchemaProperty.IsNotNull — not nullable, or an array."""
        return not self.nullable or self.type == "array"

    def is_array_or_json(self) -> bool:
        """PARITY: SchemaProperty.IsArrayOrJSON."""
        return self.type in ("object", "array")

    @classmethod
    def from_dict(cls, d: dict) -> SchemaProperty:
        items = d.get("items")
        return cls(
            type=d.get("type", ""),
            format=d.get("format", ""),
            nullable=bool(d.get("nullable", False)),
            items=ItemsType.from_dict(items) if isinstance(items, dict) else None,
            additional_properties=d.get("additionalProperties"),
            comment=d.get("$comment"),
            deprecated=d.get("deprecated"),
        )

    def __gojson__(self) -> str:
        # PARITY: schema.go SchemaProperty struct marshaling — declaration order + omitempty.
        parts = ['"type":' + marshal(self.type)]
        if self.format:
            parts.append('"format":' + marshal(self.format))
        if self.nullable:
            parts.append('"nullable":' + marshal(self.nullable))
        if self.items is not None:
            parts.append('"items":' + self.items.__gojson__())
        if self.additional_properties is not None:
            parts.append('"additionalProperties":' + marshal(self.additional_properties))
        if self.comment is not None:
            parts.append('"$comment":' + marshal(self.comment))
        if self.deprecated is not None:
            parts.append('"deprecated":' + marshal(self.deprecated))
        return "{" + ",".join(parts) + "}"


@dataclass
class Schema:
    """PARITY: schema.go Schema."""

    properties: dict[str, SchemaProperty] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    table: str = ""
    model_version: str = ""
    _columns: list[str] | None = field(default=None, repr=False, compare=False)

    def columns(self) -> list[str]:
        """PARITY: Schema.Columns — primary keys (in order) followed by the remaining property names
        sorted lexicographically. Cached after first call."""
        if self._columns is not None:
            return self._columns
        rest = sorted(name for name in self.properties if name not in self.primary_keys)
        self._columns = list(self.primary_keys) + rest
        return self._columns

    def __gojson__(self) -> str:
        # PARITY: schema.go Schema struct marshaling — declaration order (properties, required, primaryKeys,
        # table, modelVersion); no omitempty; the cached `_columns` is excluded (json:"-"). `properties` is a
        # map → marshaled with sorted keys.
        return (
            '{"properties":' + marshal(self.properties)
            + ',"required":' + marshal(self.required)
            + ',"primaryKeys":' + marshal(self.primary_keys)
            + ',"table":' + marshal(self.table)
            + ',"modelVersion":' + marshal(self.model_version)
            + "}"
        )

    @classmethod
    def from_dict(cls, d: dict) -> Schema:
        props = d.get("properties") or {}
        return cls(
            properties={k: SchemaProperty.from_dict(v) for k, v in props.items()},
            required=list(d.get("required") or []),
            primary_keys=list(d.get("primaryKeys") or []),
            table=d.get("table", ""),
            model_version=d.get("modelVersion", ""),
        )


# PARITY: schema.go SchemaMap = map[string]*Schema.
SchemaMap = dict[str, Schema]


class DatabaseSchema(dict[str, dict[str, str]]):
    """PARITY: schema.go DatabaseSchema — table -> column -> column type."""

    def columns(self, table: str) -> list[str]:
        """PARITY: DatabaseSchema.Columns — sorted column names for a table (empty if unknown)."""
        cols = self.get(table)
        return sorted(cols) if cols is not None else []

    def get_type(self, table: str, column: str) -> tuple[bool, str]:
        """PARITY: DatabaseSchema.GetType — (found, type)."""
        cols = self.get(table)
        if cols is not None and column in cols:
            return True, cols[column]
        return False, ""


class SchemaRegistry(Protocol):
    """PARITY: schema.go SchemaRegistry. Go's (…, error) returns become raise-on-error in Python."""

    def get_latest_schema(self) -> SchemaMap: ...
    def get_schema(self, table: str, version: str) -> Schema: ...
    def get_table_version(self, table: str) -> tuple[bool, str]: ...
    def set_table_version(self, table: str, version: str) -> None: ...
    def close(self) -> None: ...


class SchemaValidationError(Exception):
    """PARITY: util.ErrSchemaValidation — raised by SchemaValidator.validate on a JSON-schema mismatch (Go:
    errors.Join(ErrSchemaValidation, *js.ValidationError)). The importer treats this as a skip; any OTHER
    exception is an internal validator error that aborts the run."""


class SchemaValidator(Protocol):
    """PARITY: schema.go SchemaValidator — (found, valid, transformed-path); raises SchemaValidationError on
    a mismatch (skip) or another exception on an internal error (abort)."""

    def validate(self, event: DBChangeEvent) -> tuple[bool, bool, str]: ...
