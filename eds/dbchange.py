"""PARITY: internal/dbchange.go — the DBChangeEvent domain spine."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from eds.util.gojson import RawJson, marshal
from eds.util.gostruct import OmitEmpty, gojson_struct


@dataclass
class DBChangeEvent:
    """PARITY: dbchange.go DBChangeEvent. JSON (struct) serialization is declaration-order + omitempty
    via __gojson__ — NOT sorted like a map. before/after (json.RawMessage) are compacted + HTML-escaped on
    emit (compact_raw), exactly as Go's json.Marshal renders a RawMessage field."""

    operation: str = field(default="", metadata={"json": "operation"})
    id: str = field(default="", metadata={"json": "id"})
    table: str = field(default="", metadata={"json": "table"})
    # Go []string, no omitempty: nil -> "null", [] -> "[]" (NEVER).
    key: list[str] | None = field(default=None, metadata={"json": "key"})
    model_version: str = field(default="", metadata={"json": "modelVersion"})
    company_id: str | None = field(default=None, metadata={"json": "companyId", "omit": OmitEmpty.IF_NONE})
    location_id: str | None = field(default=None, metadata={"json": "locationId", "omit": OmitEmpty.IF_NONE})
    user_id: str | None = field(default=None, metadata={"json": "userId", "omit": OmitEmpty.IF_NONE})
    # before/after (json.RawMessage): omit when None or empty; marshal(RawJson) compacts+HTML-escapes via compact_raw.
    before: RawJson | None = field(default=None, metadata={"json": "before", "omit": OmitEmpty.IF_EMPTY_RAW})
    after: RawJson | None = field(default=None, metadata={"json": "after", "omit": OmitEmpty.IF_EMPTY_RAW})
    diff: list[str] | None = field(default=None, metadata={"json": "diff", "omit": OmitEmpty.IF_FALSY})
    timestamp: int = field(default=0, metadata={"json": "timestamp"})
    mvcc_timestamp: str = field(default="", metadata={"json": "mvccTimestamp"})
    imported: bool = field(default=False, metadata={"json": "imported", "omit": OmitEmpty.IF_FALSY})

    # Not serialized (Go json:"-" / unexported):
    nats_msg: object = field(default=None, repr=False, compare=False)
    schema_validated_path: str | None = field(default=None, compare=False)
    _object: dict | None = field(default=None, repr=False, compare=False)

    def __str__(self) -> str:
        """PARITY: DBChangeEvent.String."""
        return f"DBChangeEvent[op={self.operation},table={self.table},id={self.id},pk={self.get_primary_key()}]"

    def get_primary_key(self) -> str:
        """PARITY: GetPrimaryKey — last key element, else object["id"] if a string, else ""."""
        if self.key:
            return self.key[-1]
        obj = self.get_object()
        if obj is not None:
            v = obj.get("id")
            if isinstance(v, str):
                return v
        return ""

    def get_object(self) -> dict | None:
        """PARITY: GetObject — lazily parse After (then Before) into a map, cached.

        Numbers are parsed as float to mirror Go's ``map[string]any`` (json numbers → float64). A present
        JSON null yields no object; a present non-object (e.g. an array) raises (Go's Unmarshal-into-map error)."""
        if self.after is not None and len(self.after.value) > 0:
            return self._parse_object(self.after.value)
        if self.before is not None and len(self.before.value) > 0:
            return self._parse_object(self.before.value)
        return None

    def _parse_object(self, raw: str) -> dict | None:
        if self._object is None:
            parsed = json.loads(raw, parse_int=float)
            if parsed is None:
                return None  # JSON null -> no object
            if not isinstance(parsed, dict):
                raise ValueError("before/after is malformed")
            self._object = parsed
        return self._object

    def omit_properties(self, *props: str) -> None:
        """PARITY: OmitProperties — drop properties from the parsed object (the raw before/after are
        left untouched, exactly as Go modifies only c.object)."""
        obj = self.get_object()
        if obj is not None:
            for prop in props:
                obj.pop(prop, None)

    def to_json(self) -> str:
        """Go json.Marshal of this event (declaration field order + omitempty)."""
        return self.__gojson__()

    def __gojson__(self) -> str:
        # PARITY: declaration order + per-field omitempty; before/after RawJson route through marshal→compact_raw
        # (compacted + HTML-escaped, NOT emitted verbatim).
        return gojson_struct(self)

    @classmethod
    def from_message(cls, data: bytes, seq: int = 0) -> DBChangeEvent:
        """PARITY: dbchange.go DBChangeEventFromMessage — unmarshal, validate before/after is parseable,
        and require a non-empty primary key. ``seq`` is the consumer sequence (for the error messages).
        Takes raw bytes + seq (the jetstream.Msg wrapper is added at M5 when NATS is wired)."""
        raw = data.decode("utf-8", errors="replace")
        try:
            m = json.loads(data)
        except ValueError as e:
            raise ValueError(
                f"error unmarshalling message into DBChangeEvent: {e} (seq:{seq}) raw message:\n{raw}"
            ) from e
        if not isinstance(m, dict):
            raise ValueError(
                f"error unmarshalling message into DBChangeEvent: not an object (seq:{seq}) raw message:\n{raw}"
            )

        evt = cls(
            operation=m.get("operation", ""),
            id=m.get("id", ""),
            table=m.get("table", ""),
            key=m.get("key"),
            model_version=m.get("modelVersion", ""),
            company_id=m.get("companyId"),
            location_id=m.get("locationId"),
            user_id=m.get("userId"),
            before=_raw_field(m, "before"),
            after=_raw_field(m, "after"),
            diff=m.get("diff"),
            timestamp=int(m.get("timestamp", 0)),
            mvcc_timestamp=m.get("mvccTimestamp", ""),
            imported=bool(m.get("imported", False)),
        )

        try:
            evt.get_object()
        except ValueError as e:
            raise ValueError(
                f"error getting object (before/after is malformed): {e} (seq:{seq}) raw message:\n{raw}"
            ) from e

        if evt.get_primary_key() == "":
            raise ValueError(f"primary key is empty: {evt.id} (seq:{seq}) raw message:\n{raw}")
        return evt


def _raw_field(m: dict, key: str) -> RawJson | None:
    """Reconstruct a json.RawMessage field (before/after) from the parsed message, preserving key order
    (no sort). For the Go-marshaled upstream (compact, sorted, Go-escaped) this round-trips byte-for-byte;
    revalidated against the File/S3/Kafka goldens at M4/M7. DEVIATION: see DEVIATIONS.md#rawjson-reconstruct."""
    if key not in m:
        return None
    return RawJson(marshal(m[key], sort_keys=False))
