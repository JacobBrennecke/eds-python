"""Declarative struct serialization (Stage-2 WS2): ONE metadata-driven engine replacing the per-DTO hand-rolled
``__gojson__`` / ``to_msgpack`` methods, over the UNTOUCHED byte-exact ``marshal``/``compact_raw`` engine in
``eds.util.gojson``.

A dataclass field opts into the wire struct via ``field(metadata={"json": "<camelKey>", "omit": OmitEmpty.<RULE>})``.
Fields with no ``"json"`` metadata are skipped (internal state / Go ``json:"-"``). The omit rules reproduce Go's
per-field omitempty EXACTLY (this is load-bearing — the ``*T`` vs ``T`` distinction):

- ``NEVER``    — always emit (``None`` → ``null`` via marshal). Go value-type w/o omitempty, or ``*T`` w/o omitempty.
- ``IF_NONE``  — omit only when ``None``. Go ``*T,omitempty``: a present-but-empty pointee ("" / ``false``) STILL emits.
- ``IF_FALSY`` — omit when falsy ("" / 0 / ``False`` / ``[]`` / ``None``). Go value-type with omitempty.
- ``IF_EMPTY_RAW`` — (RawJson ``before``/``after``) omit when ``None`` OR ``value.value == ""``.

``gojson_struct`` walks ``dataclasses.fields`` in declaration order (== Go struct field order) emitting
``'"key":' + marshal(value)``; ``msgpack_dict`` builds the ``{key: raw_value}`` dict for the msgpack reply path.
Both render from the SAME metadata table.
"""

from __future__ import annotations

from dataclasses import fields
from enum import Enum
from typing import Any

from eds.util.gojson import marshal


class OmitEmpty(Enum):
    NEVER = 0
    IF_NONE = 1
    IF_FALSY = 2
    IF_EMPTY_RAW = 3


def _omitted(rule: OmitEmpty, value: Any) -> bool:
    if rule is OmitEmpty.IF_NONE:
        return value is None
    if rule is OmitEmpty.IF_FALSY:
        return not value
    if rule is OmitEmpty.IF_EMPTY_RAW:  # RawJson before/after: omit when None or the raw value is empty
        return value is None or len(value.value) == 0
    return False  # NEVER


def gojson_struct(obj: Any) -> str:
    """PARITY: a Go struct's json.Marshal — declaration-order keys + per-field omitempty, over marshal()."""
    parts: list[str] = []
    for f in fields(obj):
        key = f.metadata.get("json")
        if key is None:
            continue  # internal / json:"-"
        value = getattr(obj, f.name)
        if _omitted(f.metadata.get("omit", OmitEmpty.NEVER), value):
            continue
        parts.append('"' + key + '":' + marshal(value))
    return "{" + ",".join(parts) + "}"


def msgpack_dict(obj: Any) -> dict[str, Any]:
    """PARITY: the msgpack reply dict — same field spec as gojson_struct, raw values for msgpack.packb."""
    out: dict[str, Any] = {}
    for f in fields(obj):
        key = f.metadata.get("json")
        if key is None:
            continue
        value = getattr(obj, f.name)
        if _omitted(f.metadata.get("omit", OmitEmpty.NEVER), value):
            continue
        out[key] = value
    return out
