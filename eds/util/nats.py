"""PARITY: internal/util/nats.go — DecodeNatsMsg."""

from __future__ import annotations

import json

import msgpack

from eds.util import gojson
from eds.util.compress import gunzip


def decode_nats_msg(data: bytes, content_encoding: str | None) -> object:
    """PARITY: util.DecodeNatsMsg — decode by the ``content-encoding`` header: ``gzip/json`` → gunzip then
    JSON; ``msgpack`` → unpack then re-marshal to JSON (Go's msgpack→json→target round-trip); otherwise raw
    JSON. Returns the parsed value (Go decodes into a caller-supplied ``v``; the typed-DTO mapping is the
    caller's job — M5/M8). ``data`` + ``content_encoding`` are pulled from the NATS msg by the caller (M5)."""
    if content_encoding == "gzip/json":
        data = gunzip(data)
    elif content_encoding == "msgpack":
        obj = msgpack.unpackb(data, raw=False)
        data = gojson.marshal(obj).encode("utf-8")
    return json.loads(data)
