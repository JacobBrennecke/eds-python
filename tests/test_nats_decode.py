"""PARITY: internal/util/nats_test.go — DecodeNatsMsg across the three encodings."""

from __future__ import annotations

import gzip

import msgpack

from eds.util.nats import decode_nats_msg


def test_no_encoding() -> None:
    o = decode_nats_msg(b'{"name":"test"}', None)
    assert o["name"] == "test"


def test_gzip_json_encoding() -> None:
    data = gzip.compress(b'{"name":"test"}')
    o = decode_nats_msg(data, "gzip/json")
    assert o["name"] == "test"


def test_msgpack_encoding() -> None:
    data = msgpack.packb({"name": "test"})
    o = decode_nats_msg(data, "msgpack")
    assert o["name"] == "test"
