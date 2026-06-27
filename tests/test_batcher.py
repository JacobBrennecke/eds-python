"""PARITY: internal/util/batcher_test.go."""

from __future__ import annotations

from eds.dbchange import DBChangeEvent
from eds.util.batcher import Batcher
from eds.util.gojson import RawJson, marshal


def test_batcher() -> None:
    b = Batcher()

    insert = DBChangeEvent(
        table="user", key=["gcp-us-west1", "1"], operation="INSERT", diff=[],
        after=RawJson(marshal({"id": "1", "name": "John", "age": 19, "salary": 9, "city": "New York"})),
    )
    b.add(insert)
    assert b.records()
    assert b.records()[0].object["name"] == "John"

    update = DBChangeEvent(
        table="user", key=["gcp-us-west1", "2"], operation="UPDATE", diff=["name", "age"],
        before=RawJson(marshal({"id": "2", "age": 21, "name": "Foo"})),
        after=RawJson(marshal({"id": "2", "age": 22, "name": "Foo"})),
    )
    b.add(update)
    assert b.records()[1].object["age"] == 22.0  # PARITY: Go float64

    delete = DBChangeEvent(
        table="user", key=["gcp-us-west1", "3"], operation="DELETE",
        before=RawJson(marshal({"id": "3", "age": 56, "name": "Jim"})),
    )
    b.add(delete)
    assert b.records()[2].id == "3"
    assert len(b) == 3


def test_clear() -> None:
    b = Batcher()
    b.add(DBChangeEvent(table="t", key=["1"], operation="INSERT", after=RawJson('{"id":"1"}')))
    assert len(b) == 1
    b.clear()
    assert len(b) == 0
    assert b.records() == []
