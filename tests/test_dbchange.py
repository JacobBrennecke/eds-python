"""PARITY: internal/dbchange.go — vectors from the C# JsonTests + DBChangeEventFromMessageTests."""

from __future__ import annotations

import pytest

from eds.dbchange import DBChangeEvent
from eds.util.gojson import RawJson, marshal


def test_serializes_in_go_field_order_with_omitempty() -> None:
    evt = DBChangeEvent(
        operation="INSERT",
        id="x",
        table="t",
        key=["pk"],
        model_version="v1",
        timestamp=123,
        mvcc_timestamp="m",
        after=RawJson('{"id":"pk"}'),
    )
    assert evt.to_json() == (
        '{"operation":"INSERT","id":"x","table":"t","key":["pk"],'
        '"modelVersion":"v1","after":{"id":"pk"},"timestamp":123,"mvccTimestamp":"m"}'
    )
    # The __gojson__ protocol makes gojson.marshal(event) work too.
    assert marshal(evt) == evt.to_json()


def test_includes_optional_pointers_and_diff_when_present() -> None:
    evt = DBChangeEvent(
        operation="UPDATE",
        id="x",
        table="t",
        key=["pk"],
        model_version="v1",
        company_id="c1",
        diff=["name"],
        timestamp=1,
        mvcc_timestamp="m",
        after=RawJson('{"id":"pk","name":"a"}'),
    )
    js = evt.to_json()
    assert '"companyId":"c1"' in js
    assert '"diff":["name"]' in js
    assert "locationId" not in js
    assert "imported" not in js


def test_str() -> None:
    evt = DBChangeEvent(operation="INSERT", table="t", id="x", key=["pk"])
    assert str(evt) == "DBChangeEvent[op=INSERT,table=t,id=x,pk=pk]"


def test_get_object_parses_numbers_as_float() -> None:
    evt = DBChangeEvent(after=RawJson('{"id":"u1","age":42}'))
    obj = evt.get_object()
    assert obj == {"id": "u1", "age": 42.0}
    assert isinstance(obj["age"], float)  # PARITY: Go map[string]any -> float64


def test_omit_properties_mutates_object_not_raw() -> None:
    evt = DBChangeEvent(after=RawJson('{"id":"u1","secret":"x"}'))
    evt.omit_properties("secret")
    assert evt.get_object() == {"id": "u1"}
    assert evt.after is not None and evt.after.value == '{"id":"u1","secret":"x"}'  # raw untouched


def _msg(evt: DBChangeEvent) -> bytes:
    return evt.to_json().encode("utf-8")


def test_from_message_parses_valid() -> None:
    evt = DBChangeEvent(operation="INSERT", table="order", key=["pk1"], timestamp=123, mvcc_timestamp="m",
                        after=RawJson('{"id":"pk1"}'))
    got = DBChangeEvent.from_message(_msg(evt), 5)
    assert got.operation == "INSERT"
    assert got.table == "order"
    assert got.get_primary_key() == "pk1"


def test_from_message_falls_back_to_object_id() -> None:
    evt = DBChangeEvent(operation="INSERT", table="order", mvcc_timestamp="m", after=RawJson('{"id":"from-object"}'))
    got = DBChangeEvent.from_message(_msg(evt), 1)
    assert got.get_primary_key() == "from-object"


def test_from_message_empty_primary_key_raises() -> None:
    evt = DBChangeEvent(operation="INSERT", table="order", mvcc_timestamp="m", after=RawJson('{"name":"x"}'))
    with pytest.raises(ValueError, match="primary key is empty") as ei:
        DBChangeEvent.from_message(_msg(evt), 9)
    assert "seq:9" in str(ei.value)


def test_from_message_non_object_after_raises() -> None:
    evt = DBChangeEvent(operation="INSERT", table="order", key=["pk1"], mvcc_timestamp="m", after=RawJson("[1,2,3]"))
    with pytest.raises(ValueError, match="before/after is malformed"):
        DBChangeEvent.from_message(_msg(evt), 4)


def test_from_message_accepts_null_after() -> None:
    msg = (
        b'{"operation":"INSERT","table":"order","key":["pk1"],"modelVersion":"v1",'
        b'"after":null,"timestamp":0,"mvccTimestamp":"m"}'
    )
    got = DBChangeEvent.from_message(msg, 1)
    assert got.get_primary_key() == "pk1"


def test_from_message_malformed_json_raises() -> None:
    with pytest.raises(ValueError, match="unmarshalling"):
        DBChangeEvent.from_message(b"{not json", 3)
