"""WS2 byte-snapshot safety net — locks the exact __gojson__/to_msgpack output of the DTOs that had NO byte-exact
test before the declarative-serializer refactor. Captured from the Stage-1 hand-rolled code; must stay identical
after migrating each DTO to the metadata-driven engine (eds.util.gostruct).
"""

from __future__ import annotations

from eds.driver import DriverConfigurator, DriverField, DriverMetadata, FieldError
from eds.notification.dtos import (
    ConfigureResponse,
    DriverConfigResponse,
    GenericResponse,
    ImportResponse,
    InitBackfillResponse,
    Notification,
    SendLogsResponse,
    UpgradeResponse,
    ValidateResponse,
)
from eds.schema import ItemsType, Schema, SchemaProperty
from eds.util.batcher import Record
from eds.util.gojson import marshal


def _meta() -> DriverMetadata:
    return DriverMetadata(scheme="postgres", name="Postgres", description="d", example_url="postgres://",
                          help="h", supports_import=True, supports_migration=False)


# ---- schema (ItemsType / SchemaProperty / Schema — previously untested at byte level) ----
def test_items_type_bytes() -> None:
    assert marshal(ItemsType(type="string", enum=["a", "b"], format="date")) == \
        '{"type":"string","enum":["a","b"],"format":"date"}'
    assert marshal(ItemsType(type="string")) == '{"type":"string"}'


def test_schema_property_bytes() -> None:
    full = SchemaProperty(type="string", format="date-time", nullable=True, items=ItemsType(type="number"),
                          additional_properties=False, comment="c", deprecated=True)
    assert marshal(full) == ('{"type":"string","format":"date-time","nullable":true,"items":{"type":"number"},'
                             '"additionalProperties":false,"$comment":"c","deprecated":true}')
    assert marshal(SchemaProperty(type="string")) == '{"type":"string"}'  # *bool/None omitted, value-false omitted
    # the load-bearing *bool-vs-bool distinction: additional_properties=True emits even though "true" is truthy,
    # and =False (above) emits, while nullable=False (value-bool) omits.
    assert marshal(SchemaProperty(type="object", additional_properties=True)) == \
        '{"type":"object","additionalProperties":true}'


def test_schema_bytes() -> None:
    s = Schema(properties={"b": SchemaProperty(type="string"), "a": SchemaProperty(type="number")},
               required=["b"], primary_keys=["a"], table="orders", model_version="2")
    assert marshal(s) == ('{"properties":{"a":{"type":"number"},"b":{"type":"string"}},"required":["b"],'
                          '"primaryKeys":["a"],"table":"orders","modelVersion":"2"}')


# ---- notification JSON DTOs ----
def test_notification_json_bytes() -> None:
    assert marshal(Notification(action="ping", data={"k": "v"})) == '{"action":"ping","data":{"k":"v"}}'
    assert marshal(Notification(action="ping")) == '{"action":"ping"}'
    assert marshal(InitBackfillResponse(success=True, message="m", session_id="s1", job_id="j1")) == \
        '{"success":true,"message":"m","sessionId":"s1","jobId":"j1"}'
    assert marshal(InitBackfillResponse(success=True, session_id="s1", job_id="j1")) == \
        '{"success":true,"sessionId":"s1","jobId":"j1"}'


def test_configure_response_bytes() -> None:
    # log_path is json:"-" → must NOT appear; maskedURL key; message+maskedURL omit on None
    assert marshal(ConfigureResponse(success=True, message="ok", masked_url="postgres://u:***@h/db",
                                     session_id="s1", backfill=True, log_path="/should/skip")) == \
        '{"success":true,"message":"ok","maskedURL":"postgres://u:***@h/db","sessionId":"s1","backfill":true}'
    assert marshal(ConfigureResponse(success=False, session_id="s1")) == \
        '{"success":false,"sessionId":"s1","backfill":false}'


def test_driver_config_response_bytes() -> None:
    assert marshal(DriverConfigResponse(drivers={}, session_id="s1")) == '{"drivers":{},"sessionId":"s1"}'
    one = DriverConfigResponse(drivers={"postgres": DriverConfigurator(
        metadata=_meta(), fields=[DriverField(name="Host", description="host", required=True)])}, session_id="s1")
    assert marshal(one) == (
        '{"drivers":{"postgres":{"metadata":{"scheme":"postgres","name":"Postgres","description":"d",'
        '"exampleURL":"postgres://","help":"h","supportsImport":true,"supportsMigration":false},'
        '"fields":[{"name":"Host","type":"string","description":"host","required":true}]}},"sessionId":"s1"}')


def test_validate_response_bytes() -> None:
    full = ValidateResponse(success=False, message="bad", field_errors=[FieldError("Port", "bad port")],
                            session_id="s1", url="postgres://x")
    assert marshal(full) == ('{"success":false,"messsage":"bad","field_errors":[{"field":"Port","error":"bad port"}],'
                             '"sessionId":"s1","url":"postgres://x"}')
    assert marshal(ValidateResponse(success=True, session_id="s1")) == '{"success":true,"sessionId":"s1"}'


# ---- notification msgpack DTOs ----
def test_msgpack_dicts() -> None:
    assert SendLogsResponse(path="/p", session_id="s1").to_msgpack() == {"path": "/p", "sessionId": "s1"}
    assert GenericResponse(success=True, message="m", session_id="s1", action="pause").to_msgpack() == \
        {"success": True, "message": "m", "sessionId": "s1", "action": "pause"}
    assert GenericResponse(success=True, message=None, session_id="s1", action="pause").to_msgpack() == \
        {"success": True, "sessionId": "s1", "action": "pause"}  # None omits
    assert GenericResponse(success=True, message="", session_id="s1", action="pause").to_msgpack() == \
        {"success": True, "message": "", "sessionId": "s1", "action": "pause"}  # "" emits
    assert ImportResponse(success=False, message="m", session_id="s1", log_path="/skip", job_id="j1").to_msgpack() == \
        {"success": False, "message": "m", "sessionId": "s1", "jobId": "j1"}  # log_path skipped
    assert ImportResponse(success=True, session_id="s1", job_id="j1").to_msgpack() == \
        {"success": True, "sessionId": "s1", "jobId": "j1"}
    assert UpgradeResponse(success=True, message="", session_id="s1", log_path="/skip", version="1.0").to_msgpack() == \
        {"success": True, "sessionId": "s1", "version": "1.0"}  # "" message omits, log_path skipped
    assert UpgradeResponse(success=False, message="boom", session_id="s1", version="1.0").to_msgpack() == \
        {"success": False, "message": "boom", "sessionId": "s1", "version": "1.0"}


def test_msgpack_key_order() -> None:
    # msgpack.packb is order-SENSITIVE (== dict above is not); lock the wire key order == Stage-1 declaration order.
    assert list(SendLogsResponse(path="/p", session_id="s").to_msgpack()) == ["path", "sessionId"]
    assert list(GenericResponse(success=True, message="m", session_id="s", action="a").to_msgpack()) == \
        ["success", "message", "sessionId", "action"]
    assert list(GenericResponse(success=True, message=None, session_id="s", action="a").to_msgpack()) == \
        ["success", "sessionId", "action"]
    assert list(ImportResponse(success=True, message="m", session_id="s", log_path="/x", job_id="j").to_msgpack()) == \
        ["success", "message", "sessionId", "jobId"]  # log_path skipped mid-struct, jobId still last
    upg = UpgradeResponse(success=True, message="m", session_id="s", log_path="/x", version="1")
    assert list(upg.to_msgpack()) == ["success", "message", "sessionId", "version"]


# ---- driver tail (DriverMetadata / DriverConfigurator — previously untested at byte level) ----
def test_driver_metadata_and_configurator_bytes() -> None:
    assert marshal(_meta()) == ('{"scheme":"postgres","name":"Postgres","description":"d","exampleURL":"postgres://",'
                                '"help":"h","supportsImport":true,"supportsMigration":false}')
    cfg = DriverConfigurator(
        metadata=DriverMetadata(scheme="mysql", name="MySQL", description="d2", example_url="mysql://",
                                help="h2", supports_import=False, supports_migration=True),
        fields=[DriverField(name="Port", type="number", description="port", default="3306")])
    assert marshal(cfg) == (
        '{"metadata":{"scheme":"mysql","name":"MySQL","description":"d2","exampleURL":"mysql://","help":"h2",'
        '"supportsImport":false,"supportsMigration":true},'
        '"fields":[{"name":"Port","type":"number","default":"3306","description":"port","required":false}]}')


# ---- batcher Record (event=json:"-"; diff/object NEVER → null) ----
def test_record_bytes() -> None:
    assert marshal(Record(table="orders", id="1", operation="INSERT", diff=["a"],
                          object={"z": 1, "a": 2}, event=None)) == \
        '{"table":"orders","id":"1","operation":"INSERT","diff":["a"],"object":{"a":2,"z":1}}'
    assert marshal(Record(table="orders", id="2", operation="DELETE", diff=None, object=None, event=None)) == \
        '{"table":"orders","id":"2","operation":"DELETE","diff":null,"object":null}'
