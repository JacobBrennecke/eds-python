"""PARITY: the notification control-plane consumer + dispatch + DTO encodings."""

from __future__ import annotations

import asyncio
import json

import msgpack

from eds.driver import FieldError
from eds.notification import NotificationConsumer, NotificationHandler, get_bool
from eds.notification.dtos import (
    DriverConfigResponse,
    GenericResponse,
    ImportResponse,
    InitBackfillResponse,
    Notification,
    SendLogsResponse,
    UpgradeResponse,
    ValidateResponse,
)


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


class _Nc:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, subject, data, headers=None):
        self.published.append((subject, data, headers))


class _Msg:
    def __init__(self, data: bytes = b"", headers: dict | None = None) -> None:
        self.data = data
        self.headers = headers or {}
        self.responded: list = []

    async def respond(self, data):
        self.responded.append(data)


class _H:
    """Builds a NotificationHandler whose callbacks record calls + return configured values."""

    def __init__(self, **rets) -> None:
        self.calls: list = []
        self._rets = rets

    def _mk(self, name):
        def fn(*a):
            self.calls.append((name, a))
            return self._rets.get(name)

        return fn

    def handler(self) -> NotificationHandler:
        return NotificationHandler(
            restart=self._mk("restart"), shutdown=self._mk("shutdown"),
            pause=self._mk("pause"), unpause=self._mk("unpause"),
            upgrade=self._mk("upgrade"), send_logs=self._mk("send_logs"),
            configure=self._mk("configure"), backfill_init=self._mk("backfill_init"),
            import_action=self._mk("import_action"), driver_config=self._mk("driver_config"),
            validate=self._mk("validate"),
        )


def _consumer(h: _H, nc: _Nc) -> NotificationConsumer:
    return NotificationConsumer(_QuietLogger(), "", h.handler(), nc=nc, session_id="sess")


# ---------------------------------------------------------------- DTO encodings
def test_get_bool() -> None:
    assert get_bool(True) is True
    assert get_bool("true") is True
    assert get_bool("false") is False
    assert get_bool("yes") is False
    assert get_bool(1) is False


def test_validate_response_json_typo() -> None:
    r = ValidateResponse(success=False, message="bad", field_errors=[FieldError("url", "required")],
                         session_id="s", url="")
    j = json.loads(r.__gojson__())
    assert "messsage" in j and j["messsage"] == "bad"  # PARITY: the misspelled JSON key
    assert "message" not in j
    assert j["field_errors"][0]["error"] == "required"
    assert "url" not in j  # omitempty


def test_generic_response_message_omitempty() -> None:
    # None message → omitted; "" message → emitted (publishSimpleStatus quirk)
    assert "message" not in GenericResponse(success=True, session_id="s", action="pause").to_msgpack()
    emitted = GenericResponse(success=True, message="", session_id="s", action="restart").to_msgpack()
    assert emitted["message"] == ""


def test_import_and_upgrade_exclude_log_path() -> None:
    assert "log_path" not in ImportResponse(success=True, session_id="s", log_path="/x", job_id="j").to_msgpack()
    assert "-" not in UpgradeResponse(success=True, session_id="s", log_path="/x", version="1").to_msgpack()


# ---------------------------------------------------------------- dispatch
async def test_dispatch_restart() -> None:
    h, nc = _H(), _Nc()
    c = _consumer(h, nc)
    await c.dispatch(Notification(action="restart"), _Msg())
    assert ("restart", ()) in h.calls
    subjects = [p[0] for p in nc.published]
    assert "eds.client.sess.restart-status" in subjects
    assert "eds.client.sess.restart-response" in subjects


async def test_dispatch_ping() -> None:
    h, nc = _H(), _Nc()
    await _consumer(h, nc).dispatch(Notification(action="ping", data={"subject": "reply.42"}), _Msg())
    assert nc.published == [("reply.42", b"pong", None)]


async def test_dispatch_shutdown() -> None:
    h, nc = _H(), _Nc()
    await _consumer(h, nc).dispatch(Notification(action="shutdown", data={"message": "bye", "deleted": "true"}), _Msg())
    assert h.calls == [("shutdown", ("bye", True))]
    assert nc.published == []  # shutdown publishes no response


async def test_dispatch_pause_success_and_error() -> None:
    h, nc = _H(pause=None, unpause=RuntimeError("boom")), _Nc()
    c = _consumer(h, nc)
    await c.dispatch(Notification(action="pause"), _Msg())
    await c.dispatch(Notification(action="unpause"), _Msg())
    payloads = [msgpack.unpackb(d, raw=False) for s, d, _ in nc.published]
    assert payloads[0]["success"] is True and "message" not in payloads[0]  # pause ok
    assert payloads[1]["success"] is False and payloads[1]["message"] == "boom"  # unpause errored


async def test_dispatch_driverconfig_and_validate_reply_json() -> None:
    h = _H(driver_config=DriverConfigResponse(session_id="sess"),
           validate=ValidateResponse(success=True, url="postgres://x", session_id="sess"))
    nc = _Nc()
    c = _consumer(h, nc)
    m1 = _Msg()
    await c.dispatch(Notification(action="driverconfig"), m1)
    assert json.loads(m1.responded[0])["sessionId"] == "sess"
    m2 = _Msg()
    await c.dispatch(Notification(action="validate", data={"driver": "postgres", "config": {"x": 1}}), m2)
    assert json.loads(m2.responded[0]) == {"success": True, "sessionId": "sess", "url": "postgres://x"}


async def test_dispatch_sendlogs() -> None:
    h = _H(send_logs=SendLogsResponse(path="/logs/x", session_id="sess"))
    nc = _Nc()
    await _consumer(h, nc).dispatch(Notification(action="sendlogs"), _Msg())
    subject, data, headers = nc.published[0]
    assert subject == "eds.client.sess.sendlogs-response"
    assert headers["content-encoding"] == "msgpack"
    assert msgpack.unpackb(data, raw=False) == {"path": "/logs/x", "sessionId": "sess"}


async def test_dispatch_unknown_action_noop() -> None:
    h, nc = _H(), _Nc()
    await _consumer(h, nc).dispatch(Notification(action="bogus"), _Msg())
    assert h.calls == [] and nc.published == []


async def test_callback_case_insensitive_content_encoding_and_msgpack() -> None:
    # PARITY: HQ sends the canonical "Content-Encoding" header — the lookup must be case-insensitive,
    # and a msgpack-encoded notification body must decode + dispatch.
    h = _H(send_logs=SendLogsResponse(path="/x", session_id="sess"))
    nc = _Nc()
    c = _consumer(h, nc)
    data = msgpack.packb({"action": "sendlogs"}, use_bin_type=True)
    await c._callback(_Msg(data=data, headers={"Content-Encoding": "msgpack"}))
    assert any(s == "eds.client.sess.sendlogs-response" for s, _, _ in nc.published)


async def test_callback_malformed_data_does_not_crash() -> None:
    # a non-object "data" (here a list) must not raise out of dispatch; it is coerced to {}
    h, nc = _H(), _Nc()
    c = _consumer(h, nc)
    data = json.dumps({"action": "ping", "data": ["not", "an", "object"]}).encode()
    await c._callback(_Msg(data=data))  # ping with no usable subject → warn, no crash
    assert nc.published == []


# ---------------------------------------------------------------- importaction (mirrors notification_test.go)
async def test_importaction_init_failure_no_background() -> None:
    h = _H(backfill_init=InitBackfillResponse(success=False, message="init failed", session_id="sess"))
    nc = _Nc()
    c = _consumer(h, nc)
    msg = _Msg()
    await c.dispatch(Notification(action="import", data={"backfill": True}), msg)
    reply = json.loads(msg.responded[0])
    assert reply["success"] is False and reply["message"] == "init failed"
    assert [name for name, _ in h.calls] == ["backfill_init"]  # no import_action (no background task)
    assert len(c._tasks) == 0


async def test_importaction_init_success_runs_background_import() -> None:
    h = _H(
        backfill_init=InitBackfillResponse(success=True, session_id="sess", job_id="job-123"),
        import_action=ImportResponse(success=True, session_id="sess", job_id="job-123"),
    )
    nc = _Nc()
    c = _consumer(h, nc)
    msg = _Msg()
    await c.dispatch(Notification(action="import", data={"backfill": True}), msg)
    assert json.loads(msg.responded[0])["jobId"] == "job-123"  # JSON init reply carries the jobId
    await asyncio.gather(*c._tasks)  # drain the background import
    assert any(name == "import_action" for name, _ in h.calls)  # the long leg ran on a background task
    assert any(s == "eds.client.sess.import-response" for s, _, _ in nc.published)
