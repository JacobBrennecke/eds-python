"""PARITY: cmd/server.go session lifecycle + log upload + cmd/root.go / cmd/import.go HTTP helpers.

The Shopmonkey API session bookkeeping: sendStart (register) → fork the consumer → sendEnd + upload the rotated
log and stderr bundle to presigned PUT URLs. DTOs are reused from eds.api (already byte-parity). The HTTP
transport is a thin injectable seam (default = requests) so the lifecycle is unit-testable without a network.
"""

from __future__ import annotations

import base64
import json
import os
import socket
from collections.abc import Callable
from typing import Any

from eds.api import (
    DriverMeta,
    SessionEndResponse,
    SessionEndURLs,
    SessionStart,
    SessionStartResponse,
)
from eds.driver import get_driver_metadata_for_url
from eds.util.compress import gzip_file
from eds.util.file import list_dir
from eds.util.http import HttpRetry
from eds.util.logger import Logger
from eds.util.mask import mask_url
from eds.util.sysinfo import get_local_ip, get_machine_id, get_system_info

# Transport seam: (method, url, headers, data) -> response with .status_code, .text, .content, .headers.
Transport = Callable[[str, str, dict, Any], Any]


class AlreadyRunningError(Exception):
    """PARITY: errAlreadyRunning (server.go:53) — HTTP 409 on session start; caller retries every 5s."""


def _default_transport(method: str, url: str, headers: dict, data: Any = None) -> Any:
    import requests  # lazy (runtime dep)

    return requests.request(method, url, headers=headers, data=data, timeout=None)


def set_http_header(api_key: str, version: str) -> dict:
    """PARITY: setHTTPHeader (root.go:194-202) — REPLACES all headers; Bearer only when api_key is set."""
    headers = {
        "Content-Type": "application/json",
        "User-Agent": f"Shopmonkey EDS Server/{version}",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def get_request_id(resp: Any) -> str:
    """PARITY: getRequestID (root.go:204-206)."""
    try:
        return resp.headers.get("X-Request-Id", "") or ""
    except Exception:  # noqa: BLE001
        return ""


def _parse_error_response(buf: str, status: int, context: str, req_id: str) -> Exception:
    """PARITY: errorResponse.Parse (import.go:60-73)."""
    tag = f"(requestId={req_id})" if req_id else ""
    try:
        m = json.loads(buf)
        return RuntimeError(f"{context}: {m.get('message', '')} {tag}")
    except Exception:  # noqa: BLE001
        return RuntimeError(f"{context}: {buf} (status code={status}) {tag}")


def handle_api_error(resp: Any, context: str) -> Exception:
    """PARITY: handleAPIError (import.go:75-82)."""
    return _parse_error_response(resp.text, resp.status_code, context, get_request_id(resp))


def write_creds_to_file(data: str, filename: str) -> None:
    """PARITY: writeCredsToFile (server.go:42-51) — base64 (std alphabet) decode → 0600 file."""
    try:
        buf = base64.b64decode(data, validate=True)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to decode base64: {e}") from e
    # O_BINARY avoids Windows newline translation so the creds bytes are written verbatim (parity with Go).
    fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0), 0o600)
    try:
        os.write(fd, buf)
    finally:
        os.close(fd)


def send_start(
    logger: Logger,
    api_url: str,
    api_key: str,
    driver_url: str,
    eds_server_id: str,
    company_ids: list[str] | None,
    *,
    version: str,
    transport: Transport | None = None,
) -> Any:
    """PARITY: sendStart (server.go:55-128) → EdsSession. Raises AlreadyRunningError on 409."""
    transport = transport or _default_transport
    try:
        ip = get_local_ip()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to get local IP: {e}") from e
    try:
        machine_id = get_machine_id()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to get machine ID: {e}") from e
    try:
        hostname = socket.gethostname()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to get hostname: {e}") from e
    try:
        os_info = get_system_info()
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to get system info: {e}") from e

    body = SessionStart(
        version=version,
        hostname=hostname,
        ip_address=ip,
        machine_id=machine_id,
        os_info=os_info,
        server_id=eds_server_id,
        company_ids=company_ids or None,
    )
    if driver_url:
        meta = get_driver_metadata_for_url(driver_url)
        if meta is None:
            raise RuntimeError(f"invalid driver URL: {driver_url}")
        try:
            masked = mask_url(driver_url)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"failed to mask driver URL: {e}") from e
        body.driver = DriverMeta(id=meta.scheme, name=meta.name, description=meta.description, url=masked)

    logger.trace("sending session start: %s", body.__gojson__())
    url = f"{api_url}/v3/eds/internal"
    data = body.__gojson__().encode()
    headers = set_http_header(api_key, version)
    resp: Any = HttpRetry(lambda: transport("POST", url, headers, data), method="POST", url=url, logger=logger).do()
    status = resp.status_code
    if status == 409:
        raise AlreadyRunningError()
    if status != 200:
        raise handle_api_error(resp, "session start")
    r = SessionStartResponse.from_json(resp.content)
    if not r.success:
        raise RuntimeError(f"failed to start session: {r.message}")
    logger.trace("session %s started successfully", r.data.session_id)
    return r.data


def send_end(
    logger: Logger,
    api_url: str,
    api_key: str,
    session_id: str,
    errored: bool,
    *,
    version: str,
    transport: Transport | None = None,
) -> SessionEndURLs:
    """PARITY: sendEnd (server.go:130-156) → SessionEndURLs (presigned PUT targets)."""
    from eds.api import SessionEnd

    transport = transport or _default_transport
    url = f"{api_url}/v3/eds/internal/{session_id}"
    data = SessionEnd(errored=errored).__gojson__().encode()
    headers = set_http_header(api_key, version)
    resp: Any = HttpRetry(lambda: transport("POST", url, headers, data), method="POST", url=url, logger=logger).do()
    if resp.status_code != 200:
        raise handle_api_error(resp, "session end")
    r = SessionEndResponse.from_json(resp.content)
    if not r.success:
        raise RuntimeError(f"failed to end session: {r.message}")
    logger.trace("session %s ended successfully: %s", session_id, r.data.url)
    return r.data


def upload_file(
    logger: Logger, url: str, log_file_bundle: str, *, version: str, transport: Transport | None = None
) -> None:
    """PARITY: uploadFile (server.go:158-181) — raw presigned PUT, no auth, content-type application/x-tgz."""
    transport = transport or _default_transport
    # DEVIATION (uploadFile body+retry): read the bytes once so a retried PUT re-sends the body (Go streams an
    # un-rewound *os.File, so a real retry would PUT empty). Matches the C# ByteArrayContent approach.
    with open(log_file_bundle, "rb") as f:
        payload = f.read()
    headers = {"User-Agent": f"Shopmonkey EDS Server/{version}", "Content-Type": "application/x-tgz"}
    resp: Any = HttpRetry(lambda: transport("PUT", url, headers, payload), method="PUT", url=url, logger=logger).do()
    if resp.status_code != 200:
        raise handle_api_error(resp, "upload logs")
    logger.trace("logs uploaded successfully: %s", log_file_bundle)


def upload_log_file(
    logger: Logger, upload_url: str, log_file: str, *, version: str, transport: Transport | None = None
) -> str:
    """PARITY: uploadLogFile (server.go:183-200) — gzip then PUT; returns the presigned URL's path component."""
    from urllib.parse import urlsplit

    logger.debug("uploading logfile: %s", log_file)
    gzip_file(log_file)  # writes <log_file>.gz
    compressed = log_file + ".gz"
    try:
        upload_file(logger, upload_url, compressed, version=version, transport=transport)
    finally:
        try:
            os.remove(compressed)
        except OSError:
            pass
    return urlsplit(upload_url).path


def get_remaining_log(logs_dir: str) -> str:
    """PARITY: getRemainingLog (server.go:202-220) — the last (sorted) *.log under logs_dir, or ''."""
    files = sorted(list_dir(logs_dir))
    last = ""
    for f in files:
        if f.endswith(".log"):
            last = f
    return last


def send_end_and_upload(
    logger: Logger,
    api_url: str,
    api_key: str,
    session_id: str,
    errored: bool,
    logfile: str,
    stderr_file: str,
    *,
    version: str,
    transport: Transport | None = None,
) -> str:
    """PARITY: sendEndAndUpload (server.go:222-245) — end session, then PUT the main log (→url) and the stderr
    bundle (→errorUrl) as independent gzip objects. Returns the main log's storage path."""
    logger.info("uploading logs for session: %s", session_id)
    urls = send_end(logger, api_url, api_key, session_id, errored, version=version, transport=transport)
    log_storage_path = ""
    if logfile:
        log_storage_path = upload_log_file(logger, urls.url, logfile, version=version, transport=transport)
    if urls.error_url and stderr_file:
        upload_log_file(logger, urls.error_url, stderr_file, version=version, transport=transport)
    logger.trace("logs uploaded successfully for session: %s", session_id)
    if errored:
        logger.info("error log files saved to %s for session: %s", logfile, session_id)
    return log_storage_path


def get_log_upload_url(
    logger: Logger, api_url: str, api_key: str, session_id: str, *, version: str, transport: Transport | None = None
) -> str:
    """PARITY: getLogUploadURL (server.go:247-271) — POST .../log → fresh presigned URL (.url)."""
    transport = transport or _default_transport
    url = f"{api_url}/v3/eds/internal/{session_id}/log"
    headers = set_http_header(api_key, version)
    resp: Any = HttpRetry(lambda: transport("POST", url, headers, b"{}"), method="POST", url=url, logger=logger).do()
    if resp.status_code != 200:
        raise handle_api_error(resp, "log upload url")
    r = SessionEndResponse.from_json(resp.content)
    if not r.success:
        raise RuntimeError(f"failed to get log upload url: {r.message}")
    logger.trace("session %s log url received: %s", session_id, r.data.url)
    return r.data.url
