"""PARITY: internal/consumer/consumer.go NewNatsConnection / getNatsCreds — the nats-py connection.

DEVIATION (nats-reconnect-defaults): go-common's cnats.NewNats reconnect options aren't vendored; nats-py
library defaults are used (allow_reconnect, etc.). The dev branch (empty creds) mirrors Go's localhost path.
"""

from __future__ import annotations

import uuid
from typing import Any
from urllib.parse import urlparse

import nats

from eds.consumer.credentials import CredentialInfo, get_nats_creds
from eds.util.logger import Logger

_DEFAULT_NATS_PORT = 4222


def _ensure_nats_port(server: str) -> str:
    """DEVIATION (nats-py-no-default-port): nats-py (unlike Go's nats.go) does NOT apply NATS's default port, so a
    port-less URL such as ``nats://host`` makes asyncio connect to port 0 → ``WinError 10049`` ("the requested
    address is not valid in its context") on Windows. Append the default :4222 when no explicit port is present."""
    candidate = server if "://" in server else "nats://" + server
    parsed = urlparse(candidate)
    if parsed.hostname is None or parsed.port is not None:
        return server
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    userinfo = ""
    if parsed.username is not None:
        userinfo = parsed.username + (f":{parsed.password}" if parsed.password is not None else "") + "@"
    return f"{parsed.scheme}://{userinfo}{host}:{_DEFAULT_NATS_PORT}{parsed.path}"


async def new_nats_connection(
    logger: Logger, url: str, creds: str, **callbacks: Any
) -> tuple[Any, CredentialInfo]:
    """PARITY: NewNatsConnection — connect to NATS (JWT creds or the dev/localhost branch)."""
    if not creds:
        info = CredentialInfo(company_ids=["*"], server_id="dev", session_id=str(uuid.uuid4()))
        creds_path: str | None = None
        logger.debug("using localhost nats server")
    else:
        creds_path, info = get_nats_creds(creds)
    opts: dict[str, Any] = {
        # PARITY: Go accepts comma-separated servers. DEVIATION: ensure each has an explicit port (nats-py needs it).
        "servers": [_ensure_nats_port(s.strip()) for s in url.split(",")],
        "name": "eds-" + info.server_id,  # PARITY: cnats.NewNats connection name
        **callbacks,
    }
    if creds_path:
        opts["user_credentials"] = creds_path  # PARITY: nats.UserCredentials (JWT + nkey seed)
    nc = await nats.connect(**opts)
    return nc, info
