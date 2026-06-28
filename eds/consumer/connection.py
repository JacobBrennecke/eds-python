"""PARITY: internal/consumer/consumer.go NewNatsConnection / getNatsCreds — the nats-py connection.

DEVIATION (nats-reconnect-defaults): go-common's cnats.NewNats reconnect options aren't vendored; nats-py
library defaults are used (allow_reconnect, etc.). The dev branch (empty creds) mirrors Go's localhost path.
"""

from __future__ import annotations

import uuid
from typing import Any

import nats

from eds.consumer.credentials import CredentialInfo, get_nats_creds
from eds.util.logger import Logger


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
        "servers": [s.strip() for s in url.split(",")],  # PARITY: Go accepts comma-separated servers
        "name": "eds-" + info.server_id,  # PARITY: cnats.NewNats connection name
        **callbacks,
    }
    if creds_path:
        opts["user_credentials"] = creds_path  # PARITY: nats.UserCredentials (JWT + nkey seed)
    nc = await nats.connect(**opts)
    return nc, info
