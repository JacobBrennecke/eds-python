"""PARITY: internal/consumer/credentials.go — parse the NATS .creds file's user JWT into CredentialInfo."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field

from eds.util.file import exists

# PARITY: companyIDRE / sessionIDRE. The dbchange subject has literal NATS wildcards (escaped \*); the
# id classes are [a-f0-9-] (hex + dash). `^` anchors at start (no `$` -> prefix match).
_COMPANY_ID_RE = re.compile(r"^dbchange\.\*\.\*\.([a-f0-9-]+)\.")
_SESSION_ID_RE = re.compile(r"^eds.notify.([a-f0-9-]+)\.")


@dataclass
class CredentialInfo:
    """PARITY: consumer.CredentialInfo."""

    company_ids: list[str] = field(default_factory=list)
    server_id: str = ""
    session_id: str = ""


def _first_match(pattern: re.Pattern[str], s: str) -> str:
    m = pattern.search(s)
    return m.group(1) if m else ""


def extract_company_id_from_dbchange_subscription(sub: str) -> str:
    """PARITY: extractCompanyIdFromDBChangeSubscription."""
    return _first_match(_COMPANY_ID_RE, sub)


def extract_session_id_from_eds_subscription(sub: str) -> str:
    """PARITY: extractSessionIdFromEdsSubscription."""
    return _first_match(_SESSION_ID_RE, sub)


def get_nats_creds(creds_file: str) -> tuple[str, CredentialInfo]:
    """PARITY: getNatsCreds — read the .creds file, decode its user JWT, and pull CompanyIDs / ServerID /
    SessionID from the allowed subjects. Returns (creds_file, info); the creds file path is the NATS
    connection credential (Go returns nats.UserCredentials), consumed by the connection at M5. Raises on
    a missing file, an unparseable JWT, no company IDs, or a missing server id."""
    if not exists(creds_file):
        raise ValueError(f"credential file: {creds_file} cannot be found")
    with open(creds_file, "rb") as f:
        buf = f.read()

    claims = _decode_user_claims(_parse_decorated_jwt(buf))
    nats_perms = claims.get("nats") or {}
    sub_perms = nats_perms.get("sub") or {}
    allowed = sub_perms.get("allow") or []

    company_ids: list[str] = []
    session_id = ""
    for sub in allowed:
        maybe_session = extract_session_id_from_eds_subscription(sub)
        if maybe_session:
            session_id = maybe_session
            continue
        company_id = extract_company_id_from_dbchange_subscription(sub)
        if company_id:
            company_ids.append(company_id)

    if not company_ids:
        raise ValueError(
            "issue parsing company IDs from JWT claims. Ensure the JWT has the correct permissions"
        )
    server_id = claims.get("name", "")
    if not server_id:
        raise ValueError("missing server id in credential")
    return creds_file, CredentialInfo(company_ids=company_ids, server_id=server_id, session_id=session_id)


def _parse_decorated_jwt(buf: bytes) -> str:
    """PARITY: jwt.ParseDecoratedJWT — extract the JWT from a NATS decorated .creds file (or accept a
    bare JWT)."""
    text = buf.decode("utf-8", "replace")
    begin = "-----BEGIN NATS USER JWT-----"
    if begin in text:
        after = text.split(begin, 1)[1]
        lines = []
        for line in after.splitlines():
            if line.startswith("-----END") or line.startswith("------END"):
                break
            if line.strip():
                lines.append(line.strip())
        return "".join(lines)
    return text.strip()


def _decode_user_claims(jwt_token: str) -> dict:
    """PARITY: jwt.DecodeUserClaims — decode the (unverified) JWT payload into the NATS user claims."""
    parts = jwt_token.split(".")
    if len(parts) != 3:
        raise ValueError("parsing valid JWT: token must have 3 segments")
    try:
        padding = "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + padding))
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"decoding JWT claims: {e}") from e
    if not isinstance(claims, dict):
        raise ValueError("decoding JWT claims: payload is not an object")
    return claims
