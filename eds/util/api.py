"""PARITY: internal/util/api.go — extract the API URL (the JWT ``iss`` claim).

Named ``api`` to mirror the Go file; this is a util helper, distinct from the ``eds.api`` package (the
Shopmonkey API client, ported at M3).
"""

from __future__ import annotations

import base64
import json


def get_api_url_from_jwt(jwt_string: str) -> str:
    """PARITY: util.GetAPIURLFromJWT — parse the JWT UNVERIFIED and return its issuer, mapping the legacy
    ``https://shopmonkey.io`` issuer to ``https://api.shopmonkey.cloud``.

    No signature verification (Go uses jwt.WithoutClaimsValidation + ParseUnverified); the NATS server
    re-verifies at connect. Raises ValueError on a structurally invalid token."""
    parts = jwt_string.split(".")
    if len(parts) != 3:
        raise ValueError("failed to parse jwt: token must have 3 segments")
    try:
        payload = _b64url_decode(parts[1])
        claims = json.loads(payload)
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"failed to parse jwt: {e}") from e
    if not isinstance(claims, dict):
        raise ValueError("failed to parse jwt: claims is not an object")

    iss = claims.get("iss", "")  # PARITY: jwt RegisteredClaims.GetIssuer returns "" (no error) when absent
    if not isinstance(iss, str):
        iss = ""
    if iss == "https://shopmonkey.io":
        iss = "https://api.shopmonkey.cloud"  # PARITY: legacy-token remap
    return iss


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)  # JWT segments are unpadded base64url
    return base64.urlsafe_b64decode(segment + padding)
