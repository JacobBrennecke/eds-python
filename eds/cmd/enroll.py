"""PARITY: cmd/enroll.go — `eds enroll <code>` (exchange a one-time code for the api key, write config.toml)."""

from __future__ import annotations

import argparse
from typing import Any

from eds.api import EnrollResponse, get_api_url
from eds.cmd.config import write_config
from eds.cmd.exit_codes import EXIT_ERROR, EXIT_SUCCESS
from eds.cmd.session import handle_api_error
from eds.util.http import HttpRetry


def _default_transport(method: str, url: str, headers: dict, data: Any = None) -> Any:
    import requests

    return requests.request(method, url, headers=headers, data=data, timeout=None)


def run_enroll(args: argparse.Namespace, *, transport: Any = None) -> int:
    from eds.cmd import root as _root

    transport = transport or _default_transport
    logger = _root.new_logger(args).with_prefix("[enroll]")
    code = args.code
    api_url = args.api_url
    data_dir = _root.get_data_dir(args, logger)

    if not api_url:  # PARITY: derive the api url from the code's first letter (P/S/E/L)
        logger.debug("Getting api from prefix")
        try:
            api_url = get_api_url(code[0:1])
        except ValueError as e:
            logger.error("error getting api url: %s", e)
            return EXIT_ERROR

    url = f"{api_url}/v3/eds/internal/enroll/{code}"
    # PARITY: enroll.go sends a bare GET (no setHTTPHeader) through the HTTP retry.
    try:
        resp: Any = HttpRetry(lambda: transport("GET", url, {}, None), method="GET", url=url, logger=logger).do()
    except Exception as e:  # noqa: BLE001 — PARITY: Go logger.Fatal("failed to enroll server") → exit 1 (not panic→2)
        logger.error("failed to enroll server: %s", e)
        return EXIT_ERROR
    if resp.status_code == 404:
        logger.error("invalid enrollment code or it has already been used")
        return EXIT_ERROR
    if resp.status_code != 200:
        logger.error("%s", handle_api_error(resp, "enroll"))
        return EXIT_ERROR
    try:
        r = EnrollResponse.from_json(resp.content)
    except Exception as e:  # noqa: BLE001 — PARITY: Go logger.Fatal("failed to decode response") → exit 1
        logger.error("failed to decode response: %s", e)
        return EXIT_ERROR
    if not r.success:
        logger.error("failed to start enroll: %s", r.message)
        return EXIT_ERROR

    # PARITY: enroll.go writes toml.NewEncoder(EnrollTokenData) → config.toml (0644): token + server_id.
    try:
        write_config(data_dir, {"token": r.data.token, "server_id": r.data.server_id})
    except OSError as e:  # PARITY: Go logger.Fatal("failed to write to token file") → exit 1
        logger.error("failed to write to token file: %s", e)
        return EXIT_ERROR
    logger.info("Enrollment successful!")
    logger.info("run %s to start the server", _root.get_command_example("server"))
    return EXIT_SUCCESS
