"""PARITY: cmd/download.go — `eds download <version> <filename>` (fetch + verify + extract a release binary)."""

from __future__ import annotations

import argparse

from eds.cmd.exit_codes import EXIT_ERROR, EXIT_SUCCESS
from eds.upgrade import UpgradeConfig, build_release_urls, upgrade


def run_download(args: argparse.Namespace) -> int:
    from eds.cmd import root as _root

    logger = _root.new_logger(args).with_prefix("[download]")
    version = args.version
    filename = args.filename
    binary_url, signature_url = build_release_urls(version)
    public_key = _root.PUBLIC_PGP_KEY or _root._load_public_key()
    try:
        upgrade(
            UpgradeConfig(
                logger=logger, binary_url=binary_url, signature_url=signature_url,
                filename=filename, public_key=public_key,
            )
        )
    except Exception as e:  # noqa: BLE001 — PARITY: Go logger.Fatal on any error (exit 1)
        logger.error("%s", e)
        return EXIT_ERROR
    vversion = version if version.startswith("v") else "v" + version
    logger.info("version %s download successful, saved to %s", vversion, filename)
    return EXIT_SUCCESS
