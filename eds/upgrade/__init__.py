"""PARITY: internal/upgrade — download + PGP-verify + extract a release, and the atomic binary swap.

upgrade() is portable (it drives `eds download`); apply() is the inconshreveable/go-update rename-swap of the
RUNNING executable and is only meaningful for a single packaged binary (the M10 PyInstaller eds.exe) — under
`python -m eds` the executable is the interpreter, so the live upgrade path is gated on sys.frozen.

DEVIATIONS: upgrade-pgp-pgpy (gopenpgp → pgpy, pure-Python detached verify of the Ed25519/algo-22 key);
upgrade-apply-only-for-frozen-binary; upgrade-hidefile-ctypes; download-arch-goreleaser-mapping (amd64→x86_64,
386→i386 per .goreleaser.yaml); upgrade-archive-missing-member-raises (harden vs Go's silent fall-through, like C#).
"""

from __future__ import annotations

import os
import platform
import shutil
import sys
import tarfile
import tempfile
import time
import warnings
import zipfile
from dataclasses import dataclass
from typing import Any

from eds.cmd.session import _default_transport
from eds.util.duration import format_duration
from eds.util.http import HttpRetry
from eds.util.logger import Logger

# PARITY: .goreleaser.yaml name template (uname-style) — Python's platform.machine() differs (amd64 on Windows).
_ARCH_MAP = {"amd64": "x86_64", "x86_64": "x86_64", "386": "i386", "i386": "i386", "arm64": "arm64", "aarch64": "arm64"}


@dataclass
class UpgradeConfig:
    """PARITY: upgrade.UpgradeConfig (never serialized)."""

    logger: Logger
    binary_url: str
    signature_url: str
    filename: str
    public_key: str


class RollbackError(Exception):
    """PARITY: rollbackErr — the new→target rename failed AND the old→target rollback also failed (catastrophic)."""

    def __init__(self, original: BaseException, rollback: BaseException) -> None:
        super().__init__(str(original))
        self.original = original
        self.rollback = rollback


def rollback_error(err: BaseException | None) -> BaseException | None:
    """PARITY: RollbackError() — the inner rollback error iff err is a RollbackError, else None."""
    if err is None:
        return None
    return err.rollback if isinstance(err, RollbackError) else None


def build_release_urls(version: str) -> tuple[str, str]:
    """PARITY: download.go URL build — eds_<Platform>_<arch>.<ext> + .sig, with the goreleaser arch mapping."""
    if not version.startswith("v"):
        version = "v" + version
    os_name = platform.system()  # Windows / Linux / Darwin (already title-cased)
    arch = _ARCH_MAP.get(platform.machine().lower(), platform.machine().lower())
    ext = "zip" if os_name == "Windows" else "tar.gz"
    binary_url = f"https://github.com/shopmonkeyus/eds/releases/download/{version}/eds_{os_name}_{arch}.{ext}"
    return binary_url, binary_url + ".sig"


def verify_detached_signature(data: bytes, signature: bytes, public_key: str) -> None:
    """PARITY: gopenpgp crypto.Auto detached verify — via pgpy (armored key, armored-or-binary sig, whole-file)."""
    with warnings.catch_warnings():  # pgpy emits CryptographyDeprecationWarnings; filterwarnings=error would trip
        warnings.simplefilter("ignore")
        import pgpy

        try:
            key, _ = pgpy.PGPKey.from_blob(public_key)
            sig = pgpy.PGPSignature.from_blob(signature)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"error verifying signature data: {e}") from e
        try:
            ok = bool(key.verify(data, sig))
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"error verifying signature: {e}") from e
    if not ok:
        raise RuntimeError("error in signature verification: signature does not match")


def upgrade(config: UpgradeConfig, *, transport: Any = None) -> None:
    """PARITY: upgrade.Upgrade — download the archive + detached sig, verify, extract the binary to filename."""
    transport = transport or _default_transport
    started = time.monotonic()
    fd, tmp = tempfile.mkstemp(prefix="eds")
    os.close(fd)
    try:
        config.logger.trace("created temp file %s to download archive", tmp)
        binary = _get(transport, config.binary_url, config.logger)
        with open(tmp, "wb") as f:
            f.write(binary)
        config.logger.debug("downloaded binary of size %d bytes from %s", len(binary), config.binary_url)
        signature = _get(transport, config.signature_url, config.logger)
        config.logger.debug("downloaded signature of size %d bytes from %s", len(signature), config.signature_url)
        verify_detached_signature(binary, signature, config.public_key)
        config.logger.debug("verified signature of binary")
        _extract(config, tmp)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
        config.logger.debug("download took %s", format_duration(time.monotonic() - started))


def _get(transport: Any, url: str, logger: Logger) -> bytes:
    resp: Any = HttpRetry(lambda: transport("GET", url, {}, None), method="GET", url=url, logger=logger).do()
    return resp.content


def _extract(config: UpgradeConfig, archive: str) -> None:
    # PARITY: archive choice is URL-extension-driven, not OS-driven.
    if os.path.splitext(config.binary_url)[1] == ".zip":
        config.logger.debug("extracting zip file: %s", archive)
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                config.logger.trace("zip file: %s", info.filename)
                if info.filename.endswith(".exe"):
                    with zf.open(info) as af, open(config.filename, "wb") as out:
                        shutil.copyfileobj(af, out)
                    return  # PARITY: zip branch returns before chmod
        raise RuntimeError("no .exe found in archive")  # DEVIATION: harden vs Go's silent empty success
    config.logger.debug("extracting tar.gz file: %s", archive)
    with tarfile.open(archive, "r:gz") as tf:
        for member in tf:
            config.logger.trace("tar file: %s", member.name)
            if member.name == "eds":
                src = tf.extractfile(member)
                if src is None:
                    raise RuntimeError("eds entry is not a regular file")
                with src, open(config.filename, "wb") as out:
                    shutil.copyfileobj(src, out)
                break
        else:
            raise RuntimeError("no eds binary found in archive")  # DEVIATION: harden vs Go's wrapped-EOF
    os.chmod(config.filename, 0o755)


def apply(target_path: str, source_path: str) -> None:
    """PARITY: upgrade.Apply — stage source as .<name>.new then atomically swap it over target_path."""
    _prepare_and_check_binary(target_path, source_path)
    _commit_binary(target_path)


def _prepare_and_check_binary(target_path: str, source_path: str) -> None:
    update_dir = os.path.dirname(target_path)
    filename = os.path.basename(target_path)
    new_path = os.path.join(update_dir, f".{filename}.new")
    # close the handle BEFORE the move (Windows refuses to rename an open file)
    with open(source_path, "rb") as src, open(new_path, "wb") as fp:
        shutil.copyfileobj(src, fp)
    os.chmod(new_path, 0o755)


def _commit_binary(target_path: str) -> None:
    update_dir = os.path.dirname(target_path)
    filename = os.path.basename(target_path)
    new_path = os.path.join(update_dir, f".{filename}.new")
    old_path = os.path.join(update_dir, f".{filename}.old")
    try:  # Windows can't overwrite on rename; a prior upgrade may leave a hidden/locked .old
        os.remove(old_path)
    except OSError:
        pass
    os.replace(target_path, old_path)  # original still intact if this raises
    try:
        os.replace(new_path, target_path)
    except OSError as err:
        try:
            os.replace(old_path, target_path)  # rollback
        except OSError as rerr:
            raise RollbackError(err, rerr) from err  # catastrophic: no binary at target
        raise
    try:
        os.remove(old_path)  # running exe is locked on Windows → hide instead
    except OSError:
        # PARITY: Go discards both the remove AND the hide error after a successful swap (`_ = hideFile(oldPath)`);
        # a hide failure must NEVER turn an already-applied upgrade into a failure.
        try:
            _hide_file(old_path)
        except OSError:
            pass


def _hide_file(path: str) -> None:
    """PARITY: hideFile — SetFileAttributesW(path, FILE_ATTRIBUTE_HIDDEN); no-op off Windows."""
    if sys.platform.startswith("win"):
        import ctypes

        if not ctypes.windll.kernel32.SetFileAttributesW(str(path), 0x2):  # noqa: F821 (Windows-only)
            raise ctypes.WinError()
