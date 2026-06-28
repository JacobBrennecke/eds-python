"""PARITY: the upgrade module — release URLs, detached PGP verify, extract, atomic apply/rollback."""

from __future__ import annotations

import gzip
import io
import os
import tarfile
import warnings
import zipfile

import pytest

from eds.upgrade import (
    RollbackError,
    UpgradeConfig,
    apply,
    build_release_urls,
    rollback_error,
    upgrade,
    verify_detached_signature,
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


LOG = _QuietLogger()


# ---- PGP fixtures (generate a key in-test; pgpy emits crypto warnings) ----
def _keypair():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import pgpy
        from pgpy.constants import HashAlgorithm, KeyFlags, PubKeyAlgorithm

        key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 2048)
        key.add_uid(pgpy.PGPUID.new("Test"), usage={KeyFlags.Sign}, hashes=[HashAlgorithm.SHA256])
        return key


def _sign(key, data: bytes) -> bytes:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return bytes(key.sign(data))  # binary detached signature (like goreleaser's .sig)


def test_build_release_urls(monkeypatch) -> None:
    import platform as _p

    monkeypatch.setattr(_p, "system", lambda: "Linux")
    monkeypatch.setattr(_p, "machine", lambda: "amd64")  # goreleaser maps → x86_64
    b, s = build_release_urls("1.2.3")
    assert b == "https://github.com/shopmonkeyus/eds/releases/download/v1.2.3/eds_Linux_x86_64.tar.gz"
    assert s == b + ".sig"

    monkeypatch.setattr(_p, "system", lambda: "Windows")
    monkeypatch.setattr(_p, "machine", lambda: "AMD64")
    b2, _ = build_release_urls("v2.0.0")  # already v-prefixed; windows → zip
    assert b2 == "https://github.com/shopmonkeyus/eds/releases/download/v2.0.0/eds_Windows_x86_64.zip"


def test_verify_detached_signature_good_and_bad() -> None:
    key = _keypair()
    pub = str(key.pubkey)
    data = b"eds binary contents"
    sig = _sign(key, data)
    verify_detached_signature(data, sig, pub)  # no raise = verified
    with pytest.raises(RuntimeError, match="signature"):
        verify_detached_signature(b"tampered", sig, pub)


def test_upgrade_tar_gz_download_verify_extract(tmp_path) -> None:
    key = _keypair()
    pub = str(key.pubkey)
    # build a tar.gz whose "eds" member is the binary
    binary = b"#!/fake eds binary\n" + os.urandom(64)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name="eds")
        info.size = len(binary)
        tf.addfile(info, io.BytesIO(binary))
    archive = buf.getvalue()
    sig = _sign(key, archive)

    def transport(method, url, headers, data=None):
        body = sig if url.endswith(".sig") else archive
        return type("R", (), {"content": body, "status_code": 200})()

    out = tmp_path / "eds-new"
    cfg = UpgradeConfig(logger=LOG, binary_url="https://x/eds_Linux_x86_64.tar.gz",
                        signature_url="https://x/eds_Linux_x86_64.tar.gz.sig", filename=str(out), public_key=pub)
    upgrade(cfg, transport=transport)
    assert out.read_bytes() == binary  # extracted the "eds" member after verifying the archive sig


def test_upgrade_rejects_bad_signature(tmp_path) -> None:
    key = _keypair()
    archive = gzip.compress(b"not a real tar")
    wrong_sig = _sign(key, b"different content")

    def transport(method, url, headers, data=None):
        body = wrong_sig if url.endswith(".sig") else archive
        return type("R", (), {"content": body, "status_code": 200})()

    cfg = UpgradeConfig(logger=LOG, binary_url="https://x/a.tar.gz", signature_url="https://x/a.tar.gz.sig",
                        filename=str(tmp_path / "out"), public_key=str(key.pubkey))
    with pytest.raises(RuntimeError, match="signature"):
        upgrade(cfg, transport=transport)


def test_upgrade_zip_extracts_exe(tmp_path) -> None:
    key = _keypair()
    binary = b"MZ fake exe"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("eds.exe", binary)
    archive = buf.getvalue()
    sig = _sign(key, archive)

    def transport(method, url, headers, data=None):
        return type("R", (), {"content": (sig if url.endswith(".sig") else archive), "status_code": 200})()

    out = tmp_path / "eds.exe"
    cfg = UpgradeConfig(
        logger=LOG, binary_url="https://x/eds_Windows_x86_64.zip",
        signature_url="https://x/eds_Windows_x86_64.zip.sig", filename=str(out), public_key=str(key.pubkey),
    )
    upgrade(cfg, transport=transport)
    assert out.read_bytes() == binary


def test_apply_atomic_swap(tmp_path) -> None:
    target = tmp_path / "eds"
    target.write_bytes(b"OLD")
    source = tmp_path / "eds-new"
    source.write_bytes(b"NEW")
    apply(str(target), str(source))
    assert target.read_bytes() == b"NEW"  # swapped in
    assert not (tmp_path / ".eds.new").exists()  # staged file consumed
    assert not (tmp_path / ".eds.old").exists()  # old removed (or hidden on Windows)


def test_apply_swallows_post_swap_hide_failure(tmp_path, monkeypatch) -> None:
    # PARITY: after a successful swap Go discards both the .old remove AND the hide error; a hide failure must
    # never turn an already-applied upgrade into a failure (regression for the success→failure inversion).
    import eds.upgrade as up

    target = tmp_path / "eds"
    target.write_bytes(b"OLD")
    source = tmp_path / "eds-new"
    source.write_bytes(b"NEW")
    real_remove = os.remove

    def failing_remove(p):
        if os.path.basename(str(p)) == ".eds.old":
            raise OSError("locked")  # simulate the running-exe .old being unremovable (Windows)
        real_remove(p)

    def failing_hide(p):
        raise OSError("hide failed")

    monkeypatch.setattr(os, "remove", failing_remove)
    monkeypatch.setattr(up, "_hide_file", failing_hide)
    up.apply(str(target), str(source))  # must NOT raise
    assert target.read_bytes() == b"NEW"  # swap still succeeded


def test_rollback_error_helper() -> None:
    assert rollback_error(None) is None
    assert rollback_error(RuntimeError("plain")) is None
    re = RollbackError(RuntimeError("orig"), RuntimeError("rb"))
    assert isinstance(rollback_error(re), RuntimeError) and str(rollback_error(re)) == "rb"
