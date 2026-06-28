"""PARITY: internal/util/util.go — file / net helpers."""

from __future__ import annotations

import os
import re
import socket
import tempfile

# PARITY: isWindowsDriveLetter — a leading ASCII drive letter + colon + slash (RE2 ^[a-zA-Z]:[/\\]).
_WIN_DRIVE = re.compile(r"^[a-zA-Z]:[/\\]")


def exists(fn: str) -> bool:
    """PARITY: util.Exists — false only when the path does not exist (other stat errors → true)."""
    try:
        os.stat(fn)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        # PARITY: Go returns false only for os.IsNotExist; any other error still yields true.
        return True


def is_localhost(url: str) -> bool:
    """PARITY: util.IsLocalhost — substring match on localhost / 127.0.0.1 / 0.0.0.0."""
    return "localhost" in url or "127.0.0.1" in url or "0.0.0.0" in url


def list_dir(directory: str) -> list[str]:
    """PARITY: util.ListDir — recurse, skip ``.DS_Store``, return file paths.

    Go's os.ReadDir yields entries sorted by name; sorted() reproduces that order."""
    res: list[str] = []
    for name in sorted(os.listdir(directory)):
        full = os.path.join(directory, name)
        if os.path.isdir(full):
            res.extend(list_dir(full))
        elif name != ".DS_Store":
            res.append(full)
    return res


def _is_abs_path(p: str) -> bool:
    return p.startswith("/") or bool(_WIN_DRIVE.match(p))


def _clean_forward_slash(p: str) -> str:
    slashed = p.replace("\\", "/")
    leading = slashed.startswith("/")
    parts = [seg for seg in slashed.split("/") if seg]
    joined = "/".join(parts)
    return ("/" + joined) if leading else joined


def to_file_uri(directory: str, file: str) -> str:
    """PARITY: util.ToFileURI (SPEC §8.15). DEVIATION: OS-independent normalization (matches the reviewed C#
    Files.ToFileUri) rather than Go's os.PathSeparator/filepath.* branching, so a unix-absolute dir stays
    file:///… on Windows. A dir is absolute if it starts with '/' OR is a Windows drive path; else made
    absolute. Forward-slash-cleaned (collapse dup slashes, keep one leading slash, strip trailing); '.'/'..'
    are NOT resolved (C# also drops that — irrelevant for the CRDB temp dirs this serves)."""
    if not _is_abs_path(directory) and not _WIN_DRIVE.match(directory):
        directory = os.path.abspath(directory)
    abs_dir = _clean_forward_slash(directory)
    joined = ("/" + file) if abs_dir == "" else (abs_dir.rstrip("/") + "/" + file)
    return "file://" + joined


def is_dir_writable(path: str) -> tuple[bool, str | None]:
    """PARITY: util.IsDirWritable — probe-write a temp file (like the C# Files.IsDirWritable). (ok, message)."""
    try:
        fd, tmp = tempfile.mkstemp(dir=path)
    except OSError as e:
        return False, str(e)
    os.close(fd)
    try:
        os.remove(tmp)
    except OSError:
        pass
    return True, None


def get_free_port() -> int:
    """PARITY: util.GetFreePort — ask the kernel for a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]
