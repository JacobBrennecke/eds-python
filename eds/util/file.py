"""PARITY: internal/util/util.go — file / net helpers.

``ToFileURI`` (the OS-dependent Windows drive-letter quirk, SPEC §8 #11) is deferred to M4 with the
File driver, where the reproduce-vs-correct decision is made against its usage.
"""

from __future__ import annotations

import os
import socket


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


def get_free_port() -> int:
    """PARITY: util.GetFreePort — ask the kernel for a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]
