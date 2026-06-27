"""PARITY: internal/osext/osext.go — the running program's executable path.

Go reads the OS for the actual running binary; Python uses ``sys.executable`` — for a PyInstaller-frozen
build that IS the ``eds`` binary (the production case). Under ``python -m eds`` it is the interpreter, so
the process-fork helper builds the full re-invocation command separately (frozen → [exe, args];
dev → [python, -m, eds, args]).
"""

from __future__ import annotations

import os
import sys


def executable() -> str:
    """PARITY: osext.Executable — absolute, cleaned path to re-invoke the current program."""
    return os.path.normpath(os.path.abspath(sys.executable))


def executable_folder() -> str:
    """PARITY: osext.ExecutableFolder — the directory of the executable."""
    return os.path.dirname(executable())
