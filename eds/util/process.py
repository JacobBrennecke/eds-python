"""PARITY: go-common/command.Fork (reproduced via the C# ProcessForker) — launch a child process,
capture stdout/stderr (to files and/or this process's streams), kill it on cancellation, and return its
exit code plus the tail of stderr.

The re-invocation handles Python's two run modes: a PyInstaller-frozen build re-invokes ``[eds.exe, …]``;
under ``python -m eds`` it re-invokes ``[python, -m, eds, …]`` (Go always has a single binary).
DEVIATIONS: #fork-forwardinterrupt-no-signal-relay (trap, don't relay), #fork-kill-direct-child
(proc.kill() targets the direct child; full process-tree kill via psutil is revisited at M9).
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

_MAX_ERROR_LINES = 100  # go-common keeps a bounded stderr tail; exact bound not in the repo.


@dataclass
class ForkArgs:
    """PARITY: command.ForkArgs. ``executable`` defaults to the running EDS program (frozen exe, or the
    interpreter re-invoking ``-m eds``)."""

    executable: str | None = None
    command: str = ""
    args: list[str] = field(default_factory=list)
    log_filename_label: str = "fork"
    save_logs: bool = False
    write_to_std: bool = False
    forward_interrupt: bool = False
    dir: str = "."
    log: Any = None  # duck-typed logger
    process_callback: Callable[[subprocess.Popen], None] | None = None
    context: Any = None  # cancellation: anything with .is_set() (ShutdownSignal / threading.Event)


@dataclass
class ForkResult:
    """PARITY: command.Fork result."""

    exit_code: int = 0
    last_error_lines: str = ""


def _self_invocation() -> list[str]:
    if getattr(sys, "frozen", False):  # PyInstaller one-file build -> sys.executable IS the eds binary
        return [sys.executable]
    return [sys.executable, "-m", "eds"]


def _pump(stream, file_handle, tee_stream, tail: deque | None, tail_lock: threading.Lock | None) -> None:
    for raw in stream:
        line = raw.rstrip("\n")
        if file_handle is not None:
            file_handle.write(line + "\n")
            file_handle.flush()
        if tee_stream is not None:
            print(line, file=tee_stream, flush=True)
        if tail is not None and tail_lock is not None:
            with tail_lock:
                tail.append(line)


def fork(args: ForkArgs) -> ForkResult:
    """PARITY: command.Fork."""
    cmd = [args.executable] if args.executable else _self_invocation()
    if args.command:
        cmd.append(args.command)
    cmd.extend(args.args)

    out_file = None
    err_file = None
    if args.save_logs:
        os.makedirs(args.dir, exist_ok=True)
        out_file = open(os.path.join(args.dir, args.log_filename_label + "_stdout.txt"), "w", encoding="utf-8")
        err_file = open(os.path.join(args.dir, args.log_filename_label + "_stderr.txt"), "w", encoding="utf-8")

    err_tail: deque[str] = deque(maxlen=_MAX_ERROR_LINES)
    err_lock = threading.Lock()
    done = threading.Event()

    if args.log is not None:
        args.log.trace("forking: %s", " ".join(cmd))

    proc = subprocess.Popen(
        cmd, cwd=args.dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    )
    threads = [
        threading.Thread(
            target=_pump,
            args=(proc.stdout, out_file, sys.stdout if args.write_to_std else None, None, None),
            daemon=True,
        ),
        threading.Thread(
            target=_pump,
            args=(proc.stderr, err_file, sys.stderr if args.write_to_std else None, err_tail, err_lock),
            daemon=True,
        ),
    ]
    for t in threads:
        t.start()

    if args.process_callback is not None:
        args.process_callback(proc)

    watcher = None
    if args.context is not None:
        def _watch() -> None:
            # PARITY: ctx cancellation kills the child (like exec.CommandContext). Poll so the watcher also
            # exits once the process finishes (done).
            while not done.wait(0.1):
                if args.context.is_set():
                    try:
                        if proc.poll() is None:
                            proc.kill()
                    except OSError:
                        pass
                    return

        watcher = threading.Thread(target=_watch, daemon=True)
        watcher.start()

    try:
        proc.wait()
    finally:
        done.set()
        for t in threads:
            t.join()
        if watcher is not None:
            watcher.join(timeout=1.0)
        # Close the child pipes (readers have hit EOF) to avoid ResourceWarnings.
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        if out_file is not None:
            out_file.close()
        if err_file is not None:
            err_file.close()

    if args.log is not None:
        args.log.debug("fork exited: %s (code %d)", args.log_filename_label, proc.returncode)
    with err_lock:
        last_err = "\n".join(err_tail)
    return ForkResult(exit_code=proc.returncode, last_error_lines=last_err)


class ProcessForker(Protocol):
    """Seam over fork() so the supervisor/control-plane loops are unit-testable with a fake (M9)."""

    def fork(self, args: ForkArgs) -> ForkResult: ...


class SystemProcessForker:
    """The production forker."""

    def fork(self, args: ForkArgs) -> ForkResult:
        return fork(args)


SYSTEM_PROCESS_FORKER = SystemProcessForker()
