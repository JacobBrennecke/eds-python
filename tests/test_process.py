"""PARITY: go-common command.Fork (via the C# ProcessForker)."""

from __future__ import annotations

import sys
import threading
import time

from eds.util.process import SYSTEM_PROCESS_FORKER, ForkArgs, fork


def test_captures_exit_code() -> None:
    r = fork(ForkArgs(executable=sys.executable, args=["-c", "import sys; sys.exit(3)"]))
    assert r.exit_code == 3


def test_captures_stderr_tail() -> None:
    r = fork(
        ForkArgs(
            executable=sys.executable,
            args=["-c", "import sys; sys.stderr.write('boom\\n'); sys.exit(1)"],
        )
    )
    assert r.exit_code == 1
    assert "boom" in r.last_error_lines


def test_save_logs(tmp_path) -> None:
    r = fork(
        ForkArgs(
            executable=sys.executable,
            dir=str(tmp_path),
            save_logs=True,
            log_filename_label="child",
            args=["-c", "print('hello out')"],
        )
    )
    assert r.exit_code == 0
    assert "hello out" in (tmp_path / "child_stdout.txt").read_text()
    assert (tmp_path / "child_stderr.txt").exists()


def test_process_callback_receives_child() -> None:
    pids: list[int] = []
    fork(
        ForkArgs(
            executable=sys.executable,
            args=["-c", "pass"],
            process_callback=lambda p: pids.append(p.pid),
        )
    )
    assert len(pids) == 1
    assert pids[0] > 0


def test_system_forker_seam() -> None:
    r = SYSTEM_PROCESS_FORKER.fork(ForkArgs(executable=sys.executable, args=["-c", "import sys; sys.exit(7)"]))
    assert r.exit_code == 7


def test_context_cancel_kills_child() -> None:
    ev = threading.Event()

    def cancel_soon() -> None:
        time.sleep(0.3)
        ev.set()

    threading.Thread(target=cancel_soon, daemon=True).start()
    start = time.monotonic()
    r = fork(ForkArgs(executable=sys.executable, args=["-c", "import time; time.sleep(30)"], context=ev))
    assert time.monotonic() - start < 10  # killed promptly, not the full 30s
    assert r.exit_code != 0  # terminated, not a clean exit
