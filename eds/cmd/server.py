"""PARITY: cmd/server.go serverCmd — Layer 1 (wrapper supervisor) + Layer 2 (control plane / session loop).

DEFERRED (to M9, clearly out of scope here): the NATS notification consumer + its 12 control closures
(restart/shutdown/pause/unpause/upgrade/import/configure/backfill/...), the interactive enroll loop, and the
self-upgrade. This runner runs a configured server (an --eds-id + --url must be supplied) end to end:
sendStart → write creds → fork the consumer → map the exit code → sendEnd + upload logs. Remote NATS control and
auto-enrollment land with the notification slice.
"""

from __future__ import annotations

import argparse
import os
import queue
import shutil
import subprocess
import threading
import time

from eds.cmd.args import collect_command_args
from eds.cmd.config import init_config
from eds.cmd.exit_codes import (
    EXIT_INCORRECT_USAGE,
    EXIT_NATS_DISCONNECTED,
    EXIT_RESTART,
    EXIT_SUCCESS,
    MAX_FAILURES,
)
from eds.cmd.loopback import LoopbackServer
from eds.cmd.notification_wiring import (
    ControlPlaneContext,
    NotificationRunner,
    build_notification_handler,
    is_nats_connection_error,
)
from eds.cmd.session import (
    AlreadyRunningError,
    send_end_and_upload,
    send_start,
    write_creds_to_file,
)
from eds.notification.dtos import SendLogsResponse
from eds.util.api import get_api_url_from_jwt
from eds.util.file import get_free_port
from eds.util.logger import Logger
from eds.util.process import ForkArgs, _self_invocation, fork
from eds.util.shutdown import ShutdownSignal


def run_server(args: argparse.Namespace, argv: list[str]) -> int:
    from eds.cmd import root as _root

    logger = _root.new_logger(args).with_prefix("[server]")
    if not args.wrapper:
        return _run_wrapper_loop(logger, argv)  # Layer 1
    return _run_control_plane(logger, args, argv, _root.VERSION)  # Layer 2


# --------------------------------------------------------------------------- Layer 1
def _run_wrapper_loop(logger: Logger, argv: list[str]) -> int:
    """PARITY: runWrapperLoop (server.go:311-432) — re-exec self as --wrapper, supervise with linear backoff.

    DEVIATION: the upgrade re-exec force-terminates the child (Windows has no per-child SIGINT); a genuine Ctrl-C
    reaches the child via the shared console group. The upgrade-failure binary-reset nuance is not modeled (the
    self-upgrade closure is deferred)."""
    wport = get_free_port()
    events: queue.Queue = queue.Queue()

    def on_restart() -> tuple[int, str]:
        events.put(("restart", 0))
        return 202, ""

    server = LoopbackServer(wport, {"/restart": on_restart})
    server.start()
    child_cmd = _self_invocation() + argv + ["--wrapper", f"--parent={wport}"]

    shutdown_sig = ShutdownSignal()

    def _on_shutdown() -> None:
        shutdown_sig.wait()
        events.put(("shutdown", 0))

    threading.Thread(target=_on_shutdown, daemon=True, name="wrapper-shutdown").start()

    exit_code = 1
    failures = 0
    completed = False
    try:
        while failures < MAX_FAILURES and not completed:
            proc = subprocess.Popen(child_cmd)  # inherit std streams (no capture at Layer 1)
            threading.Thread(
                target=lambda p=proc: events.put(("exited", p.wait())), daemon=True, name="wrapper-waiter"
            ).start()
            kind, code = events.get()
            if kind == "restart":
                _terminate(proc)
                _drain_exited(events)
                continue  # re-spawn (the on-disk binary may have been swapped by an upgrade)
            if kind == "shutdown":
                # PARITY: the child already received the console Ctrl-C — let it shut down gracefully (Go signals
                # SIGINT then Waits); force-terminate only as a backstop if it hangs.
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    _terminate(proc)
                exit_code = EXIT_SUCCESS
                completed = True
            elif kind == "exited":
                exit_code = code  # PARITY: server.go:390 records the child's exit code on EVERY exit
                if code in (EXIT_SUCCESS, 1):
                    completed = True
                else:
                    failures += 1
                    time.sleep(failures)  # linear backoff: 1s, 2s, 3s, 4s
    finally:
        server.stop()
    return exit_code


def _terminate(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _drain_exited(events: queue.Queue) -> None:
    try:
        while True:
            events.get_nowait()
    except queue.Empty:
        pass


# --------------------------------------------------------------------------- Layer 2
def _run_control_plane(logger: Logger, args: argparse.Namespace, argv: list[str], version: str) -> int:  # noqa: C901
    """PARITY: serverCmd --wrapper (server.go:498-1085) — the session loop that forks the consumer."""
    data_dir = os.path.abspath(os.path.normpath(args.data_dir))
    config = init_config(data_dir, argv)

    api_key = args.api_key or config.get_string("token")
    driver_url = args.url or config.get_string("url")
    server_id = args.eds_id or config.get_string("server_id")
    keep_logs = args.keep_logs or config.get_bool("keep_logs")
    nats_url = args.nats_url
    if not api_key:
        logger.fatal("an API key is required (--api-key or $SM_APIKEY)")
    if not server_id:
        # DEFERRED: interactive enrollment (server.go:445-479) is not yet ported.
        logger.fatal("no server id found; enrollment is not yet ported — pass --eds-id")
    if not driver_url:
        # DEFERRED: configure-via-notification (server.go:1003-1014) is not yet ported.
        logger.fatal("no driver url configured; configure-via-notification is not yet ported — pass --url")

    if args.api_url is None:  # PARITY: server.go:507-515 — derive the api url from the JWT (Fatal on a bad key)
        try:
            api_url = get_api_url_from_jwt(api_key)
        except ValueError as e:
            logger.fatal("invalid API key. %s", e)
            return EXIT_INCORRECT_USAGE  # unreachable (fatal exits)
    else:
        api_url = args.api_url
        logger.info("using alternate api url: %s", api_url)
    api_url = api_url.rstrip("/")

    if "localhost" in api_url:  # PARITY: server.go:924-926 — a localhost api url forces localhost NATS
        nats_url = "nats://localhost:4222"

    port = args.port
    if args.health_port > 0:  # PARITY: server.go:531-535 — the deprecated --health-port overrides --port
        port = args.health_port

    company_ids = args.company_ids or None
    base_args = collect_command_args(argv[1:])
    base_args += ["--port", str(port), "--data-dir", data_dir, "--server", nats_url, "--api-url", api_url]

    ctx = ControlPlaneContext(
        logger=logger, port=port, api_url=api_url, api_key=api_key, version=version, keep_logs=keep_logs
    )
    handler = build_notification_handler(ctx)

    shutdown_sig = ShutdownSignal()
    failures = 0
    current_creds_file: str | None = None
    try:
        while failures < MAX_FAILURES:
            try:
                session = send_start(logger, api_url, api_key, driver_url, server_id, company_ids, version=version)
            except AlreadyRunningError:
                logger.info("another server is already running for this id; retrying in 5s")
                time.sleep(5)
                continue
            except Exception as e:  # noqa: BLE001
                logger.fatal("failed to start session: %s", e)
                return EXIT_INCORRECT_USAGE  # unreachable (fatal exits) — for the type checker

            session_id = session.session_id
            session_dir = os.path.join(data_dir, session_id)
            os.makedirs(session_dir, mode=0o700, exist_ok=True)
            if session.credential is None:
                logger.fatal("no credential found in session")
            creds_file = os.path.join(session_dir, "nats.creds")
            write_creds_to_file(session.credential, creds_file)
            current_creds_file = creds_file
            logs_dir = os.path.join(session_dir, "logs")
            ctx.session_id = session_id

            runner = NotificationRunner(logger, nats_url, handler)
            try:
                runner.start(creds_file, renew_interval=args.renew_interval)
            except Exception as e:  # noqa: BLE001
                runner.stop()
                if is_nats_connection_error(e):  # PARITY: NATS-connect failure → retry in 5s (server.go:996)
                    logger.warn("failed to connect to nats: %s; retrying in 5s", e)
                    time.sleep(5)
                    continue
                # other Start errors: the control plane is auxiliary — log and fork anyway so data keeps streaming
                logger.error("failed to start notification consumer: %s; continuing without remote control", e)

            fork_args = base_args + [
                "--creds", creds_file,
                "--logs-dir", logs_dir,
                "--url", driver_url,
                "--server", nats_url,
            ]
            try:
                ctx.fork_running = True
                try:
                    result = fork(
                        ForkArgs(
                            command="fork",
                            args=fork_args,
                            log_filename_label="server",
                            save_logs=True,
                            write_to_std=True,
                            forward_interrupt=True,
                            dir=session_dir,
                            log=logger,
                            context=shutdown_sig,
                        )
                    )
                except Exception as e:  # noqa: BLE001 — PARITY: fork spawn failure → failures++ (server.go:1036)
                    failures += 1
                    logger.error("failed to fork consumer: %s (failure %d/%d)", e, failures, MAX_FAILURES)
                    continue
                ec = result.exit_code
                ctx.fork_running = False

                if args.no_restart:
                    return ec
                if ec != EXIT_INCORRECT_USAGE:  # exit 3 never uploads logs
                    log_file = ""
                    try:
                        from eds.cmd.session import get_remaining_log

                        log_file = get_remaining_log(logs_dir)
                    except OSError:
                        pass  # PARITY: missing logs dir → proceed with an empty logfile
                    errored = ec != EXIT_SUCCESS and ec != EXIT_RESTART
                    stderr_file = os.path.join(session_dir, "server_stderr.txt")
                    try:
                        log_path = send_end_and_upload(
                            logger, api_url, api_key, session_id, errored, log_file, stderr_file, version=version
                        )
                        # PARITY: server.go:1055 — report the uploaded log path back to HQ.
                        runner.publish_send_logs_response(SendLogsResponse(path=log_path, session_id=session_id))
                    except Exception as e:  # noqa: BLE001 — log upload is best-effort; do not abort the loop
                        logger.error("failed to upload logs: %s", e)

                if ec == EXIT_NATS_DISCONNECTED:
                    time.sleep(5)
                    continue
                if ec == EXIT_SUCCESS:
                    if not keep_logs:
                        shutil.rmtree(session_dir, ignore_errors=True)
                    return EXIT_SUCCESS
                if ec == EXIT_INCORRECT_USAGE or (
                    ec == 1
                    and (
                        "error: required flag" in result.last_error_lines
                        or "Global Flags" in result.last_error_lines
                    )
                ):
                    return ec
                if ec == EXIT_RESTART:
                    logger.info("shut down as part of restart")
                    continue
                failures += 1
                logger.error("consumer exited with code %d (failure %d/%d)", ec, failures, MAX_FAILURES)
            finally:
                ctx.fork_running = False
                runner.stop()  # PARITY: notificationConsumer.Stop() each iteration (server.go:1084)

        logger.fatal("too many failures, giving up")
        return 1
    finally:
        # PARITY: server.go:522-529 defer — always scrub the (last) creds file, even with --keep-logs.
        if current_creds_file and os.path.exists(current_creds_file):
            try:
                os.remove(current_creds_file)
            except OSError:
                pass
