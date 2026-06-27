"""PARITY: internal/util/docker.go — detect running inside a (Linux) Docker/LXC container."""

from __future__ import annotations

from eds.util.file import exists


def is_running_inside_docker() -> bool:
    """PARITY: util.IsRunningInsideDocker — check /.dockerenv, else /proc/1/cgroup for docker/lxc/rt.

    The checks are Linux-specific (as in Go); on other OSes neither path exists so this returns False."""
    if exists("/.dockerenv"):
        return True
    if exists("/proc/1/cgroup"):
        try:
            with open("/proc/1/cgroup", "rb") as f:
                buf = f.read()
        except OSError:
            buf = b""
        if buf:
            contents = buf.decode("utf-8", "replace").strip()
            return "docker" in contents or "lxc" in contents or "rt" in contents
    return False
