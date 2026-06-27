"""PARITY: internal/util/sysinfo.go — system info, machine id, local IP.

get_machine_id reproduces denisbrodbeck/machineid.ProtectedID("eds") = HMAC-SHA256(key=OS machine GUID,
msg="eds") as lowercase hex. get_local_ip reproduces Go's RFC1918 check EXACTLY (Python's
ipaddress.is_private is broader and would pick a different address). DEVIATIONS (see DEVIATIONS.md):
sysinfo-hostinfo-partial (gopsutil's full host.Info subset), sysinfo-go-version (Python version substitutes
the Go runtime version).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import platform
import socket
import sys
import time
from dataclasses import dataclass

from eds.util.gojson import marshal


@dataclass
class HostInfo:
    """PARITY: gopsutil host.InfoStat (declaration order; no omitempty). Partially populated — see
    DEVIATIONS.md#sysinfo-hostinfo-partial."""

    hostname: str = ""
    uptime: int = 0
    boot_time: int = 0
    procs: int = 0
    os: str = ""
    platform: str = ""
    platform_family: str = ""
    platform_version: str = ""
    kernel_version: str = ""
    kernel_arch: str = ""
    virtualization_system: str = ""
    virtualization_role: str = ""
    host_id: str = ""

    def __gojson__(self) -> str:
        return (
            '{"hostname":' + marshal(self.hostname)
            + ',"uptime":' + marshal(self.uptime)
            + ',"bootTime":' + marshal(self.boot_time)
            + ',"procs":' + marshal(self.procs)
            + ',"os":' + marshal(self.os)
            + ',"platform":' + marshal(self.platform)
            + ',"platformFamily":' + marshal(self.platform_family)
            + ',"platformVersion":' + marshal(self.platform_version)
            + ',"kernelVersion":' + marshal(self.kernel_version)
            + ',"kernelArch":' + marshal(self.kernel_arch)
            + ',"virtualizationSystem":' + marshal(self.virtualization_system)
            + ',"virtualizationRole":' + marshal(self.virtualization_role)
            # PARITY: gopsutil's HostID tag is lowercase "hostid" (inconsistent with its camelCase siblings).
            + ',"hostid":' + marshal(self.host_id)
            + "}"
        )


@dataclass
class SystemInfo:
    """PARITY: sysinfo.go SystemInfo (declaration order; no omitempty)."""

    host: HostInfo | None = None
    num_cpu: int = 0
    go_version: str = ""

    def __gojson__(self) -> str:
        return (
            '{"host":' + marshal(self.host)
            + ',"num_cpu":' + marshal(self.num_cpu)
            + ',"go_version":' + marshal(self.go_version)
            + "}"
        )


def _go_os() -> str:
    p = sys.platform
    if p.startswith("win"):
        return "windows"
    if p == "darwin":
        return "darwin"
    if p.startswith("linux"):
        return "linux"
    return p


def get_system_info() -> SystemInfo:
    """PARITY: GetSystemInfo. DEVIATION: HostInfo is partially populated (stdlib/psutil best-effort);
    go_version is substituted with the Python version."""
    try:
        raw_id = _read_raw_machine_id()
    except OSError:
        raw_id = ""

    boot = 0
    procs = 0
    try:
        import psutil

        boot = int(psutil.boot_time())
        procs = len(psutil.pids())
    except (ImportError, OSError):
        pass

    host = HostInfo(
        hostname=socket.gethostname(),
        uptime=int(time.time()) - boot if boot else 0,
        boot_time=boot,
        procs=procs,
        os=_go_os(),
        platform=_go_os(),
        platform_version=platform.version(),
        kernel_arch=platform.machine().lower(),
        host_id=raw_id,
    )
    return SystemInfo(host=host, num_cpu=os.cpu_count() or 1, go_version=platform.python_version())


def compute_protected_id(machine_id: str, app_id: str) -> str:
    """PARITY: machineid.protect — HMAC-SHA256(key=machine_id, msg=app_id), lowercase hex."""
    return hmac.new(machine_id.encode("utf-8"), app_id.encode("utf-8"), hashlib.sha256).hexdigest()


def get_machine_id() -> str:
    """PARITY: GetMachineId = machineid.ProtectedID("eds")."""
    return compute_protected_id(_read_raw_machine_id(), "eds")


def _read_raw_machine_id() -> str:
    """PARITY: machineid.machineID() — the per-OS raw machine GUID."""
    if sys.platform.startswith("win"):
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Cryptography",
            0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        ) as key:
            guid, _ = winreg.QueryValueEx(key, "MachineGuid")
            return guid  # PARITY: Windows path does not trim
    if sys.platform == "darwin":
        import subprocess

        out = subprocess.check_output(["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"], text=True)
        for line in out.splitlines():
            if "IOPlatformUUID" in line:
                return line.split(' = ')[1].strip().strip('"')
        raise OSError("machineid: could not read IOPlatformUUID")
    # Linux: /var/lib/dbus/machine-id first, then /etc/machine-id (PARITY: Go order).
    for path in ("/var/lib/dbus/machine-id", "/etc/machine-id"):
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip("\n").strip()
        except OSError:
            continue
    raise OSError("machineid: could not read machine id")


def _is_rfc1918(addr_bytes: bytes) -> bool:
    """PARITY: Go net.IP.IsPrivate (IPv4 RFC1918): 10/8, 172.16/12, 192.168/16. NOT Python's broader
    ipaddress.is_private."""
    return (
        addr_bytes[0] == 10
        or (addr_bytes[0] == 172 and 16 <= addr_bytes[1] <= 31)
        or (addr_bytes[0] == 192 and addr_bytes[1] == 168)
    )


def get_local_ip() -> str:
    """PARITY: GetLocalIP — the first non-loopback, private (RFC1918) IPv4 address, else error."""
    import psutil

    for addrs in psutil.net_if_addrs().values():
        for a in addrs:
            if a.family != socket.AF_INET:
                continue
            packed = socket.inet_aton(a.address)
            if packed[0] == 127:  # PARITY: IsLoopback (127/8)
                continue
            if _is_rfc1918(packed):
                return a.address
    raise RuntimeError("no private IP found")
