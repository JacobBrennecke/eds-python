"""PARITY: internal/util/sysinfo.go — vectors from the C# SysInfoTests + computed HMAC vectors."""

from __future__ import annotations

import re
import socket

import pytest

from eds.util.gojson import stringify
from eds.util.sysinfo import HostInfo, SystemInfo, compute_protected_id, get_local_ip, get_machine_id, get_system_info

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


@pytest.mark.parametrize(
    ("machine_id", "app_id", "want"),
    [
        ("machine-guid", "eds", "7fd8bec7cc89aa4d96ef4dac0d807ae2f8e330c97f0ee70c520a374fa95b8f0f"),
        ("machine-guid", "other", "cac19920def7b6ecd92a2df2098f76bdd47b6589032d6d88e0095de829c30410"),
        ("other-guid", "eds", "fe4ad91282498cd8decac38f972ff608a3873deb6419cf2721ab31e12a80e2ad"),
        ("12345678-1234-1234-1234-123456789abc", "eds",
         "fe95fcbe54e18dfbbab5d550b7421971fc0d0ee67fd49334f4d0e7ac3d32392a"),
        ("", "eds", "8b85ea725684c4a58e243221c6321d8779c01b81b3d4652b81c0027a71a7eba2"),
        ("eds", "eds", "940588ee7a9dd350b35fb7498f07bd765323a2f1745a4fa8d5b7408a57c1c98c"),
    ],
)
def test_compute_protected_id(machine_id: str, app_id: str, want: str) -> None:
    assert compute_protected_id(machine_id, app_id) == want


def test_compute_protected_id_properties() -> None:
    v = compute_protected_id("machine-guid", "eds")
    assert _HEX64.match(v)
    assert compute_protected_id("machine-guid", "eds") != compute_protected_id("other-guid", "eds")  # key matters
    assert compute_protected_id("machine-guid", "eds") != compute_protected_id("machine-guid", "other")  # msg matters


def test_get_machine_id_is_stable_64_hex() -> None:
    a = get_machine_id()
    assert _HEX64.match(a)
    assert get_machine_id() == a  # stable


def test_get_local_ip_returns_private_ipv4_or_raises() -> None:
    try:
        ip = get_local_ip()
    except RuntimeError as e:
        assert "no private IP" in str(e)
        return
    socket.inet_aton(ip)  # parseable IPv4
    assert not ip.startswith("127.")


def test_get_system_info_basic_fields() -> None:
    si = get_system_info()
    assert si.host is not None
    assert si.host.hostname
    assert si.num_cpu > 0
    assert si.go_version


def test_gojson_field_order() -> None:
    si = SystemInfo(
        host=HostInfo(
            hostname="h", uptime=1, boot_time=2, procs=3, os="linux", platform="linux",
            platform_family="", platform_version="5.0", kernel_version="", kernel_arch="x86_64",
            virtualization_system="", virtualization_role="", host_id="hid",
        ),
        num_cpu=4,
        go_version="3.10.0",
    )
    assert stringify(si) == (
        '{"host":{"hostname":"h","uptime":1,"bootTime":2,"procs":3,"os":"linux","platform":"linux",'
        '"platformFamily":"","platformVersion":"5.0","kernelVersion":"","kernelArch":"x86_64",'
        '"virtualizationSystem":"","virtualizationRole":"","hostid":"hid"},"num_cpu":4,"go_version":"3.10.0"}'
    )
