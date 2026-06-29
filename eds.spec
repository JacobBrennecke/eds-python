# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller one-file spec for the EDS consumer (M10 — single-binary, mirrors the Go release artifact).

Bundles: every eds.* module (collect_submodules, so the dynamically-registered drivers are included), the embedded
PGP key (shopmonkey.asc), the jsonschema meta-schemas (jsonschema_specifications package data, else schema
validation can't find draft-07), and the DB/runtime libs that are imported lazily inside driver code (PyInstaller's
static analysis can miss function-level imports). Snowflake's connector is optional — included only if installed.
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

datas = [("eds/shopmonkey.asc", "eds")]
datas += collect_data_files("jsonschema_specifications")  # the bundled draft meta-schemas

hiddenimports = collect_submodules("eds")
# psutil is lazy-imported by metrics.py + sysinfo.py; boto3/confluent_kafka/azure.eventhub are lazy-imported
# inside the s3/kafka/eventhub drivers — PyInstaller's static analysis misses these function-level imports.
for _mod in ("psycopg", "pymysql", "pymssql", "snowflake.connector", "pgpy",
             "jsonschema", "referencing", "msgpack", "xxhash", "nats", "nkeys", "tomli",
             "psutil", "prometheus_client", "boto3", "confluent_kafka", "azure.eventhub"):
    try:
        __import__(_mod)
    except ImportError:
        continue
    hiddenimports.append(_mod)

a = Analysis(
    ["packaging/eds_entry.py"],
    pathex=["."],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["pytest", "mypy", "ruff", "PyInstaller"],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="eds",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
