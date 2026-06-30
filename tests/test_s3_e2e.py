"""Docker-gated e2e: stream events into a real S3-compatible store (MinIO) via testcontainers.

Exercises the REAL lazy-boto3 connect path (provider detect → LocalStack/MinIO http endpoint, path-style
addressing, static credentials) and the buffered Flush upload, then reads the objects back. Skipped when
Docker, testcontainers, or boto3 are unavailable. Drives a generic MinIO container (testcontainers.core), so
it needs only testcontainers + boto3 (no extra `minio` package).
"""

from __future__ import annotations

import shutil
import subprocess
import time

import pytest

pytest.importorskip("testcontainers.core.container")
pytest.importorskip("boto3")


def _docker_up() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not _docker_up(), reason="Docker not available"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),  # testcontainers internal deprecations
]

from eds.dbchange import DBChangeEvent  # noqa: E402
from eds.driver import DriverConfig  # noqa: E402
from eds.drivers.s3 import S3Driver  # noqa: E402
from eds.util.gojson import RawJson, stringify  # noqa: E402


class _QuietLogger:
    def trace(self, m, *a): ...
    def debug(self, m, *a): ...
    def info(self, m, *a): ...
    def warn(self, m, *a): ...
    def error(self, m, *a): ...
    def fatal(self, m, *a): ...
    def with_prefix(self, p): return self
    def with_fields(self, f): return self


def _make_admin_client(endpoint: str, access_key: str, secret_key: str):
    import boto3

    return boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access_key,
        aws_secret_access_key=secret_key, region_name="us-east-1",
        config=boto3.session.Config(s3={"addressing_style": "path"}),
    )


def test_streams_events_into_s3_compatible_store() -> None:
    from testcontainers.core.container import DockerContainer

    access_key = "minioadmin"
    secret_key = "minioadmin"
    bucket = "eds-test"
    container = (
        DockerContainer("minio/minio:latest")
        .with_env("MINIO_ROOT_USER", access_key)
        .with_env("MINIO_ROOT_PASSWORD", secret_key)
        .with_command("server /data")
        .with_exposed_ports(9000)
    )
    with container:
        host_ip = container.get_container_host_ip()
        port = container.get_exposed_port(9000)
        endpoint = f"http://{host_ip}:{port}"
        admin = _make_admin_client(endpoint, access_key, secret_key)

        # poll until MinIO is ready, then provision the bucket
        deadline = time.time() + 60
        while True:
            try:
                admin.create_bucket(Bucket=bucket)
                break
            except Exception:  # noqa: BLE001
                if time.time() > deadline:
                    raise
                time.sleep(1.0)

        url = (
            f"s3://{host_ip}:{port}/{bucket}/prefix"
            f"?region=us-east-1&access-key-id={access_key}&secret-access-key={secret_key}"
        )
        log = _QuietLogger()
        driver = S3Driver()
        driver.start(DriverConfig(url=url, logger=log))
        try:
            evt = DBChangeEvent(
                operation="INSERT", id="evt1", table="customer", key=["c1"], timestamp=1_700_000_000_000,
                after=RawJson('{"id":"c1","name":"Alice"}'),
            )
            assert driver.process(log, evt) is False
            driver.flush(log)

            key = "prefix/customer/1700000000-c1.json"
            obj = admin.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read().decode("utf-8")
            assert body == stringify(evt)
            assert obj["ContentType"] == "application/json"
        finally:
            driver.stop()
