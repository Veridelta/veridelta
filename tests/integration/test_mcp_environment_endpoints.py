# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests of the MCP server's rule on the endpoints its readers take from its environment.

Real Polars, real deltalake, and PyArrow under PyIceberg read a side whose
configuration names no endpoint, while the server's environment names a store
on this machine over plain HTTP, through a variable, an AWS profile, a
Google service account key, or the Azure emulator switch. A region or an
Azure account such as `x@127.0.0.1:port/` moves a reader's HTTPS host to the
store too. The store counts every request it receives, and every TLS
handshake. Each case expects the tool to refuse the side, naming the
variable, before anything reaches the store. A control turns the rule off
and shows the reader does reach the store, for the cases proven on every
release CI runs.
"""

import importlib.util
import json
import multiprocessing
import os
import socket
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Final, cast
from urllib.parse import unquote_plus

import anyio
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from mcp import Client

from veridelta import mcp_server
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.integration]

_KEY_ID: Final = "AKIAQ7ZX3LEDGER9READ"
_SECRET_KEY: Final = "wJal+rXUtn/FEMI.K7MDENG=bPxRfiCYx"
_ACCOUNT_KEY: Final = "a2V5a2V5a2V5a2V5"
_CLIENT_SECRET: Final = "Mk18Q~ab.cd_ef-gh+ij/kl=Zq9x7Pw"


@dataclass(frozen=True)
class _Request:
    """One request the store received."""

    method: str
    body: str
    authorization: str


class _Counter(BaseHTTPRequestHandler):
    """Refuse every request, and record it on the server."""

    def _refuse(self) -> None:
        """Record the request, then answer 403 with no body."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode(errors="replace") if length else ""
        authorization = self.headers.get("Authorization") or ""
        cast("_CountingServer", self.server).requests.append(
            _Request(self.command, body, authorization)
        )
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        """Refuse a read or a listing."""
        self._refuse()

    def do_HEAD(self) -> None:
        """Refuse a look at an object."""
        self._refuse()

    def do_POST(self) -> None:
        """Refuse a token request or a query."""
        self._refuse()

    def do_PUT(self) -> None:
        """Refuse a write."""
        self._refuse()

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


_TLS_HANDSHAKE: Final = b"\x16"
"""The first byte of a TLS handshake, which a reader sends to a host it reaches over HTTPS."""


class _CountingServer(ThreadingHTTPServer):
    """A store on this machine that keeps every request it receives, and counts every TLS handshake."""

    def __init__(self) -> None:
        """Listen on a free local port."""
        super().__init__(("127.0.0.1", 0), _Counter)
        self.requests: list[_Request] = []
        self.handshakes = 0

    def verify_request(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: object
    ) -> bool:
        """Count a connection that opens with a TLS handshake, and close it at once.

        A reader that a region or an account sends here speaks HTTPS, which
        this store does not, and would otherwise wait for an answer.
        """
        connection = cast("socket.socket", request)
        connection.settimeout(5)
        try:
            first = connection.recv(1, socket.MSG_PEEK)
        except OSError:
            return False
        connection.settimeout(None)
        if first == _TLS_HANDSHAKE:
            self.handshakes += 1
            return False
        return True


@dataclass(frozen=True)
class _Case:
    """A side, the environment that sends its reader to the store, and the name a refusal gives."""

    source: str
    environment: Callable[[Path, str], dict[str, str]]
    named: str
    hosts: tuple[str, ...] = ("lake",)
    delta: bool = False


def _s3(name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give the S3 readers a key pair, and the store's endpoint under `name`."""

    def environment(_: Path, endpoint: str) -> dict[str, str]:
        return {
            name: endpoint,
            "AWS_ACCESS_KEY_ID": _KEY_ID,
            "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
        }

    return environment


def _profile(folder: Path, endpoint: str) -> dict[str, str]:
    """Name the store as the endpoint_url of the AWS profile the environment selects."""
    config = folder / "aws-config"
    config.write_text(f"[profile p]\nendpoint_url = {endpoint}\n", encoding="utf-8")
    return {
        "AWS_CONFIG_FILE": str(config),
        "AWS_PROFILE": "p",
        "KEY_ID": _KEY_ID,
        "KEY_SECRET": _SECRET_KEY,
    }


def _delta(_: Path, endpoint: str) -> dict[str, str]:
    """Name the store in lowercase, which deltalake reads, with plain HTTP allowed."""
    return {
        "aws_endpoint_url": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": _KEY_ID,
        "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
    }


def _azure_endpoint(_: Path, endpoint: str) -> dict[str, str]:
    """Name the store as Azure's endpoint, with an account and its key."""
    return {
        "AZURE_STORAGE_ENDPOINT": endpoint,
        "AZURE_STORAGE_ACCOUNT_NAME": "acct",
        "AZURE_STORAGE_ACCOUNT_KEY": _ACCOUNT_KEY,
        "AZURE_ALLOW_HTTP": "true",
    }


def _emulator(_: Path, endpoint: str) -> dict[str, str]:
    """Turn the emulator switch on, with Azurite's URL at the store."""
    return {"AZURE_STORAGE_USE_EMULATOR": "true", "AZURITE_BLOB_STORAGE_URL": endpoint}


def _authority(_: Path, endpoint: str) -> dict[str, str]:
    """Send Entra ID's sign-in to the store, and give the configuration its client secret."""
    return {"AZURE_AUTHORITY_HOST": endpoint, "PROBE_SECRET": _CLIENT_SECRET}


def _google(_: Path, endpoint: str) -> dict[str, str]:
    """Name the store as Google Cloud Storage's base URL."""
    return {"GOOGLE_BASE_URL": endpoint}


def _service_account(folder: Path, **extra: object) -> str:
    """Write a service account key with a new RSA key, from which PyArrow signs its own bearer token.

    `extra` adds fields to the key, such as object_store's `gcs_base_url`.

    Returns:
        str: The key file's path.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    path = folder / "service-account.json"
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": "probe",
                "private_key_id": "0" * 40,
                "private_key": pem,
                "client_email": "reader@probe.iam.gserviceaccount.com",
                "client_id": "1",
                "token_uri": "https://oauth2.googleapis.com/token",
                **extra,
            }
        ),
        encoding="utf-8",
    )
    return str(path)


def _moved(endpoint: str) -> str:
    """Return a region or an account that moves a reader's host to the store, as `s3.x@127.0.0.1:port/.amazonaws.com` does."""
    return f"x@{endpoint.removeprefix('http://')}/"


def _region(name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give the S3 readers a key pair, and a region under `name` that moves their host to the store.

    `AWS_REGION` is unset unless it is `name`, since a reader prefers it to the others.
    """

    def environment(_: Path, endpoint: str) -> dict[str, str]:
        return {
            "AWS_REGION": "",
            name: _moved(endpoint),
            "AWS_ACCESS_KEY_ID": _KEY_ID,
            "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
        }

    return environment


def _profile_region(folder: Path, endpoint: str) -> dict[str, str]:
    """Name a region that moves the host to the store in the AWS profile the environment selects."""
    config = folder / "aws-config"
    config.write_text(f"[profile p]\nregion = {_moved(endpoint)}\n", encoding="utf-8")
    return {
        "AWS_REGION": "",
        "AWS_CONFIG_FILE": str(config),
        "AWS_PROFILE": "p",
        "AWS_ACCESS_KEY_ID": _KEY_ID,
        "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
    }


def _account(_: Path, endpoint: str) -> dict[str, str]:
    """Give the Azure readers an account key, and an account that moves their host to the store."""
    return {
        "AZURE_STORAGE_ACCOUNT_NAME": _moved(endpoint),
        "AZURE_STORAGE_ACCOUNT_KEY": _ACCOUNT_KEY,
    }


def _unprefixed(_: Path, endpoint: str) -> dict[str, str]:
    """Name the store with no AWS_ prefix, which deltalake reads as an S3 option, with plain HTTP allowed."""
    return {
        "ENDPOINT_URL": endpoint,
        "ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": _KEY_ID,
        "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
    }


def _key_with_base_url(name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give deltalake a service account key whose gcs_base_url is the store, in a file `name` names or in `name`."""

    def environment(folder: Path, endpoint: str) -> dict[str, str]:
        path = _service_account(folder, gcs_base_url=endpoint, disable_oauth=True)
        if name == "GOOGLE_SERVICE_ACCOUNT_KEY":
            return {name: Path(path).read_text(encoding="utf-8")}
        return {name: path}

    return environment


def _gcs_emulator(name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give PyArrow a service account key, and the store as Google Cloud Storage's emulator under `name`."""

    def environment(folder: Path, endpoint: str) -> dict[str, str]:
        return {name: endpoint, "GOOGLE_APPLICATION_CREDENTIALS": _service_account(folder)}

    return environment


def _here_with_gcs_files(folder: Path, endpoint: str) -> dict[str, str]:
    """Write an Iceberg table's metadata on this machine, whose snapshot lists its manifests in Google Cloud Storage."""
    metadata = folder / "t" / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "v1.metadata.json").write_text(
        json.dumps(
            {
                "format-version": 2,
                "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
                "location": "gs://lake/t",
                "last-sequence-number": 1,
                "last-updated-ms": 1,
                "last-column-id": 1,
                "current-schema-id": 0,
                "schemas": [
                    {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}],
                    }
                ],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {},
                "current-snapshot-id": 1,
                "snapshots": [
                    {
                        "snapshot-id": 1,
                        "sequence-number": 1,
                        "timestamp-ms": 1,
                        "manifest-list": "gs://lake/t/metadata/snap-1.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0,
                    }
                ],
                "snapshot-log": [],
                "metadata-log": [],
                "refs": {"main": {"snapshot-id": 1, "type": "branch"}},
            }
        ),
        encoding="utf-8",
    )
    return _gcs_emulator("CLOUD_STORAGE_TESTBENCH_ENDPOINT")(folder, endpoint)


_S3_FILE: Final = "  path: s3://lake/a.parquet\n"
_STORAGE_OPTIONS: Final = "  options:\n    storage_options:\n"

_CASES: dict[str, _Case] = {
    "aws-endpoint-url": _Case(_S3_FILE, _s3("AWS_ENDPOINT_URL"), "aws_endpoint_url"),
    "aws-endpoint": _Case(_S3_FILE, _s3("AWS_ENDPOINT"), "aws_endpoint"),
    "aws-endpoint-url-s3": _Case(_S3_FILE, _s3("AWS_ENDPOINT_URL_S3"), "aws_endpoint_url_s3"),
    "aws-profile": _Case(
        f"{_S3_FILE}{_STORAGE_OPTIONS}"
        "      aws_access_key_id: ${KEY_ID}\n      aws_secret_access_key: ${KEY_SECRET}\n",
        _profile,
        "aws profile",
    ),
    "delta-lowercase": _Case(
        "  type: delta\n  table_uri: s3://lake/t\n", _delta, "aws_endpoint_url", delta=True
    ),
    "delta-unprefixed": _Case(
        "  type: delta\n  table_uri: s3://lake/t\n",
        _unprefixed,
        "variable endpoint_url names",
        delta=True,
    ),
    "delta-opendal-s3": _Case(
        "  type: delta\n  table_uri: opendal+s3://lake/t\n",
        _s3("AWS_S3_ENDPOINT"),
        "aws_s3_endpoint",
        delta=True,
    ),
    "delta-service-account": _Case(
        "  type: delta\n  table_uri: gs://lake/t\n",
        _key_with_base_url("GOOGLE_SERVICE_ACCOUNT"),
        "variable google_service_account gives",
        delta=True,
    ),
    "delta-service-account-key": _Case(
        "  type: delta\n  table_uri: gs://lake/t\n",
        _key_with_base_url("GOOGLE_SERVICE_ACCOUNT_KEY"),
        "variable google_service_account_key gives",
        delta=True,
    ),
    "azure-storage-endpoint": _Case(
        "  path: az://lake/a.parquet\n",
        _azure_endpoint,
        "azure_storage_endpoint",
        hosts=("lake", "acct"),
    ),
    "azure-emulator": _Case(
        f"  path: az://lake/a.parquet\n{_STORAGE_OPTIONS}"
        "      azure_storage_endpoint: https://127.0.0.1:CLOSED\n",
        _emulator,
        "azure_storage_use_emulator",
        hosts=("lake", "127.0.0.1"),
    ),
    "azure-authority": _Case(
        f"  path: az://lake/a.parquet\n{_STORAGE_OPTIONS}"
        "      account_name: acct\n      azure_client_id: cid\n      azure_tenant_id: tid\n"
        "      azure_client_secret: ${PROBE_SECRET}\n",
        _authority,
        "azure_authority_host",
        hosts=("lake", "acct"),
    ),
    "google-base-url": _Case(
        f"  path: gs://lake/a.parquet\n{_STORAGE_OPTIONS}      skip_signature: 'true'\n",
        _google,
        "google_base_url",
    ),
    "iceberg-gcs-emulator": _Case(
        "  type: iceberg\n  table_uri: gs://lake/t/metadata/v1.metadata.json\n",
        _gcs_emulator("CLOUD_STORAGE_EMULATOR_ENDPOINT"),
        "cloud_storage_emulator_endpoint",
    ),
    "iceberg-here-gcs-files": _Case(
        "  type: iceberg\n  table_uri: t/metadata/v1.metadata.json\n",
        _here_with_gcs_files,
        "cloud_storage_testbench_endpoint",
    ),
    "aws-region": _Case(_S3_FILE, _region("AWS_REGION"), "variable aws_region is not a region"),
    "aws-default-region": _Case(
        _S3_FILE, _region("AWS_DEFAULT_REGION"), "variable aws_default_region is not a region"
    ),
    "aws-profile-region": _Case(
        _S3_FILE, _profile_region, "the region of the aws profile the server's environment"
    ),
    "delta-region": _Case(
        "  type: delta\n  table_uri: s3://lake/t\n",
        _region("AWS_REGION"),
        "variable aws_region is not a region",
        delta=True,
    ),
    "delta-region-unprefixed": _Case(
        "  type: delta\n  table_uri: s3://lake/t\n",
        _region("REGION"),
        "variable region is not a region",
        delta=True,
    ),
    "azure-account": _Case(
        "  path: az://lake/a.parquet\n",
        _account,
        "variable azure_storage_account_name names a storage account",
    ),
    "delta-azure-account": _Case(
        "  type: delta\n  table_uri: az://lake/t\n",
        _account,
        "variable azure_storage_account_name names a storage account",
        delta=True,
    ),
}
"""Each reader, the environment that names the store, and the name its refusal gives, by case."""

_CONTROLLED: Final = (
    "aws-endpoint-url",
    "aws-profile",
    "delta-lowercase",
    "delta-unprefixed",
    "delta-opendal-s3",
    "delta-service-account",
    "delta-service-account-key",
    "azure-authority",
    "iceberg-gcs-emulator",
    "iceberg-here-gcs-files",
    "aws-region",
    "aws-default-region",
    "delta-region",
    "delta-region-unprefixed",
    "azure-account",
    "delta-azure-account",
)
"""The cases whose reader reaches the store without the rule on every release CI runs.

Each Polars case was probed on Polars 1.39.3, the floor, and on 2.x. The
Delta Lake and Iceberg cases read through deltalake and PyArrow, whatever
the Polars release. A Delta Lake case answers once the store refuses its
listing, since the store runs in another process and the walk of the log is
skipped. Polars 1.39.3 reads no region from the AWS profile, so that case
has no control."""

_GOOGLE_CASES: Final = ("iceberg-gcs-emulator", "iceberg-here-gcs-files")
"""The cases whose reader sends the store a bearer token it signs with the service account key."""


_SIGNED_CASES: Final = ("delta-lowercase", "delta-unprefixed", "delta-opendal-s3")
"""The cases whose reader signs its requests to the store with the environment's AWS keys."""


def _closed_port() -> int:
    """Return a local port nothing listens on, so a request sent there fails at once."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture
def store() -> Iterator[_CountingServer]:
    """Run the counting store on a free local port until the test ends.

    Yields:
        _CountingServer: The store, whose `requests` lists what it received.
    """
    server = _CountingServer()
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _answer(name: str, folder: str, endpoint: str, closed: int, control: bool) -> tuple[bool, str]:
    """Set a case's environment, describe its side through the client, and return the answer.

    This runs in a child process. Every proxy points at a closed port on this
    machine, and only this machine bypasses them, so a request that would
    leave it fails at once. With `control`, the rule on environment endpoints
    is off.
    """
    case, root = _CASES[name], Path(folder)
    os.chdir(root)
    for variable in list(os.environ):
        if (
            variable.upper().startswith(
                ("AWS_", "AZURE_", "GOOGLE_", "GCE_", "CLOUD_STORAGE_", "BIGQUERY_", "MOTHERDUCK_")
            )
            or variable.upper() in mcp_server._DELTA_LAKE_ONLY
        ):
            del os.environ[variable]
    dead = f"http://127.0.0.1:{closed}"
    bypass = "127.0.0.1,localhost"
    os.environ.update(
        {
            "HTTP_PROXY": dead,
            "HTTPS_PROXY": dead,
            "ALL_PROXY": dead,
            "http_proxy": dead,
            "https_proxy": dead,
            "all_proxy": dead,
            "NO_PROXY": bypass,
            "no_proxy": bypass,
            "AWS_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            # Polars 2.x retries a request that fails to connect, which only slows a control.
            "POLARS_CLOUD_MAX_RETRIES": "0",
            "AWS_CONFIG_FILE": str(root / "no-aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(root / "no-aws-credentials"),
        },
        **(environment := case.environment(root, endpoint)),
    )
    for variable in (name for name, value in environment.items() if not value):
        # An empty value unsets the variable, as Windows does with one.
        del os.environ[variable]
    if control:
        setattr(mcp_server, "_refuse_environment_endpoints", lambda *_: None)  # noqa: B010 - the child patches as monkeypatch would
    if case.delta:
        # deltalake's storage handler lists nothing from a store on this machine and never
        # returns, so the walk of the log is skipped, should the rule let the side through.
        setattr(mcp_server, "delta_log_vectors", lambda *_: [])  # noqa: B010 - as above
    source = case.source.replace("CLOSED", str(closed))
    (root / "b.csv").write_text("id\n1\n", encoding="utf-8")
    (root / "veridelta.yaml").write_text(
        f"source:\n{source}target:\n  path: b.csv\nprimary_keys: [id]\n", encoding="utf-8"
    )
    settings = Settings((root,), allow_row_values=True, allowed_hosts=case.hosts)

    async def session() -> tuple[bool, str]:
        async with Client(build_server(settings)) as client:
            result = await client.call_tool(
                "describe_schema", {"path": "veridelta.yaml", "side": "source"}
            )
            return result.is_error, "".join(getattr(block, "text", "") for block in result.content)

    return anyio.run(session)


def _in_a_child(
    name: str, folder: Path, endpoint: str, *, control: bool = False
) -> tuple[bool, str]:
    """Run `_answer` in a new process, and return what it returns.

    Polars starts a thread on its first read from a cloud store, which reads
    the environment for as long as the process runs, so each case reads in a
    process of its own.
    """
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as pool:
        return pool.submit(_answer, name, str(folder), endpoint, _closed_port(), control).result()


@pytest.mark.parametrize("name", list(_CASES))
def test_no_reader_reaches_an_endpoint_from_the_environment(
    name: str, tmp_path: Path, store: _CountingServer
) -> None:
    """Ensure the tool refuses the side, naming the variable, before any request reaches the store."""
    endpoint = f"http://127.0.0.1:{store.server_port}"

    failed, text = _in_a_child(name, tmp_path, endpoint)

    assert failed, text
    assert _CASES[name].named in text.casefold(), text
    assert str(store.server_port) not in text
    assert store.requests == []
    assert store.handshakes == 0


@pytest.mark.parametrize("name", _CONTROLLED)
def test_each_reader_reaches_the_store_without_the_rule(
    name: str, tmp_path: Path, store: _CountingServer
) -> None:
    """Ensure each controlled reader does send its requests to the store once the rule is off.

    The Azure sign-in posts the configuration's client secret there in
    clear, PyArrow sends Google Cloud Storage a bearer token, and deltalake
    signs its S3 requests with the environment's keys. A reader that a region
    or an account moves opens a TLS handshake there.
    """
    endpoint = f"http://127.0.0.1:{store.server_port}"

    _in_a_child(name, tmp_path, endpoint, control=True)

    assert store.requests or store.handshakes
    if name == "azure-authority":
        assert any(
            request.method == "POST" and _CLIENT_SECRET in unquote_plus(request.body)
            for request in store.requests
        )
    if name in _GOOGLE_CASES:
        assert any(request.authorization.startswith("Bearer ") for request in store.requests)
    if name in _SIGNED_CASES:
        assert any(_KEY_ID in request.authorization for request in store.requests)


def test_boto3_is_installed() -> None:
    """Ensure boto3 is installed, as CI installs every extra, so the profile case reads a profile."""
    assert importlib.util.find_spec("boto3") is not None
