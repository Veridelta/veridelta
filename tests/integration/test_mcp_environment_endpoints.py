# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests of the MCP server's rule on the endpoints its readers take from its environment.

Real Polars and real deltalake read a side whose configuration names no
endpoint, while the server's environment names a store on this machine over
plain HTTP, through a variable, an AWS profile, or the Azure emulator switch.
The store counts every request it receives. Each case expects the tool to
refuse the side, naming the variable, before any request reaches the store. A
control turns the rule off and shows the reader does reach the store, for the
cases proven on every Polars release CI runs.
"""

import importlib.util
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


class _Counter(BaseHTTPRequestHandler):
    """Refuse every request, and record it on the server."""

    def _refuse(self) -> None:
        """Record the request, then answer 403 with no body."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode(errors="replace") if length else ""
        cast("_CountingServer", self.server).requests.append(_Request(self.command, body))
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


class _CountingServer(ThreadingHTTPServer):
    """A store on this machine that keeps every request it receives."""

    def __init__(self) -> None:
        """Listen on a free local port."""
        super().__init__(("127.0.0.1", 0), _Counter)
        self.requests: list[_Request] = []


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
    "azure-storage-endpoint": _Case(
        "  path: az://lake/a.parquet\n", _azure_endpoint, "azure_storage_endpoint"
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
}
"""Each reader, the environment that names the store, and the name its refusal gives, by case."""

_CONTROLLED: Final = ("aws-endpoint-url", "aws-profile", "azure-authority")
"""The cases whose reader reaches the store without the rule on every Polars release CI runs.

Each was probed on Polars 1.39.3, the floor, and on 2.x. The Delta Lake case
has no control, since deltalake's listing hangs against a store on this
machine once the rule is off."""


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
        if variable.upper().startswith(("AWS_", "AZURE_", "GOOGLE_", "BIGQUERY_", "MOTHERDUCK_")):
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
            "AWS_CONFIG_FILE": str(root / "no-aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(root / "no-aws-credentials"),
        },
        **case.environment(root, endpoint),
    )
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


@pytest.mark.parametrize("name", _CONTROLLED)
def test_each_reader_reaches_the_store_without_the_rule(
    name: str, tmp_path: Path, store: _CountingServer
) -> None:
    """Ensure each controlled reader does send its requests to the store once the rule is off.

    The Azure sign-in posts the configuration's client secret there in clear.
    """
    endpoint = f"http://127.0.0.1:{store.server_port}"

    _in_a_child(name, tmp_path, endpoint, control=True)

    assert store.requests
    if name == "azure-authority":
        assert any(
            request.method == "POST" and _CLIENT_SECRET in unquote_plus(request.body)
            for request in store.requests
        )


def test_boto3_is_installed() -> None:
    """Ensure boto3 is installed, as CI installs every extra, so the profile case reads a profile."""
    assert importlib.util.find_spec("boto3") is not None
