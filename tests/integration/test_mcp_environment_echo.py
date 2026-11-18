# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests of the MCP server's mask over the credentials its readers take from its environment.

Real Polars and real deltalake read a side from a store on this machine that
answers every request as S3 answers one it refuses: with an error that repeats
the session token, the access key id, and the request it could not verify. A
token request to Azure's authority gets its form body back, and a Unity Catalog
request gets its bearer token back, in a workspace's JSON error. The configuration
names no `${NAME}`: each reader takes its credentials from the server's
environment on its own, or from a credentials file it finds there. A control
turns the server's record of the environment and its blanking of S3's elements
off, and shows each probe does echo the value, where its reader repeats the
error's body.
"""

import json
import multiprocessing
import os
import sys
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Final
from urllib.parse import quote, unquote

import anyio
import polars as pl
import pytest
from mcp import Client
from mcp.types import CallToolResult
from packaging.version import Version

from veridelta import mcp_server
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.integration]


class _Echo(BaseHTTPRequestHandler):
    """Refuse every request, repeating what it was sent as S3, Azure, or Unity Catalog would."""

    def _refuse(self) -> None:
        """Answer 403 with the text the request's credentials would come back in."""
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode() if length else ""
        if "/unity-catalog/" in self.path:
            authorization = self.headers.get("Authorization") or ""
            text = json.dumps(
                {"error_code": "PERMISSION_DENIED", "message": f"Invalid token {authorization}"}
            )
        elif "/oauth2" in self.path:
            text = f"<Error><Code>invalid_client</Code><Message>{body}</Message></Error>"
        else:
            signed = self.headers.get("Authorization") or ""
            key_id = signed.partition("Credential=")[2].partition("/")[0]
            headers = "".join(f"{name.lower()}:{value}\n" for name, value in self.headers.items())
            text = (
                "<Error><Code>InvalidToken</Code><Message>The provided token is malformed or "
                "otherwise invalid.</Message>"
                f"<Token-0>{self.headers.get('x-amz-security-token', '')}</Token-0>"
                f"<AWSAccessKeyId>{key_id}</AWSAccessKeyId>"
                f"<CanonicalRequest>{self.command} {self.path}\n{headers}</CanonicalRequest>"
                "</Error>"
            )
        data = text.encode()
        self.send_response(403)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def do_GET(self) -> None:
        """Refuse a read or a listing."""
        self._refuse()

    def do_HEAD(self) -> None:
        """Refuse a look at an object."""
        self._refuse()

    def do_POST(self) -> None:
        """Refuse a token request."""
        self._refuse()

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


_POLARS_ECHOES_S3: Final = Version(pl.__version__) >= Version("2.0.0")
"""Whether Polars repeats the body of an S3 error, which holds the credentials.

Before 2.0.0, Polars looks an object up with a HEAD request first, and S3
answers one with no body, so its error ends at the status and holds no
credential at all. From 2.0.0 on it sends a GET, whose answer repeats them.
A probe of every release from 1.39.3, the floor, to 2.1.0 shows the change at
2.0.0."""


@dataclass(frozen=True)
class _Case:
    """A reader, a location, the credentials it takes from the environment, and whether it echoes them."""

    scheme: str
    delta: bool
    environment: Callable[[Path, str], dict[str, str]]
    echoed: tuple[str, ...]
    echoes: bool = True


_KEY_ID = "AKIAQ7ZX3LEDGER9READ"
_SECRET_KEY = "wJal+rXUtn/FEMI.K7MDENG=bPxRfiCYx"
_SESSION_TOKEN = "IQoJb3JpZ2luX2VjEJrLedgerSessionTokQ7"
_PROFILE_KEY_ID = "AKIAPROFILEQ7LEDGER9"
_PROFILE_TOKEN = "IQoJb3JpZ2luProfileSessionTokenQ7"
_CLIENT_ID = "0e2f7c1a-3b9d-4e58-9a0c-7d6e5f4a3b2c"
_TENANT_ID = "6f1c2a9e-3b7d-4e58-9a0c-1b2c3d4e5f6a"
_CLIENT_SECRET = "Mk18Q~ab.cd_ef-gh+ij/kl=Zq9x7Pw"
_FEDERATED_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJsZWRnZXIifQ.c2lnbmF0dXJlTGVkZ2VyUTc"
_DATABRICKS_TOKEN = "dapi7f3c9a1e5b2d4f6a8c0e1b3d5f7a9c2e4b"
_UNITY_TABLE: Final = "main.default.t"
"""The `catalog.schema.table` a `uc://` case reads, which is the host the server allows."""


def _s3(endpoint: str) -> dict[str, str]:
    """Send every S3 request to the echo store, over HTTP, with no instance role to fall back on."""
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }


def _keys(token_name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give the readers a key pair and a session token under `token_name`."""

    def environment(_: Path, endpoint: str) -> dict[str, str]:
        return {
            **_s3(endpoint),
            "AWS_ACCESS_KEY_ID": _KEY_ID,
            "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
            token_name: _SESSION_TOKEN,
        }

    return environment


def _profile(folder: Path, endpoint: str) -> dict[str, str]:
    """Give the readers a credentials file only, which the server never reads."""
    credentials = folder / "credentials"
    credentials.write_text(
        f"[default]\naws_access_key_id = {_PROFILE_KEY_ID}\n"
        f"aws_secret_access_key = {_SECRET_KEY}\naws_session_token = {_PROFILE_TOKEN}\n",
        encoding="utf-8",
    )
    return {
        **_s3(endpoint),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials),
        "AWS_CONFIG_FILE": str(folder / "no-config"),
    }


def _authority(endpoint: str) -> dict[str, str]:
    """Send every Azure request, and the token request, to the echo store, over HTTP."""
    return {
        "AZURE_CLIENT_ID": _CLIENT_ID,
        "AZURE_TENANT_ID": _TENANT_ID,
        "AZURE_AUTHORITY_HOST": endpoint,
        "AZURE_STORAGE_ACCOUNT_NAME": "acct",
        "AZURE_ENDPOINT": endpoint,
        "AZURE_ALLOW_HTTP": "true",
    }


def _azure(secret_name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give the readers an Entra client secret under `secret_name`."""

    def environment(_: Path, endpoint: str) -> dict[str, str]:
        return {**_authority(endpoint), secret_name: _CLIENT_SECRET}

    return environment


def _federated(file_name: str) -> Callable[[Path, str], dict[str, str]]:
    """Give the readers a federated token in a file that `file_name` names."""

    def environment(folder: Path, endpoint: str) -> dict[str, str]:
        token = folder / "token"
        token.write_text(_FEDERATED_TOKEN + "\n", encoding="utf-8")
        return {**_authority(endpoint), file_name: str(token)}

    return environment


def _unity(token_name: str) -> Callable[[Path, str], dict[str, str]]:
    """Send Unity Catalog's requests to the echo store, over HTTP, with a token under `token_name`."""

    def environment(_: Path, endpoint: str) -> dict[str, str]:
        return {
            "DATABRICKS_HOST": endpoint,
            "UNITY_ALLOW_HTTP_URL": "true",
            token_name: _DATABRICKS_TOKEN,
        }

    return environment


_CASES: dict[str, _Case] = {
    "polars-keys": _Case(
        "s3", False, _keys("AWS_SESSION_TOKEN"), (_KEY_ID, _SESSION_TOKEN), _POLARS_ECHOES_S3
    ),
    "deltalake-keys": _Case("s3", True, _keys("AWS_SESSION_TOKEN"), (_KEY_ID, _SESSION_TOKEN)),
    "polars-aws-token": _Case(
        "s3", False, _keys("AWS_TOKEN"), (_KEY_ID, _SESSION_TOKEN), _POLARS_ECHOES_S3
    ),
    "deltalake-aws-token": _Case("s3", True, _keys("AWS_TOKEN"), (_KEY_ID, _SESSION_TOKEN)),
    # deltalake reads no credentials file: it asks for an instance role instead.
    "polars-profile": _Case(
        "s3", False, _profile, (_PROFILE_KEY_ID, _PROFILE_TOKEN), _POLARS_ECHOES_S3
    ),
    "polars-azure": _Case(
        "az", False, _azure("AZURE_CLIENT_SECRET"), (_CLIENT_ID, _TENANT_ID, _CLIENT_SECRET)
    ),
    # object_store reads any case after the prefix, so it sends this name's value too.
    "polars-azure-mixed-case": _Case("az", False, _azure("AZURE_Client_Secret"), (_CLIENT_SECRET,)),
    "deltalake-azure-mixed-case": _Case(
        "az", True, _azure("AZURE_Client_Secret"), (_CLIENT_SECRET,)
    ),
    "polars-federated-mixed-case": _Case(
        "az", False, _federated("AZURE_Federated_Token_File"), (_FEDERATED_TOKEN,)
    ),
    "deltalake-unity-token": _Case("uc", True, _unity("DATABRICKS_TOKEN"), (_DATABRICKS_TOKEN,)),
    "deltalake-unity-access-token": _Case(
        "uc", True, _unity("UNITY_ACCESS_TOKEN"), (_DATABRICKS_TOKEN,)
    ),
    # deltalake's Unity Catalog reader reads any case after `DATABRICKS_` too.
    "deltalake-unity-mixed-case": _Case(
        "uc", True, _unity("DATABRICKS_Token"), (_DATABRICKS_TOKEN,)
    ),
}
"""Each reader and the credentials it takes from the environment, by name."""

_NAMES: Final = [
    pytest.param(
        name,
        marks=pytest.mark.skipif(
            sys.platform == "win32" and "mixed-case" in name,
            reason="Windows holds each environment variable's name in upper case, so no "
            "reader sees a name in mixed case.",
        ),
    )
    for name in _CASES
]
"""Each case's name, with those that set a name in mixed case kept to Linux and macOS."""

_TOOLS: list[tuple[str, dict[str, Any]]] = [
    ("describe_schema", {"side": "source"}),
    ("run_comparison", {}),
    ("validate_config", {"schemas": True}),
]
"""Each tool that reads a side, with the arguments it needs beside the path."""


@pytest.fixture
def echo_store() -> Iterator[str]:
    """Run the echo store on a free local port until the test ends.

    Yields:
        str: Its endpoint, over HTTP.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Echo)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _holds_a_form(shown: str, value: str) -> bool:
    """Say whether an answer holds `value` as written or percent-encoded, once decoded, in any case."""
    texts = (shown.casefold(), unquote(shown).casefold())
    forms = (value.casefold(), quote(value, safe="").casefold())
    return any(form in text for form in forms for text in texts)


def _answers(name: str, folder: str, endpoint: str, control: bool) -> list[str]:
    """Set a case's environment, call each tool that reads the side, and return each answer.

    This runs in a child process. It leaves each reader only the credentials
    the case gives, and reaches the store with no proxy. With `control`, the
    server neither records its environment or token files nor blanks S3's
    elements.
    """
    case, root = _CASES[name], Path(folder)
    os.chdir(root)
    for variable in list(os.environ):
        if variable.upper().startswith(("AWS_", "AZURE_", "GOOGLE_", "UNITY_", "DATABRICKS_")):
            del os.environ[variable]
    os.environ.update(
        {"NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost"},
        **case.environment(root, endpoint),
    )
    if control:
        setattr(mcp_server, "_environment_values", lambda _: set())  # noqa: B010 - the child patches as monkeypatch would
        setattr(mcp_server, "_token_file_values", lambda _: set())  # noqa: B010 - as above
        setattr(mcp_server, "_without_echoed_credentials", lambda text: text)  # noqa: B010 - as above
    bucket = _UNITY_TABLE if case.scheme == "uc" else f"bkt-{name}"
    if case.delta:
        table = bucket if case.scheme == "uc" else f"{bucket}/t"
        source = f"  type: delta\n  table_uri: {case.scheme}://{table}\n"
        # deltalake's storage handler lists nothing from a store on this machine and never
        # returns, so the walk of the log is skipped. The table's own load still runs.
        setattr(mcp_server, "delta_log_vectors", lambda *_: [])  # noqa: B010 - as above
    else:
        source = f"  path: {case.scheme}://{bucket}/a.parquet\n"
    (root / "b.csv").write_text("id\n1\n", encoding="utf-8")
    (root / "veridelta.yaml").write_text(
        f"source:\n{source}target:\n  path: b.csv\nprimary_keys: [id]\n", encoding="utf-8"
    )
    settings = Settings((root,), allow_row_values=True, allowed_hosts=(bucket,))

    async def session() -> list[CallToolResult]:
        async with Client(build_server(settings)) as client:
            return [
                await client.call_tool(tool, {"path": "veridelta.yaml", **arguments})
                for tool, arguments in _TOOLS
            ]

    return [
        f"{result.structured_content}{''.join(getattr(block, 'text', '') for block in result.content)}"
        for result in anyio.run(session)
    ]


def _in_a_child(name: str, folder: Path, endpoint: str, *, control: bool = False) -> list[str]:
    """Run `_answers` in a new process, and return what it returns.

    Polars starts a thread on its first read from a cloud store, which reads
    the environment for as long as the process runs. A later test that changes
    the environment at the same moment would crash the suite's process, so
    each case reads in a process of its own.
    """
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as pool:
        return pool.submit(_answers, name, str(folder), endpoint, control).result()


@pytest.mark.parametrize("name", _NAMES)
def test_no_credential_from_the_environment_comes_back(
    name: str, tmp_path: Path, echo_store: str
) -> None:
    """Ensure each tool's answer holds no form of a credential the readers took and sent.

    Where the reader repeats the error's body, the answer shows the mask.
    """
    case = _CASES[name]
    for shown in _in_a_child(name, tmp_path, echo_store):
        assert ("***" in shown) is case.echoes
        for value in case.echoed:
            assert not _holds_a_form(shown, value), value


@pytest.mark.parametrize("name", _NAMES)
def test_each_probe_echoes_the_credentials_without_the_mask(
    name: str, tmp_path: Path, echo_store: str
) -> None:
    """Ensure the store does echo a credential, so the test above shows the mask at work.

    A reader that does not repeat the error's body echoes nothing, so the
    case says which it is, and an answer that differs fails either way.
    """
    case = _CASES[name]
    answers = _in_a_child(name, tmp_path, echo_store, control=True)

    assert [any(_holds_a_form(shown, value) for value in case.echoed) for shown in answers] == [
        case.echoes
    ] * len(_TOOLS)
