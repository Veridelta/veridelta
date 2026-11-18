# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests of the MCP server's mask over each URL login in its environment.

Real Polars and real deltalake read a side from a store on this machine that
refuses every request and repeats none of it. The store's endpoint, in
`AWS_ENDPOINT_URL`, holds a login, which each reader writes back in its error.
A proxy URL with a login, whose port no socket can open, is written back by
the client that signs Polars in through an AWS profile. A proxy login with an
unencoded `#`, or a `://` after a login written with no scheme, is cut where
that client ends the URL's authority, so the server refuses to start on it.
The configuration names no `${NAME}`. A control turns the server's record of its environment
off, and shows each probe does echo the login.
"""

import multiprocessing
import os
import threading
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
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
from veridelta.exceptions import ConfigError
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.integration]


class _Refuse(BaseHTTPRequestHandler):
    """Refuse every request with 403, repeating nothing of it."""

    def _refuse(self) -> None:
        """Answer 403 with a body that holds no text of the request."""
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        data = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
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
        """Refuse any other request."""
        self._refuse()

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


_POLARS_ECHOES_PROXY: Final = Version(pl.__version__) >= Version("2.0.0")
"""Whether Polars signs in through an AWS profile with a client that writes a proxy URL back.

From 2.0.0 on, Polars assumes a profile's role through botocore, which writes
a proxy URL it cannot parse back whole. Polars 1.39.3, the floor, writes only
`error sending request for url (...)`, which names the store and not the proxy."""

_LOGIN: Final = "lgnreader:Q7lgnpass"
"""The login in the store's endpoint."""

_PASSWORD: Final = "Q7lgnpass"
"""The password in the store's endpoint."""

_PROXY_LOGIN: Final = "proxyuser9:Pr0xyPass9"
"""The login in the proxy's URL."""

_PROXY_PASSWORD: Final = "Pr0xyPass9"
"""The password in the proxy's URL."""

_CUT_PROXY_URL: Final = f"http://{_PROXY_LOGIN}#corp@127.0.0.1:99999"
"""A proxy URL whose password holds an unencoded `#`, which ends the URL's authority."""

_SCHEME_CUT_PROXY_URL: Final = "proxyuser9:Pr0x://yPass9@127.0.0.1:99999"
"""A proxy URL with no scheme, whose password holds a `://`.

The text before the `://` is no scheme, so botocore puts `http://` before the
value, and urllib3 ends the authority at the `/`."""

_KEY_ID: Final = "AKIAQ7ZX3LOGIN9READ"
_SECRET_KEY: Final = "wJal+rXUtn/FEMI.K7MDENG=bPxRfiCYx"
_BASE_KEY_ID: Final = "AKIAQ7ZX3PROXY9BASE"


def _endpoint(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Send every S3 request to the store, through an endpoint with a login."""
    del folder
    return (
        {
            "AWS_ENDPOINT_URL": f"http://{_LOGIN}@127.0.0.1:{port}",
            "AWS_ACCESS_KEY_ID": _KEY_ID,
            "AWS_SECRET_ACCESS_KEY": _SECRET_KEY,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_EC2_METADATA_DISABLED": "true",
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
        },
        "bkt",
        "127.0.0.1",
    )


def _proxy(
    folder: Path, port: int, url: str = f"http://{_PROXY_LOGIN}@127.0.0.1:99999"
) -> tuple[dict[str, str], str, str]:
    """Sign in through an AWS profile that assumes a role, through a proxy with a login."""
    del port
    config = folder / "aws-config"
    config.write_text(
        "[profile reader]\nrole_arn = arn:aws:iam::123456789012:role/ledger-reader\n"
        "source_profile = base\nregion = us-east-1\n\n"
        f"[profile base]\naws_access_key_id = {_BASE_KEY_ID}\n"
        f"aws_secret_access_key = {_SECRET_KEY}\n",
        encoding="utf-8",
    )
    return (
        {
            "AWS_CONFIG_FILE": str(config),
            "AWS_SHARED_CREDENTIALS_FILE": str(folder / "no-credentials"),
            "AWS_PROFILE": "reader",
            "AWS_EC2_METADATA_DISABLED": "true",
            # The port is out of range, so no socket opens.
            "HTTPS_PROXY": url,
        },
        "lake",
        "lake",
    )


def _cut_proxy(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Sign in as `_proxy` does, through a proxy whose login an unencoded `#` cuts."""
    return _proxy(folder, port, _CUT_PROXY_URL)


def _scheme_cut_proxy(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Sign in as `_proxy` does, through a proxy with no scheme whose password holds a `://`."""
    return _proxy(folder, port, _SCHEME_CUT_PROXY_URL)


_TOOLS: list[tuple[str, dict[str, Any]]] = [
    ("describe_schema", {"side": "source"}),
    ("validate_config", {"schemas": True}),
]
"""Each tool that reads a side, with the arguments it needs beside the path."""

_CASES: Final = {
    "endpoint-login-polars": (_endpoint, False),
    "endpoint-login-deltalake": (_endpoint, True),
    "proxy-login-polars": (_proxy, False),
    "cut-proxy-login-polars": (_cut_proxy, False),
    "scheme-cut-proxy-login-polars": (_scheme_cut_proxy, False),
}
"""Each case's environment and whether it reads a Delta Lake table, by name."""


@pytest.fixture
def refusing_store() -> Iterator[int]:
    """Run the refusing store on a free local port until the test ends.

    Yields:
        int: Its port.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Refuse)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _holds(shown: str, value: str) -> bool:
    """Say whether an answer holds `value` as written or percent-encoded, once decoded, in any case."""
    texts = (shown.casefold(), unquote(shown).casefold())
    forms = (value.casefold(), quote(value, safe="").casefold())
    return any(form in text for form in forms for text in texts)


def _answers(name: str, folder: str, port: int, control: bool) -> list[str]:
    """Set a case's environment, call each tool that reads the side, and return each answer.

    This runs in a child process. It leaves each reader only the credentials,
    endpoint, and proxy the case gives. With `control`, the server records
    nothing from its environment. A server that refuses to start answers its
    refusal, once.
    """
    environment, delta = _CASES[name]
    root = Path(folder)
    os.chdir(root)
    for variable in list(os.environ):
        upper = variable.upper()
        if upper.startswith(("AWS_", "AZURE_", "GOOGLE_")) or upper.endswith("_PROXY"):
            del os.environ[variable]
    values, bucket, host = environment(root, port)
    os.environ.update(values)
    # The masking is what this file tests, so an endpoint the server would refuse still reaches it.
    setattr(mcp_server, "_refuse_environment_endpoints", lambda *_: None)  # noqa: B010 - the child patches as monkeypatch would, and main has no such function
    if control:
        setattr(mcp_server, "_environment_values", lambda _: set())  # noqa: B010 - as above
    if delta:
        source = f"  type: delta\n  table_uri: s3://{bucket}/t\n"
        # deltalake's storage handler lists nothing from a store on this machine and never
        # returns, so the walk of the log is skipped. The table's own load still runs.
        setattr(mcp_server, "delta_log_vectors", lambda *_: [])  # noqa: B010 - as above
    else:
        source = f"  path: s3://{bucket}/a.parquet\n"
    (root / "b.csv").write_text("id\n1\n", encoding="utf-8")
    (root / "veridelta.yaml").write_text(
        f"source:\n{source}target:\n  path: b.csv\nprimary_keys: [id]\n", encoding="utf-8"
    )
    settings = Settings((root,), allow_row_values=True, allowed_hosts=tuple({bucket, host}))

    try:
        server = build_server(settings)
    except ConfigError as error:
        return [f"refused: {error}"]

    async def session() -> list[CallToolResult]:
        async with Client(server) as client:
            return [
                await client.call_tool(tool, {"path": "veridelta.yaml", **arguments})
                for tool, arguments in _TOOLS
            ]

    return [
        f"{result.structured_content}{''.join(getattr(block, 'text', '') for block in result.content)}"
        for result in anyio.run(session)
    ]


def _in_a_child(name: str, folder: Path, port: int, *, control: bool = False) -> list[str]:
    """Run `_answers` in a new process, and return what it returns.

    Polars starts a thread on its first read from a cloud store, which reads
    the environment for as long as the process runs, so each case reads in a
    process of its own.
    """
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as pool:
        return pool.submit(_answers, name, str(folder), port, control).result()


@pytest.mark.parametrize("name", ["endpoint-login-polars", "endpoint-login-deltalake"])
def test_no_endpoint_login_comes_back(name: str, tmp_path: Path, refusing_store: int) -> None:
    """Ensure each answer holds no form of the endpoint's login, and keeps the URL's scheme."""
    for shown in _in_a_child(name, tmp_path, refusing_store):
        assert not _holds(shown, f"{_LOGIN}@"), shown
        assert not _holds(shown, _PASSWORD), shown
        assert "://" in shown


@pytest.mark.parametrize("name", ["endpoint-login-polars", "endpoint-login-deltalake"])
def test_each_reader_echoes_the_endpoint_login_without_the_mask(
    name: str, tmp_path: Path, refusing_store: int
) -> None:
    """Ensure each reader writes the endpoint's login back, so the test above shows the mask."""
    for shown in _in_a_child(name, tmp_path, refusing_store, control=True):
        assert f"{_LOGIN}@" in shown, shown


def test_no_proxy_login_comes_back(tmp_path: Path, refusing_store: int) -> None:
    """Ensure each answer holds no form of the proxy's login."""
    for shown in _in_a_child("proxy-login-polars", tmp_path, refusing_store):
        assert not _holds(shown, _PROXY_LOGIN), shown
        assert not _holds(shown, _PROXY_PASSWORD), shown


def test_polars_echoes_the_proxy_login_without_the_mask(
    tmp_path: Path, refusing_store: int
) -> None:
    """Ensure Polars writes the proxy's login back exactly from 2.0.0 on, as the probe found.

    Before 2.0.0 its error names only the store, so the control holds no login.
    """
    answers = _in_a_child("proxy-login-polars", tmp_path, refusing_store, control=True)

    for shown in answers:
        holds = _PROXY_LOGIN in shown and _PROXY_PASSWORD in shown
        assert holds == _POLARS_ECHOES_PROXY, shown


_CUT_CASES: Final = [
    pytest.param("cut-proxy-login-polars", f"'{_PROXY_LOGIN}'", id="hash"),
    pytest.param("scheme-cut-proxy-login-polars", "'proxyuser9:Pr0x:'", id="no-scheme"),
]
"""Each case of a cut proxy login, with the head of it Polars writes back without the check."""


@pytest.mark.parametrize(("case", "head"), _CUT_CASES)
def test_it_refuses_a_proxy_login_no_client_can_parse(
    tmp_path: Path, refusing_store: int, case: str, head: str
) -> None:
    """Ensure a proxy login that a client cuts stops the server, naming only the variable."""
    del head
    (shown,) = _in_a_child(case, tmp_path, refusing_store)

    assert shown.startswith("refused: "), shown
    assert "environment variable HTTPS_PROXY must hold no '@'" in shown
    assert not _holds(shown, "proxyuser9"), shown
    assert not _holds(shown, _PROXY_PASSWORD), shown


@pytest.mark.parametrize(("case", "head"), _CUT_CASES)
def test_polars_echoes_a_cut_proxy_login_without_the_check(
    tmp_path: Path, refusing_store: int, case: str, head: str
) -> None:
    """Ensure Polars writes the head of a cut proxy login back from 2.0.0 on, which no mask matches.

    urllib3 ends the authority at the `#`, or at the `/` of a `://` that
    follows no scheme, and botocore passes its parse error on, with the user
    and the password up to that character. Before 2.0.0 the error names only
    the store.
    """
    answers = _in_a_child(case, tmp_path, refusing_store, control=True)

    for shown in answers:
        assert not shown.startswith("refused: "), shown
        assert (head in shown) == _POLARS_ECHOES_PROXY, shown
