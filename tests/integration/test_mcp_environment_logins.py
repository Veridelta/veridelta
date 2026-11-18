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
The same client reads the STS endpoint, which a login with an unencoded `/`
holds the same way. A scheme-relative proxy or STS endpoint, which starts
with `//`, keeps its login whole, and is masked.
The configuration names no `${NAME}`. A control turns the server's record of its environment
off, and shows each probe does echo the login.
"""

import multiprocessing
import os
import socket
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

_RELATIVE_PROXY_URL: Final = f"//{_PROXY_LOGIN}@127.0.0.1:99999"
"""A scheme-relative proxy URL, which botocore and requests read as `http:` and the value."""

_STS_LOGIN: Final = "stsuser9:Q7stsPass9"
"""The login in the STS endpoint."""

_STS_PASSWORD: Final = "Q7stsPass9"
"""The password in the STS endpoint."""

_CUT_STS_URL: Final = "http://stsuser9:Q7st/sPass9@127.0.0.1:{port}"
"""An STS endpoint whose password holds an unencoded `/`, as a base64 secret can."""

_RELATIVE_STS_URL: Final = f"//{_STS_LOGIN}@127.0.0.1:{{port}}"
"""A scheme-relative STS endpoint, which botocore writes back whole when it cannot connect to it."""

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
    # The port is out of range, so no socket opens.
    return _profile(folder, {"HTTPS_PROXY": url})


def _profile(folder: Path, variables: dict[str, str]) -> tuple[dict[str, str], str, str]:
    """Sign in through an AWS profile that assumes a role, with each of `variables` set too.

    From 2.0.0 on, Polars assumes the role through botocore, which reads the
    proxy and the STS endpoint.
    """
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
            **variables,
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


def _relative_proxy(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Sign in as `_proxy` does, through a scheme-relative proxy with a login."""
    return _proxy(folder, port, _RELATIVE_PROXY_URL)


def _cut_sts(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Sign in through an AWS profile, at an STS endpoint whose login an unencoded `/` cuts."""
    return _profile(folder, {"AWS_ENDPOINT_URL_STS": _CUT_STS_URL.format(port=port)})


def _relative_sts(folder: Path, port: int) -> tuple[dict[str, str], str, str]:
    """Sign in through an AWS profile, at a scheme-relative STS endpoint with a login.

    Nothing listens on the endpoint's port, so botocore cannot connect to it.
    """
    del port
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed = probe.getsockname()[1]
    return _profile(folder, {"AWS_ENDPOINT_URL_STS": _RELATIVE_STS_URL.format(port=closed)})


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
    "relative-proxy-login-polars": (_relative_proxy, False),
    "cut-sts-login-polars": (_cut_sts, False),
    "relative-sts-login-polars": (_relative_sts, False),
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


_SIGN_IN_CASES: Final = [
    pytest.param("proxy-login-polars", _PROXY_LOGIN, _PROXY_PASSWORD, id="proxy"),
    pytest.param(
        "relative-proxy-login-polars", _PROXY_LOGIN, _PROXY_PASSWORD, id="scheme-relative-proxy"
    ),
    pytest.param("relative-sts-login-polars", _STS_LOGIN, _STS_PASSWORD, id="scheme-relative-sts"),
]
"""Each case of a login botocore reads as Polars signs in, with the login and its password."""


@pytest.mark.parametrize(("case", "login", "password"), _SIGN_IN_CASES)
def test_no_sign_in_login_comes_back(
    tmp_path: Path, refusing_store: int, case: str, login: str, password: str
) -> None:
    """Ensure each answer holds no form of the proxy's or the STS endpoint's login."""
    for shown in _in_a_child(case, tmp_path, refusing_store):
        assert not shown.startswith("refused: "), shown
        assert not _holds(shown, login), shown
        assert not _holds(shown, password), shown


@pytest.mark.parametrize(
    ("case", "login", "password", "echoes"),
    [
        pytest.param(*case.values, _POLARS_ECHOES_PROXY if case.id == "proxy" else True, id=case.id)
        for case in _SIGN_IN_CASES
    ],
)
def test_polars_echoes_the_sign_in_login_without_the_mask(
    tmp_path: Path, refusing_store: int, case: str, login: str, password: str, echoes: bool
) -> None:
    """Ensure Polars writes the login back exactly where the probe found it does.

    botocore writes a proxy it cannot parse back as `http:` and the value, and
    an STS endpoint it cannot connect to back as written. Polars 1.39.3, the
    floor, does so for a scheme-relative proxy and STS endpoint too. For a
    proxy with a scheme, its error names only the store, so the control
    holds no login there before 2.0.0.
    """
    answers = _in_a_child(case, tmp_path, refusing_store, control=True)

    for shown in answers:
        holds = login in shown and password in shown
        assert holds == echoes, shown


_CUT_CASES: Final = [
    pytest.param("cut-proxy-login-polars", "HTTPS_PROXY", "proxyuser9", "Pr0x", id="hash"),
    pytest.param(
        "scheme-cut-proxy-login-polars", "HTTPS_PROXY", "proxyuser9", "Pr0x", id="no-scheme"
    ),
    pytest.param("cut-sts-login-polars", "AWS_ENDPOINT_URL_STS", "stsuser9", "Q7st", id="sts"),
]
"""Each case of a login that a client cuts, with its variable, its user, and the head of its password."""


@pytest.mark.parametrize(("case", "variable", "user", "head"), _CUT_CASES)
def test_it_refuses_a_url_login_a_client_cuts(
    tmp_path: Path, refusing_store: int, case: str, variable: str, user: str, head: str
) -> None:
    """Ensure a proxy or an STS login that a client cuts stops the server, naming only the variable."""
    (shown,) = _in_a_child(case, tmp_path, refusing_store)

    assert shown.startswith("refused: "), shown
    assert f"environment variable {variable} must hold no '@'" in shown
    assert not _holds(shown, user), shown
    assert not _holds(shown, head), shown


@pytest.mark.parametrize(
    ("case", "head", "echoes"),
    [
        pytest.param(
            "cut-proxy-login-polars", f"'{_PROXY_LOGIN}'", _POLARS_ECHOES_PROXY, id="hash"
        ),
        pytest.param("scheme-cut-proxy-login-polars", "'proxyuser9:Pr0x:'", True, id="no-scheme"),
        pytest.param("cut-sts-login-polars", "'Q7st'", True, id="sts"),
    ],
)
def test_polars_echoes_a_cut_login_without_the_check(
    tmp_path: Path, refusing_store: int, case: str, head: str, echoes: bool
) -> None:
    """Ensure Polars writes the head of a cut login back, which no mask matches.

    urllib3 ends the authority at the `#`, or at the `/` of a `://` that
    follows no scheme, and botocore passes its parse error on, with the user
    and the password up to that character. For the STS endpoint, botocore
    writes the head of the password as a port it could not read. Before
    2.0.0, Polars writes back the head cut at a `://` or in an STS endpoint,
    and for a `#` its error names only the store.
    """
    answers = _in_a_child(case, tmp_path, refusing_store, control=True)

    for shown in answers:
        assert not shown.startswith("refused: "), shown
        assert (head in shown) == echoes, shown
