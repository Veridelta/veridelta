# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests of the MCP server's rule on a Delta Lake table in WebHDFS.

Real deltalake reads a `webhdfs://` or `opendal+webhdfs://` table whose host
the server allows. Its WebHDFS reader does not connect to that host, but to
`127.0.0.1:9870`, or to the namenode its endpoint option names. Every
proxy variable names a proxy on this machine, which counts each connection
and keeps each request line, and nothing bypasses it. So a request the
reader sends to any host reaches the proxy. The rule must refuse the side
before any does. A control turns the rule off and shows the reader sends its
requests through the proxy to `127.0.0.1:9870`, a host no configuration named.
"""

import multiprocessing
import os
import socket
import threading
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Final, cast

import anyio
import pytest
from mcp import Client

from veridelta import mcp_server
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.integration]

_ALLOWED: Final = "lake.example"
"""The only host the server allows, which each table URI names."""

_PROXY_VARIABLES: Final = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
"""The variables reqwest, under deltalake's OpenDAL stores, reads a proxy from, in either case."""

_BYPASS_VARIABLES: Final = ("NO_PROXY", "no_proxy")
"""The variables that name the hosts a request reaches without the proxy."""


class _Proxy(ThreadingHTTPServer):
    """A proxy on this machine that counts each connection and keeps each request line."""

    def __init__(self) -> None:
        """Listen on a port the system chooses."""
        super().__init__(("127.0.0.1", 0), _Refuser)
        self.connections = 0
        self.lines: list[str] = []

    def verify_request(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: object
    ) -> bool:
        """Count every connection before its request is read."""
        self.connections += 1
        return True


class _Refuser(BaseHTTPRequestHandler):
    """Keep each request line, and answer 403 with no body."""

    def _refuse(self) -> None:
        """Record the request line, then refuse it."""
        cast("_Proxy", self.server).lines.append(self.requestline)
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    def do_GET(self) -> None:
        """Refuse a status, a listing, or a read."""
        self._refuse()

    def do_PUT(self) -> None:
        """Refuse a write, such as the `MKDIRS` the WebHDFS reader sends as it opens a table."""
        self._refuse()

    def do_POST(self) -> None:
        """Refuse a sign-in or an append."""
        self._refuse()

    def do_DELETE(self) -> None:
        """Refuse a deletion."""
        self._refuse()

    def do_CONNECT(self) -> None:
        """Refuse a tunnel, which a reader opens for a host it reaches over HTTPS."""
        self._refuse()

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


@pytest.fixture
def proxy() -> Iterator[_Proxy]:
    """Start the proxy, and stop it once the test ends."""
    with _Proxy() as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        yield server
        server.shutdown()
        worker.join()


def _answer(table_uri: str, folder: str, address: str, control: bool) -> tuple[bool, str]:
    """Describe a Delta Lake source at `table_uri` through the MCP client, and return whether it failed, and its text.

    It runs in a child, in the folder, the server's root, as a server
    started there does, since the server checks a URI as a path here too.
    Every proxy variable names the proxy at `address`, and none names a host
    that bypasses it. With `control`, the rule is off.
    """
    root = Path(folder)
    os.chdir(root)
    for name in _BYPASS_VARIABLES:
        os.environ.pop(name, None)
    for name in _PROXY_VARIABLES:
        # Windows reads a variable's name in any case, so the second sets the first again.
        os.environ[name] = address
        os.environ[name.lower()] = address
    if control:
        setattr(mcp_server, "_refuse_webhdfs", lambda *_: None)  # noqa: B010 - the child patches as monkeypatch would
    (root / "b.csv").write_text("id\n1\n", encoding="utf-8")
    (root / "veridelta.yaml").write_text(
        f"source:\n  type: delta\n  table_uri: {table_uri}\n"
        "target:\n  path: b.csv\nprimary_keys: [id]\n",
        encoding="utf-8",
    )
    settings = Settings((root,), allowed_hosts=(_ALLOWED,))

    async def session() -> tuple[bool, str]:
        async with Client(build_server(settings)) as client:
            result = await client.call_tool(
                "describe_schema", {"path": "veridelta.yaml", "side": "source"}
            )
            return result.is_error, "".join(getattr(block, "text", "") for block in result.content)

    return anyio.run(session)


def _in_a_child(
    table_uri: str, folder: Path, proxy: _Proxy, *, control: bool = False
) -> tuple[bool, str]:
    """Run `_answer` in a new process, and return what it returns.

    OpenDAL builds one HTTP client for the process, which reads the proxy
    variables once, so each case reads in a process of its own.
    """
    address = f"http://127.0.0.1:{proxy.server_port}"
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as pool:
        return pool.submit(_answer, table_uri, str(folder), address, control).result()


_TABLES = pytest.mark.parametrize(
    "table_uri", [f"webhdfs://{_ALLOWED}/t", f"opendal+webhdfs://{_ALLOWED}/t"]
)
"""The two schemes deltalake reads through its WebHDFS store, each at the allowed host."""


@_TABLES
def test_no_request_leaves_for_a_table_in_webhdfs(
    tmp_path: Path, proxy: _Proxy, table_uri: str
) -> None:
    """Ensure the tool refuses the side, naming the setting, before the reader sends any request."""
    failed, text = _in_a_child(table_uri, tmp_path, proxy)

    assert failed, text
    assert "The source table_uri names a Delta Lake table in WebHDFS." in text
    assert proxy.connections == 0
    assert proxy.lines == []


@_TABLES
def test_the_reader_reaches_another_host_without_the_rule(
    tmp_path: Path, proxy: _Proxy, table_uri: str
) -> None:
    """Ensure deltalake's WebHDFS reader, once the rule is off, sends its requests to `127.0.0.1:9870`.

    The table's URI names only the allowed host, which the server's host
    check passes, so that check alone lets the reader reach a host no
    configuration named, over plain HTTP.
    """
    failed, text = _in_a_child(table_uri, tmp_path, proxy, control=True)

    assert failed, text
    assert proxy.connections
    assert any("http://127.0.0.1:9870/webhdfs/v1/" in line for line in proxy.lines), proxy.lines
    assert not any(_ALLOWED in line for line in proxy.lines), proxy.lines
