# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that no real database repeats a wrong password `veridelta mcp` sends it.

`test_mcp_password_echo.py` drives each reader against a fake. Here each
reader signs in to a real server with each wrong password of
`password_corpus.CORPUS`, through the MCP client, so what the server itself
writes back is held to the same check. A server is used only when its URI is
set, in `VERIDELTA_POSTGRES_URI`, `VERIDELTA_MYSQL_URI`,
`VERIDELTA_MARIADB_URI`, `VERIDELTA_MSSQL_URI`, or
`VERIDELTA_CLICKHOUSE_URI`, as `make postgres`, `make databases`, and their CI
jobs set them. Every other run skips these tests. The URI keeps its user, its
host, and its query, and the wrong password takes the place of its own.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit, urlunsplit

import anyio
import polars as pl
import pytest
from mcp import Client

from tests.integration.password_corpus import CORPUS, holds_piece
from veridelta import mcp_server
from veridelta.mcp_server import Settings, build_server

pytestmark = [pytest.mark.integration]

_SERVERS: Final = {
    "postgres": "VERIDELTA_POSTGRES_URI",
    "mysql": "VERIDELTA_MYSQL_URI",
    "mariadb": "VERIDELTA_MARIADB_URI",
    "mssql": "VERIDELTA_MSSQL_URI",
    "clickhouse": "VERIDELTA_CLICKHOUSE_URI",
}
"""Each server under test, by the variable that holds its URI."""


def _without_password(uri: str) -> str:
    """Return the URI with its user, host, and query as written, and no password."""
    parts = urlsplit(uri)
    login, _, host = parts.netloc.rpartition("@")
    user = login.partition(":")[0]
    return urlunsplit(parts._replace(netloc=f"{user}@{host}"))


def _answer(settings: Settings, path: str) -> str:
    """Check a configuration's schemas through the client, and join all it returned."""

    async def session() -> str:
        async with Client(build_server(settings)) as client:
            result = await client.call_tool("validate_config", {"path": path, "schemas": True})
        text = "".join(getattr(block, "text", "") for block in result.content)
        return json.dumps(result.structured_content, ensure_ascii=False) + text

    return anyio.run(session)


@pytest.mark.parametrize("server", list(_SERVERS))
def test_no_server_repeats_any_piece_of_a_wrong_password(
    server: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensure a real server's refusal of each wrong password holds no piece of it.

    The right URI must still read afterwards, so a server that stopped
    answering this host after the failed sign-ins fails the test instead of
    hiding the echo.
    """
    uri = os.environ.get(_SERVERS[server])
    if not uri:
        pytest.skip(f"Set {_SERVERS[server]} to run against a live server.")
    for name in list(os.environ):
        if mcp_server._name_as_read(name) in mcp_server._ENVIRONMENT_SLOTS:  # pyright: ignore[reportPrivateUsage]  # the list is private to the server
            monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "b.csv").write_text("id\n1\n", encoding="utf-8")
    paths = []
    for number, (_, password) in enumerate(CORPUS, start=1):
        variable = f"V_PW_{number}"
        monkeypatch.setenv(variable, password)
        path = f"{variable}.yaml"
        (tmp_path / path).write_text(
            f"source:\n  type: database\n  uri: {json.dumps(_without_password(uri))}\n"
            f"  table: t\n  password: ${{{variable}}}\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
            encoding="utf-8",
        )
        paths.append(path)
    settings = Settings(
        (tmp_path,), allow_row_values=True, allowed_hosts=("127.0.0.1", "localhost")
    )

    with ThreadPoolExecutor(max_workers=len(paths)) as pool:
        answers = list(pool.map(lambda path: _answer(settings, path), paths))

    for (password_id, password), answer in zip(CORPUS, answers, strict=True):
        found = holds_piece(answer, password)
        assert found is None, f"{server}, {password_id}: the answer holds {found}: {answer}"
        assert '"valid": false' in answer, f"{server}, {password_id}: {answer}"
    assert pl.read_database_uri("SELECT 1 AS one", uri).item() == 1
