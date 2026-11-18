# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the MCP server: its tools, its folder guard, and its SDK binding.

The server is driven through the SDK's own client, connected in memory, so each
test sees what an agent's host sees: the tool list, the structured result, and
whether a call failed.
"""

import base64
import functools
import getpass
import glob
import html
import importlib.util
import json
import logging
import ntpath
import os
import re
import shutil
import sqlite3
import struct
import sys
import tempfile
import threading
import uuid
import zlib
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace
from typing import Any, Literal, TypeVar, cast, get_args
from unittest.mock import MagicMock, patch
from urllib.error import URLError
from urllib.parse import parse_qsl, quote, unquote, urlencode

import anyio
import duckdb
import polars as pl
import pytest
from deltalake import DeltaTable
from mcp import Client
from mcp.types import CallToolResult, Tool
from packaging.version import Version
from polars.exceptions import PanicException
from pyarrow import fs as arrow_fs
from pyarrow.fs import LocalFileSystem, SubTreeFileSystem
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestFile, write_manifest_list
from pytest_mock import MockerFixture

from veridelta import __version__, mcp_server
from veridelta.config import load_config, load_config_with_hook
from veridelta.connectors import base, database
from veridelta.connectors import duckdb as duckdb_connector
from veridelta.connectors.database import sqlite_path
from veridelta.connectors.duckdb import (
    MOTHERDUCK_TOKEN_VARIABLES,
    DuckDBPushdownSession,
    beside,
    sandboxed,
)
from veridelta.connectors.lakehouse import (
    CheckedDeltaTable,
    DeltaLakeConnector,
    IcebergConnector,
    deletion_vector_files,
    delta_log_vectors,
)
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, ConnectorError, VerideltaError
from veridelta.mcp_server import (
    DEFAULT_ROW_CAP,
    INSTRUCTIONS,
    RunReport,
    Settings,
    build_server,
    check_configuration,
    check_data_paths,
    describe_side,
    propose_maps,
    read_rows,
    resolve_path,
    run_configuration,
    serve,
    suggest,
)
from veridelta.models import (
    DatabaseConfig,
    DeltaLakeConfig,
    DiffResult,
    DiffSummary,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)
from veridelta.outputs import FindingDetail

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_DESCRIPTION_BUDGET = 240
"""The most characters a tool's description may take. A host sends every
description to the model on every turn, so each one costs every turn."""

_INSTRUCTIONS_BUDGET = 500
"""The most characters the server's instructions may take, for the same reason."""

_VALID = "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
"""A configuration that loads and passes every offline check."""

_QUERY = (
    "source:\n  type: database\n  uri: postgresql://db.example.com/app\n"
    "  query: SELECT * FROM t\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
)
"""A configuration whose source is a query, which no probe can describe without running it.

The database is on another machine, so no file of it has to lie under a root,
and a server reaches it only with its host allowed."""

_QUERY_HOST = "db.example.com"
"""The host of `_QUERY`'s database."""

_SECRET = "hunter2-do-not-print"

_PASSWORD_FROM_ENV = (
    "source:\n  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
    "  table: orders\n  password: ${VD_MCP_PASSWORD}\n"
    "target:\n  path: b.csv\nprimary_keys: [id]\n"
)
"""A configuration that takes a database password from `VD_MCP_PASSWORD`, as a server allows."""

_CANARY = "CANARY-123-45-6789"
"""A cell value that a server started without `--allow-row-values` never returns."""

_DUCKDB_LOGS = [
    (".wal", "WAL"),
    (".wal.checkpoint", "checkpoint WAL"),
    (".wal.recovery", "recovery WAL"),
]
"""Each log DuckDB names beside a database file, with the name a refusal gives it."""

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ci"
"""The CSV files the CI jobs and the recording compare."""

_T = TypeVar("_T")

_ANOTHER_MACHINE = [
    pytest.param("\\\\host.example\\share\\a.csv", id="unc"),
    pytest.param("//host.example/share/a.csv", id="unc-with-slashes"),
    pytest.param("\\/host.example/share/a.csv", id="unc-with-mixed-separators"),
    pytest.param("\\\\?\\UNC\\host.example\\share\\a.csv", id="nt-unc"),
    pytest.param("\\\\.\\UNC\\host.example\\share\\a.csv", id="device-unc"),
    pytest.param("//?/UNC/host.example/share/a.csv", id="nt-unc-with-slashes"),
    pytest.param("\\??\\UNC\\host.example\\share\\a.csv", id="kernel-unc"),
    pytest.param("\\\\host.example\\share\\*.csv", id="pattern"),
]
"""Paths Windows reads as on another machine, where resolving one reaches the host."""


def _as_windows(monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture) -> Callable[[], list[str]]:
    """Read paths by Windows' rules, on any system, and spy on what resolves, matches, or opens one.

    Returns:
        Callable[[], list[str]]: Lists each path resolved, matched, or opened so far.
    """
    windows = SimpleNamespace(path=ntpath, sep="\\", altsep="/")
    for module in (base, database, mcp_server):
        monkeypatch.setattr(module, "os", windows)
    spies = [
        mocker.spy(ntpath, "realpath"),
        mocker.spy(glob, "glob"),
        mocker.spy(os, "stat"),
        mocker.spy(Path, "resolve"),
    ]
    return lambda: [str(call.args[0]) for spy in spies for call in spy.call_args_list]


def _on_another_machine(paths: list[str]) -> list[str]:
    """Pick out each path Windows reads as on another machine or a device."""
    return [each for each in paths if each.replace("/", "\\").startswith(("\\\\", "\\??\\"))]


def _on_a_windows_disk(monkeypatch: pytest.MonkeyPatch, *roots: str) -> Settings:
    r"""Read paths as Windows does, with its own `realpath`, in `C:\root`, the working folder.

    Python defines Windows' `realpath` only where the `nt` module exists, so
    this loads `ntpath` again with a stand-in for `nt`. Only `C:\` and
    `C:\root` exist on its disk, and any other path fails as a missing file
    does. A `file:` URI's path converts as Windows converts one that starts
    with a drive.

    Each root is a Windows path from the start. Python 3.11 makes a Windows
    path from a POSIX one by its names, so `C:/root` would read as `C:` and
    `root`, which is `C:root`, a path relative to the drive's own folder.

    Args:
        monkeypatch (pytest.MonkeyPatch): Undoes the stand-ins after the test.
        *roots (str): The server's roots. Defaults to `C:\root` alone.

    Returns:
        Settings: A server with those roots.
    """

    def missing() -> OSError:
        error = OSError("The system cannot find the file specified")
        error.winerror = 2  # type: ignore[attr-defined]  # Only Windows' OSError declares it.
        return error

    def final(path: str) -> str:
        if ntpath.normcase(path) not in ("c:\\", "c:\\root"):
            raise missing()
        return "\\\\?\\" + path

    def fail(path: str) -> str:
        raise missing()

    nt = SimpleNamespace(_getfinalpathname=final, _findfirstfile=fail, readlink=fail)
    spec = importlib.util.spec_from_file_location("windows_ntpath", ntpath.__file__)
    assert spec is not None
    assert spec.loader is not None
    windows = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"nt": nt}):
        spec.loader.exec_module(windows)
    monkeypatch.setattr(
        windows, "os", SimpleNamespace(**{**vars(os), "getcwd": lambda: "C:\\root"})
    )
    for module in (base, mcp_server):
        monkeypatch.setattr(module, "os", SimpleNamespace(path=windows, sep="\\", altsep="/"))

    class WindowsPath(PureWindowsPath):
        """A Windows path, with no home or link to read on this disk."""

        def expanduser(self) -> "WindowsPath":
            return self

        def resolve(self) -> "WindowsPath":
            return self

    monkeypatch.setattr(mcp_server, "Path", WindowsPath)
    monkeypatch.setattr(
        mcp_server, "url2pathname", lambda path: unquote(path).removeprefix("/").replace("/", "\\")
    )
    disk = tuple(WindowsPath(root) for root in roots or ("C:\\root",))
    return Settings(cast("tuple[Path, ...]", disk))


@pytest.fixture(autouse=True)
def _in_the_first_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run each test in `tmp_path`, the first root of its server, as `veridelta mcp` runs.

    A configuration's relative data path resolves against the working
    directory, as on the command line, so a test that runs elsewhere says so.
    """
    monkeypatch.chdir(tmp_path)


def _write(folder: Path, text: str, name: str = "veridelta.yaml") -> Path:
    """Write a configuration file."""
    path = folder / name
    path.write_text(text, encoding="utf-8")
    return path


def _project(root: Path, target: str, extra: str = "") -> Path:
    """Copy the CI fixtures into a root, and write a configuration that compares them."""
    shutil.copy(_FIXTURES / "legacy.csv", root / "legacy.csv")
    shutil.copy(_FIXTURES / target, root / target)
    return _write(
        root,
        f"source:\n  path: legacy.csv\ntarget:\n  path: {target}\nprimary_keys: [id]\n{extra}",
    )


def _coded(root: Path) -> Path:
    """Write two files whose `gender` column holds the same values in two encodings.

    Each value holds five rows, the default support a proposal needs.
    """
    (root / "a.csv").write_text(
        "id,gender\n" + "".join(f"{i},{'M' if i % 2 == 0 else 'F'}\n" for i in range(10))
    )
    (root / "b.csv").write_text(
        "id,gender\n" + "".join(f"{i},{'Male' if i % 2 == 0 else 'Female'}\n" for i in range(10))
    )
    return _write(root, _VALID)


def _near(root: Path) -> Path:
    """Write two files whose `amount` differs by a little on two of three rows."""
    (root / "a.csv").write_text("id,amount\n1,10.0\n2,20.0\n3,30.0\n")
    (root / "b.csv").write_text("id,amount\n1,10.001\n2,20.0\n3,30.002\n")
    return _write(root, _VALID)


def _header_from(outside: Path) -> str:
    """Write a configuration whose source header is the second line of a file outside the roots.

    Reader options choose the header row, so a column name can carry any line
    of any file a tool opens.
    """
    (outside / "notes.txt").write_text(f"public\n{_SECRET}\n")
    return (
        f"source:\n  path: '{outside / 'notes.txt'}'\n  format: csv\n"
        "  options:\n    skip_rows: 1\n    separator: '|'\n"
        "target:\n  path: b.csv\nprimary_keys: [id]\n"
    )


def _queried(root: Path, query: str) -> Path:
    """Write a configuration whose source runs `query` in a DuckDB file under the root.

    The query is written as a JSON string, which YAML reads, so a Windows path's
    backslashes stay as they are.
    """
    with duckdb.connect(str(root / "warehouse.duckdb")) as connection:
        connection.execute("CREATE TABLE t AS SELECT 1 AS id")
    (root / "b.csv").write_text("id\n1\n")
    return _write(
        root,
        "source:\n  type: duckdb\n  database: warehouse.duckdb\n"
        f"  query: {json.dumps(query)}\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
    )


def _with_client(settings: Settings, work: Callable[[Client], Awaitable[_T]]) -> _T:
    """Connect the SDK's client to a server built from `settings`, in memory, and run `work`.

    The client's block ends before the event loop does, so no transport is left
    open for a `ResourceWarning`, which this suite turns into a failure.
    """

    async def session() -> _T:
        async with Client(build_server(settings)) as client:
            return await work(client)

    return anyio.run(session)


def _call(
    settings: Settings, arguments: dict[str, Any], tool: str = "validate_config"
) -> CallToolResult:
    """Call a tool, `validate_config` unless another is named, through the client."""

    async def work(client: Client) -> CallToolResult:
        return await client.call_tool(tool, arguments)

    return _with_client(settings, work)


def _tools(settings: Settings) -> list[Tool]:
    """List the server's tools, as a host does when it connects."""

    async def work(client: Client) -> list[Tool]:
        return (await client.list_tools()).tools

    return _with_client(settings, work)


def _text(result: CallToolResult) -> str:
    """Join the text a tool call returned for the model."""
    return "".join(getattr(block, "text", "") for block in result.content)


class TestSettings:
    """Validate the folders the person who starts the server allows."""

    def test_it_resolves_each_root(self, tmp_path: Path) -> None:
        """Ensure a root compares with a resolved path, whatever form it was given in."""
        (tmp_path / "data").mkdir()

        settings = Settings((tmp_path / "data" / "..", tmp_path / "data"))

        assert settings.roots == (tmp_path.resolve(), (tmp_path / "data").resolve())

    def test_it_needs_a_root(self) -> None:
        """Ensure a server with no folder fails when built, not on its first call."""
        with pytest.raises(ConfigError, match="at least one folder"):
            Settings(())

    def test_it_keeps_row_values_off_by_default(self, tmp_path: Path) -> None:
        """Ensure a server returns no row values unless the person who starts it allows them."""
        settings = Settings((tmp_path,))

        assert settings.allow_row_values is False
        assert settings.max_rows == DEFAULT_ROW_CAP == 50

    def test_it_needs_a_row_cap_of_at_least_one(self, tmp_path: Path) -> None:
        """Ensure a cap that would let no row through is refused when the server is built."""
        with pytest.raises(ConfigError, match="at least 1, got 0"):
            Settings((tmp_path,), max_rows=0)

    def test_it_allows_no_host_by_default_and_each_host_it_names(self, tmp_path: Path) -> None:
        """Ensure a server connects to no host unless named, and keeps each host as given."""
        settings = Settings((tmp_path,), allowed_hosts=("db.example.com", "10.0.0.5", "[::1]"))

        assert Settings((tmp_path,)).allowed_hosts == ()
        assert settings.allowed_hosts == ("db.example.com", "10.0.0.5", "[::1]")

    @pytest.mark.parametrize("host", ["LAKE", "DB.Example.com", "[::FFFF:1]"])
    def test_it_refuses_an_allowed_host_with_a_capital_letter(
        self, tmp_path: Path, host: str
    ) -> None:
        """Ensure an allowed host with a capital letter is refused, and not lowercased.

        A reader keeps a bucket's case, so lowercasing `LAKE` would allow
        bucket `lake`, which the operator never named.
        """
        with pytest.raises(ConfigError, match=r"^--allow-host takes a host in lowercase"):
            Settings((tmp_path,), allowed_hosts=(host,))

    @pytest.mark.parametrize(
        "host",
        [
            "https://db.example.com",
            "db.example.com:5432",
            "user@db",
            "",
            "::1",
            "a/b",
            "\u212aafka.internal",
        ],
    )
    def test_it_refuses_an_allowed_host_that_is_not_a_host(self, tmp_path: Path, host: str) -> None:
        """Ensure an entry with a scheme, a port, a login, or a letter outside ASCII is refused."""
        with pytest.raises(ConfigError, match="--allow-host takes a host name"):
            Settings((tmp_path,), allowed_hosts=(host,))


class TestResolvePath:
    """Validate the guard that keeps every tool inside the roots."""

    def test_it_keeps_an_absolute_path_under_a_root(self, tmp_path: Path) -> None:
        """Ensure a file under the root resolves to itself."""
        path = _write(tmp_path, _VALID)

        assert resolve_path(Settings((tmp_path,)), str(path)) == path.resolve()

    def test_it_reads_a_relative_path_against_the_first_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a relative path names a file in the first root, wherever the process runs."""
        first, second = tmp_path / "first", tmp_path / "second"
        first.mkdir()
        second.mkdir()
        monkeypatch.chdir(second)

        resolved = resolve_path(Settings((first, second)), "veridelta.yaml")

        assert resolved == (first / "veridelta.yaml").resolve()

    def test_it_accepts_a_file_under_a_later_root(self, tmp_path: Path) -> None:
        """Ensure every root counts, not only the first."""
        first, second = tmp_path / "first", tmp_path / "second"
        first.mkdir()
        second.mkdir()
        path = _write(second, _VALID)

        assert resolve_path(Settings((first, second)), str(path)) == path.resolve()

    @pytest.mark.parametrize("escape", ["../veridelta.yaml", "nested/../../veridelta.yaml"])
    def test_it_refuses_a_path_that_climbs_out(self, tmp_path: Path, escape: str) -> None:
        """Ensure `..` is folded before the check, so it cannot lead out of a root."""
        root = tmp_path / "root"
        root.mkdir()

        with pytest.raises(ConfigError, match="outside the folders") as refused:
            resolve_path(Settings((root,)), escape)

        assert str(root.resolve()) in str(refused.value)
        assert "--root" in str(refused.value)

    def test_it_refuses_a_file_elsewhere(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure an absolute path outside every root is refused."""
        outside = _write(tmp_path_factory.mktemp("outside"), _VALID)

        with pytest.raises(ConfigError, match="outside the folders"):
            resolve_path(Settings((tmp_path,)), str(outside))

    @pytest.mark.parametrize("path", _ANOTHER_MACHINE)
    def test_it_resolves_no_path_on_another_machine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a path Windows reads as another machine's is refused before it is resolved.

        Resolving it would reach the host it names, and can send the user's
        credentials there.
        """
        settings = Settings((tmp_path,))
        reached = _as_windows(monkeypatch, mocker)

        with pytest.raises(ConfigError, match="outside the folders"):
            resolve_path(settings, path)

        assert not _on_another_machine(reached())

    def test_it_resolves_a_relative_path_by_windows_rules(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a relative path with a backslash still resolves under the first root."""
        settings = Settings((tmp_path,))
        _as_windows(monkeypatch, mocker)

        resolved = resolve_path(settings, "nested\\veridelta.yaml")

        assert resolved == (tmp_path / "nested\\veridelta.yaml").resolve()


class TestCheckConfiguration:
    """Validate the check `validate_config` runs, without the SDK in the way."""

    def test_it_passes_a_configuration_that_will_run(self, tmp_path: Path) -> None:
        """Ensure a clean file is valid, with the resolved path and no findings."""
        path = _write(tmp_path, _VALID)

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report == {
            "config": str(path.resolve()),
            "valid": True,
            "errors": [],
            "warnings": [],
            "findings": [],
        }

    def test_it_reports_a_file_that_does_not_load_as_an_error(self, tmp_path: Path) -> None:
        """Ensure a load failure is a finding, as `validate` reports it."""
        _write(tmp_path, "source: {}\n")

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["valid"] is False
        assert len(report["errors"]) == 1
        assert report["errors"][0].startswith("Configuration must contain both")
        assert report["findings"] == [
            FindingDetail(severity="error", message=report["errors"][0], locations=[])
        ]

    def test_it_reports_a_missing_file_as_an_error(self, tmp_path: Path) -> None:
        """Ensure a path under a root that holds no file is a finding, not a crash."""
        report = check_configuration(Settings((tmp_path,)), "missing.yaml")

        assert report["valid"] is False
        assert report["errors"] == [
            f"Configuration file not found or is not a file: {(tmp_path / 'missing.yaml').resolve()}"
        ]

    def test_it_needs_every_variable_unless_told_otherwise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an unset variable fails by default, and is a warning when allowed."""
        monkeypatch.delenv("VD_MCP_PASSWORD", raising=False)
        _write(tmp_path, _PASSWORD_FROM_ENV)
        settings = Settings((tmp_path,))

        strict = check_configuration(settings, "veridelta.yaml")
        lenient = check_configuration(settings, "veridelta.yaml", allow_missing_env=True)

        assert strict["valid"] is False
        assert "Environment variable 'VD_MCP_PASSWORD' is not set" in strict["errors"][0]
        assert lenient["valid"] is True
        assert lenient["warnings"] == [
            "Environment variable 'VD_MCP_PASSWORD' is not set, so its references were "
            "checked as the text 'VD_MCP_PASSWORD'."
        ]

    def test_it_checks_rules_against_stored_columns_with_schemas(self, tmp_path: Path) -> None:
        """Ensure `schemas` reads the files' columns, so a rule that cannot fit fails.

        A relative data path resolves where the process runs, as on the command
        line, and only the server runs in its first root, so this test names
        each file in full.
        """
        (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
        (tmp_path / "b.csv").write_text("id,amount\n1,10\n")
        _write(
            tmp_path,
            f"source:\n  path: {tmp_path / 'a.csv'}\ntarget:\n  path: {tmp_path / 'b.csv'}\n"
            "primary_keys: [id]\nrules:\n  - column_names: [amount]\n    null_values: ['N/A']\n",
        )
        settings = Settings((tmp_path,))

        offline = check_configuration(settings, "veridelta.yaml")
        live = check_configuration(settings, "veridelta.yaml", schemas=True)

        assert offline["valid"] is True
        assert live["valid"] is False
        assert "Column 'amount' has type Int64, which cannot hold" in live["errors"][0]

    def test_it_opens_no_file_outside_the_roots_to_check_columns(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure `schemas` refuses data outside the roots, and a check without it opens nothing."""
        _write(tmp_path, _header_from(tmp_path_factory.mktemp("outside")))
        settings = Settings((tmp_path,))

        offline = check_configuration(settings, "veridelta.yaml")
        with pytest.raises(ConfigError, match="The source path") as caught:
            check_configuration(settings, "veridelta.yaml", schemas=True)

        assert offline["valid"] is True
        assert _SECRET not in str(caught.value)

    def test_it_reports_why_a_file_does_not_load_before_it_checks_data(
        self, tmp_path: Path
    ) -> None:
        """Ensure `schemas` on a file that does not load is a finding, as without it."""
        _write(tmp_path, "source: {}\n")

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml", schemas=True)

        assert report["valid"] is False
        assert report["errors"][0].startswith("Configuration must contain both")

    def test_it_checks_the_configuration_it_connects_with(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure `schemas` loads the file once, so a file swapped after the host check reaches no host.

        The file is rewritten to name a database the server does not allow right
        after the one load, as an agent racing the check would.
        """
        (tmp_path / "a.csv").write_text("id\n1\n")
        path = _write(
            tmp_path, "source: {path: a.csv}\ntarget: {path: a.csv}\nprimary_keys: [id]\n"
        )
        swapped = (
            "source: {path: a.csv}\n"
            "target: {type: database, uri: 'postgresql://u@127.0.0.1:9/db', table: t}\n"
            "primary_keys: [id]\n"
        )

        def load_then_swap(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
            loaded = load_config_with_hook(*args, **kwargs)
            path.write_text(swapped)
            return loaded

        mocker.patch.object(mcp_server, "load_config_with_hook", side_effect=load_then_swap)
        checks = mocker.spy(DiffEngine, "check_configs")

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml", schemas=True)

        assert report["valid"] is True
        checks.assert_called_once()
        assert checks.call_args.args[2] == SourceConfig(path="a.csv")


class TestRunConfiguration:
    """Validate the run `run_comparison` makes, without the SDK in the way.

    Each test runs in its root, as the server does, so the relative paths in
    the configuration resolve there.
    """

    def test_its_fields_are_the_summary_and_the_verdict(self) -> None:
        """Ensure the result names every field `run --json` prints, so neither can drift apart."""
        summary = DiffSummary(
            total_rows_source=1,
            total_rows_target=1,
            added_count=0,
            removed_count=0,
            changed_count=0,
            is_match=True,
        )

        assert RunReport.__required_keys__ == {
            "verdict",
            "exit_code",
            "artifacts_written",
            *summary.model_dump(mode="json"),
        }

    def test_it_reports_a_match(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure a match reads as `match`, with the exit code `run` gives it."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_match.csv")

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["verdict"] == "match"
        assert report["exit_code"] == 0
        assert report["total_rows_source"] == 3
        assert report["artifacts_written"] is False

    def test_it_reports_drift_as_run_json_prints_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure drift reads as `drift`, around the very summary `run --json` prints."""
        monkeypatch.chdir(tmp_path)
        path = _project(tmp_path, "modern_drift.csv")

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        printed = DiffEngine.run_from_configs(*load_config(path)).summary.model_dump(mode="json")
        assert report == {
            "verdict": "drift",
            "exit_code": 1,
            **printed,
            "artifacts_written": False,
        }
        assert (report["added_count"], report["removed_count"], report["changed_count"]) == (
            1,
            1,
            1,
        )

    def test_it_writes_the_rows_that_differ_under_a_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an `output_path` under a root is written, and the result says so."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv", "output_path: out\n")

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["artifacts_written"] is True
        assert any((tmp_path / "out").iterdir())

    def test_it_checks_a_relative_output_path_where_it_is_written(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        """Ensure `output_path` counts from the working directory, where the rows are written."""
        _write(
            tmp_path,
            f"source:\n  path: '{tmp_path / 'a.csv'}'\ntarget:\n  path: '{tmp_path / 'b.csv'}'\n"
            "primary_keys: [id]\noutput_path: out\n",
        )
        monkeypatch.chdir(tmp_path_factory.mktemp("elsewhere"))
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="output_path 'out' is outside"):
            run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        compare.assert_not_called()

    def test_it_refuses_an_output_path_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        """Ensure a run that would write rows outside the roots stops before it reads any."""
        monkeypatch.chdir(tmp_path)
        outside = tmp_path_factory.mktemp("outside")
        _project(tmp_path, "modern_drift.csv", f"output_path: '{outside}'\n")
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="is outside the folders this server writes to"):
            run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        compare.assert_not_called()
        assert list(outside.iterdir()) == []

    def test_it_opens_no_file_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
    ) -> None:
        """Ensure a run reads no data outside the roots, since its column names come back."""
        _write(tmp_path, _header_from(tmp_path_factory.mktemp("outside")))
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="The source path"):
            run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        compare.assert_not_called()

    def test_it_runs_no_query_unless_the_server_allows_queries(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a side's query, which runs as written, is refused before anything connects."""
        _queried(tmp_path, "SELECT * FROM t")
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="The source reads through a query"):
            run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        compare.assert_not_called()

    def test_it_runs_a_query_when_the_server_allows_queries(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `--allow-queries` lets a query read its database and files under the roots.

        This holds where DuckDB's check of the roots holds, as on Linux and macOS.
        """
        monkeypatch.setattr(duckdb_connector, "_FOLDERS_HOLD", True)
        (tmp_path / "a.csv").write_text("id\n1\n")
        _queried(tmp_path, f"SELECT t.id FROM t JOIN read_csv('{tmp_path / 'a.csv'}') USING (id)")

        report = run_configuration(Settings((tmp_path,), allow_queries=True), "veridelta.yaml")

        assert report["verdict"] == "match"

    def test_it_holds_an_allowed_query_to_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a query cannot read a file outside the roots, even with `--allow-queries`."""
        secret = tmp_path_factory.mktemp("outside") / "secret.txt"
        secret.write_text(f"{_SECRET}\n")
        _queried(tmp_path, f"SELECT 1 AS id, content FROM read_text('{secret}')")

        with pytest.raises(ConnectorError, match="Permission Error") as refused:
            run_configuration(Settings((tmp_path,), allow_queries=True), "veridelta.yaml")

        assert _SECRET not in str(refused.value)

    def test_it_lets_a_query_open_no_other_file_where_duckdbs_check_of_the_roots_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an allowed query reads only its database where the roots do not hold, as on Windows.

        The server cannot see the paths a query names, so DuckDB opens none.
        """
        monkeypatch.setattr(duckdb_connector, "_FOLDERS_HOLD", False)
        settings = Settings((tmp_path,), allow_queries=True)
        (tmp_path / "a.csv").write_text("id\n1\n")
        _queried(tmp_path, f"SELECT t.id FROM t JOIN read_csv('{tmp_path / 'a.csv'}') USING (id)")
        _write(
            tmp_path,
            "source:\n  type: duckdb\n  database: warehouse.duckdb\n  query: SELECT id FROM t\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
            "table.yaml",
        )

        report = run_configuration(settings, "table.yaml")

        assert report["verdict"] == "match"
        with pytest.raises(ConnectorError, match="File system LocalFileSystem has been disabled"):
            run_configuration(settings, "veridelta.yaml")

    def test_it_fails_on_a_configuration_that_cannot_run(self, tmp_path: Path) -> None:
        """Ensure a broken file stops the run, as `run` exits 3, instead of becoming a finding."""
        _write(tmp_path, "source: {}\n")

        with pytest.raises(ConfigError, match="Configuration must contain both"):
            run_configuration(Settings((tmp_path,)), "veridelta.yaml")


class TestDescribeSide:
    """Validate the read `describe_schema` makes, without the SDK in the way."""

    def test_it_lists_a_sides_columns_and_their_types(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure each column comes back in its stored order, with the type Polars reads."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv")

        report = describe_side(Settings((tmp_path,)), "veridelta.yaml", "source")

        assert report == {
            "side": "source",
            "columns": {"id": "Int64", "status": "String", "amount": "Float64"},
        }
        assert list(report["columns"]) == ["id", "status", "amount"]

    def test_it_reads_the_side_it_is_asked_for(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `target` describes the target, whose columns differ from the source's here."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "a.csv").write_text("id,amount\n1,10\n")
        (tmp_path / "b.csv").write_text("id,label\n1,x\n")
        _write(tmp_path, _VALID)

        report = describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert report["side"] == "target"
        assert report["columns"] == {"id": "Int64", "label": "String"}

    def test_it_refuses_a_configuration_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure the folder guard holds for this tool too."""
        outside = _write(tmp_path_factory.mktemp("outside"), _VALID)

        with pytest.raises(ConfigError, match="outside the folders"):
            describe_side(Settings((tmp_path,)), str(outside), "source")

    def test_it_opens_no_file_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a line of a file outside the roots cannot come back as a column name."""
        _write(tmp_path, _header_from(tmp_path_factory.mktemp("outside")))

        with pytest.raises(ConfigError, match="The source path") as caught:
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "source")

        assert _SECRET not in str(caught.value)

    def test_it_names_no_file_in_the_home_folder_for_a_folder_named_home(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a missing `./~/x` lists the folder `~` it names, not the home folder outside.

        The check reads `./~/x` under the working directory, so it passes, and
        the error for a missing file lists the data files beside it. `Path`
        drops the `./` and expands `~`, so it would list the home folder.
        """
        home = tmp_path_factory.mktemp("home")
        (home / "payroll.csv").write_text("id\n1\n", encoding="utf-8")
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USERPROFILE", str(home))
        (tmp_path / "a.csv").write_text("id\n1\n", encoding="utf-8")
        _write(tmp_path, "source:\n  path: a.csv\ntarget:\n  path: ./~/x.csv\nprimary_keys: [id]\n")

        with pytest.raises(ConnectorError, match="does not exist") as caught:
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert "payroll" not in str(caught.value)

    def test_it_will_not_run_a_query_to_learn_its_columns(self, tmp_path: Path) -> None:
        """Ensure a query side is refused with the probe's own reason, and nothing connects."""
        _write(tmp_path, _QUERY)
        settings = Settings((tmp_path,), allowed_hosts=(_QUERY_HOST,))

        with pytest.raises(ConfigError, match="A schema probe reads a 'table'"):
            describe_side(settings, "veridelta.yaml", "source")


def _snowflake() -> SnowflakeConfig:
    """Build a warehouse side, which reads no file on this machine."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table="ORDERS",
    )


def _key_pair(path: str) -> SnowflakeConfig:
    """Build a warehouse side that signs in with the key file at `path`, which the driver opens here."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table="ORDERS",
        private_key_path=path,
    )


_CLIMB = "/.." * 64
"""Enough steps up to reach `/` from any folder a test runs in."""


def _sqlite(uri: str, password: str | None = None) -> DatabaseConfig:
    """Build a SQLite side."""
    return DatabaseConfig(uri=uri, password=password, table="t")


_SQLITE_DISGUISES = [
    pytest.param(
        lambda root, file: _sqlite(f"sqlite://{root}/a{_CLIMB.replace('.', '%2E')}{file}"),
        id="encoded-dots",
    ),
    pytest.param(
        lambda root, file: _sqlite(
            f"sqlite://{root}/a{_CLIMB.replace('/', '%2F')}{quote(str(file), safe='')}"
        ),
        id="encoded-slashes",
    ),
    pytest.param(lambda _, file: _sqlite("sqlite://" + quote(str(file), safe="")), id="encoded"),
    pytest.param(lambda root, file: _sqlite(f"sqlite://{root}/a?{_CLIMB}{file}"), id="query"),
    pytest.param(lambda root, file: _sqlite(f"sqlite://{root}/a#{_CLIMB}{file}"), id="fragment"),
    pytest.param(lambda _, file: _sqlite(f"sqlite://s3://a{_CLIMB}{file}"), id="remote-look"),
    pytest.param(lambda _, file: _sqlite(f"sqlite://file:..{_CLIMB}{file}"), id="file-uri"),
    pytest.param(
        lambda _, file: _sqlite(f"sqlite://file:..%252Fz%3Fa={_CLIMB}{file}"),
        id="file-uri-query",
    ),
    pytest.param(lambda _, file: _sqlite(f"sqlite://~{_CLIMB}{file}"), id="home"),
    pytest.param(
        lambda _, file: _sqlite(f"sqlite://u@/../{file.name}", f"{_CLIMB}{file.parent}/d"),
        id="password",
    ),
]
"""SQLite sides whose URI text sits under the root, but whose decoded path is `file`."""


def _duckdb_table(path: Path, column: str) -> None:
    """Create a DuckDB file whose table `t` has one column, and close it."""
    with duckdb.connect(str(path)) as connection:
        connection.execute(f"CREATE TABLE t AS SELECT 1 AS {column}")


def _link_folder(link: Path, folder: Path) -> None:
    """Make a link to a folder, or skip the test on a machine that cannot, as Windows may not."""
    try:
        link.symlink_to(folder, target_is_directory=True)
    except OSError:
        pytest.skip("This machine cannot make a symbolic link.")


class TestCheckDataPaths:
    """Validate the folder rule on the data a tool returns rows from."""

    def test_it_accepts_data_under_a_root(self, tmp_path: Path) -> None:
        """Ensure a relative file, a file in full, and a DuckDB file under a root pass."""
        settings = Settings((tmp_path,))
        (tmp_path / "x.duckdb").write_bytes(b"")

        check_data_paths(
            settings,
            SourceConfig(path="a.csv"),
            DuckDBConfig(database=str(tmp_path / "x.duckdb"), table="t"),
        )

    @pytest.mark.parametrize(
        ("build", "setting"),
        [
            pytest.param(
                lambda folder: SourceConfig(path=str(folder / "a.csv")), "path", id="file"
            ),
            pytest.param(
                lambda folder: SourceConfig(path=(folder / "a.csv").as_uri()),
                "path",
                id="file-uri",
            ),
            pytest.param(
                lambda folder: DeltaLakeConfig(table_uri=str(folder / "events")),
                "table_uri",
                id="delta",
            ),
            pytest.param(
                lambda folder: IcebergConfig(table_uri=str(folder / "events")),
                "table_uri",
                id="iceberg",
            ),
            pytest.param(
                lambda folder: DuckDBConfig(database=str(folder / "x.duckdb"), table="t"),
                "database",
                id="duckdb",
            ),
            pytest.param(
                lambda folder: DatabaseConfig(uri=f"sqlite://{folder / 'x.db'}", table="t"),
                "SQLite file",
                id="sqlite",
            ),
        ],
    )
    def test_it_refuses_local_data_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        build: Callable[[Path], SourceRef],
        setting: str,
    ) -> None:
        """Ensure each setting that names a file on this machine is held to the roots."""
        outside = tmp_path_factory.mktemp("outside")

        with pytest.raises(
            ConfigError, match="outside the folders this server reads from"
        ) as refused:
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), build(outside))

        assert str(refused.value).startswith(f"The target {setting} '{outside}")
        assert "--root" in str(refused.value)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="The disguises climb to `/` and add a POSIX path, where Windows needs a drive.",
    )
    @pytest.mark.parametrize("build", _SQLITE_DISGUISES)
    def test_it_checks_the_sqlite_path_the_connector_opens(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        build: Callable[[Path, Path], DatabaseConfig],
    ) -> None:
        """Ensure a SQLite path is decoded as the connector decodes it, so no encoding leads out."""
        monkeypatch.chdir(tmp_path)
        settings = Settings((tmp_path,))
        inside = build(tmp_path, tmp_path / "x.db")
        outside_file = tmp_path_factory.mktemp("outside").resolve() / "x.db"
        outside = build(tmp_path, outside_file)

        check_data_paths(settings, SourceConfig(path="a.csv"), inside)
        with pytest.raises(ConfigError, match=r"The target SQLite file '.+' is outside"):
            check_data_paths(settings, SourceConfig(path="a.csv"), outside)

        # The connector opens this text too, and SQLite reads no `file:` URI in an absolute path.
        assert sqlite_path(inside) == str(tmp_path.resolve() / "x.db")
        assert sqlite_path(outside) == str(outside_file)

    def test_it_reads_a_sqlite_home_path_as_the_driver_does(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure `~` in a SQLite path names a folder in the working directory, as the driver reads it."""
        home = tmp_path_factory.mktemp("home")
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.chdir(tmp_path)
        side = _sqlite("sqlite://~/x.db")

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), side)
        with pytest.raises(ConfigError, match=r"The target SQLite file '.+' is outside"):
            check_data_paths(Settings((home,)), SourceConfig(path=str(home / "a.csv")), side)

        assert sqlite_path(side) == str(tmp_path / "~" / "x.db")

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize(
        ("build", "setting", "location"),
        [
            pytest.param(lambda path: SourceConfig(path=path), "path", "./~/x.csv", id="file"),
            pytest.param(
                lambda path: DuckDBConfig(database=path, table="t"),
                "database",
                "./~/x.duckdb",
                id="duckdb",
            ),
        ],
    )
    def test_it_reads_a_folder_named_home_as_the_readers_do(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        build: Callable[[str], SourceRef],
        setting: str,
        location: str,
    ) -> None:
        """Ensure `./~/x` names a folder `~` in the working directory, in the check and the read.

        `Path` drops the `./` and then expands `~`, so it would check the home
        folder, here under the root, while the reader opens the link named `~`.
        """
        outside = tmp_path_factory.mktemp("outside")
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        (tmp_path / "~").symlink_to(outside, target_is_directory=True)
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match=f"The target {setting} '{location}' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), build(location))

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A home folder of `/` and a path from it are POSIX forms."
    )
    def test_it_reads_a_duckdb_home_path_as_duckdb_is_handed_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `~//x` with a home folder of `/` reaches DuckDB as the resolved `/x`.

        The connector expands the text to `//x`, which Linux and macOS read as
        `/x` and DuckDB opens as no file, so the server hands DuckDB the path
        it resolved, which starts with one `/`.
        """
        monkeypatch.setenv("HOME", "/")
        (tmp_path / "x.duckdb").write_bytes(b"")
        database = f"~/{tmp_path / 'x.duckdb'}"

        _, target = check_data_paths(
            Settings((tmp_path,)),
            SourceConfig(path=str(tmp_path / "a.csv")),
            DuckDBConfig(database=database, table="t"),
        )

        assert isinstance(target, DuckDBConfig)
        assert target.database == os.path.realpath(tmp_path / "x.duckdb")

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize(
        ("database", "file"),
        [("{root}/x.duckdb", "x.duckdb"), ("~", "home")],
    )
    @pytest.mark.parametrize(("suffix", "log"), _DUCKDB_LOGS)
    def test_it_refuses_a_duckdb_write_ahead_log_linked_outside(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        database: str,
        file: str,
        suffix: str,
        log: str,
    ) -> None:
        """Ensure each log beside a DuckDB file is held to the roots, since DuckDB replays it.

        DuckDB opens the path it is handed with `.wal` added as it opens the
        file, before the sandbox applies, and follows a link there to another
        database's log. When that log ends in a checkpoint the file holds, as
        a crash during a checkpoint leaves it, a read-only open replays
        `.wal.checkpoint` instead. Python would read `~.wal` as the home of a
        user named `.wal`, where DuckDB adds `.wal` to the home folder.
        """
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        wal = tmp_path_factory.mktemp("outside") / "other.duckdb.wal"
        wal.write_bytes(b"")
        (tmp_path / file).write_bytes(b"")
        (tmp_path / f"{file}{suffix}").symlink_to(wal)
        target = DuckDBConfig(database=database.format(root=tmp_path), table="t")

        with pytest.raises(ConfigError, match=f"The target database's {log} '.+' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(sys.platform == "win32", reason="A Windows file name may not hold a `?`.")
    @pytest.mark.parametrize(("suffix", "log"), _DUCKDB_LOGS)
    def test_it_refuses_a_duckdb_write_ahead_log_that_a_question_mark_moves_outside(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, suffix: str, log: str
    ) -> None:
        """Ensure each log is checked where DuckDB puts it, before the first `?` of the path.

        For `L?/x.duckdb`, DuckDB replays `L.wal?/x.duckdb`, or
        `L.wal.checkpoint?/x.duckdb`, in another folder, which here is a link
        out of the root. `L?/x.duckdb.wal` is under the root, so a check of
        that path alone would pass.
        """
        (tmp_path / "L?").mkdir()
        (tmp_path / "L?" / "x.duckdb").write_bytes(b"")
        _link_folder(tmp_path / f"L{suffix}?", tmp_path_factory.mktemp("outside"))
        target = DuckDBConfig(database="L?/x.duckdb", table="t")

        wal = tmp_path / f"L{suffix}?" / "x.duckdb"
        with pytest.raises(ConfigError, match=re.escape(f"The target database's {log} '{wal}' is")):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(sys.platform == "win32", reason="A Windows file name may not hold a `?`.")
    def test_it_refuses_a_duckdb_write_ahead_log_that_the_working_directory_moves_outside(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a `?` in the working directory moves the WAL of a relative path, as in DuckDB.

        DuckDB makes the path absolute before it puts `.wal` before the first
        `?`, so with the server in `r?oot`, `x.duckdb` has its log at
        `r.wal?oot/x.duckdb`, here a link out of the root.
        """
        root = tmp_path / "r?oot"
        root.mkdir()
        (root / "x.duckdb").write_bytes(b"")
        _link_folder(tmp_path / "r.wal?oot", tmp_path_factory.mktemp("outside"))
        monkeypatch.chdir(root)
        target = DuckDBConfig(database="x.duckdb", table="t")

        with pytest.raises(ConfigError, match=r"The target database's WAL '.+' is outside"):
            check_data_paths(Settings((root,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    def test_it_refuses_the_write_ahead_log_beside_the_file_a_link_leads_to(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a link named as the database has its WAL checked beside the file it leads to.

        DuckDB follows the link before it adds `.wal`, so for a link
        `x.duckdb` to `sub/y.duckdb` it replays `sub/y.duckdb.wal`, here a
        link out of the root, and never looks for `x.duckdb.wal`.
        """
        wal = tmp_path_factory.mktemp("outside") / "other.duckdb.wal"
        wal.write_bytes(b"")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "y.duckdb").write_bytes(b"")
        (tmp_path / "sub" / "y.duckdb.wal").symlink_to(wal)
        (tmp_path / "x.duckdb").symlink_to(tmp_path / "sub" / "y.duckdb")
        target = DuckDBConfig(database="x.duckdb", table="t")

        named = re.escape(str(tmp_path / "sub" / "y.duckdb.wal"))
        with pytest.raises(ConfigError, match=f"The target database's WAL '{named}' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    def test_it_refuses_the_write_ahead_log_beside_the_file_a_link_and_dots_lead_to(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure `lnk/../x.duckdb` has its WAL checked in the folder above the link's target.

        DuckDB follows `lnk` to `real/deep` before the `..`, so it opens
        `real/x.duckdb` and replays `real/x.duckdb.wal`, here a link out of the
        root. Folded by text alone, the path names `x.duckdb` in the root.
        """
        wal = tmp_path_factory.mktemp("outside") / "other.duckdb.wal"
        wal.write_bytes(b"")
        (tmp_path / "real" / "deep").mkdir(parents=True)
        (tmp_path / "lnk").symlink_to(tmp_path / "real" / "deep", target_is_directory=True)
        for file in (tmp_path / "x.duckdb", tmp_path / "real" / "x.duckdb"):
            file.write_bytes(b"")
        (tmp_path / "real" / "x.duckdb.wal").symlink_to(wal)
        target = DuckDBConfig(database="lnk/../x.duckdb", table="t")

        named = re.escape(str(tmp_path / "real" / "x.duckdb.wal"))
        with pytest.raises(ConfigError, match=f"The target database's WAL '{named}' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize(
        "database",
        [
            "missing.duckdb",
            "missing/../x.duckdb",
            "lnk/../missing/../lnk2/../x.duckdb",
            "x.duckdb/",
            "x.duckdb/.",
            "x.duckdb/../x.duckdb",
            "file.duckdb/../x.duckdb",
        ],
    )
    def test_it_refuses_a_duckdb_path_that_does_not_resolve(
        self, tmp_path: Path, database: str
    ) -> None:
        """Ensure a DuckDB path the system cannot open as written is refused, on every Python.

        DuckDB follows the links up to the first part it cannot resolve, and
        folds the rest by text. So for `lnk/../missing/../lnk2/../x.duckdb` it
        opens `real/x.duckdb` and replays the log beside it. `os.path.realpath`
        follows `lnk2` too and names `other/x.duckdb`, and `os.path.abspath`
        follows no link and names `x.duckdb` in the root. A file, or a link to
        one, followed by a separator or `..` is no folder to the system, while
        `os.path.realpath` before Python 3.13 folds the `..` by text.
        """
        for folder in ("real/deep", "other/deep", "sub"):
            (tmp_path / folder).mkdir(parents=True)
        (tmp_path / "lnk").symlink_to(tmp_path / "real" / "deep", target_is_directory=True)
        (tmp_path / "real" / "lnk2").symlink_to(
            tmp_path / "other" / "deep", target_is_directory=True
        )
        for each in ("x.duckdb", "real/x.duckdb", "other/x.duckdb", "sub/x.duckdb"):
            (tmp_path / each).write_bytes(b"")
        (tmp_path / "file.duckdb").symlink_to(tmp_path / "sub" / "x.duckdb")
        target = DuckDBConfig(database=database, table="t")

        with pytest.raises(ConfigError, match=re.escape(f"The target database '{database}' does")):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    def test_it_refuses_a_duckdb_path_through_more_links_than_the_system_follows(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a chain of 41 links, each inside the root, is refused before DuckDB opens it.

        The system's `realpath` stops after 40 links, and DuckDB then folds
        `c0/..` by text, so it would open `x.duckdb` in the root and replay
        the log beside it, here a link out of the root. `os.path.realpath`
        follows every link and names `real/x.duckdb`.
        """
        wal = tmp_path_factory.mktemp("outside") / "other.duckdb.wal"
        wal.write_bytes(b"")
        (tmp_path / "real" / "deep").mkdir(parents=True)
        for file in (tmp_path / "x.duckdb", tmp_path / "real" / "x.duckdb"):
            file.write_bytes(b"")
        (tmp_path / "x.duckdb.wal").symlink_to(wal)
        (tmp_path / "c40").symlink_to(tmp_path / "real" / "deep", target_is_directory=True)
        for link in range(40):
            (tmp_path / f"c{link}").symlink_to(f"c{link + 1}", target_is_directory=True)
        target = DuckDBConfig(database="c0/../x.duckdb", table="t")

        with pytest.raises(
            ConfigError, match=re.escape("The target database 'c0/../x.duckdb' does")
        ):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(sys.platform == "win32", reason="A Windows file name may not hold a `?`.")
    def test_it_names_the_write_ahead_log_duckdb_writes(self, tmp_path: Path) -> None:
        """Ensure the server's name for the WAL is the file DuckDB writes, `?` and all."""
        (tmp_path / "L?").mkdir()
        (tmp_path / "L.wal?").mkdir()
        database = os.path.realpath(tmp_path / "L?" / "x.duckdb")
        connection = duckdb.connect(database)
        try:
            connection.execute("CREATE TABLE t (id INTEGER)")

            wal = beside(database, ".wal")

            assert Path(wal) == tmp_path / "L.wal?" / "x.duckdb"
            assert Path(wal).is_file()
        finally:
            connection.close()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize("back", [False, True], ids=["kept", "swapped-back"])
    def test_it_refuses_a_duckdb_folder_swapped_for_a_link_as_duckdb_opens_the_file(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        back: bool,
    ) -> None:
        """Ensure DuckDB reads no file but the one checked, though a folder changes after the check.

        The server checks the resolved path, and DuckDB opens it again by its
        text. Here the file's folder becomes a link out of the root just
        before DuckDB opens it, and with `back`, the folder returns before
        the connector looks again, so only the descriptor DuckDB holds shows
        which file it opened.
        """
        root, outside = tmp_path / "root", tmp_path_factory.mktemp("outside")
        for folder, column in ((root / "data", "checked"), (outside, "elsewhere")):
            folder.mkdir(parents=True, exist_ok=True)
            _duckdb_table(folder / "x.duckdb", column)
        _write(
            root,
            "source:\n  type: duckdb\n  database: data/x.duckdb\n  table: t\n"
            "target:\n  path: a.csv\nprimary_keys: [checked]\n",
        )
        monkeypatch.chdir(root)
        connect = duckdb.connect

        def swapped(*args: Any, **kwargs: Any) -> Any:
            (root / "data").rename(root / "checked")
            (root / "data").symlink_to(outside, target_is_directory=True)
            try:
                return connect(*args, **kwargs)
            finally:
                if back:
                    (root / "data").unlink()
                    (root / "checked").rename(root / "data")

        mocker.patch.object(duckdb, "connect", side_effect=swapped)
        refusal = "changed while DuckDB opened it" if back else "is a link"

        with pytest.raises(ConnectorError, match=refusal):
            describe_side(Settings((root,)), "veridelta.yaml", "source")

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows creates a symbolic link only with a privilege, and lists no descriptors.",
    )
    def test_it_refuses_a_duckdb_folder_swapped_while_another_call_holds_the_file(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        """Ensure a descriptor another tool call holds does not confirm what DuckDB opened.

        One call holds the file open through a hard link in the root, as a
        long pushdown does. Another call's folder becomes a link out of the
        root while DuckDB opens the file, and returns before the connector
        looks again, so only a descriptor DuckDB opened then may count.
        """
        root, outside = tmp_path / "root", tmp_path_factory.mktemp("outside")
        for folder, column in ((root / "data", "checked"), (outside, "elsewhere")):
            folder.mkdir(parents=True, exist_ok=True)
            _duckdb_table(folder / "x.duckdb", column)
        (root / "other").mkdir()
        os.link(root / "data" / "x.duckdb", root / "other" / "y.duckdb")
        _write(
            root,
            "source:\n  type: duckdb\n  database: data/x.duckdb\n  table: t\n"
            "target:\n  path: a.csv\nprimary_keys: [checked]\n",
        )
        monkeypatch.chdir(root)
        held = DuckDBPushdownSession(
            DuckDBConfig(database=str(root / "other" / "y.duckdb"), table="t", pushdown=True)
        )
        connect = duckdb.connect

        def swapped(*args: Any, **kwargs: Any) -> Any:
            (root / "data").rename(root / "checked")
            (root / "data").symlink_to(outside, target_is_directory=True)
            try:
                return connect(*args, **kwargs)
            finally:
                (root / "data").unlink()
                (root / "checked").rename(root / "data")

        with sandboxed([root]):
            held.connect()
            try:
                mocker.patch.object(duckdb, "connect", side_effect=swapped)
                with pytest.raises(ConnectorError, match="DuckDB did not open the DuckDB file"):
                    describe_side(Settings((root,)), "veridelta.yaml", "source")
            finally:
                held.close()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    def test_it_refuses_a_duckdb_log_put_beside_the_file_as_duckdb_opens_it(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        """Ensure a log that is a link put beside the file after the check is refused.

        DuckDB replays the log of the path it is handed, and a log from a copy
        of the file replays into it, columns and rows alike.
        """
        outside = tmp_path_factory.mktemp("outside")
        _duckdb_table(tmp_path / "x.duckdb", "checked")
        shutil.copy(tmp_path / "x.duckdb", outside / "x.duckdb")
        with duckdb.connect(str(outside / "x.duckdb")) as writer:
            writer.execute("PRAGMA disable_checkpoint_on_shutdown")
            writer.execute("SET checkpoint_threshold = '1TB'")
            writer.execute("ALTER TABLE t ADD COLUMN elsewhere INTEGER")
        _write(
            tmp_path,
            "source:\n  type: duckdb\n  database: x.duckdb\n  table: t\n"
            "target:\n  path: a.csv\nprimary_keys: [checked]\n",
        )
        monkeypatch.chdir(tmp_path)
        connect = duckdb.connect

        def logged(*args: Any, **kwargs: Any) -> Any:
            (tmp_path / "x.duckdb.wal").symlink_to(outside / "x.duckdb.wal")
            return connect(*args, **kwargs)

        mocker.patch.object(duckdb, "connect", side_effect=logged)

        with pytest.raises(ConnectorError, match=r"The DuckDB file's WAL '.+' is a link"):
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "source")

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(
                lambda settings: check_configuration(settings, "veridelta.yaml", schemas=True),
                id="validate_config",
            ),
            pytest.param(
                lambda settings: describe_side(settings, "veridelta.yaml", "target"),
                id="describe_schema",
            ),
            pytest.param(
                lambda settings: read_rows(settings, "veridelta.yaml", "added"),
                id="read_discrepancies",
            ),
            pytest.param(
                lambda settings: run_configuration(settings, "veridelta.yaml"),
                id="run_comparison",
            ),
            pytest.param(
                lambda settings: propose_maps(settings, "veridelta.yaml"),
                id="propose_value_maps",
            ),
            pytest.param(lambda settings: suggest(settings, "veridelta.yaml"), id="suggest_rules"),
        ],
    )
    def test_each_tool_hands_duckdb_the_path_it_checked(
        self, tmp_path: Path, mocker: MockerFixture, call: Callable[[Settings], object]
    ) -> None:
        """Ensure every tool opens a DuckDB file by its resolved path, which DuckDB keeps as it is.

        A path with no link and no `..` left in it is one DuckDB cannot
        resolve to another file than the one the server checked, whichever
        way its own resolution goes.
        """
        (tmp_path / "sub").mkdir()
        with duckdb.connect(str(tmp_path / "x.duckdb")) as connection:
            connection.execute("CREATE TABLE t AS SELECT 1 AS id, 'a' AS v")
        (tmp_path / "a.csv").write_text("id,v\n1,a\n", encoding="utf-8")
        _write(
            tmp_path,
            "source:\n  path: a.csv\ntarget:\n  type: duckdb\n"
            f"  database: {json.dumps(os.path.join('sub', '..', 'x.duckdb'))}\n"
            "  table: t\nprimary_keys: [id]\n",
        )
        connect = mocker.spy(duckdb, "connect")

        call(Settings((tmp_path,), allow_row_values=True))

        resolved = os.path.realpath(tmp_path / "x.duckdb")
        connect.assert_called_once_with(f"duckdb:{resolved}", read_only=True, config=mocker.ANY)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize("dots", ["..", "%2E%2E", ".%2e"])
    def test_it_checks_the_sqlite_path_a_link_leads_to(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        dots: str,
    ) -> None:
        """Ensure `..` after a link climbs from where the link leads, in the check and the read."""
        root = tmp_path_factory.mktemp("root").resolve()
        (root / "a" / "b").mkdir(parents=True)
        (root / "link").symlink_to(root / "a" / "b", target_is_directory=True)
        monkeypatch.chdir(root)
        settings = Settings((root,))
        inside = _sqlite(f"sqlite://link/{dots}/{dots}/outside/x.db")
        outside = _sqlite(f"sqlite://link/{dots}/{dots}/{dots}/{tmp_path.name}/x.db")

        check_data_paths(settings, SourceConfig(path="a.csv"), inside)
        with pytest.raises(ConfigError, match=r"The target SQLite file '.+' is outside"):
            check_data_paths(settings, SourceConfig(path="a.csv"), outside)

        # The connector opens this text, so no `..` is left for ConnectorX to fold another way.
        assert sqlite_path(inside) == str(root / "outside" / "x.db")
        assert sqlite_path(outside) == str(tmp_path.resolve() / "x.db")

    @pytest.mark.parametrize(
        "uri",
        [
            "sqlite+x://{}",
            "SQLite+A://{}",
            "sqlite+pysqlite://{}",
            "sqlite+://{}",
            " sqlite://{}",
            "\tsqli\nte://{}",
            "sqlite:{}",
        ],
    )
    def test_it_refuses_a_uri_connectorx_reads_as_sqlite_in_another_form(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, uri: str
    ) -> None:
        """Ensure no form of a SQLite URI but `sqlite://` reaches the read, even with a query.

        ConnectorX reads a URI as SQLite by its scheme up to the first `+`,
        after it drops spaces at either end and tabs or line breaks anywhere,
        and it opens the file such a URI names.
        """
        outside = tmp_path_factory.mktemp("outside") / "x.db"
        side = DatabaseConfig(uri=uri.format(outside), query="SELECT * FROM t")

        with pytest.raises(ConfigError, match="reads only one that starts with sqlite://"):
            check_data_paths(
                Settings((tmp_path,), allow_queries=True),
                SourceConfig(path=str(tmp_path / "a.csv")),
                side,
            )

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    def test_it_checks_a_sqlite_path_through_a_link_loop(self, tmp_path: Path) -> None:
        """Ensure a link loop leaves the connector to refuse the file, not a crash in the check."""
        (tmp_path / "loop-a").symlink_to(tmp_path / "loop-b")
        (tmp_path / "loop-b").symlink_to(tmp_path / "loop-a")

        check_data_paths(
            Settings((tmp_path,)),
            SourceConfig(path=str(tmp_path / "a.csv")),
            _sqlite(f"sqlite://{tmp_path}/loop-a/x.db"),
        )

    def test_it_refuses_a_sqlite_path_holding_a_nul(self, tmp_path: Path) -> None:
        """Ensure `%00` fails as a configuration error, not as a crash in the check."""
        with pytest.raises(ConfigError, match="NUL character"):
            check_data_paths(
                Settings((tmp_path,)),
                SourceConfig(path=str(tmp_path / "a.csv")),
                _sqlite(f"sqlite://{tmp_path}/x.db%00"),
            )

    def test_it_masks_the_password_of_a_refused_sqlite_path(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a refusal never repeats the `password` the driver reads as part of the path."""
        monkeypatch.chdir(tmp_path_factory.mktemp("elsewhere"))
        side = _sqlite("sqlite://u@/x.db", "hunter2")

        # Windows reads `u:` as a drive and writes it `U:\`.
        with pytest.raises(
            ConfigError, match=r"The target SQLite file '.*[uU]:\\?\*\*\*@"
        ) as refused:
            check_data_paths(
                Settings((tmp_path,)), SourceConfig(path=str(tmp_path / "a.csv")), side
            )

        assert "hunter2" not in str(refused.value)

    def test_it_checks_a_relative_path_where_the_reader_opens_it(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a relative path counts from the working directory, where Polars opens it."""
        monkeypatch.chdir(tmp_path_factory.mktemp("elsewhere"))

        with pytest.raises(ConfigError, match=r"The source path 'a\.csv' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), _snowflake())

    @pytest.mark.parametrize(
        "side",
        [
            pytest.param(SourceConfig(path="xx:" + "/.." * 64 + "/a.csv"), id="made-up"),
            pytest.param(
                SourceConfig(path="lakefs://repo" + "/.." * 64 + "/a.csv"), id="polars-unknown"
            ),
            pytest.param(
                DuckDBConfig(database="s3://bucket" + "/.." * 64 + "/x.duckdb", table="t"),
                id="duckdb-any",
            ),
        ],
    )
    def test_it_checks_a_location_with_a_scheme_as_a_path_too(
        self, tmp_path: Path, side: SourceRef
    ) -> None:
        """Ensure a scheme cannot carry a path out, since a reader that does not know it opens it here.

        Polars opens `lakefs://` as a relative path, and DuckDB opens every
        location but MotherDuck's that way, `s3://` included.
        """
        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(
                lambda uri: DeltaLakeConfig(table_uri=f"fs://{uri.removeprefix('file://')}"),
                id="fs",
            ),
            pytest.param(
                lambda uri: DeltaLakeConfig(
                    table_uri=f"opendal+fs://bucket{uri.removeprefix('file://')}"
                ),
                id="opendal-fs-with-a-host",
            ),
            pytest.param(
                lambda uri: SourceConfig(path=f"file:{uri.removeprefix('file:///')}"),
                id="file-without-slashes",
            ),
        ],
    )
    def test_it_checks_the_path_of_a_local_scheme_as_its_reader_opens_it(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        build: Callable[[str], SourceRef],
    ) -> None:
        """Ensure a URI a reader opens here is held to the roots, whatever host it names.

        Delta Lake opens `fs://` and `opendal+fs://` on this machine and ignores
        the host, and Polars reads `file:a/b` from the top, as `/a/b`.
        """
        outside = tmp_path_factory.mktemp("outside")

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(
                Settings((tmp_path,)), SourceConfig(path="a.csv"), build(outside.as_uri())
            )

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize(
        "location",
        [
            *(
                pytest.param(f"{scheme}:out/a.parquet", id=f"{scheme}-without-slashes")
                for scheme in (
                    "abfs",
                    "abfss",
                    "adl",
                    "az",
                    "azure",
                    "gcs",
                    "gs",
                    "hf",
                    "http",
                    "https",
                    "lakefs",
                    "md",
                    "motherduck",
                    "s3",
                    "s3a",
                )
            ),
            pytest.param("s3:/out/a.parquet", id="s3-with-one-slash"),
            *(
                pytest.param(f"{scheme}://out/a.parquet", id=scheme)
                for scheme in ("lakefs", "md", "motherduck")
            ),
        ],
    )
    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda location: SourceConfig(path=location), id="path"),
            pytest.param(lambda location: DeltaLakeConfig(table_uri=location), id="delta"),
            pytest.param(lambda location: IcebergConfig(table_uri=location), id="iceberg"),
        ],
    )
    def test_it_checks_a_location_polars_opens_here_as_a_path(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        location: str,
        build: Callable[[str], SourceRef],
    ) -> None:
        """Ensure a store's scheme without `//`, or a scheme Polars fetches nothing by, leads nowhere.

        Polars opens such a location as a path from the working directory, so
        a link in the root named like its first part leads out of the root.
        """
        monkeypatch.chdir(tmp_path)
        outside = tmp_path_factory.mktemp("outside")
        (tmp_path / Path(location).parts[0]).symlink_to(outside, target_is_directory=True)

        with pytest.raises(ConfigError, match=f"'{re.escape(location)}' is outside the folders"):
            check_data_paths(Settings((tmp_path,)), build(location), _snowflake())

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize("scheme", ["s3", "https", "hf"])
    @pytest.mark.parametrize(
        ("build", "setting", "shown"),
        [
            pytest.param(
                lambda file: DuckDBConfig(database=file, table="t"),
                "database",
                lambda file: file,
                id="duckdb",
            ),
            pytest.param(
                lambda file: DatabaseConfig(uri=f"sqlite://{file}", table="t"),
                "SQLite file",
                lambda file: str(Path(file).resolve()),
                id="sqlite",
            ),
        ],
    )
    def test_it_checks_a_database_file_named_with_a_store_scheme_as_a_path(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        scheme: str,
        build: Callable[[str], SourceRef],
        setting: str,
        shown: Callable[[str], str],
    ) -> None:
        """Ensure a DuckDB or SQLite file named like `s3://x/a.db` is held to the roots.

        DuckDB and SQLite open it as a file from the working directory, through
        a link in the root named `s3:`. DuckDB opens it before the sandbox holds
        its connection. A refusal names the SQLite path the driver opens, with
        the link followed.
        """
        monkeypatch.chdir(tmp_path)
        outside = tmp_path_factory.mktemp("outside")
        (tmp_path / f"{scheme}:").symlink_to(outside, target_is_directory=True)
        file = f"{scheme}://out/a.db"

        with pytest.raises(ConfigError, match=f"The source {setting} '{re.escape(shown(file))}'"):
            check_data_paths(Settings((tmp_path,)), build(file), _snowflake())

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param("FILE://{root}/a.csv", id="file-scheme-in-capitals"),
            pytest.param("S3://{root}/a.csv", id="remote-scheme-in-capitals"),
            pytest.param("file://{root}/x%2Fy/../../a.csv", id="escaped-slash"),
        ],
    )
    def test_it_checks_a_uri_where_each_reader_opens_it(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        uri: str,
    ) -> None:
        """Ensure a URI the readers open in different places is held to the roots in each.

        Polars opens a scheme in capitals as a folder of that name, and decodes
        `%` escapes. PyIceberg decodes none.
        """
        outside = tmp_path_factory.mktemp("outside")
        monkeypatch.chdir(tmp_path)
        for name in ("FILE:", "S3:"):
            (tmp_path / name).symlink_to(outside, target_is_directory=True)
        side = SourceConfig(path=uri.format(root=tmp_path))

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

    @pytest.mark.skipif(sys.platform == "win32", reason="The URI names a POSIX path.")
    def test_it_checks_a_file_uri_whose_standard_reading_does_not_parse(
        self, tmp_path: Path
    ) -> None:
        """Ensure a file URI is still checked when its reading with a backslash as `/` does not parse.

        With a backslash as `/`, the URI names a host with a fullwidth at sign,
        which a URL parser refuses, as a reader that follows the URL standard
        does, so the check holds the URI's other readings to the roots.
        """
        side = SourceConfig(path=f"file:/\\a{chr(0xFF20)}b\\a.csv")

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="The forms are POSIX paths, and a link needs a privilege a runner may lack.",
    )
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("file:{outside_relative}/a.csv", id="file-uri-without-a-slash"),
            pytest.param("file:{outside_backslashed}\\a.csv", id="file-uri-with-backslashes"),
            pytest.param("file:{root}/.. ", id="file-uri-ending-in-a-space"),
            pytest.param("{root}/link/../../{outside_name}/a.csv", id="parent-of-a-link"),
            pytest.param(
                "file://{root}/link/../../{outside_name}/a.csv", id="uri-parent-of-a-link"
            ),
        ],
    )
    def test_it_checks_a_path_as_each_reader_parses_it(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, path: str
    ) -> None:
        """Ensure a path is held to the roots however a reader parses it.

        Polars and deltalake read a file URI as the URL standard says: from
        `/`, with a backslash as `/`, and without the spaces at its ends.
        PyArrow folds `..` into the folder before it follows a link, and the
        system follows the link first.
        """
        outside = tmp_path_factory.mktemp("outside")
        (tmp_path / "deep" / "er").mkdir(parents=True)
        (tmp_path / "link").symlink_to(tmp_path / "deep" / "er", target_is_directory=True)
        side = SourceConfig(
            path=path.format(
                root=tmp_path,
                outside_relative=str(outside).lstrip("/"),
                outside_backslashed=str(outside).replace("/", "\\"),
                outside_name=outside.name,
            )
        )

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

    def test_it_reads_a_home_path_as_the_readers_do(self, tmp_path: Path) -> None:
        """Ensure `~` is expanded first, since Polars expands it, so it cannot lead out."""
        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="~/a.csv"), _snowflake())

    @pytest.mark.parametrize(
        "location",
        [
            "file:/host.example/share/a.csv",
            "file:///host.example/share/a.csv",
            "fs:/host.example/share/t",
        ],
    )
    def test_it_reads_no_file_uri_as_a_path_on_another_machine(
        self, monkeypatch: pytest.MonkeyPatch, location: str
    ) -> None:
        """Ensure each reading of a file URI's path stays on this machine under Windows' rules.

        From Python 3.13, Windows holds a path that starts with one separator
        and no drive not to be absolute, so a separator added in front of it
        would make a UNC path, which names another machine. The URI's path is
        rooted before it becomes a path, so no reading starts with two.
        """
        windows = SimpleNamespace(path=ntpath, sep="\\", altsep="/")
        monkeypatch.setattr(mcp_server, "os", windows)
        monkeypatch.setattr(mcp_server, "url2pathname", lambda url: url.replace("/", "\\"))

        readings = mcp_server._local_paths(location, "source path")  # pyright: ignore[reportPrivateUsage] - the readings alone show the path that would reach the host

        assert "\\host.example\\share\\" + location.rsplit("/", 1)[1] in readings
        assert not any(reading.startswith("\\\\") for reading in readings)

    @pytest.mark.parametrize(
        "location",
        [
            pytest.param("file:///C:/root/events", id="file"),
            pytest.param("file://localhost/C:/root/events", id="localhost"),
            pytest.param("file:///C:/root/my%20events", id="percent-escape"),
            pytest.param("fs:///C:/root/events", id="fs"),
            pytest.param("opendal+fs:///C:/root/events", id="opendal-fs"),
        ],
    )
    def test_it_reads_a_local_uri_under_a_root_as_windows_resolves_it(
        self, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, location: str
    ) -> None:
        r"""Ensure a URI under a root passes on Windows, as a table and as a file its log names.

        The check also reads the URI as written as a path, such as
        `file:\C:\root\events`, which holds `C:` as a name. Windows' own
        `realpath` adds back the names past the folders that exist with
        `join`, which reads that `C:` as a drive and drops the root before it.
        """
        settings = _on_a_windows_disk(monkeypatch)
        listed = [("deletion vector", f"{location}/deletion_vector.bin", True)]
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(listed))

        for table_uri in (location, "C:\\root\\events"):
            check_data_paths(settings, DeltaLakeConfig(table_uri=table_uri), _snowflake())

    @pytest.mark.parametrize(
        "location",
        [
            pytest.param("file:///C:/elsewhere/events", id="file"),
            pytest.param("D:data\\events", id="drive-relative"),
        ],
    )
    def test_it_refuses_a_path_outside_the_roots_as_windows_resolves_it(
        self, monkeypatch: pytest.MonkeyPatch, location: str
    ) -> None:
        r"""Ensure a path outside every root is refused on Windows, a path relative to a drive's own folder too.

        `D:data\events` lies in the folder `D:` was last in, which the server
        cannot know, so it never reads as `D:\data\events`, under a root.
        """
        settings = _on_a_windows_disk(monkeypatch, "C:\\root", "D:\\data")

        with pytest.raises(ConfigError, match=r"The source table_uri .* is outside the folders"):
            check_data_paths(settings, DeltaLakeConfig(table_uri=location), _snowflake())

    @pytest.mark.parametrize("path", _ANOTHER_MACHINE)
    def test_it_resolves_no_path_on_another_machine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a path Windows reads as another machine's is refused before it is resolved or matched.

        On Windows, resolving, matching, or opening such a path reaches the host
        it names, and can send the user's credentials there.
        """
        settings = Settings((tmp_path,))
        reached = _as_windows(monkeypatch, mocker)

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(settings, SourceConfig(path=path), _snowflake())

        assert not _on_another_machine(reached())

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param("sqlite:////host.example/share/x.db", id="unc"),
            pytest.param("sqlite://%5C%5Chost.example%5Cshare%5Cx.db", id="unc-encoded"),
            pytest.param("sqlite:///%5Chost.example/share/x.db", id="unc-with-mixed-separators"),
            pytest.param("sqlite://%5C%5C%3F%5CUNC%5Chost.example%5Cshare%5Cx.db", id="nt-unc"),
            pytest.param("sqlite://%5C%3F%3F%5CUNC%5Chost.example%5Cshare%5Cx.db", id="kernel-unc"),
        ],
    )
    def test_it_resolves_no_sqlite_path_on_another_machine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, uri: str
    ) -> None:
        """Ensure a SQLite path Windows reads as another machine's is refused before it is resolved."""
        settings = Settings((tmp_path,))
        reached = _as_windows(monkeypatch, mocker)

        with pytest.raises(ConfigError, match=r"The source SQLite file '.+' is outside"):
            check_data_paths(settings, _sqlite(uri), _snowflake())

        assert not _on_another_machine(reached())

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("//host.example", id="host-alone"),
            pytest.param("\\\\\\host.example\\share\\a.csv", id="three-backslashes"),
            pytest.param("///host.example/share/a.csv", id="three-slashes"),
        ],
    )
    def test_it_asks_about_the_text_before_windows_path_folds_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a path on another machine is refused as written, before `Path` reads it.

        On Python 3.11, Windows' `Path` drops a separator from such a path,
        and the result reads as a path on this machine. This folds it so on
        every version, for a data path and for a file a lakehouse scan opens.
        """

        class Folded(PureWindowsPath):
            """A Windows path as Python 3.11 reads one that starts with two separators."""

            def expanduser(self) -> "Folded":
                return self

        def fold(*parts: str) -> Folded:
            text = str(PureWindowsPath(*parts))
            return Folded("\\" + text.lstrip("\\") if text.startswith("\\") else text)

        settings = Settings((tmp_path,))
        reached = _as_windows(monkeypatch, mocker)
        monkeypatch.setattr(mcp_server, "Path", fold)

        with pytest.raises(ConfigError, match="outside the folders"):
            check_data_paths(settings, SourceConfig(path=path), _snowflake())
        with pytest.raises(ConfigError, match="was about to open a file outside"):
            mcp_server._refuse_opened(settings, path, remote=False)  # pyright: ignore[reportPrivateUsage] - a lakehouse scan calls it with each file it opens
        with pytest.raises(ConfigError, match="private_key_path is outside the folders"):
            check_data_paths(settings, _snowflake(), _key_pair(path))

        assert not [each for each in reached() if "host.example" in each]

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows reads a path after two separators as another machine's.",
    )
    def test_it_reads_a_path_after_two_slashes_as_local_where_only_a_slash_separates(
        self, tmp_path: Path
    ) -> None:
        """Ensure a path that starts with `//` still reads on a system that reads it as local.

        A SQLite URI written `sqlite:////` and an absolute path is one such path.
        """
        (tmp_path / "a.csv").write_text("id\n1\n", encoding="utf-8")
        side = _sqlite(f"sqlite:///{tmp_path}/x.db")

        check_data_paths(Settings((tmp_path,)), SourceConfig(path=f"/{tmp_path}/*.csv"), side)

        assert sqlite_path(side) == str(tmp_path.resolve() / "x.db")

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(URLError("file:// scheme is supported only on localhost"), id="host"),
            pytest.param(ValueError("Invalid IPv6 URL"), id="unparsed-host"),
            pytest.param(OSError("Bad URL: /a|b|c/x.csv"), id="second-colon-on-windows"),
            pytest.param(IndexError("string index out of range"), id="no-drive-on-windows"),
        ],
    )
    def test_it_refuses_a_file_uri_python_cannot_read_as_a_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception
    ) -> None:
        """Ensure a file URI whose path Python cannot convert fails as a configuration error.

        From Python 3.14, converting a path that starts with `//` and a host
        raises a URLError or a ValueError. On Windows before 3.14, converting a
        path with a second `:` raises an OSError or an IndexError. This
        simulates each on every version.
        """

        def convert(path: str) -> str:
            raise error

        monkeypatch.setattr(mcp_server, "url2pathname", convert)
        side = SourceConfig(path="file:///a:b:c/marker.csv")

        with pytest.raises(
            ConfigError, match="The source path is a file URI whose path Python cannot read"
        ) as refused:
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

        assert "marker" not in str(refused.value)

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("file:////host.example/share/a.csv", id="host"),
            pytest.param("fs:////host.example/share/a.csv", id="host-in-delta-lake-scheme"),
            pytest.param("opendal+fs:////host.example/share/a.csv", id="host-in-opendal-scheme"),
            pytest.param("file:////localhost/share/a.csv", id="localhost"),
            pytest.param("file:////this-machine/share/a.csv", id="this-machine"),
            pytest.param("file://localhost//this-machine/share/a.csv", id="after-localhost"),
            pytest.param("file:\\\\\\\\this-machine\\share\\a.csv", id="after-backslashes"),
            pytest.param("file://///share/a.csv", id="three-slashes"),
        ],
    )
    def test_it_refuses_a_uri_whose_path_starts_with_two_slashes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a URI whose path starts with `//` fails before any path is read from it.

        Python 3.14 converts `//localhost/x`, and `//` with this machine's
        name, to `/x`, while DuckDB opens `/localhost/x`, so the check and the
        read would name two files. Python 3.14 cannot convert one with another
        host, and Windows reads it as a path on another machine.
        """
        import socket

        monkeypatch.setattr(socket, "gethostname", lambda: "this-machine")
        converted = mocker.spy(mcp_server, "url2pathname")
        sides: list[SourceRef] = [SourceConfig(path=path)]
        database = path.replace("a.csv", "a.duckdb")
        if database.startswith("file:"):
            # `DuckDBConfig` refuses a `file:` URI itself, before the server reads its path.
            with pytest.raises(ValueError, match="is a file: URI"):
                DuckDBConfig(database=database, table="t")
        else:
            sides.append(DuckDBConfig(database=database, table="t"))

        for side in sides:
            with pytest.raises(
                ConfigError, match="is a URI whose path starts with '//'"
            ) as refused:
                check_data_paths(Settings((tmp_path,)), side, _snowflake())

            assert "share" not in str(refused.value)
        converted.assert_not_called()

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("file://host.example/share/a.csv", id="host"),
            pytest.param("FILE://Host.Example/share/a.csv", id="host-in-capitals"),
            pytest.param("file:\\\\host.example\\share\\a.csv", id="host-after-backslashes"),
            pytest.param("file:/\\host.example/share/a.csv", id="host-after-mixed-separators"),
            pytest.param("file://localhost./share/a.csv", id="localhost-with-a-dot"),
            pytest.param("file://user@localhost/share/a.csv", id="localhost-with-a-user"),
        ],
    )
    def test_it_refuses_a_file_uri_that_names_a_host(
        self, tmp_path: Path, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a file URI that names a host fails before any reader opens it, never quoted.

        On Windows, deltalake opens `file://host/x` on that host, and so does
        a URL parser that reads a backslash as `/`.
        """
        import deltalake

        opened = mocker.spy(deltalake, "DeltaTable")
        sides = (SourceConfig(path=path), DeltaLakeConfig(table_uri=path.removesuffix("/a.csv")))

        for side in sides:
            with pytest.raises(ConfigError, match="is a file URI that names a host") as refused:
                check_data_paths(Settings((tmp_path,)), side, _snowflake())

            assert "host.example" not in str(refused.value).lower()
        opened.assert_not_called()

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows on Python 3.11 resolves text such as 'file:' otherwise.",
    )
    def test_it_reads_a_file_uri_on_localhost_as_a_path_here(self, tmp_path: Path) -> None:
        """Ensure a file URI whose host is `localhost`, in any case, is checked as a path here."""
        file = tmp_path / "a.csv"

        for host in ("localhost", "LocalHost"):
            uri = f"file://{host}{file.as_uri().removeprefix('file://')}"
            check_data_paths(Settings((tmp_path,)), SourceConfig(path=uri), _snowflake())

    def test_it_refuses_a_listed_file_uri_that_names_a_host(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a file a table's listing names on another host fails, never quoted."""
        listed = [("data file", "file://host.example/share/part-0.parquet", False)]
        mocker.patch.object(mcp_server, "_table_files", return_value=iter(listed))
        side = DeltaLakeConfig(table_uri=str(tmp_path / "t"))

        with pytest.raises(ConfigError, match="The source data file is a file URI that") as refused:
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

        assert "host.example" not in str(refused.value)

    @pytest.mark.parametrize("scheme", ["dbfs", "DBFS"])
    def test_it_refuses_a_dbfs_uri_whatever_host_it_names(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        scheme: str,
    ) -> None:
        """Ensure a `dbfs:` table, or a file a table names that way, fails before deltalake opens it.

        Once the server's environment allows it, deltalake opens a `dbfs:`
        URI's path on this machine and ignores its host, so an allowed host
        would lead to a table outside every root.
        """
        import deltalake

        outside = tmp_path_factory.mktemp("outside") / "events"
        _ROW.write_delta(outside)
        monkeypatch.setenv("MOUNT_ALLOW_UNSAFE_RENAME", "true")
        opened = mocker.spy(deltalake, "DeltaTable")
        settings = Settings((tmp_path,), allowed_hosts=("bucket.example.com",))
        uri = f"{scheme}://bucket.example.com{outside.as_uri().removeprefix('file://')}"

        with pytest.raises(ConfigError, match="The source table_uri is a dbfs: URI"):
            check_data_paths(settings, DeltaLakeConfig(table_uri=uri), _snowflake())

        opened.assert_not_called()
        listed = [("deletion vector", f"{uri}/vector.bin", True)]
        mocker.patch.object(mcp_server, "_table_files", return_value=iter(listed))
        side = DeltaLakeConfig(table_uri=str(tmp_path / "t"))

        with pytest.raises(ConfigError, match="The source deletion vector is a dbfs: URI"):
            check_data_paths(settings, side, _snowflake())

    def test_it_refuses_a_listed_file_uri_python_cannot_read_as_a_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a file a table's listing names fails as a configuration error, never quoted."""

        def convert(path: str) -> str:
            raise URLError("file:// scheme is supported only on localhost")

        listed = [("data file", "file:///a:b:c/marker-0.parquet", False)]
        mocker.patch.object(mcp_server, "_table_files", return_value=iter(listed))
        monkeypatch.setattr(mcp_server, "url2pathname", convert)
        side = DeltaLakeConfig(table_uri=str(tmp_path / "t"))

        with pytest.raises(ConfigError, match="The source data file is a file URI") as refused:
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

        assert "marker" not in str(refused.value)

    @staticmethod
    def _link_out(root: Path, outside: Path, name: str = "link") -> None:
        """Link a folder under the root to `dir` outside it, beside a file the root must not reach.

        Through the link, `data.csv` sits in that folder and `secret.csv` one level up.
        """
        (outside / "dir").mkdir()
        (outside / "dir" / "data.csv").write_text("id\n1\n")
        (outside / "secret.csv").write_text("id\n2\n")
        _link_folder(root / name, outside / "dir")

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("lin[k]/../secret.csv", id="class"),
            pytest.param("l?nk/../secret.csv", id="any-character"),
            pytest.param("*/../secret.csv", id="wildcard"),
            pytest.param("data/**/../../secret.csv", id="recursive"),
            pytest.param("lin[k]/.[.]/secret.csv", id="dot-class"),
            pytest.param("*/.?/secret.csv", id="dot-any-character"),
            pytest.param("l*/.*/secret.csv", id="dot-wildcard"),
            pytest.param("lin[k]/.[!x]/secret.csv", id="dot-negated-class"),
        ],
    )
    def test_it_refuses_a_pattern_that_can_match_a_parent(self, tmp_path: Path, path: str) -> None:
        """Ensure a pattern that can reach `..` is refused, since a match can be a link it leaves.

        Polars matches `..` for a name with a glob character that starts with a dot, which the
        standard library never lists, so the check refuses that name as it refuses `..`.
        """
        with pytest.raises(ConfigError, match="is a glob pattern that can match '\\.\\.'"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path=path), _snowflake())

    @pytest.mark.parametrize(
        "pattern",
        [
            pytest.param("link[/]data.csv", id="class-of-a-separator"),
            pytest.param("*[/]data.csv", id="wildcard"),
            pytest.param("lin[k]/data.csv", id="class"),
            pytest.param("lin?/data.csv", id="any-character"),
            pytest.param("lin%5Bk%5D/data.csv", id="encoded"),
        ],
    )
    def test_it_refuses_a_pattern_in_a_file_uri(self, tmp_path: Path, pattern: str) -> None:
        """Ensure a `file:` URI holds no pattern, since Polars expands one with another matcher."""
        location = f"{tmp_path.as_uri()}/{pattern}"

        with pytest.raises(ConfigError, match="is a file URI with a glob character in it"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path=location), _snowflake())

    @pytest.mark.parametrize(
        ("name", "path"),
        [
            pytest.param("link", "lin[k]/data.csv", id="class"),
            pytest.param("link", "l?nk/data.csv", id="any-character"),
            pytest.param("link", "*/data.csv", id="wildcard"),
            pytest.param("link", "**/data.csv", id="recursive"),
            pytest.param(".link", "*/data.csv", id="hidden"),
        ],
    )
    def test_it_refuses_a_pattern_that_matches_a_link_out(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        name: str,
        path: str,
    ) -> None:
        """Ensure each match is resolved, as Polars opens it, so a matched link cannot lead out."""
        self._link_out(tmp_path, tmp_path_factory.mktemp("outside"), name)
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "data.csv").write_text("id\n2\n")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match=f"The source path '{re.escape(path)}' is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path=path), _snowflake())

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows folds '..' before it follows a link."
    )
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("lin[k]/../secret.csv", id="class-then-parent"),
            pytest.param("*/../secret.csv", id="wildcard-then-parent"),
            pytest.param("lin[k]/.[.]/secret.csv", id="dot-class"),
            pytest.param("*/.?/secret.csv", id="dot-any-character"),
            pytest.param("l*/.*/secret.csv", id="dot-wildcard"),
            pytest.param("lin[k]/data.csv", id="class"),
            pytest.param("**/data.csv", id="recursive"),
            pytest.param("{uri}/link[/]data.csv", id="uri-class-of-a-separator"),
            pytest.param("{uri}/*[/]data.csv", id="uri-wildcard"),
        ],
    )
    def test_it_refuses_each_pattern_polars_reads_out_through(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        path: str,
    ) -> None:
        """Ensure the check refuses each pattern that the installed Polars reads out of a root.

        Polars is asked first, so a change in how it expands a pattern fails here.
        """
        self._link_out(tmp_path, tmp_path_factory.mktemp("outside"))
        monkeypatch.chdir(tmp_path)
        location = path.format(uri=tmp_path.as_uri())
        opened = pl.scan_csv(location, include_file_paths="file").collect()["file"]

        assert not all(
            Path(file.removeprefix("file://")).resolve().is_relative_to(tmp_path.resolve())
            for file in opened
        )
        with pytest.raises(ConfigError, match="The source path"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path=location), _snowflake())

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("data/**/*.csv", id="recursive"),
            pytest.param("*/a.csv", id="wildcard-through-a-link"),
            pytest.param("lat[e]st/a.csv", id="class-of-a-link"),
            pytest.param(".hidden/*.csv", id="hidden-folder-as-written"),
            pytest.param("**/*.csv", id="everything"),
        ],
    )
    def test_it_accepts_a_pattern_whose_matches_are_under_a_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
    ) -> None:
        """Ensure a pattern passes when every match, a link's included, resolves under a root.

        Polars then reads it, and opens nothing outside the root.
        """
        (tmp_path / "data" / "2026").mkdir(parents=True)
        (tmp_path / "data" / "2026" / "a.csv").write_text("id\n1\n")
        (tmp_path / ".hidden").mkdir()
        (tmp_path / ".hidden" / "b.csv").write_text("id\n2\n")
        _link_folder(tmp_path / "latest", tmp_path / "data" / "2026")
        monkeypatch.chdir(tmp_path)

        check_data_paths(Settings((tmp_path,)), SourceConfig(path=path), _snowflake())

        opened = pl.scan_csv(path, include_file_paths="file").collect()["file"]
        assert opened.len() > 0
        assert all(Path(file).resolve().is_relative_to(tmp_path.resolve()) for file in opened)

    @pytest.mark.parametrize(
        "side",
        [
            pytest.param(SourceConfig(path="s3://bucket/a.parquet"), id="object-store"),
            pytest.param(SourceConfig(path="https://example.com/a.csv"), id="https"),
            pytest.param(DuckDBConfig(database="md:sales", table="t"), id="motherduck"),
            pytest.param(
                DuckDBConfig(database="motherduck:sales", table="t"), id="motherduck-in-full"
            ),
            pytest.param(
                DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="orders"),
                id="database-server",
            ),
            pytest.param(_snowflake(), id="warehouse"),
        ],
    )
    def test_it_leaves_remote_data_to_the_host_check(self, tmp_path: Path, side: SourceRef) -> None:
        """Ensure data on another machine is not held to the roots, but to the allowed hosts."""
        check_data_paths(Settings((tmp_path,)), side, side)

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("{outside}/k.pem", id="absolute"),
            pytest.param("../{outside_name}/k.pem", id="relative"),
            pytest.param("~/k.pem", id="home"),
            pytest.param("./~/k.pem", id="home-after-a-dot"),
        ],
    )
    def test_it_refuses_a_snowflake_key_file_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        path: str,
    ) -> None:
        """Ensure the key file the driver opens here is held to the roots, by its setting alone.

        The driver drops a leading `./` before it expands `~`, so `./~/k.pem`
        is the key in the home folder, outside the root that holds the
        working directory. The message names the setting and leaves the path
        out.
        """
        outside = tmp_path_factory.mktemp("outside")
        (outside / "k.pem").write_text("not a real key\n", encoding="utf-8")
        monkeypatch.setenv("HOME", str(outside))
        monkeypatch.setenv("USERPROFILE", str(outside))
        monkeypatch.chdir(tmp_path)
        side = _key_pair(path.format(outside=outside, outside_name=outside.name))

        with pytest.raises(
            ConfigError, match=r"^The target private_key_path is outside"
        ) as refused:
            check_data_paths(Settings((tmp_path,)), _snowflake(), side)

        assert "k.pem" not in str(refused.value)
        assert outside.name not in str(refused.value)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows creates a symbolic link only with a privilege."
    )
    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("link.pem", id="link"),
            pytest.param("deeper/../../{outside_name}/k.pem", id="parent-of-a-link"),
        ],
    )
    def test_it_refuses_a_snowflake_key_file_a_link_leads_out_to(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, path: str
    ) -> None:
        """Ensure a link cannot carry the key file out, followed before a `..` or after it."""
        outside = tmp_path_factory.mktemp("outside")
        (outside / "k.pem").write_text("not a real key\n", encoding="utf-8")
        (tmp_path / "link.pem").symlink_to(outside / "k.pem")
        (tmp_path / "deep" / "er").mkdir(parents=True)
        (tmp_path / "deeper").symlink_to(tmp_path / "deep" / "er", target_is_directory=True)
        side = _key_pair(path.format(outside_name=outside.name))

        with pytest.raises(ConfigError, match="private_key_path is outside the folders"):
            check_data_paths(Settings((tmp_path,)), side, _snowflake())

    @pytest.mark.parametrize("path", _ANOTHER_MACHINE)
    def test_it_resolves_no_snowflake_key_file_on_another_machine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, path: str
    ) -> None:
        """Ensure a key file Windows reads as another machine's is refused before it is resolved.

        On Windows, the driver's open reaches the host such a path names.
        """
        reached = _as_windows(monkeypatch, mocker)

        with pytest.raises(ConfigError, match="private_key_path is outside the folders"):
            check_data_paths(Settings((tmp_path,)), _snowflake(), _key_pair(path))

        assert not _on_another_machine(reached())

    @pytest.mark.parametrize("path", ["k.pem", "keys/../k.pem", "{root}/k.pem", "~/k.pem"])
    def test_it_accepts_a_snowflake_key_file_under_a_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
    ) -> None:
        """Ensure a key file under a root passes, with `~` expanded as the driver expands it."""
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("USERPROFILE", str(tmp_path))
        (tmp_path / "keys").mkdir()
        (tmp_path / "k.pem").write_text("not a real key\n", encoding="utf-8")

        check_data_paths(Settings((tmp_path,)), _snowflake(), _key_pair(path.format(root=tmp_path)))


_ROW = pl.DataFrame({"id": [1], "note": [_SECRET]})
"""The one row of each lakehouse table, holding the text no tool may return."""


@contextmanager
def _catalog(lake: Path, warehouse: str | None = None, **options: str) -> Iterator[SqlCatalog]:
    """Open an Iceberg catalog with one namespace, kept in a SQLite file under `lake`.

    Its tables go to `warehouse`, or else to `lake`. The catalog's SQLite
    connections close on exit, since the suite fails on the warning Python
    gives for one left open.
    """
    catalog = SqlCatalog(
        "veridelta",
        uri=f"sqlite:///{lake / 'catalog.db'}",
        warehouse=warehouse or str(lake),
        **options,
    )
    try:
        catalog.create_namespace("lake")
        yield catalog
    finally:
        catalog.engine.dispose()


def _iceberg(lake: Path, properties: dict[str, str] | None = None, appends: int = 1) -> str:
    """Write an Iceberg table that appends `_ROW` `appends` times, and return its metadata file."""
    with _catalog(lake) as catalog:
        table = catalog.create_table(
            "lake.events", schema=_ROW.to_arrow().schema, properties=properties or {}
        )
        for _ in range(appends):
            table.append(_ROW.to_arrow())
        return str(catalog.load_table("lake.events").metadata_location)


def _one_layer_outside(root: Path, outside: Path, layer: str) -> str:
    """Write an Iceberg table under `root` whose files of one layer lie in `outside`.

    Returns:
        str: The `table_uri` that reads it, under `root`.
    """
    if layer == "data file":
        return _iceberg(root, {"write.data.path": str(outside)})
    if layer == "metadata file":
        metadata = Path(_iceberg(root))
        moved = shutil.copy(metadata, outside / metadata.name)
        (metadata.parent / "version-hint.text").write_text(str(moved))
        return str(metadata.parent.parent)
    # The table's metadata file, manifest list, and manifest all lie in `outside`.
    metadata = json.loads(Path(_iceberg(root, {"write.metadata.path": str(outside)})).read_text())
    if layer == "manifest":
        for snapshot in metadata["snapshots"]:
            listed = Path(snapshot["manifest-list"])
            snapshot["manifest-list"] = str(shutil.copy(listed, root / listed.name))
    copy = root / "v1.metadata.json"
    copy.write_text(json.dumps(metadata))
    return str(copy)


class _Store(SimpleHTTPRequestHandler):
    """Answer each read of an S3 object from a local folder."""

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


_STORE_HOSTS = ("bucket", "127.0.0.1")
"""The hosts a tool must allow to read a table in `_object_store`: its bucket and endpoint."""


@contextmanager
def _object_store(folder: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, str]]:
    """Serve `folder` as an S3 store on a free local port until the block ends.

    It stands in for an object store such as one moto runs: PyArrow reads each
    object over S3 from it, the bucket being the folder's first level. A tool
    reaches an endpoint only over HTTPS, so the options name it with
    `https://`. The store has no certificate, so PyArrow's S3 file system,
    patched until the test ends, reaches it over HTTP.

    Yields:
        dict[str, str]: The `storage_options` that reach it.
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(_Store, directory=str(folder)))
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    endpoint, s3 = f"127.0.0.1:{server.server_port}", arrow_fs.S3FileSystem
    # PyIceberg imports S3FileSystem when it opens a store, and passes keywords only.
    monkeypatch.setattr(
        arrow_fs,
        "S3FileSystem",
        lambda **options: s3(**{**options, "endpoint_override": f"http://{endpoint}"}),
    )
    try:
        yield {
            "s3.endpoint": f"https://{endpoint}",
            "s3.access-key-id": "veridelta",
            "s3.secret-access-key": "veridelta",
            "s3.region": "us-east-1",
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _remote_iceberg(
    lake: Path,
    store: Path,
    options: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    properties: dict[str, str] | None = None,
) -> str:
    """Write an Iceberg table that holds `_ROW` to `s3://bucket/wh` in the store at `store`.

    PyArrow writes an S3 object as a multipart upload, which the stand-in
    store does not take, so the writes go straight to its folder. Every read
    goes over S3.

    Returns:
        str: The table's metadata file, as an `s3://` URI.
    """
    for folder in ("metadata", "data"):
        (store / "bucket" / "wh" / "lake" / "events" / folder).mkdir(parents=True)
    with monkeypatch.context() as writes:
        writes.setattr(
            PyArrowFileIO,
            "_initialize_s3_fs",
            lambda _io, _netloc: SubTreeFileSystem(str(store), LocalFileSystem()),
        )
        with _catalog(lake, "s3://bucket/wh", **options) as catalog:
            table = catalog.create_table(
                "lake.events", schema=_ROW.to_arrow().schema, properties=properties or {}
            )
            table.append(_ROW.to_arrow())
            return str(catalog.load_table("lake.events").metadata_location)


_AZURE_OPTIONS = {"adls.account-name": "acct", "adls.account-key": "a2V5"}
"""The storage options of the table `_spark_azure_iceberg` writes."""

_AZURE_HOSTS = ("lake", "acct", "acct.dfs.core.windows.net")
"""The hosts a tool must allow to read that table: its container, its account, and the account's host."""


def _spark_azure_iceberg(lake: Path, store: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Write an Iceberg table that holds `_ROW` in Azure, its files named as Spark names them.

    Each file is named `abfss://lake@acct.dfs.core.windows.net/...`. PyArrow's
    Azure file system is mocked with `store`, whose first level is the
    container, for the write and for every read after it. The mock goes into
    the module's namespace without reading the name, which raises ImportError
    on a PyArrow build with no Azure file system. PyIceberg writes such
    a location in a folder named for its whole authority, so that folder is
    renamed to the container.

    Returns:
        str: The `table_uri` that reads it, as `abfs://lake/...`.
    """
    azure = MagicMock(return_value=SubTreeFileSystem(str(store), LocalFileSystem()))
    monkeypatch.setitem(arrow_fs.__dict__, "AzureFileSystem", azure)
    authority = "lake@acct.dfs.core.windows.net"
    for folder in ("metadata", "data"):
        (store / authority / "wh" / "lake" / "events" / folder).mkdir(parents=True)
    options = {**_AZURE_OPTIONS, "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}
    with _catalog(lake, f"abfss://{authority}/wh", **options) as catalog:
        table = catalog.create_table("lake.events", schema=_ROW.to_arrow().schema)
        table.append(_ROW.to_arrow())
        metadata = str(catalog.load_table("lake.events").metadata_location)
    (store / authority).rename(store / "lake")
    return metadata.replace(f"abfss://{authority}/", "abfs://lake/")


def _read_two_ways(root: Path, outside: Path, layer: str) -> str:
    """Write an Iceberg table under `root` that names one file PyIceberg and Polars open apart.

    The table holds a row whose note is `public`. Its metadata names one file
    of `layer` by a location that PyIceberg opens as the file the table
    wrote, and that the `polars_iceberg` plugin, which parses it otherwise,
    opens as a forged file under `root`. The forged file leads to a second
    table, whose data file in `outside` holds `_SECRET`. Both readings lie
    under `root`, so the listing passes either. The working directory must
    be `root`.

    Returns:
        str: The `table_uri` that reads it.
    """
    with _catalog(root) as catalog:
        for name, note, properties in (
            ("events", "public", {}),
            ("forged", _SECRET, {"write.data.path": str(outside)}),
        ):
            table = catalog.create_table(
                f"lake.{name}", schema=_ROW.to_arrow().schema, properties=properties
            )
            table.append(_ROW.with_columns(note=pl.lit(note)).to_arrow())
        events, forged = catalog.load_table("lake.events"), catalog.load_table("lake.forged")
    metadata = Path(events.metadata_location)
    written = json.loads(metadata.read_text())
    (snapshot,) = written["snapshots"]
    listed, forged_list = snapshot["manifest-list"], forged.current_snapshot().manifest_list
    if layer == "metadata file":
        # PyIceberg opens `FILE:///x` as `/x`, and the plugin as `FILE:/x` in the working directory.
        snapshot["manifest-list"] = forged_list
        (root / f"FILE:{metadata}").parent.mkdir(parents=True)
        (root / f"FILE:{metadata}").write_text(json.dumps(written))
        return f"FILE://{metadata}"
    if layer == "manifest list":
        (root / f"FILE:{listed}").parent.mkdir(parents=True)
        shutil.copy(forged_list, root / f"FILE:{listed}")
        snapshot["manifest-list"] = f"FILE://{listed}"
    elif layer == "manifest list without a slash":
        # PyIceberg opens `file:x` as `x` in the working directory, and the plugin as `/x`.
        (root / listed.lstrip("/")).parent.mkdir(parents=True)
        shutil.copy(listed, root / listed.lstrip("/"))
        shutil.copy(forged_list, listed)
        snapshot["manifest-list"] = f"file:{listed.lstrip('/')}"
    else:
        (manifest,) = events.current_snapshot().manifests(events.io)
        (forged_manifest,) = forged.current_snapshot().manifests(forged.io)
        (root / f"FILE:{manifest.manifest_path}").parent.mkdir(parents=True)
        shutil.copy(forged_manifest.manifest_path, root / f"FILE:{manifest.manifest_path}")
        # The manifest list entry as written, but for its first field: the manifest's location.
        renamed = ManifestFile(
            f"FILE://{manifest.manifest_path}", *(manifest[i] for i in range(1, len(manifest)))
        )
        snapshot["manifest-list"] = str(root / "renamed-list.avro")
        with write_manifest_list(
            format_version=written["format-version"],
            output_file=events.io.new_output(snapshot["manifest-list"]),
            snapshot_id=snapshot["snapshot-id"],
            parent_snapshot_id=None,
            sequence_number=snapshot["sequence-number"],
            avro_compression="deflate",
        ) as writer:
            writer.add_manifests([renamed])
    metadata.write_text(json.dumps(written))
    return str(metadata)


_Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
"""The alphabet in which a Delta Lake log writes the UUID of a deletion vector at a relative path."""


def _relative_vector(folder: str, name: uuid.UUID) -> str:
    """Write the relative path a Delta Lake log records for a deletion vector: a folder, then Z85."""
    encoded = ""
    for start in range(0, 16, 4):
        number = int.from_bytes(name.bytes[start : start + 4], "big")
        encoded += "".join(_Z85[number // 85**place % 85] for place in range(4, -1, -1))
    return folder + encoded


def _with_vectors(table: Path, *vectors: Mapping[str, object]) -> None:
    """Write `_ROW` to a table that allows deletion vectors, and record each vector on its file.

    Each vector gets a commit of its own, which adds the table's one data file
    again, with the vector. No vector's file is written: the listing reads
    where the log stores each, never the file.
    """
    _ROW.write_delta(
        table, delta_write_options={"configuration": {"delta.enableDeletionVectors": "true"}}
    )
    log = table / "_delta_log"
    first = (log / f"{0:020}.json").read_text(encoding="utf-8").splitlines()
    (added,) = [json.loads(line)["add"] for line in first if '"add"' in line]
    for version, vector in enumerate(vectors, start=1):
        removed = {"path": added["path"], "dataChange": True, "deletionTimestamp": version}
        stored = {"offset": 1, "sizeInBytes": 34, "cardinality": 1, **vector}
        commit = [{"remove": removed}, {"add": dict(added, deletionVector=stored)}]
        (log / f"{version:020}.json").write_text(
            "".join(json.dumps(action) + "\n" for action in commit), encoding="utf-8"
        )


def _vector_deleting(row: int) -> bytes:
    """Write the file of a deletion vector that deletes one row.

    It holds a version byte, then the bitmap's size, Delta's portable roaring
    bitmap of the row, and its CRC-32.
    """
    bitmap = struct.pack("<II", 12346, 1) + struct.pack("<HH", 0, 0)
    bitmap = struct.pack("<iqi", 1681511377, 1, 0) + bitmap + struct.pack("<IH", 16, row)
    return b"\x01" + struct.pack(">i", len(bitmap)) + bitmap + struct.pack(">I", zlib.crc32(bitmap))


def _store_at(mocker: MockerFixture, folder: Path) -> None:
    """Stand in for deltalake's storage of a table in an object store with `folder`, which holds its log."""
    mocker.patch("deltalake.fs.DeltaStorageHandler")
    mocker.patch.object(
        arrow_fs, "PyFileSystem", return_value=SubTreeFileSystem(str(folder), LocalFileSystem())
    )


def _with_a_sidecar(table: Path, sidecar: str, form: str) -> None:
    """Write `_ROW` to a table whose checkpoint names `sidecar` as the file of its actions.

    A sidecar belongs to a v2 checkpoint, but deltalake reads one in any
    checkpoint, whatever the table's protocol says. No sidecar file is
    written: the listing refuses the table before it is loaded.
    """
    _ROW.write_delta(table)
    log = table / "_delta_log"
    named = {"path": sidecar, "sizeInBytes": 1, "modificationTime": 0}
    if form == "json":
        name = f"{1:020}.checkpoint.{uuid.uuid4()}.json"
        (log / name).write_text(json.dumps({"sidecar": named}) + "\n", encoding="utf-8")
        return
    name = f"{1:020}.checkpoint.{'' if form == 'classic' else f'{uuid.uuid4()}.'}parquet"
    pl.DataFrame({"sidecar": [None, named]}).write_parquet(log / name)


class TestLakehouseFiles:
    """Validate the folder rule on each file a Delta Lake or Iceberg table names."""

    def test_it_accepts_a_delta_table_under_a_root(self, tmp_path: Path) -> None:
        """Ensure a table whose files all lie under a root passes, at each version."""
        _ROW.write_delta(tmp_path / "events")
        _ROW.write_delta(tmp_path / "events", mode="append")
        settings = Settings((tmp_path,))

        for version in (0, None):
            target = DeltaLakeConfig(table_uri="events", version=version)
            check_data_paths(settings, SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    def test_it_refuses_a_delta_data_file_linked_outside(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a data file that links out of a root is refused, at the version that reads it.

        The Delta log keeps every file under the table's folder, so a link is
        the way out of one.
        """
        table = tmp_path / "events"
        _ROW.write_delta(table)
        _ROW.write_delta(table, mode="append")
        (added,) = set(DeltaTable(str(table)).file_uris()) - set(
            DeltaTable(str(table), version=0).file_uris()
        )
        secret = tmp_path_factory.mktemp("outside") / "secret.parquet"
        Path(added).rename(secret)
        Path(added).symlink_to(secret)
        settings = Settings((tmp_path,))

        check_data_paths(
            settings, SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri="events", version=0)
        )
        with pytest.raises(ConfigError, match=r"The target data file is outside the folders"):
            check_data_paths(
                settings, SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri="events")
            )

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize(("link", "setting"), [("commit", "log file"), ("log", "log")])
    def test_it_refuses_a_delta_log_linked_outside(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, link: str, setting: str
    ) -> None:
        """Ensure a commit, or the whole log, that links out of a root is refused before it is read.

        A commit read through a link would quote its target's text in an
        error, or name data files the scan then reports missing.
        """
        table = tmp_path / "events"
        _ROW.write_delta(table)
        outside = tmp_path_factory.mktemp("outside")
        log = table / "_delta_log"
        if link == "log":
            log.rename(outside / "_delta_log")
            log.symlink_to(outside / "_delta_log", target_is_directory=True)
        else:
            secret = outside / "commit.json"
            secret.write_text(_SECRET)
            (log / "00000000000000000001.json").symlink_to(secret)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConfigError, match=f"The target {setting} '.*' is outside") as refused:
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert _SECRET not in str(refused.value)

    @pytest.mark.parametrize(
        ("uri", "refusal"),
        [
            pytest.param("s3://lake/events/part-0.parquet", None, id="remote-file"),
            pytest.param(
                "s3://lake/events/owner=ann@x.com/part-0.parquet", None, id="at-sign-in-a-path"
            ),
            pytest.param("{outside}/part-0.parquet", "is outside", id="local-file"),
            pytest.param("s3://ann@lake/part-0.parquet", "one shape", id="at-sign-in-a-host"),
            pytest.param("s3://lake/../hidden/part-0.parquet", "segment", id="dot-segment"),
            pytest.param(
                "s3://lake/%2E%2E/hidden/part-0.parquet", "segment", id="encoded-dot-segment"
            ),
        ],
    )
    def test_it_lists_a_remote_delta_table(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        uri: str,
        refusal: str | None,
    ) -> None:
        """Ensure a table in an object store has each data file on this machine held to the roots.

        The store is mocked: the log names the files that the test gives it.
        Spark leaves an `@` in a partition's value as it is, so one after a
        file's host is read as part of its path, and one before it is refused.
        """
        outside = tmp_path_factory.mktemp("outside").as_uri()
        _store_at(mocker, tmp_path)
        table = mocker.patch("deltalake.DeltaTable")
        table.return_value.file_uris.return_value = [uri.format(outside=outside)]
        target = DeltaLakeConfig(table_uri="s3://lake/events", storage_options={"region": "eu"})
        settings = Settings((tmp_path,), allowed_hosts=("lake",))

        if refusal is not None:
            with pytest.raises(ConfigError, match=f"The target data file .*{refusal}"):
                check_data_paths(settings, SourceConfig(path="a.csv"), target)
        else:
            check_data_paths(settings, SourceConfig(path="a.csv"), target)
        table.assert_called_once_with(
            "s3://lake/events", version=None, storage_options={"region": "eu"}
        )

    def test_it_names_each_deletion_vector_where_its_log_stores_it(self, tmp_path: Path) -> None:
        """Ensure a vector at its own path or at a relative one is listed, and one inline is not.

        A checkpoint records the vectors as well, which the listing reads
        alone once the commits before it are gone. A file in the log with no
        `add` action, which deltalake never reads, is passed over, and the
        server then refuses the table for its vector at its own path.
        """
        table, own = tmp_path / "events", (tmp_path / "vectors" / "own.bin").as_uri()
        first, second = uuid.uuid4(), uuid.uuid4()
        _with_vectors(
            table,
            {"storageType": "p", "pathOrInlineDv": own},
            {"storageType": "u", "pathOrInlineDv": _relative_vector("ab", first)},
            {"storageType": "u", "pathOrInlineDv": _relative_vector("", second)},
            {"storageType": "i", "pathOrInlineDv": "inline"},
        )
        DeltaTable(str(table)).create_checkpoint()
        for commit in (table / "_delta_log").glob("*.json"):
            commit.unlink()
        pl.DataFrame({"note": ["kept"]}).write_parquet(table / "_delta_log" / "notes.parquet")

        loaded = DeltaTable(str(table))
        logged = [str(path) for path in (table / "_delta_log").iterdir()]
        named = set(deletion_vector_files(loaded, delta_log_vectors(str(table), {}, logged)))
        with pytest.raises(ConnectorError, match="vector at a path of its own"):
            check_data_paths(
                Settings((tmp_path,)),
                SourceConfig(path="a.csv"),
                DeltaLakeConfig(table_uri="events"),
            )

        # Delta Lake joins a relative vector to the table's URI, as deltalake writes it.
        folder = loaded.table_uri.rstrip("/")
        assert named == {
            own,
            f"{folder}/ab/deletion_vector_{first}.bin",
            f"{folder}/deletion_vector_{second}.bin",
        }

    @pytest.mark.parametrize("where", ["root", "outside", "copy"])
    def test_it_refuses_a_delta_table_on_this_machine_with_a_vector_at_its_own_path(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        where: str,
    ) -> None:
        """Ensure a table whose log stores a vector at its own path is refused before deltalake loads it.

        deltalake reads such a vector where the log says, apart from the
        check, so the copy could hold it only by writing where the log says.
        A vector that names the copy's folder and climbs out of it would then
        have the copy write a file of the root outside every root.
        """
        outside = tmp_path_factory.mktemp("outside")
        copy, table = outside / "copy", tmp_path / "a" / "b" / "c" / "events"

        def make_copy(**_: object) -> str:
            copy.mkdir()
            return str(copy)

        monkeypatch.setattr(tempfile, "mkdtemp", make_copy)
        named = {
            "root": (tmp_path / "vector.bin").as_uri(),
            "outside": (outside / "vector.bin").as_uri(),
            "copy": f"{(copy / '0').as_uri()}/../../vector.bin",
        }
        (tmp_path / "a" / "b").mkdir(parents=True)
        for planted in (tmp_path / "vector.bin", tmp_path / "a" / "b" / "vector.bin"):
            planted.write_bytes(_vector_deleting(0))
        _with_vectors(table, {"storageType": "p", "pathOrInlineDv": named[where]})
        loaded = mocker.spy(DeltaTable, "__init__")
        target = DeltaLakeConfig(table_uri="a/b/c/events")

        with pytest.raises(ConnectorError, match=r"vector at a path of its own.*veridelta run"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()
        assert not (outside / "vector.bin").exists()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    def test_it_refuses_a_relative_deletion_vector_linked_outside(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a vector at a relative path is checked where it lies, after its links."""
        name = uuid.uuid4()
        table = tmp_path / "events"
        _with_vectors(table, {"storageType": "u", "pathOrInlineDv": _relative_vector("", name)})
        secret = tmp_path_factory.mktemp("outside") / "vector.bin"
        secret.write_bytes(b"\x01")
        (table / f"deletion_vector_{name}.bin").symlink_to(secret)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConfigError, match=r"The target deletion vector is outside"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.parametrize(
        ("vector", "reason"),
        [
            pytest.param(
                {"storageType": "x", "pathOrInlineDv": "x"}, "unknown kind", id="unknown-kind"
            ),
            pytest.param(
                {"storageType": "u", "pathOrInlineDv": _relative_vector("../", uuid.uuid4())},
                "other characters than letters and digits",
                id="folder-that-climbs",
            ),
        ],
    )
    def test_it_refuses_a_deletion_vector_it_cannot_place(
        self, tmp_path: Path, vector: Mapping[str, object], reason: str
    ) -> None:
        """Ensure a vector whose file the listing cannot name as Delta Lake does is refused.

        Delta Lake joins a relative vector's folder to the table's URI as a
        URL, so a folder of other characters than letters and digits, such as
        `../`, can lead anywhere.
        """
        _with_vectors(tmp_path / "events", vector)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match=f"could not be listed.*{reason}"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    def test_it_holds_a_deletion_vector_elsewhere_to_the_allowed_hosts(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a vector in another bucket is refused unless its host is allowed.

        The table lies in an object store, whose storage is mocked. The log
        names the host, so the refusal leaves it out.
        """
        vector = {"storageType": "p", "pathOrInlineDv": "s3://other-bucket/vector.bin"}
        _with_vectors(tmp_path / "events", vector)
        _store_at(mocker, tmp_path / "events")
        table = mocker.patch("deltalake.DeltaTable")
        table.return_value.file_uris.return_value = []
        table.return_value.protocol.return_value.reader_features = ["deletionVectors"]
        target = DeltaLakeConfig(table_uri="s3://lake/events")
        allowed = Settings((tmp_path,), allowed_hosts=("lake", "other-bucket"))

        with pytest.raises(ConfigError, match="The target deletion vector names a host") as refused:
            check_data_paths(
                Settings((tmp_path,), allowed_hosts=("lake",)), SourceConfig(path="a.csv"), target
            )
        check_data_paths(allowed, SourceConfig(path="a.csv"), target)

        assert "other-bucket" not in str(refused.value)

    @pytest.mark.skipif(sys.platform == "win32", reason="The URIs name POSIX paths.")
    @pytest.mark.parametrize("table_uri", ["fs://{root}/events", "opendal+fs://{root}/events"])
    def test_it_reads_the_log_of_a_delta_table_under_a_local_scheme_from_this_machine(
        self, tmp_path: Path, table_uri: str
    ) -> None:
        """Ensure a table with deletion vectors under deltalake's own local schemes is listed.

        deltalake's storage crashes the process as it lists such a table's
        log, so the listing reads the files of the log it checked instead.
        """
        _with_vectors(
            tmp_path / "events",
            {"storageType": "u", "pathOrInlineDv": _relative_vector("", uuid.uuid4())},
        )
        target = DeltaLakeConfig(table_uri=table_uri.format(root=tmp_path))

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.parametrize("form", ["classic", "uuid", "json"])
    @pytest.mark.parametrize("where", ["outside", "climbs", "inside"])
    def test_it_refuses_a_delta_table_whose_checkpoint_names_a_sidecar(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        form: str,
        where: str,
    ) -> None:
        """Ensure a table whose checkpoint names a sidecar is refused before deltalake reads its log.

        deltalake reads a sidecar as it loads the table, wherever it lies, so
        one outside the roots, or one whose name climbs out of the log, would
        be read unchecked. One inside the roots is refused too: a sidecar
        belongs to a v2 checkpoint, which `veridelta run` cannot read either.
        """
        outside = (tmp_path_factory.mktemp("outside") / "sidecar.parquet").as_uri()
        named = {"outside": outside, "climbs": "../../../outside/sidecar.parquet"}
        _with_a_sidecar(tmp_path / "events", named.get(where, "sidecar.parquet"), form)
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match=r"could not be listed.*checkpoint sidecar"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()

    @pytest.mark.parametrize(
        "table_uri",
        [
            pytest.param("{root}/missing/../events", id="dot-dot"),
            pytest.param("{root}/missing/%2e%2e/events", id="encoded"),
            pytest.param("{root}/missing/%2E%2E/events", id="encoded-upper"),
            pytest.param("{root}/missing/.%2e/events", id="half-encoded"),
            pytest.param("{root}/missing/.\t./events", id="tab"),
            pytest.param("{root}/./events", id="dot"),
            pytest.param("{root}/missing/..", id="last"),
            pytest.param("{localhost}/missing/../events", id="localhost"),
            pytest.param(
                "{root}/link/../events",
                id="link",
                marks=pytest.mark.skipif(
                    sys.platform == "win32",
                    reason="A link on Windows needs a privilege a runner may lack.",
                ),
            ),
            pytest.param("fs{path}/missing/../events", id="fs"),
            pytest.param("opendal+fs{path}/missing/../events", id="opendal-fs"),
        ],
    )
    def test_it_refuses_a_delta_table_uri_with_a_dot_segment(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        table_uri: str,
    ) -> None:
        """Ensure a table URI whose path holds `.` or `..`, in any spelling, is refused before it is read.

        deltalake removes such a segment by its text, so it would read the log
        of `events`, whose vector lies outside the roots, where the listing
        walks the folder the system finds: none at all, or the one a link
        leads to. The server runs in its root, so the path a URI's host
        starts reads under it.
        """
        monkeypatch.chdir(tmp_path)
        outside = (tmp_path_factory.mktemp("outside") / "vector.bin").as_uri()
        _with_vectors(tmp_path / "events", {"storageType": "p", "pathOrInlineDv": outside})
        (tmp_path / "sub" / "deeper").mkdir(parents=True)
        if "link" in table_uri:
            (tmp_path / "link").symlink_to(tmp_path / "sub" / "deeper", target_is_directory=True)
        root = tmp_path.as_uri()
        uri = table_uri.format(
            root=root, localhost=f"file://localhost{root[7:]}", path=root.removeprefix("file")
        )
        loaded = mocker.patch("deltalake.DeltaTable")

        with pytest.raises(ConfigError, match=r"The target table_uri .* '\.' or '\.\.' segment"):
            check_data_paths(
                Settings((tmp_path,)), SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri=uri)
            )

        loaded.assert_not_called()

    @pytest.mark.parametrize(
        ("table_uri", "refusal"),
        [
            pytest.param("file:///C:/root/missing/../events", "segment", id="dot-dot"),
            pytest.param("file://localhost/C:/root/./events", "segment", id="localhost"),
            pytest.param("fs:///C:/root/missing/%2e%2e/events", "segment", id="fs"),
            pytest.param("opendal+fs:///C:/root/missing/../events", "segment", id="opendal-fs"),
            pytest.param("fs:///C:/root/%2565vents", "escape", id="fs-escape"),
            pytest.param("opendal+fs:///C:/root/ev%65nts", "escape", id="opendal-fs-escape"),
        ],
    )
    def test_it_refuses_a_delta_table_uri_by_its_own_rule_on_windows(
        self,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        table_uri: str,
        refusal: str,
    ) -> None:
        """Ensure a table URI with a dot segment or a `%` escape is refused by that rule on Windows.

        Each path the URI names lies under the root, so the folder rule passes
        it on Windows as on Linux, and the rule for the URI itself refuses it.
        """
        settings = _on_a_windows_disk(monkeypatch)
        loaded = mocker.patch("deltalake.DeltaTable")
        rules = {"segment": r"'\.' or '\.\.' segment", "escape": "URI with a '%' escape"}

        with pytest.raises(ConfigError, match=f"The target table_uri .* {rules[refusal]}"):
            check_data_paths(
                settings, SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri=table_uri)
            )

        loaded.assert_not_called()

    def test_it_reads_a_delta_table_path_with_a_dot_segment_as_the_system_does(
        self, tmp_path: Path
    ) -> None:
        """Ensure a path with `..` is listed, since deltalake and the listing both read it as the system does."""
        _ROW.write_delta(tmp_path / "events")
        target = DeltaLakeConfig(table_uri="events/../events")

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.parametrize(
        "table_uri",
        [
            pytest.param("fs{path}/%2565vents", id="fs"),
            pytest.param("opendal+fs{path}/%2565vents", id="opendal-fs"),
            pytest.param("FS{path}/%2565vents", id="upper"),
            pytest.param("fs{path}/ev%65nts", id="once"),
        ],
    )
    def test_it_refuses_an_fs_delta_table_uri_with_a_percent_escape(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        table_uri: str,
    ) -> None:
        """Ensure an `fs:` or `opendal+fs:` table URI with a `%` escape is refused before it is read.

        deltalake decodes such a URI's path twice, so it would read the log of
        `events`, whose vector lies outside the roots, where the listing
        decodes it once and walks `%65vents`, a folder an agent can make.
        """
        outside = (tmp_path_factory.mktemp("outside") / "vector.bin").as_uri()
        _with_vectors(tmp_path / "events", {"storageType": "p", "pathOrInlineDv": outside})
        (tmp_path / "%65vents").mkdir()
        uri = table_uri.format(path=tmp_path.as_uri().removeprefix("file"))
        loaded = mocker.patch("deltalake.DeltaTable")

        with pytest.raises(ConfigError, match=r"The target table_uri .* URI with a '%' escape"):
            check_data_paths(
                Settings((tmp_path,)), SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri=uri)
            )

        loaded.assert_not_called()

    def test_it_reads_a_file_delta_table_uri_with_a_percent_escape(self, tmp_path: Path) -> None:
        """Ensure a `file:` table URI may hold a `%` escape, which deltalake and the listing decode once."""
        _ROW.write_delta(tmp_path / "my events")
        target = DeltaLakeConfig(table_uri=(tmp_path / "my events").as_uri())

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert "%20" in target.table_uri

    def test_it_reads_a_delta_table_in_the_home_a_user_name_names(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure deltalake reads the table in the home `~<user>` names, the one the listing checks.

        deltalake expands only `~` and `~/`, so it would read `~alice/events`
        as a folder of that name in the working directory, whose vector lies
        outside the roots, where the listing, as Polars does, reads the log in
        alice's home. Her home is a folder under the root here.
        """
        home = tmp_path / "home"
        _ROW.with_columns(kept=pl.lit(1)).write_delta(home / "events")
        outside = (tmp_path_factory.mktemp("outside") / "vector.bin").as_uri()
        named = tmp_path / "~alice" / "events"
        _with_vectors(named, {"storageType": "p", "pathOrInlineDv": outside})
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: '~alice/events'\nprimary_keys: [id]\n",
        )
        expand = os.path.expanduser

        def expanduser(path: str | os.PathLike[str]) -> str:
            text = os.fspath(path)
            return str(home) + text[6:] if text.startswith("~alice") else expand(text)

        monkeypatch.setattr(os.path, "expanduser", expanduser)

        report = describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert "kept" in report["columns"]

    def test_it_reads_a_delta_commit_whose_values_hold_a_line_separator(
        self, tmp_path: Path
    ) -> None:
        """Ensure a commit is split at a line feed alone, as Delta Lake writes it.

        A writer leaves U+2028, U+2029, and U+0085 unescaped in a commit's
        statistics, and Python's `splitlines` would split a line at each.
        Each value gets a commit of its own, whose statistics name it.
        """
        table = tmp_path / "events"
        for note in ("one\u2028two", "three\u2029four", "five\x85six"):
            pl.DataFrame({"id": [1], "note": [note]}).write_delta(table, mode="append")
        log = "".join(
            commit.read_text(encoding="utf-8") for commit in (table / "_delta_log").glob("*.json")
        )
        target = DeltaLakeConfig(table_uri="events")

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert all(character in log for character in "\u2028\u2029\x85")

    def test_it_reads_the_log_of_a_remote_delta_table_before_the_table(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a table in an object store has its log read through its store before it is loaded.

        The store is mocked: it serves the log of a table under `tmp_path`,
        whose checkpoint names a sidecar.
        """
        _with_a_sidecar(tmp_path / "events", "sidecar.parquet", "classic")
        _store_at(mocker, tmp_path / "events")
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="s3://lake/events")
        settings = Settings((tmp_path,), allowed_hosts=("lake",))

        with pytest.raises(ConnectorError, match=r"could not be listed.*checkpoint sidecar"):
            check_data_paths(settings, SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    def test_it_lists_a_delta_table_again_as_its_scan_starts(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, mocker: MockerFixture
    ) -> None:
        """Ensure a scan checks the files of the version it reads, though the first listing missed one.

        A table can gain a file between the listing a tool starts with and
        the scan, so the scan lists the table again, and reads the version it
        checked.
        """
        _ROW.write_delta(tmp_path / "events")
        (added,) = DeltaTable(str(tmp_path / "events")).file_uris()
        secret = tmp_path_factory.mktemp("outside") / "secret.parquet"
        Path(added).rename(secret)
        Path(added).symlink_to(secret)
        mocker.patch.object(
            mcp_server, "check_data_paths", side_effect=lambda _, source, target: (source, target)
        )
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: events\nprimary_keys: [id]\n",
        )

        with pytest.raises(VerideltaError, match=r"The scanned data file '.*' is outside"):
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed")

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize(
        "table_uri",
        ["events", "fs://{root}/events", "opendal+fs://{root}/events", "fs:{root}/events"],
    )
    def test_it_stops_a_delta_scan_at_a_file_that_changed_after_it_started(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        table_uri: str,
    ) -> None:
        """Ensure a data file that becomes a link out of the roots after a scan starts is never read.

        The scan checks each data file again just before it opens it, under
        each scheme Delta Lake reads from this machine.
        """
        _ROW.with_columns(note=pl.lit("public")).write_delta(tmp_path / "events")
        (added,) = DeltaTable(str(tmp_path / "events")).file_uris()
        secret = tmp_path_factory.mktemp("outside") / "secret.parquet"
        shutil.copy(added, secret)
        start = DeltaLakeConnector.connect

        def start_then_swap(connector: DeltaLakeConnector) -> None:
            start(connector)
            Path(added).unlink()
            Path(added).symlink_to(secret)

        mocker.patch.object(
            DeltaLakeConnector, "connect", autospec=True, side_effect=start_then_swap
        )
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: delta\n  table_uri: '{table_uri.format(root=tmp_path)}'\n"
            "primary_keys: [id]\n",
        )

        with pytest.raises(VerideltaError, match="about to open a file outside") as stopped:
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed")

        assert str(secret) not in str(stopped.value)

    @pytest.mark.skipif(
        Version(pl.__version__) < Version("1.40.0"),
        reason="Polars reads a table with deletion vectors from 1.40.0 on, so an older one "
        "refuses the table before any vector is read.",
    )
    def test_it_loads_a_delta_table_from_the_log_it_checked(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, mocker: MockerFixture
    ) -> None:
        """Ensure a commit written after the scan read the log is never read, nor the vector it names.

        deltalake reads the log again as it loads a table. A commit a writer
        adds in that moment, naming a vector outside the roots, would choose
        which rows the scan drops, from a file no check saw. The scan loads
        the table from the copy of the log it read instead.
        """
        table = tmp_path / "events"
        pl.DataFrame({"id": [1, 2]}).write_delta(
            table, delta_write_options={"configuration": {"delta.enableDeletionVectors": "true"}}
        )
        vector = tmp_path_factory.mktemp("outside") / "vector.bin"
        vector.write_bytes(_vector_deleting(0))
        log = table / "_delta_log"
        first = (log / f"{0:020}.json").read_text(encoding="utf-8").splitlines()
        (added,) = [json.loads(line)["add"] for line in first if '"add"' in line]
        stored: dict[str, object] = {"storageType": "p", "pathOrInlineDv": vector.as_uri()}
        stored |= {"offset": 1, "sizeInBytes": len(vector.read_bytes()) - 9, "cardinality": 1}
        removed = {"path": added["path"], "dataChange": True, "deletionTimestamp": 1}
        commit = [{"remove": removed}, {"add": dict(added, deletionVector=stored)}]
        read = delta_log_vectors

        def read_then_commit(*args: Any) -> list[dict[str, Any]]:
            vectors = read(*args)
            (log / f"{1:020}.json").write_text(
                "".join(json.dumps(action) + "\n" for action in commit), encoding="utf-8"
            )
            return vectors

        mocker.patch.object(
            mcp_server, "check_data_paths", side_effect=lambda _, source, target: (source, target)
        )
        mocker.patch.object(mcp_server, "delta_log_vectors", side_effect=read_then_commit)
        pl.DataFrame({"id": [1, 2]}).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: events\nprimary_keys: [id]\n",
        )

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["verdict"] == "match"
        assert (log / f"{1:020}.json").exists()

    @pytest.mark.parametrize("replaced", ["log file", "deletion vector"])
    def test_it_refuses_a_file_replaced_as_it_is_checked(
        self, tmp_path: Path, mocker: MockerFixture, replaced: str
    ) -> None:
        """Ensure a commit or a vector that another file replaces just after its check is never copied.

        deltalake reads the copy of the log, and of each vector at a relative
        path, that the scan makes just after each check. A file renamed over
        one in that moment is another file, though its text is the same.
        """
        table, name = tmp_path / "events", uuid.uuid4()
        _with_vectors(table, {"storageType": "u", "pathOrInlineDv": _relative_vector("", name)})
        files = {
            "log file": table / "_delta_log" / f"{1:020}.json",
            "deletion vector": table / f"deletion_vector_{name}.bin",
        }
        files["deletion vector"].write_bytes(_vector_deleting(0))
        swapped = tmp_path / "swapped"
        shutil.copyfile(files[replaced], swapped)
        check = mcp_server._refuse_listed  # pyright: ignore[reportPrivateUsage] - the hook runs it, then replaces the file

        def check_then_replace(
            settings: Settings, side: str, setting: str, location: str, *, quoted: bool
        ) -> None:
            check(settings, side, setting, location, quoted=quoted)
            if setting == replaced and Path(location) == files[replaced]:
                os.replace(swapped, files[replaced])

        mocker.patch.object(mcp_server, "_refuse_listed", side_effect=check_then_replace)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="was replaced as this server checked it"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert not swapped.exists()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Windows cannot move a file over one a handle holds open."
    )
    @pytest.mark.parametrize("replaced", ["log file", "deletion vector"])
    def test_it_refuses_a_file_swapped_in_for_its_open_alone(
        self, tmp_path: Path, mocker: MockerFixture, replaced: str
    ) -> None:
        """Ensure a file swapped in just for the copy's open, and swapped out right after, is never copied.

        The path names the file checked both before the open and after it, so
        only the file opened shows the swap.
        """
        table, name = tmp_path / "events", uuid.uuid4()
        _with_vectors(table, {"storageType": "u", "pathOrInlineDv": _relative_vector("", name)})
        files = {
            "log file": table / "_delta_log" / f"{1:020}.json",
            "deletion vector": table / f"deletion_vector_{name}.bin",
        }
        files["deletion vector"].write_bytes(_vector_deleting(0))
        swapped, aside = tmp_path / "swapped", tmp_path / "aside"
        shutil.copyfile(files[replaced], swapped)
        opened = os.open

        def swap_for_the_open(path: str | bytes, flags: int, *args: Any, **named: Any) -> int:
            if Path(os.fsdecode(path)) != files[replaced]:
                return opened(path, flags, *args, **named)
            os.replace(path, aside)
            os.replace(swapped, path)
            try:
                return opened(path, flags, *args, **named)
            finally:
                os.replace(aside, path)

        mocker.patch.object(os, "open", side_effect=swap_for_the_open)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="was replaced as this server checked it"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert not aside.exists()

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows has no named pipe in a folder.")
    def test_it_refuses_a_named_pipe_in_a_delta_log(self, tmp_path: Path) -> None:
        """Ensure a named pipe in a table's log is refused at once, not waited on.

        The copy opens each file of the log that deltalake reads, and the
        open of a pipe that no one writes to would wait, and hold every tool
        call that lists the table, forever.
        """
        _ROW.write_delta(tmp_path / "events")
        os.mkfifo(tmp_path / "events" / "_delta_log" / f"{1:020}.json")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="is not a regular file"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize("linked", ["folder", "file"])
    def test_it_writes_no_copy_through_a_link_in_its_folder(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        linked: str,
    ) -> None:
        """Ensure the copy refuses a link in its own folder, to a folder or a file outside it.

        Only the server's own user can write in the copy's folder, but the
        copy writes nothing there that leads elsewhere all the same.
        """
        _ROW.write_delta(tmp_path / "events")
        outside = tmp_path_factory.mktemp("outside")
        copy, elsewhere = outside / "copy", outside / "elsewhere"
        elsewhere.mkdir()
        commit = elsewhere / "commit.json"
        commit.write_text("kept", encoding="utf-8")

        def make_copy(**_: object) -> str:
            copy.mkdir()
            if linked == "folder":
                (copy / "0").symlink_to(elsewhere, target_is_directory=True)
            else:
                (copy / "0" / "_delta_log").mkdir(parents=True)
                (copy / "0" / "_delta_log" / f"{0:020}.json").symlink_to(commit)
            return str(copy)

        monkeypatch.setattr(tempfile, "mkdtemp", make_copy)
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="would lie outside the copy's folder"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert sorted(each.name for each in elsewhere.iterdir()) == ["commit.json"]
        assert commit.read_text(encoding="utf-8") == "kept"

    def test_it_opens_a_file_of_a_copied_table_in_its_own_folder(self, tmp_path: Path) -> None:
        """Ensure a file a table loaded from a copy names is opened in the table's folder, and one elsewhere keeps its name.

        The check then holds a file elsewhere to the roots, as it would hold
        the table's own.
        """
        _ROW.write_delta(tmp_path / "copy")
        checked = CheckedDeltaTable(DeltaTable(str(tmp_path / "copy")), tmp_path / "events")
        elsewhere = (tmp_path / "elsewhere" / "part-0.parquet").as_uri()

        own = checked.located(f"{checked.table.table_uri.rstrip('/')}/part-0.parquet")

        assert Path(own) == tmp_path / "events" / "part-0.parquet"
        assert checked.located(elsewhere) == elsewhere

    def test_it_copies_a_vector_the_log_names_twice_once(self, tmp_path: Path) -> None:
        """Ensure a vector that two actions name is copied once, since the copy never writes a file twice.

        A checkpoint names each vector its commits name, and one file can
        hold the vectors of many data files.
        """
        table, name = tmp_path / "events", uuid.uuid4()
        vector = {"storageType": "u", "pathOrInlineDv": _relative_vector("ab", name)}
        _with_vectors(table, vector, vector)
        (table / "ab").mkdir()
        (table / "ab" / f"deletion_vector_{name}.bin").write_bytes(_vector_deleting(0))
        target = DeltaLakeConfig(table_uri="events")

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    def test_it_copies_no_log_under_a_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a table on this machine is refused when the system's temporary folder lies under a root.

        Whoever writes under the roots could change the copy of the log there
        before deltalake reads it.
        """
        _ROW.write_delta(tmp_path / "events")
        (tmp_path / "temporary").mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "temporary"))
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="temporary folder lies under a folder"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()
        assert list((tmp_path / "temporary").iterdir()) == []

    def test_it_copies_no_log_into_a_folder_whose_path_holds_a_percent(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        """Ensure a table on this machine is refused, naming the temporary folder, when its path holds a `%`.

        deltalake decodes a `%` escape in the copy's path, so it would read
        another folder, and fail with a message that names neither.
        """
        _ROW.write_delta(tmp_path / "events")
        temporary = tmp_path_factory.mktemp("outside") / "a%20b"
        temporary.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(temporary))
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match="temporary folder holds a '%'"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()
        assert list(temporary.iterdir()) == []

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A '\\' separates a Windows path's folders."
    )
    def test_it_copies_no_log_into_a_folder_whose_path_holds_a_backslash(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
    ) -> None:
        r"""Ensure a table on this machine is refused when the temporary folder's path holds a `\`.

        Outside Windows, deltalake reads a `\` in a path as `/`, so it would
        read the log of another folder, one this server never wrote or
        checked, while its `table_uri` still names the copy.
        """
        _ROW.write_delta(tmp_path / "events")
        temporary = tmp_path_factory.mktemp("outside") / "a\\b"
        temporary.mkdir()
        monkeypatch.setattr(tempfile, "tempdir", str(temporary))
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match=r"temporary folder holds a '%', or a '\\'"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()
        assert list(temporary.iterdir()) == []

    def test_it_copies_only_the_files_of_a_delta_log_that_deltalake_reads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a file of the log whose name deltalake skips is checked but never copied.

        deltalake reads only a commit, a checkpoint, a checksum, and
        `_last_checkpoint`, at the top of the log. A file of any other name,
        or in a folder of the log such as the `_sidecars` of a v2 checkpoint,
        then costs the copy nothing, however large it is.
        """
        _ROW.write_delta(tmp_path / "events")
        log = tmp_path / "events" / "_delta_log"
        (log / "notes").write_bytes(bytes(2 << 20))
        (log / "_sidecars").mkdir()
        (log / "_sidecars" / f"{1:020}.json").write_bytes(bytes(2 << 20))
        monkeypatch.setattr(mcp_server, "_DELTA_LOG_COPY_LIMIT", 1 << 20)
        copied = mocker.spy(mcp_server, "_copy_checked")
        listed = mocker.spy(mcp_server, "_refuse_listed")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: events\nprimary_keys: [id]\n",
        )

        report = describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert "note" in report["columns"]
        assert {Path(call.args[0]).name for call in copied.call_args_list} == {f"{0:020}.json"}
        assert {Path(call.args[3]) for call in listed.call_args_list} >= {
            log / "notes",
            log / "_sidecars" / f"{1:020}.json",
        }

    @pytest.mark.parametrize("version", [None, 4, 2, 0], ids=["latest", "last", "middle", "first"])
    def test_it_copies_a_delta_log_up_to_the_version_deltalake_loads(
        self, tmp_path: Path, mocker: MockerFixture, version: int | None
    ) -> None:
        """Ensure the copy holds each file of the log from version 0 to the version loaded, and no later one.

        deltalake loads a version from files at or before it, so a later
        commit or checkpoint costs the copy nothing, while every earlier one
        is there should deltalake pass over a checkpoint. The table loads from
        the copy, though `_last_checkpoint` names a checkpoint it lacks.
        """
        table = tmp_path / "events"
        for at in range(6):
            _ROW.write_delta(table, mode="append")
            if at in (1, 3):
                DeltaTable(str(table)).create_checkpoint()
        copied = mocker.spy(mcp_server, "_copy_checked")
        target = DeltaLakeConfig(table_uri="events", version=version)

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        names = sorted(Path(call.args[0]).name for call in copied.call_args_list)
        last = 5 if version is None else version
        expected = [
            name
            for name in sorted(each.name for each in (table / "_delta_log").iterdir())
            if name == "_last_checkpoint" or int(name[:20]) <= last
        ]
        assert names == expected
        assert f"{last:020}.json" in names
        assert f"{last + 1:020}.json" not in names

    @pytest.mark.parametrize(
        "name",
        [
            f"{2:020}.checkpoint.json",
            f"{2:020}.checkpoint.{1:010}.{1:010}.json",
            f"{2:020}.checkpoint.{1:010}.{1:010}.{uuid.UUID(int=2)}.parquet",
            f"{2:020}.checkpoint.{uuid.UUID(int=2)}.json",
        ],
        ids=["json", "part-json", "part-uuid", "uuid-json"],
    )
    def test_it_reads_the_rows_of_a_delta_table_beside_a_checkpoint_deltalake_passes_over(
        self, tmp_path: Path, name: str
    ) -> None:
        """Ensure an empty file named as a checkpoint leaves the rows the server reads as `veridelta run` reads them.

        deltalake reads no such file as a checkpoint, or passes over one it
        cannot read, to the commits before it. A copy that started at the
        checkpoint would lack those commits, and read only the rows of the
        commits after it, since a later commit that writes the protocol and
        the metadata again, as a change of a table property does, loads alone.
        """
        table = tmp_path / "events"
        pl.DataFrame({"id": [1, 2, 3]}).write_delta(table)
        pl.DataFrame({"id": [4]}).write_delta(table, mode="append")
        DeltaTable(str(table)).alter.set_table_properties({"delta.enableChangeDataFeed": "true"})
        pl.DataFrame({"id": [6]}).write_delta(table, mode="append")
        (table / "_delta_log" / name).write_bytes(b"")
        pl.DataFrame({"id": [1, 2, 3, 4, 6]}).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: events\nprimary_keys: [id]\n",
        )

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert sorted(pl.read_delta(str(table))["id"].to_list()) == [1, 2, 3, 4, 6]
        assert report["verdict"] == "match"
        assert report["total_rows_target"] == 5

    @pytest.mark.parametrize("limit", ["bytes", "files", "room"])
    def test_it_refuses_a_delta_log_larger_than_it_copies(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, limit: str
    ) -> None:
        """Ensure a log that would take more files or room than the copy holds is refused, not copied whole.

        Files under a root that take no room there, such as sparse files or
        hard links named as commits to one empty file, would otherwise fill
        the system's temporary folder: each copy is a file of its own, and
        takes a block even when empty. So each file counts a block and its
        bytes, and the files are counted too.
        """
        _ROW.write_delta(tmp_path / "events")
        log = tmp_path / "events" / "_delta_log"
        if limit == "bytes":
            (log / f"{1:020}.json").write_bytes(bytes(2 << 20))
            monkeypatch.setattr(mcp_server, "_DELTA_LOG_COPY_LIMIT", 1 << 20)
        else:
            (tmp_path / "empty").touch()
            for version in range(1, 61):
                os.link(tmp_path / "empty", log / f"{version:020}.json")
            if limit == "files":
                monkeypatch.setattr(mcp_server, "_DELTA_LOG_COPY_FILES", 40)
            else:
                # The bytes of all 61 files come to less than one block.
                monkeypatch.setattr(mcp_server, "_DELTA_LOG_COPY_LIMIT", 40 * 4096)
        copied = mocker.spy(mcp_server, "_copy_checked")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(
            ConnectorError, match=r"pass the 10,000 files and folders, or the 1 GiB"
        ):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert copied.call_count <= 41

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A '\\' separates a Windows path's folders."
    )
    @pytest.mark.parametrize("kind", ["file", "folder"])
    def test_it_refuses_a_name_in_a_delta_log_that_holds_a_backslash(
        self, tmp_path: Path, mocker: MockerFixture, kind: str
    ) -> None:
        r"""Ensure a table whose log holds a name with a `\` is refused before deltalake loads it.

        Outside Windows, deltalake reads a `\` in a name it lists as `/`, and
        resolves a `..`, so such a name leads it to a commit elsewhere, which
        no check saw.
        """
        _ROW.write_delta(tmp_path / "events")
        name = tmp_path / "events" / "_delta_log" / f"x\\..\\..\\zz\\_delta_log\\{1:020}.json"
        if kind == "file":
            name.write_text("{}\n", encoding="utf-8")
        else:
            name.mkdir()
        loaded = mocker.patch("deltalake.DeltaTable")
        target = DeltaLakeConfig(table_uri="events")

        with pytest.raises(ConnectorError, match=r"a name in the table's log holds a '\\'"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        loaded.assert_not_called()

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows allows no ':' in a folder's name.")
    def test_it_refuses_a_delta_table_uri_whose_readings_each_hold_a_log(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a URI that readers read as two folders, each holding a log, is refused.

        deltalake reads `file:///…/events` as the folder it names, and a
        reader that knows no scheme as a folder named `file:` in the working
        directory. The copy holds one log, so the server reads neither.
        """
        _ROW.write_delta(tmp_path / "events")
        uri = (tmp_path / "events").as_uri()
        shutil.copytree(tmp_path / "events", Path(uri).absolute())
        loaded = mocker.patch("deltalake.DeltaTable")

        with pytest.raises(ConnectorError, match="more than one holds a Delta Lake log"):
            check_data_paths(
                Settings((tmp_path,)), SourceConfig(path="a.csv"), DeltaLakeConfig(table_uri=uri)
            )

        loaded.assert_not_called()

    @pytest.mark.parametrize("listed", [True, False], ids=["listing", "scan"])
    def test_it_holds_a_file_iceberg_metadata_names_elsewhere_to_the_allowed_hosts(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        listed: bool,
    ) -> None:
        """Ensure a table in an allowed bucket cannot lead a scan to a file in another bucket.

        Its metadata names a manifest list in a bucket the server does not
        allow. The listing refuses it, and so does the scan, which checks each
        file's host just before it opens it, though the listing found nothing.
        The metadata names the host, so the refusal leaves it out.
        """
        store = tmp_path_factory.mktemp("store")
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        with _object_store(store, monkeypatch) as options:
            metadata = _remote_iceberg(tmp_path, store, options, monkeypatch)
            written = store / metadata.removeprefix("s3://")
            document = json.loads(written.read_text(encoding="utf-8"))
            for snapshot in document["snapshots"]:
                snapshot["manifest-list"] = "s3://other-bucket/list.avro"
            written.write_text(json.dumps(document), encoding="utf-8")
            if not listed:
                mocker.patch.object(mcp_server, "_table_files", return_value=iter(()))
            _write(
                tmp_path,
                "source:\n  path: a.parquet\n  format: parquet\n"
                f"target:\n  type: iceberg\n  table_uri: '{metadata}'\n  storage_options:\n"
                + "".join(f"    {key}: '{value}'\n" for key, value in options.items())
                + "primary_keys: [id]\n",
            )

            with pytest.raises(VerideltaError, match="names a host this server may not") as refused:
                read_rows(
                    Settings((tmp_path,), allow_row_values=True, allowed_hosts=_STORE_HOSTS),
                    "veridelta.yaml",
                    "changed",
                )

        assert "other-bucket" not in str(refused.value)

    def test_it_accepts_a_remote_iceberg_table(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a table whose files all lie in an object store passes."""
        store = tmp_path_factory.mktemp("store")
        with _object_store(store, monkeypatch) as options:
            metadata = _remote_iceberg(tmp_path, store, options, monkeypatch)
            target = IcebergConfig(table_uri=metadata, storage_options=options)
            settings = Settings((tmp_path,), allowed_hosts=_STORE_HOSTS)

            check_data_paths(settings, SourceConfig(path="a.csv"), target)

    def _azure_configuration(self, root: Path, table_uri: str) -> None:
        """Write a configuration that compares `_ROW` with the Azure table at `table_uri`."""
        _ROW.write_parquet(root / "a.parquet")
        _write(
            root,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{table_uri}'\n  storage_options:\n"
            + "".join(f"    {key}: '{value}'\n" for key, value in _AZURE_OPTIONS.items())
            + "primary_keys: [id]\n",
        )

    def test_it_reads_an_azure_table_whose_files_name_their_account(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a table Spark wrote in Azure is listed and read, each file in its container."""
        store = tmp_path_factory.mktemp("store")
        self._azure_configuration(tmp_path, _spark_azure_iceberg(tmp_path, store, monkeypatch))

        report = read_rows(
            Settings((tmp_path,), allow_row_values=True, allowed_hosts=_AZURE_HOSTS),
            "veridelta.yaml",
            "changed",
        )

        assert report["total"] == 0

    @pytest.mark.parametrize(
        ("authority", "refusal"),
        [
            ("other@acct.dfs.core.windows.net", "names a host this server may not"),
            ("lake@acct.blob.core.windows.net", "names a host this server may not"),
            ("lake:443@acct.dfs.core.windows.net", "does not name its host in the one shape"),
        ],
        ids=["container", "host", "container-port"],
    )
    @pytest.mark.parametrize("listed", [True, False], ids=["listing", "scan"])
    def test_it_holds_the_container_and_the_host_of_an_azure_file_to_the_allowed_hosts(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        monkeypatch: pytest.MonkeyPatch,
        authority: str,
        refusal: str,
        listed: bool,
    ) -> None:
        """Ensure a file named `container@host` needs both its container and its host allowed.

        The metadata names its manifest list in another container, or at the
        account's other host, which the server does not allow, or in a
        container with a port, which the FileIO would open as part of the
        container's name, so the server checks it whole. The listing
        refuses it, and so does the scan, though the listing found nothing.
        The metadata names the host, so the refusal leaves it out.
        """
        store = tmp_path_factory.mktemp("store")
        table_uri = _spark_azure_iceberg(tmp_path, store, monkeypatch)
        written = store / table_uri.removeprefix("abfs://")
        document = json.loads(written.read_text(encoding="utf-8"))
        for snapshot in document["snapshots"]:
            listed_at = snapshot["manifest-list"]
            snapshot["manifest-list"] = listed_at.replace(
                "lake@acct.dfs.core.windows.net", authority
            )
        written.write_text(json.dumps(document), encoding="utf-8")
        if not listed:
            mocker.patch.object(mcp_server, "_table_files", return_value=iter(()))
        self._azure_configuration(tmp_path, table_uri)

        with pytest.raises(VerideltaError, match=refusal) as refused:
            read_rows(
                Settings((tmp_path,), allow_row_values=True, allowed_hosts=_AZURE_HOSTS),
                "veridelta.yaml",
                "changed",
            )

        assert authority not in str(refused.value)

    @pytest.mark.parametrize(
        "token",
        [
            "sv=2024-01-01&sig=c2lnbmF0dXJl%2BZm9y%2FdGVzdA%3D%3D",
            "?sv=2024-01-01&sig=c2lnbmF0dXJl%2BZm9y%2FdGVzdA%3D%3D",
            "sig=c2lnbmF0dXJl%2BZm9y%2FdGVzdA%3D%3D&sv=2024-01-01&se=2030-12-31",
        ],
        ids=["bare", "with-mark", "reordered"],
    )
    @pytest.mark.parametrize("folder", ["t", "it's"], ids=["plain", "quote-in-path"])
    @pytest.mark.parametrize("listed", [True, False], ids=["listing", "scan"])
    @pytest.mark.parametrize(
        ("tool", "arguments"),
        [
            ("validate_config", {"schemas": True}),
            ("run_comparison", {}),
            ("describe_schema", {"side": "target"}),
        ],
        ids=["validate_config", "run_comparison", "describe_schema"],
    )
    def test_it_returns_no_part_of_an_azure_sas_token_from_the_environment(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        token: str,
        folder: str,
        listed: bool,
        tool: str,
        arguments: dict[str, object],
    ) -> None:
        """Ensure an Azure error that quotes a request's URL comes back without its query.

        The Azure SDK writes a SAS token's parameters back sorted, so the URL
        PyArrow quotes in its error does not hold the variable's value that
        the server masks. The mock fails each read with such a URL. The listing fails, or, with no listing, the scan, which
        `validate_config` reports as a finding. The Azure SDK encodes a quote
        in a file's path as written, so the URL's path can hold one.
        """
        monkeypatch.setenv("VD_AZURE_SAS", token)

        def refusing(**options: str) -> MagicMock:
            query = urlencode(sorted(parse_qsl(options["sas_token"].removeprefix("?"))))

            def refuse(path: str, *_: object) -> None:
                raise OSError(
                    f"GetProperties failed for 'https://acct.blob.core.windows.net/{path}?{query}'"
                    ". Azure Error: [AuthenticationFailed] 403 Forbidden"
                )

            return MagicMock(
                open_input_file=MagicMock(side_effect=refuse),
                open_input_stream=MagicMock(side_effect=refuse),
            )

        monkeypatch.setitem(arrow_fs.__dict__, "AzureFileSystem", refusing)
        if not listed:
            mocker.patch.object(mcp_server, "_table_files", return_value=iter(()))
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\ntarget:\n  type: iceberg\n"
            f"  table_uri: abfs://lake/{folder}/metadata/v1.metadata.json\n  storage_options:\n"
            "    adls.account-name: acct\n    adls.sas-token: ${VD_AZURE_SAS}\n"
            "primary_keys: [id]\n",
        )

        result = _call(
            Settings((tmp_path,), allow_row_values=True, allowed_hosts=_AZURE_HOSTS),
            {"path": "veridelta.yaml", **arguments},
            tool=tool,
        )

        shown = json.dumps(result.structured_content) + _text(result)
        # The URL ends at its path, where the query would follow.
        assert f"/lake/{folder}/metadata/v1.metadata.json'" in shown
        for value in ("c2lnbmF0dXJl", "2024-01-01", "2030-12-31"):
            assert value not in shown

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param(
                Path.as_uri,
                id="file-uri",
                marks=pytest.mark.skipif(
                    sys.platform == "win32",
                    reason="PyIceberg writes no file at a Windows file URI, such as file:///C:/x.",
                ),
            ),
            pytest.param(
                lambda folder: "file:" + str(folder).lstrip("/"),
                id="file-uri-without-a-slash",
                marks=pytest.mark.skipif(
                    sys.platform == "win32", reason="The URI names a POSIX path."
                ),
            ),
        ],
    )
    def test_it_returns_no_row_of_a_local_file_a_remote_table_names(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        uri: Callable[[Path], str],
    ) -> None:
        """Ensure a table in an object store cannot read a file on this machine outside the roots.

        Its metadata and manifests lie in the store, and a file URI names its
        data file, outside the roots. Without a slash after `file:`, Polars
        reads that path from `/`, and PyIceberg from the working directory. A
        tool refuses it before the scan opens a file.
        """
        store, outside = tmp_path_factory.mktemp("store"), tmp_path_factory.mktemp("outside")
        _ROW.with_columns(note=pl.lit("public")).write_parquet(tmp_path / "a.parquet")
        with _object_store(store, monkeypatch) as options:
            # PyIceberg writes the data file from `/`, where Polars reads it.
            monkeypatch.chdir(tmp_path.anchor)
            metadata = _remote_iceberg(
                tmp_path, store, options, monkeypatch, {"write.data.path": uri(outside)}
            )
            monkeypatch.chdir(tmp_path)
            _write(
                tmp_path,
                "source:\n  path: a.parquet\n  format: parquet\n"
                f"target:\n  type: iceberg\n  table_uri: '{metadata}'\n  storage_options:\n"
                + "".join(f"    {key}: '{value}'\n" for key, value in options.items())
                + "primary_keys: [id]\n",
            )

            with pytest.raises(ConfigError, match="The target data file") as refused:
                read_rows(
                    Settings((tmp_path,), allow_row_values=True, allowed_hosts=_STORE_HOSTS),
                    "veridelta.yaml",
                    "changed",
                )

        assert _SECRET not in str(refused.value)

    def test_it_reads_a_table_with_storage_options_alone(self, tmp_path: Path) -> None:
        """Ensure a table's own properties never say how the scan reads its files.

        Metadata that sends its files to a store, or names a FileIO class,
        would make PyIceberg read other files than the listing names, if the
        scan built its FileIO from them.
        """
        metadata = Path(_iceberg(tmp_path))
        written = json.loads(metadata.read_text())
        written["properties"].update(
            {
                "DEFAULT_SCHEME": "s3",
                "s3.endpoint": "http://127.0.0.1:9",
                "py-io-impl": "veridelta.nowhere.FileIO",
            }
        )
        metadata.write_text(json.dumps(written))
        _ROW.with_columns(note=pl.lit("public")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{metadata}'\nprimary_keys: [id]\n",
        )

        report = run_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["changed_count"] == 1

    def test_it_checks_each_path_before_it_lists_a_table(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure no table is listed, which can reach a store, while a side names a path outside."""
        listing = mocker.patch.object(mcp_server, "_table_files")
        source = IcebergConfig(table_uri="lake/events.metadata.json")
        outside = SourceConfig(path=str(tmp_path.parent / "a.csv"))

        with pytest.raises(ConfigError, match="The target path"):
            check_data_paths(Settings((tmp_path,)), source, outside)

        listing.assert_not_called()

    @pytest.mark.parametrize(
        "hint",
        [
            pytest.param(None, id="metadata-file"),
            pytest.param("{name}.metadata.json", id="hint-file-name"),
            pytest.param("{name}", id="hint-name"),
            pytest.param("7", id="hint-version"),
        ],
    )
    def test_it_accepts_an_iceberg_table_under_a_root(
        self, tmp_path: Path, hint: str | None
    ) -> None:
        """Ensure a table whose files all lie under a root passes, by its metadata file or folder.

        A folder's version hint names the metadata file in full, by name, or by
        version number, and the check finds it where PyIceberg does.
        """
        metadata = Path(_iceberg(tmp_path))
        shutil.copy(metadata, metadata.parent / "v7.metadata.json")
        table_uri = str(metadata)
        if hint is not None:
            name = metadata.name.removesuffix(".metadata.json")
            (metadata.parent / "version-hint.text").write_text(hint.format(name=name))
            table_uri = str(metadata.parent.parent)

        check_data_paths(
            Settings((tmp_path,)), SourceConfig(path="a.csv"), IcebergConfig(table_uri=table_uri)
        )

    @pytest.mark.parametrize("layer", ["metadata file", "manifest list", "manifest", "data file"])
    def test_it_refuses_an_iceberg_file_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, layer: str
    ) -> None:
        """Ensure each file the scan reads is held to the roots, before it is opened.

        A file the metadata names is a value from the table, as a partition's
        folder is, so the message quotes it only on a server that allows row
        values.
        """
        outside = tmp_path_factory.mktemp("outside")
        target = IcebergConfig(table_uri=_one_layer_outside(tmp_path, outside, layer))
        refusals: list[str] = []

        for allowed in (False, True):
            with pytest.raises(
                ConfigError, match="outside the folders this server reads from"
            ) as refused:
                check_data_paths(
                    Settings((tmp_path,), allow_row_values=allowed),
                    SourceConfig(path="a.csv"),
                    target,
                )
            refusals.append(str(refused.value))

        setting = "metadata file that '" if layer == "metadata file" else f"{layer} "
        assert refusals[0].startswith(f"The target {setting}")
        assert str(outside) not in refusals[0]
        assert str(outside) in refusals[1]

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    def test_it_refuses_a_version_hint_linked_outside(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a version hint that links out of a root is refused before it is read."""
        metadata = Path(_iceberg(tmp_path))
        secret = tmp_path_factory.mktemp("outside") / "hint"
        secret.write_text(_SECRET)
        (metadata.parent / "version-hint.text").symlink_to(secret)
        target = IcebergConfig(table_uri=str(metadata.parent.parent))

        with pytest.raises(ConfigError, match=r"The target version hint .* is outside") as refused:
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert _SECRET not in str(refused.value)

    def test_it_leaves_the_version_hint_out_of_an_error(self, tmp_path: Path) -> None:
        """Ensure the text of a version hint that names no metadata file is not quoted."""
        metadata = Path(_iceberg(tmp_path))
        hint = "text-from-the-version-hint"
        (metadata.parent / "version-hint.text").write_text(hint, encoding="utf-8")
        target = IcebergConfig(table_uri=str(metadata.parent.parent))

        with pytest.raises(ConnectorError, match="names could not be read") as refused:
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

        assert hint not in str(refused.value)

    def test_it_checks_the_snapshot_a_side_pins(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a file added after the pinned snapshot is no reason to refuse it."""
        secret = tmp_path_factory.mktemp("outside") / "secret.parquet"
        _ROW.write_parquet(secret)
        with _catalog(tmp_path) as catalog:
            table = catalog.create_table("lake.events", schema=_ROW.to_arrow().schema)
            table.append(_ROW.to_arrow())
            first = table.current_snapshot()
            assert first is not None
            table.add_files([str(secret)])
            metadata = catalog.load_table("lake.events").metadata_location
        settings = Settings((tmp_path,), allow_row_values=True)

        pinned = IcebergConfig(table_uri=metadata, snapshot_id=first.snapshot_id)
        check_data_paths(settings, SourceConfig(path="a.csv"), pinned)
        with pytest.raises(
            ConfigError, match=re.escape(f"The target data file '{secret}' is outside")
        ):
            check_data_paths(
                settings, SourceConfig(path="a.csv"), IcebergConfig(table_uri=metadata)
            )

    @pytest.mark.parametrize(
        ("appends", "snapshot_id"),
        [pytest.param(0, None, id="empty"), pytest.param(1, 1, id="unknown-snapshot")],
    )
    def test_it_lists_nothing_without_a_snapshot(
        self, tmp_path: Path, appends: int, snapshot_id: int | None
    ) -> None:
        """Ensure a table with no snapshot to read passes, and the reader then says why."""
        target = IcebergConfig(
            table_uri=_iceberg(tmp_path, appends=appends), snapshot_id=snapshot_id
        )

        check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    def test_it_holds_a_remote_file_to_the_allowed_hosts(self, tmp_path: Path) -> None:
        """Ensure a file on another machine is held to the allowed hosts, not to the roots.

        The metadata names the host, so the refusal leaves it out.
        """
        metadata = json.loads(Path(_iceberg(tmp_path)).read_text())
        for snapshot in metadata["snapshots"]:
            snapshot["manifest-list"] = "https://example.invalid/snap.avro"
        copy = tmp_path / "v1.metadata.json"
        copy.write_text(json.dumps(metadata))
        target = IcebergConfig(table_uri=str(copy))

        with pytest.raises(ConfigError, match="The target manifest list names a host") as refused:
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)
        # The host and the folder rules let the manifest list through, and this FileIO
        # opens no web address.
        with pytest.raises(ConnectorError, match="could not be listed"):
            check_data_paths(
                Settings((tmp_path,), allowed_hosts=("example.invalid",)),
                SourceConfig(path="a.csv"),
                target,
            )

        assert "example.invalid" not in str(refused.value)

    @pytest.mark.parametrize("scheme", ["hdfs", "viewfs"])
    @pytest.mark.parametrize("listed", [True, False], ids=["listing", "scan"])
    def test_it_opens_no_file_through_libhdfs(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        scheme: str,
        listed: bool,
    ) -> None:
        """Ensure neither the listing nor the scan opens a file through libhdfs.

        PyArrow opens an `hdfs` or `viewfs` file through libhdfs, which it can
        load from the working directory, the first root, where an agent can
        write one. The table lies under the root, and the server allows the
        host its manifest list names, so only the FileIO stands between the
        manifest list and that library.
        """
        monkeypatch.chdir(tmp_path)
        (tmp_path / "libhdfs.so").write_bytes(b"code an agent wrote")
        loads: list[str] = []

        def load(_io: PyArrowFileIO, scheme: str, _netloc: str | None) -> None:
            loads.append(scheme)
            raise OSError("libhdfs was loaded")

        monkeypatch.setattr(PyArrowFileIO, "_initialize_hdfs_fs", load)
        metadata = Path(_iceberg(tmp_path))
        written = json.loads(metadata.read_text())
        for snapshot in written["snapshots"]:
            snapshot["manifest-list"] = f"{scheme}://namenode/lake/list.avro"
        metadata.write_text(json.dumps(written))
        if not listed:
            mocker.patch.object(mcp_server, "_table_files", return_value=iter(()))
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{metadata}'\nprimary_keys: [id]\n",
        )

        settings = Settings((tmp_path,), allow_row_values=True, allowed_hosts=("namenode",))

        with pytest.raises(ConnectorError, match="through a scheme other than file: or an object"):
            read_rows(settings, "veridelta.yaml", "changed")

        assert loads == []

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param(DeltaLakeConfig(table_uri="missing"), id="delta"),
            pytest.param(IcebergConfig(table_uri="lake/missing.metadata.json"), id="iceberg"),
        ],
    )
    def test_it_refuses_a_table_it_cannot_list(
        self, tmp_path: Path, target: DeltaLakeConfig | IcebergConfig
    ) -> None:
        """Ensure a table whose files cannot be listed is refused, naming the table."""
        with pytest.raises(
            ConnectorError,
            match=rf"The files of the target .* table '{re.escape(target.table_uri)}' could not be listed",
        ):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    @pytest.mark.parametrize(
        ("module", "target", "extra"),
        [
            pytest.param("deltalake", DeltaLakeConfig(table_uri="events"), "delta", id="delta"),
            pytest.param(
                "pyiceberg.io.pyarrow",
                IcebergConfig(table_uri="lake/events.metadata.json"),
                "iceberg",
                id="iceberg",
            ),
        ],
    )
    def test_it_names_the_missing_extra(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        module: str,
        target: DeltaLakeConfig | IcebergConfig,
        extra: str,
    ) -> None:
        """Ensure a table read without its extra is refused with the command that installs it."""
        monkeypatch.setitem(sys.modules, module, None)

        with pytest.raises(ConnectorError, match=rf"uv add 'veridelta\[{extra}\]'"):
            check_data_paths(Settings((tmp_path,)), SourceConfig(path="a.csv"), target)

    def test_it_returns_no_row_of_a_data_file_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, mocker: MockerFixture
    ) -> None:
        """Ensure a tool reads no row of a file the table's metadata names outside the roots."""
        metadata = _one_layer_outside(tmp_path, tmp_path_factory.mktemp("outside"), "data file")
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{metadata}'\nprimary_keys: [id]\n",
        )
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="The target data file") as refused:
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed")

        assert _SECRET not in str(refused.value)
        compare.assert_not_called()

    @pytest.mark.skipif(sys.platform == "win32", reason="The URI names a POSIX path.")
    @pytest.mark.parametrize(
        ("target", "setting"),
        [
            pytest.param(
                "  type: iceberg\n  table_uri: '{metadata}'\n", "data file", id="iceberg-data-file"
            ),
            pytest.param("  path: '{secret}'\n  format: parquet\n", "path", id="path"),
        ],
    )
    def test_it_returns_no_row_of_a_file_uri_without_a_slash(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        target: str,
        setting: str,
    ) -> None:
        """Ensure a file named `file:` and a path without its first `/` is held to the roots.

        Polars reads that path from `/`, outside the roots, and PyIceberg from
        the working directory, inside one. Nothing under the root names the
        file but the table's manifest, or the configuration.
        """
        outside = tmp_path_factory.mktemp("outside")
        monkeypatch.chdir(tmp_path.anchor)  # PyIceberg writes the data file where Polars reads it.
        metadata = _iceberg(tmp_path, {"write.data.path": "file:" + str(outside).lstrip("/")})
        monkeypatch.chdir(tmp_path)
        (secret,) = outside.iterdir()
        _ROW.with_columns(note=pl.lit("public")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\ntarget:\n"
            + target.format(metadata=metadata, secret="file:" + str(secret).lstrip("/"))
            + "primary_keys: [id]\n",
        )

        with pytest.raises(ConfigError, match=f"The target {setting} '/") as refused:
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed")

        assert _SECRET not in str(refused.value)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    @pytest.mark.parametrize(
        "remote", [pytest.param(False, id="local"), pytest.param(True, id="remote")]
    )
    def test_it_returns_no_row_of_a_data_file_named_by_a_store_scheme_without_slashes(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        remote: bool,
    ) -> None:
        """Ensure a data file named `s3:` and a path, with no `//`, is held to the roots.

        PyIceberg writes `s3:link/x.parquet` to the store, where a link leads
        outside the roots. Polars fetches from a store only after `//`, so it
        opens that path from the working directory, the root, through a link
        of the same name there. The table itself lies under the root, or in the
        store.
        """
        store, outside = tmp_path_factory.mktemp("store"), tmp_path_factory.mktemp("outside")
        (store / "link").symlink_to(outside, target_is_directory=True)
        (tmp_path / "s3:link").symlink_to(outside, target_is_directory=True)
        monkeypatch.chdir(tmp_path)
        properties = {"write.data.path": "s3:link"}
        _ROW.with_columns(note=pl.lit("public")).write_parquet(tmp_path / "a.parquet")
        with _object_store(store, monkeypatch) as options:
            if remote:
                metadata = _remote_iceberg(tmp_path, store, options, monkeypatch, properties)
            else:
                monkeypatch.setattr(
                    PyArrowFileIO,
                    "_initialize_s3_fs",
                    lambda _io, _netloc: SubTreeFileSystem(str(store), LocalFileSystem()),
                )
                metadata, options = _iceberg(tmp_path, properties), {"s3.region": "us-east-1"}
            _write(
                tmp_path,
                "source:\n  path: a.parquet\n  format: parquet\n"
                f"target:\n  type: iceberg\n  table_uri: '{metadata}'\n  storage_options:\n"
                + "".join(f"    {key}: '{value}'\n" for key, value in options.items())
                + "primary_keys: [id]\n",
            )
            assert list(outside.glob("*.parquet"))

            with pytest.raises(ConfigError, match="The target data file 's3:link/") as refused:
                read_rows(
                    Settings((tmp_path,), allow_row_values=True, allowed_hosts=_STORE_HOSTS),
                    "veridelta.yaml",
                    "changed",
                )

        assert _SECRET not in str(refused.value)

    @pytest.mark.skipif(
        sys.platform == "win32", reason="A link on Windows needs a privilege a runner may lack."
    )
    def test_it_refuses_a_data_file_named_through_the_parent_of_a_link(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a data file is held to the roots where PyIceberg reads it, as well as Polars.

        A scan through PyIceberg folds `..` before it follows the link, so it
        reads a file outside the roots, where the system reads one under them.
        """
        secret = tmp_path_factory.mktemp("outside") / "secret.parquet"
        _ROW.write_parquet(secret)
        (tmp_path / "deep" / "er").mkdir(parents=True)
        (tmp_path / "link").symlink_to(tmp_path / "deep" / "er", target_is_directory=True)
        with _catalog(tmp_path) as catalog:
            table = catalog.create_table("lake.events", schema=_ROW.to_arrow().schema)
            table.add_files([f"{tmp_path}/link/../../{secret.parent.name}/{secret.name}"])
            metadata = catalog.load_table("lake.events").metadata_location

        with pytest.raises(ConfigError, match=r"The target data file '.*/link/\.\./\.\./"):
            check_data_paths(
                Settings((tmp_path,), allow_row_values=True),
                SourceConfig(path="a.csv"),
                IcebergConfig(table_uri=metadata),
            )

    @pytest.mark.skipif(sys.platform == "win32", reason="The locations name POSIX paths.")
    @pytest.mark.parametrize(
        "layer",
        ["metadata file", "manifest list", "manifest list without a slash", "manifest"],
    )
    @pytest.mark.parametrize("planner", ["", "plugin"], ids=["default-planner", "plugin-required"])
    def test_it_scans_the_files_the_listing_reads(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        layer: str,
        planner: str,
    ) -> None:
        """Ensure a scan opens the metadata files that the listing opened, however they are named.

        The `polars_iceberg` plugin would open a forged file that the listing
        never read, which leads to a data file outside the roots. Polars plans
        a scan with that plugin whenever it is installed, and only with it when
        `POLARS_ICEBERG_PLANNER` says so.
        """
        monkeypatch.setenv("POLARS_ICEBERG_PLANNER", planner)
        monkeypatch.chdir(tmp_path)
        metadata = _read_two_ways(tmp_path, tmp_path_factory.mktemp("outside"), layer)
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{metadata}'\nprimary_keys: [id]\n",
        )

        report = read_rows(
            Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed"
        )

        assert [row["note_target"] for row in report["rows"]] == ["public"]

    @pytest.mark.parametrize("layer", ["metadata file", "manifest list", "manifest", "data file"])
    def test_it_stops_a_scan_at_a_file_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        layer: str,
    ) -> None:
        """Ensure a scan opens no file outside the roots, though the listing found none.

        A file can change between the listing and the scan, so the scan checks
        each file on this machine again, just before it opens it.
        """
        outside = tmp_path_factory.mktemp("outside")
        metadata = _one_layer_outside(tmp_path, outside, layer)
        mocker.patch.object(mcp_server, "_table_files", return_value=iter(()))
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{metadata}'\nprimary_keys: [id]\n",
        )

        with pytest.raises(VerideltaError, match="about to open a file outside") as stopped:
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed")

        assert _SECRET not in str(stopped.value)
        assert str(outside) not in str(stopped.value)

    @pytest.mark.skipif(sys.platform == "win32", reason="The manifest list names a POSIX path.")
    def test_it_reads_no_metadata_file_an_earlier_scan_left_in_polars_cache(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a scan reads each metadata file afresh, never as Polars cached it.

        Polars keeps the bytes of each manifest list and manifest it reads, for
        every later scan in the process. A scan from outside the server caches
        a manifest list that leads outside the roots, which is then replaced.
        The listing reads the new one, and so must the scan.
        """
        outside = tmp_path_factory.mktemp("outside")
        with _catalog(tmp_path) as catalog:
            for name, note, properties in (
                ("events", "public", {}),
                ("forged", _SECRET, {"write.data.path": str(outside)}),
            ):
                table = catalog.create_table(
                    f"lake.{name}", schema=_ROW.to_arrow().schema, properties=properties
                )
                table.append(_ROW.with_columns(note=pl.lit(note)).to_arrow())
            events, forged = catalog.load_table("lake.events"), catalog.load_table("lake.forged")
        listed = Path(events.current_snapshot().manifest_list)
        written = listed.read_bytes()
        shutil.copy(forged.current_snapshot().manifest_list, listed)
        command_line = IcebergConnector(IcebergConfig(table_uri=str(events.metadata_location)))
        command_line.connect()
        assert command_line.lazyframe().collect()["note"].to_list() == [_SECRET]
        listed.write_bytes(written)
        _ROW.with_columns(note=pl.lit("source")).write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: iceberg\n  table_uri: '{events.metadata_location}'\n"
            "primary_keys: [id]\n",
        )

        report = read_rows(
            Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed"
        )

        assert [row["note_target"] for row in report["rows"]] == ["public"]


_ALLOWED = (
    "example.com",
    "bucket",
    "db.internal",
    "[::1]",
    "xy12345.snowflakecomputing.com",
    "xy12345.cn-north-1.aws.snowflakecomputing.cn",
    "adb-1.azuredatabricks.net",
)
"""The hosts the host tests' server allows."""

_FILE_OPTIONS = "  format: parquet\n  options:\n    storage_options: "
"""The settings of a file target up to its `storage_options`, which a flow mapping follows."""

_DATABRICKS = "  type: databricks\n  http_path: /sql/1.0/warehouses/1\n  table: orders\n"
_SNOWFLAKE = (
    "  type: snowflake\n  user: analyst\n  warehouse: WH\n  database: DB\n"
    "  schema_name: PUBLIC\n  table: ORDERS\n"
)


def _remote(root: Path, target: str) -> Path:
    """Write a configuration whose source is a file under the root and whose target is `target`."""
    return _write(root, f"source:\n  path: a.csv\ntarget:\n{target}primary_keys: [id]\n")


_ENDPOINT_TARGETS = {
    "aws_endpoint": "  path: s3://bucket/a.parquet\n" + _FILE_OPTIONS,
    "endpoint_url": "  path: s3://bucket/a.parquet\n" + _FILE_OPTIONS,
    "azure_endpoint": "  path: az://bucket/a.parquet\n" + _FILE_OPTIONS,
    "aws_endpoint_url": "  type: delta\n  table_uri: s3://bucket/events\n  storage_options: ",
    "endpoint": "  type: delta\n  table_uri: s3://bucket/events\n  storage_options: ",
    "azure_storage_endpoint": "  type: delta\n  table_uri: az://bucket/events\n  storage_options: ",
    "s3.endpoint": (
        "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n  storage_options: "
    ),
}
"""For each endpoint key a tool passes on, a target up to its `storage_options`, for a reader that takes it:
Polars for a file, deltalake for a Delta Lake table, and PyIceberg's FileIO for an Iceberg table."""

_NAME_TARGET = "  type: delta\n  table_uri: s3://bucket/events\n  storage_options: "
"""A target up to its `storage_options`, for a bucket, a container, or an account option."""

_NAME_KEYS = (
    "account_name",
    "adls.account-name",
    "aws_bucket",
    "aws_bucket_name",
    "azure_container_name",
    "azure_storage_account_name",
    "bucket",
    "bucket_name",
    "container_name",
    "google_bucket",
    "google_bucket_name",
)
"""Each storage option key a tool passes on that names a bucket, a container, or an account."""


_MOTHERDUCK_NAME = (
    "database name, which may hold only letters, digits, _, -, and /, and not start with /,"
)
"""How a refusal names a MotherDuck `database` whose text before its `?` a tool does not pass on."""


_REMOTE_PREFIXES = [
    pytest.param("s3://lake", id="s3"),
    pytest.param("s3a://lake", id="s3a"),
    pytest.param("gs://lake", id="gs"),
    pytest.param("gcs://lake", id="gcs"),
    pytest.param("az://lake", id="az"),
    pytest.param("abfs://lake", id="abfs"),
    pytest.param("abfss://lake", id="abfss"),
    pytest.param("wasb://lake", id="wasb"),
    pytest.param("wasbs://lake", id="wasbs"),
    pytest.param("oss://lake", id="oss"),
    pytest.param("https://acct.blob.core.windows.net/lake", id="https"),
]
"""The start of a location in a store, up to the bucket or container `lake`, in each remote scheme."""

_DOT_SEGMENTS = [
    pytest.param("../", id="dot-dot"),
    pytest.param("./", id="dot"),
    pytest.param("%2e%2e/", id="encoded"),
    pytest.param("%2E%2E/", id="encoded-upper"),
    pytest.param(".%2e/", id="half-encoded"),
    pytest.param("%252e%252e/", id="encoded-twice"),
    pytest.param("..%2f", id="encoded-separator"),
    pytest.param("..\\", id="backslash"),
    pytest.param(".\t./", id="tab"),
    pytest.param("x/../", id="after-a-folder"),
]
"""The spellings of a `.` or `..` segment that a parser or a store could remove, each before a folder."""

_DOT_SEGMENT_HOSTS = ("lake", "acct", "acct.dfs.core.windows.net", "acct.blob.core.windows.net")
"""The hosts a tool allows for those locations: the container or bucket, and the account's hosts."""

_DOT_SEGMENT_REFUSAL = r"^The target {} is a URI on another machine with a '\.' or '\.\.' segment"
"""How a refusal of a location in a store with a dot segment starts, for a setting."""

_CAPITAL_PREFIXES = [
    *(
        pytest.param(f"{scheme}://Lake", id=scheme)
        for scheme in (
            "s3",
            "s3a",
            "s3n",
            "gs",
            "gcs",
            "az",
            "abfs",
            "abfss",
            "wasb",
            "wasbs",
            "oss",
            "hdfs",
            "opendal+s3",
            "made-up",
        )
    ),
    pytest.param("S3://lAke", id="scheme-in-capitals"),
    pytest.param("s3://lakE:9000", id="port"),
]
"""The start of a location on another machine whose bucket, container, or host `lake` holds a capital letter."""

_CAPITAL_REFUSAL = r"^The target {} names its bucket, container, or host with a capital letter\."
"""How a refusal of a bucket or a container with a capital letter starts, for a setting."""


class TestAllowedHosts:
    """Validate the rule that holds every host a side connects to to the hosts the server allows."""

    @pytest.mark.parametrize(
        ("target", "setting"),
        [
            pytest.param("  path: https://elsewhere.example/a.csv\n", "path", id="https"),
            pytest.param("  path: http://169.254.169.254/a.csv\n", "path", id="link-local"),
            pytest.param("  path: http://127.0.0.1:8080/a.csv\n", "path", id="loopback"),
            pytest.param("  path: s3://other/a.parquet\n", "path", id="bucket"),
            pytest.param("  path: 'https:///elsewhere.example/a.csv'\n", "path", id="no-host"),
            pytest.param(
                "  path: s3://bucket/a.parquet\n  format: parquet\n  options:\n"
                "    storage_options: {aws_endpoint_url: 'http://127.0.0.1:9000'}\n",
                "storage option 'aws_endpoint_url'",
                id="endpoint-url",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {endpoint: 'localhost:9000'}\n",
                "storage option 'endpoint'",
                id="endpoint-without-scheme",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://bucket/events\n"
                "  storage_options: {azure_storage_account_name: elsewhere}\n",
                "storage option 'azure_storage_account_name'",
                id="account-name",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: gs://other/events\n", "table_uri", id="iceberg"
            ),
            pytest.param(
                "  type: delta\n  table_uri: hdfs://127.0.0.1:9000/t\n", "table_uri", id="hdfs"
            ),
            pytest.param(
                "  type: delta\n  table_uri: 'HDFS://127.0.0.1:9000/t'\n",
                "table_uri",
                id="hdfs-upper-case",
            ),
            pytest.param("  type: delta\n  table_uri: hdfs:///t\n", "table_uri", id="hdfs-no-host"),
            pytest.param("  type: delta\n  table_uri: dbfs:/t\n", "table_uri", id="dbfs"),
            pytest.param(
                "  type: delta\n  table_uri: sftp://elsewhere.example/t\n",
                "table_uri",
                id="sftp",
            ),
            pytest.param(
                "  type: delta\n  table_uri: opendal+s3://other/t\n", "table_uri", id="opendal"
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3n://other/t/metadata/v1.metadata.json\n",
                "table_uri",
                id="s3n",
            ),
            pytest.param("  path: made-up://127.0.0.1/a.csv\n", "path", id="made-up"),
            pytest.param(
                "  type: duckdb\n  database: hdfs://127.0.0.1/x.duckdb\n  table: t\n",
                "database",
                id="duckdb-made-up",
            ),
            pytest.param(
                "  type: duckdb\n  database: https://elsewhere.example/x.duckdb\n  table: t\n",
                "database",
                id="duckdb",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@elsewhere.example/db\n  table: t\n",
                "uri",
                id="database",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal,elsewhere.example/db\n"
                "  table: t\n",
                "uri",
                id="database-second-host",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal/db?host=elsewhere.example\n"
                "  table: t\n",
                "uri",
                id="database-host-parameter",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal/db?host=\n  table: t\n",
                "uri",
                id="database-empty-host-parameter",
            ),
            pytest.param(
                "  type: database\n  uri: sqliteredshift://u@elsewhere.example:5439/d\n"
                "  query: SELECT 1\n",
                "uri",
                id="sqlite-scheme-connectorx-reads-as-postgres",
            ),
            pytest.param(
                "  type: database\n  uri: sqlite-redshift://u@elsewhere.example/d\n"
                "  query: SELECT 1\n",
                "uri",
                id="sqlite-scheme-with-redshift-after-a-dash",
            ),
            pytest.param(
                "  type: database\n  uri: SQLiteRedshift://u@elsewhere.example/d\n"
                "  password: p\n  query: SELECT 1\n",
                "uri",
                id="upper-case-redshift-lowered-by-the-password",
            ),
            pytest.param(f"{_SNOWFLAKE}  account: elsewhere\n", "account", id="snowflake"),
            pytest.param(
                f"{_SNOWFLAKE}  account: xy12345.cn-north-1\n", "account", id="snowflake-in-china"
            ),
            pytest.param(
                f"{_DATABRICKS}  server_hostname: elsewhere.example\n",
                "server_hostname",
                id="databricks",
            ),
        ],
    )
    def test_it_refuses_a_host_the_server_does_not_allow(
        self, tmp_path: Path, mocker: MockerFixture, target: str, setting: str
    ) -> None:
        """Ensure each setting that picks where a side connects is refused before it connects."""
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, target)

        with pytest.raises(
            ConfigError, match="names a host this server may not connect"
        ) as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        assert str(refused.value).startswith(f"The target {setting} names a host")
        assert "--allow-host" in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("target", "key", "endpoint"),
        [
            pytest.param(
                "  path: s3://bucket/a.parquet\n" + _FILE_OPTIONS,
                "aws_endpoint",
                "http://[::1]:9000",
                id="file-on-this-machine",
            ),
            pytest.param(
                "  path: s3://bucket/a.parquet\n" + _FILE_OPTIONS,
                "AWS_ENDPOINT_URL",
                "HTTP://db.internal:9000",
                id="file-in-upper-case",
            ),
            pytest.param(
                "  path: az://bucket/a.parquet\n" + _FILE_OPTIONS,
                "azure_endpoint",
                "Http://db.internal:10000",
                id="file-in-azure",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: ",
                "s3.endpoint",
                "http://db.internal:9000",
                id="iceberg",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n  storage_options: ",
                "endpoint",
                "http://db.internal:9000/",
                id="delta",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://bucket/events\n  storage_options: ",
                "azure_storage_endpoint",
                "ws://db.internal:10000",
                id="other-scheme",
            ),
            pytest.param(
                "  path: az://bucket/a.parquet\n" + _FILE_OPTIONS,
                "azure_storage_endpoint",
                "http:2130706433",
                id="http-without-slashes",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://bucket/events\n  storage_options: ",
                "azure_storage_endpoint",
                "HTTPS://db.internal:10000",
                id="https-in-upper-case",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: ",
                "s3.endpoint",
                "Https:443",
                id="https-without-slashes",
            ),
        ],
    )
    def test_it_refuses_an_endpoint_over_plain_http(
        self, tmp_path: Path, mocker: MockerFixture, target: str, key: str, endpoint: str
    ) -> None:
        """Ensure a reader is never sent to an endpoint but over HTTPS, not even on this machine.

        Polars and PyIceberg take the scheme from the endpoint, in any case,
        so plain HTTP would carry the credentials and the rows unencrypted.
        The URL parser behind Polars reads `http:2130706433` as plain HTTP to
        127.0.0.1, where the host check reads a host named `http`, and the AWS
        SDK in PyArrow reads `HTTPS://` as part of a host name, so only
        `https://` in lowercase passes. Every host is allowed, `http` and
        `https` too, and the refusal names the option, not its value.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"{target}{{{key}: '{endpoint}'}}\n")
        settings = Settings((tmp_path,), allowed_hosts=(*_ALLOWED, "http", "https"))

        with pytest.raises(
            ConfigError, match="names a host by a scheme other than https"
        ) as refused:
            describe_side(settings, "veridelta.yaml", "target")

        assert str(refused.value).startswith(f"The target storage option '{key}' names")
        assert endpoint not in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param(
                "  path: s3://file/a.parquet\n" + _FILE_OPTIONS + "{aws_bucket: file}\n",
                id="file-in-a-bucket-named-file",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://https/events\n"
                "  storage_options: {azure_container_name: https, account_name: bucket}\n",
                id="delta-in-a-container-named-https",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://wss/events\n  storage_options: {bucket: wss}\n",
                id="delta-in-a-bucket-named-wss",
            ),
        ],
    )
    def test_it_reads_from_a_bucket_named_like_a_scheme(
        self, tmp_path: Path, mocker: MockerFixture, target: str
    ) -> None:
        """Ensure a bucket or a container named like a scheme, with no `:`, is not an endpoint."""
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, target)
        settings = Settings((tmp_path,), allowed_hosts=(*_ALLOWED, "file", "https", "wss"))

        report = describe_side(settings, "veridelta.yaml", "target")

        assert report["columns"] == {"id": "Int64"}

    def test_it_refuses_a_snowflake_key_file_outside_the_roots_before_it_connects(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory, mocker: MockerFixture
    ) -> None:
        """Ensure an allowed account does not let the driver open a key file outside the roots."""
        read = mocker.patch.object(DiffEngine, "read_schema")
        outside = tmp_path_factory.mktemp("outside")
        target = f"{_SNOWFLAKE}  account: xy12345\n  private_key_path: {outside / 'k.pem'}\n"
        _remote(tmp_path, target)

        with pytest.raises(ConfigError, match=r"^The target private_key_path is outside"):
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        read.assert_not_called()

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param("postgresql://db.internal/u:p@127.0.0.1:5440/db", id="login-in-path"),
            pytest.param("postgres://db.internal:5433/d@127.0.0.1:5433/db", id="host-in-path"),
        ],
    )
    def test_it_refuses_a_database_uri_with_an_at_sign_after_its_host(
        self, tmp_path: Path, mocker: MockerFixture, uri: str
    ) -> None:
        """Ensure a URI whose driver could read another host after an `@` never connects."""
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"  type: database\n  uri: {uri}\n  table: t\n")

        with pytest.raises(ConfigError, match="'uri' holds an '@' after its host"):
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        read.assert_not_called()

    @pytest.mark.parametrize(
        ("target", "setting"),
        [
            pytest.param(
                "  path: 'http://elsewhere.example\\@example.com/a.csv'\n",
                "path",
                id="backslash",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {aws_endpoint_url: 'http://[elsewhere'}\n",
                "storage option 'aws_endpoint_url'",
                id="url-that-does-not-parse",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: gs://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {s3.endpoint: '//127.0.0.1:5436'}\n",
                "storage option 's3.endpoint'",
                id="endpoint-without-scheme-name",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {endpoint: 'http:127.0.0.1'}\n",
                "storage option 'endpoint'",
                id="endpoint-without-slashes",
            ),
            pytest.param(
                "  type: delta\n  table_uri: '  hdfs://127.0.0.1:9000/t'\n",
                "table_uri",
                id="hdfs-after-spaces",
            ),
            pytest.param(
                "  type: delta\n  table_uri: abfss://bucket@elsewhere.example/t\n",
                "table_uri",
                id="container-at-account",
            ),
            pytest.param(
                "  type: delta\n  table_uri: 'hdfs://[::1/t'\n",
                "table_uri",
                id="authority-that-does-not-parse",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u:p@elsewhere.example,@db.internal/db\n"
                "  table: t\n",
                "uri",
                id="database-second-login",
            ),
            pytest.param(
                "  type: database\n  uri: 'postgresql://db.internal#,127.0.0.1/db'\n  table: t\n",
                "uri",
                id="database-fragment",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {s3.endpoint: 'http://127.0.0.1:1@bucket', s3.region: x}\n",
                "storage option 's3.endpoint'",
                id="iceberg-endpoint-with-a-login",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {aws_endpoint_url: 'http://127.0.0.1:1@db.internal'}\n",
                "storage option 'aws_endpoint_url'",
                id="delta-endpoint-with-a-login",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {endpoint: '127.0.0.1:1@db.internal'}\n",
                "storage option 'endpoint'",
                id="endpoint-without-scheme-with-a-login",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {endpoint: 'http://db%2einternal:9000'}\n",
                "storage option 'endpoint'",
                id="endpoint-percent-encoded",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {endpoint: 'http://db.internal:90x0'}\n",
                "storage option 'endpoint'",
                id="endpoint-with-a-port-that-is-not-digits",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                '  storage_options: {endpoint: "http://\uff44b.internal:9000"}\n',
                "storage option 'endpoint'",
                id="endpoint-outside-ascii",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                '  storage_options: {endpoint: "http://db.internal\\t:9000"}\n',
                "storage option 'endpoint'",
                id="endpoint-with-a-tab",
            ),
            pytest.param(
                "  path: https://u@example.com/a.csv\n", "path", id="location-with-a-login"
            ),
            pytest.param(
                "  path: https://example.com/a@127.0.0.1/a.csv\n",
                "path",
                id="location-with-an-at-sign-in-its-path",
            ),
            pytest.param(
                "  path: https://example.com?@127.0.0.1/a.csv\n",
                "path",
                id="location-with-a-query-in-its-authority",
            ),
            pytest.param(
                "  path: 'https://example.com:443:80/a.csv'\n", "path", id="location-two-ports"
            ),
            pytest.param(
                '  path: "https://ex\u0430mple.com/a.csv"\n', "path", id="location-outside-ascii"
            ),
            pytest.param(
                '  type: delta\n  table_uri: "hd\\tfs://127.0.0.1/t"\n',
                "table_uri",
                id="scheme-with-a-tab",
            ),
            pytest.param(
                "  type: delta\n  table_uri: 'sftp://u@db.internal/t'\n",
                "table_uri",
                id="sftp-with-a-login",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'https://example.com:1@127.0.0.1/x.duckdb'\n"
                "  table: t\n",
                "database",
                id="duckdb-with-a-login",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal:54x2/db\n  table: t\n",
                "uri",
                id="database-port-that-is-not-digits",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@%64b.internal/db\n  table: t\n",
                "uri",
                id="database-host-percent-encoded",
            ),
            pytest.param(
                "  type: database\n  uri: trino://127.0.0.1:9%2F@db.internal:8080/cat\n"
                "  table: t\n",
                "uri",
                id="trino-login-that-ends-the-host-once-decoded",
            ),
            pytest.param(
                "  type: database\n  uri: trino://127.0.0.1@db.internal:8080/cat\n"
                "  password: 9/\n  table: t\n",
                "uri",
                id="trino-password-that-ends-the-host-once-decoded",
            ),
            pytest.param(
                "  type: database\n  uri: trino://u@db.internal%5D:8080/cat\n  table: t\n",
                "uri",
                id="trino-bracket-once-decoded",
            ),
            pytest.param(
                "  type: database\n  uri: 'postgresql://u@db.internal\\x/db'\n  table: t\n",
                "uri",
                id="database-backslash",
            ),
            pytest.param(
                "  type: database\n  uri: 'postgresql://u v@db.internal/db'\n  table: t\n",
                "uri",
                id="database-login-with-a-space",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal/db?host=db.internal:1\n"
                "  table: t\n",
                "uri",
                id="database-host-parameter-with-a-port",
            ),
            pytest.param(
                f"{_SNOWFLAKE}  account: '127.0.0.1:1@xy12345'\n",
                "account",
                id="snowflake-account-with-a-login",
            ),
            pytest.param(
                f"{_DATABRICKS}  server_hostname: 'adb-1.azuredatabricks.net:8443'\n",
                "server_hostname",
                id="databricks-with-a-port",
            ),
            pytest.param(
                f"{_DATABRICKS}  server_hostname: 'https://u@adb-1.azuredatabricks.net'\n",
                "server_hostname",
                id="databricks-with-a-login",
            ),
        ],
    )
    def test_it_refuses_a_host_written_in_a_shape_readers_split_differently(
        self, tmp_path: Path, mocker: MockerFixture, target: str, setting: str
    ) -> None:
        """Ensure a host is checked only as a host and a port, never as a part of other text.

        The AWS SDK in PyArrow ends `http://127.0.0.1:1@bucket` at its first
        `:`, so it connects to 127.0.0.1, while a URL parser reads `bucket`.
        Most cases hold a host the server allows, so each would pass a check
        that read only that part of the text.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, target)

        with pytest.raises(ConfigError, match="in the one shape every reader") as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        assert str(refused.value).startswith(f"The target {setting} does not name its host")
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("target", "key", "value"),
        [
            *(
                pytest.param(_ENDPOINT_TARGETS[key], key, value, id=f"{key}-{spelling}")
                for key in _ENDPOINT_TARGETS
                for spelling, value in (
                    ("path", "https://db.internal/other"),
                    ("dot-dot", "https://db.internal/.."),
                    ("encoded-slash", "https://db.internal/other%2Fx"),
                    ("trailing-slash", "https://db.internal/other/"),
                    ("two-slashes", "https://db.internal//"),
                    ("port-and-path", "https://db.internal:9000/other"),
                    ("ipv6-and-path", "https://[::1]:9000/other"),
                    ("upper-case-scheme", "HTTPS://db.internal/other"),
                    ("scheme-without-slashes", "https:db.internal/other"),
                    ("no-scheme", "db.internal:9000/other"),
                    ("backslash", "https://db.internal\\other"),
                    ("query", "https://db.internal/?x=1"),
                    ("query-after-the-host", "https://db.internal?x=1"),
                    ("fragment", "https://db.internal#x"),
                    ("at-sign-in-the-path", "https://db.internal:9000/x@127.0.0.1"),
                )
            ),
        ],
    )
    def test_it_refuses_an_endpoint_with_a_path(
        self, tmp_path: Path, mocker: MockerFixture, target: str, key: str, value: str
    ) -> None:
        """Ensure a host option holds no path, query, or fragment, in any spelling.

        Polars and deltalake, through object_store, and PyIceberg, through the
        AWS SDK in PyArrow, put an endpoint's path before the bucket or the
        container, so `https://db.internal/other` would read bucket `other` on
        an allowed host. A query or a fragment moves the bucket out of the
        request's path. Each host is allowed, so only the path refuses it,
        and the refusal names the option, not its value.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"{target}{{{key}: '{value}'}}\n")
        settings = Settings((tmp_path,), allowed_hosts=_ALLOWED)

        with pytest.raises(
            ConfigError, match="holds a path, a query, or a fragment after its host"
        ) as refused:
            describe_side(settings, "veridelta.yaml", "target")

        assert str(refused.value).startswith(f"The target storage option '{key}' holds")
        assert "as in https://host:port." in str(refused.value)
        assert value not in str(refused.value)
        read.assert_not_called()

    def test_it_knows_a_target_for_each_endpoint_and_name_option(self) -> None:
        """Ensure the endpoint and name tests cover every host option, each under its own message."""
        assert frozenset(_ENDPOINT_TARGETS) == mcp_server._ENDPOINT_OPTIONS
        assert frozenset(_NAME_KEYS) == mcp_server._NAME_OPTIONS
        assert mcp_server._HOST_OPTIONS == mcp_server._ENDPOINT_OPTIONS | mcp_server._NAME_OPTIONS

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            pytest.param(key, value, id=f"{key}-{spelling}")
            for key in _NAME_KEYS
            for spelling, value in (
                ("path", "bucket/other"),
                ("query", "bucket?x"),
                ("fragment", "bucket#x"),
                ("backslash", "bucket\\other"),
                ("port-and-path", "bucket:9000/other"),
                ("scheme-and-path", "https://bucket/other"),
                ("scheme-without-slashes", "https:bucket/other"),
            )
        ],
    )
    def test_it_refuses_a_bucket_container_or_account_with_a_path(
        self, tmp_path: Path, mocker: MockerFixture, key: str, value: str
    ) -> None:
        """Ensure a bucket, a container, or an account option holds no path, in a message about a name.

        A reader writes such a value into the request's host or path, so text
        after the name could reach a bucket the server did not check. The host
        is allowed, so only the path refuses it. The message speaks of a name,
        not of an endpoint's host and port.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"{_NAME_TARGET}{{{key}: '{value}'}}\n")
        settings = Settings((tmp_path,), allowed_hosts=_ALLOWED)

        with pytest.raises(ConfigError) as refused:
            describe_side(settings, "veridelta.yaml", "target")

        message = str(refused.value)
        assert message.startswith(
            f"The target storage option '{key}' holds a path, a query, or a fragment after its name."
        )
        assert message.endswith("only as a name, as in lake.")
        assert "endpoint" not in message
        assert "host:port" not in message
        assert value not in message
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("target", "setting", "form"),
        [
            *(
                pytest.param(
                    f"{_ENDPOINT_TARGETS[key]}{{{key}: 'https://u@db.internal'}}\n",
                    f"storage option '{key}'",
                    "with at most a port of digits after it, as in host:port or https://host:port.",
                    id=f"{key}-with-a-login",
                )
                for key in _ENDPOINT_TARGETS
            ),
            *(
                pytest.param(
                    f"{_NAME_TARGET}{{{key}: 'u@bucket'}}\n",
                    f"storage option '{key}'",
                    "with at most a port of digits after it, as in lake.",
                    id=f"{key}-with-a-login",
                )
                for key in _NAME_KEYS
            ),
            pytest.param(
                "  type: delta\n  table_uri: 'https://u@db.internal/t'\n",
                "table_uri",
                "with at most a port of digits after it, as in host:port or "
                "scheme://host:port/path.",
                id="location-with-a-login",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal:54x2/db\n  table: t\n",
                "uri",
                "with at most a port of digits after it, as in host:port or "
                "scheme://host:port/path.",
                id="database-uri-with-a-port-that-is-not-digits",
            ),
            pytest.param(
                f"{_SNOWFLAKE}  account: 'xy12345:443'\n",
                "account",
                "with no port or path after it, as in xy12345.us-east-1.",
                id="snowflake-account-with-a-port",
            ),
            pytest.param(
                f"{_DATABRICKS}  server_hostname: 'adb-1.azuredatabricks.net:8443'\n",
                "server_hostname",
                "with no port or path after it, as in adb-1.azuredatabricks.net.",
                id="databricks-with-a-port",
            ),
        ],
    )
    def test_it_says_how_to_write_a_host_for_its_kind_of_setting(
        self, tmp_path: Path, mocker: MockerFixture, target: str, setting: str, form: str
    ) -> None:
        """Ensure a host in another shape is refused with an example that fits its setting.

        A location may have a path after its host. An endpoint may not, since
        `_refuse_endpoint_path` refuses one, and a bucket, a container, or an
        account is a name alone. A Snowflake account and a Databricks server
        take no port or path.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, target)

        with pytest.raises(ConfigError) as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        message = str(refused.value)
        assert message.startswith(f"The target {setting} does not name its host")
        assert f"in ASCII, {form} Leave out" in message
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            pytest.param(key, value, id=f"{key}-{spelling}")
            for key in _NAME_KEYS
            for spelling, value in (
                ("name", "bucket"),
                ("port", "bucket:9000"),
                ("scheme", "https://bucket"),
                ("scheme-and-slash", "https://bucket/"),
            )
        ],
    )
    def test_it_reads_with_a_bucket_container_or_account_name(
        self, tmp_path: Path, mocker: MockerFixture, key: str, value: str
    ) -> None:
        """Ensure a bucket, a container, or an account option written as an allowed name still reads."""
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, f"{_NAME_TARGET}{{{key}: '{value}'}}\n")

        report = describe_side(
            Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target"
        )

        assert report["columns"] == {"id": "Int64"}

    @pytest.mark.parametrize(
        ("target", "key", "value"),
        [
            pytest.param(_ENDPOINT_TARGETS[key], key, value, id=f"{key}-{spelling}")
            for key in _ENDPOINT_TARGETS
            for spelling, value in (
                ("no-path", "https://db.internal"),
                ("slash", "https://db.internal/"),
                ("port", "https://db.internal:9000"),
                ("port-and-slash", "https://[::1]:9000/"),
                ("no-scheme", "db.internal:9000"),
            )
        ],
    )
    def test_it_reads_through_an_endpoint_with_no_path(
        self, tmp_path: Path, mocker: MockerFixture, target: str, key: str, value: str
    ) -> None:
        """Ensure an endpoint written as a host, a port, and at most a `/` still reads."""
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, f"{target}{{{key}: '{value}'}}\n")

        report = describe_side(
            Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target"
        )

        assert report["columns"] == {"id": "Int64"}

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param("  path: https://EXAMPLE.com:8443/a.csv\n", id="https"),
            pytest.param(
                "  path: s3://bucket/a.parquet\n  format: parquet\n  options:\n"
                "    storage_options: {aws_endpoint_url: 'https://[::1]:9000', aws_region: x}\n",
                id="object-store-endpoint",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://bucket/events\n"
                "  storage_options: {azure_storage_endpoint: 'https://db.internal:10000'}\n",
                id="endpoint-over-https-in-azure",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {bucket: bucket}\n",
                id="delta",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {s3.endpoint: 'db.internal:9000', s3.region: eu-west-1, "
                "s3.access-key-id: x, s3.secret-access-key: y}\n",
                id="iceberg-credentials",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u:p@db.internal:5432/db?sslmode=require\n"
                "  table: t\n",
                id="database",
            ),
            pytest.param(
                "  type: delta\n  table_uri: hdfs://db.internal:8020/t\n", id="delta-hdfs"
            ),
            pytest.param(
                "  type: database\n  uri: trino://u@db.internal:8080/cat\n  password: 'p:ss'\n"
                "  table: t\n",
                id="trino-with-a-password",
            ),
            pytest.param(
                "  type: database\n  uri: trino://u@db.internal:8080/cat\n  password: 'p[ss'\n"
                "  table: t\n",
                id="trino-with-a-bracket-in-the-password",
            ),
            pytest.param(
                "  type: database\n  uri: trino+redshift://u:p%2F@db.internal:5439/d\n"
                "  query: SELECT 1\n",
                id="trino-scheme-connectorx-reads-as-postgres",
            ),
            pytest.param(
                "  type: database\n  uri: oracle://u:p@db.internal:1521/svc?schema=s\n  table: t\n",
                id="oracle",
            ),
            pytest.param(
                "  type: database\n  uri: oracle://u:p@db.internal/S_1.x$-:dedicated/i1\n"
                "  table: t\n",
                id="oracle-server-type-and-instance",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://u@db.internal/db?sslmode=require&%C3%A9=1\n"
                "  table: t\n",
                id="database-parameter-outside-ascii",
            ),
            pytest.param("  type: database\n  uri: sqlite://x.db\n  table: t\n", id="sqlite"),
            pytest.param("  type: duckdb\n  database: x.duckdb\n  table: t\n", id="duckdb-file"),
            pytest.param(f"{_SNOWFLAKE}  account: XY12345\n", id="snowflake"),
            pytest.param(
                f"{_SNOWFLAKE}  account: XY12345.cn-north-1.aws\n", id="snowflake-in-china"
            ),
            pytest.param(
                f"{_DATABRICKS}  server_hostname: https://adb-1.azuredatabricks.net/\n",
                id="databricks",
            ),
        ],
    )
    def test_it_reads_from_a_host_the_server_allows(
        self, tmp_path: Path, mocker: MockerFixture, target: str
    ) -> None:
        """Ensure a side whose every host is allowed is read, whatever the case or the port.

        A lakehouse table lists no files here, since the listing would connect.
        """
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, target)
        (tmp_path / "x.duckdb").write_bytes(b"")

        report = describe_side(
            Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target"
        )

        assert report["columns"] == {"id": "Int64"}

    @pytest.mark.parametrize(
        ("target", "setting"),
        [
            pytest.param(
                "  type: iceberg\n  table_uri: gs://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {gcs.service.host: '//127.0.0.1:5436', "
                "gcs.oauth2.token: x}\n",
                "storage option 'gcs.service.host'",
                id="service-host",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: abfs://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {adls.dfs-storage-authority: '127.0.0.1:8811'}\n",
                "storage option 'adls.dfs-storage-authority'",
                id="storage-authority",
            ),
            pytest.param(
                "  type: delta\n  table_uri: az://bucket/events\n"
                "  storage_options: {azure_storage_use_emulator: 'false'}\n",
                "storage option 'azure_storage_use_emulator'",
                id="emulator",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                '  storage_options: {"aws_s\\u212aip_signature": "true"}\n',
                "storage option 'aws_s\u212aip_signature'",
                id="key-with-the-kelvin-sign",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://bucket/events\n"
                "  storage_options: {AWS_REGION: 'evil.example/#'}\n",
                "storage option 'AWS_REGION'",
                id="region-that-is-not-a-region",
            ),
            pytest.param(
                "  path: s3://bucket/a.parquet\n  format: parquet\n  options:\n"
                "    storage_options: {google_service_account: sa.json}\n",
                "storage option 'google_service_account'",
                id="key-file",
            ),
            pytest.param(
                "  type: bigquery\n  project: analytics\n  table: sales.orders\n"
                "  credentials_path: sa.json\n",
                "credentials_path",
                id="bigquery-key-file",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:?motherduck_host=127.0.0.1&motherduck_use_tls=false'"
                "\n  table: t\n",
                "database option other than attach_mode, saas_mode, or session_name",
                id="motherduck-host",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'MD:sales?attach_mode=single&Motherduck_Port=80'\n"
                "  table: t\n",
                "database option other than attach_mode, saas_mode, or session_name",
                id="motherduck-option-in-any-case",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:sales?attach_mode=single;motherduck_host=x'\n"
                "  table: t\n",
                "database option 'attach_mode'",
                id="motherduck-option-that-hides-another",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:x&motherduck_host=127.0.0.1&motherduck_use_tls=false'"
                "\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-option-after-an-ampersand",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:x#motherduck_host=127.0.0.1&motherduck_use_tls=false'"
                "\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-option-after-a-hash",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:x?attach_mode=single#motherduck_host=127.0.0.1'"
                "\n  table: t\n",
                "database option 'attach_mode'",
                id="motherduck-option-after-a-hash-in-the-options",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'Motherduck:x;motherduck_host=127.0.0.1?saas_mode=1'"
                "\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-option-after-a-semicolon",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:x%26motherduck_host%3D127.0.0.1'\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-option-percent-encoded",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:x motherduck_host=127.0.0.1'\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-option-after-a-space",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md://../outside.duckdb'\n  table: t\n",
                _MOTHERDUCK_NAME,
                id="motherduck-scheme-that-duckdb-opens-as-a-file",
            ),
        ],
    )
    def test_it_refuses_a_setting_whose_host_it_cannot_check(
        self, tmp_path: Path, mocker: MockerFixture, target: str, setting: str
    ) -> None:
        """Ensure a setting that can pick a host the server cannot see is refused before it connects."""
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, target)

        with pytest.raises(ConfigError, match="is not one a tool passes on") as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        assert str(refused.value).startswith(f"The target {setting} is not one")
        read.assert_not_called()

    @pytest.mark.parametrize(
        "database",
        [
            pytest.param("sqlite:x.db", id="sqlite"),
            pytest.param("SQLITE:x.db", id="sqlite-in-any-case"),
            pytest.param("sqlite:{outside}", id="sqlite-outside-the-roots"),
            pytest.param("sqlite:../x.db", id="sqlite-relative"),
            pytest.param("sqlite_scanner:x.db", id="sqlite-extension"),
            pytest.param("postgres_scanner:host=127.0.0.1 dbname=d", id="postgres-extension"),
            pytest.param("Postgres:host=127.0.0.1", id="postgres-in-any-case"),
            pytest.param("mysql_scanner:host=127.0.0.1 user=u", id="mysql-extension"),
            pytest.param("ducklake:x.ducklake", id="ducklake"),
            pytest.param("duckdb:x.duckdb", id="duckdb"),
            pytest.param("1ab:x", id="digit-first"),
        ],
    )
    def test_it_refuses_a_duckdb_database_opened_through_an_extension(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
        database: str,
    ) -> None:
        """Ensure no text before a colon picks the extension DuckDB opens a side through.

        DuckDB loads that extension before the sandbox applies, and opens the
        text after the prefix, which the check would read as another path.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        written = database.format(outside=tmp_path_factory.mktemp("outside") / "x.db")
        _remote(tmp_path, f"  type: duckdb\n  database: {json.dumps(written)}\n  table: t\n")

        with pytest.raises(ConfigError, match="open it through an extension") as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        assert written not in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize(
        "database",
        [
            pytest.param("file://elsewhere{root}/x.duckdb", id="host"),
            pytest.param("file://LOCALHOST{root}/x.duckdb", id="localhost-uppercase"),
            pytest.param("file://elsewhere:22{root}/x.duckdb", id="host-and-port"),
            pytest.param("file://%2e%2e{root}/x.duckdb", id="escaped-host"),
            pytest.param("file:///{root}/x.duckdb#/../../x.duckdb", id="fragment"),
        ],
    )
    def test_it_refuses_a_duckdb_file_uri(
        self, tmp_path: Path, mocker: MockerFixture, database: str
    ) -> None:
        """Ensure a `file:` URI whose path lies under the roots opens no file elsewhere.

        DuckDB opens `file://elsewhere/<root>/x.duckdb` as `/elsewhere/<root>/x.duckdb`,
        outside the roots, while each path the check reads from the URI lies under them.
        """
        connect = mocker.patch("duckdb.connect")
        written = database.format(root=tmp_path.as_posix())
        _remote(tmp_path, f"  type: duckdb\n  database: {json.dumps(written)}\n  table: t\n")

        with pytest.raises(ConfigError, match="is a file: URI") as refused:
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert written not in str(refused.value)
        connect.assert_not_called()

    def test_it_refuses_a_duckdb_database_in_a_user_home(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure `~<user>/..` opens no file outside the roots that the check read inside them.

        The check reads the user's home from the system, and DuckDB adds the
        text after `~` to HOME, then folds each `..` without the system, so
        the two reach different files whenever the homes sit at different depths.
        """
        connect = mocker.patch("duckdb.connect")
        written = f"~{getpass.getuser()}/../../x.duckdb"
        _remote(tmp_path, f"  type: duckdb\n  database: {json.dumps(written)}\n  table: t\n")

        with pytest.raises(ConfigError, match="starts with ~ and a name") as refused:
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert written not in str(refused.value)
        connect.assert_not_called()

    def test_it_opens_a_sqlite_file_at_a_duckdb_path_through_no_extension(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a SQLite file under the roots, named as a DuckDB file, installs no extension.

        DuckDB reads a SQLite file's header as a reason to download its SQLite
        extension, from a host no `--allow-host` names, and load it before the
        sandbox applies. Its extensions live under the home folder, which
        points into this test's folder, so one it installed would show there.
        """
        home = tmp_path_factory.mktemp("home")
        # POSIX reads HOME, Windows USERPROFILE.
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USERPROFILE", str(home))
        monkeypatch.chdir(tmp_path)
        with sqlite3.connect(tmp_path / "x.db") as connection:
            connection.execute("CREATE TABLE t (id INTEGER)")
        connection.close()
        _remote(tmp_path, "  type: duckdb\n  database: x.db\n  table: t\n")

        with pytest.raises(ConnectorError, match="not a valid DuckDB database file"):
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert list(home.rglob("*")) == []

    @pytest.mark.parametrize(
        ("uri", "refusal"),
        [
            pytest.param("bigquery:///keys/sa.json", "reads BigQuery", id="bigquery"),
            pytest.param("BigQuery+x://db.internal/keys/sa.json", "reads BigQuery", id="bq-plus"),
            pytest.param("' bigquery://db.internal/keys/sa.json'", "reads BigQuery", id="bq-space"),
            pytest.param("redshift-iam://u@db.internal:5439/d", "through AWS", id="redshift-iam"),
            pytest.param("Redshift-IAM+x://u@db.internal/d", "through AWS", id="iam-plus"),
            pytest.param(
                "postgresql://u@db.internal/d?sslrootcert=/keys/ca.pem",
                "parameter 'sslrootcert' names a file",
                id="postgres-certificate-authority",
            ),
            pytest.param(
                "postgresql://u@db.internal/d?sslmode=require&SSLCert=c.pem&ssl%6Bey=k.pem",
                "parameter 'sslcert' names a file",
                id="postgres-client-certificate",
            ),
            pytest.param(
                "postgresql://u@db.internal/d?sslkey", "parameter 'sslkey'", id="parameter-blank"
            ),
            pytest.param(
                "mysql://u@db.internal/d?ssl-mode=VERIFY_CA&SSL-CA=/keys/ca.pem",
                "parameter 'ssl-ca' names a file",
                id="mysql-certificate-authority",
            ),
            pytest.param(
                "mssql://u@db.internal/d?trust_server_certificate_ca=/keys/ca.pem",
                "parameter 'trust_server_certificate_ca' names a file",
                id="mssql-certificate-authority",
            ),
            pytest.param("oracle://u:p@db.internal/svc?alias=true", "TNS alias", id="oracle-alias"),
            pytest.param(
                "oracle://u:p@db.internal/svc%3Fhttps_proxy%3Dexample.com",
                "TNS alias",
                id="oracle-escape-in-the-path",
            ),
            pytest.param(
                "oracle://u:p@db.internal/svc,example.com:1521/x",
                "path other than a service name",
                id="oracle-second-address-in-the-path",
            ),
            pytest.param(
                "oracle://u:p@db.internal/svc(ADDRESS=(HOST=example.com))",
                "path other than a service name",
                id="oracle-address-in-the-path",
            ),
            pytest.param(
                "Oracle+redshift-iam-x://u:p@db.internal/svc?alias=true",
                "TNS alias",
                id="oracle-with-iam-not-rewritten",
            ),
        ],
    )
    def test_it_refuses_a_database_uri_that_opens_a_file_or_signs_in_unchecked(
        self, tmp_path: Path, mocker: MockerFixture, uri: str, refusal: str
    ) -> None:
        """Ensure a URI whose reader opens a file here, or signs in where no check sees, is refused.

        Each URI's host, where it has one, is allowed, and the message
        repeats no path the URI names.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"  type: database\n  uri: {uri}\n  query: SELECT 1\n")

        with pytest.raises(ConfigError, match=refusal) as refused:
            describe_side(Settings((tmp_path,), allowed_hosts=_ALLOWED), "veridelta.yaml", "target")

        assert str(refused.value).startswith("The target uri ")
        assert "/keys" not in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param(
                "  type: bigquery\n  project: analytics\n  table: sales.orders\n", id="bigquery"
            ),
            pytest.param("  type: duckdb\n  database: md:sales\n  table: t\n", id="motherduck"),
            pytest.param(
                "  type: duckdb\n  database: 'MD:_share/sales/23b0d623-1361-421d?saas_mode=1'\n"
                "  table: t\n",
                id="motherduck-share",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:'\n  table: t\n", id="motherduck-default"
            ),
            pytest.param(
                "  type: duckdb\n  database: 'motherduck:sales?attach_mode=single&saas_mode=true"
                "&session_name=etl-1'\n  table: t\n",
                id="motherduck-options",
            ),
            pytest.param("  path: hf://datasets/org/data/a.parquet\n", id="hugging-face"),
            pytest.param(
                "  path: hf://datasets/org/data/../data/a.parquet\n",
                id="hugging-face-with-a-dot-segment",
            ),
        ],
    )
    def test_it_needs_no_host_for_a_service_that_picks_its_own(
        self, tmp_path: Path, mocker: MockerFixture, target: str
    ) -> None:
        """Ensure a side that reaches one service's own hosts needs no `--allow-host`."""
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        _remote(tmp_path, target)

        report = describe_side(Settings((tmp_path,)), "veridelta.yaml", "target")

        assert report["columns"] == {"id": "Int64"}

    @pytest.mark.parametrize(
        ("target", "setting"),
        [
            pytest.param(
                "  type: database\n  uri: postgresql://u@${VD_MCP_SECRET}.elsewhere.example/db\n"
                "  table: t\n",
                "target -> uri",
                id="database",
            ),
            pytest.param(
                "  type: delta\n  table_uri: hdfs://${VD_MCP_SECRET}.elsewhere.example/t\n",
                "target -> table_uri",
                id="hdfs",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://bucket/t/metadata/v1.metadata.json\n"
                "  storage_options: {s3.endpoint: 'http://${VD_MCP_SECRET}:1@bucket'}\n",
                "target -> storage_options -> s3.endpoint",
                id="endpoint",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:sales?${VD_MCP_SECRET}=1'\n  table: t\n",
                "target -> database",
                id="motherduck-option",
            ),
            pytest.param(
                "  type: duckdb\n  database: '${VD_MCP_SECRET}_scanner:host=127.0.0.1'\n"
                "  table: t\n",
                "target -> database",
                id="duckdb-extension",
            ),
        ],
    )
    def test_it_takes_no_host_from_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str, setting: str
    ) -> None:
        """Ensure a host from `${NAME}` is refused before any host check, by setting and name."""
        monkeypatch.setenv("VD_MCP_SECRET", _SECRET)
        _remote(tmp_path, target)

        with pytest.raises(
            ConfigError,
            match=re.escape(f"On veridelta mcp, {setting} cannot take ${{VD_MCP_SECRET}}"),
        ) as refused:
            run_configuration(Settings((tmp_path,), allowed_hosts=("bucket",)), "veridelta.yaml")

        assert _SECRET not in str(refused.value)

    def test_a_check_with_schemas_connects_to_no_host_it_does_not_allow(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure `schemas` refuses a host before it reads a column."""
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, "  path: http://127.0.0.1:8080/a.csv\n")

        with pytest.raises(ConfigError, match="The target path names a host"):
            check_configuration(Settings((tmp_path,)), "veridelta.yaml", schemas=True)

        read.assert_not_called()

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param("  type: delta\n  table_uri: s3://elsewhere/t\n", id="delta"),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://elsewhere/t/metadata/v1.metadata.json\n",
                id="iceberg",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(
                lambda settings: check_configuration(settings, "veridelta.yaml", schemas=True),
                id="check",
            ),
            pytest.param(
                lambda settings: describe_side(settings, "veridelta.yaml", "target"),
                id="describe",
            ),
        ],
    )
    def test_it_refuses_a_host_before_it_lists_a_table(
        self,
        tmp_path: Path,
        mocker: MockerFixture,
        target: str,
        call: Callable[[Settings], object],
    ) -> None:
        """Ensure a lakehouse table on a host the server does not allow is never listed.

        Listing a table's files connects to its host, so the host check comes first.
        """
        listed = mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, target)

        with pytest.raises(ConfigError, match="The target table_uri names a host"):
            call(Settings((tmp_path,), allowed_hosts=_ALLOWED))

        listed.assert_not_called()

    @pytest.mark.parametrize("spelling", _DOT_SEGMENTS)
    @pytest.mark.parametrize("prefix", _REMOTE_PREFIXES)
    def test_it_refuses_a_remote_path_with_a_dot_segment(
        self, tmp_path: Path, mocker: MockerFixture, prefix: str, spelling: str
    ) -> None:
        """Ensure a location in a store whose path holds `.` or `..`, in any spelling, is refused before it is read.

        A reader sends the path as written, and the store could remove the
        segment with the bucket or container before it, and so read another
        one. The refusal names the setting and leaves the location out.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        path = f"{prefix}/{spelling}hidden/a.parquet"
        _remote(tmp_path, f"  path: {json.dumps(path)}\n  format: parquet\n")

        with pytest.raises(ConfigError, match=_DOT_SEGMENT_REFUSAL.format("path")) as refused:
            describe_side(
                Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
            )

        assert "hidden" not in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize("kind", ["delta", "iceberg"])
    def test_it_refuses_a_remote_table_uri_with_a_dot_segment_before_it_lists_it(
        self, tmp_path: Path, mocker: MockerFixture, kind: str
    ) -> None:
        """Ensure a lakehouse table in a store, with `..` in its URI, is refused before it is listed."""
        listed = mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, f"  type: {kind}\n  table_uri: abfs://lake/%2e%2e/hidden/t\n")

        with pytest.raises(ConfigError, match=_DOT_SEGMENT_REFUSAL.format("table_uri")):
            describe_side(
                Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
            )

        listed.assert_not_called()

    @pytest.mark.parametrize("spelling", _DOT_SEGMENTS)
    @pytest.mark.parametrize(
        "prefix",
        [
            *_REMOTE_PREFIXES,
            pytest.param("abfss://lake@acct.dfs.core.windows.net", id="abfss-at-account"),
            pytest.param("wasbs://lake@acct.blob.core.windows.net", id="wasbs-at-account"),
        ],
    )
    def test_it_refuses_a_file_a_table_names_in_a_store_with_a_dot_segment(
        self, tmp_path: Path, prefix: str, spelling: str
    ) -> None:
        """Ensure a file a log or metadata names in a store, with `.` or `..` in its path, is refused.

        The listing checks the file as the table names it, and the scan checks
        it again just before it opens it. Both leave the file out.
        """
        settings = Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS)
        location = f"{prefix}/{spelling}hidden/part-0.parquet"
        refuse_listed = mcp_server._refuse_listed  # pyright: ignore[reportPrivateUsage] - the listing runs it on each file a table names
        refuse_opened = mcp_server._refuse_opened  # pyright: ignore[reportPrivateUsage] - a lakehouse scan runs it on each file it opens

        with pytest.raises(ConfigError, match=_DOT_SEGMENT_REFUSAL.format("data file")) as listed:
            refuse_listed(settings, "target", "data file", location, quoted=True)
        with pytest.raises(
            ConfigError, match="about to open is a URI on another machine"
        ) as opened:
            refuse_opened(settings, location, True)

        assert "hidden" not in str(listed.value)
        assert "hidden" not in str(opened.value)

    @pytest.mark.parametrize(
        "name",
        [
            pytest.param("..hidden", id="dots-before-a-name"),
            pytest.param(".hidden", id="hidden-name"),
            pytest.param("hidden..", id="dots-after-a-name"),
            pytest.param("%2e%2ehidden", id="encoded-dots-before-a-name"),
            pytest.param("/hidden", id="empty-segment"),
        ],
    )
    @pytest.mark.parametrize(
        "prefix",
        [
            *_REMOTE_PREFIXES,
            pytest.param("abfss://lake@acct.dfs.core.windows.net", id="abfss-at-account"),
        ],
    )
    def test_it_reads_a_file_in_a_store_whose_names_hold_dots(
        self, tmp_path: Path, prefix: str, name: str
    ) -> None:
        """Ensure a name with dots in it, or an empty segment, which removes no other, passes."""
        settings = Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS)
        location = f"{prefix}/{name}/part-0.parquet"

        mcp_server._refuse_listed(settings, "target", "data file", location, quoted=True)  # pyright: ignore[reportPrivateUsage] - the listing runs it on each file a table names
        mcp_server._refuse_opened(settings, location, True)  # pyright: ignore[reportPrivateUsage] - a lakehouse scan runs it on each file it opens

    @pytest.mark.parametrize("prefix", _CAPITAL_PREFIXES)
    def test_it_refuses_a_remote_path_whose_bucket_holds_a_capital_letter(
        self, tmp_path: Path, mocker: MockerFixture, prefix: str
    ) -> None:
        """Ensure a bucket or a container with a capital letter is refused, though its lowercase is allowed.

        A reader keeps the case of the name after any scheme but a special
        one, so `s3://Lake` reads another bucket than `lake`. The refusal
        names the setting and leaves the name out.
        """
        read = mocker.patch.object(DiffEngine, "read_schema")
        _remote(tmp_path, f"  path: {prefix}/t/a.parquet\n  format: parquet\n")

        with pytest.raises(ConfigError, match=_CAPITAL_REFUSAL.format("path")) as refused:
            describe_side(
                Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
            )

        assert prefix.partition("://")[2] not in str(refused.value)
        read.assert_not_called()

    @pytest.mark.parametrize("kind", ["delta", "iceberg"])
    def test_it_refuses_a_table_uri_whose_bucket_holds_a_capital_letter_before_it_lists_it(
        self, tmp_path: Path, mocker: MockerFixture, kind: str
    ) -> None:
        """Ensure a lakehouse table in a bucket with a capital letter is refused before it is listed."""
        listed = mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, f"  type: {kind}\n  table_uri: s3://LAKE/t/metadata/v1.metadata.json\n")

        with pytest.raises(ConfigError, match=_CAPITAL_REFUSAL.format("table_uri")) as refused:
            describe_side(
                Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
            )

        assert "LAKE" not in str(refused.value)
        listed.assert_not_called()

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            *(pytest.param(key, "Lake", id=key) for key in sorted(mcp_server._BUCKET_OPTIONS)),  # pyright: ignore[reportPrivateUsage] - the keys whose value names a bucket or a container
            pytest.param("AWS_BUCKET", "lAke", id="key-in-capitals"),
            pytest.param("bucket", "https://Lake", id="with-a-scheme"),
        ],
    )
    def test_it_refuses_a_bucket_option_with_a_capital_letter(
        self, tmp_path: Path, mocker: MockerFixture, key: str, value: str
    ) -> None:
        """Ensure a storage option that names a bucket or a container with a capital letter is refused.

        No reader takes such an option over the location's bucket today, but
        the server holds it to the allowed hosts as a bucket all the same. The
        refusal names the option and leaves its value out.
        """
        listed = mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(
            tmp_path,
            f"  type: delta\n  table_uri: s3://lake/events\n  storage_options: {{{key}: '{value}'}}\n",
        )

        with pytest.raises(
            ConfigError,
            match=f"^The target storage option '{key}' names a bucket or a container with a "
            "capital letter",
        ) as refused:
            describe_side(
                Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
            )

        assert value not in str(refused.value)
        listed.assert_not_called()

    @pytest.mark.parametrize(
        "prefix",
        [
            *_CAPITAL_PREFIXES,
            pytest.param("abfss://Lake@acct.dfs.core.windows.net", id="abfss-at-account"),
            pytest.param("wasbs://laKe@acct.blob.core.windows.net", id="wasbs-at-account"),
        ],
    )
    def test_it_refuses_a_file_a_table_names_in_a_bucket_with_a_capital_letter(
        self, tmp_path: Path, prefix: str
    ) -> None:
        """Ensure a file a log or metadata names in a bucket or a container with a capital letter is refused.

        The listing checks the file as the table names it, and the scan checks
        it again just before it opens it. Both leave the file out.
        """
        settings = Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS)
        location = f"{prefix}/t/part-0.parquet"
        refuse_listed = mcp_server._refuse_listed  # pyright: ignore[reportPrivateUsage] - the listing runs it on each file a table names
        refuse_opened = mcp_server._refuse_opened  # pyright: ignore[reportPrivateUsage] - a lakehouse scan runs it on each file it opens

        with pytest.raises(ConfigError, match=_CAPITAL_REFUSAL.format("data file")) as listed:
            refuse_listed(settings, "target", "data file", location, quoted=True)
        with pytest.raises(
            ConfigError, match="about to open names its bucket, container, or host with a capital"
        ) as opened:
            refuse_opened(settings, location, True)

        assert prefix.partition("://")[2] not in str(listed.value)
        assert prefix.partition("://")[2] not in str(opened.value)

    @pytest.mark.parametrize(
        "location",
        [
            pytest.param("abfss://lake@ACCT.dfs.core.windows.net/t/a.parquet", id="azure-host"),
            pytest.param("https://ACCT.blob.core.windows.net/lake/a.parquet", id="https"),
            pytest.param("HTTPS://Acct.Blob.Core.Windows.Net/lake/a.parquet", id="https-scheme"),
            pytest.param("s3://lake/T/A.parquet", id="path"),
            pytest.param("ftp://Lake/a.parquet", id="ftp"),
            pytest.param("FTP://Lake/a.parquet", id="ftp-scheme"),
            pytest.param("ws://Lake/a.parquet", id="ws"),
            pytest.param("wss://Lake/a.parquet", id="wss"),
        ],
    )
    def test_it_reads_a_file_whose_dns_host_or_path_holds_a_capital_letter(
        self, tmp_path: Path, location: str
    ) -> None:
        """Ensure a DNS host keeps any case, and so does a path after the bucket.

        A URL parser lowercases the host of `https`, `ftp`, `ws`, and `wss`,
        and an Azure account's host after an `@` is a DNS name too.
        """
        settings = Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS)

        mcp_server._refuse_listed(settings, "target", "data file", location, quoted=True)  # pyright: ignore[reportPrivateUsage] - the listing runs it on each file a table names
        mcp_server._refuse_opened(settings, location, True)  # pyright: ignore[reportPrivateUsage] - a lakehouse scan runs it on each file it opens

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param("  path: https://Acct.Blob.Core.Windows.Net/lake/a.parquet\n", id="https"),
            pytest.param(
                "  type: delta\n  table_uri: az://lake/T\n  storage_options: "
                "{azure_storage_account_name: ACCT, azure_storage_endpoint: 'https://ACCT'}\n",
                id="account-and-endpoint",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: s3://lake/T/metadata/v1.metadata.json\n"
                "  storage_options: {s3.endpoint: 'https://Acct:9000'}\n",
                id="iceberg-endpoint",
            ),
        ],
    )
    def test_it_reads_from_a_dns_host_in_any_case(
        self, tmp_path: Path, mocker: MockerFixture, target: str
    ) -> None:
        """Ensure an https host, an endpoint, an account, and a path keep any case on the server."""
        mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _remote(tmp_path, target)

        report = describe_side(
            Settings((tmp_path,), allowed_hosts=_DOT_SEGMENT_HOSTS), "veridelta.yaml", "target"
        )

        assert report["columns"] == {"id": "Int64"}

    def test_it_resolves_a_file_uri_with_a_dot_segment_on_this_machine(
        self, tmp_path: Path
    ) -> None:
        """Ensure a file a table names on this machine is held to the roots after `..`, and not refused for it."""
        (tmp_path / "events").mkdir()
        location = (tmp_path / "events" / ".." / "part-0.parquet").as_uri()

        mcp_server._refuse_listed(
            Settings((tmp_path,)), "target", "data file", location, quoted=True
        )  # pyright: ignore[reportPrivateUsage] - the listing runs it on each file a table names


_STORE_PASSWORD = "  options: {storage_options: {aws_secret_access_key: '${PW}'}}\n"
"""A file source's storage option that takes its secret key from `PW`."""

_S3_TARGET = "target:\n  path: s3://lake/b.csv\nprimary_keys: [id]\n"
"""A target in an allowed bucket, so a configuration's source is the only side a test varies."""

_SOURCE_MODELS = get_args(get_args(SourceRef)[0])
"""Each model a `source` or `target` block can validate as."""

_REFERENCE_HOSTS = (
    "lake",
    "127.0.0.1",
    "db.internal",
    "acct",
    "acct.dfs.core.windows.net",
    "xy12345.snowflakecomputing.com",
    "adb-1.azuredatabricks.net",
)
"""Every host the configurations in the `${NAME}` tests name, so no host check stops them first."""

_REFERENCE_TOOLS = pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("validate_config", {"schemas": True}),
        ("run_comparison", {}),
        ("describe_schema", {"side": "source"}),
    ],
    ids=["validate_config", "run_comparison", "describe_schema"],
)
"""The tools that load a configuration and open its source."""

_KEY = "Hunter2SECRETPASSWORD"
"""The value of `KEY` unless a test sets another."""

_S3_TOKEN_TARGET = (
    "target:\n  type: delta\n  table_uri: s3://lake/t\n  storage_options:\n"
    "    aws_session_token: ${KEY}\n"
)
"""A Delta Lake side in S3 whose session token is `KEY`."""

_ICEBERG_SAS = (
    "  type: iceberg\n  table_uri: abfs://lake/t/metadata/v1.metadata.json\n"
    "  storage_options:\n    adls.account-name: acct\n    adls.sas-token: {token}\n"
)
"""An Azure Iceberg source whose SAS token the test writes."""


def _with_source(root: Path, source: str) -> Path:
    """Write a configuration whose source is `source`, and whose target is a file under the root."""
    (root / "b.csv").write_text("id\n1\n", encoding="utf-8")
    return _write(root, f"source:\n{source}target:\n  path: b.csv\nprimary_keys: [id]\n")


def _shown(result: CallToolResult) -> str:
    """Join everything a tool call returned: its structured result and its text."""
    return json.dumps(result.structured_content) + _text(result)


def _forms(value: str) -> set[str]:
    """Return the forms of a value an answer must not hold: as written, encoded, and its pieces.

    A piece is a run of eight or more letters and digits, which a reader can
    return cut from the rest. Each form is casefolded.
    """
    pieces = [piece for piece in re.split(r"[^A-Za-z0-9]", value) if len(piece) >= 8]
    encoded = quote(value, safe="", errors="surrogateescape")
    return {form.casefold() for form in (value, encoded, *pieces)}


def _holds_a_form(shown: str, value: str) -> bool:
    """Say whether an answer holds a form of `value`, as written or once decoded, in any case."""
    texts = (shown.casefold(), unquote(shown).casefold())
    return any(form in text for form in _forms(value) for text in texts)


@pytest.fixture
def engine_calls(mocker: MockerFixture) -> list[MagicMock]:
    """Mock each engine call that reads a side, so a test sees whether any was made."""
    return [
        mocker.patch.object(DiffEngine, name)
        for name in ("check_configs", "read_schema", "run_from_configs")
    ]


class TestEnvironmentReferences:
    """Validate the rule that a `${NAME}` stands only as the whole value of a credential setting.

    A reader can repeat a setting's value cut, encoded, or joined to other
    text, so the server refuses a reference anywhere else as it loads the
    file, before any check or reader, and masks every value it expanded.
    """

    @pytest.mark.parametrize(
        ("source", "environment", "refusal"),
        [
            pytest.param(
                _ICEBERG_SAS.format(token='"0=\' &${VDPW}"'),
                {"VDPW": "zz=1&aa=SECRETVALUE"},
                "source -> storage_options -> adls.sas-token must be ${VDPW} alone",
                id="sas-after-a-quote",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token='"?${VDPW}"'),
                {"VDPW": "zz=1&aa=SECRETVALUE"},
                "source -> storage_options -> adls.sas-token must be ${VDPW} alone",
                id="sas-after-a-mark",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token="${VDPW}"),
                {"VDPW": "zz=1&aa=SECRETVALUE"},
                "the value of ${VDPW} in source -> storage_options -> adls.sas-token must be "
                "an Azure SAS token",
                id="sas-of-another-shape",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token='"\\x24{VDPW}"'),
                {"VDPW": "zz=1&aa=SECRETVALUE"},
                "the value of ${VDPW} in source -> storage_options -> adls.sas-token must be "
                "an Azure SAS token",
                id="sas-through-a-yaml-escape",
            ),
            pytest.param(
                '  path: "s3://lake/${KEY}.csv"\n',
                {"KEY": "Hunter2+SEC%RET"},
                "source -> path cannot take ${KEY}",
                id="path-a-reader-encodes",
            ),
            pytest.param(
                '  path: "s3://lake/\\x24{KEY}.csv"\n',
                {},
                "source -> path cannot take ${KEY}",
                id="path-through-a-yaml-escape",
            ),
            pytest.param(
                '  type: iceberg\n  table_uri: "abfs://lake/${KEY}#x/t"\n',
                {},
                "source -> table_uri cannot take ${KEY}",
                id="table-uri-a-reader-cuts",
            ),
            pytest.param(
                "  type: database\n  uri: ${VD_MCP_URI}\n  table: orders\n",
                {"VD_MCP_URI": "postgresql://db.internal/sales-ledger?application_name=orders"},
                "source -> uri cannot take ${VD_MCP_URI}",
                id="whole-uri",
            ),
            pytest.param(
                '  type: database\n  uri: "sqlite+${KEY}://x.db"\n  table: t\n',
                {"KEY": "Hunter2SECRET:PASSWORDtail"},
                "source -> uri cannot take ${KEY}",
                id="uri-scheme-after-text",
            ),
            pytest.param(
                '  type: database\n  uri: "${KEY}://x.db"\n  table: t\n',
                {"KEY": "Hunter2SECRET:PASSWORDtail"},
                "source -> uri cannot take ${KEY}",
                id="uri-scheme",
            ),
            pytest.param(
                '  type: database\n  uri: "postgresql://${KEY}@127.0.0.1:9/db"\n  table: t\n',
                {"KEY": "svc_ledger_reader:Hunter2SECRETPASS"},
                "the value of ${KEY} in source -> uri must be letters, digits",
                id="uri-login-in-the-user",
            ),
            pytest.param(
                '  type: database\n  uri: "sqlite://${KEY}@/m.db"\n  table: t\n',
                {"KEY": "svc_ledger_reader:Hunter2SECRETPASS"},
                "the value of ${KEY} in source -> uri must be letters, digits",
                id="sqlite-login-in-the-user",
            ),
            pytest.param(
                "  path: s3://lake/a.csv\n"
                "  options: {storage_options: {aws_access_key_id: '${KEY}'}}\n",
                {"KEY": "svc\\ledger_reader"},
                "the value of ${KEY} in source -> options -> storage_options -> "
                "aws_access_key_id must be a login name",
                id="login-with-a-backslash",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
                "  table: orders\n  password: ${KEY}\n",
                {"KEY": "q7Z"},
                "the value of ${KEY} in source -> password must be at least 4 characters",
                id="short-password",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
                "  table: orders\n  password: ${KEY}\n",
                {"KEY": "Hunter2\udcffSecretPass"},
                "the value of ${KEY} in source -> password must be text in UTF-8",
                id="password-outside-utf-8",
            ),
            pytest.param(
                '  type: database\n  uri: "postgresql://${KEY}:wrongpw1@db.internal/sales"\n'
                "  table: orders\n",
                {"KEY": "LedgerReaderForTheSalesTeamQ2026x"},
                "the value of ${KEY} in source -> uri must be at most 32 characters",
                id="uri-user-a-database-can-cut",
            ),
            pytest.param(
                "  path: a.csv\n  options: {row_index_name: '${KEY}'}\n",
                {},
                "source -> options -> row_index_name cannot take ${KEY}",
                id="row-index-name",
            ),
            pytest.param(
                "  path: a.csv\n  options: {new_columns: [id, '${KEY}']}\n",
                {},
                "source -> options -> new_columns -> 1 cannot take ${KEY}",
                id="new-columns",
            ),
            pytest.param(
                "  path: a.csv\n  options: {include_file_paths: '${KEY}'}\n",
                {},
                "source -> options -> include_file_paths cannot take ${KEY}",
                id="include-file-paths",
            ),
            pytest.param(
                "  type: duckdb\n  database: w.duckdb\n  query: \"select '${KEY}'\"\n",
                {},
                "source -> query cannot take ${KEY}",
                id="query",
            ),
            pytest.param(
                '  path: "g/[${KEY}].csv"\n', {}, "source -> path cannot take ${KEY}", id="glob"
            ),
            pytest.param(
                "  type: [file]\n  path: ${KEY}\n",
                {},
                "source -> path cannot take ${KEY}",
                id="type-that-is-no-text",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token="x").replace(
                    "adls.sas-token: x", "adls.account-key: ${KEY}"
                ),
                {"KEY": "Hunter2-SECRET-KEY"},
                "the value of ${KEY} in source -> storage_options -> adls.account-key must be "
                "an account key in base64",
                id="account-key-of-another-shape",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token="x").replace(
                    "adls.sas-token: x", "adls.account-key: ${KEY}"
                ),
                {"KEY": "SGVsbG8gV29ybGQhIHNlY3JldEB="},
                "the value of ${KEY} in source -> storage_options -> adls.account-key must be "
                "an account key in base64",
                id="account-key-a-reader-decodes-only-leniently",
            ),
            pytest.param(
                '  type: database\n  uri: "postgresql://vdref0x:${KEY}@db.internal/sales"\n'
                "  table: orders\n",
                {"KEY": "q7"},
                "the value of ${KEY} in source -> uri must be at least 4 characters",
                id="uri-password-after-a-user-that-looks-like-a-reference",
            ),
            pytest.param(
                "  type: database\n  uri: sqlite://u@/x.db\n  table: t\n  password: ${KEY}\n",
                {"KEY": "Hunter2Secret//TailPartXYZ"},
                "source -> password cannot take ${KEY} for a SQLite file",
                id="sqlite-password-the-system-folds",
            ),
            pytest.param(
                "  type: database\n  uri: sqlite://u@/x.db\n  table: t\n  password: ${KEY}\n",
                {"KEY": "postgresql://ledger_svc:Hunter2DbPass@db.internal:5432/sales"},
                "source -> password cannot take ${KEY} for a SQLite file",
                id="sqlite-password-that-is-a-uri",
            ),
            pytest.param(
                '  type: database\n  uri: "postgresql://u:${KEY}@db.internal/sales"\n'
                "  table: orders\n",
                {"KEY": "p@ss/word"},
                "the value of ${KEY} in source -> uri must be letters, digits, '.', '_', '~', and "
                "'-' only, so the uri reads the same encoded or decoded. Put a password with "
                "other characters in the side's 'password' setting",
                id="uri-password-a-reader-encodes",
            ),
            *(
                pytest.param(
                    _S3_TOKEN_TARGET.replace("target:\n", ""),
                    {"KEY": value},
                    "the value of ${KEY} in source -> storage_options -> aws_session_token must "
                    "be a key or a token of letters, digits",
                    id=f"session-token-{name}",
                )
                for name, value in (
                    ("s3-escapes", "Hunter2&Correct<Horse>Battery"),
                    ("s3-trims", "Hunter2SECRETpasswordXYZ "),
                    ("outside-ascii", "Hunter2\u00e9SECRET\u03a9password"),
                )
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:sales'\n  table: t\n  motherduck_token: ${KEY}\n",
                {"KEY": "eyJhbGciOi JIUzI1NiJ9"},
                "the value of ${KEY} in source -> motherduck_token must be a key or a token",
                id="motherduck-token-with-a-space",
            ),
        ],
    )
    @_REFERENCE_TOOLS
    def test_it_refuses_a_reference_before_it_reads(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        engine_calls: list[MagicMock],
        source: str,
        environment: dict[str, str],
        refusal: str,
        tool: str,
        arguments: dict[str, object],
    ) -> None:
        """Ensure each form a reader could repeat unmasked is refused, naming the setting.

        The refusal names the setting and the variable, holds no form of the
        value, and comes before any check or read.
        """
        environment = {"KEY": _KEY, **environment}
        for name, value in environment.items():
            monkeypatch.setenv(name, value)
        _with_source(tmp_path, source)
        settings = Settings(
            (tmp_path,),
            allow_row_values=True,
            allow_queries=True,
            allowed_hosts=_REFERENCE_HOSTS,
        )

        result = _call(settings, {"path": "veridelta.yaml", **arguments}, tool=tool)

        shown = _shown(result)
        if tool == "validate_config":
            assert result.structured_content is not None
            assert result.structured_content["valid"] is False
        else:
            assert result.is_error is True
        assert f"On veridelta mcp, {refusal}" in shown
        for value in environment.values():
            assert not _holds_a_form(shown, value)
        for mock in engine_calls:
            mock.assert_not_called()

    def test_it_checks_where_an_unset_variable_stands(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `allow_missing_env` still refuses a reference outside a credential setting."""
        monkeypatch.delenv("DATA", raising=False)
        _with_source(tmp_path, "  path: ${DATA}.csv\n")

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml", "allow_missing_env": True})

        report = result.structured_content
        assert report is not None
        assert report["valid"] is False
        assert "source -> path cannot take ${DATA}" in report["errors"][0]
        assert report["findings"][0]["locations"] == [{"path": ["source", "path"]}]
        assert report["warnings"] == [
            "Environment variable 'DATA' is not set, so its references were checked as the "
            "text 'DATA'."
        ]

    def test_it_takes_a_uri_user_and_password_and_masks_them_in_any_case(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a uri's whole user and whole password may each be a `${NAME}`.

        A driver error that writes the uri in lowercase still has both masked.
        """
        monkeypatch.setenv("PGUSER", "Ledger_Reader")
        monkeypatch.setenv("PGPASSWORD", "Hunter2-SECRET_pass")
        uri = "postgresql://Ledger_Reader:Hunter2-SECRET_pass@db.internal/sales"
        mocker.patch(
            "veridelta.connectors.database.pl.read_database_uri",
            side_effect=RuntimeError(f"could not connect to {uri.lower()}"),
        )
        _with_source(
            tmp_path,
            "  type: database\n"
            '  uri: "postgresql://${PGUSER}:${PGPASSWORD}@db.internal/sales"\n'
            "  table: orders\n",
        )
        settings = Settings((tmp_path,), allowed_hosts=("db.internal",), allow_row_values=True)

        checked = _call(settings, {"path": "veridelta.yaml"})
        read = _call(settings, {"path": "veridelta.yaml", "schemas": True})

        assert checked.structured_content is not None
        assert checked.structured_content["valid"] is True
        assert "could not connect to postgresql://***:***@db.internal/sales" in _shown(read)
        for value in ("Ledger_Reader", "Hunter2-SECRET_pass"):
            assert not _holds_a_form(_shown(read), value)

    def test_it_takes_a_uri_user_of_32_characters_and_masks_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a uri user of 32 characters loads, and is masked where a driver repeats it.

        No supported database cuts a user name this short before it writes the
        name into its error, so the error holds the whole value.
        """
        user = "LedgerReaderForTheSalesTeamQ2026"
        monkeypatch.setenv("VDUSER", user)
        mocker.patch(
            "veridelta.connectors.database.pl.read_database_uri",
            side_effect=RuntimeError(f'FATAL: password authentication failed for user "{user}"'),
        )
        _with_source(
            tmp_path,
            '  type: database\n  uri: "postgresql://${VDUSER}:wrongpw1@db.internal/sales"\n'
            "  table: orders\n",
        )
        settings = Settings((tmp_path,), allowed_hosts=("db.internal",), allow_row_values=True)

        checked = _call(settings, {"path": "veridelta.yaml"})
        read = _call(settings, {"path": "veridelta.yaml", "schemas": True})

        assert len(user) == 32
        assert checked.structured_content is not None
        assert checked.structured_content["valid"] is True
        assert 'failed for user \\"***\\"' in _shown(read)
        assert not _holds_a_form(_shown(read), user)

    @pytest.mark.parametrize(
        "side",
        [
            pytest.param(
                '  uri: "postgresql://${VDUSER}:e@db.internal/sales"\n', id="uri-password"
            ),
            pytest.param(
                '  uri: "postgresql://${VDUSER}@db.internal/sales"\n  password: e\n',
                id="password-setting",
            ),
        ],
    )
    def test_it_masks_a_value_whole_before_the_reader_masks_a_shorter_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, side: str
    ) -> None:
        """Ensure a password written in the file cannot split a recorded value before the mask.

        The reader masks its own password in a driver's message. A password of
        one letter, masked first, would cut the user from `${VDUSER}` into
        pieces the mask no longer finds whole, and a call for each letter
        would give the rest of it.
        """
        user = "LedgerReaderForTheSalesTeamQ2026"
        monkeypatch.setenv("VDUSER", user)
        mocker.patch(
            "veridelta.connectors.database.pl.read_database_uri",
            side_effect=RuntimeError(f'FATAL: password authentication failed for user "{user}"'),
        )
        _with_source(tmp_path, f"  type: database\n{side}  table: orders\n")
        settings = Settings((tmp_path,), allowed_hosts=("db.internal",), allow_row_values=True)

        read = _call(settings, {"path": "veridelta.yaml", "schemas": True})

        assert 'fail***d for us***r \\"***\\"' in _shown(read)
        assert "q2026" not in _shown(read).casefold()

    def test_it_leaves_a_token_that_shares_an_opening_with_a_recorded_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a recorded JWT leaves another JWT with the same header as it is.

        Every JWT signed the same way opens with the same header, so a mask of
        a value's head would garble other data for as long as the server runs.
        """
        header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
        recorded = header + "eyJzdWIiOiJsZWRnZXIifQ.c2lnbmF0dXJlTGVkZ2Vy"
        other = header + "eyJzdWIiOiJvdGhlciJ9.b3RoZXJTaWduYXR1cmU"
        monkeypatch.setenv("VD_TOKEN", recorded)
        _with_source(tmp_path, "  path: a.csv\n")
        (tmp_path / "a.csv").write_text(f"id,{other}\n1,2\n", encoding="utf-8")
        _write(
            tmp_path,
            "source:\n  type: duckdb\n  database: 'md:sales'\n  table: t\n"
            "  motherduck_token: ${VD_TOKEN}\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
            "one.yaml",
        )

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool("validate_config", {"path": "one.yaml"}),
                await client.call_tool(
                    "describe_schema", {"path": "veridelta.yaml", "side": "source"}
                ),
            ]

        loaded, later = _with_client(Settings((tmp_path,)), work)

        assert loaded.structured_content is not None
        assert loaded.structured_content["valid"] is True
        assert later.structured_content == {
            "side": "source",
            "columns": {"id": "Int64", other: "Int64"},
        }

    def test_it_masks_no_prefix_of_a_recorded_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a guess at a value's head, named as a column, comes back as written.

        An agent could otherwise name guessed heads as columns of another
        configuration and see which come back masked, learning a value one
        character per call. Only a guess of the whole value is masked.
        """
        token = "dapiQ7f3e9a1b2c4d6e8f0a1b2c3d4e5f6"
        heads = [token[:16], token[:24].upper(), token[:-1]]
        monkeypatch.setenv("VD_TOKEN", token)
        _with_source(tmp_path, "  path: a.csv\n")
        (tmp_path / "a.csv").write_text(
            ",".join(["id", *heads, token]) + "\n1,2,3,4,5\n", encoding="utf-8"
        )
        _write(
            tmp_path,
            "source:\n  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
            "  table: orders\n  password: ${VD_TOKEN}\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
            "one.yaml",
        )

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool("validate_config", {"path": "one.yaml"}),
                await client.call_tool(
                    "describe_schema", {"path": "veridelta.yaml", "side": "source"}
                ),
            ]

        loaded, later = _with_client(Settings((tmp_path,)), work)

        assert loaded.structured_content is not None
        assert loaded.structured_content["valid"] is True
        assert later.structured_content == {
            "side": "source",
            "columns": {"id": "Int64", **dict.fromkeys(heads, "Int64"), "***": "Int64"},
        }

    def test_it_builds_a_mask_for_a_value_outside_utf_8(self) -> None:
        """Ensure a recorded value with a byte outside UTF-8 still gives a mask, as its bytes read."""
        pattern = mcp_server._mask_pattern(frozenset({"Hunter2\udcffSecretPass"}))  # pyright: ignore[reportPrivateUsage]  # the mask is private to the server

        assert pattern is not None
        assert pattern.sub("***", "Hunter2%FFSecretPass") == "***"

    def test_it_takes_an_empty_credential(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a credential set but empty loads, since it holds nothing to mask."""
        monkeypatch.setenv("PW", "")
        _with_source(tmp_path, "  path: a.csv\n" + _STORE_PASSWORD)

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"})

        assert result.structured_content is not None
        assert result.structured_content["valid"] is True

    def test_it_takes_a_short_login_and_leaves_it_unmasked(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a login name of 3 characters loads, and is too short to mask.

        A login name is an identifier a service may echo, not a secret, and a
        mask that short would hide ordinary text in every answer.
        """
        monkeypatch.setenv("KID", "kQz")
        mocker.patch.object(
            DiffEngine, "read_schema", side_effect=ConfigError("The access key id kQz is unknown.")
        )
        _with_source(
            tmp_path,
            "  path: s3://lake/a.csv\n  options: {storage_options: {aws_access_key_id: '${KID}'}}\n",
        )
        settings = Settings((tmp_path,), allowed_hosts=("lake",))

        checked = _call(settings, {"path": "veridelta.yaml"})
        described = _call(
            settings, {"path": "veridelta.yaml", "side": "source"}, tool="describe_schema"
        )

        assert checked.structured_content is not None
        assert checked.structured_content["valid"] is True
        assert "The access key id kQz is unknown." in _text(described)

    def test_it_masks_a_snowflake_user_a_driver_writes_in_lowercase(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a login name from `${NAME}` is masked in any case, as a service can echo it."""
        monkeypatch.setenv("SFUSER", "SvcLedgerReader")
        monkeypatch.setenv("SFPASSWORD", _SECRET)
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError("Incorrect username or password for svcledgerreader."),
        )
        _with_source(
            tmp_path,
            "  type: snowflake\n  account: xy12345\n  user: ${SFUSER}\n"
            "  password: ${SFPASSWORD}\n  warehouse: WH\n  database: DB\n"
            "  schema_name: PUBLIC\n  table: ORDERS\n",
        )
        settings = Settings((tmp_path,), allowed_hosts=_REFERENCE_HOSTS, allow_row_values=True)

        result = _call(settings, {"path": "veridelta.yaml", "side": "source"}, "describe_schema")

        assert "Incorrect username or password for ***." in _text(result)
        assert not _holds_a_form(_shown(result), "SvcLedgerReader")

    @_REFERENCE_TOOLS
    def test_it_refuses_a_sqlite_password_before_any_path_quotes_it(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        tool: str,
        arguments: dict[str, object],
    ) -> None:
        """Ensure a `password` the driver would read into a SQLite path never comes back.

        The process runs outside the root, where the check of the path would
        quote it, with the password resolved into it. The target's reference
        is refused at the load, so no form of the value shows.
        """
        password = "p@ss/w#rd+%9/../tail"
        monkeypatch.setenv("PW", password)
        config = _write(
            tmp_path,
            "source:\n  path: a.csv\n"
            "target:\n  type: database\n  uri: sqlite://u@/x.db\n  table: t\n"
            "  password: ${PW}\nprimary_keys: [id]\n",
        )
        monkeypatch.chdir(tmp_path_factory.mktemp("elsewhere"))

        result = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": str(config), **arguments},
            tool=tool,
        )

        shown = _shown(result)
        assert "target -> password cannot take ${PW} for a SQLite file" in shown
        folded = password.casefold()
        windows = {folded[i : i + 6] for i in range(len(folded) - 5)}
        for text in (shown.casefold(), unquote(shown).casefold()):
            assert not any(window in text for window in windows)

    def test_it_masks_from_its_own_load_and_not_from_a_second_read_of_the_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a file rewritten after the load still has its value masked.

        The read rewrites the file without the reference, then fails with a
        message that quotes the password, so only the load's own record of the
        value can mask it.
        """
        monkeypatch.setenv("PW", _SECRET)
        config = _with_source(tmp_path, "  path: a.csv\n" + _STORE_PASSWORD)

        def rewrite_then_fail(*_: object) -> None:
            _with_source(tmp_path, "  path: a.csv\n")
            raise ConnectorError(f"The store refused the key {_SECRET}.")

        mocker.patch.object(DiffEngine, "read_schema", side_effect=rewrite_then_fail)

        result = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": str(config), "side": "source"},
            tool="describe_schema",
        )

        assert "The store refused the key ***." in _text(result)
        assert _SECRET not in _shown(result)

    def test_it_masks_a_value_a_later_call_echoes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a value one call expanded is masked in a later call that never names it.

        Polars caches an object store across calls, and its error can name the
        path of the call that built it. The server keeps every value it
        expanded while it runs, so the later answer masks it too.
        """
        monkeypatch.setenv("KEY", _KEY)
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError(f"HEAD failed (path: s3://lake/{_KEY}.csv)"),
        )
        _write(tmp_path, 'source:\n  path: "s3://lake/${KEY}.csv"\n' + _S3_TARGET, "one.yaml")
        _write(
            tmp_path,
            "source:\n  path: s3://lake/a.csv\n"
            "  options: {storage_options: {aws_secret_access_key: '${KEY}'}}\n" + _S3_TARGET,
            "two.yaml",
        )
        _write(tmp_path, "source:\n  path: s3://lake/plain.csv\n" + _S3_TARGET, "three.yaml")
        settings = Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",))

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool("describe_schema", {"path": name, "side": "source"})
                for name in ("one.yaml", "two.yaml", "three.yaml")
            ]

        refused, loaded, later = _with_client(settings, work)

        assert "source -> path cannot take ${KEY}" in _text(refused)
        assert "(path: s3://lake/***.csv)" in _text(loaded)
        assert "(path: s3://lake/***.csv)" in _text(later)
        for result in (refused, loaded, later):
            assert not _holds_a_form(_shown(result), _KEY)

    def test_it_masks_each_form_of_a_sas_signature(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a SAS token's signature is masked on its own, as written, decoded, and encoded.

        The Azure SDK writes the token's parameters back in another order, so
        the token's whole text may not appear where its signature does.
        """
        signature = "c2ln%2bbmF0/dXJl%3D"
        monkeypatch.setenv("VD_AZURE_SAS", f"sv=2024-01-01&sig={signature}&sp=r")
        forms = (signature, "c2ln+bmF0/dXJl=", "c2ln%2BbmF0%2FdXJl%3D")
        mocker.patch.object(DiffEngine, "read_schema", side_effect=ConnectorError(" ".join(forms)))
        _with_source(tmp_path, _ICEBERG_SAS.format(token="${VD_AZURE_SAS}"))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))

        result = _call(
            Settings((tmp_path,), allow_row_values=True, allowed_hosts=_AZURE_HOSTS),
            {"path": "veridelta.yaml", "side": "source"},
            tool="describe_schema",
        )

        assert "ConnectorError: *** *** ***" in _text(result)

    def test_it_masks_a_session_token_s3_writes_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a session token S3 repeats in its error is masked, as text and as bytes in hex.

        S3 writes a token it cannot read back XML-escaped, and a request it
        could not verify as bytes in hex. A token in its shape reads the same
        escaped, so both forms are masked.
        """
        token = "IQoJb3JpZ2luX2VjEJr//////////wEaCXVz+Q7ledger="
        monkeypatch.setenv("KEY", token)
        body = (
            "<Error><Code>InvalidToken</Code><Message>The provided token is malformed or "
            f"otherwise invalid.</Message><Token-0>{html.escape(token)}</Token-0>"
            f"<CanonicalRequestBytes>{token.encode().hex(' ')}</CanonicalRequestBytes></Error>"
        )
        mocker.patch.object(DiffEngine, "read_schema", side_effect=ConnectorError(body))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _with_source(tmp_path, _S3_TOKEN_TARGET.replace("target:\n", ""))

        result = _call(
            Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",)),
            {"path": "veridelta.yaml", "side": "source"},
            tool="describe_schema",
        )

        assert "<Token-0>***</Token-0><CanonicalRequestBytes>***<" in _text(result)
        assert not _holds_a_form(_shown(result), token)

    def test_it_masks_a_form_encoded_secret(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a client secret is masked as object_store form-encodes it, with `~` as `%7E`.

        object_store writes an OAuth request's body form-encoded, which escapes
        `~`, where percent-encoding leaves it as it is. Every Entra secret
        holds a `~`.
        """
        secret = "Mk18Q~ab.cd_ef-gh+ij/kl=Zq9x7Pw"
        monkeypatch.setenv("SECRET", secret)
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError(
                "client_secret=Mk18Q%7Eab.cd_ef-gh%2Bij%2Fkl%3DZq9x7Pw", quotes_data=True
            ),
        )
        _with_source(
            tmp_path,
            "  path: s3://lake/a.csv\n"
            "  options: {storage_options: {azure_client_secret: '${SECRET}'}}\n",
        )

        result = _call(
            Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",)),
            {"path": "veridelta.yaml", "side": "source"},
            tool="describe_schema",
        )

        shown = _shown(result)
        assert "client_secret=***" in shown
        assert not _holds_a_form(shown, secret)
        assert "%7Eab.cd_ef" not in shown

    @pytest.mark.parametrize("signature", ["", "a"])
    def test_it_masks_no_signature_too_short_to_mask(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, signature: str
    ) -> None:
        """Ensure a recorded value with a short `sig` leaves later answers as they are.

        The value is masked whole, but a signature of 1 to 3 characters, or
        none, would mask ordinary text, or every gap between characters, in
        every answer until the server stops.
        """
        monkeypatch.setenv("VD_SAS", f"sv=2022-11-02&sig={signature}")
        _with_source(tmp_path, "  path: a.csv\n")
        (tmp_path / "a.csv").write_text("id,name\n1,alpha\n", encoding="utf-8")
        _write(
            tmp_path,
            "source:\n  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
            "  table: orders\n  password: '${VD_SAS}'\n"
            "target:\n  path: b.csv\nprimary_keys: [id]\n",
            "one.yaml",
        )

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool("validate_config", {"path": "one.yaml"}),
                await client.call_tool(
                    "describe_schema", {"path": "veridelta.yaml", "side": "source"}
                ),
            ]

        loaded, later = _with_client(Settings((tmp_path,), allowed_hosts=("db.internal",)), work)

        assert loaded.structured_content is not None
        assert loaded.structured_content["valid"] is True
        assert later.structured_content == {
            "side": "source",
            "columns": {"id": "Int64", "name": "String"},
        }

    def test_each_slot_names_a_field_of_its_model(self) -> None:
        """Ensure the slot table names real fields, and the option sets split the no-host keys."""
        models = {model.model_fields["type"].default: model for model in _SOURCE_MODELS}
        option_sets = [
            mcp_server._SECRET_OPTIONS,
            mcp_server._ACCOUNT_KEY_OPTIONS,
            mcp_server._LOGIN_OPTIONS,
            mcp_server._SAS_OPTIONS,
            mcp_server._SWITCH_OPTIONS,
        ]

        for block_type, field in mcp_server._FIELD_SLOTS:
            assert field in models[block_type].model_fields
        assert sum(map(len, option_sets)) == len(mcp_server._NO_HOST_OPTIONS)
        assert frozenset().union(*option_sets) == mcp_server._NO_HOST_OPTIONS

    @pytest.mark.parametrize(
        ("block_type", "field"),
        [
            pytest.param(model.model_fields["type"].default, field, id=f"{model.__name__}-{field}")
            for model in _SOURCE_MODELS
            for field in model.model_fields
            if (model.model_fields["type"].default, field) not in mcp_server._FIELD_SLOTS
        ],
    )
    def test_it_refuses_a_reference_in_every_other_field(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, block_type: str, field: str
    ) -> None:
        """Ensure each model field that is no credential slot takes no `${NAME}`.

        The refusal comes before validation, so the field's own type does not
        matter.
        """
        monkeypatch.setenv("VD_X", _KEY)
        block = {"type": block_type, field: "${VD_X}"}
        _with_source(tmp_path, "".join(f"  {key}: '{value}'\n" for key, value in block.items()))

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["valid"] is False
        assert f"source -> {field} cannot take ${{VD_X}}" in report["errors"][0]

    def test_it_loads_a_file_only_through_its_own_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure the server loads a file only with its hook, never through `load_config`.

        Every module that binds `load_config` gets one that fails, and the hook
        must see the file's reference, so a load without it fails the test.
        """
        monkeypatch.setenv("PW", _SECRET)
        for module in ("veridelta", "veridelta.config", "veridelta.engine", "veridelta.cli"):
            monkeypatch.setattr(
                f"{module}.load_config", MagicMock(side_effect=AssertionError("loaded"))
            )
        hook = mocker.spy(mcp_server, "_record_and_check")
        _with_source(tmp_path, "  path: b.csv\n" + _STORE_PASSWORD)
        settings = Settings((tmp_path,))

        checked = _call(settings, {"path": "veridelta.yaml", "schemas": True})
        ran = _call(settings, {"path": "veridelta.yaml"}, tool="run_comparison")

        assert checked.structured_content is not None
        assert checked.structured_content["valid"] is True
        assert ran.is_error is False
        assert [call.args[0].name for call in hook.call_args_list] == ["PW", "PW"]

    @pytest.mark.parametrize("row_values", [False, True], ids=["no-rows", "rows"])
    @pytest.mark.parametrize(
        ("source", "environment"),
        [
            pytest.param(
                "  path: s3://lake/a.csv\n  options:\n    storage_options:\n"
                "      aws_access_key_id: ${V_LOGIN}\n      aws_secret_access_key: ${V_SECRET}\n"
                "      aws_session_token: ${V_TOKEN}\n",
                {
                    "V_LOGIN": "AKIAQ7ZX3LEDGER9READER",
                    "V_SECRET": "wJal+rXUtn/FEMI.K7MDENG=bPxRfiCY",
                    "V_TOKEN": "IQoJb3JpZ2luX2VjEJr//////////wEaCXVz",
                },
                id="file-s3",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://lake/t\n  storage_options:\n"
                "    AWS_SECRET_ACCESS_KEY: ${V_SECRET}\n    aws_access_key_id: ${V_LOGIN}\n",
                {"V_SECRET": "Zq8+Kx/9mN_pR2w~Tz-Q7", "V_LOGIN": "svc.delta-reader@corp"},
                id="delta",
            ),
            pytest.param(
                _ICEBERG_SAS.format(token="${V_SAS}"),
                {
                    "V_SAS": "?sv=2024-01-01&se=2031-07-15T09%3A00Z&sp=rl"
                    "&sig=QmFzZTY0U2lnbmF0dXJl%2BXyZ%2F9%3D"
                },
                id="iceberg-sas",
            ),
            pytest.param(
                "  type: iceberg\n  table_uri: abfs://lake/t/metadata/v1.metadata.json\n"
                "  storage_options:\n    adls.account-name: acct\n"
                "    adls.account-key: ${V_KEY}\n    adls.tenant-id: ${V_TENANT}\n",
                {
                    "V_KEY": "q3Zk9Lw+Xa7/Vb2NcR8tYm4Hp6Ju1Ef0Gd5Sx3Kz8Qw=",
                    "V_TENANT": "6f1c2a9e-3b7d-4e58-9a0c-ledger00tenant",
                },
                id="iceberg-account-key",
            ),
            pytest.param(
                "  type: snowflake\n  account: xy12345\n  user: ${V_USER}\n"
                "  password: ${V_SECRET}\n"
                "  warehouse: WH\n  database: DB\n  schema_name: PUBLIC\n  table: ORDERS\n",
                {
                    "V_USER": "SvcLedgerReaderQ7",
                    "V_SECRET": "Pw#9 x&y?z=Q7kLm horse battery",
                },
                id="snowflake",
            ),
            pytest.param(
                "  type: snowflake\n  account: xy12345\n  user: analyst\n"
                "  private_key_path: k.pem\n  private_key_passphrase: ${V_PHRASE}\n"
                "  warehouse: WH\n  database: DB\n  schema_name: PUBLIC\n  table: ORDERS\n",
                {"V_PHRASE": "staple Q7 horse/battery+correct"},
                id="snowflake-key-pair",
            ),
            pytest.param(
                "  type: databricks\n  server_hostname: adb-1.azuredatabricks.net\n"
                "  http_path: /sql/1.0/warehouses/1\n  table: orders\n"
                "  access_token: ${V_TOKEN}\n",
                {"V_TOKEN": "dapiQ7f3e9a1b2c4d6e8f0a1b2c3d4e5f6"},
                id="databricks",
            ),
            pytest.param(
                "  type: duckdb\n  database: 'md:sales'\n  table: t\n"
                "  motherduck_token: ${V_TOKEN}\n",
                {"V_TOKEN": "eyJhbGciOiJIUzI1NiJ9.Q7ledger.sig"},
                id="motherduck",
            ),
            pytest.param(
                "  type: database\n"
                '  uri: "postgresql://${V_USER}:${V_PASSWORD}@db.internal/sales"\n'
                "  table: orders\n",
                {"V_USER": "ledger_reader_q7", "V_PASSWORD": "Hunter2-Q7_secret.pass~x"},
                id="uri-parts",
            ),
            pytest.param(
                "  type: database\n  uri: postgresql://analyst@db.internal/sales\n"
                "  table: orders\n  password: ${V_PASSWORD}\n",
                {"V_PASSWORD": "p@ss/w#rd+%9 Q7ledger"},
                id="database-password",
            ),
            pytest.param(
                "  path: s3://lake/a.csv\n",
                {
                    "AWS_ACCESS_KEY_ID": "AKIAQ7ZX3LEDGER9READER",
                    "AWS_SECRET_ACCESS_KEY": "wJal+rXUtn/FEMI.K7MDENG=bPxRfiCY",
                    "AWS_SESSION_TOKEN": "IQoJb3JpZ2luX2VjEJr//////////wEaCXVz",
                    "AZURE_STORAGE_ACCOUNT_KEY": "bGVkZ2VyLWFjY291bnQta2V5LW9mLTMyLWJ5dGVzISE=",
                    "AZURE_CLIENT_SECRET": "Mk18Q~ab.cd_ef-gh+ij/kl=Zq9x7Pw",
                    "MOTHERDUCK_TOKEN": "eyJhbGciOiJIUzI1NiJ9.Q7ledger.sig",
                },
                id="reader-environment",
            ),
        ],
    )
    def test_no_tool_returns_any_part_of_a_value_from_the_environment(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        source: str,
        environment: dict[str, str],
        row_values: bool,
    ) -> None:
        """Ensure no answer holds six characters in a row of any value a configuration took.

        Every engine call fails with a message that echoes each value as
        written, in lowercase, percent-encoded, and as bytes in hex, as S3
        writes a request it could not verify. The check casefolds each answer
        and also reads it once percent-decoded.
        """
        for name, value in environment.items():
            monkeypatch.setenv(name, value)
        echo = " ".join(
            form
            for value in environment.values()
            for form in (value, value.lower(), quote(value, safe=""), value.encode().hex(" "))
        )
        for name in (
            "check_configs",
            "read_schema",
            "run_from_configs",
            "propose_value_maps_from_configs",
            "suggest_rules_from_configs",
        ):
            mocker.patch.object(DiffEngine, name, side_effect=ConfigError(f"echo: {echo}"))
        mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
        _with_source(tmp_path, source)
        settings = Settings(
            (tmp_path,), allow_row_values=row_values, allowed_hosts=_REFERENCE_HOSTS
        )
        calls: list[tuple[str, dict[str, Any]]] = [
            ("validate_config", {}),
            ("validate_config", {"schemas": True}),
            ("run_comparison", {}),
            ("describe_schema", {"side": "source"}),
            ("read_discrepancies", {"kind": "added"}),
            ("propose_value_maps", {}),
            ("suggest_rules", {}),
        ]

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool(tool, {"path": "veridelta.yaml", **arguments})
                for tool, arguments in calls
            ]

        results = _with_client(settings, work)

        for (tool, _), result in zip(calls, results, strict=True):
            shown = _shown(result)
            if row_values or tool in ("validate_config", "run_comparison", "describe_schema"):
                assert "echo: ***" in shown, tool
            for text in (shown.casefold(), unquote(shown).casefold()):
                for value in environment.values():
                    folded = value.casefold()
                    windows = {folded[i : i + 6] for i in range(len(folded) - 5)}
                    assert not any(window in text for window in windows), (tool, value)
                    assert value.encode().hex(" ") not in text, (tool, value)


_SLOT_VALUES: dict[str, str] = {
    "token": "IQoJb3JpZ2luX2VjEJr+Q7/ledger.tok_en~x-9=",
    "account key": base64.b64encode(b"ledger-account-key-of-32-bytes!!").decode(),
    "login": "svc.ledger-reader@corpQ7",
    "sas": "sv=2024-01-01&sp=rl&sig=QmFzZTY0U2lnbmF0dXJl%2BXyZ%2F9%3D",
}
"""A value of 20 or more characters in the shape of each slot a variable can have."""

_UNMASKABLE = [
    pytest.param("AWS_SESSION_TOKEN", "ab cd", id="token-with-a-space"),
    pytest.param("AZURE_STORAGE_ACCOUNT_KEY", "azKEY*xx", id="account-key"),
    pytest.param("MOTHERDUCK_TOKEN", "abc", id="short-token"),
    pytest.param("AWS_ACCESS_KEY_ID", "-x", id="login"),
    pytest.param("AZURE_STORAGE_SAS_TOKEN", "foo=1", id="sas"),
    pytest.param("AWS_SSE_CUSTOMER_KEY_BASE64", "QUJD=", id="base64-not-canonical"),
    pytest.param("AWS_SECRET_ACCESS_KEY", "abc\udcffdef", id="outside-utf-8"),
]
"""A reader's credential variable, each with a value of a shape the server cannot mask."""


def _echoes(value: str) -> str:
    """Write a value in each form a reader or a service can echo it in."""
    encoded = quote(value, safe="")
    return " ".join(
        (
            value,
            value.upper(),
            encoded,
            encoded.replace("~", "%7E"),
            value.encode().hex(" "),
            f"<Token-0>{value}</Token-0>",
        )
    )


def _describe(tmp_path: Path) -> CallToolResult:
    """Describe the source of the configuration in `tmp_path`, on a server that allows row values."""
    return _call(
        Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",)),
        {"path": "veridelta.yaml", "side": "source"},
        tool="describe_schema",
    )


class TestEnvironmentCredentials:
    """Validate the masking of the credentials a reader takes from the server's environment.

    A reader such as object_store or deltalake reads `AWS_SESSION_TOKEN` and its
    like on its own, with no `${NAME}` in the configuration, and a service can
    repeat such a value in an error.
    """

    def test_the_list_follows_the_option_slots(self) -> None:
        """Ensure each AWS, Azure, or Google storage option is listed as its variable."""
        slots = mcp_server._ENVIRONMENT_SLOTS

        for keys, slot in mcp_server._OPTION_SLOTS:
            for key in keys:
                if key.startswith(("aws_", "azure_", "google_")):
                    assert slots[key.upper()] == slot, key
        assert set(MOTHERDUCK_TOKEN_VARIABLES) <= set(slots)
        assert not set(mcp_server._TOKEN_FILE_VARIABLES) & set(slots)

    @pytest.mark.parametrize(
        ("name", "slot"),
        [pytest.param(name, slot, id=name) for name, slot in mcp_server._ENVIRONMENT_SLOTS.items()],
    )
    def test_it_masks_each_variable_a_reader_takes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        engine_calls: list[MagicMock],
        name: str,
        slot: str,
    ) -> None:
        """Ensure each listed variable is masked in every form an echo can take.

        The configuration names no variable, as a reader takes the value from
        the server's environment on its own.
        """
        value = _SLOT_VALUES[slot]
        monkeypatch.setenv(name, value)
        for mock in engine_calls:
            mock.side_effect = ConnectorError(f"echo: {_echoes(value)}", quotes_data=True)
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")
        settings = Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",))

        async def work(client: Client) -> list[CallToolResult]:
            return [
                await client.call_tool(
                    "describe_schema", {"path": "veridelta.yaml", "side": "source"}
                ),
                await client.call_tool("run_comparison", {"path": "veridelta.yaml"}),
                await client.call_tool(
                    "validate_config", {"path": "veridelta.yaml", "schemas": True}
                ),
            ]

        assert len(value) >= 20
        for result in _with_client(settings, work):
            shown = _shown(result)
            assert "echo: ***" in shown
            assert "<Token-0>***</Token-0>" in shown
            assert not _holds_a_form(shown, value)

    @pytest.mark.parametrize(("name", "value"), _UNMASKABLE)
    def test_it_will_not_start_on_a_value_it_cannot_mask(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, value: str
    ) -> None:
        """Ensure a listed variable of a shape the server cannot mask stops it, naming the variable.

        The refusal holds no three characters in a row of the value.
        """
        monkeypatch.setenv(name, value)

        with pytest.raises(ConfigError, match=f"the environment variable {name} must be") as error:
            build_server(Settings((tmp_path,)))

        message = str(error.value).replace(name, "")
        assert not any(value[i : i + 3] in message for i in range(len(value) - 2))

    def test_it_ignores_an_empty_or_unlisted_variable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure an empty credential and a variable outside the list neither stop nor mask.

        A region is no credential, and masking every such setting would hide
        ordinary text in every answer.
        """
        monkeypatch.setenv("AWS_SESSION_TOKEN", "")
        monkeypatch.setenv("AWS_REGION", "us-east-1-zz")
        mocker.patch.object(
            DiffEngine, "read_schema", side_effect=ConnectorError("region us-east-1-zz")
        )
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")

        result = _describe(tmp_path)

        assert "ConnectorError: region us-east-1-zz" in _text(result)

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows holds each environment variable's name in upper case, so no reader "
        "sees a name in mixed case.",
    )
    @pytest.mark.parametrize(
        ("name", "slot"),
        [
            pytest.param("AZURE_Client_Secret", "token", id="azure-client-secret"),
            pytest.param("AZURE_STORAGE_client_secret", "token", id="azure-storage-alias"),
            pytest.param("AWS_Session_Token", "token", id="aws-session-token"),
            pytest.param("GOOGLE_bearer_token", "token", id="google-bearer-token"),
            pytest.param("AZURE_Storage_Account_Key", "account key", id="azure-account-key"),
        ],
    )
    def test_it_masks_a_variable_in_any_case_after_its_prefix(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        name: str,
        slot: str,
    ) -> None:
        """Ensure a name in mixed case after `AWS_`, `AZURE_`, or `GOOGLE_` is masked.

        object_store matches the prefix as written and reads the rest of the
        name in any case, so it sends the value of `AZURE_Client_Secret`.
        """
        value = _SLOT_VALUES[slot]
        monkeypatch.setenv(name, value)
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError(f"echo: {_echoes(value)}", quotes_data=True),
        )
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")

        result = _describe(tmp_path)

        assert "echo: ***" in _shown(result)
        assert not _holds_a_form(_shown(result), value)

    def test_it_will_not_start_on_a_mixed_case_variable_it_cannot_mask(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a malformed value under a mixed-case name stops the server, naming it as set.

        Windows holds the name in upper case, so the refusal names it as
        `os.environ` holds it there.
        """
        monkeypatch.setenv("AZURE_Client_Secret", "ab cd")
        held = next(name for name in os.environ if name.upper() == "AZURE_CLIENT_SECRET")

        with pytest.raises(ConfigError, match=f"the environment variable {held} must"):
            build_server(Settings((tmp_path,)))

        assert held == ("AZURE_CLIENT_SECRET" if sys.platform == "win32" else "AZURE_Client_Secret")

    def test_it_reads_a_mixed_case_key_and_token_file(self, tmp_path: Path) -> None:
        """Ensure a Google key and a token file are read under a mixed-case name too."""
        token_file = tmp_path / "token"
        token = "eyJhbGciOiJSUzI1NiJ9.ZmVkZXJhdGVk.c2lnbmF0dXJl"
        token_file.write_text(token + "\n", encoding="utf-8")
        environ = {
            "GOOGLE_Service_Account_Key": "not json at all",
            "AZURE_Federated_Token_File": str(token_file),
        }

        assert mcp_server._environment_values(environ) == {"not json at all"}
        assert mcp_server._token_file_values(environ) == {token}

    @pytest.mark.parametrize(
        "name",
        [
            pytest.param("azure_client_secret", id="lower-case-prefix"),
            pytest.param("Aws_Session_Token", id="mixed-case-prefix"),
            pytest.param("CLIENT_SECRET", id="no-prefix"),
            pytest.param("hf_token", id="exact-name-in-another-case"),
        ],
    )
    def test_it_ignores_a_name_no_reader_takes(self, tmp_path: Path, name: str) -> None:
        """Ensure a name whose prefix is not as object_store matches it is neither checked nor kept.

        object_store matches the prefix as written, and every other reader
        reads its names as written, so none of these reaches a reader.
        """
        token_file = tmp_path / "token"
        token_file.write_text("eyJhbGciOiJSUzI1NiJ9.ZmVkZXJhdGVk.c2lnbmF0dXJl", encoding="utf-8")
        environ = {name: "ab cd", "azure_federated_token_file": str(token_file)}

        assert mcp_server._environment_values(environ) == set()
        assert mcp_server._token_file_values(environ) == set()

    def test_it_masks_the_fields_of_a_service_account_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a Google key's id, echoed on its own, is masked, and the key's whole text too."""
        key_id = "3f9c2a7e1b5d4c8a9e0f1a2b3c4d5e6f7a8b9c0d"
        key = json.dumps(
            {
                "type": "service_account",
                "private_key_id": key_id,
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANQ7ledger\n"
                "-----END PRIVATE KEY-----\n",
                "client_email": "reader@project.iam.gserviceaccount.com",
            }
        )
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_KEY", key)
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError(f"unknown key {key_id}; key {key}", quotes_data=True),
        )
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")

        result = _describe(tmp_path)

        assert "ConnectorError: unknown key ***; key ***" in _text(result)
        assert not _holds_a_form(_shown(result), key_id)
        assert "MIIEvQIBADANQ7ledger" not in _shown(result)

    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("not json at all", id="text"),
            pytest.param('["private_key_id", "3f9c2a7e1b5d"]', id="list"),
            pytest.param('{"private_key_id": "abc", "private_key": 12345}', id="short-or-no-text"),
        ],
    )
    def test_it_starts_on_any_service_account_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str
    ) -> None:
        """Ensure a key that is no JSON object, or whose fields are short or not text, starts it.

        object_store's error for such a key quotes none of it, so the server
        records the whole text and refuses nothing.
        """
        monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_KEY", key)

        assert mcp_server._environment_values(os.environ) == {key}
        assert _tools(Settings((tmp_path,)))

    def test_it_masks_a_rotated_token_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a token file is read at each call, so a rotated token is masked too.

        A projected token file rotates about hourly, so the token the server
        read as it started is not the one a reader sends an hour later.
        """
        token_file = tmp_path / "token"
        tokens = [
            "eyJhbGciOiJSUzI1NiJ9.Zmlyc3Q.c2lnbmF0dXJl",
            "eyJhbGciOiJSUzI1NiJ9.bmV4dA.c2lnbmVk",
        ]
        token_file.write_text(tokens[0] + "\n", encoding="utf-8")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))

        def echo_the_file(*_: object) -> None:
            raise ConnectorError(f"rejected {token_file.read_text().strip()}", quotes_data=True)

        mocker.patch.object(DiffEngine, "read_schema", side_effect=echo_the_file)
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")
        settings = Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",))

        async def work(client: Client) -> list[CallToolResult]:
            first = await client.call_tool(
                "describe_schema", {"path": "veridelta.yaml", "side": "source"}
            )
            token_file.write_text(tokens[1], encoding="utf-8")
            second = await client.call_tool(
                "describe_schema", {"path": "veridelta.yaml", "side": "source"}
            )
            return [first, second]

        results = _with_client(settings, work)

        for result, token in zip(results, tokens, strict=True):
            assert "ConnectorError: rejected ***" in _text(result)
            assert not _holds_a_form(_shown(result), token)

    def test_it_keeps_no_token_a_file_held_before(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure the mask of a call after a rotation holds the new token, and not the old one.

        No reader sends a token its file no longer holds, and keeping each
        one would grow the mask for as long as the server runs.
        """
        token_file = tmp_path / "token"
        tokens = [
            "eyJhbGciOiJSUzI1NiJ9.b2xk.c2lnbmF0dXJl",
            "eyJhbGciOiJSUzI1NiJ9.bmV3ZXI.c2lnbmVk",
        ]
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))
        mocker.patch.object(
            DiffEngine, "read_schema", side_effect=ConnectorError("refused", quotes_data=True)
        )
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")
        settings = Settings((tmp_path,), allow_row_values=True, allowed_hosts=("lake",))
        masked = mocker.spy(mcp_server, "_mask_pattern")

        async def mask_of_each_call(client: Client) -> list[list[frozenset[str]]]:
            used: list[list[frozenset[str]]] = []
            for content in tokens:
                token_file.write_text(content, encoding="utf-8")
                masked.reset_mock()
                await client.call_tool(
                    "describe_schema", {"path": "veridelta.yaml", "side": "source"}
                )
                used.append([call.args[0] for call in masked.call_args_list])
            return used

        first, second = _with_client(settings, mask_of_each_call)

        assert first
        assert second
        assert all(tokens[0] in values and tokens[1] not in values for values in first)
        assert all(tokens[1] in values and tokens[0] not in values for values in second)

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(None, id="missing"),
            pytest.param(b"A" * (64 * 1024 + 1), id="larger-than-64-kib"),
            pytest.param(b"eyJhbGci\xffOiJSUzI1NiJ9", id="outside-utf-8"),
            pytest.param(b"not a token at all", id="outside-the-token-shape"),
            pytest.param(b"abc", id="three-characters"),
        ],
    )
    def test_it_starts_on_a_token_file_that_holds_no_token(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: bytes | None
    ) -> None:
        """Ensure a token file the server cannot read as a token starts it and records nothing."""
        token_file = tmp_path / "token"
        if content is not None:
            token_file.write_bytes(content)
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))

        assert mcp_server._token_file_values(os.environ) == set()
        assert _tools(Settings((tmp_path,)))

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows has no named pipe in a folder.")
    def test_it_reads_no_token_file_that_is_a_named_pipe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a token file that is a named pipe is neither waited on nor read.

        A pipe with no writer would hold the server before it answers, and one
        such as `<(get-token)` gives its token once, to the first process that
        reads it, which must be the reader that signs in. The test holds the
        pipe open to read it last, so the token waits in it while the server
        starts and answers a call.
        """
        pipe = tmp_path / "token"
        os.mkfifo(pipe)
        token = b"eyJhbGciOiJSUzI1NiJ9.cGlwZQ.c2lnbmF0dXJl"
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(pipe))
        _project(tmp_path, "modern_match.csv")
        kept = os.open(pipe, os.O_RDONLY | os.O_NONBLOCK)
        writer = os.open(pipe, os.O_WRONLY)
        answered: list[CallToolResult] = []
        try:
            os.write(writer, token)
            server = threading.Thread(
                target=lambda: answered.append(
                    _call(Settings((tmp_path,)), {"path": "veridelta.yaml"})
                ),
                daemon=True,
            )
            server.start()
            server.join(timeout=60)

            assert not server.is_alive()
            assert answered[0].is_error is False
            assert os.read(kept, 1024) == token
        finally:
            # Closing the writer ends a read that waits on the pipe, so no thread is left.
            os.close(writer)
            os.close(kept)

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows has no named pipe in a folder.")
    def test_it_reads_no_pipe_swapped_in_after_its_check(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a pipe that takes a regular file's place after its check is not read."""
        pipe = tmp_path / "token"
        os.mkfifo(pipe)
        regular = tmp_path / "regular"
        regular.write_text("x", encoding="utf-8")
        stat_of = os.stat
        token = b"eyJhbGciOiJSUzI1NiJ9.c3dhcA.c2lnbmF0dXJl"
        kept = os.open(pipe, os.O_RDONLY | os.O_NONBLOCK)
        writer = os.open(pipe, os.O_WRONLY)
        try:
            os.write(writer, token)
            mocker.patch.object(
                os,
                "stat",
                side_effect=lambda path, *args, **named: stat_of(
                    regular if path == str(pipe) else path, *args, **named
                ),
            )

            values = mcp_server._token_file_values({"AWS_WEB_IDENTITY_TOKEN_FILE": str(pipe)})

            mocker.stopall()
            assert values == set()
            assert os.read(kept, 1024) == token
        finally:
            os.close(writer)
            os.close(kept)

    def test_it_reads_a_token_file_of_64_kib(self, tmp_path: Path) -> None:
        """Ensure a token file of exactly 64 KiB is read, since only a larger one is skipped."""
        token_file = tmp_path / "token"
        token_file.write_bytes(b"A" * (64 * 1024))

        values = mcp_server._token_file_values({"AZURE_FEDERATED_TOKEN_FILE": str(token_file)})

        assert values == {"A" * (64 * 1024)}

    @pytest.mark.parametrize(
        "element",
        [
            "Token-0",
            "token-12",
            "AWSAccessKeyId",
            "CanonicalRequest",
            "canonicalrequestbytes",
            "StringToSign",
            "STRINGTOSIGNBYTES",
            "SignatureProvided",
        ],
    )
    def test_it_blanks_each_element_s3_repeats_a_credential_in(self, element: str) -> None:
        """Ensure each element's content is blanked, in any case, and the text around it kept."""
        text = f"<Error><{element}>derived\nsecret</{element}><Code>X</Code></Error>"

        assert mcp_server._without_echoed_credentials(text) == (
            f"<Error><{element}>***</{element}><Code>X</Code></Error>"
        )

    def test_it_blanks_an_element_cut_at_the_end(self) -> None:
        """Ensure an element a message cuts before it closes is blanked to the end."""
        text = "<Code>InvalidToken</Code><Token-0>IQoJb3JpZ2luX2VjEJr"

        assert mcp_server._without_echoed_credentials(text) == (
            "<Code>InvalidToken</Code><Token-0>***"
        )

    def test_it_blanks_each_of_two_elements(self) -> None:
        """Ensure two elements in one message are each blanked, and nothing between them."""
        text = "<AWSAccessKeyId>AKIAQ7</AWSAccessKeyId> and <Token-1>tok</Token-1>"

        assert mcp_server._without_echoed_credentials(text) == (
            "<AWSAccessKeyId>***</AWSAccessKeyId> and <Token-1>***</Token-1>"
        )

    def test_it_keeps_the_elements_that_repeat_no_credential(self) -> None:
        """Ensure an S3 error's code, message, and request ids come back as written."""
        text = (
            "<Error><Code>AccessDenied</Code><Message>Access Denied</Message>"
            "<RequestId>4442587FB7D0A2F9</RequestId><HostId>aG9zdA==</HostId></Error>"
        )

        assert mcp_server._without_echoed_credentials(text) == text

    def test_it_blanks_a_token_it_never_saw(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Ensure a token a reader found on its own is blanked where S3 repeats it.

        Nothing is recorded, as for a token from a profile or an instance role.
        """
        mocker.patch.object(
            DiffEngine,
            "read_schema",
            side_effect=ConnectorError("<Token-0>derivedtoken</Token-0>", quotes_data=True),
        )
        _with_source(tmp_path, "  path: s3://lake/a.csv\n")

        result = _describe(tmp_path)

        assert "ConnectorError: <Token-0>***</Token-0>" in _text(result)
        assert "derivedtoken" not in _shown(result)


class TestReadRows:
    """Validate the rows `read_discrepancies` returns, without the SDK in the way.

    Each test runs in its root, as the server does, so the relative paths in
    the configuration resolve there.
    """

    def test_it_returns_nothing_without_the_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a server started without --allow-row-values refuses before it reads a row."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv")
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="started without --allow-row-values"):
            read_rows(Settings((tmp_path,)), "veridelta.yaml", "changed")

        compare.assert_not_called()

    @pytest.mark.parametrize(
        ("kind", "rows"),
        [
            ("added", [{"id": 4, "status": "open", "amount": 7.25}]),
            ("removed", [{"id": 3, "status": "open", "amount": 7.25}]),
            (
                "changed",
                [
                    {
                        "id": 2,
                        "status_source": "closed",
                        "amount_source": 20.5,
                        "status_target": "shipped",
                        "amount_target": 20.5,
                        "status_is_match": False,
                        "amount_is_match": True,
                    }
                ],
            ),
        ],
    )
    def test_it_returns_the_rows_of_each_kind(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        kind: Literal["added", "removed", "changed"],
        rows: list[dict[str, Any]],
    ) -> None:
        """Ensure each kind returns the rows a run holds for it, with the count."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv")

        report = read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", kind)

        assert report == {
            "kind": kind,
            "total": 1,
            "rows": rows,
            "truncated": False,
            "keys_only": False,
        }

    @pytest.mark.parametrize(("limit", "max_rows"), [(1, 50), (20, 1)])
    def test_it_stops_at_the_limit_or_the_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit: int, max_rows: int
    ) -> None:
        """Ensure a call gets the fewer of the rows it asks for and the server's cap."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "a.csv").write_text("id\n9\n")
        (tmp_path / "b.csv").write_text("id\n1\n2\n3\n")
        _write(tmp_path, _VALID)
        settings = Settings((tmp_path,), allow_row_values=True, max_rows=max_rows)

        report = read_rows(settings, "veridelta.yaml", "added", limit=limit)

        assert report["rows"] == [{"id": 1}]
        assert report["total"] == 3
        assert report["truncated"] is True

    def test_it_refuses_a_limit_below_one(self, tmp_path: Path) -> None:
        """Ensure a limit that would let no row through, or count from the end, is refused."""
        _write(tmp_path, _VALID)
        settings = Settings((tmp_path,), allow_row_values=True)

        with pytest.raises(ConfigError, match="limit must be at least 1, got 0"):
            read_rows(settings, "veridelta.yaml", "added", limit=0)

    def test_it_refuses_data_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
    ) -> None:
        """Ensure no row is read from a file outside the roots."""
        outside = tmp_path_factory.mktemp("outside")
        _write(
            tmp_path,
            f"source:\n  path: '{outside / 'a.csv'}'\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )
        compare = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")

        with pytest.raises(ConfigError, match="The source path"):
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "added")

        compare.assert_not_called()

    def test_it_reads_no_file_beside_another_working_directory(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure a program that serves from another folder cannot read the files there.

        The configuration under the root names its files relative to the
        working directory, so Polars would open the ones beside the program.
        """
        elsewhere = tmp_path_factory.mktemp("elsewhere")
        (elsewhere / "a.csv").write_text(f"id,note\n1,{_SECRET}\n")
        (elsewhere / "b.csv").write_text("id,note\n")
        _write(tmp_path, _VALID)
        monkeypatch.chdir(elsewhere)
        settings = Settings((tmp_path,), allow_row_values=True)

        with pytest.raises(ConfigError, match=r"The source path 'a\.csv' is outside") as caught:
            read_rows(settings, "veridelta.yaml", "removed")

        assert _SECRET not in str(caught.value)

    def test_it_refuses_an_output_path_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Ensure the run this tool makes is held to the roots, as `run_comparison` is."""
        monkeypatch.chdir(tmp_path)
        outside = tmp_path_factory.mktemp("outside")
        _project(tmp_path, "modern_drift.csv", f"output_path: '{outside}'\n")

        with pytest.raises(ConfigError, match="is outside the folders this server writes to"):
            read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "added")

    def test_it_says_when_a_pair_returns_keys_only(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a pair compared in place, which brings back keys alone, says so."""
        _write(tmp_path, _VALID)
        keys = pl.DataFrame({"id": [7]})
        summary = DiffSummary(
            total_rows_source=1,
            total_rows_target=1,
            added_count=0,
            removed_count=0,
            changed_count=1,
            is_match=False,
        )
        mocker.patch(
            "veridelta.mcp_server.DiffEngine.run_from_configs",
            return_value=DiffResult(
                summary=summary,
                added=keys.clear(),
                removed=keys.clear(),
                changed=keys,
                keys_only=True,
            ),
        )

        report = read_rows(
            Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "changed"
        )

        assert report["rows"] == [{"id": 7}]
        assert report["keys_only"] is True

    def test_it_writes_every_value_as_json_can_hold_it(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a date, a time, a decimal, bytes, and a NaN come back as text JSON can carry.

        Polars cannot write a binary column as JSON, and its failure is a panic
        that no `except Exception` catches, so the rows go through Python values.
        """
        _write(tmp_path, _VALID)
        frame = pl.DataFrame(
            {
                "id": [1],
                "day": [date(2024, 1, 2)],
                "at": [datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)],
                "clock": [time(1, 2, 3)],
                "span": [timedelta(seconds=3)],
                "amount": [Decimal("10.50")],
                "blob": [b"\x00\x01"],
                "ratio": [float("nan")],
                "sizes": [[1, 2]],
            },
            schema_overrides={"amount": pl.Decimal(10, 2)},
        )
        summary = DiffSummary(
            total_rows_source=0,
            total_rows_target=1,
            added_count=1,
            removed_count=0,
            changed_count=0,
            is_match=False,
        )
        mocker.patch(
            "veridelta.mcp_server.DiffEngine.run_from_configs",
            return_value=DiffResult(
                summary=summary, added=frame, removed=frame.clear(), changed=frame.clear()
            ),
        )

        report = read_rows(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", "added")

        assert report["rows"] == [
            {
                "id": 1,
                "day": "2024-01-02",
                "at": "2024-01-02T03:04:05+00:00",
                "clock": "01:02:03",
                "span": "0:00:03",
                "amount": "10.50",
                "blob": "0001",
                "ratio": "NaN",
                "sizes": [1, 2],
            }
        ]
        json.dumps(report, allow_nan=False)


class TestProposeMaps:
    """Validate the proposals `propose_value_maps` returns, without the SDK in the way."""

    def test_it_returns_nothing_without_the_flag(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a server started without --allow-row-values refuses before it reads a row."""
        _coded(tmp_path)
        propose = mocker.patch("veridelta.mcp_server.DiffEngine.propose_value_maps_from_configs")

        with pytest.raises(ConfigError, match="started without --allow-row-values"):
            propose_maps(Settings((tmp_path,)), "veridelta.yaml")

        propose.assert_not_called()

    def test_it_runs_no_query_unless_the_server_allows_queries(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a proposal reads no side through a query the server does not allow."""
        _queried(tmp_path, "SELECT * FROM t")
        propose = mocker.patch("veridelta.mcp_server.DiffEngine.propose_value_maps_from_configs")

        with pytest.raises(ConfigError, match="The source reads through a query"):
            propose_maps(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        propose.assert_not_called()

    def test_it_returns_what_crosswalk_prints(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure each proposal is the object `crosswalk --json` prints for it."""
        monkeypatch.chdir(tmp_path)
        path = _coded(tmp_path)

        report = propose_maps(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        printed = [
            proposal.model_dump(mode="json")
            for proposal in DiffEngine.propose_value_maps_from_configs(*load_config(path))
        ]
        assert report == {"proposals": printed, "total": 1, "truncated": False}
        assert report["proposals"][0]["value_map"] == {"M": "Male", "F": "Female"}

    def test_it_passes_the_thresholds_through(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a support no value reaches leaves nothing to propose."""
        monkeypatch.chdir(tmp_path)
        _coded(tmp_path)

        report = propose_maps(
            Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", min_support=6
        )

        assert report == {"proposals": [], "total": 0, "truncated": False}

    def test_it_leaves_out_a_proposal_past_the_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a proposal comes back whole or not at all, within the server's cap."""
        monkeypatch.chdir(tmp_path)
        _coded(tmp_path)

        report = propose_maps(
            Settings((tmp_path,), allow_row_values=True, max_rows=1), "veridelta.yaml"
        )

        assert report == {"proposals": [], "total": 1, "truncated": True}

    def test_it_refuses_data_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
    ) -> None:
        """Ensure no value is read from a file outside the roots."""
        outside = tmp_path_factory.mktemp("outside")
        _write(
            tmp_path,
            f"source:\n  path: a.csv\ntarget:\n  path: '{outside / 'b.csv'}'\nprimary_keys: [id]\n",
        )
        propose = mocker.patch("veridelta.mcp_server.DiffEngine.propose_value_maps_from_configs")

        with pytest.raises(ConfigError, match="The target path"):
            propose_maps(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        propose.assert_not_called()


class TestSuggest:
    """Validate the suggestions `suggest_rules` returns, without the SDK in the way."""

    def test_it_returns_nothing_without_the_flag(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a server started without --allow-row-values refuses before it reads a row."""
        _near(tmp_path)
        suggest_rules = mocker.patch("veridelta.mcp_server.DiffEngine.suggest_rules_from_configs")

        with pytest.raises(ConfigError, match="suggest_rules returns values from the data"):
            suggest(Settings((tmp_path,)), "veridelta.yaml")

        suggest_rules.assert_not_called()

    def test_it_runs_no_query_unless_the_server_allows_queries(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a suggestion reads no side through a query the server does not allow."""
        _queried(tmp_path, "SELECT * FROM t")
        suggest_rules = mocker.patch("veridelta.mcp_server.DiffEngine.suggest_rules_from_configs")

        with pytest.raises(ConfigError, match="The source reads through a query"):
            suggest(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        suggest_rules.assert_not_called()

    def test_it_returns_what_suggest_prints(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure each suggestion is the object `suggest --json` prints for it."""
        monkeypatch.chdir(tmp_path)
        path = _near(tmp_path)

        report = suggest(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        printed = [
            suggestion.model_dump(mode="json")
            for suggestion in DiffEngine.suggest_rules_from_configs(*load_config(path))
        ]
        assert report == {"suggestions": printed, "total": 1, "truncated": False}
        assert report["suggestions"][0]["column"] == "amount"
        assert report["suggestions"][0]["examples"] == [{"id": 1}, {"id": 3}]

    def test_it_passes_the_share_through(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a share no gap fits under leaves nothing to suggest."""
        monkeypatch.chdir(tmp_path)
        _near(tmp_path)

        report = suggest(
            Settings((tmp_path,), allow_row_values=True), "veridelta.yaml", max_share=0.00001
        )

        assert report == {"suggestions": [], "total": 0, "truncated": False}

    def test_it_leaves_out_a_suggestion_past_the_cap(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a suggestion comes back whole or not at all, within the cap on example keys."""
        monkeypatch.chdir(tmp_path)
        _near(tmp_path)

        report = suggest(Settings((tmp_path,), allow_row_values=True, max_rows=1), "veridelta.yaml")

        assert report == {"suggestions": [], "total": 1, "truncated": True}

    def test_it_refuses_data_outside_the_roots(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        mocker: MockerFixture,
    ) -> None:
        """Ensure no value is read from a file outside the roots."""
        outside = tmp_path_factory.mktemp("outside")
        _write(
            tmp_path,
            f"source:\n  path: a.csv\ntarget:\n  path: '{outside / 'b.csv'}'\nprimary_keys: [id]\n",
        )
        suggest_rules = mocker.patch("veridelta.mcp_server.DiffEngine.suggest_rules_from_configs")

        with pytest.raises(ConfigError, match="The target path"):
            suggest(Settings((tmp_path,), allow_row_values=True), "veridelta.yaml")

        suggest_rules.assert_not_called()


class TestServer:
    """Validate the server as a host sees it, through the SDK's client."""

    def test_it_lists_each_tool_with_its_arguments_and_result(self, tmp_path: Path) -> None:
        """Ensure the host sees each tool, what it takes, and what it returns.

        The SDK drops a tool's output schema without a word when pydantic cannot
        build one, as for a `typing.TypedDict` nested in another on Python 3.11,
        and the tool then returns text alone. So every tool is held to one.
        """
        tools = {tool.name: tool for tool in _tools(Settings((tmp_path,)))}

        assert list(tools) == [
            "validate_config",
            "run_comparison",
            "describe_schema",
            "read_discrepancies",
            "propose_value_maps",
            "suggest_rules",
        ]
        assert [name for name, tool in tools.items() if tool.output_schema is None] == []
        validate, run = tools["validate_config"], tools["run_comparison"]
        describe = tools["describe_schema"]
        assert validate.input_schema["required"] == ["path"]
        assert set(validate.input_schema["properties"]) == {"path", "schemas", "allow_missing_env"}
        assert validate.output_schema is not None
        assert set(validate.output_schema["properties"]) == {
            "config",
            "valid",
            "errors",
            "warnings",
            "findings",
        }
        assert run.input_schema["required"] == ["path"]
        assert set(run.input_schema["properties"]) == {"path"}
        assert run.output_schema is not None
        assert set(run.output_schema["properties"]) == RunReport.__required_keys__
        assert describe.input_schema["required"] == ["path", "side"]
        assert describe.input_schema["properties"]["side"]["enum"] == ["source", "target"]
        assert describe.output_schema is not None
        assert set(describe.output_schema["properties"]) == {"side", "columns"}
        read, propose = tools["read_discrepancies"], tools["propose_value_maps"]
        assert read.input_schema["required"] == ["path", "kind"]
        assert read.input_schema["properties"]["limit"]["minimum"] == 1
        assert read.output_schema is not None
        assert set(read.output_schema["properties"]) == {
            "kind",
            "total",
            "rows",
            "truncated",
            "keys_only",
        }
        assert propose.input_schema["required"] == ["path"]
        assert set(propose.input_schema["properties"]) == {
            "path",
            "min_confidence",
            "min_support",
            "sample_fraction",
        }
        assert propose.output_schema is not None
        assert set(propose.output_schema["properties"]) == {
            "proposals",
            "total",
            "truncated",
        }
        suggested = tools["suggest_rules"]
        assert suggested.input_schema["required"] == ["path"]
        assert set(suggested.input_schema["properties"]) == {"path", "max_share"}
        assert suggested.input_schema["properties"]["max_share"]["exclusiveMinimum"] == 0
        assert suggested.output_schema is not None
        assert set(suggested.output_schema["properties"]) == {
            "suggestions",
            "total",
            "truncated",
        }

    def test_it_keeps_every_description_within_its_budget(self, tmp_path: Path) -> None:
        """Ensure each description fits the budget and reads the same on every Python.

        Python 3.13 strips a docstring's indentation and 3.11 keeps it, so a
        description taken from `__doc__` would differ by version.
        """
        for tool in _tools(Settings((tmp_path,))):
            assert tool.description is not None
            assert len(tool.description) <= _DESCRIPTION_BUDGET, tool.name
            assert "\n " not in tool.description, tool.name
        assert len(INSTRUCTIONS) <= _INSTRUCTIONS_BUDGET

    def test_it_introduces_itself(self, tmp_path: Path) -> None:
        """Ensure the host learns the server's name, version, and instructions."""

        async def work(client: Client) -> tuple[str, str, str | None]:
            info = client.server_info
            assert info is not None
            return info.name, info.version, client.instructions

        assert _with_client(Settings((tmp_path,)), work) == (
            "veridelta",
            __version__,
            INSTRUCTIONS,
        )

    def test_it_tells_the_host_what_each_tool_does(self, tmp_path: Path) -> None:
        """Ensure the instructions name the row-value gate, and sampling reads as the CLI says.

        `sample_fraction` samples source rows by a hash of their keys before the
        join, as `--sample-fraction` does, not rows of the join.
        """
        tools = {tool.name: tool for tool in _tools(Settings((tmp_path,)))}
        sample = tools["propose_value_maps"].input_schema["properties"]["sample_fraction"]

        assert "Three tools return row values, only when the server allows them." in INSTRUCTIONS
        assert sample["description"] == "Share of source rows to read, chosen by primary key."

    def test_it_returns_what_validate_prints(self, tmp_path: Path) -> None:
        """Ensure the structured result is the object `validate --json` prints."""
        _write(tmp_path, _VALID)
        settings = Settings((tmp_path,))

        result = _call(settings, {"path": "veridelta.yaml"})

        assert result.is_error is False
        assert result.structured_content == check_configuration(settings, "veridelta.yaml")

    def test_it_passes_the_flags_through(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `allow_missing_env` reaches the check, so an unset variable only warns."""
        monkeypatch.delenv("VD_MCP_PASSWORD", raising=False)
        _write(tmp_path, _PASSWORD_FROM_ENV)

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml", "allow_missing_env": True})

        assert result.structured_content is not None
        assert result.structured_content["valid"] is True

    def test_it_runs_a_comparison(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure `run_comparison` returns the run's report as its structured result."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv")
        settings = Settings((tmp_path,))

        result = _call(settings, {"path": "veridelta.yaml"}, tool="run_comparison")

        assert result.is_error is False
        assert result.structured_content == run_configuration(settings, "veridelta.yaml")

    def test_it_fails_a_run_that_cannot_start(self, tmp_path: Path) -> None:
        """Ensure a broken file fails `run_comparison`, named as `run --json` names it."""
        _write(tmp_path, "source: {}\n")

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")

        assert result.is_error is True
        assert "ConfigError: Configuration must contain both" in _text(result)

    def test_it_describes_a_side(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Ensure `describe_schema` returns the side's columns as its structured result."""
        monkeypatch.chdir(tmp_path)
        _project(tmp_path, "modern_drift.csv")
        settings = Settings((tmp_path,))

        result = _call(
            settings, {"path": "veridelta.yaml", "side": "target"}, tool="describe_schema"
        )

        assert result.is_error is False
        assert result.structured_content == describe_side(settings, "veridelta.yaml", "target")

    def test_it_refuses_a_side_that_is_neither(self, tmp_path: Path) -> None:
        """Ensure a side other than `source` or `target` fails the call before anything is read."""
        _write(tmp_path, _VALID)

        result = _call(
            Settings((tmp_path,)),
            {"path": "veridelta.yaml", "side": "both"},
            tool="describe_schema",
        )

        assert result.is_error is True
        assert "side" in _text(result)

    def test_it_never_returns_a_line_of_a_file_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure no tool that opens a side returns a column name read from outside the roots."""
        _write(tmp_path, _header_from(tmp_path_factory.mktemp("outside")))
        settings = Settings((tmp_path,))
        calls: list[tuple[str, dict[str, Any]]] = [
            ("validate_config", {"path": "veridelta.yaml", "schemas": True}),
            ("run_comparison", {"path": "veridelta.yaml"}),
            ("describe_schema", {"path": "veridelta.yaml", "side": "source"}),
        ]

        for tool, arguments in calls:
            result = _call(settings, arguments, tool=tool)

            assert result.is_error is True, tool
            assert "ConfigError: The source path" in _text(result), tool
            assert _SECRET not in _text(result), tool

    def test_it_fails_the_call_for_a_host_it_does_not_allow(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure no tool that opens a side sends an environment value to a host not allowed."""
        monkeypatch.setenv("VD_MCP_SECRET", _SECRET)
        _remote(
            tmp_path,
            "  path: http://127.0.0.1:1/a.csv\n"
            "  options: {storage_options: {bearer_token: '${VD_MCP_SECRET}'}}\n",
        )
        settings = Settings((tmp_path,), allow_row_values=True)
        calls: list[tuple[str, dict[str, Any]]] = [
            ("validate_config", {"path": "veridelta.yaml", "schemas": True}),
            ("run_comparison", {"path": "veridelta.yaml"}),
            ("describe_schema", {"path": "veridelta.yaml", "side": "target"}),
            ("read_discrepancies", {"path": "veridelta.yaml", "kind": "added"}),
            ("propose_value_maps", {"path": "veridelta.yaml"}),
            ("suggest_rules", {"path": "veridelta.yaml"}),
        ]

        for tool, arguments in calls:
            result = _call(settings, arguments, tool=tool)

            assert result.is_error is True, tool
            assert "ConfigError: The target path names a host" in _text(result), tool

    def test_it_fails_to_describe_a_query(self, tmp_path: Path) -> None:
        """Ensure a side read through a query fails the call, named as `run --json` names it."""
        _write(tmp_path, _QUERY)

        result = _call(
            Settings((tmp_path,), allowed_hosts=(_QUERY_HOST,)),
            {"path": "veridelta.yaml", "side": "source"},
            tool="describe_schema",
        )

        assert result.is_error is True
        assert "ConfigError: A schema probe reads a 'table'" in _text(result)

    def test_it_refuses_row_values_unless_allowed(self, tmp_path: Path) -> None:
        """Ensure each row tool fails the call on a default server, naming the flag."""
        _coded(tmp_path)
        settings = Settings((tmp_path,))

        for tool, arguments in (
            ("read_discrepancies", {"path": "veridelta.yaml", "kind": "added"}),
            ("propose_value_maps", {"path": "veridelta.yaml"}),
            ("suggest_rules", {"path": "veridelta.yaml"}),
        ):
            result = _call(settings, arguments, tool=tool)

            assert result.is_error is True, tool
            assert f"ConfigError: {tool} returns values from the data" in _text(result)

    def test_it_returns_rows_when_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an allowed server returns the rows and the proposals as structured results."""
        monkeypatch.chdir(tmp_path)
        _coded(tmp_path)
        settings = Settings((tmp_path,), allow_row_values=True)

        rows = _call(
            settings, {"path": "veridelta.yaml", "kind": "changed"}, tool="read_discrepancies"
        )
        maps = _call(settings, {"path": "veridelta.yaml"}, tool="propose_value_maps")

        assert rows.is_error is False
        assert rows.structured_content == read_rows(settings, "veridelta.yaml", "changed")
        assert maps.is_error is False
        assert maps.structured_content == propose_maps(settings, "veridelta.yaml")

    def test_it_returns_suggestions_when_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure an allowed server returns the suggestions, with their example keys."""
        monkeypatch.chdir(tmp_path)
        _near(tmp_path)
        settings = Settings((tmp_path,), allow_row_values=True)

        result = _call(settings, {"path": "veridelta.yaml", "max_share": 0.5}, tool="suggest_rules")

        assert result.is_error is False
        assert result.structured_content == suggest(settings, "veridelta.yaml", max_share=0.5)
        assert result.structured_content is not None
        assert result.structured_content["total"] == 1

    def test_it_reports_a_broken_file_as_a_finding(self, tmp_path: Path) -> None:
        """Ensure a file that does not load is a result the agent can act on, not a failed call."""
        _write(tmp_path, "source: {}\n")

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"})

        assert result.is_error is False
        assert result.structured_content is not None
        assert result.structured_content["valid"] is False

    def test_it_fails_the_call_for_a_path_outside_the_roots(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a refused path fails the call, naming the error's type and the roots."""
        outside = _write(tmp_path_factory.mktemp("outside"), _VALID)

        result = _call(Settings((tmp_path,)), {"path": str(outside)})

        assert result.is_error is True
        assert "ConfigError: " in _text(result)
        assert str(tmp_path.resolve()) in _text(result)

    def test_it_names_an_unexpected_failure_by_type_and_message(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure any failure reaches the agent as `run --json` names one, not a bare refusal."""
        mocker.patch.object(DiffEngine, "check_configs", side_effect=RuntimeError("boom"))
        _write(tmp_path, _VALID)

        result = _call(Settings((tmp_path,), allow_row_values=True), {"path": "veridelta.yaml"})

        assert result.is_error is True
        assert _text(result).endswith("RuntimeError: boom")

    def test_it_leaves_out_the_message_of_an_unexpected_failure_unless_rows_are_allowed(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a message from Polars or a driver, which can quote a cell, stays out by default."""
        mocker.patch("veridelta.engine.DiffEngine.check_configs", side_effect=RuntimeError(_CANARY))
        _write(tmp_path, _VALID)

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"})

        assert result.is_error is True
        assert "RuntimeError: its message is left out" in _text(result)
        assert "--allow-row-values" in _text(result)
        assert _CANARY not in _text(result)

    def test_it_leaves_out_a_cell_a_schema_check_quotes_unless_rows_are_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure `validate_config` with `schemas` keeps a JSON value out of its findings.

        The finding is the call's result rather than a failure, so it never
        passes through the check that a failed call's message gets.
        """
        monkeypatch.chdir(tmp_path)
        rows = ",".join(f'{{"id": {i}, "v": "x"}}' for i in range(200))
        (tmp_path / "a.json").write_text(f'[{rows}, {{"id": 999, "v": {{"ssn": "{_CANARY}"}}}}]')
        (tmp_path / "b.json").write_text(f"[{rows}]")
        _write(
            tmp_path,
            "source: {path: a.json, format: json}\ntarget: {path: b.json, format: json}\n"
            "primary_keys: [id]\n",
        )
        arguments = {"path": "veridelta.yaml", "schemas": True}

        default = _call(Settings((tmp_path,)), arguments)
        allowed = _call(Settings((tmp_path,), allow_row_values=True), arguments)

        assert default.is_error is False
        assert "ConnectorError: its message is left out" in _text(default)
        assert _CANARY not in _text(default)
        assert _CANARY in _text(allowed)

    def test_it_never_returns_a_cell_a_cast_cannot_convert(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a failed `cast_to` names the column and a count, on any server."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "a.csv").write_text(f"id,amount\n1,10\n2,{_CANARY}\n")
        (tmp_path / "b.csv").write_text("id,amount\n1,10\n2,11\n")
        _write(tmp_path, _VALID + "rules: [{column_names: [amount], cast_to: Int64}]\n")

        for settings in (Settings((tmp_path,)), Settings((tmp_path,), allow_row_values=True)):
            result = _call(settings, {"path": "veridelta.yaml"}, tool="run_comparison")

            assert result.is_error is True
            assert "DataIntegrityError: Column 'amount' sets cast_to='Int64', but 1 value" in _text(
                result
            )
            assert _CANARY not in _text(result)

    def test_it_leaves_out_a_cell_that_fails_to_parse_unless_rows_are_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a CSV value past the rows the reader infers types from stays out by default.

        Polars quotes the value it cannot parse, and a third party who writes
        the file chooses that text.
        """
        monkeypatch.chdir(tmp_path)
        rows = "".join(f"{i},{i}\n" for i in range(200))
        (tmp_path / "a.csv").write_text(f"id,amount\n{rows}200,{_CANARY}\n")
        (tmp_path / "b.csv").write_text(f"id,amount\n{rows}")
        _write(tmp_path, _VALID)

        default = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")
        allowed = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": "veridelta.yaml"},
            tool="run_comparison",
        )

        assert default.is_error is True
        assert _text(default).endswith(
            "ComputeError: its message is left out, since it can quote values from the data, "
            "and this server was started without --allow-row-values. Run the configuration "
            "with veridelta run to read the message."
        )
        assert allowed.is_error is True
        assert _CANARY in _text(allowed)

    def test_it_leaves_out_a_cell_a_pushdown_cast_cannot_convert_unless_rows_are_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure DuckDB's error for a failed `CAST`, which quotes the value, stays out by default."""
        monkeypatch.chdir(tmp_path)
        with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
            connection.execute(
                f"CREATE TABLE src AS SELECT * FROM (VALUES (1, '10'), (2, '{_CANARY}')) t(id, amount)"
            )
            connection.execute(
                "CREATE TABLE tgt AS SELECT * FROM (VALUES (1, '10'), (2, '11')) t(id, amount)"
            )
        side = "  type: duckdb\n  database: warehouse.duckdb\n  pushdown: true\n"
        _write(
            tmp_path,
            f"source:\n{side}  table: src\ntarget:\n{side}  table: tgt\nprimary_keys: [id]\n"
            "rules: [{column_names: [amount], cast_to: Int64}]\n",
        )

        default = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")
        allowed = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": "veridelta.yaml"},
            tool="run_comparison",
        )

        assert "ConnectorError: its message is left out" in _text(default)
        assert _CANARY not in _text(default)
        assert _CANARY in _text(allowed)

    def test_it_leaves_out_a_driver_message_unless_rows_are_allowed(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a source error that quotes its driver stays out, and one in Veridelta's words stays."""
        _write(tmp_path, _VALID)
        run = mocker.patch("veridelta.mcp_server.DiffEngine.run_from_configs")
        quoted = ConnectorError(f"Warehouse statement failed: {_CANARY}", quotes_data=True)

        run.side_effect = quoted
        default = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")
        allowed = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": "veridelta.yaml"},
            tool="run_comparison",
        )
        run.side_effect = ConnectorError("Pushdown needs SQL Server 2022.")
        own = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")

        assert "ConnectorError: its message is left out" in _text(default)
        assert _CANARY not in _text(default)
        assert _text(allowed).endswith(f"ConnectorError: Warehouse statement failed: {_CANARY}")
        assert _text(own).endswith("ConnectorError: Pushdown needs SQL Server 2022.")

    @pytest.mark.parametrize("kind", ["iceberg", "delta"])
    def test_it_leaves_out_a_lakehouse_reader_message_unless_rows_are_allowed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
    ) -> None:
        """Ensure a reader's message about a lakehouse table's files stays out by default.

        PyIceberg quotes a file named as the table's metadata as it refuses
        it, and deltalake quotes a value in the log it cannot parse, so either
        message can hold a cell of a file named in the table's place.
        """
        monkeypatch.chdir(tmp_path)
        if kind == "iceberg":
            (tmp_path / "rows.metadata.json").write_text(
                json.dumps([{"id": 1, "ssn": _CANARY}]), encoding="utf-8"
            )
            table_uri = "./rows.metadata.json"
        else:
            log = tmp_path / "events" / "_delta_log"
            log.mkdir(parents=True)
            (log / "00000000000000000000.json").write_text(
                json.dumps({"protocol": {"minReaderVersion": _CANARY, "minWriterVersion": 2}})
                + "\n",
                encoding="utf-8",
            )
            table_uri = "events"
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            f"target:\n  type: {kind}\n  table_uri: '{table_uri}'\nprimary_keys: [id]\n",
        )

        default = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")
        allowed = _call(
            Settings((tmp_path,), allow_row_values=True),
            {"path": "veridelta.yaml"},
            tool="run_comparison",
        )

        checked = _call(Settings((tmp_path,)), {"path": "veridelta.yaml", "schemas": True})

        assert default.is_error is True
        assert "ConnectorError: its message is left out" in _text(default)
        assert _CANARY not in _text(default)
        assert allowed.is_error is True
        assert "could not be listed" in _text(allowed)
        assert _CANARY in _text(allowed)
        assert "ConnectorError: its message is left out" in _text(checked)
        assert _CANARY not in _text(checked)

    def test_it_returns_a_lakehouse_refusal_in_its_own_words_on_any_server(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a table refused for what its log records, in Veridelta's words, keeps the message."""
        monkeypatch.chdir(tmp_path)
        _with_a_sidecar(tmp_path / "events", "sidecar.parquet", "classic")
        _ROW.write_parquet(tmp_path / "a.parquet")
        _write(
            tmp_path,
            "source:\n  path: a.parquet\n  format: parquet\n"
            "target:\n  type: delta\n  table_uri: events\nprimary_keys: [id]\n",
        )

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")

        assert result.is_error is True
        assert "could not be listed" in _text(result)
        assert "checkpoint sidecar" in _text(result)

    def test_it_never_returns_a_secret_from_the_configuration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a password read from the environment stays out of a result, even in an error.

        The database read fails with the full connection URI, as a driver's
        error can, and the result still holds only the masked form. The server
        allows row values, or the driver's message would be left out whole.
        """
        monkeypatch.setenv("VD_MCP_PASSWORD", _SECRET)
        uri = f"postgresql://analyst:{_SECRET}@db.internal/sales"
        mocker.patch(
            "veridelta.connectors.database.pl.read_database_uri",
            side_effect=RuntimeError(f"could not connect to {uri}"),
        )
        (tmp_path / "b.csv").write_text("id\n1\n")
        _write(
            tmp_path,
            "source:\n  type: database\n"
            '  uri: "postgresql://analyst:${VD_MCP_PASSWORD}@db.internal/sales"\n'
            "  table: orders\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        result = _call(
            Settings((tmp_path,), allowed_hosts=("db.internal",), allow_row_values=True),
            {"path": "veridelta.yaml", "schemas": True},
        )

        shown = json.dumps(result.structured_content) + _text(result)
        assert result.structured_content is not None
        assert result.structured_content["valid"] is False
        assert "***" in shown
        assert _SECRET not in shown

    def test_it_masks_each_value_the_configuration_took_from_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a value from `${NAME}` comes back as `***`, in an error or in a result.

        A reader names the value in its error, and a column named after it
        names it in the schema, here through mocks of each.
        """
        monkeypatch.setenv("VD_MCP_SECRET", _SECRET)
        mocker.patch.object(
            DiffEngine,
            "run_from_configs",
            side_effect=ConfigError(f"The csv file '{_SECRET}.csv' does not exist."),
        )
        mocker.patch.object(
            DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64, _SECRET: pl.Int64})
        )
        _remote(
            tmp_path,
            "  path: s3://lake/a.csv\n"
            "  options: {storage_options: {aws_secret_access_key: '${VD_MCP_SECRET}'}}\n",
        )
        settings = Settings((tmp_path,), allowed_hosts=("lake",))

        failed = _call(settings, {"path": "veridelta.yaml"}, tool="run_comparison")
        described = _call(
            settings, {"path": "veridelta.yaml", "side": "target"}, tool="describe_schema"
        )

        assert failed.is_error is True
        assert "***.csv" in _text(failed)
        assert described.structured_content == {
            "side": "target",
            "columns": {"id": "Int64", "***": "Int64"},
        }
        for result in (failed, described):
            assert _SECRET not in json.dumps(result.structured_content) + _text(result)

    def test_it_fails_the_call_when_polars_panics(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a panic fails one call, named, instead of stopping the server."""
        _write(tmp_path, _VALID)
        mocker.patch(
            "veridelta.mcp_server.DiffEngine.run_from_configs",
            side_effect=PanicException("not yet implemented"),
        )

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"}, tool="run_comparison")

        assert result.is_error is True
        assert "PanicException: its message is left out" in _text(result)

    def test_it_leaves_the_root_logger_as_it_found_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure building the server does not configure logging for the whole program.

        The SDK's constructor calls `logging.basicConfig`, which acts only on a
        root logger with no handler, as in a real run of `veridelta mcp`. Pytest
        puts its own handler there, so the test removes it first. Left in place,
        the SDK's handler would print every INFO record without `--verbose`.
        """
        root = logging.getLogger()
        level = root.level
        monkeypatch.setattr(root, "handlers", [])

        build_server(Settings((tmp_path,)))

        assert root.handlers == []
        assert root.level == level


class TestSdkBinding:
    """Validate how the module reaches the SDK, which the `mcp` extra installs."""

    def test_it_explains_a_missing_extra_as_the_install_command(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a missing SDK reads as the install hint, with no import trace attached."""
        mocker.patch.object(mcp_server, "sdk", None)
        mocker.patch(
            "veridelta.mcp_server.importlib.import_module",
            side_effect=ModuleNotFoundError("No module named 'mcp'"),
        )

        with pytest.raises(VerideltaError, match=r"uv add 'veridelta\[mcp\]'") as missing:
            build_server(Settings((tmp_path,)))

        assert missing.value.__cause__ is None

    def test_it_builds_on_the_sdk_it_is_given(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Ensure a set `sdk` attribute is used as it is, which is how tests stand one in."""
        fake = MagicMock()
        mocker.patch.object(mcp_server, "sdk", fake)

        server = build_server(Settings((tmp_path,)))

        fake.server.assert_called_once_with(
            "veridelta", instructions=INSTRUCTIONS, version=__version__
        )
        assert server is fake.server.return_value

    def test_it_serves_over_stdio(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Ensure `serve` runs the server it builds on the stdio transport."""
        built = mocker.patch("veridelta.mcp_server.build_server")
        settings = Settings((tmp_path,))

        serve(settings)

        built.assert_called_once_with(settings)
        built.return_value.run.assert_called_once_with(transport="stdio")


_ENDPOINT_SIDES: Mapping[str, tuple[str, ...]] = {
    "s3": ("  path: s3://lake/a.parquet\n", "  type: delta\n  table_uri: s3://lake/t\n"),
    "azure": ("  path: az://lake/a.parquet\n", "  type: delta\n  table_uri: abfss://lake/t\n"),
    "gcs": ("  path: gs://lake/a.parquet\n",),
    "bigquery": ("  type: bigquery\n  project: analytics\n  table: sales.orders\n",),
}
"""For each store, the sides whose reader takes an endpoint from its variables."""

_STORE_TITLES: Mapping[str, str] = {
    "s3": "S3",
    "azure": "Azure",
    "gcs": "Google Cloud Storage",
    "bigquery": "BigQuery",
}
"""How a refusal names each store."""

_ENVIRONMENT_ENDPOINT_HOSTS = ("lake", "minio.internal", "h", "xy12345.snowflakecomputing.com")
"""The hosts the environment endpoint tests' server allows."""

_ENDPOINT_VALUES = (
    ("http://minio.internal:9000", "names its endpoint by a scheme other than https", "plain-http"),
    (
        "https://leak-probe.example:9000",
        "names an endpoint whose host is not given with --allow-host",
        "host-not-allowed",
    ),
    (
        "https://minio.internal:9000/other",
        "holds a path, a query, or a fragment after its host",
        "path",
    ),
    ("https://minio.internal:9000/", None, "allowed"),
)
"""Each endpoint a variable can name, how its refusal reads, or None where the side is read, and its id."""

_ENVIRONMENT_ENDPOINT_CASES = [
    pytest.param(name, store, side, value, refusal, id=f"{name}-{store}{index}-{value_id}")
    for name, store in mcp_server._ENVIRONMENT_ENDPOINTS.items()
    for index, side in enumerate(_ENDPOINT_SIDES[store])
    for value, refusal, value_id in _ENDPOINT_VALUES
]
"""Each variable, a side of its store, and each endpoint it can name."""


def _endpoint_settings() -> Settings:
    """Return the settings of the environment endpoint tests' server, which reads under the working folder."""
    return Settings((Path.cwd(),), allowed_hosts=_ENVIRONMENT_ENDPOINT_HOSTS)


def _read_stubbed(mocker: MockerFixture) -> MagicMock:
    """Stub each reader a side reaches, and return the stub of the schema read, which records each call."""
    mocker.patch.object(mcp_server, "_table_files", side_effect=lambda *_: iter(()))
    return mocker.patch.object(DiffEngine, "read_schema", return_value=pl.Schema({"id": pl.Int64}))


def _describe_refused(root: Path, source: str) -> str:
    """Describe the source `source` names, through the function and through a tool, and return the refusal.

    Both refuse with the same message, which the tool's answer holds.
    """
    _with_source(root, source)
    settings = _endpoint_settings()
    with pytest.raises(ConfigError) as refused:
        describe_side(settings, "veridelta.yaml", "source")
    result = _call(settings, {"path": "veridelta.yaml", "side": "source"}, "describe_schema")
    assert result.is_error
    assert str(refused.value) in _text(result)
    return str(refused.value)


def _describe_read(root: Path, source: str, read: MagicMock) -> None:
    """Describe the source `source` names, through the function and through a tool, and ensure both read it."""
    _with_source(root, source)
    settings = _endpoint_settings()

    report = describe_side(settings, "veridelta.yaml", "source")
    result = _call(settings, {"path": "veridelta.yaml", "side": "source"}, "describe_schema")

    assert report["columns"] == {"id": "Int64"}
    assert not result.is_error, _text(result)
    assert read.call_count == 2


class TestEnvironmentEndpoints:
    """Validate the rule that holds each endpoint a reader takes from the server's environment to the allowed hosts.

    A reader takes an endpoint from the server's environment when the
    configuration names none, so each must meet the rules a storage option's
    endpoint meets: a host given with --allow-host, no path, and HTTPS.
    """

    @pytest.mark.parametrize(
        ("name", "store", "source", "value", "refusal"), _ENVIRONMENT_ENDPOINT_CASES
    )
    def test_it_holds_each_variable_to_the_endpoint_rules(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        name: str,
        store: str,
        source: str,
        value: str,
        refusal: str | None,
    ) -> None:
        """Ensure each variable reaches a reader only as an allowed host over HTTPS, with no path.

        The refusal names the variable, and never its value or its host.
        """
        read = _read_stubbed(mocker)
        monkeypatch.setenv(name, value)

        if refusal is None:
            _describe_read(tmp_path, source, read)
            return
        message = _describe_refused(tmp_path, source)

        assert message.startswith(f"The source side reads {_STORE_TITLES[store]}, and ")
        assert refusal in message
        assert f"the server's environment variable {name}" in message
        assert "leak-probe.example" not in message
        assert "minio.internal" not in message
        read.assert_not_called()

    @pytest.mark.parametrize("name", ["aws_endpoint_url", "AWS_Endpoint_Url"])
    def test_it_reads_a_name_in_any_case(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, name: str
    ) -> None:
        """Ensure a variable is matched in any case, as deltalake reads one.

        Windows holds every name in upper case, so the message is compared in casefold.
        """
        read = _read_stubbed(mocker)
        monkeypatch.setenv(name, "http://minio.internal:9000")

        message = _describe_refused(tmp_path, "  type: delta\n  table_uri: s3://lake/t\n")

        assert f"the server's environment variable {name}".casefold() in message.casefold()
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("source", "environment"),
        [
            pytest.param("  path: a.csv\n", {"AWS_ENDPOINT_URL": "http://x.example"}, id="local"),
            pytest.param(
                "  path: s3://lake/a.parquet\n",
                {
                    "AZURE_ENDPOINT": "http://x.example",
                    "BIGQUERY_EMULATOR_HOST": "http://x.example",
                },
                id="other-stores",
            ),
            pytest.param(
                f"{_SNOWFLAKE}  account: xy12345\n",
                {
                    **dict.fromkeys(mcp_server._ENVIRONMENT_ENDPOINTS, "http://x.example"),
                    mcp_server._EMULATOR_SWITCH: "true",
                    "MOTHERDUCK_HOST": "x.example",
                },
                id="snowflake",
            ),
            pytest.param(
                "  type: duckdb\n  database: w.duckdb\n  table: t\n",
                {"AWS_ENDPOINT_URL": "http://x.example", "MOTHERDUCK_HOST": "x.example"},
                id="duckdb-file",
            ),
        ],
    )
    def test_it_checks_only_the_variables_of_the_side_s_store(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        source: str,
        environment: dict[str, str],
    ) -> None:
        """Ensure a variable that no reader of a side takes leaves the side as it was."""
        read = _read_stubbed(mocker)
        (tmp_path / "a.csv").write_text("id\n1\n", encoding="utf-8")
        with duckdb.connect(str(tmp_path / "w.duckdb")) as connection:
            connection.execute("CREATE TABLE t AS SELECT 1 AS id")
        for name, value in environment.items():
            monkeypatch.setenv(name, value)

        _describe_read(tmp_path, source, read)

    @pytest.mark.parametrize(
        ("source", "name", "store"),
        [
            pytest.param(
                "  type: iceberg\n  table_uri: s3://lake/t/metadata/v1.metadata.json\n",
                "AWS_ENDPOINT_URL",
                "S3",
                id="iceberg",
            ),
            pytest.param(
                "  type: delta\n  table_uri: hdfs://h/t\n",
                "AZURE_ENDPOINT",
                "an object store",
                id="hdfs",
            ),
            pytest.param(
                "  type: delta\n  table_uri: s3://lake/t\n"
                "  storage_options: {aws_endpoint_url: 'https://minio.internal:9000'}\n",
                "AWS_ENDPOINT_URL",
                "S3",
                id="configured-endpoint",
            ),
        ],
    )
    def test_it_checks_every_side_that_could_take_the_variable(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        source: str,
        name: str,
        store: str,
    ) -> None:
        """Ensure an Iceberg table, a location of no known store, and a side with its own endpoint are checked.

        The check runs even when the configuration names its own endpoint,
        which a reader may prefer, since that is simpler and only stricter.
        """
        read = _read_stubbed(mocker)
        monkeypatch.setenv(name, "http://minio.internal:9000")

        message = _describe_refused(tmp_path, source)

        assert message.startswith(f"The source side reads {store}, and the server's environment")
        assert "scheme other than https" in message
        read.assert_not_called()

    @pytest.mark.parametrize("value", ["true", "On", "1", " YES "])
    def test_it_refuses_the_emulator_switch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, value: str
    ) -> None:
        """Ensure the switch that sends a reader to Azurite over plain HTTP is refused on an Azure side."""
        read = _read_stubbed(mocker)
        monkeypatch.setenv(mcp_server._EMULATOR_SWITCH, value)

        message = _describe_refused(tmp_path, "  path: az://lake/a.parquet\n")

        assert message.startswith("The source side reads Azure, and the server's environment")
        assert "Azurite emulator over plain HTTP" in message
        assert mcp_server._EMULATOR_SWITCH.casefold() in message.casefold()
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("value", "source"),
        [
            pytest.param("false", "  path: az://lake/a.parquet\n", id="false"),
            pytest.param("0", "  path: az://lake/a.parquet\n", id="zero"),
            pytest.param("off", "  path: az://lake/a.parquet\n", id="off"),
            pytest.param("true", "  path: s3://lake/a.parquet\n", id="on-an-s3-side"),
        ],
    )
    def test_it_reads_with_the_switch_off_or_on_another_store(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        value: str,
        source: str,
    ) -> None:
        """Ensure the switch turned off, or on a side that does not read Azure, leaves the side read."""
        read = _read_stubbed(mocker)
        monkeypatch.setenv(mcp_server._EMULATOR_SWITCH, value)

        _describe_read(tmp_path, source, read)

    def test_it_passes_over_an_empty_value(self) -> None:
        """Ensure an empty variable, which no reader takes, is passed over.

        Windows removes a variable set to the empty text, so the environment is passed in.
        """
        config = SourceConfig(path="az://lake/a.parquet")
        environ = {"AZURE_ENDPOINT": "", mcp_server._EMULATOR_SWITCH: ""}

        mcp_server._refuse_environment_endpoints(_endpoint_settings(), "source", config, environ)

    @pytest.mark.parametrize(
        "name",
        ["MOTHERDUCK_HOST", "motherduck_port", "motherduck_use_tls", "Motherduck_Region_Host"],
    )
    def test_it_refuses_a_motherduck_setting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, name: str
    ) -> None:
        """Ensure a MotherDuck side refuses a setting the extension reads from the environment."""
        read = _read_stubbed(mocker)
        monkeypatch.setenv(name, "127.0.0.1")

        message = _describe_refused(
            tmp_path, "  type: duckdb\n  database: 'md:sales'\n  table: t\n"
        )

        assert message.startswith("The source side reads MotherDuck, and the server's environment")
        assert f"variable {name}".casefold() in message.casefold()
        assert "127.0.0.1" not in message
        read.assert_not_called()

    @pytest.mark.parametrize(
        "name", ["MOTHERDUCK_TOKEN", "motherduck_token", "motherduck_saas_mode"]
    )
    def test_it_reads_a_motherduck_side_with_its_token_or_an_option(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture, name: str
    ) -> None:
        """Ensure the token, and an option a tool passes on, leave a MotherDuck side read."""
        read = _read_stubbed(mocker)
        monkeypatch.setenv(name, "Q7mdTokenTokenTokenQ7")

        _describe_read(tmp_path, "  type: duckdb\n  database: 'md:sales'\n  table: t\n", read)

    @pytest.mark.parametrize("profile", ["ledgerprofile", None])
    def test_it_refuses_the_endpoint_of_the_aws_profile(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        profile: str | None,
    ) -> None:
        """Ensure the endpoint_url of the profile the environment selects is held to the rules.

        The message names neither the profile nor the endpoint.
        """
        read = _read_stubbed(mocker)
        section = "default" if profile is None else f"profile {profile}"
        config = tmp_path / "aws-config"
        config.write_text(
            f"[{section}]\nendpoint_url = http://leak-probe.example:9000\n", encoding="utf-8"
        )
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        if profile is not None:
            monkeypatch.setenv("AWS_PROFILE", profile)

        message = _describe_refused(tmp_path, "  path: s3://lake/a.parquet\n")

        assert message.startswith(
            "The source side reads S3, and the endpoint_url of the AWS profile"
        )
        assert "not given with --allow-host" in message
        assert "ledgerprofile" not in message
        assert "leak-probe" not in message
        assert "9000" not in message
        read.assert_not_called()

    @pytest.mark.parametrize(
        ("text", "profile"),
        [
            pytest.param(
                "[profile ledgerprofile]\nendpoint_url = https://minio.internal:9000\n",
                "ledgerprofile",
                id="allowed",
            ),
            pytest.param(
                "[default]\nservices = lake\n\n[services lake]\n"
                "s3 =\n  endpoint_url = http://leak-probe.example:9000\n",
                None,
                id="services",
            ),
            pytest.param(
                "[default]\nendpoint_url = http://leak-probe.example:9000\n",
                "missing",
                id="missing-profile",
            ),
            pytest.param(
                "[default\nendpoint_url = http://leak-probe.example:9000\n", None, id="malformed"
            ),
            pytest.param("[default]\nregion = us-east-1\n", None, id="no-endpoint"),
        ],
    )
    def test_it_reads_with_a_profile_whose_endpoint_polars_does_not_take(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        mocker: MockerFixture,
        text: str,
        profile: str | None,
    ) -> None:
        """Ensure an allowed endpoint, a services section, a missing profile, or a bad file leaves the side read.

        Polars reads no `[services]` section, and reads no endpoint from a
        profile that is missing or a file it cannot parse.
        """
        read = _read_stubbed(mocker)
        config = tmp_path / "aws-config"
        config.write_text(text, encoding="utf-8")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        if profile is not None:
            monkeypatch.setenv("AWS_PROFILE", profile)

        _describe_read(tmp_path, "  path: s3://lake/a.parquet\n", read)

    def test_it_reads_no_profile_without_boto3(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure the profile is not read when boto3 is missing, since Polars then reads none."""
        read = _read_stubbed(mocker)
        config = tmp_path / "aws-config"
        config.write_text("[default]\nendpoint_url = http://leak-probe.example:9000\n")
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        find_spec = importlib.util.find_spec
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            lambda name, *args: None if name == "boto3" else find_spec(name, *args),
        )

        _describe_read(tmp_path, "  path: s3://lake/a.parquet\n", read)

    def test_validate_config_checks_them_only_with_schemas(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, engine_calls: list[MagicMock]
    ) -> None:
        """Ensure validate_config refuses such an endpoint when it reads the schemas, and only then."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio.internal:9000")
        _with_source(tmp_path, "  path: s3://lake/a.parquet\n")
        settings = _endpoint_settings()
        for mock in engine_calls:
            mock.return_value = []

        with pytest.raises(ConfigError, match="AWS_ENDPOINT_URL names its endpoint by a scheme"):
            check_configuration(settings, "veridelta.yaml", schemas=True)
        refused = _call(settings, {"path": "veridelta.yaml", "schemas": True})
        report = check_configuration(settings, "veridelta.yaml")

        assert refused.is_error
        assert "AWS_ENDPOINT_URL names its endpoint by a scheme" in _text(refused)
        assert report["valid"]
