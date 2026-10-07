# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the MCP server: its tools, its folder guard, and its SDK binding.

The server is driven through the SDK's own client, connected in memory, so each
test sees what an agent's host sees: the tool list, the structured result, and
whether a call failed.
"""

import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar
from unittest.mock import MagicMock

import anyio
import pytest
from mcp import Client
from mcp.types import CallToolResult, Tool
from pytest_mock import MockerFixture

from veridelta import __version__, mcp_server
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.mcp_server import (
    INSTRUCTIONS,
    Settings,
    build_server,
    check_configuration,
    resolve_path,
    serve,
)

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_DESCRIPTION_BUDGET = 240
"""The most characters a tool's description may take. A host sends every
description to the model on every turn, so each one costs every turn."""

_INSTRUCTIONS_BUDGET = 500
"""The most characters the server's instructions may take, for the same reason."""

_VALID = "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
"""A configuration that loads and passes every offline check."""

_SECRET = "hunter2-do-not-print"

_T = TypeVar("_T")


def _write(folder: Path, text: str, name: str = "veridelta.yaml") -> Path:
    """Write a configuration file."""
    path = folder / name
    path.write_text(text)
    return path


def _with_client(settings: Settings, work: Callable[[Client], Awaitable[_T]]) -> _T:
    """Connect the SDK's client to a server built from `settings`, in memory, and run `work`.

    The client's block ends before the event loop does, so no transport is left
    open for a `ResourceWarning`, which this suite turns into a failure.
    """

    async def session() -> _T:
        async with Client(build_server(settings)) as client:
            return await work(client)

    return anyio.run(session)


def _call(settings: Settings, arguments: dict[str, Any]) -> CallToolResult:
    """Call `validate_config` with the arguments given, through the client."""

    async def work(client: Client) -> CallToolResult:
        return await client.call_tool("validate_config", arguments)

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
        }

    def test_it_reports_a_file_that_does_not_load_as_an_error(self, tmp_path: Path) -> None:
        """Ensure a load failure is a finding, as `validate` reports it."""
        _write(tmp_path, "source: {}\n")

        report = check_configuration(Settings((tmp_path,)), "veridelta.yaml")

        assert report["valid"] is False
        assert len(report["errors"]) == 1
        assert report["errors"][0].startswith("Configuration must contain both")

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
        monkeypatch.delenv("VD_MCP_TABLE", raising=False)
        _write(
            tmp_path,
            "source:\n  path: ${VD_MCP_TABLE}.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )
        settings = Settings((tmp_path,))

        strict = check_configuration(settings, "veridelta.yaml")
        lenient = check_configuration(settings, "veridelta.yaml", allow_missing_env=True)

        assert strict["valid"] is False
        assert "Environment variable 'VD_MCP_TABLE' is not set" in strict["errors"][0]
        assert lenient["valid"] is True
        assert lenient["warnings"] == [
            "Environment variable 'VD_MCP_TABLE' is not set, so its references were "
            "checked as the text 'VD_MCP_TABLE'."
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


class TestServer:
    """Validate the server as a host sees it, through the SDK's client."""

    def test_it_lists_one_tool_with_its_arguments_and_result(self, tmp_path: Path) -> None:
        """Ensure the host sees `validate_config`, what it takes, and what it returns."""
        tools = _tools(Settings((tmp_path,)))

        assert [tool.name for tool in tools] == ["validate_config"]
        (tool,) = tools
        assert tool.input_schema["required"] == ["path"]
        assert set(tool.input_schema["properties"]) == {"path", "schemas", "allow_missing_env"}
        assert tool.output_schema is not None
        assert set(tool.output_schema["properties"]) == {"config", "valid", "errors", "warnings"}

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
        monkeypatch.delenv("VD_MCP_TABLE", raising=False)
        _write(
            tmp_path,
            "source:\n  path: ${VD_MCP_TABLE}.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n",
        )

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml", "allow_missing_env": True})

        assert result.structured_content is not None
        assert result.structured_content["valid"] is True

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
        mocker.patch(
            "veridelta.engine.DiffEngine.check_config_file", side_effect=RuntimeError("boom")
        )
        _write(tmp_path, _VALID)

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml"})

        assert result.is_error is True
        assert _text(result).endswith("RuntimeError: boom")

    def test_it_never_returns_a_secret_from_the_configuration(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
    ) -> None:
        """Ensure a password read from the environment stays out of a result, even in an error.

        The database read fails with the full connection URI, as a driver's
        error can, and the result still holds only the masked form.
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

        result = _call(Settings((tmp_path,)), {"path": "veridelta.yaml", "schemas": True})

        shown = json.dumps(result.structured_content) + _text(result)
        assert result.structured_content is not None
        assert result.structured_content["valid"] is False
        assert "***" in shown
        assert _SECRET not in shown

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
