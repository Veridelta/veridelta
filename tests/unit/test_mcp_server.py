# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the MCP server: its tools, its folder guard, and its SDK binding.

The server is driven through the SDK's own client, connected in memory, so each
test sees what an agent's host sees: the tool list, the structured result, and
whether a call failed.
"""

import json
import logging
import shutil
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
from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.mcp_server import (
    INSTRUCTIONS,
    RunReport,
    Settings,
    build_server,
    check_configuration,
    describe_side,
    resolve_path,
    run_configuration,
    serve,
)
from veridelta.models import DiffSummary

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_DESCRIPTION_BUDGET = 240
"""The most characters a tool's description may take. A host sends every
description to the model on every turn, so each one costs every turn."""

_INSTRUCTIONS_BUDGET = 500
"""The most characters the server's instructions may take, for the same reason."""

_VALID = "source:\n  path: a.csv\ntarget:\n  path: b.csv\nprimary_keys: [id]\n"
"""A configuration that loads and passes every offline check."""

_QUERY = (
    "source:\n  type: database\n  uri: sqlite:///srv/x.db\n  query: SELECT * FROM t\n"
    "target:\n  path: b.csv\nprimary_keys: [id]\n"
)
"""A configuration whose source is a query, which no probe can describe without running it."""

_SECRET = "hunter2-do-not-print"

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ci"
"""The CSV files the CI jobs and the recording compare."""

_T = TypeVar("_T")


def _write(folder: Path, text: str, name: str = "veridelta.yaml") -> Path:
    """Write a configuration file."""
    path = folder / name
    path.write_text(text)
    return path


def _project(root: Path, target: str, extra: str = "") -> Path:
    """Copy the CI fixtures into a root, and write a configuration that compares them."""
    shutil.copy(_FIXTURES / "legacy.csv", root / "legacy.csv")
    shutil.copy(_FIXTURES / target, root / target)
    return _write(
        root,
        f"source:\n  path: legacy.csv\ntarget:\n  path: {target}\nprimary_keys: [id]\n{extra}",
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

    def test_it_will_not_run_a_query_to_learn_its_columns(self, tmp_path: Path) -> None:
        """Ensure a query side is refused with the probe's own reason, and nothing connects."""
        _write(tmp_path, _QUERY)

        with pytest.raises(ConfigError, match="A schema probe reads a 'table'"):
            describe_side(Settings((tmp_path,)), "veridelta.yaml", "source")


class TestServer:
    """Validate the server as a host sees it, through the SDK's client."""

    def test_it_lists_each_tool_with_its_arguments_and_result(self, tmp_path: Path) -> None:
        """Ensure the host sees each tool, what it takes, and what it returns.

        The SDK drops a tool's output schema without a word when pydantic cannot
        build one, as for a `typing.TypedDict` nested in another on Python 3.11,
        and the tool then returns text alone. So every tool is held to one.
        """
        tools = {tool.name: tool for tool in _tools(Settings((tmp_path,)))}

        assert list(tools) == ["validate_config", "run_comparison", "describe_schema"]
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
        }
        assert run.input_schema["required"] == ["path"]
        assert set(run.input_schema["properties"]) == {"path"}
        assert run.output_schema is not None
        assert set(run.output_schema["properties"]) == RunReport.__required_keys__
        assert describe.input_schema["required"] == ["path", "side"]
        assert describe.input_schema["properties"]["side"]["enum"] == ["source", "target"]
        assert describe.output_schema is not None
        assert set(describe.output_schema["properties"]) == {"side", "columns"}

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

    def test_it_fails_to_describe_a_query(self, tmp_path: Path) -> None:
        """Ensure a side read through a query fails the call, named as `run --json` names it."""
        _write(tmp_path, _QUERY)

        result = _call(
            Settings((tmp_path,)),
            {"path": "veridelta.yaml", "side": "source"},
            tool="describe_schema",
        )

        assert result.is_error is True
        assert "ConfigError: A schema probe reads a 'table'" in _text(result)

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
