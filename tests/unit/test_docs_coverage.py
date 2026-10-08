# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Guard the docs against drifting from the code they describe.

Each class binds one kind of page text to its source: configuration fields to
the Pydantic models, command lines and flags to the CLI's parser, the Action's
tables to `action.yml`, and the agents page's tool table to the MCP server.
"""

import argparse
import re
import shlex
from collections.abc import Iterator
from itertools import takewhile
from pathlib import Path
from typing import Any

import anyio
import pytest
import yaml
from mcp import Client
from pydantic import BaseModel

from veridelta.cli import build_parser
from veridelta.mcp_server import Settings, build_server
from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DOCS = _ROOT / "docs"

_PAGES: dict[str, tuple[type[BaseModel], ...]] = {
    "configuration.md": (DiffConfig,),
    "rules.md": (DiffRule,),
    "sources.md": (
        SourceConfig,
        SnowflakeConfig,
        DatabricksConfig,
        BigQueryConfig,
        DeltaLakeConfig,
        IcebergConfig,
        DatabaseConfig,
        DuckDBConfig,
    ),
}
"""Each guide page and the models whose every field it must name."""


class TestUserGuideCoverage:
    """Keep the user guide in lockstep with the config models."""

    @pytest.mark.parametrize("page", sorted(_PAGES))
    def test_it_documents_every_config_field(self, page: str) -> None:
        """Ensure a new field cannot ship without appearing on its guide page.

        A one-time audit goes stale the next time someone adds a field. Binding
        each page to its models makes the gap fail the suite instead. Warehouse,
        lakehouse, and database connection models are held to the same standard,
        since a credential or time-travel field nobody documents is one nobody
        can use.
        """
        text = (_DOCS / page).read_text(encoding="utf-8")
        missing = [
            f"{model.__name__}.{field}"
            for model in _PAGES[page]
            for field in model.model_fields
            if field not in text
        ]

        assert missing == [], f"These fields are missing from docs/{page}: " + ", ".join(missing)


_COMMAND_PAGES = sorted(
    [
        _ROOT / "README.md",
        _ROOT / "CONTRIBUTING.md",
        *_DOCS.rglob("*.md"),
        *_ROOT.glob("skills/*/SKILL.md"),
    ]
)
"""Every page that may show a `veridelta` command line to a reader or an agent."""
_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_COMMAND = re.compile(r"`(veridelta [^`]+)`")
_SHELL_OPERATOR = re.compile(r"[();<>|&]+")
"""A token that ends the command: a redirect, a pipe, or a separator."""


def _label(path: Path) -> str:
    """Name a file relative to the repository root, with forward slashes on every OS."""
    return path.relative_to(_ROOT).as_posix()


def _command_lines(path: Path) -> list[str]:
    """Return each `veridelta` command line on a page, in code blocks and inline code."""
    lines: list[str] = []
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        elif fenced and line.strip().startswith("veridelta "):
            lines.append(line.strip())
        elif not fenced:
            lines.extend(_INLINE_COMMAND.findall(line))
    return lines


def _arguments(line: str) -> list[str]:
    """Split a command line as a shell does, and drop the program and any redirect after it."""
    lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    words: list[str] = []
    for word in lexer:
        if _SHELL_OPERATOR.fullmatch(word):
            break
        words.append(word)
    return words[1:]


def _parses(line: str) -> bool:
    """Return whether the CLI accepts a command line. `--version` and `--help` exit 0."""
    try:
        build_parser().parse_args(_arguments(line))
    except SystemExit as exit_:
        return exit_.code == 0
    return True


class TestCommandLines:
    """Keep every command line the docs show accepted by the CLI."""

    def test_it_finds_command_lines(self) -> None:
        """Ensure a moved page or a changed fence cannot empty the check below."""
        found = {_label(page): len(_command_lines(page)) for page in _COMMAND_PAGES}

        assert found["docs/cli.md"] >= 30, found
        assert found["docs/agents.md"] >= 3, found
        assert found["skills/veridelta/SKILL.md"] >= 3, found
        assert found["README.md"] >= 3, found

    @pytest.mark.parametrize(
        ("line", "arguments"),
        [
            ("veridelta run -c veridelta.yaml", ["run", "-c", "veridelta.yaml"]),
            ("veridelta schema > veridelta.schema.json", ["schema"]),
            ("veridelta run --json | jq .is_match", ["run", "--json"]),
            ("veridelta run -c 'a b.yaml' && echo ok", ["run", "-c", "a b.yaml"]),
        ],
    )
    def test_it_reads_the_arguments_a_shell_would_pass(
        self, line: str, arguments: list[str]
    ) -> None:
        """Ensure a redirect or a pipe after a command is not read as one of its arguments."""
        assert _arguments(line) == arguments

    def test_it_refuses_a_flag_the_cli_lacks(self) -> None:
        """Ensure the check below can fail."""
        assert _parses("veridelta --version")
        assert not _parses("veridelta run --no-such-flag")
        assert not _parses("veridelta compare -c veridelta.yaml")

    @pytest.mark.parametrize("page", _COMMAND_PAGES, ids=_label)
    def test_its_command_lines_parse_with_the_cli(self, page: Path) -> None:
        """Ensure a renamed command or flag fails here, on every page that shows one."""
        refused = [line for line in _command_lines(page) if not _parses(line)]

        assert refused == [], "The CLI refuses these command lines:\n" + "\n".join(refused)


def _options(parser: argparse.ArgumentParser, command: str = "") -> Iterator[tuple[str, str]]:
    """Yield each command's name, then each of its option strings, through every subcommand."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, subparser in action.choices.items():
                yield name, ""
                yield from _options(subparser, name)
        else:
            for option in action.option_strings:
                yield command, option


def _names(text: str, command: str, option: str) -> bool:
    """Return whether a page names an option, or the command itself when `option` is empty."""
    if option:
        return re.search(rf"(?<![\w-]){re.escape(option)}(?![\w-])", text) is not None
    return re.search(rf"\bveridelta {re.escape(command)}\b", text) is not None


class TestCommandLinePage:
    """Keep the command line page naming every command and flag the CLI defines."""

    def test_it_reads_every_option(self) -> None:
        """Ensure the walk reaches subcommands, so the check below covers them."""
        options = set(_options(build_parser()))

        assert ("", "-V") in options
        assert ("run", "--baseline") in options
        assert ("suggest", "") in options

    def test_it_names_every_command_and_flag(self) -> None:
        """Ensure a new command or flag cannot ship without appearing in `docs/cli.md`."""
        text = (_DOCS / "cli.md").read_text(encoding="utf-8")
        missing = [
            " ".join(filter(None, ["veridelta", command, option]))
            for command, option in _options(build_parser())
            if option not in {"-h", "--help"} and not _names(text, command, option)
        ]

        assert missing == [], "docs/cli.md does not name: " + ", ".join(missing)


def _table(page: Path, heading: str) -> list[list[str]]:
    """Return the cells of each body row of the first table under a heading."""
    section = page.read_text(encoding="utf-8").split(f"\n{heading}\n", 1)[1].splitlines()
    start = next(index for index, line in enumerate(section) if line.startswith("|"))
    rows = list(takewhile(lambda line: line.startswith("|"), section[start:]))
    return [[cell.strip() for cell in row.strip("|").split("|")] for row in rows[2:]]


def _action() -> dict[str, Any]:
    """Return `action.yml` as parsed YAML."""
    action: dict[str, Any] = yaml.safe_load((_ROOT / "action.yml").read_text(encoding="utf-8"))
    return action


_DERIVED_DEFAULTS = {"artifact-name"}
"""Inputs whose empty default the Action replaces with a value it computes."""


def _documented_default(name: str, default: str) -> str:
    """Write an input's default as the inputs table does.

    An expression such as `${{ github.token }}` is written bare, and an empty
    default as `empty`, or `derived` where the Action computes one.
    """
    if not default:
        return "derived" if name in _DERIVED_DEFAULTS else "empty"
    expression = re.fullmatch(r"\$\{\{\s*(.+?)\s*\}\}", default)
    return f"`{expression.group(1) if expression else default}`"


class TestActionTables:
    """Keep the GitHub Action's tables in `docs/ci.md` matching `action.yml`."""

    def test_the_inputs_table_lists_each_input_and_its_default(self) -> None:
        """Ensure each input is listed once, in the order `action.yml` declares it, with its default."""
        rows = _table(_DOCS / "ci.md", "### Inputs")
        inputs = _action()["inputs"]

        assert [(row[0].strip("`"), row[1]) for row in rows] == [
            (name, _documented_default(name, str(spec.get("default", ""))))
            for name, spec in inputs.items()
        ]

    def test_the_outputs_table_lists_each_output(self) -> None:
        """Ensure each output is listed once, where a row may name several."""
        names = [
            name
            for row in _table(_DOCS / "ci.md", "### Outputs")
            for name in re.findall(r"`([^`]+)`", row[0])
        ]

        assert names == list(_action()["outputs"])


async def _tool_names(root: Path) -> list[str]:
    """List the MCP server's tools as a host does when it connects."""
    async with Client(build_server(Settings((root,)))) as client:
        return [tool.name for tool in (await client.list_tools()).tools]


class TestAgentsPage:
    """Keep the agents page's tool table matching the tools the MCP server serves."""

    def test_the_tool_table_lists_each_tool(self, tmp_path: Path) -> None:
        """Ensure a tool cannot ship, or be renamed, without its row on the agents page."""
        rows = _table(_DOCS / "agents.md", "## MCP server")
        documented = [row[0].strip("`") for row in rows]

        assert documented == anyio.run(_tool_names, tmp_path)
