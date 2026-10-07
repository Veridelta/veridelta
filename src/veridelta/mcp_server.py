# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Serve Veridelta to an AI agent as Model Context Protocol tools.

`veridelta mcp` calls `serve`, which answers an agent's host over stdio through
the official MCP SDK, from the `mcp` extra. Each tool calls a function here. A
tool with a command returns the object that command prints with `--json`, so an
agent that knows the command line knows the tools.

The person who starts the server names the folders it may read configuration
files from, and a tool refuses a path outside them. A tool returns findings,
counts, and column names, never a value from the file. Two tools return values
from the data, and only when the person who starts the server allows it: then
at most a set number of rows, from data under the same folders. The SDK is
imported by the first `build_server()`, not with this module, so `veridelta`
imports without the extra.
"""

import importlib
import inspect
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Final,
    Literal,
    NamedTuple,
    TypedDict,
    TypeVar,
    cast,
)
from urllib.parse import urlsplit
from urllib.request import url2pathname

import polars as pl
from pydantic import Field

from veridelta import __version__
from veridelta.config import load_config
from veridelta.engine import DEFAULT_MIN_CONFIDENCE, DEFAULT_MIN_SUPPORT, DiffEngine
from veridelta.exceptions import ConfigError, VerideltaError
from veridelta.models import (
    DatabaseConfig,
    DeltaLakeConfig,
    DiffConfig,
    DuckDBConfig,
    IcebergConfig,
    SourceConfig,
    SourceRef,
)

if TYPE_CHECKING:
    from mcp.server import MCPServer

INSTRUCTIONS: Final = (
    "Veridelta compares two datasets under the rules in a YAML configuration file. "
    "Check a file with validate_config, and fix each error it reports, before you run it "
    "with run_comparison. Use describe_schema to list a side's columns when a rule must name "
    "one. Report counts and column names, and leave row values out of a reply unless the user "
    "asks for them. This server reads configuration files only from the folders it was "
    "started with."
)
"""What the server tells an agent's host about itself when the host connects."""

DEFAULT_ROW_CAP: Final = 50
"""The most rows, or value map entries, one call returns unless the server is
started with another `--max-rows`."""

DEFAULT_ROW_LIMIT: Final = 20
"""The rows `read_discrepancies` returns when a call names no `limit`."""

_REMOTE_SCHEMES: Final = frozenset(
    {
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
    }
)
"""URI schemes the readers fetch from another machine: object stores, the web,
Hugging Face, lakeFS, and MotherDuck. Any other location counts as a path on
this machine, so one the readers would open here is never let through
unchecked."""

_MCP_EXTRA = "MCP extra is not installed. Install it with: uv add 'veridelta[mcp]'"

_R = TypeVar("_R")


class _Sdk(NamedTuple):
    """The two names of the MCP SDK the server uses."""

    server: Any
    """`mcp.server.MCPServer`."""

    tool_error: Any
    """`mcp.server.mcpserver.exceptions.ToolError`, whose message reaches the agent."""


sdk: _Sdk | None = None
"""The SDK, imported by the first `build_server()` rather than here, since it
brings a web stack that the rest of Veridelta never loads. Tests set this
attribute."""


def _load_sdk() -> _Sdk:
    """Return the SDK, importing it on first use.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    if sdk is not None:
        return sdk
    try:
        server = importlib.import_module("mcp.server")
        errors = importlib.import_module("mcp.server.mcpserver.exceptions")
    except ImportError:
        raise VerideltaError(_MCP_EXTRA) from None
    return _Sdk(server=server.MCPServer, tool_error=errors.ToolError)


@dataclass(frozen=True)
class Settings:
    """What the person who starts the server allows, which no tool call can change.

    Attributes:
        roots (tuple[Path, ...]): The folders a tool may read a configuration
            file from, resolved on creation. A relative path in a tool call is
            read against the first. A tool that returns rows reads data on this
            machine only from them too.
        allow_row_values (bool): Whether `read_discrepancies` and
            `propose_value_maps` may return values from the data. Defaults to
            False.
        max_rows (int): The most rows, or value map entries, one of those calls
            returns. At least 1. Defaults to 50.
    """

    roots: tuple[Path, ...]
    allow_row_values: bool = False
    max_rows: int = DEFAULT_ROW_CAP

    def __post_init__(self) -> None:
        """Resolve every root, so a path compares with them as the filesystem does.

        Raises:
            ConfigError: If no root is given, or `max_rows` is below 1.
        """
        if not self.roots:
            raise ConfigError("The MCP server needs at least one folder to read from.")
        if self.max_rows < 1:
            raise ConfigError(f"The MCP server's row cap must be at least 1, got {self.max_rows}.")
        object.__setattr__(self, "roots", tuple(Path(root).resolve() for root in self.roots))


class ValidationReport(TypedDict):
    """What `validate_config` returns, the object `veridelta validate --json` prints.

    Attributes:
        config: The configuration file, resolved.
        valid: Whether no finding is an error.
        errors: What would stop a run.
        warnings: What a run may still trip on, such as an unset variable.
    """

    config: str
    valid: bool
    errors: list[str]
    warnings: list[str]


class RunReport(TypedDict):
    """What `run_comparison` returns: the summary `veridelta run --json` prints, with its verdict.

    `verdict` is `match` when the comparison falls within `threshold`, and
    `drift` otherwise, and `exit_code` is what `veridelta run` exits with, 0
    or 1. `artifacts_written` says whether the rows that differ were written
    to `output_path`. The fields between them are `DiffSummary`'s.
    """

    verdict: Literal["match", "drift"]
    exit_code: int
    total_rows_source: int
    total_rows_target: int
    added_count: int
    removed_count: int
    changed_count: int
    column_mismatches: dict[str, int]
    is_match: bool
    total_mismatches: int
    mismatch_ratio: float
    match_rate_percentage: float
    is_perfect_match: bool
    volume_shift: int
    report_summary: str
    artifacts_written: bool


class SchemaReport(TypedDict):
    """What `describe_schema` returns: one side's columns, and none of its rows.

    Attributes:
        side: `source` or `target`.
        columns: Each column's name, as stored and before
            `normalize_column_names` or a `rename_to`, mapped to its type as
            Polars names it, such as `Int64`, in the stored order.
    """

    side: Literal["source", "target"]
    columns: dict[str, str]


class DiscrepancyReport(TypedDict):
    """What `read_discrepancies` returns: rows of one kind, up to the cap.

    Attributes:
        kind: `added`, rows only in the target; `removed`, rows only in the
            source; or `changed`, rows in both with a column that differs.
        total: How many rows of that kind the run found.
        rows: The first of them, each a JSON object. A local run's `changed`
            row holds `{column}_source`, `{column}_target`, and
            `{column}_is_match` for each compared column. A date or a time is
            ISO 8601 text, a decimal is text, and bytes are hex.
        truncated: Whether `total` is more than `rows` holds.
        keys_only: Whether the pair was compared in place, such as two
            warehouse tables, which brings back primary keys alone.
    """

    kind: Literal["added", "removed", "changed"]
    total: int
    rows: list[dict[str, Any]]
    truncated: bool
    keys_only: bool


class ProposalReport(TypedDict):
    """What `propose_value_maps` returns: the proposals, up to the cap.

    Attributes:
        proposals: Each proposal as `veridelta crosswalk --json` prints it. A
            proposal comes back whole or not at all, and the proposals stop
            before the one whose `value_map` would pass the server's cap.
        total: How many proposals there are.
        truncated: Whether `total` is more than `proposals` holds.
    """

    proposals: list[dict[str, Any]]
    total: int
    truncated: bool


def _inside(settings: Settings, path: str) -> Path | None:
    """Resolve a path against the first root, or return None when it lies outside every root."""
    resolved = (settings.roots[0] / path).resolve()
    return resolved if any(resolved.is_relative_to(root) for root in settings.roots) else None


def _roots(settings: Settings) -> str:
    """List the roots for a message."""
    return ", ".join(str(root) for root in settings.roots)


def resolve_path(settings: Settings, path: str) -> Path:
    """Resolve a path from a tool call, and refuse one outside the roots.

    A relative path is read against the first root. Resolving follows symbolic
    links and folds `..`, so neither leads out of a root.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The path from the tool call.

    Returns:
        Path: The resolved path, under one of the roots.

    Raises:
        ConfigError: If the path resolves outside every root.
    """
    resolved = _inside(settings, path)
    if resolved is None:
        raise ConfigError(
            f"'{path}' is outside the folders this server reads from: {_roots(settings)}. "
            "Ask the person who started it to add the folder with --root."
        )
    return resolved


def _local_path(location: str) -> str | None:
    """Return the path on this machine a data location names, or None for a remote one."""
    parts = urlsplit(location)
    scheme = parts.scheme.lower()
    if scheme in _REMOTE_SCHEMES:
        return None
    if scheme == "file":
        return url2pathname(parts.path)
    return location


def _data_locations(config: SourceRef) -> list[tuple[str, str]]:
    """Name each file or folder on this machine a side reads, with the setting that names it."""
    named: list[tuple[str, str]] = []
    if isinstance(config, SourceConfig):
        named = [("path", config.path)]
    elif isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        named = [("table_uri", config.table_uri)]
    elif isinstance(config, DuckDBConfig):
        named = [("database", config.database)]
    elif isinstance(config, DatabaseConfig):
        parts = urlsplit(config.uri)
        if parts.scheme.startswith("sqlite"):
            named = [("SQLite file", parts.netloc + parts.path)]
    return [
        (setting, local)
        for setting, location in named
        if (local := _local_path(location)) is not None
    ]


def check_data_paths(settings: Settings, source: SourceRef, target: SourceRef) -> None:
    """Refuse a side whose data on this machine lies outside the roots.

    A tool that returns rows calls this before it reads one. The paths are
    expanded and resolved as the readers do, so neither `~` nor a link leads
    out. Data on another machine, such as an object store, a database server,
    or a warehouse, is read as the command line reads it.

    Args:
        settings (Settings): The roots the server was started with.
        source (SourceRef): The source configuration.
        target (SourceRef): The target configuration.

    Raises:
        ConfigError: If a file or folder a side reads is outside every root.
    """
    for side, config in (("source", source), ("target", target)):
        for setting, location in _data_locations(config):
            if _inside(settings, str(Path(location).expanduser())) is None:
                raise ConfigError(
                    f"The {side} {setting} '{location}' is outside the folders this server "
                    f"reads from: {_roots(settings)}. A tool returns rows only from data under "
                    "them, so move the data into one, or ask the person who started the server "
                    "to add its folder with --root."
                )


def check_configuration(
    settings: Settings, path: str, *, schemas: bool = False, allow_missing_env: bool = False
) -> ValidationReport:
    """Check a configuration file for what would stop a run, as `validate --json` does.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.
        schemas (bool): Whether to also connect and check the rules against each
            side's columns, which reads no rows.
        allow_missing_env (bool): Whether an unset `${NAME}` is a warning rather
            than an error, so a file can be checked without its secrets.

    Returns:
        ValidationReport: The errors and warnings, with the resolved path.

    Raises:
        ConfigError: If the path is outside the roots.
    """
    resolved = resolve_path(settings, path)
    findings = DiffEngine.check_config_file(
        resolved, schemas=schemas, allow_missing_env=allow_missing_env
    )
    errors = [finding.message for finding in findings if finding.severity == "error"]
    warnings = [finding.message for finding in findings if finding.severity == "warning"]
    return ValidationReport(
        config=str(resolved), valid=not errors, errors=errors, warnings=warnings
    )


def _runnable(settings: Settings, path: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration a tool runs, refusing an `output_path` outside the roots."""
    diff, source, target = load_config(resolve_path(settings, path))
    if diff.output_path is not None and _inside(settings, diff.output_path) is None:
        raise ConfigError(
            f"output_path '{diff.output_path}' is outside the folders this server writes to: "
            f"{_roots(settings)}. A run writes the rows that differ there, so point it into "
            "one of them, or ask the person who started the server to add the folder with "
            "--root."
        )
    return diff, source, target


def run_configuration(settings: Settings, path: str) -> RunReport:
    """Compare the two datasets a configuration file names, as `veridelta run --json` does.

    A run writes the rows that differ to `output_path`, so a configuration
    whose `output_path` lies outside the roots is refused before any row is
    read.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.

    Returns:
        RunReport: The summary, with the verdict and the exit code.

    Raises:
        ConfigError: If the file or its `output_path` is outside the roots, or
            the configuration cannot run as written.
        ConnectorError: If a source cannot be read.
        DataIntegrityError: If a primary key repeats on either side.
    """
    summary = DiffEngine.run_from_configs(*_runnable(settings, path)).summary
    return cast(
        "RunReport",
        {
            "verdict": "match" if summary.is_match else "drift",
            "exit_code": 0 if summary.is_match else 1,
            **summary.model_dump(mode="json"),
            "artifacts_written": summary.artifacts_written,
        },
    )


def describe_side(settings: Settings, path: str, side: Literal["source", "target"]) -> SchemaReport:
    """List one side's columns and their types, as a run reads them before its first row.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.
        side (Literal["source", "target"]): The side to describe.

    Returns:
        SchemaReport: The side, and each of its columns mapped to its type.

    Raises:
        ConfigError: If the file is outside the roots, does not load, or reads
            the side through a `query`, which would have to run in full.
        ConnectorError: If the side cannot be reached or read.
    """
    _, source, target = load_config(resolve_path(settings, path))
    schema = DiffEngine.read_schema(source if side == "source" else target)
    return SchemaReport(side=side, columns={name: str(dtype) for name, dtype in schema.items()})


def _allow_rows(settings: Settings, tool: str) -> None:
    """Refuse a tool that returns values from the data, unless the server allows them."""
    if not settings.allow_row_values:
        raise ConfigError(
            f"{tool} returns values from the data, and this server was started without "
            "--allow-row-values. Ask the person who started it to add the flag, or report "
            "counts and column names instead."
        )


def _plain(value: object) -> str:
    """Write a value JSON has no type for as text: ISO 8601 for a date or a time, hex for bytes."""
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _records(frame: pl.DataFrame) -> list[dict[str, Any]]:
    """Turn rows into JSON objects, with NaN and infinity as text.

    The rows pass through Python values, since Polars cannot write every
    type as JSON, and its failure on one, such as binary, is a panic that
    no `except Exception` catches.
    """
    text = json.dumps(frame.to_dicts(), default=_plain)
    return cast("list[dict[str, Any]]", json.loads(text, parse_constant=str))


def read_rows(
    settings: Settings,
    path: str,
    kind: Literal["added", "removed", "changed"],
    limit: int = DEFAULT_ROW_LIMIT,
) -> DiscrepancyReport:
    """Run the comparison and return the first rows of one kind, up to the server's cap.

    Args:
        settings (Settings): The roots, the permission, and the cap.
        path (str): The configuration file, under a root.
        kind (Literal["added", "removed", "changed"]): Which rows to return.
        limit (int): The most rows the call asks for. The server's `max_rows`
            caps it.

    Returns:
        DiscrepancyReport: The rows, how many there are, and whether more were
            left out.

    Raises:
        ConfigError: If the server does not allow row values, `limit` is below
            1, the file, its data, or its `output_path` is outside the roots,
            or the configuration cannot run.
        ConnectorError: If a source cannot be read.
        DataIntegrityError: If a primary key repeats on either side.
    """
    _allow_rows(settings, "read_discrepancies")
    if limit < 1:
        raise ConfigError(f"limit must be at least 1, got {limit}.")
    diff, source, target = _runnable(settings, path)
    check_data_paths(settings, source, target)
    result = DiffEngine.run_from_configs(diff, source, target)
    found = {
        "added": (result.added, result.summary.added_count),
        "removed": (result.removed, result.summary.removed_count),
        "changed": (result.changed, result.summary.changed_count),
    }
    frame, total = found[kind]
    shown = frame.head(min(limit, settings.max_rows))
    return DiscrepancyReport(
        kind=kind,
        total=total,
        rows=_records(shown),
        truncated=total > shown.height,
        keys_only=result.keys_only,
    )


def propose_maps(
    settings: Settings,
    path: str,
    *,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    min_support: int = DEFAULT_MIN_SUPPORT,
    sample_fraction: float = 1.0,
) -> ProposalReport:
    """Propose `value_map` entries as `veridelta crosswalk --json` does, up to the server's cap.

    Args:
        settings (Settings): The roots, the permission, and the cap.
        path (str): The configuration file, under a root.
        min_confidence (float): Share of a source value's rows that must agree
            on one target value.
        min_support (int): Agreeing rows an entry needs.
        sample_fraction (float): Share of joined rows to read.

    Returns:
        ProposalReport: The proposals, how many there are, and whether some
            were left out.

    Raises:
        ConfigError: If the server does not allow row values, the file or its
            data is outside the roots, a threshold is out of range, or the
            configuration cannot be proposed from.
        ConnectorError: If a source cannot be read.
    """
    _allow_rows(settings, "propose_value_maps")
    diff, source, target = load_config(resolve_path(settings, path))
    check_data_paths(settings, source, target)
    proposals = DiffEngine.propose_value_maps_from_configs(
        diff,
        source,
        target,
        min_confidence=min_confidence,
        min_support=min_support,
        sample_fraction=sample_fraction,
    )
    kept: list[dict[str, Any]] = []
    entries = 0
    for proposal in proposals:
        entries += len(proposal.value_map)
        if entries > settings.max_rows:
            break
        kept.append(proposal.model_dump(mode="json"))
    return ProposalReport(
        proposals=kept, total=len(proposals), truncated=len(kept) < len(proposals)
    )


def _failure(exc: Exception) -> str:
    """Name a failure as `run --json` does: its type, then its message."""
    return f"{type(exc).__name__}: {str(exc).strip()}"


def _answer(tool_error: Any, work: Callable[[], _R]) -> _R:
    """Run a tool's work, and fail the call with the failure's type and message.

    The SDK passes only a `ToolError`'s message on to the agent, so every
    failure becomes one, named as `run --json` names it.
    """
    try:
        return work()
    except Exception as exc:
        raise tool_error(_failure(exc)) from exc


def build_server(settings: Settings) -> "MCPServer":
    """Build the server and register its tools.

    Args:
        settings (Settings): The roots every tool is held to.

    Returns:
        MCPServer: The SDK's server, ready for `run()`.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    loaded = _load_sdk()
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    try:
        server: MCPServer = loaded.server(
            "veridelta", instructions=INSTRUCTIONS, version=__version__
        )
    finally:
        # The SDK's constructor configures the root logger, which belongs to the
        # program. The command line sets Veridelta's own logging with --verbose.
        root.handlers[:] = handlers
        root.setLevel(level)

    def validate_config(
        path: Annotated[str, Field(description="The configuration file.")],
        schemas: Annotated[
            bool, Field(description="Also read each side's columns, but no rows.")
        ] = False,
        allow_missing_env: Annotated[
            bool, Field(description="Report an unset ${NAME} as a warning, not an error.")
        ] = False,
    ) -> ValidationReport:
        """Check a Veridelta configuration file for what would stop a run.

        Reads no rows. Returns what `veridelta validate --json` prints: its errors and warnings.
        """
        return _answer(
            loaded.tool_error,
            lambda: check_configuration(
                settings, path, schemas=schemas, allow_missing_env=allow_missing_env
            ),
        )

    def run_comparison(
        path: Annotated[str, Field(description="The configuration file.")],
    ) -> RunReport:
        """Compare the two datasets a Veridelta configuration file names.

        Returns what `veridelta run --json` prints, with the verdict and the exit code a run gives.
        """
        return _answer(loaded.tool_error, lambda: run_configuration(settings, path))

    def describe_schema(
        path: Annotated[str, Field(description="The configuration file.")],
        side: Annotated[Literal["source", "target"], Field(description="The side to describe.")],
    ) -> SchemaReport:
        """List the columns of one side a Veridelta configuration file names, with their types.

        Reads no rows. Names are as stored, before normalize_column_names or a rename_to.
        """
        return _answer(loaded.tool_error, lambda: describe_side(settings, path, side))

    def read_discrepancies(
        path: Annotated[str, Field(description="The configuration file.")],
        kind: Annotated[
            Literal["added", "removed", "changed"],
            Field(
                description=(
                    "added: rows only in the target. removed: rows only in the source. "
                    "changed: rows in both that differ."
                )
            ),
        ],
        limit: Annotated[
            int, Field(ge=1, description="The most rows to return, within the server's cap.")
        ] = DEFAULT_ROW_LIMIT,
    ) -> DiscrepancyReport:
        """Return the rows that differ between the datasets a Veridelta configuration file names.

        Runs the comparison. Needs a server started with --allow-row-values.
        """
        return _answer(loaded.tool_error, lambda: read_rows(settings, path, kind, limit))

    def propose_value_maps(
        path: Annotated[str, Field(description="The configuration file.")],
        min_confidence: Annotated[
            float,
            Field(gt=0.5, le=1, description="Share of a value's rows that must agree."),
        ] = DEFAULT_MIN_CONFIDENCE,
        min_support: Annotated[
            int, Field(ge=1, description="Agreeing rows an entry needs.")
        ] = DEFAULT_MIN_SUPPORT,
        sample_fraction: Annotated[
            float, Field(gt=0, le=1, description="Share of joined rows to read.")
        ] = 1.0,
    ) -> ProposalReport:
        """Propose value_map entries for columns that hold the same values in two encodings.

        Returns what veridelta crosswalk --json prints. Needs a server started with
        --allow-row-values.
        """
        return _answer(
            loaded.tool_error,
            lambda: propose_maps(
                settings,
                path,
                min_confidence=min_confidence,
                min_support=min_support,
                sample_fraction=sample_fraction,
            ),
        )

    for tool in (
        validate_config,
        run_comparison,
        describe_schema,
        read_discrepancies,
        propose_value_maps,
    ):
        server.tool(description=inspect.getdoc(tool))(tool)
    return server


def serve(settings: Settings) -> None:
    """Answer an agent's host over stdio until it disconnects.

    Args:
        settings (Settings): The roots every tool is held to.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    build_server(settings).run(transport="stdio")
