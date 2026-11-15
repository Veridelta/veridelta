# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Serve Veridelta to an AI agent as Model Context Protocol tools.

`veridelta mcp` calls `serve`, which answers an agent's host over stdio through
the official MCP SDK, from the `mcp` extra. Each tool calls a function here. A
tool with a command returns the object that command prints with `--json`, so an
agent that knows the command line knows the tools.

The person who starts the server names the folders it may read configuration
files and data on this machine from, and a tool refuses a path outside them. A
tool returns findings, counts, and column names, never a value from the file,
and a failed call leaves out a message that can quote one. Three tools return
values from the data, and only when the person who starts the server allows it:
then at most a set number of rows. A side's `query` runs only when that person
allows queries too. The SDK is imported by the first `build_server()`, not with
this module, so `veridelta` imports without the extra.
"""

import functools
import glob
import importlib
import inspect
import json
import logging
import os
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
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
from polars.exceptions import PanicException
from pydantic import Field

from veridelta import __version__
from veridelta.config import load_config, referenced_variables
from veridelta.connectors.base import mask_secrets, shown_location, without_location
from veridelta.connectors.duckdb import sandboxed
from veridelta.connectors.lakehouse import checking_iceberg_files
from veridelta.engine import (
    DEFAULT_MAX_SHARE,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_MIN_SUPPORT,
    DiffEngine,
)
from veridelta.exceptions import (
    ConfigError,
    ConnectorError,
    VerideltaError,
    may_quote_data,
    missing_extra,
)
from veridelta.models import (
    DatabaseConfig,
    DeltaLakeConfig,
    DiffConfig,
    DuckDBConfig,
    IcebergConfig,
    SourceConfig,
    SourceRef,
)
from veridelta.outputs import ValidationReport, validation_report

if TYPE_CHECKING:
    from mcp.server import MCPServer

INSTRUCTIONS: Final = (
    "Veridelta compares two datasets under the rules in a YAML configuration file. "
    "Check a file with validate_config, and fix each error it reports, before you run it "
    "with run_comparison. Use describe_schema to list a side's columns when a rule must name "
    "one. Report counts and column names, and leave row values out of a reply unless the user "
    "asks for them. Three tools return row values, only when the server allows them. This "
    "server reads files only from the folders it was started with."
)
"""What the server tells an agent's host about itself when the host connects."""

DEFAULT_ROW_CAP: Final = 50
"""The most rows, or value map entries, one call returns unless the server is
started with another `--max-rows`."""

DEFAULT_ROW_LIMIT: Final = 20
"""The rows `read_discrepancies` returns when a call names no `limit`."""

_REMOTE_PREFIXES: Final = tuple(
    f"{scheme}://"
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
        "s3",
        "s3a",
    )
)
"""The prefixes of a location that the readers fetch from another machine: an
object store, the web, or Hugging Face. Polars fetches a location only when one
of these schemes, in lower case, is followed by `//`, and opens any other as a
path on this machine, even `s3:x` or `md://x`. So any other location is held to
the roots. DuckDB and SQLite open even these as a file, so a DuckDB or SQLite
file is also held to the roots as written."""

_MOTHERDUCK_PREFIXES: Final = ("md:", "motherduck:")
"""How a DuckDB `database` on MotherDuck starts, which DuckDB never opens as a file."""

_URL_SPACE: Final = "".join(chr(code) for code in range(0x21))
"""The C0 controls and the space, which a URL parser strips from both ends of a URL."""

_GLOB_CHARACTERS: Final = frozenset("*?[")
"""Characters that make Polars read a local path as a pattern, which it expands
into the files it opens."""

_MCP_EXTRA = missing_extra("mcp", "veridelta mcp")

_WITHHELD: Final = (
    "its message is left out, since it can quote values from the data, and this server "
    "was started without --allow-row-values. Run the configuration with veridelta run "
    "to read the message."
)
"""What a failed call says in place of a message that can quote the data."""

_SHORTEST_MASKED: Final = 4
"""The fewest characters an environment value needs to be masked in a tool's answer.

A shorter value, such as a flag or a small number, would mask ordinary text
throughout the answer, and holds no secret worth the loss."""

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
            read against the first. Every tool reads data on this machine only
            from them too.
        allow_row_values (bool): Whether `read_discrepancies`,
            `propose_value_maps`, and `suggest_rules` may return values from the
            data, and a failed call may return a message that Polars, a reader,
            or a driver wrote. Defaults to False.
        max_rows (int): The most rows, value map entries, or example keys one of
            those calls returns. At least 1. Defaults to 50.
        allow_queries (bool): Whether a tool may run a side's `query`, which
            runs as written with the configuration's credentials. A DuckDB file
            still reads other files only from under the roots. Defaults to
            False.
    """

    roots: tuple[Path, ...]
    allow_row_values: bool = False
    max_rows: int = DEFAULT_ROW_CAP
    allow_queries: bool = False

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


class RunReport(TypedDict):
    """What `run_comparison` returns: the summary `veridelta run --json` prints, with its verdict.

    `verdict` is `match` when the comparison falls within `threshold`, and
    `drift` otherwise, and `exit_code` is what `veridelta run` exits with, 0
    or 1. `artifacts_written` says whether the rows that differ were written
    to `output_path`. The fields between them are `DiffSummary`'s.
    `provenance` is always null: the server keeps no run record, which
    `veridelta run` writes.
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
    threshold: float
    accepted_count: int
    provenance: None
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


class SuggestionReport(TypedDict):
    """What `suggest_rules` returns: the suggestions, up to the cap.

    Attributes:
        suggestions: Each suggestion as `veridelta suggest --json` prints it. A
            suggestion comes back whole or not at all, and the suggestions stop
            before the one whose example keys would pass the server's cap.
        total: How many suggestions there are.
        truncated: Whether `total` is more than `suggestions` holds.
    """

    suggestions: list[dict[str, Any]]
    total: int
    truncated: bool


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


def _under_roots(settings: Settings, resolved: Path) -> bool:
    """Whether a resolved path lies under one of the roots."""
    return any(resolved.is_relative_to(root) for root in settings.roots)


def _inside(settings: Settings, path: str) -> Path | None:
    """Resolve a tool call's path against the first root, or return None outside every root."""
    resolved = (settings.roots[0] / path).resolve()
    return resolved if _under_roots(settings, resolved) else None


def _opened_inside(settings: Settings, location: str) -> bool:
    """Whether a path a configuration names lies under a root, resolved where it is opened.

    A reader or the artifact writer resolves a relative path against the
    working directory, as the command line does, so the check does too. The
    command runs the server in its first root, where the two agree, but a
    program that calls `serve()` from another folder gets the same check.

    The readers disagree on a `..` after a link. The system follows the link
    first, and PyArrow drops the link and the `..` without following it. So
    the path must lie under a root read either way.
    """
    path = Path(location).expanduser()
    folded = Path(os.path.abspath(path))
    return all(_under_roots(settings, each.resolve()) for each in (path, folded))


def _opened(location: str) -> list[str]:
    """List the paths a reader may open for a location: itself, and each match of a pattern.

    Polars expands `*`, `?`, and `[...]` in a name, and `**` across folders.
    A wildcard matches a hidden name too, and `**` follows links. A name such
    as `lin[k]` does not exist as written, so resolving the pattern cannot
    follow the link it matches. The standard library lists the matches here,
    with hidden names and `**` included, and each is resolved on its own.
    Polars matches `.` and `..` as well, which the standard library never
    lists, so `_climbs` refuses each pattern that could match them first.
    """
    if not _GLOB_CHARACTERS.intersection(location):
        return [location]
    pattern = os.path.expanduser(location)
    return [location, *glob.glob(pattern, recursive=True, include_hidden=True)]


def _climbs(location: str) -> bool:
    """Whether a pattern could reach `..`, which leaves the folder a matched link points to.

    A pattern may not hold `..` as written. Polars matches the entries `.`
    and `..` only for a name with a glob character that starts with a dot,
    such as `.*` or `.[.]`, so such a name is refused too, and `*` is not.
    """
    return any(
        name == ".." or (name.startswith(".") and _GLOB_CHARACTERS.intersection(name))
        for name in Path(location).parts
    )


def _roots(settings: Settings) -> str:
    """List the roots for a message."""
    return ", ".join(str(root) for root in settings.roots)


def _refuse_opened(settings: Settings, path: str) -> None:
    """Refuse a file on this machine that an Iceberg scan is about to open, outside the roots.

    The path is the one PyArrow opens: relative to the working directory, and
    with no `~` expanded. PyArrow drops a folder and the `..` after it before
    it follows a link, so the path must lie under a root read either way. The
    message leaves the path out, since the table's files named it.

    Raises:
        ConfigError: If the path lies outside every root.
    """
    readings = (Path(path), Path(os.path.abspath(path)))
    if not all(_under_roots(settings, reading.resolve()) for reading in readings):
        raise ConfigError(
            "An Iceberg scan was about to open a file outside the folders this server reads "
            f"from: {_roots(settings)}, so it was stopped. A tool reads data on this machine "
            "only from under them."
        )


@contextmanager
def _reading(settings: Settings) -> Generator[None]:
    """Hold what a tool's sides read to the roots, while they read.

    A DuckDB file's connection reads other files only from under the roots,
    and an Iceberg scan opens a file on this machine only from under them,
    each checked just before it is opened.
    """
    with (
        sandboxed(settings.roots),
        checking_iceberg_files(functools.partial(_refuse_opened, settings)),
    ):
        yield


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


def _local_paths(location: str) -> list[str]:
    """Return each path on this machine a reader could open for a data location.

    The readers disagree on a file URI. Polars decodes `%` escapes and takes
    only a lower-case `file` scheme, so it opens `FILE://x` as a path relative
    to the working directory, as it does `S3://x`. PyIceberg takes the scheme in
    any case, adds a host to the path, and decodes nothing. Polars and
    deltalake start the path at `/`, so they open `file:tmp/x` as `/tmp/x`,
    where PyIceberg opens `tmp/x`. deltalake parses the URI as the URL
    standard says, so it also reads a backslash as `/` and strips the spaces
    at its ends. So a file URI
    yields each of these paths, and the caller holds every one to the roots.

    A location yields none only when every reader fetches it from another
    machine. Polars opens `s3:x/a.parquet`, with no `//`, or `md://x/a.parquet`,
    as a path relative to the working directory, so any other location yields
    itself.
    """
    if location.startswith(_REMOTE_PREFIXES):
        return []
    parts = urlsplit(location)
    if parts.scheme != "file":
        return [location]
    written = location[: len(parts.scheme)]
    standard = url2pathname(urlsplit(location.replace("\\", "/").strip(_URL_SPACE)).path)
    paths = {
        url2pathname(parts.path),
        parts.netloc + url2pathname(parts.path.replace("%", "%25")),
        standard if os.path.isabs(standard) else os.sep + standard,
    }
    if written != "file":
        paths.add(location)
    return sorted(paths)


def _data_locations(config: SourceRef) -> list[tuple[str, str]]:
    """Name each file or folder a side reads, with the setting that names it."""
    if isinstance(config, SourceConfig):
        return [("path", config.path)]
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return [("table_uri", config.table_uri)]
    if isinstance(config, DuckDBConfig):
        if config.database.startswith(_MOTHERDUCK_PREFIXES):
            return []
        return [("database", config.database)]
    if isinstance(config, DatabaseConfig):
        parts = urlsplit(config.uri)
        if parts.scheme.startswith("sqlite"):
            return [("SQLite file", parts.netloc + parts.path)]
    return []


def _delta_files(config: DeltaLakeConfig) -> Iterator[tuple[str, str, bool]]:
    """Name each file a scan of a Delta Lake table reads, from the version it reads.

    The log of a table on this machine comes first, each entry before the log
    is read, since a commit or checkpoint can be a link out of the table's
    folder. The walk follows no link, so each one is checked before it is
    opened. The data files then come from the log.
    """
    from deltalake import DeltaTable

    for local in _local_paths(config.table_uri):
        log = Path(local).expanduser() / "_delta_log"
        yield "log", str(log), True
        for folder, folders, files in os.walk(log):
            for name in sorted(folders + files):
                yield "log file", os.path.join(folder, name), True
    table = DeltaTable(
        config.table_uri, version=config.version, storage_options=config.storage_options or None
    )
    for uri in table.file_uris():
        yield "data file", uri, True


def _iceberg_files(config: IcebergConfig) -> Iterator[tuple[str, str, bool]]:
    """Name each file a scan of an Iceberg table reads, each before it is opened.

    A table folder's version hint, the manifest list, and each manifest name
    the next files to read, so the caller checks each name before this reads
    the file. The version hint is read as `StaticTable.from_metadata` reads
    it, so the check and the scan read the same metadata file, but its text is
    never quoted, since it is a value from a file. The data and delete files
    come from the manifests, as a scan plans them.

    The files are read as the scan on this server reads them: through a
    PyArrow FileIO built from `storage_options` alone, whatever the
    metadata's own properties say.
    """
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.serializers import FromInputFile

    io = PyArrowFileIO(dict(config.storage_options or {}))
    location, hint = config.table_uri, None
    if not location.endswith(".metadata.json"):
        hint = os.path.join(location, "metadata", "version-hint.text")
        yield "version hint", hint, True
        with io.new_input(hint).open() as stream:
            content = stream.read().decode("utf-8")
        if not content.endswith(".metadata.json"):
            content = f"v{content}" if content.isnumeric() else content
            content = f"{content}.metadata.json"
        location = os.path.join(location, "metadata", content)
        yield f"metadata file that '{hint}' names", location, False
    try:
        metadata = FromInputFile.table_metadata(io.new_input(location))
    except Exception:
        if hint is None:
            raise
        # The error would quote the metadata file, and so the version hint's text.
        raise OSError(f"the metadata file that '{hint}' names could not be read") from None
    snapshot = (
        metadata.current_snapshot()
        if config.snapshot_id is None
        else metadata.snapshot_by_id(config.snapshot_id)
    )
    if snapshot is None:
        # The scan reads no file: an empty table has none, and a snapshot it
        # cannot find stops it.
        return
    yield "manifest list", snapshot.manifest_list, True
    for manifest in snapshot.manifests(io):
        yield "manifest", manifest.manifest_path, True
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            yield "data file", entry.data_file.file_path, True


def _table_files(
    side: str, config: DeltaLakeConfig | IcebergConfig
) -> Iterator[tuple[str, str, bool]]:
    """Name each file that the scan of a lakehouse table reads.

    A table's log or metadata names its files by full URI, so they can lie
    anywhere. A table whose files cannot be listed is refused, since the scan
    would read the same log or metadata.

    Raises:
        ConnectorError: If the extra is missing, or the table cannot be listed.
    """
    if isinstance(config, DeltaLakeConfig):
        files, kind, extra = _delta_files(config), "Delta Lake table", "delta"
    else:
        files, kind, extra = _iceberg_files(config), "Iceberg table", "iceberg"
    try:
        yield from files
    except ImportError as exc:
        raise ConnectorError(missing_extra(extra, f"Reading a {kind}")) from exc
    except Exception as exc:
        where = shown_location(config.table_uri)
        raise ConnectorError(
            f"The files of the {side} {kind} '{where}' could not be listed, so this server "
            f"reads none of them: {without_location(str(exc), config.table_uri)}"
        ) from exc


def _refuse_outside(
    settings: Settings,
    side: str,
    setting: str,
    location: str,
    *,
    quoted: bool = True,
    as_written: bool = False,
    expanded: bool = False,
) -> None:
    """Refuse a file or folder on this machine that lies outside the roots.

    The message quotes the path only when `quoted`, since a path read from a
    file is a value from it. With `as_written`, the location is also checked
    as a path, for a reader that opens even `s3://x` as a file here. With
    `expanded`, the location is one Polars expands as a pattern, so each
    file it matches is checked, and a pattern that can match `..`, or one in
    a `file:` URI, is refused.
    """
    for path in _local_paths(location) + ([location] if as_written else []):
        named = f"{setting} '{path}'" if quoted else setting
        pattern = expanded and bool(_GLOB_CHARACTERS.intersection(location + path))
        if pattern and path != location:  # A `file:` URI, which `_local_paths` decoded.
            raise ConfigError(
                f"The {side} {setting} '{location}' is a file URI with a glob character in "
                "it. Polars expands a pattern in a URI with another matcher than a path's, "
                "so this server cannot check the files it would open. Write it as a path."
            )
        if pattern and _climbs(path):
            raise ConfigError(
                f"The {side} {named} is a glob pattern that can match '..'. "
                "A match can be a link, and '..' would then leave the folder the link "
                "points to, so this server refuses the pattern. Write the path without "
                "'..', and write out each name that starts with a dot."
            )
        opened = _opened(path) if expanded else [path]
        if not all(_opened_inside(settings, each) for each in opened):
            raise ConfigError(
                f"The {side} {named} is outside the folders this server "
                f"reads from: {_roots(settings)}. A tool reads data on this machine only "
                "from under them, so move the data into one, or ask the person who started "
                "the server to add its folder with --root."
            )


def check_data_paths(settings: Settings, source: SourceRef, target: SourceRef) -> None:
    """Refuse a side whose data on this machine lies outside the roots.

    Every tool that opens a side calls this first, since a column name or an
    error message can carry a file's text as a row does. The paths are
    expanded and resolved as the readers do, so neither `~` nor a link leads
    out. A path that holds a glob character is a pattern, and every file it
    matches must resolve under a root. A pattern may not reach `..`, since a
    match can be a link that `..` then leaves from its target. A `file:` URI
    may not hold a pattern at all, since Polars expands one with another
    matcher. A Delta Lake or Iceberg table, wherever it lies, has every file
    on this machine that its scan reads checked too, since its log or
    metadata can name a file anywhere. Data on another machine, such as an
    object store, a database server, or a warehouse, is read as the command
    line reads it.

    Every location the configuration names is checked before any file is
    opened, of either side: the listings come last.

    Args:
        settings (Settings): The roots the server was started with.
        source (SourceRef): The source configuration.
        target (SourceRef): The target configuration.

    Raises:
        ConfigError: If a file or folder a side reads is outside every root.
        ConnectorError: If the files of a lakehouse table cannot be listed.
    """
    sides = (("source", source), ("target", target))
    for side, config in sides:
        for setting, location in _data_locations(config):
            # DuckDB and SQLite open a file named with a store's scheme, such
            # as `s3://x/a.duckdb`, from the working directory.
            opened_here = isinstance(config, (DuckDBConfig, DatabaseConfig))
            _refuse_outside(
                settings, side, setting, location, as_written=opened_here, expanded=True
            )
    for side, config in sides:
        if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
            for setting, location, quoted in _table_files(side, config):
                _refuse_outside(settings, side, setting, location, quoted=quoted)


def check_configuration(
    settings: Settings, path: str, *, schemas: bool = False, allow_missing_env: bool = False
) -> ValidationReport:
    """Check a configuration file for what would stop a run, as `validate --json` does.

    Unless the server allows row values, a finding leaves out what Polars, a
    reader, or a driver said about a schema read that failed.

    Args:
        settings (Settings): The roots and the row values the server was started with.
        path (str): The configuration file, under a root.
        schemas (bool): Whether to also connect and check the rules against each
            side's columns, which reads no rows.
        allow_missing_env (bool): Whether an unset `${NAME}` is a warning rather
            than an error, so a file can be checked without its secrets.

    Returns:
        ValidationReport: The errors and warnings, with the resolved path.

    Raises:
        ConfigError: If the path is outside the roots, or, with `schemas`, the
            data a side reads on this machine is.
        ConnectorError: With `schemas`, if the files of a lakehouse table
            cannot be listed.
    """
    resolved = resolve_path(settings, path)
    if schemas:
        try:
            _, source, target = load_config(resolved, unset_env=[] if allow_missing_env else None)
        except ConfigError:
            # The check reports why the file does not load, and then reads no columns.
            pass
        else:
            check_data_paths(settings, source, target)
    with _reading(settings):
        findings = DiffEngine.check_config_file(
            resolved,
            schemas=schemas,
            allow_missing_env=allow_missing_env,
            quote_data=settings.allow_row_values,
        )
    return validation_report(str(resolved), findings)


def _load(settings: Settings, path: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration a tool opens, refusing one whose data on this machine is outside the roots."""
    diff, source, target = load_config(resolve_path(settings, path))
    check_data_paths(settings, source, target)
    return diff, source, target


def _refuse_queries(settings: Settings, source: SourceRef, target: SourceRef) -> None:
    """Refuse a side that reads through a `query`, unless the server allows queries.

    A query runs as written, with the configuration's credentials, so it can
    read anything they reach, or change a database that opens read-write.
    """
    if settings.allow_queries:
        return
    for side, config in (("source", source), ("target", target)):
        if isinstance(config, (DatabaseConfig, DuckDBConfig)) and config.query is not None:
            raise ConfigError(
                f"The {side} reads through a query, and this server was started without "
                "--allow-queries. A query runs as written, so read a 'table' instead, or "
                "ask the person who started the server to add the flag."
            )


def _runnable(settings: Settings, path: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration a tool runs, refusing a `query` or an `output_path` the server does not allow."""
    diff, source, target = _load(settings, path)
    _refuse_queries(settings, source, target)
    if diff.output_path is not None and not _opened_inside(settings, diff.output_path):
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
    whose `output_path` or data on this machine lies outside the roots is
    refused before any row is read, as is a side's `query` unless the server
    allows queries.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.

    Returns:
        RunReport: The summary, with the verdict and the exit code.

    Raises:
        ConfigError: If the file, its data, or its `output_path` is outside
            the roots, a side reads through a `query` the server does not
            allow, or the configuration cannot run as written.
        ConnectorError: If a source cannot be read.
        DataIntegrityError: If a primary key repeats on either side.
    """
    with _reading(settings):
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
        ConfigError: If the file or its data is outside the roots, the file
            does not load, or it reads the side through a `query`, which would
            have to run in full.
        ConnectorError: If the side cannot be reached or read.
    """
    _, source, target = _load(settings, path)
    with _reading(settings):
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
            a side reads through a `query` the server does not allow, or the
            configuration cannot run.
        ConnectorError: If a source cannot be read.
        DataIntegrityError: If a primary key repeats on either side.
    """
    _allow_rows(settings, "read_discrepancies")
    if limit < 1:
        raise ConfigError(f"limit must be at least 1, got {limit}.")
    with _reading(settings):
        result = DiffEngine.run_from_configs(*_runnable(settings, path))
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
        sample_fraction (float): Share of source rows to read, chosen by
            primary key.

    Returns:
        ProposalReport: The proposals, how many there are, and whether some
            were left out.

    Raises:
        ConfigError: If the server does not allow row values, the file or its
            data is outside the roots, a side reads through a `query` the
            server does not allow, a threshold is out of range, or the
            configuration cannot be proposed from.
        ConnectorError: If a source cannot be read.
    """
    _allow_rows(settings, "propose_value_maps")
    diff, source, target = _load(settings, path)
    _refuse_queries(settings, source, target)
    with _reading(settings):
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


def suggest(
    settings: Settings, path: str, *, max_share: float = DEFAULT_MAX_SHARE
) -> SuggestionReport:
    """Suggest rules as `veridelta suggest --json` does, up to the server's cap.

    Args:
        settings (Settings): The roots, the permission, and the cap.
        path (str): The configuration file, under a root.
        max_share (float): Largest gap a tolerance may explain, as a share of
            the larger of its two values.

    Returns:
        SuggestionReport: The suggestions, how many there are, and whether
            some were left out.

    Raises:
        ConfigError: If the server does not allow row values, the file or its
            data is outside the roots, a side reads through a `query` the
            server does not allow, `max_share` is out of range, or the pair is
            compared where it is stored.
        ConnectorError: If a source cannot be read.
        DataIntegrityError: If either dataset repeats a normalized primary key.
    """
    _allow_rows(settings, "suggest_rules")
    diff, source, target = _load(settings, path)
    _refuse_queries(settings, source, target)
    with _reading(settings):
        suggestions = DiffEngine.suggest_rules_from_configs(
            diff, source, target, max_share=max_share
        )
    kept: list[dict[str, Any]] = []
    examples = 0
    for suggestion in suggestions:
        examples += len(suggestion.examples)
        if examples > settings.max_rows:
            break
        kept.append(suggestion.model_dump(mode="json"))
    return SuggestionReport(
        suggestions=kept, total=len(suggestions), truncated=len(kept) < len(suggestions)
    )


def _failure(exc: BaseException, *, row_values: bool) -> str:
    """Name a failure as `run --json` does: its type, then its message.

    Polars, a reader, or a driver can quote cells in its message, so unless the
    server allows row values, such a message gives way to a fixed hint. A
    Veridelta error keeps its own, which names columns and counts, unless it
    quotes a reader's or a driver's text.
    """
    if may_quote_data(exc) and not row_values:
        return f"{type(exc).__name__}: {_WITHHELD}"
    return f"{type(exc).__name__}: {str(exc).strip()}"


def environment_values(settings: Settings, path: str) -> tuple[str, ...]:
    """Return the value of each environment variable a configuration file references.

    A tool masks each in its answer, since a path, a name, or an error built
    from the configuration carries the values it took. A file outside the
    roots, or one that cannot be read, gives none, and so does a variable
    that is unset or shorter than four characters.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file from the tool call.

    Returns:
        tuple[str, ...]: The values to mask.
    """
    try:
        text = resolve_path(settings, path).read_text(encoding="utf-8")
    except (ConfigError, OSError, UnicodeDecodeError):
        return ()
    values = (os.environ.get(name, "") for name in referenced_variables(text))
    return tuple(value for value in values if len(value) >= _SHORTEST_MASKED)


def _masked(value: Any, secrets: tuple[str, ...]) -> Any:
    """Mask each secret in every string of an answer, the keys of a mapping included."""
    if isinstance(value, str):
        return mask_secrets(value, *secrets)
    if isinstance(value, dict):
        mapping = cast("dict[str, Any]", value)
        return {_masked(key, secrets): _masked(item, secrets) for key, item in mapping.items()}
    if isinstance(value, list):
        return [_masked(item, secrets) for item in cast("list[object]", value)]
    return value


def _answer(
    tool_error: Any,
    work: Callable[[], _R],
    secrets: tuple[str, ...] = (),
    *,
    row_values: bool = False,
) -> _R:
    """Run a tool's work, masking `secrets` in its answer, or fail the call.

    The SDK passes only a `ToolError`'s message on to the agent, so every
    failure becomes one, named as `run --json` names it, with the secrets
    masked there too. Unless `row_values` is set, a message that can quote
    the data is left out.
    """
    try:
        result = work()
    except (Exception, PanicException) as exc:
        # A panic in Polars is no `Exception`, and would stop the server instead.
        failure = _failure(exc, row_values=row_values)
        raise tool_error(mask_secrets(failure, *secrets)) from exc
    return cast("_R", _masked(result, secrets))


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

        Reads no rows. Returns what `veridelta validate --json` prints: its errors and
        warnings, each placed in the file where the check can.
        """
        return _answer(
            loaded.tool_error,
            lambda: check_configuration(
                settings, path, schemas=schemas, allow_missing_env=allow_missing_env
            ),
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

    def run_comparison(
        path: Annotated[str, Field(description="The configuration file.")],
    ) -> RunReport:
        """Compare the two datasets a Veridelta configuration file names.

        Returns what `veridelta run --json` prints, with the verdict and the exit code a run gives.
        """
        return _answer(
            loaded.tool_error,
            lambda: run_configuration(settings, path),
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

    def describe_schema(
        path: Annotated[str, Field(description="The configuration file.")],
        side: Annotated[Literal["source", "target"], Field(description="The side to describe.")],
    ) -> SchemaReport:
        """List the columns of one side a Veridelta configuration file names, with their types.

        Reads no rows. Names are as stored, before normalize_column_names or a rename_to.
        """
        return _answer(
            loaded.tool_error,
            lambda: describe_side(settings, path, side),
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

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
        return _answer(
            loaded.tool_error,
            lambda: read_rows(settings, path, kind, limit),
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

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
            float,
            Field(gt=0, le=1, description="Share of source rows to read, chosen by primary key."),
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
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

    def suggest_rules(
        path: Annotated[str, Field(description="The configuration file.")],
        max_share: Annotated[
            float,
            Field(
                gt=0,
                le=1,
                description="Largest gap a tolerance may explain, as a share of the larger value.",
            ),
        ] = DEFAULT_MAX_SHARE,
    ) -> SuggestionReport:
        """Suggest rules that would explain the differences, each with its evidence.

        Returns what veridelta suggest --json prints. Needs a server started with
        --allow-row-values.
        """
        return _answer(
            loaded.tool_error,
            lambda: suggest(settings, path, max_share=max_share),
            environment_values(settings, path),
            row_values=settings.allow_row_values,
        )

    for tool in (
        validate_config,
        run_comparison,
        describe_schema,
        read_discrepancies,
        propose_value_maps,
        suggest_rules,
    ):
        server.tool(description=inspect.getdoc(tool))(tool)
    return server


def serve(settings: Settings) -> None:
    """Answer an agent's host over stdio until it disconnects.

    A configuration's relative paths resolve against the working directory,
    as on the command line, and the tools check them there. `veridelta mcp`
    runs the server in its first root; a program that calls this from
    another folder has its relative paths checked against that folder.

    Args:
        settings (Settings): The roots every tool is held to.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    build_server(settings).run(transport="stdio")
