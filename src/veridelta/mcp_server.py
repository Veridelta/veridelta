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
allows queries too, and a side connects to another machine only when that
person names its host. The SDK is imported by the first `build_server()`, not
with this module, so `veridelta` imports without the extra.
"""

import functools
import glob
import importlib
import inspect
import json
import logging
import os
import re
import shutil
import stat
from collections.abc import Callable, Generator, Iterator, Mapping
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
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit
from urllib.request import url2pathname

import polars as pl
from polars.exceptions import PanicException
from pydantic import Field

from veridelta import __version__
from veridelta._checks import error_from_exception, unset_variable_findings
from veridelta.config import load_config, referenced_variables
from veridelta.connectors.base import (
    mask_secrets,
    names_another_machine,
    shown_location,
    without_location,
)
from veridelta.connectors.database import connection_uri, connectorx_url, sqlite_path
from veridelta.connectors.duckdb import sandboxed
from veridelta.connectors.lakehouse import (
    CheckedDeltaTable,
    DeltaLogCopy,
    checked_file_io,
    checking_table_files,
    deletion_vector_files,
    delta_log_vectors,
    delta_reads_here,
)
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
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
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

_SERVICE_SCHEMES: Final = frozenset({"hf", "md", "motherduck"})
"""URI schemes whose reader reaches one service's own hosts, Hugging Face's or
MotherDuck's, so a configuration chooses no host with them."""

_LOCAL_SCHEMES: Final = frozenset({"file", "fs", "opendal+fs"})
"""URI schemes whose reader opens the URI's path on this machine: `file` for
every reader, which `_local_paths` refuses with a host other than `localhost`,
and `fs` and `opendal+fs` for Delta Lake's, whatever host they name.

Any other scheme of two or more characters names another machine, so its host
must be allowed, even one no reader is known to take: a reader that knows more
schemes than this module, such as `hdfs`, `sftp`, or `s3n`, connects to the host
all the same."""

_TWICE_DECODED: Final = frozenset({"fs", "opendal+fs"})
"""URI schemes whose path deltalake decodes twice before it reads a table's log."""

_URL_SPACE: Final = "".join(chr(code) for code in range(0x21))
"""The C0 controls and the space, which a URL parser strips from both ends of a URL."""

_URI_SCHEME: Final = re.compile(r"[A-Za-z][A-Za-z0-9+.-]+:")
"""The start of a URI: a scheme of two or more characters, since one letter is a Windows drive."""

_HOST_OPTIONS: Final = frozenset(
    {
        "account_name",
        "adls.account-name",
        "aws_bucket",
        "aws_bucket_name",
        "aws_endpoint",
        "aws_endpoint_url",
        "azure_container_name",
        "azure_endpoint",
        "azure_storage_account_name",
        "azure_storage_endpoint",
        "bucket",
        "bucket_name",
        "container_name",
        "endpoint",
        "endpoint_url",
        "google_bucket",
        "google_bucket_name",
        "s3.endpoint",
    }
)
"""Storage option keys, in lowercase, whose value names the host a reader connects
to: an endpoint, with or without a scheme, a bucket, or an account name."""

_REGION_OPTIONS: Final = frozenset(
    {"aws_default_region", "aws_region", "client.region", "default_region", "region", "s3.region"}
)
"""Storage option keys, in lowercase, whose value a reader writes into an AWS host
name, so a tool passes on only a value of letters, digits, and hyphens."""

_REGION: Final = re.compile(r"[A-Za-z0-9-]+")
"""A region a tool passes on, such as `us-east-1`."""

_NO_HOST_OPTIONS: Final = frozenset(
    {
        "access_key",
        "access_key_id",
        "account_key",
        "adls.account-key",
        "adls.client-id",
        "adls.client-secret",
        "adls.sas-token",
        "adls.tenant-id",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_skip_signature",
        "aws_token",
        "azure_client_id",
        "azure_client_secret",
        "azure_storage_access_key",
        "azure_storage_account_key",
        "azure_storage_client_id",
        "azure_storage_client_secret",
        "azure_storage_sas_key",
        "azure_storage_sas_token",
        "azure_storage_tenant_id",
        "azure_storage_token",
        "azure_tenant_id",
        "bearer_token",
        "client.access-key-id",
        "client.secret-access-key",
        "client.session-token",
        "client_id",
        "client_secret",
        "gcs.oauth2.token",
        "hf.token",
        "s3.access-key-id",
        "s3.anonymous",
        "s3.secret-access-key",
        "s3.session-token",
        "sas_key",
        "sas_token",
        "secret_access_key",
        "session_token",
        "skip_signature",
        "tenant_id",
        "token",
    }
)
"""Storage option keys, in lowercase, that hold a credential or a switch and choose
no host. A tool refuses any key in none of these sets, since a reader can take
an unlisted one, such as a service host, a proxy, a key file, or an emulator, as
a place to connect."""

_HOST_AND_PORT: Final = re.compile(
    r"([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*|\[[0-9A-Fa-f:.]+\])(?::[0-9]+)?"
)
"""A host with an optional port, in the one shape every reader splits alike: DNS
labels or an IP address in ASCII, and a port of digits.

Readers split other text in different places. The AWS SDK in PyArrow ends a host
at its first `:`, a URL parser at its last `@`, and a driver can decode `%` or
take a `\\` as a `/`. So a tool checks a host only when it is written this way,
and nothing else stands in its authority."""

_PAST_AUTHORITY: Final = r"[^@\\\x00-\x1f\x7f]*"
"""What may follow a URL's authority: anything but an `@`, a `\\`, or a control character."""

_URL: Final = re.compile(
    rf"[A-Za-z][A-Za-z0-9+.-]*:(?://([^/]*)(?:/{_PAST_AUTHORITY})?|(?!//){_PAST_AUTHORITY})"
)
"""A location or an endpoint with a scheme, its authority, if any, in the group.

The authority runs to the first `/`, where every reader ends it. No `@`, `\\`, or
control character may follow, since a reader that looks for a login or a
separator further on would find a host there."""

_NAMED_FILE: Final = re.compile(
    rf"[A-Za-z][A-Za-z0-9+.-]*:(?://([^/]*)(?:/[^\\\x00-\x1f\x7f]*)?|(?!//){_PAST_AUTHORITY})"
)
"""The URI of a file a table's log or metadata names, its authority, if any, in the group.

It is `_URL`'s shape, but with an `@` allowed after the authority, since a
Delta Lake log can name a file in a partition such as `owner=ann@x.com`. Only
the authority chooses the host for the readers of such a file: deltalake's
store, fixed to the table's host, and PyIceberg's FileIO, which reads the
authority up to the first `/`."""

_DATABASE_URI: Final = re.compile(
    r"[A-Za-z][A-Za-z0-9+.-]*://(?:[^@/?#\\\x00-\x20\x7f]*@)?([^@/?#\\\x00-\x20\x7f]*)"
    r"(?:[/?][^@#\\\x00-\x1f\x7f]*)?"
)
"""A database URI: a scheme, an optional login, its hosts, separated by commas, and
then its database and options, with the hosts in the group.

Drivers split a URI in other places than `urlsplit` does: tokio-postgres reads
the login up to the first `@` anywhere in it, and takes a `#` into the host. So
a URI passes only with no `@` past its login, no `#`, and no `\\`, space, or
control character where a host or a login stands."""

_MOTHERDUCK_NAME: Final = re.compile(
    r"(?:md|motherduck):(?:[A-Za-z0-9_-][A-Za-z0-9_/-]*)?", re.IGNORECASE | re.ASCII
)
"""A MotherDuck `database` before its `?`, such as `md:sales` or `md:_share/s/1a-2b`,
which a tool passes on.

MotherDuck's extension reads options after a `&` or a `#` too, and DuckDB opens
`md://x` as a file and not through MotherDuck, so a tool passes on no other
character there. Case is ignored in ASCII only, as DuckDB ignores it, so the
Kelvin sign does not count as a `k`."""

_MOTHERDUCK_OPTIONS: Final = frozenset({"attach_mode", "saas_mode", "session_name"})
"""Options of a MotherDuck `database`, after its `?`, that a tool passes on.

MotherDuck's extension reads its settings there, and some, such as
`motherduck_host`, choose the host it connects to, so a tool passes on only
these, which choose none."""

_OPTION_VALUE: Final = re.compile(r"[A-Za-z0-9_-]*")
"""A MotherDuck option's value a tool passes on, such as `single`."""

_HOST_PARAMETERS: Final = frozenset({"host", "hostaddr", "socket"})
"""Query parameters of a database URI that name the server to connect to, which a
driver reads in place of, or beside, the URI's host."""

_FILE_PARAMETERS: Final = frozenset(
    {"sslcert", "sslkey", "sslrootcert", "ssl-ca", "trust_server_certificate_ca"}
)
"""Query parameters of a database URI that name a file on this machine, which
ConnectorX opens for TLS: Postgres's certificate, key, and certificate
authority, MySQL's certificate authority, and SQL Server's."""

_ORACLE_PATH: Final = re.compile(r"(?:/[A-Za-z0-9_.$-]*(?::[A-Za-z]+)?(?:/[A-Za-z0-9_.$-]+)?)?")
"""An Oracle URI's path a tool passes on: a service name, then at most a server
type and an instance name, as in `/sales:dedicated/sales1`.

ConnectorX writes the path, decoded, after the host in Oracle's connect string,
where other characters can add an address or an option that names a proxy."""

_HOST: Final = re.compile(r"[a-z0-9_-]+(?:\.[a-z0-9_-]+)*|\[[0-9a-f:.]+\]")
"""An allowed host: DNS labels, an IPv4 address, or an IPv6 address in brackets.

A destination counts as allowed only when it equals one of them, so a host
written any other way, such as with a percent sign, is refused rather than read
as a driver might read it."""

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
        allowed_hosts (tuple[str, ...]): The hosts a side may connect to, in
            lowercase on creation. A side that names any other host, such as a
            database server, an object store's bucket, or a URL's host, is
            refused before it connects. Defaults to none, so every tool reads
            only data on this machine, MotherDuck, Hugging Face, or BigQuery with
            the server's own credentials.
    """

    roots: tuple[Path, ...]
    allow_row_values: bool = False
    max_rows: int = DEFAULT_ROW_CAP
    allow_queries: bool = False
    allowed_hosts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Resolve every root, so a path compares with them as the filesystem does.

        Raises:
            ConfigError: If no root is given, `max_rows` is below 1, or an
                allowed host is not a host.
        """
        if not self.roots:
            raise ConfigError("The MCP server needs at least one folder to read from.")
        if self.max_rows < 1:
            raise ConfigError(f"The MCP server's row cap must be at least 1, got {self.max_rows}.")
        object.__setattr__(self, "roots", tuple(Path(root).resolve() for root in self.roots))
        object.__setattr__(
            self, "allowed_hosts", tuple(allowed_host(host) for host in self.allowed_hosts)
        )


def allowed_host(text: str) -> str:
    """Check a host the person who starts the server allows, and write it in lowercase.

    Args:
        text (str): A host name, an IPv4 address, or an IPv6 address in
            brackets, with no scheme, login, or port.

    Returns:
        str: The host in lowercase.

    Raises:
        ConfigError: If the text is not a host written that way.
    """
    host = text.lower()
    if not text.isascii() or _HOST.fullmatch(host) is None:
        raise ConfigError(
            f"--allow-host takes a host name, an IPv4 address, or an IPv6 address in "
            f"brackets, with no scheme or port, got {text!r}."
        )
    return host


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
    """Resolve a tool call's path against the first root, or return None outside every root.

    A path on another machine is outside every root, and is not resolved.
    """
    if names_another_machine(path):
        return None
    resolved = (settings.roots[0] / path).resolve()
    return resolved if _under_roots(settings, resolved) else None


def _under_both_readings(settings: Settings, text: str) -> bool:
    """Whether a path lies under a root when the links are followed before a `..`, and after.

    A path on another machine never does, and is not resolved. The text is
    asked about before `Path` reads it, since on Python 3.11 Windows' `Path`
    drops a separator from a path such as `//host`, and the result reads as
    one on this machine.
    """
    if names_another_machine(text):
        return False
    path = Path(text)
    folded = Path(os.path.abspath(path))
    return all(_under_roots(settings, Path(_real_path(str(each)))) for each in (path, folded))


def _real_path(text: str) -> str:
    r"""Resolve a path as the system does, following its links.

    Windows' `realpath` adds back each name past the part it found with
    `join`, which reads a name such as `C:` as a drive and drops the folders
    before it. A URI read as a path holds one, as in `file:\C:\x`, so the
    path would read as relative to the drive. Where the result has no root,
    the path is resolved up to its last name, and the name is added with a
    separator. A result that still has no root comes back as `realpath` gave it.
    """
    # realpath, not Path.resolve, since Python 3.11 and 3.12 raise on a symbolic link loop.
    resolved = os.path.realpath(text)
    head, name = os.path.split(os.path.normpath(text))
    if os.path.isabs(resolved) or not name:
        return resolved
    parent = _real_path(head)
    return f"{parent.rstrip(os.sep)}{os.sep}{name}" if os.path.isabs(parent) else resolved


def _opened_inside(settings: Settings, location: str) -> bool:
    """Whether a path a configuration names lies under a root, resolved where it is opened.

    A reader or the artifact writer resolves a relative path against the
    working directory, as the command line does, so the check does too. The
    command runs the server in its first root, where the two agree, but a
    program that calls `serve()` from another folder gets the same check.

    The readers disagree on a `..` after a link. The system follows the link
    first, and PyArrow drops the link and the `..` without following it. So
    the path must lie under a root read either way. The text as written is
    held off another machine before `Path` reads it, as `_under_both_readings`
    says.

    A leading `~` is expanded by `os.path.expanduser` on the text as written,
    as the readers expand it. `Path` would drop a leading `./` and fold `//`
    first, and so read `./~/x` as a file in the home folder.
    """
    expanded = os.path.expanduser(location)
    return not names_another_machine(location) and _under_both_readings(settings, expanded)


def _opened(location: str) -> list[str]:
    """List the paths a reader may open for a location: itself, and each match of a pattern.

    Polars expands `*`, `?`, and `[...]` in a name, and `**` across folders.
    A wildcard matches a hidden name too, and `**` follows links. A name such
    as `lin[k]` does not exist as written, so resolving the pattern cannot
    follow the link it matches. The standard library lists the matches here,
    with hidden names and `**` included, and each is resolved on its own.
    Polars matches `.` and `..` as well, which the standard library never
    lists, so `_climbs` refuses each pattern that could match them first.
    A pattern on another machine is not matched, since that would reach the
    machine.
    """
    pattern = os.path.expanduser(location)
    if not _GLOB_CHARACTERS.intersection(location) or names_another_machine(pattern):
        return [location]
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


def _refuse_opened(settings: Settings, location: str, remote: bool) -> None:
    """Refuse a file that a lakehouse scan is about to open, outside the roots or on a host not allowed.

    A path on this machine is the one PyArrow opens: relative to the working
    directory, and with no `~` expanded. PyArrow drops a folder and the `..`
    after it before it follows a link, so the path must lie under a root read
    either way. A file on another machine is held to the allowed hosts, as a
    configuration's location is. The message leaves the file out, since the
    table's files named it.

    Raises:
        ConfigError: If the path lies outside every root, or the file on a host
            the server does not allow.
    """
    if remote:
        for _, host in _location_hosts("file", location, _NAMED_FILE):
            _refuse_host(settings, "A file that a lakehouse scan was about to open", host)
    elif not _under_both_readings(settings, location):
        raise ConfigError(
            "A Delta Lake or Iceberg scan was about to open a file outside the folders this "
            f"server reads from: {_roots(settings)}, so it was stopped. A tool reads data on "
            "this machine only from under them."
        )


@contextmanager
def _reading(settings: Settings) -> Generator[None]:
    """Hold what a tool's sides read to the roots, while they read.

    A DuckDB file's connection reads other files only from under the roots.
    A Delta Lake or Iceberg scan opens a file on this machine only from under
    them, and one elsewhere only on an allowed host, each checked just before
    it is opened. A Delta Lake scan lists and checks its table's files again
    as it starts, and reads the version it checked.
    """
    with (
        sandboxed(settings.roots),
        checking_table_files(
            functools.partial(_refuse_opened, settings),
            functools.partial(_checked_delta_table, settings),
        ),
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


def _split(location: str) -> SplitResult | None:
    """Split a location as a URL parser does, or return None for an authority that does not parse."""
    try:
        return urlsplit(location)
    except ValueError:
        return None


def _local_paths(location: str, named: str) -> list[str]:
    """Return each path on this machine a reader could open for a data location.

    Readers disagree on a URI. Polars and DuckDB open one whose scheme they do
    not know, such as `lakefs://r/t`, as a relative path. Polars opens even
    `s3:x/a.parquet`, with no `//`, `md://x/a.parquet`, or `FILE://x` that
    way, and DuckDB opens `lakefs://r/x.duckdb` too. So the location is checked as
    written, whatever its scheme.

    A local scheme's URI opens its path, and the readers read that path
    differently. Polars decodes `%` escapes. PyIceberg adds a host to the path
    and decodes nothing, so it opens `file:tmp/x` as `tmp/x`. Polars and
    deltalake start the path at `/`, so they open it as `/tmp/x`, and
    deltalake parses the URI as the URL standard says, so it also reads a
    backslash as `/` and strips the spaces at its ends. So a local scheme's URI
    yields each of these paths too, and the caller holds every one to the
    roots.

    A `file:` URI that names a host other than `localhost`, in either way of
    parsing it, is refused: deltalake opens `file://host/x` on that host on
    Windows, Polars refuses it, and PyIceberg reads the host as a folder.
    Delta Lake opens `fs:` and `opendal+fs:` here whatever host they name. A
    local scheme's URI whose path starts with `//`, in either way of parsing
    it, is refused too: Python 3.14 converts `//localhost/x`, or `//` and this
    machine's name, to `/x`, while DuckDB opens `/localhost/x`, and Windows
    reads `//host/x` as a path on another machine. So is a URI whose path
    Python cannot convert, such as one with a second `:` on Windows before
    3.14. The message names `named`, the setting, but not the value, which a
    table's files can hold.

    A `dbfs:` URI is refused whatever its host: deltalake opens its path on
    this machine, and ignores the host, once the server's environment allows
    it to.

    Raises:
        ConfigError: If a `file:` URI names a host, a local scheme's URI has
            a path that starts with `//`, Python cannot convert one of the
            URI's paths, or the location is a `dbfs:` URI.
    """
    parts = _split(location)
    if parts is not None and parts.scheme == "dbfs":
        raise ConfigError(
            f"The {named} is a dbfs: URI. deltalake opens its path on this machine whatever "
            "host it names, so this server refuses it. Write it as a path under the folders "
            "this server reads from."
        )
    if parts is None or parts.scheme not in _LOCAL_SCHEMES:
        return [location]
    standard = _split(location.replace("\\", "/").strip(_URL_SPACE))
    hosts = {parts.netloc, "" if standard is None else standard.netloc}
    if parts.scheme == "file" and any(
        host and not (host.isascii() and host.lower() == "localhost") for host in hosts
    ):
        raise ConfigError(
            f"The {named} is a file URI that names a host, and on Windows a reader opens it on "
            "that machine, so this server refuses it. Write it with nothing between '//' and "
            "the path's '/', or as a path under the folders this server reads from."
        )
    if any(each.path.startswith("//") for each in (parts, standard) if each is not None):
        raise ConfigError(
            f"The {named} is a URI whose path starts with '//'. Python 3.14 reads a host there "
            "that DuckDB reads as a folder, and Windows reads a path on another machine, so "
            "this server refuses it. Write it with three slashes after the scheme's ':', or "
            "as a path under the folders this server reads from."
        )
    try:
        paths = [
            url2pathname(parts.path),
            parts.netloc + url2pathname(parts.path.replace("%", "%25")),
        ]
        if standard is not None:
            # Rooted at `/` before it is converted, since on Windows a separator added after
            # the conversion can make a path such as `\\host\share`, which names another machine.
            path = standard.path
            paths.append(url2pathname(path if path.startswith("/") else f"/{path}"))
    except (OSError, ValueError, IndexError):
        # Before 3.14, Windows raises an OSError or an IndexError for a path with a second
        # `:`. Python 3.14 raises a URLError, an OSError, for a host after `//`, which the
        # check above refuses first.
        raise ConfigError(
            f"The {named} is a file URI whose path Python cannot read as a path on this "
            "machine, so this server refuses it. Write it as a path under the folders this "
            "server reads from."
        ) from None
    # The decoded path comes first, so a refusal names the file the reader opens, and the
    # same check refuses first on every system. The text as written is checked last.
    return [*dict.fromkeys(paths), location]


# Each log DuckDB names beside a database file, by the setting a refusal names.
# A read-only open replays `.wal`, and `.wal.checkpoint` after a crash during a
# checkpoint. Only a read-write open writes `.wal.recovery`, but it is checked too.
_DUCKDB_LOGS = (
    ("database's WAL", ".wal"),
    ("database's checkpoint WAL", ".wal.checkpoint"),
    ("database's recovery WAL", ".wal.recovery"),
)


def _wal_beside(path: str, suffix: str = ".wal") -> str:
    """Add a log's suffix to a database's path as DuckDB's `AddSuffixToPath` does.

    It goes before the first `?` in the path rather than at the end, so
    `L?/x.duckdb` has its log at `L.wal?/x.duckdb`, in another folder.
    """
    mark = path.find("?")
    return f"{path}{suffix}" if mark < 0 else f"{path[:mark]}{suffix}{path[mark:]}"


def _resolved_database(side: str, database: str) -> str:
    """Resolve a DuckDB file's path to the text the server checks and hands DuckDB.

    DuckDB makes the path absolute and resolves it with the system's
    `realpath`. Where that fails, as past the links the system follows, it
    folds the rest of the path by text, which no reading here matches. So
    the server resolves the path itself, with every link followed and every
    `..` folded, and the connector opens that text, in which DuckDB finds
    nothing left to resolve. The system must also open the text as written,
    so a file followed by a separator or by `..` is refused on every Python
    version, where `os.path.realpath` before 3.13 folds it by text.

    Raises:
        ConfigError: If the path does not lead to an existing file.
    """
    expanded = os.path.expanduser(database)
    try:
        os.stat(expanded)
        return os.path.realpath(expanded, strict=True)
    except OSError as exc:
        raise ConfigError(
            f"The {side} database '{database}' does not resolve to an existing file, so "
            "this server cannot tell which file and write-ahead logs DuckDB would open. "
            "Write the path of an existing DuckDB file."
        ) from exc


def _url_text(location: str) -> str:
    """Return a URI as a URL parser reads it: with no space at its ends, and no tab or line break."""
    text = location.strip(_URL_SPACE)
    for character in "\t\n\r":
        text = text.replace(character, "")
    return text


def _holds_dot_segment(location: str) -> bool:
    """Tell whether a URI holds a `.` or `..` segment, in any spelling a URL parser removes.

    deltalake parses a table URI as the URL standard says. That drops the tabs
    and line breaks anywhere in it, reads `%2e` as a dot in a segment, and
    removes each `.` and `..` segment by its text, before any folder is read.
    This server reads the path a URI names as the system does, which follows
    a link before `..` leaves it, and finds no folder under one that does not
    exist. So the two could read different logs. This decodes every `%`
    escape, as often as one remains, and splits at each character that can
    end a segment, so it finds more segments than a parser removes, never
    fewer. A path with no scheme is not a URI, and deltalake opens it as the
    system does.
    """
    text = _url_text(location)
    if _URI_SCHEME.match(text) is None:
        return False
    while (decoded := unquote(text)) != text:
        text = decoded
    return not {".", ".."}.isdisjoint(re.split(r"[/\\?#]", text))


def _decoded_twice(location: str) -> bool:
    """Tell whether a table URI holds a `%` escape that deltalake decodes twice.

    deltalake decodes the path of an `fs:` or `opendal+fs:` URI twice before
    it reads the log, so it reads `%2565` as `e`. This server and the files
    the table lists decode it once, so the log the listing reads and the one
    deltalake reads could differ. Any `%` in such a URI counts.
    """
    text = _url_text(location)
    scheme = _URI_SCHEME.match(text)
    return scheme is not None and scheme.group()[:-1].lower() in _TWICE_DECODED and "%" in text


def _data_locations(side: str, config: SourceRef) -> list[tuple[str, str, str]]:
    """Name each path on this machine a side could read: its setting, location, and path."""
    named: list[tuple[str, str]] = []
    if isinstance(config, SourceConfig):
        named = [("path", config.path)]
    elif isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        named = [("table_uri", config.table_uri)]
    elif isinstance(config, DuckDBConfig) and not config.is_motherduck:
        named = [("database", config.database)]
    elif isinstance(config, DatabaseConfig):
        # The connector opens this resolved text, so a link or `..` leads nowhere else.
        path = sqlite_path(config)
        return [] if path is None else [("SQLite file", path, path)]
    return [
        (setting, location, path)
        for setting, location in named
        for path in _local_paths(location, f"{side} {setting}")
    ]


def _file_at(path: str) -> os.stat_result | None:
    """Return the file at a path, following its links, or None when none is there."""
    try:
        return os.stat(path)
    except FileNotFoundError:
        return None


def _refuse_outside_copy(target: Path, copy: DeltaLogCopy) -> None:
    """Refuse a path in a table's log copy that is a link, or that a link leads out of the copy's folder.

    Raises:
        ValueError: If the path is a link, or does not lie in the copy's
            folder once its links are followed.
    """
    inside = Path(os.path.realpath(target)).is_relative_to(os.path.realpath(copy.folder))
    if os.path.islink(target) or not inside:
        raise ValueError(
            "a file of the copy of the table's log would lie outside the copy's folder, so "
            "this server reads none of the table"
        )


def _copy_checked(
    source: str, copy: DeltaLogCopy, target: Path, seen: os.stat_result | None
) -> None:
    """Copy a file just checked into a table's log copy, when the file opened is the one checked.

    `seen` is the file at `source` before the check, or None when none was
    there, which leaves the copy without it, as the table was. The file
    opened must be that same file, by its device and number, and a regular
    file. Otherwise another file was swapped in, before the check or after
    it, so it is refused before it is read: a file swapped in could lie
    anywhere. The file is opened without waiting for a writer, so a named
    pipe is refused, not waited on. Two swaps that bracket the check, one
    file for the check and the first again by the open, pass, as
    `rules/security.md` says.

    `target` is a name this server builds in the copy's folder, never a path
    a log names. It must lie in that folder after its links, before and after
    its folders are made, and the file is created there new, never through a
    link, so the copy writes nothing outside its folder.

    Raises:
        ValueError: If the file opened is not the one at the path before the
            check, or is not a regular file, or `target` leads out of the
            copy's folder.
    """
    if seen is None:
        return
    # Only Windows has O_BINARY, and O_NONBLOCK opens a named pipe without waiting for a writer.
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0)
    with open(os.open(source, flags), "rb") as file:
        opened = os.fstat(file.fileno())
        if not os.path.samestat(seen, opened):
            raise ValueError(
                "a file of the table was replaced as this server checked it, so it reads none "
                "of the table"
            )
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError(
                "a file of the table is not a regular file, so this server reads none of the table"
            )
        _refuse_outside_copy(target, copy)
        target.parent.mkdir(parents=True, exist_ok=True)
        _refuse_outside_copy(target, copy)
        # O_EXCL creates the file or fails, and fails at a link too. Only POSIX has O_NOFOLLOW.
        new = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        written = os.open(target, new | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with open(written, "wb") as copied:
            shutil.copyfileobj(file, copied)


def _delta_log(
    side: str, uri: str, copy: DeltaLogCopy | None
) -> Generator[tuple[str, str, bool], None, tuple[list[str], dict[Path, Path]]]:
    """Name each entry of the log of a table on this machine, each before it is read.

    With a `copy`, for a table deltalake reads from this machine, each file of
    the log is copied into it just after its check, in a folder of its own
    for each folder the table's location names. A table deltalake reads from
    its store has its entries here checked, but none read.

    Returns:
        tuple[list[str], dict[Path, Path]]: The copy of each file of the log,
            and the folder of the copy of each of the table's folders that
            holds a log.
    """
    logged: list[str] = []
    logs: dict[Path, Path] = {}
    for index, local in enumerate(_local_paths(uri, f"{side} table_uri")):
        # Absolute, so a reading such as `s3:/b/t`, from `s3://b/t`, names no host.
        folder = Path(os.path.expanduser(local)).absolute()
        log = folder / "_delta_log"
        yield "log", str(log), True
        for parent, folders, files in os.walk(log):
            for name in sorted(folders + files):
                path = os.path.join(parent, name)
                seen = _file_at(path) if copy is not None and name in files else None
                yield "log file", path, True
                if copy is not None and seen is not None:
                    logs[folder] = copy.folder / str(index)
                    # The walk's own names, each one entry of a folder, under the copy's folder.
                    copied = logs[folder] / os.path.relpath(path, folder)
                    _copy_checked(path, copy, copied, seen)
                    logged.append(str(copied))
    return logged, logs


def _delta_files(
    settings: Settings, side: str, config: DeltaLakeConfig
) -> Generator[tuple[str, str, bool], None, CheckedDeltaTable]:
    """Name each file a scan of a Delta Lake table reads, from the version it reads.

    The log of a table on this machine comes first, each entry before the log
    is read, since a commit or checkpoint can be a link out of the table's
    folder. The walk follows no link, so each one is checked before it is
    opened. The data files then come from the log, and so do the deletion
    vectors, which a log can store at a path of their own, anywhere. A
    checkpoint can name a sidecar anywhere too, which deltalake reads as it
    loads the table, so the log is read before deltalake reads it, and a
    table whose log records a sidecar is refused.

    deltalake reads the log again as it loads the table, and a vector as a
    scan starts, in Rust, where no check runs. So for a table deltalake reads
    from this machine, each file of the log, and each vector at a path
    relative to the table, is copied just after its check into a private
    folder outside every root, and the log read and the table loaded are that
    copy. A file opened for the copy that is not the one checked is refused.
    Each file of the copy lies at a name this server builds in the copy's
    folder: the walk's own names for the log, and a vector's folder of
    letters and digits and its UUID. deltalake would read a vector that the
    log stores at its own path where the log says, which a copy could change
    only by writing there or by rewriting the log, so such a table is
    refused.

    A path that starts with `~` is expanded as Polars expands it, and the
    walk reads the expanded path, since deltalake expands only `~` and `~/`
    and would read `~alice/t` as a folder of that name.

    Returns:
        CheckedDeltaTable: The table, at the version whose files were named.

    Raises:
        ValueError: If the copy would lie under a root or in a folder whose
            path deltalake reads as another folder's, more than one folder
            the table's location names holds a log, or the log of a table on
            this machine stores a vector at its own path.
    """
    from deltalake import DeltaTable

    uri = config.table_uri
    if uri.startswith("~"):
        # deltalake expands only `~` and `~/`, and reads `~alice/t` as a folder of that name,
        # where Polars reads the home of the user it names. So the walk, and deltalake for a
        # table it reads from its store, read the path Polars reads.
        uri = os.path.expanduser(uri)
    copy = DeltaLogCopy() if delta_reads_here(uri) else None
    if copy is not None and _inside(settings, str(copy.folder)) is not None:
        raise ValueError(
            "the system's temporary folder lies under a folder this server reads from, where "
            "this server would copy the table's log for deltalake to read. Start the server "
            "with TMPDIR set to a folder outside them"
        )
    # deltalake decodes a `%` escape in the copy's path, and outside Windows reads a `\` there
    # as `/`, so it would read the log of another folder, which no check saw. Its `table_uri`
    # still names the copy, so only the path itself can show it.
    rewritten = "%" if os.sep == "\\" else "%\\"
    if copy is not None and any(character in rewritten for character in str(copy.folder)):
        raise ValueError(
            "the path of the system's temporary folder holds a '%', or a '\\' outside Windows, "
            "which deltalake reads as other text as it reads the copy of the table's log there. "
            "Start the server with TMPDIR set to a folder whose path holds neither"
        )
    logged, logs = yield from _delta_log(side, uri, copy)
    if len(logs) > 1:
        raise ValueError(
            "readers read the table's location as more than one folder, and more than one "
            "holds a Delta Lake log, so this server reads none of them"
        )
    # deltalake reads each sidecar a checkpoint names as it loads the table, so the log is
    # read first, and a log that names one is refused.
    vectors = delta_log_vectors(uri, config.storage_options, logged)
    if copy is None:
        options = config.storage_options or None
        checked = CheckedDeltaTable(
            DeltaTable(uri, version=config.version, storage_options=options)
        )
        for name in checked.table.file_uris():
            yield "data file", name, True
        for location in deletion_vector_files(checked.table, vectors):
            yield "deletion vector", location, True
        return checked
    return (yield from _copied_delta_files(config, copy, logs, vectors))


def _copied_delta_files(
    config: DeltaLakeConfig,
    copy: DeltaLogCopy,
    logs: dict[Path, Path],
    vectors: list[dict[str, Any]],
) -> Generator[tuple[str, str, bool], None, CheckedDeltaTable]:
    """Load a table on this machine from the copy of its log, and name each file a scan reads.

    Each vector, at a path relative to the table, is copied into the copy just
    after its check, at a name built from its folder of letters and digits
    and its UUID.

    Args:
        config (DeltaLakeConfig): The table.
        copy (DeltaLogCopy): The copy, which holds the checked log.
        logs (dict[Path, Path]): The folder of the copy of the table's folder
            that holds a log, or nothing for a table with no log.
        vectors (list[dict[str, Any]]): Each vector the log records.

    Returns:
        CheckedDeltaTable: The table, at the version whose files were named.

    Raises:
        ValueError: If the log stores a vector at its own path.
    """
    from deltalake import DeltaTable

    if any(vector.get("storageType") == "p" for vector in vectors):
        # deltalake opens such a vector where the log says, so a copy could hold it only in
        # a folder the log names, or with the log rewritten.
        raise ValueError(
            "the log stores a deletion vector at a path of its own, which deltalake reads "
            "where the log says, apart from this server's check, so this server reads none "
            "of the table. Read such a table with veridelta run"
        )
    # A copy that holds no log, as of a table that does not exist, fails as the table would.
    found = next(iter(logs), None)
    root = copy.folder if found is None else logs[found]
    options = config.storage_options or None
    table = DeltaTable(str(root), version=config.version, storage_options=options)
    checked = CheckedDeltaTable(table, found, copy)
    for name in checked.table.file_uris():
        yield "data file", checked.located(name), True
    copied: set[str] = set()
    for location in deletion_vector_files(checked.table, vectors):
        # A vector at a relative path, in a folder of letters and digits under the table's,
        # named by the UUID the log encodes. One file can hold many, and a log name one twice.
        local = checked.located(location)
        seen = None if local in copied else _file_at(local)
        yield "deletion vector", local, True
        _copy_checked(local, copy, root / os.path.relpath(local, found), seen)
        copied.add(local)
    return checked


def _iceberg_files(
    config: IcebergConfig, check: Callable[[str, bool], None]
) -> Generator[tuple[str, str, bool], None, None]:
    """Name each file a scan of an Iceberg table reads, each before it is opened.

    A table folder's version hint, the manifest list, and each manifest name
    the next files to read, so the caller checks each name before this reads
    the file. The version hint is read as `StaticTable.from_metadata` reads
    it, so the check and the scan read the same metadata file, but its text is
    never quoted, since it is a value from a file. The data and delete files
    come from the manifests, as a scan plans them.

    The files are read as the scan on this server reads them: through the
    FileIO `checked_file_io` builds from `storage_options` alone, whatever the
    metadata's own properties say, which runs `check` on each file and opens
    none through a scheme outside an object store's.
    """
    from pyiceberg.serializers import FromInputFile

    io = checked_file_io(dict(config.storage_options or {}), check)
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
    settings: Settings, side: str, config: DeltaLakeConfig | IcebergConfig
) -> Generator[tuple[str, str, bool], None, CheckedDeltaTable | None]:
    """Name each file that the scan of a lakehouse table reads.

    A table's log or metadata names its files by full URI, so they can lie
    anywhere. A table whose files cannot be listed is refused, since the scan
    would read the same log or metadata. An Iceberg table's files are read
    with the scan's own check, as well as the caller's.

    Returns:
        CheckedDeltaTable | None: A Delta Lake table, at the version listed, or None
            for an Iceberg table.

    Raises:
        ConfigError: If a Delta Lake table URI could lead deltalake to another
            log than the one this server lists.
        ConnectorError: If the extra is missing, or the table cannot be listed.
    """
    files: Generator[tuple[str, str, bool], None, CheckedDeltaTable | None]
    if isinstance(config, DeltaLakeConfig) and _holds_dot_segment(config.table_uri):
        raise ConfigError(
            f"The {side} table_uri '{shown_location(config.table_uri)}' is a URI with a '.' "
            "or '..' segment in its path. deltalake removes such a segment by its text before "
            "it reads the table, where this server reads the folder the system finds, so the "
            "two could read different tables. Write the URI without the segment, or write "
            "the table as a path."
        )
    if isinstance(config, DeltaLakeConfig) and _decoded_twice(config.table_uri):
        raise ConfigError(
            f"The {side} table_uri '{shown_location(config.table_uri)}' is an fs: or "
            "opendal+fs: URI with a '%' escape. deltalake decodes such a URI's path twice "
            "before it reads the table, where this server decodes it once, so the two could "
            "read different tables. Write the table as a path, or as a file: URI."
        )
    if isinstance(config, DeltaLakeConfig):
        files, kind, extra = _delta_files(settings, side, config), "Delta Lake table", "delta"
    else:
        check = functools.partial(_refuse_opened, settings)
        files, kind, extra = _iceberg_files(config, check), "Iceberg table", "iceberg"
    try:
        return (yield from files)
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
    local: str,
    *,
    quoted: bool = True,
    expanded: bool = False,
    password: str | None = None,
) -> None:
    """Refuse a path on this machine that a side could read for a location, outside the roots.

    The message quotes the path only when `quoted`, since a path read from a
    file is a value from it, and masks `password` in it. With `expanded`, the
    location is one Polars expands as a pattern, so each file it matches is
    checked, and a pattern that can match `..`, or one in a `file:` URI, is
    refused.
    """
    named = f"{setting} '{mask_secrets(local, password)}'" if quoted else setting
    pattern = expanded and bool(_GLOB_CHARACTERS.intersection(location + local))
    if pattern and local != location:  # A `file:` URI, which `_local_paths` decoded.
        raise ConfigError(
            f"The {side} {setting} '{location}' is a file URI with a glob character in "
            "it. Polars expands a pattern in a URI with another matcher than a path's, "
            "so this server cannot check the files it would open. Write it as a path."
        )
    if pattern and _climbs(local):
        raise ConfigError(
            f"The {side} {named} is a glob pattern that can match '..'. "
            "A match can be a link, and '..' would then leave the folder the link "
            "points to, so this server refuses the pattern. Write the path without "
            "'..', and write out each name that starts with a dot."
        )
    opened = _opened(local) if expanded else [local]
    if not all(_opened_inside(settings, each) for each in opened):
        raise ConfigError(
            f"The {side} {named} is outside the folders this server "
            f"reads from: {_roots(settings)}. A tool reads data on this machine only "
            "from under them, so move the data into one, or ask the person who started "
            "the server to add its folder with --root."
        )


def _refuse_key_file(settings: Settings, side: str, config: SourceRef) -> None:
    """Refuse a Snowflake key file outside the roots, before the driver opens it.

    The driver opens `private_key_path` on this machine before it connects,
    with `~` expanded and relative to the working directory, as a reader opens
    a data path. So the key file is held to the same rules: a path on another
    machine is refused before anything resolves it, and the path must lie
    under a root read either way. The message names the setting and not the
    path, which can hold a `${NAME}` value.

    The driver expands `~` with `Path.expanduser`, unlike the readers, and
    `Path` drops a leading `./` first, so the driver opens `./~/k.pem` in the
    home folder. The check expands the text the same way, once the text as
    written is held off another machine.

    Raises:
        ConfigError: If the key file lies outside every root.
    """
    if not isinstance(config, SnowflakeConfig) or config.private_key_path is None:
        return
    location = config.private_key_path
    as_opened = str(Path(location).expanduser())
    if names_another_machine(location) or not _under_both_readings(settings, as_opened):
        raise ConfigError(
            f"The {side} private_key_path is outside the folders this server reads from: "
            f"{_roots(settings)}. The driver opens the key file on this machine, and a tool "
            "reads files here only from under them, so move the key into one, or ask the "
            "person who started the server to add its folder with --root."
        )


def _as_checked(settings: Settings, side: str, config: SourceRef) -> SourceRef:
    """Return a side as a tool opens it, with a DuckDB file named by the path checked.

    DuckDB replays the write-ahead logs beside the file as it opens it, before
    the sandbox applies, so each log is held to the roots as the file is. A
    resolved path that names another machine, as a Windows long path does,
    is refused too.
    """
    if not isinstance(config, DuckDBConfig) or config.is_motherduck:
        return config
    database = _resolved_database(side, config.database)
    _refuse_outside(settings, side, "database", database, database)
    for setting, suffix in _DUCKDB_LOGS:
        log = _wal_beside(database, suffix)
        _refuse_outside(settings, side, setting, log, log)
    return config.model_copy(update={"database": database})


def check_data_paths(
    settings: Settings, source: SourceRef, target: SourceRef
) -> tuple[SourceRef, SourceRef]:
    """Refuse a side whose data on this machine lies outside the roots.

    Every tool that opens a side calls this first, since a column name or an
    error message can carry a file's text as a row does. The paths are
    expanded and resolved as the readers do, so neither `~` nor a link leads
    out. A location with a scheme, such as `s3://`, is checked as a path too,
    since a reader that does not know the scheme opens it here. A path that
    holds a glob character is a pattern, and every file it matches must
    resolve under a root. A pattern may not reach `..`, since a match can be a
    link that `..` then leaves from its target. A `file:` URI may not hold a
    pattern at all, since Polars expands one with another matcher. A DuckDB
    file's path must resolve to an existing file, and the side returned names
    the file by that resolved path, so DuckDB opens the file checked and the
    logs beside it. A Delta Lake or Iceberg table, wherever it lies, has
    every file on this machine that its scan reads checked too, since its log
    or metadata can name a file anywhere, and every file elsewhere held to the
    hosts the server allows. Data on another machine that a side names, such
    as an object store, a database server, or a warehouse, is held to those
    hosts by `_refuse_hosts`, which a caller runs first, since listing a
    table's files connects to its host. A Snowflake key file is a file on this
    machine, which the driver opens before it connects, so it must lie under
    a root too.

    Every location the configuration names is checked before any file is
    opened, of either side: the listings come last.

    Args:
        settings (Settings): The roots the server was started with.
        source (SourceRef): The source configuration.
        target (SourceRef): The target configuration.

    Returns:
        tuple[SourceRef, SourceRef]: The source and the target to open, each
            DuckDB file named by its resolved path.

    Raises:
        ConfigError: If a file or folder a side reads, or a Snowflake key file,
            is outside every root, a DuckDB file's path does not resolve, or a
            file a lakehouse table names lies on a host the server does not
            allow.
        ConnectorError: If the files of a lakehouse table cannot be listed.
    """
    sides = (("source", source), ("target", target))
    checked: list[SourceRef] = []
    for side, config in sides:
        # A SQLite path carries the `password` field, as the driver reads it.
        password = config.password if isinstance(config, DatabaseConfig) else None
        for setting, location, local in _data_locations(side, config):
            _refuse_outside(
                settings, side, setting, location, local, expanded=True, password=password
            )
        _refuse_key_file(settings, side, config)
        checked.append(_as_checked(settings, side, config))
    for side, config in sides:
        if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
            for setting, location, quoted in _table_files(settings, side, config):
                _refuse_listed(settings, side, setting, location, quoted=quoted)
    return checked[0], checked[1]


def _refuse_listed(
    settings: Settings, side: str, setting: str, location: str, *, quoted: bool
) -> None:
    """Refuse a file a lakehouse table names, outside the roots or on a host the server does not allow.

    A log or metadata names a file by any location, so the file is checked as
    a configuration's location is: each path on this machine a reader could
    open for it, and the host it names. The message about a host leaves the
    location out, since a file named it.
    """
    for local in _local_paths(location, f"{side} {setting}"):
        _refuse_outside(settings, side, setting, location, local, quoted=quoted)
    for _, host in _location_hosts(setting, location, _NAMED_FILE):
        _refuse_host(settings, f"The {side} {setting}", host)


def _checked_delta_table(settings: Settings, config: DeltaLakeConfig) -> CheckedDeltaTable:
    """List and check every file a scan of a Delta Lake table reads, and return the version checked.

    A scan on this server calls this as it starts, and reads only the files of
    the version returned, so a file the table gains after `check_data_paths`
    listed it is checked too.

    Raises:
        ConfigError: If a file lies outside every root, or on a host the
            server does not allow.
        ConnectorError: If the files cannot be listed.
    """
    files = _table_files(settings, "scanned", config)
    while True:
        try:
            setting, location, quoted = next(files)
        except StopIteration as listed:
            return cast("CheckedDeltaTable", listed.value)
        _refuse_listed(settings, "scanned", setting, location, quoted=quoted)


def _host(text: str) -> str:
    """Return the host of a host with an optional port, or the text whole when it is not one.

    Text in no other shape comes back whole, which no allowed host equals, so a
    tool refuses it rather than check a part of it that a reader may not read.
    """
    written = _HOST_AND_PORT.fullmatch(text)
    return text if written is None else written.group(1)


def _url_host(url: str, shape: re.Pattern[str] = _URL) -> str:
    """Return the host a URL in `shape` names, the empty host for none, or the URL whole when it is unclear."""
    written = shape.fullmatch(url)
    if written is None:
        return url
    authority = written.group(1)
    return _host(authority) if authority else ""


def _location_hosts(
    setting: str, location: str, shape: re.Pattern[str] = _URL
) -> list[tuple[str, str]]:
    """Name the host a data location reaches, or none for a path here or a service's own hosts.

    Every scheme of two or more characters counts, other than a local or a
    service one, so a scheme this module has never heard of still needs its
    host allowed. One character is a drive letter, such as `C:`, and no scheme.
    `urlsplit` finds the scheme as a reader that skips leading spaces does, and
    the location must then match `shape` as written: `_URL`, or `_NAMED_FILE`
    for a file a table's log or metadata names. A location with no host,
    such as `hdfs:///t`, names the empty host, which is never allowed, since its
    reader would pick the host from elsewhere.
    """
    parts = _split(location)
    if parts is not None and (
        len(parts.scheme) < 2 or parts.scheme in _LOCAL_SCHEMES | _SERVICE_SCHEMES
    ):
        return []
    return [(setting, _url_host(location, shape))]


def _option_hosts(options: object) -> Iterator[tuple[str, str | None]]:
    """Name each host a side's storage options send a reader to, with the option that names it.

    A host of None marks an option a tool does not pass on: one outside the
    known keys, a key outside ASCII, or a region that a reader could not write
    into a host name. An endpoint is a host with an optional port, or a URL.
    """
    if not isinstance(options, Mapping):
        return
    for key, value in cast("Mapping[object, object]", options).items():
        name, text = str(key), str(value)
        setting = f"storage option '{key}'"
        if not name.isascii():
            yield setting, None
        elif name.lower() in _HOST_OPTIONS:
            yield setting, _url_host(text) if "://" in text else _host(text)
        elif name.lower() in _REGION_OPTIONS:
            if _REGION.fullmatch(text) is None:
                yield setting, None
        elif name.lower() not in _NO_HOST_OPTIONS:
            yield setting, None


def _refuse_database_uri(side: str, config: DatabaseConfig) -> None:
    """Refuse a database `uri` whose reader opens a file here, or signs in where no check sees.

    ConnectorX's BigQuery reader opens the key file the path names, and the
    key file names the host its sign-in goes to. Its Redshift IAM reader signs
    in through AWS at hosts its options choose. Postgres, MySQL, and SQL
    Server open the TLS files `_FILE_PARAMETERS` names. Oracle looks an
    `alias` up in a file on this machine, and writes the path into its
    connect string, so a path passes only in `_ORACLE_PATH`'s shape. A
    parameter is matched in ASCII only, as the readers match it.
    """
    parts = connectorx_url(config)
    if parts.scheme == "bigquery":
        raise ConfigError(
            f"The {side} uri reads BigQuery through ConnectorX, which signs in with a key file "
            "on this machine, and the key file names the host the sign-in goes to, so a tool "
            "does not read it. Use a source of type bigquery instead, which signs in with "
            "the server's own credentials."
        )
    if parts.scheme == "redshift-iam":
        raise ConfigError(
            f"The {side} uri signs in to Redshift through AWS, at hosts its options choose, "
            "so a tool does not read it. Write it as redshift:// with a user and a password."
        )
    names = {
        key.lower() for key, _ in parse_qsl(parts.query, keep_blank_values=True) if key.isascii()
    }
    files = sorted(names & _FILE_PARAMETERS)
    if files:
        raise ConfigError(
            f"The {side} uri parameter '{files[0]}' names a file on this machine, which the "
            "driver opens and this server does not check, so a tool does not pass it on. "
            "Leave it out."
        )
    if parts.scheme == "oracle" and (
        "alias" in names or _ORACLE_PATH.fullmatch(parts.path) is None
    ):
        raise ConfigError(
            f"The {side} uri has Oracle find its host in a TNS alias, or in a path other than "
            "a service name, which ConnectorX writes into the connect string, so the server "
            "cannot check which host it connects to. Write the host in the uri, and the path "
            "as /service, with at most :server_type and /instance after it, in letters, "
            "digits, '_', '.', '$', and '-'."
        )


def _database_hosts(config: DatabaseConfig) -> list[tuple[str, str]]:
    """Name each server a database URI connects to, from its host and its query alike.

    A URI that does not match `_DATABASE_URI` comes back whole, which no allowed
    host equals. A host parameter can list several servers, separated by
    commas, as a URI's authority can. ConnectorX's Trino reader decodes the
    whole URI, with the `password` field in it, before it finds the host, so
    such a URI is checked decoded. Only a URI ConnectorX reads as SQLite names
    no server, since the connector then opens the file `sqlite_path` names,
    which `check_data_paths` checks.
    """
    reader = connectorx_url(config).scheme
    if reader == "sqlite":
        return []
    uri = unquote(connection_uri(config)) if reader == "trino" else config.uri
    written = _DATABASE_URI.fullmatch(uri)
    if written is None:
        return [("uri", uri)]
    hosts = [_host(part) for part in written.group(1).split(",")]
    # The pattern allows no `#`, so the query runs from the first `?` to the end. A decoded
    # URI can hold a bracket urlsplit raises on, so it is not split there.
    hosts += [
        host
        for key, value in parse_qsl(uri.partition("?")[2], keep_blank_values=True)
        if key.lower() in _HOST_PARAMETERS
        for host in value.split(",")
    ]
    return [("uri", host) for host in hosts]


def _duckdb_hosts(config: DuckDBConfig) -> list[tuple[str, str | None]]:
    """Name each host a DuckDB `database` reaches, or None for one the server cannot check.

    A MotherDuck option a tool does not pass on, such as `motherduck_host`,
    names None. A MotherDuck database with none of those reaches MotherDuck's
    own hosts, and names none. The whole text is checked, since the extension
    finds an option after a `&` or a `#` before the `?` as well. The text can
    hold a `${NAME}` value, so only a known option's name is repeated.

    Any other `database` is a file's path, checked as any location is:
    `DuckDBConfig` refuses one that DuckDB would open through an extension,
    such as `sqlite:x.db`, a `file:` URI, whose host DuckDB opens as a
    folder, and `~name`, which DuckDB reads as HOME followed by `name`. The
    connector expands `~/` as the check does, and opens the file as a DuckDB
    database, whatever it holds.
    """
    if config.is_motherduck:
        name, _, query = config.database.partition("?")
        if _MOTHERDUCK_NAME.fullmatch(name) is None:
            setting = "database name, which may hold only letters, digits, _, -, and /"
            return [(f"{setting}, and not start with /,", None)]
        for option in query.split("&") if query else ():
            key, _, value = option.partition("=")
            if key not in _MOTHERDUCK_OPTIONS:
                return [
                    ("database option other than attach_mode, saas_mode, or session_name", None)
                ]
            if _OPTION_VALUE.fullmatch(value) is None:
                return [(f"database option '{key}'", None)]
        return []
    return [*_location_hosts("database", config.database)]


def _destinations(config: SourceRef) -> list[tuple[str, str | None]]:
    """Name each host a side connects to that its configuration chooses, with the setting.

    A host of None marks a setting a tool does not pass on, since the server
    cannot check the host it sends a reader to. A BigQuery key file is one: its
    `token_uri` names the host the sign-in goes to. Hugging Face, MotherDuck
    with no option that moves it, and BigQuery with the server's own
    credentials reach their own service's hosts, so they name none.
    """
    if isinstance(config, SourceConfig):
        return [
            *_location_hosts("path", config.path),
            *_option_hosts(config.options.get("storage_options")),
        ]
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return [
            *_location_hosts("table_uri", config.table_uri),
            *_option_hosts(config.storage_options),
        ]
    if isinstance(config, DuckDBConfig):
        return _duckdb_hosts(config)
    if isinstance(config, DatabaseConfig):
        return [*_database_hosts(config)]
    if isinstance(config, SnowflakeConfig):
        # The driver ends the host in `.cn` when the account's second label starts with `cn-`.
        labels = config.account.split(".")
        china = config.account.find(".") > 0 and labels[1].lower().startswith("cn-")
        return [("account", f"{config.account}.snowflakecomputing.{'cn' if china else 'com'}")]
    if isinstance(config, DatabricksConfig):
        return [("server_hostname", config.server_hostname.removeprefix("https://").rstrip("/"))]
    return [] if config.credentials_path is None else [("credentials_path", None)]


def _refuse_hosts(settings: Settings, source: SourceRef, target: SourceRef) -> None:
    """Refuse a side that connects to a host the server does not allow, before it connects.

    A configuration takes `${NAME}` values from the server's environment, so a
    side that could reach any host could send one there, or reach a service on
    this machine or its network. A host written in any shape but
    `_HOST_AND_PORT`'s is refused on its own, since readers split it in
    different places. The message names the setting and not the host, which
    can hold such a value.
    """
    for side, config in (("source", source), ("target", target)):
        if isinstance(config, DatabaseConfig):
            _refuse_database_uri(side, config)
        for setting, host in _destinations(config):
            _refuse_host(settings, f"The {side} {setting}", host)


def _refuse_host(settings: Settings, named: str, host: str | None) -> None:
    """Refuse a host the server does not allow, in a message that starts with `named` and leaves the host out.

    Raises:
        ConfigError: If the host is None, not in `_HOST_AND_PORT`'s shape, or
            not allowed.
    """
    if host is None:
        raise ConfigError(
            f"{named} is not one a tool passes on, since the server cannot check which host "
            "it sends a reader to. Leave it out. A credential can come from the server's "
            "environment instead, which the person who started it sets."
        )
    if host and (not host.isascii() or _HOST.fullmatch(host.lower()) is None):
        raise ConfigError(
            f"{named} does not name its host in the one shape every reader splits alike, so "
            "a tool could check one host and a reader connect to another. Write the host as "
            "DNS labels or an IP address in ASCII, with at most a port of digits after it, as "
            "in host:port or scheme://host:port/path. Leave out '@', '%', '\\', spaces, and "
            "control characters, other than in a database URI's login."
        )
    if host.lower() not in settings.allowed_hosts:
        raise ConfigError(
            f"{named} names a host this server may not connect to. A tool connects only to "
            "the hosts the person who started the server names with --allow-host, so read "
            "data under the roots instead, or ask them to add the host."
        )


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
            data a side reads on this machine is, or a side names a host the
            server does not allow.
        ConnectorError: With `schemas`, if the files of a lakehouse table
            cannot be listed.
    """
    resolved = resolve_path(settings, path)
    unset: list[str] | None = [] if allow_missing_env else None
    # The file is loaded once, and the checks connect with what was checked, so a
    # file swapped between the two cannot reach a host the server does not allow.
    try:
        diff, source, target = load_config(resolved, unset_env=unset)
    except ConfigError as exc:
        findings = [error_from_exception(exc)]
    else:
        if schemas:
            _refuse_hosts(settings, source, target)
            source, target = check_data_paths(settings, source, target)
        with _reading(settings):
            findings = DiffEngine.check_configs(
                diff, source, target, schemas=schemas, quote_data=settings.allow_row_values
            )
    return validation_report(str(resolved), [*unset_variable_findings(unset or []), *findings])


def _load(settings: Settings, path: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration a tool opens, refusing data outside the roots or on a host not allowed."""
    diff, source, target = load_config(resolve_path(settings, path))
    _refuse_hosts(settings, source, target)
    source, target = check_data_paths(settings, source, target)
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
    refused before any row is read, as is a side on a host the server does not
    allow, and a side's `query` unless the server allows queries.

    Args:
        settings (Settings): The roots the server was started with.
        path (str): The configuration file, under a root.

    Returns:
        RunReport: The summary, with the verdict and the exit code.

    Raises:
        ConfigError: If the file, its data, or its `output_path` is outside
            the roots, a side names a host the server does not allow, a side
            reads through a `query` the server does not allow, or the
            configuration cannot run as written.
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
        ConfigError: If the file or its data is outside the roots, a side
            names a host the server does not allow, the file does not load, or
            it reads the side through a `query`, which would have to run in
            full.
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
            a side names a host or reads through a `query` the server does not
            allow, or the configuration cannot run.
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
            data is outside the roots, a side names a host or reads through a
            `query` the server does not allow, a threshold is out of range, or the
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
            data is outside the roots, a side names a host or reads through a
            `query` the server does not allow, `max_share` is out of range, or the pair is
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
    another folder has its relative paths checked against that folder, and
    each location with a scheme too, since some reader opens one as a path.

    Args:
        settings (Settings): The roots every tool is held to.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
    """
    build_server(settings).run(transport="stdio")
