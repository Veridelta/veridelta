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

import base64
import functools
import glob
import importlib
import inspect
import json
import logging
import os
import re
import stat
import threading
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
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
from urllib.parse import SplitResult, parse_qsl, quote, unquote, urlsplit
from urllib.request import url2pathname

import polars as pl
from polars.exceptions import PanicException
from pydantic import Field

from veridelta import __version__
from veridelta._checks import error_from_exception, unset_variable_findings
from veridelta.config import Reference, load_config_with_hook
from veridelta.connectors.base import (
    mask_secrets,
    masking_first,
    names_another_machine,
    shown_location,
    without_location,
    without_queries,
)
from veridelta.connectors.database import connection_uri, connectorx_url, sqlite_path
from veridelta.connectors.duckdb import (
    MOTHERDUCK_TOKEN_VARIABLES,
    WRITE_AHEAD_LOGS,
    beside,
    sandboxed,
)
from veridelta.connectors.lakehouse import (
    BASE64,
    CheckedDeltaTable,
    DeltaLogCopy,
    azure_account,
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
    ConfigLocation,
    ConnectorError,
    VerideltaError,
    may_quote_data,
    missing_extra,
)
from veridelta.models import (
    BigQueryConfig,
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

_DELTA_LOG_NAME: Final = re.compile(
    r"_last_checkpoint|[0-9]{20}\.(?:json|crc|checkpoint\.parquet"
    r"|checkpoint\.[0-9]{10}\.[0-9]{10}\.parquet"
    r"|checkpoint\.[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}\.(?:json|parquet)"
    r"|[0-9]{20}\.compacted\.json)"
)
"""The names of the files at the top of a Delta Lake log that deltalake reads: `_last_checkpoint`,
and a version's commit, checksum, checkpoint, checkpoint part, or checkpoint named by a UUID, or
the compacted commits of a range of versions. deltalake skips any other name, and so does the
copy of the log."""

_DELTA_LOG_COPY_LIMIT: Final = 1 << 30
"""The most room the copy of one table's log and vectors takes, 1 GiB, so the files under a root
cannot fill the system's temporary folder. Each file copied counts its bytes, as they are read,
and one block of the copy's folder, its `st_blksize` or 4 KiB, whichever is larger, since even an
empty file takes room there. Each folder made for a file counts one block too."""

_DELTA_LOG_COPY_FILES: Final = 10_000
"""The most files and folders the copy of one table's log and vectors holds, so links named as
commits to one file under a root cannot fill the system's temporary folder with files."""

_URL_SPACE: Final = "".join(chr(code) for code in range(0x21))
"""The C0 controls and the space, which a URL parser strips from both ends of a URL."""

_URI_SCHEME: Final = re.compile(r"[A-Za-z][A-Za-z0-9+.-]+:")
"""The start of a URI: a scheme of two or more characters, since one letter is a Windows drive."""

_SPECIAL_SCHEMES: Final = frozenset({"file", "ftp", "http", "https", "ws", "wss"})
"""The schemes a URL parser reads without their slashes, as in `http:host`."""

_ENDPOINT_OPTIONS: Final = frozenset(
    {
        "aws_endpoint",
        "aws_endpoint_url",
        "azure_endpoint",
        "azure_storage_endpoint",
        "endpoint",
        "endpoint_url",
        "s3.endpoint",
    }
)
"""Storage option keys, in lowercase, whose value names an endpoint, with or without a scheme."""

_BUCKET_OPTIONS: Final = frozenset(
    {
        "aws_bucket",
        "aws_bucket_name",
        "azure_container_name",
        "bucket",
        "bucket_name",
        "container_name",
        "google_bucket",
        "google_bucket_name",
    }
)
"""Storage option keys, in lowercase, whose value names a bucket or a container,
which a tool passes on only in lowercase, as `_refuse_capital_bucket` says."""

_NAME_OPTIONS: Final = _BUCKET_OPTIONS | frozenset(
    {"account_name", "adls.account-name", "azure_storage_account_name"}
)
"""Storage option keys, in lowercase, whose value names a bucket, a container, or an
account, which a reader writes into the request's host or path."""

_HOST_OPTIONS: Final = _ENDPOINT_OPTIONS | _NAME_OPTIONS
"""Storage option keys, in lowercase, whose value names the host a reader connects
to: an endpoint, with or without a scheme, a bucket, or an account name."""

_LOCATION_FORM: Final = (
    "with at most a port of digits after it, as in host:port or scheme://host:port/path"
)
"""How `_refuse_host` says to write a location's host, which may have a path after it."""

_ENDPOINT_FORM: Final = (
    "with at most a port of digits after it, as in host:port or https://host:port"
)
"""How `_refuse_host` says to write an endpoint, which `_refuse_endpoint_path` holds to no path."""

_NAME_FORM: Final = "with at most a port of digits after it, as in lake"
"""How `_refuse_host` says to write a bucket, a container, or an account, as a name alone."""

_ACCOUNT_FORM: Final = "with no port or path after it, as in xy12345.us-east-1"
"""How `_refuse_host` says to write a Snowflake account, which the driver ends in its own domain."""

_SERVER_FORM: Final = "with no port or path after it, as in adb-1.azuredatabricks.net"
"""How `_refuse_host` says to write a Databricks server's host, which takes no port or path."""

_REGION_OPTIONS: Final = frozenset(
    {"aws_default_region", "aws_region", "client.region", "default_region", "region", "s3.region"}
)
"""Storage option keys, in lowercase, whose value a reader writes into an AWS host
name, so a tool passes on only a value of letters, digits, and hyphens."""

_REGION: Final = re.compile(r"[A-Za-z0-9-]+")
"""A region a tool passes on, such as `us-east-1`."""

_SECRET_OPTIONS: Final = frozenset(
    {
        "aws_secret_access_key",
        "secret_access_key",
        "s3.secret-access-key",
        "client.secret-access-key",
        "aws_session_token",
        "aws_token",
        "session_token",
        "s3.session-token",
        "client.session-token",
        "azure_client_secret",
        "client_secret",
        "azure_storage_client_secret",
        "adls.client-secret",
        "bearer_token",
        "token",
        "azure_storage_token",
        "hf.token",
        "gcs.oauth2.token",
    }
)
"""Storage option keys, in lowercase, that hold a secret key or a token.

A reader signs a request with such a value, or sends it in a header, and
never writes it into a path, a host, or a message. A service can still
echo a value it rejects: S3 writes a session token it cannot read back
into its error XML-escaped, trimmed, and with each letter outside ASCII
garbled. So a value must be in `_TOKEN`'s shape, whose echo is the text
the mask matches."""

_ACCOUNT_KEY_OPTIONS: Final = frozenset(
    {
        "account_key",
        "access_key",
        "azure_storage_account_key",
        "azure_storage_access_key",
        "adls.account-key",
    }
)
"""Storage option keys, in lowercase, that hold an Azure account key.

In object_store, `access_key` is the Azure account key. A reader signs
with the key, and never writes it into a request or a message, so no
reader echoes it. Its shape is base64 as an encoder writes it, which a
reader has no cause to encode. object_store's error for a key it cannot
decode names a character of it, so `_is_account_key` refuses every other
key before a reader sees it."""

_LOGIN_OPTIONS: Final = frozenset(
    {
        "aws_access_key_id",
        "access_key_id",
        "s3.access-key-id",
        "client.access-key-id",
        "azure_client_id",
        "client_id",
        "azure_storage_client_id",
        "adls.client-id",
        "azure_tenant_id",
        "tenant_id",
        "azure_storage_tenant_id",
        "adls.tenant-id",
    }
)
"""Storage option keys, in lowercase, that hold an access key id, a client id, or a tenant id.

A service can echo such an id, as AWS does in a signature error. A value
in `_LOGIN_NAME`'s shape reads the same raw, encoded, and decoded, so an
echo is the text the mask matches, in any case."""

_SAS_OPTIONS: Final = frozenset(
    {"adls.sas-token", "azure_storage_sas_token", "azure_storage_sas_key", "sas_token", "sas_key"}
)
"""Storage option keys, in lowercase, that hold an Azure SAS token.

The Azure SDK quotes a signed request's URL, with the token's parameters
sorted. A token in the shape `_is_sas_token` checks holds only parameters
the mask finds one by one: the server masks the whole token, and the
`sig` parameter as written, decoded, and encoded, and leaves out each
URL's query."""

_SWITCH_OPTIONS: Final = frozenset({"aws_skip_signature", "skip_signature", "s3.anonymous"})
"""Storage option keys, in lowercase, that turn signing off."""

_NO_HOST_OPTIONS: Final = (
    _SECRET_OPTIONS | _ACCOUNT_KEY_OPTIONS | _LOGIN_OPTIONS | _SAS_OPTIONS | _SWITCH_OPTIONS
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

_CAPITAL: Final = re.compile(r"[A-Z]")
"""A capital letter in ASCII, which a bucket or a container may not hold on the server."""

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
            still reads other files only from under the roots, and on Windows
            none at all. Defaults to False.
        allowed_hosts (tuple[str, ...]): The hosts a side may connect to, each
            in lowercase, as `allowed_host` checks on creation. A side that names any other host, such as a
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
                allowed host is not a host in lowercase.
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
    """Check a host the person who starts the server allows, which is in lowercase.

    `_refuse_host` compares a side's host in lowercase, as DNS does. A reader
    keeps a bucket's case, though, as `_refuse_capital_bucket` says, and the
    server cannot tell a bucket from a DNS name here. So it takes a host only
    in lowercase: writing `LAKE` in lowercase would allow bucket `lake`,
    another bucket, which nobody named.

    Args:
        text (str): A host name, an IPv4 address, or an IPv6 address in
            brackets, with no scheme, login, or port, in lowercase.

    Returns:
        str: The host, as given.

    Raises:
        ConfigError: If the text is not a host written that way.
    """
    if not text.isascii() or _HOST.fullmatch(text.lower()) is None:
        raise ConfigError(
            f"--allow-host takes a host name, an IPv4 address, or an IPv6 address in "
            f"brackets, with no scheme or port, got {text!r}."
        )
    if _CAPITAL.search(text):
        raise ConfigError(
            f"--allow-host takes a host in lowercase, got {text!r}. A bucket named in "
            "another case is another bucket, so the server does not lowercase it. Write a "
            "DNS name in lowercase. A bucket with a capital letter does not open on this "
            "server, so read it with veridelta run."
        )
    return text


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
    either way. A file on another machine is held to the allowed hosts, and
    may hold no `.` or `..` segment, nor a capital letter in its bucket or
    container, as a configuration's location is. The message leaves the file
    out, since the table's files named it.

    Raises:
        ConfigError: If the path lies outside every root, or the file on a host
            the server does not allow, with a `.` or `..` segment, or with a
            capital letter in its bucket or container.
    """
    if remote:
        named = "A file that a lakehouse scan was about to open"
        _refuse_dot_segment(named, location, _NAMED_FILE)
        for _, host in _location_hosts("file", location, _NAMED_FILE):
            _refuse_host(settings, named, host)
        _refuse_capital_bucket(named, location, _NAMED_FILE)
    elif not _under_both_readings(settings, location):
        raise ConfigError(
            "A Delta Lake or Iceberg scan was about to open a file outside the folders this "
            f"server reads from: {_roots(settings)}, so it was stopped. A tool reads data on "
            "this machine only from under them."
        )


@contextmanager
def _reading(settings: Settings) -> Generator[None]:
    """Hold what a tool's sides read to the roots, while they read.

    A DuckDB file opens only by a path with no link in it, its connection reads
    other files only from under the roots, and on Windows none at all, and it
    spills only into a folder the block creates. A Delta Lake or Iceberg scan
    opens a file on this machine only from under them, and one elsewhere only
    on an allowed host, each checked just before it is opened. A Delta Lake
    scan lists and checks its table's files again as it starts, and reads the
    version it checked.
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


def _named_locations(config: SourceRef) -> list[tuple[str, str]]:
    """Name each data location a side's configuration writes as a path or a URI, with its setting."""
    if isinstance(config, SourceConfig):
        return [("path", config.path)]
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return [("table_uri", config.table_uri)]
    if isinstance(config, DuckDBConfig) and not config.is_motherduck:
        return [("database", config.database)]
    return []


def _data_locations(side: str, config: SourceRef) -> list[tuple[str, str, str]]:
    """Name each path on this machine a side could read: its setting, location, and path."""
    if isinstance(config, DatabaseConfig):
        # The connector opens this resolved text, so a link or `..` leads nowhere else.
        path = sqlite_path(config)
        return [] if path is None else [("SQLite file", path, path)]
    return [
        (setting, location, path)
        for setting, location in _named_locations(config)
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
    link, so the copy writes nothing outside its folder. The copy holds at
    most `_DELTA_LOG_COPY_FILES` files and folders, and takes at most
    `_DELTA_LOG_COPY_LIMIT`: each file, and each folder made for it, counts
    one block of the copy's folder before it is made, and the file's bytes
    count as they are read.

    Raises:
        ValueError: If the file opened is not the one at the path before the
            check, or is not a regular file, or `target` leads out of the
            copy's folder, or the copy would grow past its limits.
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
        # Even an empty file takes a block, as does each folder made for it, so each counts
        # before it is made: links to one file under a root take no room there.
        made = 1 + sum(not folder.exists() for folder in target.parents)
        copy.files += made
        _charge_copy(copy, made * copy.block)
        target.parent.mkdir(parents=True, exist_ok=True)
        _refuse_outside_copy(target, copy)
        # O_EXCL creates the file or fails, and fails at a link too. Only POSIX has O_NOFOLLOW.
        new = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        written = os.open(target, new | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with open(written, "wb") as copied:
            # The size is counted as the file is read, since a file can grow after its stat.
            while chunk := file.read(1 << 20):
                _charge_copy(copy, len(chunk))
                copied.write(chunk)


def _charge_copy(copy: DeltaLogCopy, room: int) -> None:
    """Count room a table's log copy takes, and refuse a copy past its limits.

    Raises:
        ValueError: If the copy holds more than `_DELTA_LOG_COPY_FILES` files
            and folders, or takes more than `_DELTA_LOG_COPY_LIMIT`.
    """
    copy.size += room
    if copy.files > _DELTA_LOG_COPY_FILES or copy.size > _DELTA_LOG_COPY_LIMIT:
        raise ValueError(
            "the copy of the files of the table's log and deletion vectors that deltalake "
            "reads would pass the 10,000 files and folders, or the 1 GiB, this server allows "
            "it, so it reads none of the table. Read such a table with veridelta run"
        )


def _delta_log(
    side: str, uri: str, copy: DeltaLogCopy | None, version: int | None
) -> Generator[tuple[str, str, bool], None, tuple[list[str], dict[Path, Path]]]:
    r"""Name each entry of the log of a table on this machine, each before it is read.

    With a `copy`, for a table deltalake reads from this machine, each file at
    the top of the log whose name deltalake reads, for `version` or one
    before it, is copied into it just after its check, in a folder of its own
    for each folder the table's location names: `_last_checkpoint`, and each
    commit, checksum, checkpoint, and compacted file from version 0 to
    `version`, or to the latest. deltalake skips any other name, and a file
    of a later version when it loads `version`, and reads the copy, so the
    other entries are checked but never read. The copy starts at version 0,
    not at a checkpoint, since deltalake passes over a checkpoint it cannot
    use to the commits before it, and a name alone does not say whether it
    can. A table deltalake reads from its store has its entries here
    checked, but none read.

    Returns:
        tuple[list[str], dict[Path, Path]]: The copy of each file of the log,
            and the folder of the copy of each of the table's folders that
            holds a log.

    Raises:
        ValueError: If a name in the log holds a `\` outside Windows, or
            `_copy_checked` refuses a file of the log.
    """
    logged: list[str] = []
    logs: dict[Path, Path] = {}
    for index, local in enumerate(_local_paths(uri, f"{side} table_uri")):
        # Absolute, so a reading such as `s3:/b/t`, from `s3://b/t`, names no host.
        folder = Path(os.path.expanduser(local)).absolute()
        log = folder / "_delta_log"
        yield "log", str(log), True
        for parent, folders, files in os.walk(log):
            top = Path(parent) == log
            for name in sorted(folders + files):
                path = os.path.join(parent, name)
                if copy is not None and os.sep != "\\" and "\\" in name:
                    # deltalake reads a `\` in a name it lists as `/`, and resolves a `..`, so
                    # such a name leads it to a file elsewhere. The copy holds no such name, but
                    # `veridelta run` would read that file, so neither reading is the table's.
                    raise ValueError(
                        "a name in the table's log holds a '\\', which deltalake reads as a "
                        "'/' that could lead out of the log, so this server reads none of the "
                        "table"
                    )
                read = top and _DELTA_LOG_NAME.fullmatch(name) is not None
                # Every name but `_last_checkpoint` starts with the version it is a file of.
                hint = name == "_last_checkpoint"
                read = read and (hint or version is None or int(name[:20]) <= version)
                seen = _file_at(path) if copy is not None and read and name in files else None
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
    r"""Name each file a scan of a Delta Lake table reads, from the version it reads.

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
    from this machine, each file of the log that deltalake reads for the
    version, and each vector at a path relative to the table, is copied just
    after its check into a private folder outside every root, up to 10,000
    files and folders and 1 GiB in all, and the log read and the table loaded
    are that copy. A file opened for the copy that is not the one checked is
    refused. Each file of the copy lies at a name this server builds in the
    copy's folder: the walk's own names for the log, and a vector's folder of
    letters and digits and its UUID.
    deltalake would read a vector that the log stores at its own path where
    the log says, which a copy could change only by writing there or by
    rewriting the log, so such a table is refused.

    A path that starts with `~` is expanded as Polars expands it, and the
    walk reads the expanded path, since deltalake expands only `~` and `~/`
    and would read `~alice/t` as a folder of that name.

    Each file comes with its setting, its location, and whether a refusal may
    quote it on any server: the log and its entries may, and a file the log
    names may not, since a data file's folder can hold a partition's value.

    Returns:
        CheckedDeltaTable: The table, at the version whose files were named.

    Raises:
        ValueError: If the copy would lie under a root or in a folder whose
            path deltalake reads as another folder's, more than one folder
            the table's location names holds a log, or the log of a table on
            this machine holds a name with a `\` outside Windows, outgrows the
            copy, or stores a vector at its own path.
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
    logged, logs = yield from _delta_log(side, uri, copy, config.version)
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
            yield "data file", name, False
        for location in deletion_vector_files(checked.table, vectors):
            yield "deletion vector", location, False
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
        yield "data file", checked.located(name), False
    copied: set[str] = set()
    for location in deletion_vector_files(checked.table, vectors):
        # A vector at a relative path, in a folder of letters and digits under the table's,
        # named by the UUID the log encodes. One file can hold many, and a log name one twice.
        local = checked.located(location)
        seen = None if local in copied else _file_at(local)
        yield "deletion vector", local, False
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
    it, so the check and the scan read the same metadata file. The data and
    delete files come from the manifests, as a scan plans them. Each file
    comes with its setting, its location, and whether a refusal may quote it
    on any server: the version hint may, and a file named by a file's text may
    not, since that text is a value from the table.

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
        raise ConnectorError(f"the metadata file that '{hint}' names could not be read") from None
    snapshot = (
        metadata.current_snapshot()
        if config.snapshot_id is None
        else metadata.snapshot_by_id(config.snapshot_id)
    )
    if snapshot is None:
        # The scan reads no file: an empty table has none, and a snapshot it
        # cannot find stops it.
        return
    yield "manifest list", snapshot.manifest_list, False
    for manifest in snapshot.manifests(io):
        yield "manifest", manifest.manifest_path, False
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            yield "data file", entry.data_file.file_path, False


def _table_files(
    settings: Settings, side: str, config: DeltaLakeConfig | IcebergConfig
) -> Generator[tuple[str, str, bool], None, CheckedDeltaTable | None]:
    """Name each file that the scan of a lakehouse table reads.

    A table's log or metadata names its files by full URI, so they can lie
    anywhere. A table whose files cannot be listed is refused, since the scan
    would read the same log or metadata. An Iceberg table's files are read
    with the scan's own check, as well as the caller's. The error of a table
    that cannot be listed quotes the reader's message, which can hold a value
    from the table's files, so it sets `quotes_data` unless Veridelta wrote
    that message.

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
        # deltalake, PyIceberg, and PyArrow can quote a value from the table's files, where
        # a refusal in Veridelta's words quotes none.
        raise ConnectorError(
            f"The files of the {side} {kind} '{where}' could not be listed, so this server "
            f"reads none of them: {without_location(str(exc), config.table_uri)}",
            quotes_data=may_quote_data(exc),
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
    is refused too. Inside `sandboxed`, the connector then opens the file by
    this path only through no link, and confirms DuckDB opened that file.
    """
    if not isinstance(config, DuckDBConfig) or config.is_motherduck:
        return config
    database = _resolved_database(side, config.database)
    _refuse_outside(settings, side, "database", database, database)
    for name, suffix in WRITE_AHEAD_LOGS:
        log = beside(database, suffix)
        _refuse_outside(settings, side, f"database's {name}", log, log)
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
    location out, since a file named it. The message about a path quotes it
    when `quoted`, or on a server that allows row values, since a path a
    table's files name is a value from them, such as a partition's. A file
    on another machine may hold no `.` or `..` segment, as `_refuse_dot_segment` says,
    and its bucket or container no capital letter, as `_refuse_capital_bucket` says.
    """
    quoted = quoted or settings.allow_row_values
    for local in _local_paths(location, f"{side} {setting}"):
        _refuse_outside(settings, side, setting, location, local, quoted=quoted)
    _refuse_dot_segment(f"The {side} {setting}", location, _NAMED_FILE)
    for _, host in _location_hosts(setting, location, _NAMED_FILE):
        _refuse_host(settings, f"The {side} {setting}", host)
    _refuse_capital_bucket(f"The {side} {setting}", location, _NAMED_FILE)


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

    A file a table names in Azure as `container@host`, as Java Iceberg and
    Spark write it, names two hosts: its container, as `abfs://container/x`
    does, and the account's host, such as `acct.dfs.core.windows.net`.
    PyIceberg's checked FileIO opens such a file in that container, and only
    at the account its storage options name.
    """
    parts = _split(location)
    if parts is None:
        return [(setting, _url_host(location, shape))]
    if len(parts.scheme) < 2 or parts.scheme in _LOCAL_SCHEMES | _SERVICE_SCHEMES:
        return []
    named = shape.fullmatch(location) if shape is _NAMED_FILE else None
    account = None if named is None else azure_account(parts.scheme, named.group(1) or "")
    if account is not None:
        # The FileIO opens the container as written, port and all, so it is checked whole.
        container, host = account
        return [(setting, container), (setting, _host(host))]
    return [(setting, _url_host(location, shape))]


def _option_hosts(options: object) -> Iterator[tuple[str, str | None, str]]:
    """Name each host a side's storage options send a reader to, with the option and its form.

    A host of None marks an option a tool does not pass on: one outside the
    known keys, a key outside ASCII, or a region that a reader could not write
    into a host name. An endpoint is a host with an optional port, or a URL.
    The form is how a refusal says to write the host: `_ENDPOINT_FORM` for an
    endpoint, and `_NAME_FORM` for a bucket, a container, or an account.
    """
    if not isinstance(options, Mapping):
        return
    for key, value in cast("Mapping[object, object]", options).items():
        name, text = str(key), str(value)
        setting = f"storage option '{key}'"
        if not name.isascii():
            yield setting, None, _NAME_FORM
        elif name.lower() in _HOST_OPTIONS:
            form = _ENDPOINT_FORM if name.lower() in _ENDPOINT_OPTIONS else _NAME_FORM
            yield setting, _url_host(text) if "://" in text else _host(text), form
        elif name.lower() in _REGION_OPTIONS:
            if _REGION.fullmatch(text) is None:
                yield setting, None, _NAME_FORM
        elif name.lower() not in _NO_HOST_OPTIONS:
            yield setting, None, _NAME_FORM


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


def _storage_options(config: SourceRef) -> object:
    """Return the `storage_options` a side hands its reader, or None for a side that takes none."""
    if isinstance(config, SourceConfig):
        return config.options.get("storage_options")
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return config.storage_options
    return None


def _host_option_values(config: SourceRef) -> Iterator[tuple[object, str]]:
    """Yield each storage option of a side whose key is in `_HOST_OPTIONS`, with its value as text."""
    options = _storage_options(config)
    if not isinstance(options, Mapping):
        return
    for key, value in cast("Mapping[object, object]", options).items():
        name = str(key)
        if name.isascii() and name.lower() in _HOST_OPTIONS:
            yield key, str(value)


def _scheme_length(text: str) -> int:
    """Return how much of a host option's value its scheme takes, with its `:` and any `//`, or 0.

    A value has a scheme when it holds `://`, or when it holds a `:` and the
    text before the first one is one of `_SPECIAL_SCHEMES`, in ASCII and any
    case, since the URL parser behind Polars reads such a value as a URL
    without its slashes, and a number after it as an IPv4 address. A value
    with no `:`, such as a bucket named `https`, has none.
    """
    if "://" in text:
        return text.index("://") + len("://")
    scheme, colon, _ = text.partition(":")
    special = colon and scheme.isascii() and scheme.lower() in _SPECIAL_SCHEMES
    return len(scheme) + len(colon) if special else 0


def _refuse_endpoint_path(side: str, config: SourceRef) -> None:
    r"""Refuse a host option whose value holds a path, a query, or a fragment after its host.

    Every reader joins the endpoint's path with the bucket or the container:
    object_store, behind Polars and deltalake, requests
    `{endpoint}/{bucket}/{key}` in S3 through a path-style endpoint, and
    `{endpoint}/{container}/{key}` in Azure, and the AWS SDK in PyArrow,
    behind PyIceberg, does so with or without a scheme. So
    `https://minio.internal/other` reads bucket `other`, or a bucket under
    it, on the same host, with the same credential, though only the one
    written was allowed. A query or a fragment moves the bucket out of the
    request's path. So the value, after its scheme as `_scheme_length`
    finds it, passes only as `_HOST_AND_PORT`'s shape with nothing after it
    but a single `/`. Its path starts at the first `/`, `\`, `?`, or `#`
    after the host and port, where a URL parser ends the authority, so
    every spelling of a path, `%2F` and `..` included, is refused whole.
    Other text after the host, such as a login, is left to `_refuse_host`.
    A bucket, a container, or an account option is held to the same shape,
    since a reader writes its value into the request's host or path, and
    its message speaks of a name rather than an endpoint. The message names
    the option and not its value, which can hold a `${NAME}` value.

    Raises:
        ConfigError: If a host option's value holds a path, a query, or a fragment.
    """
    for key, text in _host_option_values(config):
        rest = text[_scheme_length(text) :]
        written = _HOST_AND_PORT.match(rest)
        after = "" if written is None else rest[written.end() :]
        if after not in ("", "/") and after[0] in "/\\?#":
            if str(key).lower() in _NAME_OPTIONS:
                raise ConfigError(
                    f"The {side} storage option '{key}' holds a path, a query, or a fragment "
                    "after its name. A reader writes the value into the request's host or path, "
                    "so it could read a bucket, a container, or an account this server did not "
                    "check. A tool passes on such an option only as a name, as in lake."
                )
            raise ConfigError(
                f"The {side} storage option '{key}' holds a path, a query, or a fragment after "
                "its host, which a reader puts before the bucket or the container, so it could "
                "read one this server did not check. A tool passes on an endpoint only as a "
                "host and a port, with at most a '/' after them, as in https://host:port."
            )


def _refuse_plain_http(side: str, config: SourceRef) -> None:
    """Refuse a storage option that sends a reader to its endpoint over anything but HTTPS.

    Polars and PyIceberg take the scheme from the endpoint, so an `http` one
    sends the credentials and the rows unencrypted, as `aws_allow_http` would
    for deltalake, which a tool refuses as a key it does not know. So a host
    option's value with a scheme, as `_scheme_length` finds one, passes only
    when it starts with `https://`, in lowercase, the one form every reader
    takes as HTTPS: the AWS SDK in PyArrow takes any other as a host name. No
    host is excepted, this machine's included, as none is for
    `aws_allow_http`. Other text, such as `minio.internal:9000`, or a bucket
    named `https` with no `:`, passes:
    PyArrow then uses HTTPS, and object_store fails before it connects. The
    message names the option and not its value, which can hold a `${NAME}`
    value.

    Raises:
        ConfigError: If a host option's value has a scheme and does not start with `https://`.
    """
    for key, text in _host_option_values(config):
        if not text.startswith("https://") and _scheme_length(text):
            raise ConfigError(
                f"The {side} storage option '{key}' names a host by a scheme other than "
                "https, so a reader could send its credentials and rows unencrypted. A tool "
                "reaches an endpoint only over HTTPS, on this machine too, so write it as "
                "https://host:port, in lowercase."
            )


def _destinations(config: SourceRef) -> list[tuple[str, str | None, str]]:
    """Name each host a side connects to that its configuration chooses, with the setting and its form.

    A host of None marks a setting a tool does not pass on, since the server
    cannot check the host it sends a reader to. A BigQuery key file is one: its
    `token_uri` names the host the sign-in goes to. Hugging Face, MotherDuck
    with no option that moves it, and BigQuery with the server's own
    credentials reach their own service's hosts, so they name none. The form
    is how `_refuse_host` says to write the host: a location may have a path,
    and an endpoint, a bucket, an account, or a server may not.
    """
    if isinstance(config, SnowflakeConfig):
        # The driver ends the host in `.cn` when the account's second label starts with `cn-`.
        labels = config.account.split(".")
        china = config.account.find(".") > 0 and labels[1].lower().startswith("cn-")
        host = f"{config.account}.snowflakecomputing.{'cn' if china else 'com'}"
        return [("account", host, _ACCOUNT_FORM)]
    if isinstance(config, DatabricksConfig):
        host = config.server_hostname.removeprefix("https://").rstrip("/")
        return [("server_hostname", host, _SERVER_FORM)]
    return [
        *((setting, host, _LOCATION_FORM) for setting, host in _location_destinations(config)),
        *_option_hosts(_storage_options(config)),
    ]


def _location_destinations(config: SourceRef) -> Sequence[tuple[str, str | None]]:
    """Name each host a side's location, database, or key file reaches, with its setting."""
    if isinstance(config, SourceConfig):
        return _location_hosts("path", config.path)
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return _location_hosts("table_uri", config.table_uri)
    if isinstance(config, DuckDBConfig):
        return _duckdb_hosts(config)
    if isinstance(config, DatabaseConfig):
        return _database_hosts(config)
    if isinstance(config, BigQueryConfig) and config.credentials_path is not None:
        return [("credentials_path", None)]
    return []


def _refuse_hosts(settings: Settings, source: SourceRef, target: SourceRef) -> None:
    """Refuse a side that connects to a host the server does not allow, before it connects.

    A configuration takes `${NAME}` values from the server's environment, so a
    side that could reach any host could send one there, or reach a service on
    this machine or its network. A host written in any shape but
    `_HOST_AND_PORT`'s is refused on its own, since readers split it in
    different places. An endpoint is refused with a path, by
    `_refuse_endpoint_path`, and over plain HTTP, by `_refuse_plain_http`.
    A location on another machine may hold no `.` or `..` segment, as
    `_refuse_dot_segment` says, and a bucket or a container no capital
    letter, as `_refuse_capital_bucket` says. The message names the setting
    and not the host, which can hold such a value.
    """
    for side, config in (("source", source), ("target", target)):
        if isinstance(config, DatabaseConfig):
            _refuse_database_uri(side, config)
        for setting, location in _named_locations(config):
            _refuse_dot_segment(f"The {side} {setting}", location)
        _refuse_endpoint_path(side, config)
        for setting, host, form in _destinations(config):
            _refuse_host(settings, f"The {side} {setting}", host, form)
        _refuse_plain_http(side, config)
        for setting, location in _named_locations(config):
            _refuse_capital_bucket(f"The {side} {setting}", location)
        _refuse_capital_option(side, config)


def _refuse_dot_segment(named: str, location: str, shape: re.Pattern[str] = _URL) -> None:
    r"""Refuse a location on another machine whose path holds a `.` or `..` segment, in any spelling.

    PyArrow and the other readers send such a path to the store as written.
    The store, or an HTTP layer before it, can then remove the segment and
    the one before it. Where the bucket or the container is the first
    segment of the request's path, as in Azure and in S3 through a
    path-style endpoint, the read reaches another bucket or container on
    the same host, with the same credential, though only the one written
    was allowed. `_holds_dot_segment` finds such a segment in every spelling
    a parser or a store could remove: `%2e`, decoded as often as an escape
    remains, `\` as a separator, and a tab or line break inside it. An empty
    segment removes no other, so it passes. A location `_location_hosts`
    names no host for, on this machine or a service's own hosts, is left to
    the other checks. The message names the setting or the file in `named`,
    and not the location, which can hold a `${NAME}` value or a value from
    a table's files.

    Raises:
        ConfigError: If the location names a host in `shape` and holds such a segment.
    """
    if _holds_dot_segment(location) and _location_hosts("", location, shape):
        raise ConfigError(
            f"{named} is a URI on another machine with a '.' or '..' segment in its path, "
            "in some spelling. The store can remove the segment with the bucket or container "
            "before it, and so read one this server did not check, so a tool does not pass "
            "it on. Write the location without the segment."
        )


def _refuse_capital_bucket(named: str, location: str, shape: re.Pattern[str] = _URL) -> None:
    """Refuse a location on another machine whose bucket or container holds a capital letter.

    `_refuse_host` compares a host with the allowed ones in lowercase, as DNS
    does. A URL parser lowercases the host of `http`, `https`, and the other
    special schemes, but keeps the case of any other scheme's authority. So
    do object_store, behind Polars and deltalake, and PyIceberg's
    `parse_location`, and each writes it into the request as the bucket or
    the container: `s3://LAKE/x` reads bucket `LAKE`, and not `lake`. A legacy
    S3 bucket in `us-east-1` may hold capital letters, so that is another
    bucket. The server cannot tell which scheme's reader takes its authority
    as a DNS name, as `hdfs` does, so it refuses a capital letter after any
    of them. In an Azure `container@host`, only the container counts, since
    the host is the account's DNS name. A caller runs this after
    `_refuse_host`, which checks the host's shape first. The message names
    the setting or the file in `named`, and not the location, which can hold
    a value from a table's files.

    Raises:
        ConfigError: If the first host `_location_hosts` names for a scheme
            other than a special one holds a capital letter in ASCII.
    """
    parts = _split(location)
    hosts = _location_hosts("", location, shape)
    special = parts is not None and parts.scheme in _SPECIAL_SCHEMES
    if hosts and not special and _CAPITAL.search(hosts[0][1]):
        raise ConfigError(
            f"{named} names its bucket, container, or host with a capital letter. A reader "
            "keeps the case of that name for any scheme whose host a URL parser does not "
            "lowercase, such as s3, and a bucket named in another case is another bucket, "
            "which this server did not check. Write the name in lowercase."
        )


def _refuse_capital_option(side: str, config: SourceRef) -> None:
    """Refuse a storage option that names a bucket or a container with a capital letter.

    A reader keeps the case of a bucket or a container, as
    `_refuse_capital_bucket` says, while `_refuse_host` compares it in
    lowercase. The message names the option and not its value, which can
    hold a `${NAME}` value.

    Raises:
        ConfigError: If an option in `_BUCKET_OPTIONS` holds a capital letter in ASCII.
    """
    for key, text in _host_option_values(config):
        if str(key).lower() in _BUCKET_OPTIONS and _CAPITAL.search(text):
            raise ConfigError(
                f"The {side} storage option '{key}' names a bucket or a container with a "
                "capital letter. A reader keeps its case, and a bucket named in another case "
                "is another bucket, which this server did not check. Write the name in "
                "lowercase."
            )


def _refuse_host(
    settings: Settings, named: str, host: str | None, form: str = _LOCATION_FORM
) -> None:
    """Refuse a host the server does not allow, in a message that starts with `named` and leaves the host out.

    A host in another shape is refused with `form`, which says how to write
    it for its kind of setting: a location may have a path after its host,
    and an endpoint may not.

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
            f"DNS labels or an IP address in ASCII, {form}. Leave out '@', '%', '\\', spaces, "
            "and control characters, other than in a database URI's login."
        )
    if host.lower() not in settings.allowed_hosts:
        raise ConfigError(
            f"{named} names a host this server may not connect to. A tool connects only to "
            "the hosts the person who started the server names with --allow-host, so read "
            "data under the roots instead, or ask them to add the host."
        )


_Slot = Literal["secret", "token", "account key", "login", "sas", "uri user", "uri password"]
"""What a setting that may take a `${NAME}` holds, which sets the shape its value needs."""

_FIELD_SLOTS: Final[Mapping[tuple[str, str | int], _Slot]] = {
    ("snowflake", "password"): "secret",
    ("snowflake", "private_key_passphrase"): "secret",
    ("databricks", "access_token"): "token",
    ("database", "password"): "secret",
    ("duckdb", "motherduck_token"): "token",
    ("snowflake", "user"): "login",
}
"""The model fields, by block type, that may take a `${NAME}` on the server.

A password or a passphrase takes free text, since no reader repeats one. A
database password, from the `password` setting or the uri, reaches only
ConnectorX, percent-encoded into the uri by `connection_uri`, and only for a
reader in `_PASSWORD_READERS`. A Snowflake password or passphrase reaches
only `snowflake.connector.connect`. The corpus tests drive each of those
readers with a wrong password of every character class, against a service
that refuses it, and no answer holds 4 characters in a row of it. They drive
ClickHouse, whose host an agent chooses and whose error repeats a refusal
whole, against a service that writes its request back too. The mask matches no escaped form
of a value, such as doubled quotes, backslashes, JSON, ODBC braces, libpq
quoting, or base64, and no cut of one, because no reader writes a password
in such a form. A reader that did would leave `_PASSWORD_READERS`, or its
password would get a shape in `_shape_needed`.

A token keeps `_TOKEN`'s shape, as a storage option's does, which costs a
real token nothing. Snowflake can echo the `user`, a login name whose shape
`_LOGIN_NAME` holds to text that reads the same raw, encoded, and decoded,
so an echo is the text the mask matches."""

_PASSWORD_READERS: Final = frozenset(
    {"postgres", "postgresql", "mysql", "mssql", "oracle", "clickhouse"}
)
"""The ConnectorX readers, by the name `connectorx_url` gives, that may take a
database password from a `${NAME}` on the server, in `password` or in the uri.

`connectorx_url` drops a `+suffix` and reads `redshift` as `postgresql`, so
Redshift is here as `postgresql`, and `redshift-iam` is not. What each reader
sends, and what its error says:

- postgres: the raw password, or only a SCRAM or MD5 proof. The server says
  "password authentication failed for user".
- mysql: a scramble only. The driver refuses `mysql_clear_password`. ERR 1045
  names the user, never the password.
- mssql: tiberius sends LOGIN7, and the error is "Timed out in bb8".
- clickhouse: the `x-clickhouse-key` header, percent-encoded as
  `connection_uri` wrote it, which the mask matches. The server says
  "Authentication failed", and the error repeats the whole body of a refusal,
  so a service that writes the header back shows only the mask.
- oracle: ODPI-C. The error is DPI-1047 when there is no client, and
  ORA-01017 is fixed text. CI does not reach Oracle's network path.

Trino is not here. Its reader sends the user and password as basic auth,
base64 the mask does not match, to whatever host and port the uri names, and
its error repeats the whole body of a refusal. So a service on an allowed
host that writes its request headers back would hand the password over.

tests/integration/test_mcp_password_echo.py and
tests/integration/test_mcp_password_servers.py are the proof. A reader joins
only with a case in the corpus."""

_OPTION_SLOTS: Final[tuple[tuple[frozenset[str], _Slot], ...]] = (
    (_SECRET_OPTIONS, "token"),
    (_ACCOUNT_KEY_OPTIONS, "account key"),
    (_LOGIN_OPTIONS, "login"),
    (_SAS_OPTIONS, "sas"),
)
"""The storage option keys that may take a `${NAME}` on the server, each set's
docstring saying why no reader echoes its value, or why an echo is the text
the mask matches."""

_READER_ONLY_VARIABLES: Final[Mapping[str, _Slot]] = {
    **dict.fromkeys(
        (
            "AWS_SECURITY_TOKEN",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "AZURE_FABRIC_TOKEN",
            "AZURE_FABRIC_SESSION_TOKEN",
            "IDENTITY_HEADER",
            "MSI_SECRET",
            "AZURE_MSI_SECRET",
            "GOOGLE_BEARER_TOKEN",
            "HF_TOKEN",
            "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
            "ALIBABA_CLOUD_SECURITY_TOKEN",
            "OSS_ACCESS_KEY_SECRET",
            "OSS_SESSION_TOKEN",
            "TENCENTCLOUD_SECRET_KEY",
            "TKE_SECRET_KEY",
            "TENCENTCLOUD_TOKEN",
            "TENCENTCLOUD_SECURITY_TOKEN",
            "QCLOUD_SECRET_TOKEN",
            "HUAWEI_CLOUD_SECRET_ACCESS_KEY",
            "HUAWEI_CLOUD_SECURITY_TOKEN",
            "AZURE_STORAGE_BEARER_TOKEN",
            "SYSTEM_ACCESSTOKEN",
            "UNITY_ACCESS_TOKEN",
            "DATABRICKS_TOKEN",
            "DATABRICKS_ACCESS_TOKEN",
            "UNITY_CLIENT_SECRET",
            "DATABRICKS_CLIENT_SECRET",
            *MOTHERDUCK_TOKEN_VARIABLES,
        ),
        "token",
    ),
    "AZURE_STORAGE_MASTER_KEY": "account key",
    "AZBLOB_ACCOUNT_KEY": "account key",
    "AWS_SSE_CUSTOMER_KEY_BASE64": "account key",
    **dict.fromkeys(
        (
            "AZURE_AUTHORITY_ID",
            "AZURE_STORAGE_AUTHORITY_ID",
            "ARM_CLIENT_ID",
            "ARM_TENANT_ID",
            "ALIBABA_CLOUD_ACCESS_KEY_ID",
            "OSS_ACCESS_KEY_ID",
            "TENCENTCLOUD_SECRET_ID",
            "TKE_SECRET_ID",
            "HUAWEI_CLOUD_ACCESS_KEY_ID",
            "UNITY_CLIENT_ID",
            "DATABRICKS_CLIENT_ID",
        ),
        "login",
    ),
}
"""The environment variables a reader takes a credential from that no storage option names.

Each name, its reader, and the evidence that the reader sends its value:

- `AWS_SECURITY_TOKEN`: boto3 behind Polars' credential provider, and
  libarrow. Probed, and echoed.
- `AWS_CONTAINER_AUTHORIZATION_TOKEN`: botocore, aws-sdk-cpp, and
  deltalake's AWS SDK. Probed, and echoed through `CredentialRetrievalError`.
- `AZURE_FABRIC_TOKEN` and `AZURE_FABRIC_SESSION_TOKEN`: object_store.
- `IDENTITY_HEADER`, `MSI_SECRET`, and `AZURE_MSI_SECRET`: managed identity
  in object_store, deltalake, and libarrow.
- `GOOGLE_BEARER_TOKEN`: object_store. Probed, and echoed by a fake store.
- `HF_TOKEN`: Polars, for an `hf://` location, a scheme in `_SERVICE_SCHEMES`.
- Each name in `MOTHERDUCK_TOKEN_VARIABLES`: Veridelta's DuckDB connector.
- `AZURE_STORAGE_MASTER_KEY`: object_store's alias of the account key.
- `AWS_SSE_CUSTOMER_KEY_BASE64`: object_store. Probed, and echoed with
  sse-c. It is base64 of a 32-byte key, so the canonical shape costs a real
  key nothing.
- `AZURE_AUTHORITY_ID` and `AZURE_STORAGE_AUTHORITY_ID`: object_store's
  aliases of the tenant id.
- `UNITY_ACCESS_TOKEN` and its aliases `DATABRICKS_TOKEN` and
  `DATABRICKS_ACCESS_TOKEN`: deltalake's Unity Catalog reader, at a `uc://`
  table. Probed, sent as a bearer token to the workspace `DATABRICKS_HOST`
  names, and echoed in the reader's error, which quotes the workspace's.
- `UNITY_CLIENT_ID`, `UNITY_CLIENT_SECRET`, and their aliases
  `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`: the same reader.
  Probed, and sent in the form body of its request to the workspace's token
  service.

deltalake's OpenDAL stores, at an `opendal+` scheme, read the rest. Each
store needs an `opendal.` storage option, such as `opendal.endpoint`, which
the server refuses, so no configuration reaches them on the server yet. They
are listed so that the mask does not rest on that refusal. A secret key
takes the token slot, as `aws_secret_access_key` does.

- `ALIBABA_CLOUD_ACCESS_KEY_ID`, `ALIBABA_CLOUD_ACCESS_KEY_SECRET`,
  `ALIBABA_CLOUD_SECURITY_TOKEN`, and their aliases `OSS_ACCESS_KEY_ID`,
  `OSS_ACCESS_KEY_SECRET`, and `OSS_SESSION_TOKEN`: the OSS store. Probed,
  and sent in each request's headers.
- `TENCENTCLOUD_SECRET_ID`, `TENCENTCLOUD_SECRET_KEY`, and
  `TENCENTCLOUD_TOKEN`, their aliases `TKE_SECRET_ID`, `TKE_SECRET_KEY`,
  `TENCENTCLOUD_SECURITY_TOKEN`, and `QCLOUD_SECRET_TOKEN`: the COS store.
  Probed, and sent in each request's headers.
- `HUAWEI_CLOUD_ACCESS_KEY_ID`, `HUAWEI_CLOUD_SECRET_ACCESS_KEY`, and
  `HUAWEI_CLOUD_SECURITY_TOKEN`: the OBS store. Probed, and sent in each
  request's headers.
- `AZBLOB_ACCOUNT_KEY` and `AZURE_STORAGE_BEARER_TOKEN`: the Azure store,
  which signs with the key. Probed, and the token sent as a bearer token.
- `SYSTEM_ACCESSTOKEN`, `ARM_CLIENT_ID`, and `ARM_TENANT_ID`: the Azure
  store's sign-in from Azure Pipelines. Probed, and the token sent to the
  host `SYSTEM_OIDCREQUESTURI` names. The ids go to Microsoft Entra ID.

Some names are left out. ConnectorX reads none of `PGPASSWORD`, `MYSQL_PWD`,
`MSSQL_PASSWORD`, and `SQLCMDPASSWORD`, as its binary and a probe show.
Veridelta passes the Snowflake and Databricks connectors every setting as an
argument, so neither reads a `SNOWFLAKE_` or `DATABRICKS_` variable. The
Unity Catalog reader also reads the authority id, object id, managed identity
resource id, and federated token file under `UNITY_` and `DATABRICKS_`, but a
probe shows it sends none of them: it signs in only with a token or a client
secret.
`AZURE_CLIENT_CERTIFICATE_PASSWORD` unlocks a file on this machine and is
never sent."""

_ENVIRONMENT_SLOTS: Final[Mapping[str, _Slot]] = {
    **{
        key.upper(): slot
        for keys, slot in _OPTION_SLOTS
        for key in keys
        if key.startswith(("aws_", "azure_", "google_"))
    },
    **_READER_ONLY_VARIABLES,
}
"""The environment variables whose value `veridelta mcp` masks, each with its slot.

object_store's `from_env` reads `AWS_X` as the option `aws_x`, so the part
taken from the option table cannot drift from it. `_name_as_read` gives the
name a variable is looked up by here. A variable joins only with evidence
that a reader sends its value to a service, and with the slot whose shape
makes an echo the text the mask matches. Never record a prefix, since
`AWS_ALLOW_HTTP=true` would then mask `true` in every answer."""

_CASELESS_PREFIXES: Final = ("AWS_", "AZURE_", "GOOGLE_", "UNITY_", "DATABRICKS_")
"""The prefixes after which a reader's `from_env` ignores a name's case.

object_store matches the prefix as written, then reads the whole name in
lower case, so it reads `AZURE_Client_Secret` as the option
`azure_client_secret`. deltalake's Unity Catalog reader does the same after
`UNITY_` and `DATABRICKS_`, so it sends the value of `DATABRICKS_Token`, as a
probe showed."""

_KEY_JSON_VARIABLE: Final = "GOOGLE_SERVICE_ACCOUNT_KEY"
"""The environment variable object_store reads a Google service account key from, as JSON."""

_KEY_JSON_FIELDS: Final = ("private_key_id", "private_key", "client_secret", "refresh_token")
"""The fields of a Google key whose value a service can echo on its own."""

_TOKEN_FILE_VARIABLES: Final = (
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AZURE_FEDERATED_TOKEN_FILE",
    "ALIBABA_CLOUD_OIDC_TOKEN_FILE",
    "TENCENTCLOUD_WEB_IDENTITY_TOKEN_FILE",
    "TKE_IDENTITY_TOKEN_FILE",
)
"""The environment variables that name a file a reader reads a token from.

deltalake's OpenDAL OSS store sends the token in `ALIBABA_CLOUD_OIDC_TOKEN_FILE`'s
file to Alibaba Cloud's STS in a URL's query, which a probe's STS echoed. Its
COS store sends the token in the other two names' file to Tencent Cloud's
STS, as its binary shows, not probed. Neither store is reached on the
server yet, as `_READER_ONLY_VARIABLES` says."""

_LONGEST_TOKEN_FILE: Final = 64 * 1024
"""The most bytes of a token file the server reads. A larger file holds no token."""

_URI_LOGIN: Final = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://([^@/?#]*)@")
"""A database uri's scheme and login, up to the `@` that ends it."""

_PLACEHOLDER: Final = "vdref{}x"
"""Letters that stand for one reference while a uri is split into its parts."""

_LOGIN_NAME: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@-]{0,255}")
"""A login name a service may echo: no character a reader encodes or reads as a boundary."""

_TOKEN: Final = re.compile(r"[A-Za-z0-9+/=._~-]+")
"""A secret key or a token: base64, base64url, or a JWT, in ASCII.

A service that echoes such a value writes it back as the same text, since
XML and JSON escape none of these characters, none is space a service
trims, and none changes with the text's encoding."""

_UNRESERVED: Final = re.compile(r"[A-Za-z0-9._~-]+")
"""A uri's user or password whose raw, encoded, and decoded forms are the same text."""

_LONGEST_URI_USER: Final = 32
"""The most characters a uri user from the environment may hold.

A database can cut a user name before it repeats the name in an error, and
the mask matches only a whole value. No supported database cuts a name of 32
characters or fewer: MySQL keeps 32 characters, PostgreSQL 63 bytes, Redshift
127 bytes, and SQL Server and Oracle 128. MySQL before 5.7.8 kept 16, and is
out of support. A uri user is in `_UNRESERVED`'s shape, ASCII, so its
characters are its bytes."""

_SAS_NAMES: Final = frozenset(
    {
        "sv", "ss", "srt", "sp", "se", "st", "sip", "spr", "sig", "sr", "si", "sdd", "ses",
        "skoid", "sktid", "skt", "ske", "sks", "skv", "saoid", "suoid", "scid", "sduoid",
        "rscc", "rscd", "rsce", "rscl", "rsct",
    }
)  # fmt: skip
"""The parameters an Azure SAS token holds."""

_SAS_VALUE: Final = re.compile(r"[A-Za-z0-9%._~:,+/=-]*")
"""A SAS parameter's value, as Azure writes one: encoded, with no `&`, `#`, quote, or space."""


def _is_sas_token(value: str) -> bool:
    """Say whether a value is an Azure SAS token: `name=value` parameters joined by `&`, each once."""
    parameters = [part.partition("=") for part in value.removeprefix("?").split("&")]
    names = {name for name, _, _ in parameters}
    return len(names) == len(parameters) and all(
        name in _SAS_NAMES and equals and _SAS_VALUE.fullmatch(written)
        for name, equals, written in parameters
    )


def _is_account_key(value: str) -> bool:
    """Say whether a value is base64 as an encoder writes it, so a reader decodes it without an error.

    object_store decodes a key strictly, and its error for a last symbol with
    bits it does not use names that symbol and where it stands. A key that
    decodes only leniently is refused before a reader sees it.
    """
    return (
        BASE64.fullmatch(value) is not None
        and base64.b64encode(base64.b64decode(value)).decode() == value
    )


def _uri_slot(match: re.Match[str]) -> _Slot | None:
    """Say whether a reference is a database uri's whole user or its whole password.

    Each reference in the uri stands as letters while the login is split, so
    no value moves a boundary.
    """
    template, mine, last = "", (-1, -1), 0
    for index, other in enumerate(match.re.finditer(match.string)):
        template += match.string[last : other.start()]
        placeholder = _PLACEHOLDER.format(index)
        if other.start() == match.start():
            mine = (len(template), len(template) + len(placeholder))
        template += placeholder
        last = other.end()
    login = _URI_LOGIN.match(template + match.string[last:])
    if login is None:
        return None
    # The part is found by where the reference stands, since the uri's own text
    # can hold the letters that stand for it.
    user, colon, _ = login[1].partition(":")
    start = login.start(1)
    if mine == (start, start + len(user)):
        return "uri user"
    return "uri password" if colon and mine == (start + len(user) + 1, login.end(1)) else None


_OPTION_PATHS: Final[Mapping[str, tuple[str, ...]]] = {
    "delta": ("storage_options",),
    "iceberg": ("storage_options",),
    "file": ("options", "storage_options"),
}
"""Where each block type holds its storage options, after the side's name."""


def _option_key(block_type: str, path: tuple[str | int, ...]) -> str | None:
    """Return the storage option key a setting's path names, in lowercase, if it names one."""
    if _OPTION_PATHS.get(block_type) != path[1:-1]:
        return None
    key = path[-1]
    return key.lower() if isinstance(key, str) and key.isascii() else None


def _slot(reference: Reference, match: re.Match[str]) -> _Slot | None:
    """Return the credential slot a reference stands in, or None when it stands elsewhere."""
    if not isinstance(reference.block_type, str):
        return None
    if len(reference.path) == 2:
        field = (reference.block_type, reference.path[1])
        if field == ("database", "uri"):
            return _uri_slot(match)
        return _FIELD_SLOTS.get(field)
    key = _option_key(reference.block_type, reference.path)
    return next((slot for keys, slot in _OPTION_SLOTS if key in keys), None)


def _uri_part_needed(slot: _Slot, value: str) -> str | None:
    """Say what shape a uri's user or password needs, or None when it has it."""
    if _UNRESERVED.fullmatch(value) is None:
        unreserved = (
            "letters, digits, '.', '_', '~', and '-' only, so the uri reads the same encoded or "
            "decoded"
        )
        if slot == "uri user":
            return unreserved
        return (
            f"{unreserved}. Put a password with other characters in the side's 'password' setting"
        )
    if slot == "uri user" and len(value) > _LONGEST_URI_USER:
        return (
            f"at most {_LONGEST_URI_USER} characters, since a database can cut a longer user "
            "name in its error, where the mask cannot match it. Write a longer user in the "
            "file, or put a token in the side's 'password' setting"
        )
    return None


def _shape_needed(slot: _Slot, value: str) -> str | None:
    """Say what shape a slot's value needs, or None when it has it.

    An empty value passes, since it holds nothing to mask.
    """
    if not value:
        return None
    try:
        value.encode()
    except UnicodeEncodeError:
        return (
            "text in UTF-8, since a driver cannot send other bytes as text, and a reader can "
            "repeat them in forms no mask matches"
        )
    if slot in ("secret", "token", "account key", "uri password") and len(value) < _SHORTEST_MASKED:
        return f"at least {_SHORTEST_MASKED} characters, since a shorter one cannot be masked"
    if slot == "token" and _TOKEN.fullmatch(value) is None:
        return (
            "a key or a token of letters, digits, '+', '/', '=', '.', '_', '~', and '-' only, "
            "since a service can repeat a value it rejects in other forms"
        )
    if slot == "account key" and not _is_account_key(value):
        return "an account key in base64, as Azure gives one"
    if slot == "login" and _LOGIN_NAME.fullmatch(value) is None:
        return (
            "a login name of letters, digits, '.', '_', '@', and '-' that starts with a letter "
            "or a digit, since a service can repeat it in other forms"
        )
    if slot in ("uri user", "uri password"):
        return _uri_part_needed(slot, value)
    if slot == "sas" and not _is_sas_token(value):
        return (
            "an Azure SAS token: name=value parameters joined by '&', such as sv=...&sig=..., "
            "each named once"
        )
    return None


def _check_reference(reference: Reference, match: re.Match[str]) -> None:
    """Refuse a `${NAME}` that stands anywhere but as the whole value of a credential setting.

    A reader can repeat a setting's value cut, encoded, or joined to other
    text, in forms no mask matches. A credential slot holds a value no reader
    echoes, or one whose shape makes an echo the text the mask matches. A
    default, or an unset name, is text from the file and needs no shape.

    Raises:
        ConfigError: If the reference stands outside a slot, shares its setting
            with other text, or its value lacks the slot's shape. The message
            names the setting and the variable, never the value.
    """
    where = " -> ".join(str(step) for step in reference.path)
    named = f"${{{reference.name}}}"
    locations = (ConfigLocation(path=reference.path),)
    slot = _slot(reference, match)
    if slot is None:
        raise ConfigError(
            f"On veridelta mcp, {where} cannot take {named}. A reader can repeat a setting's "
            "value in forms this server cannot mask, so here a ${NAME} may stand only as the "
            "whole value of a credential setting, such as a password, a token, or a key. Write "
            "the value in the file, or run the configuration with veridelta run.",
            locations=locations,
        )
    if slot not in ("uri user", "uri password") and not reference.whole:
        raise ConfigError(
            f"On veridelta mcp, {where} must be {named} alone, with no text around it, since a "
            "reader can cut or encode a value joined to other text.",
            locations=locations,
        )
    needed = None if reference.value is None else _shape_needed(slot, reference.value)
    if needed is not None:
        raise ConfigError(
            f"On veridelta mcp, the value of {named} in {where} must be {needed}.",
            locations=locations,
        )


_RECORDED: ContextVar[set[str] | None] = ContextVar("_RECORDED", default=None)
"""The values the running server has expanded from its environment, for the call in progress."""

_TOKENS: ContextVar[frozenset[str]] = ContextVar("_TOKENS", default=frozenset())
"""The tokens the token files held as the call in progress started.

They are kept apart from `_RECORDED`, since a file rotates and a token it
held before is no longer sent."""

_RECORDING: Final = threading.Lock()
"""Held while a recorded set is read or added to, since the SDK runs each call in a thread."""


def _record_and_check(reference: Reference, match: re.Match[str]) -> None:
    """Refuse a reference outside a credential slot, else record the value it expands to."""
    _check_reference(reference, match)
    recorded = _RECORDED.get()
    if recorded is not None and reference.value is not None:
        with _RECORDING:
            recorded.add(reference.value)


def _name_as_read(name: str) -> str:
    """Return the name a reader takes an environment variable as.

    A name that starts with one of `_CASELESS_PREFIXES`, as written, is read
    in any case after it, so it is returned in upper case. Any other name is
    read as written, and returned as it is. On Windows, `os.environ` ignores
    case already.
    """
    return name.upper() if name.startswith(_CASELESS_PREFIXES) else name


def _environment_values(environ: Mapping[str, str]) -> set[str]:
    """Return each credential a reader takes from the server's environment, to be masked.

    Each variable is looked up by `_name_as_read`, so a name such as
    `AZURE_Client_Secret` is masked and checked as object_store reads it. A
    Google key is recorded whole, and each field of it that a service can echo
    on its own too. Its JSON is never refused, since object_store's errors for
    it quote none of its text.

    Raises:
        ConfigError: If a variable's value lacks its slot's shape. The message
            names the variable as it is set, never the value.
    """
    values: set[str] = set()
    for name, value in environ.items():
        if not value:
            continue
        read_as = _name_as_read(name)
        if read_as == _KEY_JSON_VARIABLE:
            values.update(_key_json_values(value))
            continue
        slot = _ENVIRONMENT_SLOTS.get(read_as)
        if slot is None:
            continue
        needed = _shape_needed(slot, value)
        if needed is not None:
            raise ConfigError(
                f"On veridelta mcp, the environment variable {name} must be {needed}. A reader "
                "takes it from the server's environment, and a service can repeat it in an "
                "error. Correct the value, or leave the variable out of the environment the "
                "host starts the server with."
            )
        values.add(value)
    return values


def _key_json_values(key: str) -> set[str]:
    """Return a Google key, whole, and each field of it a service can echo on its own."""
    values = {key}
    try:
        fields = json.loads(key)
    except ValueError:
        fields = None
    if isinstance(fields, dict):
        found = cast("dict[str, object]", fields)
        values.update(
            text
            for field in _KEY_JSON_FIELDS
            if isinstance(text := found.get(field), str) and len(text) >= _SHORTEST_MASKED
        )
    return values


def _token_file_values(environ: Mapping[str, str]) -> set[str]:
    """Return the token in each file a token file variable names, to be masked.

    A file that cannot be read, is larger than `_LONGEST_TOKEN_FILE`, is not
    UTF-8, or holds no value in `_TOKEN`'s shape of at least
    `_SHORTEST_MASKED` characters is skipped, since it holds no token a
    reader sends. Only a regular file is read. A named pipe with no writer
    would hold the server, and one such as `<(get-token)` gives its token
    once, to the first process that reads it, which must be the reader. The
    file's path is not recorded. Each variable is looked up by
    `_name_as_read`, as in `_environment_values`.
    """
    # Only Windows has O_BINARY, and O_NONBLOCK opens a named pipe without waiting for a writer.
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0)
    values: set[str] = set()
    for name, path in environ.items():
        if not path or _name_as_read(name) not in _TOKEN_FILE_VARIABLES:
            continue
        try:
            if not stat.S_ISREG(os.stat(path).st_mode):
                continue
            with open(os.open(path, flags), "rb") as file:
                # A pipe swapped in after the check is opened, but not read.
                if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                    continue
                read = file.read(_LONGEST_TOKEN_FILE + 1)
            if len(read) > _LONGEST_TOKEN_FILE:
                continue
            token = read.decode("utf-8").strip()
        except (OSError, UnicodeDecodeError):
            continue
        if len(token) >= _SHORTEST_MASKED and _TOKEN.fullmatch(token) is not None:
            values.add(token)
    return values


def _load_checked(
    resolved: Path, unset: list[str] | None
) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration the server opens, refusing a `${NAME}` outside a credential slot.

    This is the server's one way to load a file, so the masked values come
    from the load itself, and the refusal comes before any check or reader.

    A SQLite file has no password. The driver reads a database side's
    `password` into the file's path, which the system resolves, so a message
    could repeat the value with a `/` changed or a `..` folded away. So a
    `${NAME}` in the `password` of a SQLite side is refused too. So is one in
    the `password`, or the uri's password, of any other side whose reader is
    not in `_PASSWORD_READERS`, since no test shows that reader never repeats
    it.

    Raises:
        ConfigError: If a reference stands outside a credential setting, or in
            the `password` of a SQLite side. Also if it stands in the
            `password`, or the uri's password, of a database side whose reader
            no test has probed.
    """
    references: list[tuple[Reference, bool]] = []

    def check_and_keep(reference: Reference, match: re.Match[str]) -> None:
        _record_and_check(reference, match)
        references.append((reference, _slot(reference, match) in ("secret", "uri password")))

    loaded = load_config_with_hook(resolved, unset_env=unset, on_reference=check_and_keep)
    sides = {"source": loaded[1], "target": loaded[2]}
    for reference, password in references:
        side = sides.get(str(reference.path[0]))
        if (
            reference.path[1:] == ("password",)
            and isinstance(side, DatabaseConfig)
            and sqlite_path(side) is not None
        ):
            raise ConfigError(
                f"On veridelta mcp, {reference.path[0]} -> password cannot take "
                f"${{{reference.name}}} for a SQLite file. SQLite has no password, and the "
                "driver reads this one into the file's path, in forms this server cannot "
                "mask. Remove the password.",
                locations=(ConfigLocation(path=reference.path),),
            )
        elif (
            password
            and isinstance(side, DatabaseConfig)
            and connectorx_url(side).scheme not in _PASSWORD_READERS
        ):
            where = " -> ".join(str(step) for step in reference.path)
            raise ConfigError(
                f"On veridelta mcp, {where} cannot take ${{{reference.name}}} for this uri. A "
                "test shows that no reader of a PostgreSQL, Redshift, MySQL, SQL Server, Oracle, "
                "or ClickHouse uri repeats a password, so the server takes one from its "
                "environment only for those. Use one of their schemes, such as mysql:// for "
                "MariaDB, or run the configuration with veridelta run.",
                locations=(ConfigLocation(path=reference.path),),
            )
    return loaded


def check_configuration(
    settings: Settings,
    path: str,
    *,
    schemas: bool = False,
    allow_missing_env: bool = False,
) -> ValidationReport:
    """Check a configuration file for what would stop a run, as `validate --json` does.

    A `${NAME}` outside a credential setting is an error, as the server loads
    every file. Unless the server allows row values, a finding leaves out what
    Polars, a reader, or a driver said about a schema read that failed. In a
    tool call, each finding has the values the server expanded masked, and
    then each URL in it loses its query, which can hold a credential the
    reader signed with. The masking comes first, so a value that holds a `?`
    is masked whole. A direct call records no value, so it masks nothing.

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
        diff, source, target = _load_checked(resolved, unset)
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
    findings = [
        finding.model_copy(
            update={
                "message": without_queries(_mask(finding.message)),
                "locations": [
                    location.model_copy(
                        update={"message": location.message and _mask(location.message)}
                    )
                    for location in finding.locations
                ],
            }
        )
        for finding in findings
    ]
    return validation_report(str(resolved), [*unset_variable_findings(unset or []), *findings])


def _load(settings: Settings, path: str) -> tuple[DiffConfig, SourceRef, SourceRef]:
    """Load a configuration a tool opens, refusing data outside the roots or on a host not allowed."""
    diff, source, target = _load_checked(resolve_path(settings, path), None)
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


def _sas_signatures(value: str) -> tuple[str, ...]:
    """Return each form of the `sig` parameter a SAS token holds: as written, decoded, and encoded."""
    parameters = (part.partition("=") for part in value.removeprefix("?").split("&"))
    return tuple(
        form
        for name, _, written in parameters
        if name == "sig"
        for form in (written, unquote(written), quote(unquote(written), safe=""))
    )


@functools.lru_cache(maxsize=8)
def _mask_pattern(values: frozenset[str]) -> re.Pattern[str] | None:
    """Join every form of the recorded values into one pattern, longest first, in any case.

    A value's forms are its text, its percent-encoding, its form encoding,
    where `~` becomes `%7E`, as object_store writes an OAuth request's body,
    its bytes in hex with a space between each, as S3 writes a request it
    could not verify, and the `sig` of a SAS token. A byte outside UTF-8 is encoded as it was
    read, so no value stops the pattern from being built.

    No head of a value is a form. A head would let an agent learn a value one
    character per call, by naming guesses as columns and seeing which come
    back masked, and tokens of one kind share their opening, such as a JWT's
    header, so a head would mask other data for as long as the server runs.
    """
    forms = {
        form
        for value in values
        for form in (
            value,
            quote(value, safe="", errors="surrogateescape"),
            quote(value, safe="", errors="surrogateescape").replace("~", "%7E"),
            value.encode(errors="surrogateescape").hex(" "),
            *_sas_signatures(value),
        )
        if len(form) >= _SHORTEST_MASKED
    }
    if not forms:
        return None
    ordered = sorted(forms, key=len, reverse=True)
    # Matching in any case can mask more than the value, which is the safe side
    # here, unlike the check of a scheme or a host, which matches ASCII only.
    return re.compile("|".join(map(re.escape, ordered)), re.IGNORECASE)


_ECHOED_CREDENTIAL: Final = re.compile(
    r"(<(Token-\d+|AWSAccessKeyId|CanonicalRequest|CanonicalRequestBytes|StringToSign"
    r"|StringToSignBytes|SignatureProvided)>)(.*?)(</\2>|$)",
    re.IGNORECASE | re.DOTALL,
)
"""The elements of an S3 error that repeat a credential, with their content.

S3 writes a session token it rejected, the access key id, or a canonical
request that holds `x-amz-security-token` into these elements. The server
never sees a credential a reader finds on its own, from a profile, SSO, or
an instance role, so it blanks these elements whatever they hold. `$`
catches an element cut at the message's end."""


def _without_echoed_credentials(text: str) -> str:
    """Blank the content of each element of an S3 error that repeats a credential."""
    return _ECHOED_CREDENTIAL.sub(r"\1***\4", text)


def _mask(text: str) -> str:
    """Mask every credential the server knows of, in each form, in any case.

    It first blanks each element of an S3 error that repeats a credential,
    since a reader can sign with one the server never saw. Then it masks
    each value the server has expanded from its environment, and each
    credential a reader takes from it. It reads the recorded values when the
    answer is built, so a value a reader echoes in a later call, such as
    from Polars' cache of object stores, is masked there too. A finding of
    `validate_config` reaches the agent without `_failure`, so the blanking
    is here, where every answer passes. It masks the tokens the token files
    held as the call started too.
    """
    text = _without_echoed_credentials(text)
    with _RECORDING:
        values = frozenset(_RECORDED.get() or ()) | _TOKENS.get()
    pattern = _mask_pattern(values)
    return text if pattern is None else pattern.sub("***", text)


def _masked(value: Any) -> Any:
    """Mask the recorded values in every string of an answer, the keys of a mapping included."""
    if isinstance(value, str):
        return _mask(value)
    if isinstance(value, dict):
        mapping = cast("dict[str, Any]", value)
        return {_masked(key): _masked(item) for key, item in mapping.items()}
    if isinstance(value, list):
        return [_masked(item) for item in cast("list[object]", value)]
    return value


def _answer(
    tool_error: Any,
    work: Callable[[], _R],
    recorded: set[str],
    *,
    row_values: bool = False,
) -> _R:
    """Run a tool's work, masking the server's recorded values in its answer, or fail the call.

    `recorded` holds every value the server has expanded from its
    environment since it started, and the work adds to it as it loads a
    file. The SDK passes only a `ToolError`'s message on to the agent, so
    every failure becomes one, named as `run --json` names it, with the
    values masked there too. Unless `row_values` is set, a message that can
    quote the data is left out. Each URL in a failure's message then loses
    its query, which can hold a credential the reader signed with. The
    masking comes first, so a value that holds a `?` is masked whole.

    A projected token file rotates about hourly, so each call masks the
    token each file holds as the call starts, and none it held before, which
    no reader sends any more and which would grow the mask for as long as
    the server runs.
    """
    tokens = _TOKENS.set(frozenset(_token_file_values(os.environ)))
    token = _RECORDED.set(recorded)
    try:
        try:
            # A reader masks its own secrets in a driver's message, and one written
            # in the file can be a single letter, so each value is masked whole first.
            with masking_first(_mask):
                result = work()
        except (Exception, PanicException) as exc:
            # A panic in Polars is no `Exception`, and would stop the server instead.
            failure = _failure(exc, row_values=row_values)
            raise tool_error(without_queries(_mask(failure))) from exc
        return cast("_R", _masked(result))
    finally:
        _RECORDED.reset(token)
        _TOKENS.reset(tokens)


def build_server(settings: Settings) -> "MCPServer":
    """Build the server and register its tools.

    Args:
        settings (Settings): The roots every tool is held to.

    Returns:
        MCPServer: The SDK's server, ready for `run()`.

    Raises:
        VerideltaError: If the `mcp` extra is not installed.
        ConfigError: If a credential variable a reader takes from the
            server's environment lacks the shape the server can mask.
    """
    loaded = _load_sdk()
    # Every value this server expands from its environment, kept while it runs,
    # since a reader's cache can echo one in a later call that never named it,
    # and every credential a reader takes from it. Each call reads the token files.
    seen = _environment_values(os.environ)
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
            seen,
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
            seen,
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
            seen,
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
            seen,
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
            seen,
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
            seen,
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
