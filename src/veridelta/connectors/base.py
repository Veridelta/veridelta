# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The connector contracts: the shared lifecycle, the readers, and the pushdown sessions."""

import importlib
from abc import ABC, abstractmethod
from types import ModuleType, TracebackType
from typing import ClassVar, Final, Literal, Self

import polars as pl

from veridelta.connectors.sql import SQLPushdownCompiler
from veridelta.exceptions import ConnectorError
from veridelta.models import redacted_location


def mask_secrets(text: str, *secrets: str | None) -> str:
    """Replace each set secret in driver output with `***`, longest first.

    Args:
        text (str): Driver output that may repeat a credential.
        secrets (str | None): The credentials a connector holds. An unset one is skipped.

    Returns:
        str: The text with every secret replaced.
    """
    # Longest first, so a secret containing another is masked whole.
    for secret in sorted({secret for secret in secrets if secret}, key=len, reverse=True):
        text = text.replace(secret, "***")
    return text


def shown_location(location: str) -> str:
    """Name a path or URL in a log line or an error, with anything secret left out.

    Args:
        location (str): A file path, or the URL of a file or a table.

    Returns:
        str: The location as `redacted_location` returns it, or a note that it
            does not parse as a URL.
    """
    return redacted_location(location) or "a location that does not parse as a URL"


def without_location(text: str, location: str) -> str:
    """Replace a location inside driver text with the form `shown_location` gives.

    A reader's error can quote the location it was given whole, query included.

    Args:
        text (str): The driver's message.
        location (str): The location the driver was given.

    Returns:
        str: The message, with the location's secrets left out.
    """
    return text.replace(location, shown_location(location))


PROBE_NEEDS_A_TABLE: Final = "A schema probe reads a 'table'; a 'query' would have to run in full."
"""Why a database or DuckDB schema probe refuses a side that sets `query`."""


def read_subject(table: str | None) -> str:
    """Name what a database or DuckDB side reads, without repeating any SQL."""
    if table is not None:
        return f"table '{table}'"
    return "the configured query"


def optional_module(name: str) -> ModuleType | None:
    """Import the module of an optional extra, or return None when it is not installed.

    A module that reads through an extra binds the result to a module attribute,
    which its tests patch, so no test depends on the extras installed where it runs.

    Args:
        name (str): The module to import, such as `snowflake.connector`.

    Returns:
        ModuleType | None: The module, or `None` when the import fails.
    """
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


PushdownQueryType = Literal[
    "mismatch",
    "added",
    "missing",
    "count",
    "duplicates",
    "columns",
    "schema",
    "value_maps",
    "settings",
    "samples",
]
"""Warehouse pushdown round-trip: comparison rows, tallies, totals, key checks,
probes, value map evidence, a check of the server's settings, or a sample of
changed rows with their values."""


class VerideltaConnector(ABC):
    """The lifecycle every connector shares: `connect()`, `close()`, and the context manager.

    A connector is one of two kinds, and the engine routes each source to one
    by its configuration:

    - A `ReaderConnector` reads a source for the local engine. The lakehouse
      connectors (`DeltaLakeConnector`, `IcebergConnector`) open a Polars
      `scan_*` handle, and the database and DuckDB connectors
      (`DatabaseConnector`, `DuckDBConnector`) read one table or query when
      they connect. Each hands the rows to the engine through `lazyframe()`,
      and the diff runs in Polars.
    - A `PushdownSession` runs compiled SQL where the data lives. The
      warehouse connectors (`SnowflakeConnector`, `DatabricksConnector`,
      `BigQueryConnector`) hold a driver session, and the pushdown sessions
      (`PostgresPushdownSession`, `DuckDBPushdownSession`) serve two tables
      that both set `pushdown`. The engine compiles comparison SQL with the
      session's `compiler` and calls `execute_pushdown` for each round-trip;
      results come back as Arrow wrapped in a LazyFrame.

    Call `connect()` before anything else and `close()` when finished; the
    connector is also a context manager whose exit calls `close()`. After
    `close()` the connector is back in its unconnected state, so any further
    call raises `ConnectorError` until `connect()` runs again.
    """

    @abstractmethod
    def connect(self) -> None:
        """Open the driver session, the scan, or the read that later calls use.

        Raises:
            ConnectorError: If the backend cannot be reached or its extra is missing.
        """

    def close(self) -> None:  # noqa: B027 - deliberate no-op default, see below
        """Release what `connect()` opened.

        Safe to call before `connect()` and safe to call twice. The default
        holds no resources; connectors that open a driver session, a scan, or
        a read override it. It is not abstract, so a subclass that holds
        nothing need not define it.
        """

    def __enter__(self) -> Self:
        """Return the connector unchanged; `connect()` stays an explicit call.

        Returns:
            The connector itself, so `with SnowflakeConnector(cfg) as c:` binds it.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connector when leaving the context, error or not.

        Args:
            exc_type (type[BaseException] | None): Pending exception type.
            exc (BaseException | None): Pending exception.
            traceback (TracebackType | None): Pending traceback.
        """
        self.close()


class ReaderConnector(VerideltaConnector):
    """A connector the local engine reads through `lazyframe()`.

    `connect()` opens a scan or reads the rows and keeps them as `_frame`,
    `lazyframe()` hands them to the engine, and the comparison runs in Polars.
    `close()` drops them. A reader has no `execute_pushdown`: nothing is pushed
    into a source that is compared locally.
    """

    _unconnected: ClassVar[str] = "Connector is not connected. Call connect() first."
    """What `lazyframe()` raises before `connect()`, naming the kind of source."""

    _frame: pl.LazyFrame | None = None
    """What `connect()` opened, and None before it and after `close()`."""

    def lazyframe(self) -> pl.LazyFrame:
        """Return what `connect()` opened, as an unevaluated LazyFrame.

        Returns:
            pl.LazyFrame: A lazy scan, or a lazy wrapper over the rows read.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(self._unconnected)
        return self._frame

    def close(self) -> None:
        """Drop what `connect()` opened. Idempotent; `connect()` opens it again."""
        self._frame = None


class PushdownSession(VerideltaConnector):
    """A connector that runs compiled comparison SQL where the data lives.

    The engine compiles every statement with `compiler` and runs it through
    `execute_pushdown`, one round-trip per `PushdownQueryType`. Only what a
    statement returns comes back: counts, keys, and the samples asked for,
    never the tables themselves.

    Attributes:
        compiler (SQLPushdownCompiler): The dialect's compiler, which the
            subclass sets before `connect()`.
    """

    compiler: SQLPushdownCompiler

    @abstractmethod
    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiled SQL and return an unevaluated result graph.

        Args:
            statement (str): Compiler-produced SQL. Never user text.
            query_type (PushdownQueryType): Which round-trip the SQL represents
                (`mismatch`, `added`, `missing`, `count`, `duplicates`,
                `columns`, `schema`, `value_maps`, `settings`, or `samples`).

        Returns:
            pl.LazyFrame: Unevaluated result graph. Must not be collected here.

        Raises:
            ConnectorError: If the session is not connected or the statement
                fails.
        """
