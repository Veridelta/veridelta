# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Abstract connector interface for warehouse and lakehouse backends."""

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Literal, Protocol, Self, runtime_checkable

import polars as pl

from veridelta.connectors.sql import SQLPushdownCompiler

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


@runtime_checkable
class PushdownSession(Protocol):
    """The two members the pushdown summary needs from a connector.

    Narrower than `VerideltaConnector`, which also covers lakehouse scans that
    have no compiler. Stating the requirement structurally keeps the summary
    reusable by anything that can compile and execute, including the
    differential test harness.
    """

    compiler: SQLPushdownCompiler

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiled SQL and return an unevaluated result graph.

        Args:
            statement (str): Compiler-produced SQL.
            query_type (PushdownQueryType): Which round-trip this represents.

        Returns:
            pl.LazyFrame: Unevaluated result graph.
        """
        ...


class VerideltaConnector(ABC):
    """Session and compute contract for remote or table-format data sources.

    Four families implement it, and they divide the work differently:

    - Warehouse connectors (`SnowflakeConnector`, `DatabricksConnector`,
      `BigQueryConnector`) hold
      a driver session plus a `compiler`. The engine compiles comparison SQL
      and calls `execute_pushdown` for each round-trip; results come back as
      Arrow wrapped in a LazyFrame. They also satisfy `PushdownSession`.
    - Lakehouse connectors (`DeltaLakeConnector`, `IcebergConnector`) open a
      Polars `scan_*` handle and expose it through `lazyframe()`. The diff then
      runs in the local engine; their `execute_pushdown` always raises.
    - The database and DuckDB connectors (`DatabaseConnector`,
      `DuckDBConnector`) read one table or query when they connect, through
      ConnectorX or DuckDB, and expose the rows through `lazyframe()`. The
      diff runs in the local engine, as for a lakehouse.
    - The pushdown sessions (`PostgresPushdownSession`,
      `DuckDBPushdownSession`) run compiled SQL inside Postgres or DuckDB, for
      two tables that both set `pushdown`. Like a warehouse connector, each
      satisfies `PushdownSession`.

    Call `connect()` before anything else and `close()` when finished; the
    connector is also a context manager whose exit calls `close()`. After
    `close()` the connector is back in its unconnected state, so any further
    call raises `ConnectorError` until `connect()` runs again.

    `fetch_schema()` reads column metadata without collecting rows, but what
    it describes depends on the family: the scanned table for lakehouse
    connectors, the rows read for the database and DuckDB connectors, and the
    result of the most recent `execute_pushdown` statement for warehouse
    connectors and pushdown sessions. The engine itself probes warehouse
    columns through `SQLPushdownCompiler.compile_schema_probe_query` rather
    than this method.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish a warehouse session or lakehouse lazy-scan handle.

        Raises:
            ConnectorError: If the backend is unimplemented or extras are missing.
        """

    @abstractmethod
    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute dialect-specific compute and return an unevaluated LazyFrame.

        Args:
            statement (str): SQL (warehouse) or deferred predicate payload.
            query_type (PushdownQueryType): Which round-trip the SQL represents
                (`mismatch`, `added`, `missing`, `count`, `duplicates`,
                `columns`, `schema`, `value_maps`, `settings`, or `samples`).

        Returns:
            pl.LazyFrame: Unevaluated result graph. Must not be collected here.

        Raises:
            ConnectorError: If pushdown is unimplemented or not applicable.
        """

    @abstractmethod
    def fetch_schema(self) -> pl.Schema:
        """Return column metadata without fully materializing the dataset.

        Returns:
            pl.Schema: Deterministic column names and dtypes.

        Raises:
            ConnectorError: If called before `connect()` or if the backend is
                unimplemented.
        """

    def close(self) -> None:  # noqa: B027 - deliberate no-op default, see below
        """Release the session or scan handle established by `connect()`.

        Safe to call before `connect()` and safe to call twice. The default
        holds no resources; connectors that open a driver session or a scan
        override it. It is not abstract so that subclasses written against the
        three-method contract keep working.
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
