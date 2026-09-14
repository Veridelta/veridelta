# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Abstract connector interface for warehouse and lakehouse backends."""

from abc import ABC, abstractmethod
from typing import Literal, Protocol, runtime_checkable

import polars as pl

from veridelta.connectors.sql import SQLPushdownCompiler

PushdownQueryType = Literal["mismatch", "added", "missing", "count", "columns", "schema"]
"""Warehouse pushdown round-trip: comparison rows, tallies, totals, or probes."""


@runtime_checkable
class PushdownSession(Protocol):
    """The two members the pushdown summary actually needs from a connector.

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

    Two families implement it, and they divide the work differently:

    - Warehouse connectors (`SnowflakeConnector`, `DatabricksConnector`) hold
      a driver session plus a `compiler`. The engine compiles comparison SQL
      and calls `execute_pushdown` for each round-trip; results come back as
      Arrow wrapped in a LazyFrame. They also satisfy `PushdownSession`.
    - Lakehouse connectors (`DeltaLakeConnector`, `IcebergConnector`) open a
      Polars `scan_*` handle and expose it through `lazyframe()`. The diff then
      runs in the local engine; their `execute_pushdown` always raises.

    Call `connect()` before anything else. `fetch_schema()` reads column
    metadata without collecting rows, but what it describes depends on the
    family: the scanned table for lakehouse connectors, and the result of the
    most recent `execute_pushdown` statement for warehouse connectors. The
    engine itself probes warehouse columns through
    `SQLPushdownCompiler.compile_schema_probe_query` rather than this method.
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
                (`mismatch`, `added`, `missing`, `count`, `columns`, or `schema`).

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
