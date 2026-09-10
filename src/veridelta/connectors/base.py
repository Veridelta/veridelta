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
    """Session and compute contract for remote or table-format data sources."""

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
