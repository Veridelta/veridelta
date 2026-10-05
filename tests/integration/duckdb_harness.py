# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential harness that executes compiled pushdown SQL against DuckDB.

The harness runs one pair of frames twice: once through the local Polars engine
and once through `SQLPushdownCompiler` output executed by DuckDB. Divergence in
null propagation, three-valued logic, or transform semantics surfaces as a
summary mismatch rather than as a silently different answer in production.

DuckDB stands in for the warehouse, not for a specific vendor. It validates that
the compiled SQL is semantically correct, but it cannot validate dialect-specific
behavior: Snowflake's `TRY_TO_TIMESTAMP` format language, Databricks' Java
format patterns, and vendor cast quirks are unreachable from here and must be
covered by string assertions in `tests/unit/test_sql_compiler.py`.

DuckDB's `levenshtein` also counts UTF-8 bytes where Snowflake, Databricks,
and the local engine count characters, so edit-distance parity cases use ASCII
text, on which the two agree.

Setting `VERIDELTA_PARITY_BACKEND=postgres` runs the same cases against a live
Postgres through `postgres_harness` instead: the comparison runs inside
Postgres, and the local engine reads the same tables back.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl

from tests.integration import postgres_harness
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler
from veridelta.engine import DiffEngine, _collect_pushdown_summary, _collect_value_map_proposals
from veridelta.exceptions import ConnectorError

if TYPE_CHECKING:
    from types import TracebackType

    from veridelta.models import DiffConfig, DiffResult, DiffSummary, ValueMapProposal

SOURCE_TABLE = "src_data"
"""Relation name the harness registers the source frame under."""

TARGET_TABLE = "tgt_data"
"""Relation name the harness registers the target frame under."""

PARITY_BACKEND = os.environ.get("VERIDELTA_PARITY_BACKEND", "duckdb")
"""Database the pushdown side runs in: `duckdb`, or `postgres` for a live server."""

if PARITY_BACKEND not in {"duckdb", "postgres"}:
    raise RuntimeError(
        f"VERIDELTA_PARITY_BACKEND must be duckdb or postgres, not {PARITY_BACKEND!r}."
    )

REFUSED_RULES = postgres_harness.REFUSED_RULES if PARITY_BACKEND == "postgres" else frozenset()
"""Rule fields the selected backend's pushdown refuses before running any query."""

UNPADDED_DTYPES: frozenset[type[pl.DataType]] = (
    postgres_harness.NUMERIC_DTYPES if PARITY_BACKEND == "postgres" else frozenset()
)
"""Dtypes the selected backend writes as different text on its two sides."""


class DuckDBPushdownSession:
    """A `PushdownSession` backed by an in-memory DuckDB database.

    Structurally satisfies the protocol the engine's pushdown path depends on,
    so parity tests exercise the real `_collect_pushdown_summary` code rather
    than a reimplementation of it.
    """

    def __init__(self, source: pl.DataFrame, target: pl.DataFrame) -> None:
        """Register the two frames as queryable relations.

        Args:
            source (pl.DataFrame): Rows to expose as `SOURCE_TABLE`.
            target (pl.DataFrame): Rows to expose as `TARGET_TABLE`.
        """
        self.compiler = SQLPushdownCompiler(SQLDialect.DUCKDB)
        self.statements: list[str] = []
        self._connection = duckdb.connect(":memory:")
        # ICU ships with the Python wheel and backs `timezone(...)`, which the
        # compiler emits for the `timezone` rule.
        self._connection.execute("SET TimeZone = 'UTC'")
        self._connection.register(SOURCE_TABLE, source.to_arrow())
        self._connection.register(TARGET_TABLE, target.to_arrow())

    def connect(self) -> None:
        """Satisfy the connector lifecycle. The database opens on construction."""

    def execute_pushdown(self, statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
        """Execute compiled SQL and wrap the Arrow result lazily.

        Mirrors the warehouse connectors: fetch Arrow, then hand it to Polars
        without touching dtypes, so the probed schema the compiler reasons about
        is the schema the executing engine actually reports.

        Args:
            statement (str): SQL produced by the compiler.
            query_type (str): Which comparison round-trip this represents.

        Returns:
            pl.LazyFrame: Unevaluated frame over the Arrow result.

        Raises:
            ConnectorError: If DuckDB rejects the statement.
        """
        self.statements.append(statement)
        try:
            table: Any = self._connection.execute(statement).to_arrow_table()
        except Exception as exc:  # pragma: no cover - surfaced as a test failure
            raise ConnectorError(f"DuckDB rejected pushdown SQL: {exc}\n{statement}") from exc
        frame = pl.from_arrow(table)
        if not isinstance(frame, pl.DataFrame):  # pragma: no cover - defensive
            raise ConnectorError("DuckDB did not return a tabular Arrow result.")
        return frame.lazy()

    def fetch_schema(self) -> pl.Schema:
        """Report the source relation's schema.

        Returns:
            pl.Schema: Column names and dtypes as DuckDB reports them.
        """
        probe = self.execute_pushdown(
            self.compiler.compile_schema_probe_query(SOURCE_TABLE), query_type="schema"
        )
        return probe.collect_schema()

    def close(self) -> None:
        """Release the in-memory database."""
        self._connection.close()

    def __enter__(self) -> DuckDBPushdownSession:
        """Enter a context that closes the database on exit.

        Returns:
            DuckDBPushdownSession: This session.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the database when leaving the context.

        Args:
            exc_type (type[BaseException] | None): Pending exception type.
            exc (BaseException | None): Pending exception.
            traceback (TracebackType | None): Pending traceback.
        """
        self.close()


def run_pushdown(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
) -> tuple[DiffResult, list[str]]:
    """Compile the comparison and execute every statement in the selected backend.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        tuple[DiffResult, list[str]]: The pushdown result and the SQL that
            produced it, in execution order.
    """
    if PARITY_BACKEND == "postgres":
        return postgres_harness.run_pushdown(config, source, target)
    with DuckDBPushdownSession(source, target) as session:
        result = _collect_pushdown_summary(session, SOURCE_TABLE, TARGET_TABLE, config)
        return result, list(session.statements)


def run_value_map_pushdown(
    config: DiffConfig,
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    min_confidence: float = 0.95,
    min_support: int = 5,
    sample_fraction: float = 1.0,
) -> tuple[list[ValueMapProposal], list[str]]:
    """Propose value maps from the frames through compiled SQL in the selected backend.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.
        min_confidence (float): Share of rows that must agree.
        min_support (int): Agreeing rows a proposal needs.
        sample_fraction (float): Share of source keys to read.

    Returns:
        tuple[list[ValueMapProposal], list[str]]: The proposals and the SQL
            that produced them, in execution order.
    """
    if PARITY_BACKEND == "postgres":
        return postgres_harness.run_value_map_pushdown(
            config,
            source,
            target,
            min_confidence=min_confidence,
            min_support=min_support,
            sample_fraction=sample_fraction,
        )
    with DuckDBPushdownSession(source, target) as session:
        proposals = _collect_value_map_proposals(
            session,
            SOURCE_TABLE,
            TARGET_TABLE,
            config,
            min_confidence=min_confidence,
            min_support=min_support,
            sample_fraction=sample_fraction,
        )
        return proposals, list(session.statements)


def run_local(config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame) -> DiffResult:
    """Execute the same comparison through the local Polars engine.

    Against Postgres, the frames are loaded and read back first, so the local
    engine sees the tables the pushdown side compared.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        DiffResult: The local engine's verdict and discrepancy rows.
    """
    if PARITY_BACKEND == "postgres":
        return postgres_harness.run_local(config, source, target)
    return DiffEngine(config, source.lazy(), target.lazy()).run()


def assert_parity(config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame) -> DiffSummary:
    """Assert the local engine and compiled SQL reach identical verdicts.

    Compares the fields both paths can populate. Pushdown projects primary keys
    only, so row contents are out of scope; the counts, the per-column drift
    tally, the set of columns actually compared, and the pass/fail verdict are
    not.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        DiffSummary: The agreed-upon local summary, for further assertions.
    """
    local = run_local(config, source, target)
    pushdown, statements = run_pushdown(config, source, target)

    context = "\n".join(statements)
    assert pushdown.summary.total_rows_source == local.summary.total_rows_source, context
    assert pushdown.summary.total_rows_target == local.summary.total_rows_target, context
    assert pushdown.summary.added_count == local.summary.added_count, context
    assert pushdown.summary.removed_count == local.summary.removed_count, context
    assert pushdown.summary.changed_count == local.summary.changed_count, context
    assert pushdown.summary.column_mismatches == local.summary.column_mismatches, context
    assert pushdown.summary.is_match == local.summary.is_match, context
    # A column silently dropped from one pipeline would otherwise still agree on
    # every count above, as long as that column happened to match everywhere.
    assert set(pushdown.compared_columns) == set(local.compared_columns), context
    return local.summary
