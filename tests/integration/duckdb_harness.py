# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Differential harness that executes compiled pushdown SQL against DuckDB.

The harness runs one pair of frames twice: once through the local Polars engine
and once through `SQLPushdownCompiler` output executed by DuckDB. Divergence in
null propagation, three-valued logic, or transform semantics surfaces as a
summary mismatch rather than as a silently different answer in production.

Each case writes its frames into a temporary DuckDB file and runs pushdown
through `DuckDBPushdownSession`, the session a `pushdown: true` DuckDB pair
opens, so the shipped connection code runs too. DuckDB also stands in for the
warehouses: it validates that the compiled SQL is semantically correct, but it
cannot validate dialect-specific behavior. Snowflake's `TRY_TO_TIMESTAMP`
format language, Databricks' Java format patterns, and vendor cast quirks are
unreachable from here and must be covered by string assertions in
`tests/unit/test_sql_compiler.py`.

DuckDB pushdown refuses `max_levenshtein_distance`, because its `levenshtein`
counts UTF-8 bytes. The edit-distance parity cases restore the function and
use ASCII text, on which bytes and characters agree, so DuckDB still checks
the SQL the warehouses that count characters run.

Setting `VERIDELTA_PARITY_BACKEND=postgres` runs the same cases against a live
Postgres through `postgres_harness` instead: the comparison runs inside
Postgres, and the local engine reads the same tables back.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from tests.integration import postgres_harness
from veridelta.connectors.duckdb import DuckDBPushdownSession
from veridelta.engine import DiffEngine, _collect_pushdown_summary, _collect_value_map_proposals
from veridelta.models import DuckDBConfig

if TYPE_CHECKING:
    from collections.abc import Iterator

    import polars as pl

    from veridelta.connectors.base import PushdownQueryType
    from veridelta.models import DiffConfig, DiffResult, DiffSummary, ValueMapProposal

SOURCE_TABLE = "src_data"
"""Table the harness writes the source frame to."""

TARGET_TABLE = "tgt_data"
"""Table the harness writes the target frame to."""

PARITY_BACKEND = os.environ.get("VERIDELTA_PARITY_BACKEND", "duckdb")
"""Database the pushdown side runs in: `duckdb`, or `postgres` for a live server."""

if PARITY_BACKEND not in {"duckdb", "postgres"}:
    raise RuntimeError(
        f"VERIDELTA_PARITY_BACKEND must be duckdb or postgres, not {PARITY_BACKEND!r}."
    )

REFUSED_RULES = (
    postgres_harness.REFUSED_RULES
    if PARITY_BACKEND == "postgres"
    else frozenset({"max_levenshtein_distance"})
)
"""Rule fields the selected backend's pushdown refuses before running any query."""


class _RecordingSession(DuckDBPushdownSession):
    """The shipped DuckDB session, keeping each statement for failure messages."""

    def __init__(self, config: DuckDBConfig) -> None:
        super().__init__(config)
        self.statements: list[str] = []

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        self.statements.append(statement)
        return super().execute_pushdown(statement, query_type)


@contextmanager
def _duckdb_session(source: pl.DataFrame, target: pl.DataFrame) -> Iterator[_RecordingSession]:
    """Write both frames into a temporary DuckDB file and open the shipped session on it."""
    with tempfile.TemporaryDirectory() as folder:
        database = str(Path(folder, "parity.duckdb"))
        # The writer closes first, since the session opens the file read-only.
        with duckdb.connect(database) as writer:
            writer.register("source_frame", source.to_arrow())
            writer.register("target_frame", target.to_arrow())
            writer.execute(f"CREATE TABLE {SOURCE_TABLE} AS SELECT * FROM source_frame")
            writer.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM target_frame")
        session = _RecordingSession(
            DuckDBConfig(database=database, table=SOURCE_TABLE, pushdown=True)
        )
        session.connect()
        try:
            yield session
        finally:
            # Windows cannot delete a file a connection still holds.
            session.close()


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
    with _duckdb_session(source, target) as session:
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
    with _duckdb_session(source, target) as session:
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
