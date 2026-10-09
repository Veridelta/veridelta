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

`VERIDELTA_PARITY_BACKEND` picks where the pushdown side runs, from `BACKENDS`.
`postgres` runs the same cases against a live Postgres through
`postgres_harness`, and `bigquery`, `databricks`, `motherduck`, and
`snowflake` against a live warehouse through `warehouse_harness`. In each,
the comparison runs inside the database, and the local engine reads the same
tables back. A case one backend refuses or cannot store carries a `skip_on`
marker, and one that pins a single backend's behavior an `only_on` marker.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Final, NamedTuple, Protocol

import duckdb

from tests.integration import postgres_harness, warehouse_harness
from veridelta._pushdown import collect_pushdown_summary
from veridelta.connectors.duckdb import DuckDBPushdownSession
from veridelta.engine import DiffEngine, _collect_value_map_proposals
from veridelta.models import DuckDBConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    import polars as pl

    from veridelta.connectors.base import PushdownQueryType
    from veridelta.models import DiffConfig, DiffResult, DiffSummary, ValueMapProposal

SOURCE_TABLE = "src_data"
"""Table the harness writes the source frame to."""

TARGET_TABLE = "tgt_data"
"""Table the harness writes the target frame to."""

PARITY_BACKEND = os.environ.get("VERIDELTA_PARITY_BACKEND", "duckdb")
"""Database the pushdown side runs in, a key of `BACKENDS`."""


class _ValueMapRun(Protocol):
    """Proposes value maps inside a database."""

    def __call__(
        self,
        config: DiffConfig,
        source: pl.DataFrame,
        target: pl.DataFrame,
        *,
        min_confidence: float,
        min_support: int,
        sample_fraction: float,
    ) -> tuple[list[ValueMapProposal], list[str]]:
        """Return the proposals and the SQL that produced them."""


class Backend(NamedTuple):
    """The functions that run the parity cases in one database."""

    refused_rules: frozenset[str]
    """Rule fields its pushdown refuses before running any query."""

    run_pushdown: Callable[[DiffConfig, pl.DataFrame, pl.DataFrame], tuple[DiffResult, list[str]]]
    """Compares two frames inside the database, returning the result and its SQL."""

    run_value_map_pushdown: _ValueMapRun
    """Proposes value maps from two frames inside the database."""

    run_local: Callable[[DiffConfig, pl.DataFrame, pl.DataFrame], DiffResult]
    """Compares two frames through the local engine, as the database stores them."""


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


def _duckdb_pushdown(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
) -> tuple[DiffResult, list[str]]:
    """Compare the frames inside a temporary DuckDB file."""
    with _duckdb_session(source, target) as session:
        result = collect_pushdown_summary(session, SOURCE_TABLE, TARGET_TABLE, config)
        return result, list(session.statements)


def _duckdb_value_map_pushdown(
    config: DiffConfig,
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    min_confidence: float,
    min_support: int,
    sample_fraction: float,
) -> tuple[list[ValueMapProposal], list[str]]:
    """Propose value maps from the frames inside a temporary DuckDB file."""
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


def _duckdb_local(config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame) -> DiffResult:
    """Compare the frames through the local engine, as written."""
    return DiffEngine(config, source.lazy(), target.lazy()).run()


def _warehouse(name: str) -> Callable[[], Backend]:
    """Return a factory for a live warehouse's backend, which reads its settings."""

    def build() -> Backend:
        live = warehouse_harness.backend(name)
        return Backend(
            live.refused_rules, live.run_pushdown, live.run_value_map_pushdown, live.run_local
        )

    return build


BACKENDS: Final[dict[str, Callable[[], Backend]]] = {
    "duckdb": lambda: Backend(
        frozenset({"max_levenshtein_distance"}),
        _duckdb_pushdown,
        _duckdb_value_map_pushdown,
        _duckdb_local,
    ),
    "postgres": lambda: Backend(
        postgres_harness.REFUSED_RULES,
        postgres_harness.run_pushdown,
        postgres_harness.run_value_map_pushdown,
        postgres_harness.run_local,
    ),
    **{name: _warehouse(name) for name in warehouse_harness.SERVICES},
}
"""Each database the pushdown side can run in. Only the selected one is built."""

if PARITY_BACKEND not in BACKENDS:
    raise RuntimeError(
        f"VERIDELTA_PARITY_BACKEND must be one of {', '.join(sorted(BACKENDS))}, "
        f"not {PARITY_BACKEND!r}."
    )

_BACKEND = BACKENDS[PARITY_BACKEND]()

REFUSED_RULES = _BACKEND.refused_rules
"""Rule fields the selected backend's pushdown refuses before running any query."""


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
    return _BACKEND.run_pushdown(config, source, target)


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
    return _BACKEND.run_value_map_pushdown(
        config,
        source,
        target,
        min_confidence=min_confidence,
        min_support=min_support,
        sample_fraction=sample_fraction,
    )


def run_local(config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame) -> DiffResult:
    """Execute the same comparison through the local Polars engine.

    Against a live database, the frames are loaded and read back first, so the
    local engine sees the tables the pushdown side compared.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        DiffResult: The local engine's verdict and discrepancy rows.
    """
    return _BACKEND.run_local(config, source, target)


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
