# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run the parity harness against a live Postgres instead of DuckDB.

Selected by `VERIDELTA_PARITY_BACKEND=postgres`, with `VERIDELTA_POSTGRES_URI`
naming a server the tests may create and drop tables on; `make postgres` sets
the first. Each run loads its two frames into freshly named tables with
psycopg's `COPY`, compares them, and drops them, so runs never share rows.

Pushdown runs through `PostgresPushdownSession`, the session a `pushdown: true`
pair opens, so every compiled statement executes inside Postgres. The local run
reads the same tables back through `DatabaseConnector`, as a `pushdown: false`
pair does. The suite therefore compares the two settings a user chooses between
on one database, rather than Polars types against Postgres ones.

Each dtype is stored as the nearest Postgres type. Postgres has no one-byte or
unsigned integers, so `Int8` and `UInt8` become `smallint`, `UInt16` becomes
`integer`, `UInt32` becomes `bigint`, and `UInt64` becomes `numeric(20, 0)`.
ConnectorX reads every `numeric` back as `Decimal(38, 10)`. A dtype with no
counterpart, or a column name past Postgres' 63-byte identifier limit, raises
`NotImplementedError`; the tests that need one carry the `duckdb_only` marker.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import polars as pl
import psycopg
from psycopg import sql

from veridelta.connectors.database import PostgresPushdownSession
from veridelta.engine import DiffEngine, _collect_pushdown_summary, _collect_value_map_proposals
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Iterator

    from veridelta.connectors.base import PushdownQueryType
    from veridelta.models import DiffConfig, DiffResult, ValueMapProposal

URI_VARIABLE = "VERIDELTA_POSTGRES_URI"
"""Environment variable naming the server the Postgres backend loads tables into."""

REFUSED_RULES = frozenset({"datetime_format", "max_levenshtein_distance"})
"""Rule fields Postgres pushdown refuses with a `ConfigError` before any query."""

NUMERIC_DTYPES: frozenset[type[pl.DataType]] = frozenset({pl.UInt64, pl.Decimal})
"""Dtypes stored as `numeric`, which a local run reads back as `Decimal(38, 10)`.

A local run therefore writes them as text with ten decimal places, where
Postgres writes the stored scale, so the two settings disagree on `pad_zeros`.
`test_postgres_pushdown` pins that difference.
"""

_IDENTIFIER_BYTES = 63
"""Longest identifier Postgres keeps; it silently truncates anything longer."""

_COLUMN_TYPES: dict[type[pl.DataType], sql.SQL] = {
    pl.Int8: sql.SQL("smallint"),
    pl.Int16: sql.SQL("smallint"),
    pl.Int32: sql.SQL("integer"),
    pl.Int64: sql.SQL("bigint"),
    pl.UInt8: sql.SQL("smallint"),
    pl.UInt16: sql.SQL("integer"),
    pl.UInt32: sql.SQL("bigint"),
    pl.UInt64: sql.SQL("numeric(20, 0)"),
    pl.Float32: sql.SQL("real"),
    pl.Float64: sql.SQL("double precision"),
    pl.String: sql.SQL("text"),
    pl.Boolean: sql.SQL("boolean"),
    pl.Date: sql.SQL("date"),
}
"""Postgres column type for each dtype that maps without a parameter."""


class RecordingPostgresSession(PostgresPushdownSession):
    """The session a `pushdown: true` pair opens, keeping every statement it runs."""

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session with an empty statement log.

        Args:
            config (DatabaseConfig): A Postgres table that sets `pushdown`.
        """
        super().__init__(config)
        self.statements: list[str] = []

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run a compiled statement, recording it and naming it in any failure.

        Args:
            statement (str): SQL from the Postgres compiler.
            query_type (PushdownQueryType): Which round-trip this is.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If Postgres rejects the statement, with its SQL appended.
        """
        self.statements.append(statement)
        try:
            return super().execute_pushdown(statement, query_type)
        except ConnectorError as exc:
            raise ConnectorError(f"{exc}\n{statement}") from exc


def run_pushdown(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
) -> tuple[DiffResult, list[str]]:
    """Load the frames into Postgres and compare them there.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        tuple[DiffResult, list[str]]: The pushdown result and the SQL that
            produced it, in execution order.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        session = RecordingPostgresSession(database_source(source_table, pushdown=True))
        with session:
            session.connect()
            result = _collect_pushdown_summary(session, source_table, target_table, config)
        return result, session.statements


def run_value_map_pushdown(
    config: DiffConfig,
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    min_confidence: float,
    min_support: int,
    sample_fraction: float,
) -> tuple[list[ValueMapProposal], list[str]]:
    """Load the frames into Postgres and propose value maps there.

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
    with loaded_tables(source, target) as (source_table, target_table):
        session = RecordingPostgresSession(database_source(source_table, pushdown=True))
        with session:
            session.connect()
            proposals = _collect_value_map_proposals(
                session,
                source_table,
                target_table,
                config,
                min_confidence=min_confidence,
                min_support=min_support,
                sample_fraction=sample_fraction,
            )
        return proposals, session.statements


def run_local(config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame) -> DiffResult:
    """Load the frames into Postgres, read them back, and compare them locally.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.

    Returns:
        DiffResult: The local engine's verdict on the tables as Postgres holds them.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        return DiffEngine.run_from_configs(
            config, database_source(source_table), database_source(target_table)
        )


def database_source(table: str, *, pushdown: bool = False) -> DatabaseConfig:
    """Describe one loaded table as a database source.

    Args:
        table (str): Name of the loaded table.
        pushdown (bool): Whether the source opts into comparing inside Postgres.

    Returns:
        DatabaseConfig: The source a configuration file would declare for it.
    """
    return DatabaseConfig(uri=_uri(), table=table, pushdown=pushdown)


@contextmanager
def loaded_tables(source: pl.DataFrame, target: pl.DataFrame) -> Iterator[tuple[str, str]]:
    """Load both frames into fresh tables and drop them afterwards.

    Args:
        source (pl.DataFrame): Rows for the source table.
        target (pl.DataFrame): Rows for the target table.

    Yields:
        tuple[str, str]: The source and target table names.
    """
    stem = f"vd_{uuid.uuid4().hex}"
    names = (f"{stem}_source", f"{stem}_target")
    with psycopg.connect(_uri(), autocommit=True) as connection:
        try:
            _load(connection, names[0], source)
            _load(connection, names[1], target)
            yield names
        finally:
            for name in names:
                connection.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))


def _uri() -> str:
    """Return the server URI the backend loads tables into.

    Returns:
        str: The value of `VERIDELTA_POSTGRES_URI`.

    Raises:
        RuntimeError: If the variable is unset or empty.
    """
    uri = os.environ.get(URI_VARIABLE)
    if not uri:
        raise RuntimeError(
            f"Set {URI_VARIABLE} to a Postgres server the parity suite may create tables on."
        )
    return uri


def _load(connection: psycopg.Connection[Any], table: str, frame: pl.DataFrame) -> None:
    """Create a table shaped like the frame and copy its rows in.

    Args:
        connection (psycopg.Connection[Any]): Open autocommit connection.
        table (str): Name of the table to create.
        frame (pl.DataFrame): Rows to load.
    """
    columns = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(_column_name(name)), _column_type(name, dtype))
        for name, dtype in frame.schema.items()
    )
    connection.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table), columns))
    statement = sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(table))
    with connection.cursor() as cursor, cursor.copy(statement) as copy:
        for row in frame.iter_rows():
            copy.write_row(row)


def _column_name(name: str) -> str:
    """Return a column name Postgres keeps whole.

    Args:
        name (str): Frame column name.

    Returns:
        str: The name, unchanged.

    Raises:
        NotImplementedError: If Postgres would truncate it.
    """
    if len(name.encode()) > _IDENTIFIER_BYTES:
        raise NotImplementedError(
            f"Postgres truncates identifiers to {_IDENTIFIER_BYTES} bytes; {name!r} is longer."
        )
    return name


def _column_type(name: str, dtype: pl.DataType) -> sql.Composable:
    """Return the Postgres type that stores a column without loss.

    Args:
        name (str): Frame column name, for the error.
        dtype (pl.DataType): The column's dtype.

    Returns:
        sql.Composable: The column type.

    Raises:
        NotImplementedError: If no Postgres type holds the dtype's values exactly.
    """
    if isinstance(dtype, pl.Decimal):
        return sql.SQL("numeric({}, {})").format(
            sql.Literal(dtype.precision or 38), sql.Literal(dtype.scale)
        )
    if isinstance(dtype, pl.Datetime) and dtype.time_unit != "ns":
        return sql.SQL("timestamp" if dtype.time_zone is None else "timestamptz")
    column_type = _COLUMN_TYPES.get(type(dtype))
    if column_type is None:
        raise NotImplementedError(f"Postgres has no column type for {name!r}, a {dtype} column.")
    return column_type
