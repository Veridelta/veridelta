# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run the parity harness against a live SQL Server instead of DuckDB.

Selected by `VERIDELTA_PARITY_BACKEND=sqlserver`, with `VERIDELTA_MSSQL_URI`
naming a server the tests may create and drop tables on; `make sqlserver` sets
the first. Each run loads its two frames into freshly named tables, compares
them, and drops them, so runs never share rows.

Pushdown runs through `TSqlPushdownSession`, the session a `pushdown: true`
pair on an `mssql://` connection opens, so every compiled statement executes
inside SQL Server. The local run reads the same tables back through
`DatabaseConnector`, as a `pushdown: false` pair does. Text columns use a
collation that ignores case and accents, and `=` ignores trailing spaces under
every collation, so these runs check that the T-SQL dialect compares text as
Polars does.

Rows go in as multi-row `INSERT` statements this module writes itself. The
driver's own parameters would put text into a literal without guarding a
backslash before a line break, which SQL Server drops as a line continuation.

Each dtype is stored as the nearest SQL Server type: text as `NVARCHAR(MAX)`
under `TEXT_COLLATION`, and `UInt64` as `DECIMAL(20, 0)`. SQL Server's `FLOAT` holds no NaN or
infinity, so a frame with one raises `NotImplementedError`, as do a dtype with
no counterpart and a column name past 128 characters. The tests that need one
carry a `skip_on("sqlserver")` marker.
"""

from __future__ import annotations

import math
import os
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

import polars as pl

from veridelta._pushdown import collect_pushdown_summary
from veridelta._value_maps import collect_value_map_proposals
from veridelta.connectors.database import TSqlPushdownSession
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Iterator

    from veridelta.connectors.base import PushdownQueryType
    from veridelta.models import Baseline, DiffConfig, DiffResult, ValueMapProposal

URI_VARIABLE = "VERIDELTA_MSSQL_URI"
"""Environment variable naming the server the SQL Server backend loads tables into."""

REFUSED_RULES = frozenset({"datetime_format", "max_levenshtein_distance", "regex_replace"})
"""Rule fields SQL Server pushdown refuses with a `ConfigError` before any query."""

TEXT_COLLATION = "Latin1_General_100_CI_AI"
"""Collation of every text column: it ignores case and accents, so pushdown must not."""

_IDENTIFIER_CHARACTERS = 128
"""Longest identifier SQL Server accepts."""

_ROWS_PER_INSERT = 1000
"""Most rows one `INSERT ... VALUES` statement may hold."""

_LINE_CONTINUATION = re.compile(r"\\(?=[\r\n])")
"""A backslash before a line break, which SQL Server drops along with the break."""

_COLUMN_TYPES: dict[type[pl.DataType], str] = {
    pl.Int8: "SMALLINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INT",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INT",
    pl.UInt32: "BIGINT",
    pl.UInt64: "DECIMAL(20, 0)",
    pl.Float32: "REAL",
    pl.Float64: "FLOAT",
    pl.String: f"NVARCHAR(MAX) COLLATE {TEXT_COLLATION}",
    pl.Boolean: "BIT",
    pl.Date: "DATE",
}
"""SQL Server column type for each dtype that maps without a parameter."""


class RecordingSqlServerSession(TSqlPushdownSession):
    """The session a `pushdown: true` pair opens, keeping every statement it runs."""

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session with an empty statement log.

        Args:
            config (DatabaseConfig): A SQL Server table that sets `pushdown`.
        """
        super().__init__(config)
        self.statements: list[str] = []

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run a compiled statement, recording it and naming it in any failure.

        Args:
            statement (str): SQL from the T-SQL compiler.
            query_type (PushdownQueryType): Which round-trip this is.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If SQL Server rejects the statement, with its SQL appended.
        """
        self.statements.append(statement)
        try:
            return super().execute_pushdown(statement, query_type)
        except ConnectorError as exc:
            raise ConnectorError(f"{exc}\n{statement}") from exc


def run_pushdown(
    config: DiffConfig,
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    baseline: Baseline | None = None,
) -> tuple[DiffResult, list[str]]:
    """Load the frames into SQL Server and compare them there.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.
        baseline (Baseline | None): Drift the run accepts.

    Returns:
        tuple[DiffResult, list[str]]: The pushdown result and the SQL that
            produced it, in execution order.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        session = RecordingSqlServerSession(database_source(source_table, pushdown=True))
        with session:
            session.connect()
            result = collect_pushdown_summary(
                session, source_table, target_table, config, baseline=baseline
            )
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
    """Load the frames into SQL Server and propose value maps there.

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
        session = RecordingSqlServerSession(database_source(source_table, pushdown=True))
        with session:
            session.connect()
            proposals = collect_value_map_proposals(
                session,
                source_table,
                target_table,
                config,
                min_confidence=min_confidence,
                min_support=min_support,
                sample_fraction=sample_fraction,
            )
        return proposals, session.statements


def run_local(
    config: DiffConfig,
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    baseline: Baseline | None = None,
) -> DiffResult:
    """Load the frames into SQL Server, read them back, and compare them locally.

    Args:
        config (DiffConfig): Comparison rules and keys.
        source (pl.DataFrame): Source rows.
        target (pl.DataFrame): Target rows.
        baseline (Baseline | None): Drift the run accepts.

    Returns:
        DiffResult: The local engine's verdict on the tables as SQL Server holds them.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        return DiffEngine.run_from_configs(
            config,
            database_source(source_table),
            database_source(target_table),
            baseline=baseline,
        )


def database_source(table: str, *, pushdown: bool = False) -> DatabaseConfig:
    """Describe one loaded table as a database source.

    Args:
        table (str): Name of the loaded table.
        pushdown (bool): Whether the source opts into comparing inside SQL Server.

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
    # Only this backend needs the driver, so other jobs never install it.
    import pymssql

    parts = urlsplit(_uri())
    stem = f"vd_{uuid.uuid4().hex}"
    names = (f"{stem}_source", f"{stem}_target")
    connection = pymssql.connect(
        server=parts.hostname or "",
        port=str(parts.port or 1433),
        user=unquote(parts.username or ""),
        password=unquote(parts.password or ""),
        database=parts.path.lstrip("/"),
        autocommit=True,
    )
    try:
        cursor = connection.cursor()
        try:
            _load(cursor, names[0], source)
            _load(cursor, names[1], target)
            yield names
        finally:
            for name in names:
                cursor.execute(f"DROP TABLE IF EXISTS {_quoted(name)}")
    finally:
        connection.close()


def _uri() -> str:
    """Return the server URI the backend loads tables into.

    Returns:
        str: The value of `VERIDELTA_MSSQL_URI`.

    Raises:
        RuntimeError: If the variable is unset or empty.
    """
    uri = os.environ.get(URI_VARIABLE)
    if not uri:
        raise RuntimeError(
            f"Set {URI_VARIABLE} to a SQL Server the parity suite may create tables on."
        )
    return uri


def _load(cursor: Any, table: str, frame: pl.DataFrame) -> None:
    """Create a table shaped like the frame and insert its rows.

    Args:
        cursor (Any): Cursor on an open autocommit connection.
        table (str): Name of the table to create.
        frame (pl.DataFrame): Rows to load.
    """
    columns = ", ".join(
        f"{_quoted(_column_name(name))} {_column_type(name, dtype)}"
        for name, dtype in frame.schema.items()
    )
    cursor.execute(f"CREATE TABLE {_quoted(table)} ({columns})")
    rows = [f"({', '.join(_literal(value) for value in row)})" for row in frame.iter_rows()]
    for start in range(0, len(rows), _ROWS_PER_INSERT):
        values = ", ".join(rows[start : start + _ROWS_PER_INSERT])
        cursor.execute(f"INSERT INTO {_quoted(table)} VALUES {values}")


def _quoted(name: str) -> str:
    """Bracket a name for SQL Server, doubling any `]` in it."""
    return "[" + name.replace("]", "]]") + "]"


def _literal(value: object) -> str:
    """Write one cell as a SQL Server literal the column's type reads exactly.

    Args:
        value (object): A value from `DataFrame.iter_rows`.

    Returns:
        str: The literal.

    Raises:
        NotImplementedError: If the value is a NaN or an infinity, or of a
            type the loader does not write.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NotImplementedError(f"SQL Server's FLOAT holds no {value}.")
        return repr(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, str):
        # Closing the literal after the backslash keeps the line break apart from it.
        text = _LINE_CONTINUATION.sub("\\\\' + N'", value.replace("'", "''"))
        return f"N'{text}'"
    if isinstance(value, (datetime, date)):
        return f"'{value.isoformat()}'"
    raise NotImplementedError(f"The SQL Server loader does not write {type(value).__name__}.")


def _column_name(name: str) -> str:
    """Return a column name SQL Server accepts.

    Args:
        name (str): Frame column name.

    Returns:
        str: The name, unchanged.

    Raises:
        NotImplementedError: If SQL Server would refuse it as too long.
    """
    if len(name) > _IDENTIFIER_CHARACTERS:
        raise NotImplementedError(
            f"SQL Server identifiers hold {_IDENTIFIER_CHARACTERS} characters; {name!r} is longer."
        )
    return name


def _column_type(name: str, dtype: pl.DataType) -> str:
    """Return the SQL Server type that stores a column without loss.

    Args:
        name (str): Frame column name, for the error.
        dtype (pl.DataType): The column's dtype.

    Returns:
        str: The column type.

    Raises:
        NotImplementedError: If no SQL Server type holds the dtype's values exactly.
    """
    if isinstance(dtype, pl.Decimal):
        return f"DECIMAL({dtype.precision or 38}, {dtype.scale})"
    if isinstance(dtype, pl.Datetime) and dtype.time_unit != "ns":
        return "DATETIME2(6)" if dtype.time_zone is None else "DATETIMEOFFSET(6)"
    column_type = _COLUMN_TYPES.get(type(dtype))
    if column_type is None:
        raise NotImplementedError(f"SQL Server has no column type for {name!r}, a {dtype} column.")
    return column_type
