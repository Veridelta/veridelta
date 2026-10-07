# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Database sources read through ConnectorX into Polars for a local comparison.

A database source is compared by the local engine, so it pairs with files,
lakehouse tables, or another database. `connect()` reads the configured `table`
or `query` exactly once and holds the result: Polars has no lazy SQL reader, and
wrapping the read in `pl.defer` would run the query again every time the engine
asks for a schema. Requires the `database` extra (`uv add 'veridelta[database]'`).

Two Postgres tables that opt into `pushdown` are compared inside the database
instead: `PostgresPushdownSession` runs each compiled statement through the same
driver and reads back only counts and keys.

A password never reaches a log line or an error. Log lines carry the URI with
its password masked and the table name, never SQL text. A failed read is
reported with every form of the password replaced, and raised without the
driver's exception attached, because Polars re-raises the unscrubbed driver
error as the cause.
"""

import logging
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import polars as pl

from veridelta.connectors.base import (
    PushdownQueryType,
    VerideltaConnector,
    mask_secrets,
    optional_module,
    read_subject,
)
from veridelta.connectors.sql import (
    SQLDialect,
    SQLPushdownCompiler,
    compile_database_partition_counts,
    compile_database_partition_range,
    compile_database_probe,
    compile_database_select,
    compile_mssql_utc_select,
    compile_postgres_columns_query,
    compile_postgres_text_select,
)
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DatabaseConfig

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# The tests patch each module attribute below, so no test depends on the
# extras installed where it runs.
connectorx: Any = optional_module("connectorx")

_DATABASE_EXTRA = "Database extra is not installed. Install it with: uv add 'veridelta[database]'"
_UNCONNECTED = "Database connector is not connected. Call connect() first."
_SQLITE_PREFIX = "sqlite://"
_POSTGRES_UNCONNECTED = "Postgres pushdown session is not connected. Call connect() first."
_LITERAL_RULES = "SELECT current_setting('standard_conforming_strings') AS value"

_POSTGRES_SCHEMES: Final = frozenset({"postgres", "postgresql"})
"""URI schemes whose `table` reads keep each `numeric` column's declared scale."""

_MSSQL_SCHEME: Final = "mssql"
"""URI scheme whose `table` reads move each `DATETIMEOFFSET` column to offset zero."""

_MAX_DECIMAL_PRECISION: Final = 38
"""Widest decimal Polars holds. A wider `numeric` keeps ConnectorX's default read."""


class DatabaseConnector(VerideltaConnector):
    """Read one database table or query into Polars through ConnectorX.

    `connect()` reads eagerly and keeps the frame, and `lazyframe()` hands it to
    the local engine as a LazyFrame over those rows. The read is the one place
    the rows are fetched, so it happens once per `connect()`. With
    `partition_on` set, ConnectorX splits that read into ranges over parallel
    connections, after Veridelta confirms the column holds no NULL and reads
    its lowest and highest values.
    `execute_pushdown` always raises: the comparison runs in Polars, never in
    the database.
    """

    def __init__(self, config: DatabaseConfig, *, probe: bool = False) -> None:
        """Initialize the connector with validated database settings.

        Args:
            config (DatabaseConfig): Frozen URI, credentials, and table or query.
            probe (bool): Whether to read the table's columns and no rows, for a
                schema check. Only a `table` can be probed.
        """
        self._config = config
        self._probe = probe
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Read the configured table or query into memory.

        Raises:
            ConnectorError: If the `database` extra is missing, a SQLite file
                does not exist, the partition column holds a NULL or anything
                but integers, or the read fails.
            ConfigError: If `table` names a database Veridelta cannot quote for,
                or a probe was asked of a `query`.
        """
        if connectorx is None:
            raise ConnectorError(_DATABASE_EXTRA)
        scheme = urlsplit(self._config.uri).scheme.lower()
        statement = self._statement(scheme)
        uri = _connection_uri(self._config)
        if scheme == "sqlite":
            uri = _existing_sqlite_uri(uri)

        started = time.perf_counter()
        declared: dict[str, pl.Decimal] = {}
        try:
            if scheme in _POSTGRES_SCHEMES and self._config.table is not None:
                statement, declared = self._declared_statement(self._config.table, statement, uri)
            elif scheme == _MSSQL_SCHEME and self._config.table is not None and not self._probe:
                statement = self._utc_statement(self._config.table, statement, uri)
            frame = self._read(scheme, statement, uri)
        except ConnectorError:
            raise
        except ImportError:
            # A missing pyarrow surfaces here, and it is the same missing extra.
            raise ConnectorError(_DATABASE_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "Database read of %s from %s failed after %.3fs",
                self._subject,
                self._config.redacted_uri,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"Database read of {self._subject} from '{self._config.redacted_uri}' "
                f"failed: {_scrub(self._config, str(exc))}"
            ) from None
        logger.info(
            "Read %d rows of %s from %s in %.3fs",
            frame.height,
            self._subject,
            self._config.redacted_uri,
            time.perf_counter() - started,
        )
        self._frame = _with_declared_scale(frame, declared, self._subject).lazy()

    def lazyframe(self) -> pl.LazyFrame:
        """Return the rows `connect()` read, as a LazyFrame for the local engine.

        Returns:
            pl.LazyFrame: Lazy wrapper over the materialized rows.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        if self._frame is None:
            raise ConnectorError(_UNCONNECTED)
        return self._frame

    def close(self) -> None:
        """Drop the rows read. Idempotent; `connect()` reads them again."""
        self._frame = None

    def _statement(self, scheme: str) -> str:
        """Return the SQL to read: the compiled table select, or the query as written."""
        if self._config.table is not None and self._probe:
            return compile_database_probe(scheme, self._config.table)
        if self._config.table is not None:
            return compile_database_select(scheme, self._config.table)
        if self._probe:
            # Wrapping a statement is not portable: Oracle refuses `AS` on a
            # derived table, and SQL Server refuses `ORDER BY` inside one.
            raise ConfigError(
                "A schema probe reads a 'table'; a 'query' would have to run in full."
            )
        # DatabaseConfig requires exactly one of `table` and `query`.
        return cast("str", self._config.query)

    def _read(self, scheme: str, statement: str, uri: str) -> pl.DataFrame:
        """Run the read, split into ranges over parallel connections when configured."""
        column = self._config.partition_on
        if column is None or self._probe:
            return pl.read_database_uri(statement, uri)
        # The model sets `table` and `partitions` whenever `partition_on` is set.
        table, partitions = cast("str", self._config.table), cast("int", self._config.partitions)
        counts = compile_database_partition_counts(scheme, table, column)
        nulls, valued = pl.read_database_uri(counts, uri).row(0)
        if nulls:
            # ConnectorX reads only rows inside its ranges, and a NULL is in none of them.
            raise ConnectorError(
                f"Column '{column}' of {self._subject} is NULL in {nulls:,} of its rows, "
                "which a partitioned read would leave out. Partition on a column without "
                "NULLs, or remove 'partition_on' and 'partitions'."
            )
        if not valued:
            # An empty table has no range to split, and one read keeps its columns.
            return pl.read_database_uri(statement, uri)
        bounds = compile_database_partition_range(scheme, table, column)
        low, high = pl.read_database_uri(bounds, uri).row(0)
        if not (isinstance(low, int) and isinstance(high, int)):
            raise ConnectorError(
                f"Column '{column}' of {self._subject} does not hold integers, so the read "
                "cannot be split on it. Partition on an integer column."
            )
        # Passing the range skips ConnectorX's own lookup, whose SQLite version opens the
        # path without percent-decoding it: every path on Windows, or one with a space.
        return pl.read_database_uri(
            statement,
            uri,
            partition_on=column,
            partition_num=partitions,
            partition_range=(low, high),
        )

    def _declared_statement(
        self, table: str, statement: str, uri: str
    ) -> tuple[str, dict[str, pl.Decimal]]:
        """Look up a Postgres table's `numeric` columns and read them as text, to keep scale."""
        catalog = pl.read_database_uri(compile_postgres_columns_query(table), uri)
        declared = _declared_decimals(catalog)
        if not declared:
            return statement, declared
        columns = catalog.get_column("attname").to_list()
        return compile_postgres_text_select(table, columns, declared, probe=self._probe), declared

    def _utc_statement(self, table: str, statement: str, uri: str) -> str:
        """Find a SQL Server table's `DATETIMEOFFSET` columns, and read them at offset zero.

        ConnectorX shifts a `DATETIMEOFFSET` value by its offset a second time.
        A read of the table's columns and no rows finds them, as the only
        columns ConnectorX reads as a time in UTC. The read itself would type
        them the same way, which a catalog query could only approximate.
        """
        columns = pl.read_database_uri(compile_database_probe(_MSSQL_SCHEME, table), uri).schema
        in_utc = [
            name
            for name, dtype in columns.items()
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
        ]
        if not in_utc:
            return statement
        bracketed = [name for name in columns if "]" in name]
        if bracketed and self._config.partition_on is not None:
            # ConnectorX parses a partitioned read to split it. Its parser reads a
            # doubled `]` as one and writes it back undoubled, which SQL Server refuses.
            raise ConnectorError(
                f"Column '{bracketed[0]}' of {self._subject} has ']' in its name, which a "
                "partitioned read cannot pass through ConnectorX. Remove 'partition_on' and "
                "'partitions' to read the table in one piece."
            )
        return compile_mssql_utc_select(table, list(columns), in_utc)

    @property
    def _subject(self) -> str:
        """Name what is read without repeating any SQL."""
        return read_subject(self._config.table)


class PostgresPushdownSession(VerideltaConnector):
    """Run compiled comparison SQL inside Postgres, through ConnectorX.

    Opened for two database sources on one Postgres connection that both set
    `pushdown`. Each statement is one ConnectorX read, which opens its own
    connection and returns Arrow, so nothing is held between statements and
    `close()` only forgets the connection. Only counts and keys come back.

    `connect()` checks that the server reads string literals by the SQL
    standard, as the Postgres dialect writes them: with
    `standard_conforming_strings` off, a backslash would escape the closing
    quote of a value that ends in one.
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DatabaseConfig): A Postgres table that sets `pushdown`.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.POSTGRES)
        self._uri: str | None = None

    def connect(self) -> None:
        """Check the server and keep the connection URI for the statements to come.

        Raises:
            ConnectorError: If the `database` extra is missing, the server cannot
                be reached, or it reads backslashes in string literals as escapes.
        """
        if connectorx is None:
            raise ConnectorError(_DATABASE_EXTRA)
        uri = _connection_uri(self._config)
        setting = self._read(_LITERAL_RULES, uri, query_type="settings")
        if setting.item(0, 0) != "on":
            raise ConnectorError(
                f"standard_conforming_strings is off on Postgres at "
                f"'{self._config.redacted_uri}', so it would read a backslash in a string "
                "literal as an escape. Turn it on for the role or database, or compare "
                "locally (pushdown: false)."
            )
        self._uri = uri

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run one compiled statement and return its rows lazily.

        Args:
            statement (str): SQL from the Postgres compiler.
            query_type (PushdownQueryType): Which round-trip this is, for logs
                and errors.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If the session is not connected or the statement fails.
        """
        frame = self._read(statement, self._connected_uri(), query_type=query_type)
        return frame.lazy()

    def declared_types(self, table: str) -> dict[str, pl.Decimal]:
        """Return the declared precision and scale of a table's `numeric` columns.

        ConnectorX describes every `numeric` as `Decimal(38, 10)`, so a schema
        probe alone cannot tell `numeric(10, 2)` from `numeric(12, 4)`.

        Args:
            table (str): The table, as configured.

        Returns:
            dict[str, pl.Decimal]: Each `numeric` column Polars holds exactly,
                by name. A column without a declared precision, wider than 38
                digits, or with a negative scale is left out.

        Raises:
            ConnectorError: If the session is not connected or the query fails.
        """
        statement = compile_postgres_columns_query(table)
        return _declared_decimals(self._read(statement, self._connected_uri(), query_type="schema"))

    def close(self) -> None:
        """Forget the connection. Idempotent; `connect()` checks the server again."""
        self._uri = None

    def _connected_uri(self) -> str:
        """Return the URI `connect()` kept."""
        if self._uri is None:
            raise ConnectorError(_POSTGRES_UNCONNECTED)
        return self._uri

    def _read(self, statement: str, uri: str, *, query_type: str) -> pl.DataFrame:
        """Run a statement through ConnectorX, logging and reporting without secrets."""
        started = time.perf_counter()
        try:
            frame = pl.read_database_uri(statement, uri)
        except ImportError:
            raise ConnectorError(_DATABASE_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "Postgres %s statement on %s failed after %.3fs",
                query_type,
                self._config.redacted_uri,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"Postgres {query_type} statement on '{self._config.redacted_uri}' failed: "
                f"{_scrub(self._config, str(exc))}"
            ) from None
        logger.info(
            "Ran Postgres %s statement on %s in %.3fs, %d rows",
            query_type,
            self._config.redacted_uri,
            time.perf_counter() - started,
            frame.height,
        )
        return frame


def _declared_decimals(catalog: pl.DataFrame) -> dict[str, pl.Decimal]:
    """Map each `numeric` column Polars holds exactly to its declared precision and scale."""
    declared: dict[str, pl.Decimal] = {}
    for name, typmod, is_numeric in catalog.iter_rows():
        # A typmod packs (precision << 16 | scale) + 4. An unconstrained numeric's
        # -1 decodes to precision 65535, and a negative scale to one above 1000.
        precision, scale = ((typmod - 4) >> 16) & 0xFFFF, (typmod - 4) & 0xFFFF
        if is_numeric and scale <= precision <= _MAX_DECIMAL_PRECISION:
            declared[name] = pl.Decimal(precision, scale)
    return declared


def _with_declared_scale(
    frame: pl.DataFrame, declared: Mapping[str, pl.Decimal], subject: str
) -> pl.DataFrame:
    """Cast the `numeric` columns read as text back to their declared precision and scale."""
    for name, dtype in declared.items():
        try:
            frame = frame.with_columns(pl.col(name).cast(dtype, strict=True))
        except pl.exceptions.InvalidOperationError:
            raise ConnectorError(
                f"Column '{name}' of {subject} holds a value with no decimal form, such as "
                "NaN. Leave such values out with a 'query'."
            ) from None
    return frame


def _connection_uri(config: DatabaseConfig) -> str:
    """Return the URI to connect with, carrying the `password` field if set."""
    password = config.password
    if password is None:
        return config.uri
    parts = urlsplit(config.uri)
    # The model guarantees a user name and no password in the URI whenever `password` is set.
    user, _, host = parts.netloc.rpartition("@")
    netloc = f"{user}:{quote(password, safe='')}@{host}"
    return urlunsplit(parts._replace(netloc=netloc))


def _scrub(config: DatabaseConfig, text: str) -> str:
    """Replace every form of the password in driver output with `***`."""
    secrets: set[str] = set()
    if config.password:
        secrets.update({config.password, quote(config.password, safe="")})
    embedded = urlsplit(config.uri).password
    if embedded:
        secrets.update({embedded, unquote(embedded)})
    return mask_secrets(text, *secrets)


def _existing_sqlite_uri(uri: str) -> str:
    """Encode a SQLite URI's path for ConnectorX and require the file to exist."""
    # ConnectorX percent-decodes the text after `sqlite://`: decode it here, re-encode on return.
    path = unquote(uri[len(_SQLITE_PREFIX) :])
    # ConnectorX opens SQLite in create mode, so a missing file leaves an empty database behind.
    if not Path(path).is_file():
        raise ConnectorError(
            f"SQLite database '{path}' does not exist. Point 'uri' at an existing file, "
            "written as sqlite:// followed by its path."
        )
    return _SQLITE_PREFIX + quote(path)
