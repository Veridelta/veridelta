# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""DuckDB and MotherDuck sources read into Polars for a local comparison.

A DuckDB source is compared by the local engine, so it pairs with files,
lakehouse tables, databases, or another DuckDB source. `connect()` opens the
database, reads the configured `table` or `query` once through DuckDB's own
Polars export, and closes the connection. Requires the `duckdb` extra
(`uv add 'veridelta[duckdb]'`).

Two tables in one database that both opt into `pushdown` are compared inside
DuckDB instead: `DuckDBPushdownSession` runs each compiled statement on one
connection and reads back only counts and keys.

A file opens read-only, so the read can never change it. A MotherDuck database
opens read-write, because a read-only connection needs a read-scaling token.
Each session reads time in UTC, so a timestamp with a time zone, or a date cast
in a `query`, does not depend on the machine.

A token never reaches a log line or an error. It goes to DuckDB as a
connection setting, never in the connection string, and a failed read is
reported with the token replaced and without the driver's exception attached.
"""

import logging
import os
import time
from typing import Any, Final, cast

import polars as pl

from veridelta.connectors.base import (
    PushdownQueryType,
    PushdownSession,
    ReaderConnector,
    mask_secrets,
    optional_module,
    read_subject,
)
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler, compile_duckdb_select
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DuckDBConfig

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# The tests patch each module attribute below, so no test depends on the
# extras installed where it runs.
duckdb: Any = optional_module("duckdb")

_DUCKDB_EXTRA = "DuckDB extra is not installed. Install it with: uv add 'veridelta[duckdb]'"
_UNCONNECTED = "DuckDB connector is not connected. Call connect() first."
_SESSION_UNCONNECTED = "DuckDB pushdown session is not connected. Call connect() first."
_TOKEN_VARIABLES: Final = ("MOTHERDUCK_TOKEN", "motherduck_token")
"""Environment variables a MotherDuck token is read from, in order.

MotherDuck's own documentation spells the variable in lowercase.
"""

_UNREADABLE_TYPES: Final = frozenset({"interval", "union"})
"""DuckDB types Polars cannot import: it raises on some results and panics on others."""

_NESTED_TYPES: Final = frozenset({"struct", "list", "array", "map"})
"""DuckDB types whose members can hold an unreadable type."""


class DuckDBConnector(ReaderConnector):
    """Read one DuckDB or MotherDuck table or query into Polars.

    `connect()` reads eagerly and keeps the frame, and `lazyframe()` hands it to
    the local engine as a LazyFrame over those rows. The read is the one place
    the rows are fetched, so it happens once per `connect()`. The comparison
    runs in Polars, never in DuckDB.
    """

    def __init__(self, config: DuckDBConfig, *, probe: bool = False) -> None:
        """Initialize the connector with validated DuckDB settings.

        Args:
            config (DuckDBConfig): Frozen database, token, and table or query.
            probe (bool): Whether to read the table's columns and no rows, for a
                schema check. Only a `table` can be probed.
        """
        self._config = config
        self._probe = probe
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Read the configured table or query into memory.

        Raises:
            ConnectorError: If the `duckdb` extra is missing, a MotherDuck
                database has no token, a column has no Polars type, or the
                read fails.
            ConfigError: If a probe was asked of a `query`.
        """
        if duckdb is None:
            raise ConnectorError(_DUCKDB_EXTRA)
        statement = self._statement()
        token = _token(self._config)
        started = time.perf_counter()
        try:
            connection = _open(self._config, token)
            try:
                connection.execute("SET TimeZone = 'UTC'")
                frame = _read(connection, statement, self._subject)
            finally:
                connection.close()
        except ConnectorError:
            raise
        except ImportError:
            # A missing pyarrow surfaces here, and it is the same missing extra.
            raise ConnectorError(_DUCKDB_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "DuckDB read of %s from %s failed after %.3fs",
                self._subject,
                self._config.database,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"DuckDB read of {self._subject} from '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), token)}"
            ) from None
        logger.info(
            "Read %d rows of %s from %s in %.3fs",
            frame.height,
            self._subject,
            self._config.database,
            time.perf_counter() - started,
        )
        self._frame = frame.lazy()

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

    def _statement(self) -> str:
        """Return the SQL to read: the compiled table select, or the query as written."""
        if self._config.table is not None:
            return compile_duckdb_select(self._config.table, probe=self._probe)
        if self._probe:
            raise ConfigError(
                "A schema probe reads a 'table'; a 'query' would have to run in full."
            )
        # DuckDBConfig requires exactly one of `table` and `query`.
        return cast("str", self._config.query)

    @property
    def _subject(self) -> str:
        """Name what is read without repeating any SQL."""
        return read_subject(self._config.table)


class DuckDBPushdownSession(PushdownSession):
    """Run compiled comparison SQL inside DuckDB or MotherDuck.

    Opened for two tables in one database that both set `pushdown`. It holds
    one connection from `connect()` to `close()`, opened as a read opens it:
    a file read-only, MotherDuck read-write with its token, both in UTC. Only
    counts and keys come back, and a row sample when one is asked for.
    """

    def __init__(self, config: DuckDBConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DuckDBConfig): A DuckDB table that sets `pushdown`.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.DUCKDB)
        self._connection: Any = None
        self._token: str | None = None

    def connect(self) -> None:
        """Open the database for the statements to come.

        Raises:
            ConnectorError: If the `duckdb` extra is missing, a MotherDuck
                database has no token, or the database cannot be opened.
        """
        if duckdb is None:
            raise ConnectorError(_DUCKDB_EXTRA)
        token = _token(self._config)
        try:
            connection = _open(self._config, token)
            connection.execute("SET TimeZone = 'UTC'")
        except Exception as exc:
            logger.warning("DuckDB connection to %s failed", self._config.database)
            raise ConnectorError(
                f"DuckDB connection to '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), token)}"
            ) from None
        logger.info("Connected to DuckDB database %s", self._config.database)
        self._connection, self._token = connection, token

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run one compiled statement and return its rows lazily.

        Args:
            statement (str): SQL from the DuckDB compiler.
            query_type (PushdownQueryType): Which round-trip this is, for logs
                and errors.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If the session is not connected, a result column
                has no Polars type, or the statement fails.
        """
        frame = self._run(statement, query_type)
        return frame.lazy()

    def close(self) -> None:
        """Close the connection. Idempotent; `connect()` opens it again."""
        if self._connection is not None:
            self._connection.close()
            logger.info("Closed DuckDB database %s", self._config.database)
        self._connection = None

    def _run(self, statement: str, query_type: str) -> pl.DataFrame:
        """Run a statement on the open connection, logging and reporting without secrets."""
        if self._connection is None:
            raise ConnectorError(_SESSION_UNCONNECTED)
        started = time.perf_counter()
        try:
            frame = _read(self._connection, statement, f"the {query_type} statement")
        except ConnectorError:
            raise
        except ImportError:
            raise ConnectorError(_DUCKDB_EXTRA) from None
        except Exception as exc:
            logger.warning(
                "DuckDB %s statement on %s failed after %.3fs",
                query_type,
                self._config.database,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"DuckDB {query_type} statement on '{self._config.database}' failed: "
                f"{mask_secrets(str(exc), self._token)}"
            ) from None
        logger.info(
            "Ran DuckDB %s statement on %s in %.3fs, %d rows",
            query_type,
            self._config.database,
            time.perf_counter() - started,
            frame.height,
        )
        return frame


def _token(config: DuckDBConfig) -> str | None:
    """Return the MotherDuck token to connect with: the field's, then the environment's."""
    if not config.is_motherduck:
        return None
    token = config.motherduck_token or next(
        (os.environ[name] for name in _TOKEN_VARIABLES if os.environ.get(name)), None
    )
    if not token:
        # Without one, MotherDuck opens a browser sign-in, which a CI job never finishes.
        raise ConnectorError(
            f"MotherDuck database '{config.database}' needs a token. Set "
            "'motherduck_token', or the MOTHERDUCK_TOKEN environment variable."
        )
    return token


def _open(config: DuckDBConfig, token: str | None) -> Any:
    """Open a file read-only, or a MotherDuck database with its token as a setting."""
    if token is None:
        return duckdb.connect(config.database, read_only=True)
    # A read-only MotherDuck connection needs a read-scaling token, so it opens read-write.
    return duckdb.connect(config.database, config={"motherduck_token": token})


def _read(connection: Any, statement: str, subject: str) -> pl.DataFrame:
    """Run the statement and convert its result, refusing a column Polars cannot hold."""
    relation = connection.sql(statement)
    if relation is None:
        # Only a statement that returns no result, such as SET, gets here: a table read
        # always selects.
        raise ConnectorError(
            "The configured query returned no rows to compare. Write a statement that "
            "returns rows, such as a SELECT."
        )
    for name, kind in zip(relation.columns, relation.types, strict=True):
        if _unreadable(kind):
            raise ConnectorError(
                f"Column '{name}' of {subject} holds {kind}, which Polars cannot read. "
                "Cast it in a view or a 'query', such as to VARCHAR."
            )
    return cast("pl.DataFrame", relation.pl())


def _unreadable(kind: Any) -> bool:
    """Return whether a DuckDB type, or one nested in it, has no Polars form."""
    if kind.id in _UNREADABLE_TYPES:
        return True
    if kind.id not in _NESTED_TYPES:
        return False
    # An array's members include its size, which is a number rather than a type.
    return any(_unreadable(member) for _, member in kind.children if hasattr(member, "id"))
