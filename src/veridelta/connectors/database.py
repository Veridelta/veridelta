# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Database sources read through ConnectorX into Polars for a local comparison.

A database source is compared by the local engine, so it pairs with files,
lakehouse tables, or another database. `connect()` reads the configured `table`
or `query` exactly once and holds the result: Polars has no lazy SQL reader, and
wrapping the read in `pl.defer` would run the query again every time the engine
asks for a schema. Requires the `database` extra (`uv add 'veridelta[database]'`).

Two Postgres, two SQL Server, or two Redshift tables that opt into `pushdown`
are compared inside the database instead: `PostgresPushdownSession`,
`TSqlPushdownSession`, and `RedshiftPushdownSession` run each compiled
statement through the same driver and read back only counts and keys.

A password never reaches a log line or an error. Log lines carry the URI with
its password masked and the table name, never SQL text. A failed read is
reported with every form of the password replaced, and raised without the
driver's exception attached, because Polars re-raises the unscrubbed driver
error as the cause.
"""

import logging
import os
import re
import time
from abc import abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Final, cast
from urllib.parse import SplitResult, parse_qsl, quote, unquote, urlsplit, urlunsplit

import polars as pl

from veridelta.connectors.base import (
    PROBE_NEEDS_A_TABLE,
    PushdownQueryType,
    PushdownSession,
    ReaderConnector,
    mask_secrets,
    names_another_machine,
    optional_module,
    read_subject,
)
from veridelta.connectors.sql import (
    SERVER_CHECK_TEXT,
    SQLDialect,
    SQLPushdownCompiler,
    compile_database_partition_counts,
    compile_database_partition_range,
    compile_database_probe,
    compile_database_select,
    compile_mssql_utc_select,
    compile_postgres_columns_query,
    compile_postgres_text_select,
    compile_redshift_server_check,
    compile_tsql_server_checks,
)
from veridelta.exceptions import ConfigError, ConnectorError, missing_extra
from veridelta.models import POSTGRES_SCHEMES, DatabaseConfig

logger = logging.getLogger(__name__)

# The tests patch each module attribute below, so no test depends on the
# extras installed where it runs.
connectorx: Any = optional_module("connectorx")

_DATABASE_EXTRA = missing_extra("database", "Reading a database table")
_UNCONNECTED = "Database connector is not connected. Call connect() first."
_SQLITE_PREFIX = "sqlite://"
_LITERAL_RULES = "SELECT current_setting('standard_conforming_strings') AS value"

_URL_SCHEME: Final = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*(?=:)")
"""A URI's scheme, as a URL parser reads it before the first `:`."""

_URL_TRIMMED: Final = "".join(map(chr, range(0x21)))
"""Control characters and the space, which a URL parser drops at either end of a URI."""

_URL_REMOVED: Final = dict.fromkeys(map(ord, "\t\n\r"))
"""Tabs and line breaks, which a URL parser drops from anywhere in a URI."""

_CLICKHOUSE_SCHEME: Final = "clickhouse"
"""URI scheme whose empty reads are counted, since ConnectorX can drop every row."""

_MSSQL_SCHEME: Final = "mssql"
"""URI scheme whose `table` reads move each `DATETIMEOFFSET` column to offset zero."""

_MAX_DECIMAL_PRECISION: Final = 38
"""Widest decimal Polars holds. A wider `numeric` keeps ConnectorX's default read."""


class DatabaseConnector(ReaderConnector):
    """Read one database table or query into Polars through ConnectorX.

    `connect()` reads eagerly and keeps the frame, and `lazyframe()` hands it to
    the local engine as a LazyFrame over those rows. The read is the one place
    the rows are fetched, so it happens once per `connect()`. With
    `partition_on` set, ConnectorX splits that read into ranges over parallel
    connections, after Veridelta confirms the column holds no NULL and reads
    its lowest and highest values. The comparison runs in Polars, never in the
    database.
    """

    _unconnected = _UNCONNECTED

    def __init__(self, config: DatabaseConfig, *, probe: bool = False) -> None:
        """Initialize the connector with validated database settings.

        Args:
            config (DatabaseConfig): Frozen URI, credentials, and table or query.
            probe (bool): Whether to read the table's columns and no rows, for a
                schema check. Only a `table` can be probed.
        """
        self._config = config
        self._probe = probe

    def connect(self) -> None:
        """Read the configured table or query into memory.

        Raises:
            ConnectorError: If the `database` extra is missing, a SQLite file
                does not exist, the partition column holds a NULL or anything
                but integers, or the read fails.
            ConfigError: If `table` names a database Veridelta cannot quote for,
                a probe was asked of a `query`, or a SQLite URI does not start
                with `sqlite://` or holds a NUL character.
        """
        if connectorx is None:
            raise ConnectorError(_DATABASE_EXTRA)
        path = sqlite_path(self._config)
        scheme = urlsplit(self._config.uri).scheme.lower()
        statement = self._statement(scheme)
        uri = connection_uri(self._config) if path is None else _existing_sqlite_uri(path)

        started = time.perf_counter()
        declared: dict[str, pl.Decimal] = {}
        try:
            # A Postgres `table` read keeps each `numeric` column's declared scale.
            if scheme in POSTGRES_SCHEMES and self._config.table is not None:
                statement, declared = self._declared_statement(self._config.table, statement, uri)
            elif scheme == _MSSQL_SCHEME and self._config.table is not None and not self._probe:
                statement = self._utc_statement(self._config.table, statement, uri)
            frame = self._read(scheme, statement, uri)
            if scheme == _CLICKHOUSE_SCHEME and frame.height == 0 and not self._probe:
                self._refuse_dropped_rows(statement, uri)
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
                f"failed: {_scrub(self._config, str(exc))}",
                quotes_data=True,
            ) from None
        logger.info(
            "Read %d rows of %s from %s in %.3fs",
            frame.height,
            self._subject,
            self._config.redacted_uri,
            time.perf_counter() - started,
        )
        self._frame = _with_declared_scale(frame, declared, self._subject).lazy()

    def _statement(self, scheme: str) -> str:
        """Return the SQL to read: the compiled table select, or the query as written."""
        if self._config.table is not None and self._probe:
            return compile_database_probe(scheme, self._config.table)
        if self._config.table is not None:
            return compile_database_select(scheme, self._config.table)
        if self._probe:
            # Wrapping a statement is not portable: Oracle refuses `AS` on a
            # derived table, and SQL Server refuses `ORDER BY` inside one.
            raise ConfigError(PROBE_NEEDS_A_TABLE)
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

    def _refuse_dropped_rows(self, statement: str, uri: str) -> None:
        """Refuse a ClickHouse read that returned no rows while ClickHouse holds some.

        ConnectorX returns no rows and no error when a column is a `Decimal`
        with precision above 18. Counting the same statement tells an empty
        result from a dropped one, at the cost of one query when a read is empty.
        """
        held = pl.read_database_uri(_clickhouse_count(statement), uri).item()
        if held:
            raise ConnectorError(
                f"Database read of {self._subject} from '{self._config.redacted_uri}' "
                f"returned no rows, but ClickHouse holds {held:,}. ConnectorX returns no "
                "rows when a column is a Decimal with precision above 18. Read it with a "
                "'query' that casts each such column: to a Decimal of precision 18 or less "
                "when its values fit, or with toString() or toFloat64()."
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


class _ConnectorXPushdownSession(PushdownSession):
    """Run compiled comparison SQL inside a database, through ConnectorX.

    Opened for two database sources on one connection that both set
    `pushdown`. Each statement is one ConnectorX read, which opens its own
    connection and returns Arrow, so nothing is held between statements and
    `close()` only forgets the connection. Only counts and keys come back.

    A subclass names its backend for logs and errors, sets its dialect, and
    checks in `_check_server` that the server reads the SQL its dialect
    writes the way the compiler assumes.
    """

    _backend: ClassVar[str]
    """The backend's name in log lines and errors, such as `Postgres`."""

    def __init__(self, config: DatabaseConfig, dialect: SQLDialect) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DatabaseConfig): A database table that sets `pushdown`.
            dialect (SQLDialect): The dialect the backend reads.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(dialect)
        self._uri: str | None = None

    def connect(self) -> None:
        """Check the server and keep the connection URI for the statements to come.

        Raises:
            ConnectorError: If the `database` extra is missing, the server cannot
                be reached, or it would read the dialect's SQL another way.
        """
        if connectorx is None:
            raise ConnectorError(_DATABASE_EXTRA)
        uri = connection_uri(self._config)
        self._check_server(uri)
        self._uri = uri

    @abstractmethod
    def _check_server(self, uri: str) -> None:
        """Refuse a server that would read the dialect's SQL another way.

        Args:
            uri (str): The connection URI, with its password.

        Raises:
            ConnectorError: If the server cannot be reached or fails the check.
        """

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Run one compiled statement and return its rows lazily.

        Args:
            statement (str): SQL from the session's compiler.
            query_type (PushdownQueryType): Which round-trip this is, for logs
                and errors.

        Returns:
            pl.LazyFrame: The statement's result.

        Raises:
            ConnectorError: If the session is not connected or the statement fails.
        """
        frame = self._read(statement, self._connected_uri(), query_type=query_type)
        return frame.lazy()

    def close(self) -> None:
        """Forget the connection. Idempotent; `connect()` checks the server again."""
        self._uri = None

    def _connected_uri(self) -> str:
        """Return the URI `connect()` kept."""
        if self._uri is None:
            raise ConnectorError(
                f"{self._backend} pushdown session is not connected. Call connect() first."
            )
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
                "%s %s statement on %s failed after %.3fs",
                self._backend,
                query_type,
                self._config.redacted_uri,
                time.perf_counter() - started,
            )
            raise ConnectorError(
                f"{self._backend} {query_type} statement on '{self._config.redacted_uri}' "
                f"failed: {_scrub(self._config, str(exc))}",
                quotes_data=True,
            ) from None
        logger.info(
            "Ran %s %s statement on %s in %.3fs, %d rows",
            self._backend,
            query_type,
            self._config.redacted_uri,
            time.perf_counter() - started,
            frame.height,
        )
        return frame


class PostgresPushdownSession(_ConnectorXPushdownSession):
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

    _backend = "Postgres"

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DatabaseConfig): A Postgres table that sets `pushdown`.
        """
        super().__init__(config, SQLDialect.POSTGRES)

    def _check_server(self, uri: str) -> None:
        """Refuse a server that reads a backslash in a string literal as an escape."""
        setting = self._read(_LITERAL_RULES, uri, query_type="settings")
        if setting.item(0, 0) != "on":
            raise ConnectorError(
                f"standard_conforming_strings is off on Postgres at "
                f"'{self._config.redacted_uri}', so it would read a backslash in a string "
                "literal as an escape. Turn it on for the role or database, or compare "
                "locally (pushdown: false)."
            )

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


class TSqlPushdownSession(_ConnectorXPushdownSession):
    """Run compiled comparison SQL inside SQL Server, Azure SQL, or Synapse, through ConnectorX.

    Opened for two database sources on one `mssql://` connection that both set
    `pushdown`. Each statement is one ConnectorX read, which opens its own
    connection and returns Arrow, so nothing is held between statements and
    `close()` only forgets the connection. Only counts and keys come back.

    `connect()` checks that the server reads the T-SQL dialect as the compiler
    assumes. A string literal must come back as written. A NULL must stay NULL
    when text is appended to it, as it does while `CONCAT_NULL_YIELDS_NULL` is
    on; otherwise a NULL would compare equal to an empty value. Exact text must
    tell case, accents, and trailing spaces apart. `LTRIM` and `RTRIM` must take
    the characters to strip, as they do from SQL Server 2022. The check logs
    the server's engine edition.
    """

    _backend = "SQL Server"

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DatabaseConfig): A SQL Server table that sets `pushdown`.
        """
        super().__init__(config, SQLDialect.TSQL)

    def _check_server(self, uri: str) -> None:
        """Refuse a server that would read the T-SQL dialect another way."""
        settings, trims = compile_tsql_server_checks()
        text, nulls_stay_null, exact_text, edition = self._read(
            settings, uri, query_type="settings"
        ).row(0)
        where = f"SQL Server at '{self._config.redacted_uri}'"
        logger.info("%s is engine edition %s", where, edition)
        if text != SERVER_CHECK_TEXT:
            raise ConnectorError(
                f"{where} read the string literal {SERVER_CHECK_TEXT!r} back as {text!r}, so "
                "it would compare other values than the configuration names. Compare "
                "locally (pushdown: false)."
            )
        if nulls_stay_null != 1:
            raise ConnectorError(
                f"CONCAT_NULL_YIELDS_NULL is off on {where}, so a NULL would compare equal "
                "to an empty value. Turn it on for the database, or compare locally "
                "(pushdown: false)."
            )
        if exact_text != 1:
            raise ConnectorError(
                f"{where} compares text that differs in case, accents, or trailing spaces as "
                "equal even under a binary collation. Compare locally (pushdown: false)."
            )
        try:
            trimmed = self._read(trims, uri, query_type="settings").item(0, 0)
        except ConnectorError as exc:
            raise ConnectorError(
                f"{where} refused LTRIM and RTRIM with the characters to strip. Pushdown "
                "needs them, which SQL Server has from 2022. Compare locally "
                f"(pushdown: false), or upgrade. {exc}",
                quotes_data=exc.quotes_data,
            ) from None
        if trimmed != 1:
            raise ConnectorError(
                f"{where} trims whitespace another way than Polars does. Compare locally "
                "(pushdown: false)."
            )


class RedshiftPushdownSession(_ConnectorXPushdownSession):
    """Run compiled comparison SQL inside Redshift, through ConnectorX.

    Opened for two database sources on one `redshift://` connection that both
    set `pushdown`. Each statement is one ConnectorX read over the Postgres
    protocol, which opens its own connection and returns Arrow, so nothing is
    held between statements and `close()` only forgets the connection. Only
    counts and keys come back.

    `connect()` checks that the server reads the Redshift dialect as the
    compiler assumes. A string literal must come back as written, with its
    backslash read as an escape. A NULL must stay NULL when text is appended
    to it. Exact text must tell case and trailing spaces apart, even in a
    database created to ignore case. NaN must equal NaN, as in a local run.
    Each trim must strip the whitespace Polars strips.
    """

    _backend = "Redshift"

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the session for one side's connection settings.

        Args:
            config (DatabaseConfig): A Redshift table that sets `pushdown`.
        """
        super().__init__(config, SQLDialect.REDSHIFT)

    def _check_server(self, uri: str) -> None:
        """Refuse a server that would read the Redshift dialect another way."""
        text, nulls_stay_null, exact_text, nan_equal, trims = self._read(
            compile_redshift_server_check(), uri, query_type="settings"
        ).row(0)
        where = f"Redshift at '{self._config.redacted_uri}'"
        refusals = (
            (
                text != SERVER_CHECK_TEXT,
                f"read the string literal {SERVER_CHECK_TEXT!r} back as {text!r}, so it "
                "would compare other values than the configuration names",
            ),
            (
                nulls_stay_null != 1,
                "joins NULL and text into text, so a NULL would compare equal to an empty value",
            ),
            (
                exact_text != 1,
                "compares text that differs in case or trailing spaces as equal, even "
                "under the case_sensitive collation",
            ),
            (nan_equal != 1, "does not read NaN as equal to NaN, as a local run does"),
            (trims != 1, "trims whitespace another way than Polars does"),
        )
        for refused, reason in refusals:
            if refused:
                raise ConnectorError(f"{where} {reason}. Compare locally (pushdown: false).")


def _clickhouse_count(statement: str) -> str:
    """Return a ClickHouse statement that counts the rows `statement` selects."""
    return f"SELECT count() AS held FROM ({statement.rstrip().rstrip(';')})"


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


def connection_uri(config: DatabaseConfig) -> str:
    """Return the URI the connector hands ConnectorX, carrying the `password` field if set.

    Args:
        config (DatabaseConfig): A database source.

    Returns:
        str: `uri`, with `password` percent-encoded into its login when set.
            A ClickHouse URI holds its password as written instead, as
            `_clickhouse_uri` says.

    Raises:
        ConfigError: If a ClickHouse password holds a character ConnectorX
            would send percent-encoded.
    """
    read = _connectorx_parts(config.uri)
    if _reader(read) == _CLICKHOUSE_SCHEME:
        return _clickhouse_uri(config, read)
    password = config.password
    if password is None:
        return config.uri
    parts = urlsplit(config.uri)
    # The model guarantees a user name and no password in the URI whenever `password` is set.
    user, _, host = parts.netloc.rpartition("@")
    netloc = f"{user}:{quote(password, safe='')}@{host}"
    return urlunsplit(parts._replace(netloc=netloc))


_CLICKHOUSE_PASSWORD: Final = re.compile(r"[A-Za-z0-9!$%&'()*+,._~-]*")
"""A ClickHouse password that ConnectorX sends as written.

ConnectorX's URL parser keeps these characters in a URI's password as they
are, a `%` included, and percent-encodes every other one. ConnectorX then
sends the password in the `X-ClickHouse-Key` header without decoding it."""


def _clickhouse_uri(config: DatabaseConfig, parts: SplitResult) -> str:
    """Return a ClickHouse URI whose login holds the password as written.

    ConnectorX sends a ClickHouse password without decoding it, so the
    password goes into the login unencoded: the `password` field as written,
    or the URI's own password decoded. A password with a character outside
    `_CLICKHOUSE_PASSWORD` cannot reach ClickHouse whole, so it is refused
    before any connection.

    Args:
        config (DatabaseConfig): A database source ConnectorX reads as ClickHouse.
        parts (SplitResult): Its URI as ConnectorX parses it, with the scheme as written.

    Returns:
        str: The URI with its password unencoded, or `uri` when there is no password.

    Raises:
        ConfigError: If the password holds a character outside `_CLICKHOUSE_PASSWORD`.
    """
    password = config.password if parts.password is None else unquote(parts.password)
    if password is None:
        return config.uri
    if _CLICKHOUSE_PASSWORD.fullmatch(password) is None:
        # The message names neither the password nor the character, which is part of it.
        raise ConfigError(
            "A ClickHouse password can hold only letters, digits, and the characters "
            "! $ % & ' ( ) * + , . _ ~ -. ConnectorX sends a ClickHouse password "
            "percent-encoded as its uri holds it, so ClickHouse refuses one with any other "
            "character. Give the reading user a password of those characters only."
        )
    # The model guarantees a user name whenever `password` is set.
    login, _, host = parts.netloc.rpartition("@")
    user = login.partition(":")[0]
    return urlunsplit(parts._replace(netloc=f"{user}:{password}@{host}"))


_CREDENTIAL_PARAMETERS: Final = ("pass", "pwd", "secret", "token", "key")
"""Words that mark a URI parameter, such as `password` or `sslkey`, as holding a credential."""


def _scrub(config: DatabaseConfig, text: str) -> str:
    """Replace every form of the password, or of a credential parameter, in driver output."""
    secrets: set[str] = set()
    if config.password:
        secrets.update({config.password, quote(config.password, safe="")})
    parts = urlsplit(config.uri)
    if parts.password:
        secrets.update({parts.password, unquote(parts.password)})
    for name, value in parse_qsl(parts.query):
        if any(word in name.lower() for word in _CREDENTIAL_PARAMETERS):
            secrets.update({value, quote(value, safe="")})
    return mask_secrets(text, *secrets)


def connectorx_url(config: DatabaseConfig) -> SplitResult:
    """Split the URI ConnectorX reads for a database source, with the scheme it picks a reader by.

    The connector hands ConnectorX `connection_uri`. ConnectorX's Python
    package first rewrites a URI whose text before the first `:` holds
    `redshift`, in lowercase and anywhere in it, and not `-iam`, to start with
    `postgresql:`, so it reads `sqliteredshift://` with its Postgres reader.
    It then parses the URI as a URL parser does, after it drops spaces and
    control characters at either end and every tab or line break, and picks
    its reader by the scheme in lowercase up to the first `+`, so it reads
    `BigQuery+x://` as BigQuery. The scheme here is that reader's name, and
    empty for a URI with no scheme.

    Args:
        config (DatabaseConfig): A database source.

    Returns:
        SplitResult: The URI's parts, with the reader's name for a scheme.
    """
    parts = _connectorx_parts(connection_uri(config))
    return parts._replace(scheme=_reader(parts))


def _connectorx_parts(uri: str) -> SplitResult:
    """Split `uri` as ConnectorX parses it, with its scheme as written."""
    backend, _, rest = uri.partition(":")
    if "redshift" in backend and "-iam" not in backend:
        uri = f"postgresql:{rest}"
    return urlsplit(uri.strip(_URL_TRIMMED).translate(_URL_REMOVED))


def _reader(parts: SplitResult) -> str:
    """Name the reader ConnectorX picks for a URI it parsed: its scheme, up to the first `+`."""
    return parts.scheme.split("+")[0]


def sqlite_path(config: DatabaseConfig) -> str | None:
    """Return the path of the file a SQLite source opens, as the connector opens it.

    ConnectorX reads a URI as SQLite when its scheme, up to the first `+`, is
    `sqlite`, as in `sqlite+x://`, and it finds the scheme as a URL parser
    does, after it drops spaces and control characters at either end and every
    tab or line break. This reads the scheme the same way, and refuses every
    such URI that does not start with `sqlite://` itself.

    ConnectorX percent-decodes all the text after `sqlite://`, so this does too,
    with any `?` or `#` and the `password` field kept in it. The decoded path is
    then resolved as the operating system resolves it: made absolute against the
    working directory, with every link followed and every `.` and `..` folded.
    The connector hands ConnectorX this text, encoded, so nothing is left for its
    URL parser to fold another way, SQLite reads no `file:` URI of its own in
    it, and no reader expands a leading `~`. The MCP server checks this path
    against its roots, so the check and the read name one file. A path the
    system reads as one on another machine, as `names_another_machine` says, is
    returned decoded but not resolved, since resolving it would reach that
    machine; the MCP server refuses it as it is.

    Args:
        config (DatabaseConfig): A database source.

    Returns:
        str | None: The decoded path, resolved unless it is on another machine,
            or None for a URI ConnectorX does not read as SQLite.

    Raises:
        ConfigError: If ConnectorX reads the URI as SQLite but it does not start
            with `sqlite://`, or the decoded path holds a NUL character, which
            no file name can.
    """
    text = config.uri.strip(_URL_TRIMMED).translate(_URL_REMOVED)
    scheme = _URL_SCHEME.match(text)
    if scheme is None or scheme[0].lower().split("+")[0] != "sqlite":
        return None
    if config.uri[: len(_SQLITE_PREFIX)].lower() != _SQLITE_PREFIX:
        raise ConfigError(
            f"ConnectorX reads a 'uri' with the scheme '{scheme[0]}' as a SQLite file, but "
            "Veridelta reads only one that starts with sqlite://. Write 'uri' as sqlite:// "
            "followed by the file's path."
        )
    decoded = unquote(connection_uri(config)[len(_SQLITE_PREFIX) :])
    if "\0" in decoded:
        raise ConfigError(
            "A SQLite path holds a NUL character, such as '%00', which no file name can hold. "
            "Write 'uri' as sqlite:// followed by the file's path."
        )
    if names_another_machine(decoded):
        return decoded
    # realpath, not Path.resolve, since Python 3.11 and 3.12 raise on a symbolic link loop.
    return os.path.realpath(decoded)


def _existing_sqlite_uri(path: str) -> str:
    """Encode a SQLite file's path for ConnectorX and require the file to exist."""
    # ConnectorX opens SQLite in create mode, so a missing file leaves an empty database behind.
    if not Path(path).is_file():
        raise ConnectorError(
            f"SQLite database '{path}' does not exist. Point 'uri' at an existing file, "
            "written as sqlite:// followed by its path."
        )
    return _SQLITE_PREFIX + quote(path)
