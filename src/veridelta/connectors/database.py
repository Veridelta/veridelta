# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Database sources read through ConnectorX into Polars for a local comparison.

A database source is compared by the local engine, so it pairs with files,
lakehouse tables, or another database. `connect()` reads the configured `table`
or `query` exactly once and holds the result: Polars has no lazy SQL reader, and
wrapping the read in `pl.defer` would run the query again every time the engine
asks for a schema. Requires the `database` extra (`uv add 'veridelta[database]'`).

A password never reaches a log line or an error. Log lines carry the URI with
its password masked and the table name, never SQL text. A failed read is
reported with every form of the password replaced, and raised without the
driver's exception attached, because Polars re-raises the unscrubbed driver
error as the cause.
"""

import logging
import time
from pathlib import Path
from typing import Any, cast
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import polars as pl

from veridelta.connectors.base import PushdownQueryType, VerideltaConnector
from veridelta.connectors.sql import compile_database_select
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Which branch of the probe runs depends on the extras installed in the
# environment, so neither is a coverage target; the tests patch the module
# attribute instead.
connectorx: Any = None
try:
    import connectorx as _connectorx
except ImportError:  # pragma: no cover
    pass
else:  # pragma: no cover
    connectorx = _connectorx

_DATABASE_EXTRA = "Database extra is not installed. Install it with: uv add 'veridelta[database]'"
_UNCONNECTED = "Database connector is not connected. Call connect() first."
_PUSHDOWN_UNSUPPORTED = (
    "Database sources are compared locally; they have no SQL pushdown. "
    "Call connect() and read the frame instead."
)
_SQLITE_PREFIX = "sqlite://"


class DatabaseConnector(VerideltaConnector):
    """Read one database table or query into Polars through ConnectorX.

    `connect()` reads eagerly and keeps the frame, and `lazyframe()` hands it to
    the local engine as a LazyFrame over those rows. The read is the one place
    the rows are fetched, so it happens once per `connect()`. `execute_pushdown`
    always raises: the comparison runs in Polars, never in the database.
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize the connector with validated database settings.

        Args:
            config (DatabaseConfig): Frozen URI, credentials, and table or query.
        """
        self._config = config
        self._frame: pl.LazyFrame | None = None

    def connect(self) -> None:
        """Read the configured table or query into memory.

        Raises:
            ConnectorError: If the `database` extra is missing, a SQLite file
                does not exist, or the read fails.
            ConfigError: If `table` names a database Veridelta cannot quote for.
        """
        if connectorx is None:
            raise ConnectorError(_DATABASE_EXTRA)
        scheme = urlsplit(self._config.uri).scheme.lower()
        statement = self._statement(scheme)
        uri = self._connection_uri()
        if scheme == "sqlite":
            uri = _existing_sqlite_uri(uri)

        started = time.perf_counter()
        try:
            frame = pl.read_database_uri(statement, uri)
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
                f"failed: {self._scrub(str(exc))}"
            ) from None
        logger.info(
            "Read %d rows of %s from %s in %.3fs",
            frame.height,
            self._subject,
            self._config.redacted_uri,
            time.perf_counter() - started,
        )
        self._frame = frame.lazy()

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Reject SQL pushdown; a database source is compared locally.

        Args:
            statement (str): Unused SQL payload reserved by the ABC.
            query_type (PushdownQueryType): Unused warehouse round-trip tag.

        Returns:
            pl.LazyFrame: Never returned; read the source through `connect()`.

        Raises:
            ConnectorError: Always.
        """
        _ = statement
        _ = query_type
        raise ConnectorError(_PUSHDOWN_UNSUPPORTED)

    def fetch_schema(self) -> pl.Schema:
        """Return the schema of the rows `connect()` read.

        Returns:
            pl.Schema: Column names and dtypes.

        Raises:
            ConnectorError: If `connect()` has not been called.
        """
        return self.lazyframe().collect_schema()

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
        """Return the SQL to read: the compiled table select, or the query as written.

        Args:
            scheme (str): Lowercase URI scheme, which picks the table's quoting.

        Returns:
            str: Statement for the driver.

        Raises:
            ConfigError: If `table` names a database Veridelta cannot quote for.
        """
        if self._config.table is not None:
            return compile_database_select(scheme, self._config.table)
        # DatabaseConfig requires exactly one of `table` and `query`.
        return cast("str", self._config.query)

    @property
    def _subject(self) -> str:
        """Name what is read without repeating any SQL.

        Returns:
            str: `table '<name>'`, or `the configured query`.
        """
        if self._config.table is not None:
            return f"table '{self._config.table}'"
        return "the configured query"

    def _connection_uri(self) -> str:
        """Return the URI to connect with, carrying the `password` field if set.

        The model guarantees a user name and no password of its own in the URI
        whenever `password` is set.

        Returns:
            str: The configured URI, with the password percent-encoded into its
                user information when the `password` field holds one.
        """
        password = self._config.password
        if password is None:
            return self._config.uri
        parts = urlsplit(self._config.uri)
        user, _, host = parts.netloc.rpartition("@")
        netloc = f"{user}:{quote(password, safe='')}@{host}"
        return urlunsplit(parts._replace(netloc=netloc))

    def _scrub(self, text: str) -> str:
        """Replace every form of the password in driver output with `***`.

        Args:
            text (str): Message from the driver or Polars.

        Returns:
            str: The message with the raw and percent-encoded forms of the
                `password` field and of any password in the URI masked.
        """
        secrets: set[str] = set()
        if self._config.password:
            secrets.update({self._config.password, quote(self._config.password, safe="")})
        embedded = urlsplit(self._config.uri).password
        if embedded:
            secrets.update({embedded, unquote(embedded)})
        # Longest first, so a secret containing another is masked whole.
        for secret in sorted(secrets, key=len, reverse=True):
            text = text.replace(secret, "***")
        return text


def _existing_sqlite_uri(uri: str) -> str:
    """Encode a SQLite URI's path for ConnectorX and require the file to exist.

    ConnectorX percent-decodes everything after `sqlite://`, so the path is
    re-encoded here and may be written plainly, spaces and a Windows drive
    included. It opens SQLite files in create mode, so a missing path would
    leave an empty database behind and fail on the first table instead.

    Args:
        uri (str): Connection URI with the `sqlite` scheme.

    Returns:
        str: `sqlite://` followed by the percent-encoded path.

    Raises:
        ConnectorError: If no file exists at the path.
    """
    path = unquote(uri[len(_SQLITE_PREFIX) :])
    if not Path(path).is_file():
        raise ConnectorError(
            f"SQLite database '{path}' does not exist. Point 'uri' at an existing file, "
            "written as sqlite:// followed by its path."
        )
    return _SQLITE_PREFIX + quote(path)
