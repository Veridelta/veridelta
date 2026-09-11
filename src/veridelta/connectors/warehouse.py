# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Warehouse pushdown connectors for Snowflake and Databricks.

Drivers are optional extras. Missing packages raise `ConnectorError` with an
install hint. Query results are fetched as Arrow tables and wrapped in a
Polars LazyFrame.
"""

from typing import Any

import polars as pl

from veridelta.connectors.base import PushdownQueryType, VerideltaConnector
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabricksConfig, SnowflakeConfig

snowflake_connector: Any = None
try:
    import snowflake.connector as _snowflake_connector
except ImportError:
    pass
else:
    snowflake_connector = _snowflake_connector

databricks_sql: Any = None
try:
    import databricks.sql as _databricks_sql
except ImportError:
    pass
else:
    databricks_sql = _databricks_sql

_SNOWFLAKE_EXTRA = "Snowflake extra is not installed. Install it with: uv sync --extra snowflake"
_DATABRICKS_EXTRA = "Databricks extra is not installed. Install it with: uv sync --extra databricks"
_UNCONNECTED = "Warehouse connector is not connected. Call connect() first."
_NO_STATEMENT = "Call execute_pushdown before fetch_schema."
_NON_TABULAR = "Warehouse cursor did not return a tabular Arrow result."


def _lazy_from_arrow(table: Any) -> pl.LazyFrame:
    """Convert a driver Arrow payload into an unevaluated Polars LazyFrame.

    Args:
        table (Any): Arrow table returned by a warehouse cursor.

    Returns:
        pl.LazyFrame: Lazy wrapper around the tabular Arrow payload.

    Raises:
        ConnectorError: If the payload is missing or not a table.
    """
    if table is None:
        raise ConnectorError(_NON_TABULAR)
    frame = pl.from_arrow(table)  # pyright: ignore[reportUnknownMemberType]
    if not isinstance(frame, pl.DataFrame):
        raise ConnectorError(_NON_TABULAR)
    return frame.lazy()


def _schema_from_arrow(table: Any, description: Any) -> pl.Schema:
    """Build a Polars schema from an Arrow table, falling back to cursor metadata.

    Args:
        table (Any): Arrow table from a `LIMIT 0` query, if available.
        description (Any): Cursor `description` listing column names.

    Returns:
        pl.Schema: Deterministic column names and dtypes.

    Raises:
        ConnectorError: If neither Arrow nor cursor description is usable.
    """
    if table is not None:
        frame = pl.from_arrow(table)  # pyright: ignore[reportUnknownMemberType]
        if isinstance(frame, pl.DataFrame):
            return frame.schema

    if not description:
        raise ConnectorError("Warehouse cursor did not return a schema.")
    return pl.Schema({str(col[0]): pl.String() for col in description})


def _run_arrow_query(session: Any, statement: str, fetch_method: str) -> tuple[Any, Any]:
    """Execute SQL on a native session and fetch an Arrow payload.

    Args:
        session (Any): Open warehouse connection.
        statement (str): SQL to execute.
        fetch_method (str): Cursor method name that returns Arrow.

    Returns:
        tuple[Any, Any]: Arrow payload and cursor description metadata.

    Raises:
        ConnectorError: If the driver raises during execute or fetch.
    """
    cursor = session.cursor()
    try:
        cursor.execute(statement)
        table = getattr(cursor, fetch_method)()
        return table, getattr(cursor, "description", None)
    except ConnectorError:
        raise
    except Exception as exc:
        raise ConnectorError(f"Warehouse statement failed: {exc}") from exc
    finally:
        closer = getattr(cursor, "close", None)
        if callable(closer):
            closer()


class SnowflakeConnector(VerideltaConnector):
    """Snowflake SQL warehouse connector backed by the optional Snowflake extra."""

    def __init__(self, config: SnowflakeConfig) -> None:
        """Initialize the connector with validated Snowflake settings.

        Args:
            config (SnowflakeConfig): Frozen account, warehouse, and database
                settings.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.SNOWFLAKE)
        self._session: Any = None
        self._last_statement: str | None = None

    def connect(self) -> None:
        """Open a Snowflake session for subsequent pushdown statements.

        Raises:
            ConnectorError: If the Snowflake extra is missing or authentication
                fails.
        """
        if snowflake_connector is None:
            raise ConnectorError(_SNOWFLAKE_EXTRA)
        try:
            self._session = snowflake_connector.connect(
                account=self._config.account,
                user=self._config.user,
                password=self._config.password,
                warehouse=self._config.warehouse,
                database=self._config.database,
                schema=self._config.schema_name,
                role=self._config.role,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Failed to connect to Snowflake: {exc}") from exc

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiler SQL on Snowflake and return a LazyFrame.

        Args:
            statement (str): SQL produced by `SQLPushdownCompiler`.
            query_type (PushdownQueryType): Which comparison round-trip this
                statement represents.

        Returns:
            pl.LazyFrame: Unevaluated frame wrapped around the Arrow result.

        Raises:
            ConnectorError: If the extra is missing, the session is closed, or
                the cursor does not return a table.
        """
        _ = query_type
        self._require_session()
        table, _description = _run_arrow_query(self._session, statement, "fetch_arrow_all")
        self._last_statement = statement
        return _lazy_from_arrow(table)

    def fetch_schema(self) -> pl.Schema:
        """Describe the last pushdown result via a `LIMIT 0` query.

        Returns:
            pl.Schema: Column names and dtypes from the empty Arrow result.

        Raises:
            ConnectorError: If the session is missing or no statement has run.
        """
        self._require_session()
        if self._last_statement is None:
            raise ConnectorError(_NO_STATEMENT)
        schema_sql = f"SELECT * FROM ({self._last_statement}) AS _veridelta_schema LIMIT 0"
        table, description = _run_arrow_query(self._session, schema_sql, "fetch_arrow_all")
        return _schema_from_arrow(table, description)

    def _require_session(self) -> None:
        """Ensure the Snowflake extra is present and a session is open.

        Raises:
            ConnectorError: If the extra is missing or `connect()` was not called.
        """
        if snowflake_connector is None:
            raise ConnectorError(_SNOWFLAKE_EXTRA)
        if self._session is None:
            raise ConnectorError(_UNCONNECTED)


class DatabricksConnector(VerideltaConnector):
    """Databricks SQL warehouse connector backed by the optional Databricks extra."""

    def __init__(self, config: DatabricksConfig) -> None:
        """Initialize the connector with validated Databricks settings.

        Args:
            config (DatabricksConfig): Frozen workspace hostname and HTTP path.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.DATABRICKS)
        self._session: Any = None
        self._last_statement: str | None = None

    def connect(self) -> None:
        """Open a Databricks SQL session for subsequent pushdown statements.

        Raises:
            ConnectorError: If the Databricks extra is missing or authentication
                fails.
        """
        if databricks_sql is None:
            raise ConnectorError(_DATABRICKS_EXTRA)
        try:
            self._session = databricks_sql.connect(
                server_hostname=self._config.server_hostname,
                http_path=self._config.http_path,
                access_token=self._config.access_token,
                catalog=self._config.catalog,
                schema=self._config.schema_name,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(f"Failed to connect to Databricks: {exc}") from exc

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiler SQL on Databricks and return a LazyFrame.

        Args:
            statement (str): SQL produced by `SQLPushdownCompiler`.
            query_type (PushdownQueryType): Which comparison round-trip this
                statement represents.

        Returns:
            pl.LazyFrame: Unevaluated frame wrapped around the Arrow result.

        Raises:
            ConnectorError: If the extra is missing, the session is closed, or
                the cursor does not return a table.
        """
        _ = query_type
        self._require_session()
        table, _description = _run_arrow_query(self._session, statement, "fetchall_arrow")
        self._last_statement = statement
        return _lazy_from_arrow(table)

    def fetch_schema(self) -> pl.Schema:
        """Describe the last pushdown result via a `LIMIT 0` query.

        Returns:
            pl.Schema: Column names and dtypes from the empty Arrow result.

        Raises:
            ConnectorError: If the session is missing or no statement has run.
        """
        self._require_session()
        if self._last_statement is None:
            raise ConnectorError(_NO_STATEMENT)
        schema_sql = f"SELECT * FROM ({self._last_statement}) AS _veridelta_schema LIMIT 0"
        table, description = _run_arrow_query(self._session, schema_sql, "fetchall_arrow")
        return _schema_from_arrow(table, description)

    def _require_session(self) -> None:
        """Ensure the Databricks extra is present and a session is open.

        Raises:
            ConnectorError: If the extra is missing or `connect()` was not called.
        """
        if databricks_sql is None:
            raise ConnectorError(_DATABRICKS_EXTRA)
        if self._session is None:
            raise ConnectorError(_UNCONNECTED)
