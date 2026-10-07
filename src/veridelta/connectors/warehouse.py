# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Warehouse pushdown connectors for Snowflake, Databricks, and BigQuery.

Drivers are optional extras. Missing packages raise `ConnectorError` with an
install hint. Query results are fetched as Arrow tables and wrapped in a
Polars LazyFrame.

Connection and statement lifecycle is logged under the `veridelta.connectors.
warehouse` logger. Log lines carry the backend, the pushdown round-trip kind,
and timings, never SQL text or credentials.
"""

import importlib
import logging
import time
from collections.abc import Mapping
from typing import Any, Final

import polars as pl

from veridelta.connectors.base import (
    PushdownQueryType,
    PushdownSession,
    mask_secrets,
    optional_module,
)
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler
from veridelta.exceptions import ConnectorError
from veridelta.models import BigQueryConfig, DatabricksConfig, SnowflakeConfig

logger = logging.getLogger(__name__)

# The tests patch each module attribute below, so no test depends on the
# extras installed where it runs.
snowflake_connector: Any = optional_module("snowflake.connector")

databricks_sql: Any = optional_module("databricks.sql")

_SNOWFLAKE_EXTRA = (
    "Snowflake extra is not installed. Install it with: uv add 'veridelta[snowflake]'"
)
_DATABRICKS_EXTRA = (
    "Databricks extra is not installed. Install it with: uv add 'veridelta[databricks]'"
)
bigquery: Any = None
"""`google.cloud.bigquery`, imported by the first BigQuery `connect()` rather than
here: google-api-core warns at import time on Python versions near their end of
life, which would fail `import veridelta` wherever warnings are errors. Tests
patch this attribute."""

_BIGQUERY_EXTRA = "BigQuery extra is not installed. Install it with: uv add 'veridelta[bigquery]'"
_NO_ARROW_BATCHES = "BigQuery returned no Arrow batches, so the result has no columns."
_UNCONNECTED = "Warehouse connector is not connected. Call connect() first."
_NON_TABULAR = "Warehouse cursor did not return a tabular Arrow result."

# Without `force_return_table`, the driver returns None, not an empty table, for zero rows.
_SNOWFLAKE_FETCH_KWARGS: Final[Mapping[str, Any]] = {"force_return_table": True}
"""Arguments for `SnowflakeCursor.fetch_arrow_all`."""


def _lazy_from_arrow(table: Any) -> pl.LazyFrame:
    """Convert a driver Arrow payload into an unevaluated Polars LazyFrame."""
    if table is None:
        raise ConnectorError(_NON_TABULAR)
    frame = pl.from_arrow(table)  # pyright: ignore[reportUnknownMemberType] - untyped Arrow
    if not isinstance(frame, pl.DataFrame):
        raise ConnectorError(_NON_TABULAR)
    return frame.lazy()


def _run_arrow_query(
    session: Any,
    statement: str,
    fetch_method: str,
    *,
    backend: str,
    query_type: str,
    fetch_kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Execute SQL on a native session and fetch an Arrow payload."""
    started = time.perf_counter()
    cursor = session.cursor()
    try:
        cursor.execute(statement)
        table = getattr(cursor, fetch_method)(**(fetch_kwargs or {}))
    except ConnectorError:
        raise
    except Exception as exc:
        # The statement itself stays out of the log: it can embed value_map
        # and null_values literals. The raised error carries the driver text.
        logger.warning(
            "%s %s statement failed after %.3fs",
            backend,
            query_type,
            time.perf_counter() - started,
        )
        raise ConnectorError(f"Warehouse statement failed: {exc}") from exc
    else:
        logger.info(
            "%s %s statement completed in %.3fs",
            backend,
            query_type,
            time.perf_counter() - started,
        )
        return table
    finally:
        closer = getattr(cursor, "close", None)
        if callable(closer):
            closer()


def _run_bigquery_query(client: Any, statement: str, job_config: Any, *, query_type: str) -> Any:
    """Run SQL as a BigQuery job and fetch its result as Arrow record batches."""
    started = time.perf_counter()
    try:
        rows = client.query(statement, job_config=job_config).result()
        # `to_arrow_iterable` is the Arrow path that neither warns nor needs the storage extra.
        batches = list(rows.to_arrow_iterable())
    except Exception as exc:
        # The statement stays out of the log, as for the other warehouses.
        logger.warning(
            "BigQuery %s statement failed after %.3fs", query_type, time.perf_counter() - started
        )
        raise ConnectorError(f"Warehouse statement failed: {exc}") from exc
    logger.info(
        "BigQuery %s statement completed in %.3fs", query_type, time.perf_counter() - started
    )
    # A zero-row result still yields one batch, which carries the schema.
    if not batches:
        raise ConnectorError(_NO_ARROW_BATCHES)
    return batches


def _close_session(session: Any, backend: str) -> None:
    """Close a driver session, logging rather than raising if the driver objects."""
    closer = getattr(session, "close", None)
    if not callable(closer):
        return
    # Closing runs from `finally` blocks, so a driver error must not mask the result or an
    # exception already propagating.
    try:
        closer()
    except Exception as exc:
        # The driver's own text can echo connection details, so only its type is a warning.
        logger.warning("%s session did not close cleanly: %s", backend, type(exc).__name__)
        logger.debug("%s session close failed", backend, exc_info=True)
    else:
        logger.info("Closed %s session", backend)


def _snowflake_credentials(config: SnowflakeConfig) -> dict[str, str | None]:
    """Return the driver arguments that sign in: a password, or a key file."""
    if config.private_key_path is None:
        return {"password": config.password}
    # A key file alone switches the driver to key-pair sign-in.
    credentials: dict[str, str | None] = {"private_key_file": config.private_key_path}
    if config.private_key_passphrase is not None:
        credentials["private_key_file_pwd"] = config.private_key_passphrase
    return credentials


class SnowflakeConnector(PushdownSession):
    """Snowflake SQL warehouse connector backed by the optional Snowflake extra.

    `connect()` opens a `snowflake.connector` session from the frozen
    `SnowflakeConfig`; `execute_pushdown` runs compiler SQL on a fresh cursor
    and fetches the result as Arrow. Install the driver with
    `uv add 'veridelta[snowflake]'`; without it, `connect()` raises
    `ConnectorError` with that hint instead of an `ImportError`.

    Attributes:
        compiler (SQLPushdownCompiler): Snowflake-dialect compiler the engine
            uses to build every statement this connector executes.
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        """Initialize the connector with validated Snowflake settings.

        Args:
            config (SnowflakeConfig): Frozen account, warehouse, and database
                settings.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.SNOWFLAKE)
        self._session: Any = None

    def connect(self) -> None:
        """Open a Snowflake session for subsequent pushdown statements.

        Raises:
            ConnectorError: If the Snowflake extra is missing or authentication
                fails.
        """
        if snowflake_connector is None:
            raise ConnectorError(_SNOWFLAKE_EXTRA)
        config = self._config
        try:
            self._session = snowflake_connector.connect(
                account=config.account,
                user=config.user,
                warehouse=config.warehouse,
                database=config.database,
                schema=config.schema_name,
                role=config.role,
                **_snowflake_credentials(config),
            )
        except ConnectorError:
            raise
        except Exception as exc:
            logger.warning("Snowflake connection to account %s failed", config.account)
            # Not chained: a traceback would print the driver's message unmasked.
            message = mask_secrets(str(exc), config.password, config.private_key_passphrase)
            raise ConnectorError(f"Failed to connect to Snowflake: {message}") from None
        logger.info(
            "Connected to Snowflake account %s, warehouse %s",
            self._config.account,
            self._config.warehouse,
        )

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiler SQL on Snowflake and return a LazyFrame.

        Args:
            statement (str): SQL produced by `SQLPushdownCompiler`.
            query_type (PushdownQueryType): Which comparison round-trip this
                statement represents; recorded in the log line for the call.

        Returns:
            pl.LazyFrame: Unevaluated frame wrapped around the Arrow result.

        Raises:
            ConnectorError: If the extra is missing, the session is closed, or
                the cursor does not return a table.
        """
        self._require_session()
        table = _run_arrow_query(
            self._session,
            statement,
            "fetch_arrow_all",
            backend="Snowflake",
            query_type=query_type,
            fetch_kwargs=_SNOWFLAKE_FETCH_KWARGS,
        )
        return _lazy_from_arrow(table)

    def close(self) -> None:
        """Close the Snowflake session, if one is open.

        Idempotent. Afterwards `execute_pushdown` raises `ConnectorError` until
        `connect()` is called again.
        """
        if self._session is None:
            return
        session, self._session = self._session, None
        _close_session(session, "Snowflake")

    def _require_session(self) -> None:
        """Ensure the Snowflake extra is present and a session is open."""
        if snowflake_connector is None:
            raise ConnectorError(_SNOWFLAKE_EXTRA)
        if self._session is None:
            raise ConnectorError(_UNCONNECTED)


class DatabricksConnector(PushdownSession):
    """Databricks SQL warehouse connector backed by the optional Databricks extra.

    `connect()` opens a `databricks.sql` session against the configured SQL
    warehouse HTTP path; `execute_pushdown` runs compiler SQL on a fresh cursor
    and fetches the result as Arrow. Install the driver with
    `uv add 'veridelta[databricks]'`; without it, `connect()` raises
    `ConnectorError` with that hint instead of an `ImportError`.

    Attributes:
        compiler (SQLPushdownCompiler): Databricks-dialect compiler (backtick
            quoting, Spark type names) the engine uses for every statement.
    """

    def __init__(self, config: DatabricksConfig) -> None:
        """Initialize the connector with validated Databricks settings.

        Args:
            config (DatabricksConfig): Frozen workspace hostname and HTTP path.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.DATABRICKS)
        self._session: Any = None

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
            logger.warning("Databricks connection to %s failed", self._config.server_hostname)
            # Not chained: a traceback would print the driver's message unmasked.
            message = mask_secrets(str(exc), self._config.access_token)
            raise ConnectorError(f"Failed to connect to Databricks: {message}") from None
        logger.info(
            "Connected to Databricks host %s, path %s",
            self._config.server_hostname,
            self._config.http_path,
        )

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiler SQL on Databricks and return a LazyFrame.

        Args:
            statement (str): SQL produced by `SQLPushdownCompiler`.
            query_type (PushdownQueryType): Which comparison round-trip this
                statement represents; recorded in the log line for the call.

        Returns:
            pl.LazyFrame: Unevaluated frame wrapped around the Arrow result.

        Raises:
            ConnectorError: If the extra is missing, the session is closed, or
                the cursor does not return a table.
        """
        self._require_session()
        table = _run_arrow_query(
            self._session,
            statement,
            "fetchall_arrow",
            backend="Databricks",
            query_type=query_type,
        )
        return _lazy_from_arrow(table)

    def close(self) -> None:
        """Close the Databricks session, if one is open.

        Idempotent. Afterwards `execute_pushdown` raises `ConnectorError` until
        `connect()` is called again.
        """
        if self._session is None:
            return
        session, self._session = self._session, None
        _close_session(session, "Databricks")

    def _require_session(self) -> None:
        """Ensure the Databricks extra is present and a session is open."""
        if databricks_sql is None:
            raise ConnectorError(_DATABRICKS_EXTRA)
        if self._session is None:
            raise ConnectorError(_UNCONNECTED)


class BigQueryConnector(PushdownSession):
    """BigQuery warehouse connector backed by the optional BigQuery extra.

    `connect()` builds a `bigquery.Client` for the configured project, from a
    service account key file when `credentials_path` is set and from
    Application Default Credentials otherwise. Every statement runs as a
    GoogleSQL job under one `QueryJobConfig`, which names the default dataset
    and the `maximum_bytes_billed` cap. Install the client with
    `uv add 'veridelta[bigquery]'`; it is imported on first connect.

    Attributes:
        compiler (SQLPushdownCompiler): BigQuery-dialect compiler (backtick
            quoting, GoogleSQL type names) the engine uses for every statement.
    """

    def __init__(self, config: BigQueryConfig) -> None:
        """Initialize the connector with validated BigQuery settings.

        Args:
            config (BigQueryConfig): Frozen project, table, and job settings.
        """
        self._config = config
        self.compiler = SQLPushdownCompiler(SQLDialect.BIGQUERY)
        self._client: Any = None
        self._job_config: Any = None

    def connect(self) -> None:
        """Create the BigQuery client and the job settings every statement uses.

        Raises:
            ConnectorError: If the BigQuery extra is missing or the client
                cannot be created, such as when no credentials are found.
        """
        driver = bigquery
        if driver is None:
            try:
                driver = importlib.import_module("google.cloud.bigquery")
            except ImportError:
                raise ConnectorError(_BIGQUERY_EXTRA) from None
        project, location = self._config.project, self._config.location
        # Legacy SQL rejects backtick quoting, so GoogleSQL is set explicitly
        # rather than trusted to stay the client's default.
        settings: dict[str, Any] = {"use_legacy_sql": False}
        if self._config.dataset is not None:
            settings["default_dataset"] = f"{project}.{self._config.dataset}"
        if self._config.maximum_bytes_billed is not None:
            settings["maximum_bytes_billed"] = self._config.maximum_bytes_billed
        try:
            if self._config.credentials_path is None:
                client = driver.Client(project=project, location=location)
            else:
                client = driver.Client.from_service_account_json(
                    self._config.credentials_path, project=project, location=location
                )
            job_config = driver.QueryJobConfig(**settings)
        except Exception as exc:
            logger.warning("BigQuery connection to project %s failed", project)
            # Not chained, as for the other warehouses: the message already holds the
            # driver's reason, and its traceback can quote the credentials it read.
            raise ConnectorError(f"Failed to connect to BigQuery: {exc}") from None
        self._client, self._job_config = client, job_config
        logger.info("Connected to BigQuery project %s", project)

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Execute compiler SQL as a BigQuery job and return a LazyFrame.

        Args:
            statement (str): SQL produced by `SQLPushdownCompiler`.
            query_type (PushdownQueryType): Which comparison round-trip this
                statement represents; recorded in the log line for the call.

        Returns:
            pl.LazyFrame: Unevaluated frame wrapped around the Arrow result.

        Raises:
            ConnectorError: If the connector is not connected, the job fails,
                or it returns no Arrow batches.
        """
        self._require_client()
        batches = _run_bigquery_query(
            self._client, statement, self._job_config, query_type=query_type
        )
        return _lazy_from_arrow(batches)

    def close(self) -> None:
        """Close the BigQuery client, if one is open.

        Idempotent. Afterwards `execute_pushdown` raises `ConnectorError` until
        `connect()` is called again.
        """
        if self._client is None:
            return
        client = self._client
        self._client = self._job_config = None
        _close_session(client, "BigQuery")

    def _require_client(self) -> None:
        """Ensure `connect()` has created a client."""
        if self._client is None:
            raise ConnectorError(_UNCONNECTED)
