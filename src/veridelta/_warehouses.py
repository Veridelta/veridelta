# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Recognize the warehouse sides a comparison pushes down, and open their sessions.

A pair of sides runs in place only when both name the same warehouse. This module
holds the table of warehouses, refuses a mixed pair, and opens one session for a
pushed-down pair. A warehouse type is its own backend; a database source that sets
`pushdown` runs on the backend its URI scheme names.
"""

from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Final, TypeAlias, TypeGuard, TypeVar, cast
from urllib.parse import urlsplit

from veridelta.connectors.base import PushdownSession
from veridelta.connectors.database import (
    PostgresPushdownSession,
    RedshiftPushdownSession,
    TSqlPushdownSession,
)
from veridelta.connectors.duckdb import DuckDBPushdownSession
from veridelta.connectors.sql import SQLDialect
from veridelta.connectors.warehouse import (
    BigQueryConnector,
    DatabricksConnector,
    SnowflakeConnector,
)
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DuckDBConfig,
    SnowflakeConfig,
    SourceRef,
)

_T = TypeVar("_T")


_WarehouseConfig: TypeAlias = (
    SnowflakeConfig | DatabricksConfig | BigQueryConfig | DatabaseConfig | DuckDBConfig
)
"""Connection configs whose comparisons compile to SQL and run in place."""


@dataclass(frozen=True)
class _Warehouse:
    """How the engine identifies and opens one warehouse backend."""

    name: str
    dialect: SQLDialect
    session: Callable[[Any], PushdownSession]


_WAREHOUSES: Final[dict[type[object], _Warehouse]] = {
    # The lambdas look the connector class up when a session opens, so a test
    # that patches `veridelta._warehouses.SnowflakeConnector` still intercepts it.
    SnowflakeConfig: _Warehouse(
        "Snowflake",
        SQLDialect.SNOWFLAKE,
        lambda config: SnowflakeConnector(config),
    ),
    DatabricksConfig: _Warehouse(
        "Databricks",
        SQLDialect.DATABRICKS,
        lambda config: DatabricksConnector(config),
    ),
    BigQueryConfig: _Warehouse(
        "BigQuery",
        SQLDialect.BIGQUERY,
        lambda config: BigQueryConnector(config),
    ),
    # Only a DuckDB source that sets `pushdown` is routed here.
    DuckDBConfig: _Warehouse(
        "DuckDB",
        SQLDialect.DUCKDB,
        lambda config: DuckDBPushdownSession(config),
    ),
}
"""Every warehouse the engine pushes comparisons down to, keyed by config type.

A database source is routed by its URI scheme instead, through `_DATABASE_WAREHOUSES`.
"""

_POSTGRES: Final = _Warehouse(
    "Postgres",
    SQLDialect.POSTGRES,
    lambda config: PostgresPushdownSession(config),
)

_DATABASE_WAREHOUSES: Final[dict[str, _Warehouse]] = {
    # Both spellings name one backend, so a pair may mix them.
    "postgres": _POSTGRES,
    "postgresql": _POSTGRES,
    # One dialect serves SQL Server, Azure SQL, and Synapse, which one scheme names.
    "mssql": _Warehouse(
        "SQL Server",
        SQLDialect.TSQL,
        lambda config: TSqlPushdownSession(config),
    ),
    "redshift": _Warehouse(
        "Redshift",
        SQLDialect.REDSHIFT,
        lambda config: RedshiftPushdownSession(config),
    ),
}
"""The backend a database source that sets `pushdown` runs on, keyed by URI scheme.

The keys equal `DATABASE_PUSHDOWN_SCHEMES`, which the model checks a URI against,
so every database source that sets `pushdown` has a backend here.
"""


def _warehouse_of(config: _WarehouseConfig) -> _Warehouse:
    """Return the backend one warehouse side runs on."""
    if isinstance(config, DatabaseConfig):
        return _DATABASE_WAREHOUSES[urlsplit(config.uri).scheme.lower()]
    return _WAREHOUSES[type(config)]


def is_warehouse(config: SourceRef) -> TypeGuard[_WarehouseConfig]:
    """Return whether a source reference is a warehouse connection."""
    if isinstance(config, (DatabaseConfig, DuckDBConfig)):
        return config.pushdown
    return type(config) in _WAREHOUSES


_MIXED_BACKENDS: Final = "Mixed file/lakehouse/database and warehouse backends are unsupported."


_HALF_PUSHDOWN: Final = (
    "Set pushdown on both sides to compare the tables where they are stored, "
    "or on neither to read them and compare locally."
)


def table_name(config: _WarehouseConfig) -> str:
    """Return the table a pushdown side names."""
    # `DatabaseConfig` and `DuckDBConfig` require a `table` whenever they set `pushdown`.
    return cast("str", config.table)


@dataclass(frozen=True)
class WarehousePair:
    """Two warehouse tables cleared to share one pushdown session."""

    warehouse: _Warehouse
    source: _WarehouseConfig
    target: _WarehouseConfig

    def with_session(self, work: Callable[[PushdownSession, str, str], _T]) -> _T:
        """Open one session, run `work` on it, and close it whatever happens."""
        with warehouse_session(self.source) as session:
            return work(session, table_name(self.source), table_name(self.target))


@contextmanager
def warehouse_session(config: _WarehouseConfig) -> Generator[PushdownSession]:
    """Connect to the warehouse one side names, and close the session whatever happens."""
    session = _warehouse_of(config).session(config)
    session.connect()
    try:
        yield session
    finally:
        session.close()


def check_backend_pairing(source: SourceRef, target: SourceRef) -> WarehousePair | None:
    """Refuse a pair no engine can compare, without connecting to anything."""
    # Two sides of one kind disagree only when a database or DuckDB pair half sets `pushdown`.
    if type(source) is type(target) and is_warehouse(source) != is_warehouse(target):
        raise ConfigError(_HALF_PUSHDOWN)
    if not is_warehouse(source):
        if is_warehouse(target):
            raise ConnectorError(_MIXED_BACKENDS)
        return None
    if not is_warehouse(target):
        raise ConnectorError(_MIXED_BACKENDS)
    warehouse = _warehouse_of(source)
    target_warehouse = _warehouse_of(target)
    if target_warehouse is not warehouse:
        raise ConnectorError(
            "Cross-dialect warehouse pushdown is unsupported: the source is "
            f"{warehouse.name} and the target is {target_warehouse.name}. Source and "
            "target must use the same warehouse connection."
        )
    # Compared only for equality: the dumps hold passwords and tokens.
    if source.model_dump(exclude={"table"}) != target.model_dump(exclude={"table"}):
        raise ConnectorError(
            "Cross-account warehouse pushdown is unsupported. "
            f"Source and target {warehouse.name} connections must match."
        )
    # One connection by now, so one name is one table, and it would always match.
    if source.table == target.table:
        raise ConfigError(
            f"Source and target both name the same table '{source.table}' on one "
            "connection, so the comparison could only ever match. Point one side "
            "at the table it should be compared with."
        )
    return WarehousePair(warehouse, source, target)
