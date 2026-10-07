# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Warehouse pushdown, lakehouse-native, database, and DuckDB connector abstractions."""

from veridelta.connectors.base import (
    PushdownQueryType,
    PushdownSession,
    ReaderConnector,
    VerideltaConnector,
)
from veridelta.connectors.database import DatabaseConnector, PostgresPushdownSession
from veridelta.connectors.duckdb import DuckDBConnector, DuckDBPushdownSession
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler
from veridelta.connectors.warehouse import (
    BigQueryConnector,
    DatabricksConnector,
    SnowflakeConnector,
)

__all__ = [
    "BigQueryConnector",
    "DatabaseConnector",
    "DatabricksConnector",
    "DeltaLakeConnector",
    "DuckDBConnector",
    "DuckDBPushdownSession",
    "IcebergConnector",
    "PostgresPushdownSession",
    "PushdownQueryType",
    "PushdownSession",
    "ReaderConnector",
    "SQLDialect",
    "SQLPushdownCompiler",
    "SnowflakeConnector",
    "VerideltaConnector",
]
