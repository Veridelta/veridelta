# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Warehouse pushdown, lakehouse-native, and database connector abstractions."""

from veridelta.connectors.base import PushdownQueryType, VerideltaConnector
from veridelta.connectors.database import DatabaseConnector
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler
from veridelta.connectors.warehouse import DatabricksConnector, SnowflakeConnector

__all__ = [
    "DatabaseConnector",
    "DatabricksConnector",
    "DeltaLakeConnector",
    "IcebergConnector",
    "PushdownQueryType",
    "SQLDialect",
    "SQLPushdownCompiler",
    "SnowflakeConnector",
    "VerideltaConnector",
]
