# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Warehouse pushdown connectors for Snowflake and Databricks.

These classes define the execution surface for out-of-core semantic diffs.
SDK imports stay out of this module until optional extras exist.
"""

import polars as pl

from veridelta.connectors.base import VerideltaConnector
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabricksConfig, SnowflakeConfig

_SNOWFLAKE_UNIMPLEMENTED = "Snowflake warehouse pushdown is not implemented yet."
_DATABRICKS_UNIMPLEMENTED = "Databricks warehouse pushdown is not implemented yet."


class SnowflakeConnector(VerideltaConnector):
    """Snowflake SQL warehouse connector (scaffold).

    Stores an immutable `SnowflakeConfig`. Runtime methods raise
    `ConnectorError` until a native driver extra is wired.
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        """Initialize the connector with validated Snowflake settings.

        Args:
            config (SnowflakeConfig): Frozen account, warehouse, and database
                settings.
        """
        self._config = config

    def connect(self) -> None:
        """Open a Snowflake session for subsequent pushdown statements.

        Raises:
            ConnectorError: Always; the Snowflake extra is not packaged yet.
        """
        raise ConnectorError(_SNOWFLAKE_UNIMPLEMENTED)

    def execute_pushdown(self, statement: str) -> pl.LazyFrame:
        """Translate semantic rules into Snowflake SQL and return a LazyFrame.

        A future translator will map `DiffRule` fields onto warehouse compute:
        - `absolute_tolerance` / `relative_tolerance` → `ABS(src.col - tgt.col)`
          and relative predicates evaluated in the virtual warehouse
        - `value_map` → `IFF` / `DECODE` crosswalks
        - `regex_replace` → `REGEXP_REPLACE`
        - `null_values` / `treat_null_as_equal` → `NULLIF` / `EQUAL_NULL`

        Args:
            statement (str): Dialect-specific SQL produced by the translator.

        Returns:
            pl.LazyFrame: Never returned until the extra is implemented.

        Raises:
            ConnectorError: Always; pushdown SQL is not executed yet.
        """
        _ = statement
        raise ConnectorError(_SNOWFLAKE_UNIMPLEMENTED)

    def fetch_schema(self) -> pl.Schema:
        """Describe the active Snowflake result schema without a full extract.

        Raises:
            ConnectorError: Always; schema introspection is not implemented yet.
        """
        raise ConnectorError(_SNOWFLAKE_UNIMPLEMENTED)


class DatabricksConnector(VerideltaConnector):
    """Databricks SQL warehouse connector (scaffold).

    Stores an immutable `DatabricksConfig`. Runtime methods raise
    `ConnectorError` until a native driver extra is wired.
    """

    def __init__(self, config: DatabricksConfig) -> None:
        """Initialize the connector with validated Databricks settings.

        Args:
            config (DatabricksConfig): Frozen workspace hostname and HTTP path.
        """
        self._config = config

    def connect(self) -> None:
        """Open a Databricks SQL session for subsequent pushdown statements.

        Raises:
            ConnectorError: Always; the Databricks extra is not packaged yet.
        """
        raise ConnectorError(_DATABRICKS_UNIMPLEMENTED)

    def execute_pushdown(self, statement: str) -> pl.LazyFrame:
        """Translate semantic rules into Databricks SQL and return a LazyFrame.

        A future translator will map `DiffRule` fields onto warehouse compute:
        - `absolute_tolerance` / `relative_tolerance` → `ABS(src.col - tgt.col)`
          and relative predicates executed on the SQL warehouse
        - `value_map` → `CASE` crosswalks
        - `regex_replace` → `REGEXP_REPLACE`
        - `null_values` / `treat_null_as_equal` → `NULLIF` / `<=>` null-safe eq

        Args:
            statement (str): Dialect-specific SQL produced by the translator.

        Returns:
            pl.LazyFrame: Never returned until the extra is implemented.

        Raises:
            ConnectorError: Always; pushdown SQL is not executed yet.
        """
        _ = statement
        raise ConnectorError(_DATABRICKS_UNIMPLEMENTED)

    def fetch_schema(self) -> pl.Schema:
        """Describe the active Databricks result schema without a full extract.

        Raises:
            ConnectorError: Always; schema introspection is not implemented yet.
        """
        raise ConnectorError(_DATABRICKS_UNIMPLEMENTED)
