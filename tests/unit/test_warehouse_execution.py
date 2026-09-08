# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse connector execution over mocked drivers."""

from typing import Any

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.connectors import DatabricksConnector, SnowflakeConnector
from veridelta.connectors.sql import SQLDialect
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabricksConfig, DiffRule, SnowflakeConfig


def _snowflake_config() -> SnowflakeConfig:
    """Build a minimal valid Snowflake configuration."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        password="secret",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        role="SYSADMIN",
        table="ANALYTICS.PUBLIC.LEGACY_EVENTS",
    )


def _databricks_config() -> DatabricksConfig:
    """Build a minimal valid Databricks configuration."""
    return DatabricksConfig(
        server_hostname="adb.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="dapi",
        catalog="main",
        schema_name="default",
        table="main.default.legacy_events",
    )


def _arrow_table() -> Any:
    """Return a small Arrow table for mocked cursor fetches."""
    return pl.DataFrame({"id": [1, 2], "status": ["open", "closed"]}).to_arrow()


def _patch_snowflake_session(mocker: MockerFixture, table: Any) -> tuple[Any, Any]:
    """Patch the Snowflake driver and return the mocked session and cursor."""
    cursor = mocker.MagicMock()
    cursor.fetch_arrow_all.return_value = table
    session = mocker.MagicMock()
    session.cursor.return_value = cursor
    driver = mocker.MagicMock()
    driver.connect.return_value = session
    mocker.patch("veridelta.connectors.warehouse.snowflake_connector", driver)
    return session, cursor


def _patch_databricks_session(mocker: MockerFixture, table: Any) -> tuple[Any, Any]:
    """Patch the Databricks driver and return the mocked session and cursor."""
    cursor = mocker.MagicMock()
    cursor.fetchall_arrow.return_value = table
    session = mocker.MagicMock()
    session.cursor.return_value = cursor
    driver = mocker.MagicMock()
    driver.connect.return_value = session
    mocker.patch("veridelta.connectors.warehouse.databricks_sql", driver)
    return session, cursor


@pytest.mark.unit
@pytest.mark.fast
class TestSnowflakeExecution:
    """Validate Snowflake connect, pushdown, and schema over mocked Arrow."""

    def test_it_maps_config_fields_and_executes_compiled_sql(self, mocker: MockerFixture) -> None:
        """Ensure connect kwargs and execute_pushdown pass compiler SQL to the cursor."""
        table = _arrow_table()
        _session, cursor = _patch_snowflake_session(mocker, table)
        connector = SnowflakeConnector(_snowflake_config())
        assert connector.compiler.dialect is SQLDialect.SNOWFLAKE

        connector.connect()
        statement = connector.compiler.compile_query(
            "analytics.public.source_orders",
            "analytics.public.target_orders",
            ["id"],
            [DiffRule(column_names=["status"])],
        )
        result = connector.execute_pushdown(statement)

        from veridelta.connectors.warehouse import snowflake_connector

        snowflake_connector.connect.assert_called_once_with(
            account="xy12345",
            user="analyst",
            password="secret",
            warehouse="COMPUTE_WH",
            database="ANALYTICS",
            schema="PUBLIC",
            role="SYSADMIN",
        )
        cursor.execute.assert_called_once_with(statement)
        cursor.fetch_arrow_all.assert_called_once()
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.columns == ["id", "status"]
        assert collected.height == 2

    def test_it_executes_count_and_probe_statements_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure count and schema round-trips reach the cursor unmodified."""
        count_table = pl.DataFrame({"_veridelta_total": [4096]}).to_arrow()
        _session, cursor = _patch_snowflake_session(mocker, count_table)
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        count_sql = connector.compiler.compile_count_query("analytics.public.source_orders")
        probe_sql = connector.compiler.compile_schema_probe_query("analytics.public.source_orders")
        count_frame = connector.execute_pushdown(count_sql, query_type="count")
        connector.execute_pushdown(probe_sql, query_type="schema")

        assert [call.args[0] for call in cursor.execute.call_args_list] == [count_sql, probe_sql]
        assert count_frame.collect().item() == 4096

    def test_it_fetches_schema_with_a_limit_zero_query(self, mocker: MockerFixture) -> None:
        """Ensure fetch_schema wraps the last statement in LIMIT 0."""
        table = _arrow_table()
        _session, cursor = _patch_snowflake_session(mocker, table)
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()
        statement = connector.compiler.compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["status"])],
        )
        connector.execute_pushdown(statement)
        schema = connector.fetch_schema()

        schema_sql = cursor.execute.call_args_list[1].args[0]
        assert statement in schema_sql
        assert "LIMIT 0" in schema_sql
        assert "_veridelta_schema" in schema_sql
        assert schema.names() == ["id", "status"]

    def test_it_raises_when_not_connected(self, mocker: MockerFixture) -> None:
        """Ensure pushdown and schema require an open Snowflake session."""
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", mocker.MagicMock())
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

    def test_it_raises_when_fetching_schema_before_pushdown(self, mocker: MockerFixture) -> None:
        """Ensure schema introspection requires a prior execute_pushdown call."""
        _patch_snowflake_session(mocker, _arrow_table())
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with pytest.raises(ConnectorError, match="execute_pushdown"):
            connector.fetch_schema()

    def test_it_wraps_driver_connect_failures(self, mocker: MockerFixture) -> None:
        """Ensure Snowflake driver exceptions become ConnectorError."""
        driver = mocker.MagicMock()
        driver.connect.side_effect = RuntimeError("auth failed")
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", driver)
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match="Failed to connect to Snowflake"):
            connector.connect()

    def test_it_raises_when_arrow_payload_is_missing(self, mocker: MockerFixture) -> None:
        """Ensure a null Arrow fetch is rejected as a non-tabular result."""
        _session, cursor = _patch_snowflake_session(mocker, None)
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with pytest.raises(ConnectorError, match="tabular Arrow"):
            connector.execute_pushdown("SELECT 1")
        cursor.fetch_arrow_all.assert_called_once()


@pytest.mark.unit
@pytest.mark.fast
class TestDatabricksExecution:
    """Validate Databricks connect, pushdown, and schema over mocked Arrow."""

    def test_it_maps_config_fields_and_executes_compiled_sql(self, mocker: MockerFixture) -> None:
        """Ensure connect kwargs and execute_pushdown pass compiler SQL to the cursor."""
        table = _arrow_table()
        _session, cursor = _patch_databricks_session(mocker, table)
        connector = DatabricksConnector(_databricks_config())
        assert connector.compiler.dialect is SQLDialect.DATABRICKS

        connector.connect()
        statement = connector.compiler.compile_query(
            "main.default.source_orders",
            "main.default.target_orders",
            ["id"],
            [DiffRule(column_names=["status"])],
        )
        result = connector.execute_pushdown(statement)

        from veridelta.connectors.warehouse import databricks_sql

        databricks_sql.connect.assert_called_once_with(
            server_hostname="adb.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/abc",
            access_token="dapi",
            catalog="main",
            schema="default",
        )
        cursor.execute.assert_called_once_with(statement)
        cursor.fetchall_arrow.assert_called_once()
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().columns == ["id", "status"]

    def test_it_fetches_schema_with_a_limit_zero_query(self, mocker: MockerFixture) -> None:
        """Ensure fetch_schema wraps the last statement in LIMIT 0."""
        table = _arrow_table()
        _session, cursor = _patch_databricks_session(mocker, table)
        connector = DatabricksConnector(_databricks_config())
        connector.connect()
        statement = "SELECT `src`.`id` FROM `src_tbl` AS `src`"
        connector.execute_pushdown(statement)
        schema = connector.fetch_schema()

        schema_sql = cursor.execute.call_args_list[1].args[0]
        assert "LIMIT 0" in schema_sql
        assert schema.names() == ["id", "status"]

    def test_it_raises_when_not_connected(self, mocker: MockerFixture) -> None:
        """Ensure pushdown and schema require an open Databricks session."""
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", mocker.MagicMock())
        connector = DatabricksConnector(_databricks_config())

        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

    def test_it_raises_when_databricks_extra_is_missing(self, mocker: MockerFixture) -> None:
        """Ensure a missing Databricks driver raises an extra-install error."""
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", None)
        connector = DatabricksConnector(_databricks_config())

        with pytest.raises(ConnectorError, match="uv sync --extra databricks"):
            connector.connect()
