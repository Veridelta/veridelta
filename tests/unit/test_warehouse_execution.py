# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse connector execution over mocked drivers."""

import logging
from collections.abc import Callable
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


def _snowflake_fetch_contract(table: Any) -> Callable[..., Any]:
    """Mimic `SnowflakeCursor.fetch_arrow_all`, including its zero-row behavior.

    The real driver returns None instead of an empty table unless the caller
    passes `force_return_table=True`, so a mock that always hands back the
    table would hide exactly the case every pushdown run starts with.
    """

    def fetch_arrow_all(force_return_table: bool = False) -> Any:
        if getattr(table, "num_rows", None) == 0 and not force_return_table:
            return None
        return table

    return fetch_arrow_all


def _patch_snowflake_session(mocker: MockerFixture, table: Any) -> tuple[Any, Any]:
    """Patch the Snowflake driver and return the mocked session and cursor."""
    cursor = mocker.MagicMock()
    cursor.fetch_arrow_all.side_effect = _snowflake_fetch_contract(table)
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
        cursor.fetch_arrow_all.assert_called_once_with(force_return_table=True)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.columns == ["id", "status"]
        assert collected.height == 2

    def test_it_returns_an_empty_frame_for_a_zero_row_result(self, mocker: MockerFixture) -> None:
        """Ensure an empty result keeps its columns instead of failing the run.

        Every pushdown run opens with a `WHERE 1 = 0` schema probe, and a clean
        comparison returns empty sets, so reading zero rows is the common case.
        """
        empty = pl.DataFrame(schema={"id": pl.Int64, "status": pl.String}).to_arrow()
        _session, cursor = _patch_snowflake_session(mocker, empty)
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        probe = connector.execute_pushdown(
            connector.compiler.compile_schema_probe_query("analytics.public.source_orders"),
            query_type="schema",
        )

        assert probe.collect_schema() == pl.Schema({"id": pl.Int64, "status": pl.String})
        assert probe.collect().height == 0
        cursor.fetch_arrow_all.assert_called_once_with(force_return_table=True)

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
        assert cursor.fetch_arrow_all.call_args_list[1].kwargs == {"force_return_table": True}

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

    def test_it_closes_the_session_once_and_requires_a_reconnect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure close() releases the driver session and resets pushdown state."""
        session, _cursor = _patch_snowflake_session(mocker, _arrow_table())
        connector = SnowflakeConnector(_snowflake_config())
        connector.close()  # before connect: nothing to release
        session.close.assert_not_called()

        connector.connect()
        connector.execute_pushdown("SELECT 1")
        connector.close()
        connector.close()  # idempotent

        session.close.assert_called_once()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

        connector.connect()
        # The prior statement was forgotten with the session it ran on.
        with pytest.raises(ConnectorError, match="execute_pushdown"):
            connector.fetch_schema()

    def test_it_closes_on_context_exit(self, mocker: MockerFixture) -> None:
        """Ensure the connector releases its session when a with-block ends."""
        session, cursor = _patch_snowflake_session(mocker, _arrow_table())

        with SnowflakeConnector(_snowflake_config()) as connector:
            connector.connect()
            connector.execute_pushdown("SELECT 1")

        session.close.assert_called_once()
        cursor.execute.assert_called_once_with("SELECT 1")

    def test_it_logs_a_failed_close_instead_of_raising(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure a driver error during close cannot mask a finished comparison."""
        session, _cursor = _patch_snowflake_session(mocker, _arrow_table())
        session.close.side_effect = RuntimeError("socket already gone")
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with caplog.at_level(logging.WARNING, logger="veridelta.connectors.warehouse"):
            connector.close()

        assert "Snowflake session did not close cleanly" in caplog.text
        assert "socket already gone" in caplog.text
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")

    def test_it_logs_lifecycle_by_query_type_without_sql_or_secrets(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure log lines name the backend and round-trip but never SQL or credentials."""
        _patch_snowflake_session(mocker, _arrow_table())
        connector = SnowflakeConnector(_snowflake_config())
        statement = "SELECT * FROM t WHERE status = 'O''Brien'"

        with caplog.at_level(logging.DEBUG, logger="veridelta.connectors.warehouse"):
            connector.connect()
            connector.execute_pushdown(statement, query_type="count")
            connector.fetch_schema()
            connector.close()

        messages = [record.getMessage() for record in caplog.records]
        assert "Connected to Snowflake account xy12345, warehouse COMPUTE_WH" in messages
        assert any(m.startswith("Snowflake count statement completed in") for m in messages)
        assert any(m.startswith("Snowflake schema statement completed in") for m in messages)
        assert "Closed Snowflake session" in messages
        assert "O''Brien" not in caplog.text
        assert "secret" not in caplog.text

    def test_it_rejects_a_non_tabular_arrow_payload(self, mocker: MockerFixture) -> None:
        """Ensure an Arrow array, rather than a table, is refused instead of half-wrapped."""
        _session, cursor = _patch_snowflake_session(mocker, pl.Series("id", [1, 2]).to_arrow())
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with pytest.raises(ConnectorError, match="tabular Arrow"):
            connector.execute_pushdown("SELECT 1")
        cursor.fetch_arrow_all.assert_called_once()

    @pytest.mark.parametrize(
        "schema_payload",
        [
            pytest.param(None, id="no-arrow"),
            pytest.param(pl.Series("id", [1]).to_arrow(), id="arrow-array-not-table"),
        ],
    )
    def test_it_falls_back_to_the_cursor_description_for_the_schema(
        self, mocker: MockerFixture, schema_payload: Any
    ) -> None:
        """Ensure fetch_schema still names the columns when the LIMIT 0 fetch yields no table."""
        _session, cursor = _patch_snowflake_session(mocker, _arrow_table())
        cursor.fetch_arrow_all.side_effect = [_arrow_table(), schema_payload]
        cursor.description = [("ID", 0), ("STATUS", 2)]
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()
        connector.execute_pushdown("SELECT 1")

        schema = connector.fetch_schema()

        assert schema.names() == ["ID", "STATUS"]
        assert set(schema.dtypes()) == {pl.String()}

    def test_it_raises_when_neither_arrow_nor_description_describes_the_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a schema fetch with nothing to read fails rather than returning empty."""
        _session, cursor = _patch_snowflake_session(mocker, _arrow_table())
        cursor.fetch_arrow_all.side_effect = [_arrow_table(), None]
        cursor.description = None
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()
        connector.execute_pushdown("SELECT 1")

        with pytest.raises(ConnectorError, match="did not return a schema"):
            connector.fetch_schema()

    def test_it_passes_connector_errors_from_the_driver_layer_through_unwrapped(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a ConnectorError raised below is not re-wrapped with a generic prefix."""
        session, cursor = _patch_snowflake_session(mocker, _arrow_table())
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()
        cursor.execute.side_effect = ConnectorError("quota exhausted")

        with pytest.raises(ConnectorError, match=r"^quota exhausted$"):
            connector.execute_pushdown("SELECT 1")

        from veridelta.connectors.warehouse import snowflake_connector

        snowflake_connector.connect.side_effect = ConnectorError("blocked by policy")
        with pytest.raises(ConnectorError, match=r"^blocked by policy$"):
            SnowflakeConnector(_snowflake_config()).connect()
        session.close.assert_not_called()

    def test_it_tolerates_a_session_without_a_close_method(self, mocker: MockerFixture) -> None:
        """Ensure close() copes with a driver session object that has nothing to close."""
        session, _cursor = _patch_snowflake_session(mocker, _arrow_table())
        del session.close
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        connector.close()

        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")

    def test_it_logs_a_warning_when_a_statement_fails(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure a driver failure is logged by round-trip and re-raised with its cause."""
        _session, cursor = _patch_snowflake_session(mocker, _arrow_table())
        cursor.execute.side_effect = RuntimeError("SQL compilation error")
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with (
            caplog.at_level(logging.WARNING, logger="veridelta.connectors.warehouse"),
            pytest.raises(ConnectorError, match="SQL compilation error"),
        ):
            connector.execute_pushdown("SELECT mismatch", query_type="mismatch")

        assert any(
            m.startswith("Snowflake mismatch statement failed after")
            for m in (record.getMessage() for record in caplog.records)
        )
        cursor.close.assert_called_once()


@pytest.mark.unit
@pytest.mark.fast
class TestDatabricksExecution:
    """Validate Databricks connect, pushdown, and schema over mocked Arrow."""

    def test_it_executes_count_and_probe_statements_verbatim(self, mocker: MockerFixture) -> None:
        """Ensure count and schema round-trips reach the cursor unmodified."""
        count_table = pl.DataFrame({"_veridelta_total": [4096]}).to_arrow()
        _session, cursor = _patch_databricks_session(mocker, count_table)
        connector = DatabricksConnector(_databricks_config())
        connector.connect()

        count_sql = connector.compiler.compile_count_query("main.default.source_orders")
        probe_sql = connector.compiler.compile_schema_probe_query("main.default.source_orders")
        count_frame = connector.execute_pushdown(count_sql, query_type="count")
        connector.execute_pushdown(probe_sql, query_type="schema")

        assert [call.args[0] for call in cursor.execute.call_args_list] == [count_sql, probe_sql]
        assert count_frame.collect().item() == 4096

    def test_it_raises_when_fetching_schema_before_pushdown(self, mocker: MockerFixture) -> None:
        """Ensure schema introspection requires a prior execute_pushdown call."""
        _patch_databricks_session(mocker, _arrow_table())
        connector = DatabricksConnector(_databricks_config())
        connector.connect()

        with pytest.raises(ConnectorError, match="execute_pushdown"):
            connector.fetch_schema()

    def test_it_wraps_driver_connect_failures(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure Databricks driver exceptions become ConnectorError and are logged."""
        driver = mocker.MagicMock()
        driver.connect.side_effect = RuntimeError("token rejected")
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", driver)
        connector = DatabricksConnector(_databricks_config())

        with (
            caplog.at_level(logging.WARNING, logger="veridelta.connectors.warehouse"),
            pytest.raises(ConnectorError, match="Failed to connect to Databricks: token rejected"),
        ):
            connector.connect()

        assert "Databricks connection to adb.azuredatabricks.net failed" in caplog.text
        assert "dapi" not in caplog.text

    def test_it_raises_when_arrow_payload_is_missing(self, mocker: MockerFixture) -> None:
        """Ensure a null Arrow fetch is rejected as a non-tabular result."""
        _session, cursor = _patch_databricks_session(mocker, None)
        connector = DatabricksConnector(_databricks_config())
        connector.connect()

        with pytest.raises(ConnectorError, match="tabular Arrow"):
            connector.execute_pushdown("SELECT 1")
        cursor.fetchall_arrow.assert_called_once()

    def test_it_passes_a_connector_error_from_connect_through_unwrapped(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a ConnectorError from the driver layer keeps its own message."""
        driver = mocker.MagicMock()
        driver.connect.side_effect = ConnectorError("workspace suspended")
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", driver)

        with pytest.raises(ConnectorError, match=r"^workspace suspended$"):
            DatabricksConnector(_databricks_config()).connect()

    def test_it_closes_the_session_once_and_requires_a_reconnect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure close() releases the driver session and resets pushdown state."""
        session, _cursor = _patch_databricks_session(mocker, _arrow_table())
        connector = DatabricksConnector(_databricks_config())

        connector.connect()
        connector.execute_pushdown("SELECT 1")
        connector.close()
        connector.close()

        session.close.assert_called_once()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="not connected"):
            connector.fetch_schema()

    def test_it_logs_lifecycle_by_query_type_without_sql_or_secrets(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure Databricks log lines mirror Snowflake's: backend, round-trip, timing only."""
        _patch_databricks_session(mocker, _arrow_table())
        connector = DatabricksConnector(_databricks_config())

        with caplog.at_level(logging.DEBUG, logger="veridelta.connectors.warehouse"):
            connector.connect()
            connector.execute_pushdown("SELECT `id` FROM `t`", query_type="added")
            connector.close()

        messages = [record.getMessage() for record in caplog.records]
        assert (
            "Connected to Databricks host adb.azuredatabricks.net, path /sql/1.0/warehouses/abc"
            in messages
        )
        assert any(m.startswith("Databricks added statement completed in") for m in messages)
        assert "Closed Databricks session" in messages
        assert "FROM `t`" not in caplog.text
        assert "dapi" not in caplog.text

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

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[databricks\]'"):
            connector.connect()
