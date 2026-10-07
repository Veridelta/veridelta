# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse connector execution over mocked drivers."""

import logging
import traceback
from collections.abc import Callable
from typing import Any

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.connectors import DatabricksConnector, SnowflakeConnector
from veridelta.connectors.sql import SQLDialect
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabricksConfig, DiffRule, SnowflakeConfig

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


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


_CREDENTIAL = "hunter2-do-not-print"
"""Credential that must never appear in an error message."""


def _snowflake_key_pair_config(passphrase: str | None) -> SnowflakeConfig:
    """Build a Snowflake configuration that signs in with a key file."""
    return SnowflakeConfig(
        account="xy12345",
        user="SVC_VERIDELTA",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table="ANALYTICS.PUBLIC.LEGACY_EVENTS",
        private_key_path="/keys/rsa_key.p8",
        private_key_passphrase=passphrase,
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

    def test_it_raises_when_not_connected(self, mocker: MockerFixture) -> None:
        """Ensure pushdown requires an open Snowflake session."""
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", mocker.MagicMock())
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")

    def test_it_wraps_driver_connect_failures(self, mocker: MockerFixture) -> None:
        """Ensure Snowflake driver exceptions become ConnectorError."""
        driver = mocker.MagicMock()
        driver.connect.side_effect = RuntimeError("auth failed")
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", driver)
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match="Failed to connect to Snowflake"):
            connector.connect()

    @pytest.mark.parametrize(
        ("passphrase", "unlock"),
        [(None, {}), ("open-sesame", {"private_key_file_pwd": "open-sesame"})],
        ids=["unencrypted", "encrypted"],
    )
    def test_it_signs_in_with_a_key_pair(
        self, mocker: MockerFixture, passphrase: str | None, unlock: dict[str, str]
    ) -> None:
        """Ensure a key file reaches the driver in place of a password.

        The driver signs in with a key pair whenever it is given a key file, so
        no `password` argument goes with it.
        """
        _patch_snowflake_session(mocker, _arrow_table())

        SnowflakeConnector(_snowflake_key_pair_config(passphrase)).connect()

        from veridelta.connectors.warehouse import snowflake_connector

        snowflake_connector.connect.assert_called_once_with(
            account="xy12345",
            user="SVC_VERIDELTA",
            warehouse="COMPUTE_WH",
            database="ANALYTICS",
            schema="PUBLIC",
            role=None,
            private_key_file="/keys/rsa_key.p8",
            **unlock,
        )

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(
                lambda: _snowflake_config().model_copy(update={"password": _CREDENTIAL}),
                id="password",
            ),
            pytest.param(lambda: _snowflake_key_pair_config(_CREDENTIAL), id="passphrase"),
        ],
    )
    def test_it_masks_credentials_in_connection_errors(
        self, mocker: MockerFixture, build: Callable[[], SnowflakeConfig]
    ) -> None:
        """Ensure a driver error that repeats a credential never shows it.

        The message keeps the driver's reason with the credential masked. The
        driver's own exception is not chained, since a traceback prints it whole.
        """
        driver = mocker.MagicMock()
        driver.connect.side_effect = RuntimeError(f"could not sign in with {_CREDENTIAL}")
        mocker.patch("veridelta.connectors.warehouse.snowflake_connector", driver)

        with pytest.raises(ConnectorError) as exc_info:
            SnowflakeConnector(build()).connect()

        assert str(exc_info.value) == "Failed to connect to Snowflake: could not sign in with ***"
        assert _CREDENTIAL not in "".join(traceback.format_exception(exc_info.value))
        assert exc_info.value.__cause__ is None

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
        """Ensure close() releases the driver session, refuses work, and allows a reconnect."""
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

        connector.connect()
        connector.execute_pushdown("SELECT 1")

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
        """Ensure a driver error during close cannot mask a finished comparison.

        The warning names only the error's type, since `--verbose` prints it and
        a driver's text can echo connection details. The traceback is a debug record.
        """
        session, _cursor = _patch_snowflake_session(mocker, _arrow_table())
        session.close.side_effect = RuntimeError("socket already gone")
        connector = SnowflakeConnector(_snowflake_config())
        connector.connect()

        with caplog.at_level(logging.DEBUG, logger="veridelta.connectors.warehouse"):
            connector.close()

        by_level = {record.levelno: record for record in caplog.records}
        warning, debug = by_level[logging.WARNING], by_level[logging.DEBUG]
        assert warning.getMessage() == "Snowflake session did not close cleanly: RuntimeError"
        assert warning.exc_info is None
        assert debug.exc_info is not None
        assert "socket already gone" in str(debug.exc_info[1])
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")

    def test_it_logs_lifecycle_by_query_type_without_sql_or_secrets(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure INFO lines, which `--verbose` prints, name each round-trip but no SQL or secret."""
        _patch_snowflake_session(mocker, _arrow_table())
        connector = SnowflakeConnector(_snowflake_config())
        statement = "SELECT * FROM t WHERE status = 'O''Brien'"

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.warehouse"):
            connector.connect()
            connector.execute_pushdown(statement, query_type="count")
            connector.execute_pushdown(statement, query_type="schema")
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
        """Ensure close() releases the driver session and refuses work afterwards."""
        session, _cursor = _patch_databricks_session(mocker, _arrow_table())
        connector = DatabricksConnector(_databricks_config())

        connector.connect()
        connector.execute_pushdown("SELECT 1")
        connector.close()
        connector.close()

        session.close.assert_called_once()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")

    def test_it_logs_lifecycle_by_query_type_without_sql_or_secrets(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure Databricks log lines mirror Snowflake's: backend, round-trip, timing only."""
        _patch_databricks_session(mocker, _arrow_table())
        connector = DatabricksConnector(_databricks_config())

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.warehouse"):
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

    def test_it_raises_when_not_connected(self, mocker: MockerFixture) -> None:
        """Ensure pushdown requires an open Databricks session."""
        mocker.patch("veridelta.connectors.warehouse.databricks_sql", mocker.MagicMock())
        connector = DatabricksConnector(_databricks_config())

        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
