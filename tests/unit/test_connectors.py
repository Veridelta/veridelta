# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for warehouse and lakehouse connector scaffolding."""

import polars as pl
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta.config import (
    DatabricksConfig,
    DeltaLakeConfig,
    IcebergConfig,
    SnowflakeConfig,
)
from veridelta.connectors import (
    DatabricksConnector,
    DeltaLakeConnector,
    IcebergConnector,
    SnowflakeConnector,
    VerideltaConnector,
)
from veridelta.exceptions import ConnectorError


def _snowflake_config() -> SnowflakeConfig:
    """Build a minimal valid Snowflake configuration."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
    )


def _databricks_config() -> DatabricksConfig:
    """Build a minimal valid Databricks configuration."""
    return DatabricksConfig(
        server_hostname="adb.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
    )


def _delta_config() -> DeltaLakeConfig:
    """Build a minimal valid Delta Lake configuration."""
    return DeltaLakeConfig(table_uri="s3://lake/events")


def _iceberg_config() -> IcebergConfig:
    """Build a minimal valid Iceberg configuration."""
    return IcebergConfig(table_uri="s3://lake/iceberg/events")


def _sample_lazy_frame() -> pl.LazyFrame:
    """Return a small unevaluated frame for mocked lakehouse scans."""
    return pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.0]}).lazy()


@pytest.mark.unit
@pytest.mark.fast
class TestConnectorConfigValidation:
    """Validate frozen, extra-forbid credential models."""

    def test_it_forbids_unrecognized_fields_on_snowflake_config(self) -> None:
        """Ensure typos in Snowflake settings raise ValidationError."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            SnowflakeConfig(
                account="xy12345",
                user="analyst",
                warehouse="COMPUTE_WH",
                database="ANALYTICS",
                schema_name="PUBLIC",
                region="us-east-1",  # type: ignore[call-arg]
            )

    def test_it_forbids_unrecognized_fields_on_databricks_config(self) -> None:
        """Ensure typos in Databricks settings raise ValidationError."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DatabricksConfig(
                server_hostname="adb.azuredatabricks.net",
                http_path="/sql/1.0/warehouses/abc",
                cluster_id="ignored",  # type: ignore[call-arg]
            )

    def test_it_forbids_unrecognized_fields_on_delta_and_iceberg_configs(self) -> None:
        """Ensure lakehouse configs reject unknown keys."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DeltaLakeConfig(table_uri="s3://lake/events", catalog="main")  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            IcebergConfig(table_uri="s3://lake/iceberg/events", catalog="main")  # type: ignore[call-arg]

    def test_it_rejects_mutation_on_frozen_connector_configs(self) -> None:
        """Ensure credential models are immutable after construction."""
        snowflake = _snowflake_config()
        databricks = _databricks_config()
        delta = _delta_config()
        iceberg = _iceberg_config()

        with pytest.raises(ValidationError, match="frozen"):
            snowflake.account = "other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            databricks.http_path = "/sql/other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            delta.table_uri = "s3://other"  # type: ignore[misc]
        with pytest.raises(ValidationError, match="frozen"):
            iceberg.table_uri = "s3://other"  # type: ignore[misc]


@pytest.mark.unit
@pytest.mark.fast
class TestConnectorInterface:
    """Validate ABC instantiation and warehouse stubs."""

    def test_it_cannot_instantiate_the_abstract_connector(self) -> None:
        """Ensure VerideltaConnector remains an abstract interface."""
        with pytest.raises(TypeError, match="abstract"):
            VerideltaConnector()  # type: ignore[abstract]

    def test_it_raises_connector_error_for_unimplemented_snowflake_methods(self) -> None:
        """Ensure Snowflake runtime methods stay blocked until extras exist."""
        connector = SnowflakeConnector(_snowflake_config())

        with pytest.raises(ConnectorError, match="Snowflake warehouse pushdown"):
            connector.connect()
        with pytest.raises(ConnectorError, match="Snowflake warehouse pushdown"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="Snowflake warehouse pushdown"):
            connector.fetch_schema()

    def test_it_raises_connector_error_for_unimplemented_databricks_methods(self) -> None:
        """Ensure Databricks runtime methods stay blocked until extras exist."""
        connector = DatabricksConnector(_databricks_config())

        with pytest.raises(ConnectorError, match="Databricks warehouse pushdown"):
            connector.connect()
        with pytest.raises(ConnectorError, match="Databricks warehouse pushdown"):
            connector.execute_pushdown("SELECT 1")
        with pytest.raises(ConnectorError, match="Databricks warehouse pushdown"):
            connector.fetch_schema()


@pytest.mark.unit
@pytest.mark.fast
class TestLakehouseConnectors:
    """Validate lazy Delta and Iceberg scan wiring without optional extras."""

    def test_it_raises_when_fetching_schema_before_connect(self) -> None:
        """Ensure schema reads require an established lazy-scan handle."""
        delta = DeltaLakeConnector(_delta_config())
        iceberg = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="not connected"):
            delta.fetch_schema()
        with pytest.raises(ConnectorError, match="not connected"):
            iceberg.fetch_schema()

    def test_it_rejects_sql_pushdown_on_lakehouse_connectors(self) -> None:
        """Ensure lakehouse backends do not accept warehouse SQL."""
        delta = DeltaLakeConnector(_delta_config())
        iceberg = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="warehouse-only"):
            delta.execute_pushdown("SELECT * FROM events")
        with pytest.raises(ConnectorError, match="warehouse-only"):
            iceberg.execute_pushdown("SELECT * FROM events")

    def test_it_connects_delta_via_scan_delta_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_delta and fetch_schema is lazy."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_delta", return_value=lazy)
        connector = DeltaLakeConnector(
            DeltaLakeConfig(
                table_uri="s3://lake/events",
                version=3,
                storage_options={"AWS_REGION": "us-east-1"},
            )
        )

        connector.connect()
        schema = connector.fetch_schema()

        scan.assert_called_once_with(
            "s3://lake/events",
            storage_options={"AWS_REGION": "us-east-1"},
            version=3,
        )
        assert schema.names() == ["id", "amount"]
        assert isinstance(lazy, pl.LazyFrame)

    def test_it_connects_iceberg_via_scan_iceberg_and_returns_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure connect() calls pl.scan_iceberg and fetch_schema is lazy."""
        lazy = _sample_lazy_frame()
        scan = mocker.patch("veridelta.connectors.lakehouse.pl.scan_iceberg", return_value=lazy)
        connector = IcebergConnector(
            IcebergConfig(
                table_uri="s3://lake/iceberg/events",
                storage_options={"AWS_REGION": "us-east-1"},
            )
        )

        connector.connect()
        schema = connector.fetch_schema()

        scan.assert_called_once_with(
            "s3://lake/iceberg/events",
            storage_options={"AWS_REGION": "us-east-1"},
        )
        assert schema.names() == ["id", "amount"]

    def test_it_wraps_missing_delta_extra_as_connector_error(self, mocker: MockerFixture) -> None:
        """Ensure ImportError from pl.scan_delta becomes ConnectorError."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta",
            side_effect=ImportError("deltalake is required"),
        )
        connector = DeltaLakeConnector(_delta_config())

        with pytest.raises(ConnectorError, match="deltalake"):
            connector.connect()

    def test_it_wraps_missing_iceberg_extra_as_connector_error(self, mocker: MockerFixture) -> None:
        """Ensure PolarsError from pl.scan_iceberg becomes ConnectorError."""
        mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_iceberg",
            side_effect=pl.exceptions.ComputeError("pyiceberg is required"),
        )
        connector = IcebergConnector(_iceberg_config())

        with pytest.raises(ConnectorError, match="pyiceberg"):
            connector.connect()
