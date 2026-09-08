# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for source-union routing, lakehouse loads, and warehouse pushdown."""

from pathlib import Path
from typing import Any

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.config import load_config
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)


def _snowflake_config(*, table: str, account: str = "xy12345") -> SnowflakeConfig:
    """Build a Snowflake source with a distinct table name."""
    return SnowflakeConfig(
        account=account,
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table=table,
    )


def _databricks_config(*, table: str, host: str = "adb.azuredatabricks.net") -> DatabricksConfig:
    """Build a Databricks source with a distinct table name."""
    return DatabricksConfig(
        server_hostname=host,
        http_path="/sql/1.0/warehouses/abc",
        table=table,
    )


def _frame_with_ids(height: int) -> pl.LazyFrame:
    """Return a LazyFrame whose height matches the requested row count."""
    return pl.DataFrame({"id": list(range(height))}).lazy()


def _pushdown_by_query_type(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
    """Return distinct-height frames so DiffSummary counts are independently asserted."""
    _ = statement
    heights = {"mismatch": 2, "added": 3, "missing": 4}
    return _frame_with_ids(heights[query_type])


def _configure_warehouse_compiler(connector: Any) -> None:
    """Stub compile_* helpers with distinct SQL strings for call assertions."""
    connector.compiler.compile_query.return_value = "SELECT mismatch"
    connector.compiler.compile_added_query.return_value = "SELECT added"
    connector.compiler.compile_missing_query.return_value = "SELECT missing"
    connector.execute_pushdown.side_effect = _pushdown_by_query_type


@pytest.mark.integration
class TestEngineConnectorRouting:
    """Validate LoaderFactory lakehouse scans and DiffEngine warehouse routing."""

    def test_it_loads_delta_config_as_lazyframe_after_mocked_connect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure Delta configs scan through the public lazyframe() API."""
        frame = pl.DataFrame({"id": [1, 2], "amount": [10.0, 20.0]}).lazy()
        mocker.patch.object(DeltaLakeConnector, "connect")
        mocker.patch.object(DeltaLakeConnector, "lazyframe", return_value=frame)
        loaded = LoaderFactory.load(DeltaLakeConfig(table_uri="s3://lake/events"))

        assert loaded.collect().equals(frame.collect())

    def test_it_loads_iceberg_config_as_lazyframe_after_mocked_connect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure Iceberg configs scan through the public lazyframe() API."""
        frame = pl.DataFrame({"id": [3], "amount": [30.0]}).lazy()
        mocker.patch.object(IcebergConnector, "connect")
        mocker.patch.object(IcebergConnector, "lazyframe", return_value=frame)
        loaded = LoaderFactory.load(IcebergConfig(table_uri="s3://lake/iceberg/events"))

        assert loaded.collect().equals(frame.collect())

    def test_it_rejects_warehouse_configs_on_loader_factory(self) -> None:
        """Ensure Snowflake sources cannot be scanned as local LazyFrames."""
        with pytest.raises(ConnectorError, match="Warehouse sources cannot be loaded"):
            LoaderFactory.load(_snowflake_config(table="ANALYTICS.PUBLIC.SRC"))

    def test_it_pushdown_executes_matching_snowflake_fingerprints_without_file_loaders(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure same-account Snowflake pairs compile SQL and skip DataIngestor."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        ingestor_cls = mocker.patch("veridelta.engine.DataIngestor")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        source = _snowflake_config(table="ANALYTICS.PUBLIC.SRC")
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT")
        diff = DiffConfig(primary_keys=["id"])

        summary = DiffEngine.run_from_configs(diff, source, target)

        ingestor_cls.assert_not_called()
        connector.connect.assert_called_once()
        connector.compiler.compile_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC", "ANALYTICS.PUBLIC.TGT", ["id"], []
        )
        connector.compiler.compile_added_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC", "ANALYTICS.PUBLIC.TGT", ["id"]
        )
        connector.compiler.compile_missing_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC", "ANALYTICS.PUBLIC.TGT", ["id"]
        )
        assert connector.execute_pushdown.call_count == 3
        connector.execute_pushdown.assert_any_call("SELECT mismatch", query_type="mismatch")
        connector.execute_pushdown.assert_any_call("SELECT added", query_type="added")
        connector.execute_pushdown.assert_any_call("SELECT missing", query_type="missing")
        assert summary.changed_count == 2
        assert summary.added_count == 3
        assert summary.removed_count == 4
        assert summary.is_match is False

    def test_it_pushdown_executes_matching_databricks_fingerprints_without_file_loaders(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure same-workspace Databricks pairs compile SQL and skip DataIngestor."""
        connector_cls = mocker.patch("veridelta.engine.DatabricksConnector")
        ingestor_cls = mocker.patch("veridelta.engine.DataIngestor")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        source = _databricks_config(table="main.default.src")
        target = _databricks_config(table="main.default.tgt")
        diff = DiffConfig(primary_keys=["id"])

        summary = DiffEngine.run_from_configs(diff, source, target)

        ingestor_cls.assert_not_called()
        connector.connect.assert_called_once()
        connector.compiler.compile_query.assert_called_once_with(
            "main.default.src", "main.default.tgt", ["id"], []
        )
        connector.compiler.compile_added_query.assert_called_once_with(
            "main.default.src", "main.default.tgt", ["id"]
        )
        connector.compiler.compile_missing_query.assert_called_once_with(
            "main.default.src", "main.default.tgt", ["id"]
        )
        assert connector.execute_pushdown.call_count == 3
        connector.execute_pushdown.assert_any_call("SELECT mismatch", query_type="mismatch")
        connector.execute_pushdown.assert_any_call("SELECT added", query_type="added")
        connector.execute_pushdown.assert_any_call("SELECT missing", query_type="missing")
        assert summary.changed_count == 2
        assert summary.added_count == 3
        assert summary.removed_count == 4

    def test_it_raises_connector_error_for_cross_dialect_warehouse_pairs(self) -> None:
        """Ensure Snowflake source plus Databricks target is rejected."""
        source = _snowflake_config(table="ANALYTICS.PUBLIC.SRC")
        target = _databricks_config(table="main.default.tgt")
        diff = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConnectorError, match="Cross-dialect"):
            DiffEngine.run_from_configs(diff, source, target)

    def test_it_raises_connector_error_for_mismatched_snowflake_fingerprints(self) -> None:
        """Ensure distinct Snowflake accounts cannot share a pushdown session."""
        source = _snowflake_config(table="ANALYTICS.PUBLIC.SRC", account="acct_a")
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT", account="acct_b")
        diff = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConnectorError, match="Cross-account"):
            DiffEngine.run_from_configs(diff, source, target)

    def test_it_raises_connector_error_for_mixed_warehouse_and_file_backends(self) -> None:
        """Ensure a warehouse cannot be compared directly to a local file."""
        source = _snowflake_config(table="ANALYTICS.PUBLIC.SRC")
        target = SourceConfig(path="local.csv", format="csv")
        diff = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConnectorError, match="Mixed file/lakehouse"):
            DiffEngine.run_from_configs(diff, source, target)

    def test_it_ingests_file_sources_through_run_from_configs(self, tmp_path: Path) -> None:
        """Ensure file pairs still evaluate locally via DataIngestor and DiffEngine.run."""
        src_file = tmp_path / "source.csv"
        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1], "val": ["A"]}).write_csv(src_file)
        pl.DataFrame({"id": [1], "val": ["B"]}).write_csv(tgt_file)

        source = SourceConfig(path=str(src_file), format="csv")
        target = SourceConfig(path=str(tgt_file), format="csv")
        summary = DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        assert summary.is_match is False
        assert summary.changed_count == 1

    def test_it_round_trips_file_yaml_when_type_is_omitted(self, tmp_path: Path) -> None:
        """Ensure existing file YAML remains valid without an explicit type."""
        config_path = tmp_path / "file.yaml"
        config_path.write_text(
            "source:\n"
            "  path: legacy.csv\n"
            "  format: csv\n"
            "target:\n"
            "  path: modern.parquet\n"
            "  format: parquet\n"
            "primary_keys:\n"
            "  - id\n"
        )

        diff_cfg, source, target = load_config(config_path)

        assert isinstance(source, SourceConfig)
        assert isinstance(target, SourceConfig)
        assert source.type == "file"
        assert target.type == "file"
        assert source.path == "legacy.csv"
        assert target.format == "parquet"
        assert diff_cfg.primary_keys == ["id"]

    def test_it_round_trips_delta_and_snowflake_typed_yaml_blocks(self, tmp_path: Path) -> None:
        """Ensure discriminated YAML blocks map to lakehouse and warehouse models."""
        delta_path = tmp_path / "delta.yaml"
        delta_path.write_text(
            "source:\n"
            "  type: delta\n"
            "  table_uri: s3://lake/legacy\n"
            "target:\n"
            "  type: delta\n"
            "  table_uri: s3://lake/modern\n"
            "primary_keys:\n"
            "  - id\n"
        )
        snowflake_path = tmp_path / "snowflake.yaml"
        snowflake_path.write_text(
            "source:\n"
            "  type: snowflake\n"
            "  table: ANALYTICS.PUBLIC.SRC\n"
            "  account: xy12345\n"
            "  user: analyst\n"
            "  warehouse: COMPUTE_WH\n"
            "  database: ANALYTICS\n"
            "  schema_name: PUBLIC\n"
            "target:\n"
            "  type: snowflake\n"
            "  table: ANALYTICS.PUBLIC.TGT\n"
            "  account: xy12345\n"
            "  user: analyst\n"
            "  warehouse: COMPUTE_WH\n"
            "  database: ANALYTICS\n"
            "  schema_name: PUBLIC\n"
            "primary_keys:\n"
            "  - id\n"
        )

        _, delta_source, delta_target = load_config(delta_path)
        _, snow_source, snow_target = load_config(snowflake_path)

        assert isinstance(delta_source, DeltaLakeConfig)
        assert isinstance(delta_target, DeltaLakeConfig)
        assert delta_source.table_uri == "s3://lake/legacy"
        assert isinstance(snow_source, SnowflakeConfig)
        assert isinstance(snow_target, SnowflakeConfig)
        assert snow_source.table == "ANALYTICS.PUBLIC.SRC"
        assert snow_target.table == "ANALYTICS.PUBLIC.TGT"
        assert snow_source.type == "snowflake"
