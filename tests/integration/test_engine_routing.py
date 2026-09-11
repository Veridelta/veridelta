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
from veridelta.connectors.sql import COUNT_ALIAS
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
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


SOURCE_TOTAL = 1000
TARGET_TOTAL = 1001


def _frame_with_ids(height: int) -> pl.LazyFrame:
    """Return a LazyFrame whose height matches the requested row count."""
    return pl.DataFrame({"id": list(range(height))}).lazy()


PROBE_SCHEMA = pl.Schema({"id": pl.Int64, "amount": pl.Float64})
"""Types the stubbed probe reports, which the compiler uses to filter sentinels."""


def _probe_frame() -> pl.LazyFrame:
    """Return a zero-row frame standing in for a warehouse column probe."""
    return pl.DataFrame(schema=PROBE_SCHEMA).lazy()


def _pushdown_by_query_type(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
    """Return distinct-height frames so DiffSummary counts are independently asserted."""
    if query_type == "schema":
        return _probe_frame()
    if query_type == "count":
        is_source = statement.upper().endswith("SRC")
        total = SOURCE_TOTAL if is_source else TARGET_TOTAL
        return pl.DataFrame({COUNT_ALIAS: [total]}).lazy()
    if query_type == "columns":
        # A fully matching column is included so the zero-count filter is exercised.
        return pl.DataFrame({"amount": [5], "quantity": [0]}).lazy()
    heights = {"mismatch": 2, "added": 3, "missing": 4}
    return _frame_with_ids(heights[query_type])


def _synthesized_amount_rule() -> DiffRule:
    """Return the rule the engine derives for the unruled probed 'amount' column."""
    return DiffRule(
        column_names=["amount"],
        absolute_tolerance=0.0,
        relative_tolerance=0.0,
        treat_null_as_equal=True,
        whitespace_mode="none",
        null_values=[],
    )


def _configure_warehouse_compiler(connector: Any) -> None:
    """Stub compile_* helpers with distinct SQL strings for call assertions."""
    connector.compiler.compile_query.return_value = "SELECT mismatch"
    connector.compiler.compile_added_query.return_value = "SELECT added"
    connector.compiler.compile_missing_query.return_value = "SELECT missing"
    connector.compiler.compile_column_mismatch_query.return_value = "SELECT columns"
    connector.compiler.compile_count_query.side_effect = lambda table: f"SELECT count FROM {table}"
    connector.compiler.compile_schema_probe_query.side_effect = lambda table: (
        f"SELECT probe FROM {table}"
    )
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

        summary = DiffEngine.run_from_configs(diff, source, target).summary

        ingestor_cls.assert_not_called()
        connector.connect.assert_called_once()
        connector.compiler.compile_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC",
            "ANALYTICS.PUBLIC.TGT",
            ["id"],
            [_synthesized_amount_rule()],
            source_types=PROBE_SCHEMA,
            target_types=PROBE_SCHEMA,
        )
        connector.compiler.compile_added_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC", "ANALYTICS.PUBLIC.TGT", ["id"]
        )
        connector.compiler.compile_missing_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC", "ANALYTICS.PUBLIC.TGT", ["id"]
        )
        assert connector.execute_pushdown.call_count == 8
        connector.execute_pushdown.assert_any_call("SELECT mismatch", query_type="mismatch")
        connector.execute_pushdown.assert_any_call("SELECT added", query_type="added")
        connector.execute_pushdown.assert_any_call("SELECT missing", query_type="missing")
        connector.execute_pushdown.assert_any_call("SELECT columns", query_type="columns")
        connector.execute_pushdown.assert_any_call(
            "SELECT count FROM ANALYTICS.PUBLIC.SRC", query_type="count"
        )
        connector.execute_pushdown.assert_any_call(
            "SELECT probe FROM ANALYTICS.PUBLIC.TGT", query_type="schema"
        )
        assert summary.changed_count == 2
        assert summary.added_count == 3
        assert summary.removed_count == 4
        assert summary.total_rows_source == SOURCE_TOTAL
        assert summary.total_rows_target == TARGET_TOTAL
        assert summary.is_match is False
        assert summary.column_mismatches == {"amount": 5}

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

        summary = DiffEngine.run_from_configs(diff, source, target).summary

        ingestor_cls.assert_not_called()
        connector.connect.assert_called_once()
        connector.compiler.compile_query.assert_called_once_with(
            "main.default.src",
            "main.default.tgt",
            ["id"],
            [_synthesized_amount_rule()],
            source_types=PROBE_SCHEMA,
            target_types=PROBE_SCHEMA,
        )
        connector.compiler.compile_added_query.assert_called_once_with(
            "main.default.src", "main.default.tgt", ["id"]
        )
        connector.compiler.compile_missing_query.assert_called_once_with(
            "main.default.src", "main.default.tgt", ["id"]
        )
        assert connector.execute_pushdown.call_count == 8
        connector.execute_pushdown.assert_any_call("SELECT mismatch", query_type="mismatch")
        connector.execute_pushdown.assert_any_call("SELECT added", query_type="added")
        connector.execute_pushdown.assert_any_call("SELECT missing", query_type="missing")
        assert summary.changed_count == 2
        assert summary.added_count == 3
        assert summary.removed_count == 4
        assert summary.total_rows_source == SOURCE_TOTAL
        assert summary.total_rows_target == TARGET_TOTAL

    def test_it_honors_non_zero_threshold_against_pushdown_row_totals(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure nine discrepancies in a thousand source rows clear a 1% threshold."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], threshold=0.01),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        ).summary

        assert summary.total_mismatches == 9
        assert summary.mismatch_ratio == pytest.approx(0.009)
        assert summary.is_match is True

    def test_it_writes_pushdown_artifacts_under_a_primary_key_only_suffix(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure pushdown files are persisted and named for their key-only contents."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format="csv"),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        ).summary

        assert summary.artifacts_written is True
        assert sorted(path.name for path in tmp_path.iterdir()) == [
            "added_rows_pks_only.csv",
            "changed_rows_pks_only.csv",
            "removed_rows_pks_only.csv",
        ]
        assert pl.read_csv(tmp_path / "added_rows_pks_only.csv").columns == ["id"]

    def test_it_omits_pushdown_artifacts_when_no_output_path_is_configured(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure pushdown writes nothing when the user asked for no artifacts."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        ).summary

        assert summary.artifacts_written is False

    def test_it_keeps_explicit_rules_and_skips_ignored_columns_during_synthesis(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure synthesis fills only unruled shared columns and honors ignore."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        wide_schema = pl.Schema(
            {"id": pl.Int64, "amount": pl.Float64, "notes": pl.String, "legacy": pl.String}
        )

        def _wide_probe(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "schema":
                return pl.DataFrame(schema=wide_schema).lazy()
            if query_type == "columns":
                return pl.DataFrame({"amount": [1], "notes": [2]}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _wide_probe
        explicit = DiffRule(column_names=["amount"], absolute_tolerance=0.5)

        DiffEngine.run_from_configs(
            DiffConfig(
                primary_keys=["id"],
                rules=[explicit, DiffRule(column_names=["legacy"], ignore=True)],
            ),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        )

        compiled: list[DiffRule] = connector.compiler.compile_query.call_args.args[3]
        assert [rule.column_names for rule in compiled] == [["amount"], ["notes"]]
        assert compiled[0].absolute_tolerance == 0.5
        connector.compiler.compile_column_mismatch_query.assert_called_once_with(
            "ANALYTICS.PUBLIC.SRC",
            "ANALYTICS.PUBLIC.TGT",
            ["id"],
            compiled,
            source_types=wide_schema,
            target_types=wide_schema,
        )

    def test_it_rejects_a_pushdown_rule_the_probed_type_cannot_match(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure the probe catches unusable explicit sentinels before any scan."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        with pytest.raises(ConfigError, match="cannot hold any of the null_values"):
            DiffEngine.run_from_configs(
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["amount"], null_values=["N/A"])],
                ),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

        connector.compiler.compile_query.assert_not_called()

    def test_it_forwards_global_sentinels_to_the_compiler_unfiltered(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a global list survives synthesis so the compiler filters per column."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], default_null_values=["N/A", -999]),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        )

        compiled: list[DiffRule] = connector.compiler.compile_query.call_args.args[3]
        assert compiled[0].null_values == ["N/A", -999]

    def test_it_skips_the_tally_when_no_column_is_comparable(self, mocker: MockerFixture) -> None:
        """Ensure a None aggregate leaves column_mismatches empty without a round trip."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)
        connector.compiler.compile_column_mismatch_query.return_value = None

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        ).summary

        assert summary.column_mismatches == {}
        assert connector.execute_pushdown.call_count == 7

    def test_it_raises_connector_error_when_the_tally_returns_multiple_rows(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a malformed aggregate fails loudly instead of reporting partial drift."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        def _multi_row_tally(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "columns":
                return pl.DataFrame({"amount": [1, 2]}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _multi_row_tally

        with pytest.raises(ConnectorError, match="did not return exactly one row"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

    def test_it_raises_config_error_when_probed_columns_violate_exact_schema(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure schema_mode is enforced on warehouse relations before comparison."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        def _drifted_probe(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "schema" and statement.upper().endswith("TGT"):
                return pl.DataFrame(schema={"id": pl.Int64, "surcharge": pl.Float64}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _drifted_probe

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"], schema_mode="exact"),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

        connector.compiler.compile_query.assert_not_called()

    def test_it_raises_connector_error_when_row_count_is_not_a_single_value(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a malformed count result fails loudly instead of skewing the ratio."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        def _multi_row_count(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "count":
                return _frame_with_ids(2)
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _multi_row_count

        with pytest.raises(ConnectorError, match="did not return a single value"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

    def test_it_raises_connector_error_when_row_count_is_not_numeric(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a non-numeric count scalar is rejected rather than coerced."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        connector: Any = connector_cls.return_value
        _configure_warehouse_compiler(connector)

        def _text_count(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "count":
                return pl.DataFrame({COUNT_ALIAS: ["many"]}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _text_count

        with pytest.raises(ConnectorError, match="returned a non-numeric value"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

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

    def test_it_raises_connector_error_for_mismatched_databricks_fingerprints(self) -> None:
        """Ensure distinct Databricks workspaces cannot share a pushdown session."""
        source = _databricks_config(table="main.default.src", host="adb-a.azuredatabricks.net")
        target = _databricks_config(table="main.default.tgt", host="adb-b.azuredatabricks.net")
        diff = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConnectorError, match="Cross-account"):
            DiffEngine.run_from_configs(diff, source, target)

    def test_it_rejects_a_claimed_warehouse_pair_that_is_neither_dialect(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure the final guard fires when both sides are marked warehouse but are not."""
        mocker.patch("veridelta.engine._is_warehouse", return_value=True)
        source = SourceConfig(path="src.csv", format="csv")
        target = SourceConfig(path="tgt.csv", format="csv")

        with pytest.raises(ConnectorError, match="Mixed file/lakehouse"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

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
        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]), source, target
        ).summary

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
