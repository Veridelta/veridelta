# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for source-union routing, lakehouse loads, and warehouse pushdown."""

from pathlib import Path
from typing import Any, get_args

import polars as pl
import pytest
from pytest_mock import MockerFixture

from veridelta.config import load_config
from veridelta.connectors.database import DatabaseConnector, PostgresPushdownSession
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.sql import COUNT_ALIAS, SampleQuery
from veridelta.engine import (
    _WAREHOUSES,
    DiffEngine,
    LoaderFactory,
    _validate_pushdown_schema,
    _WarehouseConfig,
)
from veridelta.exceptions import ConfigError, ConnectorError, DataIntegrityError
from veridelta.models import (
    ArtifactFormat,
    BigQueryConfig,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)

pytestmark = [pytest.mark.integration]


def _snowflake_config(
    *,
    table: str,
    account: str = "xy12345",
    password: str | None = None,
    role: str | None = None,
) -> SnowflakeConfig:
    """Build a Snowflake source with a distinct table name."""
    return SnowflakeConfig(
        account=account,
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        password=password,
        role=role,
        table=table,
    )


def _databricks_config(
    *,
    table: str,
    host: str = "adb.azuredatabricks.net",
    access_token: str | None = None,
) -> DatabricksConfig:
    """Build a Databricks source with a distinct table name."""
    return DatabricksConfig(
        server_hostname=host,
        http_path="/sql/1.0/warehouses/abc",
        access_token=access_token,
        table=table,
    )


_WAREHOUSE_PAIRS: dict[str, tuple[_WarehouseConfig, _WarehouseConfig]] = {
    "Snowflake": (
        _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
        _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
    ),
    "Databricks": (
        _databricks_config(table="main.default.src"),
        _databricks_config(table="main.default.tgt"),
    ),
    "BigQuery": (
        BigQueryConfig(project="analytics-prod", table="sales.src"),
        BigQueryConfig(project="analytics-prod", table="sales.tgt"),
    ),
}
"""A source and target on one connection per warehouse, keyed by its connector class prefix."""


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
    if query_type == "duplicates":
        return pl.DataFrame({COUNT_ALIAS: [0]}).lazy()
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


def _synthesized_id_key_rule() -> DiffRule:
    """Return the rule the engine derives for the unruled 'id' primary key."""
    return DiffRule(column_names=["id"], whitespace_mode="none", null_values=[])


def _configure_warehouse_compiler(mocker: MockerFixture, name: str = "SnowflakeConnector") -> Any:
    """Patch a `veridelta.engine` class, stub its instance's SQL, and return the class mock."""
    connector_cls = mocker.patch(f"veridelta.engine.{name}")
    connector = connector_cls.return_value
    connector.compiler.compile_query.return_value = "SELECT mismatch"
    connector.compiler.compile_added_query.return_value = "SELECT added"
    connector.compiler.compile_missing_query.return_value = "SELECT missing"
    connector.compiler.compile_column_mismatch_query.return_value = "SELECT columns"
    connector.compiler.compile_count_query.side_effect = lambda table: f"SELECT count FROM {table}"
    connector.compiler.compile_duplicate_key_query.side_effect = lambda table, *_, **__: (
        f"SELECT duplicates FROM {table}"
    )
    connector.compiler.compile_schema_probe_query.side_effect = lambda table: (
        f"SELECT probe FROM {table}"
    )
    connector.execute_pushdown.side_effect = _pushdown_by_query_type
    return connector_cls


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

    def test_it_reads_a_database_config_through_its_connector_and_closes_it(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a database source is read once and its connector released after."""
        frame = pl.DataFrame({"id": [4], "amount": [40.0]}).lazy()
        connect = mocker.patch.object(DatabaseConnector, "connect")
        mocker.patch.object(DatabaseConnector, "lazyframe", return_value=frame)
        close = mocker.spy(DatabaseConnector, "close")

        loaded = LoaderFactory.load(
            DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="public.orders")
        )

        assert loaded.collect().equals(frame.collect())
        connect.assert_called_once_with()
        close.assert_called_once()

    def test_it_rejects_warehouse_configs_on_loader_factory(self) -> None:
        """Ensure Snowflake sources cannot be scanned as local LazyFrames."""
        with pytest.raises(ConnectorError, match="Warehouse sources cannot be loaded"):
            LoaderFactory.load(_snowflake_config(table="ANALYTICS.PUBLIC.SRC"))

    @pytest.mark.parametrize(
        "backend",
        [pytest.param("Snowflake", id="snowflake"), pytest.param("Databricks", id="databricks")],
    )
    def test_it_pushdown_executes_matching_warehouse_fingerprints_without_file_loaders(
        self, mocker: MockerFixture, backend: str
    ) -> None:
        """Ensure same-account or same-workspace pairs compile SQL and skip `DataIngestor`."""
        connector = _configure_warehouse_compiler(mocker, f"{backend}Connector").return_value
        ingestor_cls = mocker.patch("veridelta.engine.DataIngestor")
        source, target = _WAREHOUSE_PAIRS[backend]
        diff = DiffConfig(primary_keys=["id"])

        summary = DiffEngine.run_from_configs(diff, source, target).summary

        ingestor_cls.assert_not_called()
        connector.connect.assert_called_once()
        connector.compiler.compile_query.assert_called_once_with(
            source.table,
            target.table,
            ["id"],
            [_synthesized_amount_rule()],
            source_types=PROBE_SCHEMA,
            target_types=PROBE_SCHEMA,
            key_rules=[_synthesized_id_key_rule()],
            wide_integers=frozenset(),
            type_drift=frozenset(),
        )
        connector.compiler.compile_added_query.assert_called_once_with(
            source.table,
            target.table,
            ["id"],
            source_types=PROBE_SCHEMA,
            target_types=PROBE_SCHEMA,
            key_rules=[_synthesized_id_key_rule()],
        )
        connector.compiler.compile_missing_query.assert_called_once_with(
            source.table,
            target.table,
            ["id"],
            source_types=PROBE_SCHEMA,
            target_types=PROBE_SCHEMA,
            key_rules=[_synthesized_id_key_rule()],
        )
        assert connector.execute_pushdown.call_count == 10
        connector.execute_pushdown.assert_any_call("SELECT mismatch", query_type="mismatch")
        connector.execute_pushdown.assert_any_call("SELECT added", query_type="added")
        connector.execute_pushdown.assert_any_call("SELECT missing", query_type="missing")
        connector.execute_pushdown.assert_any_call("SELECT columns", query_type="columns")
        connector.execute_pushdown.assert_any_call(
            f"SELECT count FROM {source.table}", query_type="count"
        )
        connector.execute_pushdown.assert_any_call(
            f"SELECT probe FROM {target.table}", query_type="schema"
        )
        connector.compiler.compile_duplicate_key_query.assert_any_call(
            source.table,
            ["id"],
            is_source=True,
            key_rules=[_synthesized_id_key_rule()],
            types=PROBE_SCHEMA,
        )
        connector.compiler.compile_duplicate_key_query.assert_any_call(
            target.table,
            ["id"],
            is_source=False,
            key_rules=[_synthesized_id_key_rule()],
            types=PROBE_SCHEMA,
        )
        connector.execute_pushdown.assert_any_call(
            f"SELECT duplicates FROM {target.table}", query_type="duplicates"
        )
        assert summary.changed_count == 2
        assert summary.added_count == 3
        assert summary.removed_count == 4
        assert summary.total_rows_source == SOURCE_TOTAL
        assert summary.total_rows_target == TARGET_TOTAL
        assert summary.is_match is False
        assert summary.column_mismatches == {"amount": 5}

    @pytest.mark.parametrize("backend", ["Snowflake", "Databricks", "BigQuery"])
    def test_it_closes_the_warehouse_session_after_a_completed_pushdown(
        self, mocker: MockerFixture, backend: str
    ) -> None:
        """Ensure every pushdown run releases its session once the result is built."""
        connector = _configure_warehouse_compiler(mocker, f"{backend}Connector").return_value
        source, target = _WAREHOUSE_PAIRS[backend]

        DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        connector.connect.assert_called_once()
        connector.close.assert_called_once()
        # close() runs after the last statement, not between round-trips.
        method_names = [name for name, _args, _kwargs in connector.mock_calls]
        assert method_names.index("close") > method_names.index("execute_pushdown")

    @pytest.mark.parametrize(
        "backend",
        [pytest.param("Snowflake", id="snowflake"), pytest.param("Databricks", id="databricks")],
    )
    def test_it_raises_config_error_when_probed_columns_violate_exact_schema(
        self, mocker: MockerFixture, backend: str
    ) -> None:
        """Ensure a schema violation fails before comparison SQL and still releases the session."""
        connector = _configure_warehouse_compiler(mocker, f"{backend}Connector").return_value
        source, target = _WAREHOUSE_PAIRS[backend]

        def _drifted_probe(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "schema" and statement.upper().endswith("TGT"):
                return pl.DataFrame(schema={"id": pl.Int64, "surcharge": pl.Float64}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _drifted_probe

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"], schema_mode="exact"), source, target
            )

        connector.compiler.compile_query.assert_not_called()
        connector.close.assert_called_once()

    def test_it_rejects_duplicate_warehouse_keys_before_any_join(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a repeated key fails before a join can fan out, and still releases the session."""
        connector = _configure_warehouse_compiler(mocker).return_value

        def _duplicated_target(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "duplicates" and statement.upper().endswith("TGT"):
                return pl.DataFrame({COUNT_ALIAS: [3]}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _duplicated_target

        with pytest.raises(
            DataIntegrityError, match=r"not unique in TARGET dataset\. Found 3 duplicate rows"
        ):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

        connector.compiler.compile_query.assert_not_called()
        connector.compiler.compile_added_query.assert_not_called()
        connector.compiler.compile_missing_query.assert_not_called()
        connector.compiler.compile_count_query.assert_not_called()
        connector.close.assert_called_once()

    @pytest.mark.parametrize(
        ("backend", "expected"),
        [
            pytest.param(
                "Snowflake",
                {"total_mismatches": 9, "mismatch_ratio": pytest.approx(0.009), "is_match": True},
                id="snowflake",
            ),
            pytest.param(
                "Databricks",
                {
                    "total_mismatches": 9,
                    "mismatch_ratio": pytest.approx(0.009),
                    "is_match": True,
                    "column_mismatches": {"amount": 5},
                },
                id="databricks",
            ),
        ],
    )
    def test_it_honors_non_zero_threshold_against_pushdown_row_totals(
        self, mocker: MockerFixture, backend: str, expected: dict[str, object]
    ) -> None:
        """Ensure nine discrepancies in a thousand source rows clear a 1% threshold."""
        _configure_warehouse_compiler(mocker, f"{backend}Connector")
        source, target = _WAREHOUSE_PAIRS[backend]

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], threshold=0.01), source, target
        ).summary

        assert {key: getattr(summary, key) for key in expected} == expected

    @pytest.mark.parametrize(
        ("backend", "output_format"),
        [
            pytest.param("Snowflake", "csv", id="snowflake-csv"),
            pytest.param("Databricks", "parquet", id="databricks-parquet"),
        ],
    )
    def test_it_writes_pushdown_artifacts_under_a_primary_key_only_suffix(
        self, mocker: MockerFixture, tmp_path: Path, backend: str, output_format: ArtifactFormat
    ) -> None:
        """Ensure pushdown files are persisted and named for their key-only contents."""
        _configure_warehouse_compiler(mocker, f"{backend}Connector")
        source, target = _WAREHOUSE_PAIRS[backend]

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format=output_format),
            source,
            target,
        )

        assert result.keys_only is True
        assert result.summary.artifacts_written is True
        assert sorted(path.name for path in tmp_path.iterdir()) == [
            f"added_rows_pks_only.{output_format}",
            f"changed_rows_pks_only.{output_format}",
            f"removed_rows_pks_only.{output_format}",
        ]
        read = pl.read_csv if output_format == "csv" else pl.read_parquet
        assert read(tmp_path / f"added_rows_pks_only.{output_format}").columns == ["id"]
        assert read(tmp_path / f"removed_rows_pks_only.{output_format}").height == 4

    def test_it_omits_pushdown_artifacts_when_no_output_path_is_configured(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure pushdown writes nothing when the user asked for no artifacts."""
        _configure_warehouse_compiler(mocker)

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
        connector = _configure_warehouse_compiler(mocker).return_value

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
            key_rules=[_synthesized_id_key_rule()],
            wide_integers=frozenset(),
            type_drift=frozenset(),
        )

    def test_it_rejects_a_pushdown_rule_the_probed_type_cannot_match(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure the probe catches unusable explicit sentinels before any scan."""
        connector = _configure_warehouse_compiler(mocker).return_value

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

    def test_it_refuses_jaro_winkler_before_any_warehouse_query(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a limit no warehouse can reproduce fails after the probes alone."""
        connector = _configure_warehouse_compiler(mocker).return_value
        text_schema = pl.Schema({"id": pl.Int64, "name": pl.String})

        def _text_probe(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "schema":
                return pl.DataFrame(schema=text_schema).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _text_probe

        with pytest.raises(ConfigError, match="Column 'name' sets min_jaro_winkler_similarity"):
            DiffEngine.run_from_configs(
                DiffConfig(
                    primary_keys=["id"],
                    rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)],
                ),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
            )

        executed = [
            query.kwargs["query_type"] for query in connector.execute_pushdown.call_args_list
        ]
        assert executed == ["schema", "schema"]
        connector.compiler.compile_duplicate_key_query.assert_not_called()
        connector.compiler.compile_query.assert_not_called()

    def test_it_hands_an_edit_distance_to_the_compiler(self, mocker: MockerFixture) -> None:
        """Ensure a Levenshtein limit on a text column reaches the comparison SQL."""
        connector = _configure_warehouse_compiler(mocker).return_value
        text_schema = pl.Schema({"id": pl.Int64, "name": pl.String})

        def _text_probe(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "schema":
                return pl.DataFrame(schema=text_schema).lazy()
            if query_type == "columns":
                return pl.DataFrame({"name": [0]}).lazy()
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _text_probe

        DiffEngine.run_from_configs(
            DiffConfig(
                primary_keys=["id"],
                rules=[DiffRule(column_names=["name"], max_levenshtein_distance=2)],
            ),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        )

        (compiled,) = connector.compiler.compile_query.call_args.args[3]
        assert compiled.max_levenshtein_distance == 2
        assert compiled.min_jaro_winkler_similarity is None

    def test_it_forwards_global_sentinels_to_the_compiler_unfiltered(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a global list survives synthesis so the compiler filters per column."""
        connector = _configure_warehouse_compiler(mocker).return_value

        DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], default_null_values=["N/A", -999]),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        )

        compiled: list[DiffRule] = connector.compiler.compile_query.call_args.args[3]
        assert compiled[0].null_values == ["N/A", -999]

    def test_it_skips_the_tally_when_no_column_is_comparable(self, mocker: MockerFixture) -> None:
        """Ensure a None aggregate leaves column_mismatches empty without a round trip."""
        connector = _configure_warehouse_compiler(mocker).return_value
        connector.compiler.compile_column_mismatch_query.return_value = None

        summary = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        ).summary

        assert summary.column_mismatches == {}
        assert connector.execute_pushdown.call_count == 9

    @pytest.mark.parametrize(
        ("broken", "result", "match"),
        [
            # A malformed aggregate fails loudly instead of reporting partial drift.
            pytest.param(
                "columns",
                pl.DataFrame({"amount": [1, 2]}).lazy(),
                "did not return exactly one row",
                id="tally-with-two-rows",
            ),
            # A malformed count fails loudly instead of skewing the ratio.
            pytest.param(
                "count",
                _frame_with_ids(2),
                "did not return a single value",
                id="count-with-two-rows",
            ),
            pytest.param(
                "count",
                pl.DataFrame({COUNT_ALIAS: ["many"]}).lazy(),
                "returned a non-numeric value",
                id="text-count",
            ),
        ],
    )
    def test_it_raises_connector_error_for_a_malformed_result(
        self, mocker: MockerFixture, broken: str, result: pl.LazyFrame, match: str
    ) -> None:
        """Ensure a malformed result is rejected rather than coerced."""
        connector = _configure_warehouse_compiler(mocker).return_value

        def _malformed(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == broken:
                return result
            return _pushdown_by_query_type(statement, query_type)

        connector.execute_pushdown.side_effect = _malformed

        with pytest.raises(ConnectorError, match=match):
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

        with pytest.raises(
            ConnectorError, match="the source is Snowflake and the target is Databricks"
        ):
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

    @pytest.mark.parametrize(
        ("source_kwargs", "target_kwargs"),
        [
            pytest.param({"password": "left"}, {"password": "right"}, id="password"),
            pytest.param({"password": "same"}, {}, id="password-vs-none"),
            pytest.param({"role": "SYSADMIN"}, {"role": "ANALYST"}, id="role"),
        ],
    )
    def test_it_treats_snowflake_credentials_as_part_of_the_fingerprint(
        self, mocker: MockerFixture, source_kwargs: dict[str, str], target_kwargs: dict[str, str]
    ) -> None:
        """Ensure two sides that differ only in password or role never share one session."""
        connector_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        source = _snowflake_config(table="ANALYTICS.PUBLIC.SRC", **source_kwargs)
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT", **target_kwargs)

        with pytest.raises(ConnectorError, match="Cross-account"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        connector_cls.assert_not_called()

    def test_it_refuses_to_compare_a_warehouse_table_with_itself(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a copy-pasted table name fails instead of reporting a perfect match.

        A relation compared with itself always matches, so the run would pass
        whatever the data held. It is refused before any session opens.
        """
        snowflake_cls = mocker.patch("veridelta.engine.SnowflakeConnector")
        databricks_cls = mocker.patch("veridelta.engine.DatabricksConnector")
        diff = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConfigError, match="same table"):
            DiffEngine.run_from_configs(
                diff,
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
                _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            )
        with pytest.raises(ConfigError, match="same table"):
            DiffEngine.run_from_configs(
                diff,
                _databricks_config(table="main.default.src"),
                _databricks_config(table="main.default.src"),
            )

        snowflake_cls.assert_not_called()
        databricks_cls.assert_not_called()

    def test_it_treats_the_databricks_token_as_part_of_the_fingerprint(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a token mismatch on an otherwise identical workspace is refused."""
        connector_cls = mocker.patch("veridelta.engine.DatabricksConnector")
        source = _databricks_config(table="main.default.src", access_token="dapi-a")
        target = _databricks_config(table="main.default.tgt", access_token="dapi-b")

        with pytest.raises(ConnectorError, match="Cross-account"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        connector_cls.assert_not_called()

    def test_it_raises_connector_error_for_mixed_lakehouse_and_warehouse_backends(self) -> None:
        """Ensure a Delta scan cannot be paired with a Snowflake relation."""
        source = DeltaLakeConfig(table_uri="s3://lake/legacy_events")
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT")

        with pytest.raises(ConnectorError, match="Mixed file/lakehouse"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

    def test_it_raises_connector_error_for_mixed_database_and_warehouse_backends(self) -> None:
        """Ensure a database read cannot be paired with a Snowflake relation, and says so."""
        source = DatabaseConfig(uri="postgresql://analyst@db.internal/sales", table="orders")
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT")

        with pytest.raises(ConnectorError, match="Mixed file/lakehouse/database and warehouse"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

    def test_it_registers_every_warehouse_config_type(self) -> None:
        """Ensure the registry and the warehouse config union name the same backends.

        Routing looks a config's type up in the registry, so a backend added to
        one and not the other would either be read as a local source or fail
        its type narrowing.
        """
        assert set(_WAREHOUSES) == set(get_args(_WarehouseConfig))

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

    def test_it_applies_renames_once_through_run_from_configs(self, tmp_path: Path) -> None:
        """Ensure the YAML path renames once, so swapped names stay swapped.

        Loading used to rename through the ingestor, and `run()` then renamed
        the already-aligned frames again, undoing a swap and collapsing a chain.
        """
        src_file = tmp_path / "source.csv"
        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1], "lat": [10.0], "lon": [20.0]}).write_csv(src_file)
        pl.DataFrame({"id": [1], "lat": [20.0], "lon": [10.0]}).write_csv(tgt_file)
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["lat"], rename_to="lon"),
                DiffRule(column_names=["lon"], rename_to="lat"),
            ],
        )

        result = DiffEngine.run_from_configs(
            config,
            SourceConfig(path=str(src_file), format="csv"),
            SourceConfig(path=str(tgt_file), format="csv"),
        )

        assert result.summary.is_perfect_match is True

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

    def test_it_round_trips_iceberg_and_databricks_typed_yaml_blocks(self, tmp_path: Path) -> None:
        """Ensure the remaining two discriminators, with their optional pins, load from YAML."""
        config_path = tmp_path / "mixed.yaml"
        config_path.write_text(
            "source:\n"
            "  type: iceberg\n"
            "  table_uri: s3://lake/iceberg/legacy\n"
            "  snapshot_id: 883142\n"
            "  storage_options:\n"
            "    AWS_REGION: us-east-1\n"
            "target:\n"
            "  type: databricks\n"
            "  table: main.default.modern\n"
            "  server_hostname: adb.azuredatabricks.net\n"
            "  http_path: /sql/1.0/warehouses/abc\n"
            "  catalog: main\n"
            "  schema_name: default\n"
            "primary_keys:\n"
            "  - id\n"
        )

        diff_cfg, source, target = load_config(config_path)

        assert isinstance(source, IcebergConfig)
        assert source.snapshot_id == 883142
        assert source.storage_options == {"AWS_REGION": "us-east-1"}
        assert isinstance(target, DatabricksConfig)
        assert target.catalog == "main"
        assert target.access_token is None
        # The pair parses but cannot run: lakehouse scans and warehouse SQL do not mix.
        with pytest.raises(ConnectorError, match="Mixed file/lakehouse"):
            DiffEngine.run_from_configs(diff_cfg, source, target)

    def test_it_runs_a_delta_pair_locally_through_run_from_configs(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure two lakehouse scans take the DataIngestor path and produce a full diff."""
        source_frame = pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]}).lazy()
        target_frame = pl.DataFrame({"id": [1, 2, 4], "amount": [10.0, 25.0, 40.0]}).lazy()
        scan = mocker.patch(
            "veridelta.connectors.lakehouse.pl.scan_delta",
            side_effect=[source_frame, target_frame],
        )

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            DeltaLakeConfig(table_uri="s3://lake/legacy", version=7),
            DeltaLakeConfig(table_uri="s3://lake/modern"),
        )

        assert scan.call_count == 2
        assert scan.call_args_list[0].kwargs["version"] == 7
        assert result.keys_only is False
        assert result.summary.added_count == 1
        assert result.summary.removed_count == 1
        assert result.summary.changed_count == 1
        assert result.summary.column_mismatches == {"amount": 1}
        assert result.changed["amount_target"].to_list() == [25.0]


_POSTGRES_URI = "postgresql://analyst@db.internal:5432/sales"


def _postgres_config(
    *,
    table: str,
    uri: str = _POSTGRES_URI,
    pushdown: bool = True,
    password: str | None = None,
) -> DatabaseConfig:
    """Build a Postgres table source, opted into pushdown unless told otherwise."""
    return DatabaseConfig(uri=uri, table=table, pushdown=pushdown, password=password)


class TestPostgresPushdownRouting:
    """Validate how two database sources that set `pushdown` are routed."""

    def test_it_compares_two_opted_in_tables_inside_postgres(self, mocker: MockerFixture) -> None:
        """Ensure the pair runs through one Postgres session and no row is read locally."""
        session_cls = _configure_warehouse_compiler(mocker, "PostgresPushdownSession")
        session = session_cls.return_value
        load = mocker.patch.object(LoaderFactory, "load")
        source = _postgres_config(table="public.src")
        target = _postgres_config(table="public.tgt")

        result = DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        session_cls.assert_called_once_with(source)
        session.connect.assert_called_once()
        session.close.assert_called_once()
        load.assert_not_called()
        assert result.keys_only is True
        assert result.summary.total_rows_source == SOURCE_TOTAL
        assert result.summary.total_rows_target == TARGET_TOTAL

    @pytest.mark.parametrize("opted_in", ["source", "target"])
    def test_it_asks_for_pushdown_on_both_sides(self, mocker: MockerFixture, opted_in: str) -> None:
        """Ensure a pair that half opts in names the fix instead of reading one side."""
        session_cls = mocker.patch("veridelta.engine.PostgresPushdownSession")
        source = _postgres_config(table="src", pushdown=opted_in == "source")
        target = _postgres_config(table="tgt", pushdown=opted_in == "target")

        with pytest.raises(ConfigError, match="Set pushdown on both database sources"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        session_cls.assert_not_called()

    @pytest.mark.parametrize(
        "target",
        [
            pytest.param(
                _postgres_config(table="tgt", uri="postgresql://analyst@db.internal:5432/archive"),
                id="other-database",
            ),
            pytest.param(_postgres_config(table="tgt", password="other"), id="other-password"),
        ],
    )
    def test_it_needs_one_connection(self, target: DatabaseConfig) -> None:
        """Ensure two tables are compared in place only when one connection reaches both."""
        source = _postgres_config(table="src")

        with pytest.raises(ConnectorError, match="Postgres connections must match"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

    def test_it_refuses_to_compare_a_table_with_itself(self) -> None:
        """Ensure a copy-pasted side cannot turn into a run that always matches."""
        source = _postgres_config(table="public.orders")

        with pytest.raises(ConfigError, match="same table"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, source)

    def test_it_refuses_a_postgres_table_paired_with_a_warehouse(self) -> None:
        """Ensure two engines are never asked to share one statement."""
        source = _postgres_config(table="src")
        target = _snowflake_config(table="ANALYTICS.PUBLIC.TGT")

        with pytest.raises(
            ConnectorError, match="the source is Postgres and the target is Snowflake"
        ):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

    def test_it_compares_each_numeric_at_its_declared_type(self, mocker: MockerFixture) -> None:
        """Ensure pushdown sees `numeric(10, 2)` and `numeric(12, 4)` as a local read does.

        The zero-row probe reports every `numeric` as Decimal(38, 10), so
        `strict_types` could not tell the two apart from the probe alone.
        """
        # (precision << 16 | scale) + 4 for numeric(10, 2) and numeric(12, 4).
        typmods = {'"src"': 655366, '"tgt"': 786440}

        def _answer(statement: str, uri: str) -> pl.DataFrame:
            if statement.startswith("SELECT current_setting"):
                return pl.DataFrame({"value": ["on"]})
            if statement.startswith("SELECT attname"):
                typmod = next(m for table, m in typmods.items() if table in statement)
                return pl.DataFrame(
                    {
                        "attname": ["id", "amount"],
                        "atttypmod": [-1, typmod],
                        "is_numeric": [False, True],
                    }
                )
            return pl.DataFrame(schema={"id": pl.Int64, "amount": pl.Decimal(38, 10)})

        mocker.patch("veridelta.connectors.database.connectorx", object())
        mocker.patch("veridelta.connectors.database.pl.read_database_uri", side_effect=_answer)
        session = PostgresPushdownSession(_postgres_config(table="src"))
        session.connect()

        source, target = _validate_pushdown_schema(
            session, "src", "tgt", DiffConfig(primary_keys=["id"])
        )

        assert source == pl.Schema({"id": pl.Int64(), "amount": pl.Decimal(10, 2)})
        assert target == pl.Schema({"id": pl.Int64(), "amount": pl.Decimal(12, 4)})


_SAMPLE = SampleQuery(
    "SELECT sample",
    {
        "_veridelta_key_0": "id",
        "_veridelta_source_0": "amount_source",
        "_veridelta_target_0": "amount_target",
        "_veridelta_match_0": "amount_is_match",
    },
)


def _pushdown_with_a_sample(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
    """Answer each round trip as `_pushdown_by_query_type` does, plus a two-row sample."""
    if query_type == "samples":
        return pl.DataFrame(
            {
                "_veridelta_key_0": [0, 1],
                "_veridelta_source_0": [1.0, 2.0],
                "_veridelta_target_0": [1.5, 2.5],
                "_veridelta_match_0": [False, False],
            }
        ).lazy()
    return _pushdown_by_query_type(statement, query_type)


class TestPushdownRowSamples:
    """Validate the opt-in fetch of changed rows with both sides' values."""

    def _connector(self, mocker: MockerFixture) -> Any:
        connector = _configure_warehouse_compiler(mocker).return_value
        connector.compiler.compile_changed_sample_query.return_value = _SAMPLE
        connector.execute_pushdown.side_effect = _pushdown_with_a_sample
        return connector

    def _run(self, **diff: Any) -> DiffResult:
        return DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"], **diff),
            _snowflake_config(table="ANALYTICS.PUBLIC.SRC"),
            _snowflake_config(table="ANALYTICS.PUBLIC.TGT"),
        )

    @staticmethod
    def _query_types(connector: Any) -> list[str]:
        return [call.kwargs["query_type"] for call in connector.execute_pushdown.call_args_list]

    def test_it_fetches_no_values_unless_asked(self, mocker: MockerFixture) -> None:
        """Ensure a default run issues exactly the statements it always has."""
        connector = self._connector(mocker)

        result = self._run()

        assert result.changed_sample is None
        connector.compiler.compile_changed_sample_query.assert_not_called()
        assert "samples" not in self._query_types(connector)

    def test_it_fetches_changed_rows_with_values_when_asked(self, mocker: MockerFixture) -> None:
        """Ensure the sample reads the changed rows the counts came from, under local names.

        It is compiled with exactly the arguments of the changed-row query, so
        it samples the rows that query counted, and the counts are unchanged.
        """
        connector = self._connector(mocker)

        result = self._run(pushdown_sample_rows=5)

        sample_call = connector.compiler.compile_changed_sample_query.call_args
        query_call = connector.compiler.compile_query.call_args
        assert sample_call.args == query_call.args
        assert sample_call.kwargs == {**query_call.kwargs, "limit": 5}
        assert self._query_types(connector).count("samples") == 1
        assert result.changed_sample is not None
        assert result.changed_sample.columns == [
            "id",
            "amount_source",
            "amount_target",
            "amount_is_match",
        ]
        assert result.changed.columns == ["id"]
        assert result.summary.changed_count == 2

    def test_it_skips_the_sample_when_no_row_changed(self, mocker: MockerFixture) -> None:
        """Ensure a clean comparison spends no statement on an empty sample."""
        connector = self._connector(mocker)

        def nothing_changed(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "mismatch":
                return _frame_with_ids(0)
            return _pushdown_with_a_sample(statement, query_type)

        connector.execute_pushdown.side_effect = nothing_changed

        result = self._run(pushdown_sample_rows=5)

        assert result.changed_sample is None
        connector.compiler.compile_changed_sample_query.assert_not_called()

    def test_it_writes_the_sample_as_an_artifact_of_its_own(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure the values land in a file that cannot be taken for the key list."""
        self._connector(mocker)

        self._run(pushdown_sample_rows=5, output_path=str(tmp_path), output_format="csv")

        assert sorted(path.name for path in tmp_path.iterdir()) == [
            "added_rows_pks_only.csv",
            "changed_rows_pks_only.csv",
            "changed_rows_sample.csv",
            "removed_rows_pks_only.csv",
        ]
        sample = pl.read_csv(tmp_path / "changed_rows_sample.csv")
        assert sample.columns == ["id", "amount_source", "amount_target", "amount_is_match"]

    def test_it_rejects_a_sample_missing_the_columns_it_asked_for(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a malformed result fails loudly instead of naming the wrong values."""
        connector = self._connector(mocker)

        def short_sample(statement: str, query_type: str = "mismatch") -> pl.LazyFrame:
            if query_type == "samples":
                return pl.DataFrame({"_veridelta_key_0": [0]}).lazy()
            return _pushdown_with_a_sample(statement, query_type)

        connector.execute_pushdown.side_effect = short_sample

        with pytest.raises(ConnectorError, match=r"sample.*_veridelta_source_0"):
            self._run(pushdown_sample_rows=5)
