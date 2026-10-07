# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for BigQuery: its connection model, SQL dialect, and connector."""

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta.config import load_config
from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler, _reads_offset
from veridelta.connectors.warehouse import BigQueryConnector
from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import BigQueryConfig, DiffConfig, DiffRule, SnowflakeConfig

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]

_BASE: dict[str, Any] = {"project": "analytics-prod", "table": "sales.orders"}
"""The fields every BigQuery config needs."""


class TestBigQueryConfig:
    """Validate BigQuery connection settings."""

    def test_it_accepts_a_dataset_qualified_table(self) -> None:
        """Ensure the common shape loads, with every optional field unset."""
        config = BigQueryConfig(**_BASE)

        assert config.type == "bigquery"
        assert (config.dataset, config.location, config.credentials_path) == (None, None, None)
        assert config.maximum_bytes_billed is None

    def test_it_reads_a_bare_table_from_the_default_dataset(self) -> None:
        """Ensure a one-segment table is allowed once a dataset is named."""
        config = BigQueryConfig(project="analytics-prod", table="orders", dataset="sales")

        assert (config.dataset, config.table) == ("sales", "orders")

    def test_it_needs_a_dataset_for_a_bare_table(self) -> None:
        """Ensure a table BigQuery could not resolve fails when the config loads."""
        with pytest.raises(ValidationError, match="needs 'dataset'"):
            BigQueryConfig(project="analytics-prod", table="orders")

    @pytest.mark.parametrize(
        "project",
        [
            pytest.param("Analytics-prod", id="uppercase"),
            pytest.param("short", id="too-short"),
            pytest.param("analytics-prod-", id="trailing-hyphen"),
            pytest.param("1analytics", id="leading-digit"),
            pytest.param("example.com:analytics", id="domain-scoped"),
        ],
    )
    def test_it_refuses_a_project_id_google_would_not_issue(self, project: str) -> None:
        """Ensure only lowercase project ids of six to thirty characters pass."""
        with pytest.raises(ValidationError, match="project"):
            BigQueryConfig(**{**_BASE, "project": project})

    @pytest.mark.parametrize(
        "table",
        [
            pytest.param("project.sales.orders", id="three-segments"),
            pytest.param("sales.order-lines", id="hyphen"),
            pytest.param("1sales.orders", id="leading-digit"),
        ],
    )
    def test_it_refuses_tables_outside_the_identifier_allowlist(self, table: str) -> None:
        """Ensure the table is one or two plain identifiers, so it can be quoted safely."""
        with pytest.raises(ValidationError, match="table"):
            BigQueryConfig(**{**_BASE, "table": table})

    @pytest.mark.parametrize("limit", [0, "1000", True, 1.5])
    def test_it_takes_a_byte_cap_only_as_a_positive_integer(self, limit: object) -> None:
        """Ensure the cost cap cannot be coerced from text, a bool, or a float."""
        with pytest.raises(ValidationError, match="maximum_bytes_billed"):
            BigQueryConfig(**{**_BASE, "maximum_bytes_billed": limit})

    def test_it_keeps_the_key_file_path_out_of_its_repr(self) -> None:
        """Ensure a printed config does not reveal where the service account key lives."""
        config = BigQueryConfig(**_BASE, credentials_path="/secrets/bq-key.json")

        assert "bq-key" not in repr(config)
        assert config.model_dump()["credentials_path"] == "/secrets/bq-key.json"


def _bigquery() -> SQLPushdownCompiler:
    """Return a BigQuery-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.BIGQUERY)


def _predicate(**rule: Any) -> str:
    """Compile one column's match predicate for BigQuery."""
    return _bigquery().compile_column_predicate(DiffRule(column_names=["x"], **rule), "x")


_EQUAL = "(`src`.`x` = `tgt`.`x` OR (`src`.`x` IS NOT DISTINCT FROM `tgt`.`x` AND `src`.`x` IS NOT NULL))"
"""BigQuery value equality: `=`, or two NaNs, which `=` calls different."""


class TestBigQueryDialect:
    """Validate the BigQuery spelling of each compiled stage."""

    def test_it_quotes_each_segment_with_backticks(self) -> None:
        """Ensure a dataset-qualified table is quoted segment by segment."""
        assert "FROM `sales`.`orders`" in _bigquery().compile_schema_probe_query("sales.orders")

    def test_it_matches_nan_with_nan(self) -> None:
        """Ensure equality matches two NaNs, as Polars does, though BigQuery's `=` does not."""
        assert _predicate() == _EQUAL

    def test_it_compares_nulls_with_is_not_distinct_from(self) -> None:
        """Ensure null-safe equality uses the operator BigQuery has, which is also NaN-safe."""
        assert _predicate(treat_null_as_equal=True) == "`src`.`x` IS NOT DISTINCT FROM `tgt`.`x`"

    def test_it_spells_a_tolerance_with_its_infinity_and_nan_safe_equality(self) -> None:
        """Ensure the finite guard uses a FLOAT64 infinity, and equal values still match first."""
        predicate = _predicate(absolute_tolerance=0.5)

        assert predicate.startswith(f"({_EQUAL[:-1]}")
        assert "ABS(`src`.`x`) < CAST('inf' AS FLOAT64)" in predicate

    def test_it_widens_integers_to_numeric(self) -> None:
        """Ensure an integer tolerance is measured in NUMERIC, wide enough for any INT64 difference."""
        sql = _bigquery().compile_query(
            "s",
            "t",
            ["id"],
            [DiffRule(column_names=["qty"], absolute_tolerance=1.0)],
            wide_integers=frozenset({"qty"}),
        )

        assert "ABS(CAST(`tgt`.`qty` AS NUMERIC) - CAST(`src`.`qty` AS NUMERIC))" in sql

    @pytest.mark.parametrize(
        ("cast_to", "keyword"),
        [
            pytest.param("Int64", "INT64", id="int"),
            pytest.param("Float64", "FLOAT64", id="float"),
            pytest.param("String", "STRING", id="text"),
            pytest.param("Boolean", "BOOL", id="bool"),
            pytest.param("Date", "DATE", id="date"),
            pytest.param("Datetime", "DATETIME", id="naive-timestamp"),
        ],
    )
    def test_it_casts_with_bigquery_type_names(self, cast_to: str, keyword: str) -> None:
        """Ensure a naive `Datetime` becomes DATETIME, which has no zone, like Polars'."""
        assert f"AS {keyword})" in _predicate(cast_to=cast_to)

    def test_it_tests_numbers_against_zero_to_make_booleans(self) -> None:
        """Ensure `cast_to: Boolean` on a FLOAT64 or NUMERIC column compares it with zero.

        GoogleSQL casts only INT64 and STRING to BOOL, so a `CAST` would fail the
        statement for a float or a decimal. `<> 0` is true for every nonzero
        number, NaN included, which is how Polars reads a number as a boolean.
        """
        rule = DiffRule(column_names=["flag"], cast_to="Boolean")

        numbers = _bigquery().compile_column_predicate(
            rule, "flag", source_dtype=pl.Float64(), target_dtype=pl.Decimal(10, 2)
        )
        others = _bigquery().compile_column_predicate(
            rule, "flag", source_dtype=pl.String(), target_dtype=pl.Boolean()
        )

        assert "(`src`.`flag` <> 0)" in numbers
        assert "(`tgt`.`flag` <> 0)" in numbers
        assert "AS BOOL)" not in numbers
        assert "CAST(`src`.`flag` AS BOOL)" in others
        assert "CAST(`tgt`.`flag` AS BOOL)" in others

    def test_it_parses_a_naive_timestamp_format_first(self) -> None:
        """Ensure the non-throwing parse takes its format before its value."""
        predicate = _predicate(datetime_format="%Y-%m-%d %H:%M:%S")

        assert "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', `src`.`x`)" in predicate

    def test_it_parses_an_offset_into_a_timestamp(self) -> None:
        """Ensure a format that reads an offset parses to an aware TIMESTAMP."""
        predicate = _predicate(datetime_format="%Y-%m-%dT%H:%M:%S%z")

        assert "SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%Ez', `src`.`x`)" in predicate

    @pytest.mark.parametrize(
        ("fmt", "offset"),
        [
            pytest.param("%Y-%m-%d %z", True, id="offset"),
            pytest.param("%Y%%z", False, id="escaped-percent"),
            pytest.param("%Y%%%z", True, id="escape-then-offset"),
            pytest.param("%Y-%m-%d", False, id="naive"),
        ],
    )
    def test_it_finds_an_offset_only_outside_an_escape(self, fmt: str, offset: bool) -> None:
        """Ensure `%%z` is a literal percent and a `z`, never an offset directive."""
        assert _reads_offset(fmt) is offset

    def test_it_refuses_a_fraction_of_a_second(self) -> None:
        """Ensure `%f`, which BigQuery spells only as part of `%E*S`, is refused."""
        with pytest.raises(ConfigError, match="%f"):
            _predicate(datetime_format="%Y-%m-%d %H:%M:%S.%f")

    def test_it_measures_edit_distance_in_characters(self) -> None:
        """Ensure a Levenshtein limit uses BigQuery's EDIT_DISTANCE."""
        assert "EDIT_DISTANCE(`src`.`x`, `tgt`.`x`) <= 1" in _predicate(max_levenshtein_distance=1)

    def test_it_replaces_every_regex_match_without_a_flag(self) -> None:
        """Ensure REGEXP_REPLACE gets no extra argument, since BigQuery replaces every match."""
        assert "REGEXP_REPLACE(`src`.`x`, '-', '')" in _predicate(regex_replace={"-": ""})

    def test_it_samples_by_a_fingerprint_of_the_keys_as_one_value(self) -> None:
        """Ensure a value map sample hashes all keys at once and folds the sign back."""
        sql = _bigquery().compile_value_map_query(
            "s",
            "t",
            ["id", "region"],
            [DiffRule(column_names=["gender"])],
            min_support=5,
            sample_fraction=0.5,
        )

        assert sql is not None
        assert (
            "WHERE MOD(MOD(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(`src`.`id`, `src`.`region`))), "
            "1000000) + 1000000, 1000000) < 500000"
        ) in sql

    def test_it_escapes_line_breaks_in_literals(self) -> None:
        """Ensure a value with a line break stays one valid literal."""
        predicate = _predicate(value_map={"a\nb": "c"})

        assert "'a\\nb'" in predicate
        assert "\n" not in predicate


def _driver(mocker: MockerFixture, batches: list[pa.RecordBatch] | None = None) -> MagicMock:
    """Install a stand-in `google.cloud.bigquery` whose queries return `batches`."""
    driver = MagicMock()
    client = driver.Client.return_value
    client.query.return_value.result.return_value.to_arrow_iterable.return_value = iter(
        batches if batches is not None else [pa.record_batch({"n": [1, 2]})]
    )
    driver.Client.from_service_account_json.return_value = client
    mocker.patch("veridelta.connectors.warehouse.bigquery", driver)
    return driver


class TestBigQueryConnector:
    """Validate the BigQuery connector against a stand-in client."""

    def test_it_connects_with_application_default_credentials(self, mocker: MockerFixture) -> None:
        """Ensure the client runs in the configured project, with GoogleSQL and the cost cap."""
        driver = _driver(mocker)
        config = BigQueryConfig(
            project="analytics-prod",
            table="orders",
            dataset="sales",
            location="EU",
            maximum_bytes_billed=10**9,
        )

        BigQueryConnector(config).connect()

        driver.Client.assert_called_once_with(project="analytics-prod", location="EU")
        driver.QueryJobConfig.assert_called_once_with(
            use_legacy_sql=False,
            default_dataset="analytics-prod.sales",
            maximum_bytes_billed=10**9,
        )

    def test_it_connects_with_a_service_account_key(self, mocker: MockerFixture) -> None:
        """Ensure a key file is loaded by the client's own loader, not a deprecated option."""
        driver = _driver(mocker)
        config = BigQueryConfig(**_BASE, credentials_path="/secrets/bq.json")

        BigQueryConnector(config).connect()

        driver.Client.from_service_account_json.assert_called_once_with(
            "/secrets/bq.json", project="analytics-prod", location=None
        )
        driver.Client.assert_not_called()
        driver.QueryJobConfig.assert_called_once_with(use_legacy_sql=False)

    def test_it_reports_a_failed_connection(self, mocker: MockerFixture) -> None:
        """Ensure an authentication failure is a connector error naming BigQuery."""
        driver = _driver(mocker)
        driver.Client.side_effect = RuntimeError("no default credentials")

        with pytest.raises(ConnectorError, match="Failed to connect to BigQuery"):
            BigQueryConnector(BigQueryConfig(**_BASE)).connect()

    def test_it_imports_the_client_only_when_connecting(self, mocker: MockerFixture) -> None:
        """Ensure the driver loads on first use, so importing Veridelta never imports it."""
        mocker.patch("veridelta.connectors.warehouse.bigquery", None)
        driver = MagicMock()
        importer = mocker.patch(
            "veridelta.connectors.warehouse.importlib.import_module", return_value=driver
        )

        BigQueryConnector(BigQueryConfig(**_BASE)).connect()

        importer.assert_called_once_with("google.cloud.bigquery")
        driver.Client.assert_called_once()

    def test_it_explains_a_missing_extra(self, mocker: MockerFixture) -> None:
        """Ensure a missing client reads as the install hint, not an import trace."""
        mocker.patch("veridelta.connectors.warehouse.bigquery", None)
        mocker.patch(
            "veridelta.connectors.warehouse.importlib.import_module",
            side_effect=ModuleNotFoundError("No module named 'google'"),
        )

        with pytest.raises(ConnectorError, match=r"uv add 'veridelta\[bigquery\]'") as info:
            BigQueryConnector(BigQueryConfig(**_BASE)).connect()

        assert info.value.__cause__ is None

    def test_it_runs_statements_with_the_job_settings(self, mocker: MockerFixture) -> None:
        """Ensure each statement runs under the job config and comes back as Arrow."""
        driver = _driver(mocker)
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        connector.connect()

        frame = connector.execute_pushdown("SELECT 1", query_type="count").collect()

        client = driver.Client.return_value
        client.query.assert_called_once_with(
            "SELECT 1", job_config=driver.QueryJobConfig.return_value
        )
        assert frame.to_dict(as_series=False) == {"n": [1, 2]}

    def test_it_logs_each_statement_by_its_kind_at_info(
        self, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Ensure `--verbose` shows each round-trip and its timing, but never its SQL."""
        _driver(mocker)
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        connector.connect()

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.warehouse"):
            connector.execute_pushdown("SELECT 1 AS hidden_column", query_type="count")

        messages = [record.getMessage() for record in caplog.records]
        assert any(m.startswith("BigQuery count statement completed in") for m in messages)
        assert "hidden_column" not in caplog.text

    def test_it_keeps_the_columns_of_an_empty_result(self, mocker: MockerFixture) -> None:
        """Ensure a zero-row probe still reports its columns."""
        _driver(mocker, [pa.record_batch({"n": pa.array([], pa.int64())})])
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        connector.connect()

        schema = connector.execute_pushdown("SELECT 1", query_type="schema").collect_schema()

        assert schema == pl.Schema({"n": pl.Int64})

    def test_it_refuses_a_result_with_no_batches(self, mocker: MockerFixture) -> None:
        """Ensure no batches is an error, not a table that seems to have no columns."""
        _driver(mocker, [])
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        connector.connect()

        with pytest.raises(ConnectorError, match="no Arrow batches"):
            connector.execute_pushdown("SELECT 1", query_type="schema")

    def test_it_reports_a_failed_statement(self, mocker: MockerFixture) -> None:
        """Ensure a query error is a connector error carrying BigQuery's message."""
        driver = _driver(mocker)
        driver.Client.return_value.query.side_effect = RuntimeError("Not found: Table orders")
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        connector.connect()

        with pytest.raises(ConnectorError, match="Not found: Table orders"):
            connector.execute_pushdown("SELECT 1")

    def test_it_closes_once_and_then_refuses_work(self, mocker: MockerFixture) -> None:
        """Ensure close is idempotent and a closed connector says so."""
        driver = _driver(mocker)
        connector = BigQueryConnector(BigQueryConfig(**_BASE))
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")
        connector.connect()

        connector.close()
        connector.close()

        driver.Client.return_value.close.assert_called_once()
        with pytest.raises(ConnectorError, match="not connected"):
            connector.execute_pushdown("SELECT 1")


def _pair(**overrides: Any) -> tuple[BigQueryConfig, BigQueryConfig]:
    """Build a BigQuery source, and a target that differs by `overrides`."""
    return (
        BigQueryConfig(**{**_BASE, "table": "sales.src"}),
        BigQueryConfig(**{**_BASE, "table": "sales.tgt", **overrides}),
    )


class TestBigQueryRouting:
    """Validate which BigQuery pairs share one pushdown session."""

    def test_it_pushes_a_pair_down_to_bigquery(self, mocker: MockerFixture) -> None:
        """Ensure two tables on one connection open one BigQuery session and close it."""
        connector = mocker.patch("veridelta.engine.BigQueryConnector")
        connector.return_value.execute_pushdown.side_effect = ConnectorError("stop here")
        source, target = _pair()

        with pytest.raises(ConnectorError, match="stop here"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        connector.assert_called_once_with(source)
        connector.return_value.close.assert_called_once()

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({"project": "analytics-dev"}, id="project"),
            pytest.param({"location": "EU"}, id="location"),
            pytest.param({"credentials_path": "/secrets/other.json"}, id="credentials"),
            pytest.param({"maximum_bytes_billed": 10}, id="cost-cap"),
            pytest.param({"dataset": "archive"}, id="default-dataset"),
        ],
    )
    def test_it_keeps_different_connections_apart(
        self, mocker: MockerFixture, overrides: dict[str, Any]
    ) -> None:
        """Ensure every connection setting, but not the table, is part of the fingerprint."""
        connector = mocker.patch("veridelta.engine.BigQueryConnector")
        source, target = _pair(**overrides)

        with pytest.raises(ConnectorError, match="BigQuery connections must match"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, target)

        connector.assert_not_called()

    def test_it_refuses_a_table_compared_with_itself(self) -> None:
        """Ensure one table on one connection is refused before connecting."""
        source = BigQueryConfig(**_BASE)

        with pytest.raises(ConfigError, match="same table"):
            DiffEngine.run_from_configs(DiffConfig(primary_keys=["id"]), source, source)

    def test_it_refuses_bigquery_against_another_warehouse(self) -> None:
        """Ensure a cross-dialect pair names both vendors."""
        snowflake = SnowflakeConfig(
            account="xy12345",
            user="analyst",
            warehouse="COMPUTE_WH",
            database="ANALYTICS",
            schema_name="PUBLIC",
            table="SRC",
        )

        with pytest.raises(ConnectorError, match="source is BigQuery and the target is Snowflake"):
            DiffEngine.run_from_configs(
                DiffConfig(primary_keys=["id"]), BigQueryConfig(**_BASE), snowflake
            )

    def test_validate_reports_a_missing_bigquery_extra(self, mocker: MockerFixture) -> None:
        """Ensure `veridelta validate` finds the client by name, without importing it."""
        find_spec = mocker.patch("veridelta.engine.find_spec", side_effect=ModuleNotFoundError)
        source, target = _pair()

        [finding] = DiffEngine.check_configs(DiffConfig(primary_keys=["id"]), source, target)

        assert finding.message.startswith(
            "Reading the source and target needs the optional 'bigquery'"
        )
        find_spec.assert_called_with("google.cloud.bigquery")

    def test_validate_accepts_an_installed_bigquery_extra(self, mocker: MockerFixture) -> None:
        """Ensure a findable client is enough for the offline check."""
        mocker.patch("veridelta.engine.find_spec", return_value=object())
        source, target = _pair()

        assert DiffEngine.check_configs(DiffConfig(primary_keys=["id"]), source, target) == []

    def test_it_loads_from_yaml_with_the_project_from_the_environment(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ensure a `type: bigquery` block loads, and a `${NAME}` project expands."""
        monkeypatch.setenv("VD_BQ_PROJECT", "analytics-prod")
        path = tmp_path / "veridelta.yaml"
        path.write_text(
            "source:\n  type: bigquery\n  project: ${VD_BQ_PROJECT}\n  table: sales.src\n"
            "target:\n  type: bigquery\n  project: ${VD_BQ_PROJECT}\n  table: sales.tgt\n"
            "primary_keys: [id]\n"
        )

        _, source, target = load_config(path)

        assert isinstance(source, BigQueryConfig)
        assert isinstance(target, BigQueryConfig)
        assert (source.project, target.table) == ("analytics-prod", "sales.tgt")
