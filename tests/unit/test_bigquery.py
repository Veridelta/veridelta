# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for BigQuery: its connection model, SQL dialect, and connector."""

from typing import Any

import pytest
from pydantic import ValidationError

from veridelta.connectors.sql import SQLDialect, SQLPushdownCompiler, _reads_offset
from veridelta.exceptions import ConfigError
from veridelta.models import BigQueryConfig, DiffRule

_BASE: dict[str, Any] = {"project": "analytics-prod", "table": "sales.orders"}
"""The fields every BigQuery config needs."""


@pytest.mark.unit
@pytest.mark.fast
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


@pytest.mark.unit
@pytest.mark.fast
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

    def test_it_escapes_line_breaks_in_literals(self) -> None:
        """Ensure a value with a line break stays one valid literal."""
        predicate = _predicate(value_map={"a\nb": "c"})

        assert "'a\\nb'" in predicate
        assert "\n" not in predicate
