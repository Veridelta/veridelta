# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Veridelta configuration and result data models."""

import pytest
from pydantic import ValidationError

from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    DiffSummary,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
)


@pytest.mark.unit
@pytest.mark.fast
class TestDiffRuleValidation:
    """Validate specific field constraints and regex parsing within individual rules."""

    def test_it_rejects_invalid_regex_patterns_at_initialization(self) -> None:
        """Ensure malformed regex in the 'pattern' field raises a ValidationError."""
        with pytest.raises(ValidationError, match="Invalid regex pattern"):
            DiffRule(pattern="[unclosed_bracket")

    def test_it_rejects_invalid_regex_replace_patterns(self) -> None:
        """Ensure malformed regex keys in 'regex_replace' dictionary are caught."""
        with pytest.raises(ValidationError, match="Invalid regex replace pattern"):
            DiffRule(column_names=["col"], regex_replace={"*bad_regex": "clean"})

    def test_it_enforces_positive_numeric_constraints_on_tolerances(self) -> None:
        """Ensure negative values for absolute or relative tolerance are rejected."""
        with pytest.raises(ValidationError):
            DiffRule(column_names=["col"], absolute_tolerance=-1.0)


def _snowflake_table(table: str) -> SnowflakeConfig:
    """Build a Snowflake config with a caller-supplied table name."""
    return SnowflakeConfig(
        account="xy12345",
        user="analyst",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        schema_name="PUBLIC",
        table=table,
    )


def _databricks_table(table: str) -> DatabricksConfig:
    """Build a Databricks config with a caller-supplied table name."""
    return DatabricksConfig(
        server_hostname="adb.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        table=table,
    )


@pytest.mark.unit
@pytest.mark.fast
class TestWarehouseTableAllowlist:
    """Validate dotted SQL identifier allowlists on warehouse table fields."""

    def test_it_accepts_one_to_three_identifier_segments(self) -> None:
        """Ensure catalog.schema.table paths used in YAML remain valid."""
        assert _snowflake_table("ANALYTICS.PUBLIC.SRC").table == "ANALYTICS.PUBLIC.SRC"
        assert _databricks_table("main.default.events").table == "main.default.events"
        assert _snowflake_table("EVENTS").table == "EVENTS"

    def test_it_rejects_sql_metacharacters_spaces_and_extra_segments(self) -> None:
        """Ensure injected SQL, quotes, spaces, and four-part paths fail at parse time."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            _snowflake_table("src; DROP")
        with pytest.raises(ValidationError, match="String should match pattern"):
            _snowflake_table("a.b.c.d")
        with pytest.raises(ValidationError, match="String should match pattern"):
            _databricks_table('tgt"')
        with pytest.raises(ValidationError, match="String should match pattern"):
            _databricks_table("main default events")


@pytest.mark.unit
@pytest.mark.fast
class TestStrictNumericFields:
    """Validate that SQL-adjacent numerics reject string coercion."""

    def test_it_rejects_string_tolerances(self) -> None:
        """Ensure tolerance strings cannot reach SQL numeric literals."""
        with pytest.raises(ValidationError):
            DiffRule.model_validate({"column_names": ["col"], "absolute_tolerance": "0.01"})
        with pytest.raises(ValidationError):
            DiffConfig.model_validate(
                {"primary_keys": ["id"], "default_relative_tolerance": "0.05"}
            )

    def test_it_rejects_coerced_pad_zeros_widths(self) -> None:
        """Ensure a padding width cannot arrive as text or a float."""
        with pytest.raises(ValidationError):
            DiffRule.model_validate({"column_names": ["zip"], "pad_zeros": "5"})
        with pytest.raises(ValidationError):
            DiffRule.model_validate({"column_names": ["zip"], "pad_zeros": 5.0})

        assert DiffRule.model_validate({"column_names": ["zip"], "pad_zeros": 5}).pad_zeros == 5
        assert DiffRule(column_names=["zip"]).pad_zeros is None

    def test_it_preserves_the_declared_type_of_every_sentinel(self) -> None:
        """Ensure -999 stays an int, since coercion would retarget the sentinel."""
        rule = DiffRule.model_validate(
            {"column_names": ["col"], "null_values": ["-999", -999, -9.5, False]}
        )
        assert rule.null_values is not None
        assert [type(value) for value in rule.null_values] == [str, int, float, bool]

        config = DiffConfig.model_validate({"primary_keys": ["id"], "default_null_values": [0]})
        assert config.default_null_values == [0]
        assert type(config.default_null_values[0]) is int

    def test_it_rejects_sentinels_that_could_never_match(self) -> None:
        """Ensure NaN and infinity are refused rather than silently never firing."""
        with pytest.raises(ValidationError, match="non-finite"):
            DiffRule.model_validate({"column_names": ["col"], "null_values": [float("nan")]})
        with pytest.raises(ValidationError, match="non-finite"):
            DiffConfig.model_validate(
                {"primary_keys": ["id"], "default_null_values": [float("inf")]}
            )

    def test_it_rejects_string_time_travel_arguments(self) -> None:
        """Ensure version and snapshot_id cannot be injected as strings."""
        with pytest.raises(ValidationError):
            DeltaLakeConfig.model_validate({"table_uri": "s3://lake/events", "version": "12"})
        with pytest.raises(ValidationError):
            IcebergConfig.model_validate(
                {"table_uri": "s3://lake/iceberg/events", "snapshot_id": "12;"}
            )


@pytest.mark.unit
@pytest.mark.fast
class TestDiffConfigNormalization:
    """Validate the post-initialization normalization logic (lowercase/stripping)."""

    def test_it_standardizes_primary_keys_when_normalization_is_enabled(self) -> None:
        """Ensure primary key headers are trimmed and lowercased automatically."""
        config = DiffConfig(primary_keys=["  User_ID  ", "EMAIL"], normalize_column_names=True)
        assert config.primary_keys == ["user_id", "email"]

    def test_it_standardizes_rule_column_names_when_normalization_is_enabled(self) -> None:
        """Ensure column names within DiffRules are also standardized."""
        config = DiffConfig(
            primary_keys=["id"],
            normalize_column_names=True,
            rules=[DiffRule(column_names=["  ACCOUNT_BAL  "])],
        )
        assert config.rules[0].column_names == ["account_bal"]

    def test_it_preserves_original_casing_when_normalization_is_disabled(self) -> None:
        """Ensure configuration values remain untouched if normalization is False."""
        config = DiffConfig(primary_keys=["User_ID"], normalize_column_names=False)
        assert config.primary_keys == ["User_ID"]


@pytest.mark.unit
@pytest.mark.fast
class TestModelStrictness:
    """Validate that models strictly adhere to the defined schema and typing boundaries."""

    def test_it_forbids_unrecognized_fields_in_config_initialization(self) -> None:
        """Ensure typos in configuration keys trigger an immediate ValidationError."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DiffConfig(primary_keys=["id"], unsupported_plugin_setting=True)  # type: ignore[call-arg]

    def test_it_rejects_thresholds_outside_the_valid_percentage_range(self) -> None:
        """Ensure threshold values strictly evaluate between 0.0 and 1.0."""
        with pytest.raises(ValidationError, match="Input should be less than or equal to 1"):
            DiffConfig(primary_keys=["id"], threshold=1.5)

        with pytest.raises(ValidationError, match="Input should be greater than or equal to 0"):
            DiffConfig(primary_keys=["id"], threshold=-0.1)

    def test_it_rejects_invalid_schema_modes_and_source_formats(self) -> None:
        """Ensure Literal types catch typos and unsupported configurations."""
        with pytest.raises(ValidationError, match="Input should be"):
            DiffConfig(primary_keys=["id"], schema_mode="fuzzy_match")  # type: ignore[arg-type]

        with pytest.raises(ValidationError, match="Input should be"):
            SourceConfig(path="data.csv", format="xls")  # type: ignore[arg-type]


@pytest.mark.unit
@pytest.mark.fast
class TestDiffSummaryCalculations:
    """Validate the computed properties and report generation in the execution summary."""

    def test_it_calculates_aggregated_match_metrics_accurately(self) -> None:
        """Ensure math logic for mismatch ratios and match percentages is correct."""
        summary = DiffSummary(
            total_rows_source=100,
            total_rows_target=110,
            added_count=10,
            removed_count=5,
            changed_count=5,
            is_match=False,
        )

        assert summary.total_mismatches == 20  # 10 + 5 + 5
        assert summary.mismatch_ratio == 0.2  # 20 / 100
        assert summary.match_rate_percentage == 80.0  # (1 - 0.2) * 100
        assert summary.volume_shift == 10  # 110 - 100

    def test_it_generates_a_valid_markdown_report_summary(self) -> None:
        """Ensure the report_summary property produces the expected formatted string."""
        summary = DiffSummary(
            total_rows_source=10,
            total_rows_target=10,
            added_count=0,
            removed_count=0,
            changed_count=0,
            is_match=True,
        )

        assert "Status:        PASSED (Perfect Match)" in summary.report_summary
        assert "Match Rate:    100.0%" in summary.report_summary

    def test_it_omits_column_drifts_from_the_report_when_limit_is_zero(self) -> None:
        """Ensure setting report_limit to 0 safely truncates the markdown output."""
        summary = DiffSummary(
            total_rows_source=100,
            total_rows_target=100,
            added_count=0,
            removed_count=0,
            changed_count=5,
            column_mismatches={"status": 5, "amount": 2},
            is_match=True,
            report_limit=0,
        )

        # The base report should exist, but the column breakdown should be completely absent
        assert "Veridelta Execution Summary" in summary.report_summary
        assert "Top Column-Level Drifts" not in summary.report_summary
        assert "status" not in summary.report_summary

    def test_it_includes_and_sorts_column_drifts_up_to_the_report_limit(self) -> None:
        """Ensure the summary correctly formats, sorts, and truncates column mismatches."""
        summary = DiffSummary(
            total_rows_source=1000,
            total_rows_target=1000,
            added_count=0,
            removed_count=0,
            changed_count=100,
            is_match=False,
            report_limit=2,  # Restrict to only the top 2
            column_mismatches={
                "minor_drift": 5,
                "massive_drift": 80,
                "moderate_drift": 15,
                "tiny_drift": 1,
            },
        )

        report = summary.report_summary

        assert "Top Column-Level Drifts:" in report

        assert "- massive_drift: 80 mismatches" in report
        assert "- moderate_drift: 15 mismatches" in report

        assert "minor_drift" not in report
        assert "tiny_drift" not in report

        assert report.find("massive_drift") < report.find("moderate_drift")
