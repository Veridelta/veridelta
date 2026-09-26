# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for Veridelta configuration and result data models."""

import polars as pl
import pytest
from pydantic import BaseModel, ValidationError
from pytest_mock import MockerFixture

from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import (
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    DiffSummary,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    ValueMapEntry,
    ValueMapProposal,
)


@pytest.mark.unit
@pytest.mark.fast
class TestDiffRuleValidation:
    """Validate specific field constraints and regex parsing within individual rules."""

    def test_it_rejects_invalid_regex_patterns_at_initialization(self) -> None:
        """Ensure malformed regex in the 'pattern' field raises a ValidationError."""
        with pytest.raises(ValidationError, match="Invalid regex pattern"):
            DiffRule(pattern="[unclosed_bracket")

    def test_it_accepts_an_explicitly_empty_pattern(self) -> None:
        """Ensure the validator's None branch is taken, not only the compile path."""
        assert DiffRule.model_validate({"pattern": None}).pattern is None

    def test_it_rejects_invalid_regex_replace_patterns(self) -> None:
        """Ensure malformed regex keys in 'regex_replace' dictionary are caught."""
        with pytest.raises(ValidationError, match="Invalid regex replace pattern"):
            DiffRule(column_names=["col"], regex_replace={"*bad_regex": "clean"})

    def test_it_enforces_positive_numeric_constraints_on_tolerances(self) -> None:
        """Ensure negative values for absolute or relative tolerance are rejected."""
        with pytest.raises(ValidationError):
            DiffRule(column_names=["col"], absolute_tolerance=-1.0)


_SECRET = "hunter2-do-not-print"
"""Credential that must never appear in an error message."""


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

    def test_it_rejects_infinite_tolerances(self) -> None:
        """Ensure an infinite tolerance fails when the config loads.

        Locally it quietly passes every row, and in a warehouse the bare `inf`
        it renders as is not a valid literal, so the two engines could only
        disagree. `ignore` is the way to stop comparing a column.
        """
        with pytest.raises(ValidationError):
            DiffRule(column_names=["amount"], absolute_tolerance=float("inf"))
        with pytest.raises(ValidationError):
            DiffRule(column_names=["amount"], relative_tolerance=float("inf"))
        with pytest.raises(ValidationError):
            DiffConfig(primary_keys=["id"], default_absolute_tolerance=float("inf"))
        with pytest.raises(ValidationError):
            DiffConfig(primary_keys=["id"], default_relative_tolerance=float("inf"))

    def test_it_rejects_an_empty_primary_key_list(self) -> None:
        """Ensure a comparison cannot be configured without a join key."""
        with pytest.raises(ValidationError, match="at least 1"):
            DiffConfig(primary_keys=[])

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
class TestSimilarityThresholds:
    """Validate the text similarity limits a rule can loosen a comparison by."""

    def test_it_accepts_one_similarity_measure_per_rule(self) -> None:
        """Ensure either measure loads on its own, with an integral floor widened."""
        edits = DiffRule.model_validate({"column_names": ["name"], "max_levenshtein_distance": 2})
        similar = DiffRule.model_validate(
            {"column_names": ["name"], "min_jaro_winkler_similarity": 1}
        )

        assert edits.max_levenshtein_distance == 2
        assert edits.min_jaro_winkler_similarity is None
        assert similar.min_jaro_winkler_similarity == 1.0
        assert DiffRule(column_names=["name"]).max_levenshtein_distance is None

    def test_it_rejects_both_measures_on_one_rule(self) -> None:
        """Ensure a rule cannot ask for two verdicts on the same pair of values."""
        with pytest.raises(ValidationError, match="not both"):
            DiffRule(
                column_names=["name"],
                max_levenshtein_distance=1,
                min_jaro_winkler_similarity=0.9,
            )

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param(0, "greater than or equal to 1", id="zero"),
            pytest.param(-1, "greater than or equal to 1", id="negative"),
            pytest.param(2.0, "valid integer", id="float"),
            pytest.param("2", "valid integer", id="text"),
            pytest.param(True, "valid integer", id="bool"),
        ],
    )
    def test_it_rejects_an_unusable_edit_distance(self, value: object, message: str) -> None:
        """Ensure the limit is a whole number of edits that can reach SQL as written."""
        with pytest.raises(ValidationError, match=message):
            DiffRule.model_validate({"column_names": ["name"], "max_levenshtein_distance": value})

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            pytest.param(0.0, "greater than 0", id="zero"),
            pytest.param(1.5, "less than or equal to 1", id="above-one"),
            pytest.param(float("nan"), "finite number", id="nan"),
            pytest.param(float("inf"), "finite number", id="infinity"),
            pytest.param("0.9", "valid number", id="text"),
            pytest.param(True, "valid number", id="bool"),
        ],
    )
    def test_it_rejects_an_unusable_similarity_floor(self, value: object, message: str) -> None:
        """Ensure the floor is a finite share of a perfect score."""
        with pytest.raises(ValidationError, match=message):
            DiffRule.model_validate(
                {"column_names": ["name"], "min_jaro_winkler_similarity": value}
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

    def test_it_standardizes_rename_targets_when_normalization_is_enabled(self) -> None:
        """Ensure a rename lands on the normalized target header rather than beside it.

        Headers are lowercased on both sides, so a `rename_to` left in its
        original case would pair with nothing, and under `intersection` the
        column would silently drop out of the comparison.
        """
        config = DiffConfig(
            primary_keys=["id"],
            normalize_column_names=True,
            rules=[DiffRule(column_names=[" Legacy_Amt "], rename_to=" Amount ")],
        )
        assert config.rules[0].column_names == ["legacy_amt"]
        assert config.rules[0].rename_to == "amount"

    def test_it_leaves_the_callers_rules_untouched(self) -> None:
        """Ensure normalization copies rules instead of rewriting objects the caller holds."""
        rule = DiffRule(column_names=["Amount"], rename_to="Total")

        config = DiffConfig(primary_keys=["id"], normalize_column_names=True, rules=[rule])

        assert rule.column_names == ["Amount"]
        assert rule.rename_to == "Total"
        assert config.rules[0].column_names == ["amount"]

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

    @pytest.mark.parametrize(
        ("model", "fields"),
        [
            pytest.param(
                SnowflakeConfig,
                {
                    "table": "T",
                    "user": "u",
                    "warehouse": "w",
                    "database": "d",
                    "schema_name": "s",
                    "password": _SECRET,
                },
                id="snowflake-password",
            ),
            pytest.param(
                DatabricksConfig,
                {"table": "t", "http_path": "/sql", "access_token": _SECRET},
                id="databricks-token",
            ),
            pytest.param(
                DeltaLakeConfig, {"storage_options": {"AWS_SECRET_ACCESS_KEY": _SECRET}}, id="delta"
            ),
            pytest.param(
                IcebergConfig, {"storage_options": {"AWS_SECRET_ACCESS_KEY": _SECRET}}, id="iceberg"
            ),
            pytest.param(
                SourceConfig,
                {"options": {"storage_options": {"AWS_SECRET_ACCESS_KEY": _SECRET}}},
                id="file-options",
            ),
            pytest.param(
                DatabaseConfig,
                {"uri": f"postgresql://analyst:{_SECRET}@db.internal/sales"},
                id="database-uri",
            ),
            pytest.param(
                DatabaseConfig,
                {"uri": "postgresql://analyst@db.internal/sales", "password": _SECRET},
                id="database-password",
            ),
        ],
    )
    def test_it_keeps_credentials_out_of_validation_errors(
        self, model: type[BaseModel], fields: dict[str, object]
    ) -> None:
        """Ensure a rejected connection never repeats the credentials it was given.

        Pydantic quotes the whole input in its error text, so a config missing
        one required field used to print its password alongside the complaint.
        """
        with pytest.raises(ValidationError) as exc_info:
            model(**fields)

        assert _SECRET not in str(exc_info.value)

    @pytest.mark.parametrize(
        ("config", "field", "value", "replacement"),
        [
            pytest.param(
                SnowflakeConfig(
                    table="T",
                    account="a",
                    user="u",
                    warehouse="w",
                    database="d",
                    schema_name="s",
                    password=_SECRET,
                ),
                "password",
                _SECRET,
                "another-secret",
                id="snowflake-password",
            ),
            pytest.param(
                DatabricksConfig(
                    table="t", server_hostname="h", http_path="/sql", access_token=_SECRET
                ),
                "access_token",
                _SECRET,
                "another-secret",
                id="databricks-token",
            ),
            pytest.param(
                DeltaLakeConfig(
                    table_uri="s3://lake/events",
                    storage_options={"AWS_SECRET_ACCESS_KEY": _SECRET},
                ),
                "storage_options",
                {"AWS_SECRET_ACCESS_KEY": _SECRET},
                {"AWS_SECRET_ACCESS_KEY": "another-secret"},
                id="delta-storage-options",
            ),
            pytest.param(
                IcebergConfig(
                    table_uri="s3://lake/iceberg/events",
                    storage_options={"AWS_SECRET_ACCESS_KEY": _SECRET},
                ),
                "storage_options",
                {"AWS_SECRET_ACCESS_KEY": _SECRET},
                {"AWS_SECRET_ACCESS_KEY": "another-secret"},
                id="iceberg-storage-options",
            ),
            pytest.param(
                DatabaseConfig(
                    uri="postgresql://analyst@db.internal/sales", password=_SECRET, table="orders"
                ),
                "password",
                _SECRET,
                "another-secret",
                id="database-password",
            ),
        ],
    )
    def test_it_keeps_credentials_out_of_printed_configs(
        self, config: BaseModel, field: str, value: object, replacement: object
    ) -> None:
        """Ensure printing or logging a connection never shows its secret.

        The connector still needs the value, so it stays readable as an
        attribute and in `model_dump()`, and still tells two connections apart.
        """
        assert _SECRET not in repr(config)
        assert _SECRET not in str(config)
        assert _SECRET not in f"{config}"
        assert field not in repr(config)
        assert getattr(config, field) == value
        assert config.model_dump()[field] == value
        assert config != config.model_copy(update={field: replacement})

    def test_it_keeps_nested_storage_options_out_of_printed_file_configs(self) -> None:
        """Ensure a reader's object-store credentials are hidden while its settings still show.

        Reader `options` mostly hold settings worth seeing when debugging, so
        only the nested `storage_options` map is left out. The reader still
        receives it, so it stays in the attribute and in `model_dump()`.
        """
        options = {"separator": ";", "storage_options": {"aws_secret_access_key": _SECRET}}
        config = SourceConfig(path="s3://lake/events.csv", options=options)

        assert _SECRET not in repr(config)
        assert _SECRET not in str(config)
        assert _SECRET not in f"{config}"
        # Rich displays are built from the same arguments.
        assert _SECRET not in str(list(config.__repr_args__()))
        assert "storage_options" not in repr(config)
        assert "options={'separator': ';'}" in repr(config)
        assert config.options == options
        assert config.model_dump()["options"] == options
        assert config != config.model_copy(
            update={"options": {**options, "storage_options": {"aws_secret_access_key": "other"}}}
        )

    def test_it_prints_file_options_without_storage_options_unchanged(self) -> None:
        """Ensure a config with no nested credentials prints every reader option."""
        config = SourceConfig(path="data.csv", options={"separator": ";", "has_header": False})

        assert repr(config) == (
            "SourceConfig(type='file', path='data.csv', format='csv', "
            "options={'separator': ';', 'has_header': False})"
        )


@pytest.mark.unit
@pytest.mark.fast
class TestDatabaseConfig:
    """Validate the settings a database source reads a table or query with."""

    def test_it_reads_a_table_or_runs_a_query(self) -> None:
        """Ensure either way of naming the rows is accepted on its own."""
        by_table = DatabaseConfig(
            uri="postgresql://analyst@db.internal/sales", table="public.orders"
        )
        by_query = DatabaseConfig(uri="sqlite:///srv/legacy.db", query="SELECT * FROM orders")

        assert by_table.type == "database"
        assert (by_table.table, by_table.query) == ("public.orders", None)
        assert (by_query.table, by_query.query) == (None, "SELECT * FROM orders")

    @pytest.mark.parametrize(
        "fields",
        [
            pytest.param({}, id="neither"),
            pytest.param({"table": "orders", "query": "SELECT 1"}, id="both"),
        ],
    )
    def test_it_requires_exactly_one_of_table_or_query(self, fields: dict[str, str]) -> None:
        """Ensure a source never leaves open which rows it reads."""
        with pytest.raises(ValidationError, match="set exactly one"):
            DatabaseConfig.model_validate({"uri": "sqlite:///srv/legacy.db", **fields})

    def test_it_rejects_an_empty_query(self) -> None:
        """Ensure an empty statement fails at load time rather than in the driver."""
        with pytest.raises(ValidationError, match="at least 1 character"):
            DatabaseConfig(uri="sqlite:///srv/legacy.db", query="")

    @pytest.mark.parametrize(
        "table",
        [
            pytest.param("orders; DROP TABLE orders", id="statement"),
            pytest.param("a.b.c.d", id="four-segments"),
            pytest.param('"orders"', id="quoted"),
        ],
    )
    def test_it_rejects_a_table_outside_the_allowlist(self, table: str) -> None:
        """Ensure a table name can never carry SQL of its own."""
        with pytest.raises(ValidationError, match="should match pattern"):
            DatabaseConfig(uri="sqlite:///srv/legacy.db", table=table)

    def test_it_requires_a_scheme(self) -> None:
        """Ensure a bare path fails with a hint rather than inside the driver."""
        with pytest.raises(ValidationError, match="needs a scheme"):
            DatabaseConfig(uri="/srv/legacy.db", table="orders")

    def test_it_rejects_a_password_in_both_places(self) -> None:
        """Ensure there is never a question which of two passwords is used."""
        with pytest.raises(ValidationError, match="not both"):
            DatabaseConfig(
                uri="postgresql://analyst:first@db.internal/sales", password="second", table="t"
            )

    def test_it_requires_a_user_for_the_password(self) -> None:
        """Ensure a password is never sent without a user to go with it."""
        with pytest.raises(ValidationError, match="needs a user name"):
            DatabaseConfig(uri="postgresql://db.internal/sales", password=_SECRET, table="t")

    def test_it_reports_an_unparseable_uri_without_repeating_it(self) -> None:
        """Ensure a malformed URI names the problem but never echoes its secret."""
        with pytest.raises(ValidationError, match="Invalid IPv6 URL") as exc_info:
            DatabaseConfig(uri=f"postgresql://analyst:{_SECRET}@[::1/sales", table="t")

        assert _SECRET not in str(exc_info.value)

    def test_it_is_frozen_and_forbids_unknown_fields(self) -> None:
        """Ensure a typo fails at load time and a loaded source cannot change."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            DatabaseConfig(uri="sqlite:///srv/legacy.db", table="t", tabel="t")  # type: ignore[call-arg]

        config = DatabaseConfig(uri="sqlite:///srv/legacy.db", table="t")
        with pytest.raises(ValidationError, match="frozen"):
            config.table = "other"  # type: ignore[misc]

    def test_it_masks_the_password_inside_a_printed_uri(self) -> None:
        """Ensure a password written into the URI is hidden while the rest stays readable.

        The URI is the part worth seeing when a connection fails, so only its
        password is replaced. The connector still needs the real one, which
        stays in the attribute and in `model_dump()`.
        """
        uri = f"postgresql://analyst:{_SECRET}@db.internal:5432/sales?sslmode=require"
        config = DatabaseConfig(uri=uri, table="orders")
        masked = "postgresql://analyst:***@db.internal:5432/sales?sslmode=require"

        assert config.redacted_uri == masked
        assert _SECRET not in repr(config)
        assert _SECRET not in str(config)
        # Rich displays are built from the same arguments.
        assert _SECRET not in str(list(config.__repr_args__()))
        assert f"uri='{masked}'" in repr(config)
        assert config.uri == uri
        assert config.model_dump()["uri"] == uri

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param("postgresql://analyst@db.internal/sales", id="user-only"),
            pytest.param("sqlite:///srv/legacy.db", id="sqlite"),
        ],
    )
    def test_it_prints_a_uri_without_a_password_unchanged(self, uri: str) -> None:
        """Ensure masking only ever replaces a password that is there."""
        config = DatabaseConfig(uri=uri, table="orders")

        assert config.redacted_uri == uri
        assert f"uri='{uri}'" in repr(config)


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


@pytest.mark.unit
@pytest.mark.fast
class TestDiffResultRowAccess:
    """Validate row-level access to the discrepancies behind the summary."""

    @staticmethod
    def _run() -> DiffResult:
        """Run a two-column comparison with drift in each column.

        Returns:
            DiffResult: Result carrying the changed rows.
        """
        src = pl.DataFrame({"id": [1, 2, 3], "status": ["A", "B", "C"], "amount": [10, 20, 30]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "status": ["A", "X", "C"], "amount": [10, 20, 99]})
        return DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

    def test_it_exposes_the_frames_behind_the_counts(self) -> None:
        """Ensure the rows the engine already materialized are reachable."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})

        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        assert result.added.height == result.summary.added_count == 1
        assert result.removed.height == result.summary.removed_count == 1
        assert result.changed.height == result.summary.changed_count == 1
        assert result.primary_keys == ("id",)
        assert result.keys_only is False

    def test_it_isolates_the_rows_where_one_column_drifted(self) -> None:
        """Ensure get_mismatches narrows to a single column's disagreements."""
        mismatches = self._run().get_mismatches("amount")

        assert mismatches.columns == ["id", "amount_source", "amount_target"]
        assert mismatches.to_dicts() == [{"id": 3, "amount_source": 30, "amount_target": 99}]

    def test_it_excludes_rows_that_drifted_only_in_another_column(self) -> None:
        """Ensure a row changed elsewhere does not appear under this column.

        The changed frame holds every row with any drift, so filtering on the
        wrong flag would report row 3 as a status mismatch.
        """
        mismatches = self._run().get_mismatches("status")

        assert mismatches.to_dicts() == [{"id": 2, "status_source": "B", "status_target": "X"}]

    def test_it_rejects_a_column_that_was_not_compared(self) -> None:
        """Ensure a mistyped or excluded column fails loudly."""
        with pytest.raises(ConfigError, match="was not compared"):
            self._run().get_mismatches("amont")

    def test_it_rejects_an_ignored_column(self) -> None:
        """Ensure an ignored column is reported as uncompared, not as clean."""
        src = pl.DataFrame({"id": [1], "val": ["A"], "audit": ["x"]})
        tgt = pl.DataFrame({"id": [1], "val": ["A"], "audit": ["y"]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["audit"], ignore=True)]
        )

        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ("val",)
        with pytest.raises(ConfigError, match="was not compared"):
            result.get_mismatches("audit")

    def test_it_names_the_column_by_its_target_name_after_a_rename(self) -> None:
        """Ensure a renamed column is addressed the way the target spells it."""
        src = pl.DataFrame({"legacy_id": [1], "val": ["A"]})
        tgt = pl.DataFrame({"user_id": [1], "val": ["B"]})
        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )

        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ("val",)
        assert result.get_mismatches("val").height == 1

    def test_it_returns_an_empty_frame_for_a_clean_column(self) -> None:
        """Ensure a compared column with no drift yields zero rows, not an error."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"], "note": ["x", "CHANGED"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "B"], "note": ["x", "y"]})

        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        assert result.get_mismatches("val").height == 0
        assert result.get_mismatches("note").height == 1

    def test_it_converts_the_changed_rows_to_pandas(self) -> None:
        """Ensure notebook users can reach the drift without exporting artifacts."""
        pytest.importorskip("pandas")

        frame = self._run().to_pandas()

        assert list(frame.columns)[:1] == ["id"]
        assert len(frame) == 2

    def test_it_explains_a_missing_pandas_install(self, mocker: MockerFixture) -> None:
        """Ensure the optional dependency surfaces as a Veridelta error.

        Polars raises a bare `ModuleNotFoundError` here, which reaches a CLI or
        notebook user as an unhandled traceback rather than as guidance.
        """
        mocker.patch.object(pl.DataFrame, "to_pandas", side_effect=ImportError("no pandas"))

        with pytest.raises(ConfigError, match="requires pandas and pyarrow"):
            self._run().to_pandas()


@pytest.mark.unit
@pytest.mark.fast
class TestValueMapProposalModels:
    """Validate the evidence and rules a value_map proposal carries."""

    def test_it_reports_confidence_as_the_share_of_rows_that_agree(self) -> None:
        """Ensure 999 agreeing rows out of 1,000 read as 99.9%, in Python and in JSON."""
        entry = ValueMapEntry(source_value="M", target_value="Male", rows=1000, agreeing_rows=999)

        assert entry.confidence == 0.999
        assert entry.model_dump(mode="json")["confidence"] == 0.999

    def test_it_is_frozen(self) -> None:
        """Ensure evidence cannot be edited after the engine reports it."""
        entry = ValueMapEntry(source_value="M", target_value="Male", rows=5, agreeing_rows=5)

        with pytest.raises(ValidationError, match="frozen"):
            entry.rows = 6  # type: ignore[misc]

    @pytest.mark.parametrize(
        ("rows", "agreeing_rows", "message"),
        [
            pytest.param(0, 1, "greater than or equal to 1", id="no-rows"),
            pytest.param(5, 0, "greater than or equal to 1", id="nothing-agrees"),
            pytest.param(5, 6, "cannot exceed", id="more-agree-than-exist"),
        ],
    )
    def test_it_rejects_counts_that_cannot_happen(
        self, rows: int, agreeing_rows: int, message: str
    ) -> None:
        """Ensure a share above one or an empty entry cannot be constructed."""
        with pytest.raises(ValidationError, match=message):
            ValueMapEntry(
                source_value="M", target_value="Male", rows=rows, agreeing_rows=agreeing_rows
            )

    def test_it_renders_a_standalone_rule_for_the_column(self) -> None:
        """Ensure the proposed map becomes a rule a configuration can hold as written."""
        proposal = ValueMapProposal(
            column="gender",
            value_map={"F": "Female", "M": "Male"},
            entries=(
                ValueMapEntry(source_value="M", target_value="Male", rows=5, agreeing_rows=5),
            ),
            governing_rule_index=0,
        )

        rule = proposal.to_rule()

        assert rule.model_dump(exclude_none=True, exclude_defaults=True) == {
            "column_names": ["gender"],
            "value_map": {"F": "Female", "M": "Male"},
        }
