# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the core DiffEngine, DataIngestor, and Loaders."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from veridelta.engine import DataIngestor, DiffEngine, LoaderFactory
from veridelta.exceptions import ConfigError, DataIntegrityError
from veridelta.models import DiffConfig, DiffRule, SourceConfig


@pytest.mark.unit
@pytest.mark.fast
class TestDataIngestorAndLoaders:
    """Validate data ingestion, loader factories, and pre-engine dataset preparation."""

    def test_it_raises_not_implemented_error_for_unsupported_source_types(self) -> None:
        """Ensure the LoaderFactory guards against unsupported file formats."""
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            LoaderFactory.get_loader("json")

    def test_it_normalizes_headers_by_stripping_and_lowercasing_when_configured(self) -> None:
        """Ensure messy CSV headers are standardized before structural alignment."""
        df = pl.DataFrame({"  Messy_COL  ": [1], "CleanCol": [2]})
        config = DiffConfig(primary_keys=["id"], normalize_column_names=True)

        dummy_cfg = SourceConfig(path="dummy.csv", format="csv")
        ingestor = DataIngestor(config, source_config=dummy_cfg, target_config=dummy_cfg)
        normalized = ingestor._normalize_headers(df.lazy())  # pyright: ignore[reportPrivateUsage]

        assert normalized.collect_schema().names() == ["messy_col", "cleancol"]


@pytest.mark.unit
@pytest.mark.fast
class TestStructuralAlignment:
    """Validate the structural alignment and schema validation heuristics."""

    def test_it_maps_legacy_names_to_the_authoritative_target_schema_before_validation(
        self,
    ) -> None:
        """Ensure structural alignment occurs first to prevent ConfigError on asymmetric schemas."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"user_id": [1, 2], "val": ["A", "B"]})

        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["id"], rename_to="user_id")],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.total_mismatches == 0

    def test_it_drops_columns_flagged_with_ignore_from_both_datasets(self) -> None:
        """Ensure PII or irrelevant columns are excluded from the comparison."""
        src = pl.DataFrame({"id": [1], "secret_hash": ["abc"], "val": [10]})
        tgt = pl.DataFrame({"id": [1], "secret_hash": ["xyz"], "val": [10]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["secret_hash"], ignore=True)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.total_mismatches == 0

    def test_it_aborts_with_config_error_when_primary_keys_are_completely_missing(self) -> None:
        """Ensure validation catches unmapped schemas lacking the required primary key in the source."""
        src = pl.DataFrame({"legacy_id": [1]})
        tgt = pl.DataFrame({"modern_id": [1]})
        config = DiffConfig(primary_keys=["modern_id"])

        with pytest.raises(ConfigError, match="Primary keys missing in SOURCE"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_it_aborts_with_config_error_when_target_is_missing_primary_keys(self) -> None:
        """Ensure validation catches schemas where the target dataset lacks the required primary key."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"wrong_id": [1]})
        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(ConfigError, match="Primary keys missing in TARGET"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_schema_mode_exact_fails_when_target_has_unmapped_columns(self) -> None:
        """Ensure 'exact' schema mode prevents comparisons when schemas deviate at all."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "extra_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="exact")

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_it_validates_schemas_from_zero_row_frames_without_comparing(self) -> None:
        """Ensure validate_schemas enforces SchemaMode on metadata-only frames."""
        src = pl.DataFrame(schema={"id": pl.Int64, "amount": pl.Float64}).lazy()
        drifted = pl.DataFrame(schema={"id": pl.Int64, "surcharge": pl.Float64}).lazy()
        config = DiffConfig(primary_keys=["id"], schema_mode="exact")

        DiffEngine.validate_schemas(config, src, src)

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine.validate_schemas(config, src, drifted)

    def test_schema_mode_allow_additions_passes_when_target_has_new_columns(self) -> None:
        """Ensure 'allow_additions' permits structural drift where target has extra columns."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "new_modern_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_additions")

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()
        assert summary.is_match is True

    def test_schema_mode_allow_additions_fails_when_target_is_missing_source_columns(self) -> None:
        """Ensure 'allow_additions' fails if the target drops any baseline source columns."""
        src = pl.DataFrame({"id": [1], "legacy_col": ["A"]})
        tgt = pl.DataFrame({"id": [1]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_additions")

        with pytest.raises(ConfigError, match="Target is missing required source columns"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_schema_mode_allow_removals_passes_when_target_drops_legacy_columns(self) -> None:
        """Ensure 'allow_removals' permits structural drift where legacy columns are deprecated."""
        src = pl.DataFrame({"id": [1], "legacy_col": ["A"]})
        tgt = pl.DataFrame({"id": [1]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_removals")

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()
        assert summary.is_match is True

    def test_schema_mode_allow_removals_fails_when_target_adds_unauthorized_columns(self) -> None:
        """Ensure 'allow_removals' fails if the target introduces net-new columns."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "new_modern_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_removals")

        with pytest.raises(ConfigError, match="Target contains unauthorized additional columns"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_schema_mode_intersection_safely_ignores_asymmetric_columns_on_both_sides(self) -> None:
        """Ensure 'intersection' (default) cleanly compares only shared columns without throwing errors."""
        src = pl.DataFrame({"id": [1], "shared": ["A"], "only_source": [1]})
        tgt = pl.DataFrame({"id": [1], "shared": ["A"], "only_target": [2]})
        config = DiffConfig(primary_keys=["id"], schema_mode="intersection")

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.total_mismatches == 0


@pytest.mark.unit
@pytest.mark.fast
class TestSemanticNormalization:
    """Validate complex data transformations, strings, and numeric tolerances."""

    def test_it_applies_rules_using_regex_patterns_to_match_multiple_columns(self) -> None:
        """Ensure users can target multiple columns dynamically without explicit naming."""
        src = pl.DataFrame({"id": [1], "amt_usd": [10.5], "amt_eur": [20.0]})
        tgt = pl.DataFrame({"id": [1], "amt_usd": [10.51], "amt_eur": [20.02]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(pattern=r"^amt_.*", absolute_tolerance=0.05)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_chains_regex_type_casting_and_tolerances_successfully_on_a_single_column(
        self, complex_src: pl.DataFrame, complex_tgt: pl.DataFrame
    ) -> None:
        """Ensure the semantic pipeline safely executes multi-step transformations."""
        config = DiffConfig(
            primary_keys=["invoice_id"],
            rules=[
                DiffRule(
                    column_names=["total_billed"],
                    regex_replace={"\\$": ""},
                    cast_to="Float64",
                    absolute_tolerance=0.05,
                ),
                DiffRule(
                    column_names=["category"],
                    value_map={"Enterprise": "ENT", "Premium": "PRM", "Standard": "STD"},
                ),
            ],
        )
        summary = DiffEngine(config, complex_src.lazy(), complex_tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.changed_count == 0

    def test_it_evaluates_strings_as_matches_when_casing_and_whitespace_rules_are_applied(
        self,
    ) -> None:
        """Ensure string normalization rules resolve formatting discrepancies."""
        src = pl.DataFrame({"id": [1], "name": ["   John Doe  "]})
        tgt = pl.DataFrame({"id": [1], "name": ["john doe"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], whitespace_mode="both", case_insensitive=True)],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_sanitizes_strings_using_regex_replace_dictionary_before_comparison(self) -> None:
        """Ensure string contents can be dynamically replaced before diffing occurs."""
        src = pl.DataFrame({"id": [1, 2], "cost": ["$100", "€50"]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": ["100", "50"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["cost"], regex_replace={r"\$|€": ""})],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_coerces_specific_string_values_to_null_before_comparison(self) -> None:
        """Ensure predefined string markers are actively converted to true nulls during processing."""
        src = pl.DataFrame({"id": [1, 2], "status": ["N/A", "Active"]})
        tgt = pl.DataFrame({"id": [1, 2], "status": [None, "Active"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["status"], null_values=["N/A"], treat_null_as_equal=True)
            ],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_evaluates_numeric_differences_using_relative_tolerance_percentage(self) -> None:
        """Ensure proportional differences within a calculated relative tolerance pass validation."""
        src = pl.DataFrame({"id": [1, 2], "metric": [100.0, 100.0]})
        # Row 1 is exactly a 5% difference (passes with 0.05), Row 2 is 6% difference (fails)
        tgt = pl.DataFrame({"id": [1, 2], "metric": [105.0, 106.0]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["metric"], relative_tolerance=0.05)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is False
        assert summary.changed_count == 1
        assert summary.column_mismatches["metric"] == 1

    def test_it_ignores_value_mismatches_in_columns_flagged_to_be_ignored(self) -> None:
        """Ensure columns flagged for explicit ignoring do not trigger value mismatch failures."""
        src = pl.DataFrame({"id": [1], "noise": [100]})
        tgt = pl.DataFrame({"id": [1], "noise": [999]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["noise"], ignore=True)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.changed_count == 0


@pytest.mark.unit
@pytest.mark.fast
class TestEvaluationStrictness:
    """Validate how the engine handles typing mismatches and null evaluations."""

    def test_it_fails_mixed_type_comparisons_when_strict_types_is_enforced(self) -> None:
        """Ensure 'strict_types' immediately flags mismatched data types as failures."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=True)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is False

    def test_it_soft_casts_mixed_types_when_strict_types_is_disabled(self) -> None:
        """Ensure the engine safely soft-casts targets to source types by default."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=False)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_evaluates_nulls_as_mismatches_when_treat_null_as_equal_is_disabled(self) -> None:
        """Ensure identical null records flag as failures when strict null matching is explicitly turned off."""
        src = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Utf8})
        tgt = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Utf8})

        config = DiffConfig(primary_keys=["id"], default_treat_null_as_equal=False)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is False
        assert summary.changed_count == 1


@pytest.mark.unit
@pytest.mark.fast
class TestDataIntegrityAndSetDifferences:
    """Validate set-difference logic, uniqueness constraints, and artifact generation."""

    def test_it_aborts_execution_when_primary_keys_contain_duplicates(self) -> None:
        """Ensure validation catches duplicate keys in the source to prevent Cartesian join explosions."""
        src = pl.DataFrame({"id": [1, 1, 2], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "C"]})
        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(DataIntegrityError, match="not unique in SOURCE dataset"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_it_aborts_execution_when_target_primary_keys_contain_duplicates(self) -> None:
        """Ensure validation catches duplicate keys natively inside the target dataset."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [1, 1, 2], "val": ["A", "A", "B"]})
        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(DataIntegrityError, match="not unique in TARGET dataset"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()

    def test_it_correctly_isolates_added_and_removed_records(self) -> None:
        """Ensure anti-joins accurately route missing records to the correct summary tallies."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["B", "C"]})
        config = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.removed_count == 1
        assert summary.added_count == 1
        assert summary.changed_count == 0

    def test_it_gracefully_handles_completely_empty_dataframes(self) -> None:
        """Ensure edge-case comparisons of empty datasets return a clean pass."""
        src = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        tgt = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        config = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_exports_discrepancy_artifacts_when_output_path_is_provided(
        self, tmp_path: Path
    ) -> None:
        """Ensure the engine writes 'Added', 'Removed', and 'Changed' datasets to disk."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})

        config = DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format="parquet")
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert (tmp_path / "added_rows.parquet").exists()
        assert (tmp_path / "removed_rows.parquet").exists()
        assert (tmp_path / "changed_rows.parquet").exists()
        assert summary.artifacts_written is True

    def test_it_skips_artifact_export_when_dataset_is_completely_empty(
        self, tmp_path: Path
    ) -> None:
        """Ensure entirely matching datasets do not generate empty zero-byte discrepancy files on disk."""
        src = pl.DataFrame({"id": [1], "val": ["A"]})
        tgt = pl.DataFrame({"id": [1], "val": ["A"]})

        config = DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format="parquet")
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert not (tmp_path / "added_rows.parquet").exists()
        assert not (tmp_path / "removed_rows.parquet").exists()
        assert not (tmp_path / "changed_rows.parquet").exists()
        assert summary.artifacts_written is False

    def test_it_raises_not_implemented_error_when_exporting_to_unsupported_formats(
        self, tmp_path: Path
    ) -> None:
        """Ensure the I/O layer guards against unsupported artifact export formats."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})

        config = DiffConfig(
            primary_keys=["id"],
            output_path=str(tmp_path),
            output_format="excel",
        )

        with pytest.raises(NotImplementedError, match="not yet implemented"):
            DiffEngine(config, src.lazy(), tgt.lazy()).run()


def _normalized(config: DiffConfig, frame: pl.DataFrame) -> pl.DataFrame:
    """Run one frame through the source-side normalizer and materialize it."""
    engine = DiffEngine(config, frame.lazy(), frame.lazy())
    return engine._normalize_frame(  # pyright: ignore[reportPrivateUsage]
        frame.lazy(), is_source=True
    ).collect()


@pytest.mark.unit
@pytest.mark.fast
class TestCanonicalTransformPipeline:
    """Validate the single normalization pass shared by keys and compared columns."""

    def test_it_applies_regex_replace_exactly_once(self) -> None:
        """Ensure a non-idempotent pattern is not applied twice across two code paths."""
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["code"], regex_replace={"a": "aa"})],
        )
        result = _normalized(config, pl.DataFrame({"id": [1], "code": ["a"]}))

        assert result["code"][0] == "aa"

    def test_it_normalizes_primary_keys_so_they_join_after_cleaning(self) -> None:
        """Ensure whitespace and casing rules now reach key columns before the join."""
        src = pl.DataFrame({"id": ["  A  "], "val": [1]})
        tgt = pl.DataFrame({"id": ["a"], "val": [1]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["id"], whitespace_mode="both", case_insensitive=True)],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.added_count == 0
        assert summary.removed_count == 0
        assert summary.is_match is True

    def test_it_detects_duplicate_keys_created_by_normalization(self) -> None:
        """Ensure uniqueness is asserted on normalized keys, not the raw ones."""
        frame = pl.DataFrame({"id": ["A", "a"], "val": [1, 2]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["id"], case_insensitive=True)]
        )

        with pytest.raises(DataIntegrityError, match="not unique in SOURCE dataset"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_skips_string_sentinels_on_numeric_columns(self) -> None:
        """Ensure a global null_values list cannot fail a run containing numbers."""
        src = pl.DataFrame({"id": [1], "amount": [10.0], "status": ["N/A"]})
        tgt = pl.DataFrame({"id": [1], "amount": [10.0], "status": [None]})

        config = DiffConfig(primary_keys=["id"], default_null_values=["N/A"])
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_pads_both_sides_so_numeric_and_text_codes_converge(self) -> None:
        """Ensure pad_zeros stringifies first, letting 123 match '00123'."""
        src = pl.DataFrame({"id": [1], "code": [123]})
        tgt = pl.DataFrame({"id": [1], "code": ["00123"]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["code"], pad_zeros=5)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True
        assert summary.changed_count == 0

    def test_it_parses_strings_into_datetimes_so_they_compare_as_timestamps(self) -> None:
        """Ensure datetime_format crosses the dtype branch instead of comparing text."""
        src = pl.DataFrame({"id": [1], "ts": ["2024-01-05 10:00:00"]})
        tgt = pl.DataFrame({"id": [1], "ts": [datetime(2024, 1, 5, 10, 0, 0)]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S")],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert summary.is_match is True

    def test_it_nulls_values_that_do_not_match_the_datetime_format(self) -> None:
        """Ensure unparseable timestamps become NULL rather than aborting the run."""
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d")],
        )
        result = _normalized(config, pl.DataFrame({"id": [1], "ts": ["not-a-date"]}))

        assert result["ts"][0] is None
        assert result.schema["ts"] == pl.Datetime("us")

    def test_it_converts_timezone_aware_columns_to_the_requested_zone(self) -> None:
        """Ensure tz-aware timestamps are shifted into the configured zone."""
        frame = pl.DataFrame({"id": [1], "ts": [datetime(2024, 1, 5, 15, 0)]}).with_columns(
            pl.col("ts").dt.replace_time_zone("UTC")
        )
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], timezone="America/New_York")],
        )
        result = _normalized(config, frame)
        dtype = result.schema["ts"]

        assert isinstance(dtype, pl.Datetime)
        assert dtype.time_zone == "America/New_York"
        assert result["ts"][0].hour == 10

    def test_it_refuses_to_guess_a_zone_for_naive_timestamps(self) -> None:
        """Ensure naive data fails loudly rather than being silently read as UTC."""
        frame = pl.DataFrame({"id": [1], "ts": [datetime(2024, 1, 5, 15, 0)]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["ts"], timezone="UTC")]
        )

        with pytest.raises(ConfigError, match="timezone-naive"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_rejects_a_timezone_rule_on_a_non_timestamp_column(self) -> None:
        """Ensure a zone cannot be applied to text that was never parsed."""
        frame = pl.DataFrame({"id": [1], "ts": ["2024-01-05"]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["ts"], timezone="UTC")]
        )

        with pytest.raises(ConfigError, match=r"not a\s+timestamp"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_rejects_an_unknown_timezone_name(self) -> None:
        """Ensure a typo surfaces as a ConfigError naming the column, not a raw Polars error."""
        frame = pl.DataFrame({"id": [1], "ts": [datetime(2024, 1, 5, 15, 0)]}).with_columns(
            pl.col("ts").dt.replace_time_zone("UTC")
        )
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["ts"], timezone="Americas/New_York")],
        )

        with pytest.raises(ConfigError, match="unusable timezone"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_parses_then_converts_when_the_format_carries_an_offset(self) -> None:
        """Ensure stage 6a feeds stage 6b, so parsed offsets become convertible."""
        frame = pl.DataFrame({"id": [1], "ts": ["2024-01-05 15:00:00+0000"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["ts"],
                    datetime_format="%Y-%m-%d %H:%M:%S%z",
                    timezone="America/New_York",
                )
            ],
        )
        result = _normalized(config, frame)
        dtype = result.schema["ts"]

        assert isinstance(dtype, pl.Datetime)
        assert dtype.time_zone == "America/New_York"
        assert result["ts"][0].hour == 10

    def test_it_applies_the_value_map_to_the_source_side_only(self) -> None:
        """Ensure the crosswalk rewrites source values without touching the target."""
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["tier"], value_map={"M": "Male"})]
        )
        frame = pl.DataFrame({"id": [1], "tier": ["M"]})
        engine = DiffEngine(config, frame.lazy(), frame.lazy())
        target = engine._normalize_frame(  # pyright: ignore[reportPrivateUsage]
            frame.lazy(), is_source=False
        ).collect()

        assert _normalized(config, frame)["tier"][0] == "Male"
        assert target["tier"][0] == "M"
