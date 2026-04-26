"""Unit tests for the core DiffEngine, DataIngestor, and Loaders."""

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
        normalized = ingestor._normalize_headers(df)  # pyright: ignore[reportPrivateUsage]

        assert normalized.columns == ["messy_col", "cleancol"]


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
        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is True
        assert summary.total_mismatches == 0

    def test_it_drops_columns_flagged_with_ignore_from_both_datasets(self) -> None:
        """Ensure PII or irrelevant columns are excluded from the comparison."""
        src = pl.DataFrame({"id": [1], "secret_hash": ["abc"], "val": [10]})
        tgt = pl.DataFrame({"id": [1], "secret_hash": ["xyz"], "val": [10]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["secret_hash"], ignore=True)]
        )
        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is True
        assert summary.total_mismatches == 0

    def test_it_aborts_with_config_error_when_primary_keys_are_completely_missing(self) -> None:
        """Ensure validation catches unmapped schemas lacking the required primary key."""
        src = pl.DataFrame({"legacy_id": [1]})
        tgt = pl.DataFrame({"modern_id": [1]})
        config = DiffConfig(primary_keys=["modern_id"])

        with pytest.raises(ConfigError, match="Primary keys missing in SOURCE"):
            DiffEngine(config, src, tgt).run()

    def test_schema_mode_exact_fails_when_target_has_unmapped_columns(self) -> None:
        """Ensure 'exact' schema mode prevents comparisons when schemas deviate at all."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "extra_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="exact")

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine(config, src, tgt).run()

    def test_schema_mode_allow_additions_passes_when_target_has_new_columns(self) -> None:
        """Ensure 'allow_additions' permits structural drift where target has extra columns."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "new_modern_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_additions")

        summary = DiffEngine(config, src, tgt).run()
        assert summary.is_match is True

    def test_schema_mode_allow_additions_fails_when_target_is_missing_source_columns(self) -> None:
        """Ensure 'allow_additions' fails if the target drops any baseline source columns."""
        src = pl.DataFrame({"id": [1], "legacy_col": ["A"]})
        tgt = pl.DataFrame({"id": [1]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_additions")

        with pytest.raises(ConfigError, match="Target is missing required source columns"):
            DiffEngine(config, src, tgt).run()

    def test_schema_mode_allow_removals_passes_when_target_drops_legacy_columns(self) -> None:
        """Ensure 'allow_removals' permits structural drift where legacy columns are deprecated."""
        src = pl.DataFrame({"id": [1], "legacy_col": ["A"]})
        tgt = pl.DataFrame({"id": [1]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_removals")

        summary = DiffEngine(config, src, tgt).run()
        assert summary.is_match is True

    def test_schema_mode_allow_removals_fails_when_target_adds_unauthorized_columns(self) -> None:
        """Ensure 'allow_removals' fails if the target introduces net-new columns."""
        src = pl.DataFrame({"id": [1]})
        tgt = pl.DataFrame({"id": [1], "new_modern_col": ["A"]})
        config = DiffConfig(primary_keys=["id"], schema_mode="allow_removals")

        with pytest.raises(ConfigError, match="Target contains unauthorized additional columns"):
            DiffEngine(config, src, tgt).run()

    def test_schema_mode_intersection_safely_ignores_asymmetric_columns_on_both_sides(self) -> None:
        """Ensure 'intersection' (default) cleanly compares only shared columns without throwing errors."""
        src = pl.DataFrame({"id": [1], "shared": ["A"], "only_source": [1]})
        tgt = pl.DataFrame({"id": [1], "shared": ["A"], "only_target": [2]})
        config = DiffConfig(primary_keys=["id"], schema_mode="intersection")

        summary = DiffEngine(config, src, tgt).run()

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
        summary = DiffEngine(config, src, tgt).run()

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
        summary = DiffEngine(config, complex_src, complex_tgt).run()

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
        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is True


@pytest.mark.unit
@pytest.mark.fast
class TestEvaluationStrictness:
    """Validate how the engine handles typing mismatches and null evaluations."""

    def test_it_fails_mixed_type_comparisons_when_strict_types_is_enforced(self) -> None:
        """Ensure 'strict_types' immediately flags mismatched data types as failures."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=True)
        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is False

    def test_it_soft_casts_mixed_types_when_strict_types_is_disabled(self) -> None:
        """Ensure the engine safely soft-casts targets to source types by default."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=False)
        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is True


@pytest.mark.unit
@pytest.mark.fast
class TestDataIntegrityAndSetDifferences:
    """Validate set-difference logic, uniqueness constraints, and artifact generation."""

    def test_it_aborts_execution_when_primary_keys_contain_duplicates(self) -> None:
        """Ensure validation catches duplicate keys to prevent Cartesian join explosions."""
        src = pl.DataFrame({"id": [1, 1, 2], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "C"]})
        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(DataIntegrityError, match="not unique in SOURCE dataset"):
            DiffEngine(config, src, tgt).run()

    def test_it_correctly_isolates_added_and_removed_records(self) -> None:
        """Ensure anti-joins accurately route missing records to the correct summary tallies."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["B", "C"]})
        config = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(config, src, tgt).run()

        assert summary.removed_count == 1
        assert summary.added_count == 1
        assert summary.changed_count == 0

    def test_it_gracefully_handles_completely_empty_dataframes(self) -> None:
        """Ensure edge-case comparisons of empty datasets return a clean pass."""
        src = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        tgt = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        config = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(config, src, tgt).run()

        assert summary.is_match is True

    def test_it_exports_discrepancy_artifacts_when_output_path_is_provided(
        self, tmp_path: Path
    ) -> None:
        """Ensure the engine writes 'Added', 'Removed', and 'Changed' datasets to disk."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})

        config = DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format="parquet")
        DiffEngine(config, src, tgt).run()

        assert (tmp_path / "added_rows.parquet").exists()
        assert (tmp_path / "removed_rows.parquet").exists()
        assert (tmp_path / "changed_rows.parquet").exists()

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
            DiffEngine(config, src, tgt).run()
