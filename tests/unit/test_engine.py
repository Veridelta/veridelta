# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the core DiffEngine, DataIngestor, and Loaders."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import get_args

import polars as pl
import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from veridelta.engine import (
    _ARTIFACT_WRITERS,
    BaseLoader,
    DataIngestor,
    DiffEngine,
    LoaderFactory,
    _column_mismatches_from_frame,
    _optional_module,
    _resolve_pushdown_rules,
)
from veridelta.exceptions import ConfigError, ConnectorError, DataIntegrityError
from veridelta.models import ArtifactFormat, DiffConfig, DiffRule, SourceConfig, SourceType


@pytest.mark.unit
@pytest.mark.fast
class TestDataIngestorAndLoaders:
    """Validate data ingestion, loader factories, and pre-engine dataset preparation."""

    def test_it_raises_config_error_for_unsupported_source_types(self) -> None:
        """Ensure an unloadable format fails as configuration, naming what works."""
        with pytest.raises(ConfigError, match="arrow, csv, excel"):
            LoaderFactory.get_loader("netcdf")

    def test_it_implements_a_loader_for_every_declared_source_type(self) -> None:
        """Ensure the config surface and the loader registry cannot drift apart.

        `SourceType` used to advertise formats nobody had written, so a config
        naming one validated cleanly and then failed mid-run.
        """
        assert set(get_args(SourceType)) == set(LoaderFactory._loaders)

    def test_it_implements_a_writer_for_every_declared_artifact_format(self) -> None:
        """Ensure the same binding holds on the export side."""
        assert set(get_args(ArtifactFormat)) == set(_ARTIFACT_WRITERS)

    @pytest.mark.parametrize(
        ("fmt", "write"),
        [
            ("csv", lambda frame, path: frame.write_csv(path)),
            ("parquet", lambda frame, path: frame.write_parquet(path)),
            ("json", lambda frame, path: frame.write_json(path)),
            ("ndjson", lambda frame, path: frame.write_ndjson(path)),
            ("arrow", lambda frame, path: frame.write_ipc(path)),
        ],
    )
    def test_it_round_trips_every_file_format(
        self,
        tmp_path: Path,
        fmt: str,
        write: Callable[[pl.DataFrame, Path], None],
    ) -> None:
        """Ensure each loader reads back what its matching writer produced."""
        frame = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        path = tmp_path / f"data.{fmt}"
        write(frame, path)

        loaded = LoaderFactory.load(SourceConfig(path=str(path), format=fmt))  # type: ignore[arg-type]

        assert loaded.collect().equals(frame)

    def test_it_reads_an_excel_workbook(self, tmp_path: Path) -> None:
        """Ensure the Excel loader materializes a sheet through the optional extra."""
        pytest.importorskip("fastexcel")
        xlsxwriter = pytest.importorskip("xlsxwriter")
        _ = xlsxwriter

        frame = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        path = tmp_path / "data.xlsx"
        frame.write_excel(path)

        loaded = LoaderFactory.load(SourceConfig(path=str(path), format="excel"))

        assert loaded.collect().equals(frame)

    def test_it_explains_a_missing_excel_extra(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Ensure a missing optional dependency reads as an install hint."""
        mocker.patch("veridelta.engine.fastexcel", None)

        with pytest.raises(ConfigError, match=r"veridelta\[excel\]"):
            LoaderFactory.load(SourceConfig(path=str(tmp_path / "x.xlsx"), format="excel"))

    def test_it_rejects_an_excel_source_naming_several_sheets(
        self, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        """Ensure a multi-sheet read fails rather than reaching the engine as a dict.

        `pl.read_excel` returns a mapping when the options select more than one
        worksheet, which is not something the comparison pipeline can consume.
        """
        mocker.patch("veridelta.engine.pl.read_excel", return_value={"a": pl.DataFrame()})

        with pytest.raises(ConfigError, match="multiple worksheets"):
            LoaderFactory.load(SourceConfig(path=str(tmp_path / "x.xlsx"), format="excel"))

    def test_it_normalizes_headers_by_stripping_and_lowercasing_when_configured(self) -> None:
        """Ensure messy CSV headers are standardized before structural alignment."""
        df = pl.DataFrame({"  Messy_COL  ": [1], "CleanCol": [2]})
        config = DiffConfig(primary_keys=["id"], normalize_column_names=True)

        dummy_cfg = SourceConfig(path="dummy.csv", format="csv")
        ingestor = DataIngestor(config, source_config=dummy_cfg, target_config=dummy_cfg)
        normalized = ingestor._normalize_headers(df.lazy())  # pyright: ignore[reportPrivateUsage]

        assert normalized.collect_schema().names() == ["messy_col", "cleancol"]

    def test_it_renames_and_drops_columns_during_ingest_alignment(self) -> None:
        """Ensure DataIngestor applies ignore and rename_to before the engine sees the frame."""
        source = pl.DataFrame({"legacy_id": [1], "secret": ["x"], "val": ["A"]}).lazy()
        dummy = SourceConfig(path="dummy.csv", format="csv")
        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[
                DiffRule(column_names=["legacy_id"], rename_to="user_id"),
                DiffRule(column_names=["secret"], ignore=True),
                DiffRule(pattern="^sec"),
            ],
        )
        ingestor = DataIngestor(config, source_config=dummy, target_config=dummy)
        aligned = ingestor._align_columns(source, is_source=True)  # pyright: ignore[reportPrivateUsage]

        assert aligned.collect_schema().names() == ["user_id", "val"]

    def test_it_leaves_target_names_alone_when_aligning_the_target_side(self) -> None:
        """Ensure rename_to is a source-only mapping."""
        target = pl.DataFrame({"user_id": [1], "val": ["A"]}).lazy()
        dummy = SourceConfig(path="dummy.csv", format="csv")
        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )
        ingestor = DataIngestor(config, source_config=dummy, target_config=dummy)
        aligned = ingestor._align_columns(target, is_source=False)  # pyright: ignore[reportPrivateUsage]

        assert aligned.collect_schema().names() == ["user_id", "val"]

    def test_it_executes_the_abstract_loader_body(self) -> None:
        """Ensure the ABC placeholder is not an untested pass."""

        class _Probe(BaseLoader):
            def load(self, config: SourceConfig) -> pl.LazyFrame:
                return pl.DataFrame({"id": [1]}).lazy()

        probe = _Probe()
        assert BaseLoader.load(probe, SourceConfig(path="dummy.csv", format="csv")) is None

    def test_it_treats_a_missing_optional_module_as_absent(self) -> None:
        """Ensure the excel extra probe degrades to None instead of raising."""
        assert _optional_module("veridelta_no_such_optional_module") is None


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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True
        assert summary.total_mismatches == 0

    def test_it_drops_columns_flagged_with_ignore_from_both_datasets(self) -> None:
        """Ensure PII or irrelevant columns are excluded from the comparison."""
        src = pl.DataFrame({"id": [1], "secret_hash": ["abc"], "val": [10]})
        tgt = pl.DataFrame({"id": [1], "secret_hash": ["xyz"], "val": [10]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["secret_hash"], ignore=True)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary
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

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary
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

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, complex_src.lazy(), complex_tgt.lazy()).run().summary

        assert summary.is_match is True
        assert summary.changed_count == 0

    def test_it_rejects_a_cast_target_outside_the_closed_set(self) -> None:
        """Ensure an unknown cast name fails at load time, not by skipping the cast."""
        with pytest.raises(ValidationError, match="cast_to"):
            DiffRule(column_names=["n"], cast_to="Int128")  # type: ignore[arg-type]

    def test_it_maps_every_cast_target_to_a_polars_dtype(self) -> None:
        """Ensure `_CAST_TARGETS` cannot drift from the closed `CastTarget` set."""
        from veridelta.engine import _CAST_TARGETS
        from veridelta.models import CastTarget

        assert set(get_args(CastTarget)) == set(_CAST_TARGETS)

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_sanitizes_strings_using_regex_replace_dictionary_before_comparison(self) -> None:
        """Ensure string contents can be dynamically replaced before diffing occurs."""
        src = pl.DataFrame({"id": [1, 2], "cost": ["$100", "€50"]})
        tgt = pl.DataFrame({"id": [1, 2], "cost": ["100", "50"]})

        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["cost"], regex_replace={r"\$|€": ""})],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_evaluates_numeric_differences_using_relative_tolerance_percentage(self) -> None:
        """Ensure proportional differences within a calculated relative tolerance pass validation."""
        src = pl.DataFrame({"id": [1, 2], "metric": [100.0, 100.0]})
        # Row 1 is exactly a 5% difference (passes with 0.05), Row 2 is 6% difference (fails)
        tgt = pl.DataFrame({"id": [1, 2], "metric": [105.0, 106.0]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["metric"], relative_tolerance=0.05)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is False

    def test_it_soft_casts_mixed_types_when_strict_types_is_disabled(self) -> None:
        """Ensure the engine safely soft-casts targets to source types by default."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=False)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_evaluates_nulls_as_mismatches_when_treat_null_as_equal_is_disabled(self) -> None:
        """Ensure identical null records flag as failures when strict null matching is explicitly turned off."""
        src = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Utf8})
        tgt = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Utf8})

        config = DiffConfig(primary_keys=["id"], default_treat_null_as_equal=False)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.removed_count == 1
        assert summary.added_count == 1
        assert summary.changed_count == 0

    def test_it_gracefully_handles_completely_empty_dataframes(self) -> None:
        """Ensure edge-case comparisons of empty datasets return a clean pass."""
        src = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        tgt = pl.DataFrame({"id": [], "val": []}, schema={"id": pl.Int64, "val": pl.Utf8})
        config = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_exports_discrepancy_artifacts_when_output_path_is_provided(
        self, tmp_path: Path
    ) -> None:
        """Ensure the engine writes 'Added', 'Removed', and 'Changed' datasets to disk."""
        src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})

        config = DiffConfig(primary_keys=["id"], output_path=str(tmp_path), output_format="parquet")
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert not (tmp_path / "added_rows.parquet").exists()
        assert not (tmp_path / "removed_rows.parquet").exists()
        assert not (tmp_path / "changed_rows.parquet").exists()
        assert summary.artifacts_written is False

    def test_it_rejects_an_unwritable_export_format_at_load_time(self, tmp_path: Path) -> None:
        """Ensure an unwritable artifact format fails before any comparison runs.

        Excel is readable through the optional extra but not writable, so it is
        valid as a source format and invalid as an output one.
        """
        with pytest.raises(ValidationError, match="output_format"):
            DiffConfig(
                primary_keys=["id"],
                output_path=str(tmp_path),
                output_format="excel",  # type: ignore[arg-type]
            )

    def test_it_rejects_an_unsupported_export_format_even_without_drift(
        self, tmp_path: Path
    ) -> None:
        """Ensure the runtime guard holds when validation is bypassed, drift or not.

        The check sits above the write loop rather than inside it, so a clean
        comparison surfaces a bad format instead of passing silently.
        """
        frame = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
        config = DiffConfig.model_construct(
            primary_keys=["id"],
            rules=[],
            output_path=str(tmp_path),
            # Deliberately off the Literal: the point is the runtime guard.
            output_format="netcdf",  # type: ignore[arg-type]
        )

        with pytest.raises(ConfigError, match="arrow, csv, json"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()


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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_applies_each_sentinel_only_to_columns_of_a_matching_type(self) -> None:
        """Ensure one mixed list nulls the right value in every column type."""
        frame = pl.DataFrame(
            {
                "text": ["N/A", "keep"],
                "whole": [-999, 7],
                "ratio": [0.0, 1.5],
                "flag": [False, True],
            }
        )
        config = DiffConfig(primary_keys=["text"], default_null_values=["N/A", -999, 0, False])

        result = _normalized(config, frame)

        assert result["text"].to_list() == [None, "keep"]
        assert result["whole"].to_list() == [None, 7]
        # 0 is an int sentinel, but numeric sentinels span the whole family.
        assert result["ratio"].to_list() == [None, 1.5]
        assert result["flag"].to_list() == [None, True]

    def test_it_keeps_quoted_and_unquoted_sentinels_distinct(self) -> None:
        """Ensure '-999' targets text while -999 targets numbers."""
        frame = pl.DataFrame({"code": ["-999", "x"], "amount": [-999, 1]})
        config = DiffConfig(primary_keys=["code"], default_null_values=["-999"])

        result = _normalized(config, frame)

        assert result["code"].to_list() == [None, "x"]
        assert result["amount"].to_list() == [-999, 1]

    def test_it_nulls_sentinels_on_categorical_columns(self) -> None:
        """Ensure dictionary-encoded text, as Snowflake returns, is covered."""
        frame = pl.DataFrame(
            {"id": [1, 2], "status": ["N/A", "OPEN"]},
            schema={"id": pl.Int64, "status": pl.Categorical},
        )
        config = DiffConfig(primary_keys=["id"], default_null_values=["N/A"])

        result = _normalized(config, frame)

        assert result["status"].to_list() == [None, "OPEN"]

    def test_it_rejects_an_explicit_rule_no_sentinel_can_ever_match(self) -> None:
        """Ensure a named column with unusable sentinels fails loudly."""
        frame = pl.DataFrame({"id": [1], "amount": [10]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["amount"], null_values=["N/A"])]
        )

        with pytest.raises(ConfigError, match="cannot hold any of the null_values"):
            _normalized(config, frame)

    def test_it_pads_both_sides_so_numeric_and_text_codes_converge(self) -> None:
        """Ensure pad_zeros stringifies first, letting 123 match '00123'."""
        src = pl.DataFrame({"id": [1], "code": [123]})
        tgt = pl.DataFrame({"id": [1], "code": ["00123"]})

        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["code"], pad_zeros=5)]
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

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

    def test_it_strips_leading_or_trailing_whitespace_when_asked(self) -> None:
        """Ensure the left and right modes are not collapsed into both."""
        frame = pl.DataFrame({"id": [1], "name": ["  A  "]})
        left = _normalized(
            DiffConfig(
                primary_keys=["id"],
                rules=[DiffRule(column_names=["name"], whitespace_mode="left")],
            ),
            frame,
        )
        right = _normalized(
            DiffConfig(
                primary_keys=["id"],
                rules=[DiffRule(column_names=["name"], whitespace_mode="right")],
            ),
            frame,
        )

        assert left["name"][0] == "A  "
        assert right["name"][0] == "  A"


@pytest.mark.unit
@pytest.mark.fast
class TestPushdownRuleHelpers:
    """Validate the pushdown rule expander and tally reducer at the edges."""

    def test_it_carries_rename_to_into_the_synthesized_pushdown_rule(self) -> None:
        """Ensure a source-only name is compared against its target alias."""
        source = pl.Schema({"id": pl.Int64, "legacy_amt": pl.Float64})
        target = pl.Schema({"id": pl.Int64, "amount": pl.Float64})
        rules = _resolve_pushdown_rules(
            DiffConfig(
                primary_keys=["id"],
                rules=[DiffRule(column_names=["legacy_amt"], rename_to="amount")],
            ),
            source,
            target,
        )

        assert len(rules) == 1
        assert rules[0].column_names == ["legacy_amt"]
        assert rules[0].rename_to == "amount"

    def test_it_skips_source_columns_that_have_no_target_counterpart(self) -> None:
        """Ensure a source-only measure does not become a dangling predicate."""
        source = pl.Schema({"id": pl.Int64, "extra": pl.Int64})
        target = pl.Schema({"id": pl.Int64})
        rules = _resolve_pushdown_rules(DiffConfig(primary_keys=["id"]), source, target)

        assert rules == []

    def test_it_skips_timezone_enforcement_when_a_side_has_no_dtype(self) -> None:
        """Ensure a missing probe type is not treated as a naive timestamp."""

        class _PartialSchema:
            def names(self) -> list[str]:
                return ["id", "ts"]

            def get(self, name: str) -> pl.DataType | None:
                if name == "ts":
                    return None
                return pl.Int64()

        schema = _PartialSchema()
        rules = _resolve_pushdown_rules(
            DiffConfig(
                primary_keys=["id"],
                rules=[DiffRule(column_names=["ts"], timezone="UTC")],
            ),
            schema,  # type: ignore[arg-type]
            schema,  # type: ignore[arg-type]
        )

        assert len(rules) == 1
        assert rules[0].timezone == "UTC"

    def test_it_drops_null_mismatch_counts_and_rejects_non_numeric_ones(self) -> None:
        """Ensure an empty join and a garbled tally are both handled."""
        assert _column_mismatches_from_frame(pl.DataFrame({"amount": [None]})) == {}

        with pytest.raises(ConnectorError, match="was not numeric"):
            _column_mismatches_from_frame(pl.DataFrame({"amount": ["five"]}))


@pytest.mark.unit
@pytest.mark.fast
class TestRemainingEngineBranches:
    """Close the last branch gaps that a typical happy-path run never takes."""

    def test_it_treats_both_nulls_as_a_match_under_strict_type_mismatch(self) -> None:
        """Ensure treat_null still applies when strict_types short-circuits the values."""
        src = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Float64})
        tgt = pl.DataFrame({"id": [1], "val": [None]}, schema={"id": pl.Int64, "val": pl.Int64})
        config = DiffConfig(
            primary_keys=["id"],
            strict_types=True,
            default_treat_null_as_equal=True,
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_ignores_a_rename_whose_source_column_is_absent(self) -> None:
        """Ensure a stale rename does not abort alignment."""
        src = pl.DataFrame({"id": [1], "val": ["A"]})
        tgt = pl.DataFrame({"id": [1], "val": ["A"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    def test_it_skips_ignored_columns_that_survive_alignment(self, mocker: MockerFixture) -> None:
        """Ensure ignore is still honored if structural drop did not run."""
        src = pl.DataFrame({"id": [1], "noise": [1], "val": [10], "other": ["a"]})
        tgt = pl.DataFrame({"id": [1], "noise": [9], "val": [10], "other": ["a"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["noise"], ignore=True),
                DiffRule(column_names=["val"], cast_to="Int64"),
            ],
        )
        engine = DiffEngine(config, src.lazy(), tgt.lazy())
        mocker.patch.object(engine, "_align_structure")

        summary = engine.run().summary

        assert summary.is_match is True
        assert "noise" not in summary.column_mismatches

    def test_it_enters_the_temporal_pass_then_emits_nothing_for_ignored_casts(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure an ignored cast_to does not rewrite the remaining columns."""
        src = pl.DataFrame({"id": [1], "noise": ["1"], "val": [10]})
        tgt = pl.DataFrame({"id": [1], "noise": ["1"], "val": [10]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["noise"], ignore=True, cast_to="Int64")],
        )
        engine = DiffEngine(config, src.lazy(), tgt.lazy())
        mocker.patch.object(engine, "_align_structure")

        summary = engine.run().summary

        assert summary.is_match is True

    def test_it_flags_strict_type_mismatches_when_nulls_are_not_equal(self) -> None:
        """Ensure the strict-types miss path does not require treat_null."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})
        config = DiffConfig(
            primary_keys=["id"],
            strict_types=True,
            default_treat_null_as_equal=False,
        )
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is False
