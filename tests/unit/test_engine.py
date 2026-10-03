# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the core DiffEngine, DataIngestor, and Loaders."""

from collections.abc import Callable, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import get_args
from unittest.mock import call

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
    _alignment_maps,
    _column_mismatches_from_frame,
    _compares_as_text,
    _compares_numerically,
    _fold_rule_defaults,
    _match_rule,
    _optional_module,
    _resolve_pushdown_keys,
    _resolve_pushdown_rules,
    _score_differing_pairs,
    _similarity_test,
)
from veridelta.exceptions import ConfigError, ConnectorError, DataIntegrityError
from veridelta.models import (
    ArtifactFormat,
    DiffConfig,
    DiffRule,
    SnowflakeConfig,
    SourceConfig,
    SourceType,
    ValueMapProposal,
)


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

    def test_it_drops_pattern_ignored_columns_from_both_sides_under_exact_schema(self) -> None:
        """Ensure a pattern ignore rule strips the target side too, so exact mode holds.

        The target lookup used to consult only `column_names` and `rename_to`,
        so a pattern rule removed the matched columns from the source alone and
        `schema_mode="exact"` then reported drift that was never there.
        """
        src = pl.DataFrame(
            {
                "id": [1, 2],
                "amount": [10.0, 20.0],
                "_etl_batch": [1, 1],
                "_etl_loaded_at": ["a", "a"],
            }
        )
        tgt = pl.DataFrame(
            {
                "id": [1, 2],
                "amount": [10.0, 20.0],
                "_etl_batch": [7, 7],
                "_etl_loaded_at": ["b", "b"],
            }
        )
        config = DiffConfig(
            primary_keys=["id"],
            schema_mode="exact",
            rules=[DiffRule(pattern=r"^_etl_", ignore=True)],
        )

        DiffEngine.validate_schemas(config, src.lazy(), tgt.lazy())
        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ("amount",)
        assert result.summary.is_match is True
        assert "_etl_batch" not in result.changed.columns

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
class TestHeaderNormalization:
    """Validate that `normalize_column_names` holds on every entry point."""

    def test_it_normalizes_headers_on_a_direct_run(self) -> None:
        """Ensure `DiffEngine(...).run()` honors the flag, not only the YAML path.

        The config's keys and rule names were lowercased while the frames kept
        their headers, so a direct run failed to find its own primary key.
        """
        src = pl.DataFrame({" ID ": [1, 2], "Legacy_Amt": [10.0, 20.0]})
        tgt = pl.DataFrame({"id": [1, 2], "AMOUNT": [10.0, 20.04]})
        config = DiffConfig(
            primary_keys=["ID"],
            normalize_column_names=True,
            rules=[
                DiffRule(column_names=["Legacy_Amt"], rename_to="Amount", absolute_tolerance=0.05)
            ],
        )

        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ("amount",)
        assert result.summary.is_perfect_match is True

    def test_it_normalizes_headers_before_validating_schemas(self) -> None:
        """Ensure a schema dry run sees the same headers a real run would."""
        src = pl.DataFrame(schema={"ID": pl.Int64, "Amount": pl.Float64})
        tgt = pl.DataFrame(schema={"id": pl.Int64, "amount": pl.Float64})
        config = DiffConfig(primary_keys=["id"], schema_mode="exact", normalize_column_names=True)

        DiffEngine.validate_schemas(config, src.lazy(), tgt.lazy())

    def test_it_rejects_headers_that_collide_once_normalized(self) -> None:
        """Ensure two headers that normalize to one name fail as a configuration error."""
        frame = pl.DataFrame({"id": [1], "Amount": [1.0], "amount ": [2.0]})
        config = DiffConfig(primary_keys=["id"], normalize_column_names=True)

        with pytest.raises(ConfigError, match="normalize_column_names"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()


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

    def test_it_matches_mixed_types_by_value_when_strict_types_is_disabled(self) -> None:
        """Ensure a float and an integer holding the same number match by default."""
        src = pl.DataFrame({"id": [1], "val": [10.0]})
        tgt = pl.DataFrame({"id": [1], "val": [10]})

        config = DiffConfig(primary_keys=["id"], strict_types=False)
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True

    @pytest.mark.parametrize(
        ("source", "target"),
        [
            pytest.param(pl.Series([10]), pl.Series([10.7]), id="int-vs-float"),
            pytest.param(pl.Series([10.7]), pl.Series([10]), id="float-vs-int"),
            pytest.param(pl.Series([10]), pl.Series([Decimal("10.50")]), id="int-vs-decimal"),
            pytest.param(pl.Series([Decimal("10.70")]), pl.Series([10.704]), id="decimal-vs-float"),
            pytest.param(
                pl.Series([0.5], dtype=pl.Float32),
                pl.Series([0.5000000001]),
                id="float32-vs-float64",
            ),
            pytest.param(
                pl.Series([Decimal("10.50")], dtype=pl.Decimal(10, 2)),
                pl.Series([Decimal("10.5049")], dtype=pl.Decimal(12, 4)),
                id="decimal-scales",
            ),
        ],
    )
    def test_it_compares_mixed_numeric_types_by_value(
        self, source: pl.Series, target: pl.Series
    ) -> None:
        """Ensure a difference survives when the two sides store numbers differently.

        Casting the target to the source's type used to truncate a Float64
        `10.7` to an Int64 `10`, so the pair matched locally while a warehouse,
        which promotes both sides, reported the difference.
        """
        src = pl.DataFrame({"id": [1], "val": source})
        tgt = pl.DataFrame({"id": [1], "val": target})

        summary = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == 1

    @pytest.mark.parametrize(
        ("source_dtype", "target_dtype"),
        [
            pytest.param(pl.Int32, pl.Int64, id="int-widths"),
            pytest.param(pl.UInt8, pl.Int8, id="unsigned-vs-signed"),
            pytest.param(pl.Decimal(10, 2), pl.Int64, id="decimal-vs-int"),
            pytest.param(pl.Float32, pl.Float64, id="float-widths"),
            pytest.param(pl.Int64, pl.Float64, id="int-vs-float"),
        ],
    )
    def test_it_matches_equal_values_stored_as_different_numeric_types(
        self, source_dtype: pl.DataType, target_dtype: pl.DataType
    ) -> None:
        """Ensure comparing by value still matches the same number across types."""
        src = pl.DataFrame({"id": [1], "val": pl.Series([5], dtype=source_dtype)})
        tgt = pl.DataFrame({"id": [1], "val": pl.Series([5], dtype=target_dtype)})

        summary = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run().summary

        assert summary.is_perfect_match is True

    @pytest.mark.parametrize(
        ("source", "target", "expected_changed"),
        [
            pytest.param(pl.Series([float("nan")]), pl.Series([1_000_000.0]), 1, id="nan-source"),
            pytest.param(pl.Series([float("inf")]), pl.Series([1e308]), 1, id="infinite-source"),
            pytest.param(pl.Series([float("nan")]), pl.Series([float("nan")]), 0, id="nan-pair"),
            pytest.param(
                pl.Series([Decimal("10.00")]),
                pl.Series([Decimal("10.40")]),
                0,
                id="decimal-within",
            ),
        ],
    )
    def test_it_never_forgives_a_non_finite_source_under_a_tolerance(
        self, source: pl.Series, target: pl.Series, expected_changed: int
    ) -> None:
        """Ensure NaN matches only NaN, and an infinity only itself, whatever the tolerance.

        The allowance is `abs + rel * |src|`, and `0 * inf` is NaN. Polars sorts
        NaN above every number, so `|diff| <= NaN` used to accept any target.
        """
        src = pl.DataFrame({"id": [1], "val": source})
        tgt = pl.DataFrame({"id": [1], "val": target})
        config = DiffConfig(primary_keys=["id"], default_absolute_tolerance=0.5)

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == expected_changed

    @pytest.mark.parametrize(
        ("source", "target", "expected_changed"),
        [
            pytest.param(
                pl.Series([7], dtype=pl.UInt64),
                pl.Series([5], dtype=pl.UInt64),
                0,
                id="target-below-source",
            ),
            pytest.param(
                pl.Series([5], dtype=pl.UInt64),
                pl.Series([7], dtype=pl.UInt64),
                0,
                id="target-above-source",
            ),
            pytest.param(
                pl.Series([10], dtype=pl.UInt64),
                pl.Series([5], dtype=pl.UInt64),
                1,
                id="outside-the-tolerance",
            ),
            pytest.param(
                pl.Series([255], dtype=pl.UInt8),
                pl.Series([0], dtype=pl.UInt8),
                1,
                id="full-range",
            ),
            pytest.param(
                pl.Series([7], dtype=pl.UInt64),
                pl.Series([5], dtype=pl.Int64),
                0,
                id="unsigned-vs-signed",
            ),
        ],
    )
    def test_it_measures_unsigned_differences_without_wrapping(
        self, source: pl.Series, target: pl.Series, expected_changed: int
    ) -> None:
        """Ensure a tolerance judges unsigned columns by their true distance.

        `tgt - src` on two unsigned values wraps around below zero, so `5 - 7`
        measured 18446744073709551614 and the verdict depended on which side
        held the larger number.
        """
        src = pl.DataFrame({"id": [1], "val": source})
        tgt = pl.DataFrame({"id": [1], "val": target})
        config = DiffConfig(primary_keys=["id"], default_absolute_tolerance=3.0)

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == expected_changed

    def test_it_still_soft_casts_text_to_a_numeric_source(self) -> None:
        """Ensure a text target keeps being cast to the source type rather than compared as text."""
        src = pl.DataFrame({"id": [1, 2], "val": [10, 10]})
        tgt = pl.DataFrame({"id": [1, 2], "val": ["10", "10.7"]})

        summary = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == 1

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

    def test_it_counts_rows_whose_first_key_is_null(self) -> None:
        """Ensure the totals count rows, not non-null key values."""
        frame = pl.DataFrame({"id": [1, None], "val": ["A", "B"]})

        summary = DiffEngine(DiffConfig(primary_keys=["id"]), frame.lazy(), frame.lazy()).run()

        assert summary.summary.total_rows_source == 2
        assert summary.summary.total_rows_target == 2

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
class TestFuzzyTextMatching:
    """Validate stage 8's text similarity limits on the local engine."""

    def test_it_forgives_typos_within_the_edit_distance(self) -> None:
        """Ensure a one-letter slip matches while a different name still does not."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1, 2, 3], "name": ["Jon", "Smith", "Jonathan"]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "name": ["John", "Smyth", "John"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], max_levenshtein_distance=1)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == 1
        assert summary.column_mismatches == {"name": 1}

    @pytest.mark.parametrize(("limit", "changed"), [(2, 1), (3, 0)])
    def test_it_counts_every_edit_toward_the_limit(self, limit: int, changed: int) -> None:
        """Ensure kitten and sitting, three edits apart, need a limit of three."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1], "word": ["kitten"]})
        tgt = pl.DataFrame({"id": [1], "word": ["sitting"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["word"], max_levenshtein_distance=limit)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == changed

    @pytest.mark.parametrize(("floor", "changed"), [(0.96, 0), (0.97, 1)])
    def test_it_forgives_names_above_the_jaro_winkler_floor(
        self, floor: float, changed: int
    ) -> None:
        """Ensure MARTHA and MARHTA, which score 0.961, match only under a lower floor."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1], "name": ["MARTHA"]})
        tgt = pl.DataFrame({"id": [1], "name": ["MARHTA"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=floor)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == changed

    @pytest.mark.parametrize(("case_insensitive", "changed"), [(False, 1), (True, 0)])
    def test_it_measures_case_unless_the_rule_folds_it_first(
        self, case_insensitive: bool, changed: int
    ) -> None:
        """Ensure `ABD` and `abc` are three edits apart until stage 3 lowercases both."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1], "code": ["ABD"]})
        tgt = pl.DataFrame({"id": [1], "code": ["abc"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["code"],
                    max_levenshtein_distance=1,
                    case_insensitive=case_insensitive,
                )
            ],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == changed

    @pytest.mark.parametrize(("treat_null", "changed"), [(True, 1), (False, 2)])
    def test_it_leaves_nulls_to_treat_null_as_equal(self, treat_null: bool, changed: int) -> None:
        """Ensure a missing value is never within any distance of a present one."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1, 2, 3], "name": ["Jon", None, None]})
        tgt = pl.DataFrame({"id": [1, 2, 3], "name": ["John", "x", None]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(
                    column_names=["name"],
                    max_levenshtein_distance=1,
                    treat_null_as_equal=treat_null,
                )
            ],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == changed

    @pytest.mark.parametrize(
        ("source", "target", "rule", "changed"),
        [
            pytest.param(
                pl.Series([12]), pl.Series([13]), DiffRule(column_names=["val"]), 1, id="integer"
            ),
            pytest.param(
                pl.Series(["12"]),
                pl.Series(["13"]),
                DiffRule(column_names=["val"], cast_to="Int64"),
                1,
                id="cast-to-integer",
            ),
            pytest.param(
                pl.Series(["2024-01-02"]),
                pl.Series(["2024-01-03"]),
                DiffRule(column_names=["val"], datetime_format="%Y-%m-%d"),
                1,
                id="parsed-date",
            ),
            pytest.param(
                pl.Series([7]),
                pl.Series([8]),
                DiffRule(column_names=["val"], pad_zeros=5),
                0,
                id="padded-to-text",
            ),
            pytest.param(
                pl.Series([12]),
                pl.Series([13]),
                DiffRule(column_names=["val"], cast_to="String"),
                0,
                id="cast-to-text",
            ),
        ],
    )
    def test_it_only_loosens_columns_that_compare_as_text(
        self, source: pl.Series, target: pl.Series, rule: DiffRule, changed: int
    ) -> None:
        """Ensure numbers and dates one apart still differ, while text one edit apart matches."""
        pytest.importorskip("rapidfuzz")
        config = DiffConfig(
            primary_keys=["id"],
            rules=[rule.model_copy(update={"max_levenshtein_distance": 1})],
        )
        src = pl.DataFrame({"id": [1], "val": source})
        tgt = pl.DataFrame({"id": [1], "val": target})

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == changed

    def test_it_never_loosens_a_primary_key(self) -> None:
        """Ensure keys one edit apart stay separate rows, since keys join on equality."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"code": ["abc"], "val": [1]})
        tgt = pl.DataFrame({"code": ["abd"], "val": [1]})
        config = DiffConfig(
            primary_keys=["code"],
            rules=[DiffRule(pattern=".*", max_levenshtein_distance=1)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert (summary.added_count, summary.removed_count) == (1, 1)

    def test_it_measures_a_numeric_target_as_text_against_a_text_source(self) -> None:
        """Ensure the soft cast to the source's type runs before the distance."""
        pytest.importorskip("rapidfuzz")
        src = pl.DataFrame({"id": [1], "val": ["12"]})
        tgt = pl.DataFrame({"id": [1], "val": [13]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["val"], max_levenshtein_distance=1)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == 0

    def test_it_scores_only_pairs_that_still_differ(self, mocker: MockerFixture) -> None:
        """Ensure equal and missing values never leave Polars to be scored."""
        measures = mocker.patch("veridelta.engine.rapidfuzz_distance")
        measures.Levenshtein.distance.return_value = 0
        src = pl.DataFrame({"id": [1, 2, 3, 4], "name": ["Jon", "same", None, None]})
        tgt = pl.DataFrame({"id": [1, 2, 3, 4], "name": ["John", "same", "x", None]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], max_levenshtein_distance=1)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert measures.Levenshtein.distance.call_args_list == [call("Jon", "John", score_cutoff=1)]
        assert summary.changed_count == 1

    def test_it_names_the_extra_when_rapidfuzz_is_missing(self, mocker: MockerFixture) -> None:
        """Ensure a missing scorer reads as an install hint rather than an ImportError."""
        mocker.patch("veridelta.engine.rapidfuzz_distance", None)
        frame = pl.DataFrame({"id": [1], "name": ["Jon"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)],
        )

        with pytest.raises(ConfigError, match=r"veridelta\[fuzzy\]"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_reports_the_missing_extra_before_checking_keys(self, mocker: MockerFixture) -> None:
        """Ensure a configuration problem surfaces before any rows are collected."""
        mocker.patch("veridelta.engine.rapidfuzz_distance", None)
        frame = pl.DataFrame({"id": [1, 1], "name": ["Jon", "Ann"]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], max_levenshtein_distance=1)],
        )

        with pytest.raises(ConfigError, match=r"veridelta\[fuzzy\]"):
            DiffEngine(config, frame.lazy(), frame.lazy()).run()

    def test_it_needs_no_extra_when_no_text_column_uses_a_limit(
        self, mocker: MockerFixture
    ) -> None:
        """Ensure a limit on a numeric column neither loosens it nor needs rapidfuzz."""
        mocker.patch("veridelta.engine.rapidfuzz_distance", None)
        src = pl.DataFrame({"id": [1], "val": [12]})
        tgt = pl.DataFrame({"id": [1], "val": [13]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["val"], max_levenshtein_distance=1)],
        )

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.changed_count == 1

    def test_it_hands_only_differing_present_pairs_to_the_scorer(self) -> None:
        """Ensure equal and missing pairs are skipped and hits keep their row positions.

        Polars runs this helper on its own threads, which coverage cannot see,
        so it is also called directly here.
        """
        pairs = pl.DataFrame(
            {
                "source": ["Jon", "same", None, "Ann", None],
                "target": ["John", "same", "x", "Bob", None],
            }
        ).to_struct("pairs")
        scored: list[tuple[str, str]] = []

        def _first_name_only(left: str, right: str) -> bool:
            scored.append((left, right))
            return left == "Jon"

        mask = _score_differing_pairs(pairs, test=_first_name_only)

        assert mask.to_list() == [True, False, False, False, False]
        assert scored == [("Jon", "John"), ("Ann", "Bob")]

    @pytest.mark.parametrize(
        ("rule", "matches"),
        [
            pytest.param(
                DiffRule(column_names=["name"], max_levenshtein_distance=1),
                [True, False],
                id="levenshtein",
            ),
            pytest.param(
                DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.96),
                [False, True],
                id="jaro-winkler",
            ),
        ],
    )
    def test_it_builds_the_test_for_the_limit_a_rule_sets(
        self, rule: DiffRule, matches: list[bool]
    ) -> None:
        """Ensure each limit is compared in its own direction: at most, or at least."""
        pytest.importorskip("rapidfuzz")
        config = DiffConfig(primary_keys=["id"], rules=[rule])

        test = _similarity_test(_fold_rule_defaults(rule, config))

        assert test is not None
        assert [test("Jon", "John"), test("MARTHA", "MARHTA")] == matches
        assert _similarity_test(_fold_rule_defaults(None, config)) is None

    def test_it_folds_similarity_limits_without_a_global_default(self) -> None:
        """Ensure only a rule sets a limit, so no column is loosened by default."""
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)],
        )

        unruled = _fold_rule_defaults(None, config)
        ruled = _fold_rule_defaults(config.rules[0], config)

        assert unruled["max_levenshtein_distance"] is None
        assert unruled["min_jaro_winkler_similarity"] is None
        assert ruled["max_levenshtein_distance"] is None
        assert ruled["min_jaro_winkler_similarity"] == 0.9


_NORMALIZER_CASES = [
    pytest.param([1.5], pl.Float64, DiffRule(column_names=["val"]), id="float"),
    pytest.param(
        [Decimal("1.50")], pl.Decimal(10, 2), DiffRule(column_names=["val"]), id="decimal"
    ),
    pytest.param(["x"], pl.String, DiffRule(column_names=["val"]), id="text"),
    pytest.param([True], pl.Boolean, DiffRule(column_names=["val"]), id="boolean"),
    pytest.param(
        ["1.5"], pl.String, DiffRule(column_names=["val"], cast_to="Float64"), id="cast-up"
    ),
    pytest.param(
        [1.5], pl.Float64, DiffRule(column_names=["val"], cast_to="String"), id="cast-down"
    ),
    pytest.param([7], pl.Int64, DiffRule(column_names=["val"], pad_zeros=3), id="padded"),
    pytest.param(
        [7],
        pl.Int64,
        DiffRule(column_names=["val"], pad_zeros=8, datetime_format="%Y%m%d"),
        id="padded-then-parsed",
    ),
    pytest.param(
        ["2024-01-02"],
        pl.String,
        DiffRule(column_names=["val"], datetime_format="%Y-%m-%d"),
        id="parsed-text",
    ),
    pytest.param(
        [20240102],
        pl.Int64,
        DiffRule(column_names=["val"], datetime_format="%Y%m%d"),
        id="format-skips-an-integer",
    ),
    pytest.param(
        ["2024"],
        pl.Categorical,
        DiffRule(column_names=["val"], datetime_format="%Y"),
        id="format-skips-a-categorical",
    ),
    pytest.param(
        [7], pl.Int64, DiffRule(column_names=["val"], cast_to="String"), id="cast-to-text"
    ),
]
"""Probed dtypes and rules paired with what the local normalizer turns them into.

Pushdown predicts the compared dtype from the probe alone, so every prediction
helper is pinned to the real normalizer over these cases."""


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

    def test_it_zeroes_tolerances_the_local_engine_would_skip(self) -> None:
        """Ensure only columns that compare numerically carry a tolerance to the warehouse.

        Folding `default_absolute_tolerance` into every column used to emit
        `ABS(tgt - src)` over text, booleans, and dates, which a warehouse
        either rejects or quietly coerces.
        """
        schema = pl.Schema(
            {
                "id": pl.Int64,
                "amount": pl.Float64,
                "name": pl.String,
                "flag": pl.Boolean,
                "day": pl.Date,
                "code": pl.Int64,
                "raw": pl.String,
                "stamp": pl.String,
            }
        )
        config = DiffConfig(
            primary_keys=["id"],
            default_absolute_tolerance=0.5,
            default_relative_tolerance=0.1,
            rules=[
                DiffRule(column_names=["name"], absolute_tolerance=2.0),
                DiffRule(column_names=["code"], pad_zeros=5),
                DiffRule(column_names=["raw"], cast_to="Float64"),
                DiffRule(column_names=["stamp"], datetime_format="%Y-%m-%d"),
            ],
        )

        tolerances = {
            rule.column_names[0]: (rule.absolute_tolerance, rule.relative_tolerance)
            for rule in _resolve_pushdown_rules(config, schema, schema)
        }

        assert tolerances == {
            "amount": (0.5, 0.1),
            "name": (0.0, 0.0),
            "flag": (0.0, 0.0),
            "day": (0.0, 0.0),
            "code": (0.0, 0.0),
            "raw": (0.5, 0.1),
            "stamp": (0.0, 0.0),
        }

    @pytest.mark.parametrize(("values", "dtype", "rule"), _NORMALIZER_CASES)
    def test_it_predicts_what_the_local_normalizer_compares(
        self, values: list[object], dtype: pl.DataType, rule: DiffRule
    ) -> None:
        """Ensure the pushdown prediction agrees with the dtype Polars actually compares.

        Pushdown never materializes the normalized column, so it predicts the
        dtype from the probe and the rule. This pins that prediction to the
        real normalizer so the two cannot drift apart.
        """
        config = DiffConfig(primary_keys=["id"], rules=[rule])
        frame = pl.DataFrame({"id": [1], "val": pl.Series(values, dtype=dtype)})
        effective = _fold_rule_defaults(_match_rule(config.rules, "val"), config)

        compared = _normalized(config, frame).schema["val"]

        assert _compares_numerically(effective, frame.schema["val"]) is compared.is_numeric()

    @pytest.mark.parametrize(("values", "dtype", "rule"), _NORMALIZER_CASES)
    def test_it_predicts_which_columns_compare_as_text(
        self, values: list[object], dtype: pl.DataType, rule: DiffRule
    ) -> None:
        """Ensure pushdown loosens exactly the columns a local run measures as text."""
        config = DiffConfig(primary_keys=["id"], rules=[rule])
        frame = pl.DataFrame({"id": [1], "val": pl.Series(values, dtype=dtype)})
        effective = _fold_rule_defaults(_match_rule(config.rules, "val"), config)

        compared = _normalized(config, frame).schema["val"]

        assert _compares_as_text(effective, frame.schema["val"]) is isinstance(
            compared, (pl.String, pl.Utf8)
        )

    def test_it_refuses_a_jaro_winkler_floor_on_a_text_column(self) -> None:
        """Ensure a limit no warehouse can reproduce fails before any query runs."""
        schema = pl.Schema({"id": pl.Int64, "name": pl.String})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)],
        )

        with pytest.raises(ConfigError, match="Column 'name' sets min_jaro_winkler_similarity"):
            _resolve_pushdown_rules(config, schema, schema)

    def test_it_forwards_an_edit_distance_only_to_columns_compared_as_text(self) -> None:
        """Ensure the warehouse loosens exactly the columns a local run measures as text."""
        schema = pl.Schema(
            {
                "id": pl.Int64,
                "name": pl.String,
                "code": pl.Int64,
                "padded": pl.Int64,
                "parsed": pl.String,
                "counted": pl.String,
            }
        )
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["name", "code"], max_levenshtein_distance=2),
                DiffRule(column_names=["padded"], max_levenshtein_distance=2, pad_zeros=5),
                DiffRule(
                    column_names=["parsed"],
                    max_levenshtein_distance=2,
                    datetime_format="%Y-%m-%d",
                ),
                DiffRule(column_names=["counted"], max_levenshtein_distance=2, cast_to="Int64"),
            ],
        )

        limits = {
            rule.column_names[0]: rule.max_levenshtein_distance
            for rule in _resolve_pushdown_rules(config, schema, schema)
        }

        assert limits == {"name": 2, "code": None, "padded": 2, "parsed": None, "counted": None}

    @pytest.mark.parametrize(
        ("field", "value"),
        [("min_jaro_winkler_similarity", 0.9), ("max_levenshtein_distance", 1)],
    )
    def test_it_drops_a_similarity_limit_from_a_column_compared_as_a_number(
        self, field: str, value: float
    ) -> None:
        """Ensure a limit on a column that never compares as text is not refused or sent."""
        schema = pl.Schema({"id": pl.Int64, "code": pl.Int64})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule.model_validate({"column_names": ["code"], field: value})],
        )

        (rule,) = _resolve_pushdown_rules(config, schema, schema)

        assert rule.max_levenshtein_distance is None
        assert rule.min_jaro_winkler_similarity is None

    def test_it_folds_global_defaults_into_key_rules(self) -> None:
        """Ensure a key is normalized by the same folded rule a local run applies.

        Keys only pass through stages 1-7, so the comparison fields, tolerances
        and null-safe equality, are left off the rule the compiler receives.
        """
        config = DiffConfig(
            primary_keys=["id"],
            default_whitespace_mode="both",
            default_null_values=["N/A"],
            default_absolute_tolerance=0.5,
            rules=[DiffRule(column_names=["id"], case_insensitive=True, absolute_tolerance=1.0)],
        )
        schema = pl.Schema({"id": pl.String, "val": pl.Int64})

        (key_rule,) = _resolve_pushdown_keys(config, schema, schema)

        assert key_rule.column_names == ["id"]
        assert key_rule.rename_to is None
        assert key_rule.whitespace_mode == "both"
        assert key_rule.null_values == ["N/A"]
        assert key_rule.case_insensitive is True
        assert key_rule.absolute_tolerance is None
        assert key_rule.treat_null_as_equal is None

    def test_it_reads_a_renamed_key_under_its_stored_name(self) -> None:
        """Ensure the key rule names the source's stored column and the key's new name."""
        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id", pad_zeros=3)],
        )

        (key_rule,) = _resolve_pushdown_keys(
            config,
            pl.Schema({"legacy_id": pl.Int64, "val": pl.Int64}),
            pl.Schema({"user_id": pl.String, "val": pl.Int64}),
        )

        assert key_rule.column_names == ["legacy_id"]
        assert key_rule.rename_to == "user_id"
        assert key_rule.pad_zeros == 3

    def test_it_reads_a_swapped_key_from_the_column_renamed_onto_it(self) -> None:
        """Ensure a swap reads the key from the other stored column, under that column's rule.

        After `a -> b` and `b -> a`, the key `a` is the column stored as `b`,
        and the rule listing `b` is the one that governs it locally.
        """
        config = DiffConfig(
            primary_keys=["a"],
            rules=[
                DiffRule(column_names=["a"], rename_to="b", case_insensitive=True),
                DiffRule(column_names=["b"], rename_to="a", whitespace_mode="both"),
            ],
        )
        schema = pl.Schema({"a": pl.String, "b": pl.String})

        (key_rule,) = _resolve_pushdown_keys(config, schema, schema)

        assert key_rule.column_names == ["b"]
        assert key_rule.rename_to == "a"
        assert key_rule.whitespace_mode == "both"
        assert key_rule.case_insensitive is None

    def test_it_reads_a_key_as_stored_when_its_rename_source_is_absent(self) -> None:
        """Ensure a stale rename does not send the join to a column the source lacks."""
        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )
        schema = pl.Schema({"user_id": pl.Int64, "val": pl.Int64})

        (key_rule,) = _resolve_pushdown_keys(config, schema, schema)

        assert key_rule.column_names == ["user_id"]
        assert key_rule.rename_to is None

    def test_it_enforces_rule_preconditions_on_key_columns(self) -> None:
        """Ensure a key rule its probed types cannot satisfy fails as it would locally."""
        sentinels = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["id"], null_values=[-1])]
        )
        text = pl.Schema({"id": pl.String})
        zoned = DiffConfig(
            primary_keys=["ts"], rules=[DiffRule(column_names=["ts"], timezone="UTC")]
        )
        naive = pl.Schema({"ts": pl.Datetime()})

        with pytest.raises(ConfigError, match="cannot hold"):
            _resolve_pushdown_keys(sentinels, text, text)
        with pytest.raises(ConfigError, match="timezone-naive"):
            _resolve_pushdown_keys(zoned, naive, naive)

    def test_it_drops_null_mismatch_counts_and_rejects_non_numeric_ones(self) -> None:
        """Ensure an empty join and a garbled tally are both handled."""
        assert _column_mismatches_from_frame(pl.DataFrame({"amount": [None]})) == {}

        with pytest.raises(ConnectorError, match="was not numeric"):
            _column_mismatches_from_frame(pl.DataFrame({"amount": ["five"]}))


@pytest.mark.unit
@pytest.mark.fast
class TestSharedRuleAndAlignmentHelpers:
    """Pin the helpers the local and pushdown paths now share."""

    def test_it_folds_global_defaults_identically_for_both_engines(self) -> None:
        """Ensure an unruled column resolves the same whether folded directly or via the engine."""
        config = DiffConfig(
            primary_keys=["id"],
            default_absolute_tolerance=0.5,
            default_whitespace_mode="both",
            default_null_values=["N/A"],
            rules=[DiffRule(column_names=["amount"], relative_tolerance=0.1, cast_to="Float64")],
        )
        engine = DiffEngine(config, pl.LazyFrame(), pl.LazyFrame())

        unruled = _fold_rule_defaults(None, config)
        ruled = _fold_rule_defaults(config.rules[0], config)

        assert unruled == engine._get_effective_rule("other")  # pyright: ignore[reportPrivateUsage]
        assert ruled == engine._get_effective_rule("amount")  # pyright: ignore[reportPrivateUsage]
        assert unruled["abs_tol"] == 0.5
        assert unruled["null_values_explicit"] is False
        # The rule overrides only what it sets; every other field inherits the default.
        assert ruled["abs_tol"] == 0.5
        assert ruled["rel_tol"] == 0.1
        assert ruled["cast_to"] == "Float64"
        assert ruled["whitespace"] == "both"

    def test_it_matches_pushdown_rules_to_the_local_defaults(self) -> None:
        """Ensure the synthesized warehouse rule carries the same folded values as the local path."""
        config = DiffConfig(
            primary_keys=["id"],
            default_absolute_tolerance=0.25,
            default_treat_null_as_equal=False,
            rules=[DiffRule(column_names=["amount"], null_values=[-1])],
        )
        schema = pl.Schema({"id": pl.Int64, "amount": pl.Float64})

        (rule,) = _resolve_pushdown_rules(config, schema, schema)
        local = _fold_rule_defaults(config.rules[0], config)

        assert rule.absolute_tolerance == local["abs_tol"] == 0.25
        assert rule.treat_null_as_equal is local["treat_null"] is False
        assert rule.null_values == local["null_values"] == [-1]
        assert rule.case_insensitive is None

    def test_it_only_renames_on_the_source_side(self) -> None:
        """Ensure rename_to is a source-only mapping while ignore applies to both sides."""
        rules = [
            DiffRule(column_names=["legacy_id"], rename_to="user_id"),
            DiffRule(pattern="^tmp_", ignore=True),
            DiffRule(column_names=["a", "b"], rename_to="ab"),
        ]
        columns = ["legacy_id", "tmp_1", "tmp_2", "a", "b", "val"]

        source_renames, source_drops = _alignment_maps(rules, columns, rename=True)
        target_renames, target_drops = _alignment_maps(rules, columns, rename=False)

        assert source_renames == {"legacy_id": "user_id"}
        assert source_drops == target_drops == {"tmp_1", "tmp_2"}
        # A multi-column rule cannot rename, and the target never renames.
        assert target_renames == {}

    def test_it_drops_only_columns_whose_governing_rule_ignores_them(self) -> None:
        """Ensure `ignore` follows the same precedence as every other field."""
        rules = [
            DiffRule(pattern="^tmp_", ignore=True),
            DiffRule(column_names=["tmp_keep"]),
            DiffRule(column_names=["val"]),
            DiffRule(column_names=["val"], ignore=True),
        ]
        columns = ["tmp_drop", "tmp_keep", "val"]

        source_renames, source_drops = _alignment_maps(rules, columns, rename=True)
        _target_renames, target_drops = _alignment_maps(rules, columns, rename=False)

        assert source_renames == {}
        assert source_drops == target_drops == {"tmp_drop"}

    def test_it_renames_with_the_first_rule_that_declares_a_rename(self) -> None:
        """Ensure both engines pair a column the same way when rules split its settings."""
        rules = [
            DiffRule(column_names=["s"], absolute_tolerance=0.05),
            DiffRule(column_names=["s"], rename_to="c"),
            DiffRule(column_names=["s"], rename_to="d"),
        ]

        renames, drops = _alignment_maps(rules, ["id", "s"], rename=True)
        (rule,) = _resolve_pushdown_rules(
            DiffConfig(primary_keys=["id"], rules=rules),
            pl.Schema({"id": pl.Int64, "s": pl.Float64}),
            pl.Schema({"id": pl.Int64, "c": pl.Float64}),
        )

        assert renames == {"s": "c"}
        assert drops == set()
        assert rule.column_names == ["s"]
        assert rule.rename_to == "c"
        assert rule.absolute_tolerance == 0.05

    def test_it_does_not_rename_a_column_its_governing_rule_ignores(self) -> None:
        """Ensure an ignored column is dropped, not dropped and then renamed.

        The two maps used to be built independently, so `drop` removed the
        column and `rename` then failed on it with a raw Polars error.
        """
        src = pl.DataFrame({"id": [1], "s": [1]})
        tgt = pl.DataFrame({"id": [1], "c": [2]})
        config = DiffConfig(
            primary_keys=["id"],
            rules=[
                DiffRule(column_names=["s"], ignore=True),
                DiffRule(column_names=["s"], rename_to="c"),
            ],
        )

        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ()
        assert result.summary.is_perfect_match is True

    def test_it_drops_both_spellings_when_an_ignored_rule_renames(self) -> None:
        """Ensure an ignored rename removes the target spelling as well as the source one."""
        src = pl.DataFrame({"id": [1], "s": [1]})
        tgt = pl.DataFrame({"id": [1], "c": [2]})
        config = DiffConfig(
            primary_keys=["id"],
            schema_mode="exact",
            rules=[DiffRule(column_names=["s"], rename_to="c", ignore=True)],
        )

        DiffEngine.validate_schemas(config, src.lazy(), tgt.lazy())
        result = DiffEngine(config, src.lazy(), tgt.lazy()).run()

        assert result.compared_columns == ()

    def test_it_resolves_swapped_columns_by_their_source_rule(self) -> None:
        """Ensure a swap does not hand each column the other's settings.

        After `a -> b` and `b -> a`, the column now called `b` came from `a`.
        A rule listing `b` governs the column that started as `b`, so the
        target spelling must not win here the way it does for a plain rename.
        """
        rules = [
            DiffRule(column_names=["a"], rename_to="b", absolute_tolerance=1.0),
            DiffRule(column_names=["b"], rename_to="a", absolute_tolerance=0.1),
        ]
        config = DiffConfig(primary_keys=["id"], rules=rules)
        engine = DiffEngine(config, pl.LazyFrame(), pl.LazyFrame())
        schema = pl.Schema({"id": pl.Int64, "a": pl.Float64, "b": pl.Float64})

        pushdown = {
            rule.column_names[0]: (rule.rename_to, rule.absolute_tolerance)
            for rule in _resolve_pushdown_rules(config, schema, schema)
        }

        assert engine._get_effective_rule("b")["abs_tol"] == 1.0  # pyright: ignore[reportPrivateUsage]
        assert engine._get_effective_rule("a")["abs_tol"] == 0.1  # pyright: ignore[reportPrivateUsage]
        assert pushdown == {"a": ("b", 1.0), "b": ("a", 0.1)}


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


def _propose(
    source: pl.DataFrame,
    target: pl.DataFrame,
    *,
    rules: list[DiffRule] | None = None,
    primary_keys: list[str] | None = None,
    strict_types: bool = False,
    min_confidence: float = 0.95,
    min_support: int = 5,
    sample_fraction: float = 1.0,
) -> list[ValueMapProposal]:
    """Propose value maps for two in-memory frames keyed on `id` by default."""
    config = DiffConfig(
        primary_keys=primary_keys or ["id"], rules=rules or [], strict_types=strict_types
    )
    return DiffEngine(config, source.lazy(), target.lazy()).propose_value_maps(
        min_confidence=min_confidence, min_support=min_support, sample_fraction=sample_fraction
    )


def _codes(
    source: Sequence[str | None], target: Sequence[str | None]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Pair two code columns row by row under ascending ids."""
    ids = list(range(len(source)))
    return (
        pl.DataFrame({"id": ids, "gender": pl.Series(source, dtype=pl.String)}),
        pl.DataFrame({"id": ids, "gender": pl.Series(target, dtype=pl.String)}),
    )


@pytest.mark.unit
@pytest.mark.fast
class TestValueMapProposals:
    """Validate value_map proposals drawn from how source and target values line up."""

    def test_it_proposes_a_mapping_the_rows_overwhelmingly_agree_on(self) -> None:
        """Ensure M lines up with Male in 999 of 1,000 rows, counting the one that already matches."""
        src, tgt = _codes(["M"] * 1000, ["Male"] * 999 + ["M"])

        (proposal,) = _propose(src, tgt)

        assert proposal.column == "gender"
        assert proposal.value_map == {"M": "Male"}
        assert proposal.governing_rule_index is None
        (entry,) = proposal.entries
        assert (entry.source_value, entry.target_value) == ("M", "Male")
        assert (entry.rows, entry.agreeing_rows) == (1000, 999)
        assert entry.confidence == 0.999

    def test_it_holds_back_a_mapping_below_the_confidence_floor(self) -> None:
        """Ensure 90 agreeing rows out of 100 is not enough at the default 95%."""
        src, tgt = _codes(["M"] * 100, ["Male"] * 90 + ["Man"] * 10)

        assert _propose(src, tgt) == []

    @pytest.mark.parametrize(("min_support", "proposed"), [(5, False), (4, True)])
    def test_it_needs_enough_agreeing_rows(self, min_support: int, proposed: bool) -> None:
        """Ensure four coincidences stay a coincidence unless the caller lowers the bar."""
        src, tgt = _codes(["M"] * 4, ["Male"] * 4)

        proposals = _propose(src, tgt, min_support=min_support)

        assert bool(proposals) is proposed

    def test_it_proposes_nothing_for_values_that_already_match(self) -> None:
        """Ensure an identity is never offered as a mapping."""
        src, tgt = _codes(["M"] * 10, ["M"] * 10)

        assert _propose(src, tgt) == []

    @pytest.mark.parametrize(("min_confidence", "proposed"), [(0.96, False), (0.95, True)])
    def test_it_counts_rows_with_a_null_target_against_the_mapping(
        self, min_confidence: float, proposed: bool
    ) -> None:
        """Ensure 95 Male targets and 5 missing ones make a 95% mapping, not a certain one."""
        src, tgt = _codes(["M"] * 100, ["Male"] * 95 + [None] * 5)

        proposals = _propose(src, tgt, min_confidence=min_confidence)

        assert bool(proposals) is proposed
        if proposals:
            assert proposals[0].entries[0].rows == 100

    def test_it_ignores_rows_whose_source_value_is_missing(self) -> None:
        """Ensure a NULL is never a key, since a value_map cannot map one."""
        src, tgt = _codes([None] * 10 + ["M"] * 5, ["Male"] * 15)

        (proposal,) = _propose(src, tgt)

        assert proposal.value_map == {"M": "Male"}

    def test_it_keeps_existing_entries_and_lists_only_the_new_ones(self) -> None:
        """Ensure a proposal extends the governing rule's map instead of replacing it."""
        src, tgt = _codes(["M"] * 5 + ["F"] * 5, ["Male"] * 5 + ["Female"] * 5)
        rules = [DiffRule(column_names=["gender"], value_map={"F": "Female"})]

        (proposal,) = _propose(src, tgt, rules=rules)

        assert proposal.value_map == {"F": "Female", "M": "Male"}
        assert [entry.source_value for entry in proposal.entries] == ["M"]
        assert proposal.governing_rule_index == 0

    def test_it_cannot_propose_for_values_an_existing_entry_produces(self) -> None:
        """Ensure rows already mapped by the rule are left out, whatever they now compare to."""
        src, tgt = _codes(["M"] * 5, ["Male"] * 5)
        rules = [DiffRule(column_names=["gender"], value_map={"M": "Man"})]

        assert _propose(src, tgt, rules=rules) == []

    def test_it_proposes_folded_values_when_the_rule_folds_case(self) -> None:
        """Ensure keys match the lowercased text the value_map stage actually sees."""
        src, tgt = _codes(["M"] * 5, ["Male"] * 5)
        rules = [DiffRule(column_names=["gender"], case_insensitive=True)]

        (proposal,) = _propose(src, tgt, rules=rules)

        assert proposal.value_map == {"m": "male"}

    def test_it_skips_columns_whose_mapped_text_is_transformed_again(self) -> None:
        """Ensure a map is proposed only where its output is compared exactly as written."""
        codes = ["M"] * 5
        src = pl.DataFrame(
            {
                "id": list(range(5)),
                "number": [1] * 5,
                "padded": codes,
                "parsed": codes,
                "counted": codes,
                "texted": codes,
            }
        )
        tgt = pl.DataFrame(
            {
                "id": list(range(5)),
                "number": [2] * 5,
                "padded": ["Male"] * 5,
                "parsed": ["Male"] * 5,
                "counted": ["Male"] * 5,
                "texted": ["Male"] * 5,
            }
        )
        rules = [
            DiffRule(column_names=["padded"], pad_zeros=3),
            DiffRule(column_names=["parsed"], datetime_format="%Y"),
            DiffRule(column_names=["counted"], cast_to="Int64"),
            DiffRule(column_names=["texted"], cast_to="String"),
        ]

        proposals = _propose(src, tgt, rules=rules)

        assert [proposal.column for proposal in proposals] == ["texted"]

    def test_it_skips_keys_ignored_columns_and_columns_the_target_lacks(self) -> None:
        """Ensure only compared columns are considered."""
        src = pl.DataFrame(
            {"code": ["A", "B", "C", "D", "E"], "ignored": ["M"] * 5, "legacy": ["M"] * 5}
        )
        tgt = pl.DataFrame({"code": ["A", "B", "C", "D", "E"], "ignored": ["Male"] * 5})
        rules = [DiffRule(column_names=["ignored"], ignore=True)]

        assert _propose(src, tgt, rules=rules, primary_keys=["code"]) == []

    @pytest.mark.parametrize(("strict_types", "expected"), [(False, {"Y": "1"}), (True, None)])
    def test_it_maps_text_onto_the_text_form_of_a_non_text_target(
        self, strict_types: bool, expected: dict[str, str] | None
    ) -> None:
        """Ensure the target is read as the text it is compared as, unless types must match."""
        src = pl.DataFrame({"id": list(range(5)), "flag": ["Y"] * 5})
        tgt = pl.DataFrame({"id": list(range(5)), "flag": [1] * 5})

        proposals = _propose(src, tgt, strict_types=strict_types)

        assert (proposals[0].value_map if proposals else None) == expected

    def test_it_joins_on_keys_normalized_by_their_own_value_map(self) -> None:
        """Ensure keys are mapped exactly as a run maps them, so rows still pair up."""
        src = pl.DataFrame({"code": [f"K{i}" for i in range(5)], "gender": ["M"] * 5})
        tgt = pl.DataFrame({"code": [f"k{i}" for i in range(5)], "gender": ["Male"] * 5})
        rules = [DiffRule(column_names=["code"], value_map={f"K{i}": f"k{i}" for i in range(5)})]

        (proposal,) = _propose(src, tgt, rules=rules, primary_keys=["code"])

        assert proposal.value_map == {"M": "Male"}
        assert proposal.entries[0].rows == 5

    def test_it_reports_a_renamed_column_under_its_target_name(self) -> None:
        """Ensure the proposal names the column as results do and points at its rule."""
        src = pl.DataFrame({"id": list(range(5)), "sex": ["M"] * 5})
        tgt = pl.DataFrame({"id": list(range(5)), "gender": ["Male"] * 5})
        rules = [DiffRule(column_names=["sex"], rename_to="gender")]

        (proposal,) = _propose(src, tgt, rules=rules)

        assert proposal.column == "gender"
        assert proposal.governing_rule_index == 0

    def test_it_refuses_duplicate_keys(self) -> None:
        """Ensure a key that repeats fails as it does in a run, rather than inflating counts."""
        src, tgt = _codes(["M"] * 5, ["Male"] * 5)
        src = src.with_columns(pl.lit(1).alias("id"))

        with pytest.raises(DataIntegrityError):
            _propose(src, tgt)

    @pytest.mark.parametrize(
        ("setting", "value", "message"),
        [
            pytest.param("min_confidence", 0.5, "min_confidence", id="confidence-at-half"),
            pytest.param("min_confidence", 1.5, "min_confidence", id="confidence-above-one"),
            pytest.param("min_confidence", float("nan"), "min_confidence", id="confidence-nan"),
            pytest.param("min_support", 0, "min_support", id="support-zero"),
            pytest.param("sample_fraction", 0.0, "sample_fraction", id="fraction-zero"),
            pytest.param("sample_fraction", 1.5, "sample_fraction", id="fraction-above-one"),
        ],
    )
    def test_it_rejects_thresholds_that_cannot_work(
        self, setting: str, value: float, message: str
    ) -> None:
        """Ensure an unusable threshold fails as configuration before any rows are read."""
        src, tgt = _codes(["M"] * 5, ["Male"] * 5)
        engine = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy())

        with pytest.raises(ConfigError, match=message):
            engine.propose_value_maps(**{setting: value})  # type: ignore[arg-type]

    def test_it_samples_the_same_rows_every_time(self) -> None:
        """Ensure a sample is drawn by key, so repeating a run repeats its counts."""
        src, tgt = _codes(["M"] * 1000, ["Male"] * 1000)

        (first,) = _propose(src, tgt, sample_fraction=0.5)
        (second,) = _propose(src, tgt, sample_fraction=0.5)
        (everything,) = _propose(src, tgt)

        assert first == second
        assert 0 < first.entries[0].rows < 1000
        assert everything.entries[0].rows == 1000

    def test_it_leaves_the_engine_ready_to_run(self) -> None:
        """Ensure proposing works on a copy, so the same engine still compares as before."""
        src, tgt = _codes(["M"] * 5 + ["F"], ["Male"] * 5 + ["F"])
        config = DiffConfig(primary_keys=["id"])
        engine = DiffEngine(config, src.lazy(), tgt.lazy())

        engine.propose_value_maps()

        assert engine.run().summary == DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

    def test_it_proposes_nothing_without_a_text_column(self) -> None:
        """Ensure numeric-only data returns an empty list rather than an error."""
        src = pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]})
        tgt = pl.DataFrame({"id": [1, 2], "amount": [1.5, 2.5]})

        assert _propose(src, tgt) == []

    def test_it_loads_file_sources_from_configs(self, tmp_path: Path) -> None:
        """Ensure the configuration entry point reads files the way a run does."""
        src, tgt = _codes(["M"] * 5, ["Male"] * 5)
        src.write_parquet(tmp_path / "source.parquet")
        tgt.write_parquet(tmp_path / "target.parquet")

        (proposal,) = DiffEngine.propose_value_maps_from_configs(
            DiffConfig(primary_keys=["id"]),
            SourceConfig(path=str(tmp_path / "source.parquet"), format="parquet"),
            SourceConfig(path=str(tmp_path / "target.parquet"), format="parquet"),
        )

        assert proposal.value_map == {"M": "Male"}

    def test_it_refuses_warehouse_sources_without_connecting(self, mocker: MockerFixture) -> None:
        """Ensure a warehouse table is refused with advice, before any session opens."""
        connector = mocker.patch("veridelta.engine.SnowflakeConnector")
        warehouse = SnowflakeConfig(
            table="ANALYTICS.PUBLIC.EVENTS",
            account="xy12345",
            user="analyst",
            warehouse="COMPUTE_WH",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        with pytest.raises(ConnectorError, match="file, lakehouse, or database sources"):
            DiffEngine.propose_value_maps_from_configs(
                DiffConfig(primary_keys=["id"]), warehouse, SourceConfig(path="target.csv")
            )

        connector.assert_not_called()
