# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests validating the module boundaries and object handoffs."""

from pathlib import Path

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, SourceConfig


@pytest.mark.integration
class TestModuleBoundaryHandoffs:
    """Validate that Config, Ingestor, and Engine modules interact seamlessly."""

    def test_golden_pipeline_from_config_to_engine_execution(self, tmp_path: Path) -> None:
        """Ensure the data handoff works from disk to computation graph."""
        src_file = tmp_path / "source.csv"
        tgt_file = tmp_path / "target.csv"
        pl.DataFrame({"id": [1, 2], "val": ["A", "B"]}).write_csv(src_file)
        pl.DataFrame({"id": [1, 2], "val": ["A", "B_CHANGED"]}).write_csv(tgt_file)

        src_cfg = SourceConfig(path=str(src_file), format="csv")
        tgt_cfg = SourceConfig(path=str(tgt_file), format="csv")
        diff_cfg = DiffConfig(primary_keys=["id"])

        summary = DiffEngine(diff_cfg).execute(src_cfg, tgt_cfg)

        assert summary.is_match is False
        assert summary.changed_count == 1
        assert summary.total_rows_source == 2

    def test_strict_types_violation_correctly_flags_mismatches_on_schema_drift(
        self, tmp_path: Path
    ) -> None:
        """Ensure the engine respects strict_types when reading strongly-typed Parquet files."""
        src_file = tmp_path / "source.parquet"
        tgt_file = tmp_path / "target.parquet"

        # Setup schema drift: Int64 vs String
        pl.DataFrame({"id": [1], "metric": [100]}).write_parquet(src_file)
        pl.DataFrame({"id": [1], "metric": ["100"]}).write_parquet(tgt_file)

        src_cfg = SourceConfig(path=str(src_file), format="parquet")
        tgt_cfg = SourceConfig(path=str(tgt_file), format="parquet")
        diff_cfg = DiffConfig(primary_keys=["id"], strict_types=True)

        summary = DiffEngine(diff_cfg).execute(src_cfg, tgt_cfg)

        # Because strict_types=True, 100 != "100"
        assert summary.is_match is False
        assert summary.changed_count == 1
        assert summary.column_mismatches["metric"] == 1

    def test_schema_mode_exact_aborts_pipeline_when_target_has_unauthorized_columns(
        self, tmp_path: Path
    ) -> None:
        """Ensure configuration constraints can successfully abort the engine execution."""
        src_file = tmp_path / "source.csv"
        tgt_file = tmp_path / "target.csv"

        pl.DataFrame({"id": [1], "val": ["A"]}).write_csv(src_file)
        pl.DataFrame({"id": [1], "val": ["A"], "rogue_column": [99]}).write_csv(tgt_file)

        src_cfg = SourceConfig(path=str(src_file), format="csv")
        tgt_cfg = SourceConfig(path=str(tgt_file), format="csv")
        diff_cfg = DiffConfig(primary_keys=["id"], schema_mode="exact")

        with pytest.raises(ConfigError, match="EXACT schema match failed"):
            DiffEngine(diff_cfg).execute(src_cfg, tgt_cfg)
