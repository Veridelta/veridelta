# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the pure Python API of the Veridelta Engine."""

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.exceptions import DataIntegrityError
from veridelta.models import DiffConfig, DiffRule


@pytest.mark.integration
class TestEnginePythonAPI:
    """Validate the DiffEngine executes flawlessly in pure Python environments (e.g. Airflow/Jupyter)."""

    def test_complex_semantic_match_evaluates_successfully_in_memory(self) -> None:
        """Ensure programmatic rules (aliases, tolerances, null-mapping) bridge messy data architectures."""
        src = pl.DataFrame(
            {"legacy_id": [1, 2], "cost": ["$10.00", "$20.50"], "status": ["Active", "N/A"]}
        )
        tgt = pl.DataFrame(
            {
                "user_id": [1, 2],
                "cost": [10.02, 20.48],  # Drift within 0.05 absolute tolerance
                "status": ["Active", None],  # Null mapping
            }
        )

        config = DiffConfig(
            primary_keys=["user_id"],
            rules=[
                DiffRule(column_names=["legacy_id"], rename_to="user_id"),
                DiffRule(
                    column_names=["cost"],
                    regex_replace={"\\$": ""},
                    cast_to="Float64",
                    absolute_tolerance=0.05,
                ),
                DiffRule(column_names=["status"], null_values=["N/A"], treat_null_as_equal=True),
            ],
        )

        summary = DiffEngine(config).compare_lazyframes(src.lazy(), tgt.lazy())

        assert summary.is_match is True
        assert summary.total_mismatches == 0
        assert summary.added_count == 0
        assert summary.removed_count == 0
        assert summary.changed_count == 0

    def test_discrepancy_counter_accurately_tallies_asymmetric_and_mutated_records(self) -> None:
        """Ensure exact discrepancy counts are calculated without requiring file I/O exports."""
        src = pl.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})
        tgt = pl.DataFrame({"id": [1, 2, 4], "val": ["A", "CHANGED", "D"]})

        config = DiffConfig(primary_keys=["id"])
        summary = DiffEngine(config).compare_lazyframes(src.lazy(), tgt.lazy())

        assert summary.is_match is False
        assert summary.removed_count == 1  # ID 3
        assert summary.added_count == 1  # ID 4
        assert summary.changed_count == 1  # ID 2

    def test_zero_row_dataframes_execute_computation_graph_safely(self) -> None:
        """Ensure empty datasets (e.g. from an empty upstream SQL query) do not crash the Polars DAG."""
        schema: dict[str, pl.DataType] = {
            "id": pl.Int64(),
            "metric": pl.Float64(),
        }
        src = pl.DataFrame({"id": [], "metric": []}, schema=schema)
        tgt = pl.DataFrame({"id": [], "metric": []}, schema=schema)

        config = DiffConfig(primary_keys=["id"])
        summary = DiffEngine(config).compare_lazyframes(src.lazy(), tgt.lazy())

        assert summary.is_match is True
        assert summary.total_rows_source == 0
        assert summary.total_rows_target == 0

    def test_duplicate_primary_keys_abort_execution_before_cartesian_explosion(self) -> None:
        """Ensure hostile data with duplicate keys raises a fatal error before memory is exhausted."""
        src = pl.DataFrame(
            {
                "id": [1, 1, 2],  # Duplicate PK
                "val": ["A", "B", "C"],
            }
        )
        tgt = pl.DataFrame({"id": [1, 2], "val": ["A", "C"]})

        config = DiffConfig(primary_keys=["id"])

        with pytest.raises(DataIntegrityError, match="not unique in SOURCE dataset"):
            DiffEngine(config).compare_lazyframes(src.lazy(), tgt.lazy())
