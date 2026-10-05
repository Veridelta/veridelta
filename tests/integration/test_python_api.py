# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the pure Python API of the Veridelta Engine."""

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffRule

pytestmark = [pytest.mark.integration]


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

        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True
        assert summary.total_mismatches == 0
        assert summary.added_count == 0
        assert summary.removed_count == 0
        assert summary.changed_count == 0

    def test_zero_row_dataframes_execute_computation_graph_safely(self) -> None:
        """Ensure empty datasets (e.g. from an empty upstream SQL query) do not crash the Polars DAG."""
        schema: dict[str, pl.DataType] = {
            "id": pl.Int64(),
            "metric": pl.Float64(),
        }
        src = pl.DataFrame({"id": [], "metric": []}, schema=schema)
        tgt = pl.DataFrame({"id": [], "metric": []}, schema=schema)

        config = DiffConfig(primary_keys=["id"])
        summary = DiffEngine(config, src.lazy(), tgt.lazy()).run().summary

        assert summary.is_match is True
        assert summary.total_rows_source == 0
        assert summary.total_rows_target == 0
