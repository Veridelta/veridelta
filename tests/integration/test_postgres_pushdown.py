# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Live checks of Postgres pushdown that only a real server can answer.

The parity suite runs the compiled statements against Postgres through the
harness. These tests go through `DiffEngine.run_from_configs`, as `veridelta
run` does, so the routing that opens a pushdown session is exercised too. They
run under `make postgres`, which sets `VERIDELTA_PARITY_BACKEND=postgres`.
"""

import logging
from decimal import Decimal

import polars as pl
import pytest

from tests.integration.duckdb_harness import PARITY_BACKEND
from tests.integration.postgres_harness import database_source, loaded_tables
from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffResult, DiffRule

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        PARITY_BACKEND != "postgres", reason="Needs a live Postgres; run make postgres."
    ),
]


def _both_settings(
    config: DiffConfig, source: pl.DataFrame, target: pl.DataFrame
) -> tuple[DiffResult, DiffResult]:
    """Compare two loaded tables with `pushdown` on, then off.

    Returns:
        tuple[DiffResult, DiffResult]: The pushdown result, then the local one.
    """
    with loaded_tables(source, target) as (source_table, target_table):
        pushdown = DiffEngine.run_from_configs(
            config,
            database_source(source_table, pushdown=True),
            database_source(target_table, pushdown=True),
        )
        local = DiffEngine.run_from_configs(
            config, database_source(source_table), database_source(target_table)
        )
    return pushdown, local


class TestPostgresPushdownRouting:
    """Validate that two opted-in Postgres tables are compared inside Postgres."""

    def test_it_agrees_with_reading_the_same_tables(self, caplog: pytest.LogCaptureFixture) -> None:
        """Ensure `pushdown: true` runs in Postgres and reaches the local verdict."""
        source = pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
        target = pl.DataFrame({"id": [2, 3, 4], "amount": [20.0, 31.0, 40.0]})

        with caplog.at_level(logging.INFO, logger="veridelta.connectors.database"):
            pushdown, local = _both_settings(DiffConfig(primary_keys=["id"]), source, target)

        assert "Ran Postgres mismatch statement" in caplog.text
        assert (pushdown.summary.added_count, local.summary.added_count) == (1, 1)
        assert (pushdown.summary.removed_count, local.summary.removed_count) == (1, 1)
        assert (pushdown.summary.changed_count, local.summary.changed_count) == (1, 1)
        assert pushdown.summary.column_mismatches == local.summary.column_mismatches
        assert pushdown.summary.column_mismatches == {"amount": 1}

    def test_it_reads_every_numeric_as_one_type(self) -> None:
        """Pin that `strict_types` cannot tell Postgres numerics apart by scale.

        ConnectorX reports every `numeric` as `Decimal(38, 10)`, so both
        settings compare `numeric(10, 2)` with `numeric(12, 4)` by value.
        """
        source = pl.DataFrame(
            {"id": [1, 2], "val": [Decimal("1.50"), Decimal("2.00")]},
            schema={"id": pl.Int64(), "val": pl.Decimal(10, 2)},
        )
        target = pl.DataFrame(
            {"id": [1, 2], "val": [Decimal("1.5000"), Decimal("2.0001")]},
            schema={"id": pl.Int64(), "val": pl.Decimal(12, 4)},
        )

        pushdown, local = _both_settings(
            DiffConfig(primary_keys=["id"], strict_types=True), source, target
        )

        assert pushdown.summary.changed_count == local.summary.changed_count == 1
        assert pushdown.summary.column_mismatches == local.summary.column_mismatches

    def test_it_writes_a_numeric_as_text_at_the_scale_each_setting_sees(self) -> None:
        """Pin the one place the two settings disagree: a numeric written as text.

        Postgres writes `numeric(20, 0)` seven as `7`, so padding gives `007`. A
        local run reads the column as `Decimal(38, 10)` and writes `7.0000000000`,
        which padding leaves alone. If this starts failing, ConnectorX keeps the
        declared scale, and the documented difference can go.
        """
        source = pl.DataFrame(
            {"id": [1], "val": [7]}, schema={"id": pl.Int64(), "val": pl.UInt64()}
        )
        target = pl.DataFrame({"id": [1], "val": ["007"]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["val"], pad_zeros=3)]
        )

        pushdown, local = _both_settings(config, source, target)

        assert pushdown.summary.changed_count == 0
        assert local.summary.changed_count == 1
