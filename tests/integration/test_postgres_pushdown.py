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
import psycopg
import pytest
from psycopg import sql

from tests.integration.duckdb_harness import PARITY_BACKEND
from tests.integration.postgres_harness import database_source, loaded_tables
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig, DiffConfig, DiffResult, DiffRule

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

    def test_it_tells_numerics_apart_by_declared_scale(self) -> None:
        """Ensure `strict_types` sees `numeric(10, 2)` and `numeric(12, 4)` as two types.

        Both settings read the declared precision and scale from the catalog, so
        every row of the column fails, as it would for two decimal files.
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

        assert pushdown.summary.changed_count == local.summary.changed_count == 2
        assert pushdown.summary.column_mismatches == local.summary.column_mismatches

    def test_it_writes_a_numeric_as_text_at_its_declared_scale(self) -> None:
        """Ensure both settings write `numeric(20, 0)` seven as `7`, so padding gives `007`."""
        source = pl.DataFrame(
            {"id": [1], "val": [7]}, schema={"id": pl.Int64(), "val": pl.UInt64()}
        )
        target = pl.DataFrame({"id": [1], "val": ["007"]})
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["val"], pad_zeros=3)]
        )

        pushdown, local = _both_settings(config, source, target)

        assert pushdown.summary.changed_count == local.summary.changed_count == 0

    def test_it_reads_a_numeric_with_more_than_18_integer_digits(self) -> None:
        """Ensure a wide value reads locally, where ConnectorX alone fails the whole read."""
        wide = Decimal("123456789012345678901234.5")
        source = pl.DataFrame(
            {"id": [1, 2], "val": [wide, Decimal("1.5")]},
            schema={"id": pl.Int64(), "val": pl.Decimal(30, 1)},
        )
        target = source.with_columns(pl.Series("val", [wide, Decimal("2.5")], pl.Decimal(30, 1)))

        pushdown, local = _both_settings(DiffConfig(primary_keys=["id"]), source, target)

        assert pushdown.summary.changed_count == local.summary.changed_count == 1
        assert local.changed["val_source"].to_list() == [Decimal("1.5")]

    def test_it_names_a_nan_a_local_read_cannot_hold(self) -> None:
        """Ensure a stored NaN fails a local read with its column named."""
        frame = pl.DataFrame(
            {"id": [1], "val": [Decimal("1.50")]},
            schema={"id": pl.Int64(), "val": pl.Decimal(10, 2)},
        )

        with loaded_tables(frame, frame) as (source_table, target_table):
            source = database_source(source_table)
            with psycopg.connect(source.uri, autocommit=True) as connection:
                update = sql.SQL("UPDATE {} SET val = 'NaN'").format(sql.Identifier(source_table))
                connection.execute(update)
            with pytest.raises(ConnectorError, match=r"Column 'val' of table .+ no decimal form"):
                DiffEngine.run_from_configs(
                    DiffConfig(primary_keys=["id"]), source, database_source(target_table)
                )


class TestPostgresPartitionedReads:
    """Validate a partitioned `table` read against a live Postgres."""

    def test_it_splits_the_read_that_keeps_declared_scale(self) -> None:
        """Ensure every range reads `numeric(10, 2)` as text and casts it back at that scale."""
        frame = pl.DataFrame(
            {"id": list(range(1, 11)), "val": [Decimal(f"{index}.25") for index in range(1, 11)]},
            schema={"id": pl.Int64(), "val": pl.Decimal(10, 2)},
        )

        with loaded_tables(frame, frame) as (table, _):
            whole = database_source(table)
            split = DatabaseConfig.model_validate(
                whole.model_dump() | {"partition_on": "id", "partitions": 3}
            )
            read_whole = LoaderFactory.load(whole).collect()
            read_split = LoaderFactory.load(split).collect()

        assert read_split.schema == pl.Schema({"id": pl.Int64(), "val": pl.Decimal(10, 2)})
        assert read_split.sort("id").equals(read_whole.sort("id"))
        assert read_split.sort("id").equals(frame)
