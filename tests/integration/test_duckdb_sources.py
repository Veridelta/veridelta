# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that read real DuckDB files.

DuckDB is part of every CI environment, so these run without an import guard.
MotherDuck needs an account and a network, so its connection is covered by
the unit tests alone.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import polars as pl
import pytest

from veridelta.connectors import DuckDBConnector
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import DiffConfig, DiffRule, DuckDBConfig, SourceConfig

pytestmark = [pytest.mark.integration]


def _duckdb(path: Path, *statements: str) -> str:
    """Create a DuckDB file, run each statement, close it, and return its path.

    The writer closes before any read opens the file, since one process cannot
    hold a file read-write and read-only at once.
    """
    with duckdb.connect(str(path)) as connection:
        for statement in statements:
            connection.execute(statement)
    return str(path)


def _orders(path: Path) -> str:
    """Create `sales.orders`, with one row per type the tests read and one row of NULLs."""
    return _duckdb(
        path,
        "CREATE SCHEMA sales",
        "CREATE TABLE sales.orders (id INTEGER, total DECIMAL(10, 2), status VARCHAR, "
        "placed DATE, shipped TIMESTAMP, paid_at TIMESTAMPTZ, paid BOOLEAN, "
        "ratio DOUBLE, sizes INTEGER[3])",
        "INSERT INTO sales.orders VALUES (1, 10.50, 'shipped', DATE '2024-01-02', "
        "TIMESTAMP '2024-01-02 03:04:05', TIMESTAMPTZ '2024-01-02 03:04:05+00', true, "
        "0.25, [1, 2, 3])",
        "INSERT INTO sales.orders VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    )


class TestDuckDBSources:
    """Validate DuckDB sources end to end against real files."""

    def test_it_reads_each_type_as_its_polars_type(self, tmp_path: Path) -> None:
        """Ensure declared types survive the read, a decimal's scale and an array included."""
        database = _orders(tmp_path / "legacy.duckdb")

        frame = LoaderFactory.load(DuckDBConfig(database=database, table="sales.orders")).collect()

        assert frame.schema == pl.Schema(
            {
                "id": pl.Int32(),
                "total": pl.Decimal(10, 2),
                "status": pl.String(),
                "placed": pl.Date(),
                "shipped": pl.Datetime("us"),
                "paid_at": pl.Datetime("us", "UTC"),
                "paid": pl.Boolean(),
                "ratio": pl.Float64(),
                "sizes": pl.Array(pl.Int32(), 3),
            }
        )
        assert frame.row(0) == (
            1,
            Decimal("10.50"),
            "shipped",
            date(2024, 1, 2),
            datetime(2024, 1, 2, 3, 4, 5),
            datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            True,
            0.25,
            [1, 2, 3],
        )
        assert frame.row(1) == (2, None, None, None, None, None, None, None, None)

    def test_it_reads_the_same_rows_by_table_by_query_and_by_full_name(
        self, tmp_path: Path
    ) -> None:
        """Ensure a table, its three-part name, and the equivalent statement agree."""
        database = _orders(tmp_path / "legacy.duckdb")

        by_table = LoaderFactory.load(DuckDBConfig(database=database, table="sales.orders"))
        by_name = LoaderFactory.load(DuckDBConfig(database=database, table="legacy.sales.orders"))
        by_query = LoaderFactory.load(
            DuckDBConfig(database=database, query="SELECT * FROM sales.orders ORDER BY id")
        )

        assert by_table.collect().equals(by_query.collect())
        assert by_name.collect().equals(by_query.collect())

    def test_it_compares_a_duckdb_table_with_a_parquet_file(self, tmp_path: Path) -> None:
        """Ensure a DuckDB source runs through the same local comparison as a file."""
        database = _duckdb(
            tmp_path / "legacy.duckdb",
            "CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'gold', 10.0), "
            "(2, 'silver', 20.0), (3, 'bronze', 30.0)) AS t(id, tier, balance)",
        )
        modern = tmp_path / "modern.parquet"
        pl.DataFrame(
            {"id": [1, 2, 4], "tier": ["gold", "platinum", "bronze"], "balance": [10.0, 20.0, 5.0]}
        ).write_parquet(modern)

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            DuckDBConfig(database=database, table="customers"),
            SourceConfig(path=str(modern), format="parquet"),
        )

        assert (result.summary.added_count, result.summary.removed_count) == (1, 1)
        assert result.summary.changed_count == 1
        assert result.summary.column_mismatches == {"tier": 1}
        assert not result.keys_only

    def test_it_proposes_value_maps_from_a_duckdb_source(self, tmp_path: Path) -> None:
        """Ensure `crosswalk` reads a DuckDB source as it reads a file."""
        database = _duckdb(
            tmp_path / "legacy.duckdb",
            "CREATE TABLE people AS SELECT range AS id, CASE WHEN range % 2 = 0 THEN 'M' "
            "ELSE 'F' END AS gender FROM range(20)",
        )
        modern = tmp_path / "modern.parquet"
        pl.DataFrame(
            {
                "id": list(range(20)),
                "gender": ["Male" if i % 2 == 0 else "Female" for i in range(20)],
            }
        ).write_parquet(modern)

        [proposal] = DiffEngine.propose_value_maps_from_configs(
            DiffConfig(primary_keys=["id"]),
            DuckDBConfig(database=database, table="people"),
            SourceConfig(path=str(modern), format="parquet"),
        )

        assert proposal.column == "gender"
        assert proposal.value_map == {"M": "Male", "F": "Female"}


class TestDuckDBReadFailures:
    """Validate that a DuckDB read fails with its reason, and changes nothing."""

    def test_it_leaves_no_file_behind_for_a_missing_database(self, tmp_path: Path) -> None:
        """Ensure a mistyped path fails instead of creating an empty database."""
        missing = tmp_path / "typo.duckdb"

        with pytest.raises(ConnectorError, match="DuckDB read of table 'orders'"):
            LoaderFactory.load(DuckDBConfig(database=str(missing), table="orders"))

        assert not missing.exists()

    def test_it_refuses_to_write_to_a_file(self, tmp_path: Path) -> None:
        """Ensure a `query` that writes fails, because a file opens read-only."""
        database = _orders(tmp_path / "legacy.duckdb")

        with pytest.raises(ConnectorError, match="read-only mode"):
            LoaderFactory.load(
                DuckDBConfig(database=database, query="CREATE TABLE copied AS SELECT 1 AS id")
            )

        with duckdb.connect(database, read_only=True) as connection:
            assert connection.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'copied'"
            ).fetchone() == (0,)

    def test_it_names_a_table_that_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure a missing table fails with the table named and DuckDB's reason."""
        database = _orders(tmp_path / "legacy.duckdb")

        with pytest.raises(ConnectorError, match="DuckDB read of table 'returns'") as info:
            DuckDBConnector(DuckDBConfig(database=database, table="returns")).connect()

        assert "does not exist" in str(info.value)

    def test_it_refuses_a_query_that_returns_nothing(self, tmp_path: Path) -> None:
        """Ensure a statement DuckDB answers with no result fails rather than comparing nothing."""
        database = _orders(tmp_path / "legacy.duckdb")

        with pytest.raises(ConnectorError, match="returned no rows to compare"):
            LoaderFactory.load(DuckDBConfig(database=database, query="SET threads = 1"))

    def test_it_explains_a_file_another_connection_holds(self, tmp_path: Path) -> None:
        """Ensure a file open read-write elsewhere fails with DuckDB's reason, not a crash."""
        database = _orders(tmp_path / "legacy.duckdb")

        with (
            duckdb.connect(database),
            pytest.raises(ConnectorError, match="different configuration"),
        ):
            LoaderFactory.load(DuckDBConfig(database=database, table="sales.orders"))

    @pytest.mark.parametrize(
        ("column", "kind"),
        [
            pytest.param("span INTERVAL", "INTERVAL", id="interval"),
            pytest.param("detail STRUCT(span INTERVAL)", "STRUCT(span INTERVAL)", id="nested"),
            pytest.param("spans INTERVAL[]", "INTERVAL[]", id="list"),
            pytest.param("choice UNION(n INTEGER, s VARCHAR)", "UNION", id="union"),
        ],
    )
    @pytest.mark.parametrize("rows", [True, False], ids=["rows", "empty"])
    def test_it_names_a_column_polars_cannot_hold(
        self, tmp_path: Path, column: str, kind: str, rows: bool
    ) -> None:
        """Ensure an unreadable type fails with its column named, where Polars would panic.

        Polars raises on some of these results and aborts the conversion on
        others, nested ones and empty ones among them, so the read checks the
        types before converting.
        """
        statements = [f"CREATE TABLE odd (id INTEGER, sizes INTEGER[3], {column})"]
        if rows:
            statements.append("INSERT INTO odd (id) VALUES (1)")
        database = _duckdb(tmp_path / "odd.duckdb", *statements)
        name = column.split()[0]

        with pytest.raises(ConnectorError, match=f"Column '{name}' of table 'odd' holds") as info:
            LoaderFactory.load(DuckDBConfig(database=database, table="odd"))

        assert kind in str(info.value)


class TestDuckDBSchemaChecks:
    """Validate `veridelta validate --schemas` against real DuckDB files."""

    def test_it_checks_rules_against_a_zero_row_probe(self, tmp_path: Path) -> None:
        """Ensure the probe reads a real table's columns and types, and no rows."""
        database = _orders(tmp_path / "legacy.duckdb")
        side = DuckDBConfig(database=database, table="sales.orders")
        sound = DiffConfig(primary_keys=["id"])
        broken = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["paid"], null_values=["N/A"])]
        )

        assert DiffEngine.check_configs(sound, side, side, schemas=True) == []
        [finding] = DiffEngine.check_configs(broken, side, side, schemas=True)
        with DuckDBConnector(side, probe=True) as probe:
            probe.connect()
            assert probe.lazyframe().collect().height == 0

        assert finding.severity == "error"
        assert "Column 'paid' has type Boolean, which cannot hold" in finding.message

    def test_it_reports_an_unreadable_column_instead_of_crashing(self, tmp_path: Path) -> None:
        """Ensure an empty probe of an INTERVAL column, where Polars would panic, is a finding."""
        database = _duckdb(tmp_path / "odd.duckdb", "CREATE TABLE odd (id INTEGER, span INTERVAL)")
        side = DuckDBConfig(database=database, table="odd")

        [finding] = DiffEngine.check_configs(
            DiffConfig(primary_keys=["id"]), side, side, schemas=True
        )

        assert finding.severity == "error"
        assert "Column 'span' of table 'odd' holds INTERVAL" in finding.message

    def test_it_skips_the_schema_check_for_a_query(self, tmp_path: Path) -> None:
        """Ensure a query is never run by `validate`, which says so instead."""
        database = _orders(tmp_path / "legacy.duckdb")
        source = DuckDBConfig(database=database, query="SELECT * FROM sales.orders")
        target = DuckDBConfig(database=database, table="sales.orders")

        [finding] = DiffEngine.check_configs(
            DiffConfig(primary_keys=["id"]), source, target, schemas=True
        )

        assert finding.severity == "warning"
        assert "The source reads a query, which validate does not run" in finding.message
