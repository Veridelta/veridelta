# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that read real SQLite databases through ConnectorX.

The `database` extra is part of every CI environment, so these run without an
import guard: a broken lock or a missing wheel fails loudly instead of skipping.
"""

import contextlib
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import quote

import polars as pl
import pytest

from veridelta.connectors import DatabaseConnector
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig, DiffConfig, SourceConfig


def _sqlite(
    path: Path, *statements: str, rows: Sequence[tuple[str, tuple[object, ...]]] = ()
) -> str:
    """Create a SQLite database and return the URI that reads it.

    Connections are closed explicitly: `with sqlite3.connect()` only commits,
    and a connection left to the garbage collector warns on Python 3.13.

    Args:
        path (Path): File to create.
        *statements (str): DDL to run first.
        rows (Sequence[tuple[str, tuple[object, ...]]]): Parameterized inserts.

    Returns:
        str: `sqlite://` followed by the percent-encoded path, which is the form
            ConnectorX reads on every platform.
    """
    with contextlib.closing(sqlite3.connect(path)) as connection, connection:
        for statement in statements:
            connection.execute(statement)
        for insert, values in rows:
            connection.execute(insert, values)
    return "sqlite://" + quote(str(path))


def _orders(path: Path) -> str:
    """Create an `orders` table with one row per declared type the tests read."""
    return _sqlite(
        path,
        "CREATE TABLE orders (id INTEGER, total REAL, status TEXT, placed DATE, "
        "shipped DATETIME, paid BOOLEAN, weight NUMERIC)",
        rows=[
            (
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)",
                # ISO text, not `datetime.date`: the default date adapter is
                # deprecated on Python 3.12 and warnings fail the suite.
                (1, 10.5, "shipped", "2024-01-02", "2024-01-02 03:04:05", 1, 1.25),
            ),
            (
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)",
                (2, None, None, None, None, None, None),
            ),
        ],
    )


@pytest.mark.integration
class TestDatabaseSources:
    """Validate database sources end to end against real SQLite files."""

    def test_it_reads_declared_sqlite_types(self, tmp_path: Path) -> None:
        """Ensure each declared column type arrives as the Polars type it names."""
        uri = _orders(tmp_path / "legacy.db")

        frame = LoaderFactory.load(DatabaseConfig(uri=uri, table="orders")).collect()

        assert frame.schema == pl.Schema(
            {
                "id": pl.Int64(),
                "total": pl.Float64(),
                "status": pl.String(),
                "placed": pl.Date(),
                "shipped": pl.Datetime("us"),
                "paid": pl.Boolean(),
                "weight": pl.Float64(),
            }
        )
        assert frame.row(0) == (
            1,
            10.5,
            "shipped",
            pl.Series(["2024-01-02"]).str.to_date().item(),
            pl.Series(["2024-01-02 03:04:05"]).str.to_datetime().item(),
            True,
            1.25,
        )
        assert frame.row(1) == (2, None, None, None, None, None, None)

    def test_it_reads_the_same_rows_by_table_and_by_query(self, tmp_path: Path) -> None:
        """Ensure a table read and the equivalent statement agree."""
        uri = _orders(tmp_path / "legacy.db")

        by_table = LoaderFactory.load(DatabaseConfig(uri=uri, table="orders")).collect()
        by_query = LoaderFactory.load(
            DatabaseConfig(uri=uri, query="SELECT * FROM orders ORDER BY id")
        ).collect()

        assert by_table.equals(by_query)

    def test_it_keeps_the_stored_case_of_a_quoted_table(self, tmp_path: Path) -> None:
        """Ensure a mixed-case table name is quoted rather than folded."""
        uri = _sqlite(
            tmp_path / "legacy.db",
            'CREATE TABLE "LegacyOrders" (id INTEGER)',
            rows=[('INSERT INTO "LegacyOrders" VALUES (?)', (7,))],
        )

        frame = LoaderFactory.load(DatabaseConfig(uri=uri, table="LegacyOrders")).collect()

        assert frame["id"].to_list() == [7]

    def test_it_compares_a_database_with_a_parquet_file(self, tmp_path: Path) -> None:
        """Ensure a database source runs through the same local comparison as a file."""
        uri = _sqlite(
            tmp_path / "legacy.db",
            "CREATE TABLE customers (id INTEGER, tier TEXT, balance REAL)",
            rows=[
                ("INSERT INTO customers VALUES (?, ?, ?)", (1, "gold", 10.0)),
                ("INSERT INTO customers VALUES (?, ?, ?)", (2, "silver", 20.0)),
                ("INSERT INTO customers VALUES (?, ?, ?)", (3, "bronze", 30.0)),
            ],
        )
        modern = tmp_path / "modern.parquet"
        pl.DataFrame(
            {"id": [1, 2, 4], "tier": ["gold", "platinum", "bronze"], "balance": [10.0, 20.0, 5.0]}
        ).write_parquet(modern)

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            DatabaseConfig(uri=uri, table="customers"),
            SourceConfig(path=str(modern), format="parquet"),
        )

        assert result.summary.total_rows_source == 3
        assert result.summary.added_count == 1
        assert result.summary.removed_count == 1
        assert result.summary.changed_count == 1
        assert result.summary.column_mismatches == {"tier": 1}
        assert not result.keys_only

    def test_it_proposes_value_maps_from_a_database_source(self, tmp_path: Path) -> None:
        """Ensure crosswalk proposals read database rows like any local source."""
        codes = [(index, "M" if index % 2 else "F") for index in range(12)]
        uri = _sqlite(
            tmp_path / "legacy.db",
            "CREATE TABLE people (id INTEGER, gender TEXT)",
            rows=[("INSERT INTO people VALUES (?, ?)", row) for row in codes],
        )
        modern = tmp_path / "modern.parquet"
        pl.DataFrame(
            {
                "id": [index for index, _ in codes],
                "gender": ["Male" if code == "M" else "Female" for _, code in codes],
            }
        ).write_parquet(modern)

        proposals = DiffEngine.propose_value_maps_from_configs(
            DiffConfig(primary_keys=["id"]),
            DatabaseConfig(uri=uri, table="people"),
            SourceConfig(path=str(modern), format="parquet"),
        )

        assert [(proposal.column, proposal.value_map) for proposal in proposals] == [
            ("gender", {"F": "Female", "M": "Male"})
        ]

    def test_it_names_the_table_when_it_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure a missing table fails with the table named and the driver's reason."""
        uri = _orders(tmp_path / "legacy.db")

        with pytest.raises(ConnectorError, match="Database read of table 'returns'") as info:
            DatabaseConnector(DatabaseConfig(uri=uri, table="returns")).connect()

        assert "no such table" in str(info.value)

    def test_it_leaves_no_file_behind_for_a_missing_database(self, tmp_path: Path) -> None:
        """Ensure a mistyped path is refused before ConnectorX can create it."""
        missing = tmp_path / "typo.db"

        with pytest.raises(ConnectorError, match="does not exist"):
            LoaderFactory.load(DatabaseConfig(uri="sqlite://" + quote(str(missing)), table="t"))

        assert not missing.exists()
