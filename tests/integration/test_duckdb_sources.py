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

from veridelta.connectors import DuckDBConnector, DuckDBPushdownSession
from veridelta.connectors.duckdb import sandboxed
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConfigError, ConnectorError
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

    def test_it_reads_a_tables_columns_through_the_probe(self, tmp_path: Path) -> None:
        """Ensure `read_schema` names each stored column and its type, and reads no rows."""
        database = _orders(tmp_path / "legacy.duckdb")

        schema = DiffEngine.read_schema(DuckDBConfig(database=database, table="sales.orders"))

        assert schema == pl.Schema(
            {
                "id": pl.Int32(),
                "total": pl.Decimal(10, 2),
                "status": pl.String(),
                "placed": pl.Date(),
                "shipped": pl.Datetime("us"),
                "paid_at": pl.Datetime("us", "UTC"),
                "paid": pl.Boolean(),
                "ratio": pl.Float64(),
                "sizes": pl.Array(pl.Int32, 3),
            }
        )

    def test_it_will_not_describe_a_query(self, tmp_path: Path) -> None:
        """Ensure a query side is refused, since only running it would name its columns."""
        database = _orders(tmp_path / "legacy.duckdb")

        with pytest.raises(ConfigError, match="A schema probe reads a 'table'"):
            DiffEngine.read_schema(
                DuckDBConfig(database=database, query="SELECT * FROM sales.orders")
            )

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


def _pair(path: Path) -> str:
    """Create `legacy` and `modern` tables that differ by one added, removed, and changed row."""
    return _duckdb(
        path,
        "CREATE TABLE legacy AS SELECT * FROM (VALUES (1, 'gold', 10.0), (2, 'silver', 20.0), "
        "(3, 'bronze', 30.0)) AS t(id, tier, balance)",
        "CREATE TABLE modern AS SELECT * FROM (VALUES (1, 'gold', 10.0), (2, 'platinum', 20.0), "
        "(4, 'bronze', 5.0)) AS t(id, tier, balance)",
    )


def _sides(database: str, *, pushdown: bool) -> tuple[DuckDBConfig, DuckDBConfig]:
    """Name the two tables of `_pair`, compared in DuckDB or read into Polars."""
    return (
        DuckDBConfig(database=database, table="legacy", pushdown=pushdown),
        DuckDBConfig(database=database, table="modern", pushdown=pushdown),
    )


class TestDuckDBPushdown:
    """Validate two DuckDB tables compared inside DuckDB, against a local run of the same tables."""

    def test_it_reaches_the_local_verdict_inside_duckdb(self, tmp_path: Path) -> None:
        """Ensure pushdown runs in the database and counts what a local run counts."""
        database = _pair(tmp_path / "warehouse.duckdb")
        config = DiffConfig(primary_keys=["id"])

        pushdown = DiffEngine.run_from_configs(config, *_sides(database, pushdown=True))
        local = DiffEngine.run_from_configs(config, *_sides(database, pushdown=False))

        assert pushdown.keys_only is True
        assert local.keys_only is False
        assert pushdown.summary.model_dump() == local.summary.model_dump()
        assert pushdown.summary.column_mismatches == {"tier": 1}

    def test_it_fetches_a_row_sample_with_values(self, tmp_path: Path) -> None:
        """Ensure `pushdown_sample_rows` brings back the changed row's values."""
        database = _pair(tmp_path / "warehouse.duckdb")
        config = DiffConfig(primary_keys=["id"], pushdown_sample_rows=5)

        result = DiffEngine.run_from_configs(config, *_sides(database, pushdown=True))

        assert result.changed_sample is not None
        assert result.changed_sample.select("id", "tier_source", "tier_target").rows() == [
            (2, "silver", "platinum")
        ]

    def test_it_refuses_an_edit_distance_before_comparing(self, tmp_path: Path) -> None:
        """Ensure the shipped session never runs a distance that counts bytes."""
        database = _pair(tmp_path / "warehouse.duckdb")
        config = DiffConfig(
            primary_keys=["id"],
            rules=[DiffRule(column_names=["tier"], max_levenshtein_distance=1)],
        )

        with pytest.raises(ConfigError, match="counts UTF-8 bytes"):
            DiffEngine.run_from_configs(config, *_sides(database, pushdown=True))

    def test_it_proposes_value_maps_inside_duckdb(self, tmp_path: Path) -> None:
        """Ensure `crosswalk` on a pushdown pair counts its evidence in the database."""
        database = _duckdb(
            tmp_path / "warehouse.duckdb",
            "CREATE TABLE legacy AS SELECT range AS id, CASE WHEN range % 2 = 0 THEN 'M' "
            "ELSE 'F' END AS gender FROM range(20)",
            "CREATE TABLE modern AS SELECT range AS id, CASE WHEN range % 2 = 0 THEN 'Male' "
            "ELSE 'Female' END AS gender FROM range(20)",
        )

        [proposal] = DiffEngine.propose_value_maps_from_configs(
            DiffConfig(primary_keys=["id"]), *_sides(database, pushdown=True)
        )

        assert proposal.value_map == {"M": "Male", "F": "Female"}

    def test_it_checks_a_pushdown_pair_without_comparing(self, tmp_path: Path) -> None:
        """Ensure `validate --schemas` probes both tables and compiles the statements."""
        database = _pair(tmp_path / "warehouse.duckdb")
        source, target = _sides(database, pushdown=True)

        assert (
            DiffEngine.check_configs(DiffConfig(primary_keys=["id"]), source, target, schemas=True)
            == []
        )

    def test_it_describes_a_table_compared_in_place(self, tmp_path: Path) -> None:
        """Ensure `read_schema` reads a pushdown table with the probe a run starts with."""
        database = _pair(tmp_path / "warehouse.duckdb")
        source, _ = _sides(database, pushdown=True)

        assert DiffEngine.read_schema(source) == pl.Schema(
            {"id": pl.Int32(), "tier": pl.String(), "balance": pl.Decimal(3, 1)}
        )

    def test_it_reads_time_in_utc(self, tmp_path: Path) -> None:
        """Ensure each session sets UTC, whatever zone the machine runs in."""
        database = _pair(tmp_path / "warehouse.duckdb")
        session = DuckDBPushdownSession(_sides(database, pushdown=True)[0])
        session.connect()
        try:
            zone = session.execute_pushdown("SELECT current_setting('TimeZone') AS zone")
            assert zone.collect().item() == "UTC"
        finally:
            session.close()

    def test_it_reads_only_files_under_its_folders_when_sandboxed(
        self, tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """Ensure a query reads a file under the folders, and none beside them, even by prefix."""
        root = tmp_path / "root"
        root.mkdir()
        database = _duckdb(root / "warehouse.duckdb", "CREATE TABLE t AS SELECT 1 AS id")
        (root / "a.csv").write_text("id\n1\n")
        beside = tmp_path / "root-beside"
        beside.mkdir()
        (beside / "secret.txt").write_text("hunter2-do-not-print\n")

        def read(query: str) -> pl.DataFrame:
            connector = DuckDBConnector(DuckDBConfig(database=database, query=query))
            connector.connect()
            return connector.lazyframe().collect()

        with sandboxed([root]):
            inside = read(f"SELECT * FROM read_csv('{root / 'a.csv'}')")
            table = read("SELECT * FROM t")
            with pytest.raises(ConnectorError, match="Permission Error") as refused:
                read(f"SELECT content FROM read_text('{beside / 'secret.txt'}')")
            with pytest.raises(ConnectorError, match="lock"):
                read("SET enable_external_access = true")

        assert inside["id"].to_list() == [1]
        assert table["id"].to_list() == [1]
        assert "hunter2" not in str(refused.value)
        assert read(f"SELECT content FROM read_text('{beside / 'secret.txt'}')").height == 1

    def test_it_compares_a_view_that_casts_what_polars_cannot_read(self, tmp_path: Path) -> None:
        """Ensure an INTERVAL column fails the probe by name, and a casting view compares."""
        database = _duckdb(
            tmp_path / "warehouse.duckdb",
            "CREATE TABLE legacy AS SELECT range AS id, to_days(range::INTEGER) AS span "
            "FROM range(3)",
            "CREATE TABLE modern AS SELECT * FROM legacy",
            "CREATE VIEW legacy_text AS SELECT id, CAST(span AS VARCHAR) AS span FROM legacy",
            "CREATE VIEW modern_text AS SELECT id, CAST(span AS VARCHAR) AS span FROM modern",
        )
        config = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["span"], ignore=True)]
        )

        with pytest.raises(ConnectorError, match=r"Column 'span' .* INTERVAL.* view"):
            DiffEngine.run_from_configs(config, *_sides(database, pushdown=True))
        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            DuckDBConfig(database=database, table="legacy_text", pushdown=True),
            DuckDBConfig(database=database, table="modern_text", pushdown=True),
        )
        assert result.summary.is_match is True
        assert result.compared_columns == ("span",)
