# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that read real MySQL and SQL Server tables.

Each test loads rows into a live server through the server's own Python
driver, then reads them back through Veridelta and ConnectorX. A server is
used only when its URI is set, in `VERIDELTA_MYSQL_URI` or
`VERIDELTA_MSSQL_URI`, as `make databases` and the Database Servers CI job
set them. Every other run skips these tests. Each read passes the URI's
password through `password`, so a password holding `@`, `:`, or `/` checks
the percent-encoding against the real driver.
"""

import os
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from uuid import uuid4

import polars as pl
import pytest

from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import DatabaseConfig, DiffConfig, DiffRule, SourceConfig

pytestmark = [pytest.mark.integration]


class _CreateTable(Protocol):
    """Creates a table from column types and rows, and returns its name."""

    def __call__(
        self, columns: dict[str, str], rows: Sequence[Sequence[Any]], name: str | None = None
    ) -> str:
        """Create the table, with a name of its own unless one is given."""


@dataclass(frozen=True)
class _Server:
    """A database server under test, and how to load rows into it."""

    scheme: str
    variable: str
    quotes: tuple[str, str]
    types: dict[str, str]
    """The DDL type of each column in the types table."""
    dtypes: dict[str, pl.DataType]
    """The Polars type each of those columns reads as."""

    @property
    def uri(self) -> str | None:
        """Return the server's URI, or None when its variable is unset."""
        return os.environ.get(self.variable) or None

    def quoted(self, name: str) -> str:
        """Quote a table or column name for this server, doubling any closing quote in it."""
        opening, closing = self.quotes
        return f"{opening}{name.replace(closing, closing * 2)}{closing}"

    def connect(self) -> Any:
        """Open a connection through the server's own driver, committing each statement."""
        parts = urlsplit(self.uri or "")
        user = unquote(parts.username or "")
        password = unquote(parts.password or "")
        database = parts.path.lstrip("/")
        if self.scheme == "mysql":
            import pymysql

            return pymysql.connect(
                host=parts.hostname,
                port=parts.port or 3306,
                user=user,
                password=password,
                database=database,
                charset="utf8mb4",
                autocommit=True,
            )
        import pymssql

        return pymssql.connect(
            server=parts.hostname or "",
            port=str(parts.port or 1433),
            user=user,
            password=password,
            database=database,
            autocommit=True,
        )

    def source(self, **fields: Any) -> DatabaseConfig:
        """Build a source on this server, passing the URI's password through `password`."""
        parts = urlsplit(self.uri or "")
        host = parts.netloc.rpartition("@")[2]
        uri = urlunsplit(parts._replace(netloc=f"{parts.username}@{host}"))
        password = unquote(parts.password) if parts.password else None
        return DatabaseConfig(uri=uri, password=password, **fields)


_MYSQL = _Server(
    scheme="mysql",
    variable="VERIDELTA_MYSQL_URI",
    quotes=("`", "`"),
    types={
        "id": "INT NOT NULL PRIMARY KEY",
        "amount": "DECIMAL(10, 2)",
        "ratio": "DOUBLE",
        "flag": "TINYINT(1)",
        "bits": "BIT(1)",
        "name": "VARCHAR(50) CHARACTER SET utf8mb4",
        "opened": "DATE",
        "seen": "DATETIME(6)",
        "counter": "INT UNSIGNED",
    },
    dtypes={
        "id": pl.Int32(),
        "amount": pl.Decimal(38, 10),
        "ratio": pl.Float64(),
        "flag": pl.Int8(),
        "bits": pl.Binary(),
        "name": pl.String(),
        "opened": pl.Date(),
        "seen": pl.Datetime("us"),
        "counter": pl.UInt32(),
    },
)

_MSSQL = _Server(
    scheme="mssql",
    variable="VERIDELTA_MSSQL_URI",
    quotes=("[", "]"),
    types={
        "id": "INT NOT NULL PRIMARY KEY",
        "amount": "DECIMAL(10, 2)",
        "ratio": "FLOAT",
        "flag": "BIT",
        "tiny": "TINYINT",
        "name": "NVARCHAR(50)",
        "opened": "DATE",
        "seen": "DATETIME2(6)",
        "stamp": "DATETIMEOFFSET(3)",
    },
    dtypes={
        "id": pl.Int64(),
        "amount": pl.Decimal(38, 10),
        "ratio": pl.Float64(),
        "flag": pl.Boolean(),
        "tiny": pl.Int64(),
        "name": pl.String(),
        "opened": pl.Date(),
        "seen": pl.Datetime("us"),
        "stamp": pl.Datetime("us", "UTC"),
    },
)

_TEXT = "Zoë 🚀 日本"
"""Non-ASCII text, an emoji outside the Basic Multilingual Plane included."""


@pytest.fixture(params=[_MYSQL, _MSSQL], ids=lambda server: server.scheme)
def server(request: pytest.FixtureRequest) -> _Server:
    """Return each server whose URI is set, and skip the others."""
    chosen: _Server = request.param
    if chosen.uri is None:
        pytest.skip(f"Set {chosen.variable} to run against a live server; see `make databases`.")
    return chosen


@pytest.fixture
def create_table(server: _Server) -> Iterator[_CreateTable]:
    """Create tables on the server for one test, and drop them after it."""
    connection = server.connect()
    created: list[str] = []

    def create(
        columns: dict[str, str], rows: Sequence[Sequence[Any]], name: str | None = None
    ) -> str:
        name = name or f"vd_{uuid4().hex[:12]}"
        definition = ", ".join(
            f"{server.quoted(column)} {kind}" for column, kind in columns.items()
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE {server.quoted(name)} ({definition})")
        created.append(name)
        if rows:
            marks = ", ".join(["%s"] * len(columns))
            cursor.executemany(f"INSERT INTO {server.quoted(name)} VALUES ({marks})", list(rows))
        cursor.close()
        return name

    try:
        yield create
    finally:
        cursor = connection.cursor()
        for name in created:
            cursor.execute(f"DROP TABLE {server.quoted(name)}")
        cursor.close()
        connection.close()


def _types_rows(server: _Server) -> list[list[Any]]:
    """Return a row of values, a row of NULLs, and a second row of values.

    SQL Server gets its times as text: pymssql writes a `datetime` parameter
    with milliseconds only, and the server reads text at full precision. Its
    two stamps differ only in their offsets, `+02:00` and `+00:00`.
    """
    seen = datetime(2026, 1, 1, 12, 0, 0, 123456)
    if server.scheme == "mysql":
        return [
            [1, Decimal("10.50"), 0.25, 1, b"\x01", _TEXT, date(2026, 1, 1), seen, 4_000_000_000],
            [2, None, None, None, None, None, None, None, None],
            [3, Decimal("-1.05"), 2.0, 0, b"\x00", "plain", date(2026, 1, 2), seen, 0],
        ]
    seen_text = seen.isoformat(sep=" ")
    stamps = ("2026-01-01 12:00:00.123 +02:00", "2026-01-01 12:00:00.123 +00:00")
    return [
        [1, Decimal("10.50"), 0.25, True, 255, _TEXT, date(2026, 1, 1), seen_text, stamps[0]],
        [2, None, None, None, None, None, None, None, None],
        [3, Decimal("-1.05"), 2.0, False, 0, "plain", date(2026, 1, 2), seen_text, stamps[1]],
    ]


class TestDatabaseServers:
    """Validate table, query, and partitioned reads against live servers."""

    def test_it_reads_each_type_as_its_polars_type(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure each type arrives as the Polars type the docs give, NULLs included.

        A `DATETIMEOFFSET` arrives at the instant it holds, in UTC, whatever its
        offset: `12:00 +02:00` as 10:00, and `12:00 +00:00` as 12:00.
        """
        table = create_table(server.types, _types_rows(server))

        frame = LoaderFactory.load(server.source(table=table)).collect().sort("id")

        assert dict(frame.schema) == server.dtypes, f"Read as {dict(frame.schema)}"
        first, empty, third = frame.to_dicts()
        assert first["amount"] == Decimal("10.50")
        assert first["name"] == _TEXT
        assert first["seen"] == datetime(2026, 1, 1, 12, 0, 0, 123456)
        if "stamp" in first:
            assert first["stamp"] == datetime(2026, 1, 1, 10, 0, 0, 123000, tzinfo=UTC)
            assert third["stamp"] == datetime(2026, 1, 1, 12, 0, 0, 123000, tzinfo=UTC)
        assert [value for key, value in empty.items() if key != "id"] == [None] * (
            len(server.types) - 1
        )

    @pytest.mark.parametrize("partitions", [None, 2], ids=["whole", "partitioned"])
    def test_it_reads_the_instant_each_datetimeoffset_holds(
        self, server: _Server, create_table: _CreateTable, partitions: int | None
    ) -> None:
        """Ensure a `table` read returns the instant each `DATETIMEOFFSET` holds, whole or split.

        ConnectorX applies the offset a second time, so `12:00 +02:00` would
        arrive as 08:00 in UTC. A partitioned read sends the rewritten select
        through ConnectorX's own parser, so the column name holds a space.
        """
        if server.scheme != "mssql":
            pytest.skip("DATETIMEOFFSET is a SQL Server type.")
        table = create_table(
            {"id": "INT NOT NULL", "Placed At": "DATETIMEOFFSET(3)"},
            [
                [1, "2026-01-01 12:00:00.123 +02:00"],
                [2, "2026-01-01 12:00:00.123 -05:30"],
                [3, None],
            ],
        )
        split = {"partition_on": "id", "partitions": partitions} if partitions else {}

        frame = LoaderFactory.load(server.source(table=table, **split)).collect().sort("id")

        assert frame["Placed At"].to_list() == [
            datetime(2026, 1, 1, 10, 0, 0, 123000, tzinfo=UTC),
            datetime(2026, 1, 1, 17, 30, 0, 123000, tzinfo=UTC),
            None,
        ]

    def test_it_reads_a_datetimeoffset_named_with_a_bracket(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure a `]` in a column name is doubled inside its brackets, as SQL Server reads it.

        A partitioned read would pass the name through ConnectorX's parser,
        which writes it back undoubled, so that read is refused before any row.
        """
        if server.scheme != "mssql":
            pytest.skip("DATETIMEOFFSET is a SQL Server type.")
        table = create_table(
            {"id": "INT NOT NULL", "at] UTC": "DATETIMEOFFSET(3)"},
            [[1, "2026-01-01 12:00:00.123 +02:00"]],
        )

        frame = LoaderFactory.load(server.source(table=table)).collect()

        assert frame["at] UTC"].to_list() == [datetime(2026, 1, 1, 10, 0, 0, 123000, tzinfo=UTC)]
        with pytest.raises(ConnectorError, match=r"Column 'at\] UTC' .* has '\]' in its name"):
            LoaderFactory.load(server.source(table=table, partition_on="id", partitions=2))

    def test_it_sends_a_query_as_written_so_a_datetimeoffset_needs_switchoffset(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure a `query` keeps ConnectorX's doubled offset, and the docs' workaround fixes it.

        This pins what the docs say about a `query`. When ConnectorX reads the
        offset once, the first assertion fails, and the docs need updating.
        """
        if server.scheme != "mssql":
            pytest.skip("DATETIMEOFFSET is a SQL Server type.")
        table = create_table(
            {"id": "INT", "stamp": "DATETIMEOFFSET(3)"}, [[1, "2026-01-01 12:00:00.123 +02:00"]]
        )
        relation = server.quoted(table)

        raw = LoaderFactory.load(server.source(query=f"SELECT stamp FROM {relation}")).collect()
        switched = LoaderFactory.load(
            server.source(query=f"SELECT SWITCHOFFSET(stamp, '+00:00') AS stamp FROM {relation}")
        ).collect()

        assert raw["stamp"].to_list() == [datetime(2026, 1, 1, 8, 0, 0, 123000, tzinfo=UTC)]
        assert switched["stamp"].to_list() == [datetime(2026, 1, 1, 10, 0, 0, 123000, tzinfo=UTC)]

    def test_it_reads_a_mixed_case_table_by_its_quoted_name(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure a table name keeps its case, as the docs promise for quoted names."""
        name = f"Vd_Mixed_{uuid4().hex[:8]}"
        table = create_table({"OrderId": "INT", "Status": "VARCHAR(20)"}, [[1, "open"]], name)

        frame = LoaderFactory.load(server.source(table=table)).collect()

        assert frame.columns == ["OrderId", "Status"]
        assert frame.rows() == [(1, "open")]

    def test_it_reads_a_query_as_written(self, server: _Server, create_table: _CreateTable) -> None:
        """Ensure a `query` runs on the server, filter and column choice included."""
        table = create_table({"id": "INT", "status": "VARCHAR(20)"}, [[1, "a"], [2, "b"], [3, "c"]])
        query = f"SELECT id FROM {server.quoted(table)} WHERE id > 1"

        frame = LoaderFactory.load(server.source(query=query)).collect().sort("id")

        assert frame["id"].to_list() == [2, 3]

    def test_it_reads_a_table_in_partitions(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure a partitioned read returns the same rows as a read in one piece."""
        rows = [[key, f"row {key}"] for key in range(1, 41)]
        table = create_table({"id": "INT NOT NULL", "label": "VARCHAR(20)"}, rows)

        whole = LoaderFactory.load(server.source(table=table)).collect()
        split = LoaderFactory.load(server.source(table=table, partition_on="id", partitions=3))

        assert split.collect().sort("id").equals(whole.sort("id"))

    def test_it_compares_a_table_with_a_parquet_file(
        self, server: _Server, create_table: _CreateTable, tmp_path: Path
    ) -> None:
        """Ensure a server table runs through the local comparison against a file."""
        table = create_table(
            {"id": "INT NOT NULL PRIMARY KEY", "name": "VARCHAR(50)"},
            [[1, "ada"], [2, "grace"], [3, "linus"]],
        )
        modern = tmp_path / "modern.parquet"
        pl.DataFrame({"id": [1, 2, 4], "name": ["ada", "Grace", "ken"]}).write_parquet(modern)

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            server.source(table=table),
            SourceConfig(path=str(modern), format="parquet"),
        )

        summary = result.summary
        assert (summary.added_count, summary.removed_count, summary.changed_count) == (1, 1, 1)
        assert summary.column_mismatches == {"name": 1}

    def test_it_checks_rules_against_the_table_schema(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure `validate --schemas` reads the stored column types, and no rows."""
        table = create_table({"id": "INT NOT NULL PRIMARY KEY", "qty": "INT"}, [[1, 5]])
        side = server.source(table=table)
        broken = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["qty"], null_values=["N/A"])]
        )

        assert (
            DiffEngine.check_configs(DiffConfig(primary_keys=["id"]), side, side, schemas=True)
            == []
        )
        [finding] = DiffEngine.check_configs(broken, side, side, schemas=True)
        assert finding.severity == "error"
        assert f"Column 'qty' has type {server.dtypes['id']}, which cannot hold" in finding.message

    def test_it_keeps_the_password_out_of_a_failed_read(self, server: _Server) -> None:
        """Ensure a read the server refuses names the source, never the password."""
        source = server.source(table=f"vd_missing_{uuid4().hex[:8]}")

        with pytest.raises(ConnectorError, match=re.escape(source.redacted_uri)) as info:
            LoaderFactory.load(source)

        password = source.password or ""
        assert password, f"{server.variable} needs a password for this test."
        assert password not in str(info.value)
        assert quote(password, safe="") not in str(info.value)

    def test_it_refuses_a_decimal_beyond_the_read_precision(
        self, server: _Server, create_table: _CreateTable
    ) -> None:
        """Ensure a decimal too wide for `Decimal(38, 10)` fails the read with its source named.

        ConnectorX reads every decimal at scale 10, so a value with 20 digits
        before the point cannot be held. The docs state the limit.
        """
        table = create_table(
            {"id": "INT NOT NULL PRIMARY KEY", "amount": "DECIMAL(30, 4)"},
            [[1, Decimal("12345678901234567890.1234")]],
        )
        source = server.source(table=table)

        with pytest.raises(ConnectorError, match=re.escape(source.redacted_uri)):
            LoaderFactory.load(source)
