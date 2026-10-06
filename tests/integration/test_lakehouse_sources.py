# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that read real Delta Lake and Iceberg tables.

Each test writes its tables under `tmp_path`: Delta through `deltalake`, and
Iceberg through a pyiceberg catalog kept in a SQLite file. So the scans read
the same transaction logs and metadata files a lakehouse holds. Object stores
need an account, so `storage_options` are covered by the unit tests alone.
"""

import re
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from veridelta.connectors import DeltaLakeConnector, IcebergConnector
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConnectorError
from veridelta.models import DeltaLakeConfig, DiffConfig, DiffRule, IcebergConfig, SourceConfig

pytestmark = [pytest.mark.integration]

_EVENTS = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "tier": ["gold", "silver", None],
        "balance": [Decimal("10.50"), Decimal("20.25"), None],
        "opened": [date(2026, 1, 1), date(2026, 1, 2), None],
        "seen": [datetime(2026, 1, 1, 12), None, datetime(2026, 1, 3, 8, 30)],
        "active": [True, False, None],
        "ratio": [0.25, None, 2.0],
    },
    schema_overrides={"balance": pl.Decimal(10, 2)},
)
"""One column per type the tests read, each with a NULL except the key."""

_CHANGED = _EVENTS.with_columns(
    pl.when(pl.col("id") == 2).then(pl.lit("platinum")).otherwise(pl.col("tier")).alias("tier")
)
"""The same rows with one tier changed, written as the table's second version."""

_LakehouseTable = Callable[[bool], DeltaLakeConfig | IcebergConfig]
"""Builds a source for a table's latest version, or for its first when given True."""


def _delta_table(path: Path) -> _LakehouseTable:
    """Write `_EVENTS` as version 0 and `_CHANGED` as version 1 of a Delta table."""
    _EVENTS.write_delta(path)
    _CHANGED.write_delta(path, mode="overwrite")

    def source(first: bool) -> DeltaLakeConfig:
        return DeltaLakeConfig(table_uri=str(path), version=0 if first else None)

    return source


def _catalog(path: Path) -> SqlCatalog:
    """Open an Iceberg catalog with one namespace, kept in a SQLite file under `path`."""
    catalog = SqlCatalog(
        "veridelta", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=path.as_uri()
    )
    catalog.create_namespace("lake")
    return catalog


def _iceberg_table(path: Path) -> _LakehouseTable:
    """Append `_EVENTS`, then overwrite it with `_CHANGED`, in an Iceberg table."""
    catalog = _catalog(path)
    table = catalog.create_table("lake.events", schema=_EVENTS.to_arrow().schema)
    table.append(_EVENTS.to_arrow())
    snapshot = table.current_snapshot()
    assert snapshot is not None
    first_id = snapshot.snapshot_id
    table.overwrite(_CHANGED.to_arrow())
    metadata = catalog.load_table("lake.events").metadata_location

    def source(first: bool) -> IcebergConfig:
        return IcebergConfig(table_uri=metadata, snapshot_id=first_id if first else None)

    return source


@pytest.fixture(params=["delta", "iceberg"])
def events(request: pytest.FixtureRequest, tmp_path: Path) -> _LakehouseTable:
    """Write the events table in each lakehouse format, with two versions."""
    if request.param == "delta":
        return _delta_table(tmp_path / "events")
    return _iceberg_table(tmp_path)


class TestLakehouseSources:
    """Validate Delta Lake and Iceberg sources end to end against real tables."""

    def test_it_reads_each_type_as_its_polars_type(self, events: _LakehouseTable) -> None:
        """Ensure each type survives the table's own encoding, a decimal's scale included."""
        frame = LoaderFactory.load(events(False))

        assert frame.collect_schema() == _EVENTS.schema
        assert frame.collect().sort("id").equals(_CHANGED)

    def test_it_reads_the_first_version(self, events: _LakehouseTable) -> None:
        """Ensure `version` and `snapshot_id` read the table as it was, not as it is."""
        assert LoaderFactory.load(events(True)).collect().sort("id").equals(_EVENTS)

    def test_it_compares_an_earlier_version_with_a_parquet_file(
        self, events: _LakehouseTable, tmp_path: Path
    ) -> None:
        """Ensure a lakehouse source runs through the same local comparison as a file."""
        modern = tmp_path / "modern.parquet"
        _CHANGED.write_parquet(modern)

        result = DiffEngine.run_from_configs(
            DiffConfig(primary_keys=["id"]),
            events(True),
            SourceConfig(path=str(modern), format="parquet"),
        )

        assert (result.summary.added_count, result.summary.removed_count) == (0, 0)
        assert result.summary.changed_count == 1
        assert result.summary.column_mismatches == {"tier": 1}

    def test_it_checks_rules_against_the_table_schema(self, events: _LakehouseTable) -> None:
        """Ensure `validate --schemas` reads a real table's columns and types."""
        side = events(False)
        sound = DiffConfig(primary_keys=["id"])
        broken = DiffConfig(
            primary_keys=["id"], rules=[DiffRule(column_names=["active"], null_values=["N/A"])]
        )

        assert DiffEngine.check_configs(sound, side, side, schemas=True) == []
        [finding] = DiffEngine.check_configs(broken, side, side, schemas=True)
        assert finding.severity == "error"
        assert "Column 'active' has type Boolean, which cannot hold" in finding.message


class TestEmptyLakehouseTables:
    """Validate that a table with no rows still has its columns."""

    def test_it_keeps_the_schema_of_an_empty_delta_table(self, tmp_path: Path) -> None:
        """Ensure an empty Delta table reads as zero rows, with each column typed."""
        pl.DataFrame(schema=_EVENTS.schema).write_delta(tmp_path / "empty")

        frame = LoaderFactory.load(DeltaLakeConfig(table_uri=str(tmp_path / "empty"))).collect()

        assert frame.height == 0
        assert frame.schema == _EVENTS.schema

    def test_it_keeps_the_schema_of_an_iceberg_table_with_no_snapshot(self, tmp_path: Path) -> None:
        """Ensure a table that was created but never written reads as zero rows."""
        table = _catalog(tmp_path).create_table("lake.empty", schema=_EVENTS.to_arrow().schema)

        frame = LoaderFactory.load(IcebergConfig(table_uri=table.metadata_location)).collect()

        assert frame.height == 0
        assert frame.schema == _EVENTS.schema


class TestLakehouseScanFailures:
    """Validate that a table, version, or snapshot that cannot be read fails at connect.

    Polars defers a scan until its rows are read, so these failures used to
    escape mid-run, unwrapped, and the CLI called them a bug in Veridelta.
    """

    def test_it_names_a_delta_table_that_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure a missing table fails with its URI, before the comparison starts."""
        missing = str(tmp_path / "missing")

        with pytest.raises(
            ConnectorError, match=re.escape(f"Delta Lake scan of '{missing}' failed")
        ):
            DeltaLakeConnector(DeltaLakeConfig(table_uri=missing)).connect()

    def test_it_names_a_directory_that_holds_no_delta_table(self, tmp_path: Path) -> None:
        """Ensure a directory with no transaction log fails as a scan, not as a bug."""
        (tmp_path / "plain").mkdir()
        (tmp_path / "plain" / "notes.txt").write_text("not a table", encoding="utf-8")

        with pytest.raises(ConnectorError, match="Delta Lake scan of"):
            DeltaLakeConnector(DeltaLakeConfig(table_uri=str(tmp_path / "plain"))).connect()

    def test_it_names_a_delta_version_that_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure time travel past the latest version fails at connect."""
        _delta_table(tmp_path / "events")
        config = DeltaLakeConfig(table_uri=str(tmp_path / "events"), version=7)

        with pytest.raises(ConnectorError, match="Delta Lake scan of"):
            DeltaLakeConnector(config).connect()

    def test_it_names_iceberg_metadata_that_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure a missing metadata file fails with its URI, before the comparison starts."""
        missing = str(tmp_path / "metadata" / "v1.metadata.json")

        with pytest.raises(ConnectorError, match="Iceberg scan of"):
            IcebergConnector(IcebergConfig(table_uri=missing)).connect()

    def test_it_names_an_iceberg_snapshot_that_does_not_exist(self, tmp_path: Path) -> None:
        """Ensure time travel to an unknown snapshot fails at connect.

        Polars only looks a snapshot up once it reads rows, so a schema read
        alone would let this one through.
        """
        metadata = _iceberg_table(tmp_path)(False).table_uri
        config = IcebergConfig(table_uri=metadata, snapshot_id=12345)

        with pytest.raises(ConnectorError, match="Iceberg scan of") as info:
            IcebergConnector(config).connect()

        assert "12345" in str(info.value)
