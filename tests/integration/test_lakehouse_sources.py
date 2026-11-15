# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Integration tests that read real Delta Lake and Iceberg tables.

Each test writes its tables under `tmp_path`: Delta through `deltalake`, and
Iceberg through a pyiceberg catalog kept in a SQLite file. So the scans read
the same transaction logs and metadata files a lakehouse holds. Object stores
need an account, so `storage_options` are covered by the unit tests alone.
"""

import json
import re
import shutil
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from veridelta.connectors import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.lakehouse import (
    _checked_file_io,  # pyright: ignore[reportPrivateUsage] - pins the path the FileIO checks
    _static_table,  # pyright: ignore[reportPrivateUsage] - pins the table the loader builds
    checking_iceberg_files,
)
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


@contextmanager
def _catalog(path: Path) -> Iterator[SqlCatalog]:
    """Open an Iceberg catalog with one namespace, kept in a SQLite file under `path`.

    The warehouse is a plain path, since pyiceberg reads `file:///C:/...` as
    `/C:/...`, which Windows rejects, but takes a drive path as local. The
    catalog's SQLite connections close on exit: Python 3.13 warns about any
    left open, and the test suite fails on every warning.
    """
    catalog = SqlCatalog("veridelta", uri=f"sqlite:///{path / 'catalog.db'}", warehouse=str(path))
    try:
        catalog.create_namespace("lake")
        yield catalog
    finally:
        catalog.engine.dispose()


def _iceberg_table(path: Path) -> _LakehouseTable:
    """Append `_EVENTS`, then overwrite it with `_CHANGED`, in an Iceberg table."""
    with _catalog(path) as catalog:
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


class MarkerFileIO:
    """Stand in for a class a table's metadata names in `py-io-impl`, and count each load."""

    loads = 0

    def __init__(self, properties: dict[str, str]) -> None:
        """Count the load PyIceberg makes when it builds a FileIO from this class."""
        del properties
        MarkerFileIO.loads += 1


_CLASS_PROPERTIES = ("py-io-impl", "s3.retry-strategy-impl", "write.py-location-provider.impl")
"""The PyIceberg properties whose class it imports, which a table's metadata can set."""


def _name_a_class_in_the_metadata(metadata: str) -> None:
    """Write each property in `_CLASS_PROPERTIES`, naming `MarkerFileIO`, into a table's metadata file."""
    path = Path(metadata)
    document = json.loads(path.read_text(encoding="utf-8"))
    for key in _CLASS_PROPERTIES:
        document["properties"][key] = f"{__name__}.{MarkerFileIO.__name__}"
    path.write_text(json.dumps(document), encoding="utf-8")


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
        with _catalog(tmp_path) as catalog:
            table = catalog.create_table("lake.empty", schema=_EVENTS.to_arrow().schema)

        frame = LoaderFactory.load(IcebergConfig(table_uri=table.metadata_location)).collect()

        assert frame.height == 0
        assert frame.schema == _EVENTS.schema


class TestIcebergMetadataProperties:
    """Validate that a table's metadata cannot choose a class for PyIceberg to import.

    PyIceberg, and Polars given a path, build a table's FileIO from its
    metadata properties, and import the class `py-io-impl` names there.
    """

    @pytest.mark.parametrize("first", [False, True], ids=["latest", "snapshot"])
    def test_it_reads_a_table_without_loading_the_class_its_metadata_names(
        self, tmp_path: Path, first: bool
    ) -> None:
        """Ensure the table reads its rows, and the class is never imported."""
        source = _iceberg_table(tmp_path)(first)
        _name_a_class_in_the_metadata(source.table_uri)
        MarkerFileIO.loads = 0

        frame = LoaderFactory.load(source).collect()

        assert frame.sort("id").equals(_EVENTS if first else _CHANGED)
        assert MarkerFileIO.loads == 0

    def test_it_keeps_no_property_that_names_a_class(self, tmp_path: Path) -> None:
        """Ensure the loaded table drops each class property, so no later reader imports it."""
        metadata = _iceberg_table(tmp_path)(False).table_uri
        _name_a_class_in_the_metadata(metadata)

        table = _static_table(metadata, {})

        assert not set(_CLASS_PROPERTIES) & set(table.metadata.properties)

    @pytest.mark.parametrize("hint", ["name", "version", "stem"])
    def test_it_reads_a_table_folder_through_its_version_hint(
        self, tmp_path: Path, hint: str
    ) -> None:
        """Ensure a folder resolves its metadata file each way PyIceberg's version hint names it."""
        metadata = Path(_iceberg_table(tmp_path)(False).table_uri)
        _name_a_class_in_the_metadata(str(metadata))
        if hint == "version":
            shutil.copyfile(metadata, metadata.with_name("v7.metadata.json"))
        text = {
            "name": metadata.name,
            "version": "7",
            "stem": metadata.name.removesuffix(".metadata.json"),
        }[hint]
        (metadata.parent / "version-hint.text").write_text(text, encoding="utf-8")
        MarkerFileIO.loads = 0

        frame = LoaderFactory.load(IcebergConfig(table_uri=str(metadata.parent.parent))).collect()

        assert frame.sort("id").equals(_CHANGED)
        assert MarkerFileIO.loads == 0


class TestCheckedIcebergScan:
    """Validate the Iceberg scan the MCP server runs, which checks each file before it opens it.

    Polars plans a scan with the `polars_iceberg` plugin, and reads the data
    files itself. Inside `checking_iceberg_files`, PyIceberg plans and reads
    every file through one FileIO instead, so each one passes the check.
    """

    @pytest.mark.parametrize("first", [False, True], ids=["latest", "snapshot"])
    def test_it_checks_each_file_and_reads_the_same_rows(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
        first: bool,
    ) -> None:
        """Ensure the scan checks every file it opens, never plans with the plugin, and loses no row."""
        source = _iceberg_table(tmp_path)(first)
        monkeypatch.setenv("POLARS_VERBOSE", "1")
        checked: list[str] = []

        with checking_iceberg_files(checked.append):
            frame = LoaderFactory.load(source).collect()

        assert frame.sort("id").equals(_EVENTS if first else _CHANGED)
        assert source.table_uri in checked
        assert any(path.endswith(".parquet") for path in checked)
        assert all(Path(path).is_relative_to(tmp_path) for path in checked)
        assert "polars_iceberg plugin" not in capfd.readouterr().err

    def test_it_checks_only_a_file_it_opens_from_this_machine(self) -> None:
        """Ensure a file in a store is left to the store, and a local one is checked as opened."""
        checked: list[str] = []
        io = _checked_file_io({"s3.region": "us-east-1"}, checked.append)

        io.new_input("s3://lake/events/data.parquet")
        io.new_input("FILE:///lake/events/data.parquet")

        assert checked == ["/lake/events/data.parquet"]


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
