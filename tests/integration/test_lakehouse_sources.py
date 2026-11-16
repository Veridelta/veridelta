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
import struct
import sys
import tempfile
import uuid
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler
from packaging.version import Version
from pyarrow.fs import LocalFileSystem
from pyiceberg.catalog.sql import SqlCatalog
from pytest_mock import MockerFixture

from veridelta import mcp_server
from veridelta.connectors import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.lakehouse import (
    CheckedDeltaTable,
    _static_table,  # pyright: ignore[reportPrivateUsage] - pins the table the loader builds
    checked_file_io,
    checking_table_files,
)
from veridelta.engine import DiffEngine, LoaderFactory
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.mcp_server import Settings
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


def _listed(config: DeltaLakeConfig) -> CheckedDeltaTable:
    """Stand in for the MCP server's listing: return the table at the version a scan reads."""
    return CheckedDeltaTable(DeltaTable(config.table_uri, version=config.version))


def _unlisted(config: DeltaLakeConfig) -> CheckedDeltaTable:
    """Stand in for the listing where no Delta Lake table is read."""
    raise AssertionError(f"An Iceberg scan listed the Delta Lake table {config.table_uri}.")


_Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
"""The alphabet in which a Delta Lake log writes the UUID of a deletion vector at a relative path."""


def _delete_with_a_vector(table: Path, vector: Path | None, row: int) -> None:
    """Delete one row of the data file the table's first commit adds, with a vector at `vector`.

    deltalake rewrites a file to delete from it, so the test writes the
    deletion vector and the commit that records it, as Spark would. The file
    holds a version byte, then the bitmap's size, Delta's portable roaring
    bitmap of the deleted row, and its CRC-32. The commit records the vector
    at its own path, as a URI, or, with no `vector`, at a path relative to
    the table, in a folder `ab`, as Spark writes it.
    """
    bitmap = struct.pack("<II", 12346, 1) + struct.pack("<HH", 0, 0)
    bitmap = struct.pack("<iqi", 1681511377, 1, 0) + bitmap + struct.pack("<IH", 16, row)
    if vector is None:
        name = uuid.uuid4()
        vector = table / "ab" / f"deletion_vector_{name}.bin"
        vector.parent.mkdir()
        encoded = ""
        for start in range(0, 16, 4):
            number = int.from_bytes(name.bytes[start : start + 4], "big")
            encoded += "".join(_Z85[number // 85**place % 85] for place in range(4, -1, -1))
        stored = {"storageType": "u", "pathOrInlineDv": f"ab{encoded}"}
    else:
        stored = {"storageType": "p", "pathOrInlineDv": vector.as_uri()}
    vector.write_bytes(
        b"\x01" + struct.pack(">i", len(bitmap)) + bitmap + struct.pack(">I", zlib.crc32(bitmap))
    )
    log = table / "_delta_log"
    actions = [json.loads(line) for line in (log / f"{0:020}.json").read_text("utf-8").splitlines()]
    (added,) = [action["add"] for action in actions if "add" in action]
    descriptor = {**stored, "offset": 1, "sizeInBytes": len(bitmap), "cardinality": 1}
    removed = {"path": added["path"], "dataChange": True, "deletionTimestamp": 1}
    commit = [{"remove": removed}, {"add": dict(added, deletionVector=descriptor)}]
    version = len(list(log.glob("*.json")))
    (log / f"{version:020}.json").write_text(
        "".join(json.dumps(action) + "\n" for action in commit), encoding="utf-8"
    )


def _versioned(path: Path) -> DeltaLakeConfig:
    """Write a table whose first version a scan reads, overwritten since."""
    _EVENTS.write_delta(path)
    _CHANGED.write_delta(path, mode="overwrite")
    return DeltaLakeConfig(table_uri=str(path), version=0)


def _evolved(path: Path) -> DeltaLakeConfig:
    """Write a partitioned table that gained a column, then lost a row to a delete."""
    _EVENTS.write_delta(path, delta_write_options={"partition_by": ["tier"]})
    _EVENTS.with_columns(note=pl.lit("added")).write_delta(
        path, mode="append", delta_write_options={"schema_mode": "merge"}
    )
    DeltaTable(str(path)).delete("id = 2")
    return DeltaLakeConfig(table_uri=str(path))


def _vectored(path: Path) -> DeltaLakeConfig:
    """Write a table of two files, one of whose rows a deletion vector deletes."""
    _EVENTS.write_delta(
        path, delta_write_options={"configuration": {"delta.enableDeletionVectors": "true"}}
    )
    _CHANGED.write_delta(path, mode="append")
    _delete_with_a_vector(path, path.parent / "vector.bin", 1)
    return DeltaLakeConfig(table_uri=str(path))


def _relatively_vectored(path: Path) -> DeltaLakeConfig:
    """Write a table of two files, one of whose rows a vector at a path relative to the table deletes."""
    _EVENTS.write_delta(
        path, delta_write_options={"configuration": {"delta.enableDeletionVectors": "true"}}
    )
    _CHANGED.write_delta(path, mode="append")
    _delete_with_a_vector(path, None, 1)
    return DeltaLakeConfig(table_uri=str(path))


def _checkpointed(path: Path) -> DeltaLakeConfig:
    """Write a table whose log holds a checkpoint, and a commit after it."""
    _EVENTS.write_delta(path)
    _CHANGED.write_delta(path, mode="append")
    DeltaTable(str(path)).create_checkpoint()
    _EVENTS.with_columns(pl.col("id") + 10).write_delta(path, mode="append")
    return DeltaLakeConfig(table_uri=str(path))


def _blanked(path: Path) -> DeltaLakeConfig:
    """Write a table partitioned by a text column, one of whose values is `''` and one null.

    deltalake reads `''` as null, where Polars reads the folder `tier=` as `''`.
    """
    pl.DataFrame({"id": [1, 2, 3, 4], "tier": ["", "gold", None, "a b"]}).write_delta(
        path, delta_write_options={"partition_by": ["tier"]}
    )
    return DeltaLakeConfig(table_uri=str(path))


def _emptied(path: Path) -> DeltaLakeConfig:
    """Write a table with no rows."""
    pl.DataFrame(schema=_EVENTS.schema).write_delta(path)
    return DeltaLakeConfig(table_uri=str(path))


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
    files itself. Inside `checking_table_files`, PyIceberg plans and reads
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
        checked: list[tuple[str, bool]] = []

        with checking_table_files(lambda *opened: checked.append(opened), _unlisted):
            frame = LoaderFactory.load(source).collect()

        assert frame.sort("id").equals(_EVENTS if first else _CHANGED)
        assert not any(remote for _, remote in checked)
        # Windows writes the path checked with backslashes alone, and the table_uri with both.
        assert Path(source.table_uri) in {Path(path) for path, _ in checked}
        assert any(path.endswith(".parquet") for path, _ in checked)
        assert all(Path(path).is_relative_to(tmp_path) for path, _ in checked)
        assert "polars_iceberg plugin" not in capfd.readouterr().err

    def test_it_checks_a_file_by_its_path_here_and_by_its_host_elsewhere(self) -> None:
        """Ensure a file in a store is checked by its URI, and a local one by the path it opens."""
        checked: list[tuple[str, bool]] = []
        io = checked_file_io({"s3.region": "us-east-1"}, lambda *opened: checked.append(opened))

        io.new_input("s3://lake/events/data.parquet")
        io.new_input("FILE:///lake/events/data.parquet")

        assert checked == [
            ("s3://lake/events/data.parquet", True),
            ("/lake/events/data.parquet", False),
        ]

    @pytest.mark.parametrize(
        "location",
        ["hdfs://namenode/lake/data.parquet", "viewfs://cluster/lake/data.parquet"],
    )
    def test_it_opens_no_file_through_libhdfs(self, location: str) -> None:
        """Ensure a file in HDFS is refused, since PyArrow can load libhdfs from the working directory."""
        checked: list[tuple[str, bool]] = []
        io = checked_file_io({}, lambda *opened: checked.append(opened))

        with pytest.raises(ConnectorError, match="scheme other than file: or an object store's"):
            io.new_input(location)

        assert checked == []


class TestCheckedDeltaScan:
    """Validate the Delta Lake scan the MCP server runs, which checks each data file as it opens it.

    Polars and deltalake read a table's files in Rust. Inside
    `checking_table_files`, the scan reads the version the listing returns,
    and its data files through a PyArrow file system that checks each one.
    """

    @pytest.mark.parametrize(
        "table",
        [
            _versioned,
            _evolved,
            pytest.param(
                _vectored,
                id="_vectored",
                marks=pytest.mark.skipif(
                    Version(pl.__version__) < Version("1.40.0"),
                    reason="Polars reads a table with deletion vectors from 1.40.0 on, "
                    "so an older one gives no rows to compare with.",
                ),
            ),
            _blanked,
            _emptied,
        ],
    )
    def test_it_reads_the_rows_polars_reads(
        self, tmp_path: Path, table: Callable[[Path], DeltaLakeConfig]
    ) -> None:
        """Ensure the checked scan reads every row, column, and type the command line reads, in order."""
        config = table(tmp_path / "events")
        expected = LoaderFactory.load(config).collect()
        checked: list[tuple[str, bool]] = []

        with checking_table_files(lambda *opened: checked.append(opened), _listed):
            frame = LoaderFactory.load(config).collect()

        assert frame.equals(expected)
        assert frame.schema == expected.schema
        assert not any(remote for _, remote in checked)
        listed = _listed(config).table.file_uris()
        assert sorted(Path(path) for path, _ in checked) == sorted(Path(uri) for uri in listed)

    @pytest.mark.parametrize(
        "table",
        [
            _versioned,
            _evolved,
            *(
                pytest.param(
                    vectored,
                    id=vectored.__name__,
                    marks=pytest.mark.skipif(
                        Version(pl.__version__) < Version("1.40.0"),
                        reason="Polars reads a table with deletion vectors from 1.40.0 on, "
                        "so an older one gives no rows to compare with.",
                    ),
                )
                for vectored in (_vectored, _relatively_vectored)
            ),
            _checkpointed,
            _blanked,
            _emptied,
        ],
    )
    def test_the_server_reads_the_rows_polars_reads(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
        table: Callable[[Path], DeltaLakeConfig],
    ) -> None:
        """Ensure the MCP server's scan reads every row the command line reads, and leaves no copy behind.

        The server loads a table on this machine from a copy of the log it
        checked, in the system's temporary folder, and runs in its first root.
        """
        monkeypatch.chdir(tmp_path)
        temporary = tmp_path_factory.mktemp("temporary")
        monkeypatch.setattr(tempfile, "tempdir", str(temporary))
        config = table(tmp_path / "events")
        expected = LoaderFactory.load(config).collect()

        with mcp_server._reading(Settings((tmp_path,))):  # pyright: ignore[reportPrivateUsage] - the block every tool reads its sides in
            frame = LoaderFactory.load(config).collect()

        assert frame.equals(expected)
        assert frame.schema == expected.schema
        assert list(temporary.iterdir()) == []

    def test_it_drops_the_rows_a_deletion_vector_deletes(self, tmp_path: Path) -> None:
        """Ensure a row a vector deletes is gone, from the file it was deleted from alone."""
        config = _vectored(tmp_path / "events")

        with checking_table_files(lambda *_: None, _listed):
            frame = LoaderFactory.load(config).collect()

        assert frame["id"].to_list() == [1, 3, 1, 2, 3]

    def test_it_checks_each_data_file_as_it_opens_it(self, tmp_path: Path) -> None:
        """Ensure a check that refuses a data file stops the read, though the scan had started."""
        config = DeltaLakeConfig(table_uri=str(tmp_path / "events"))
        _versioned(tmp_path / "events")
        checked: list[tuple[str, bool]] = []
        with checking_table_files(lambda *opened: checked.append(opened), _listed):
            started = DeltaLakeConnector(config)
            started.connect()
        assert checked == []

        def refuse(location: str, remote: bool) -> None:
            raise ConfigError(f"refused {location} {remote}")

        with checking_table_files(refuse, _listed):
            stopped = DeltaLakeConnector(config)
            stopped.connect()
        started.lazyframe().collect()
        with pytest.raises(ConfigError, match=r"refused .* False"):
            stopped.lazyframe().collect()

        assert len(checked) == 1

    def test_it_reads_no_file_it_cannot_pair_with_a_fragment(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a table whose files and fragments come in another order is refused."""
        config = _evolved(tmp_path / "events")
        listed = DeltaTable.file_uris
        mocker.patch.object(
            DeltaTable, "file_uris", autospec=True, side_effect=lambda t: listed(t)[::-1]
        )

        with (
            checking_table_files(lambda *_: None, _listed),
            pytest.raises(ConnectorError, match="do not pair up"),
        ):
            DeltaLakeConnector(config).connect()

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="deltalake's own store turns an `fs:` URI into a Windows path in a way "
        "this test has no means to check, as `fs:///C:/...` names its drive after a slash.",
    )
    @pytest.mark.parametrize("scheme", ["fs", "opendal+fs"])
    def test_it_checks_each_file_of_a_local_scheme_by_the_path_it_opens(
        self, tmp_path: Path, scheme: str
    ) -> None:
        """Ensure a table that deltalake reads from this machine under its own scheme is checked here.

        deltalake names such a table's files by URI, not by path, so the scan
        checks each file at the path it then opens, as for a `file:` table.
        """
        config = _vectored(tmp_path / "events")
        with checking_table_files(lambda *_: None, _listed):
            expected = LoaderFactory.load(config).collect()
        uri = Path(config.table_uri).as_uri().replace("file:", f"{scheme}:", 1)
        local = config.model_copy(update={"table_uri": uri})
        checked: list[tuple[str, bool]] = []

        with checking_table_files(lambda *opened: checked.append(opened), _listed):
            frame = LoaderFactory.load(local).collect()

        assert frame.equals(expected)
        assert not any(remote for _, remote in checked)
        listed = _listed(config).table.file_uris()
        assert sorted(Path(path) for path, _ in checked) == sorted(Path(uri) for uri in listed)

    def test_it_opens_a_remote_file_through_deltalake(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Ensure a table in an object store has each file checked by its URI, then read through its store.

        The store is mocked: the table lies under `tmp_path`, but names an
        `s3://` URI, and its store opens each file from the folder.
        """
        config = DeltaLakeConfig(table_uri=str(tmp_path / "events"))
        _versioned(tmp_path / "events")
        mocker.patch.object(
            DeltaTable, "table_uri", new_callable=mocker.PropertyMock, return_value="s3://lake/t/"
        )
        mocker.patch.object(DeltaStorageHandler, "__init__", return_value=None)
        opened = mocker.patch.object(
            DeltaStorageHandler,
            "open_input_file",
            autospec=True,
            side_effect=lambda _, path: LocalFileSystem().open_input_file(
                str(tmp_path / "events" / path)
            ),
        )
        checked: list[tuple[str, bool]] = []

        with checking_table_files(lambda *opened: checked.append(opened), _listed):
            frame = LoaderFactory.load(config).collect()

        assert frame.sort("id").equals(_CHANGED)
        assert [remote for _, remote in checked] == [True]
        opened.assert_called_once()

    @pytest.mark.parametrize(
        "where",
        [
            "path",
            pytest.param(
                "fs",
                marks=pytest.mark.skipif(
                    sys.platform == "win32",
                    reason="deltalake's own store turns an `fs:` URI into a Windows path in a "
                    "way this test has no means to check.",
                ),
            ),
            "elsewhere",
        ],
    )
    def test_it_checks_a_file_opened_on_the_calling_thread(
        self, tmp_path: Path, mocker: MockerFixture, where: str
    ) -> None:
        """Ensure the scan's file system checks each file that Python's own thread opens through it.

        PyArrow opens a scan's files from threads of its own, where no tracer
        that `sys.settrace` sets runs. This opens each file through the same
        file system from the test's thread, so the check runs where one does.
        """
        table = tmp_path / "events"
        _versioned(table)
        config = DeltaLakeConfig(table_uri=str(table))
        if where == "fs":
            config = DeltaLakeConfig(table_uri=table.as_uri().replace("file:", "fs:", 1))
        if where == "elsewhere":
            mocker.patch.object(
                DeltaTable,
                "table_uri",
                new_callable=mocker.PropertyMock,
                return_value="s3://lake/t/",
            )
            mocker.patch.object(DeltaStorageHandler, "__init__", return_value=None)
            mocker.patch.object(
                DeltaStorageHandler,
                "open_input_file",
                autospec=True,
                side_effect=lambda _, path: LocalFileSystem().open_input_file(str(table / path)),
            )
        scanned = mocker.spy(pl, "scan_pyarrow_dataset")
        checked: list[tuple[str, bool]] = []
        with checking_table_files(lambda *opened: checked.append(opened), _listed):
            LoaderFactory.load(config)
        checked.clear()

        for call in scanned.call_args_list:
            for fragment in call.args[0].get_fragments():
                with fragment.open() as file:
                    assert file.read(4) == b"PAR1"

        listed = DeltaTable(str(table)).file_uris()
        assert [remote for _, remote in checked] == [where == "elsewhere"] * len(listed)
        if where != "elsewhere":
            assert sorted(Path(path) for path, _ in checked) == sorted(map(Path, listed))


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
