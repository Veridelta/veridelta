# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Lakehouse-native connectors for Delta Lake and Apache Iceberg.

Scanners return unevaluated Polars LazyFrames. The `deltalake` extra is
imported only through `pl.scan_delta`. An Iceberg table's metadata is loaded
here, through `pyiceberg`, and the table handed to `pl.scan_iceberg`.

A missing extra and a failed scan are reported separately: the former names
the extra to install, the latter names the table and carries the scanner's own
message, so a bad snapshot or an unreachable bucket never reads as an install
problem. Scan lifecycle is logged under the `veridelta.connectors.lakehouse`
logger; log lines carry the table URI and pin, never `storage_options`.

Inside `checking_iceberg_files`, which the MCP server opens, an Iceberg scan
reads every file through one PyIceberg FileIO that checks each path on this
machine before it opens it.
"""

import logging
import os
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

import polars as pl

from veridelta.connectors.base import ReaderConnector, shown_location, without_location
from veridelta.exceptions import ConnectorError, missing_extra
from veridelta.models import DeltaLakeConfig, IcebergConfig, names_a_pyiceberg_class

if TYPE_CHECKING:
    from pyiceberg.io import FileIO
    from pyiceberg.table import StaticTable

logger = logging.getLogger(__name__)

_UNCONNECTED = "Lakehouse connector is not connected. Call connect() first."
_DELTA_EXTRA = missing_extra("delta", "Reading a Delta Lake table")
_ICEBERG_EXTRA = missing_extra("iceberg", "Reading an Iceberg table")

_file_check: ContextVar[Callable[[str], None] | None] = ContextVar(
    "veridelta_iceberg_file_check", default=None
)
"""What checks each path on this machine an Iceberg scan opens, or None for no check."""


@contextmanager
def checking_iceberg_files(check: Callable[[str], None]) -> Generator[None]:
    """Check each file on this machine that an Iceberg scan inside the block opens.

    Polars plans a scan with the `polars_iceberg` plugin when it is installed,
    which reads the metadata, the manifest lists, and the manifests through
    Polars' own storage, and it reads the data files itself. Polars parses a
    location otherwise than PyIceberg does, and keeps a cache of metadata
    files across scans. So inside the block, a scan plans and reads through
    PyIceberg alone, with one FileIO that calls `check` with the path PyArrow
    is about to open, just before it opens it. No other reader opens a file,
    and Polars' cache never serves a FileIO of this kind.

    Args:
        check (Callable[[str], None]): Raises to refuse a path, which is
            absolute or relative to the working directory.

    Yields:
        None: Control, for the scans to run inside the block.
    """
    token = _file_check.set(check)
    try:
        yield
    finally:
        _file_check.reset(token)


def _checked_file_io(properties: dict[str, str], check: Callable[[str], None]) -> "FileIO":
    """Build a PyArrow FileIO that calls `check` before it opens a file on this machine.

    The path checked is the one the FileIO opens, since both come from its own
    `parse_location`. PyArrow opens a location from this machine's disk only
    when its scheme parses as `file`, as a location with none does unless
    `DEFAULT_SCHEME` names another.
    """
    from pyiceberg.io.pyarrow import PyArrowFile, PyArrowFileIO

    class CheckedFileIO(PyArrowFileIO):  # type: ignore[misc]  # mypy skips PyIceberg, so its classes read as Any.
        """A PyArrow FileIO that checks each path on this machine before it opens it."""

        def new_input(self, location: str) -> PyArrowFile:
            """Check the path PyArrow opens for a location on this machine, then open it."""
            scheme, _, path = self.parse_location(location, self.properties)
            if scheme == "file":
                check(path)
            return super().new_input(location)

    return CheckedFileIO(properties)


class DeltaLakeConnector(ReaderConnector):
    """Delta Lake scanner backed by `pl.scan_delta`.

    `connect()` opens a lazy scan of `DeltaLakeConfig.table_uri`, pinned to
    `version` when one is set, and `lazyframe()` hands that scan to the local
    engine. No SQL is involved: the comparison runs in Polars. Requires the
    `delta` extra (`uv add 'veridelta[delta]'`).
    """

    _unconnected = _UNCONNECTED

    def __init__(self, config: DeltaLakeConfig) -> None:
        """Initialize the connector with validated Delta Lake settings.

        Args:
            config (DeltaLakeConfig): Frozen table URI and optional version.
        """
        self._config = config

    def connect(self) -> None:
        """Open a lazy `pl.scan_delta` of the configured table and read its log.

        Raises:
            ConnectorError: If the `deltalake` extra is missing, or the table
                or version cannot be read.
        """
        storage_options = self._config.storage_options or None
        try:
            frame = pl.scan_delta(
                self._config.table_uri,
                version=self._config.version,
                storage_options=storage_options,
            )
            # Polars defers the scan, so read the log now: a missing table or
            # version then fails here, named, instead of partway through a run.
            frame.collect_schema()
        except ImportError as exc:
            raise ConnectorError(_DELTA_EXTRA) from exc
        except Exception as exc:
            where = shown_location(self._config.table_uri)
            logger.warning("Delta Lake scan of %s failed", where)
            raise ConnectorError(
                f"Delta Lake scan of '{where}' failed: "
                f"{without_location(str(exc), self._config.table_uri)}"
            ) from exc
        self._frame = frame
        logger.info(
            "Opened Delta Lake scan of %s (version=%s)",
            shown_location(self._config.table_uri),
            "latest" if self._config.version is None else self._config.version,
        )


def _metadata_location(table_uri: str, io: "FileIO") -> str:
    """Name the metadata file of a table, from its folder's version hint when it is a folder.

    This follows PyIceberg's `StaticTable.from_metadata`.

    Args:
        table_uri (str): The table's metadata file or folder.
        io (FileIO): Reads the table's files.

    Returns:
        str: The location of the table's current metadata file.
    """
    if table_uri.endswith(".metadata.json"):
        return table_uri
    folder = os.path.join(table_uri, "metadata")
    with io.new_input(os.path.join(folder, "version-hint.text")).open() as stream:
        hint = stream.read().decode("utf-8")
    if hint.endswith(".metadata.json"):
        return os.path.join(folder, hint)
    if hint.isnumeric():
        return os.path.join(folder, f"v{hint}.metadata.json")
    return os.path.join(folder, f"{hint}.metadata.json")


def _static_table(table_uri: str, storage_options: dict[str, str]) -> "StaticTable":
    """Load an Iceberg table from its metadata, with a FileIO built from `storage_options` alone.

    PyIceberg's `StaticTable.from_metadata`, and Polars when given a path,
    build the table's FileIO from its metadata properties too, and import the
    class a `py-io-impl` property names. Whoever writes a table writes those
    properties. So this FileIO reads `storage_options` only, which the model
    has already refused any key in that names a class, and the table keeps no
    such property for a later reader to import. Inside
    `checking_iceberg_files`, the FileIO checks each path before it opens it.

    Args:
        table_uri (str): The table's metadata file or folder.
        storage_options (dict[str, str]): The validated storage options.

    Returns:
        StaticTable: The table, to hand to `pl.scan_iceberg`.
    """
    from pyiceberg.catalog.noop import NoopCatalog
    from pyiceberg.io import load_file_io
    from pyiceberg.serializers import FromInputFile
    from pyiceberg.table import StaticTable

    check = _file_check.get()
    io = (
        load_file_io(dict(storage_options), location=table_uri)
        if check is None
        else _checked_file_io(dict(storage_options), check)
    )
    location = _metadata_location(table_uri, io)
    metadata = FromInputFile.table_metadata(io.new_input(location))
    properties = {
        key: value for key, value in metadata.properties.items() if not names_a_pyiceberg_class(key)
    }
    return StaticTable(
        identifier=("static-table", location),
        metadata_location=location,
        metadata=metadata.model_copy(update={"properties": properties}),
        io=io,
        catalog=NoopCatalog("static-table"),
    )


class IcebergConnector(ReaderConnector):
    """Apache Iceberg scanner backed by `pl.scan_iceberg`.

    `connect()` loads the metadata of `IcebergConfig.table_uri` itself, with a
    FileIO built from `storage_options` alone, and opens a lazy scan of that
    table, pinned to `snapshot_id` when one is set. `lazyframe()` hands that
    scan to the local engine. As with Delta Lake, the comparison runs in
    Polars. Requires the `iceberg` extra (`uv add 'veridelta[iceberg]'`).
    """

    _unconnected = _UNCONNECTED

    def __init__(self, config: IcebergConfig) -> None:
        """Initialize the connector with validated Iceberg settings.

        Args:
            config (IcebergConfig): Frozen table URI and storage options.
        """
        self._config = config

    def connect(self) -> None:
        """Read the configured table's metadata and open a lazy `pl.scan_iceberg` of it.

        Raises:
            ConnectorError: If the `pyiceberg` extra is missing, or the table
                or snapshot cannot be read.
        """
        try:
            frame = pl.scan_iceberg(
                _static_table(self._config.table_uri, self._config.storage_options),
                snapshot_id=self._config.snapshot_id,
                storage_options=self._config.storage_options or None,
                # PyIceberg then plans the scan and reads every file, through
                # the table's FileIO, where Polars would read them itself.
                reader_override=None if _file_check.get() is None else "pyiceberg",
            )
            # Polars looks a snapshot up only to read rows, so time travel reads
            # one now: a missing snapshot then fails here, named, instead of
            # partway through a run.
            if self._config.snapshot_id is None:
                frame.collect_schema()
            else:
                frame.head(1).collect()
        except ImportError as exc:
            raise ConnectorError(_ICEBERG_EXTRA) from exc
        except Exception as exc:
            where = shown_location(self._config.table_uri)
            logger.warning("Iceberg scan of %s failed", where)
            raise ConnectorError(
                f"Iceberg scan of '{where}' failed: "
                f"{without_location(str(exc), self._config.table_uri)}"
            ) from exc
        self._frame = frame
        logger.info(
            "Opened Iceberg scan of %s (snapshot_id=%s)",
            shown_location(self._config.table_uri),
            "latest" if self._config.snapshot_id is None else self._config.snapshot_id,
        )
