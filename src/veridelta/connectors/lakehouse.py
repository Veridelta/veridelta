# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Lakehouse-native connectors for Delta Lake and Apache Iceberg.

Scanners return unevaluated Polars LazyFrames. The `deltalake` extra is
imported through `pl.scan_delta`, and here only for the checked scan the MCP
server runs. An Iceberg table's metadata is loaded here, through `pyiceberg`,
and the table handed to `pl.scan_iceberg`.

A missing extra and a failed scan are reported separately: the former names
the extra to install, the latter names the table and carries the scanner's own
message, so a bad snapshot or an unreachable bucket never reads as an install
problem. Scan lifecycle is logged under the `veridelta.connectors.lakehouse`
logger; log lines carry the table URI and pin, never `storage_options`.

Inside `checking_table_files`, which the MCP server opens, an Iceberg scan
reads every file through one PyIceberg FileIO, and a Delta Lake scan reads its
data files through one PyArrow file system. Each checks every file just before
it opens it.
"""

import json
import logging
import os
import re
import sys
import uuid
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from importlib import import_module
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlsplit
from urllib.request import url2pathname

import polars as pl

from veridelta.connectors.base import ReaderConnector, shown_location, without_location
from veridelta.exceptions import ConnectorError, VerideltaError, missing_extra
from veridelta.models import DeltaLakeConfig, IcebergConfig, names_a_pyiceberg_class

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from pyiceberg.io import FileIO
    from pyiceberg.table import StaticTable

logger = logging.getLogger(__name__)

_UNCONNECTED = "Lakehouse connector is not connected. Call connect() first."
_DELTA_EXTRA = missing_extra("delta", "Reading a Delta Lake table")
_ICEBERG_EXTRA = missing_extra("iceberg", "Reading an Iceberg table")

_file_check: ContextVar[Callable[[str, bool], None] | None] = ContextVar(
    "veridelta_table_file_check", default=None
)
"""What checks each file a lakehouse scan opens, or None for no check.

It takes a path on this machine, with False, or the URI of a file on another
machine, with True."""

_delta_listing: ContextVar[Callable[[DeltaLakeConfig], "DeltaTable"] | None] = ContextVar(
    "veridelta_delta_listing", default=None
)
"""What lists and checks every file of a Delta Lake table, and returns the version it checked."""

_CHECKED_SCHEMES: Final = frozenset(
    {"file", "s3", "s3a", "s3n", "oss", "gs", "gcs", "abfs", "abfss", "wasb", "wasbs"}
)
"""The schemes a checked FileIO opens a file through: `file`, whose path it
checks, and the object stores', whose host it checks. The endpoint comes from
`storage_options`, which the MCP server checks, or is the service's own.

PyArrow opens `hdfs` and `viewfs` through libhdfs, which it can load from the
working directory, the server's first root. A table whose files name either
would then run a library that anyone who can write to that root put there."""


@contextmanager
def checking_table_files(
    check: Callable[[str, bool], None], delta_table: Callable[[DeltaLakeConfig], "DeltaTable"]
) -> Generator[None]:
    """Check each file that a Delta Lake or Iceberg scan inside the block opens.

    Polars plans an Iceberg scan with the `polars_iceberg` plugin when it is
    installed, which reads the metadata, the manifest lists, and the
    manifests through Polars' own storage, and it reads the data files
    itself. Polars parses a location otherwise than PyIceberg does, and keeps
    a cache of metadata files across scans. So inside the block, an Iceberg
    scan plans and reads through PyIceberg alone, with one FileIO that calls
    `check` with the path PyArrow is about to open, just before it opens it.
    No other reader opens a file, and Polars' cache never serves a FileIO of
    this kind.

    Polars and deltalake read a Delta Lake table's files in Rust, where no
    check can run. So inside the block, `delta_table` lists and checks every
    file of the version a scan reads, and the scan reads that version's data
    files through one PyArrow file system that calls `check` with each one
    just before it opens it. The log and the deletion vectors are read in
    Rust, right after `delta_table` checks them.

    Args:
        check (Callable[[str, bool], None]): Raises to refuse a file: a path on
            this machine, absolute or relative to the working directory, with
            False, or the URI of a file on another machine, with True.
        delta_table (Callable[[DeltaLakeConfig], DeltaTable]): Lists and
            checks every file of a Delta Lake table, and returns the table at
            the version it checked.

    Yields:
        None: Control, for the scans to run inside the block.
    """
    check_token = _file_check.set(check)
    listing_token = _delta_listing.set(delta_table)
    try:
        yield
    finally:
        _delta_listing.reset(listing_token)
        _file_check.reset(check_token)


def checked_file_io(properties: dict[str, str], check: Callable[[str, bool], None]) -> "FileIO":
    """Build a PyArrow FileIO that calls `check` before it opens a file.

    The file checked is the one the FileIO opens, since both come from its own
    `parse_location`. PyArrow opens a location from this machine's disk only
    when its scheme parses as `file`, as a location with none does unless
    `DEFAULT_SCHEME` names another, and `check` gets its path. Any other
    location reaches the host the location names, and `check` gets it as a
    URI, with that host. A scheme outside `_CHECKED_SCHEMES` is refused before
    PyArrow builds a file system for it.

    Args:
        properties (dict[str, str]): The validated storage options.
        check (Callable[[str, bool], None]): Raises to refuse a file: a path
            on this machine, absolute or relative to the working directory,
            with False, or a URI, with True.

    Returns:
        FileIO: The FileIO, which raises `ConnectorError` for a refused scheme.
    """
    from pyiceberg.io.pyarrow import PyArrowFile, PyArrowFileIO

    class CheckedFileIO(PyArrowFileIO):  # type: ignore[misc]  # mypy skips PyIceberg, so its classes read as Any.
        """A PyArrow FileIO that checks each file before it opens it."""

        def new_input(self, location: str) -> PyArrowFile:
            """Check the path or the host PyArrow opens for a location, then open it."""
            scheme, _, path = self.parse_location(location, self.properties)
            if scheme not in _CHECKED_SCHEMES:
                # The scheme is a value from a file, so the message leaves it out.
                raise ConnectorError(
                    "An Iceberg scan was about to open a file through a scheme other than "
                    "file: or an object store's, so it was stopped."
                )
            if scheme == "file":
                check(path, False)
            else:
                # An object store's path starts with the host PyArrow reads it from.
                check(f"{scheme}://{path}", True)
            return super().new_input(location)

    return CheckedFileIO(properties)


_DELTA_LOCAL_SCHEMES: Final = frozenset({"file", "fs", "opendal+fs"})
"""The schemes whose table deltalake reads from this machine's disk, whatever host they name."""


_Z85: Final = (
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
)
"""The Z85 alphabet, in which a Delta Lake log writes a deletion vector's UUID."""

_VECTOR_PREFIX: Final = re.compile(r"[A-Za-z0-9]*")
"""The folder a deletion vector at a relative path may name: letters and digits, as Delta writers choose.

Delta Lake joins the folder to the table's URI as a URL, so a `/`, a `..`, or a
scheme in it would lead elsewhere."""


def _vector_uuid(text: str) -> uuid.UUID:
    """Decode the UUID a deletion vector's relative path ends with, from 20 characters of Z85."""
    value = bytearray()
    for start in range(0, len(text), 5):
        number = 0
        for character in text[start : start + 5]:
            number = number * 85 + _Z85.index(character)
        value += number.to_bytes(4, "big")
    return uuid.UUID(bytes=bytes(value))


def _vector_location(table_uri: str, vector: dict[str, Any]) -> str | None:
    """Name the file a deletion vector is stored in, as Delta Lake reads it, or None for one inline.

    A vector stored at its own path (`p`) names an absolute URI. One at a
    relative path (`u`) names a folder, then a UUID in its last 20 characters,
    and lies in the table's folder as `<folder>/deletion_vector_<uuid>.bin`.

    Raises:
        ValueError: If the vector is of an unknown kind, or its relative path
            names a folder of other characters than letters and digits.
    """
    kind, text = vector.get("storageType"), str(vector.get("pathOrInlineDv"))
    if kind == "i":
        return None
    if kind == "p":
        return text
    if kind != "u":
        raise ValueError("the log records a deletion vector of an unknown kind")
    folder, name = text[:-20], f"deletion_vector_{_vector_uuid(text[-20:])}.bin"
    if _VECTOR_PREFIX.fullmatch(folder) is None:
        raise ValueError(
            "the log records a deletion vector in a folder named with other characters "
            "than letters and digits"
        )
    return (
        f"{table_uri.rstrip('/')}/{folder}/{name}" if folder else f"{table_uri.rstrip('/')}/{name}"
    )


def _arrow() -> Any:
    """Import PyArrow, with the parts this module reads through, as a module of no known type.

    PyArrow ships no type stubs, so a checker would type every call into it as
    unknown.
    """
    for part in ("dataset", "fs", "parquet"):
        import_module(f"pyarrow.{part}")
    return import_module("pyarrow")


def _log_actions(store: Any, path: str) -> tuple[list[Any], list[Any]]:
    """Read the deletion vectors of the `add` actions, and the `sidecar` actions, that one log file records.

    Args:
        store (Any): The PyArrow file system that reads the file.
        path (str): The file, a commit or a checkpoint, as `store` names it.

    Returns:
        tuple[list[Any], list[Any]]: The vectors, and the sidecars, each None
            where an action records none.
    """
    arrow = _arrow()
    if path.endswith(".json"):
        with store.open_input_stream(path) as stream:
            text: str = stream.read().decode("utf-8")
        # A commit is JSON, one action to a line that ends at `\n` alone: a writer leaves a
        # character such as U+2028, which `splitlines` would also split at, unescaped in a value.
        actions: list[dict[str, Any]] = [
            json.loads(line) for line in text.split("\n") if line.strip()
        ]
        added: list[dict[str, Any]] = [action.get("add") or {} for action in actions]
        return (
            [action.get("deletionVector") for action in added],
            [action.get("sidecar") for action in actions],
        )
    if not path.endswith(".parquet"):
        return [], []
    schema = arrow.parquet.read_schema(path, filesystem=store)
    vectors: list[Any] = []
    index = schema.get_field_index("add")
    if index >= 0 and schema.field(index).type.get_field_index("deletionVector") >= 0:
        # The vectors alone, so a large checkpoint's statistics stay unread.
        read = arrow.parquet.read_table(path, filesystem=store, columns=["add.deletionVector"])
        vectors = read[0].to_pylist()
    sidecars: list[Any] = []
    if "sidecar" in schema.names:
        read = arrow.parquet.read_table(path, filesystem=store, columns=["sidecar"])
        sidecars = read[0].to_pylist()
    return vectors, sidecars


def delta_log_vectors(
    table_uri: str, storage_options: dict[str, str], files: Iterable[str]
) -> list[dict[str, Any]]:
    """Read each deletion vector a Delta Lake table's log records, and refuse a log that records a sidecar.

    deltalake reports which rows a vector deletes, but never where it read
    them. As it loads a table, it also reads each sidecar a checkpoint names,
    wherever the sidecar lies and whatever the table's protocol says, and
    reports none either. So this reads the log before deltalake does: the
    files of a log on this machine that the caller checked, and, for a table
    on another machine, each file in its log through deltalake's own storage,
    with `storage_options`. A vector a later commit removed counts too, so the
    vectors can name more files than a scan reads, but never fewer.

    Args:
        table_uri (str): The table, as its configuration names it.
        storage_options (dict[str, str]): The validated storage options.
        files (Iterable[str]): The files of the table's log on this machine,
            each a path the caller checked.

    Returns:
        list[dict[str, Any]]: Each vector an `add` action records.

    Raises:
        ValueError: If the log records a sidecar.
    """
    from deltalake.fs import DeltaStorageHandler

    arrow = _arrow()
    found: list[tuple[Any, str]] = [(arrow.fs.LocalFileSystem(), path) for path in files]
    scheme = urlsplit(table_uri).scheme
    if len(scheme) > 1 and scheme not in _DELTA_LOCAL_SCHEMES:
        # One letter is a Windows drive. deltalake's storage of a table on this machine
        # can crash as it lists a folder, so only a table elsewhere is read through it.
        store = arrow.fs.PyFileSystem(DeltaStorageHandler(table_uri, storage_options or None))
        selector = arrow.fs.FileSelector("_delta_log", allow_not_found=True, recursive=True)
        found += [(store, each.path) for each in store.get_file_info(selector)]
    vectors: list[dict[str, Any]] = []
    for store, path in found:
        added, sidecars = _log_actions(store, path)
        if any(sidecar is not None for sidecar in sidecars):
            raise ValueError(
                "the log records a checkpoint sidecar, a file this server does not check, "
                "so it reads none of the table"
            )
        vectors += [vector for vector in added if vector is not None]
    return vectors


def deletion_vector_files(table: "DeltaTable", vectors: list[dict[str, Any]]) -> Iterator[str]:
    """Name the file of each deletion vector that `delta_log_vectors` read from a table's log.

    A table whose protocol has no deletion vectors has none a scan reads.

    Args:
        table (DeltaTable): The table, at any version.
        vectors (list[dict[str, Any]]): The vectors its log records.

    Yields:
        str: Each vector's file, as a URI, or as a path on this machine.

    Raises:
        ValueError: If the log records a vector this cannot place.
    """
    if "deletionVectors" not in (table.protocol().reader_features or []):
        return
    for vector in vectors:
        location = _vector_location(table.table_uri, vector)
        if location is not None:
            yield location


def _deleted_rows(table: "DeltaTable") -> dict[str, list[int]]:
    """Map each data file a deletion vector covers to the rows it deletes, keyed as `file_uris` names it.

    deltalake names a file by URI, where `file_uris` names one on this machine
    by its path, so the `file:` scheme comes off, as Polars' own scan takes it.
    """
    if "deletionVectors" not in (table.protocol().reader_features or []):
        return {}
    vectors = pl.DataFrame(table.deletion_vectors())
    scheme = "file:///" if sys.platform == "win32" else "file://"
    return {
        str(path).removeprefix(scheme): [row for row, kept in enumerate(keep) if not kept]
        for path, keep in zip(vectors["filepath"], vectors["selection_vector"], strict=True)
    }


def _checked_delta_scan(
    table: "DeltaTable", check: Callable[[str, bool], None], storage_options: dict[str, str]
) -> pl.LazyFrame:
    """Scan a Delta Lake table's data files through a file system that checks each just before it opens it.

    Polars' own scan reads the data files in Rust, where no check can run. So
    this builds the PyArrow fragments deltalake's `to_pyarrow_dataset` builds,
    one for each file the table's version lists, and hands their reads to
    Polars. deltalake refuses that dataset for a table with deletion vectors,
    so this reads the rows each vector deletes once, right after the listing
    checked the vector's file, and drops them from the file's rows as Polars'
    scan does. Polars checks the table's protocol first, and refuses a table
    it cannot read.

    Polars reads a partition's value from the file's folder, where deltalake
    reads `''` in a text column as null. So a file in a folder such as `k=`
    has its null values of `k` read as `''`, as Polars reads them.

    A table on this machine, under any scheme deltalake reads from its disk,
    has each file opened here, at the path that `check` gets: the one
    `file_uris` names, or the path of the URI it names. One in an object
    store has each file opened through deltalake's own storage, which reaches
    only the table's host, and `check` gets the file's URI.

    Args:
        table (DeltaTable): The table, at the version that the listing checked.
        check (Callable[[str, bool], None]): Raises to refuse a file.
        storage_options (dict[str, str]): The validated storage options.

    Returns:
        pl.LazyFrame: The table's rows, in the order of its files.

    Raises:
        ConnectorError: If the files the table lists and the fragments it
            builds do not pair up.
    """
    from deltalake.fs import DeltaStorageHandler

    arrow = _arrow()
    # deltalake's handler returns PyArrow files, of no known type.
    handler: Any = DeltaStorageHandler

    class CheckedDeltaFiles(handler):  # type: ignore[misc]  # mypy cannot check a subclass of Any.
        """Open only the data files a table's version lists, each checked just before it opens."""

        def __init__(self, files: dict[str, str]) -> None:
            handler.__init__(self, table.table_uri, storage_options or None)
            self.files = files
            self.here = urlsplit(table.table_uri).scheme in _DELTA_LOCAL_SCHEMES

        def open_input_file(self, path: str) -> Any:
            """Check the file a fragment names, then open it."""
            # A path the version does not list raises a KeyError, and opens nothing.
            location = self.files[path]
            if not self.here:
                check(location, True)
                return handler.open_input_file(self, path)
            # `file_uris` names a file of a `file:` table by its path, and one of an
            # `fs:` or `opendal+fs:` table by a URI, whose path deltalake would open.
            parts = urlsplit(location)
            if parts.scheme in _DELTA_LOCAL_SCHEMES:
                location = url2pathname(parts.path)
            check(location, False)
            return arrow.fs.LocalFileSystem().open_input_file(location)

        open_input_stream = open_input_file

    # Polars refuses a protocol it cannot read, as `veridelta run` does.
    pl.scan_delta(table).collect_schema()
    schema = arrow.schema(table.schema().to_arrow())
    # deltalake's own fragments, which its `to_pyarrow_dataset` builds the same way.
    fragments = table._table.dataset_partitions(schema)  # pyright: ignore[reportPrivateUsage] - deltalake offers no public way to them.
    pairs = list(zip(table.file_uris(), fragments, strict=True))
    if any(not uri.replace("\\", "/").endswith(f"/{path}") for uri, (path, _) in pairs):
        raise ConnectorError(
            "The files a Delta Lake table lists do not pair up with the fragments it builds, "
            "so this server reads none of them."
        )
    files = arrow.fs.PyFileSystem(CheckedDeltaFiles({path: uri for uri, (path, _) in pairs}))
    form = arrow.dataset.ParquetFileFormat(
        default_fragment_scan_options=arrow.dataset.ParquetFragmentScanOptions(pre_buffer=True)
    )

    def scan(fragments: list[Any]) -> pl.LazyFrame:
        dataset = arrow.dataset.FileSystemDataset(fragments, schema, form, files)
        return pl.scan_pyarrow_dataset(dataset)  # pyright: ignore[reportUnknownMemberType] - Polars types the dataset as PyArrow's, which ships no stubs.

    deleted = _deleted_rows(table)
    texts = [
        field.name
        for field in table.schema().fields
        if field.name in table.metadata().partition_columns
        and getattr(field.type, "type", None) == "string"
    ]
    names: list[str] = schema.names
    # Longer than every column's name, so it is none of them.
    row = max(names, key=len, default="") + "_row"
    frames: list[pl.LazyFrame] = []
    run: list[Any] = []
    for uri, (path, partition) in pairs:
        fragment = form.make_fragment(path, filesystem=files, partition_expression=partition)
        blank = [name for name in texts if f"{name}=" in path.split("/")[:-1]]
        if uri not in deleted and not blank:
            run.append(fragment)
            continue
        kept = scan([fragment])
        if uri in deleted:
            kept = kept.with_row_index(row).filter(~pl.col(row).is_in(deleted[uri])).drop(row)
        frames += [scan(run), kept.with_columns(pl.col(name).fill_null("") for name in blank)]
        run = []
    return pl.concat([*frames, scan(run)])


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

        Inside `checking_table_files`, the table's files are listed and
        checked, and the scan reads the version they were listed from, each
        data file checked again just before it is opened.

        Raises:
            ConnectorError: If the `deltalake` extra is missing, or the table
                or version cannot be read.
            VerideltaError: Inside `checking_table_files`, if the listing
                refuses a file of the table.
        """
        storage_options = self._config.storage_options or None
        listing, check = _delta_listing.get(), _file_check.get()
        try:
            if listing is None or check is None:
                frame = pl.scan_delta(
                    self._config.table_uri,
                    version=self._config.version,
                    storage_options=storage_options,
                )
            else:
                frame = _checked_delta_scan(
                    listing(self._config), check, self._config.storage_options
                )
            # Polars defers the scan, so read the log now: a missing table or
            # version then fails here, named, instead of partway through a run.
            frame.collect_schema()
        except ImportError as exc:
            raise ConnectorError(_DELTA_EXTRA) from exc
        except VerideltaError:
            # A refusal of a file the table lists, which names the file itself.
            raise
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
    `checking_table_files`, the FileIO checks each file before it opens it.

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
        else checked_file_io(dict(storage_options), check)
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
