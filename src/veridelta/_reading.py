# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Read one side of a comparison into a lazy frame: a file, a lakehouse table, or a database.

`LoaderFactory` picks the loader for a file format, or the connector for a source
that names one. `veridelta.engine` re-exports the loaders as public API.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar, Final

import polars as pl

from veridelta.connectors.base import (
    ReaderConnector,
    optional_module,
    shown_location,
    without_location,
)
from veridelta.connectors.database import DatabaseConnector
from veridelta.connectors.duckdb import DuckDBConnector
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.exceptions import ConfigError, ConnectorError, missing_extra
from veridelta.models import (
    DatabaseConfig,
    DeltaLakeConfig,
    DuckDBConfig,
    IcebergConfig,
    SourceConfig,
    SourceRef,
)

# Named for the module the loaders lived in. `--verbose` prints the name, and the
# 0.14.4 changelog promised it, so it stays `veridelta.engine` wherever they live.
logger = logging.getLogger("veridelta.engine")


fastexcel = optional_module("fastexcel")
"""Presence probe for the `excel` extra. Polars imports this itself, but only
at call time, so checking here turns a bare ImportError into an install hint."""


class BaseLoader(ABC):
    """Base class for the file loaders, each turning a `SourceConfig` into a LazyFrame.

    Each file format has a subclass registered in `LoaderFactory._loaders`, keyed by
    its `SourceType`. A loader prefers a Polars `scan_*` reader, so the comparison
    stays lazy end to end; the eager loaders say why in their own docstrings.
    `SourceConfig.options` reach the reader unchanged, so any keyword the Polars
    function accepts is valid.
    """

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Load a source into a LazyFrame.

        Args:
            config (SourceConfig): Path, format, and reader options.

        Returns:
            pl.LazyFrame: The unevaluated rows.
        """


class CSVLoader(BaseLoader):
    """Streaming CSV loader over `pl.scan_csv`.

    Delimiters, encodings, and header handling are all controlled through
    `SourceConfig.options`, for example `{"separator": ";"}`.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Scan a CSV file.

        Args:
            config (SourceConfig): Source configuration, whose options go to `pl.scan_csv`.

        Returns:
            pl.LazyFrame: The unevaluated rows.
        """
        return pl.scan_csv(config.path, **config.options)


class ParquetLoader(BaseLoader):
    """Streaming Parquet loader over `pl.scan_parquet`.

    Accepts whatever path or glob the Polars scanner accepts. Because the scan
    stays lazy, columns dropped by `ignore` rules are never read from disk.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Scan a Parquet file.

        Args:
            config (SourceConfig): Source configuration, whose options go to `pl.scan_parquet`.

        Returns:
            pl.LazyFrame: The unevaluated rows.
        """
        return pl.scan_parquet(config.path, **config.options)


class NDJSONLoader(BaseLoader):
    """Streaming loader for newline-delimited JSON over `pl.scan_ndjson`.

    One record per line is the JSON shape Polars can read incrementally, so
    this is the format to prefer over `json` for large exports.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Scan a newline-delimited JSON file.

        Args:
            config (SourceConfig): Source configuration, whose options go to `pl.scan_ndjson`.

        Returns:
            pl.LazyFrame: The unevaluated rows.
        """
        return pl.scan_ndjson(config.path, **config.options)


class ArrowLoader(BaseLoader):
    """Streaming loader for Arrow IPC (Feather v2) files over `pl.scan_ipc`.

    IPC files carry their schema, so no type inference runs and the dtypes the
    engine compares are exactly the ones the writer stored.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Scan an Arrow IPC file.

        Args:
            config (SourceConfig): Source configuration, whose options go to `pl.scan_ipc`.

        Returns:
            pl.LazyFrame: The unevaluated rows.
        """
        return pl.scan_ipc(config.path, **config.options)


class AvroLoader(BaseLoader):
    """Loader for Avro object container files, read eagerly with `pl.read_avro`.

    Polars has no lazy Avro reader, so the file is read whole and wrapped, like
    JSON and Excel. Avro carries its schema, so the dtypes compared are the
    writer's, with no inference. The reader takes a local path only.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Read an Avro file.

        Args:
            config (SourceConfig): Source configuration, whose `columns` and `n_rows`
                options go to `pl.read_avro`.

        Returns:
            pl.LazyFrame: A lazy wrapper over the rows read.
        """
        return pl.read_avro(config.path, **config.options).lazy()


class JSONLoader(BaseLoader):
    """Loader for a JSON file holding one array of records.

    Polars has no lazy JSON reader: a JSON array cannot be parsed incrementally the
    way newline-delimited records can. The file is read whole and wrapped. Prefer
    `ndjson` for large files.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Read a JSON file.

        Args:
            config (SourceConfig): Source configuration, whose options go to `pl.read_json`.

        Returns:
            pl.LazyFrame: A lazy wrapper over the rows read.
        """
        return pl.read_json(config.path, **config.options).lazy()


class ExcelLoader(BaseLoader):
    """Loader for Excel workbooks, backed by the optional `excel` extra.

    Like JSON, this is eager: a spreadsheet is a random-access container with
    no streaming reader.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Read one worksheet.

        Args:
            config (SourceConfig): Source configuration, whose options go to
                `pl.read_excel`, such as `sheet_name`.

        Returns:
            pl.LazyFrame: A lazy wrapper over the rows read.

        Raises:
            ConfigError: If the `excel` extra is missing, or the options select more
                than one worksheet.
        """
        if fastexcel is None:
            raise ConfigError(missing_extra("excel", "Reading an Excel file"))
        loaded = pl.read_excel(  # pyright: ignore[reportUnknownVariableType] - untyped **options
            config.path, **config.options
        )
        if not isinstance(loaded, pl.DataFrame):
            raise ConfigError(
                f"Excel source '{shown_location(config.path)}' resolved to multiple worksheets. "
                "Name exactly one with the 'sheet_name' or 'sheet_id' option."
            )
        return loaded.lazy()


def _describe_source(config: SourceRef) -> str:
    """Name a side by the settings the user wrote, and never by a credential.

    Args:
        config (SourceRef): The side's configuration.

    Returns:
        str: The file and the format it was read as, or the table, query, or
            table URI under the `type` the user wrote.
    """
    if isinstance(config, SourceConfig):
        described = f"`{shown_location(config.path)}` read as {config.format}"
        if "format" in config.model_fields_set:
            return described
        return f"{described} since `format` is not set"
    if isinstance(config, (DeltaLakeConfig, IcebergConfig)):
        return f"the `{config.type}` source `{shown_location(config.table_uri)}`"
    if config.table is not None:
        return f"the `{config.type}` table `{config.table}`"
    return f"the `{config.type}` query"


def describe_sides(source: SourceRef, target: SourceRef) -> dict[str, str]:
    """Name both sides as `_describe_source` does, for an error about their columns."""
    return {"source": _describe_source(source), "target": _describe_source(target)}


_READERS: Final[dict[type[object], Callable[[Any], ReaderConnector]]] = {
    # The lambdas look the connector class up when a source is read, as the
    # warehouse table does, so a test that patches one still intercepts it.
    DeltaLakeConfig: lambda config: DeltaLakeConnector(config),
    IcebergConfig: lambda config: IcebergConnector(config),
    DatabaseConfig: lambda config: DatabaseConnector(config),
    DuckDBConfig: lambda config: DuckDBConnector(config),
}
"""Every source the local engine reads through a connector, keyed by config type."""


class LoaderFactory:
    """Resolve a file, lakehouse, database, or DuckDB `SourceRef` to a LazyFrame.

    A file source goes to the loader for its `format`, and its schema is read
    before it returns, so a file that cannot be read fails here, by name. A
    Delta Lake or Iceberg source returns its connector's lazy scan, and a database or DuckDB source is
    read once through its connector, which then closes. A warehouse source is
    refused: its comparison runs as SQL pushdown through
    `DiffEngine.run_from_configs`.

    Attributes:
        _loaders (ClassVar[dict[str, BaseLoader]]): Format name to loader. It is
            the one list of formats, and `SourceType` names the same set. Error
            messages list the supported formats from it.
    """

    _loaders: ClassVar[dict[str, BaseLoader]] = {
        "csv": CSVLoader(),
        "parquet": ParquetLoader(),
        "json": JSONLoader(),
        "ndjson": NDJSONLoader(),
        "arrow": ArrowLoader(),
        "avro": AvroLoader(),
        "excel": ExcelLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: str) -> BaseLoader:
        """Return the loader for a file format.

        Args:
            source_type (str): Format name, such as `csv` or `parquet`.

        Returns:
            BaseLoader: The loader.

        Raises:
            ConfigError: If the format has no loader.
        """
        loader = cls._loaders.get(source_type)
        if loader is None:
            supported = ", ".join(sorted(cls._loaders))
            raise ConfigError(
                f"Source format '{source_type}' has no loader. Supported formats: {supported}."
            )
        return loader

    @classmethod
    def load(cls, config: SourceRef) -> pl.LazyFrame:
        """Load a file, lakehouse, database, or DuckDB source into a LazyFrame.

        Args:
            config (SourceRef): File, Delta, Iceberg, database, or DuckDB
                configuration.

        Returns:
            pl.LazyFrame: Unevaluated scan graph, or a lazy wrapper over the
                rows a database or DuckDB source read.

        Raises:
            ConnectorError: If `config` is a warehouse source, a file is missing
                or cannot be read, or a lakehouse scan, database read, or DuckDB
                read fails.
            ConfigError: If the file format has no loader, or a database
                `table` names a scheme Veridelta cannot quote for.
        """
        reader = _READERS.get(type(config))
        if reader is not None:
            with reader(config) as connector:
                connector.connect()
                return connector.lazyframe()
        if isinstance(config, SourceConfig):
            loader = cls.get_loader(config.format)
            try:
                frame = loader.load(config)
                # A scan reads nothing until the comparison runs, where a missing file
                # would fail unexplained. Reading the schema opens the file now.
                columns = len(frame.collect_schema())
            except FileNotFoundError as exc:
                raise ConnectorError(
                    f"The {config.format} file '{shown_location(config.path)}' does not exist."
                ) from exc
            except (OSError, pl.exceptions.PolarsError) as exc:
                raise ConnectorError(
                    f"Reading the {config.format} file '{shown_location(config.path)}' failed: "
                    f"{without_location(str(exc), config.path)}"
                ) from exc
            logger.info(
                "Opened the %s file '%s' (%d columns)",
                config.format,
                shown_location(config.path),
                columns,
            )
            return frame
        raise ConnectorError(
            "Warehouse sources cannot be loaded via LoaderFactory; "
            "use DiffEngine.run_from_configs for SQL pushdown."
        )


def schema_frame(config: SourceRef) -> pl.LazyFrame:
    """Read one local side's columns and types, as a frame with no rows."""
    if isinstance(config, DatabaseConfig):
        with DatabaseConnector(config, probe=True) as database:
            database.connect()
            return database.lazyframe()
    if isinstance(config, DuckDBConfig):
        with DuckDBConnector(config, probe=True) as duck:
            duck.connect()
            return duck.lazyframe()
    return pl.LazyFrame(schema=LoaderFactory.load(config).collect_schema())
