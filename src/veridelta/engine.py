# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Load, align, and compare datasets.

Holds the file loaders and the `DiffEngine` that loads, aligns, and compares
the two sides with Polars.
"""

import logging
import math
import re
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from importlib.util import find_spec
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    ClassVar,
    Final,
    NamedTuple,
    TypeAlias,
    TypedDict,
    TypeGuard,
    TypeVar,
    cast,
)
from urllib.parse import urlsplit

import polars as pl

from veridelta.config import load_config
from veridelta.connectors import database as database_connectors
from veridelta.connectors import duckdb as duckdb_connectors
from veridelta.connectors import warehouse as warehouse_connectors
from veridelta.connectors.base import (
    PushdownQueryType,
    PushdownSession,
    ReaderConnector,
    optional_module,
    shown_location,
    without_location,
)
from veridelta.connectors.database import DatabaseConnector, PostgresPushdownSession
from veridelta.connectors.duckdb import DuckDBConnector, DuckDBPushdownSession
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.sql import (
    SAMPLE_BUCKETS,
    VALUE_MAP_AGREEING_ALIAS,
    VALUE_MAP_COLUMN_ALIAS,
    VALUE_MAP_ROWS_ALIAS,
    VALUE_MAP_SOURCE_ALIAS,
    VALUE_MAP_TARGET_ALIAS,
    SampleQuery,
    SQLDialect,
    SQLPushdownCompiler,
    compile_database_select,
)
from veridelta.connectors.warehouse import (
    BigQueryConnector,
    DatabricksConnector,
    SnowflakeConnector,
)
from veridelta.exceptions import ConfigError, ConnectorError, DataIntegrityError, VerideltaError
from veridelta.models import (
    AcceptedChange,
    ArtifactFormat,
    Baseline,
    BigQueryConfig,
    CastTarget,
    ConfigFinding,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    DiffSummary,
    DuckDBConfig,
    IcebergConfig,
    RuleSuggestion,
    SentinelValue,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    SuggestedSetting,
    ValueMapEntry,
    ValueMapProposal,
    WhitespaceMode,
    mismatch_ratio_of,
    normalize_column_name,
)
from veridelta.sentinels import usable_sentinels

logger = logging.getLogger(__name__)


fastexcel = optional_module("fastexcel")
"""Presence probe for the `excel` extra. Polars imports this itself, but only
at call time, so checking here turns a bare ImportError into an install hint."""

rapidfuzz_distance = optional_module("rapidfuzz.distance")
"""Presence probe for the `fuzzy` extra, whose scorers evaluate
`max_levenshtein_distance` and `min_jaro_winkler_similarity` locally."""


class EffectiveRule(TypedDict):
    """Per-column settings after specific, pattern, and global rules are merged."""

    abs_tol: float
    rel_tol: float
    treat_null: bool
    whitespace: WhitespaceMode
    null_values: list[SentinelValue]
    null_values_explicit: bool
    case_insensitive: bool
    regex_replace: dict[str, str] | None
    value_map: dict[str, str] | None
    pad_zeros: int | None
    datetime_format: str | None
    timezone: str | None
    cast_to: CastTarget | None
    ignore: bool
    max_levenshtein_distance: int | None
    min_jaro_winkler_similarity: float | None


def _unusable_sentinel_error(
    column: str, dtype: pl.DataType, sentinels: Sequence[SentinelValue]
) -> ConfigError:
    """Build the error for an explicit rule whose sentinels can never match."""
    return ConfigError(
        f"Column '{column}' has type {dtype}, which cannot hold any of the "
        f"null_values {list(sentinels)!r} configured for it. Quote text sentinels "
        "and leave numbers unquoted so each one matches its column type."
    )


def _duplicate_keys_error(keys: list[str], side: str, count: int) -> DataIntegrityError:
    """Build the error for primary keys that repeat within one dataset."""
    return DataIntegrityError(
        f"Primary keys {keys} are not unique in {side} dataset. "
        f"Found {count} duplicate rows. Clean your data before diffing."
    )


def _reject_unzoned_timezone(column: str, dtype: pl.DataType, zone: str) -> None:
    """Raise unless a column can take a `timezone` rule: a timestamp that carries its zone."""
    if not isinstance(dtype, pl.Datetime):
        raise ConfigError(
            f"Column '{column}' sets timezone='{zone}' but holds {dtype}, not a "
            "timestamp. Parse it with datetime_format first."
        )
    if dtype.time_zone is None:
        raise ConfigError(
            f"Column '{column}' sets timezone='{zone}' but its timestamps are "
            "timezone-naive. Veridelta will not assume an origin zone, because "
            "guessing wrong shifts every value silently. Store the column with a "
            "timezone, or parse it with a datetime_format carrying an offset such as "
            "'%z', or compare it without a timezone rule."
        )


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
            raise ConfigError(
                "Reading Excel requires the optional 'excel' extra. "
                "Install it with: uv add 'veridelta[excel]'"
            )
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


def _describe_sides(source: SourceRef, target: SourceRef) -> dict[str, str]:
    """Name both sides as `_describe_source` does, for an error about their columns."""
    return {"source": _describe_source(source), "target": _describe_source(target)}


def _quoted_list(names: Sequence[str]) -> str:
    """Join names as `'a'`, `'a' and 'b'`, or `'a', 'b', and 'c'`."""
    quoted = [repr(name) for name in names]
    if len(quoted) < 3:
        return " and ".join(quoted)
    return f"{', '.join(quoted[:-1])}, and {quoted[-1]}"


_READERS: Final[dict[type[object], Callable[[Any], ReaderConnector]]] = {
    # The lambdas look the connector class up when a source is read, as the
    # warehouse table below does, so a test that patches one still intercepts it.
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


_T = TypeVar("_T")

_WarehouseConfig: TypeAlias = (
    SnowflakeConfig | DatabricksConfig | BigQueryConfig | DatabaseConfig | DuckDBConfig
)
"""Connection configs whose comparisons compile to SQL and run in place."""


@dataclass(frozen=True)
class _Warehouse:
    """How the engine identifies and opens one warehouse backend."""

    name: str
    dialect: SQLDialect
    session: Callable[[Any], PushdownSession]


_WAREHOUSES: Final[dict[type[object], _Warehouse]] = {
    # The lambdas look the connector class up when a session opens, so a test
    # that patches `veridelta.engine.SnowflakeConnector` still intercepts it.
    SnowflakeConfig: _Warehouse(
        "Snowflake",
        SQLDialect.SNOWFLAKE,
        lambda config: SnowflakeConnector(config),
    ),
    DatabricksConfig: _Warehouse(
        "Databricks",
        SQLDialect.DATABRICKS,
        lambda config: DatabricksConnector(config),
    ),
    BigQueryConfig: _Warehouse(
        "BigQuery",
        SQLDialect.BIGQUERY,
        lambda config: BigQueryConnector(config),
    ),
    # Only a database or DuckDB source that sets `pushdown` is routed here; the
    # database model allows that on a Postgres table alone.
    DatabaseConfig: _Warehouse(
        "Postgres",
        SQLDialect.POSTGRES,
        lambda config: PostgresPushdownSession(config),
    ),
    DuckDBConfig: _Warehouse(
        "DuckDB",
        SQLDialect.DUCKDB,
        lambda config: DuckDBPushdownSession(config),
    ),
}
"""Every warehouse the engine pushes comparisons down to, keyed by config type."""


def _is_warehouse(config: SourceRef) -> TypeGuard[_WarehouseConfig]:
    """Return whether a source reference is a warehouse connection."""
    if isinstance(config, (DatabaseConfig, DuckDBConfig)):
        return config.pushdown
    return type(config) in _WAREHOUSES


def _rename_pairs(rules: Sequence[DiffRule]) -> dict[str, str]:
    """Map each renamed source column to its target spelling."""
    pairs: dict[str, str] = {}
    for rule in rules:
        # An `ignore` rule counts too: its pair tells the target side which column to drop.
        if rule.rename_to and len(rule.column_names) == 1:
            pairs.setdefault(rule.column_names[0], rule.rename_to)
    return pairs


def _rule_spellings(pairs: Mapping[str, str], column: str) -> tuple[str, ...]:
    """List the names a rule may use for a column, in order of precedence."""
    source = next((src for src, tgt in pairs.items() if tgt == column), column)
    if source == column:
        return (column,)
    # In a swap or chain, a rule naming `column` governs the source column renamed away from it.
    if pairs.get(column, column) != column:
        return (source,)
    return (column, source)


def _match_rule(rules: Sequence[DiffRule], column: str) -> DiffRule | None:
    """Resolve the single rule governing a column, exact names before patterns."""
    for name in _rule_spellings(_rename_pairs(rules), column):
        for rule in rules:
            if name in rule.column_names:
                return rule
    for rule in rules:
        if rule.pattern and re.match(rule.pattern, column):
            return rule
    return None


def _alignment_maps(
    rules: list[DiffRule], columns: Sequence[str], *, rename: bool
) -> tuple[dict[str, str], set[str]]:
    """Derive the `rename_to` map and `ignore` drop set for one frame's columns."""
    pairs = _rename_pairs(rules)
    rename_map: dict[str, str] = {}
    to_drop: set[str] = set()
    for column in columns:
        aligned = pairs.get(column, column) if rename else column
        rule = _match_rule(rules, aligned)
        if rule is not None and rule.ignore:
            to_drop.add(column)
        elif aligned != column:
            rename_map[column] = aligned
    return rename_map, to_drop


def _normalize_header_names(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Strip and lowercase every column name, as `normalize_column_names` asks."""
    names = frame.collect_schema().names()
    normalized = [normalize_column_name(name) for name in names]
    collisions = sorted(name for name, count in Counter(normalized).items() if count > 1)
    if collisions:
        raise ConfigError(
            f"normalize_column_names maps more than one header onto {collisions}. "
            "Rename the duplicates at the source, or disable normalize_column_names."
        )
    return frame.rename(dict(zip(names, normalized, strict=True)))


def _fold_rule_defaults(rule: DiffRule | None, diff: DiffConfig) -> EffectiveRule:
    """Layer one matched rule over the configuration's `default_*` settings."""
    effective: EffectiveRule = {
        "abs_tol": diff.default_absolute_tolerance,
        "rel_tol": diff.default_relative_tolerance,
        "treat_null": diff.default_treat_null_as_equal,
        "whitespace": diff.default_whitespace_mode,
        "null_values": diff.default_null_values,
        "null_values_explicit": False,
        "case_insensitive": False,
        "regex_replace": None,
        "value_map": None,
        "pad_zeros": None,
        "datetime_format": None,
        "timezone": None,
        "cast_to": None,
        "ignore": False,
        # No `default_*` counterpart: loosening every text column at once would
        # also forgive identifiers and codes that must match exactly.
        "max_levenshtein_distance": None,
        "min_jaro_winkler_similarity": None,
    }
    if rule is None:
        return effective

    if rule.absolute_tolerance is not None:
        effective["abs_tol"] = rule.absolute_tolerance
    if rule.relative_tolerance is not None:
        effective["rel_tol"] = rule.relative_tolerance
    if rule.treat_null_as_equal is not None:
        effective["treat_null"] = rule.treat_null_as_equal
    if rule.whitespace_mode is not None:
        effective["whitespace"] = rule.whitespace_mode
    if rule.null_values is not None:
        effective["null_values"] = rule.null_values
        effective["null_values_explicit"] = True
    if rule.case_insensitive is not None:
        effective["case_insensitive"] = rule.case_insensitive

    effective["regex_replace"] = rule.regex_replace
    effective["value_map"] = rule.value_map
    effective["pad_zeros"] = rule.pad_zeros
    effective["datetime_format"] = rule.datetime_format
    effective["timezone"] = rule.timezone
    effective["cast_to"] = rule.cast_to
    effective["ignore"] = rule.ignore
    effective["max_levenshtein_distance"] = rule.max_levenshtein_distance
    effective["min_jaro_winkler_similarity"] = rule.min_jaro_winkler_similarity
    return effective


def _enforce_pushdown_preconditions(
    effective: EffectiveRule, sides: tuple[tuple[str, pl.Schema], ...]
) -> None:
    """Fail a pushdown rule that the probed column types can never satisfy."""
    if effective["null_values_explicit"] and effective["null_values"]:
        for name, schema in sides:
            dtype = schema.get(name)
            if dtype is not None and not usable_sentinels(effective["null_values"], dtype):
                raise _unusable_sentinel_error(name, dtype, effective["null_values"])
    if effective["timezone"]:
        for name, schema in sides:
            # The zone rule reads the column after padding and parsing, as locally.
            dtype = _parsed_dtype(effective, schema.get(name))
            # Stage 6b emits no SQL: a zone label cannot change a verdict. Its precondition
            # still holds, so pushdown never compares columns a local run refuses.
            if dtype is not None:
                _reject_unzoned_timezone(name, dtype, effective["timezone"])


_OFFSET_DIRECTIVE: Final = re.compile(r"%%|%[:#]*z")
"""A literal `%%`, or a `%z` offset directive in any of its chrono spellings."""


def _normalized_dtype(effective: EffectiveRule, dtype: pl.DataType | None) -> pl.DataType | None:
    """Predict a column's dtype after stages 1 through 7, from its stored dtype."""
    if effective["cast_to"] is not None:
        return _CAST_TARGETS[effective["cast_to"]]
    dtype = _parsed_dtype(effective, dtype)
    if effective["timezone"] and isinstance(dtype, pl.Datetime):
        dtype = pl.Datetime(dtype.time_unit, effective["timezone"])
    return dtype


def _parsed_dtype(effective: EffectiveRule, dtype: pl.DataType | None) -> pl.DataType | None:
    """Predict a column's dtype after stages 1 through 6a, before timezone and cast."""
    if effective["pad_zeros"] is not None:
        dtype = pl.String()
    fmt = effective["datetime_format"]
    if fmt and isinstance(dtype, pl.String):
        # A `%z` outside a `%%` escape reads a UTC offset, so the result is aware.
        aware = any(token != "%%" for token in _OFFSET_DIRECTIVE.findall(fmt))
        dtype = pl.Datetime("us", "UTC" if aware else None)
    return dtype


_FRACTION_SPELLINGS: Final[dict[str, str]] = {"%%": "%%", ".%f": "%.f", "%f": "%6f"}
"""Polars spellings of Python's fraction directive, plus the escape that hides one."""

_FRACTION_DIRECTIVE: Final = re.compile(r"%%|\.%f|%f")
"""A literal `%%`, which may precede an `f`, or `%f` with or without its dot."""


def _polars_datetime_format(fmt: str) -> str:
    """Spell a Python `strptime` format the way Polars reads it."""
    # Polars' `%f` counts nanoseconds, so a Python fraction such as `.5` would read as 5 ns.
    return _FRACTION_DIRECTIVE.sub(lambda match: _FRACTION_SPELLINGS[match[0]], fmt)


def _tolerance_match(
    src: pl.Expr,
    tgt: pl.Expr,
    rule: EffectiveRule,
    dtype: pl.DataType,
    tgt_dtype: pl.DataType | None,
) -> pl.Expr:
    """Match two numeric values within a rule's absolute and relative tolerance."""
    if dtype.is_integer() and tgt_dtype is not None and tgt_dtype.is_integer():
        src = src.cast(pl.Int128)
        tgt = tgt.cast(pl.Int128)
    # Subtract the smaller value from the larger: `tgt - src` on unsigned
    # columns wraps below zero instead of going negative.
    abs_diff = pl.when(tgt >= src).then(tgt - src).otherwise(src - tgt)
    threshold = rule["abs_tol"] + (rule["rel_tol"] * src.abs())
    within = abs_diff <= threshold
    if dtype.is_float():
        # `0 * inf` is NaN, and Polars sorts NaN above every number, so a
        # non-finite source must never reach the allowance.
        within = within & src.is_finite()
    # Equality matches outright, so NaN meets NaN and an infinity meets itself.
    return (src == tgt) | within


def _pushdown_rule(
    stored: str,
    aligned: str,
    rule: DiffRule | None,
    effective: EffectiveRule,
    *,
    absolute_tolerance: float | None = None,
    relative_tolerance: float | None = None,
    treat_null_as_equal: bool | None = None,
    max_levenshtein_distance: int | None = None,
) -> DiffRule:
    """Re-materialize a folded rule as the fully specified `DiffRule` the compiler reads."""
    return DiffRule(
        column_names=[stored],
        rename_to=None if aligned == stored else aligned,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
        treat_null_as_equal=treat_null_as_equal,
        whitespace_mode=effective["whitespace"],
        null_values=effective["null_values"],
        # No `default_*` counterpart exists, so the compiler sees it as written.
        case_insensitive=rule.case_insensitive if rule is not None else None,
        regex_replace=effective["regex_replace"],
        value_map=effective["value_map"],
        pad_zeros=effective["pad_zeros"],
        datetime_format=effective["datetime_format"],
        timezone=effective["timezone"],
        cast_to=effective["cast_to"],
        # No Jaro-Winkler counterpart: the resolver refuses that limit, since
        # no warehouse can reproduce it.
        max_levenshtein_distance=max_levenshtein_distance,
    )


def _resolve_pushdown_keys(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> list[DiffRule]:
    """Expand configuration into the normalization each primary key receives."""
    source_names = set(source_schema.names())
    pairs = _rename_pairs(diff.rules)

    resolved: list[DiffRule] = []
    for key in diff.primary_keys:
        # Schema validation has already proven the key survives alignment, so
        # either a present column is renamed onto it or it is stored as is.
        stored = next(
            (src for src, tgt in pairs.items() if tgt == key and src in source_names), key
        )
        rule = _match_rule(diff.rules, key)
        effective = _fold_rule_defaults(rule, diff)
        _enforce_pushdown_preconditions(effective, ((stored, source_schema), (key, target_schema)))
        resolved.append(_pushdown_rule(stored, key, rule, effective))
    return resolved


def _pushdown_columns(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> Iterator[tuple[str, str, DiffRule | None, EffectiveRule]]:
    """Walk the probed source columns a warehouse statement compares."""
    target_lookup = set(target_schema.names())
    keys = set(diff.primary_keys)
    pairs = _rename_pairs(diff.rules)
    for column in source_schema.names():
        aligned = pairs.get(column, column)
        if aligned in keys or aligned not in target_lookup:
            continue
        rule = _match_rule(diff.rules, aligned)
        if rule is not None and rule.ignore:
            continue
        effective = _fold_rule_defaults(rule, diff)
        _enforce_pushdown_preconditions(
            effective, ((column, source_schema), (aligned, target_schema))
        )
        yield column, aligned, rule, effective


def _resolve_pushdown_rules(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> list[DiffRule]:
    """Expand configuration into one fully specified rule per compared column."""
    resolved: list[DiffRule] = []
    for column, aligned, rule, effective in _pushdown_columns(diff, source_schema, target_schema):
        # As in `_build_match_expr`, a tolerance loosens only a column compared as
        # a number, and a similarity limit only one compared as text. An unknown
        # type keeps both, unless `datetime_format` would parse the text.
        normalized = _normalized_dtype(effective, source_schema.get(column))
        numeric = normalized is None or normalized.is_numeric()
        if normalized is None:
            text = not effective["datetime_format"]
        else:
            text = isinstance(normalized, pl.String)
        if text and effective["min_jaro_winkler_similarity"] is not None:
            raise ConfigError(
                f"Column '{aligned}' sets min_jaro_winkler_similarity, which warehouse "
                "pushdown cannot evaluate the way a local run does: Snowflake's "
                "JAROWINKLER_SIMILARITY ignores case and returns a whole number from 0 to "
                "100, and Databricks has no Jaro-Winkler function. Use "
                "max_levenshtein_distance, which compiles to SQL, or compare file or "
                "lakehouse copies of these tables locally."
            )

        resolved.append(
            _pushdown_rule(
                column,
                aligned,
                rule,
                effective,
                absolute_tolerance=effective["abs_tol"] if numeric else 0.0,
                relative_tolerance=effective["rel_tol"] if numeric else 0.0,
                treat_null_as_equal=effective["treat_null"],
                max_levenshtein_distance=effective["max_levenshtein_distance"] if text else None,
            )
        )
    return resolved


def _compared_dtypes(
    diff: DiffConfig, rule: DiffRule, source_schema: pl.Schema, target_schema: pl.Schema
) -> tuple[pl.DataType | None, pl.DataType | None]:
    """Predict the dtypes a local run would compare one pushdown column as."""
    effective = _fold_rule_defaults(rule, diff)
    stored = rule.column_names[0]
    return (
        _normalized_dtype(effective, source_schema.get(stored)),
        _normalized_dtype(effective, target_schema.get(rule.rename_to or stored)),
    )


def _wide_integer_columns(
    diff: DiffConfig, rules: Sequence[DiffRule], source_schema: pl.Schema, target_schema: pl.Schema
) -> frozenset[str]:
    """Name the tolerance columns a warehouse must subtract in a wider integer type."""
    return frozenset(
        rule.rename_to or rule.column_names[0]
        for rule in rules
        if (rule.absolute_tolerance or rule.relative_tolerance)
        and all(
            dtype is not None and dtype.is_integer()
            for dtype in _compared_dtypes(diff, rule, source_schema, target_schema)
        )
    )


def _type_drift_columns(
    diff: DiffConfig, rules: Sequence[DiffRule], source_schema: pl.Schema, target_schema: pl.Schema
) -> frozenset[str]:
    """Name the columns `strict_types` fails because their two sides differ in type."""
    if not diff.strict_types:
        return frozenset()
    drift: set[str] = set()
    for rule in rules:
        source, target = _compared_dtypes(diff, rule, source_schema, target_schema)
        if source is not None and target is not None and source != target:
            drift.add(rule.rename_to or rule.column_names[0])
    return frozenset(drift)


def _column_mismatches_from_frame(frame: pl.DataFrame) -> dict[str, int]:
    """Reduce the single-row mismatch tally to positive per-column counts."""
    if frame.height != 1:
        raise ConnectorError("Column mismatch query did not return exactly one row.")

    counts: dict[str, int] = {}
    for column, value in frame.row(0, named=True).items():
        # SUM over an empty join returns NULL rather than zero.
        if value is None:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError) as exc:
            raise ConnectorError(f"Column mismatch count for '{column}' was not numeric.") from exc
        if count > 0:
            counts[column] = count
    return counts


def _local_column_mismatches(changed: pl.DataFrame, compared_columns: list[str]) -> dict[str, int]:
    """Count, per compared column, how many changed rows failed its match flag."""
    if not compared_columns or changed.is_empty():
        return {}
    tally = changed.select(
        [(~pl.col(f"{col}_is_match")).sum().alias(col) for col in compared_columns]
    ).row(0, named=True)
    return {column: count for column, count in tally.items() if count > 0}


_CAST_TARGETS: Final[dict[CastTarget, pl.DataType]] = {
    "Int64": pl.Int64(),
    "Float64": pl.Float64(),
    "String": pl.String(),
    "Boolean": pl.Boolean(),
    "Date": pl.Date(),
    "Datetime": pl.Datetime(),
}
"""`cast_to` name to the dtype it resolves to."""

_UNCASTABLE: Final[dict[type[pl.DataType], frozenset[CastTarget]]] = {
    pl.Binary: frozenset({"Int64", "Float64", "Boolean", "Date", "Datetime"}),
    pl.String: frozenset({"Boolean"}),
    pl.Categorical: frozenset({"Float64", "Boolean", "Date", "Datetime"}),
    pl.Decimal: frozenset({"Date", "Datetime"}),
    pl.Date: frozenset({"Boolean"}),
    pl.Datetime: frozenset({"Boolean"}),
    pl.Time: frozenset({"Boolean", "Date", "Datetime"}),
    pl.Duration: frozenset({"String", "Boolean", "Date", "Datetime"}),
    pl.List: frozenset(_CAST_TARGETS),
}
"""`cast_to` targets Polars refuses for each column type, whatever its values.

Polars refuses them only once a value reaches the cast, which in a run is
mid-comparison. Checking the type first turns that into a `ConfigError` before
any row is read, in a run and in `validate --schemas` alike. A test holds the
table to what Polars does.
"""


def _fuzzy_measures() -> ModuleType:
    """Return rapidfuzz's distance module, or explain how to install it."""
    if rapidfuzz_distance is None:
        raise ConfigError(
            "max_levenshtein_distance and min_jaro_winkler_similarity need the optional "
            "'fuzzy' extra to compare text locally. Install it with: uv add 'veridelta[fuzzy]'"
        )
    return rapidfuzz_distance


def _similarity_test(rule: EffectiveRule) -> Callable[[str, str], bool] | None:
    """Build the test a differing text pair must pass to match at stage 8."""
    limit = rule["max_levenshtein_distance"]
    if limit is not None:
        distance = _fuzzy_measures().Levenshtein.distance
        # Past `score_cutoff` rapidfuzz stops counting and reports limit + 1.
        return lambda left, right: bool(distance(left, right, score_cutoff=limit) <= limit)
    floor = rule["min_jaro_winkler_similarity"]
    if floor is not None:
        similarity = _fuzzy_measures().JaroWinkler.similarity
        # Below `score_cutoff` rapidfuzz reports a similarity of 0.
        return lambda left, right: bool(similarity(left, right, score_cutoff=floor) >= floor)
    return None


def _null_equality(match: pl.Expr, src: pl.Expr, tgt: pl.Expr, rule: EffectiveRule) -> pl.Expr:
    """Apply stage 9: two nulls match under `treat_null`, and any other null does not."""
    if rule["treat_null"]:
        return (match | (src.is_null() & tgt.is_null())).fill_null(False)
    return match.fill_null(False)


def _score_differing_pairs(pairs: pl.Series, *, test: Callable[[str, str], bool]) -> pl.Series:
    """Mark the pairs that still differ after normalization but pass `test`."""
    differing = (
        pairs.struct.unnest()
        .with_row_index("row")
        .filter((pl.col("source") != pl.col("target")).fill_null(False))
    )
    hits = [row for row, source, target in differing.iter_rows() if test(source, target)]
    return pl.repeat(False, len(pairs), dtype=pl.Boolean, eager=True).scatter(hits, True)


def _similarity_expr(src: pl.Expr, tgt: pl.Expr, test: Callable[[str, str], bool]) -> pl.Expr:
    """Evaluate a similarity test over two aligned text columns, lazily."""
    return pl.struct(src.alias("source"), tgt.alias("target")).map_batches(
        partial(_score_differing_pairs, test=test),
        return_dtype=pl.Boolean,
        is_elementwise=True,
    )


_ARTIFACT_WRITERS: Final[dict[ArtifactFormat, Callable[[pl.DataFrame, Path], None]]] = {
    "csv": lambda frame, path: frame.write_csv(path),
    "parquet": lambda frame, path: frame.write_parquet(path),
    "json": lambda frame, path: frame.write_json(path),
    "ndjson": lambda frame, path: frame.write_ndjson(path),
    "arrow": lambda frame, path: frame.write_ipc(path),
}
"""Artifact format to writer."""


_TEXT_FORMATS: Final[frozenset[ArtifactFormat]] = frozenset({"csv", "json", "ndjson"})
"""Artifact formats with no type for bytes."""


def _holds_binary(dtype: object) -> bool:
    """Return whether a type is binary, or nests a binary type anywhere inside it.

    A nested type names its members as instances or as bare classes, so both count.
    """
    if dtype == pl.Binary:
        return True
    if isinstance(dtype, (pl.List, pl.Array)):
        return _holds_binary(dtype.inner)
    if isinstance(dtype, pl.Struct):
        return any(_holds_binary(field.dtype) for field in dtype.fields)
    return False


def _writable(frame: pl.DataFrame, output_format: ArtifactFormat) -> pl.DataFrame:
    """Write each binary column as hexadecimal text in a format that has no bytes type.

    Polars refuses a binary column in CSV and panics on one in JSON, a panic no
    `except Exception` catches, so the bytes become text first.

    Raises:
        ConfigError: If a column nests binary values in a list or a struct,
            which a text format cannot hold.
    """
    if output_format not in _TEXT_FORMATS:
        return frame
    binary: list[str] = []
    for name, dtype in frame.schema.items():
        if isinstance(dtype, pl.Binary):
            binary.append(name)
        elif _holds_binary(dtype):
            raise ConfigError(
                f"Column '{name}' nests binary values, which output_format '{output_format}' "
                "cannot hold. Set output_format to parquet or arrow."
            )
    return frame.with_columns(pl.col(binary).bin.encode("hex")) if binary else frame


def _export_artifacts(
    frames: dict[str, pl.DataFrame], output_path: str | None, output_format: ArtifactFormat
) -> bool:
    """Persist non-empty discrepancy frames to the configured directory."""
    if output_path is None:
        return False
    # Checked before the loop so a misconfigured format fails the same way on a
    # clean run as on a drifted one, rather than only when a frame reaches disk.
    if output_format not in _ARTIFACT_WRITERS:
        supported = ", ".join(sorted(_ARTIFACT_WRITERS))
        raise ConfigError(
            f"Artifact format '{output_format}' has no writer. Supported formats: {supported}."
        )

    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    written = False
    for name, frame in frames.items():
        if frame.height == 0:
            continue
        _ARTIFACT_WRITERS[output_format](
            _writable(frame, output_format), out_dir / f"{name}.{output_format}"
        )
        written = True
    return written


def _summary(
    diff: DiffConfig,
    changed: pl.DataFrame,
    added: pl.DataFrame,
    removed: pl.DataFrame,
    source_total: int,
    target_total: int,
    column_mismatches: dict[str, int],
    artifacts_written: bool,
    accepted_count: int = 0,
) -> DiffSummary:
    """Count a comparison's discrepancies and apply the threshold, for either engine."""
    changed_count = changed.height
    added_count = added.height
    removed_count = removed.height
    total_mismatches = added_count + removed_count + changed_count
    return DiffSummary(
        total_rows_source=source_total,
        total_rows_target=target_total,
        added_count=added_count,
        removed_count=removed_count,
        changed_count=changed_count,
        column_mismatches=column_mismatches,
        is_match=mismatch_ratio_of(total_mismatches, source_total) <= diff.threshold,
        accepted_count=accepted_count,
        report_limit=diff.report_top_columns_limit,
        artifacts_written=artifacts_written,
    )


def _baseline_keys(
    keys: Sequence[Mapping[str, Any]], names: list[str], schema: pl.Schema
) -> pl.DataFrame:
    """Build a baseline's keys as a frame, typed as the result's key columns.

    JSON has no type for a date or a timestamp, so a key read as text casts to the
    column's type, and one that cannot cast matches no row.
    """
    frame = pl.DataFrame({name: [key[name] for key in keys] for name in names}, strict=False)
    return frame.select(pl.col(name).cast(schema[name], strict=False) for name in names)


def _unaccepted_rows(
    frame: pl.DataFrame, keys: Sequence[Mapping[str, Any]], names: list[str]
) -> pl.DataFrame:
    """Drop the rows of an added or removed frame whose key a baseline lists."""
    if not keys or frame.is_empty():
        return frame
    accepted = _baseline_keys(keys, names, frame.schema)
    return frame.join(accepted, on=names, how="anti", nulls_equal=True)


def _unaccepted_changes(
    changed: pl.DataFrame,
    entries: Sequence[AcceptedChange],
    names: list[str],
    compared_columns: list[str],
) -> pl.DataFrame:
    """Mark accepted columns as matching on their rows, and drop rows left matching."""
    if not entries or changed.is_empty():
        return changed
    accepted = (
        _baseline_keys([entry.key for entry in entries], names, changed.schema)
        .with_columns(
            pl.Series(
                _ACCEPTED_COLUMNS, [entry.columns for entry in entries], dtype=pl.List(pl.String)
            )
        )
        # A key listed twice keeps one row, with every column either entry names.
        .group_by(names)
        .agg(pl.col(_ACCEPTED_COLUMNS).list.explode(keep_nulls=False, empty_as_null=False))
    )
    flags = [f"{column}_is_match" for column in compared_columns]
    return (
        changed.join(accepted, on=names, how="left", nulls_equal=True)
        .with_columns(
            (
                pl.col(flag)
                | pl.col(_ACCEPTED_COLUMNS).list.contains(column).fill_null(value=False)
            ).alias(flag)
            for column, flag in zip(compared_columns, flags, strict=True)
        )
        .drop(_ACCEPTED_COLUMNS)
        .filter(~pl.all_horizontal(flags))
    )


_ACCEPTED_COLUMNS: Final = "__veridelta_accepted_columns"
"""A working column for the columns a baseline accepts on each changed row."""


def _accept_baseline(
    baseline: Baseline,
    names: list[str],
    frames: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame],
    compared_columns: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, int]:
    """Leave the drift a baseline lists out of the added, removed, and changed rows.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, int]: The rows left, and
            how many rows the baseline accepted.

    Raises:
        ConfigError: If the baseline names other primary keys than the run.
    """
    if baseline.primary_keys != names:
        raise ConfigError(
            f"The baseline lists rows by the primary keys {baseline.primary_keys}, and "
            f"this configuration compares on {names}. Save the baseline again from a run "
            "of this configuration."
        )
    added, removed, changed = frames
    kept = (
        _unaccepted_rows(added, baseline.added, names),
        _unaccepted_rows(removed, baseline.removed, names),
        _unaccepted_changes(changed, baseline.changed, names, compared_columns),
    )
    accepted = sum(before.height - after.height for before, after in zip(frames, kept, strict=True))
    return (*kept, accepted)


def _pushdown_scalar(
    connector: PushdownSession,
    statement: str,
    query_type: PushdownQueryType,
    label: str,
) -> int:
    """Collect the single integer an aggregate pushdown statement returns."""
    frame = connector.execute_pushdown(statement, query_type=query_type).collect()
    if frame.height != 1 or frame.width != 1:
        raise ConnectorError(f"{label} did not return a single value.")
    try:
        # Drivers may surface COUNT(*) or SUM as an integer, float, or decimal scalar.
        return int(frame.item())
    except (TypeError, ValueError) as exc:
        raise ConnectorError(f"{label} returned a non-numeric value.") from exc


def _reject_duplicate_pushdown_keys(
    connector: PushdownSession,
    diff: DiffConfig,
    key_rules: Sequence[DiffRule],
    tables: tuple[str, str],
    schemas: tuple[pl.Schema, pl.Schema],
) -> None:
    """Fail a pair whose normalized primary keys repeat on either side, as a local run does."""
    for table, types, side in zip(tables, schemas, ("SOURCE", "TARGET"), strict=True):
        statement = connector.compiler.compile_duplicate_key_query(
            table, diff.primary_keys, is_source=side == "SOURCE", key_rules=key_rules, types=types
        )
        duplicates = _pushdown_scalar(
            connector, statement, "duplicates", f"Duplicate key query for '{table}'"
        )
        if duplicates:
            raise _duplicate_keys_error(diff.primary_keys, side, duplicates)


def _reject_warehouse_header_normalization(diff: DiffConfig, *schemas: pl.Schema) -> None:
    """Refuse `normalize_column_names` where it would rename a warehouse column."""
    if not diff.normalize_column_names:
        return
    changed = [
        name for schema in schemas for name in schema.names() if name != normalize_column_name(name)
    ]
    if changed:
        raise ConfigError(
            f"normalize_column_names would rename the warehouse columns {changed}. "
            "Pushdown quotes identifiers exactly as they are stored, so disable "
            "normalize_column_names and write column names in their stored case."
        )


def _probe_relation(connector: PushdownSession, table: str) -> tuple[pl.LazyFrame, pl.Schema]:
    """Read a relation's columns with a zero-row probe, as a warehouse run starts.

    Returns the probe, which `validate_schemas` reads, and the columns with the
    types the warehouse declares.
    """
    probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(table), query_type="schema"
    )
    schema = probe.collect_schema()
    if isinstance(connector, database_connectors.PostgresPushdownSession):
        # The probe reads every numeric as Decimal(38, 10); the catalog has the declared type.
        schema = _with_declared_types(schema, connector.declared_types(table))
    return probe, schema


def _validate_pushdown_schema(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> tuple[pl.Schema, pl.Schema]:
    """Enforce `schema_mode` against warehouse relations before comparing them."""
    source_probe, source_schema = _probe_relation(connector, source_table)
    target_probe, target_schema = _probe_relation(connector, target_table)
    _reject_warehouse_header_normalization(diff, source_schema, target_schema)
    DiffEngine.validate_schemas(diff, source_probe, target_probe)
    return source_schema, target_schema


def _with_declared_types(schema: pl.Schema, declared: Mapping[str, pl.DataType]) -> pl.Schema:
    """Replace probed column types with the types the catalog declares."""
    return pl.Schema({name: declared.get(name, dtype) for name, dtype in schema.items()})


class _PushdownPlan(NamedTuple):
    """What a warehouse run learns before it reads a row."""

    source_schema: pl.Schema
    target_schema: pl.Schema
    key_rules: list[DiffRule]
    rules: list[DiffRule]


def _plan_pushdown(
    connector: PushdownSession, source_table: str, target_table: str, diff: DiffConfig
) -> _PushdownPlan:
    """Probe both relations, then resolve the keys and rules against them."""
    source_schema, target_schema = _validate_pushdown_schema(
        connector, source_table, target_table, diff
    )
    return _PushdownPlan(
        source_schema,
        target_schema,
        _resolve_pushdown_keys(diff, source_schema, target_schema),
        _resolve_pushdown_rules(diff, source_schema, target_schema),
    )


def _check_pushdown_plan(
    connector: PushdownSession, source_table: str, target_table: str, diff: DiffConfig
) -> None:
    """Do what a warehouse run does before reading a row, and compile the rest."""
    plan = _plan_pushdown(connector, source_table, target_table, diff)
    compiler = connector.compiler
    keys = diff.primary_keys
    for table, schema, is_source in (
        (source_table, plan.source_schema, True),
        (target_table, plan.target_schema, False),
    ):
        compiler.compile_duplicate_key_query(
            table, keys, is_source=is_source, key_rules=plan.key_rules, types=schema
        )
    wide_integers = _wide_integer_columns(diff, plan.rules, plan.source_schema, plan.target_schema)
    type_drift = _type_drift_columns(diff, plan.rules, plan.source_schema, plan.target_schema)
    for compile_rows in (compiler.compile_query, compiler.compile_column_mismatch_query):
        compile_rows(
            source_table,
            target_table,
            keys,
            plan.rules,
            source_types=plan.source_schema,
            target_types=plan.target_schema,
            key_rules=plan.key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )
    for compile_keys in (compiler.compile_added_query, compiler.compile_missing_query):
        compile_keys(
            source_table,
            target_table,
            keys,
            source_types=plan.source_schema,
            target_types=plan.target_schema,
            key_rules=plan.key_rules,
        )
    if diff.pushdown_sample_rows:
        compiler.compile_changed_sample_query(
            source_table,
            target_table,
            keys,
            plan.rules,
            limit=diff.pushdown_sample_rows,
            source_types=plan.source_schema,
            target_types=plan.target_schema,
            key_rules=plan.key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )


def _collect_pushdown_summary(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> DiffResult:
    """Compile and collect the warehouse key checks, counts, mismatches, and anti-joins."""
    plan = _plan_pushdown(connector, source_table, target_table, diff)
    source_schema, target_schema, key_rules, rules = plan
    # As in a local run, a ConfigError from rule resolution wins over repeated
    # keys, and repeated keys stop the run before any count or join executes.
    _reject_duplicate_pushdown_keys(
        connector, diff, key_rules, (source_table, target_table), (source_schema, target_schema)
    )

    source_total, target_total = (
        _pushdown_scalar(
            connector,
            connector.compiler.compile_count_query(table),
            "count",
            f"Row count query for '{table}'",
        )
        for table in (source_table, target_table)
    )
    # Every join reads normalized keys, so a key the rules transform matches
    # across the two relations exactly where a local run would match it.
    wide_integers = _wide_integer_columns(diff, rules, source_schema, target_schema)
    type_drift = _type_drift_columns(diff, rules, source_schema, target_schema)
    mismatch_sql = connector.compiler.compile_query(
        source_table,
        target_table,
        diff.primary_keys,
        rules,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
        wide_integers=wide_integers,
        type_drift=type_drift,
    )
    added_sql = connector.compiler.compile_added_query(
        source_table,
        target_table,
        diff.primary_keys,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
    )
    missing_sql = connector.compiler.compile_missing_query(
        source_table,
        target_table,
        diff.primary_keys,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
    )
    changed = connector.execute_pushdown(mismatch_sql, query_type="mismatch").collect()
    added = connector.execute_pushdown(added_sql, query_type="added").collect()
    removed = connector.execute_pushdown(missing_sql, query_type="missing").collect()

    column_mismatches: dict[str, int] = {}
    columns_sql = connector.compiler.compile_column_mismatch_query(
        source_table,
        target_table,
        diff.primary_keys,
        rules,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
        wide_integers=wide_integers,
        type_drift=type_drift,
    )
    if columns_sql is not None:
        tally = connector.execute_pushdown(columns_sql, query_type="columns").collect()
        column_mismatches = _column_mismatches_from_frame(tally)
    changed_sample = _collect_changed_sample(
        connector,
        source_table,
        target_table,
        diff,
        plan,
        changed,
        wide_integers=wide_integers,
        type_drift=type_drift,
    )

    # Pushdown projects primary keys only, never full rows, so the suffix keeps
    # these files from being mistaken for local artifacts.
    frames = {
        "added_rows_pks_only": added,
        "removed_rows_pks_only": removed,
        "changed_rows_pks_only": changed,
    }
    if changed_sample is not None:
        # A sample holds values, so its own name sets it apart from the keys.
        frames["changed_rows_sample"] = changed_sample
    artifacts_written = _export_artifacts(frames, diff.output_path, diff.output_format)

    return DiffResult(
        summary=_summary(
            diff,
            changed,
            added,
            removed,
            source_total,
            target_total,
            column_mismatches,
            artifacts_written,
        ),
        added=added,
        removed=removed,
        changed=changed,
        primary_keys=tuple(diff.primary_keys),
        # Post-rename names, matching what the local path records, so the same
        # column reads the same way whichever engine ran it.
        compared_columns=tuple(rule.rename_to or rule.column_names[0] for rule in rules),
        keys_only=True,
        changed_sample=changed_sample,
    )


def _collect_changed_sample(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
    plan: _PushdownPlan,
    changed: pl.DataFrame,
    *,
    wide_integers: frozenset[str],
    type_drift: frozenset[str],
) -> pl.DataFrame | None:
    """Fetch up to `pushdown_sample_rows` changed rows with both sides' values."""
    if diff.pushdown_sample_rows == 0 or changed.is_empty():
        return None
    # A changed row means a column was compared, so the compiler returns a query.
    sample = cast(
        "SampleQuery",
        connector.compiler.compile_changed_sample_query(
            source_table,
            target_table,
            diff.primary_keys,
            plan.rules,
            limit=diff.pushdown_sample_rows,
            source_types=plan.source_schema,
            target_types=plan.target_schema,
            key_rules=plan.key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        ),
    )
    frame = connector.execute_pushdown(sample.statement, query_type="samples").collect()
    missing = [alias for alias in sample.renames if alias not in frame.columns]
    if missing:
        raise ConnectorError(f"The warehouse returned a row sample without {', '.join(missing)}.")
    return frame.select(list(sample.renames)).rename(sample.renames)


_MIXED_BACKENDS: Final = "Mixed file/lakehouse/database and warehouse backends are unsupported."

_HALF_PUSHDOWN: Final = (
    "Set pushdown on both sides to compare the tables where they are stored, "
    "or on neither to read them and compare locally."
)


def _table_name(config: _WarehouseConfig) -> str:
    """Return the table a pushdown side names."""
    # `DatabaseConfig` and `DuckDBConfig` require a `table` whenever they set `pushdown`.
    return cast("str", config.table)


@dataclass(frozen=True)
class _WarehousePair:
    """Two warehouse tables cleared to share one pushdown session."""

    warehouse: _Warehouse
    source: _WarehouseConfig
    target: _WarehouseConfig

    def with_session(self, work: Callable[[PushdownSession, str, str], _T]) -> _T:
        """Open one session, run `work` on it, and close it whatever happens."""
        with _warehouse_session(self.source) as session:
            return work(session, _table_name(self.source), _table_name(self.target))


@contextmanager
def _warehouse_session(config: SourceRef) -> Generator[PushdownSession]:
    """Connect to the warehouse one side names, and close the session whatever happens."""
    session = _WAREHOUSES[type(config)].session(config)
    session.connect()
    try:
        yield session
    finally:
        session.close()


def _check_backend_pairing(source: SourceRef, target: SourceRef) -> _WarehousePair | None:
    """Refuse a pair no engine can compare, without connecting to anything."""
    # Two sides of one kind disagree only when a database or DuckDB pair half sets `pushdown`.
    if type(source) is type(target) and _is_warehouse(source) != _is_warehouse(target):
        raise ConfigError(_HALF_PUSHDOWN)
    if not _is_warehouse(source):
        if _is_warehouse(target):
            raise ConnectorError(_MIXED_BACKENDS)
        return None
    if not _is_warehouse(target):
        raise ConnectorError(_MIXED_BACKENDS)
    warehouse = _WAREHOUSES[type(source)]
    target_warehouse = _WAREHOUSES[type(target)]
    if target_warehouse is not warehouse:
        raise ConnectorError(
            "Cross-dialect warehouse pushdown is unsupported: the source is "
            f"{warehouse.name} and the target is {target_warehouse.name}. Source and "
            "target must use the same warehouse connection."
        )
    # Compared only for equality: the dumps hold passwords and tokens.
    if source.model_dump(exclude={"table"}) != target.model_dump(exclude={"table"}):
        raise ConnectorError(
            "Cross-account warehouse pushdown is unsupported. "
            f"Source and target {warehouse.name} connections must match."
        )
    # One connection by now, so one name is one table, and it would always match.
    if source.table == target.table:
        raise ConfigError(
            f"Source and target both name the same table '{source.table}' on one "
            "connection, so the comparison could only ever match. Point one side "
            "at the table it should be compared with."
        )
    return _WarehousePair(warehouse, source, target)


_EXTRA_PROBES: Final[dict[type[object], tuple[str, Callable[[], bool]]]] = {
    # Each probe reads its module attribute when called, so tests can patch it,
    # and a lakehouse reader is looked up by name rather than imported.
    DeltaLakeConfig: ("delta", lambda: _findable("deltalake")),
    IcebergConfig: ("iceberg", lambda: _findable("pyiceberg")),
    DatabaseConfig: ("database", lambda: database_connectors.connectorx is not None),
    DuckDBConfig: ("duckdb", lambda: duckdb_connectors.duckdb is not None),
    SnowflakeConfig: ("snowflake", lambda: warehouse_connectors.snowflake_connector is not None),
    DatabricksConfig: ("databricks", lambda: warehouse_connectors.databricks_sql is not None),
    # The BigQuery client is imported only on connect, so it is found by name.
    BigQueryConfig: ("bigquery", lambda: _findable("google.cloud.bigquery")),
}
"""The optional extra each connection type reads through, and whether it is installed."""


def _findable(module: str) -> bool:
    """Return whether a dotted module could be imported, without importing it."""
    # `find_spec` imports a dotted name's parents, and raises when one is not installed.
    try:
        return find_spec(module) is not None
    except ModuleNotFoundError:
        return False


def _required_extra(config: SourceRef) -> tuple[str, Callable[[], bool]] | None:
    """Name the optional extra one side reads through, with its probe."""
    if isinstance(config, SourceConfig):
        if config.format == "excel":
            return "excel", lambda: fastexcel is not None
        return None
    return _EXTRA_PROBES[type(config)]


def _error(message: str) -> ConfigFinding:
    """Build a finding that would stop a run."""
    return ConfigFinding(severity="error", message=message)


def _warning(message: str) -> ConfigFinding:
    """Build a finding that stops a run only for some stored names or types."""
    return ConfigFinding(severity="warning", message=message)


def _missing_extra_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report each optional extra a side reads through that is not installed."""
    sides: dict[str, list[str]] = {}
    for label, config in (("source", source), ("target", target)):
        required = _required_extra(config)
        if required is not None and not required[1]():
            sides.setdefault(required[0], []).append(label)
    return [
        _error(
            f"Reading the {' and '.join(labels)} needs the optional '{extra}' extra, which "
            f"is not installed. Install it with: uv add 'veridelta[{extra}]'"
        )
        for extra, labels in sides.items()
    ]


def _database_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report a database `table` whose URI scheme Veridelta cannot quote for."""
    findings: list[ConfigFinding] = []
    for label, config in (("source", source), ("target", target)):
        if isinstance(config, DatabaseConfig) and config.table is not None:
            try:
                compile_database_select(urlsplit(config.uri).scheme.lower(), config.table)
            except (ConfigError, ConnectorError) as exc:
                findings.append(_error(f"{label}: {exc}"))
    return findings


def _polars_regex_error(pattern: str, replacement: str) -> str | None:
    """Return why Polars' regular expression engine rejects a pattern, if it does."""
    try:
        pl.select(pl.lit("").str.replace_all(pattern, replacement))
    except pl.exceptions.PolarsError as exc:
        detail = str(exc)
        return next(
            (
                line.removeprefix("error: ")
                for line in detail.splitlines()
                if line.startswith("error: ")
            ),
            detail.strip(),
        )
    return None


def _regex_findings(diff: DiffConfig, *, pushdown: bool) -> list[ConfigFinding]:
    """Report `regex_replace` patterns that Polars' regular expression engine rejects."""
    # The models compile each pattern with Python's `re`, which accepts look-around
    # and backreferences that Polars rejects.
    findings: list[ConfigFinding] = []
    for index, rule in enumerate(diff.rules):
        for pattern, replacement in (rule.regex_replace or {}).items():
            reason = _polars_regex_error(pattern, replacement)
            if reason is None:
                continue
            message = (
                f"rules[{index}] regex_replace pattern {pattern!r} does not compile in "
                f"Polars ({reason})."
            )
            if pushdown:
                findings.append(
                    _warning(
                        f"{message} A warehouse run hands it to the warehouse's own regular "
                        "expression engine, which may accept it, but a local run would fail."
                    )
                )
            else:
                findings.append(_error(message))
    return findings


def _fuzzy_extra_findings(diff: DiffConfig) -> list[ConfigFinding]:
    """Report similarity rules a local run cannot score without the `fuzzy` extra."""
    if rapidfuzz_distance is not None:
        return []
    return [
        _error(
            f"rules[{index}] sets a similarity limit, which a local run scores with the "
            "optional 'fuzzy' extra, and it is not installed. Install it with: "
            "uv add 'veridelta[fuzzy]'"
        )
        for index, rule in enumerate(diff.rules)
        if rule.max_levenshtein_distance is not None or rule.min_jaro_winkler_similarity is not None
    ]


def _pushdown_findings(diff: DiffConfig, pair: _WarehousePair) -> list[ConfigFinding]:
    """Report settings a warehouse run refuses for some stored names or types."""
    name = pair.warehouse.name
    compiler = SQLPushdownCompiler(pair.warehouse.dialect)
    findings: list[ConfigFinding] = []
    if diff.normalize_column_names:
        findings.append(
            _warning(
                f"normalize_column_names is on. A {name} run refuses it if any stored column "
                "name has uppercase letters or surrounding spaces, because pushdown quotes "
                "names exactly as they are stored."
            )
        )
    for index, rule in enumerate(diff.rules):
        if rule.min_jaro_winkler_similarity is not None:
            findings.append(
                _warning(
                    f"rules[{index}] sets min_jaro_winkler_similarity, which a {name} run "
                    "refuses on any column it compares as text. Use max_levenshtein_distance, "
                    "which compiles to SQL, or compare local copies of the tables."
                )
            )
        for setting, probe in (
            (
                "datetime_format",
                DiffRule(column_names=["probe"], datetime_format=rule.datetime_format),
            ),
            ("regex_replace", DiffRule(column_names=["probe"], regex_replace=rule.regex_replace)),
            (
                "max_levenshtein_distance",
                DiffRule(
                    column_names=["probe"], max_levenshtein_distance=rule.max_levenshtein_distance
                ),
            ),
        ):
            try:
                compiler.compile_column_predicate(
                    probe, "probe", source_dtype=pl.String(), target_dtype=pl.String()
                )
            except ConfigError as exc:
                findings.append(
                    _warning(
                        f"rules[{index}] {setting} has no {name} spelling, so a run "
                        f"refuses it on any column stored as text: {exc}"
                    )
                )
    return findings


def _schema_frame(config: SourceRef) -> pl.LazyFrame:
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


def _pushdown_schema_findings(diff: DiffConfig, pair: _WarehousePair) -> list[ConfigFinding]:
    """Probe a warehouse pair and compile its statements without running them."""
    try:
        pair.with_session(
            lambda session, source_table, target_table: _check_pushdown_plan(
                session, source_table, target_table, diff
            )
        )
    except VerideltaError as exc:
        return [_error(str(exc))]
    return []


DEFAULT_MIN_CONFIDENCE: Final = 0.95
"""Share of a source value's rows that must agree on one target value before it
is proposed. Above one half, at most one target can qualify, and 5% leaves room
for noise in legacy data."""

DEFAULT_MIN_SUPPORT: Final = 5
"""Agreeing rows a proposal needs, so a one-off coincidence is never offered."""


def _is_real_number(value: object) -> TypeGuard[int | float]:
    """Return whether a value is an `int` or `float`, and not a `bool`."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_value_map_thresholds(
    min_confidence: object, min_support: object, sample_fraction: object
) -> None:
    """Reject proposal thresholds that cannot produce a meaningful answer."""
    if not _is_real_number(min_confidence):
        raise ConfigError(f"min_confidence must be a number, got {min_confidence!r}.")
    if not _is_real_number(sample_fraction):
        raise ConfigError(f"sample_fraction must be a number, got {sample_fraction!r}.")
    if not _is_real_number(min_support) or not isinstance(min_support, int):
        raise ConfigError(f"min_support must be a whole number, got {min_support!r}.")
    if not 0.5 < min_confidence <= 1:
        raise ConfigError(
            f"min_confidence must be above 0.5 and at most 1, got {min_confidence}. "
            "Above one half, at most one target value can qualify for a source value."
        )
    if min_support < 1:
        raise ConfigError(f"min_support must be at least 1, got {min_support}.")
    if not 0 < sample_fraction <= 1:
        raise ConfigError(f"sample_fraction must be above 0 and at most 1, got {sample_fraction}.")


def _compares_mapped_text(effective: EffectiveRule, dtype: pl.DataType) -> bool:
    """Decide whether a `value_map` output reaches the comparison unchanged."""
    return (
        isinstance(dtype, pl.String)
        and effective["pad_zeros"] is None
        and not effective["datetime_format"]
        and effective["cast_to"] in (None, "String")
    )


def _value_map_query(
    joined: pl.LazyFrame,
    column: str,
    *,
    mapped_values: Sequence[str],
    min_confidence: float,
    min_support: int,
) -> pl.LazyFrame:
    """Count how each source value lines up with the target, and keep strong pairs."""
    source = pl.col("source_value")
    target = pl.col("target_value")
    agreeing = pl.col("agreeing_rows")
    return (
        joined.select(
            pl.col(f"{column}_source").alias("source_value"),
            # The same soft cast the comparison applies to a non-text target.
            pl.col(f"{column}_target").cast(pl.String, strict=False).alias("target_value"),
        )
        .filter(source.is_not_null() & ~source.is_in(list(mapped_values)))
        .group_by("source_value", "target_value")
        .agg(pl.len().alias("agreeing_rows"))
        # Rows that already match count too, so a proposal that breaks one pays in confidence.
        .with_columns(agreeing.sum().over("source_value").alias("rows"))
        # A NULL target makes the inequality NULL, so it counts but is never proposed.
        .filter((target != source) & (agreeing >= min_support))
        .pipe(_confident_pairs, min_confidence)
    )


def _confident_pairs(pairs: pl.LazyFrame, min_confidence: float) -> pl.LazyFrame:
    """Keep the counted pairs that meet `min_confidence`, most agreeing rows first."""
    return pairs.filter(pl.col("agreeing_rows") / pl.col("rows") >= min_confidence).sort(
        ["agreeing_rows", "source_value"], descending=[True, False]
    )


def _value_map_proposal(
    config: DiffConfig, column: str, frame: pl.DataFrame
) -> ValueMapProposal | None:
    """Turn one column's qualifying pairs into a proposal."""
    if frame.is_empty():
        return None
    entries = tuple(ValueMapEntry(**row) for row in frame.iter_rows(named=True))
    governing = _match_rule(config.rules, column)
    existing = governing.value_map if governing is not None and governing.value_map else {}
    return ValueMapProposal(
        column=column,
        value_map={**existing, **{entry.source_value: entry.target_value for entry in entries}},
        entries=entries,
        governing_rule_index=None if governing is None else config.rules.index(governing),
    )


DEFAULT_MAX_SHARE: Final = 0.01
"""Largest gap a suggested tolerance may explain, as a share of the larger of its
two values. A gap past it is a change, not noise, so the column gets no suggestion."""

_SUGGESTED_EXAMPLES: Final = 3
"""Primary keys a suggestion names as examples."""


def _check_max_share(max_share: object) -> None:
    """Reject a share that cannot bound a tolerance."""
    if not _is_real_number(max_share):
        raise ConfigError(f"max_share must be a number, got {max_share!r}.")
    if not 0 < max_share <= 1:
        raise ConfigError(f"max_share must be above 0 and at most 1, got {max_share}.")


def _round_up(value: float) -> float:
    """Return the smallest of 1, 2, or 5 times a power of ten that is above `value`."""
    power = 10.0 ** math.floor(math.log10(value))
    # A step of 10 is the next power, which is always above `value`.
    step = next(step for step in (1, 2, 5, 10) if step * power > value)
    # Read back through text, so 5 * 0.001 is 0.005 and not 0.005000000000000001.
    return float(f"{step * power:.12g}")


def _spread(values: pl.Series) -> float:
    """Return how much the values vary, as their standard deviation over their mean."""
    mean = cast("float", values.mean())
    deviation = values.std()
    return 0.0 if deviation is None or mean == 0 else cast("float", deviation) / mean


class _Proposal(NamedTuple):
    """Settings a column's differing rows call for, before a run confirms them."""

    settings: dict[str, SuggestedSetting]
    largest_gap: float | None = None


def _compares_numbers(changed: pl.DataFrame, column: str) -> bool:
    """Return whether a column's compared values are numbers on both sides."""
    schema = changed.schema
    return all(schema[f"{column}_{side}"].is_numeric() for side in ("source", "target"))


def _tolerance_gaps(changed: pl.DataFrame, column: str) -> pl.DataFrame:
    """Measure the gaps between a numeric column's differing values that a tolerance closes."""
    names = (f"{column}_source", f"{column}_target")
    # Integers subtract exactly, as the comparison does: through Float64, two Int64
    # values above 2**53, such as nanosecond times, can round to one number.
    exact = pl.Int128 if all(changed.schema[name].is_integer() for name in names) else pl.Float64
    source, target = (pl.col(name).cast(exact) for name in names)
    gap = (target - source).abs().cast(pl.Float64)
    return (
        # A NULL flag is not a mismatch, as the column counts treat it.
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            gap.alias("gap"),
            (gap / pl.max_horizontal(source.abs(), target.abs()).cast(pl.Float64)).alias("share"),
            (gap / source.abs().cast(pl.Float64)).alias("relative"),
        )
        # A gap of 0 is a type `strict_types` refuses, and a gap to an infinity, NaN,
        # or NULL is not finite: no tolerance closes either.
        .filter(pl.col("gap").is_finite() & (pl.col("gap") > 0))
    )


def _tolerance_proposal(changed: pl.DataFrame, column: str, max_share: float) -> _Proposal | None:
    """Propose a tolerance when every gap in a numeric column is at most `max_share`.

    Rounding leaves gaps of about one size whatever the values, which an absolute
    tolerance describes. A rate change leaves gaps that grow with the values, which a
    relative tolerance describes. The kind whose measure varies less wins, and a
    source value of 0, which no relative tolerance reaches, picks absolute. The value
    is the round number just above the largest gap.
    """
    gaps = _tolerance_gaps(changed, column)
    if gaps.is_empty() or cast("float", gaps["share"].max()) > max_share:
        return None
    largest = cast("float", gaps["gap"].max())
    relative = gaps["relative"]
    if not relative.is_finite().all() or _spread(gaps["gap"]) <= _spread(relative):
        return _Proposal({"absolute_tolerance": _round_up(largest)}, largest)
    return _Proposal({"relative_tolerance": _round_up(cast("float", relative.max()))}, largest)


def _compares_text(changed: pl.DataFrame, column: str) -> bool:
    """Return whether a column's compared values are text on both sides."""
    schema = changed.schema
    return all(schema[f"{column}_{side}"] == pl.String for side in ("source", "target"))


def _text_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose trimming, case folding, or both, when differing text matches without them.

    A setting is proposed only when the rows need it: trimming alone when it matches
    every row that trimming and case folding together match, then case folding alone,
    and both otherwise. Trimming strips both ends, before case folding, as the
    comparison does.
    """
    source = pl.col(f"{column}_source")
    target = pl.col(f"{column}_target")
    # A pair with a NULL compares to NULL, which `sum` skips: no setting matches it.
    counts = (
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            trimmed=(source.str.strip_chars() == target.str.strip_chars()).sum(),
            folded=(source.str.to_lowercase() == target.str.to_lowercase()).sum(),
            both=(
                source.str.strip_chars().str.to_lowercase()
                == target.str.strip_chars().str.to_lowercase()
            ).sum(),
        )
        .row(0, named=True)
    )
    if counts["both"] == 0:
        return None
    if counts["trimmed"] == counts["both"]:
        return _Proposal({"whitespace_mode": "both"})
    if counts["folded"] == counts["both"]:
        return _Proposal({"case_insensitive": True})
    return _Proposal({"whitespace_mode": "both", "case_insensitive": True})


_NULL_SPELLINGS: Final = frozenset(
    {"", "-", "--", "?", "n/a", "na", "#n/a", "null", "(null)", "<null>", "none", "nil", "nan"}
)
"""Text a system writes in place of NULL, compared stripped and in lowercase."""

_NULL_NUMBERS: Final = frozenset({-1, -9, -99, -999, -9999, -99999})
"""Numbers a system writes in place of NULL."""


def _looks_like_null(value: object) -> bool:
    """Return whether a value is a common spelling of NULL, such as `N/A` or -999."""
    if isinstance(value, str):
        return value.strip().lower() in _NULL_SPELLINGS
    # A flag is never proposed: `false` where the other side is NULL is too often meant.
    return isinstance(value, int | float) and not isinstance(value, bool) and value in _NULL_NUMBERS


def _sentinel_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose `null_values` for spellings of NULL that stand where the other side is NULL."""
    differing = pl.col(f"{column}_is_match").eq(False)
    found: set[SentinelValue] = set()
    for side, other in (("source", "target"), ("target", "source")):
        values = changed.filter(
            differing
            & pl.col(f"{column}_{other}").is_null()
            & pl.col(f"{column}_{side}").is_not_null()
        )[f"{column}_{side}"]
        found.update(value for value in values.unique().to_list() if _looks_like_null(value))
    if not found:
        return None
    return _Proposal({"null_values": sorted(found, key=repr)})


_DATE_FORMATS: Final = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
)
"""Formats tried on dates written as text, day first before month first on a tie."""


def _format_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose a `datetime_format` when text on one side reads as the dates on the other.

    Each format in `_DATE_FORMATS` parses the text side as the comparison would, and
    the one that matches the most differing rows wins. A timestamp with a zone is
    left out, since text without an offset names no instant to match it.
    """
    dtypes = {side: changed.schema[f"{column}_{side}"] for side in ("source", "target")}
    texts = [side for side, dtype in dtypes.items() if dtype == pl.String]
    dates = [
        side
        for side, dtype in dtypes.items()
        if isinstance(dtype, pl.Date) or (isinstance(dtype, pl.Datetime) and not dtype.time_zone)
    ]
    if len(texts) != 1 or len(dates) != 1:
        return None
    text = pl.col(f"{column}_{texts[0]}")
    other = pl.col(f"{column}_{dates[0]}").cast(pl.Datetime)
    counts = (
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            (
                text.str.strptime(pl.Datetime, format=_polars_datetime_format(fmt), strict=False)
                == other
            )
            .sum()
            .alias(fmt)
            for fmt in _DATE_FORMATS
        )
        .row(0)
    )
    best = max(range(len(_DATE_FORMATS)), key=counts.__getitem__)
    if counts[best] == 0:
        return None
    return _Proposal({"datetime_format": _DATE_FORMATS[best]})


def _proposal(changed: pl.DataFrame, column: str, max_share: float) -> _Proposal | None:
    """Propose what a column's differing rows call for, its null sentinels included.

    Numbers can take a tolerance, text can take trimming and case folding, and text
    beside dates can take a date format. Any of them can take null sentinels beside,
    in one rule.
    """
    if _compares_numbers(changed, column):
        typed = _tolerance_proposal(changed, column, max_share)
    elif _compares_text(changed, column):
        typed = _text_proposal(changed, column)
    else:
        typed = _format_proposal(changed, column)
    sentinels = _sentinel_proposal(changed, column)
    if typed is None or sentinels is None:
        return typed or sentinels
    return _Proposal({**typed.settings, **sentinels.settings}, typed.largest_gap)


def _suggested_rule(
    governing: DiffRule | None,
    column: str,
    settings: dict[str, SuggestedSetting],
    default_null_values: list[SentinelValue],
) -> DiffRule:
    """Name the column alone, keep the settings that govern it today, and add `settings`.

    Suggested null sentinels join the column's sentinels today, since a rule's
    `null_values` replaces `default_null_values` rather than adding to them.
    """
    kept = (
        {}
        if governing is None
        else governing.model_dump(
            exclude_unset=True, exclude={"column_names", "pattern", "rename_to"}
        )
    )
    added = dict(settings)
    sentinels = settings.get("null_values")
    if isinstance(sentinels, list):
        current = kept.get("null_values", default_null_values)
        added["null_values"] = [*current, *(value for value in sentinels if value not in current)]
    return DiffRule.model_validate({**kept, "column_names": [column], **added})


def _differing_only_in(
    first: pl.DataFrame, second: pl.DataFrame, keys: list[str], column: str
) -> pl.DataFrame:
    """Return the keys of the rows whose column differs in `first` and not in `second`."""
    differing = pl.col(f"{column}_is_match").eq(False)
    return (
        first.filter(differing)
        .select(keys)
        .join(second.filter(differing).select(keys), on=keys, how="anti", nulls_equal=True)
        .sort(keys)
    )


_VALUE_MAP_RESULT_COLUMNS: Final[dict[str, pl.DataType]] = {
    # A warehouse may deliver text as Categorical and counts as wide decimals.
    VALUE_MAP_COLUMN_ALIAS: pl.Int64(),
    VALUE_MAP_SOURCE_ALIAS: pl.String(),
    VALUE_MAP_TARGET_ALIAS: pl.String(),
    VALUE_MAP_ROWS_ALIAS: pl.Int64(),
    VALUE_MAP_AGREEING_ALIAS: pl.Int64(),
}
"""Columns `compile_value_map_query` returns, and the type each is read as."""

_VALUE_MAP_FIELDS: Final[dict[str, str]] = {
    VALUE_MAP_SOURCE_ALIAS: "source_value",
    VALUE_MAP_TARGET_ALIAS: "target_value",
    VALUE_MAP_ROWS_ALIAS: "rows",
    VALUE_MAP_AGREEING_ALIAS: "agreeing_rows",
}
"""Result columns renamed to the `ValueMapEntry` fields they fill."""


def _value_map_pairs(result: pl.DataFrame) -> pl.DataFrame:
    """Read the value map statement's result into typed, labeled pairs."""
    missing = [name for name in _VALUE_MAP_RESULT_COLUMNS if name not in result.columns]
    if missing:
        raise ConnectorError(f"Value map query result is missing the columns {missing}.")
    try:
        typed = result.select(
            pl.col(name).cast(dtype) for name, dtype in _VALUE_MAP_RESULT_COLUMNS.items()
        )
    except pl.exceptions.PolarsError as exc:
        raise ConnectorError(f"Value map query returned values of the wrong type: {exc}") from exc
    return typed.rename(_VALUE_MAP_FIELDS)


def _collect_value_map_proposals(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
    *,
    min_confidence: float,
    min_support: int,
    sample_fraction: float,
) -> list[ValueMapProposal]:
    """Propose `value_map` entries from warehouse tables, in one counting statement."""
    source_schema, target_schema = _validate_pushdown_schema(
        connector, source_table, target_table, diff
    )
    key_rules = _resolve_pushdown_keys(diff, source_schema, target_schema)
    # A column qualifies when it is text on both sides and nothing after stage 4
    # changes what a map produces. Unlike a local run, a non-text target never
    # qualifies: each engine writes numbers and timestamps as text its own way.
    rules = [
        _pushdown_rule(column, aligned, rule, effective)
        for column, aligned, rule, effective in _pushdown_columns(
            diff, source_schema, target_schema
        )
        if _compares_mapped_text(effective, source_schema[column])
        and isinstance(target_schema[aligned], pl.String)
    ]
    # As locally, repeated keys stop the run even when no column qualifies.
    _reject_duplicate_pushdown_keys(
        connector, diff, key_rules, (source_table, target_table), (source_schema, target_schema)
    )
    statement = connector.compiler.compile_value_map_query(
        source_table,
        target_table,
        diff.primary_keys,
        rules,
        min_support=min_support,
        sample_fraction=sample_fraction,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
    )
    if statement is None:
        return []
    pairs = _value_map_pairs(
        connector.execute_pushdown(statement, query_type="value_maps").collect()
    )
    proposals: list[ValueMapProposal] = []
    for label, rule in enumerate(rules):
        column_pairs = pairs.filter(pl.col(VALUE_MAP_COLUMN_ALIAS) == label).drop(
            VALUE_MAP_COLUMN_ALIAS
        )
        frame = _confident_pairs(column_pairs.lazy(), min_confidence).collect()
        proposal = _value_map_proposal(diff, rule.rename_to or rule.column_names[0], frame)
        if proposal is not None:
            proposals.append(proposal)
    return proposals


class DiffEngine:
    """Compare two datasets on their primary keys and report what differs.

    The engine takes two Polars `LazyFrame` inputs and a `DiffConfig`, applies
    the nine-stage `DiffRule` pipeline to each side, then joins on the primary
    keys to classify rows as added (target-only), removed (source-only), or
    changed (present on both sides with at least one compared column
    differing). Nothing is materialized until `run()` collects the joins, so
    the source frames can be `scan_*` graphs over files far larger than memory.

    These entry points cover the usual situations:

    - `DiffEngine(config, source, target).run()` for frames you already hold.
      In-memory `DataFrame` inputs must be wrapped with `.lazy()` first.
    - `DiffEngine.run_from_configs(diff, source, target)` for `SourceRef`
      pairs from YAML. File, lakehouse, database, and DuckDB pairs load
      through `LoaderFactory` and run locally. A same-warehouse pair, or a
      Postgres or DuckDB pair that sets `pushdown`, compiles to SQL and runs
      where it is stored.
    - `DiffEngine.validate_schemas(...)` to enforce `schema_mode` and primary
      key presence on metadata alone, before any rows are read.
    - `DiffEngine.validate_rules(...)` to also resolve every rule and build
      each column's comparison, still on metadata alone.
    - `DiffEngine(config, source, target).propose_value_maps()`, or
      `DiffEngine.propose_value_maps_from_configs(...)` for any `SourceRef`
      pair, counted in the warehouse for same-warehouse pairs, to suggest
      `value_map` entries from how the two sides' values line up, without
      running the comparison.

    Attributes:
        config (DiffConfig): Primary keys, rules, defaults, and threshold.
        source (pl.LazyFrame): Source side; mutated in place as alignment and
            normalization stages run.
        target (pl.LazyFrame): Target side, treated as the authoritative schema.

    Raises:
        ConfigError: From `run()` when primary keys are missing, `schema_mode`
            is violated, or a rule cannot apply to the column it names.
        DataIntegrityError: From `run()` when primary keys are not unique on
            either side after normalization.
    """

    def __init__(
        self, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> None:
        """Hold the configuration and the two datasets to compare.

        `run()` aligns the datasets and compares them.

        Args:
            config (DiffConfig): Keys, rules, and settings for the comparison.
            source_df (pl.LazyFrame): Source rows, as stored.
            target_df (pl.LazyFrame): Target rows, as stored.
        """
        self.config = config
        self.source = source_df
        self.target = target_df
        # How each side was read, set by the entry points that load a `SourceRef`,
        # so an error can name the file and its format rather than only a column.
        self._sides: dict[str, str] | None = None

    @classmethod
    def _on_sources(cls, diff: DiffConfig, source: SourceRef, target: SourceRef) -> "DiffEngine":
        """Load both sides and build an engine whose errors name them."""
        engine = cls(diff, LoaderFactory.load(source), LoaderFactory.load(target))
        engine._sides = _describe_sides(source, target)
        return engine

    @classmethod
    def run_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        baseline: Baseline | None = None,
    ) -> DiffResult:
        """Route a comparison to pushdown or local Polars evaluation.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source file, lakehouse, database, DuckDB, or warehouse config.
            target (SourceRef): Target file, lakehouse, database, DuckDB, or warehouse config.
            baseline (Baseline | None): Drift to accept, which a local run leaves out
                of the counts and the verdict.

        Returns:
            DiffResult: The result. A pushdown pair returns counts and keys, and a
                local pair also returns the differing rows.

        Raises:
            ConfigError: If primary keys are missing, `schema_mode` is violated, or a
                baseline is given for a pair compared where it is stored.
            DataIntegrityError: If either dataset repeats a normalized primary key.
            ConnectorError: If warehouse backends are mixed or connections differ.

        Examples:
            >>> from veridelta.models import DiffConfig, SourceConfig
            >>> diff = DiffConfig(primary_keys=["order_id"])
            >>> source = SourceConfig(path="legacy/orders.parquet", format="parquet")
            >>> target = SourceConfig(path="modern/orders.parquet", format="parquet")
            >>> result = DiffEngine.run_from_configs(diff, source, target)  # doctest: +SKIP
        """
        pair = _check_backend_pairing(source, target)
        if pair is not None:
            if baseline is not None:
                raise ConfigError(
                    "A baseline applies to a run that compares both sides locally, and this "
                    "pair is compared where it is stored. Leave out --baseline, or compare "
                    "files exported from it."
                )
            return pair.with_session(
                lambda session, source_table, target_table: _collect_pushdown_summary(
                    session, source_table, target_table, diff
                )
            )

        # `run()` normalizes headers and applies renames exactly once, so the
        # frames go to it straight from the loaders: aligning them first and
        # renaming again would undo a swap and collapse a chain.
        return cls._on_sources(diff, source, target).run(baseline=baseline)

    @classmethod
    def validate_schemas(
        cls, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> None:
        """Align structure and enforce `SchemaMode` without comparing any rows.

        Operates on schema metadata only, so callers may pass zero-row frames.

        Args:
            config (DiffConfig): Comparison settings and rules.
            source_df (pl.LazyFrame): Source frame or column probe.
            target_df (pl.LazyFrame): Target frame or column probe.

        Raises:
            ConfigError: If primary keys are missing or schema constraints are violated.
        """
        engine = cls(config, source_df, target_df)
        engine._align_structure()
        engine._validate_schema()

    @classmethod
    def validate_rules(
        cls, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> list[str]:
        """Check everything a run checks before it reads a row.

        Goes past `validate_schemas`: every rule is resolved against the aligned
        columns, both schemas are normalized, and each column's comparison is built. A
        rule the run could not honor fails here, such as a null sentinel its column's
        type cannot hold, or a similarity limit without the `fuzzy` extra. Operates on
        schema metadata only, so callers may pass zero-row frames. Repeated keys and
        invalid regular expressions surface only when rows are read.

        Args:
            config (DiffConfig): Comparison settings and rules.
            source_df (pl.LazyFrame): Source frame or column probe.
            target_df (pl.LazyFrame): Target frame or column probe.

        Returns:
            list[str]: The columns a run would compare, in source order, under their
                target names.

        Raises:
            ConfigError: If primary keys are missing, schema constraints are
                violated, or a rule cannot apply as configured.
        """
        return cls(config, source_df, target_df)._plan()[0]

    @staticmethod
    def check_configs(
        diff: DiffConfig, source: SourceRef, target: SourceRef, *, schemas: bool = False
    ) -> list[ConfigFinding]:
        """Check a loaded configuration for what would stop a run.

        By default nothing connects and no rows are read, so the checks need
        only the configuration and the installed extras:

        - the pair is one an engine can compare: both local, or two tables on
          one warehouse connection;
        - every extra a side reads through, or a local run scores with, is
          installed;
        - a database `table` uses a URI scheme Veridelta can quote for;
        - each `regex_replace` pattern compiles in Polars;
        - on a warehouse pair, settings the warehouse refuses for some stored
          names or types, reported as warnings.

        With `schemas`, and no errors so far, each side's columns are read too,
        but never its rows. Local sides are checked with `validate_rules`; a
        database `table` is read with a zero-row probe, and a `query` is not
        run at all. A warehouse pair runs the schema probes a run starts with,
        then compiles every comparison statement without executing it, which
        settles the warnings above one way or the other.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            schemas (bool): Whether to also connect and check the rules against
                the stored columns.

        Returns:
            list[ConfigFinding]: Errors and warnings, empty when nothing is
                wrong. A configuration with no errors is expected to start.
        """
        findings: list[ConfigFinding] = []
        pair: _WarehousePair | None = None
        try:
            pair = _check_backend_pairing(source, target)
        except (ConfigError, ConnectorError) as exc:
            findings.append(_error(str(exc)))
        findings += _missing_extra_findings(source, target)
        findings += _database_findings(source, target)
        findings += _regex_findings(diff, pushdown=pair is not None)
        if pair is None:
            findings += _fuzzy_extra_findings(diff)
        if not schemas:
            return findings if pair is None else findings + _pushdown_findings(diff, pair)
        if any(finding.severity == "error" for finding in findings):
            return [*findings, _warning("Schemas were not checked, because of the errors above.")]
        if pair is None:
            return findings + DiffEngine._local_schema_findings(diff, source, target)
        return findings + _pushdown_schema_findings(diff, pair)

    @staticmethod
    def check_config_file(
        path: str | Path, *, schemas: bool = False, allow_missing_env: bool = False
    ) -> list[ConfigFinding]:
        """Load a configuration file and check it for what would stop a run.

        A file that does not load is one error finding, with the loader's
        message, so every problem is reported the same way. A file that loads
        gets the checks of `check_configs`. `veridelta validate` and the MCP
        server's `validate_config` tool both report through this method.

        Args:
            path (str | Path): The configuration file.
            schemas (bool): Whether to also connect and check the rules against
                the stored columns.
            allow_missing_env (bool): Whether to read an unset `${NAME}` as the
                text `NAME` and warn, instead of failing, so a file can be
                checked without its secrets.

        Returns:
            list[ConfigFinding]: A warning for each unset variable first, then
                the findings of `check_configs`, or the error that stopped the
                load.
        """
        unset: list[str] | None = [] if allow_missing_env else None
        try:
            diff, source, target = load_config(path, unset_env=unset)
            findings = DiffEngine.check_configs(diff, source, target, schemas=schemas)
        except ConfigError as exc:
            findings = [_error(str(exc).strip())]
        unset_findings = [
            _warning(
                f"Environment variable '{name}' is not set, so its references were checked "
                f"as the text '{name}'."
            )
            for name in unset or []
        ]
        return [*unset_findings, *findings]

    @staticmethod
    def read_schema(config: SourceRef) -> pl.Schema:
        """Read one side's columns and their types, and return none of its rows.

        Each side is read as a run reads it before its first row. A file is
        read by its loader, and a CSV file's types come from its first rows; a
        lakehouse table gives its schema; a database or DuckDB `table` is read
        with a probe that returns no rows. A warehouse table, or a table with
        `pushdown`, gets the probe a pushdown run starts with, in a session of
        its own that is closed after.

        Args:
            config (SourceRef): One side of a configuration.

        Returns:
            pl.Schema: The columns in their stored order and with their stored
                names, before `normalize_column_names` or a `rename_to`.

        Raises:
            ConfigError: If the side reads a database or DuckDB `query`, which a
                probe would have to run in full.
            ConnectorError: If the side cannot be reached or read.
        """
        if not _is_warehouse(config):
            return _schema_frame(config).collect_schema()
        with _warehouse_session(config) as session:
            return _probe_relation(session, _table_name(config))[1]

    @classmethod
    def _local_schema_findings(
        cls, diff: DiffConfig, source: SourceRef, target: SourceRef
    ) -> list[ConfigFinding]:
        """Check the rules against two local sides' stored columns."""
        queries = [
            _warning(
                f"The {label} reads a query, which validate does not run, so the rules were "
                "not checked against stored columns."
            )
            for label, config in (("source", source), ("target", target))
            if isinstance(config, (DatabaseConfig, DuckDBConfig)) and config.query is not None
        ]
        if queries:
            return queries
        try:
            source_frame, target_frame = _schema_frame(source), _schema_frame(target)
        except (VerideltaError, OSError, pl.exceptions.PolarsError) as exc:
            return [_error(f"Could not read the schemas: {exc}")]
        engine = cls(diff, source_frame, target_frame)
        engine._sides = _describe_sides(source, target)
        try:
            engine._plan()
        except ConfigError as exc:
            return [_error(str(exc))]
        return []

    @classmethod
    def propose_value_maps_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        min_support: int = DEFAULT_MIN_SUPPORT,
        sample_fraction: float = 1.0,
    ) -> list[ValueMapProposal]:
        """Propose `value_map` entries for a `SourceRef` pair, wherever it lives.

        File, lakehouse, and database pairs are loaded and proposed locally. A
        pair of tables on one warehouse connection is counted in the warehouse
        instead, in one statement, for columns stored as text on both sides;
        rows never leave it. A sampled warehouse run reads a different, though
        equally repeatable, set of keys than a local one.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            min_confidence (float): Share of rows that must agree, above 0.5.
            min_support (int): Agreeing rows a proposal needs.
            sample_fraction (float): Share of source rows to read.

        Returns:
            list[ValueMapProposal]: One proposal per column with new entries.

        Raises:
            ConnectorError: If only one side is a warehouse table, the sides use
                different warehouses or connections, or a warehouse returns a
                malformed result.
            ConfigError: If a threshold is out of range, or the configuration
                fails as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.
        """
        # Bad thresholds fail before a session opens or a file is read.
        _check_value_map_thresholds(min_confidence, min_support, sample_fraction)
        pair = _check_backend_pairing(source, target)
        if pair is not None:
            return pair.with_session(
                lambda session, source_table, target_table: _collect_value_map_proposals(
                    session,
                    source_table,
                    target_table,
                    diff,
                    min_confidence=min_confidence,
                    min_support=min_support,
                    sample_fraction=sample_fraction,
                )
            )
        engine = cls._on_sources(diff, source, target)
        return engine.propose_value_maps(
            min_confidence=min_confidence,
            min_support=min_support,
            sample_fraction=sample_fraction,
        )

    def propose_value_maps(
        self,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        min_support: int = DEFAULT_MIN_SUPPORT,
        sample_fraction: float = 1.0,
    ) -> list[ValueMapProposal]:
        """Propose `value_map` entries from how source and target values line up.

        Rows are aligned, normalized, and joined as `run()` does, on a copy, so this
        engine can still run afterward. For each compared text column that stage 4 can
        map, a source value is proposed for the target value it lines up with in at
        least `min_confidence` of its joined rows, provided at least `min_support` rows
        agree. Values are read as the `value_map` stage sees them, so a
        `case_insensitive` column gets lowercase keys. Rows a column's existing map
        already translates are left out, so a raw value equal to one of that map's
        outputs cannot receive an entry.

        Args:
            min_confidence (float): Share of rows that must agree, above 0.5.
            min_support (int): Agreeing rows a proposal needs.
            sample_fraction (float): Share of source rows to read, picked by a hash of
                the primary keys, so the same data samples the same rows under one
                Polars version.

        Returns:
            list[ValueMapProposal]: One proposal per column with new entries, in source
                column order.

        Raises:
            ConfigError: If a threshold is out of range, or the configuration fails as
                it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": range(6), "sex": ["M"] * 6})
            >>> target = pl.LazyFrame({"id": range(6), "sex": ["Male"] * 6})
            >>> engine = DiffEngine(DiffConfig(primary_keys=["id"]), source, target)
            >>> proposals = engine.propose_value_maps()
            >>> proposals[0].to_rule().value_map
            {'M': 'Male'}
        """
        _check_value_map_thresholds(min_confidence, min_support, sample_fraction)
        prepared = type(self)(self.config, self.source, self.target)
        prepared._sides = self._sides
        prepared._align_structure()
        prepared._validate_schema()
        stored = prepared.source.collect_schema()
        prepared.source = prepared._normalize_frame(prepared.source, is_source=True)
        prepared.target = prepared._normalize_frame(prepared.target, is_source=False)
        prepared._check_uniqueness()

        columns = prepared._value_map_columns(stored)
        if not columns:
            return []
        joined = prepared._value_map_join(columns, sample_fraction)
        frames = pl.collect_all(
            _value_map_query(
                joined,
                column,
                mapped_values=list(
                    (prepared._get_effective_rule(column)["value_map"] or {}).values()
                ),
                min_confidence=min_confidence,
                min_support=min_support,
            )
            for column in columns
        )
        proposals = (
            _value_map_proposal(prepared.config, column, frame)
            for column, frame in zip(columns, frames, strict=True)
        )
        return [proposal for proposal in proposals if proposal is not None]

    @classmethod
    def suggest_rules_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        max_share: float = DEFAULT_MAX_SHARE,
    ) -> list[RuleSuggestion]:
        """Suggest rules for a `SourceRef` pair, read locally.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            max_share (float): Largest gap a tolerance may explain, as a share of
                the larger of its two values, above 0 and at most 1.

        Returns:
            list[RuleSuggestion]: One suggestion per column it can explain.

        Raises:
            ConfigError: If `max_share` is out of range, the pair is compared where
                it is stored, or the configuration fails as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.
        """
        _check_max_share(max_share)
        if _check_backend_pairing(source, target) is not None:
            raise ConfigError(
                "veridelta suggest reads both sides locally, and this pair is compared "
                "where it is stored. Suggest rules on files exported from it, or on a "
                "database pair that does not set pushdown."
            )
        engine = cls._on_sources(diff, source, target)
        return engine.suggest_rules(max_share=max_share)

    def suggest_rules(self, *, max_share: float = DEFAULT_MAX_SHARE) -> list[RuleSuggestion]:
        """Suggest rules that would explain the differences in each compared column.

        The comparison runs as `run()` runs it, on a copy, without writing artifacts,
        so this engine can still run afterward. A numeric column gets a tolerance when
        every gap between its differing values is at most `max_share` of the larger
        of the two: an absolute tolerance when the gaps stay about one size, and a
        relative one when they grow with the values. Each tolerance is the round value
        just above the largest gap, and the comparison runs again with it to count
        the rows it explains. No model is called.

        Args:
            max_share (float): Largest gap a tolerance may explain, as a share of the
                larger of its two values, above 0 and at most 1.

        Returns:
            list[RuleSuggestion]: One suggestion per column it can explain, in
                compared column order.

        Raises:
            ConfigError: If `max_share` is out of range, or the configuration fails
                as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": [1, 2, 3], "fare": [10.0, 20.0, 30.0]})
            >>> target = pl.LazyFrame({"id": [1, 2, 3], "fare": [10.004, 20.004, 30.003]})
            >>> engine = DiffEngine(DiffConfig(primary_keys=["id"]), source, target)
            >>> [(s.column, s.settings) for s in engine.suggest_rules()]
            [('fare', {'absolute_tolerance': 0.005})]
        """
        _check_max_share(max_share)
        result = self._run_copy(self.config)
        keys = list(result.primary_keys)
        suggestions: list[RuleSuggestion] = []
        for column in result.compared_columns:
            differing = result.summary.column_mismatches.get(column, 0)
            if differing == 0:
                continue
            proposal = _proposal(result.changed, column, max_share)
            if proposal is None:
                continue
            governing = _match_rule(self.config.rules, column)
            rule = _suggested_rule(
                governing, column, proposal.settings, self.config.default_null_values
            )
            try:
                tried = self._run_copy(
                    self.config.model_copy(update={"rules": [rule, *self.config.rules]})
                )
            except ConfigError:
                # The configuration refuses the rule, such as a sentinel that one side's
                # type cannot hold, so it explains nothing.
                continue
            explained = _differing_only_in(result.changed, tried.changed, keys, column)
            # A rule that makes a matching row differ, such as case folding ahead of a
            # `value_map` written in capitals, is no explanation.
            broken = _differing_only_in(tried.changed, result.changed, keys, column)
            if explained.is_empty() or not broken.is_empty():
                continue
            suggestions.append(
                RuleSuggestion(
                    column=column,
                    settings=proposal.settings,
                    differing=differing,
                    explained=explained.height,
                    largest_gap=proposal.largest_gap,
                    examples=tuple(explained.head(_SUGGESTED_EXAMPLES).to_dicts()),
                    rule=rule,
                    governing_rule_index=(
                        None if governing is None else self.config.rules.index(governing)
                    ),
                )
            )
        return suggestions

    def _run_copy(self, config: DiffConfig) -> DiffResult:
        """Run a comparison of this engine's data under `config`, writing no artifacts."""
        engine = type(self)(
            config.model_copy(update={"output_path": None}), self.source, self.target
        )
        engine._sides = self._sides
        return engine.run()

    def _value_map_columns(self, stored: pl.Schema) -> list[str]:
        """Pick the compared columns a `value_map` proposal can apply to."""
        keys = set(self.config.primary_keys)
        target_schema = self.target.collect_schema()
        columns: list[str] = []
        for column, dtype in stored.items():
            if column in keys or column not in target_schema:
                continue
            rule = self._get_effective_rule(column)
            if rule["ignore"] or not _compares_mapped_text(rule, dtype):
                continue
            if self.config.strict_types and not isinstance(target_schema[column], pl.String):
                # Types that differ always mismatch under strict_types; no map helps.
                continue
            columns.append(column)
        return columns

    def _value_map_join(self, columns: list[str], sample_fraction: float) -> pl.LazyFrame:
        """Pair each candidate column's normalized values on the primary keys."""
        keys = self.config.primary_keys
        source = self.source.select(*keys, pl.col(columns).name.suffix("_source"))
        target = self.target.select(*keys, pl.col(columns).name.suffix("_target"))
        if sample_fraction < 1:
            cutoff = round(sample_fraction * SAMPLE_BUCKETS)
            source = source.filter(pl.struct(keys).hash(seed=0) % SAMPLE_BUCKETS < cutoff)
        return source.join(target, on=keys, how="inner")

    def _get_effective_rule(self, col_name: str) -> EffectiveRule:
        """Resolve all rules (Specific > Pattern > Global) into a unified dictionary."""
        return _fold_rule_defaults(_match_rule(self.config.rules, col_name), self.config)

    def _check_uniqueness(self) -> None:
        """Verify that primary keys are unique in both datasets."""
        pks = self.config.primary_keys
        for side, frame in (("SOURCE", self.source), ("TARGET", self.target)):
            keys = frame.select(pks).collect()
            duplicated = keys.is_duplicated()
            if duplicated.any():
                raise _duplicate_keys_error(pks, side, keys.filter(duplicated).height)

    def _normalize_frame(self, frame: pl.LazyFrame, *, is_source: bool) -> pl.LazyFrame:
        """Apply stages 1 through 7 of the canonical transform order to one dataset."""
        schema = frame.collect_schema()
        rules = {
            column: rule
            for column in schema.names()
            if not (rule := self._get_effective_rule(column))["ignore"]
        }

        value_exprs: list[pl.Expr] = []
        for column, rule in rules.items():
            value_expr = self._normalize_value_expr(
                column, rule, schema[column], is_source=is_source
            )
            if value_expr is not None:
                value_exprs.append(value_expr)
        if value_exprs:
            frame = frame.with_columns(value_exprs)

        temporal = [column for column, rule in rules.items() if rule["timezone"] or rule["cast_to"]]
        if not temporal:
            return frame
        # A second pass, since every expression in one `with_columns` reads the
        # input frame. Stage 6a can turn a String column into a Datetime, so the
        # timezone guard inspects the post-parse schema.
        parsed_schema = frame.collect_schema()
        return frame.with_columns(
            self._normalize_temporal_expr(column, rules[column], parsed_schema[column])
            for column in temporal
        )

    def _normalize_value_expr(
        self, column: str, rule: EffectiveRule, dtype: pl.DataType, *, is_source: bool
    ) -> pl.Expr | None:
        """Build stages 1 through 6a for one column: sentinels through datetime parsing."""
        expr = pl.col(column)
        applied = False
        # Deliberately narrower than `is_text_dtype`: `is_in` accepts Categorical
        # and Enum, but the `.str` namespace used below rejects both.
        is_text = isinstance(dtype, pl.String)

        sentinels = usable_sentinels(rule["null_values"], dtype)
        if sentinels:
            expr = pl.when(expr.is_in(sentinels)).then(None).otherwise(expr)
            applied = True
        # Only an explicit rule raises: a global default is expected to span a mixed schema.
        elif rule["null_values"] and rule["null_values_explicit"]:
            raise _unusable_sentinel_error(column, dtype, rule["null_values"])

        if is_text:
            expr, text_applied = self._normalize_text_expr(expr, rule, is_source=is_source)
            applied = applied or text_applied

        if rule["pad_zeros"] is not None:
            # Stringify first so a numeric 123 and a text '00123' converge.
            expr = expr.cast(pl.String).str.zfill(rule["pad_zeros"])
            applied = True
            is_text = True

        if rule["datetime_format"] and is_text:
            expr = expr.str.strptime(
                pl.Datetime,
                format=_polars_datetime_format(rule["datetime_format"]),
                strict=False,
            )
            applied = True

        return expr.alias(column) if applied else None

    @staticmethod
    def _normalize_text_expr(
        expr: pl.Expr, rule: EffectiveRule, *, is_source: bool
    ) -> tuple[pl.Expr, bool]:
        """Build stages 2 through 4 for a text column: regex, whitespace, case, value map."""
        applied = False
        if rule["regex_replace"]:
            for pattern, replacement in rule["regex_replace"].items():
                expr = expr.str.replace_all(pattern, replacement)
            applied = True

        mode = rule["whitespace"]
        if mode == "left":
            expr = expr.str.strip_chars_start()
            applied = True
        elif mode == "right":
            expr = expr.str.strip_chars_end()
            applied = True
        elif mode == "both":
            expr = expr.str.strip_chars()
            applied = True

        if rule["case_insensitive"]:
            expr = expr.str.to_lowercase()
            applied = True

        if is_source and rule["value_map"]:
            expr = expr.replace(rule["value_map"])
            applied = True

        return expr, applied

    def _normalize_temporal_expr(
        self, column: str, rule: EffectiveRule, dtype: pl.DataType
    ) -> pl.Expr:
        """Build stages 6b and 7 for one column: timezone conversion, then cast."""
        expr = pl.col(column)
        if rule["timezone"]:
            expr = self._convert_time_zone(column, expr, dtype, rule["timezone"])
        if rule["cast_to"]:
            target = rule["cast_to"]
            if target in _UNCASTABLE.get(type(dtype), frozenset()):
                raise ConfigError(
                    f"Column '{column}' sets cast_to='{target}', but holds {dtype}, which "
                    f"cannot be cast to {target}. Convert it where it is read, such as in "
                    "a database query."
                )
            expr = expr.cast(_CAST_TARGETS[target])
        return expr.alias(column)

    def _convert_time_zone(
        self, column: str, expr: pl.Expr, dtype: pl.DataType, zone: str
    ) -> pl.Expr:
        """Convert a timezone-aware column to `zone`, refusing to guess for naive data."""
        # `convert_time_zone` treats a naive timestamp as UTC instead of refusing it.
        _reject_unzoned_timezone(column, dtype, zone)
        try:
            return expr.dt.convert_time_zone(zone)
        except pl.exceptions.ComputeError as exc:
            raise ConfigError(f"Column '{column}' sets an unusable timezone. {exc}") from exc

    def _build_match_expr(self, col_name: str, rule: EffectiveRule, dtype: pl.DataType) -> pl.Expr:
        """Build stages 8 and 9 of the transform order: comparison and null equality."""
        src = pl.col(f"{col_name}_source")
        tgt = pl.col(f"{col_name}_target")

        tgt_dtype = self.target.collect_schema().get(col_name)
        # The type the target is compared as, once any soft cast below applies.
        compared_tgt_dtype = tgt_dtype

        if dtype != tgt_dtype:
            if self.config.strict_types:
                return _null_equality(pl.lit(False), src, tgt, rule)
            # Numbers skip the cast, which would truncate a Float64 `10.7` to an Int64 `10`.
            elif not (dtype.is_numeric() and tgt_dtype is not None and tgt_dtype.is_numeric()):
                tgt = tgt.cast(dtype, strict=False)
                compared_tgt_dtype = dtype

        similar = _similarity_test(rule) if isinstance(dtype, pl.String) else None
        if dtype.is_numeric() and (rule["abs_tol"] != 0.0 or rule["rel_tol"] != 0.0):
            val_match = _tolerance_match(src, tgt, rule, dtype, compared_tgt_dtype)
        elif similar is not None:
            # Equal text matches outright, so only a differing pair is scored.
            val_match = (src == tgt) | _similarity_expr(src, tgt, similar)
        else:
            val_match = src == tgt
        return _null_equality(val_match, src, tgt, rule)

    def _align_structure(self) -> None:
        """Perform structural normalization to reconcile asymmetrical schemas."""
        if self.config.normalize_column_names:
            self.source = _normalize_header_names(self.source)
            self.target = _normalize_header_names(self.target)

        src_cols = self.source.collect_schema().names()
        tgt_cols = self.target.collect_schema().names()

        src_rename, src_drop = _alignment_maps(self.config.rules, src_cols, rename=True)
        # The target already carries the post-rename spellings, which the
        # resolver matches under either name.
        _, tgt_drop = _alignment_maps(self.config.rules, tgt_cols, rename=False)

        self.source = self.source.drop(list(src_drop)).rename(src_rename)
        self.target = self.target.drop(list(tgt_drop))

    def _missing_keys(self, side: str, columns: list[str]) -> str | None:
        """Say which primary keys a side lacks, and what it holds instead.

        Args:
            side (str): `source` or `target`.
            columns (list[str]): The side's columns after alignment.

        Returns:
            str | None: The message, or `None` when every key is present.
        """
        present = set(columns)
        missing = [key for key in self.config.primary_keys if key not in present]
        if not missing:
            return None
        where = f"the {side}"
        if self._sides is not None:
            where += f", {self._sides[side]}"
        keys = _quoted_list(missing)
        noun, verb = ("key", "is") if len(missing) == 1 else ("keys", "are")
        if not columns:
            return f"The primary {noun} {keys} {verb} not among the columns of {where}. No column was read."
        shown = ", ".join(repr(column)[:60] for column in columns[:10])
        if len(columns) > 10:
            shown += f", and {len(columns) - 10} more"
        return (
            f"The primary {noun} {keys} {verb} not among the columns of {where}. "
            f"The columns read are: {shown}."
        )

    def _validate_schema(self) -> None:
        """Enforce the configured `SchemaMode` before comparison."""
        source_names = self.source.collect_schema().names()
        target_names = self.target.collect_schema().names()
        source_cols, target_cols = set(source_names), set(target_names)

        for side, names in (("source", source_names), ("target", target_names)):
            message = self._missing_keys(side, names)
            if message is not None:
                raise ConfigError(message)

        # Sorted, since a set prints in an order that changes from run to run.
        only_source = sorted(source_cols - target_cols)
        only_target = sorted(target_cols - source_cols)
        mode = self.config.schema_mode
        if mode == "exact" and (only_source or only_target):
            parts: list[str] = []
            if only_source:
                parts.append(f"only the source has {_quoted_list(only_source)}")
            if only_target:
                parts.append(f"only the target has {_quoted_list(only_target)}")
            message = f"EXACT schema match failed: {'; '.join(parts)}."
        elif mode == "allow_additions" and only_source:
            message = f"Target is missing required source columns: {_quoted_list(only_source)}."
        elif mode == "allow_removals" and only_target:
            message = (
                f"Target contains unauthorized additional columns: {_quoted_list(only_target)}."
            )
        else:
            return
        if self._sides is not None:
            message += f" The source is {self._sides['source']}, and the target is {self._sides['target']}."
        raise ConfigError(message)

    def run(self, *, baseline: Baseline | None = None) -> DiffResult:
        """Compare the two datasets and return the result.

        The comparison stays lazy until it collects the joins, so the inputs can be
        scans over files larger than memory. A run takes these steps, in order:

        1. Align the columns: apply renames, drop ignored columns, and treat the
           target as the authoritative schema.
        2. Check that the primary keys exist and that `schema_mode` holds.
        3. Apply stages 1 to 7 of the `DiffRule` transform order to each side, and
           build each column's stage 8 and 9 comparison, so a rule the run cannot
           honor fails before any rows move.
        4. Check that the normalized primary keys are unique on each side.
        5. Find the added, removed, and changed rows, leave out the drift
           `baseline` accepts, and count the mismatches.
        6. Write the artifacts, when `output_path` is set.

        Args:
            baseline (Baseline | None): Drift to accept: rows by kind and key, and
                changed columns by row. The counts, the verdict, the artifacts,
                and the reports leave it out, and `accepted_count` counts it.

        Returns:
            DiffResult: Counts, column-level drift, and the differing rows.

        Raises:
            ConfigError: If a primary key is missing, `schema_mode` is violated, a
                similarity limit needs the missing `fuzzy` extra, or the artifact
                format has no writer.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
            >>> target = pl.LazyFrame({"id": [2, 3, 4], "amount": [20.0, 31.0, 40.0]})
            >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
            >>> result.summary.added_count, result.summary.removed_count
            (1, 1)
            >>> result.summary.column_mismatches
            {'amount': 1}
        """
        compared_columns, match_expressions = self._plan()

        # Runs after normalization: case folding or sentinel coercion on a key
        # column can collapse distinct rows into duplicates, and that must fail
        # here rather than silently exploding the joins below.
        self._check_uniqueness()

        keys = self.config.primary_keys
        added_df = self.target.join(self.source, on=keys, how="anti").collect()
        removed_df = self.source.join(self.target, on=keys, how="anti").collect()
        changed_df = self._collect_changed_rows(compared_columns, match_expressions)
        accepted = 0
        if baseline is not None:
            added_df, removed_df, changed_df, accepted = _accept_baseline(
                baseline, list(keys), (added_df, removed_df, changed_df), compared_columns
            )
        return self._build_result(added_df, removed_df, changed_df, compared_columns, accepted)

    def _plan(self) -> tuple[list[str], list[pl.Expr]]:
        """Align, validate, and normalize both frames, then build the comparisons."""
        self._align_structure()
        self._validate_schema()
        self.source = self._normalize_frame(self.source, is_source=True)
        self.target = self._normalize_frame(self.target, is_source=False)
        return self._match_expressions()

    def _match_expressions(self) -> tuple[list[str], list[pl.Expr]]:
        """Build one boolean match expression per compared column."""
        # Re-read the schema here: normalization may have retyped columns.
        source_schema = self.source.collect_schema()
        target_columns = set(self.target.collect_schema().names())
        keys = set(self.config.primary_keys)

        compared: list[str] = []
        expressions: list[pl.Expr] = []
        for column in source_schema.names():
            if column in keys or column not in target_columns:
                continue
            rule = self._get_effective_rule(column)
            if rule["ignore"]:
                continue
            expr = self._build_match_expr(column, rule, source_schema[column])
            compared.append(column)
            expressions.append(expr.alias(f"{column}_is_match"))
        return compared, expressions

    def _collect_changed_rows(
        self, compared_columns: list[str], match_expressions: list[pl.Expr]
    ) -> pl.DataFrame:
        """Inner-join both sides and keep the rows where any compared column differs."""
        if not match_expressions:
            return pl.DataFrame()

        keys = self.config.primary_keys
        source = self.source.rename(lambda col: col if col in keys else f"{col}_source")
        target = self.target.rename(lambda col: col if col in keys else f"{col}_target")
        common_lazy = source.join(target, on=keys, how="inner")

        all_matched = pl.all_horizontal([f"{col}_is_match" for col in compared_columns])
        return common_lazy.with_columns(match_expressions).filter(~all_matched).collect()

    def _build_result(
        self,
        added_df: pl.DataFrame,
        removed_df: pl.DataFrame,
        changed_df: pl.DataFrame,
        compared_columns: list[str],
        accepted_count: int = 0,
    ) -> DiffResult:
        """Count totals, apply the threshold, export artifacts, and assemble the result."""
        column_mismatches = _local_column_mismatches(changed_df, compared_columns)

        # Count rows, as the warehouse's COUNT(*) does. Counting a key column
        # would skip rows whose key is null, which still count as removed or
        # added and so belong in the threshold's denominator.
        src_total = self.source.select(pl.len()).collect().item()
        tgt_total = self.target.select(pl.len()).collect().item()

        artifacts_written = _export_artifacts(
            {"added_rows": added_df, "removed_rows": removed_df, "changed_rows": changed_df},
            self.config.output_path,
            self.config.output_format,
        )
        return DiffResult(
            summary=_summary(
                self.config,
                changed_df,
                added_df,
                removed_df,
                src_total,
                tgt_total,
                column_mismatches,
                artifacts_written,
                accepted_count,
            ),
            added=added_df,
            removed=removed_df,
            changed=changed_df,
            primary_keys=tuple(self.config.primary_keys),
            compared_columns=tuple(compared_columns),
        )
