# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Core engine for data ingestion and alignment.

This module houses the I/O loaders, the `DataIngestor` for dataset preparation,
and the `DiffEngine` which performs the high-performance Polars comparisons.
"""

import importlib
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import ClassVar, Final, TypedDict

import polars as pl

from veridelta.connectors.base import PushdownSession
from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.warehouse import DatabricksConnector, SnowflakeConnector
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    ArtifactFormat,
    CastTarget,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    DiffSummary,
    IcebergConfig,
    SentinelValue,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    WhitespaceMode,
)
from veridelta.sentinels import usable_sentinels


def _optional_module(name: str) -> ModuleType | None:
    """Import an optional extra, or return None when it is not installed.

    Args:
        name (str): Module name to import.

    Returns:
        ModuleType | None: The imported module, or None on ImportError.
    """
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


fastexcel = _optional_module("fastexcel")
"""Presence probe for the `excel` extra. Polars imports this itself, but only
at call time, so checking here turns a bare ImportError into an install hint."""


class EffectiveRule(TypedDict):
    """Flattened per-column parameters after specific, pattern, and global merge."""

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


def _unusable_sentinel_error(
    column: str, dtype: pl.DataType, sentinels: Sequence[SentinelValue]
) -> ConfigError:
    """Build the error for an explicit rule whose sentinels can never match.

    Only explicit rules raise. A global `default_null_values` is expected to
    cover a mixed schema, so columns it cannot apply to are skipped instead.

    Args:
        column (str): Column the rule resolved to.
        dtype (pl.DataType): Type that column actually holds.
        sentinels (Sequence[SentinelValue]): Sentinels configured for it.

    Returns:
        ConfigError: Error naming the column, its type, and the sentinels.
    """
    return ConfigError(
        f"Column '{column}' has type {dtype}, which cannot hold any of the "
        f"null_values {list(sentinels)!r} configured for it. Quote text sentinels "
        "and leave numbers unquoted so each one matches its column type."
    )


def _reject_unzoned_timezone(column: str, dtype: pl.DataType, zone: str) -> None:
    """Enforce the local engine's `timezone` preconditions on a warehouse column.

    The compiler emits nothing for stage 6b, because Polars' `convert_time_zone`
    only rewrites a column's timezone label: every downstream cast and
    comparison still reads the underlying UTC instant, so the conversion cannot
    change a verdict. Warehouses have no per-column zone label to rewrite --
    Spark most notably, whose `TIMESTAMP` is a bare instant -- and any function
    that looks like the equivalent instead shifts the value to a wall clock,
    which would make pushdown disagree with a local run.

    What the rule does carry is a precondition, and that has to survive
    pushdown. A run that would fail locally on naive or non-temporal data must
    fail here too, rather than quietly comparing columns the local engine
    refuses to touch.

    Args:
        column (str): Column the rule resolved to.
        dtype (pl.DataType): Probed type for that side of the comparison.
        zone (str): Configured target timezone.

    Raises:
        ConfigError: If the column is not a timestamp or carries no timezone.
    """
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
            "timezone, or compare it without a timezone rule."
        )


class BaseLoader(ABC):
    """Contract for turning one `SourceConfig` into an unevaluated LazyFrame.

    Every file format Veridelta reads is a subclass registered in
    `LoaderFactory._loaders`, keyed by the `SourceType` literal. Implementations
    should prefer a Polars `scan_*` reader so the comparison graph stays lazy
    end to end; the two eager loaders (`JSONLoader`, `ExcelLoader`) say why in
    their own docstrings. `SourceConfig.options` are forwarded to the reader
    unchanged, so any keyword the underlying Polars function accepts is valid.
    """

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads data from a source into a Polars LazyFrame.

        Args:
            config (SourceConfig): The configuration detailing the path, format,
                and format-specific parsing options.

        Returns:
            pl.LazyFrame: The lazy-loaded dataset graph.
        """
        pass


class CSVLoader(BaseLoader):
    """Streaming CSV loader over `pl.scan_csv`.

    Delimiters, encodings, and header handling are all controlled through
    `SourceConfig.options`, for example `{"separator": ";"}`.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads a CSV file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_csv`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_csv(config.path, **config.options)


class ParquetLoader(BaseLoader):
    """Streaming Parquet loader over `pl.scan_parquet`.

    Accepts whatever path or glob the Polars scanner accepts. Because the scan
    stays lazy, columns dropped by `ignore` rules are never read from disk.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads a Parquet file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_parquet`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_parquet(config.path, **config.options)


class NDJSONLoader(BaseLoader):
    """Streaming loader for newline-delimited JSON over `pl.scan_ndjson`.

    One record per line is the JSON shape Polars can read incrementally, so
    this is the format to prefer over `json` for large exports.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads an NDJSON file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_ndjson`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_ndjson(config.path, **config.options)


class ArrowLoader(BaseLoader):
    """Streaming loader for Arrow IPC (Feather v2) files over `pl.scan_ipc`.

    IPC files carry their schema, so no type inference runs and the dtypes the
    engine compares are exactly the ones the writer stored.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads an Arrow IPC file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_ipc`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_ipc(config.path, **config.options)


class JSONLoader(BaseLoader):
    """Loader for a single JSON document holding an array of records.

    Polars has no lazy JSON reader, because a JSON array cannot be parsed
    incrementally the way newline-delimited records can. The file is therefore
    read whole and wrapped, which is a deliberate exception to the lazy-first
    rule. Prefer `ndjson` for anything large enough to care about.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads a JSON file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.read_json`.

        Returns:
            pl.LazyFrame: A lazy wrapper over the fully materialized document.
        """
        return pl.read_json(config.path, **config.options).lazy()


class ExcelLoader(BaseLoader):
    """Loader for Excel workbooks, backed by the optional `excel` extra.

    Like JSON, this is eager: a spreadsheet is a random-access container with
    no streaming reader.
    """

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads one worksheet into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.read_excel` (for example `sheet_name`).

        Returns:
            pl.LazyFrame: A lazy wrapper over the fully materialized sheet.

        Raises:
            ConfigError: If the `excel` extra is missing, or the options select
                more than one worksheet.
        """
        if fastexcel is None:
            raise ConfigError(
                "Reading Excel requires the optional 'excel' extra. "
                "Install it with: uv add 'veridelta[excel]'"
            )
        loaded = pl.read_excel(  # pyright: ignore[reportUnknownVariableType]
            config.path, **config.options
        )
        if not isinstance(loaded, pl.DataFrame):
            raise ConfigError(
                f"Excel source '{config.path}' resolved to multiple worksheets. "
                "Name exactly one with the 'sheet_name' or 'sheet_id' option."
            )
        return loaded.lazy()


class LoaderFactory:
    """Resolve any file or lakehouse `SourceRef` to an unevaluated LazyFrame.

    File sources are dispatched by their `format` through the `_loaders`
    registry; Delta Lake and Iceberg sources open a connector and return its
    lazy scan. Warehouse sources are refused here because their comparison
    runs as SQL pushdown via `DiffEngine.run_from_configs`, never as a local
    scan.

    The registry is the single source of truth for which formats exist. Tests
    bind it to the `SourceType` literal in both directions, so a format cannot
    be advertised without a loader or shipped without appearing in the literal.

    Attributes:
        _loaders (ClassVar[dict[str, BaseLoader]]): Format name to loader
            instance. Error messages derive their supported-format list from
            this mapping rather than a hardcoded string.
    """

    _loaders: ClassVar[dict[str, BaseLoader]] = {
        "csv": CSVLoader(),
        "parquet": ParquetLoader(),
        "json": JSONLoader(),
        "ndjson": NDJSONLoader(),
        "arrow": ArrowLoader(),
        "excel": ExcelLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: str) -> BaseLoader:
        """Retrieves the correct loader instance for the given data format.

        Args:
            source_type (str): The format identifier (e.g., 'csv', 'parquet').

        Returns:
            BaseLoader: An instantiated data loader.

        Raises:
            ConfigError: If the requested format has no loader.
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
        """Load a file or lakehouse source into an unevaluated LazyFrame.

        Args:
            config (SourceRef): File, Delta, or Iceberg configuration.

        Returns:
            pl.LazyFrame: Unevaluated scan graph.

        Raises:
            ConnectorError: If `config` is a warehouse source.
            ConfigError: If the file format has no loader.
        """
        if isinstance(config, DeltaLakeConfig):
            delta_connector = DeltaLakeConnector(config)
            delta_connector.connect()
            return delta_connector.lazyframe()
        if isinstance(config, IcebergConfig):
            iceberg_connector = IcebergConnector(config)
            iceberg_connector.connect()
            return iceberg_connector.lazyframe()
        if isinstance(config, SourceConfig):
            return cls.get_loader(config.format).load(config)
        raise ConnectorError(
            "Warehouse sources cannot be loaded via LoaderFactory; "
            "use DiffEngine.run_from_configs for SQL pushdown."
        )


def _is_warehouse(config: SourceRef) -> bool:
    """Return whether a source reference is a warehouse connection.

    Args:
        config (SourceRef): Parsed source or target configuration.

    Returns:
        bool: True for Snowflake or Databricks configs.
    """
    return isinstance(config, (SnowflakeConfig, DatabricksConfig))


def _snowflake_fingerprint(config: SnowflakeConfig) -> tuple[object, ...]:
    """Return connection identity excluding the compared table name.

    Args:
        config (SnowflakeConfig): Snowflake source or target.

    Returns:
        tuple[object, ...]: Account, user, warehouse, database, schema, secrets.
    """
    return (
        config.account,
        config.user,
        config.warehouse,
        config.database,
        config.schema_name,
        config.password,
        config.role,
    )


def _databricks_fingerprint(config: DatabricksConfig) -> tuple[object, ...]:
    """Return connection identity excluding the compared table name.

    Args:
        config (DatabricksConfig): Databricks source or target.

    Returns:
        tuple[object, ...]: Host, path, token, catalog, and schema.
    """
    return (
        config.server_hostname,
        config.http_path,
        config.access_token,
        config.catalog,
        config.schema_name,
    )


def _rename_pairs(rules: Sequence[DiffRule]) -> dict[str, str]:
    """Map each renamed source column to its target spelling.

    The first single-column rule that declares a `rename_to` for a column
    wins. Rules that also `ignore` the column count too: the pair still tells
    the target side which spelling belongs to the ignored column.

    Args:
        rules (Sequence[DiffRule]): Declared rules, in configuration order.

    Returns:
        dict[str, str]: Source name to target name.
    """
    pairs: dict[str, str] = {}
    for rule in rules:
        if rule.rename_to and len(rule.column_names) == 1:
            pairs.setdefault(rule.column_names[0], rule.rename_to)
    return pairs


def _rule_spellings(pairs: Mapping[str, str], column: str) -> tuple[str, ...]:
    """List the names a rule may use for a column, in order of precedence.

    `column` is the post-rename name that every aligned frame carries. A
    renamed column answers to its target spelling first and its source
    spelling second, so a rule written against either one governs the pair.
    When the target spelling also names a source column that is itself
    renamed away -- a swap or a chain -- a rule listing it governs that other
    column, and only the source spelling counts.

    Args:
        pairs (Mapping[str, str]): Source-to-target renames from `_rename_pairs`.
        column (str): Post-rename column name.

    Returns:
        tuple[str, ...]: Names to try for an exact-name match, in order.
    """
    source = next((src for src, tgt in pairs.items() if tgt == column), column)
    if source == column:
        return (column,)
    if pairs.get(column, column) != column:
        return (source,)
    return (column, source)


def _match_rule(rules: Sequence[DiffRule], column: str) -> DiffRule | None:
    """Resolve the single rule governing a column, exact names before patterns.

    Both engines resolve through here, keyed by the post-rename name, so a
    rule means the same thing wherever it runs: to `ignore` as much as to a
    tolerance, and to a renamed column as much as to one that kept its name.

    Args:
        rules (Sequence[DiffRule]): Declared rules, in configuration order.
        column (str): Post-rename column name to resolve.

    Returns:
        DiffRule | None: The first rule naming one of the column's spellings,
            else the first whose pattern matches it, else None.
    """
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
    """Derive the `rename_to` map and `ignore` drop set for one frame's columns.

    Shared by `DataIngestor._align_columns` and `DiffEngine._align_structure`
    for both sides. Each column is dropped only when the rule governing it
    ignores it, so an exact-name rule keeps a column a broader ignore pattern
    would otherwise remove, and a column is never both dropped and renamed.

    Args:
        rules (list[DiffRule]): Declared rules, in configuration order.
        columns (Sequence[str]): Column names present in the frame.
        rename (bool): Whether `rename_to` applies. It is a source-only mapping,
            so the target side, which already carries the post-rename
            spellings, passes False and only collects drops.

    Returns:
        tuple[dict[str, str], set[str]]: Rename map and the columns to drop.
    """
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


def _fold_rule_defaults(rule: DiffRule | None, diff: DiffConfig) -> EffectiveRule:
    """Layer one matched rule over the configuration's `default_*` settings.

    Both execution paths read from this. The local engine consumes the result
    directly, and the pushdown resolver re-materializes it as a fully specified
    `DiffRule` for the compiler. One folder means an unspecified field cannot
    mean one thing locally and another in a warehouse.

    Args:
        rule (DiffRule | None): Rule resolved by `_match_rule`, if any.
        diff (DiffConfig): Master configuration supplying the global defaults.

    Returns:
        EffectiveRule: Flattened operational parameters for one column.
    """
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
        # A global default silently skips columns it cannot apply to,
        # whereas an explicit rule that can never fire is a config error.
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
    return effective


def _enforce_pushdown_preconditions(
    effective: EffectiveRule, sides: tuple[tuple[str, pl.Schema], ...]
) -> None:
    """Fail a pushdown rule that the probed column types can never satisfy.

    Only an explicit `null_values` rule is checked. A global default is expected
    to span a mixed schema, so the compiler simply skips the columns it cannot
    hold, exactly as the local engine does. The `timezone` precondition applies
    either way: the compiler emits nothing for that stage, so the check is the
    only thing keeping a warehouse run from comparing columns the local engine
    would refuse.

    Args:
        effective (EffectiveRule): Rule with global defaults already folded in.
        sides (tuple[tuple[str, pl.Schema], ...]): Column name and probed
            schema for each side of the comparison.

    Raises:
        ConfigError: If none of the explicit sentinels fit a probed type, or a
            `timezone` rule targets a column that is not a zoned timestamp.
    """
    if effective["null_values_explicit"] and effective["null_values"]:
        for name, schema in sides:
            dtype = schema.get(name)
            if dtype is not None and not usable_sentinels(effective["null_values"], dtype):
                raise _unusable_sentinel_error(name, dtype, effective["null_values"])
    if effective["timezone"]:
        for name, schema in sides:
            dtype = schema.get(name)
            if dtype is not None:
                _reject_unzoned_timezone(name, dtype, effective["timezone"])


def _compares_numerically(effective: EffectiveRule, dtype: pl.DataType | None) -> bool:
    """Predict whether the local engine compares a source column as a number.

    `_build_match_expr` applies tolerances only when the normalized source
    column is numeric and compares everything else exactly. Pushdown never
    materializes the normalized column, so the answer is predicted from the
    probed dtype by following the same stage gates as `_normalize_value_expr`
    and `_normalize_temporal_expr`.

    Args:
        effective (EffectiveRule): Rule with global defaults folded in.
        dtype (pl.DataType | None): Probed source dtype, or None when the
            probe did not report one, in which case the tolerance is kept.

    Returns:
        bool: True when a tolerance should reach the warehouse predicate.
    """
    if effective["cast_to"] is not None:
        return _CAST_TARGETS[effective["cast_to"]].is_numeric()
    if effective["pad_zeros"] is not None:
        # Stage 5 stringifies, and a datetime_format then parses the text.
        return False
    if effective["datetime_format"] and isinstance(dtype, (pl.String, pl.Utf8)):
        return False
    return dtype is None or dtype.is_numeric()


def _resolve_pushdown_rules(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> list[DiffRule]:
    """Expand configuration into one fully specified rule per compared column.

    The compiler reads semantics from `DiffRule` alone, while the local engine
    layers each rule over the `default_*` settings. Without this expansion a
    warehouse run would ignore global tolerances and would skip every column
    lacking an explicit rule, reporting all joined rows as changed. Each probed
    source column is paired with its post-rename spelling and resolved under
    that name, exactly as the local engine resolves its aligned frames.

    Args:
        diff (DiffConfig): Master comparison rules, keys, and global defaults.
        source_schema (pl.Schema): Schema probed from the source relation.
        target_schema (pl.Schema): Schema probed from the target relation.

    Returns:
        list[DiffRule]: One rule per shared, non-key, non-ignored column, with
            global defaults already folded in.

    Raises:
        ConfigError: If a column carries an explicit `null_values` rule whose
            sentinels none of its probed types can hold, or a `timezone` rule
            the probed types cannot satisfy.
    """
    target_lookup = set(target_schema.names())
    keys = set(diff.primary_keys)
    pairs = _rename_pairs(diff.rules)

    resolved: list[DiffRule] = []
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
        # A global tolerance reaches every column, but only a numeric one may
        # compare within it; the rest compare exactly, as they do locally.
        numeric = _compares_numerically(effective, source_schema.get(column))

        resolved.append(
            DiffRule(
                column_names=[column],
                rename_to=None if aligned == column else aligned,
                absolute_tolerance=effective["abs_tol"] if numeric else 0.0,
                relative_tolerance=effective["rel_tol"] if numeric else 0.0,
                treat_null_as_equal=effective["treat_null"],
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
            )
        )
    return resolved


def _column_mismatches_from_frame(frame: pl.DataFrame) -> dict[str, int]:
    """Reduce the single-row mismatch tally to positive per-column counts.

    Args:
        frame (pl.DataFrame): Result of `compile_column_mismatch_query`.

    Returns:
        dict[str, int]: Columns with at least one mismatch. Columns that fully
            matched are dropped, matching the local engine's filtering.

    Raises:
        ConnectorError: If the aggregate is not a single row of numbers.
    """
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
    """Count, per compared column, how many changed rows failed its match flag.

    Args:
        changed (pl.DataFrame): Changed rows carrying one `<column>_is_match`
            flag per compared column.
        compared_columns (list[str]): Columns that were evaluated.

    Returns:
        dict[str, int]: Columns with at least one mismatch. Fully matched
            columns are dropped, mirroring `_column_mismatches_from_frame`.
    """
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
"""`cast_to` name to the dtype it resolves to. An explicit table rather than a
`getattr(pl, ...)` lookup, which returned None for anything unrecognized and
skipped the cast without a word."""


_ARTIFACT_WRITERS: Final[dict[ArtifactFormat, Callable[[pl.DataFrame, Path], None]]] = {
    "csv": lambda frame, path: frame.write_csv(path),
    "parquet": lambda frame, path: frame.write_parquet(path),
    "json": lambda frame, path: frame.write_json(path),
    "ndjson": lambda frame, path: frame.write_ndjson(path),
    "arrow": lambda frame, path: frame.write_ipc(path),
}
"""Artifact format to writer. Single source of truth for the guard and dispatch,
so a format can never be accepted without something actually writing it.

Deliberately not the same set as `LoaderFactory._loaders`: Excel is readable
through the `excel` extra but not writable, since emitting a workbook needs a
second dependency that a discrepancy dump does not justify."""


def _export_artifacts(
    frames: dict[str, pl.DataFrame], output_path: str, output_format: ArtifactFormat
) -> bool:
    """Persist non-empty discrepancy frames to the configured directory.

    Shared by the local and pushdown paths so both report `artifacts_written`
    from the same rules about what actually reaches disk.

    Args:
        frames (dict[str, pl.DataFrame]): Artifact base name mapped to its rows.
        output_path (str): Directory to create and write into.
        output_format (ArtifactFormat): Format to write, which must have an
            entry in `_ARTIFACT_WRITERS`.

    Returns:
        bool: True when at least one file was written. Empty frames are skipped,
            so a clean comparison leaves no artifacts behind.

    Raises:
        ConfigError: If `output_format` has no writer.
    """
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
        _ARTIFACT_WRITERS[output_format](frame, out_dir / f"{name}.{output_format}")
        written = True
    return written


def _summary_from_pushdown(
    diff: DiffConfig,
    changed: pl.DataFrame,
    added: pl.DataFrame,
    removed: pl.DataFrame,
    source_total: int,
    target_total: int,
    column_mismatches: dict[str, int],
    artifacts_written: bool,
) -> DiffSummary:
    """Map mismatch and anti-join pushdown rows to a DiffSummary.

    Args:
        diff (DiffConfig): Comparison rules including `threshold`.
        changed (pl.DataFrame): Inner-join mismatch rows.
        added (pl.DataFrame): Target-only anti-join rows.
        removed (pl.DataFrame): Source-only anti-join rows.
        source_total (int): `COUNT(*)` of the source relation.
        target_total (int): `COUNT(*)` of the target relation.
        column_mismatches (dict[str, int]): Per-column drift counts from the tally.
        artifacts_written (bool): Whether key artifacts reached disk.

    Returns:
        DiffSummary: Counts from the three pushdown result heights, ratioed
            against the source total exactly as the local engine does.
    """
    changed_count = changed.height
    added_count = added.height
    removed_count = removed.height
    total_mismatches = added_count + removed_count + changed_count
    mismatch_ratio = float(total_mismatches) / float(max(source_total, 1))
    return DiffSummary(
        total_rows_source=source_total,
        total_rows_target=target_total,
        added_count=added_count,
        removed_count=removed_count,
        changed_count=changed_count,
        column_mismatches=column_mismatches,
        is_match=mismatch_ratio <= diff.threshold,
        report_limit=diff.report_top_columns_limit,
        artifacts_written=artifacts_written,
    )


def _pushdown_row_count(
    connector: PushdownSession,
    table: str,
) -> int:
    """Collect a single `COUNT(*)` scalar from a warehouse relation.

    Args:
        connector (PushdownSession): Connected session with a matching compiler.
        table (str): Relation to count.

    Returns:
        int: Total row count for the relation.

    Raises:
        ConnectorError: If the warehouse does not return a single numeric value.
    """
    statement = connector.compiler.compile_count_query(table)
    frame = connector.execute_pushdown(statement, query_type="count").collect()
    if frame.height != 1 or frame.width != 1:
        raise ConnectorError(f"Row count query for '{table}' did not return a single value.")
    try:
        # Drivers may surface COUNT(*) as an integer, float, or decimal scalar.
        return int(frame.item())
    except (TypeError, ValueError) as exc:
        raise ConnectorError(
            f"Row count query for '{table}' returned a non-numeric value."
        ) from exc


def _validate_pushdown_schema(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> tuple[pl.Schema, pl.Schema]:
    """Enforce `schema_mode` against warehouse relations before comparing them.

    Zero-row probes expose the stored column names, so misconfigured keys surface
    as a `ConfigError` instead of a driver failure mid-comparison. Names are
    compared exactly as the compiler quotes them, without case folding.

    Args:
        connector (PushdownSession): Connected session with a matching compiler.
        source_table (str): Source relation name.
        target_table (str): Target relation name.
        diff (DiffConfig): Master comparison rules and keys.

    Returns:
        tuple[pl.Schema, pl.Schema]: Raw source and target schemas, before any
            rename or drop. The driver's Arrow result carries dtypes as well as
            names, and the compiler needs both to filter null sentinels.

    Raises:
        ConfigError: If primary keys are missing, a key exists on the source
            only under a `rename_to` spelling, or schema constraints are violated.
    """
    source_probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(source_table), query_type="schema"
    )
    target_probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(target_table), query_type="schema"
    )
    DiffEngine.validate_schemas(diff, source_probe, target_probe)
    source_schema = source_probe.collect_schema()
    # Validation aligns names first, so a renamed key passes it. The compiled
    # joins read stored names, though, and would reference a missing column.
    renamed_keys = [key for key in diff.primary_keys if key not in source_schema.names()]
    if renamed_keys:
        raise ConfigError(
            f"Primary keys {renamed_keys} exist in the source only through rename_to. "
            "Warehouse pushdown joins on stored column names, so renaming a key "
            "is supported for local runs only."
        )
    return source_schema, target_probe.collect_schema()


def _collect_pushdown_summary(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> DiffResult:
    """Compile and collect the warehouse count, mismatch, and anti-join queries.

    Args:
        connector (PushdownSession): Connected warehouse session whose compiler
            matches the dialect.
        source_table (str): Source relation name.
        target_table (str): Target relation name.
        diff (DiffConfig): Master comparison rules and keys.

    Returns:
        DiffResult: Heights from the three collected LazyFrames, ratioed against
            the source relation's total row count, alongside the primary-key
            frames themselves. Flagged `keys_only`, since the comparison SQL
            never projects values.

    Raises:
        ConfigError: If the probed relations violate `schema_mode` or omit a
            primary key.
        ConnectorError: If a rule uses a field the compiler cannot express or the
            warehouse returns a malformed aggregate.
    """
    source_schema, target_schema = _validate_pushdown_schema(
        connector, source_table, target_table, diff
    )
    rules = _resolve_pushdown_rules(diff, source_schema, target_schema)

    source_total = _pushdown_row_count(connector, source_table)
    target_total = _pushdown_row_count(connector, target_table)
    mismatch_sql = connector.compiler.compile_query(
        source_table,
        target_table,
        diff.primary_keys,
        rules,
        source_types=source_schema,
        target_types=target_schema,
    )
    added_sql = connector.compiler.compile_added_query(
        source_table, target_table, diff.primary_keys
    )
    missing_sql = connector.compiler.compile_missing_query(
        source_table, target_table, diff.primary_keys
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
    )
    if columns_sql is not None:
        tally = connector.execute_pushdown(columns_sql, query_type="columns").collect()
        column_mismatches = _column_mismatches_from_frame(tally)

    artifacts_written = False
    if isinstance(diff.output_path, str):
        # Pushdown projects primary keys only, never full rows, so the suffix
        # keeps these files from being mistaken for local artifacts.
        artifacts_written = _export_artifacts(
            {
                "added_rows_pks_only": added,
                "removed_rows_pks_only": removed,
                "changed_rows_pks_only": changed,
            },
            diff.output_path,
            diff.output_format,
        )

    return DiffResult(
        summary=_summary_from_pushdown(
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
    )


def _run_warehouse_pushdown(diff: DiffConfig, source: SourceRef, target: SourceRef) -> DiffResult:
    """Execute same-warehouse SQL pushdown or raise for unsupported pairings.

    Args:
        diff (DiffConfig): Master comparison rules and keys.
        source (SourceRef): Source configuration.
        target (SourceRef): Target configuration.

    Returns:
        DiffResult: Mismatch and anti-join counts from the pushdown statements,
            with the primary-key frames they were derived from.

    Raises:
        ConfigError: If the probed relations violate `schema_mode`.
        ConnectorError: If backends are mixed, dialects differ, or connections
            do not share a fingerprint.
    """
    source_wh = _is_warehouse(source)
    target_wh = _is_warehouse(target)
    if source_wh != target_wh:
        raise ConnectorError("Mixed file/lakehouse and warehouse backends are unsupported.")
    if type(source) is not type(target):
        raise ConnectorError(
            "Cross-dialect warehouse pushdown is unsupported. "
            "Source and target must use the same Snowflake or Databricks connection."
        )
    if isinstance(source, SnowflakeConfig) and isinstance(target, SnowflakeConfig):
        if _snowflake_fingerprint(source) != _snowflake_fingerprint(target):
            raise ConnectorError(
                "Cross-account warehouse pushdown is unsupported. "
                "Source and target Snowflake connections must match."
            )
        snowflake = SnowflakeConnector(source)
        snowflake.connect()
        try:
            return _collect_pushdown_summary(snowflake, source.table, target.table, diff)
        finally:
            snowflake.close()
    if isinstance(source, DatabricksConfig) and isinstance(target, DatabricksConfig):
        if _databricks_fingerprint(source) != _databricks_fingerprint(target):
            raise ConnectorError(
                "Cross-account warehouse pushdown is unsupported. "
                "Source and target Databricks connections must match."
            )
        databricks = DatabricksConnector(source)
        databricks.connect()
        try:
            return _collect_pushdown_summary(databricks, source.table, target.table, diff)
        finally:
            databricks.close()
    raise ConnectorError("Mixed file/lakehouse and warehouse backends are unsupported.")


class DataIngestor:
    """Coordinates the loading, renaming, and structural alignment of datasets.

    This class prepares raw external data for comparison by normalizing headers
    and dropping ignored columns before handing them off to the DiffEngine.
    """

    def __init__(
        self, diff_config: DiffConfig, source_config: SourceRef, target_config: SourceRef
    ) -> None:
        """Initializes the ingestor.

        Args:
            diff_config (DiffConfig): The master comparison configuration.
            source_config (SourceRef): File or lakehouse settings for the source.
            target_config (SourceRef): File or lakehouse settings for the target.
        """
        self.config = diff_config
        self.source_config = source_config
        self.target_config = target_config

    def _normalize_headers(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Standardizes column names based on the master configuration.

        Args:
            df (pl.LazyFrame): The raw lazy dataframe.

        Returns:
            pl.LazyFrame: A dataframe with lowercased/stripped headers if enabled.
        """
        if not self.config.normalize_column_names:
            return df

        cols = df.collect_schema().names()
        rename_map = {col: col.strip().lower() for col in cols}
        return df.rename(rename_map)

    def _align_columns(self, df: pl.LazyFrame, is_source: bool = True) -> pl.LazyFrame:
        """Applies configured renames and drops ignored columns.

        Args:
            df (pl.LazyFrame): The lazy dataframe to process.
            is_source (bool): True if processing the source data, False for target.

        Returns:
            pl.LazyFrame: The structurally aligned lazy dataframe.
        """
        rename_map, to_drop = _alignment_maps(
            self.config.rules, df.collect_schema().names(), rename=is_source
        )
        return df.drop(list(to_drop)).rename(rename_map)

    def get_dataframes(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Loads and aligns both source and target datasets.

        Returns:
            tuple[pl.LazyFrame, pl.LazyFrame]: The prepared (source_df, target_df).
        """
        source_df = (
            LoaderFactory.load(self.source_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=True)
        )

        target_df = (
            LoaderFactory.load(self.target_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=False)
        )

        return source_df, target_df


class DiffEngine:
    """Compare two datasets on their primary keys and report what differs.

    The engine takes two Polars `LazyFrame` inputs and a `DiffConfig`, applies
    the nine-stage `DiffRule` pipeline to each side, then joins on the primary
    keys to classify rows as added (target-only), removed (source-only), or
    changed (present on both sides with at least one compared column
    differing). Nothing is materialized until `run()` collects the joins, so
    the source frames can be `scan_*` graphs over files far larger than memory.

    Three entry points cover the usual situations:

    - `DiffEngine(config, source, target).run()` for frames you already hold.
      In-memory `DataFrame` inputs must be wrapped with `.lazy()` first.
    - `DiffEngine.run_from_configs(diff, source, target)` for `SourceRef`
      pairs from YAML. File and lakehouse pairs load through `DataIngestor`
      and run locally; same-warehouse pairs compile to SQL pushdown instead.
    - `DiffEngine.validate_schemas(...)` to enforce `schema_mode` and primary
      key presence on metadata alone, before any rows are read.

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
        """Initializes the engine with datasets already aligned by the DataIngestor.

        Args:
            config (DiffConfig): The master validation rules configuration.
            source_df (pl.LazyFrame): The aligned 'Left' (Legacy) dataset.
            target_df (pl.LazyFrame): The aligned 'Right' (Modern) dataset.
        """
        self.config = config
        self.source = source_df
        self.target = target_df

    @classmethod
    def run_from_configs(cls, diff: DiffConfig, source: SourceRef, target: SourceRef) -> DiffResult:
        """Route a comparison to warehouse pushdown or local Polars evaluation.

        Args:
            diff (DiffConfig): Master comparison rules and keys.
            source (SourceRef): Source file, lakehouse, or warehouse config.
            target (SourceRef): Target file, lakehouse, or warehouse config.

        Returns:
            DiffResult: Pushdown mismatch and anti-join counts, or a full Polars
                diff for file and lakehouse pairs.

        Raises:
            ConfigError: If primary keys are missing or `schema_mode` is violated.
            ConnectorError: If warehouse backends are mixed or connections differ.
        """
        source_is_warehouse = _is_warehouse(source)
        target_is_warehouse = _is_warehouse(target)
        if source_is_warehouse or target_is_warehouse:
            return _run_warehouse_pushdown(diff, source, target)

        ingestor = DataIngestor(diff, source, target)
        source_df, target_df = ingestor.get_dataframes()
        return cls(diff, source_df, target_df).run()

    @classmethod
    def validate_schemas(
        cls, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> None:
        """Align structure and enforce `SchemaMode` without comparing any rows.

        Operates on schema metadata only, so callers may pass zero-row frames.

        Args:
            config (DiffConfig): The master validation rules configuration.
            source_df (pl.LazyFrame): Source frame or column probe.
            target_df (pl.LazyFrame): Target frame or column probe.

        Raises:
            ConfigError: If primary keys are missing or schema constraints are violated.
        """
        engine = cls(config, source_df, target_df)
        engine._align_structure()
        engine._validate_schema()

    def _get_effective_rule(self, col_name: str) -> EffectiveRule:
        """Resolves all rules (Specific > Pattern > Global) into a unified dictionary.

        Args:
            col_name (str): The name of the column to resolve rules for.

        Returns:
            EffectiveRule: A flattened dictionary of operational parameters.
        """
        return _fold_rule_defaults(_match_rule(self.config.rules, col_name), self.config)

    def _check_uniqueness(self) -> None:
        """Verifies that primary keys are unique in both datasets.

        Raises:
            DataIntegrityError: If duplicates are found in the primary keys of either
                dataset, preventing join explosions.
        """
        from veridelta.exceptions import DataIntegrityError

        pks = self.config.primary_keys

        src_pks = self.source.select(pks).collect()
        if src_pks.is_duplicated().any():
            dupes = src_pks.filter(src_pks.is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in SOURCE dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

        tgt_pks = self.target.select(pks).collect()
        if tgt_pks.is_duplicated().any():
            dupes = tgt_pks.filter(tgt_pks.is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in TARGET dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

    def _normalize_frame(self, frame: pl.LazyFrame, *, is_source: bool) -> pl.LazyFrame:
        """Apply stages 1 through 7 of the canonical transform order to one dataset.

        Normalization runs per frame before any join, so primary keys are treated
        exactly like compared columns and a rule naming a target-only column still
        fires. See `DiffRule` for the authoritative stage ordering.

        Args:
            frame (pl.LazyFrame): Structurally aligned source or target dataset.
            is_source (bool): True when normalizing the source, which is the only
                side `value_map` rewrites.

        Returns:
            pl.LazyFrame: Frame with the normalization expressions appended lazily.

        Raises:
            ConfigError: If `timezone` targets a column that is not timezone-aware
                or names a zone Polars does not recognize.
        """
        schema = frame.collect_schema()
        rules = {column: self._get_effective_rule(column) for column in schema.names()}

        value_exprs: list[pl.Expr] = []
        for column, rule in rules.items():
            if rule["ignore"]:
                continue
            value_expr = self._normalize_value_expr(
                column, rule, schema[column], is_source=is_source
            )
            if value_expr is not None:
                value_exprs.append(value_expr)
        if value_exprs:
            frame = frame.with_columns(value_exprs)

        if not any(rule["timezone"] or rule["cast_to"] for rule in rules.values()):
            return frame

        # Stage 6a can turn a String column into a Datetime, so the timezone guard
        # has to inspect the post-parse schema rather than the original one.
        parsed_schema = frame.collect_schema()
        temporal_exprs: list[pl.Expr] = []
        for column, rule in rules.items():
            if rule["ignore"]:
                continue
            temporal_expr = self._normalize_temporal_expr(column, rule, parsed_schema[column])
            if temporal_expr is not None:
                temporal_exprs.append(temporal_expr)
        if temporal_exprs:
            frame = frame.with_columns(temporal_exprs)
        return frame

    def _normalize_value_expr(
        self, column: str, rule: EffectiveRule, dtype: pl.DataType, *, is_source: bool
    ) -> pl.Expr | None:
        """Build stages 1 through 6a for one column: sentinels through datetime parsing.

        Text stages are gated on the column actually holding text, so a global
        `default_null_values` or `default_whitespace_mode` cannot fail a run that
        also contains numeric or temporal columns.

        Args:
            column (str): Column being normalized.
            rule (EffectiveRule): Parameters resolved by `_get_effective_rule`.
            dtype (pl.DataType): Current dtype of the column within its own frame.
            is_source (bool): True when normalizing the source frame.

        Returns:
            pl.Expr | None: Aliased expression, or None when no stage applies.
        """
        expr = pl.col(column)
        applied = False
        # Deliberately narrower than `is_text_dtype`: `is_in` accepts Categorical
        # and Enum, but the `.str` namespace used below rejects both.
        is_text = isinstance(dtype, (pl.String, pl.Utf8))

        sentinels = usable_sentinels(rule["null_values"], dtype)
        if sentinels:
            expr = pl.when(expr.is_in(sentinels)).then(None).otherwise(expr)
            applied = True
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
            expr = expr.str.strptime(pl.Datetime, format=rule["datetime_format"], strict=False)
            applied = True

        return expr.alias(column) if applied else None

    @staticmethod
    def _normalize_text_expr(
        expr: pl.Expr, rule: EffectiveRule, *, is_source: bool
    ) -> tuple[pl.Expr, bool]:
        """Build stages 2 through 4 for a text column: regex, whitespace, case, value map.

        Mirrors the compiler's `_apply_regex_replace`, `_apply_whitespace`,
        `_apply_case`, and `_apply_value_map` so the two engines keep the same
        stage boundaries. Callers gate on the column holding text; the `.str`
        namespace used here rejects Categorical and Enum.

        Args:
            expr (pl.Expr): Expression produced by the sentinel stage.
            rule (EffectiveRule): Parameters resolved by `_get_effective_rule`.
            is_source (bool): True when normalizing the source frame, the only
                side `value_map` rewrites.

        Returns:
            tuple[pl.Expr, bool]: The transformed expression and whether any
                stage applied.
        """
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
    ) -> pl.Expr | None:
        """Build stages 6b and 7 for one column: timezone conversion, then cast.

        Args:
            column (str): Column being normalized.
            rule (EffectiveRule): Parameters resolved by `_get_effective_rule`.
            dtype (pl.DataType): Dtype after the value stages have been applied.

        Returns:
            pl.Expr | None: Aliased expression, or None when no stage applies.

        Raises:
            ConfigError: If `timezone` cannot be applied to the column.
        """
        expr = pl.col(column)
        applied = False

        if rule["timezone"]:
            expr = self._convert_time_zone(column, expr, dtype, rule["timezone"])
            applied = True

        if rule["cast_to"]:
            expr = expr.cast(_CAST_TARGETS[rule["cast_to"]])
            applied = True

        return expr.alias(column) if applied else None

    def _convert_time_zone(
        self, column: str, expr: pl.Expr, dtype: pl.DataType, zone: str
    ) -> pl.Expr:
        """Convert a timezone-aware column to `zone`, refusing to guess for naive data.

        Polars would happily read a naive timestamp as UTC here, which silently
        shifts every value by the real offset, so naive input is rejected outright.

        Args:
            column (str): Column being converted, used for error messages.
            expr (pl.Expr): Expression produced by the earlier stages.
            dtype (pl.DataType): Dtype of the column after datetime parsing.
            zone (str): Target timezone name.

        Returns:
            pl.Expr: Expression converted to the requested zone.

        Raises:
            ConfigError: If the column is not a timestamp, carries no timezone, or
                the zone name is unknown.
        """
        if not isinstance(dtype, pl.Datetime):
            raise ConfigError(
                f"Column '{column}' sets timezone='{zone}' but holds {dtype}, not a "
                "timestamp. Parse it with datetime_format first."
            )
        if dtype.time_zone is None:
            raise ConfigError(
                f"Column '{column}' sets timezone='{zone}' but its timestamps are "
                "timezone-naive. Veridelta will not assume an origin zone, because "
                "guessing wrong shifts every value silently. Supply timezone-aware "
                "data, or use a datetime_format carrying an offset such as '%z'."
            )
        try:
            return expr.dt.convert_time_zone(zone)
        except pl.exceptions.ComputeError as exc:
            raise ConfigError(f"Column '{column}' sets an unusable timezone. {exc}") from exc

    def _build_match_expr(self, col_name: str, rule: EffectiveRule, dtype: pl.DataType) -> pl.Expr:
        """Builds stages 8 and 9 of the transform order: comparison and null equality.

        Both sides have already been normalized by `_normalize_frame`, so this
        evaluates the aligned pair without applying any further transformations.

        Implicit Type Alignment:
            Polars is strictly typed. Comparing a Float64 to an Int64 or String raises
            a ComputeError. If a schema drift is detected between Source and Target:
            - If `strict_types=True`: The mismatch is immediately evaluated as `False`.
            - If `strict_types=False` (Default): The target column is dynamically soft-cast
              to the source's data type purely for the mathematical evaluation.

        Args:
            col_name (str): The column being compared.
            rule (EffectiveRule): The unified rules to apply.
            dtype (pl.DataType): The data type of the source column.

        Returns:
            pl.Expr: A boolean expression evaluating to True where the row values match.
        """
        src = pl.col(f"{col_name}_source")
        tgt = pl.col(f"{col_name}_target")

        tgt_dtype = self.target.collect_schema().get(col_name)

        if dtype != tgt_dtype:
            if self.config.strict_types:
                val_match = pl.lit(False)
                if rule["treat_null"]:
                    null_match = src.is_null() & tgt.is_null()
                    return (val_match | null_match).fill_null(False)
                return val_match
            else:
                tgt = tgt.cast(dtype, strict=False)

        if dtype.is_numeric() and (rule["abs_tol"] != 0.0 or rule["rel_tol"] != 0.0):
            abs_diff = (tgt - src).abs()
            threshold = rule["abs_tol"] + (rule["rel_tol"] * src.abs())
            val_match = abs_diff <= threshold
        else:
            val_match = src == tgt

        if rule["treat_null"]:
            null_match = src.is_null() & tgt.is_null()
            return (val_match | null_match).fill_null(False)

        return val_match.fill_null(False)

    def _align_structure(self) -> None:
        """Perform structural normalization to reconcile asymmetrical schemas.

        Maps Source headers to Target counterparts and drops excluded fields based
        on declarative rules. This establishes the Target system's schema as the
        authoritative state, ensuring subsequent validation and comparison operate
        against a single source of truth.

        Side Effects:
            Mutates `self.source` and `self.target` to reflect the aligned structure.

        Note:
            Mandatory prerequisite for `_validate_schema`. Validating raw data
            metadata before alignment results in `ConfigError` during migrations.
        """
        src_cols = self.source.collect_schema().names()
        tgt_cols = self.target.collect_schema().names()

        src_rename, src_drop = _alignment_maps(self.config.rules, src_cols, rename=True)
        # The target already carries the post-rename spellings, which the
        # resolver matches under either name.
        _, tgt_drop = _alignment_maps(self.config.rules, tgt_cols, rename=False)

        self.source = self.source.drop(list(src_drop)).rename(src_rename)
        self.target = self.target.drop(list(tgt_drop))

    def _validate_schema(self) -> None:
        """Enforces the configured SchemaMode before comparison.

        Raises:
            ConfigError: If primary keys are missing or schema constraints are violated.
        """
        source_cols = set(self.source.collect_schema().names())
        target_cols = set(self.target.collect_schema().names())
        pks = set(self.config.primary_keys)

        if not pks.issubset(source_cols):
            raise ConfigError(
                f"Primary keys missing in SOURCE after alignment: {pks - source_cols}"
            )
        if not pks.issubset(target_cols):
            raise ConfigError(f"Primary keys missing in TARGET: {pks - target_cols}")

        if self.config.schema_mode == "exact" and source_cols != target_cols:
            raise ConfigError(
                f"EXACT schema match failed.\nSource: {source_cols}\nTarget: {target_cols}"
            )

        elif self.config.schema_mode == "allow_additions":
            missing_in_target = source_cols - target_cols
            if missing_in_target:
                raise ConfigError(f"Target is missing required source columns: {missing_in_target}")

        elif self.config.schema_mode == "allow_removals":
            extra_in_target = target_cols - source_cols
            if extra_in_target:
                raise ConfigError(
                    f"Target contains unauthorized additional columns: {extra_in_target}"
                )

    def run(self) -> DiffResult:
        """Execute the end-to-end dataset comparison pipeline lazily.

        Builds an optimized Polars computation graph (DAG) to guarantee deterministic
        alignment, preventing compute errors and memory exhaustion on large datasets.
        Data is only materialized into memory when absolutely necessary for execution.

        Execution Pipeline:
            1. Structural Alignment: Maps and prunes schemas to establish the
               Target as the authoritative structural contract.
            2. Validation: Asserts primary key existence and enforces the `SchemaMode`.
            3. Semantic Normalization: Applies stages 1-7 of the `DiffRule` transform
               order to each dataset independently, then asserts key uniqueness on
               the normalized keys (triggering a localized collection).
            4. Relational Joins: Formulates the lazy anti-joins ('Added', 'Removed')
               and inner-joins ('Changed') to isolate discrepancies.
            5. Graph Execution: Executes the computation DAG via `.collect()` to
               evaluate vectorized match expressions and compute exact row counts.
            6. Artifact Persistence: Exports the materialized discrepancy dataframes
               to the configured storage backend, if requested.

        Returns:
            DiffResult: Execution report detailing match status, discrepancy counts,
                and column-level drift metrics.

        Raises:
            ConfigError: If schema constraints or primary keys are violated
                post-alignment, or the requested artifact export format has no writer.
            DataIntegrityError: If duplicate primary keys prevent deterministic joins.
        """
        self._align_structure()
        self._validate_schema()

        self.source = self._normalize_frame(self.source, is_source=True)
        self.target = self._normalize_frame(self.target, is_source=False)

        # Runs after normalization: case folding or sentinel coercion on a key
        # column can collapse distinct rows into duplicates, and that must fail
        # here rather than silently exploding the joins below.
        self._check_uniqueness()

        added_df, removed_df = self._collect_key_discrepancies()
        compared_columns, match_expressions = self._match_expressions()
        changed_df = self._collect_changed_rows(compared_columns, match_expressions)
        return self._build_result(added_df, removed_df, changed_df, compared_columns)

    def _collect_key_discrepancies(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Materialize the rows present on only one side of the comparison.

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: Added rows (target-only) and
                removed rows (source-only), joined on the normalized keys.
        """
        keys = self.config.primary_keys
        added_lazy = self.target.join(self.source, on=keys, how="anti")
        removed_lazy = self.source.join(self.target, on=keys, how="anti")
        return added_lazy.collect(), removed_lazy.collect()

    def _match_expressions(self) -> tuple[list[str], list[pl.Expr]]:
        """Build one boolean match expression per compared column.

        Columns are compared when they survive alignment on both sides, are not
        primary keys, and are not ignored by their effective rule. Each
        expression is aliased `<column>_is_match` so the tally can find it.

        Returns:
            tuple[list[str], list[pl.Expr]]: Compared column names, in source
                order, and their parallel match expressions.
        """
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
        """Inner-join both sides and keep the rows where any compared column differs.

        Non-key columns are suffixed `_source` / `_target` so the two values sit
        side by side in the collected frame together with their match flags.

        Args:
            compared_columns (list[str]): Columns that carry a match expression.
            match_expressions (list[pl.Expr]): Expressions from `_match_expressions`.

        Returns:
            pl.DataFrame: Changed rows, or an empty frame when nothing is compared.
        """
        if not match_expressions:
            return pl.DataFrame()

        keys = self.config.primary_keys
        src_renamed = self.source.rename(
            {
                col: f"{col}_source"
                for col in self.source.collect_schema().names()
                if col not in keys
            }
        )
        tgt_renamed = self.target.rename(
            {
                col: f"{col}_target"
                for col in self.target.collect_schema().names()
                if col not in keys
            }
        )
        common_lazy = src_renamed.join(tgt_renamed, on=keys, how="inner")

        all_matched = pl.all_horizontal([f"{col}_is_match" for col in compared_columns])
        return common_lazy.with_columns(match_expressions).filter(~all_matched).collect()

    def _build_result(
        self,
        added_df: pl.DataFrame,
        removed_df: pl.DataFrame,
        changed_df: pl.DataFrame,
        compared_columns: list[str],
    ) -> DiffResult:
        """Count totals, apply the threshold, export artifacts, and assemble the result.

        Args:
            added_df (pl.DataFrame): Target-only rows.
            removed_df (pl.DataFrame): Source-only rows.
            changed_df (pl.DataFrame): Rows whose compared values differ.
            compared_columns (list[str]): Columns that were evaluated.

        Returns:
            DiffResult: Summary plus the materialized discrepancy frames.

        Raises:
            ConfigError: If the requested artifact export format has no writer.
        """
        column_mismatches = _local_column_mismatches(changed_df, compared_columns)

        pk_col = self.config.primary_keys[0]
        src_total = self.source.select(pl.col(pk_col).count()).collect().item()
        tgt_total = self.target.select(pl.col(pk_col).count()).collect().item()

        total_mismatches = added_df.height + removed_df.height + changed_df.height
        is_match = total_mismatches / max(src_total, 1) <= self.config.threshold

        artifacts_written = False
        if isinstance(self.config.output_path, str):
            artifacts_written = _export_artifacts(
                {
                    "added_rows": added_df,
                    "removed_rows": removed_df,
                    "changed_rows": changed_df,
                },
                self.config.output_path,
                self.config.output_format,
            )

        return DiffResult(
            summary=DiffSummary(
                total_rows_source=src_total,
                total_rows_target=tgt_total,
                added_count=added_df.height,
                removed_count=removed_df.height,
                changed_count=changed_df.height,
                column_mismatches=column_mismatches,
                is_match=is_match,
                report_limit=self.config.report_top_columns_limit,
                artifacts_written=artifacts_written,
            ),
            added=added_df,
            removed=removed_df,
            changed=changed_df,
            primary_keys=tuple(self.config.primary_keys),
            compared_columns=tuple(compared_columns),
        )
