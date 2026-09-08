# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Core engine for data ingestion and alignment.

This module houses the I/O loaders, the `DataIngestor` for dataset preparation,
and the `DiffEngine` which performs the high-performance Polars comparisons.
"""

import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

import polars as pl

from veridelta.connectors.lakehouse import DeltaLakeConnector, IcebergConnector
from veridelta.connectors.warehouse import DatabricksConnector, SnowflakeConnector
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import (
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffSummary,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
)


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

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
    """Loader for CSV files utilizing the fast Polars CSV scanner."""

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
    """Loader for Parquet files utilizing the Polars Parquet engine."""

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Loads a Parquet file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_parquet`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_parquet(config.path, **config.options)


class LoaderFactory:
    """Factory to return the appropriate loader based on the configured SourceType."""

    _loaders: ClassVar[dict[str, BaseLoader]] = {
        "csv": CSVLoader(),
        "parquet": ParquetLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: str) -> BaseLoader:
        """Retrieves the correct loader instance for the given data format.

        Args:
            source_type (str): The format identifier (e.g., 'csv', 'parquet').

        Returns:
            BaseLoader: An instantiated data loader.

        Raises:
            NotImplementedError: If the requested format is not yet supported.
        """
        loader = cls._loaders.get(source_type)
        if not loader:
            raise NotImplementedError(
                f"Support for '{source_type}' is planned but not yet implemented."
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
            NotImplementedError: If the file format has no loader.
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


def _summary_from_pushdown(
    diff: DiffConfig,
    changed: pl.DataFrame,
    added: pl.DataFrame,
    removed: pl.DataFrame,
    source_total: int,
    target_total: int,
) -> DiffSummary:
    """Map mismatch and anti-join pushdown rows to a DiffSummary.

    Args:
        diff (DiffConfig): Comparison rules including `threshold`.
        changed (pl.DataFrame): Inner-join mismatch rows.
        added (pl.DataFrame): Target-only anti-join rows.
        removed (pl.DataFrame): Source-only anti-join rows.
        source_total (int): `COUNT(*)` of the source relation.
        target_total (int): `COUNT(*)` of the target relation.

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
        column_mismatches={},
        is_match=mismatch_ratio <= diff.threshold,
        report_limit=diff.report_top_columns_limit,
    )


def _pushdown_row_count(
    connector: SnowflakeConnector | DatabricksConnector,
    table: str,
) -> int:
    """Collect a single `COUNT(*)` scalar from a warehouse relation.

    Args:
        connector (SnowflakeConnector | DatabricksConnector): Connected session.
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
    connector: SnowflakeConnector | DatabricksConnector,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> None:
    """Enforce `schema_mode` against warehouse relations before comparing them.

    Zero-row probes expose the stored column names, so misconfigured keys surface
    as a `ConfigError` instead of a driver failure mid-comparison. Names are
    compared exactly as the compiler quotes them, without case folding.

    Args:
        connector (SnowflakeConnector | DatabricksConnector): Connected session.
        source_table (str): Source relation name.
        target_table (str): Target relation name.
        diff (DiffConfig): Master comparison rules and keys.

    Raises:
        ConfigError: If primary keys are missing or schema constraints are violated.
    """
    source_probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(source_table), query_type="schema"
    )
    target_probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(target_table), query_type="schema"
    )
    DiffEngine.validate_schemas(diff, source_probe, target_probe)


def _collect_pushdown_summary(
    connector: SnowflakeConnector | DatabricksConnector,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> DiffSummary:
    """Compile and collect the warehouse count, mismatch, and anti-join queries.

    Args:
        connector (SnowflakeConnector | DatabricksConnector): Connected warehouse
            session whose compiler matches the dialect.
        source_table (str): Source relation name.
        target_table (str): Target relation name.
        diff (DiffConfig): Master comparison rules and keys.

    Returns:
        DiffSummary: Heights from the three collected LazyFrames, ratioed against
            the source relation's total row count.

    Raises:
        ConfigError: If the probed relations violate `schema_mode` or omit a
            primary key.
    """
    _validate_pushdown_schema(connector, source_table, target_table, diff)
    source_total = _pushdown_row_count(connector, source_table)
    target_total = _pushdown_row_count(connector, target_table)
    mismatch_sql = connector.compiler.compile_query(
        source_table, target_table, diff.primary_keys, diff.rules
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
    return _summary_from_pushdown(diff, changed, added, removed, source_total, target_total)


def _run_warehouse_pushdown(diff: DiffConfig, source: SourceRef, target: SourceRef) -> DiffSummary:
    """Execute same-warehouse SQL pushdown or raise for unsupported pairings.

    Args:
        diff (DiffConfig): Master comparison rules and keys.
        source (SourceRef): Source configuration.
        target (SourceRef): Target configuration.

    Returns:
        DiffSummary: Mismatch and anti-join counts from the pushdown statements.

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
        return _collect_pushdown_summary(snowflake, source.table, target.table, diff)
    if isinstance(source, DatabricksConfig) and isinstance(target, DatabricksConfig):
        if _databricks_fingerprint(source) != _databricks_fingerprint(target):
            raise ConnectorError(
                "Cross-account warehouse pushdown is unsupported. "
                "Source and target Databricks connections must match."
            )
        databricks = DatabricksConnector(source)
        databricks.connect()
        return _collect_pushdown_summary(databricks, source.table, target.table, diff)
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
        rename_map: dict[str, str] = {}
        to_drop: set[str] = set()
        cols = df.collect_schema().names()

        for rule in self.config.rules:
            matched_cols = [
                col
                for col in cols
                if col in rule.column_names or (rule.pattern and re.match(rule.pattern, col))
            ]

            if rule.ignore:
                to_drop.update(matched_cols)
                continue

            if (
                is_source
                and rule.rename_to
                and len(rule.column_names) == 1
                and rule.column_names[0] in cols
            ):
                rename_map[rule.column_names[0]] = rule.rename_to

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
    """The core mathematical engine that evaluates differences between datasets."""

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
    def run_from_configs(
        cls, diff: DiffConfig, source: SourceRef, target: SourceRef
    ) -> DiffSummary:
        """Route a comparison to warehouse pushdown or local Polars evaluation.

        Args:
            diff (DiffConfig): Master comparison rules and keys.
            source (SourceRef): Source file, lakehouse, or warehouse config.
            target (SourceRef): Target file, lakehouse, or warehouse config.

        Returns:
            DiffSummary: Pushdown mismatch and anti-join counts, or a full Polars
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

    def _get_effective_rule(self, col_name: str) -> dict[str, Any]:
        """Resolves all rules (Specific > Pattern > Global) into a unified dictionary.

        Args:
            col_name (str): The name of the column to resolve rules for.

        Returns:
            dict[str, Any]: A flattened dictionary of operational parameters.
        """
        eff: dict[str, Any] = {
            "abs_tol": self.config.default_absolute_tolerance,
            "rel_tol": self.config.default_relative_tolerance,
            "treat_null": self.config.default_treat_null_as_equal,
            "whitespace": self.config.default_whitespace_mode,
            "null_values": self.config.default_null_values,
            "case_insensitive": False,
            "regex_replace": None,
            "value_map": None,
            "cast_to": None,
            "ignore": False,
        }

        matched_rule = None
        for rule in self.config.rules:
            if col_name in rule.column_names:
                matched_rule = rule
                break

        if not matched_rule:
            for rule in self.config.rules:
                if rule.pattern and re.match(rule.pattern, col_name):
                    matched_rule = rule
                    break

        if matched_rule:
            if matched_rule.absolute_tolerance is not None:
                eff["abs_tol"] = matched_rule.absolute_tolerance
            if matched_rule.relative_tolerance is not None:
                eff["rel_tol"] = matched_rule.relative_tolerance
            if matched_rule.treat_null_as_equal is not None:
                eff["treat_null"] = matched_rule.treat_null_as_equal
            if matched_rule.whitespace_mode is not None:
                eff["whitespace"] = matched_rule.whitespace_mode
            if matched_rule.null_values is not None:
                eff["null_values"] = matched_rule.null_values
            if matched_rule.case_insensitive is not None:
                eff["case_insensitive"] = matched_rule.case_insensitive

            eff["regex_replace"] = matched_rule.regex_replace
            eff["value_map"] = matched_rule.value_map
            eff["cast_to"] = matched_rule.cast_to
            eff["ignore"] = matched_rule.ignore

        return eff

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

    def _apply_string_rules(self, series: pl.Expr, rule: dict[str, Any]) -> pl.Expr:
        """Applies whitespace, casing, and regex cleaning to a string expression.

        Args:
            series (pl.Expr): The Polars expression representing the string column.
            rule (dict[str, Any]): The operational parameters for string transformation.

        Returns:
            pl.Expr: The transformed string expression ready for comparison.
        """
        if rule["regex_replace"]:
            for pattern, replacement in rule["regex_replace"].items():
                series = series.str.replace_all(pattern, replacement)

        mode = rule["whitespace"]
        if mode == "left":
            series = series.str.strip_chars_start()
        elif mode == "right":
            series = series.str.strip_chars_end()
        elif mode == "both":
            series = series.str.strip_chars()

        if rule["case_insensitive"]:
            series = series.str.to_lowercase()

        return series

    def _build_match_expr(self, col_name: str, rule: dict[str, Any], dtype: pl.DataType) -> pl.Expr:
        """Builds a robust comparison expression based on data type and user rules.

        Implicit Type Alignment:
            Polars is strictly typed. Comparing a Float64 to an Int64 or String raises
            a ComputeError. If a schema drift is detected between Source and Target:
            - If `strict_types=True`: The mismatch is immediately evaluated as `False`.
            - If `strict_types=False` (Default): The target column is dynamically soft-cast
              to the source's data type purely for the mathematical evaluation.

        Args:
            col_name (str): The column being compared.
            rule (dict[str, Any]): The unified rules to apply.
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

        if rule["value_map"]:
            src = src.replace(rule["value_map"])

        if isinstance(dtype, (pl.String, pl.Utf8)):
            src = self._apply_string_rules(src, rule)
            tgt = self._apply_string_rules(tgt, rule)
            val_match = src == tgt

        elif dtype.is_numeric():
            if rule["abs_tol"] == 0.0 and rule["rel_tol"] == 0.0:
                val_match = src == tgt
            else:
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
        src_rename: dict[str, str] = {}
        src_drop: set[str] = set()
        tgt_drop: set[str] = set()

        src_cols = self.source.collect_schema().names()
        tgt_cols = self.target.collect_schema().names()

        for rule in self.config.rules:
            matched_src = [
                col
                for col in src_cols
                if col in rule.column_names or (rule.pattern and re.match(rule.pattern, col))
            ]

            target_lookup = rule.rename_to if rule.rename_to else rule.column_names
            matched_tgt = [
                col
                for col in tgt_cols
                if col in (target_lookup if isinstance(target_lookup, list) else [target_lookup])
            ]

            if rule.ignore:
                src_drop.update(matched_src)
                tgt_drop.update(matched_tgt)
                continue

            if rule.rename_to and len(rule.column_names) == 1:
                col_name = rule.column_names[0]
                if col_name in src_cols:
                    src_rename[col_name] = rule.rename_to

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

    def run(self) -> DiffSummary:
        """Execute the end-to-end dataset comparison pipeline lazily.

        Builds an optimized Polars computation graph (DAG) to guarantee deterministic
        alignment, preventing compute errors and memory exhaustion on large datasets.
        Data is only materialized into memory when absolutely necessary for execution.

        Execution Pipeline:
            1. Structural Alignment: Maps and prunes schemas to establish the
               Target as the authoritative structural contract.
            2. Validation & Integrity: Asserts primary key existence and uniqueness
               (triggering a localized collection), and enforces the `SchemaMode`.
            3. Lazy Graph Construction: Builds the computation DAG for semantic
               normalization (regex sanitization) and type coercion.
            4. Relational Joins: Formulates the lazy anti-joins ('Added', 'Removed')
               and inner-joins ('Changed') to isolate discrepancies.
            5. Graph Execution: Executes the computation DAG via `.collect()` to
               evaluate vectorized match expressions and compute exact row counts.
            6. Artifact Persistence: Exports the materialized discrepancy dataframes
               to the configured storage backend, if requested.

        Returns:
            DiffSummary: Execution report detailing match status, discrepancy counts,
                and column-level drift metrics.

        Raises:
            ConfigError: If schema constraints or primary keys are violated post-alignment.
            DataIntegrityError: If duplicate primary keys prevent deterministic joins.
            NotImplementedError: If the requested artifact export format is unsupported.
        """
        self._align_structure()
        self._validate_schema()
        self._check_uniqueness()

        src_cols = self.source.collect_schema().names()
        tgt_cols = self.target.collect_schema().names()

        for col in src_cols:
            rule = self._get_effective_rule(col)

            if rule["null_values"]:
                null_list = rule["null_values"]
                if col in src_cols:
                    self.source = self.source.with_columns(
                        pl.when(pl.col(col).is_in(null_list))
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                if col in tgt_cols:
                    self.target = self.target.with_columns(
                        pl.when(pl.col(col).is_in(null_list))
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            if rule["regex_replace"]:
                for pattern, replacement in rule["regex_replace"].items():
                    if col in src_cols and isinstance(
                        self.source.collect_schema().get(col), (pl.String, pl.Utf8)
                    ):
                        self.source = self.source.with_columns(
                            pl.col(col).str.replace_all(pattern, replacement)
                        )
                    if col in tgt_cols and isinstance(
                        self.target.collect_schema().get(col), (pl.String, pl.Utf8)
                    ):
                        self.target = self.target.with_columns(
                            pl.col(col).str.replace_all(pattern, replacement)
                        )

            if rule["cast_to"]:
                dtype = getattr(pl, rule["cast_to"], None)
                if dtype:
                    if col in src_cols:
                        self.source = self.source.with_columns(pl.col(col).cast(dtype))
                    if col in tgt_cols:
                        self.target = self.target.with_columns(pl.col(col).cast(dtype))

        added_lazy = self.target.join(self.source, on=self.config.primary_keys, how="anti")
        removed_lazy = self.source.join(self.target, on=self.config.primary_keys, how="anti")

        # Re-fetch schema names in case rules altered them
        src_cols_final = self.source.collect_schema().names()
        tgt_cols_final = self.target.collect_schema().names()

        src_renamed = self.source.rename(
            {col: f"{col}_source" for col in src_cols_final if col not in self.config.primary_keys}
        )
        tgt_renamed = self.target.rename(
            {col: f"{col}_target" for col in tgt_cols_final if col not in self.config.primary_keys}
        )
        common_lazy = src_renamed.join(tgt_renamed, on=self.config.primary_keys, how="inner")

        match_expressions: list[pl.Expr] = []
        match_cols: list[str] = []

        for col in src_cols_final:
            if col in self.config.primary_keys or col not in tgt_cols_final:
                continue

            rule = self._get_effective_rule(col)
            if rule["ignore"]:
                continue

            dtype = self.source.collect_schema()[col]
            expr = self._build_match_expr(col, rule, dtype).alias(f"{col}_is_match")
            match_expressions.append(expr)
            match_cols.append(f"{col}_is_match")

        added_df = added_lazy.collect()
        removed_df = removed_lazy.collect()

        changed_count = 0
        changed_df = pl.DataFrame()
        column_mismatches: dict[str, int] = {}

        if match_expressions:
            evaluated_lazy = common_lazy.with_columns(match_expressions)
            all_matched = pl.all_horizontal(match_cols)
            changed_lazy = evaluated_lazy.filter(~all_matched)

            changed_df = changed_lazy.collect()
            changed_count = changed_df.height

            if changed_count > 0:
                mismatch_exprs = [
                    (~pl.col(c)).sum().alias(c.replace("_is_match", "")) for c in match_cols
                ]
                raw_counts = changed_df.select(mismatch_exprs).to_dicts()[0]
                column_mismatches = {k: v for k, v in raw_counts.items() if v > 0}

        pk_col = self.config.primary_keys[0]
        src_total = self.source.select(pl.col(pk_col).count()).collect().item()
        tgt_total = self.target.select(pl.col(pk_col).count()).collect().item()

        mismatch_ratio = (added_df.height + removed_df.height + changed_count) / max(src_total, 1)
        is_match = mismatch_ratio <= self.config.threshold

        artifacts_written = False
        output_path_str = getattr(self.config, "output_path", None)
        if isinstance(output_path_str, str):
            out_dir = Path(output_path_str)
            out_dir.mkdir(parents=True, exist_ok=True)
            fmt = getattr(self.config, "output_format", "parquet")

            def _export_artifact(df: pl.DataFrame, name: str) -> bool:
                if df.height == 0:
                    return False
                file_path = out_dir / f"{name}.{fmt}"
                if fmt == "csv":
                    df.write_csv(file_path)
                elif fmt == "parquet":
                    df.write_parquet(file_path)
                else:
                    raise NotImplementedError(
                        f"Export support for format '{fmt}' is not yet implemented."
                    )
                return True

            artifacts_written = any(
                [
                    _export_artifact(added_df, "added_rows"),
                    _export_artifact(removed_df, "removed_rows"),
                    _export_artifact(changed_df, "changed_rows"),
                ]
            )

        return DiffSummary(
            total_rows_source=src_total,
            total_rows_target=tgt_total,
            added_count=added_df.height,
            removed_count=removed_df.height,
            changed_count=changed_count,
            column_mismatches=column_mismatches,
            is_match=is_match,
            report_limit=self.config.report_top_columns_limit,
            artifacts_written=artifacts_written,
        )
