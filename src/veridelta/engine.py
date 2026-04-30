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

from veridelta.exceptions import ConfigError
from veridelta.models import (
    DiffConfig,
    DiffSummary,
    SourceConfig,
)


class BaseLoader(ABC):
    """Abstract interface for dataset loaders."""

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Load data from a source into a Polars LazyFrame.

        Args:
            config (SourceConfig): The configuration detailing the path, format,
                and format-specific parsing options.

        Returns:
            pl.LazyFrame: The lazy-loaded dataset graph.
        """
        pass


class CSVLoader(BaseLoader):
    """Loader for CSV files utilizing the Polars CSV scanner."""

    def load(self, config: SourceConfig) -> pl.LazyFrame:
        """Load a CSV file into a Polars LazyFrame.

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
        """Load a Parquet file into a Polars LazyFrame.

        Args:
            config (SourceConfig): The source configuration. Extra options are
                passed directly to `pl.scan_parquet`.

        Returns:
            pl.LazyFrame: The lazy dataset graph.
        """
        return pl.scan_parquet(config.path, **config.options)


class LoaderFactory:
    """Factory to retrieve the appropriate loader based on data format."""

    _loaders: ClassVar[dict[str, BaseLoader]] = {
        "csv": CSVLoader(),
        "parquet": ParquetLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: str) -> BaseLoader:
        """Instantiate the correct loader for the given data format.

        Args:
            source_type (str): The format identifier (e.g., 'csv', 'parquet').

        Returns:
            BaseLoader: An instantiated data loader.

        Raises:
            NotImplementedError: If the requested format is not supported.
        """
        loader = cls._loaders.get(source_type)
        if not loader:
            raise NotImplementedError(
                f"Support for '{source_type}' is planned but not yet implemented."
            )
        return loader


class DataIngestor:
    """Coordinator for loading, renaming, and structurally aligning datasets."""

    def __init__(
        self, diff_config: DiffConfig, source_config: SourceConfig, target_config: SourceConfig
    ) -> None:
        """Initialize the ingestor.

        Args:
            diff_config (DiffConfig): The master comparison configuration.
            source_config (SourceConfig): File and format settings for the source.
            target_config (SourceConfig): File and format settings for the target.
        """
        self.config = diff_config
        self.source_config = source_config
        self.target_config = target_config

    def _normalize_headers(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Standardize column names based on the master configuration.

        Args:
            lf (pl.LazyFrame): The raw lazy dataframe.

        Returns:
            pl.LazyFrame: A dataframe with lowercased/stripped headers if enabled.
        """
        if not self.config.normalize_column_names:
            return lf

        cols = lf.collect_schema().names()
        rename_map = {col: col.strip().lower() for col in cols}
        return lf.rename(rename_map)

    def _align_columns(self, lf: pl.LazyFrame, is_source: bool = True) -> pl.LazyFrame:
        """Apply configured renames and drop ignored columns.

        Args:
            lf (pl.LazyFrame): The lazy dataframe to process.
            is_source (bool): True if processing the source data, False for target.

        Returns:
            pl.LazyFrame: The structurally aligned lazy dataframe.
        """
        rename_map: dict[str, str] = {}
        to_drop: set[str] = set()
        cols = lf.collect_schema().names()

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

        return lf.drop(list(to_drop)).rename(rename_map)

    def get_lazyframes(self) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Load and structurally align source and target datasets lazily.

        Returns:
            tuple[pl.LazyFrame, pl.LazyFrame]: The prepared (source_lf, target_lf).
        """
        source_loader = LoaderFactory.get_loader(self.source_config.format)
        target_loader = LoaderFactory.get_loader(self.target_config.format)

        source_lf = (
            source_loader.load(self.source_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=True)
        )

        target_lf = (
            target_loader.load(self.target_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=False)
        )

        return source_lf, target_lf


class DiffEngine:
    """Stateless mathematical engine for evaluating dataset differences.

    Allows a single configured instance to validate multiple source/target
    dataset pairs safely in a multi-threaded pipeline without retaining state.
    """

    def __init__(self, config: DiffConfig) -> None:
        """Initialize the engine with the master ruleset.

        Args:
            config (DiffConfig): The master validation rules configuration.
        """
        self.config = config

    def _get_effective_rule(self, col_name: str) -> dict[str, Any]:
        """Resolve all configured rules into a unified dictionary.

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

    def _check_uniqueness(self, source_lf: pl.LazyFrame, target_lf: pl.LazyFrame) -> None:
        """Verify that primary keys are strictly unique in both datasets.

        Args:
            source_lf (pl.LazyFrame): The source dataset.
            target_lf (pl.LazyFrame): The target dataset.

        Raises:
            DataIntegrityError: If duplicates are found in the primary keys of either
                dataset, preventing deterministic joins.
        """
        from veridelta.exceptions import DataIntegrityError

        pks = self.config.primary_keys

        src_pks = source_lf.select(pks).collect()
        if src_pks.is_duplicated().any():
            dupes = src_pks.filter(src_pks.is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in SOURCE dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

        tgt_pks = target_lf.select(pks).collect()
        if tgt_pks.is_duplicated().any():
            dupes = tgt_pks.filter(tgt_pks.is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in TARGET dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

    def _apply_string_rules(self, series: pl.Expr, rule: dict[str, Any]) -> pl.Expr:
        """Apply whitespace, casing, and regex cleaning to a string expression.

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

    def _build_match_expr(
        self, col_name: str, rule: dict[str, Any], dtype: pl.DataType, target_lf: pl.LazyFrame
    ) -> pl.Expr:
        """Build a robust comparison expression based on data type and user rules.

        Args:
            col_name (str): The column being compared.
            rule (dict[str, Any]): The unified rules to apply.
            dtype (pl.DataType): The data type of the source column.
            target_lf (pl.LazyFrame): The target dataframe, needed for type checks.

        Returns:
            pl.Expr: A boolean expression evaluating to True where row values match.

        Notes:
            If a schema drift is detected between Source and Target:
            - `strict_types=True`: The mismatch immediately evaluates as `False`.
            - `strict_types=False`: The target column dynamically soft-casts
              to the source's data type purely for mathematical evaluation.
        """
        src = pl.col(f"{col_name}_source")
        tgt = pl.col(f"{col_name}_target")

        tgt_dtype = target_lf.collect_schema().get(col_name)

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

    def _align_structure(
        self, source_lf: pl.LazyFrame, target_lf: pl.LazyFrame
    ) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Align source and target schemas according to configuration rules.

        Apply column renames to the source dataset and drop ignored columns
        from both datasets to establish a uniform structure for comparison.

        Args:
            source_lf (pl.LazyFrame): The unaligned source dataset.
            target_lf (pl.LazyFrame): The unaligned target dataset.

        Returns:
            tuple[pl.LazyFrame, pl.LazyFrame]: The aligned (source, target) datasets.

        Notes:
            This is a mandatory prerequisite for `_validate_schema`.
        """
        src_rename: dict[str, str] = {}
        src_drop: set[str] = set()
        tgt_drop: set[str] = set()

        src_cols = source_lf.collect_schema().names()
        tgt_cols = target_lf.collect_schema().names()

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

        source_lf = source_lf.drop(list(src_drop)).rename(src_rename)
        target_lf = target_lf.drop(list(tgt_drop))

        return source_lf, target_lf

    def _validate_schema(self, source_lf: pl.LazyFrame, target_lf: pl.LazyFrame) -> None:
        """Enforce the configured schema mode before comparison.

        Args:
            source_lf (pl.LazyFrame): The structurally aligned source dataset.
            target_lf (pl.LazyFrame): The structurally aligned target dataset.

        Raises:
            ConfigError: If primary keys are missing or schema constraints are violated.
        """
        source_cols = set(source_lf.collect_schema().names())
        target_cols = set(target_lf.collect_schema().names())
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

    def compare_lazyframes(self, source_lf: pl.LazyFrame, target_lf: pl.LazyFrame) -> DiffSummary:
        """Evaluate the difference between two lazy frames.

        Build an optimized Polars computation graph to isolate discrepancies
        using relational anti-joins and inner-joins. Bypass disk I/O entirely
        to enable high-speed, in-memory unit testing.

        Args:
            source_lf (pl.LazyFrame): The legacy dataset graph.
            target_lf (pl.LazyFrame): The modern dataset graph.

        Returns:
            DiffSummary: Execution report detailing match status and discrepancy metrics.

        Examples:
            Execute a pure in-memory comparison for unit testing:

            ```python
            import polars as pl
            from veridelta.models import DiffConfig, DiffRule
            from veridelta.engine import DiffEngine

            src = pl.DataFrame({"id": [1], "val": [10]})
            tgt = pl.DataFrame({"id": [1], "val": [11]})

            config = DiffConfig(primary_keys=["id"])
            summary = DiffEngine(config).compare_lazyframes(src.lazy(), tgt.lazy())

            assert summary.changed_count == 1
            ```
        """
        source_lf, target_lf = self._align_structure(source_lf, target_lf)
        self._validate_schema(source_lf, target_lf)
        self._check_uniqueness(source_lf, target_lf)

        src_cols = source_lf.collect_schema().names()
        tgt_cols = target_lf.collect_schema().names()

        for col in src_cols:
            rule = self._get_effective_rule(col)

            if rule["null_values"]:
                null_list = rule["null_values"]
                if col in src_cols:
                    source_lf = source_lf.with_columns(
                        pl.when(pl.col(col).is_in(null_list))
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                if col in tgt_cols:
                    target_lf = target_lf.with_columns(
                        pl.when(pl.col(col).is_in(null_list))
                        .then(None)
                        .otherwise(pl.col(col))
                        .alias(col)
                    )

            if rule["regex_replace"]:
                for pattern, replacement in rule["regex_replace"].items():
                    if col in src_cols and isinstance(
                        source_lf.collect_schema().get(col), (pl.String, pl.Utf8)
                    ):
                        source_lf = source_lf.with_columns(
                            pl.col(col).str.replace_all(pattern, replacement)
                        )
                    if col in tgt_cols and isinstance(
                        target_lf.collect_schema().get(col), (pl.String, pl.Utf8)
                    ):
                        target_lf = target_lf.with_columns(
                            pl.col(col).str.replace_all(pattern, replacement)
                        )

            if rule["cast_to"]:
                dtype = getattr(pl, rule["cast_to"], None)
                if dtype:
                    if col in src_cols:
                        source_lf = source_lf.with_columns(pl.col(col).cast(dtype))
                    if col in tgt_cols:
                        target_lf = target_lf.with_columns(pl.col(col).cast(dtype))

        added_lazy = target_lf.join(source_lf, on=self.config.primary_keys, how="anti")
        removed_lazy = source_lf.join(target_lf, on=self.config.primary_keys, how="anti")

        src_cols_final = source_lf.collect_schema().names()
        tgt_cols_final = target_lf.collect_schema().names()

        src_renamed = source_lf.rename(
            {col: f"{col}_source" for col in src_cols_final if col not in self.config.primary_keys}
        )
        tgt_renamed = target_lf.rename(
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

            dtype = source_lf.collect_schema()[col]
            expr = self._build_match_expr(col, rule, dtype, target_lf).alias(f"{col}_is_match")
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
        src_total = source_lf.select(pl.col(pk_col).count()).collect().item()
        tgt_total = target_lf.select(pl.col(pk_col).count()).collect().item()

        mismatch_ratio = (added_df.height + removed_df.height + changed_count) / max(src_total, 1)
        is_match = mismatch_ratio <= self.config.threshold

        output_path_str = getattr(self.config, "output_path", None)
        if isinstance(output_path_str, str):
            out_dir = Path(output_path_str)
            out_dir.mkdir(parents=True, exist_ok=True)
            fmt = getattr(self.config, "output_format", "parquet")

            def _export_artifact(df: pl.DataFrame, name: str) -> None:
                if df.height == 0:
                    return
                file_path = out_dir / f"{name}.{fmt}"
                if fmt == "csv":
                    df.write_csv(file_path)
                elif fmt == "parquet":
                    df.write_parquet(file_path)
                else:
                    raise NotImplementedError(
                        f"Export support for format '{fmt}' is not yet implemented."
                    )

            _export_artifact(added_df, "added_rows")
            _export_artifact(removed_df, "removed_rows")
            _export_artifact(changed_df, "changed_rows")

        return DiffSummary(
            total_rows_source=src_total,
            total_rows_target=tgt_total,
            added_count=added_df.height,
            removed_count=removed_df.height,
            changed_count=changed_count,
            column_mismatches=column_mismatches,
            is_match=is_match,
            report_limit=self.config.report_top_columns_limit,
        )

    def execute(self, source_config: SourceConfig, target_config: SourceConfig) -> DiffSummary:
        """Execute the end-to-end dataset comparison pipeline from disk configurations.

        Delegate dataset loading to the data ingestor, then evaluate structural
        and row-level differences via the core mathematical engine.

        Args:
            source_config (SourceConfig): Configuration for the legacy dataset.
            target_config (SourceConfig): Configuration for the modern dataset.

        Returns:
            DiffSummary: Execution report detailing match status and discrepancy metrics.

        Examples:
            Execute a file-based pipeline utilizing automated Polars I/O abstractions:

            ```python
            from veridelta.config import load_config
            from veridelta.engine import DiffEngine

            diff_cfg, source, target = load_config("veridelta.yaml")

            # The engine automatically resolves standard I/O paths (local, s3://, etc.)
            summary = DiffEngine(diff_cfg).execute(source, target)
            print(summary.report_summary)
            ```
        """
        ingestor = DataIngestor(self.config, source_config, target_config)
        source_lf, target_lf = ingestor.get_lazyframes()

        return self.compare_lazyframes(source_lf, target_lf)
