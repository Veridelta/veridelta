# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Core engine for data ingestion and alignment."""

import re
from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from veridelta.exceptions import ConfigError
from veridelta.models import (
    DiffConfig,
    DiffSummary,
    SourceConfig,
    SourceType,
)


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    @abstractmethod
    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Load data into a Polars DataFrame."""
        pass


class CSVLoader(BaseLoader):
    """Loader for CSV files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Load a CSV file into a Polars DataFrame using the provided config options."""
        return pl.read_csv(config.path, **config.options)


class ParquetLoader(BaseLoader):
    """Loader for Parquet files using Polars."""

    def load(self, config: SourceConfig) -> pl.DataFrame:
        """Loader for Parquet files using Polars."""
        return pl.read_parquet(config.path, **config.options)


class LoaderFactory:
    """Factory to return the appropriate loader based on SourceType."""

    _loaders: dict[SourceType, BaseLoader] = {
        "csv": CSVLoader(),
        "parquet": ParquetLoader(),
    }

    @classmethod
    def get_loader(cls, source_type: SourceType) -> BaseLoader:
        """Returns the loader for the given type."""
        loader = cls._loaders.get(source_type)
        if not loader:
            raise NotImplementedError(
                f"Support for '{source_type}' is planned but not yet implemented."
            )
        return loader


class DataIngestor:
    """Coordinates loading and alignment of source and target datasets."""

    def __init__(
        self, diff_config: DiffConfig, source_config: SourceConfig, target_config: SourceConfig
    ) -> None:
        """Initialize the ingestor with I/O configs and the comparison config."""
        self.config = diff_config
        self.source_config = source_config
        self.target_config = target_config

    def _normalize_headers(self, df: pl.DataFrame) -> pl.DataFrame:
        """Standardizes column names to lowercase and strips whitespace if configured."""
        if not self.config.normalize_column_names:
            return df

        rename_map = {col: col.strip().lower() for col in df.columns}
        return df.rename(rename_map)

    def _align_columns(self, df: pl.DataFrame, is_source: bool = True) -> pl.DataFrame:
        """Applies renames and drops ignored columns using specific names or regex patterns."""
        rename_map = {}
        to_drop = set()

        for rule in self.config.rules:
            matched_cols = []
            for col in df.columns:
                if col in rule.column_names or (rule.pattern and re.match(rule.pattern, col)):
                    matched_cols.append(col)

            if rule.ignore:
                to_drop.update(matched_cols)
                continue

            if (
                is_source
                and rule.rename_to
                and len(rule.column_names) == 1
                and rule.column_names[0] in df.columns
            ):
                rename_map[rule.column_names[0]] = rule.rename_to

        return df.drop(list(to_drop)).rename(rename_map)

    def get_dataframes(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Loads and aligns both datasets using idiomatic method chaining."""
        source_loader = LoaderFactory.get_loader(self.source_config.format)
        target_loader = LoaderFactory.get_loader(self.target_config.format)

        source_df = (
            source_loader.load(self.source_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=True)
        )

        target_df = (
            target_loader.load(self.target_config)
            .pipe(self._normalize_headers)
            .pipe(self._align_columns, is_source=False)
        )

        return source_df, target_df


class DiffEngine:
    """The core mathematical engine that calculates differences."""

    def __init__(
        self, config: DiffConfig, source_df: pl.DataFrame, target_df: pl.DataFrame
    ) -> None:
        """Initialize the engine with datasets already aligned by the DataIngestor.

        Args:
            config: The master configuration.
            source_df: The aligned 'Left' dataset.
            target_df: The aligned 'Right' dataset.
        """
        self.config = config
        self.source = source_df
        self.target = target_df

    def _get_effective_rule(self, col_name: str) -> dict[str, Any]:
        """Resolves all rules (Specific > Pattern > Global) into a unified dictionary."""
        eff: dict[str, Any] = {
            "abs_tol": self.config.default_absolute_tolerance,
            "rel_tol": self.config.default_relative_tolerance,
            "treat_null": self.config.default_treat_null_as_equal,
            "whitespace": self.config.default_whitespace_mode,
            "case_insensitive": False,
            "regex_replace": None,
            "value_map": None,
            "cast_to": None,
            "ignore": False,
        }

        # Find first matching rule (Specific matches take priority over Patterns)
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
            if matched_rule.case_insensitive is not None:
                eff["case_insensitive"] = matched_rule.case_insensitive

            eff["regex_replace"] = matched_rule.regex_replace
            eff["value_map"] = matched_rule.value_map
            eff["cast_to"] = matched_rule.cast_to
            eff["ignore"] = matched_rule.ignore

        return eff

    def _check_uniqueness(self) -> None:
        """Verifies that primary keys are unique in both datasets to prevent join explosions."""
        from veridelta.exceptions import DataIntegrityError

        pks = self.config.primary_keys

        if self.source.select(pks).is_duplicated().any():
            dupes = self.source.filter(self.source.select(pks).is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in SOURCE dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

        if self.target.select(pks).is_duplicated().any():
            dupes = self.target.filter(self.target.select(pks).is_duplicated()).height
            raise DataIntegrityError(
                f"Primary keys {pks} are not unique in TARGET dataset. "
                f"Found {dupes} duplicate rows. Clean your data before diffing."
            )

    def _apply_string_rules(self, series: pl.Expr, rule: dict[str, Any]) -> pl.Expr:
        """Applies whitespace, casing, and regex cleaning to a string expression."""
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
        """Builds a smart comparison expression based on data type and user rules."""
        src = pl.col(f"{col_name}_source")
        tgt = pl.col(f"{col_name}_target")

        if rule["value_map"]:
            src = src.replace(rule["value_map"])

        if dtype == pl.String or dtype == pl.Utf8:
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

    def _validate_schema(self) -> None:
        """Enforces the configured SchemaMode before comparison."""
        source_cols = set(self.source.columns)
        target_cols = set(self.target.columns)

        pks = set(self.config.primary_keys)

        missing_pks_source = pks - source_cols
        if missing_pks_source:
            raise ConfigError(f"Primary keys missing in SOURCE: {missing_pks_source}")

        missing_pks_target = pks - target_cols
        if missing_pks_target:
            raise ConfigError(f"Primary keys missing in TARGET: {missing_pks_target}")

        if self.config.schema_mode == "exact":
            if self.source.columns != self.target.columns:
                raise ConfigError(
                    f"EXACT schema match failed.\nSource: {self.source.columns}\nTarget: {self.target.columns}"
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
        """Executes the comparison logic with safety checks and type alignment."""
        self._validate_schema()
        self._check_uniqueness()

        for col in self.source.columns:
            rule = self._get_effective_rule(col)
            if rule["cast_to"]:
                dtype = getattr(pl, rule["cast_to"], None)
                if dtype:
                    if col in self.source.columns:
                        self.source = self.source.with_columns(pl.col(col).cast(dtype))
                    if col in self.target.columns:
                        self.target = self.target.with_columns(pl.col(col).cast(dtype))

        added = self.target.join(self.source, on=self.config.primary_keys, how="anti")
        removed = self.source.join(self.target, on=self.config.primary_keys, how="anti")

        src_renamed = self.source.rename(
            {c: f"{c}_source" for c in self.source.columns if c not in self.config.primary_keys}
        )
        tgt_renamed = self.target.rename(
            {c: f"{c}_target" for c in self.target.columns if c not in self.config.primary_keys}
        )
        common = src_renamed.join(tgt_renamed, on=self.config.primary_keys, how="inner")

        match_expressions = []
        match_cols = []

        for col in self.source.columns:
            if col in self.config.primary_keys or col not in self.target.columns:
                continue

            rule = self._get_effective_rule(col)
            if rule["ignore"]:
                continue

            dtype = self.source.schema[col]
            expr = self._build_match_expr(col, rule, dtype).alias(f"{col}_is_match")
            match_expressions.append(expr)
            match_cols.append(f"{col}_is_match")

        changed_count = 0
        changed_rows = pl.DataFrame()

        if match_expressions and not common.is_empty():
            evaluated = common.with_columns(match_expressions)
            all_matched = pl.all_horizontal(match_cols)
            changed_rows = evaluated.filter(~all_matched)
            changed_count = changed_rows.height

        mismatch_ratio = (added.height + removed.height + changed_count) / max(
            self.source.height, 1
        )
        is_match = mismatch_ratio <= self.config.threshold

        return DiffSummary(
            total_rows_source=self.source.height,
            total_rows_target=self.target.height,
            added_count=added.height,
            removed_count=removed.height,
            changed_count=changed_count,
            is_match=is_match,
        )
