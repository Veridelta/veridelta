# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Data models for Veridelta configuration and results."""

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

SourceType = Literal[
    "csv",
    "json",
    "parquet",
    "fixed_width",
    "netcdf",
    "shapefile",
    "geopackage",
    "excel",
    "sql",
    "delta",
    "avro",
    "xml",
    "arrow",
]
"""Supported and roadmap data formats for ingestion."""

SchemaMode = Literal[
    "exact",
    "allow_additions",
    "allow_removals",
    "intersection",
]
"""Defines how strictly the engine enforces column schemas between datasets.

* `"exact"`: Strict 1:1 mapping. Columns must be identical and in the exact same order.
* `"allow_additions"`: Target can have new columns, but must contain every column present in the Source.
* `"allow_removals"`: Target is allowed to drop legacy columns, but cannot add any new columns.
* `"intersection"`: Only diff columns that exist in both datasets, ignoring all others. (Default)
"""

WhitespaceMode = Literal[
    "none",
    "left",
    "right",
    "both",
]
"""Granular control over string whitespace stripping.

* `"none"`: Do not strip any whitespace.
* `"left"`: Strip leading whitespace only.
* `"right"`: Strip trailing whitespace only.
* `"both"`: Strip both leading and trailing whitespace.
"""


class SourceConfig(BaseModel):
    """Configuration for a specific data source.

    Attributes:
        path: File system path or URI to the data.
        format: The format of the file (e.g., 'csv', 'parquet').
        options: Format-specific kwargs passed to the loader.
    """

    path: str = Field(..., description="File system path or URI to the data.")
    format: SourceType = Field("csv", description="The format of the file.")
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific options (e.g., {'separator': ';'}).",
    )


class ColumnRule(BaseModel):
    """Specific overrides for one or more columns using exact names or regex.

    Attributes:
        column_names: Exact names of the columns in the source dataset.
        pattern: Regex pattern to match multiple columns (e.g., '^AMT_.*').
        absolute_tolerance: The maximum allowed absolute difference.
        relative_tolerance: The maximum allowed relative difference (e.g., 0.01 for 1%).
        case_insensitive: If True, ignores case differences in strings.
        whitespace_mode: Granular control over stripping leading/trailing whitespace.
        regex_replace: Dictionary of {pattern: replacement} to sanitize text before comparison.
        pad_zeros: Left-pad numeric strings to this exact length (e.g., 5 -> '00123').
        value_map: Translate Source values to Target values before comparison (e.g., {'M': 'Male'}).
        null_values: Specific string values to actively coerce to NULL (e.g., ['N/A', '-999']).
        treat_null_as_equal: If True, evaluates NULL == NULL as a successful match.
        datetime_format: Expected strptime format for dates (e.g., '%Y-%m-%d %H:%M:%S').
        timezone: Target timezone to normalize dates to before comparison.
        cast_to: Explicitly cast column to this Polars datatype (e.g., 'Float64').
        ignore: Whether to skip this column entirely during comparison.
        rename_to: The name in the target dataset if it differs from the source.
    """

    column_names: list[str] = Field(
        default_factory=list, description="Exact names of the columns in the source."
    )
    pattern: str | None = Field(
        default=None, description="Regex pattern to match multiple columns (e.g., '^AMT_.*')."
    )

    absolute_tolerance: float | None = Field(
        default=None, ge=0.0, description="Absolute tolerance for numeric differences."
    )
    relative_tolerance: float | None = Field(
        default=None, ge=0.0, description="Relative tolerance (e.g., 0.01 for 1%)."
    )

    case_insensitive: bool | None = Field(
        default=None, description="Ignore case for string comparisons."
    )
    whitespace_mode: WhitespaceMode | None = Field(
        default=None,
        description="Whitespace stripping mode: 'none', 'left', 'right', or 'both'.",
    )
    regex_replace: dict[str, str] | None = Field(
        default=None,
        description="Dictionary of {regex_pattern: replacement_string} to sanitize text.",
    )
    pad_zeros: int | None = Field(
        default=None,
        ge=0,
        description="Left-pad numeric strings to this length (e.g., 5 -> '00123').",
    )

    value_map: dict[str, str] | None = Field(
        default=None, description="Translate Source values to Target values (e.g., {'M': 'Male'})."
    )
    null_values: list[str] | None = Field(
        default=None, description="Specific string values to treat as NULL (e.g., ['N/A', '-999'])."
    )
    treat_null_as_equal: bool | None = Field(
        default=None, description="Treat missing values (NULL/None) in both sources as a match."
    )

    datetime_format: str | None = Field(
        default=None, description="Expected strptime format (e.g., '%Y-%m-%d %H:%M:%S')."
    )
    timezone: str | None = Field(
        default=None, description="Target timezone to normalize dates to before comparison."
    )

    cast_to: str | None = Field(
        default=None,
        description="Explicitly cast column to this Polars datatype (e.g., 'Float64').",
    )
    ignore: bool = Field(
        default=False, description="If True, this column will be excluded from the comparison."
    )
    rename_to: str | None = Field(
        default=None,
        description="Name in target dataset if different (use only for single columns).",
    )

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str | None) -> str | None:
        """Ensure the provided regex pattern is a valid expression at configuration time.

        Args:
            v: The string regex pattern to validate, or None.

        Returns:
            The validated regex string, or None if not provided.

        Raises:
            ValueError: If the regex pattern cannot be compiled.
        """
        if v is not None:
            try:
                re.compile(v)
            except re.error as err:  # noqa: PERF203
                raise ValueError(f"Invalid regex pattern '{v}': {err}") from err
        return v

    @field_validator("regex_replace")
    @classmethod
    def validate_regex_replace(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        """Ensure all keys in the regex replacement dictionary are valid regex patterns.

        Args:
            v: A dictionary mapping regex patterns to their replacements, or None.

        Returns:
            The validated dictionary, or None if not provided.

        Raises:
            ValueError: If any key in the dictionary is an invalid regex pattern.
        """
        if v is not None:
            for pattern in v:
                try:
                    re.compile(pattern)
                except re.error as err:
                    raise ValueError(f"Invalid regex replace pattern '{pattern}': {err}") from err
        return v


class DiffConfig(BaseModel):
    """The master configuration for a Veridelta comparison run.

    Attributes:
        primary_keys: Columns used to join and align the datasets.
        schema_mode: How strictly to enforce column existence and matching between sources.
        strict_types: If False, the engine will attempt to cast target columns to source types.
        default_absolute_tolerance: Global absolute tolerance for all numeric columns.
        default_relative_tolerance: Global relative tolerance for all numeric columns.
        default_treat_null_as_equal: Global setting for handling NULL == NULL.
        default_whitespace_mode: Global setting for stripping whitespace in strings.
        default_null_values: Global list of string values to aggressively coerce to NULL.
        rules: List of per-column comparison overrides.
        threshold: Allowed mismatch percentage (0.0 to 1.0) before failure.
        output_path: Path to save the resulting diff report and artifacts.
    """

    primary_keys: list[str] = Field(..., description="Columns used to join datasets.")

    schema_mode: SchemaMode = Field(
        default="intersection",
        description="Schema enforcement mode: 'exact', 'allow_additions', 'allow_removals', or 'intersection'.",
    )
    strict_types: bool = Field(
        default=False,
        description="If False, engine attempts to safely cast Target columns to Source types.",
    )

    normalize_column_names: bool = Field(
        default=False,
        description="If True, strips whitespace and lowercases all column headers before processing.",
    )

    default_absolute_tolerance: float = Field(
        default=0.0, ge=0.0, description="Global absolute tolerance for numeric columns."
    )
    default_relative_tolerance: float = Field(
        default=0.0, ge=0.0, description="Global relative tolerance for numeric columns."
    )
    default_treat_null_as_equal: bool = Field(
        default=True, description="Globally treat NULL == NULL as a match."
    )
    default_whitespace_mode: WhitespaceMode = Field(
        default="none",
        description="Global string whitespace stripping mode: 'none', 'left', 'right', or 'both'.",
    )
    default_null_values: list[str] = Field(
        default_factory=list, description="Global list of string values to coerce to NULL."
    )

    rules: list[ColumnRule] = Field(default_factory=list, description="Column overrides.")
    threshold: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Allowed mismatch percentage (0.0 to 1.0)."
    )
    output_path: str | None = Field(
        default=None, description="Optional path to save the detailed diff report."
    )

    @model_validator(mode="after")
    def apply_schema_normalization(self) -> "DiffConfig":
        """Automatically lowercase user config keys if normalization is enabled."""
        if self.normalize_column_names:
            self.primary_keys = [pk.strip().lower() for pk in self.primary_keys]

            for rule in self.rules:
                rule.column_names = [col.strip().lower() for col in rule.column_names]

                # do NOT touch rule.pattern (regex is intentionally case-sensitive)
                # do NOT touch rule.rename_to (user might want to rename to a capitalized word)

        return self


class DiffSummary(BaseModel):
    """The high-level results of a Veridelta comparison.

    Attributes:
        total_rows_source: Number of rows in the source dataset.
        total_rows_target: Number of rows in the target dataset.
        added_count: Rows found only in the target (missing from source).
        removed_count: Rows found only in the source (missing from target).
        changed_count: Rows present in both datasets but with value differences.
        is_match: Boolean indicating if the overall diff falls within the allowed threshold.
    """

    total_rows_source: int
    total_rows_target: int
    added_count: int
    removed_count: int
    changed_count: int
    is_match: bool
