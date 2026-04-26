# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Data models for Veridelta configuration and results.

This module defines the Pydantic models used to configure data ingestion,
comparison rules, and format the output summaries. It acts as the strict
schema definition for the YAML configuration files.
"""

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, computed_field, field_validator, model_validator

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
        path (str): File system path or URI to the data.
        format (SourceType): The format of the file (e.g., 'csv', 'parquet').
        options (dict[str, Any]): Format-specific keyword arguments passed
            directly to the underlying Polars reader (e.g., `{'separator': ';'}`).
    """

    path: str = Field(..., description="File system path or URI to the data.")
    format: SourceType = Field("csv", description="The format of the file.")
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific options (e.g., {'separator': ';'}).",
    )


class DiffRule(BaseModel):
    """Specific overrides for one or more columns using exact names or regex.

    Attributes:
        column_names (list[str]): Exact names of the columns in the source dataset.
        pattern (str | None): Regex pattern to match multiple columns (e.g., '^AMT_.*').
        absolute_tolerance (float | None): The maximum allowed absolute difference
            for numeric mathematical comparisons.
        relative_tolerance (float | None): The maximum allowed relative difference
            (e.g., 0.01 for 1%).
        case_insensitive (bool | None): If True, ignores case differences in strings.
        whitespace_mode (WhitespaceMode | None): Granular control over stripping
            leading/trailing whitespace prior to string comparison.
        regex_replace (dict[str, str] | None): Dictionary of `{pattern: replacement}`
            to sanitize text. Implicitly executed *before* type coercion to ensure
            text sanitization completes safely before casting.
        pad_zeros (int | None): Left-pad numeric strings to this exact length
            (e.g., 5 -> '00123').
        value_map (dict[str, str] | None): Translate Source values to Target values
            before comparison (e.g., `{'M': 'Male'}`).
        null_values (list[str] | None): Specific string values to actively coerce
            to NULL (e.g., `['N/A', '-999']`).
        treat_null_as_equal (bool | None): If True, evaluates NULL == NULL as a
            successful match rather than a missing value mismatch.
        datetime_format (str | None): Expected strptime format for dates
            (e.g., '%Y-%m-%d %H:%M:%S').
        timezone (str | None): Target timezone to normalize dates to before comparison.
        cast_to (str | None): Explicitly cast column to this Polars datatype
            (e.g., 'Float64'). Evaluated *after* string transformations.
        ignore (bool): Whether to skip this column entirely during comparison.
        rename_to (str | None): The name in the target dataset if it differs from
            the source. Only valid when `column_names` contains exactly one entry.
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
        """Ensures the provided regex pattern is a valid expression at configuration time.

        Args:
            v (str | None): The string regex pattern to validate.

        Returns:
            str | None: The validated regex string.

        Raises:
            ValueError: If the regex pattern cannot be compiled.
        """
        if v is not None:
            try:
                re.compile(v)
            except re.error as err:
                raise ValueError(f"Invalid regex pattern '{v}': {err}") from err
        return v

    @field_validator("regex_replace")
    @classmethod
    def validate_regex_replace(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        """Ensures all keys in the regex replacement dictionary are valid regex patterns.

        Args:
            v (dict[str, str] | None): A mapping of regex patterns to replacements.

        Returns:
            dict[str, str] | None: The validated dictionary.

        Raises:
            ValueError: If any key in the dictionary is an invalid regex pattern.
        """
        if v is not None:
            for pattern in v:
                try:
                    re.compile(pattern)
                except re.error as err:  # noqa: PERF203
                    raise ValueError(f"Invalid regex replace pattern '{pattern}': {err}") from err
        return v


class DiffConfig(BaseModel):
    """The master configuration for a Veridelta comparison run.

    Attributes:
        primary_keys (list[str]): Columns used to join and align the datasets.
            Must be unique in both datasets.
        schema_mode (SchemaMode): How strictly to enforce column existence and
            matching between sources.
        strict_types (bool): If False (default), the engine implicitly soft-casts
            target columns to source types purely for the comparison expression,
            preventing execution crashes on type mismatches. If True, type mismatches
            will automatically evaluate as row failures.
        normalize_column_names (bool): If True, strips whitespace and lowercases
            all column headers prior to schema alignment.
        default_absolute_tolerance (float): Global absolute tolerance for numeric columns.
        default_relative_tolerance (float): Global relative tolerance for numeric columns.
        default_treat_null_as_equal (bool): Global setting for handling NULL == NULL.
        default_whitespace_mode (WhitespaceMode): Global string whitespace stripping mode.
        default_null_values (list[str]): Global list of string values to aggressively
            coerce to NULL.
        rules (list[DiffRule]): List of per-column comparison overrides. Specific
            `column_names` take precedence over regex `pattern` rules.
        threshold (float): Allowed mismatch ratio (0.0 to 1.0) before the `is_match`
            flag evaluates to False.
        report_top_columns_limit (int): Max number of drifted columns to display
            in the generated markdown report summary.
        output_path (str | None): Optional path to save the resulting diff report
            and artifacts (added, removed, and changed rows).
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

    rules: list[DiffRule] = Field(default_factory=list, description="Column overrides.")

    threshold: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Allowed mismatch percentage (0.0 to 1.0)."
    )

    report_top_columns_limit: int = Field(
        default=5,
        ge=0,
        description="Max number of top drifted columns to show in the report summary.",
    )

    output_path: str | None = Field(
        default=None, description="Optional path to save the detailed diff report."
    )

    @model_validator(mode="after")
    def apply_schema_normalization(self) -> "DiffConfig":
        """Automatically lowercases and strips config keys if normalization is enabled.

        Returns:
            DiffConfig: The mutated configuration instance.
        """
        if self.normalize_column_names:
            self.primary_keys = [pk.strip().lower() for pk in self.primary_keys]

            rules: list[DiffRule] = self.rules
            for rule in rules:
                rule.column_names = [col.strip().lower() for col in rule.column_names]

        return self


class DiffSummary(BaseModel):
    """The high-level execution results of a Veridelta comparison.

    Attributes:
        total_rows_source (int): Number of rows in the source dataset.
        total_rows_target (int): Number of rows in the target dataset.
        added_count (int): Rows found only in the target (missing from source).
        removed_count (int): Rows found only in the source (missing from target).
        changed_count (int): Rows present in both datasets but with value differences.
        column_mismatches (dict[str, int]): Dictionary mapping column names to the
            exact count of mismatched rows for that specific column.
        is_match (bool): Boolean indicating if the overall diff falls within the
            allowed mismatch threshold.
        total_mismatches (int): (Computed) The sum of all added, removed, and changed rows.
        mismatch_ratio (float): (Computed) The ratio of mismatched rows to the baseline
            source dataset.
        match_rate_percentage (float): (Computed) The overall match rate expressed
            as a percentage (e.g., 99.98).
        is_perfect_match (bool): (Computed) True only if there are exactly 0 mismatches.
        volume_shift (int): (Computed) The net change in row volume (Target - Source).
        report_summary (str): (Computed) A pre-formatted, human-readable markdown
            status report intended for CI/CD logs or PR comments.
        report_limit (int): Internal configuration dictating the max columns to display
            in the `report_summary`. Implicitly excluded from JSON serialization.
    """

    total_rows_source: int
    total_rows_target: int
    added_count: int
    removed_count: int
    changed_count: int
    column_mismatches: dict[str, int] = Field(default_factory=dict)
    is_match: bool

    report_limit: int = Field(default=5, exclude=True)

    @computed_field
    @property
    def total_mismatches(self) -> int:
        """Calculates the sum of all added, removed, and changed rows.

        Returns:
            int: The total count of discrepancy events.
        """
        return self.added_count + self.removed_count + self.changed_count

    @computed_field
    @property
    def mismatch_ratio(self) -> float:
        """Calculates the ratio of mismatched rows to the baseline source dataset.

        Returns:
            float: A float representing the ratio (0.0 to 1.0+).
        """
        return float(self.total_mismatches) / float(max(self.total_rows_source, 1))

    @computed_field
    @property
    def match_rate_percentage(self) -> float:
        """Calculates the overall match rate expressed as a percentage.

        Returns:
            float: The match percentage rounded to two decimal places (e.g., 99.98).
        """
        return round((1.0 - self.mismatch_ratio) * 100.0, 2)

    @computed_field
    @property
    def is_perfect_match(self) -> bool:
        """Evaluates if the datasets are completely identical under the configured rules.

        Returns:
            bool: True if there are zero total mismatches.
        """
        return self.total_mismatches == 0

    @computed_field
    @property
    def volume_shift(self) -> int:
        """Calculates the net change in row volume between the systems.

        Returns:
            int: The net shift (Target rows - Source rows).
        """
        return self.total_rows_target - self.total_rows_source

    @computed_field
    @property
    def report_summary(self) -> str:
        """Generates a pre-formatted, human-readable status report.

        This string aggregates all metrics and top column-level drifts into a
        clean markdown format ready for immediate pipeline logging.

        Returns:
            str: The formatted execution summary.
        """
        status_icon = "PASSED" if self.is_match else "FAILED"
        perfect_tag = " (Perfect Match)" if self.is_perfect_match else ""

        base_report = (
            f"Veridelta Execution Summary\n"
            f"===========================\n"
            f"Status:        {status_icon}{perfect_tag}\n"
            f"Match Rate:    {self.match_rate_percentage}%\n"
            f"Source Rows:   {self.total_rows_source:,}\n"
            f"Target Rows:   {self.total_rows_target:,}\n"
            f"Volume Shift:  {self.volume_shift:+,} rows\n"
            f"\nRow-Level Discrepancies:\n"
            f"---------------------------\n"
            f"Added:         {self.added_count:,}\n"
            f"Removed:       {self.removed_count:,}\n"
            f"Changed:       {self.changed_count:,}\n"
            f"Total Issues:  {self.total_mismatches:,}\n"
        )

        if not self.column_mismatches or self.report_limit == 0:
            return base_report

        top_cols = sorted(self.column_mismatches.items(), key=lambda x: x[1], reverse=True)[
            : self.report_limit
        ]

        col_report = "\nTop Column-Level Drifts:\n---------------------------\n"
        for col, count in top_cols:
            col_report += f"- {col}: {count:,} mismatches\n"

        return base_report + col_report
