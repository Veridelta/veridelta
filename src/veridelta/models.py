# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration and result models.

The Pydantic models here define every configuration setting, in YAML or in
Python, and the results a comparison returns.
"""

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal, cast
from urllib.parse import urlsplit, urlunsplit

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from veridelta.exceptions import ConfigError

if TYPE_CHECKING:
    import pandas as pd  # pyright: ignore[reportMissingTypeStubs] - no pandas-stubs

_CONTAINER_SCHEMES: Final[frozenset[str]] = frozenset({"abfs", "abfss", "wasb", "wasbs"})
"""Azure schemes whose `container@account` user part names a container, not a login."""


def redacted_location(location: str) -> str | None:
    """Return a path or URL with anything secret left out, safe to print or log.

    A URL keeps its scheme, host, and path. Its query goes, since it can hold a
    token or the signature of a pre-signed link, and so does its user part,
    which can hold a login, unless an Azure scheme names a container there. A
    path on this machine comes back as it is.

    Args:
        location (str): A file path, or the URL of a file or a table.

    Returns:
        str | None: The location without its secrets, or None when it does not
            parse as a URL.

    Examples:
        >>> redacted_location("s3://bucket/events.parquet?X-Amz-Signature=abc")
        's3://bucket/events.parquet'
        >>> redacted_location("abfss://lake@account.dfs.core.windows.net/events")
        'abfss://lake@account.dfs.core.windows.net/events'
    """
    try:
        parts = urlsplit(location)
    except ValueError:
        return None
    if not (parts.scheme and parts.netloc):
        return location
    user, _, host = parts.netloc.rpartition("@")
    if user and parts.scheme in _CONTAINER_SCHEMES and ":" not in user:
        host = f"{user}@{host}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


SQL_IDENTIFIER_SEGMENT_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"
"""Unquoted SQL identifier: letter or underscore, then alphanumeric or underscore."""

SQL_IDENTIFIER_SEGMENT = re.compile(SQL_IDENTIFIER_SEGMENT_PATTERN)
"""Compiled allowlist applied by the warehouse SQL compiler before quoting."""

SQL_RELATION_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
"""Warehouse table path: one to three identifier segments joined by dots."""

BIGQUERY_TABLE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
"""BigQuery table path: a table, or a dataset and table. The project is set
separately, since project ids may hold hyphens, which an identifier may not."""

BIGQUERY_PROJECT_PATTERN = r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$"
"""Google Cloud project id: six to thirty lowercase letters, digits, or hyphens,
starting with a letter and not ending with a hyphen. Older domain-scoped ids,
such as `example.com:project`, are refused rather than guessed at."""

SourceType = Literal[
    "csv",
    "parquet",
    "json",
    "ndjson",
    "arrow",
    "avro",
    "excel",
]
"""File formats Veridelta can read.

Exactly the set `LoaderFactory` implements. Delta Lake is not here: it is a
table format, read through the `delta` source type rather than as a file.
"""

_SUFFIX_FORMATS: dict[str, SourceType] = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".pq": "parquet",
    ".json": "json",
    ".ndjson": "ndjson",
    ".jsonl": "ndjson",
    ".arrow": "arrow",
    ".ipc": "arrow",
    ".feather": "arrow",
    ".avro": "avro",
    ".xlsx": "excel",
    ".xls": "excel",
}
"""The format each file suffix names, in lowercase. A suffix not here keeps the default."""


def normalize_column_name(name: str) -> str:
    """Strip and lowercase a column name, as `normalize_column_names` asks.

    Args:
        name (str): A header, or a name a configuration gives.

    Returns:
        str: The name the engine compares by.
    """
    return name.strip().lower()


def mismatch_ratio_of(mismatches: int, source_rows: int) -> float:
    """Divide the mismatches by the source row count, which the threshold applies to.

    Args:
        mismatches (int): Added, removed, and changed rows together.
        source_rows (int): The rows the source holds. An empty source counts as one.

    Returns:
        float: The ratio, which can exceed 1.
    """
    return float(mismatches) / float(max(source_rows, 1))


def _infer_format(path: str) -> SourceType | None:
    """Return the format a path's suffix names, or None when the suffix says nothing.

    The suffix is the file name's, after any `?` query or `#` fragment is
    dropped, and its case does not matter: `data.PARQUET?version=3` names `parquet`.
    """
    name = path.split("?", 1)[0].split("#", 1)[0].replace("\\", "/").rsplit("/", 1)[-1]
    return _SUFFIX_FORMATS.get(PurePosixPath(name).suffix.lower())


SchemaMode = Literal[
    "exact",
    "allow_additions",
    "allow_removals",
    "intersection",
]
"""How strictly the two datasets' columns must agree.

- `exact`: both sides have the same columns, in any order.
- `allow_additions`: the target may add columns, but keeps every source column.
- `allow_removals`: the target may drop columns, but adds none.
- `intersection`, the default: compare only the columns on both sides.
"""

SentinelValue = str | int | float | bool
"""One `null_values` entry, which keeps the type written in the config.

In YAML, `-999` is an integer sentinel and `"-999"` a text one. Each applies only
to columns of a matching type.
"""

ArtifactFormat = Literal[
    "csv",
    "parquet",
    "json",
    "ndjson",
    "arrow",
]
"""File format for exported discrepancy artifacts.

A closed set that matches the engine's writers, so a typo fails when the config
loads instead of after the comparison runs. Excel can be read but not written:
writing a workbook needs another dependency.
"""

CastTarget = Literal[
    "Int64",
    "Float64",
    "String",
    "Boolean",
    "Date",
    "Datetime",
]
"""Polars type a column may be cast to before comparison.

A closed set rather than a free-form name, for two reasons. A typo fails when
the config loads instead of skipping the cast. And the SQL compiler renders
this field into `CAST(x AS <type>)`, where a type name cannot be quoted or
bound, so only a closed set keeps configuration text out of the SQL. Every
member maps to a fixed keyword in each dialect.
"""

WhitespaceMode = Literal[
    "none",
    "left",
    "right",
    "both",
]
"""Which ends of a string to strip whitespace from.

- `none`: strip nothing.
- `left`: strip leading whitespace.
- `right`: strip trailing whitespace.
- `both`: strip both ends.
"""


class SourceConfig(BaseModel):
    """Settings for a file source.

    Attributes:
        type (Literal["file"]): Source kind. A YAML file source may omit it.
        path (str): Local path or URI of the file.
        format (SourceType): File format, such as `parquet`. When absent, the path's
            suffix decides it, and `csv` is the default for a suffix Veridelta does
            not know.
        options (dict[str, Any]): Keyword arguments for the Polars reader, such as
            `{'separator': ';'}`. A nested `storage_options` map is left out when the
            config is printed, but kept by `model_dump()`, which the reader needs.
    """

    # Reader options can carry object-store credentials, which Pydantic would
    # otherwise quote in its errors.
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    type: Literal["file"] = Field("file", description="Discriminator for file-backed sources.")
    path: str = Field(..., description="File system path or URI to the data.")
    format: SourceType = Field(
        "csv",
        description=(
            "The format of the file. When absent, the path's suffix decides it, and csv "
            "is the default for a suffix Veridelta does not know."
        ),
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Options for the Polars reader, such as {'separator': ';'}.",
    )

    @model_validator(mode="before")
    @classmethod
    def _format_from_suffix(cls, data: Any) -> Any:
        """Fill an absent `format` from the path's suffix.

        A `format` the user wrote always wins. A suffix Veridelta does not know
        leaves the key absent, so the default applies and the error for a
        missing primary key can say that `format` is not set.
        """
        if not isinstance(data, dict):
            return data
        values = cast("dict[str, Any]", data)
        if "format" in values:
            return values
        inferred = _infer_format(str(values.get("path", "")))
        return values if inferred is None else {**values, "format": inferred}

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        """Leave object-store credentials out of the printed reader options.

        A cloud path's credentials travel in a nested `storage_options` map.
        Printing drops that one key and keeps the other options, which are the
        useful part when debugging. `repr()`, `str()`, and rich displays all
        read from here, while `model_dump()` and the reader get the full map.

        Yields:
            tuple[str | None, Any]: Each field name with the value to print.
        """
        for name, value in super().__repr_args__():
            if name == "options" and "storage_options" in self.options:
                yield (
                    name,
                    {key: item for key, item in self.options.items() if key != "storage_options"},
                )
            else:
                yield name, value


def _reject_non_finite_sentinels(values: Iterable[SentinelValue] | None) -> None:
    """Reject NaN and infinity in a sentinel list."""
    for value in values or ():
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(
                f"null_values cannot contain the non-finite value {value!r}. "
                "NaN never compares equal, and infinities have no SQL literal."
            )


class DiffRule(BaseModel):
    """Overrides for one or more columns, chosen by exact name or by pattern.

    Each setting runs at a fixed stage of the
    [transform order](https://veridelta.github.io/veridelta/rules/#transform-order),
    the same in a local run and in a warehouse.

    Attributes:
        column_names (list[str]): Exact source column names the rule governs.
        pattern (str | None): Regular expression that selects columns by name, such
            as `^AMT_.*`.
        absolute_tolerance (float | None): Largest absolute difference that still
            matches, for numeric columns. Must be finite.
        relative_tolerance (float | None): Largest difference relative to the source
            value that still matches, such as `0.01` for 1%. Must be finite. Neither
            tolerance forgives a non-finite value: NaN matches only NaN, and an
            infinity only itself.
        max_levenshtein_distance (int | None): Most single-character insertions,
            deletions, and substitutions that still match, for columns compared as
            text. Needs the `fuzzy` extra locally.
        min_jaro_winkler_similarity (float | None): Lowest Jaro-Winkler similarity,
            above 0 and at most 1, that still matches, for columns compared as text.
            Needs the `fuzzy` extra, and runs locally only: warehouse pushdown refuses
            it before any query runs. A rule sets at most one of the two limits.
        case_insensitive (bool | None): Whether to ignore case in text.
        whitespace_mode (WhitespaceMode | None): Which ends of text to strip
            whitespace from.
        regex_replace (dict[str, str] | None): `{pattern: replacement}` pairs applied
            to text columns.
        pad_zeros (int | None): Width to left-pad values to with zeros, such as `5`
            for `00123`. Other types become text first, so a numeric `123` matches a
            text `'00123'`.
        value_map (dict[str, str] | None): Source values to translate to target
            values before comparison, such as `{'M': 'Male'}`.
        null_values (list[SentinelValue] | None): Values to read as NULL, such as
            `['N/A', -999, false]`. Each applies only to columns whose type can hold
            it: text reaches string, categorical, and enum columns, numbers reach
            numeric columns, decimals included, and booleans reach boolean columns.
            An explicit rule whose sentinels fit no column type raises `ConfigError`.
        treat_null_as_equal (bool | None): Whether two NULLs match.
        datetime_format (str | None): Format for `strptime`, such as
            `%Y-%m-%d %H:%M:%S`, that parses text columns into datetimes. A value that does not match
            becomes NULL. Pushdown translates the directives from a fixed table and
            raises `ConfigError` for any directive outside it.
        timezone (str | None): Timezone to convert timestamps to, such as `UTC`. The
            column must be timezone-aware once parsed: a naive timestamp raises
            `ConfigError` instead of being assigned a guessed zone.
        cast_to (CastTarget | None): Polars type to cast the column to, such as
            `Float64`. A closed set, so a typo fails when the config loads.
        ignore (bool): Whether to leave the column out of the comparison. Defaults
            to False.
        rename_to (str | None): Target column name, when it differs from the source.
            Valid only when `column_names` holds exactly one name.
    """

    model_config = ConfigDict(extra="forbid")

    column_names: list[str] = Field(
        default_factory=list, description="Exact names of the columns in the source."
    )
    pattern: str | None = Field(
        default=None, description="Regex pattern that selects columns by name, such as '^AMT_.*'."
    )

    absolute_tolerance: float | None = Field(
        default=None,
        ge=0.0,
        strict=True,
        allow_inf_nan=False,
        description="Absolute tolerance for numeric differences.",
    )
    relative_tolerance: float | None = Field(
        default=None,
        ge=0.0,
        strict=True,
        allow_inf_nan=False,
        description="Relative tolerance, such as 0.01 for 1%.",
    )
    max_levenshtein_distance: int | None = Field(
        default=None,
        ge=1,
        strict=True,
        description="Most character edits between two text values that still match.",
    )
    min_jaro_winkler_similarity: float | None = Field(
        default=None,
        gt=0.0,
        le=1.0,
        strict=True,
        allow_inf_nan=False,
        description="Lowest Jaro-Winkler similarity between two text values that still matches.",
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
        strict=True,
        description="Left-pad numeric strings to this length, such as 5 for '00123'.",
    )

    value_map: dict[str, str] | None = Field(
        default=None,
        description="Source values to translate to target values, such as {'M': 'Male'}.",
    )
    null_values: list[SentinelValue] | None = Field(
        default=None,
        strict=True,
        description="Values to read as NULL, such as ['N/A', -999, false].",
    )
    treat_null_as_equal: bool | None = Field(
        default=None, description="Treat missing values (NULL/None) in both sources as a match."
    )

    datetime_format: str | None = Field(
        default=None, description="Format for strptime, such as '%Y-%m-%d %H:%M:%S'."
    )
    timezone: str | None = Field(
        default=None, description="Target timezone to normalize dates to before comparison."
    )

    cast_to: CastTarget | None = Field(
        default=None,
        description="Polars type to cast the column to, such as 'Float64'.",
    )
    ignore: bool = Field(
        default=False, description="Whether to leave this column out of the comparison."
    )
    rename_to: str | None = Field(
        default=None,
        description="Name in target dataset if different (use only for single columns).",
    )

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v: str | None) -> str | None:
        """Reject a `pattern` that is not a valid regular expression.

        Args:
            v (str | None): Configured pattern.

        Returns:
            str | None: The pattern, unchanged.

        Raises:
            ValueError: If the pattern does not compile.
        """
        if v is not None:
            try:
                re.compile(v)
            except re.error as err:
                raise ValueError(f"Invalid regex pattern '{v}': {err}") from err
        return v

    @field_validator("null_values")
    @classmethod
    def validate_null_values(cls, v: list[SentinelValue] | None) -> list[SentinelValue] | None:
        """Reject a sentinel that can never match, such as NaN.

        Args:
            v (list[SentinelValue] | None): Configured sentinels.

        Returns:
            list[SentinelValue] | None: The sentinels, unchanged.

        Raises:
            ValueError: If any entry is a non-finite float.
        """
        _reject_non_finite_sentinels(v)
        return v

    @field_validator("regex_replace")
    @classmethod
    def validate_regex_replace(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        """Reject a `regex_replace` key that is not a valid regular expression.

        Args:
            v (dict[str, str] | None): Patterns and their replacements.

        Returns:
            dict[str, str] | None: The mapping, unchanged.

        Raises:
            ValueError: If a pattern does not compile.
        """
        if v is not None:
            for pattern in v:
                try:
                    re.compile(pattern)
                except re.error as err:
                    raise ValueError(f"Invalid regex replace pattern '{pattern}': {err}") from err
        return v

    @model_validator(mode="after")
    def validate_similarity_measure(self) -> "DiffRule":
        """Reject a rule that sets both text similarity limits.

        Returns:
            DiffRule: The validated rule.

        Raises:
            ValueError: If both `max_levenshtein_distance` and
                `min_jaro_winkler_similarity` are set.
        """
        if (
            self.max_levenshtein_distance is not None
            and self.min_jaro_winkler_similarity is not None
        ):
            raise ValueError(
                "Set max_levenshtein_distance or min_jaro_winkler_similarity, not both: "
                "a rule compares text with one similarity measure."
            )
        return self


class DiffConfig(BaseModel):
    """Settings and rules for one comparison.

    Attributes:
        primary_keys (list[str]): Columns that join the datasets. At least one is
            required, and together they must be unique in each dataset.
        schema_mode (SchemaMode): How strictly the two sides' columns must agree.
            Defaults to `intersection`.
        strict_types (bool): Whether a column stored as different types on the two
            sides fails every row. Defaults to False, which compares such a column
            anyway: two numeric types compare by value, so an integer `10` and a
            float `10.7` differ, and any other pair casts the target to the source
            type, with values that do not convert becoming NULL.
        normalize_column_names (bool): Whether to strip whitespace from column
            names and lowercase them before alignment. Defaults to False.
        default_absolute_tolerance (float): Absolute tolerance for numeric columns
            whose rule sets none. Defaults to 0.
        default_relative_tolerance (float): Relative tolerance for numeric columns
            whose rule sets none. Defaults to 0.
        default_treat_null_as_equal (bool): Whether two NULLs match in columns whose
            rule does not say. Defaults to True.
        default_whitespace_mode (WhitespaceMode): Whitespace mode for text columns
            whose rule sets none. Defaults to `none`.
        default_null_values (list[SentinelValue]): Values to read as NULL in every
            column. Each applies only to columns whose type can hold it, and the
            rest are skipped without an error.
        rules (list[DiffRule]): Per-column overrides. A rule naming a column wins
            over a `pattern` rule.
        threshold (float): Largest share of mismatched rows, from 0 to 1, that
            still counts as a match. Defaults to 0.
        report_top_columns_limit (int): Most drifted columns to list in
            `report_summary` and the Markdown summary. Defaults to 5.
        pushdown_sample_rows (int): Changed rows a pushdown run fetches with both
            sides' values, so the HTML report and the result can show values
            instead of keys. Defaults to 0, which fetches none, so no value leaves
            the warehouse. A local run holds every row already.
        output_path (str | None): Folder to write the added, removed, and changed
            rows to. None, the default, writes nothing.
        output_format (ArtifactFormat): File format of those rows. Defaults to
            `parquet`.
    """

    model_config = ConfigDict(extra="forbid")

    primary_keys: list[str] = Field(
        ..., min_length=1, description="Columns used to join datasets (at least one)."
    )

    schema_mode: SchemaMode = Field(
        default="intersection",
        description="Schema enforcement mode: 'exact', 'allow_additions', 'allow_removals', or 'intersection'.",
    )
    strict_types: bool = Field(
        default=False,
        description=(
            "Whether differing column types fail every row. If False, numeric types compare "
            "by value and other types cast the target to the source type."
        ),
    )

    normalize_column_names: bool = Field(
        default=False,
        description="Whether to strip whitespace from column names and lowercase them.",
    )

    default_absolute_tolerance: float = Field(
        default=0.0,
        ge=0.0,
        strict=True,
        allow_inf_nan=False,
        description="Global absolute tolerance for numeric columns.",
    )
    default_relative_tolerance: float = Field(
        default=0.0,
        ge=0.0,
        strict=True,
        allow_inf_nan=False,
        description="Global relative tolerance for numeric columns.",
    )
    default_treat_null_as_equal: bool = Field(
        default=True, description="Globally treat NULL == NULL as a match."
    )
    default_whitespace_mode: WhitespaceMode = Field(
        default="none",
        description="Global string whitespace stripping mode: 'none', 'left', 'right', or 'both'.",
    )
    default_null_values: list[SentinelValue] = Field(
        default_factory=list[SentinelValue],
        strict=True,
        description="Global list of values to coerce to NULL, applied per matching dtype.",
    )

    rules: list[DiffRule] = Field(default_factory=list[DiffRule], description="Column overrides.")

    threshold: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Allowed mismatch ratio (0.0 to 1.0)."
    )

    report_top_columns_limit: int = Field(
        default=5,
        ge=0,
        description="Max number of top drifted columns to show in the report summary.",
    )

    pushdown_sample_rows: int = Field(
        default=0,
        ge=0,
        strict=True,
        description=(
            "Pushdown only: fetch up to this many changed rows with both "
            "sides' values. 0 fetches none, so no value leaves the warehouse."
        ),
    )

    output_path: str | None = Field(
        default=None, description="Directory to write discrepancy artifacts to."
    )
    output_format: ArtifactFormat = Field(
        default="parquet",
        description="File format for exported discrepancy artifacts, such as 'parquet'.",
    )

    @field_validator("default_null_values")
    @classmethod
    def validate_default_null_values(cls, v: list[SentinelValue]) -> list[SentinelValue]:
        """Reject a default sentinel that can never match, such as NaN.

        Args:
            v (list[SentinelValue]): Configured default sentinels.

        Returns:
            list[SentinelValue]: The sentinels, unchanged.

        Raises:
            ValueError: If any entry is a non-finite float.
        """
        _reject_non_finite_sentinels(v)
        return v

    @model_validator(mode="after")
    def apply_schema_normalization(self) -> "DiffConfig":
        r"""Lowercase and strip configured column names when normalization is enabled.

        Keys, rule `column_names`, and `rename_to` are normalized the same way
        the engine normalizes headers, so every name still refers to a column.
        A `pattern` is left alone: lowercasing a regex changes what it means
        (`\D` is not `\d`), so patterns are written against the lowercase names.
        Rules are copied rather than edited, since the caller may still hold them.

        Returns:
            DiffConfig: The configuration with normalized names.
        """
        if self.normalize_column_names:
            self.primary_keys = [normalize_column_name(pk) for pk in self.primary_keys]
            self.rules = [
                rule.model_copy(
                    update={
                        "column_names": [normalize_column_name(col) for col in rule.column_names],
                        "rename_to": rule.rename_to and normalize_column_name(rule.rename_to),
                    }
                )
                for rule in self.rules
            ]

        return self


class DiffSummary(BaseModel):
    """Counts and verdict for one comparison.

    Attributes:
        total_rows_source (int): Rows in the source dataset.
        total_rows_target (int): Rows in the target dataset.
        added_count (int): Rows only in the target.
        removed_count (int): Rows only in the source.
        changed_count (int): Rows in both datasets with at least one differing
            column.
        column_mismatches (dict[str, int]): Mismatched rows per compared column.
        is_match (bool): Whether the mismatch ratio is within `threshold`.
        total_mismatches (int): Added, removed, and changed rows together.
        mismatch_ratio (float): `total_mismatches` divided by the source row count.
        match_rate_percentage (float): Match rate as a percentage, such as `99.98`.
        is_perfect_match (bool): Whether nothing mismatched.
        volume_shift (int): Target rows minus source rows.
        report_summary (str): Plain-text report for CI logs.
        report_limit (int): Most columns `report_summary` lists. Left out of JSON.
        artifacts_written (bool): Whether artifacts were written to `output_path`.
            Pushdown writes primary keys only, under `_pks_only` file names. Left
            out of JSON.
    """

    model_config = ConfigDict(extra="forbid")

    total_rows_source: int
    total_rows_target: int
    added_count: int
    removed_count: int
    changed_count: int
    column_mismatches: dict[str, int] = Field(default_factory=dict)
    is_match: bool

    report_limit: int = Field(default=5, exclude=True)
    artifacts_written: bool = Field(default=False, exclude=True)

    @computed_field
    @property
    def total_mismatches(self) -> int:
        """Count added, removed, and changed rows together.

        Returns:
            int: The total.
        """
        return self.added_count + self.removed_count + self.changed_count

    @computed_field
    @property
    def mismatch_ratio(self) -> float:
        """Divide the mismatches by the source row count.

        Returns:
            float: The ratio, which can exceed 1.
        """
        return mismatch_ratio_of(self.total_mismatches, self.total_rows_source)

    @computed_field
    @property
    def match_rate_percentage(self) -> float:
        """Express the match rate as a percentage.

        Returns:
            float: The percentage, rounded to two decimal places, such as `99.98`.
        """
        return round((1.0 - self.mismatch_ratio) * 100.0, 2)

    @computed_field
    @property
    def is_perfect_match(self) -> bool:
        """Return whether nothing mismatched under the configured rules.

        Returns:
            bool: Whether `total_mismatches` is zero.
        """
        return self.total_mismatches == 0

    @computed_field
    @property
    def volume_shift(self) -> int:
        """Subtract the source row count from the target's.

        Returns:
            int: Target rows minus source rows.
        """
        return self.total_rows_target - self.total_rows_source

    @computed_field
    @property
    def report_summary(self) -> str:
        """Format the counts and the most drifted columns as plain text for a CI log.

        Returns:
            str: The report.
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

        col_report = "".join(f"- {col}: {count:,} mismatches\n" for col, count in top_cols)
        return f"{base_report}\nTop Column-Level Drifts:\n---------------------------\n{col_report}"


@dataclass(frozen=True)
class DiffResult:
    """A completed comparison: the counts plus the rows behind them.

    `DiffSummary` serializes to JSON, so it cannot carry frames. This class pairs it
    with the discrepancy rows the engine already materialized.

    Attributes:
        summary (DiffSummary): Counts, ratios, and the formatted report.
        added (pl.DataFrame): Rows present only in the target.
        removed (pl.DataFrame): Rows present only in the source.
        changed (pl.DataFrame): Rows present in both with at least one differing
            column. A local run carries `{column}_source`, `{column}_target`, and
            `{column}_is_match` for every compared column, and a pushdown run
            carries primary keys alone.
        primary_keys (tuple[str, ...]): Join keys, in configured order.
        compared_columns (tuple[str, ...]): Columns compared, after renames and
            exclusions. Recorded so `get_mismatches` refuses a mistyped name on both
            engines, including pushdown, whose frames cannot show which columns
            were compared.
        keys_only (bool): Whether the run was pushdown, which returns primary keys
            instead of rows.
        changed_sample (pl.DataFrame | None): Pushdown only, when
            `pushdown_sample_rows` is set: up to that many changed rows, in key
            order, with `{column}_source`, `{column}_target`, and
            `{column}_is_match` for every compared column, as a local run's
            `changed` carries them. None when no sample was asked for, nothing
            changed, or the run was local, where `changed` holds every row.
    """

    summary: DiffSummary
    added: pl.DataFrame
    removed: pl.DataFrame
    changed: pl.DataFrame
    primary_keys: tuple[str, ...] = ()
    compared_columns: tuple[str, ...] = ()
    keys_only: bool = False
    changed_sample: pl.DataFrame | None = None

    def get_mismatches(self, column: str) -> pl.DataFrame:
        """Isolate the rows where one column disagreed.

        Args:
            column (str): Compared column to isolate, named as it appears after
                any `rename_to`.

        Returns:
            pl.DataFrame: Primary keys alongside the source and target values,
            restricted to rows where this column differed. Pushdown runs return
            every changed primary key instead, since the warehouse never
            projected the values and cannot attribute a row to one column.

        Raises:
            ConfigError: If the column was not part of the comparison.

        Examples:
            >>> import polars as pl
            >>> from veridelta.engine import DiffEngine
            >>> source = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 20.0]})
            >>> target = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 21.5]})
            >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
            >>> result.get_mismatches("amount")["id"].to_list()
            [2]
        """
        if column not in self.compared_columns:
            compared = ", ".join(self.compared_columns) or "none"
            raise ConfigError(
                f"Column '{column}' was not compared, so it has no mismatches to "
                f"report. Compared columns: {compared}."
            )
        if self.keys_only:
            return self.changed
        return self.changed.filter(~pl.col(f"{column}_is_match")).select(
            [*self.primary_keys, f"{column}_source", f"{column}_target"]
        )

    def to_pandas(self) -> "pd.DataFrame":
        """Convert the changed rows to pandas for notebook use.

        Returns:
            pd.DataFrame: `changed` as a pandas DataFrame. The other frames
            convert the same way through Polars' own `to_pandas`.

        Raises:
            ConfigError: If pandas or pyarrow is not installed.
        """
        try:
            return self.changed.to_pandas()
        except ImportError as exc:
            raise ConfigError(
                "Converting to pandas requires pandas and pyarrow, which Veridelta "
                f"does not depend on. Install them to use this method. ({exc})"
            ) from exc


class ValueMapEntry(BaseModel):
    """One proposed `value_map` entry and the rows that support it.

    Attributes:
        source_value (str): Source text as the `value_map` stage sees it, after
            null sentinels, regex replacement, whitespace, and case folding.
        target_value (str): The target value those rows compare against, as text.
        rows (int): Joined rows whose source holds `source_value`, whatever
            their target, NULL included.
        agreeing_rows (int): Those rows whose target is `target_value`.
        confidence (float): `agreeing_rows / rows`, the share of the source
            value's rows the entry would make match.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_value: str = Field(..., description="Source text as the value_map stage sees it.")
    target_value: str = Field(..., description="Target value the rows compare against.")
    rows: int = Field(..., ge=1, description="Joined rows holding the source value.")
    agreeing_rows: int = Field(
        ..., ge=1, description="Rows among them whose target is the target value."
    )

    @computed_field
    @property
    def confidence(self) -> float:
        """Return the share of the source value's rows that agree.

        Returns:
            float: `agreeing_rows / rows`.
        """
        return self.agreeing_rows / self.rows

    @model_validator(mode="after")
    def validate_counts(self) -> "ValueMapEntry":
        """Reject more agreeing rows than rows.

        Returns:
            ValueMapEntry: The validated entry.

        Raises:
            ValueError: If `agreeing_rows` exceeds `rows`.
        """
        if self.agreeing_rows > self.rows:
            raise ValueError(
                f"agreeing_rows ({self.agreeing_rows}) cannot exceed rows ({self.rows})."
            )
        return self


class ValueMapProposal(BaseModel):
    """A proposed `value_map` for one column, with the evidence for each new entry.

    Attributes:
        column (str): Compared column, named as it appears after any `rename_to`.
        value_map (dict[str, str]): The governing rule's existing entries plus
            the proposed ones.
        entries (tuple[ValueMapEntry, ...]): The proposed entries alone, most
            agreeing rows first.
        governing_rule_index (int | None): Position in `DiffConfig.rules` of the
            rule that governs the column today, or None when no rule does. Only
            one rule governs a column, so new entries belong in that rule.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    column: str = Field(..., description="Compared column, after any rename_to.")
    value_map: dict[str, str] = Field(..., description="Existing plus proposed entries.")
    entries: tuple[ValueMapEntry, ...] = Field(..., description="Proposed entries alone.")
    governing_rule_index: int | None = Field(
        default=None, description="Index of the rule that governs the column today."
    )

    def to_rule(self) -> DiffRule:
        """Build a rule for the column carrying the proposed map.

        When `governing_rule_index` is set, merge `value_map` into that rule
        instead, since a second rule for the column would not apply.

        Returns:
            DiffRule: Rule naming the column, with the full proposed map.
        """
        return DiffRule(column_names=[self.column], value_map=self.value_map)


FindingSeverity = Literal["error", "warning"]
"""How sure a configuration check is: an `error` stops the run, and a `warning`
stops it only if the tables hold what the setting cannot handle."""


class ConfigFinding(BaseModel):
    """One problem found by checking a configuration without running it.

    Attributes:
        severity (FindingSeverity): `error` when the run would fail, `warning`
            when it depends on stored column names or types.
        message (str): What is wrong and what to do about it.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    severity: FindingSeverity = Field(..., description="error or warning.")
    message: str = Field(..., description="What is wrong and what to do about it.")


class SnowflakeConfig(BaseModel):
    """Immutable connection settings for Snowflake warehouse pushdown.

    Attributes:
        type (Literal["snowflake"]): Source kind, which selects this model.
        table (str): Fully qualified table or view to compare.
        account (str): Snowflake account identifier.
        user (str): Login name used to authenticate the session.
        warehouse (str): Virtual warehouse that executes pushdown SQL.
        database (str): Default database for unqualified object names.
        schema_name (str): Default schema for unqualified object names.
        password (str | None): Optional password or programmatic access
            token, unset for a key pair or SSO. Left out when the config is
            printed, but kept by `model_dump()`, which the connector needs.
        private_key_path (str | None): Optional path to a PEM private key, for
            key-pair sign-in instead of a password. Left out when printed.
        private_key_passphrase (str | None): Passphrase of an encrypted
            `private_key_path`. Left out when printed.
        role (str | None): Optional role assumed after authentication.
    """

    # Credentials pass through here, and Pydantic quotes raw input in its errors.
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["snowflake"] = Field("snowflake", description="Discriminator for Snowflake.")
    table: str = Field(
        ...,
        pattern=SQL_RELATION_PATTERN,
        description="Fully qualified table or view to compare.",
    )
    account: str = Field(..., description="Snowflake account identifier.")
    user: str = Field(..., description="Login name used to authenticate the session.")
    warehouse: str = Field(..., description="Virtual warehouse that executes pushdown SQL.")
    database: str = Field(..., description="Default database for unqualified object names.")
    schema_name: str = Field(..., description="Default schema for unqualified object names.")
    password: str | None = Field(
        default=None,
        repr=False,
        description="Password or programmatic access token; omitted for a key pair or SSO.",
    )
    private_key_path: str | None = Field(
        default=None,
        repr=False,
        description="Path to a PEM private key, for key-pair sign-in instead of a password.",
    )
    private_key_passphrase: str | None = Field(
        default=None, repr=False, description="Passphrase of an encrypted private_key_path."
    )
    role: str | None = Field(
        default=None, description="Optional role assumed after authentication."
    )

    @model_validator(mode="after")
    def validate_sign_in(self) -> "SnowflakeConfig":
        """Reject credentials that leave unclear how the session signs in.

        Returns:
            SnowflakeConfig: The validated instance.

        Raises:
            ValueError: If both `password` and `private_key_path` are set, or
                `private_key_passphrase` is set without `private_key_path`.
        """
        if self.password is not None and self.private_key_path is not None:
            raise ValueError(
                "Snowflake signs in with a 'password' or a 'private_key_path', not both."
            )
        if self.private_key_passphrase is not None and self.private_key_path is None:
            raise ValueError(
                "'private_key_passphrase' decrypts 'private_key_path', which is not set."
            )
        return self


class DatabricksConfig(BaseModel):
    """Immutable connection settings for Databricks SQL warehouse pushdown.

    Attributes:
        type (Literal["databricks"]): Source kind, which selects this model.
        table (str): Fully qualified table or view to compare.
        server_hostname (str): Workspace hostname for the SQL warehouse.
        http_path (str): HTTP path of the SQL warehouse or cluster.
        access_token (str | None): Optional personal access token. Left out
            when the config is printed, but kept by `model_dump()`, which the
            connector needs.
        catalog (str | None): Optional Unity Catalog name.
        schema_name (str | None): Optional default schema name.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["databricks"] = Field("databricks", description="Discriminator for Databricks.")
    table: str = Field(
        ...,
        pattern=SQL_RELATION_PATTERN,
        description="Fully qualified table or view to compare.",
    )
    server_hostname: str = Field(..., description="Workspace hostname for the SQL warehouse.")
    http_path: str = Field(..., description="HTTP path of the SQL warehouse or cluster.")
    access_token: str | None = Field(
        default=None, repr=False, description="Optional personal access token."
    )
    catalog: str | None = Field(default=None, description="Optional Unity Catalog name.")
    schema_name: str | None = Field(default=None, description="Optional default schema name.")


class BigQueryConfig(BaseModel):
    """Immutable connection settings for BigQuery warehouse pushdown.

    Attributes:
        type (Literal["bigquery"]): Source kind, which selects this model.
        table (str): Table to compare, as `dataset.table`, or `table` when
            `dataset` names the default dataset.
        project (str): Google Cloud project that runs the queries and holds
            the data. It never appears in SQL.
        dataset (str | None): Default dataset for a table named without one.
        location (str | None): Location the queries run in, such as `US` or
            `europe-west2`. BigQuery infers it when unset.
        credentials_path (str | None): Service account key file. Application
            Default Credentials are used when unset. Left out when the config
            is printed, but kept by `model_dump()`, which the connector needs.
        maximum_bytes_billed (int | None): Fail any statement that would bill
            more bytes than this, instead of running it.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["bigquery"] = Field("bigquery", description="Discriminator for BigQuery.")
    table: str = Field(
        ...,
        pattern=BIGQUERY_TABLE_PATTERN,
        description="Table to compare, as dataset.table, or table with a default dataset.",
    )
    project: str = Field(
        ...,
        pattern=BIGQUERY_PROJECT_PATTERN,
        description="Google Cloud project that runs the queries and holds the data.",
    )
    dataset: str | None = Field(
        default=None,
        pattern=SQL_IDENTIFIER_SEGMENT_PATTERN,
        description="Default dataset for a table named without one.",
    )
    location: str | None = Field(
        default=None, description="Location the queries run in, such as US or europe-west2."
    )
    credentials_path: str | None = Field(
        default=None,
        repr=False,
        description="Service account key file; Application Default Credentials when unset.",
    )
    maximum_bytes_billed: int | None = Field(
        default=None,
        ge=1,
        strict=True,
        description="Fail any statement that would bill more bytes than this.",
    )

    @model_validator(mode="after")
    def validate_dataset(self) -> "BigQueryConfig":
        """Require a default dataset when the table is named without one.

        Returns:
            BigQueryConfig: The validated configuration.

        Raises:
            ValueError: If `table` has one segment and `dataset` is unset.
        """
        if "." not in self.table and self.dataset is None:
            raise ValueError(
                "A 'table' without a dataset needs 'dataset'; write it as dataset.table, "
                "or name the default dataset."
            )
        return self


class DeltaLakeConfig(BaseModel):
    """Immutable settings for a Delta Lake table scan.

    Attributes:
        type (Literal["delta"]): Source kind, which selects this model.
        table_uri (str): Filesystem path or object-store URI of the table.
        version (int | None): Optional table version to time-travel.
        storage_options (dict[str, str]): Object-store credentials and options.
            Left out when the config is printed, but kept by `model_dump()`,
            which the scanner needs.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["delta"] = Field("delta", description="Discriminator for Delta Lake.")
    table_uri: str = Field(..., description="Filesystem path or object-store URI of the table.")
    version: int | None = Field(
        default=None,
        ge=0,
        strict=True,
        description="Optional table version to time-travel.",
    )
    storage_options: dict[str, str] = Field(
        default_factory=dict,
        repr=False,
        description="Object-store credentials and options passed to Polars.",
    )


class IcebergConfig(BaseModel):
    """Immutable settings for an Apache Iceberg table scan.

    Attributes:
        type (Literal["iceberg"]): Source kind, which selects this model.
        table_uri (str): Catalog identifier or filesystem URI of the table.
        snapshot_id (int | None): Optional snapshot to time-travel.
        storage_options (dict[str, str]): Object-store credentials and options.
            Left out when the config is printed, but kept by `model_dump()`,
            which the scanner needs.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["iceberg"] = Field("iceberg", description="Discriminator for Apache Iceberg.")
    table_uri: str = Field(..., description="Catalog identifier or filesystem URI of the table.")
    snapshot_id: int | None = Field(
        default=None,
        ge=0,
        strict=True,
        description="Optional snapshot identifier to time-travel.",
    )
    storage_options: dict[str, str] = Field(
        default_factory=dict,
        repr=False,
        description="Object-store credentials and options passed to Polars.",
    )


POSTGRES_SCHEMES: Final = frozenset({"postgres", "postgresql"})
"""The URI schemes that name a Postgres database."""

DATABASE_PUSHDOWN_SCHEMES: Final = POSTGRES_SCHEMES
"""URI schemes whose tables a database source can compare inside the database."""


class DatabaseConfig(BaseModel):
    """Immutable settings for reading a database table or query into a local comparison.

    The rows are read through ConnectorX into Polars and compared by the local
    engine, so a database pairs with files, lakehouse tables, or another
    database. Requires the `database` extra (`uv add 'veridelta[database]'`).
    Two Postgres tables on one connection can instead be compared inside the
    database, without reading their rows, when both sides set `pushdown`.

    Attributes:
        type (Literal["database"]): Source kind, which selects this model.
        uri (str): ConnectorX connection URI, such as
            `postgresql://analyst@db.internal:5432/sales` or
            `sqlite:///srv/data/legacy.db`. A password written inside it is
            masked when the config is printed, but kept by `model_dump()`,
            which the connector needs.
        password (str | None): Optional password, percent-encoded into the
            URI's user information when the connector reads. Left out when the
            config is printed, but kept by `model_dump()`.
        table (str | None): Table or view to read whole, as one to three
            unquoted identifier segments.
        query (str | None): SQL statement to run instead, sent to the database
            exactly as written. Set exactly one of `table` and `query`.
        pushdown (bool): Whether to compare inside the database instead of
            reading the rows. Postgres `table` sources only, and both sides must
            set it. Defaults to False.
        partition_on (str | None): Integer column to split a `table` read on,
            so ConnectorX reads its ranges over several connections at once.
            Set it with `partitions`. The column may hold no NULL, since a
            range read leaves those rows out, so the read checks it first.
        partitions (int | None): How many ranges to split the read into: 2
            or more. Set it with `partition_on`.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["database"] = Field("database", description="Discriminator for databases.")
    uri: str = Field(
        ..., description="ConnectorX connection URI, such as postgresql://user@host/db."
    )
    password: str | None = Field(
        default=None,
        repr=False,
        description="Optional password, percent-encoded into the URI when the source is read.",
    )
    table: str | None = Field(
        default=None,
        pattern=SQL_RELATION_PATTERN,
        description="Table or view to read whole.",
    )
    query: str | None = Field(
        default=None,
        min_length=1,
        description="SQL statement to run instead of reading a table, sent as written.",
    )
    pushdown: bool = Field(
        default=False,
        strict=True,
        description=(
            "Compare inside the database instead of reading the rows. Postgres tables "
            "only; set it on both sides."
        ),
    )
    partition_on: str | None = Field(
        default=None,
        pattern=SQL_IDENTIFIER_SEGMENT_PATTERN,
        description=(
            "Integer column to split a table read on, so ConnectorX reads its ranges in "
            "parallel. Set it with partitions."
        ),
    )
    partitions: int | None = Field(
        default=None,
        ge=2,
        strict=True,
        description="How many ranges to split the read into, 2 or more. Set it with partition_on.",
    )

    @model_validator(mode="after")
    def validate_connection(self) -> "DatabaseConfig":
        """Reject a source that is ambiguous about its rows or its password.

        Returns:
            DatabaseConfig: The validated instance.

        Raises:
            ValueError: If both or neither of `table` and `query` are set, the
                URI has no scheme, `password` conflicts with the URI, or
                `pushdown` is set on anything but a Postgres table.
        """
        if (self.table is None) == (self.query is None):
            raise ValueError(
                "A database source reads a 'table' or runs a 'query'; set exactly one."
            )
        parts = urlsplit(self.uri)
        if not parts.scheme:
            raise ValueError("'uri' needs a scheme such as postgresql:// or sqlite://.")
        if self.pushdown and parts.scheme.lower() not in DATABASE_PUSHDOWN_SCHEMES:
            raise ValueError(
                "'pushdown' compares inside the database and works on postgresql:// "
                "connections only."
            )
        if self.pushdown and self.table is None:
            raise ValueError("'pushdown' compares two tables, so set 'table' rather than 'query'.")
        if self.password is not None:
            if parts.password is not None:
                raise ValueError("Set the password in 'password' or inside 'uri', not both.")
            if not parts.username:
                raise ValueError(
                    "'password' needs a user name in 'uri', as in "
                    "postgresql://analyst@db.internal/sales."
                )
        return self

    @model_validator(mode="after")
    def validate_partitions(self) -> "DatabaseConfig":
        """Reject a split read that names half its settings or has nothing to split.

        Returns:
            DatabaseConfig: The validated instance.

        Raises:
            ValueError: If only one of `partition_on` and `partitions` is set,
                or they are set on a `query` or with `pushdown`.
        """
        if (self.partition_on is None) != (self.partitions is None):
            raise ValueError("Set 'partition_on' and 'partitions' together.")
        if self.partition_on is not None and self.table is None:
            raise ValueError(
                "A partitioned read can only split a 'table'; a 'query' runs as written."
            )
        if self.partition_on is not None and self.pushdown:
            raise ValueError("'pushdown' reads no rows, so there is no read to partition.")
        return self

    @property
    def redacted_uri(self) -> str:
        """Return the URI with any password in it replaced by `***`, and no query.

        The query goes whole, since a parameter such as `?password=` can carry
        a credential too.

        Returns:
            str: The URI, safe to print or log.
        """
        parts = urlsplit(self.uri)
        if parts.password is None and not parts.query:
            return self.uri
        netloc = parts.netloc
        if parts.password is not None:
            userinfo, _, hostinfo = netloc.rpartition("@")
            netloc = f"{userinfo.partition(':')[0]}:***@{hostinfo}"
        # Written out, since `urlunsplit` drops the `//` of a URI with no host, such as SQLite's.
        return f"{parts.scheme}://{netloc}{parts.path}"

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        """Print the URI with its password masked.

        `repr()`, `str()`, and rich displays all read from here, while
        `model_dump()` and the connector get the URI as written.

        Yields:
            tuple[str | None, Any]: Each field name with the value to print.
        """
        for name, value in super().__repr_args__():
            yield (name, self.redacted_uri) if name == "uri" else (name, value)


_MOTHERDUCK_PREFIXES: Final = ("md:", "motherduck:")
"""Prefixes that make a DuckDB `database` a MotherDuck database rather than a file."""


class DuckDBConfig(BaseModel):
    """Immutable settings for reading a DuckDB or MotherDuck table or query into a local comparison.

    The rows are read through DuckDB into Polars and compared by the local
    engine, so a DuckDB source pairs with files, lakehouse tables, databases,
    or another DuckDB source. Requires the `duckdb` extra
    (`uv add 'veridelta[duckdb]'`). Two tables in one database can instead be
    compared inside DuckDB, without reading their rows, when both sides set
    `pushdown`.

    Attributes:
        type (Literal["duckdb"]): Source kind, which selects this model.
        database (str): Path to a DuckDB file, which opens read-only, or a
            MotherDuck database written as `md:name` or `motherduck:name`.
        table (str | None): Table or view to read whole, as one to three
            unquoted identifier segments, such as `main.orders`.
        query (str | None): SQL statement to run instead, sent to DuckDB
            exactly as written. Set exactly one of `table` and `query`.
        motherduck_token (str | None): Token for a MotherDuck database.
            When unset, the `MOTHERDUCK_TOKEN` environment variable supplies
            it, then `motherduck_token`. Left out when the config is printed,
            but kept by `model_dump()`.
        pushdown (bool): Whether to compare inside DuckDB instead of reading
            the rows. `table` sources only, and both sides must set it.
            Defaults to False.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["duckdb"] = Field(
        "duckdb", description="Discriminator for DuckDB and MotherDuck."
    )
    database: str = Field(
        ...,
        min_length=1,
        description="DuckDB file path, or md:name or motherduck:name for a MotherDuck database.",
    )
    table: str | None = Field(
        default=None,
        pattern=SQL_RELATION_PATTERN,
        description="Table or view to read whole.",
    )
    query: str | None = Field(
        default=None,
        min_length=1,
        description="SQL statement to run instead of reading a table, sent as written.",
    )
    motherduck_token: str | None = Field(
        default=None,
        repr=False,
        description="Token for a MotherDuck database. Defaults to MOTHERDUCK_TOKEN.",
    )
    pushdown: bool = Field(
        default=False,
        strict=True,
        description=(
            "Compare inside DuckDB instead of reading the rows. Tables only; set it on both sides."
        ),
    )

    @property
    def is_motherduck(self) -> bool:
        """Return whether `database` names a MotherDuck database rather than a file.

        Returns:
            bool: Whether `database` starts with `md:` or `motherduck:`, in any case.
        """
        return self.database.lower().startswith(_MOTHERDUCK_PREFIXES)

    @model_validator(mode="after")
    def validate_connection(self) -> "DuckDBConfig":
        """Reject a source that is ambiguous about its rows or would expose its token.

        Returns:
            DuckDBConfig: The validated instance.

        Raises:
            ValueError: If both or neither of `table` and `query` are set,
                `database` is in memory or holds a token, a token is set for a
                file, or `pushdown` is set on a `query`.
        """
        if (self.table is None) == (self.query is None):
            raise ValueError("A DuckDB source reads a 'table' or runs a 'query'; set exactly one.")
        if self.pushdown and self.table is None:
            raise ValueError("'pushdown' compares two tables, so set 'table' rather than 'query'.")
        # Messages never repeat `database`, which may hold the token they refuse.
        if self.database.lower().startswith(":memory:"):
            raise ValueError(
                "':memory:' opens a new, empty database. Point 'database' at a DuckDB "
                "file or an md: database."
            )
        if self.is_motherduck and "token" in self.database.partition("?")[2].lower():
            raise ValueError(
                "Set the MotherDuck token in 'motherduck_token' or MOTHERDUCK_TOKEN rather "
                "than in 'database', which is printed and logged."
            )
        if self.motherduck_token is not None and not self.is_motherduck:
            raise ValueError("'motherduck_token' is for an md: database; a DuckDB file has none.")
        return self


SourceRef = Annotated[
    SourceConfig
    | SnowflakeConfig
    | DatabricksConfig
    | BigQueryConfig
    | DeltaLakeConfig
    | IcebergConfig
    | DatabaseConfig
    | DuckDBConfig,
    Field(discriminator="type"),
]
"""YAML/Python source or target: file, warehouse, lakehouse, database, or DuckDB."""
