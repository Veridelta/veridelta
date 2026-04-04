"""Data models for Veridelta configuration and results."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    """Supported and roadmap data formats for ingestion."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    FIXED_WIDTH = "fixed_width"
    NETCDF = "netcdf"
    SHAPEFILE = "shapefile"
    GEOPACKAGE = "geopackage"
    EXCEL = "excel"
    SQL = "sql"
    DELTA = "delta"
    AVRO = "avro"
    XML = "xml"
    ARROW = "arrow"


class SourceConfig(BaseModel):
    """Configuration for a specific data source.

    Attributes:
        path: File system path or URI to the data.
        format: The format of the file (e.g., CSV, Parquet).
        options: Format-specific kwargs passed to the loader.
    """

    path: str = Field(..., description="File system path or URI to the data.")
    format: SourceType = Field(SourceType.CSV, description="The format of the file.")
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific options (e.g., {'separator': ';'}).",
    )


class ColumnRule(BaseModel):
    """Rules for comparing a specific column across datasets.

    Attributes:
        name: The name of the column in the source dataset.
        tolerance: Absolute tolerance for numeric differences.
        ignore: Whether to skip this column during comparison.
        rename_to: The name in the target dataset if it differs from the source.
    """

    name: str = Field(..., description="The name of the column in the source.")
    tolerance: float | None = Field(
        default=None, description="Absolute tolerance for numeric differences (e.g., 0.001)."
    )
    ignore: bool = Field(
        default=False, description="If True, this column will be excluded from the comparison."
    )
    rename_to: str | None = Field(None, description="Name in target dataset if different.")


class DiffConfig(BaseModel):
    """The master configuration for a Veridelta comparison run.

    Attributes:
        source: Configuration for the 'Left' dataset.
        target: Configuration for the 'Right' dataset.
        primary_keys: Columns used to join and align the datasets.
        rules: List of per-column comparison overrides.
        threshold: Allowed mismatch percentage (0.0 to 1.0) before failure.
        output_path: Path to save the resulting diff report.
    """

    source: SourceConfig = Field(..., description="Config for the 'Left' dataset.")
    target: SourceConfig = Field(..., description="Config for the 'Right' dataset.")
    primary_keys: list[str] = Field(..., description="Columns used to join datasets.")
    rules: list[ColumnRule] = Field(default_factory=list, description="Column overrides.")
    threshold: float = Field(0.0, description="Allowed mismatch percentage (0.0 to 1.0).")
    output_path: str | None = Field(
        default=None, description="Optional path to save the detailed diff report."
    )


class DiffSummary(BaseModel):
    """The high-level results of a Veridelta comparison.

    Attributes:
        total_rows_source: Number of rows in the source dataset.
        total_rows_target: Number of rows in the target dataset.
        added_count: Rows found only in the target.
        removed_count: Rows found only in the source.
        changed_count: Rows present in both but with value differences.
        is_match: Boolean indicating if the diff is within the threshold.
    """

    total_rows_source: int
    total_rows_target: int
    added_count: int
    removed_count: int
    changed_count: int
    is_match: bool
