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
    """Configuration for a specific data source (Source or Target)."""

    path: str = Field(..., description="File system path or URI to the data.")
    format: SourceType = Field(
        SourceType.CSV, description="The format of the file. Defaults to CSV."
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Format-specific options (e.g., {'separator': ';', 'has_header': True}).",
    )


class ColumnRule(BaseModel):
    """Rules for comparing a specific column across datasets."""

    name: str = Field(..., description="The name of the column in the source dataset.")
    tolerance: float | None = Field(
        None, description="Absolute tolerance for numeric differences (e.g., 0.001)."
    )
    ignore: bool = Field(
        False, description="If True, this column will be excluded from the comparison."
    )
    rename_to: str | None = Field(
        None, description="The name of the corresponding column in the target dataset if different."
    )


class DiffConfig(BaseModel):
    """The master configuration for a Veridelta comparison run."""

    source: SourceConfig = Field(..., description="Configuration for the 'Left' (Source) dataset.")
    target: SourceConfig = Field(..., description="Configuration for the 'Right' (Target) dataset.")
    primary_keys: list[str] = Field(
        ..., description="List of columns used to join and align the datasets."
    )
    rules: list[ColumnRule] = Field(
        default_factory=list, description="Specific per-column comparison overrides."
    )
    threshold: float = Field(
        0.0,
        description="The percentage (0.0 to 1.0) of mismatch allowed before the run fails.",
    )
    output_path: str | None = Field(
        None, description="Optional path to save the detailed diff report."
    )
