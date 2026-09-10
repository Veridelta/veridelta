# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Veridelta: Semantic diffing for mission-critical data pipelines."""

from veridelta import datasets
from veridelta.config import load_config
from veridelta.engine import DataIngestor, DiffEngine
from veridelta.exceptions import (
    ConfigError,
    ConnectorError,
    DataIntegrityError,
    VerideltaError,
)
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
    SchemaMode,
    SentinelValue,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    SourceType,
    WhitespaceMode,
)

__version__ = "0.6.0"

__all__ = [
    "ArtifactFormat",
    "CastTarget",
    "ConfigError",
    "ConnectorError",
    "DataIngestor",
    "DataIntegrityError",
    "DatabricksConfig",
    "DeltaLakeConfig",
    "DiffConfig",
    "DiffEngine",
    "DiffResult",
    "DiffRule",
    "DiffSummary",
    "IcebergConfig",
    "SchemaMode",
    "SentinelValue",
    "SnowflakeConfig",
    "SourceConfig",
    "SourceRef",
    "SourceType",
    "VerideltaError",
    "WhitespaceMode",
    "datasets",
    "load_config",
]
