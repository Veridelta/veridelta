# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Compare two datasets on their primary keys under rules you declare, on a laptop, in CI, or inside a warehouse."""

import logging

from veridelta import datasets
from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.exceptions import (
    ConfigError,
    ConnectorError,
    DataIntegrityError,
    DatasetError,
    VerideltaError,
)
from veridelta.models import (
    ArtifactFormat,
    BigQueryConfig,
    CastTarget,
    ConfigFinding,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    DiffSummary,
    DuckDBConfig,
    IcebergConfig,
    SchemaMode,
    SentinelValue,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    SourceType,
    ValueMapEntry,
    ValueMapProposal,
    WhitespaceMode,
)

__version__ = "0.19.19"

# A library leaves its log output to the program that uses it. Every module's
# logger sits under this one, so no record reaches Python's last-resort
# handler on stderr unless that program adds a handler.
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "ArtifactFormat",
    "BigQueryConfig",
    "CastTarget",
    "ConfigError",
    "ConfigFinding",
    "ConnectorError",
    "DataIntegrityError",
    "DatabaseConfig",
    "DatabricksConfig",
    "DatasetError",
    "DeltaLakeConfig",
    "DiffConfig",
    "DiffEngine",
    "DiffResult",
    "DiffRule",
    "DiffSummary",
    "DuckDBConfig",
    "IcebergConfig",
    "SchemaMode",
    "SentinelValue",
    "SnowflakeConfig",
    "SourceConfig",
    "SourceRef",
    "SourceType",
    "ValueMapEntry",
    "ValueMapProposal",
    "VerideltaError",
    "WhitespaceMode",
    "datasets",
    "load_config",
]
