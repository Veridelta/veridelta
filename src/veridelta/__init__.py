# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Veridelta: Semantic diffing for mission-critical data pipelines."""

from veridelta import datasets
from veridelta.engine import DataIngestor, DiffEngine
from veridelta.models import (
    ColumnRule,
    DiffConfig,
    DiffSummary,
    SourceConfig,
)

__version__ = "0.1.0"

__all__ = [
    "ColumnRule",
    "DataIngestor",
    "DiffConfig",
    "DiffEngine",
    "DiffSummary",
    "SourceConfig",
    "datasets",
]
