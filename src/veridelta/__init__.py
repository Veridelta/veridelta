# Copyright 2026 Nicholas Harder
# SPDX-License-Identifier: Apache-2.0

"""Veridelta: Semantic diffing for mission-critical data pipelines."""

from veridelta.engine import DataIngestor, DiffEngine
from veridelta.models import ColumnRule, DiffConfig, DiffSummary, SourceConfig

__version__ = "0.1.0"
__all__ = [
    "DiffConfig",
    "SourceConfig",
    "ColumnRule",
    "DiffSummary",
    "DiffEngine",
    "DataIngestor",
]
