# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Veridelta: Semantic diffing for mission-critical data pipelines."""

from veridelta import datasets
from veridelta.config import load_config
from veridelta.engine import DataIngestor, DiffEngine
from veridelta.models import (
    DiffConfig,
    DiffRule,
    DiffSummary,
    SourceConfig,
)

__version__ = "0.2.0"

__all__ = [
    "DataIngestor",
    "DiffConfig",
    "DiffEngine",
    "DiffRule",
    "DiffSummary",
    "SourceConfig",
    "datasets",
    "load_config",
]
