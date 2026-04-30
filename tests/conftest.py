# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Global pytest fixtures for Veridelta test suites."""

import polars as pl
import pytest


@pytest.fixture
def base_src() -> pl.DataFrame:
    """Provide a minimal Source DataFrame for baseline testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "status": ["Active", "UNKNOWN", "Closed"],
            "amount": [10.5, 20.0, 30.0],
        }
    )


@pytest.fixture
def base_tgt() -> pl.DataFrame:
    """Provide a minimal Target DataFrame for baseline testing."""
    return pl.DataFrame(
        {
            "user_id": [1, 2, 3],  # Renamed from 'id'
            "status": ["Active", None, "Closed"],  # Native null instead of "UNKNOWN"
            "amount": [10.5, 20.0, 30.0],
        }
    )


@pytest.fixture
def complex_src() -> pl.DataFrame:
    """Provide a Source dataset with dirty strings and legacy formats."""
    return pl.DataFrame(
        {
            "invoice_id": ["INV-001", "INV-002", "INV-003"],
            "total_billed": ["$10.00", "$99.99", "$50.00"],  # Requires Regex + Cast
            "category": ["Enterprise", "Premium", "Standard"],
        }
    )


@pytest.fixture
def complex_tgt() -> pl.DataFrame:
    """Provide a Target dataset with strict typing and numeric drift."""
    return pl.DataFrame(
        {
            "invoice_id": ["INV-001", "INV-002", "INV-003"],
            "total_billed": [10.00, 99.98, 50.00],  # Float64, minus 1-cent tax bug on INV-002
            "category": ["ENT", "PRM", "STD"],  # Requires Value Map
        }
    )
