# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Fit the integration suite to the database `VERIDELTA_PARITY_BACKEND` selects.

A case marked `skip_on("postgres", reason=...)` skips on each backend it names,
because that backend's pushdown refuses it or cannot store its data. A case
marked `only_on("duckdb", reason=...)` pins one backend's own behavior and
skips on every other. On a live warehouse, the session first drops the tables
an earlier run left behind.
"""

import pytest

from tests.integration import warehouse_harness
from tests.integration.duckdb_harness import PARITY_BACKEND


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip a case its markers keep off the selected backend."""
    for marker in item.iter_markers("skip_on"):
        if PARITY_BACKEND in marker.args:
            pytest.skip(marker.kwargs["reason"])
    for marker in item.iter_markers("only_on"):
        if PARITY_BACKEND not in marker.args:
            pytest.skip(marker.kwargs["reason"])


@pytest.fixture(scope="session", autouse=True)
def _drop_stale_warehouse_tables() -> None:
    """Drop the tables a failed run left on a live warehouse, once a day old."""
    if PARITY_BACKEND in warehouse_harness.SERVICES:
        warehouse_harness.drop_stale_tables(PARITY_BACKEND)
