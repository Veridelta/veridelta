# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Property test: the local engine and compiled pushdown SQL reach the same verdict.

The hand-written parity suite pins the cases someone thought of. This one
draws configurations and data from `parity_strategies`, runs each case through
both engines, and requires the same outcome: the same counts, the same
per-column drift, and the same compared columns, or the same error.

The default `ci` profile is derandomized, so every run tries the same cases
and a failure reproduces. Set `VERIDELTA_HYPOTHESIS_PROFILE=deep` to search
further before a release.
"""

import os
from collections.abc import Callable

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings

from tests.integration.duckdb_harness import run_local, run_pushdown
from tests.integration.parity_strategies import comparison_cases
from veridelta.exceptions import ConnectorError, VerideltaError
from veridelta.models import DiffConfig, DiffResult

settings.register_profile(
    "ci",
    max_examples=60,
    derandomize=True,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.register_profile(
    "deep",
    max_examples=2000,
    database=None,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)

_PROFILE = settings.get_profile(os.environ.get("VERIDELTA_HYPOTHESIS_PROFILE", "ci"))

Outcome = tuple[object, ...]


def _outcome(run: Callable[[], DiffResult]) -> Outcome:
    """Reduce a run to what both engines can report, or to the error it raised.

    A `ConnectorError` means DuckDB rejected the compiled SQL, which is never
    an acceptable outcome, so it propagates and fails the case.
    """
    try:
        result = run()
    except ConnectorError:
        raise
    except VerideltaError as exc:
        return ("error", type(exc).__name__)
    summary = result.summary
    return (
        summary.total_rows_source,
        summary.total_rows_target,
        summary.added_count,
        summary.removed_count,
        summary.changed_count,
        summary.column_mismatches,
        frozenset(result.compared_columns),
    )


@pytest.mark.integration
@pytest.mark.property
class TestFuzzedParity:
    """Validate that drawn comparisons reach one verdict on both engines."""

    @_PROFILE
    @given(comparison_cases())
    def test_both_engines_reach_the_same_verdict(
        self, case: tuple[DiffConfig, pl.DataFrame, pl.DataFrame]
    ) -> None:
        """Ensure a local run and a pushdown run of the same case agree."""
        config, source, target = case

        local = _outcome(lambda: run_local(config, source, target))
        pushdown = _outcome(lambda: run_pushdown(config, source, target)[0])

        assert pushdown == local, (config.model_dump(exclude_defaults=True), source, target)
