# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run the examples in public docstrings, so the API reference stays true."""

import doctest
from types import ModuleType

import pytest

from veridelta import config, engine, models, report, telemetry

pytestmark = [pytest.mark.unit, pytest.mark.fast]


@pytest.mark.parametrize(
    "module", [config, engine, models, report, telemetry], ids=lambda module: module.__name__
)
def test_it_runs_every_docstring_example(module: ModuleType) -> None:
    """Fail when an example's output differs from the one its docstring shows."""
    results = doctest.testmod(module, optionflags=doctest.NORMALIZE_WHITESPACE)

    assert results.attempted > 0
    assert results.failed == 0
