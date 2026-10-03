# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for value map proposals: thresholds and the warehouse route."""

from decimal import Decimal
from typing import Any

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig


def _engine() -> DiffEngine:
    """Build an engine over one matching row, enough to reach the threshold checks."""
    frame = pl.DataFrame({"id": [1], "code": ["M"]}).lazy()
    return DiffEngine(DiffConfig(primary_keys=["id"]), frame, frame)


@pytest.mark.unit
@pytest.mark.fast
class TestValueMapThresholdTypes:
    """Validate that proposal thresholds are real numbers before anything reads rows."""

    @pytest.mark.parametrize(
        ("thresholds", "match"),
        [
            pytest.param(
                {"min_confidence": True}, "min_confidence must be a number", id="bool-confidence"
            ),
            pytest.param(
                {"min_confidence": Decimal("0.95")},
                "min_confidence must be a number",
                id="decimal-confidence",
            ),
            pytest.param(
                {"sample_fraction": True}, "sample_fraction must be a number", id="bool-share"
            ),
            pytest.param(
                {"min_support": True}, "min_support must be a whole number", id="bool-support"
            ),
            pytest.param(
                {"min_support": 5.0}, "min_support must be a whole number", id="float-support"
            ),
            pytest.param(
                {"min_support": Decimal("5")},
                "min_support must be a whole number",
                id="decimal-support",
            ),
        ],
    )
    def test_it_rejects_thresholds_that_are_not_real_numbers(
        self, thresholds: dict[str, Any], match: str
    ) -> None:
        """Ensure `True` or a Decimal never passes as a threshold, since `min_support` reaches SQL."""
        with pytest.raises(ConfigError, match=match):
            _engine().propose_value_maps(**thresholds)

    def test_it_accepts_whole_numbers_where_a_share_is_expected(self) -> None:
        """Ensure `1` is as good as `1.0` for a confidence or a sample share."""
        assert (
            _engine().propose_value_maps(min_confidence=1, sample_fraction=1, min_support=1) == []
        )
