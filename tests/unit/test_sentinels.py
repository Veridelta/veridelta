# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for dtype-aware null sentinel filtering."""

from decimal import Decimal

import polars as pl
import pytest

from veridelta.models import SentinelValue
from veridelta.sentinels import is_text_dtype, usable_sentinels

MIXED: list[SentinelValue] = ["N/A", -999, 0.5, False]


@pytest.mark.unit
@pytest.mark.fast
class TestUsableSentinels:
    """Validate which sentinels survive against each column type."""

    @pytest.mark.parametrize(
        ("dtype", "expected"),
        [
            (pl.String(), ["N/A"]),
            (pl.Categorical(), ["N/A"]),
            (pl.Enum(["N/A"]), ["N/A"]),
            (pl.Int64(), [-999, 0.5]),
            (pl.UInt8(), [-999, 0.5]),
            (pl.Float32(), [-999, 0.5]),
            (pl.Decimal(38, 0), [-999, 0.5]),
            (pl.Boolean(), [False]),
            (pl.Date(), []),
        ],
    )
    def test_it_keeps_only_the_sentinels_a_dtype_can_hold(
        self, dtype: pl.DataType, expected: list[SentinelValue]
    ) -> None:
        """Ensure each column type narrows one mixed list to its own family."""
        assert usable_sentinels(MIXED, dtype) == expected

    def test_it_separates_booleans_from_integers_in_both_directions(self) -> None:
        """Ensure isinstance(False, int) does not leak booleans into numerics."""
        assert usable_sentinels([False, True], pl.Int64()) == []
        assert usable_sentinels([0, 1], pl.Boolean()) == []

    def test_it_preserves_configured_order(self) -> None:
        """Ensure filtering never reorders the emitted IN list."""
        assert usable_sentinels([3, 1, 2], pl.Int64()) == [3, 1, 2]

    def test_it_returns_empty_for_missing_or_empty_input(self) -> None:
        """Ensure an unset list short-circuits without touching the dtype."""
        assert usable_sentinels(None, pl.String()) == []
        assert usable_sentinels([], pl.String()) == []

    def test_it_treats_decimal_columns_as_numeric(self) -> None:
        """Ensure Snowflake NUMBER(38,0) still matches integer sentinels."""
        frame = pl.DataFrame(
            {"amount": [Decimal(-999), Decimal(5)]}, schema={"amount": pl.Decimal(38, 0)}
        )
        dtype = frame.schema["amount"]
        assert usable_sentinels([-999], dtype) == [-999]
        assert frame.select(pl.col("amount").is_in([-999]))["amount"].to_list() == [True, False]


@pytest.mark.unit
@pytest.mark.fast
class TestIsTextDtype:
    """Validate the text family used for sentinel matching."""

    def test_it_accepts_dictionary_encoded_text(self) -> None:
        """Ensure categorical and enum count as text, as Arrow probes return them."""
        assert is_text_dtype(pl.Categorical()) is True
        assert is_text_dtype(pl.Enum(["a"])) is True
        assert is_text_dtype(pl.String()) is True

    def test_it_rejects_non_text_dtypes(self) -> None:
        """Ensure numeric and temporal columns are not treated as text."""
        assert is_text_dtype(pl.Int64()) is False
        assert is_text_dtype(pl.Datetime()) is False
