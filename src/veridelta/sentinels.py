# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Dtype-aware filtering for null sentinel values.

`null_values` accepts mixed scalars, but a sentinel is only meaningful against a
column whose type can hold it. Polars refuses the mismatches outright, and so do
warehouses, so both the local engine and the SQL compiler filter through this
module to guarantee they agree on which sentinels apply where.
"""

from collections.abc import Sequence

import polars as pl

from veridelta.models import SentinelValue


def is_text_dtype(dtype: pl.DataType) -> bool:
    """Return whether a column holds text.

    `Categorical` and `Enum` count as text. Arrow dictionary-encoded strings,
    which Snowflake emits for some VARCHAR columns, land as `Categorical`, so
    excluding them would silently skip every string transform on those columns.

    Args:
        dtype (pl.DataType): Column dtype to classify.

    Returns:
        bool: True for string, categorical, and enum columns.
    """
    return isinstance(dtype, (pl.String, pl.Utf8, pl.Categorical, pl.Enum))


def _sentinel_matches(value: SentinelValue, dtype: pl.DataType) -> bool:
    """Return whether one sentinel can be compared against a column dtype.

    Args:
        value (SentinelValue): Configured sentinel.
        dtype (pl.DataType): Column dtype to compare it against.

    Returns:
        bool: True when the comparison is well-defined.
    """
    # bool is checked first because isinstance(False, int) is True in Python,
    # and Polars refuses booleans against numeric columns in both directions.
    if isinstance(value, bool):
        return isinstance(dtype, pl.Boolean)
    if isinstance(value, (int, float)):
        # Covers Int, UInt, Float, and Decimal. Decimal matters because
        # Snowflake's default INTEGER is NUMBER(38,0), which exceeds int64 and
        # arrives as Arrow Decimal128 rather than an integer type.
        return dtype.is_numeric()
    return is_text_dtype(dtype)


def usable_sentinels(
    values: Sequence[SentinelValue] | None, dtype: pl.DataType
) -> list[SentinelValue]:
    """Keep only the sentinels a column's dtype can actually be compared against.

    Filtering is required rather than merely tidy: building a Polars expression
    from a mixed list raises before any dtype check runs, and a warehouse throws
    a cast error on `int_col IN ('N/A')`.

    Args:
        values (Sequence[SentinelValue] | None): Configured sentinels, if any.
        dtype (pl.DataType): Dtype of the column being normalized.

    Returns:
        list[SentinelValue]: Applicable sentinels, preserving configured order.
            Empty when the column type matches none of them.
    """
    if not values:
        return []
    return [value for value in values if _sentinel_matches(value, dtype)]
