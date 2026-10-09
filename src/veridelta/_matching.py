# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Build the expressions that decide whether two values match in a local run.

Numbers match within a tolerance, text within a similarity limit when the `fuzzy`
extra is installed, and nulls by `treat_null_as_equal`. Pushdown compiles the same
rules to SQL in `veridelta.connectors.sql`.
"""

from collections.abc import Callable
from functools import partial
from types import ModuleType

import polars as pl

from veridelta._resolution import EffectiveRule
from veridelta.connectors.base import optional_module
from veridelta.exceptions import ConfigError, missing_extra

rapidfuzz_distance = optional_module("rapidfuzz.distance")
"""Presence probe for the `fuzzy` extra, whose scorers evaluate
`max_levenshtein_distance` and `min_jaro_winkler_similarity` locally."""


def tolerance_match(
    src: pl.Expr,
    tgt: pl.Expr,
    rule: EffectiveRule,
    dtype: pl.DataType,
    tgt_dtype: pl.DataType | None,
) -> pl.Expr:
    """Match two numeric values within a rule's absolute and relative tolerance."""
    if dtype.is_integer() and tgt_dtype is not None and tgt_dtype.is_integer():
        src = src.cast(pl.Int128)
        tgt = tgt.cast(pl.Int128)
    # Subtract the smaller value from the larger: `tgt - src` on unsigned
    # columns wraps below zero instead of going negative.
    abs_diff = pl.when(tgt >= src).then(tgt - src).otherwise(src - tgt)
    threshold = rule["abs_tol"] + (rule["rel_tol"] * src.abs())
    within = abs_diff <= threshold
    if dtype.is_float():
        # `0 * inf` is NaN, and Polars sorts NaN above every number, so a
        # non-finite source must never reach the allowance.
        within = within & src.is_finite()
    # Equality matches outright, so NaN meets NaN and an infinity meets itself.
    return (src == tgt) | within


def pairable(source: pl.DataType, target: pl.DataType) -> bool:
    """Whether a join pairs rows on keys of these two types: one type, two integers, or two floats."""
    if source.is_integer() and target.is_integer():
        return True
    if source.is_float() and target.is_float():
        return True
    return source == target


def _fuzzy_measures() -> ModuleType:
    """Return rapidfuzz's distance module, or explain how to install it."""
    if rapidfuzz_distance is None:
        raise ConfigError(
            missing_extra(
                "fuzzy",
                "Comparing text locally under max_levenshtein_distance or "
                "min_jaro_winkler_similarity",
            )
        )
    return rapidfuzz_distance


def similarity_test(rule: EffectiveRule) -> Callable[[str, str], bool] | None:
    """Build the test a differing text pair must pass to match at stage 8."""
    limit = rule["max_levenshtein_distance"]
    if limit is not None:
        distance = _fuzzy_measures().Levenshtein.distance
        # Past `score_cutoff` rapidfuzz stops counting and reports limit + 1.
        return lambda left, right: bool(distance(left, right, score_cutoff=limit) <= limit)
    floor = rule["min_jaro_winkler_similarity"]
    if floor is not None:
        similarity = _fuzzy_measures().JaroWinkler.similarity
        # Below `score_cutoff` rapidfuzz reports a similarity of 0.
        return lambda left, right: bool(similarity(left, right, score_cutoff=floor) >= floor)
    return None


def null_equality(match: pl.Expr, src: pl.Expr, tgt: pl.Expr, rule: EffectiveRule) -> pl.Expr:
    """Apply stage 9: two nulls match under `treat_null`, and any other null does not."""
    if rule["treat_null"]:
        return (match | (src.is_null() & tgt.is_null())).fill_null(False)
    return match.fill_null(False)


def _score_differing_pairs(pairs: pl.Series, *, test: Callable[[str, str], bool]) -> pl.Series:
    """Mark the pairs that still differ after normalization but pass `test`."""
    differing = (
        pairs.struct.unnest()
        .with_row_index("row")
        .filter((pl.col("source") != pl.col("target")).fill_null(False))
    )
    hits = [row for row, source, target in differing.iter_rows() if test(source, target)]
    return pl.repeat(False, len(pairs), dtype=pl.Boolean, eager=True).scatter(hits, True)


def similarity_expr(src: pl.Expr, tgt: pl.Expr, test: Callable[[str, str], bool]) -> pl.Expr:
    """Evaluate a similarity test over two aligned text columns, lazily."""
    return pl.struct(src.alias("source"), tgt.alias("target")).map_batches(
        partial(_score_differing_pairs, test=test),
        return_dtype=pl.Boolean,
        is_elementwise=True,
    )
