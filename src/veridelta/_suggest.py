# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Propose a rule for each column that differs, from the rows that differ, for `suggest`.

Each proposal is one setting a person could write by hand, with the rows it would
explain. `DiffEngine.suggest_rules` runs the comparison and tries each proposal.
"""

import math
from typing import Final, NamedTuple, cast

import polars as pl

from veridelta._resolution import is_real_number, polars_datetime_format
from veridelta.exceptions import ConfigError
from veridelta.models import DiffRule, SentinelValue, SuggestedSetting

DEFAULT_MAX_SHARE: Final = 0.01
"""Largest gap a suggested tolerance may explain, as a share of the larger of its
two values. A gap past it is a change, not noise, so the column gets no suggestion."""


SUGGESTED_EXAMPLES: Final = 3
"""Primary keys a suggestion names as examples."""


def check_max_share(max_share: object) -> None:
    """Reject a share that cannot bound a tolerance."""
    if not is_real_number(max_share):
        raise ConfigError(f"max_share must be a number, got {max_share!r}.")
    if not 0 < max_share <= 1:
        raise ConfigError(f"max_share must be above 0 and at most 1, got {max_share}.")


def _round_up(value: float) -> float:
    """Return the smallest of 1, 2, or 5 times a power of ten that is above `value`."""
    power = 10.0 ** math.floor(math.log10(value))
    # A step of 10 is the next power, which is always above `value`.
    step = next(step for step in (1, 2, 5, 10) if step * power > value)
    # Read back through text, so 5 * 0.001 is 0.005 and not 0.005000000000000001.
    return float(f"{step * power:.12g}")


def _spread(values: pl.Series) -> float:
    """Return how much the values vary, as their standard deviation over their mean."""
    mean = cast("float", values.mean())
    deviation = values.std()
    return 0.0 if deviation is None or mean == 0 else cast("float", deviation) / mean


class _Proposal(NamedTuple):
    """Settings a column's differing rows call for, before a run confirms them."""

    settings: dict[str, SuggestedSetting]
    largest_gap: float | None = None


def _compares_numbers(changed: pl.DataFrame, column: str) -> bool:
    """Return whether a column's compared values are numbers on both sides."""
    schema = changed.schema
    return all(schema[f"{column}_{side}"].is_numeric() for side in ("source", "target"))


def _tolerance_gaps(changed: pl.DataFrame, column: str) -> pl.DataFrame:
    """Measure the gaps between a numeric column's differing values that a tolerance closes."""
    names = (f"{column}_source", f"{column}_target")
    # Integers subtract exactly, as the comparison does: through Float64, two Int64
    # values above 2**53, such as nanosecond times, can round to one number.
    exact = pl.Int128 if all(changed.schema[name].is_integer() for name in names) else pl.Float64
    source, target = (pl.col(name).cast(exact) for name in names)
    gap = (target - source).abs().cast(pl.Float64)
    return (
        # A NULL flag is not a mismatch, as the column counts treat it.
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            gap.alias("gap"),
            (gap / pl.max_horizontal(source.abs(), target.abs()).cast(pl.Float64)).alias("share"),
            (gap / source.abs().cast(pl.Float64)).alias("relative"),
        )
        # A gap of 0 is a type `strict_types` refuses, and a gap to an infinity, NaN,
        # or NULL is not finite: no tolerance closes either.
        .filter(pl.col("gap").is_finite() & (pl.col("gap") > 0))
    )


def _tolerance_proposal(changed: pl.DataFrame, column: str, max_share: float) -> _Proposal | None:
    """Propose a tolerance when every gap in a numeric column is at most `max_share`.

    Rounding leaves gaps of about one size whatever the values, which an absolute
    tolerance describes. A rate change leaves gaps that grow with the values, which a
    relative tolerance describes. The kind whose measure varies less wins, and a
    source value of 0, which no relative tolerance reaches, picks absolute. The value
    is the round number just above the largest gap.
    """
    gaps = _tolerance_gaps(changed, column)
    if gaps.is_empty() or cast("float", gaps["share"].max()) > max_share:
        return None
    largest = cast("float", gaps["gap"].max())
    relative = gaps["relative"]
    if not relative.is_finite().all() or _spread(gaps["gap"]) <= _spread(relative):
        return _Proposal({"absolute_tolerance": _round_up(largest)}, largest)
    return _Proposal({"relative_tolerance": _round_up(cast("float", relative.max()))}, largest)


def _compares_text(changed: pl.DataFrame, column: str) -> bool:
    """Return whether a column's compared values are text on both sides."""
    schema = changed.schema
    return all(schema[f"{column}_{side}"] == pl.String for side in ("source", "target"))


def _text_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose trimming, case folding, or both, when differing text matches without them.

    A setting is proposed only when the rows need it: trimming alone when it matches
    every row that trimming and case folding together match, then case folding alone,
    and both otherwise. Trimming strips both ends, before case folding, as the
    comparison does.
    """
    source = pl.col(f"{column}_source")
    target = pl.col(f"{column}_target")
    # A pair with a NULL compares to NULL, which `sum` skips: no setting matches it.
    counts = (
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            trimmed=(source.str.strip_chars() == target.str.strip_chars()).sum(),
            folded=(source.str.to_lowercase() == target.str.to_lowercase()).sum(),
            both=(
                source.str.strip_chars().str.to_lowercase()
                == target.str.strip_chars().str.to_lowercase()
            ).sum(),
        )
        .row(0, named=True)
    )
    if counts["both"] == 0:
        return None
    if counts["trimmed"] == counts["both"]:
        return _Proposal({"whitespace_mode": "both"})
    if counts["folded"] == counts["both"]:
        return _Proposal({"case_insensitive": True})
    return _Proposal({"whitespace_mode": "both", "case_insensitive": True})


_NULL_SPELLINGS: Final = frozenset(
    {"", "-", "--", "?", "n/a", "na", "#n/a", "null", "(null)", "<null>", "none", "nil", "nan"}
)
"""Text a system writes in place of NULL, compared stripped and in lowercase."""


_NULL_NUMBERS: Final = frozenset({-1, -9, -99, -999, -9999, -99999})
"""Numbers a system writes in place of NULL."""


def _looks_like_null(value: object) -> bool:
    """Return whether a value is a common spelling of NULL, such as `N/A` or -999."""
    if isinstance(value, str):
        return value.strip().lower() in _NULL_SPELLINGS
    # A flag is never proposed: `false` where the other side is NULL is too often meant.
    return isinstance(value, int | float) and not isinstance(value, bool) and value in _NULL_NUMBERS


def _sentinel_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose `null_values` for spellings of NULL that stand where the other side is NULL."""
    differing = pl.col(f"{column}_is_match").eq(False)
    found: set[SentinelValue] = set()
    for side, other in (("source", "target"), ("target", "source")):
        values = changed.filter(
            differing
            & pl.col(f"{column}_{other}").is_null()
            & pl.col(f"{column}_{side}").is_not_null()
        )[f"{column}_{side}"]
        found.update(value for value in values.unique().to_list() if _looks_like_null(value))
    if not found:
        return None
    return _Proposal({"null_values": sorted(found, key=repr)})


_DATE_FORMATS: Final = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
)
"""Formats tried on dates written as text, day first before month first on a tie."""


def _format_proposal(changed: pl.DataFrame, column: str) -> _Proposal | None:
    """Propose a `datetime_format` when text on one side reads as the dates on the other.

    Each format in `_DATE_FORMATS` parses the text side as the comparison would, and
    the one that matches the most differing rows wins. A timestamp with a zone is
    left out, since text without an offset names no instant to match it.
    """
    dtypes = {side: changed.schema[f"{column}_{side}"] for side in ("source", "target")}
    texts = [side for side, dtype in dtypes.items() if dtype == pl.String]
    dates = [
        side
        for side, dtype in dtypes.items()
        if isinstance(dtype, pl.Date) or (isinstance(dtype, pl.Datetime) and not dtype.time_zone)
    ]
    if len(texts) != 1 or len(dates) != 1:
        return None
    text = pl.col(f"{column}_{texts[0]}")
    other = pl.col(f"{column}_{dates[0]}").cast(pl.Datetime)
    counts = (
        changed.filter(pl.col(f"{column}_is_match").eq(False))
        .select(
            (
                text.str.strptime(pl.Datetime, format=polars_datetime_format(fmt), strict=False)
                == other
            )
            .sum()
            .alias(fmt)
            for fmt in _DATE_FORMATS
        )
        .row(0)
    )
    best = max(range(len(_DATE_FORMATS)), key=counts.__getitem__)
    if counts[best] == 0:
        return None
    return _Proposal({"datetime_format": _DATE_FORMATS[best]})


def column_proposal(changed: pl.DataFrame, column: str, max_share: float) -> _Proposal | None:
    """Propose what a column's differing rows call for, its null sentinels included.

    Numbers can take a tolerance, text can take trimming and case folding, and text
    beside dates can take a date format. Any of them can take null sentinels beside,
    in one rule.
    """
    if _compares_numbers(changed, column):
        typed = _tolerance_proposal(changed, column, max_share)
    elif _compares_text(changed, column):
        typed = _text_proposal(changed, column)
    else:
        typed = _format_proposal(changed, column)
    sentinels = _sentinel_proposal(changed, column)
    if typed is None or sentinels is None:
        return typed or sentinels
    return _Proposal({**typed.settings, **sentinels.settings}, typed.largest_gap)


def suggested_rule(
    governing: DiffRule | None,
    column: str,
    settings: dict[str, SuggestedSetting],
    default_null_values: list[SentinelValue],
) -> DiffRule:
    """Name the column alone, keep the settings that govern it today, and add `settings`.

    Suggested null sentinels join the column's sentinels today, since a rule's
    `null_values` replaces `default_null_values` rather than adding to them.
    """
    kept = (
        {}
        if governing is None
        else governing.model_dump(
            exclude_unset=True, exclude={"column_names", "pattern", "rename_to"}
        )
    )
    added = dict(settings)
    sentinels = settings.get("null_values")
    if isinstance(sentinels, list):
        current = kept.get("null_values", default_null_values)
        added["null_values"] = [*current, *(value for value in sentinels if value not in current)]
    return DiffRule.model_validate({**kept, "column_names": [column], **added})


def differing_only_in(
    first: pl.DataFrame, second: pl.DataFrame, keys: list[str], column: str
) -> pl.DataFrame:
    """Return the keys of the rows whose column differs in `first` and not in `second`."""
    differing = pl.col(f"{column}_is_match").eq(False)
    return (
        first.filter(differing)
        .select(keys)
        .join(second.filter(differing).select(keys), on=keys, how="anti", nulls_equal=True)
        .sort(keys)
    )
