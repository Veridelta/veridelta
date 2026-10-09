# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Resolve the rule that governs each column, and predict its type after its rules.

The engine, pushdown, `suggest`, and the config checks all read these helpers, so
this module sits below the other private modules and imports none of them.
"""

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Final, TypedDict, TypeGuard

import polars as pl

from veridelta.exceptions import ConfigError, DataIntegrityError
from veridelta.models import (
    CastTarget,
    DiffConfig,
    DiffRule,
    SentinelValue,
    WhitespaceMode,
    normalize_column_name,
)


class EffectiveRule(TypedDict):
    """Per-column settings after specific, pattern, and global rules are merged."""

    abs_tol: float
    rel_tol: float
    treat_null: bool
    whitespace: WhitespaceMode
    null_values: list[SentinelValue]
    null_values_explicit: bool
    case_insensitive: bool
    regex_replace: dict[str, str] | None
    value_map: dict[str, str] | None
    pad_zeros: int | None
    datetime_format: str | None
    timezone: str | None
    cast_to: CastTarget | None
    ignore: bool
    max_levenshtein_distance: int | None
    min_jaro_winkler_similarity: float | None


def unusable_sentinel_error(
    column: str, dtype: pl.DataType, sentinels: Sequence[SentinelValue]
) -> ConfigError:
    """Build the error for an explicit rule whose sentinels can never match."""
    return ConfigError(
        f"Column '{column}' has type {dtype}, which cannot hold any of the "
        f"null_values {list(sentinels)!r} configured for it. Quote text sentinels "
        "and leave numbers unquoted so each one matches its column type."
    )


def duplicate_keys_error(keys: list[str], side: str, count: int) -> DataIntegrityError:
    """Build the error for primary keys that repeat within one dataset."""
    return DataIntegrityError(
        f"Primary keys {keys} are not unique in {side} dataset. "
        f"Found {count} duplicate rows. Clean your data before diffing."
    )


def reject_unzoned_timezone(column: str, dtype: pl.DataType, zone: str) -> None:
    """Raise unless a column can take a `timezone` rule: a timestamp that carries its zone."""
    if not isinstance(dtype, pl.Datetime):
        raise ConfigError(
            f"Column '{column}' sets timezone='{zone}' but holds {dtype}, not a "
            "timestamp. Parse it with datetime_format first."
        )
    if dtype.time_zone is None:
        raise ConfigError(
            f"Column '{column}' sets timezone='{zone}' but its timestamps are "
            "timezone-naive. Veridelta will not assume an origin zone, because "
            "guessing wrong shifts every value silently. Store the column with a "
            "timezone, or parse it with a datetime_format carrying an offset such as "
            "'%z', or compare it without a timezone rule."
        )


def quoted_list(names: Sequence[str]) -> str:
    """Join names as `'a'`, `'a' and 'b'`, or `'a', 'b', and 'c'`."""
    quoted = [repr(name) for name in names]
    if len(quoted) < 3:
        return " and ".join(quoted)
    return f"{', '.join(quoted[:-1])}, and {quoted[-1]}"


def rename_pairs(rules: Sequence[DiffRule]) -> dict[str, str]:
    """Map each renamed source column to its target spelling."""
    pairs: dict[str, str] = {}
    for rule in rules:
        # An `ignore` rule counts too: its pair tells the target side which column to drop.
        if rule.rename_to and len(rule.column_names) == 1:
            pairs.setdefault(rule.column_names[0], rule.rename_to)
    return pairs


def _rule_spellings(pairs: Mapping[str, str], column: str) -> tuple[str, ...]:
    """List the names a rule may use for a column, in order of precedence."""
    source = next((src for src, tgt in pairs.items() if tgt == column), column)
    if source == column:
        return (column,)
    # In a swap or chain, a rule naming `column` governs the source column renamed away from it.
    if pairs.get(column, column) != column:
        return (source,)
    return (column, source)


def match_rule(rules: Sequence[DiffRule], column: str) -> DiffRule | None:
    """Resolve the single rule governing a column, exact names before patterns."""
    for name in _rule_spellings(rename_pairs(rules), column):
        for rule in rules:
            if name in rule.column_names:
                return rule
    for rule in rules:
        if rule.pattern and re.match(rule.pattern, column):
            return rule
    return None


def alignment_maps(
    rules: list[DiffRule], columns: Sequence[str], *, rename: bool
) -> tuple[dict[str, str], set[str]]:
    """Derive the `rename_to` map and `ignore` drop set for one frame's columns."""
    pairs = rename_pairs(rules)
    rename_map: dict[str, str] = {}
    to_drop: set[str] = set()
    for column in columns:
        aligned = pairs.get(column, column) if rename else column
        rule = match_rule(rules, aligned)
        if rule is not None and rule.ignore:
            to_drop.add(column)
        elif aligned != column:
            rename_map[column] = aligned
    return rename_map, to_drop


def normalize_header_names(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Strip and lowercase every column name, as `normalize_column_names` asks."""
    names = frame.collect_schema().names()
    normalized = [normalize_column_name(name) for name in names]
    collisions = sorted(name for name, count in Counter(normalized).items() if count > 1)
    if collisions:
        raise ConfigError(
            f"normalize_column_names maps more than one header onto {collisions}. "
            "Rename the duplicates at the source, or disable normalize_column_names."
        )
    return frame.rename(dict(zip(names, normalized, strict=True)))


def fold_rule_defaults(rule: DiffRule | None, diff: DiffConfig) -> EffectiveRule:
    """Layer one matched rule over the configuration's `default_*` settings."""
    effective: EffectiveRule = {
        "abs_tol": diff.default_absolute_tolerance,
        "rel_tol": diff.default_relative_tolerance,
        "treat_null": diff.default_treat_null_as_equal,
        "whitespace": diff.default_whitespace_mode,
        "null_values": diff.default_null_values,
        "null_values_explicit": False,
        "case_insensitive": False,
        "regex_replace": None,
        "value_map": None,
        "pad_zeros": None,
        "datetime_format": None,
        "timezone": None,
        "cast_to": None,
        "ignore": False,
        # No `default_*` counterpart: loosening every text column at once would
        # also forgive identifiers and codes that must match exactly.
        "max_levenshtein_distance": None,
        "min_jaro_winkler_similarity": None,
    }
    if rule is None:
        return effective

    if rule.absolute_tolerance is not None:
        effective["abs_tol"] = rule.absolute_tolerance
    if rule.relative_tolerance is not None:
        effective["rel_tol"] = rule.relative_tolerance
    if rule.treat_null_as_equal is not None:
        effective["treat_null"] = rule.treat_null_as_equal
    if rule.whitespace_mode is not None:
        effective["whitespace"] = rule.whitespace_mode
    if rule.null_values is not None:
        effective["null_values"] = rule.null_values
        effective["null_values_explicit"] = True
    if rule.case_insensitive is not None:
        effective["case_insensitive"] = rule.case_insensitive

    effective["regex_replace"] = rule.regex_replace
    effective["value_map"] = rule.value_map
    effective["pad_zeros"] = rule.pad_zeros
    effective["datetime_format"] = rule.datetime_format
    effective["timezone"] = rule.timezone
    effective["cast_to"] = rule.cast_to
    effective["ignore"] = rule.ignore
    effective["max_levenshtein_distance"] = rule.max_levenshtein_distance
    effective["min_jaro_winkler_similarity"] = rule.min_jaro_winkler_similarity
    return effective


CAST_TARGETS: Final[dict[CastTarget, pl.DataType]] = {
    "Int64": pl.Int64(),
    "Float64": pl.Float64(),
    "String": pl.String(),
    "Boolean": pl.Boolean(),
    "Date": pl.Date(),
    "Datetime": pl.Datetime(),
}
"""`cast_to` name to the dtype it resolves to."""


UNCASTABLE: Final[dict[type[pl.DataType], frozenset[CastTarget]]] = {
    pl.Binary: frozenset({"Int64", "Float64", "Boolean", "Date", "Datetime"}),
    pl.String: frozenset({"Boolean"}),
    pl.Categorical: frozenset({"Float64", "Boolean", "Date", "Datetime"}),
    pl.Decimal: frozenset({"Date", "Datetime"}),
    pl.Date: frozenset({"Boolean"}),
    pl.Datetime: frozenset({"Boolean"}),
    pl.Time: frozenset({"Boolean", "Date", "Datetime"}),
    pl.Duration: frozenset({"String", "Boolean", "Date", "Datetime"}),
    pl.List: frozenset(CAST_TARGETS),
}
"""`cast_to` targets Polars refuses for each column type, whatever its values.

Polars refuses them only once a value reaches the cast, which in a run is
mid-comparison. Checking the type first turns that into a `ConfigError` before
any row is read, in a run and in `validate --schemas` alike. A test holds the
table to what Polars does.
"""


_OFFSET_DIRECTIVE: Final = re.compile(r"%%|%[:#]*z")
"""A literal `%%`, or a `%z` offset directive in any of its chrono spellings."""


def normalized_dtype(effective: EffectiveRule, dtype: pl.DataType | None) -> pl.DataType | None:
    """Predict a column's dtype after stages 1 through 7, from its stored dtype."""
    if effective["cast_to"] is not None:
        return CAST_TARGETS[effective["cast_to"]]
    dtype = parsed_dtype(effective, dtype)
    if effective["timezone"] and isinstance(dtype, pl.Datetime):
        dtype = pl.Datetime(dtype.time_unit, effective["timezone"])
    return dtype


def parsed_dtype(effective: EffectiveRule, dtype: pl.DataType | None) -> pl.DataType | None:
    """Predict a column's dtype after stages 1 through 6a, before timezone and cast."""
    if effective["pad_zeros"] is not None:
        dtype = pl.String()
    fmt = effective["datetime_format"]
    if fmt and isinstance(dtype, pl.String):
        # A `%z` outside a `%%` escape reads a UTC offset, so the result is aware.
        aware = any(token != "%%" for token in _OFFSET_DIRECTIVE.findall(fmt))
        dtype = pl.Datetime("us", "UTC" if aware else None)
    return dtype


_FRACTION_SPELLINGS: Final[dict[str, str]] = {"%%": "%%", ".%f": "%.f", "%f": "%6f"}
"""Polars spellings of Python's fraction directive, plus the escape that hides one."""


_FRACTION_DIRECTIVE: Final = re.compile(r"%%|\.%f|%f")
"""A literal `%%`, which may precede an `f`, or `%f` with or without its dot."""


def polars_datetime_format(fmt: str) -> str:
    """Spell a Python `strptime` format the way Polars reads it."""
    # Polars' `%f` counts nanoseconds, so a Python fraction such as `.5` would read as 5 ns.
    return _FRACTION_DIRECTIVE.sub(lambda match: _FRACTION_SPELLINGS[match[0]], fmt)


def is_real_number(value: object) -> TypeGuard[int | float]:
    """Return whether a value is an `int` or `float`, and not a `bool`."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def drop_and_rename(
    rules: list[DiffRule], source: pl.LazyFrame, target: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Drop each side's ignored columns, and give the source's renamed ones their target names."""
    src_cols = source.collect_schema().names()
    tgt_cols = target.collect_schema().names()

    src_rename, src_drop = alignment_maps(rules, src_cols, rename=True)
    # The target already carries the post-rename spellings, which the
    # resolver matches under either name.
    _, tgt_drop = alignment_maps(rules, tgt_cols, rename=False)

    return source.drop(list(src_drop)).rename(src_rename), target.drop(list(tgt_drop))


def missing_keys(
    primary_keys: Sequence[str], sides: Mapping[str, str] | None, side: str, columns: list[str]
) -> str | None:
    """Say which primary keys a side lacks, and what it holds instead.

    Args:
        primary_keys (Sequence[str]): The configuration's primary keys.
        sides (Mapping[str, str] | None): How each side was read, when known.
        side (str): `source` or `target`.
        columns (list[str]): The side's columns after alignment.

    Returns:
        str | None: The message, or `None` when every key is present.
    """
    present = set(columns)
    missing = [key for key in primary_keys if key not in present]
    if not missing:
        return None
    where = f"the {side}"
    if sides is not None:
        where += f", {sides[side]}"
    keys = quoted_list(missing)
    noun, verb = ("key", "is") if len(missing) == 1 else ("keys", "are")
    if not columns:
        return f"The primary {noun} {keys} {verb} not among the columns of {where}. No column was read."
    shown = ", ".join(repr(column)[:60] for column in columns[:10])
    if len(columns) > 10:
        shown += f", and {len(columns) - 10} more"
    return (
        f"The primary {noun} {keys} {verb} not among the columns of {where}. "
        f"The columns read are: {shown}."
    )


def check_schema(
    config: DiffConfig,
    source: pl.LazyFrame,
    target: pl.LazyFrame,
    sides: Mapping[str, str] | None,
) -> None:
    """Enforce the primary keys and the configured `SchemaMode` on two aligned sides."""
    source_names = source.collect_schema().names()
    target_names = target.collect_schema().names()
    source_cols, target_cols = set(source_names), set(target_names)

    for side, names in (("source", source_names), ("target", target_names)):
        message = missing_keys(config.primary_keys, sides, side, names)
        if message is not None:
            raise ConfigError(message)

    # Sorted, since a set prints in an order that changes from run to run.
    only_source = sorted(source_cols - target_cols)
    only_target = sorted(target_cols - source_cols)
    mode = config.schema_mode
    if mode == "exact" and (only_source or only_target):
        parts: list[str] = []
        if only_source:
            parts.append(f"only the source has {quoted_list(only_source)}")
        if only_target:
            parts.append(f"only the target has {quoted_list(only_target)}")
        message = f"EXACT schema match failed: {'; '.join(parts)}."
    elif mode == "allow_additions" and only_source:
        message = f"Target is missing required source columns: {quoted_list(only_source)}."
    elif mode == "allow_removals" and only_target:
        message = f"Target contains unauthorized additional columns: {quoted_list(only_target)}."
    else:
        return
    if sides is not None:
        message += f" The source is {sides['source']}, and the target is {sides['target']}."
    raise ConfigError(message)


def validate_probe_schemas(config: DiffConfig, source: pl.LazyFrame, target: pl.LazyFrame) -> None:
    """Align two column probes and enforce `schema_mode`, as `DiffEngine.validate_schemas` does."""
    if config.normalize_column_names:
        source = normalize_header_names(source)
        target = normalize_header_names(target)
    source, target = drop_and_rename(config.rules, source, target)
    check_schema(config, source, target, None)
