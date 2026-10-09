# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Plan a comparison that runs as SQL inside the warehouse that holds both sides.

It resolves each column's rule against the probed schemas, refuses what SQL
cannot compare as a local run does, and leaves every statement to the compiler
in `veridelta.connectors.sql`.
"""

from collections.abc import Iterator, Sequence

import polars as pl

from veridelta._resolution import (
    EffectiveRule,
    fold_rule_defaults,
    match_rule,
    normalized_dtype,
    parsed_dtype,
    reject_unzoned_timezone,
    rename_pairs,
    unusable_sentinel_error,
)
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffRule
from veridelta.sentinels import usable_sentinels


def _enforce_pushdown_preconditions(
    effective: EffectiveRule, sides: tuple[tuple[str, pl.Schema], ...]
) -> None:
    """Fail a pushdown rule that the probed column types can never satisfy."""
    if effective["null_values_explicit"] and effective["null_values"]:
        for name, schema in sides:
            dtype = schema.get(name)
            if dtype is not None and not usable_sentinels(effective["null_values"], dtype):
                raise unusable_sentinel_error(name, dtype, effective["null_values"])
    if effective["timezone"]:
        for name, schema in sides:
            # The zone rule reads the column after padding and parsing, as locally.
            dtype = parsed_dtype(effective, schema.get(name))
            # Stage 6b emits no SQL: a zone label cannot change a verdict. Its precondition
            # still holds, so pushdown never compares columns a local run refuses.
            if dtype is not None:
                reject_unzoned_timezone(name, dtype, effective["timezone"])


def pushdown_rule(
    stored: str,
    aligned: str,
    rule: DiffRule | None,
    effective: EffectiveRule,
    *,
    absolute_tolerance: float | None = None,
    relative_tolerance: float | None = None,
    treat_null_as_equal: bool | None = None,
    max_levenshtein_distance: int | None = None,
) -> DiffRule:
    """Re-materialize a folded rule as the fully specified `DiffRule` the compiler reads."""
    return DiffRule(
        column_names=[stored],
        rename_to=None if aligned == stored else aligned,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
        treat_null_as_equal=treat_null_as_equal,
        whitespace_mode=effective["whitespace"],
        null_values=effective["null_values"],
        # No `default_*` counterpart exists, so the compiler sees it as written.
        case_insensitive=rule.case_insensitive if rule is not None else None,
        regex_replace=effective["regex_replace"],
        value_map=effective["value_map"],
        pad_zeros=effective["pad_zeros"],
        datetime_format=effective["datetime_format"],
        timezone=effective["timezone"],
        cast_to=effective["cast_to"],
        # No Jaro-Winkler counterpart: the resolver refuses that limit, since
        # no warehouse can reproduce it.
        max_levenshtein_distance=max_levenshtein_distance,
    )


def resolve_pushdown_keys(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> list[DiffRule]:
    """Expand configuration into the normalization each primary key receives."""
    source_names = set(source_schema.names())
    pairs = rename_pairs(diff.rules)

    resolved: list[DiffRule] = []
    for key in diff.primary_keys:
        # Schema validation has already proven the key survives alignment, so
        # either a present column is renamed onto it or it is stored as is.
        stored = next(
            (src for src, tgt in pairs.items() if tgt == key and src in source_names), key
        )
        rule = match_rule(diff.rules, key)
        effective = fold_rule_defaults(rule, diff)
        _enforce_pushdown_preconditions(effective, ((stored, source_schema), (key, target_schema)))
        resolved.append(pushdown_rule(stored, key, rule, effective))
    return resolved


def pushdown_columns(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> Iterator[tuple[str, str, DiffRule | None, EffectiveRule]]:
    """Walk the probed source columns a warehouse statement compares."""
    target_lookup = set(target_schema.names())
    keys = set(diff.primary_keys)
    pairs = rename_pairs(diff.rules)
    for column in source_schema.names():
        aligned = pairs.get(column, column)
        if aligned in keys or aligned not in target_lookup:
            continue
        rule = match_rule(diff.rules, aligned)
        if rule is not None and rule.ignore:
            continue
        effective = fold_rule_defaults(rule, diff)
        _enforce_pushdown_preconditions(
            effective, ((column, source_schema), (aligned, target_schema))
        )
        yield column, aligned, rule, effective


def resolve_pushdown_rules(
    diff: DiffConfig, source_schema: pl.Schema, target_schema: pl.Schema
) -> list[DiffRule]:
    """Expand configuration into one fully specified rule per compared column."""
    resolved: list[DiffRule] = []
    for column, aligned, rule, effective in pushdown_columns(diff, source_schema, target_schema):
        # As in `_build_match_expr`, a tolerance loosens only a column compared as
        # a number, and a similarity limit only one compared as text. An unknown
        # type keeps both, unless `datetime_format` would parse the text.
        normalized = normalized_dtype(effective, source_schema.get(column))
        numeric = normalized is None or normalized.is_numeric()
        if normalized is None:
            text = not effective["datetime_format"]
        else:
            text = isinstance(normalized, pl.String)
        if text and effective["min_jaro_winkler_similarity"] is not None:
            raise ConfigError(
                f"Column '{aligned}' sets min_jaro_winkler_similarity, which warehouse "
                "pushdown cannot evaluate the way a local run does: Snowflake's "
                "JAROWINKLER_SIMILARITY ignores case and returns a whole number from 0 to "
                "100, and Databricks has no Jaro-Winkler function. Use "
                "max_levenshtein_distance, which compiles to SQL, or compare file or "
                "lakehouse copies of these tables locally."
            )

        resolved.append(
            pushdown_rule(
                column,
                aligned,
                rule,
                effective,
                absolute_tolerance=effective["abs_tol"] if numeric else 0.0,
                relative_tolerance=effective["rel_tol"] if numeric else 0.0,
                treat_null_as_equal=effective["treat_null"],
                max_levenshtein_distance=effective["max_levenshtein_distance"] if text else None,
            )
        )
    return resolved


def _compared_dtypes(
    diff: DiffConfig, rule: DiffRule, source_schema: pl.Schema, target_schema: pl.Schema
) -> tuple[pl.DataType | None, pl.DataType | None]:
    """Predict the dtypes a local run would compare one pushdown column as."""
    effective = fold_rule_defaults(rule, diff)
    stored = rule.column_names[0]
    return (
        normalized_dtype(effective, source_schema.get(stored)),
        normalized_dtype(effective, target_schema.get(rule.rename_to or stored)),
    )


def wide_integer_columns(
    diff: DiffConfig, rules: Sequence[DiffRule], source_schema: pl.Schema, target_schema: pl.Schema
) -> frozenset[str]:
    """Name the tolerance columns a warehouse must subtract in a wider integer type."""
    return frozenset(
        rule.rename_to or rule.column_names[0]
        for rule in rules
        if (rule.absolute_tolerance or rule.relative_tolerance)
        and all(
            dtype is not None and dtype.is_integer()
            for dtype in _compared_dtypes(diff, rule, source_schema, target_schema)
        )
    )


def type_drift_columns(
    diff: DiffConfig, rules: Sequence[DiffRule], source_schema: pl.Schema, target_schema: pl.Schema
) -> frozenset[str]:
    """Name the columns `strict_types` fails because their two sides differ in type."""
    if not diff.strict_types:
        return frozenset()
    drift: set[str] = set()
    for rule in rules:
        source, target = _compared_dtypes(diff, rule, source_schema, target_schema)
        if source is not None and target is not None and source != target:
            drift.add(rule.rename_to or rule.column_names[0])
    return frozenset(drift)


def _comparable_in_sql(source: pl.DataType, target: pl.DataType) -> bool:
    """Whether a database compares the pair as a local run does: one type, or two numbers."""
    if source.is_numeric() and target.is_numeric():
        return True
    return source.base_type() == target.base_type()


def refuse_mixed_pushdown_types(
    diff: DiffConfig, rules: Sequence[DiffRule], source_schema: pl.Schema, target_schema: pl.Schema
) -> None:
    """Refuse a compared column whose two sides hold types a database would convert itself.

    A local run casts the target to the source type, and a database converts
    one side by its own rules, so text against a number, a date against a
    timestamp, or text against a timestamp can reach different verdicts.
    Two numeric types compare by value in both, and `strict_types` fails a
    pair of types in both.

    Raises:
        ConfigError: If a compared column holds two such types.
    """
    if diff.strict_types:
        return
    mixed: list[str] = []
    for rule in rules:
        source, target = _compared_dtypes(diff, rule, source_schema, target_schema)
        if source is not None and target is not None and not _comparable_in_sql(source, target):
            mixed.append(
                f"'{rule.rename_to or rule.column_names[0]}' ({source} in the source, "
                f"{target} in the target)"
            )
    if mixed:
        raise ConfigError(
            f"Pushdown compares two types only when both are numeric, and these columns "
            f"hold two other types: {', '.join(mixed)}. A database would convert one side "
            "by its own rules, where a local run casts the target to the source type. "
            "Give each a rule with cast_to, or set strict_types: true."
        )
