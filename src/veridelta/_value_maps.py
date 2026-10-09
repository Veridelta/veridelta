# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Propose a `value_map` for each text column whose two sides spell the same values differently.

Behind `veridelta crosswalk`: a value is paired with the one it lines up with on
most rows, locally or as SQL inside the warehouse, and a pair is proposed only
when enough rows agree.
"""

from collections.abc import Sequence
from typing import Final

import polars as pl

from veridelta._pushdown import (
    duplicate_key_statements,
    pushdown_columns,
    pushdown_rule,
    reject_duplicate_pushdown_keys,
    resolve_pushdown_keys,
    validate_pushdown_schema,
)
from veridelta._resolution import EffectiveRule, is_real_number, match_rule
from veridelta.connectors.base import PushdownSession
from veridelta.connectors.sql import (
    VALUE_MAP_AGREEING_ALIAS,
    VALUE_MAP_COLUMN_ALIAS,
    VALUE_MAP_ROWS_ALIAS,
    VALUE_MAP_SOURCE_ALIAS,
    VALUE_MAP_TARGET_ALIAS,
)
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DiffConfig, ValueMapEntry, ValueMapProposal

DEFAULT_MIN_CONFIDENCE: Final = 0.95
"""Share of a source value's rows that must agree on one target value before it
is proposed. Above one half, at most one target can qualify, and 5% leaves room
for noise in legacy data."""


DEFAULT_MIN_SUPPORT: Final = 5
"""Agreeing rows a proposal needs, so a one-off coincidence is never offered."""


def check_value_map_thresholds(
    min_confidence: object, min_support: object, sample_fraction: object
) -> None:
    """Reject proposal thresholds that cannot produce a meaningful answer."""
    if not is_real_number(min_confidence):
        raise ConfigError(f"min_confidence must be a number, got {min_confidence!r}.")
    if not is_real_number(sample_fraction):
        raise ConfigError(f"sample_fraction must be a number, got {sample_fraction!r}.")
    if not is_real_number(min_support) or not isinstance(min_support, int):
        raise ConfigError(f"min_support must be a whole number, got {min_support!r}.")
    if not 0.5 < min_confidence <= 1:
        raise ConfigError(
            f"min_confidence must be above 0.5 and at most 1, got {min_confidence}. "
            "Above one half, at most one target value can qualify for a source value."
        )
    if min_support < 1:
        raise ConfigError(f"min_support must be at least 1, got {min_support}.")
    if not 0 < sample_fraction <= 1:
        raise ConfigError(f"sample_fraction must be above 0 and at most 1, got {sample_fraction}.")


def compares_mapped_text(effective: EffectiveRule, dtype: pl.DataType) -> bool:
    """Decide whether a `value_map` output reaches the comparison unchanged."""
    return (
        isinstance(dtype, pl.String)
        and effective["pad_zeros"] is None
        and not effective["datetime_format"]
        and effective["cast_to"] in (None, "String")
    )


def value_map_query(
    joined: pl.LazyFrame,
    column: str,
    *,
    mapped_values: Sequence[str],
    min_confidence: float,
    min_support: int,
) -> pl.LazyFrame:
    """Count how each source value lines up with the target, and keep strong pairs."""
    source = pl.col("source_value")
    target = pl.col("target_value")
    agreeing = pl.col("agreeing_rows")
    return (
        joined.select(
            pl.col(f"{column}_source").alias("source_value"),
            # The same soft cast the comparison applies to a non-text target.
            pl.col(f"{column}_target").cast(pl.String, strict=False).alias("target_value"),
        )
        .filter(source.is_not_null() & ~source.is_in(list(mapped_values)))
        .group_by("source_value", "target_value")
        .agg(pl.len().alias("agreeing_rows"))
        # Rows that already match count too, so a proposal that breaks one pays in confidence.
        .with_columns(agreeing.sum().over("source_value").alias("rows"))
        # A NULL target makes the inequality NULL, so it counts but is never proposed.
        .filter((target != source) & (agreeing >= min_support))
        .pipe(_confident_pairs, min_confidence)
    )


def _confident_pairs(pairs: pl.LazyFrame, min_confidence: float) -> pl.LazyFrame:
    """Keep the counted pairs that meet `min_confidence`, most agreeing rows first."""
    return pairs.filter(pl.col("agreeing_rows") / pl.col("rows") >= min_confidence).sort(
        ["agreeing_rows", "source_value"], descending=[True, False]
    )


def value_map_proposal(
    config: DiffConfig, column: str, frame: pl.DataFrame
) -> ValueMapProposal | None:
    """Turn one column's qualifying pairs into a proposal."""
    if frame.is_empty():
        return None
    entries = tuple(ValueMapEntry(**row) for row in frame.iter_rows(named=True))
    governing = match_rule(config.rules, column)
    existing = governing.value_map if governing is not None and governing.value_map else {}
    return ValueMapProposal(
        column=column,
        value_map={**existing, **{entry.source_value: entry.target_value for entry in entries}},
        entries=entries,
        governing_rule_index=None if governing is None else config.rules.index(governing),
    )


_VALUE_MAP_RESULT_COLUMNS: Final[dict[str, pl.DataType]] = {
    # A warehouse may deliver text as Categorical and counts as wide decimals.
    VALUE_MAP_COLUMN_ALIAS: pl.Int64(),
    VALUE_MAP_SOURCE_ALIAS: pl.String(),
    VALUE_MAP_TARGET_ALIAS: pl.String(),
    VALUE_MAP_ROWS_ALIAS: pl.Int64(),
    VALUE_MAP_AGREEING_ALIAS: pl.Int64(),
}
"""Columns `compile_value_map_query` returns, and the type each is read as."""


_VALUE_MAP_FIELDS: Final[dict[str, str]] = {
    VALUE_MAP_SOURCE_ALIAS: "source_value",
    VALUE_MAP_TARGET_ALIAS: "target_value",
    VALUE_MAP_ROWS_ALIAS: "rows",
    VALUE_MAP_AGREEING_ALIAS: "agreeing_rows",
}
"""Result columns renamed to the `ValueMapEntry` fields they fill."""


def _value_map_pairs(result: pl.DataFrame) -> pl.DataFrame:
    """Read the value map statement's result into typed, labeled pairs."""
    missing = [name for name in _VALUE_MAP_RESULT_COLUMNS if name not in result.columns]
    if missing:
        raise ConnectorError(f"Value map query result is missing the columns {missing}.")
    try:
        typed = result.select(
            pl.col(name).cast(dtype) for name, dtype in _VALUE_MAP_RESULT_COLUMNS.items()
        )
    except pl.exceptions.PolarsError as exc:
        raise ConnectorError(f"Value map query returned values of the wrong type: {exc}") from exc
    return typed.rename(_VALUE_MAP_FIELDS)


def collect_value_map_proposals(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
    *,
    min_confidence: float,
    min_support: int,
    sample_fraction: float,
) -> list[ValueMapProposal]:
    """Propose `value_map` entries from warehouse tables, in one counting statement."""
    source_schema, target_schema = validate_pushdown_schema(
        connector, source_table, target_table, diff
    )
    key_rules = resolve_pushdown_keys(diff, source_schema, target_schema)
    # A column qualifies when it is text on both sides and nothing after stage 4
    # changes what a map produces. Unlike a local run, a non-text target never
    # qualifies: each engine writes numbers and timestamps as text its own way.
    rules = [
        pushdown_rule(column, aligned, rule, effective)
        for column, aligned, rule, effective in pushdown_columns(diff, source_schema, target_schema)
        if compares_mapped_text(effective, source_schema[column])
        and isinstance(target_schema[aligned], pl.String)
    ]
    # As locally, repeated keys stop the run even when no column qualifies.
    tables = (source_table, target_table)
    reject_duplicate_pushdown_keys(
        connector,
        diff,
        tables,
        duplicate_key_statements(
            connector.compiler, diff, key_rules, tables, (source_schema, target_schema)
        ),
    )
    statement = connector.compiler.compile_value_map_query(
        source_table,
        target_table,
        diff.primary_keys,
        rules,
        min_support=min_support,
        sample_fraction=sample_fraction,
        source_types=source_schema,
        target_types=target_schema,
        key_rules=key_rules,
    )
    if statement is None:
        return []
    pairs = _value_map_pairs(
        connector.execute_pushdown(statement, query_type="value_maps").collect()
    )
    proposals: list[ValueMapProposal] = []
    for label, rule in enumerate(rules):
        column_pairs = pairs.filter(pl.col(VALUE_MAP_COLUMN_ALIAS) == label).drop(
            VALUE_MAP_COLUMN_ALIAS
        )
        frame = _confident_pairs(column_pairs.lazy(), min_confidence).collect()
        proposal = value_map_proposal(diff, rule.rename_to or rule.column_names[0], frame)
        if proposal is not None:
            proposals.append(proposal)
    return proposals
