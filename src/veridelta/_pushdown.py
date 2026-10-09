# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Plan a comparison that runs as SQL inside the warehouse that holds both sides.

It resolves each column's rule against the probed schemas, refuses what SQL
cannot compare as a local run does, and leaves every statement to the compiler
in `veridelta.connectors.sql`.
"""

from collections.abc import Iterator, Mapping, Sequence
from typing import NamedTuple

import polars as pl

from veridelta._resolution import (
    EffectiveRule,
    duplicate_keys_error,
    fold_rule_defaults,
    match_rule,
    normalized_dtype,
    parsed_dtype,
    reject_unzoned_timezone,
    rename_pairs,
    unusable_sentinel_error,
    validate_probe_schemas,
)
from veridelta._results import build_summary, export_artifacts
from veridelta.connectors import database as database_connectors
from veridelta.connectors.base import PushdownQueryType, PushdownSession
from veridelta.connectors.sql import SampleQuery, SQLPushdownCompiler
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DiffConfig, DiffResult, DiffRule, normalize_column_name
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


def _column_mismatches_from_frame(frame: pl.DataFrame) -> dict[str, int]:
    """Reduce the single-row mismatch tally to positive per-column counts."""
    if frame.height != 1:
        raise ConnectorError("Column mismatch query did not return exactly one row.")

    counts: dict[str, int] = {}
    for column, value in frame.row(0, named=True).items():
        # SUM over an empty join returns NULL rather than zero.
        if value is None:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError) as exc:
            raise ConnectorError(f"Column mismatch count for '{column}' was not numeric.") from exc
        if count > 0:
            counts[column] = count
    return counts


def _pushdown_scalar(
    connector: PushdownSession,
    statement: str,
    query_type: PushdownQueryType,
    label: str,
) -> int:
    """Collect the single integer an aggregate pushdown statement returns."""
    frame = connector.execute_pushdown(statement, query_type=query_type).collect()
    if frame.height != 1 or frame.width != 1:
        raise ConnectorError(f"{label} did not return a single value.")
    try:
        # Drivers may surface COUNT(*) or SUM as an integer, float, or decimal scalar.
        return int(frame.item())
    except (TypeError, ValueError) as exc:
        raise ConnectorError(f"{label} returned a non-numeric value.") from exc


def duplicate_key_statements(
    compiler: SQLPushdownCompiler,
    diff: DiffConfig,
    key_rules: Sequence[DiffRule],
    tables: tuple[str, str],
    schemas: tuple[pl.Schema, pl.Schema],
) -> tuple[str, str]:
    """Compile the count of repeated normalized keys for the source, then the target."""
    source_table, target_table = tables
    source_types, target_types = schemas
    return (
        compiler.compile_duplicate_key_query(
            source_table, diff.primary_keys, is_source=True, key_rules=key_rules, types=source_types
        ),
        compiler.compile_duplicate_key_query(
            target_table,
            diff.primary_keys,
            is_source=False,
            key_rules=key_rules,
            types=target_types,
        ),
    )


def reject_duplicate_pushdown_keys(
    connector: PushdownSession,
    diff: DiffConfig,
    tables: tuple[str, str],
    statements: tuple[str, str],
) -> None:
    """Fail a pair whose normalized primary keys repeat on either side, as a local run does."""
    for table, statement, side in zip(tables, statements, ("SOURCE", "TARGET"), strict=True):
        duplicates = _pushdown_scalar(
            connector, statement, "duplicates", f"Duplicate key query for '{table}'"
        )
        if duplicates:
            raise duplicate_keys_error(diff.primary_keys, side, duplicates)


def _reject_warehouse_header_normalization(diff: DiffConfig, *schemas: pl.Schema) -> None:
    """Refuse `normalize_column_names` where it would rename a warehouse column."""
    if not diff.normalize_column_names:
        return
    changed = [
        name for schema in schemas for name in schema.names() if name != normalize_column_name(name)
    ]
    if changed:
        raise ConfigError(
            f"normalize_column_names would rename the warehouse columns {changed}. "
            "Pushdown quotes identifiers exactly as they are stored, so disable "
            "normalize_column_names and write column names in their stored case."
        )


def probe_relation(connector: PushdownSession, table: str) -> tuple[pl.LazyFrame, pl.Schema]:
    """Read a relation's columns with a zero-row probe, as a warehouse run starts.

    Returns the probe, which `validate_probe_schemas` reads, and the columns with the
    types the warehouse declares.
    """
    probe = connector.execute_pushdown(
        connector.compiler.compile_schema_probe_query(table), query_type="schema"
    )
    schema = probe.collect_schema()
    if isinstance(connector, database_connectors.PostgresPushdownSession):
        # The probe reads every numeric as Decimal(38, 10); the catalog has the declared type.
        schema = _with_declared_types(schema, connector.declared_types(table))
    return probe, schema


def validate_pushdown_schema(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> tuple[pl.Schema, pl.Schema]:
    """Enforce `schema_mode` against warehouse relations before comparing them."""
    source_probe, source_schema = probe_relation(connector, source_table)
    target_probe, target_schema = probe_relation(connector, target_table)
    _reject_warehouse_header_normalization(diff, source_schema, target_schema)
    validate_probe_schemas(diff, source_probe, target_probe)
    return source_schema, target_schema


def _with_declared_types(schema: pl.Schema, declared: Mapping[str, pl.DataType]) -> pl.Schema:
    """Replace probed column types with the types the catalog declares."""
    return pl.Schema({name: declared.get(name, dtype) for name, dtype in schema.items()})


class _PushdownPlan(NamedTuple):
    """What a warehouse run learns before it reads a row."""

    source_schema: pl.Schema
    target_schema: pl.Schema
    key_rules: list[DiffRule]
    rules: list[DiffRule]


def plan_pushdown(
    connector: PushdownSession, source_table: str, target_table: str, diff: DiffConfig
) -> _PushdownPlan:
    """Probe both relations, then resolve the keys and rules against them."""
    source_schema, target_schema = validate_pushdown_schema(
        connector, source_table, target_table, diff
    )
    rules = resolve_pushdown_rules(diff, source_schema, target_schema)
    refuse_mixed_pushdown_types(diff, rules, source_schema, target_schema)
    return _PushdownPlan(
        source_schema,
        target_schema,
        resolve_pushdown_keys(diff, source_schema, target_schema),
        rules,
    )


class _PushdownStatements(NamedTuple):
    """Every statement a pushdown run executes, compiled once from its plan."""

    duplicates: tuple[str, str]
    """The count of repeated normalized keys in the source, then the target."""
    counts: tuple[str, str]
    """The row count of the source, then the target."""
    changed: str
    added: str
    missing: str
    columns: str | None
    """The per-column mismatch tally, or None when no column is compared."""
    sample: SampleQuery | None
    """The changed rows with values, or None without `pushdown_sample_rows` or a compared column."""


def compile_pushdown(
    compiler: SQLPushdownCompiler, tables: tuple[str, str], diff: DiffConfig, plan: _PushdownPlan
) -> _PushdownStatements:
    """Compile every statement a pushdown run executes, so a check compiles what a run runs.

    Every join reads normalized keys, so a key the rules transform matches across
    the two relations exactly where a local run would match it.
    """
    source_table, target_table = tables
    keys = diff.primary_keys
    source_types, target_types, key_rules, rules = plan
    wide_integers = wide_integer_columns(diff, rules, source_types, target_types)
    type_drift = type_drift_columns(diff, rules, source_types, target_types)
    sample = (
        compiler.compile_changed_sample_query(
            source_table,
            target_table,
            keys,
            rules,
            limit=diff.pushdown_sample_rows,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )
        if diff.pushdown_sample_rows
        else None
    )
    return _PushdownStatements(
        duplicates=duplicate_key_statements(
            compiler, diff, key_rules, tables, (source_types, target_types)
        ),
        counts=(
            compiler.compile_count_query(source_table),
            compiler.compile_count_query(target_table),
        ),
        changed=compiler.compile_query(
            source_table,
            target_table,
            keys,
            rules,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        ),
        added=compiler.compile_added_query(
            source_table,
            target_table,
            keys,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
        ),
        missing=compiler.compile_missing_query(
            source_table,
            target_table,
            keys,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
        ),
        columns=compiler.compile_column_mismatch_query(
            source_table,
            target_table,
            keys,
            rules,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
            wide_integers=wide_integers,
            type_drift=type_drift,
        ),
        sample=sample,
    )


def collect_pushdown_summary(
    connector: PushdownSession,
    source_table: str,
    target_table: str,
    diff: DiffConfig,
) -> DiffResult:
    """Compile and collect the warehouse key checks, counts, mismatches, and anti-joins."""
    tables = (source_table, target_table)
    plan = plan_pushdown(connector, source_table, target_table, diff)
    # As in a local run, a ConfigError from rule resolution or compiling wins
    # over repeated keys, and repeated keys stop the run before any count or
    # join executes.
    statements = compile_pushdown(connector.compiler, tables, diff, plan)
    reject_duplicate_pushdown_keys(connector, diff, tables, statements.duplicates)

    source_total, target_total = (
        _pushdown_scalar(connector, statement, "count", f"Row count query for '{table}'")
        for table, statement in zip(tables, statements.counts, strict=True)
    )
    changed = connector.execute_pushdown(statements.changed, query_type="mismatch").collect()
    added = connector.execute_pushdown(statements.added, query_type="added").collect()
    removed = connector.execute_pushdown(statements.missing, query_type="missing").collect()

    column_mismatches: dict[str, int] = {}
    if statements.columns is not None:
        tally = connector.execute_pushdown(statements.columns, query_type="columns").collect()
        column_mismatches = _column_mismatches_from_frame(tally)
    changed_sample = (
        None
        if statements.sample is None or changed.is_empty()
        else _collect_changed_sample(connector, statements.sample)
    )

    # Pushdown projects primary keys only, never full rows, so the suffix keeps
    # these files from being mistaken for local artifacts.
    frames = {
        "added_rows_pks_only": added,
        "removed_rows_pks_only": removed,
        "changed_rows_pks_only": changed,
    }
    if changed_sample is not None:
        # A sample holds values, so its own name sets it apart from the keys.
        frames["changed_rows_sample"] = changed_sample
    artifacts_written = export_artifacts(frames, diff.output_path, diff.output_format)

    return DiffResult(
        summary=build_summary(
            diff,
            changed,
            added,
            removed,
            source_total,
            target_total,
            column_mismatches,
            artifacts_written,
        ),
        added=added,
        removed=removed,
        changed=changed,
        primary_keys=tuple(diff.primary_keys),
        # Post-rename names, matching what the local path records, so the same
        # column reads the same way whichever engine ran it.
        compared_columns=tuple(rule.rename_to or rule.column_names[0] for rule in plan.rules),
        keys_only=True,
        changed_sample=changed_sample,
    )


def _collect_changed_sample(connector: PushdownSession, sample: SampleQuery) -> pl.DataFrame:
    """Fetch up to `pushdown_sample_rows` changed rows with both sides' values."""
    frame = connector.execute_pushdown(sample.statement, query_type="samples").collect()
    missing = [alias for alias in sample.renames if alias not in frame.columns]
    if missing:
        raise ConnectorError(f"The warehouse returned a row sample without {', '.join(missing)}.")
    return frame.select(list(sample.renames)).rename(sample.renames)
