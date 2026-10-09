# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Load, align, and compare datasets.

Holds the `DiffEngine` that loads, aligns, and compares the two sides with
Polars. Its helpers live in private modules beside it, and `__all__` lists the
public names, the file loaders from `_reading.py` among them.
"""

from collections.abc import Callable, Iterable, Sequence
from importlib.util import find_spec
from pathlib import Path
from typing import (
    Final,
)
from urllib.parse import urlsplit

import polars as pl

from veridelta import _matching, _reading
from veridelta._matching import (
    null_equality,
    pairable,
    similarity_expr,
    similarity_test,
    tolerance_match,
)
from veridelta._pushdown import (
    collect_pushdown_summary,
    compile_pushdown,
    duplicate_key_statements,
    plan_pushdown,
    probe_relation,
    pushdown_columns,
    pushdown_rule,
    reject_duplicate_pushdown_keys,
    resolve_pushdown_keys,
    validate_pushdown_schema,
)
from veridelta._reading import (
    ArrowLoader,
    AvroLoader,
    BaseLoader,
    CSVLoader,
    ExcelLoader,
    JSONLoader,
    LoaderFactory,
    NDJSONLoader,
    ParquetLoader,
    describe_sides,
    schema_frame,
)
from veridelta._resolution import (
    CAST_TARGETS,
    UNCASTABLE,
    EffectiveRule,
    check_schema,
    drop_and_rename,
    duplicate_keys_error,
    fold_rule_defaults,
    is_real_number,
    match_rule,
    normalize_header_names,
    polars_datetime_format,
    quoted_list,
    reject_unzoned_timezone,
    unusable_sentinel_error,
)
from veridelta._results import (
    accept_baseline,
    build_summary,
    export_artifacts,
    local_column_mismatches,
)
from veridelta._suggest import (
    DEFAULT_MAX_SHARE,
    SUGGESTED_EXAMPLES,
    check_max_share,
    column_proposal,
    differing_only_in,
    suggested_rule,
)
from veridelta._warehouses import (
    WarehousePair,
    check_backend_pairing,
    is_warehouse,
    table_name,
    warehouse_session,
)
from veridelta.config import load_config
from veridelta.connectors import database as database_connectors
from veridelta.connectors import duckdb as duckdb_connectors
from veridelta.connectors import warehouse as warehouse_connectors
from veridelta.connectors.base import (
    PushdownSession,
)
from veridelta.connectors.sql import (
    SAMPLE_BUCKETS,
    VALUE_MAP_AGREEING_ALIAS,
    VALUE_MAP_COLUMN_ALIAS,
    VALUE_MAP_ROWS_ALIAS,
    VALUE_MAP_SOURCE_ALIAS,
    VALUE_MAP_TARGET_ALIAS,
    SQLPushdownCompiler,
    compile_database_select,
)
from veridelta.exceptions import (
    ConfigError,
    ConnectorError,
    VerideltaError,
    missing_extra,
)
from veridelta.models import (
    Baseline,
    BigQueryConfig,
    ConfigFinding,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffResult,
    DiffRule,
    DuckDBConfig,
    IcebergConfig,
    RuleSuggestion,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    ValueMapEntry,
    ValueMapProposal,
    normalize_column_name,
)
from veridelta.sentinels import usable_sentinels

__all__ = [
    "DEFAULT_MAX_SHARE",
    "DEFAULT_MIN_CONFIDENCE",
    "DEFAULT_MIN_SUPPORT",
    "ArrowLoader",
    "AvroLoader",
    "BaseLoader",
    "CSVLoader",
    "DiffEngine",
    "EffectiveRule",
    "ExcelLoader",
    "JSONLoader",
    "LoaderFactory",
    "NDJSONLoader",
    "ParquetLoader",
]


def _check_pushdown_plan(
    connector: PushdownSession, source_table: str, target_table: str, diff: DiffConfig
) -> list[ConfigFinding]:
    """Do what a warehouse run does before reading a row, and compile the rest.

    Returns the warning for a rule's column name that neither table holds, if any.
    """
    plan = plan_pushdown(connector, source_table, target_table, diff)
    compile_pushdown(connector.compiler, (source_table, target_table), diff, plan)
    return _unknown_column_findings(diff, plan.source_schema, plan.target_schema)


_EXTRA_PROBES: Final[dict[type[object], tuple[str, Callable[[], bool]]]] = {
    # Each probe reads its module attribute when called, so tests can patch it,
    # and a lakehouse reader is looked up by name rather than imported.
    DeltaLakeConfig: ("delta", lambda: _findable("deltalake")),
    IcebergConfig: ("iceberg", lambda: _findable("pyiceberg")),
    DatabaseConfig: ("database", lambda: database_connectors.connectorx is not None),
    DuckDBConfig: ("duckdb", lambda: duckdb_connectors.duckdb is not None),
    SnowflakeConfig: ("snowflake", lambda: warehouse_connectors.snowflake_connector is not None),
    DatabricksConfig: ("databricks", lambda: warehouse_connectors.databricks_sql is not None),
    # The BigQuery client is imported only on connect, so it is found by name.
    BigQueryConfig: ("bigquery", lambda: _findable("google.cloud.bigquery")),
}
"""The optional extra each connection type reads through, and whether it is installed."""


def _findable(module: str) -> bool:
    """Return whether a dotted module could be imported, without importing it."""
    # `find_spec` imports a dotted name's parents, and raises when one is not installed.
    try:
        return find_spec(module) is not None
    except ModuleNotFoundError:
        return False


def _required_extra(config: SourceRef) -> tuple[str, Callable[[], bool]] | None:
    """Name the optional extra one side reads through, with its probe."""
    if isinstance(config, SourceConfig):
        if config.format == "excel":
            return "excel", lambda: _reading.fastexcel is not None
        return None
    return _EXTRA_PROBES[type(config)]


def _error(message: str) -> ConfigFinding:
    """Build a finding that would stop a run."""
    return ConfigFinding(severity="error", message=message)


def _warning(message: str) -> ConfigFinding:
    """Build a finding that stops a run only for some stored names or types."""
    return ConfigFinding(severity="warning", message=message)


def _missing_extra_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report each optional extra a side reads through that is not installed."""
    sides: dict[str, list[str]] = {}
    for label, config in (("source", source), ("target", target)):
        required = _required_extra(config)
        if required is not None and not required[1]():
            sides.setdefault(required[0], []).append(label)
    return [
        _error(missing_extra(extra, f"Reading the {' and '.join(labels)}"))
        for extra, labels in sides.items()
    ]


def _database_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report a database `table` whose URI scheme Veridelta cannot quote for."""
    findings: list[ConfigFinding] = []
    for label, config in (("source", source), ("target", target)):
        if isinstance(config, DatabaseConfig) and config.table is not None:
            try:
                compile_database_select(urlsplit(config.uri).scheme.lower(), config.table)
            except (ConfigError, ConnectorError) as exc:
                findings.append(_error(f"{label}: {exc}"))
    return findings


def _polars_regex_error(pattern: str, replacement: str) -> str | None:
    """Return why Polars' regular expression engine rejects a pattern, if it does."""
    try:
        pl.select(pl.lit("").str.replace_all(pattern, replacement))
    except pl.exceptions.PolarsError as exc:
        detail = str(exc)
        return next(
            (
                line.removeprefix("error: ")
                for line in detail.splitlines()
                if line.startswith("error: ")
            ),
            detail.strip(),
        )
    return None


def _regex_findings(diff: DiffConfig, *, pushdown: bool) -> list[ConfigFinding]:
    """Report `regex_replace` patterns that Polars' regular expression engine rejects."""
    # The models compile each pattern with Python's `re`, which accepts look-around
    # and backreferences that Polars rejects.
    findings: list[ConfigFinding] = []
    for index, rule in enumerate(diff.rules):
        for pattern, replacement in (rule.regex_replace or {}).items():
            reason = _polars_regex_error(pattern, replacement)
            if reason is None:
                continue
            message = (
                f"rules[{index}] regex_replace pattern {pattern!r} does not compile in "
                f"Polars ({reason})."
            )
            if pushdown:
                findings.append(
                    _warning(
                        f"{message} A warehouse run hands it to the warehouse's own regular "
                        "expression engine, which may accept it, but a local run would fail."
                    )
                )
            else:
                findings.append(_error(message))
    return findings


def _unknown_column_findings(
    diff: DiffConfig, source_names: Iterable[str], target_names: Iterable[str]
) -> list[ConfigFinding]:
    """Warn about a rule's column name that neither side holds.

    Such a rule does nothing. That is safe, since it only fails to forgive, but a
    misspelled name would otherwise go unnoticed.
    """
    stored = {*source_names, *target_names}
    if diff.normalize_column_names:
        stored = {normalize_column_name(name) for name in stored}
    unknown = sorted({name for rule in diff.rules for name in rule.column_names} - stored)
    if not unknown:
        return []
    pronoun = "it" if len(unknown) == 1 else "them"
    return [
        _warning(
            f"Neither side has a column named {quoted_list(unknown)}, so the rules that "
            f"name {pronoun} do nothing. Check the spelling."
        )
    ]


def _fuzzy_extra_findings(diff: DiffConfig) -> list[ConfigFinding]:
    """Report similarity rules a local run cannot score without the `fuzzy` extra."""
    if _matching.rapidfuzz_distance is not None:
        return []
    return [
        _error(
            missing_extra("fuzzy", f"rules[{index}], whose similarity limit a local run scores,")
        )
        for index, rule in enumerate(diff.rules)
        if rule.max_levenshtein_distance is not None or rule.min_jaro_winkler_similarity is not None
    ]


def _pushdown_findings(diff: DiffConfig, pair: WarehousePair) -> list[ConfigFinding]:
    """Report settings a warehouse run refuses for some stored names or types."""
    name = pair.warehouse.name
    compiler = SQLPushdownCompiler(pair.warehouse.dialect)
    findings: list[ConfigFinding] = []
    if diff.normalize_column_names:
        findings.append(
            _warning(
                f"normalize_column_names is on. A {name} run refuses it if any stored column "
                "name has uppercase letters or surrounding spaces, because pushdown quotes "
                "names exactly as they are stored."
            )
        )
    for index, rule in enumerate(diff.rules):
        if rule.min_jaro_winkler_similarity is not None:
            findings.append(
                _warning(
                    f"rules[{index}] sets min_jaro_winkler_similarity, which a {name} run "
                    "refuses on any column it compares as text. Use max_levenshtein_distance, "
                    "which compiles to SQL, or compare local copies of the tables."
                )
            )
        for setting, probe in (
            (
                "datetime_format",
                DiffRule(column_names=["probe"], datetime_format=rule.datetime_format),
            ),
            ("regex_replace", DiffRule(column_names=["probe"], regex_replace=rule.regex_replace)),
            (
                "max_levenshtein_distance",
                DiffRule(
                    column_names=["probe"], max_levenshtein_distance=rule.max_levenshtein_distance
                ),
            ),
        ):
            try:
                compiler.compile_column_predicate(
                    probe, "probe", source_dtype=pl.String(), target_dtype=pl.String()
                )
            except ConfigError as exc:
                findings.append(
                    _warning(
                        f"rules[{index}] {setting} has no {name} spelling, so a run "
                        f"refuses it on any column stored as text: {exc}"
                    )
                )
    return findings


def _pushdown_schema_findings(diff: DiffConfig, pair: WarehousePair) -> list[ConfigFinding]:
    """Probe a warehouse pair and compile its statements without running them."""
    try:
        return pair.with_session(
            lambda session, source_table, target_table: _check_pushdown_plan(
                session, source_table, target_table, diff
            )
        )
    except VerideltaError as exc:
        return [_error(str(exc))]


DEFAULT_MIN_CONFIDENCE: Final = 0.95
"""Share of a source value's rows that must agree on one target value before it
is proposed. Above one half, at most one target can qualify, and 5% leaves room
for noise in legacy data."""

DEFAULT_MIN_SUPPORT: Final = 5
"""Agreeing rows a proposal needs, so a one-off coincidence is never offered."""


def _check_value_map_thresholds(
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


def _compares_mapped_text(effective: EffectiveRule, dtype: pl.DataType) -> bool:
    """Decide whether a `value_map` output reaches the comparison unchanged."""
    return (
        isinstance(dtype, pl.String)
        and effective["pad_zeros"] is None
        and not effective["datetime_format"]
        and effective["cast_to"] in (None, "String")
    )


def _value_map_query(
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


def _value_map_proposal(
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


def _collect_value_map_proposals(
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
        if _compares_mapped_text(effective, source_schema[column])
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
        proposal = _value_map_proposal(diff, rule.rename_to or rule.column_names[0], frame)
        if proposal is not None:
            proposals.append(proposal)
    return proposals


class DiffEngine:
    """Compare two datasets on their primary keys and report what differs.

    The engine takes two Polars `LazyFrame` inputs and a `DiffConfig`, applies
    the nine-stage `DiffRule` pipeline to each side, then joins on the primary
    keys to classify rows as added (target-only), removed (source-only), or
    changed (present on both sides with at least one compared column
    differing). Nothing is materialized until `run()` collects the joins, so
    the source frames can be `scan_*` graphs over files far larger than memory.

    These entry points cover the usual situations:

    - `DiffEngine(config, source, target).run()` for frames you already hold.
      In-memory `DataFrame` inputs must be wrapped with `.lazy()` first.
    - `DiffEngine.run_from_configs(diff, source, target)` for `SourceRef`
      pairs from YAML. File, lakehouse, database, and DuckDB pairs load
      through `LoaderFactory` and run locally. A same-warehouse pair, or a
      Postgres or DuckDB pair that sets `pushdown`, compiles to SQL and runs
      where it is stored.
    - `DiffEngine.validate_schemas(...)` to enforce `schema_mode` and primary
      key presence on metadata alone, before any rows are read.
    - `DiffEngine.validate_rules(...)` to also resolve every rule and build
      each column's comparison, still on metadata alone.
    - `DiffEngine(config, source, target).propose_value_maps()`, or
      `DiffEngine.propose_value_maps_from_configs(...)` for any `SourceRef`
      pair, counted in the warehouse for same-warehouse pairs, to suggest
      `value_map` entries from how the two sides' values line up, without
      running the comparison.

    Attributes:
        config (DiffConfig): Primary keys, rules, defaults, and threshold.
        source (pl.LazyFrame): Source side; mutated in place as alignment and
            normalization stages run.
        target (pl.LazyFrame): Target side, treated as the authoritative schema.

    Raises:
        ConfigError: From `run()` when primary keys are missing, `schema_mode`
            is violated, or a rule cannot apply to the column it names.
        DataIntegrityError: From `run()` when primary keys are not unique on
            either side after normalization.
    """

    def __init__(
        self, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> None:
        """Hold the configuration and the two datasets to compare.

        `run()` aligns the datasets and compares them.

        Args:
            config (DiffConfig): Keys, rules, and settings for the comparison.
            source_df (pl.LazyFrame): Source rows, as stored.
            target_df (pl.LazyFrame): Target rows, as stored.
        """
        self.config = config
        self.source = source_df
        self.target = target_df
        # How each side was read, set by the entry points that load a `SourceRef`,
        # so an error can name the file and its format rather than only a column.
        self._sides: dict[str, str] | None = None

    @classmethod
    def _on_sources(cls, diff: DiffConfig, source: SourceRef, target: SourceRef) -> "DiffEngine":
        """Load both sides and build an engine whose errors name them."""
        engine = cls(diff, LoaderFactory.load(source), LoaderFactory.load(target))
        engine._sides = describe_sides(source, target)
        return engine

    @classmethod
    def run_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        baseline: Baseline | None = None,
    ) -> DiffResult:
        """Route a comparison to pushdown or local Polars evaluation.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source file, lakehouse, database, DuckDB, or warehouse config.
            target (SourceRef): Target file, lakehouse, database, DuckDB, or warehouse config.
            baseline (Baseline | None): Drift to accept, which a local run leaves out
                of the counts and the verdict.

        Returns:
            DiffResult: The result. A pushdown pair returns counts and keys, and a
                local pair also returns the differing rows.

        Raises:
            ConfigError: If primary keys are missing, `schema_mode` is violated, or a
                baseline is given for a pair compared where it is stored.
            DataIntegrityError: If either dataset repeats a normalized primary key.
            ConnectorError: If warehouse backends are mixed or connections differ.

        Examples:
            >>> from veridelta.models import DiffConfig, SourceConfig
            >>> diff = DiffConfig(primary_keys=["order_id"])
            >>> source = SourceConfig(path="legacy/orders.parquet", format="parquet")
            >>> target = SourceConfig(path="modern/orders.parquet", format="parquet")
            >>> result = DiffEngine.run_from_configs(diff, source, target)  # doctest: +SKIP
        """
        pair = check_backend_pairing(source, target)
        if pair is not None:
            if baseline is not None:
                raise ConfigError(
                    "A baseline applies to a run that compares both sides locally, and this "
                    "pair is compared where it is stored. Leave out --baseline, or compare "
                    "files exported from it."
                )
            return pair.with_session(
                lambda session, source_table, target_table: collect_pushdown_summary(
                    session, source_table, target_table, diff
                )
            )

        # `run()` normalizes headers and applies renames exactly once, so the
        # frames go to it straight from the loaders: aligning them first and
        # renaming again would undo a swap and collapse a chain.
        return cls._on_sources(diff, source, target).run(baseline=baseline)

    @classmethod
    def validate_schemas(
        cls, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> None:
        """Align structure and enforce `SchemaMode` without comparing any rows.

        Operates on schema metadata only, so callers may pass zero-row frames.

        Args:
            config (DiffConfig): Comparison settings and rules.
            source_df (pl.LazyFrame): Source frame or column probe.
            target_df (pl.LazyFrame): Target frame or column probe.

        Raises:
            ConfigError: If primary keys are missing or schema constraints are violated.
        """
        engine = cls(config, source_df, target_df)
        engine._align_structure()
        engine._validate_schema()

    @classmethod
    def validate_rules(
        cls, config: DiffConfig, source_df: pl.LazyFrame, target_df: pl.LazyFrame
    ) -> list[str]:
        """Check everything a run checks before it reads a row.

        Goes past `validate_schemas`: every rule is resolved against the aligned
        columns, both schemas are normalized, and each column's comparison is built. A
        rule the run could not honor fails here, such as a null sentinel its column's
        type cannot hold, or a similarity limit without the `fuzzy` extra. Operates on
        schema metadata only, so callers may pass zero-row frames. Repeated keys and
        invalid regular expressions surface only when rows are read.

        Args:
            config (DiffConfig): Comparison settings and rules.
            source_df (pl.LazyFrame): Source frame or column probe.
            target_df (pl.LazyFrame): Target frame or column probe.

        Returns:
            list[str]: The columns a run would compare, in source order, under their
                target names.

        Raises:
            ConfigError: If primary keys are missing or hold two types a join
                cannot pair, schema constraints are violated, or a rule cannot
                apply as configured.
        """
        return cls(config, source_df, target_df)._plan()[0]

    @staticmethod
    def check_configs(
        diff: DiffConfig, source: SourceRef, target: SourceRef, *, schemas: bool = False
    ) -> list[ConfigFinding]:
        """Check a loaded configuration for what would stop a run.

        By default nothing connects and no rows are read, so the checks need
        only the configuration and the installed extras:

        - the pair is one an engine can compare: both local, or two tables on
          one warehouse connection;
        - every extra a side reads through, or a local run scores with, is
          installed;
        - a database `table` uses a URI scheme Veridelta can quote for;
        - each `regex_replace` pattern compiles in Polars;
        - on a warehouse pair, settings the warehouse refuses for some stored
          names or types, reported as warnings.

        With `schemas`, and no errors so far, each side's columns are read too,
        but never its rows. Local sides are checked with `validate_rules`; a
        database `table` is read with a zero-row probe, and a `query` is not
        run at all. A warehouse pair runs the schema probes a run starts with,
        then compiles every comparison statement without executing it, which
        settles the warnings above one way or the other. Either way, a name in a
        rule's `column_names` that neither side has is a warning, since that rule
        does nothing.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            schemas (bool): Whether to also connect and check the rules against
                the stored columns.

        Returns:
            list[ConfigFinding]: Errors and warnings, empty when nothing is
                wrong. A configuration with no errors is expected to start.
        """
        findings: list[ConfigFinding] = []
        pair: WarehousePair | None = None
        try:
            pair = check_backend_pairing(source, target)
        except (ConfigError, ConnectorError) as exc:
            findings.append(_error(str(exc)))
        findings += _missing_extra_findings(source, target)
        findings += _database_findings(source, target)
        findings += _regex_findings(diff, pushdown=pair is not None)
        if pair is None:
            findings += _fuzzy_extra_findings(diff)
        if not schemas:
            return findings if pair is None else findings + _pushdown_findings(diff, pair)
        if any(finding.severity == "error" for finding in findings):
            return [*findings, _warning("Schemas were not checked, because of the errors above.")]
        if pair is None:
            return findings + DiffEngine._local_schema_findings(diff, source, target)
        return findings + _pushdown_schema_findings(diff, pair)

    @staticmethod
    def check_config_file(
        path: str | Path, *, schemas: bool = False, allow_missing_env: bool = False
    ) -> list[ConfigFinding]:
        """Load a configuration file and check it for what would stop a run.

        A file that does not load is one error finding, with the loader's
        message, so every problem is reported the same way. A file that loads
        gets the checks of `check_configs`. `veridelta validate` and the MCP
        server's `validate_config` tool both report through this method.

        Args:
            path (str | Path): The configuration file.
            schemas (bool): Whether to also connect and check the rules against
                the stored columns.
            allow_missing_env (bool): Whether to read an unset `${NAME}` as the
                text `NAME` and warn, instead of failing, so a file can be
                checked without its secrets.

        Returns:
            list[ConfigFinding]: A warning for each unset variable first, then
                the findings of `check_configs`, or the error that stopped the
                load.
        """
        unset: list[str] | None = [] if allow_missing_env else None
        try:
            diff, source, target = load_config(path, unset_env=unset)
            findings = DiffEngine.check_configs(diff, source, target, schemas=schemas)
        except ConfigError as exc:
            findings = [_error(str(exc).strip())]
        unset_findings = [
            _warning(
                f"Environment variable '{name}' is not set, so its references were checked "
                f"as the text '{name}'."
            )
            for name in unset or []
        ]
        return [*unset_findings, *findings]

    @staticmethod
    def read_schema(config: SourceRef) -> pl.Schema:
        """Read one side's columns and their types, and return none of its rows.

        Each side is read as a run reads it before its first row. A file is
        read by its loader, and a CSV file's types come from its first rows; a
        lakehouse table gives its schema; a database or DuckDB `table` is read
        with a probe that returns no rows. A warehouse table, or a table with
        `pushdown`, gets the probe a pushdown run starts with, in a session of
        its own that is closed after.

        Args:
            config (SourceRef): One side of a configuration.

        Returns:
            pl.Schema: The columns in their stored order and with their stored
                names, before `normalize_column_names` or a `rename_to`.

        Raises:
            ConfigError: If the side reads a database or DuckDB `query`, which a
                probe would have to run in full.
            ConnectorError: If the side cannot be reached or read.
        """
        if not is_warehouse(config):
            return schema_frame(config).collect_schema()
        with warehouse_session(config) as session:
            return probe_relation(session, table_name(config))[1]

    @classmethod
    def _local_schema_findings(
        cls, diff: DiffConfig, source: SourceRef, target: SourceRef
    ) -> list[ConfigFinding]:
        """Check the rules against two local sides' stored columns."""
        queries = [
            _warning(
                f"The {label} reads a query, which validate does not run, so the rules were "
                "not checked against stored columns."
            )
            for label, config in (("source", source), ("target", target))
            if isinstance(config, (DatabaseConfig, DuckDBConfig)) and config.query is not None
        ]
        if queries:
            return queries
        try:
            source_frame, target_frame = schema_frame(source), schema_frame(target)
        except (VerideltaError, OSError, pl.exceptions.PolarsError) as exc:
            return [_error(f"Could not read the schemas: {exc}")]
        engine = cls(diff, source_frame, target_frame)
        engine._sides = describe_sides(source, target)
        try:
            engine._plan()
        except ConfigError as exc:
            return [_error(str(exc))]
        return _unknown_column_findings(
            diff, source_frame.collect_schema().names(), target_frame.collect_schema().names()
        )

    @classmethod
    def propose_value_maps_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        min_support: int = DEFAULT_MIN_SUPPORT,
        sample_fraction: float = 1.0,
    ) -> list[ValueMapProposal]:
        """Propose `value_map` entries for a `SourceRef` pair, wherever it lives.

        File, lakehouse, and database pairs are loaded and proposed locally. A
        pair of tables on one warehouse connection is counted in the warehouse
        instead, in one statement, for columns stored as text on both sides;
        rows never leave it. A sampled warehouse run reads a different, though
        equally repeatable, set of keys than a local one.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            min_confidence (float): Share of rows that must agree, above 0.5.
            min_support (int): Agreeing rows a proposal needs.
            sample_fraction (float): Share of source rows to read.

        Returns:
            list[ValueMapProposal]: One proposal per column with new entries.

        Raises:
            ConnectorError: If only one side is a warehouse table, the sides use
                different warehouses or connections, or a warehouse returns a
                malformed result.
            ConfigError: If a threshold is out of range, or the configuration
                fails as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.
        """
        # Bad thresholds fail before a session opens or a file is read.
        _check_value_map_thresholds(min_confidence, min_support, sample_fraction)
        pair = check_backend_pairing(source, target)
        if pair is not None:
            return pair.with_session(
                lambda session, source_table, target_table: _collect_value_map_proposals(
                    session,
                    source_table,
                    target_table,
                    diff,
                    min_confidence=min_confidence,
                    min_support=min_support,
                    sample_fraction=sample_fraction,
                )
            )
        engine = cls._on_sources(diff, source, target)
        return engine.propose_value_maps(
            min_confidence=min_confidence,
            min_support=min_support,
            sample_fraction=sample_fraction,
        )

    def propose_value_maps(
        self,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        min_support: int = DEFAULT_MIN_SUPPORT,
        sample_fraction: float = 1.0,
    ) -> list[ValueMapProposal]:
        """Propose `value_map` entries from how source and target values line up.

        Rows are aligned, normalized, and joined as `run()` does, on a copy, so this
        engine can still run afterward. For each compared text column that stage 4 can
        map, a source value is proposed for the target value it lines up with in at
        least `min_confidence` of its joined rows, provided at least `min_support` rows
        agree. Values are read as the `value_map` stage sees them, so a
        `case_insensitive` column gets lowercase keys. Rows a column's existing map
        already translates are left out, so a raw value equal to one of that map's
        outputs cannot receive an entry.

        Args:
            min_confidence (float): Share of rows that must agree, above 0.5.
            min_support (int): Agreeing rows a proposal needs.
            sample_fraction (float): Share of source rows to read, picked by a hash of
                the primary keys, so the same data samples the same rows under one
                Polars version.

        Returns:
            list[ValueMapProposal]: One proposal per column with new entries, in source
                column order.

        Raises:
            ConfigError: If a threshold is out of range, or the configuration fails as
                it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": range(6), "sex": ["M"] * 6})
            >>> target = pl.LazyFrame({"id": range(6), "sex": ["Male"] * 6})
            >>> engine = DiffEngine(DiffConfig(primary_keys=["id"]), source, target)
            >>> proposals = engine.propose_value_maps()
            >>> proposals[0].to_rule().value_map
            {'M': 'Male'}
        """
        _check_value_map_thresholds(min_confidence, min_support, sample_fraction)
        prepared = type(self)(self.config, self.source, self.target)
        prepared._sides = self._sides
        prepared._align_structure()
        prepared._validate_schema()
        stored = prepared.source.collect_schema()
        prepared._normalize_sides()
        prepared._check_uniqueness()

        columns = prepared._value_map_columns(stored)
        if not columns:
            return []
        joined = prepared._value_map_join(columns, sample_fraction)
        frames = pl.collect_all(
            _value_map_query(
                joined,
                column,
                mapped_values=list(
                    (prepared._get_effective_rule(column)["value_map"] or {}).values()
                ),
                min_confidence=min_confidence,
                min_support=min_support,
            )
            for column in columns
        )
        proposals = (
            _value_map_proposal(prepared.config, column, frame)
            for column, frame in zip(columns, frames, strict=True)
        )
        return [proposal for proposal in proposals if proposal is not None]

    @classmethod
    def suggest_rules_from_configs(
        cls,
        diff: DiffConfig,
        source: SourceRef,
        target: SourceRef,
        *,
        max_share: float = DEFAULT_MAX_SHARE,
    ) -> list[RuleSuggestion]:
        """Suggest rules for a `SourceRef` pair, read locally.

        Args:
            diff (DiffConfig): Comparison settings and rules.
            source (SourceRef): Source configuration.
            target (SourceRef): Target configuration.
            max_share (float): Largest gap a tolerance may explain, as a share of
                the larger of its two values, above 0 and at most 1.

        Returns:
            list[RuleSuggestion]: One suggestion per column it can explain.

        Raises:
            ConfigError: If `max_share` is out of range, the pair is compared where
                it is stored, or the configuration fails as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.
        """
        check_max_share(max_share)
        if check_backend_pairing(source, target) is not None:
            raise ConfigError(
                "veridelta suggest reads both sides locally, and this pair is compared "
                "where it is stored. Suggest rules on files exported from it, or on a "
                "database pair that does not set pushdown."
            )
        engine = cls._on_sources(diff, source, target)
        return engine.suggest_rules(max_share=max_share)

    def suggest_rules(self, *, max_share: float = DEFAULT_MAX_SHARE) -> list[RuleSuggestion]:
        """Suggest rules that would explain the differences in each compared column.

        The comparison runs as `run()` runs it, on a copy, without writing artifacts,
        so this engine can still run afterward. A numeric column gets a tolerance when
        every gap between its differing values is at most `max_share` of the larger
        of the two: an absolute tolerance when the gaps stay about one size, and a
        relative one when they grow with the values. Each tolerance is the round value
        just above the largest gap, and the comparison runs again with it to count
        the rows it explains. No model is called.

        Args:
            max_share (float): Largest gap a tolerance may explain, as a share of the
                larger of its two values, above 0 and at most 1.

        Returns:
            list[RuleSuggestion]: One suggestion per column it can explain, in
                compared column order.

        Raises:
            ConfigError: If `max_share` is out of range, or the configuration fails
                as it would in a run.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": [1, 2, 3], "fare": [10.0, 20.0, 30.0]})
            >>> target = pl.LazyFrame({"id": [1, 2, 3], "fare": [10.004, 20.004, 30.003]})
            >>> engine = DiffEngine(DiffConfig(primary_keys=["id"]), source, target)
            >>> [(s.column, s.settings) for s in engine.suggest_rules()]
            [('fare', {'absolute_tolerance': 0.005})]
        """
        check_max_share(max_share)
        result = self._run_copy(self.config)
        keys = list(result.primary_keys)
        suggestions: list[RuleSuggestion] = []
        for column in result.compared_columns:
            differing = result.summary.column_mismatches.get(column, 0)
            if differing == 0:
                continue
            proposal = column_proposal(result.changed, column, max_share)
            if proposal is None:
                continue
            governing = match_rule(self.config.rules, column)
            rule = suggested_rule(
                governing, column, proposal.settings, self.config.default_null_values
            )
            try:
                tried = self._run_copy(
                    self.config.model_copy(update={"rules": [rule, *self.config.rules]})
                )
            except ConfigError:
                # The configuration refuses the rule, such as a sentinel that one side's
                # type cannot hold, so it explains nothing.
                continue
            explained = differing_only_in(result.changed, tried.changed, keys, column)
            # A rule that makes a matching row differ, such as case folding ahead of a
            # `value_map` written in capitals, is no explanation.
            broken = differing_only_in(tried.changed, result.changed, keys, column)
            if explained.is_empty() or not broken.is_empty():
                continue
            suggestions.append(
                RuleSuggestion(
                    column=column,
                    settings=proposal.settings,
                    differing=differing,
                    explained=explained.height,
                    largest_gap=proposal.largest_gap,
                    examples=tuple(explained.head(SUGGESTED_EXAMPLES).to_dicts()),
                    rule=rule,
                    governing_rule_index=(
                        None if governing is None else self.config.rules.index(governing)
                    ),
                )
            )
        return suggestions

    def _run_copy(self, config: DiffConfig) -> DiffResult:
        """Run a comparison of this engine's data under `config`, writing no artifacts."""
        engine = type(self)(
            config.model_copy(update={"output_path": None}), self.source, self.target
        )
        engine._sides = self._sides
        return engine.run()

    def _value_map_columns(self, stored: pl.Schema) -> list[str]:
        """Pick the compared columns a `value_map` proposal can apply to."""
        keys = set(self.config.primary_keys)
        target_schema = self.target.collect_schema()
        columns: list[str] = []
        for column, dtype in stored.items():
            if column in keys or column not in target_schema:
                continue
            rule = self._get_effective_rule(column)
            if rule["ignore"] or not _compares_mapped_text(rule, dtype):
                continue
            if self.config.strict_types and not isinstance(target_schema[column], pl.String):
                # Types that differ always mismatch under strict_types; no map helps.
                continue
            columns.append(column)
        return columns

    def _value_map_join(self, columns: list[str], sample_fraction: float) -> pl.LazyFrame:
        """Pair each candidate column's normalized values on the primary keys."""
        keys = self.config.primary_keys
        source = self.source.select(*keys, pl.col(columns).name.suffix("_source"))
        target = self.target.select(*keys, pl.col(columns).name.suffix("_target"))
        if sample_fraction < 1:
            cutoff = round(sample_fraction * SAMPLE_BUCKETS)
            source = source.filter(pl.struct(keys).hash(seed=0) % SAMPLE_BUCKETS < cutoff)
        return source.join(target, on=keys, how="inner")

    def _get_effective_rule(self, col_name: str) -> EffectiveRule:
        """Resolve all rules (Specific > Pattern > Global) into a unified dictionary."""
        return fold_rule_defaults(match_rule(self.config.rules, col_name), self.config)

    def _check_uniqueness(self) -> None:
        """Verify that primary keys are unique in both datasets."""
        pks = self.config.primary_keys
        for side, frame in (("SOURCE", self.source), ("TARGET", self.target)):
            keys = frame.select(pks).collect()
            duplicated = keys.is_duplicated()
            if duplicated.any():
                raise duplicate_keys_error(pks, side, keys.filter(duplicated).height)

    def _normalize_sides(self) -> None:
        """Normalize both frames, then check that their primary keys can pair rows."""
        self.source = self._normalize_frame(self.source, is_source=True)
        self.target = self._normalize_frame(self.target, is_source=False)
        self._check_key_types()

    def _check_key_types(self) -> None:
        """Refuse a primary key whose two sides hold types a join cannot pair.

        A join pairs keys of one type, of two integer types, or of two float
        types, and fails on any other pair with an error that names no side.
        A rule with `cast_to` on the key brings both sides to one type before
        rows are paired.

        Raises:
            ConfigError: If a key holds two such types after normalization.
        """
        source = self.source.collect_schema()
        target = self.target.collect_schema()
        mixed = [
            f"'{key}' ({source[key]} in the source, {target[key]} in the target)"
            for key in self.config.primary_keys
            if not pairable(source[key], target[key])
        ]
        if mixed:
            raise ConfigError(
                "Rows pair only on keys of one type, or of two integer or two float types, "
                f"and these primary keys hold two other types: {', '.join(mixed)}. Give each "
                "a rule with cast_to, such as cast_to: Int64, so both sides hold one type."
            )

    def _normalize_frame(self, frame: pl.LazyFrame, *, is_source: bool) -> pl.LazyFrame:
        """Apply stages 1 through 7 of the canonical transform order to one dataset."""
        schema = frame.collect_schema()
        rules = {
            column: rule
            for column in schema.names()
            if not (rule := self._get_effective_rule(column))["ignore"]
        }

        value_exprs: list[pl.Expr] = []
        for column, rule in rules.items():
            value_expr = self._normalize_value_expr(
                column, rule, schema[column], is_source=is_source
            )
            if value_expr is not None:
                value_exprs.append(value_expr)
        if value_exprs:
            frame = frame.with_columns(value_exprs)

        temporal = [column for column, rule in rules.items() if rule["timezone"] or rule["cast_to"]]
        if not temporal:
            return frame
        # A second pass, since every expression in one `with_columns` reads the
        # input frame. Stage 6a can turn a String column into a Datetime, so the
        # timezone guard inspects the post-parse schema.
        parsed_schema = frame.collect_schema()
        return frame.with_columns(
            self._normalize_temporal_expr(column, rules[column], parsed_schema[column])
            for column in temporal
        )

    def _normalize_value_expr(
        self, column: str, rule: EffectiveRule, dtype: pl.DataType, *, is_source: bool
    ) -> pl.Expr | None:
        """Build stages 1 through 6a for one column: sentinels through datetime parsing."""
        expr = pl.col(column)
        applied = False
        # Deliberately narrower than `is_text_dtype`: `is_in` accepts Categorical
        # and Enum, but the `.str` namespace used below rejects both.
        is_text = isinstance(dtype, pl.String)

        sentinels = usable_sentinels(rule["null_values"], dtype)
        if sentinels:
            expr = pl.when(expr.is_in(sentinels)).then(None).otherwise(expr)
            applied = True
        # Only an explicit rule raises: a global default is expected to span a mixed schema.
        elif rule["null_values"] and rule["null_values_explicit"]:
            raise unusable_sentinel_error(column, dtype, rule["null_values"])

        if is_text:
            expr, text_applied = self._normalize_text_expr(expr, rule, is_source=is_source)
            applied = applied or text_applied

        if rule["pad_zeros"] is not None:
            # Stringify first so a numeric 123 and a text '00123' converge.
            expr = expr.cast(pl.String).str.zfill(rule["pad_zeros"])
            applied = True
            is_text = True

        if rule["datetime_format"] and is_text:
            expr = expr.str.strptime(
                pl.Datetime,
                format=polars_datetime_format(rule["datetime_format"]),
                strict=False,
            )
            applied = True

        return expr.alias(column) if applied else None

    @staticmethod
    def _normalize_text_expr(
        expr: pl.Expr, rule: EffectiveRule, *, is_source: bool
    ) -> tuple[pl.Expr, bool]:
        """Build stages 2 through 4 for a text column: regex, whitespace, case, value map."""
        applied = False
        if rule["regex_replace"]:
            for pattern, replacement in rule["regex_replace"].items():
                expr = expr.str.replace_all(pattern, replacement)
            applied = True

        mode = rule["whitespace"]
        if mode == "left":
            expr = expr.str.strip_chars_start()
            applied = True
        elif mode == "right":
            expr = expr.str.strip_chars_end()
            applied = True
        elif mode == "both":
            expr = expr.str.strip_chars()
            applied = True

        if rule["case_insensitive"]:
            expr = expr.str.to_lowercase()
            applied = True

        if is_source and rule["value_map"]:
            expr = expr.replace(rule["value_map"])
            applied = True

        return expr, applied

    def _normalize_temporal_expr(
        self, column: str, rule: EffectiveRule, dtype: pl.DataType
    ) -> pl.Expr:
        """Build stages 6b and 7 for one column: timezone conversion, then cast."""
        expr = pl.col(column)
        if rule["timezone"]:
            expr = self._convert_time_zone(column, expr, dtype, rule["timezone"])
        if rule["cast_to"]:
            target = rule["cast_to"]
            if target in UNCASTABLE.get(type(dtype), frozenset()):
                raise ConfigError(
                    f"Column '{column}' sets cast_to='{target}', but holds {dtype}, which "
                    f"cannot be cast to {target}. Convert it where it is read, such as in "
                    "a database query."
                )
            expr = expr.cast(CAST_TARGETS[target])
        return expr.alias(column)

    def _convert_time_zone(
        self, column: str, expr: pl.Expr, dtype: pl.DataType, zone: str
    ) -> pl.Expr:
        """Convert a timezone-aware column to `zone`, refusing to guess for naive data."""
        # `convert_time_zone` treats a naive timestamp as UTC instead of refusing it.
        reject_unzoned_timezone(column, dtype, zone)
        try:
            return expr.dt.convert_time_zone(zone)
        except pl.exceptions.ComputeError as exc:
            raise ConfigError(f"Column '{column}' sets an unusable timezone. {exc}") from exc

    def _build_match_expr(self, col_name: str, rule: EffectiveRule, dtype: pl.DataType) -> pl.Expr:
        """Build stages 8 and 9 of the transform order: comparison and null equality."""
        src = pl.col(f"{col_name}_source")
        tgt = pl.col(f"{col_name}_target")

        tgt_dtype = self.target.collect_schema().get(col_name)
        # The type the target is compared as, once any soft cast below applies.
        compared_tgt_dtype = tgt_dtype

        if dtype != tgt_dtype:
            if self.config.strict_types:
                return null_equality(pl.lit(False), src, tgt, rule)
            # Numbers skip the cast, which would truncate a Float64 `10.7` to an Int64 `10`.
            elif not (dtype.is_numeric() and tgt_dtype is not None and tgt_dtype.is_numeric()):
                tgt = tgt.cast(dtype, strict=False)
                compared_tgt_dtype = dtype

        similar = similarity_test(rule) if isinstance(dtype, pl.String) else None
        if dtype.is_numeric() and (rule["abs_tol"] != 0.0 or rule["rel_tol"] != 0.0):
            val_match = tolerance_match(src, tgt, rule, dtype, compared_tgt_dtype)
        elif similar is not None:
            # Equal text matches outright, so only a differing pair is scored.
            val_match = (src == tgt) | similarity_expr(src, tgt, similar)
        else:
            val_match = src == tgt
        return null_equality(val_match, src, tgt, rule)

    def _align_structure(self) -> None:
        """Perform structural normalization to reconcile asymmetrical schemas."""
        if self.config.normalize_column_names:
            self.source = normalize_header_names(self.source)
            self.target = normalize_header_names(self.target)
        self.source, self.target = drop_and_rename(self.config.rules, self.source, self.target)

    def _validate_schema(self) -> None:
        """Enforce the configured `SchemaMode` before comparison."""
        check_schema(self.config, self.source, self.target, self._sides)

    def run(self, *, baseline: Baseline | None = None) -> DiffResult:
        """Compare the two datasets and return the result.

        The comparison stays lazy until it collects the joins, so the inputs can be
        scans over files larger than memory. A run takes these steps, in order:

        1. Align the columns: apply renames, drop ignored columns, and treat the
           target as the authoritative schema.
        2. Check that the primary keys exist and that `schema_mode` holds.
        3. Apply stages 1 to 7 of the `DiffRule` transform order to each side, and
           build each column's stage 8 and 9 comparison, so a rule the run cannot
           honor fails before any rows move.
        4. Check that the normalized primary keys are unique on each side.
        5. Find the added, removed, and changed rows, leave out the drift
           `baseline` accepts, and count the mismatches.
        6. Write the artifacts, when `output_path` is set.

        Args:
            baseline (Baseline | None): Drift to accept: rows by kind and key, and
                changed columns by row. The counts, the verdict, the artifacts,
                and the reports leave it out, and `accepted_count` counts it.

        Returns:
            DiffResult: Counts, column-level drift, and the differing rows.

        Raises:
            ConfigError: If a primary key is missing or holds two types a join
                cannot pair, `schema_mode` is violated, a similarity limit needs
                the missing `fuzzy` extra, or the artifact format has no writer.
            DataIntegrityError: If either dataset repeats a normalized primary key.

        Examples:
            >>> import polars as pl
            >>> from veridelta.models import DiffConfig
            >>> source = pl.LazyFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
            >>> target = pl.LazyFrame({"id": [2, 3, 4], "amount": [20.0, 31.0, 40.0]})
            >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
            >>> result.summary.added_count, result.summary.removed_count
            (1, 1)
            >>> result.summary.column_mismatches
            {'amount': 1}
        """
        compared_columns, match_expressions = self._plan()

        # Runs after normalization: case folding or sentinel coercion on a key
        # column can collapse distinct rows into duplicates, and that must fail
        # here rather than silently exploding the joins below.
        self._check_uniqueness()

        keys = self.config.primary_keys
        added_df = self.target.join(self.source, on=keys, how="anti").collect()
        removed_df = self.source.join(self.target, on=keys, how="anti").collect()
        changed_df = self._collect_changed_rows(compared_columns, match_expressions)
        if baseline is None:
            return self._build_result(added_df, removed_df, changed_df, compared_columns)
        kept = accept_baseline(
            baseline, list(keys), (added_df, removed_df, changed_df), compared_columns
        )
        return self._build_result(
            kept.added, kept.removed, kept.changed, compared_columns, kept.rows, kept.accepted
        )

    def _plan(self) -> tuple[list[str], list[pl.Expr]]:
        """Align, validate, and normalize both frames, then build the comparisons."""
        self._align_structure()
        self._validate_schema()
        self._normalize_sides()
        return self._match_expressions()

    def _match_expressions(self) -> tuple[list[str], list[pl.Expr]]:
        """Build one boolean match expression per compared column."""
        # Re-read the schema here: normalization may have retyped columns.
        source_schema = self.source.collect_schema()
        target_columns = set(self.target.collect_schema().names())
        keys = set(self.config.primary_keys)

        compared: list[str] = []
        expressions: list[pl.Expr] = []
        for column in source_schema.names():
            if column in keys or column not in target_columns:
                continue
            rule = self._get_effective_rule(column)
            if rule["ignore"]:
                continue
            expr = self._build_match_expr(column, rule, source_schema[column])
            compared.append(column)
            expressions.append(expr.alias(f"{column}_is_match"))
        return compared, expressions

    def _collect_changed_rows(
        self, compared_columns: list[str], match_expressions: list[pl.Expr]
    ) -> pl.DataFrame:
        """Inner-join both sides and keep the rows where any compared column differs."""
        if not match_expressions:
            return pl.DataFrame()

        keys = self.config.primary_keys
        source = self.source.rename(lambda col: col if col in keys else f"{col}_source")
        target = self.target.rename(lambda col: col if col in keys else f"{col}_target")
        common_lazy = source.join(target, on=keys, how="inner")

        all_matched = pl.all_horizontal([f"{col}_is_match" for col in compared_columns])
        return common_lazy.with_columns(match_expressions).filter(~all_matched).collect()

    def _build_result(
        self,
        added_df: pl.DataFrame,
        removed_df: pl.DataFrame,
        changed_df: pl.DataFrame,
        compared_columns: list[str],
        accepted_count: int = 0,
        accepted: Baseline | None = None,
    ) -> DiffResult:
        """Count totals, apply the threshold, export artifacts, and assemble the result."""
        column_mismatches = local_column_mismatches(changed_df, compared_columns)

        # Count rows, as the warehouse's COUNT(*) does. Counting a key column
        # would skip rows whose key is null, which still count as removed or
        # added and so belong in the threshold's denominator.
        src_total = self.source.select(pl.len()).collect().item()
        tgt_total = self.target.select(pl.len()).collect().item()

        artifacts_written = export_artifacts(
            {"added_rows": added_df, "removed_rows": removed_df, "changed_rows": changed_df},
            self.config.output_path,
            self.config.output_format,
        )
        return DiffResult(
            summary=build_summary(
                self.config,
                changed_df,
                added_df,
                removed_df,
                src_total,
                tgt_total,
                column_mismatches,
                artifacts_written,
                accepted_count,
            ),
            added=added_df,
            removed=removed_df,
            changed=changed_df,
            primary_keys=tuple(self.config.primary_keys),
            compared_columns=tuple(compared_columns),
            accepted=accepted,
        )
