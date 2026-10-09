# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Find what would stop a comparison before it reads a row, for `veridelta validate`.

Each check returns findings rather than raising, so `validate` can report every
problem at once: a missing extra, a database driver, a pattern Polars cannot read,
a rule that names no column, or a pushdown the warehouse cannot run.
"""

from collections.abc import Callable, Iterable
from importlib.util import find_spec
from typing import Final
from urllib.parse import urlsplit

import polars as pl

from veridelta import _matching, _reading
from veridelta._pushdown import compile_pushdown, plan_pushdown
from veridelta._resolution import quoted_list
from veridelta._warehouses import WarehousePair
from veridelta.connectors import database as database_connectors
from veridelta.connectors import duckdb as duckdb_connectors
from veridelta.connectors import warehouse as warehouse_connectors
from veridelta.connectors.base import PushdownSession
from veridelta.connectors.sql import SQLPushdownCompiler, compile_database_select
from veridelta.exceptions import ConfigError, ConnectorError, VerideltaError, missing_extra
from veridelta.models import (
    BigQueryConfig,
    ConfigFinding,
    DatabaseConfig,
    DatabricksConfig,
    DeltaLakeConfig,
    DiffConfig,
    DiffRule,
    DuckDBConfig,
    IcebergConfig,
    SnowflakeConfig,
    SourceConfig,
    SourceRef,
    normalize_column_name,
)

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


def error_finding(message: str) -> ConfigFinding:
    """Build a finding that would stop a run."""
    return ConfigFinding(severity="error", message=message)


def warning_finding(message: str) -> ConfigFinding:
    """Build a finding that stops a run only for some stored names or types."""
    return ConfigFinding(severity="warning", message=message)


def missing_extra_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report each optional extra a side reads through that is not installed."""
    sides: dict[str, list[str]] = {}
    for label, config in (("source", source), ("target", target)):
        required = _required_extra(config)
        if required is not None and not required[1]():
            sides.setdefault(required[0], []).append(label)
    return [
        error_finding(missing_extra(extra, f"Reading the {' and '.join(labels)}"))
        for extra, labels in sides.items()
    ]


def database_findings(source: SourceRef, target: SourceRef) -> list[ConfigFinding]:
    """Report a database `table` whose URI scheme Veridelta cannot quote for."""
    findings: list[ConfigFinding] = []
    for label, config in (("source", source), ("target", target)):
        if isinstance(config, DatabaseConfig) and config.table is not None:
            try:
                compile_database_select(urlsplit(config.uri).scheme.lower(), config.table)
            except (ConfigError, ConnectorError) as exc:
                findings.append(error_finding(f"{label}: {exc}"))
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


def regex_findings(diff: DiffConfig, *, pushdown: bool) -> list[ConfigFinding]:
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
                    warning_finding(
                        f"{message} A warehouse run hands it to the warehouse's own regular "
                        "expression engine, which may accept it, but a local run would fail."
                    )
                )
            else:
                findings.append(error_finding(message))
    return findings


def unknown_column_findings(
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
        warning_finding(
            f"Neither side has a column named {quoted_list(unknown)}, so the rules that "
            f"name {pronoun} do nothing. Check the spelling."
        )
    ]


def fuzzy_extra_findings(diff: DiffConfig) -> list[ConfigFinding]:
    """Report similarity rules a local run cannot score without the `fuzzy` extra."""
    if _matching.rapidfuzz_distance is not None:
        return []
    return [
        error_finding(
            missing_extra("fuzzy", f"rules[{index}], whose similarity limit a local run scores,")
        )
        for index, rule in enumerate(diff.rules)
        if rule.max_levenshtein_distance is not None or rule.min_jaro_winkler_similarity is not None
    ]


def _check_pushdown_plan(
    connector: PushdownSession, source_table: str, target_table: str, diff: DiffConfig
) -> list[ConfigFinding]:
    """Do what a warehouse run does before reading a row, and compile the rest.

    Returns the warning for a rule's column name that neither table holds, if any.
    """
    plan = plan_pushdown(connector, source_table, target_table, diff)
    compile_pushdown(connector.compiler, (source_table, target_table), diff, plan)
    return unknown_column_findings(diff, plan.source_schema, plan.target_schema)


def pushdown_findings(diff: DiffConfig, pair: WarehousePair) -> list[ConfigFinding]:
    """Report settings a warehouse run refuses for some stored names or types."""
    name = pair.warehouse.name
    compiler = SQLPushdownCompiler(pair.warehouse.dialect)
    findings: list[ConfigFinding] = []
    if diff.normalize_column_names:
        findings.append(
            warning_finding(
                f"normalize_column_names is on. A {name} run refuses it if any stored column "
                "name has uppercase letters or surrounding spaces, because pushdown quotes "
                "names exactly as they are stored."
            )
        )
    for index, rule in enumerate(diff.rules):
        if rule.min_jaro_winkler_similarity is not None:
            findings.append(
                warning_finding(
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
                    warning_finding(
                        f"rules[{index}] {setting} has no {name} spelling, so a run "
                        f"refuses it on any column stored as text: {exc}"
                    )
                )
    return findings


def pushdown_schema_findings(diff: DiffConfig, pair: WarehousePair) -> list[ConfigFinding]:
    """Probe a warehouse pair and compile its statements without running them."""
    try:
        return pair.with_session(
            lambda session, source_table, target_table: _check_pushdown_plan(
                session, source_table, target_table, diff
            )
        )
    except VerideltaError as exc:
        return [error_finding(str(exc))]
