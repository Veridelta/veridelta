# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Golden SQL: every statement the pushdown compiler writes for fixed comparisons.

Each case is a configuration and the column types both tables report. For
each dialect, `render` plans and compiles the case the way a pushdown run
does, through a session that answers only the schema probes, and writes every
statement in run order. `tests/unit/test_sql_corpus.py` compares the result
with the files under `tests/fixtures/sql/<dialect>/`, so a change to the
compiler shows up as a diff of the SQL it writes. After a deliberate change,
rewrite the files with:

    uv run python -m tests.unit.sql_corpus
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, NamedTuple

import polars as pl

from veridelta._pushdown import (
    compile_pushdown,
    plan_pushdown,
    type_drift_columns,
    wide_integer_columns,
)
from veridelta._value_maps import collect_value_map_proposals
from veridelta.connectors.base import PushdownQueryType, PushdownSession
from veridelta.connectors.sql import (
    COUNT_ALIAS,
    VALUE_MAP_AGREEING_ALIAS,
    VALUE_MAP_COLUMN_ALIAS,
    VALUE_MAP_ROWS_ALIAS,
    VALUE_MAP_SOURCE_ALIAS,
    VALUE_MAP_TARGET_ALIAS,
    SampleQuery,
    SQLDialect,
    SQLPushdownCompiler,
)
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DiffConfig, DiffRule

if TYPE_CHECKING:
    from collections.abc import Mapping

FIXTURES: Final = Path(__file__).resolve().parent.parent / "fixtures" / "sql"
"""Where the golden files live, one folder per dialect."""

TABLES: Final = ("shop.orders_v1", "shop.orders_v2")
"""The source and target relation every case compares."""

STATEMENTS: Final[dict[str, str]] = {
    "compile_schema_probe_query": "probe",
    "compile_duplicate_key_query": "duplicates",
    "compile_count_query": "count",
    "compile_query": "changed",
    "compile_added_query": "added",
    "compile_missing_query": "missing",
    "compile_column_mismatch_query": "columns",
    "compile_changed_sample_query": "sample",
    "compile_changed_flags_query": "flags",
    "compile_value_map_query": "value_maps",
    "compile_column_predicate": "changed",
}
"""Each compiler method, and the statement label it writes or helps write."""

_VALUE_MAP_RESULT: Final = pl.Schema(
    {
        VALUE_MAP_COLUMN_ALIAS: pl.Int64(),
        VALUE_MAP_SOURCE_ALIAS: pl.String(),
        VALUE_MAP_TARGET_ALIAS: pl.String(),
        VALUE_MAP_ROWS_ALIAS: pl.Int64(),
        VALUE_MAP_AGREEING_ALIAS: pl.Int64(),
    }
)


class Case(NamedTuple):
    """One comparison, compiled for every dialect.

    Attributes:
        name (str): The fixture's file name, without `.sql`.
        diff (DiffConfig): The comparison settings.
        source (pl.Schema): The column types the source table reports.
        target (pl.Schema): The column types the target table reports.
        flags (tuple[dict[str, object], ...]): Changed-row keys a baseline
            accepts, to compile the flags statement for.
        value_maps (bool): Whether to compile the value map proposal statement.
        sample_fraction (float): The share of joined rows a value map reads.
    """

    name: str
    diff: DiffConfig
    source: pl.Schema
    target: pl.Schema
    flags: tuple[dict[str, object], ...] = ()
    value_maps: bool = False
    sample_fraction: float = 1.0


def _same(schema: Mapping[str, pl.DataType]) -> tuple[pl.Schema, pl.Schema]:
    """Return one schema for both sides."""
    return pl.Schema(schema), pl.Schema(schema)


def _tolerances() -> Case:
    """Two keys, numeric tolerances, a wide integer, and null-safe text."""
    source, target = _same(
        {
            "id": pl.Int64(),
            "region": pl.String(),
            "amount": pl.Float64(),
            "qty": pl.Int64(),
            "note": pl.String(),
            "active": pl.Boolean(),
            "price": pl.Decimal(12, 2),
        }
    )
    diff = DiffConfig(
        primary_keys=["id", "region"],
        pushdown_sample_rows=10,
        rules=[
            DiffRule(column_names=["amount"], absolute_tolerance=0.5, relative_tolerance=0.01),
            DiffRule(column_names=["qty"], absolute_tolerance=1),
            DiffRule(column_names=["note"], treat_null_as_equal=True),
            DiffRule(column_names=["price"], absolute_tolerance=0.01),
        ],
    )
    return Case("tolerances", diff, source, target, flags=({"id": 7, "region": "east"},))


def _text_rules() -> Case:
    """Sentinels, regex, every whitespace mode, case, a value map, and a normalized key."""
    source, target = _same(
        {
            "code": pl.String(),
            "status": pl.String(),
            "sku": pl.String(),
            "city": pl.String(),
            "first": pl.String(),
            "last": pl.String(),
            "score": pl.Int64(),
        }
    )
    diff = DiffConfig(
        primary_keys=["code"],
        pushdown_sample_rows=5,
        rules=[
            DiffRule(column_names=["code"], whitespace_mode="both", case_insensitive=True),
            DiffRule(
                column_names=["status"],
                null_values=["N/A", ""],
                value_map={"A": "Active", "I": "Inactive", "O'Brien": "Quoted"},
                case_insensitive=True,
            ),
            DiffRule(column_names=["sku"], regex_replace={"-": "", "([a-z]+)_([0-9]+)": "$2$1"}),
            DiffRule(column_names=["city"], whitespace_mode="both"),
            DiffRule(column_names=["first"], whitespace_mode="left"),
            DiffRule(column_names=["last"], whitespace_mode="right"),
            DiffRule(column_names=["score"], null_values=[-1]),
        ],
    )
    return Case("text_rules", diff, source, target)


def _casts() -> Case:
    """Zero padding and every `cast_to` target."""
    source = pl.Schema(
        {
            "id": pl.Int64(),
            "zip": pl.Int64(),
            "rounded": pl.Float64(),
            "ratio": pl.Int64(),
            "label": pl.Int64(),
            "flag": pl.Int64(),
            "day": pl.Datetime(),
            "stamp": pl.Date(),
        }
    )
    target = pl.Schema(
        {
            "id": pl.Int64(),
            "zip": pl.String(),
            "rounded": pl.Int64(),
            "ratio": pl.Float64(),
            "label": pl.String(),
            "flag": pl.Boolean(),
            "day": pl.Date(),
            "stamp": pl.Datetime(),
        }
    )
    diff = DiffConfig(
        primary_keys=["id"],
        rules=[
            DiffRule(column_names=["zip"], pad_zeros=5),
            DiffRule(column_names=["rounded"], cast_to="Int64"),
            DiffRule(column_names=["ratio"], cast_to="Float64"),
            DiffRule(column_names=["label"], cast_to="String"),
            DiffRule(column_names=["flag"], cast_to="Boolean"),
            DiffRule(column_names=["day"], cast_to="Date"),
            DiffRule(column_names=["stamp"], cast_to="Datetime"),
        ],
    )
    return Case("casts", diff, source, target)


def _datetime_formats() -> Case:
    """Text read as timestamps, with and without an offset."""
    source = pl.Schema({"id": pl.Int64(), "seen": pl.String(), "sent": pl.String()})
    target = pl.Schema(
        {"id": pl.Int64(), "seen": pl.Datetime(), "sent": pl.Datetime(time_zone="UTC")}
    )
    diff = DiffConfig(
        primary_keys=["id"],
        rules=[
            DiffRule(column_names=["seen"], datetime_format="%Y-%m-%d %H:%M:%S"),
            DiffRule(column_names=["sent"], datetime_format="%d/%m/%Y %H:%M %z"),
        ],
    )
    return Case("datetime_formats", diff, source, target)


def _type_drift() -> Case:
    """Wide integers, and columns `strict_types` fails, with and without null equality."""
    source = pl.Schema(
        {"id": pl.Int64(), "units": pl.Int64(), "kept": pl.Int64(), "forgiven": pl.Int64()}
    )
    target = pl.Schema(
        {"id": pl.Int64(), "units": pl.Int32(), "kept": pl.String(), "forgiven": pl.String()}
    )
    diff = DiffConfig(
        primary_keys=["id"],
        strict_types=True,
        pushdown_sample_rows=3,
        rules=[
            DiffRule(column_names=["units"], absolute_tolerance=2, relative_tolerance=0.1),
            DiffRule(column_names=["forgiven"], treat_null_as_equal=True),
        ],
    )
    return Case("type_drift", diff, source, target)


def _edit_distance() -> Case:
    """A Levenshtein limit on text."""
    source, target = _same({"id": pl.Int64(), "name": pl.String()})
    diff = DiffConfig(
        primary_keys=["id"],
        rules=[DiffRule(column_names=["name"], max_levenshtein_distance=2)],
    )
    return Case("edit_distance", diff, source, target)


def _renamed() -> Case:
    """A key and a compared column that the target stores under other names."""
    source = pl.Schema({"customer_id": pl.Int64(), "full_name": pl.String()})
    target = pl.Schema({"cust_id": pl.Int64(), "name": pl.String()})
    diff = DiffConfig(
        primary_keys=["cust_id"],
        pushdown_sample_rows=2,
        rules=[
            DiffRule(column_names=["customer_id"], rename_to="cust_id"),
            DiffRule(column_names=["full_name"], rename_to="name", case_insensitive=True),
        ],
    )
    return Case("renamed", diff, source, target)


def _baseline_keys() -> Case:
    """Accepted changed rows keyed by every type a baseline can name."""
    source, target = _same(
        {
            "id": pl.Int64(),
            "code": pl.String(),
            "day": pl.Date(),
            "at": pl.Datetime(),
            "on": pl.Boolean(),
            "total": pl.Float64(),
        }
    )
    diff = DiffConfig(primary_keys=["id", "code", "day", "at", "on"])
    keys: tuple[dict[str, object], ...] = (
        {"id": 1, "code": "O'Brien", "day": "2026-10-06", "at": "2026-10-06T09:30:00", "on": True},
        {"id": 2, "code": "C:\\path\\", "day": date(2026, 1, 2), "at": None, "on": False},
        {"id": None, "code": None, "day": None, "at": datetime(2026, 1, 2, 3, 4, 5), "on": None},
    )
    return Case("baseline_keys", diff, source, target, flags=keys)


def _value_maps() -> Case:
    """Value map proposals on a sample of rows, with a map already declared."""
    source, target = _same({"id": pl.Int64(), "status": pl.String(), "tier": pl.String()})
    diff = DiffConfig(
        primary_keys=["id"],
        rules=[DiffRule(column_names=["status"], value_map={"A": "Active"})],
    )
    return Case("value_maps", diff, source, target, value_maps=True, sample_fraction=0.25)


def _jaro_winkler() -> Case:
    """A similarity limit every dialect refuses."""
    source, target = _same({"id": pl.Int64(), "name": pl.String()})
    diff = DiffConfig(
        primary_keys=["id"],
        rules=[DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)],
    )
    return Case("jaro_winkler", diff, source, target)


CASES: Final[tuple[Case, ...]] = (
    _tolerances(),
    _text_rules(),
    _casts(),
    _datetime_formats(),
    _type_drift(),
    _edit_distance(),
    _renamed(),
    _baseline_keys(),
    _value_maps(),
    _jaro_winkler(),
)
"""Every case the golden files hold."""


class _ProbeSession(PushdownSession):
    """A session that answers schema probes and empty counts, and records every statement."""

    def __init__(self, dialect: SQLDialect, case: Case) -> None:
        """Hold the case's column types behind its two probe statements."""
        self.compiler = SQLPushdownCompiler(dialect)
        source, target = TABLES
        self._schemas = {
            self.compiler.compile_schema_probe_query(source): case.source,
            self.compiler.compile_schema_probe_query(target): case.target,
        }
        self.statements: list[tuple[PushdownQueryType, str]] = []

    def connect(self) -> None:
        """Connect to nothing."""

    def execute_pushdown(
        self, statement: str, query_type: PushdownQueryType = "mismatch"
    ) -> pl.LazyFrame:
        """Record the statement and return what a run would read before comparing."""
        self.statements.append((query_type, statement))
        if query_type == "schema":
            return pl.LazyFrame(schema=self._schemas[statement])
        if query_type == "value_maps":
            return pl.LazyFrame(schema=_VALUE_MAP_RESULT)
        return pl.LazyFrame({COUNT_ALIAS: [0]})


def escape(sql: str) -> str:
    """Write every whitespace character but the space, and every unprintable one, by code point.

    The statements hold raw tabs, line breaks, and Unicode spaces inside the
    whitespace literal, which an editor or git would otherwise rewrite.
    """
    return "".join(
        char
        if char == " " or (char.isprintable() and not char.isspace())
        else f"⟨U+{ord(char):04X}⟩"
        for char in sql
    )


def _statement(label: str, sql: str) -> list[str]:
    """Render one statement under its label."""
    return [f"-- statement: {label}", escape(sql), ""]


def _sample(label: str, query: SampleQuery) -> list[str]:
    """Render a statement whose positional aliases the engine renames."""
    renames = json.dumps(query.renames, ensure_ascii=False)
    return [f"-- statement: {label}", f"-- renames: {escape(renames)}", escape(query.statement), ""]


def _refused(label: str, exc: Exception) -> list[str]:
    """Render a refusal in place of the statement it stopped."""
    return [f"-- statement: {label}", f"-- refused: {type(exc).__name__}: {escape(str(exc))}", ""]


def _run_statements(case: Case, session: _ProbeSession) -> list[str]:
    """Plan and compile the case as a run does, then its flags statement."""
    try:
        plan = plan_pushdown(session, *TABLES, case.diff)
    except (ConfigError, ConnectorError) as exc:
        return _refused("plan", exc)
    lines = [line for _, sql in session.statements for line in _statement("probe", sql)]
    try:
        compiled = compile_pushdown(session.compiler, TABLES, case.diff, plan)
    except (ConfigError, ConnectorError) as exc:
        return [*lines, *_refused("compile", exc)]
    for side, sql in zip(("source", "target"), compiled.duplicates, strict=True):
        lines += _statement(f"duplicates {side}", sql)
    for side, sql in zip(("source", "target"), compiled.counts, strict=True):
        lines += _statement(f"count {side}", sql)
    lines += _statement("changed", compiled.changed)
    lines += _statement("added", compiled.added)
    lines += _statement("missing", compiled.missing)
    if compiled.columns is not None:
        lines += _statement("columns", compiled.columns)
    if compiled.sample is not None:
        lines += _sample("sample", compiled.sample)
    if case.flags:
        source_types, target_types, key_rules, rules = plan
        lines += _flags(case, session.compiler, (source_types, target_types), key_rules, rules)
    return lines


def _flags(
    case: Case,
    compiler: SQLPushdownCompiler,
    types: tuple[pl.Schema, pl.Schema],
    key_rules: list[DiffRule],
    rules: list[DiffRule],
) -> list[str]:
    """Compile the flags statement for the case's accepted changed rows."""
    source_types, target_types = types
    try:
        query = compiler.compile_changed_flags_query(
            *TABLES,
            case.diff.primary_keys,
            rules,
            case.flags,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
            wide_integers=wide_integer_columns(case.diff, rules, source_types, target_types),
            type_drift=type_drift_columns(case.diff, rules, source_types, target_types),
        )
    except (ConfigError, ConnectorError) as exc:
        return _refused("flags", exc)
    return [] if query is None else _sample("flags", query)


def _value_map_statement(case: Case, dialect: SQLDialect) -> list[str]:
    """Compile the value map proposal statement as `suggest` does."""
    session = _ProbeSession(dialect, case)
    try:
        collect_value_map_proposals(
            session,
            *TABLES,
            case.diff,
            min_confidence=0.9,
            min_support=5,
            sample_fraction=case.sample_fraction,
        )
    except (ConfigError, ConnectorError) as exc:
        return _refused("value_maps", exc)
    return [
        line
        for query_type, sql in session.statements
        if query_type == "value_maps"
        for line in _statement("value_maps", sql)
    ]


def render(case: Case, dialect: SQLDialect) -> str:
    """Return every statement the compiler writes for one case in one dialect."""
    lines = [f"-- case: {case.name}", f"-- dialect: {dialect.value}", ""]
    lines += _run_statements(case, _ProbeSession(dialect, case))
    if case.value_maps:
        lines += _value_map_statement(case, dialect)
    return "\n".join(lines)


def fixture(case: Case, dialect: SQLDialect) -> Path:
    """Return the golden file for one case in one dialect."""
    return FIXTURES / dialect.value / f"{case.name}.sql"


def main() -> int:
    """Rewrite every golden file from the compiler, and remove stale ones."""
    written = 0
    for dialect in SQLDialect:
        folder = FIXTURES / dialect.value
        folder.mkdir(parents=True, exist_ok=True)
        expected = {fixture(case, dialect) for case in CASES}
        for stale in set(folder.glob("*.sql")) - expected:
            stale.unlink()
        for case in CASES:
            fixture(case, dialect).write_text(render(case, dialect), encoding="utf-8")
            written += 1
    sys.stdout.write(f"Wrote {written} golden SQL files under {FIXTURES}.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
