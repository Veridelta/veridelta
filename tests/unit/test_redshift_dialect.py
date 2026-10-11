# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Redshift dialect.

Redshift speaks a Postgres-like SQL, but not Postgres: it has no `IS DISTINCT
FROM`, no `::` text casts for every type, no `hashtextextended`, and it turns
`TEXT` into `VARCHAR(256)`. It reads backslash escapes inside string literals,
ignores trailing spaces when it compares text, and can be created to ignore
case. No emulator runs it, so these tests pin the spellings the compiler
writes, and the live service checks them against a real cluster.
"""

import re
from typing import Final

import polars as pl
import pytest

from tests.unit.sql_corpus import CASES, Case, fixture
from veridelta.connectors.sql import (
    SERVER_CHECK_TEXT,
    SQLDialect,
    SQLPushdownCompiler,
    compile_redshift_server_check,
)
from veridelta.exceptions import ConfigError
from veridelta.models import DiffRule

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_EXACT: Final = "|| '|', 'case_sensitive')"
"""The end of every exact text operand."""


def _redshift() -> SQLPushdownCompiler:
    """Return a Redshift compiler."""
    return SQLPushdownCompiler(SQLDialect.REDSHIFT)


def _predicate(rule: DiffRule, dtype: pl.DataType | None = None) -> str:
    """Compile one column's match predicate with both sides of one stored type."""
    return _redshift().compile_column_predicate(
        rule, rule.column_names[0], source_dtype=dtype, target_dtype=dtype
    )


def _mismatch(rules: list[DiffRule], types: pl.Schema) -> str:
    """Compile the mismatch query for two tables of one schema, keyed on `id`."""
    return _redshift().compile_query(
        "s", "t", ["id"], rules, source_types=types, target_types=types
    )


class TestSpellings:
    """Validate the tokens Redshift spells its own way."""

    def test_it_spells_casts_with_redshift_type_names(self) -> None:
        """Ensure text casts never use TEXT, which Redshift turns into VARCHAR(256)."""
        spelled = {
            target: _predicate(DiffRule(column_names=["x"], cast_to=target), pl.Int64())
            for target in ("Int64", "Float64", "String", "Date", "Datetime")
        }

        assert "AS BIGINT)" in spelled["Int64"]
        assert "AS DOUBLE PRECISION)" in spelled["Float64"]
        assert "AS VARCHAR(MAX))" in spelled["String"]
        assert "AS DATE)" in spelled["Date"]
        assert "AS TIMESTAMP)" in spelled["Datetime"]

    def test_a_number_becomes_a_boolean_by_comparing_it_with_zero(self) -> None:
        """Ensure `cast_to: Boolean` never relies on a numeric cast Redshift may refuse."""
        sql = _predicate(DiffRule(column_names=["x"], cast_to="Boolean"), pl.Int64())

        assert sql == '("src"."x" <> 0) = ("tgt"."x" <> 0)'

    def test_it_doubles_backslashes_and_quotes_in_literals(self) -> None:
        """Ensure a backslash reaches Redshift as one, since it reads escapes in literals."""
        sql = _predicate(
            DiffRule(column_names=["path"], value_map={"C:\\dir": "O'Brien"}), pl.String()
        )

        assert "'C:\\\\dir'" in sql
        assert "'O''Brien'" in sql

    def test_a_tolerance_guards_infinity_with_a_double(self) -> None:
        """Ensure an infinite source never passes a tolerance, as in a local run."""
        sql = _predicate(DiffRule(column_names=["x"], absolute_tolerance=0.5), pl.Float64())

        assert "CAST('Infinity' AS DOUBLE PRECISION)" in sql

    def test_it_trims_with_the_standard_side_keywords(self) -> None:
        """Ensure each `whitespace_mode` names its side, as Redshift's TRIM documents."""
        for mode, side in (("left", "LEADING"), ("right", "TRAILING"), ("both", "BOTH")):
            rule = DiffRule.model_validate({"column_names": ["x"], "whitespace_mode": mode})

            assert f"TRIM({side} '" in _predicate(rule, pl.String())

    def test_it_replaces_every_match_with_pcre(self) -> None:
        """Ensure the pattern reads as PCRE, nearer Polars' regex, from the first character."""
        sql = _predicate(DiffRule(column_names=["x"], regex_replace={"-": ""}), pl.String())

        assert "REGEXP_REPLACE(\"src\".\"x\", '-', '', 1, 'p')" in sql


class TestExactText:
    """Validate that text compares as Polars compares it, case and trailing spaces counted."""

    _TYPES: Final = pl.Schema({"id": pl.Int64(), "name": pl.String()})

    def test_a_text_column_compares_under_the_case_sensitive_collation(self) -> None:
        """Ensure `a` and `A`, or `a ` and `a`, never match in a case-insensitive database."""
        sql = _mismatch([DiffRule(column_names=["name"])], self._TYPES)

        assert f'COLLATE("src"."name" {_EXACT}' in sql
        assert f'COLLATE("tgt"."name" {_EXACT}' in sql

    def test_two_nulls_match_without_is_distinct_from(self) -> None:
        """Ensure null-safe equality is spelled out, since Redshift has no IS DISTINCT FROM."""
        sql = _mismatch([DiffRule(column_names=["name"], treat_null_as_equal=True)], self._TYPES)

        assert '"src"."name" IS NULL AND "tgt"."name" IS NULL' in sql
        assert "DISTINCT FROM" not in sql

    def test_a_number_column_compares_as_stored(self) -> None:
        """Ensure only text takes the collation and the sentinel."""
        types = pl.Schema({"id": pl.Int64(), "qty": pl.Int64()})

        sql = _mismatch([DiffRule(column_names=["qty"])], types)

        assert "COLLATE(" not in sql


class TestSampling:
    """Validate how Redshift samples value map evidence by key."""

    def test_it_chains_fnv_hash_across_the_keys(self) -> None:
        """Ensure each key seeds the next one's hash, as FNV_HASH takes one value and a seed."""
        types = pl.Schema({"id": pl.Int64(), "k": pl.String(), "status": pl.String()})
        sql = _redshift().compile_value_map_query(
            "s",
            "t",
            ["id", "k"],
            [DiffRule(column_names=["status"])],
            min_support=5,
            sample_fraction=0.5,
            source_types=types,
            target_types=types,
        )

        assert sql is not None
        hashed = 'FNV_HASH("src"."k", FNV_HASH("src"."id"))'
        assert f"MOD(MOD({hashed}, 1000000) + 1000000, 1000000) < 500000" in sql


class TestStatementsRedshiftRuns:
    """Validate that no golden statement uses a Postgres spelling Redshift lacks.

    Redshift accepts much of Postgres' syntax, which makes a Postgres habit easy
    to miss. No CI service runs Redshift, so these spellings are kept out of
    every Redshift statement the golden files hold.
    """

    _FORBIDDEN: Final = (
        r"\bDISTINCT FROM\b",
        r"::",
        r"\bROW\(",
        r"\bhashtextextended\b",
        r"\bAS TEXT\b",
        r"\bN'",
        r"\bTOP \(",
        r"\blevenshtein\b",
        r"\bEDITDISTANCE\b",
        r"\btry_",
        r"\bSWITCHOFFSET\b",
    )

    @pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
    def test_no_statement_uses_a_spelling_redshift_lacks(self, case: Case) -> None:
        """Ensure each Redshift statement avoids every Postgres or T-SQL only spelling."""
        text = fixture(case, SQLDialect.REDSHIFT).read_text(encoding="utf-8")
        statements = [line for line in text.splitlines() if line and not line.startswith("--")]

        found = [
            (pattern, statement[:80])
            for statement in statements
            for pattern in self._FORBIDDEN
            if re.search(pattern, statement, re.IGNORECASE)
        ]

        assert found == []

    def test_the_server_check_uses_no_spelling_redshift_lacks(self) -> None:
        """Ensure the statement a session checks the server with runs on Redshift too."""
        check = compile_redshift_server_check()

        assert [
            pattern for pattern in self._FORBIDDEN if re.search(pattern, check, re.IGNORECASE)
        ] == []


class TestRefusals:
    """Validate what Redshift refuses before any statement runs, and why."""

    @pytest.mark.parametrize(
        ("rule", "match"),
        [
            pytest.param(
                DiffRule(column_names=["x"], datetime_format="%Y-%m-%d"),
                "TO_TIMESTAMP fails on a value it cannot read",
                id="datetime-format",
            ),
            pytest.param(
                DiffRule(column_names=["x"], max_levenshtein_distance=1),
                "no edit distance function",
                id="edit-distance",
            ),
            pytest.param(
                DiffRule(column_names=["x"], regex_replace={"(a)": "$1"}),
                "documents no way to refer to a group",
                id="group-reference",
            ),
        ],
    )
    def test_it_refuses_a_text_rule_it_cannot_run(self, rule: DiffRule, match: str) -> None:
        """Ensure the refusal names the reason and the way out."""
        with pytest.raises(ConfigError, match=match) as info:
            _predicate(rule, pl.String())

        assert "pushdown: false" in str(info.value)

    @pytest.mark.parametrize(
        "rule",
        [
            pytest.param(DiffRule(column_names=["x"], pad_zeros=4), id="pad-zeros"),
            pytest.param(DiffRule(column_names=["x"], cast_to="String"), id="cast-to-string"),
        ],
    )
    def test_it_refuses_to_write_a_float_as_text(self, rule: DiffRule) -> None:
        """Ensure two floats can never match because Redshift wrote both with 15 digits."""
        with pytest.raises(ConfigError, match="15 significant digits"):
            _predicate(rule, pl.Float64())


class TestServerCheck:
    """Validate the statement a Redshift pushdown session checks the server with."""

    def test_it_reads_back_a_literal_with_a_doubled_backslash(self) -> None:
        """Ensure the round trip fails if Redshift ever stops reading backslash escapes."""
        check = compile_redshift_server_check()

        assert check.startswith("SELECT 'O''Br\u00efen \\\\ \u4e2d \U0001f680' AS \"text\", ")
        assert "\\" in SERVER_CHECK_TEXT

    def test_it_asks_whether_nan_equals_nan(self) -> None:
        """Ensure a NaN on both sides matches, as a local run reads it."""
        nan = "CAST('NaN' AS DOUBLE PRECISION)"

        assert f"CASE WHEN {nan} = {nan} THEN 1 ELSE 0 END" in compile_redshift_server_check()

    def test_it_checks_case_trailing_spaces_and_each_trim_side(self) -> None:
        """Ensure each difference the default comparison hides, and each trim, is checked."""
        check = compile_redshift_server_check()

        assert f"COLLATE('a' {_EXACT} <> COLLATE('A' {_EXACT}" in check
        assert f"COLLATE('a ' {_EXACT} <> COLLATE('a' {_EXACT}" in check
        for side in ("LEADING", "TRAILING", "BOTH"):
            assert f"TRIM({side} '" in check
