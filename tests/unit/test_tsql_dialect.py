# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the T-SQL dialect, which SQL Server, Azure SQL, and Synapse share.

The dialect is written only in what Synapse dedicated SQL pools run, since a
`mssql://` connection cannot tell the three apart. These tests pin the
spellings that keep a pushdown verdict equal to a local one: brackets and `N''`
literals, no boolean values, `TOP` for a row limit, `COUNT_BIG`, and text
compared character for character under a binary collation with trailing spaces
counted.
"""

import re
from typing import Final

import polars as pl
import pytest

from tests.unit.sql_corpus import CASES, Case, fixture
from veridelta._resolution import fold_rule_defaults, normalized_dtype
from veridelta.connectors.sql import (
    SERVER_CHECK_TEXT,
    SQLDialect,
    SQLPushdownCompiler,
    compile_tsql_server_checks,
)
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffRule

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_EXACT: Final = "COLLATE Latin1_General_100_BIN2"
"""The collation every exact text comparison names."""


def _tsql() -> SQLPushdownCompiler:
    """Return a T-SQL compiler."""
    return SQLPushdownCompiler(SQLDialect.TSQL)


def _predicate(rule: DiffRule, dtype: pl.DataType | None = None) -> str:
    """Compile one column's match predicate with both sides of one stored type."""
    return _tsql().compile_column_predicate(
        rule, rule.column_names[0], source_dtype=dtype, target_dtype=dtype
    )


class TestSpellings:
    """Validate the tokens T-SQL spells its own way."""

    def test_it_spells_casts_with_tsql_type_names(self) -> None:
        """Ensure `cast_to` uses types every SQL Server and Synapse knows."""
        spelled = {
            target: _predicate(DiffRule(column_names=["x"], cast_to=target), pl.Int64())
            for target in ("Int64", "Float64", "String", "Boolean", "Date", "Datetime")
        }

        assert "AS BIGINT)" in spelled["Int64"]
        assert "AS FLOAT)" in spelled["Float64"]
        assert "AS NVARCHAR(MAX))" in spelled["String"]
        assert "AS BIT)" in spelled["Boolean"]
        assert "AS DATE)" in spelled["Date"]
        assert "AS DATETIME2)" in spelled["Datetime"]

    def test_it_quotes_names_in_brackets_and_literals_as_unicode(self) -> None:
        """Ensure names need no session setting and literals keep any character."""
        sql = _predicate(DiffRule(column_names=["city"], value_map={"Zürich": "ZH"}), pl.String())

        assert "[src].[city]" in sql
        assert "N'Zürich'" in sql
        assert '"' not in sql

    def test_it_truncates_floats_toward_zero_with_ceiling(self) -> None:
        """Ensure `cast_to: Int64` on a float uses T-SQL's CEILING, which has no CEIL."""
        sql = _predicate(DiffRule(column_names=["x"], cast_to="Int64"), pl.Float64())

        assert "CEILING([src].[x])" in sql
        assert "CEIL(" not in sql

    def test_a_number_becomes_a_bit_by_a_cast(self) -> None:
        """Ensure `cast_to: Boolean` casts a number to BIT, which reads any nonzero as 1."""
        sql = _predicate(DiffRule(column_names=["x"], cast_to="Boolean"), pl.Int64())

        assert "CAST([src].[x] AS BIT)" in sql
        assert "<> 0" not in sql

    def test_boolean_sentinels_are_written_as_bits(self) -> None:
        """Ensure `null_values: [true]` on a BIT column compares with 1, as T-SQL has no TRUE."""
        sql = _predicate(DiffRule(column_names=["x"], null_values=[True]), pl.Boolean())

        assert "IN (1)" in sql
        assert "TRUE" not in sql


class TestNoBooleanValues:
    """Validate statements that would project or coalesce a predicate elsewhere."""

    _RULES: Final = [DiffRule(column_names=["amount"], absolute_tolerance=0.5)]
    _TYPES: Final = pl.Schema({"id": pl.Int64(), "amount": pl.Float64()})

    def test_the_changed_condition_reads_each_predicate_through_case(self) -> None:
        """Ensure an unknown predicate counts as a mismatch without COALESCE(..., FALSE)."""
        sql = _tsql().compile_query(
            "s", "t", ["id"], self._RULES, source_types=self._TYPES, target_types=self._TYPES
        )

        assert "WHERE NOT ((CASE WHEN " in sql
        assert "THEN 1 ELSE 0 END = 1)" in sql
        assert "COALESCE(" not in sql
        assert "FALSE" not in sql

    def test_the_sample_projects_flags_as_bits_and_limits_with_top(self) -> None:
        """Ensure a sample's match flags read as booleans and its rows are limited first."""
        sample = _tsql().compile_changed_sample_query(
            "s", "t", ["id"], self._RULES, limit=7, source_types=self._TYPES
        )

        assert sample is not None
        assert " SELECT TOP (7) " in sample.statement
        assert "LIMIT" not in sample.statement
        assert "THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS [_veridelta_match_0]" in (
            sample.statement
        )

    def test_counts_and_tallies_are_wide(self) -> None:
        """Ensure no count or tally overflows T-SQL's INT past two billion rows."""
        compiler = _tsql()
        count = compiler.compile_count_query("s")
        duplicates = compiler.compile_duplicate_key_query("s", ["id"], is_source=True)
        tally = compiler.compile_column_mismatch_query("s", "t", ["id"], self._RULES)

        assert count.startswith("SELECT COUNT_BIG(*) AS ")
        assert "COUNT(*)" not in duplicates
        assert tally is not None
        assert "SUM(CAST(CASE WHEN " in tally
        assert "AS BIGINT)) AS [amount]" in tally

    def test_a_drifted_column_never_matches(self) -> None:
        """Ensure a column `strict_types` fails compiles to a predicate no row meets."""
        sql = _tsql().compile_query(
            "s",
            "t",
            ["id"],
            [DiffRule(column_names=["amount"])],
            type_drift=frozenset({"amount"}),
        )

        assert "(1 = 0)" in sql


class TestExactText:
    """Validate that text compares character for character, as a local run compares it.

    The default collation ignores case and accents, and `=` ignores trailing
    spaces under every collation, so plain `=` would call `"a"`, `"A"`, and
    `"a "` equal where Polars calls them three values.
    """

    _TYPES: Final = pl.Schema({"code": pl.String(), "name": pl.String(), "n": pl.Int64()})

    def test_text_columns_and_keys_compare_exactly_and_numbers_do_not(self) -> None:
        """Ensure the join, the text predicate, and nothing else, name the binary collation."""
        sql = _tsql().compile_query(
            "s",
            "t",
            ["code"],
            [DiffRule(column_names=["name"]), DiffRule(column_names=["n"])],
            source_types=self._TYPES,
            target_types=self._TYPES,
        )

        assert f"ON ([src].[code] + N'|') {_EXACT} = ([tgt].[code] + N'|') {_EXACT}" in sql
        assert f"([src].[name] + N'|') {_EXACT} = ([tgt].[name] + N'|') {_EXACT}" in sql
        assert "[src].[n] = [tgt].[n]" in sql

    def test_stored_text_enters_as_unicode_under_a_binary_collation(self) -> None:
        """Ensure trimming, lowercasing, and sentinels match characters by code point."""
        sql = _tsql().compile_query(
            "s",
            "t",
            ["n"],
            [DiffRule(column_names=["name"], whitespace_mode="both")],
            source_types=self._TYPES,
            target_types=self._TYPES,
        )

        assert "TRIM(N'" in sql
        assert f"FROM CAST([src].[name] AS NVARCHAR(MAX)) {_EXACT})" in sql

    def test_sentinels_and_value_maps_compare_exactly(self) -> None:
        """Ensure `"N/A "` is not a sentinel `"N/A"`, and `"a"` is not a map key `"A"`."""
        sql = _predicate(
            DiffRule(column_names=["name"], null_values=["N/A"], value_map={"A": "Active"}),
            pl.String(),
        )

        assert f"IN ((N'N/A' + N'|') {_EXACT})" in sql
        assert f"= (N'A' + N'|') {_EXACT} THEN N'Active'" in sql

    def test_duplicate_keys_group_exactly(self) -> None:
        """Ensure `"a"` and `"A "` are two keys, as they are locally."""
        sql = _tsql().compile_duplicate_key_query(
            "s", ["code", "n"], is_source=True, types=self._TYPES
        )

        assert f"GROUP BY ([code] + N'|') {_EXACT}, [n] HAVING" in sql

    def test_an_unprobed_side_compares_as_stored(self) -> None:
        """Ensure text is only wrapped where both sides are known to hold it."""
        sql = _tsql().compile_query("s", "t", ["code"], [DiffRule(column_names=["name"])])

        assert _EXACT not in sql


class TestStatementsSynapseRuns:
    """Validate that the dialect stays inside what a Synapse dedicated SQL pool runs.

    CI checks the dialect on SQL Server 2022, which accepts more than Synapse.
    These spellings are refused by Synapse, so none may appear in any T-SQL
    statement the golden files hold.
    """

    _FORBIDDEN: Final = (
        r"\bDISTINCT FROM\b",
        r"\bREGEXP_",
        r"\bEDIT_DISTANCE\b",
        r"\bLIMIT\b",
        r"\bOFFSET\b",
        r"\bFETCH\b",
        r"\bTRUE\b",
        r"\bFALSE\b",
        r"\|\|",
        r"\bLPAD\b",
        r"\bLEADING\b",
        r"\bTRAILING\b",
        r"\bBOTH\b",
        r"\bTRANSLATE\b",
        r"\bCHOOSE\b",
        r"\bPARSE\(",
        r"\bSTRING_ESCAPE\b",
        r"::",
        r"\bCOUNT\(\*\)",
    )

    @pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
    def test_no_statement_uses_a_spelling_synapse_refuses(self, case: Case) -> None:
        """Ensure each T-SQL statement avoids every spelling Synapse lacks."""
        text = fixture(case, SQLDialect.TSQL).read_text(encoding="utf-8")
        statements = [line for line in text.splitlines() if line and not line.startswith("--")]

        found = [
            (pattern, statement[:80])
            for statement in statements
            for pattern in self._FORBIDDEN
            if re.search(pattern, statement)
        ]

        assert found == []

    def test_the_server_checks_use_no_spelling_synapse_refuses(self) -> None:
        """Ensure the statements a session checks the server with also run on Synapse."""
        found = [
            pattern
            for statement in compile_tsql_server_checks()
            for pattern in self._FORBIDDEN
            if re.search(pattern, statement)
        ]

        assert found == []


class TestServerChecks:
    """Validate the statements a SQL Server pushdown session checks the server with."""

    def test_the_settings_check_reads_back_a_literal_the_dialect_writes(self) -> None:
        """Ensure the literal holds a quote, a backslash, and text outside Latin-1, after N."""
        settings, _ = compile_tsql_server_checks()

        assert settings.startswith("SELECT N'O''Br\u00efen \\ \u4e2d \U0001f680' AS [text], ")
        assert "'" in SERVER_CHECK_TEXT
        assert "\\" in SERVER_CHECK_TEXT

    def test_the_settings_check_appends_the_sentinel_to_a_null(self) -> None:
        """Ensure the NULL check runs the exact text wrapper every comparison uses."""
        settings, _ = compile_tsql_server_checks()

        assert f"CASE WHEN (NULL + N'|') {_EXACT} IS NULL THEN 1 ELSE 0 END" in settings

    def test_the_settings_check_compares_case_accents_and_trailing_spaces(self) -> None:
        """Ensure exact text must tell apart each difference the default collation hides."""
        settings, _ = compile_tsql_server_checks()

        for left, right in (("a", "A"), ("e", "\u00e9"), ("a ", "a")):
            entries = [
                f"(CAST(N'{text}' AS NVARCHAR(MAX)) {_EXACT} + N'|') {_EXACT}"
                for text in (left, right)
            ]
            assert " <> ".join(entries) in settings

    def test_the_trim_check_strips_with_each_form_the_dialect_writes(self) -> None:
        """Ensure the trim check runs the functions a `whitespace_mode` compiles to."""
        _, trims = compile_tsql_server_checks()

        for rule, function in (
            (DiffRule(column_names=["x"], whitespace_mode="left"), "LTRIM("),
            (DiffRule(column_names=["x"], whitespace_mode="right"), "RTRIM("),
            (DiffRule(column_names=["x"], whitespace_mode="both"), "TRIM(N'"),
        ):
            assert function in _predicate(rule)
            assert function in trims
        assert trims.endswith(" THEN 1 ELSE 0 END AS [trims]")


class TestRefusals:
    """Validate what T-SQL refuses before any statement runs, and why."""

    @pytest.mark.parametrize(
        ("rule", "reason"),
        [
            pytest.param(
                DiffRule(column_names=["x"], regex_replace={"-": ""}),
                "no REGEXP_REPLACE",
                id="regex",
            ),
            pytest.param(
                DiffRule(column_names=["x"], datetime_format="%Y-%m-%d"),
                "not a format string",
                id="datetime-format",
            ),
            pytest.param(
                DiffRule(column_names=["x"], max_levenshtein_distance=1),
                "no edit distance function",
                id="edit-distance",
            ),
        ],
    )
    def test_it_refuses_a_setting_it_cannot_run_on_text(self, rule: DiffRule, reason: str) -> None:
        """Ensure each setting T-SQL cannot express is refused with its reason."""
        with pytest.raises(ConfigError, match=reason):
            _predicate(rule, pl.String())

    @pytest.mark.parametrize(
        "rule",
        [
            pytest.param(DiffRule(column_names=["x"], pad_zeros=5), id="pad-zeros"),
            pytest.param(DiffRule(column_names=["x"], cast_to="String"), id="cast-to-string"),
        ],
    )
    def test_it_refuses_to_write_a_float_as_text(self, rule: DiffRule) -> None:
        """Ensure a float is not turned into text with six digits, which could match two numbers."""
        with pytest.raises(ConfigError, match="six significant digits"):
            _predicate(rule, pl.Float64())

    @pytest.mark.parametrize("dtype", [pl.Int64(), pl.Decimal(10, 2), pl.String()])
    def test_it_writes_other_numbers_and_text_as_text(self, dtype: pl.DataType) -> None:
        """Ensure only floats are refused, since integers and decimals become exact text."""
        assert "NVARCHAR(MAX)" in _predicate(DiffRule(column_names=["x"], pad_zeros=5), dtype)


class TestPaddingAndTimestamps:
    """Validate T-SQL forms of zero padding and timestamps with a zone."""

    def test_it_pads_zeros_without_lpad_and_counts_trailing_spaces(self) -> None:
        """Ensure padding keeps a sign first and measures text by DATALENGTH, not LEN."""
        sql = _predicate(DiffRule(column_names=["zip"], pad_zeros=5), pl.Int64())

        assert "DATALENGTH(CAST([src].[zip] AS NVARCHAR(MAX))) / 2 >= 5" in sql
        assert "RIGHT(REPLICATE(N'0', 5) + CAST([src].[zip] AS NVARCHAR(MAX)), 5)" in sql
        assert "RIGHT(REPLICATE(N'0', 4) + SUBSTRING(" in sql
        assert "LEN(" not in sql

    def test_a_timestamp_with_a_zone_is_read_at_offset_zero(self) -> None:
        """Ensure keys and samples come back as instants, since ConnectorX doubles the offset."""
        aware = pl.Schema({"at": pl.Datetime(time_zone="UTC"), "n": pl.Int64()})
        naive = pl.Schema({"at": pl.Datetime(), "n": pl.Int64()})

        zoned = _tsql().compile_query(
            "s", "t", ["at"], [DiffRule(column_names=["n"])], source_types=aware, target_types=aware
        )
        plain = _tsql().compile_query(
            "s", "t", ["at"], [DiffRule(column_names=["n"])], source_types=naive, target_types=naive
        )

        assert "SWITCHOFFSET([src].[at], '+00:00') AS [at]" in zoned
        assert "SWITCHOFFSET" not in plain

    def test_a_tolerance_needs_no_infinity_guard(self) -> None:
        """Ensure a T-SQL FLOAT, which holds no infinity, compares without one."""
        sql = _predicate(DiffRule(column_names=["x"], absolute_tolerance=0.5), pl.Float64())

        assert "inf" not in sql.lower()
        assert "ABS([tgt].[x] - [src].[x]) <= 0.5" in sql

    def test_a_value_map_sample_hashes_keys_with_sha_256(self) -> None:
        """Ensure a sampled value map keeps a non-negative bucket of a hash that mixes keys.

        BINARY_CHECKSUM leaves a small integer nearly as it is, so it would read
        the lowest keys rather than a share spread across all of them.
        """
        types = pl.Schema({"id": pl.Int64(), "status": pl.String()})
        sql = _tsql().compile_value_map_query(
            "s",
            "t",
            ["id"],
            [DiffRule(column_names=["status"])],
            min_support=5,
            sample_fraction=0.5,
            source_types=types,
            target_types=types,
        )

        assert sql is not None
        hashed = "CAST(SUBSTRING(HASHBYTES('SHA2_256', CONCAT([src].[id], N'|')), 1, 4) AS INT)"
        assert f"(({hashed} % 1000000) + 1000000) % 1000000 < 500000" in sql
        assert "CHECKSUM" not in sql


class TestTextPrediction:
    """Validate that the compiler and the local engine agree on which columns hold text."""

    _RULES: Final = [
        DiffRule(column_names=["x"]),
        DiffRule(column_names=["x"], pad_zeros=4),
        DiffRule(column_names=["x"], datetime_format="%Y-%m-%d"),
        DiffRule(column_names=["x"], pad_zeros=4, datetime_format="%Y%m%d"),
        *(
            DiffRule(column_names=["x"], cast_to=target)
            for target in ("Int64", "Float64", "String", "Boolean", "Date", "Datetime")
        ),
        DiffRule(column_names=["x"], pad_zeros=2, cast_to="Int64"),
    ]
    _DTYPES: Final = [
        pl.String(),
        pl.Int64(),
        pl.Float64(),
        pl.Decimal(10, 2),
        pl.Boolean(),
        pl.Date(),
        pl.Datetime(),
        pl.Categorical(),
    ]

    @pytest.mark.parametrize("dtype", _DTYPES, ids=str)
    @pytest.mark.parametrize(
        "rule", _RULES, ids=lambda rule: repr(rule.model_dump(exclude_none=True))
    )
    def test_it_predicts_text_as_normalized_dtype_does(
        self, rule: DiffRule, dtype: pl.DataType
    ) -> None:
        """Ensure an exact comparison wraps exactly the columns a local run compares as text."""
        effective = fold_rule_defaults(rule, DiffConfig(primary_keys=["id"]))
        local = isinstance(normalized_dtype(effective, dtype), pl.String)

        predicted = SQLPushdownCompiler._output_is_text(rule, dtype)  # pyright: ignore[reportPrivateUsage]

        assert predicted == local

    def test_a_key_without_a_rule_is_text_when_stored_as_text(self) -> None:
        """Ensure a key compared as stored is text exactly when its column is."""
        is_text = SQLPushdownCompiler._output_is_text  # pyright: ignore[reportPrivateUsage]

        assert is_text(None, pl.String())
        assert not is_text(None, pl.Int64())
        assert not is_text(None, None)


@pytest.mark.parametrize("dialect", list(SQLDialect), ids=lambda dialect: dialect.value)
@pytest.mark.parametrize(
    ("rule", "dtype"),
    [
        pytest.param(DiffRule(column_names=["x"], datetime_format="%Y"), pl.String(), id="fmt"),
        pytest.param(DiffRule(column_names=["x"], regex_replace={"a": "b"}), pl.String(), id="re"),
        pytest.param(
            DiffRule(column_names=["x"], max_levenshtein_distance=1), pl.String(), id="edit"
        ),
        *(
            pytest.param(DiffRule(column_names=["x"], whitespace_mode=mode), pl.String(), id=mode)
            for mode in ("left", "right", "both")
        ),
        pytest.param(DiffRule(column_names=["x"], pad_zeros=3), pl.Float64(), id="pad-float"),
        pytest.param(DiffRule(column_names=["x"], cast_to="String"), pl.Float64(), id="text-float"),
    ],
)
def test_every_dialect_compiles_or_refuses_a_setting_never_crashes(
    dialect: SQLDialect, rule: DiffRule, dtype: pl.DataType
) -> None:
    """Ensure a setting a dialect lacks is a ConfigError `validate` can report, never a KeyError."""
    compiler = SQLPushdownCompiler(dialect)
    try:
        sql = compiler.compile_column_predicate(rule, "x", source_dtype=dtype, target_dtype=dtype)
    except ConfigError:
        return
    assert sql
