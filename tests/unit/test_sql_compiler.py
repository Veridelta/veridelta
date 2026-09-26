# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for dialect-specific SQL pushdown compilation."""

import polars as pl
import pytest

from veridelta.connectors import SQLDialect, SQLPushdownCompiler
from veridelta.connectors.sql import (
    _EDIT_DISTANCE_FUNCTIONS,
    _LITERAL_ESCAPES,
    COUNT_ALIAS,
    SCHEMA_ALIAS,
)
from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import DiffRule

_BACKSLASH_ESCAPE_DIALECTS = frozenset({SQLDialect.SNOWFLAKE, SQLDialect.DATABRICKS})
"""Dialects whose single-quoted strings read backslash escape sequences."""


def _read_literal(sql: str, dialect: SQLDialect) -> tuple[str, str]:
    """Lex one single-quoted literal off the front of `sql` the way `dialect` does.

    A reference model of the vendor rules the compiler has to satisfy, kept
    independent of the code under test. Snowflake and Databricks treat a
    backslash inside quotes as an escape character. Snowflake and DuckDB read a
    doubled quote as one quote; Databricks reads it as the end of one literal
    and the start of the next, which it then concatenates, so the quote is lost.

    Args:
        sql (str): SQL text starting with an opening quote.
        dialect (SQLDialect): Dialect whose lexing rules apply.

    Returns:
        tuple[str, str]: The decoded value and the SQL after its closing quote.
    """
    assert sql.startswith("'")
    decoded: list[str] = []
    index = 1
    while index < len(sql):
        char = sql[index]
        if char == "\\" and dialect in _BACKSLASH_ESCAPE_DIALECTS:
            decoded.append(sql[index + 1])
            index += 2
        elif char == "'":
            if dialect is not SQLDialect.DATABRICKS and sql[index + 1 : index + 2] == "'":
                decoded.append("'")
                index += 2
            else:
                return "".join(decoded), sql[index + 1 :]
        else:
            decoded.append(char)
            index += 1
    raise AssertionError(f"Literal never closed: {sql}")


def _snowflake() -> SQLPushdownCompiler:
    """Return a Snowflake-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.SNOWFLAKE)


def _databricks() -> SQLPushdownCompiler:
    """Return a Databricks-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.DATABRICKS)


def _duckdb() -> SQLPushdownCompiler:
    """Return a DuckDB-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.DUCKDB)


@pytest.mark.unit
@pytest.mark.fast
class TestSQLDialect:
    """Validate the warehouse dialect enum."""

    def test_it_exposes_the_supported_dialect_values(self) -> None:
        """Ensure the enum is a closed set of compilable dialects."""
        assert SQLDialect.SNOWFLAKE.value == "snowflake"
        assert SQLDialect.DATABRICKS.value == "databricks"
        assert SQLDialect.DUCKDB.value == "duckdb"
        assert {member.value for member in SQLDialect} == {
            "snowflake",
            "databricks",
            "duckdb",
        }


@pytest.mark.unit
@pytest.mark.fast
class TestIdentifierQuoting:
    """Validate identifier quoting and literal escaping."""

    def test_it_quotes_snowflake_identifiers_with_double_quotes(self) -> None:
        """Ensure Snowflake column refs use double quotes."""
        sql = _snowflake().compile_column_predicate(DiffRule(column_names=["amount"]), "amount")
        assert '"src"."amount"' in sql
        assert '"tgt"."amount"' in sql

    def test_it_quotes_databricks_identifiers_with_backticks(self) -> None:
        """Ensure Databricks column refs use backticks."""
        sql = _databricks().compile_column_predicate(DiffRule(column_names=["amount"]), "amount")
        assert "`src`.`amount`" in sql
        assert "`tgt`.`amount`" in sql

    def test_it_escapes_apostrophes_in_maps_sentinels_and_regex(self) -> None:
        """Ensure every literal escapes an apostrophe the way its dialect reads it.

        Snowflake and DuckDB double it. Databricks cannot: it concatenates
        adjacent literals, so `'O''Brien'` would reach it as `OBrien`.
        """
        rule = DiffRule(
            column_names=["status"],
            value_map={"O'Brien": "OB"},
            null_values=["N/A'"],
            regex_replace={"x'": "y'"},
        )
        snowflake = _snowflake().compile_column_predicate(rule, "status")
        databricks = _databricks().compile_column_predicate(rule, "status")
        duckdb = _duckdb().compile_column_predicate(rule, "status")

        for sql in (snowflake, duckdb):
            assert "O''Brien" in sql
            assert "N/A''" in sql
            assert "x''" in sql
            assert "y''" in sql
        assert r"O\'Brien" in databricks
        assert r"N/A\'" in databricks
        assert r"x\'" in databricks
        assert r"y\'" in databricks
        assert "O''Brien" not in databricks


@pytest.mark.unit
@pytest.mark.fast
class TestStringLiteralEscaping:
    """Validate that configured text survives the trip into a SQL literal.

    The differential harness executes DuckDB, whose strings follow the SQL
    standard, so it cannot observe how Snowflake or Databricks read a literal.
    These tests check the emitted text against `_read_literal`, a reference
    model of each vendor's rules.
    """

    def test_it_defines_escapes_for_every_dialect(self) -> None:
        """Ensure a new dialect cannot inherit another dialect's escaping."""
        assert set(_LITERAL_ESCAPES) == set(SQLDialect)

    @pytest.mark.parametrize("dialect", list(SQLDialect), ids=lambda d: d.value)
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("O'Brien", id="apostrophe"),
            pytest.param("C:\\", id="trailing-backslash"),
            pytest.param("x\\' OR 1=1 --", id="escaped-quote-injection"),
            pytest.param("\\N", id="mysql-null-marker"),
            pytest.param("\\d+\\.\\d{2}", id="regex-classes"),
            pytest.param("''", id="doubled-quotes"),
            pytest.param("\\\\server\\share", id="unc-path"),
        ],
    )
    def test_it_round_trips_text_through_a_literal(self, dialect: SQLDialect, value: str) -> None:
        """Ensure the dialect decodes the literal back to the configured text.

        The literal must also end exactly at its closing quote. A value that
        escapes the closing quote would carry the rest of the statement into
        the string, or configuration text out of it.
        """
        literal = SQLPushdownCompiler(dialect)._literal(value)  # pyright: ignore[reportPrivateUsage]

        decoded, remainder = _read_literal(literal, dialect)

        assert decoded == value
        assert remainder == ""

    def test_it_doubles_backslashes_for_warehouses_that_unescape_them(self) -> None:
        r"""Ensure a regex class, a sentinel, and a map key reach the warehouse intact.

        Unescaped, `\d` arrives in Snowflake or Databricks as `d`, and `\N`
        as `N`, so the rule silently matches something else.
        """
        rule = DiffRule(
            column_names=["code"],
            regex_replace={"\\d+": ""},
            null_values=["\\N"],
            value_map={"C:\\": "root"},
        )

        for compiler in (_snowflake(), _databricks()):
            sql = compiler.compile_column_predicate(rule, "code")
            assert r"'\\d+'" in sql
            assert r"'\\N'" in sql
            assert r"'C:\\'" in sql

        duckdb = _duckdb().compile_column_predicate(rule, "code")
        assert r"'\d+'" in duckdb
        assert r"'\N'" in duckdb
        assert r"'C:\'" in duckdb


@pytest.mark.unit
@pytest.mark.fast
class TestPredicateCompilation:
    """Validate DiffRule field translation into SQL fragments."""

    def test_it_emits_nested_regexp_replace_on_both_sides(self) -> None:
        """Ensure regex sanitization uses REGEXP_REPLACE in rule order."""
        rule = DiffRule(
            column_names=["amount"],
            regex_replace={"[^0-9.]": "", "^\\$": ""},
        )
        sql = _snowflake().compile_column_predicate(rule, "amount")
        assert sql.count("REGEXP_REPLACE(") == 4
        assert "'[^0-9.]'" in sql
        # Snowflake unescapes backslashes inside quotes, so the regex needs two.
        assert r"'^\\$'" in sql

    def test_it_emits_snowflake_iff_for_value_map(self) -> None:
        """Ensure Snowflake value maps nest IFF expressions on the source side."""
        rule = DiffRule(column_names=["status"], value_map={"M": "Male", "F": "Female"})
        sql = _snowflake().compile_column_predicate(rule, "status")
        assert "IFF(" in sql
        assert "CASE " not in sql
        assert "'M'" in sql
        assert "'Male'" in sql
        assert '"tgt"."status"' in sql
        assert sql.count("IFF(") == 2

    def test_it_emits_databricks_case_for_value_map(self) -> None:
        """Ensure Databricks value maps use CASE WHEN on the source side."""
        rule = DiffRule(column_names=["status"], value_map={"M": "Male", "F": "Female"})
        sql = _databricks().compile_column_predicate(rule, "status")
        assert "CASE " in sql
        assert "WHEN " in sql
        assert "IFF(" not in sql
        assert "`tgt`.`status`" in sql

    def test_it_nulls_sentinel_values_with_a_single_case_per_side(self) -> None:
        """Ensure sentinels collapse into one IN list on each side."""
        rule = DiffRule(column_names=["status"], null_values=["N/A", "-999"])
        sql = _snowflake().compile_column_predicate(rule, "status")
        assert sql.count("IN ('N/A', '-999') THEN NULL") == 2
        assert "NULLIF(" not in sql

    def test_it_emits_numeric_and_boolean_sentinels_unquoted(self) -> None:
        """Ensure only text sentinels are quoted, since quoting breaks casts."""
        sql = _snowflake().compile_column_predicate(
            DiffRule(column_names=["amount"], null_values=[-999, 0.5]), "amount"
        )
        assert "IN (-999, 0.5)" in sql
        assert "'-999'" not in sql

        flag_sql = _snowflake().compile_column_predicate(
            DiffRule(column_names=["flag"], null_values=[False, True]), "flag"
        )
        assert "IN (FALSE, TRUE)" in flag_sql

    def test_it_drops_sentinels_the_probed_column_cannot_hold(self) -> None:
        """Ensure a mixed list is narrowed to the sentinels each dtype accepts."""
        rule = DiffRule(column_names=["amount"], null_values=["N/A", -999, False])
        sql = _snowflake().compile_column_predicate(
            rule, "amount", source_dtype=pl.Float64(), target_dtype=pl.String()
        )
        assert 'CASE WHEN "src"."amount" IN (-999) THEN NULL' in sql
        assert 'CASE WHEN "tgt"."amount" IN (\'N/A\') THEN NULL' in sql
        assert "FALSE)" not in sql

    def test_it_skips_the_case_wrapper_when_no_sentinel_applies(self) -> None:
        """Ensure a column no sentinel fits compares as a bare identifier."""
        rule = DiffRule(column_names=["amount"], null_values=["N/A"])
        sql = _snowflake().compile_column_predicate(
            rule, "amount", source_dtype=pl.Int64(), target_dtype=pl.Int64()
        )
        assert "CASE" not in sql
        assert sql == '"src"."amount" = "tgt"."amount"'

    def test_it_filters_sentinels_per_dialect_in_aggregate_queries(self) -> None:
        """Ensure probed types reach predicates built by the query compilers."""
        rules = [DiffRule(column_names=["amount"], null_values=["N/A", -999])]
        schema = pl.Schema({"id": pl.Int64(), "amount": pl.Int64()})
        for compiler, quote in ((_snowflake(), '"'), (_databricks(), "`")):
            sql = compiler.compile_column_mismatch_query(
                "SRC", "TGT", ["id"], rules, source_types=schema, target_types=schema
            )
            assert sql is not None
            assert f"{quote}src{quote}.{quote}amount{quote} IN (-999)" in sql
            assert "'N/A'" not in sql

    def test_it_uses_equal_null_on_snowflake_when_nulls_match(self) -> None:
        """Ensure Snowflake exact equality uses EQUAL_NULL when requested."""
        rule = DiffRule(column_names=["status"], treat_null_as_equal=True)
        sql = _snowflake().compile_column_predicate(rule, "status")
        assert "EQUAL_NULL(" in sql
        assert "<=>" not in sql

    def test_it_uses_null_safe_eq_on_databricks_when_nulls_match(self) -> None:
        """Ensure Databricks exact equality uses <=> when requested."""
        rule = DiffRule(column_names=["status"], treat_null_as_equal=True)
        sql = _databricks().compile_column_predicate(rule, "status")
        assert "<=>" in sql
        assert "EQUAL_NULL(" not in sql

    def test_it_emits_engine_tolerance_formula(self) -> None:
        """Ensure numeric compare matches ABS(tgt-src) <= abs + rel * ABS(src).

        The allowance only applies to a finite source, and equal values match
        outright, so a NaN or an infinity can match nothing but itself.
        """
        rule = DiffRule(
            column_names=["amount"],
            absolute_tolerance=0.01,
            relative_tolerance=0.05,
        )
        sql = _snowflake().compile_column_predicate(rule, "amount")
        assert sql == (
            '("src"."amount" = "tgt"."amount" OR '
            """(ABS("src"."amount") < 'inf'::FLOAT AND """
            'ABS("tgt"."amount" - "src"."amount") <= 0.01 + (0.05 * ABS("src"."amount"))))'
        )

    @pytest.mark.parametrize(
        ("dialect", "guard"),
        [
            pytest.param(
                SQLDialect.SNOWFLAKE, """ABS("src"."amount") < 'inf'::FLOAT AND """, id="snowflake"
            ),
            pytest.param(
                SQLDialect.DATABRICKS,
                "ABS(`src`.`amount`) < CAST('Infinity' AS DOUBLE) AND ",
                id="databricks",
            ),
            pytest.param(
                SQLDialect.DUCKDB, """ABS("src"."amount") < 'inf'::DOUBLE AND """, id="duckdb"
            ),
        ],
    )
    def test_it_confines_the_tolerance_to_a_finite_source(
        self, dialect: SQLDialect, guard: str
    ) -> None:
        """Ensure each dialect spells infinity so a non-finite source never uses the allowance.

        `0 * ABS(inf)` is NaN, and every supported engine sorts NaN above all
        numbers, so without the guard `ABS(diff) <= NaN` accepted any target.
        """
        rule = DiffRule(column_names=["amount"], absolute_tolerance=0.5)

        sql = SQLPushdownCompiler(dialect).compile_column_predicate(rule, "amount")

        assert guard in sql

    def test_it_emits_exact_equality_when_tolerances_are_zero(self) -> None:
        """Ensure zero tolerances compile to `=` rather than ABS predicates."""
        rule = DiffRule(
            column_names=["amount"],
            absolute_tolerance=0.0,
            relative_tolerance=0.0,
        )
        sql = _snowflake().compile_column_predicate(rule, "amount")
        assert '"src"."amount" = "tgt"."amount"' in sql
        assert "ABS(" not in sql

    def test_it_disjoins_null_equality_with_numeric_tolerance(self) -> None:
        """Ensure null-safe numeric compare ORs both-null with the ABS predicate."""
        rule = DiffRule(
            column_names=["amount"],
            absolute_tolerance=0.1,
            treat_null_as_equal=True,
        )
        sql = _databricks().compile_column_predicate(rule, "amount")
        assert "IS NULL AND" in sql
        assert "ABS(" in sql
        assert "<=>" not in sql

    def test_it_applies_trim_and_lower_for_string_normalization(self) -> None:
        """Ensure whitespace and case flags emit TRIM/LTRIM/RTRIM and LOWER."""
        both = _snowflake().compile_column_predicate(
            DiffRule(column_names=["name"], whitespace_mode="both", case_insensitive=True),
            "name",
        )
        left = _snowflake().compile_column_predicate(
            DiffRule(column_names=["name"], whitespace_mode="left"),
            "name",
        )
        right = _snowflake().compile_column_predicate(
            DiffRule(column_names=["name"], whitespace_mode="right"),
            "name",
        )
        assert "TRIM(" in both
        assert "LOWER(" in both
        assert "LTRIM(" in left
        assert "RTRIM(" in right

    def test_it_uses_rename_to_as_the_target_identifier(self) -> None:
        """Ensure rename_to selects a different target column name."""
        rule = DiffRule(column_names=["user_id"], rename_to="customer_id")
        sql = _snowflake().compile_column_predicate(rule, "user_id", "customer_id")
        assert '"src"."user_id"' in sql
        assert '"tgt"."customer_id"' in sql
        assert '"tgt"."user_id"' not in sql


@pytest.mark.unit
@pytest.mark.fast
class TestQueryAssembly:
    """Validate SELECT / JOIN / WHERE assembly."""

    def test_it_builds_changed_row_query_with_two_keys_and_two_rules(self) -> None:
        """Ensure the full statement shape uses INNER JOIN and WHERE NOT."""
        rules = [
            DiffRule(column_names=["status"], treat_null_as_equal=True),
            DiffRule(column_names=["amount"], absolute_tolerance=0.01),
        ]
        sql = _snowflake().compile_query(
            "analytics.public.source_orders",
            "analytics.public.target_orders",
            ["id", "line_id"],
            rules,
        )
        assert sql.startswith('WITH "_src_normalized" AS (')
        assert 'FROM "analytics"."public"."source_orders" AS "src"' in sql
        assert 'FROM "analytics"."public"."target_orders" AS "tgt"' in sql
        assert 'INNER JOIN "_tgt_normalized" AS "tgt"' in sql
        assert 'ON "src"."id" = "tgt"."id" AND "src"."line_id" = "tgt"."line_id"' in sql
        assert "WHERE NOT (" in sql
        assert "EQUAL_NULL(" in sql
        assert "ABS(" in sql

    def test_it_omits_ignored_columns_from_the_where_clause(self) -> None:
        """Ensure ignore rules do not contribute match predicates."""
        sql = _snowflake().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [
                DiffRule(column_names=["notes"], ignore=True),
                DiffRule(column_names=["status"]),
            ],
        )
        assert '"src"."status"' in sql
        assert '"src"."notes"' not in sql
        assert "WHERE NOT" in sql

    def test_it_selects_no_rows_when_every_rule_is_ignored(self) -> None:
        """Ensure a comparison with no compared columns reports no changed rows.

        A bare inner join would return every shared key, which the engine
        would then count as drift even though nothing was compared.
        """
        sql = _databricks().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["notes"], ignore=True)],
        )
        assert "INNER JOIN" in sql
        assert sql.endswith("WHERE 1 = 0")
        assert "WHERE NOT" not in sql

    def test_it_coalesces_each_predicate_so_a_one_sided_null_is_a_mismatch(self) -> None:
        """Ensure `WHERE NOT` never sees a NULL predicate.

        `src = NULL` is NULL, not FALSE, and `NOT NULL` is still NULL, which
        `WHERE` drops. Without the coalesce a row where only one side is NULL
        would disappear from the changed set while the tally still counted it.
        """
        sql = _snowflake().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [
                DiffRule(column_names=["status"], treat_null_as_equal=False),
                DiffRule(column_names=["amount"], absolute_tolerance=0.5),
            ],
        )
        where_clause = sql.split("WHERE NOT (", 1)[1]
        assert where_clause.startswith('COALESCE("src"."status" = "tgt"."status", FALSE) AND ')
        assert 'COALESCE(("src"."amount" = "tgt"."amount" OR (ABS(' in where_clause
        assert where_clause.endswith(", FALSE))")

    def test_it_expands_multi_column_rules_into_separate_predicates(self) -> None:
        """Ensure one DiffRule with two names produces two match predicates."""
        sql = _snowflake().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["a", "b"])],
        )
        assert '"src"."a" = "tgt"."a"' in sql
        assert '"src"."b" = "tgt"."b"' in sql


@pytest.mark.unit
@pytest.mark.fast
class TestAntiJoinAssembly:
    """Validate LEFT/RIGHT JOIN SQL for added and removed warehouse rows."""

    def test_it_builds_snowflake_left_join_for_missing_source_rows(self) -> None:
        """Ensure removed rows use LEFT JOIN with target keys IS NULL."""
        sql = _snowflake().compile_missing_query("src_tbl", "tgt_tbl", ["id"])
        assert sql.startswith('WITH "_src_normalized" AS (SELECT "src"."id" AS "id" FROM')
        assert 'FROM "src_tbl" AS "src"' in sql
        assert 'FROM "tgt_tbl" AS "tgt"' in sql
        assert 'SELECT "src"."id" FROM "_src_normalized" AS "src"' in sql
        assert 'LEFT JOIN "_tgt_normalized" AS "tgt"' in sql
        assert 'ON "src"."id" = "tgt"."id"' in sql
        assert 'WHERE "tgt"."id" IS NULL' in sql
        assert "INNER JOIN" not in sql
        assert "RIGHT JOIN" not in sql

    def test_it_builds_snowflake_right_join_for_added_target_rows(self) -> None:
        """Ensure added rows use RIGHT JOIN with source keys IS NULL."""
        sql = _snowflake().compile_added_query("src_tbl", "tgt_tbl", ["id"])
        assert 'SELECT "tgt"."id" FROM "_src_normalized" AS "src"' in sql
        assert 'RIGHT JOIN "_tgt_normalized" AS "tgt"' in sql
        assert 'ON "src"."id" = "tgt"."id"' in sql
        assert 'WHERE "src"."id" IS NULL' in sql
        assert "LEFT JOIN" not in sql

    def test_it_quotes_databricks_anti_joins_with_backticks(self) -> None:
        """Ensure Databricks anti-joins use backtick identifiers."""
        missing = _databricks().compile_missing_query(
            "catalog.schema.src", "catalog.schema.tgt", ["id"]
        )
        added = _databricks().compile_added_query(
            "catalog.schema.src", "catalog.schema.tgt", ["id"]
        )
        assert "FROM `catalog`.`schema`.`tgt` AS `tgt`" in missing
        assert "LEFT JOIN `_tgt_normalized` AS `tgt`" in missing
        assert "WHERE `tgt`.`id` IS NULL" in missing
        assert "RIGHT JOIN `_tgt_normalized` AS `tgt`" in added
        assert "WHERE `src`.`id` IS NULL" in added
        assert "`src`.`id`" in missing
        assert "`tgt`.`id`" in added

    def test_it_ands_composite_keys_on_join_and_null_filters(self) -> None:
        """Ensure composite keys appear in ON and every missing-side IS NULL check."""
        missing = _snowflake().compile_missing_query(
            "analytics.public.src",
            "analytics.public.tgt",
            ["id", "line_id"],
        )
        added = _snowflake().compile_added_query(
            "analytics.public.src",
            "analytics.public.tgt",
            ["id", "line_id"],
        )
        on_clause = 'ON "src"."id" = "tgt"."id" AND "src"."line_id" = "tgt"."line_id"'
        assert on_clause in missing
        assert on_clause in added
        assert 'WHERE "tgt"."id" IS NULL AND "tgt"."line_id" IS NULL' in missing
        assert 'WHERE "src"."id" IS NULL AND "src"."line_id" IS NULL' in added
        assert 'SELECT "src"."id", "src"."line_id" FROM "_src_normalized"' in missing
        assert 'SELECT "tgt"."id", "tgt"."line_id" FROM "_src_normalized"' in added

    def test_it_rejects_empty_primary_keys_on_anti_joins(self) -> None:
        """Ensure anti-joins cannot be compiled without keys."""
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_missing_query("src_tbl", "tgt_tbl", [])
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_added_query("src_tbl", "tgt_tbl", [])


@pytest.mark.unit
@pytest.mark.fast
class TestKeyNormalization:
    """Validate that primary keys pass through stages 1-7 before any join.

    Keys used to be projected and joined as stored, so a key that differed
    only by whitespace, case, or padding split into one added and one removed
    row on the warehouse path while the local engine matched it.
    """

    _TEXT_TYPES: dict[str, pl.DataType] = {"id": pl.String(), "val": pl.String()}  # noqa: RUF012

    def test_it_normalizes_keys_in_both_ctes(self) -> None:
        """Ensure a key rule rewrites the key on both sides of the join."""
        sql = _duckdb().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["val"])],
            key_rules=[
                DiffRule(column_names=["id"], whitespace_mode="both", case_insensitive=True)
            ],
            source_types=self._TEXT_TYPES,
            target_types=self._TEXT_TYPES,
        )

        assert 'LOWER(TRIM("src"."id")) AS "id"' in sql
        assert 'LOWER(TRIM("tgt"."id")) AS "id"' in sql
        assert 'ON "src"."id" = "tgt"."id"' in sql

    def test_it_reads_a_renamed_key_under_its_stored_name(self) -> None:
        """Ensure a renamed key reads the old spelling on the source and joins on the new one."""
        sql = _snowflake().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["user_id"],
            [],
            key_rules=[DiffRule(column_names=["legacy_id"], rename_to="user_id")],
        )

        assert '"src"."legacy_id" AS "user_id"' in sql
        assert '"tgt"."user_id" AS "user_id"' in sql
        assert 'ON "src"."user_id" = "tgt"."user_id"' in sql

    def test_it_maps_key_values_on_the_source_side_only(self) -> None:
        """Ensure a key crosswalk rewrites legacy codes without touching the target."""
        sql = _duckdb().compile_missing_query(
            "src_tbl",
            "tgt_tbl",
            ["code"],
            key_rules=[DiffRule(column_names=["code"], value_map={"A": "Alpha"})],
        )
        source_cte, target_cte = sql.split('"_tgt_normalized" AS (', 1)

        assert "THEN 'Alpha'" in source_cte
        assert "THEN 'Alpha'" not in target_cte.split(")", 1)[0]

    def test_it_joins_normalized_keys_in_every_join_query(self) -> None:
        """Ensure added, removed, changed, and tallied rows all match on the same keys."""
        compiler = _snowflake()
        key_rules = [DiffRule(column_names=["id"], case_insensitive=True)]
        rules = [DiffRule(column_names=["val"])]

        statements = [
            compiler.compile_query("src_tbl", "tgt_tbl", ["id"], rules, key_rules=key_rules),
            compiler.compile_missing_query("src_tbl", "tgt_tbl", ["id"], key_rules=key_rules),
            compiler.compile_added_query("src_tbl", "tgt_tbl", ["id"], key_rules=key_rules),
            compiler.compile_column_mismatch_query(
                "src_tbl", "tgt_tbl", ["id"], rules, key_rules=key_rules
            ),
        ]

        for sql in statements:
            assert sql is not None
            assert 'LOWER("src"."id") AS "id"' in sql
            assert 'LOWER("tgt"."id") AS "id"' in sql

    def test_it_passes_a_key_through_when_no_rule_normalizes_it(self) -> None:
        """Ensure a key without a rule is projected bare, exactly as stored."""
        sql = _snowflake().compile_added_query("src_tbl", "tgt_tbl", ["id", "line_id"])

        assert '"src"."id" AS "id", "src"."line_id" AS "line_id"' in sql

    @pytest.mark.parametrize(
        ("key_rule", "message"),
        [
            pytest.param(DiffRule(column_names=["a", "b"]), "exactly one", id="two-columns"),
            pytest.param(DiffRule(column_names=["other"]), "not a primary key", id="not-a-key"),
        ],
    )
    def test_it_rejects_a_key_rule_it_cannot_place(self, key_rule: DiffRule, message: str) -> None:
        """Ensure a malformed key rule fails instead of silently leaving a key raw."""
        with pytest.raises(ConnectorError, match=message):
            _snowflake().compile_added_query("src_tbl", "tgt_tbl", ["id"], key_rules=[key_rule])


@pytest.mark.unit
@pytest.mark.fast
class TestDuplicateKeyQuery:
    """Validate the per-side check that keys are unique after normalization."""

    def test_it_counts_the_rows_in_every_duplicated_key_group(self) -> None:
        """Ensure the statement sums group sizes, matching the local duplicate count.

        The local engine reports `filter(is_duplicated()).height`: every row
        that shares its key with another. Summing each group's size over the
        groups larger than one is the same number.
        """
        sql = _snowflake().compile_duplicate_key_query("src_tbl", ["id"], is_source=True)

        assert sql == (
            'SELECT COALESCE(SUM("_veridelta_rows"), 0) AS "_veridelta_total" '
            'FROM (SELECT COUNT(*) AS "_veridelta_rows" '
            'FROM (SELECT "src"."id" AS "id" FROM "src_tbl" AS "src") AS "_veridelta_keys" '
            'GROUP BY "id" HAVING COUNT(*) > 1) AS "_veridelta_duplicates"'
        )

    def test_it_groups_composite_keys_with_dialect_quoting(self) -> None:
        """Ensure every key column participates and Databricks quotes with backticks."""
        sql = _databricks().compile_duplicate_key_query(
            "main.default.src", ["tenant", "id"], is_source=True
        )

        assert "GROUP BY `tenant`, `id` HAVING COUNT(*) > 1" in sql
        assert "FROM `main`.`default`.`src` AS `src`" in sql

    def test_it_groups_the_normalized_keys(self) -> None:
        """Ensure duplicates are judged after normalization, as the local engine does."""
        sql = _duckdb().compile_duplicate_key_query(
            "src_tbl",
            ["id"],
            is_source=True,
            key_rules=[DiffRule(column_names=["id"], case_insensitive=True)],
            types={"id": pl.String()},
        )

        assert 'SELECT LOWER("src"."id") AS "id" FROM "src_tbl" AS "src"' in sql

    def test_it_reads_the_target_under_its_own_spelling(self) -> None:
        """Ensure the target side reads the renamed key and skips the source-only crosswalk."""
        sql = _snowflake().compile_duplicate_key_query(
            "tgt_tbl",
            ["user_id"],
            is_source=False,
            key_rules=[
                DiffRule(column_names=["legacy_id"], rename_to="user_id", value_map={"A": "B"})
            ],
        )

        assert 'SELECT "tgt"."user_id" AS "user_id" FROM "tgt_tbl" AS "tgt"' in sql
        assert "legacy_id" not in sql

    def test_it_rejects_empty_primary_keys(self) -> None:
        """Ensure the check cannot be compiled without keys."""
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_duplicate_key_query("src_tbl", [], is_source=True)


@pytest.mark.unit
@pytest.mark.fast
class TestCountAndProbeAssembly:
    """Validate the row-count and column-probe statements backing DiffSummary."""

    def test_it_counts_snowflake_rows_under_a_stable_alias(self) -> None:
        """Ensure counts project a dialect-neutral alias for scalar extraction."""
        sql = _snowflake().compile_count_query("analytics.public.src")

        assert sql == 'SELECT COUNT(*) AS "_veridelta_total" FROM "analytics"."public"."src"'
        assert COUNT_ALIAS == "_veridelta_total"

    def test_it_counts_databricks_rows_with_backtick_quoting(self) -> None:
        """Ensure the count query honors the Databricks quoting style."""
        sql = _databricks().compile_count_query("main.default.tgt")

        assert sql == "SELECT COUNT(*) AS `_veridelta_total` FROM `main`.`default`.`tgt`"

    def test_it_probes_columns_without_scanning_rows(self) -> None:
        """Ensure schema probes select every column but filter all rows out."""
        snowflake_sql = _snowflake().compile_schema_probe_query("analytics.public.src")
        databricks_sql = _databricks().compile_schema_probe_query("main.default.src")

        assert snowflake_sql == 'SELECT * FROM "analytics"."public"."src" WHERE 1 = 0'
        assert databricks_sql == "SELECT * FROM `main`.`default`.`src` WHERE 1 = 0"

    def test_it_wraps_a_prior_statement_to_read_its_result_schema(self) -> None:
        """Ensure the LIMIT 0 wrapper is assembled by the compiler, not a connector."""
        inner = 'SELECT "src"."id" FROM "src_tbl" AS "src"'
        snowflake_sql = _snowflake().compile_result_schema_query(inner)
        databricks_sql = _databricks().compile_result_schema_query("SELECT `id` FROM `t`")

        assert snowflake_sql == f'SELECT * FROM ({inner}) AS "_veridelta_schema" LIMIT 0'
        assert (
            databricks_sql == "SELECT * FROM (SELECT `id` FROM `t`) AS `_veridelta_schema` LIMIT 0"
        )
        assert SCHEMA_ALIAS == "_veridelta_schema"

    def test_it_rejects_unsafe_relations_on_counts_and_probes(self) -> None:
        """Ensure the identifier allowlist covers the count and probe statements."""
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_count_query('src"; DROP TABLE t')
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_schema_probe_query("src tbl")
        with pytest.raises(ConnectorError, match="Table name"):
            _databricks().compile_count_query("  ")
        with pytest.raises(ConnectorError, match="three dotted segments"):
            _databricks().compile_schema_probe_query("a.b.c.d")


@pytest.mark.unit
@pytest.mark.fast
class TestColumnMismatchAggregate:
    """Validate the per-column tally backing DiffSummary.column_mismatches."""

    def test_it_sums_one_case_expression_per_column_for_snowflake(self) -> None:
        """Ensure a single-column tally compiles to an exact, quoted aggregate."""
        sql = _snowflake().compile_column_mismatch_query(
            "src_tbl", "tgt_tbl", ["id"], [DiffRule(column_names=["amount"])]
        )

        assert sql is not None
        assert sql.startswith('WITH "_src_normalized" AS (')
        assert (
            'SUM(CASE WHEN COALESCE("src"."amount" = "tgt"."amount", FALSE) '
            'THEN 0 ELSE 1 END) AS "amount"'
        ) in sql
        assert 'FROM "src_tbl" AS "src"' in sql
        assert 'FROM "tgt_tbl" AS "tgt"' in sql
        assert 'INNER JOIN "_tgt_normalized" AS "tgt"' in sql
        assert 'ON "src"."id" = "tgt"."id"' in sql

    def test_it_quotes_the_databricks_aggregate_with_backticks(self) -> None:
        """Ensure the tally honors the Databricks quoting style."""
        sql = _databricks().compile_column_mismatch_query(
            "main.default.src", "main.default.tgt", ["id"], [DiffRule(column_names=["amount"])]
        )

        assert sql is not None
        assert "COALESCE(`src`.`amount` = `tgt`.`amount`, FALSE)" in sql
        assert "AS `amount`" in sql
        assert "FROM `main`.`default`.`tgt` AS `tgt`" in sql
        assert "INNER JOIN `_tgt_normalized` AS `tgt`" in sql

    def test_it_coalesces_null_predicates_so_they_count_as_mismatches(self) -> None:
        """Ensure three-valued logic cannot silently score NULL comparisons as matches."""
        sql = _snowflake().compile_column_mismatch_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["amount"], absolute_tolerance=0.5)],
        )

        assert sql is not None
        assert sql.count("COALESCE(") == 1
        assert 'COALESCE(("src"."amount" = "tgt"."amount" OR (ABS(' in sql
        assert ", FALSE) THEN 0 ELSE 1 END)" in sql

    def test_it_aliases_renamed_columns_by_their_target_name(self) -> None:
        """Ensure the tally key matches the post-alignment name the local engine reports."""
        sql = _snowflake().compile_column_mismatch_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["legacy_amt"], rename_to="amount")],
        )

        assert sql is not None
        assert '"src"."legacy_amt" AS "amount"' in sql
        assert '"src"."amount" = "tgt"."amount"' in sql
        assert 'AS "legacy_amt"' not in sql

    def test_it_emits_one_term_per_column_across_composite_keys(self) -> None:
        """Ensure multi-column rules and composite keys expand correctly."""
        sql = _snowflake().compile_column_mismatch_query(
            "src_tbl",
            "tgt_tbl",
            ["id", "line_id"],
            [DiffRule(column_names=["a", "b"])],
        )

        assert sql is not None
        assert sql.count("SUM(CASE WHEN") == 2
        assert 'AS "a"' in sql
        assert 'AS "b"' in sql
        assert 'ON "src"."id" = "tgt"."id" AND "src"."line_id" = "tgt"."line_id"' in sql

    def test_it_keeps_the_first_rule_when_two_rules_name_one_column(self) -> None:
        """Ensure duplicate aliases cannot reach the select list, matching rule precedence."""
        sql = _snowflake().compile_column_mismatch_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [
                DiffRule(column_names=["amount"], absolute_tolerance=0.5),
                DiffRule(column_names=["amount"], case_insensitive=True),
            ],
        )

        assert sql is not None
        assert sql.count("SUM(CASE WHEN") == 1
        assert "ABS(" in sql
        assert "LOWER(" not in sql

    def test_it_returns_none_when_no_column_is_comparable(self) -> None:
        """Ensure an all-ignored or empty rule set skips the round trip entirely."""
        compiler = _snowflake()

        assert compiler.compile_column_mismatch_query("src_tbl", "tgt_tbl", ["id"], []) is None
        assert (
            compiler.compile_column_mismatch_query(
                "src_tbl", "tgt_tbl", ["id"], [DiffRule(column_names=["notes"], ignore=True)]
            )
            is None
        )

    def test_it_rejects_unsafe_identifiers_and_empty_keys(self) -> None:
        """Ensure the allowlist and key guard cover the aggregate statement too."""
        compiler = _snowflake()
        with pytest.raises(ConnectorError, match="primary key"):
            compiler.compile_column_mismatch_query(
                "src_tbl", "tgt_tbl", [], [DiffRule(column_names=["amount"])]
            )
        with pytest.raises(ConnectorError, match="identifier"):
            compiler.compile_column_mismatch_query(
                "src_tbl", "tgt_tbl", ["id"], [DiffRule(column_names=["amount; DROP TABLE t"])]
            )
        with pytest.raises(ConnectorError, match="identifier"):
            compiler.compile_column_mismatch_query(
                'src"; DROP', "tgt_tbl", ["id"], [DiffRule(column_names=["amount"])]
            )


@pytest.mark.unit
@pytest.mark.fast
class TestCompilerErrors:
    """Validate ConnectorError guards for unsupported or invalid input."""

    def test_it_rejects_empty_primary_keys(self) -> None:
        """Ensure joins cannot be compiled without keys."""
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_query("src_tbl", "tgt_tbl", [], [])

    def test_it_rejects_empty_table_names(self) -> None:
        """Ensure blank source or target relations raise ConnectorError."""
        with pytest.raises(ConnectorError, match="Table name"):
            _snowflake().compile_query(" ", "tgt_tbl", ["id"], [])
        with pytest.raises(ConnectorError, match="Table name"):
            _snowflake().compile_query("src_tbl", "", ["id"], [])

    def test_it_rejects_empty_column_identifiers(self) -> None:
        """Ensure blank column names cannot be quoted."""
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_column_predicate(DiffRule(), "  ")

    def test_it_rejects_sql_metacharacters_in_keys_and_tables(self) -> None:
        """Ensure injected SQL in identifiers fails closed before quoting."""
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_query("src_tbl", "tgt_tbl", ["id; DROP TABLE t"], [])
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_query('src"; DROP', "tgt_tbl", ["id"], [])
        with pytest.raises(ConnectorError, match="identifier"):
            _databricks().compile_missing_query("src_tbl", "tgt`x", ["id"])
        with pytest.raises(ConnectorError, match="identifier"):
            _snowflake().compile_added_query("src_tbl", "tgt_tbl", ["id dropped"])

    def test_it_rejects_relations_with_more_than_three_segments(self) -> None:
        """Ensure catalog.schema.table is the longest allowed relation path."""
        with pytest.raises(ConnectorError, match="three dotted segments"):
            _snowflake().compile_query("a.b.c.d", "tgt_tbl", ["id"], [])

    def test_it_rejects_pattern_only_rules(self) -> None:
        """Ensure pattern rules cannot expand without a resolved column list."""
        with pytest.raises(ConnectorError, match="Pattern-only"):
            _snowflake().compile_query(
                "src_tbl",
                "tgt_tbl",
                ["id"],
                [DiffRule(pattern="^AMT_")],
            )

    def test_it_rejects_rename_to_with_multiple_columns(self) -> None:
        """Ensure rename_to stays restricted to single-column rules."""
        with pytest.raises(ConnectorError, match="rename_to"):
            _snowflake().compile_query(
                "src_tbl",
                "tgt_tbl",
                ["id"],
                [DiffRule(column_names=["a", "b"], rename_to="c")],
            )

    def test_it_compiles_every_transform_stage(self) -> None:
        """Ensure no transform stage is rejected as unimplemented any more.

        The only setting the compiler refuses is `min_jaro_winkler_similarity`,
        which no warehouse computes the way a local run does.
        """
        compiler = _snowflake()
        rule = DiffRule(
            column_names=["ts"],
            pad_zeros=8,
            datetime_format="%Y-%m-%d",
            timezone="UTC",
            cast_to="Date",
        )

        sql = compiler.compile_column_predicate(rule, "ts")

        assert "LPAD" in sql
        assert "TRY_TO_TIMESTAMP" in sql
        assert "CAST" in sql


@pytest.mark.unit
@pytest.mark.fast
class TestCastCompilation:
    """Validate stage 7 across the dialect keyword table."""

    @pytest.mark.parametrize(
        ("target", "keyword"),
        [
            ("Int64", "BIGINT"),
            ("Float64", "FLOAT"),
            ("String", "VARCHAR"),
            ("Boolean", "BOOLEAN"),
            ("Date", "DATE"),
            ("Datetime", "TIMESTAMP_NTZ"),
        ],
    )
    def test_it_maps_snowflake_cast_keywords(self, target: str, keyword: str) -> None:
        """Ensure each cast target resolves to its Snowflake type name."""
        rule = DiffRule(column_names=["c"], cast_to=target)  # type: ignore[arg-type]

        sql = _snowflake().compile_column_predicate(rule, "c")

        assert f'CAST("src"."c" AS {keyword})' in sql

    @pytest.mark.parametrize(
        ("target", "keyword"),
        [
            ("Int64", "BIGINT"),
            ("Float64", "DOUBLE"),
            ("String", "STRING"),
            ("Boolean", "BOOLEAN"),
            ("Date", "DATE"),
            ("Datetime", "TIMESTAMP"),
        ],
    )
    def test_it_maps_databricks_cast_keywords(self, target: str, keyword: str) -> None:
        """Ensure each cast target resolves to its Databricks type name."""
        rule = DiffRule(column_names=["c"], cast_to=target)  # type: ignore[arg-type]

        sql = _databricks().compile_column_predicate(rule, "c")

        assert f"CAST(`src`.`c` AS {keyword})" in sql

    def test_it_truncates_a_float_bound_for_an_integer(self) -> None:
        """Ensure the float-to-integer cast truncates instead of rounding.

        Polars truncates toward zero where SQL rounds, so the guard has to be
        emitted whenever the probed dtype says a float reaches the cast.
        """
        rule = DiffRule(column_names=["n"], cast_to="Int64")

        sql = _snowflake().compile_column_predicate(
            rule, "n", source_dtype=pl.Float64(), target_dtype=pl.Float64()
        )

        assert 'CASE WHEN "src"."n" < 0 THEN CEIL("src"."n") ELSE FLOOR("src"."n") END' in sql

    def test_it_skips_truncation_when_an_earlier_stage_changed_the_type(self) -> None:
        """Ensure the float guard is dropped once stage 5 has stringified.

        The probed dtype describes the column, not the value reaching stage 7.
        Comparing a padded string against zero would be a type error.
        """
        rule = DiffRule(column_names=["n"], pad_zeros=4, cast_to="Int64")

        sql = _snowflake().compile_column_predicate(
            rule, "n", source_dtype=pl.Float64(), target_dtype=pl.Float64()
        )

        assert "FLOOR" not in sql

    def test_it_leaves_an_integer_cast_alone(self) -> None:
        """Ensure a non-float source skips the truncation guard."""
        rule = DiffRule(column_names=["n"], cast_to="Int64")

        sql = _snowflake().compile_column_predicate(
            rule, "n", source_dtype=pl.Int32(), target_dtype=pl.Int32()
        )

        assert "FLOOR" not in sql
        assert 'CAST("src"."n" AS BIGINT)' in sql

    def test_it_rejects_a_cast_target_with_no_keyword(self) -> None:
        """Ensure an unmapped cast target fails closed rather than reaching SQL.

        Unreachable through a validated config. It exists so that adding a new
        `CastTarget` without filling in every dialect breaks loudly instead of
        emitting an unquoted config string into a `CAST`.
        """
        with pytest.raises(ConnectorError, match="Decimal"):
            _snowflake()._cast_keyword("Decimal")  # type: ignore[arg-type]


@pytest.mark.unit
@pytest.mark.fast
class TestPadZerosCompilation:
    """Validate stage 5, which has no single-function SQL equivalent."""

    def test_it_pads_after_a_sign(self) -> None:
        """Ensure the emitted SQL splits the sign off before padding."""
        rule = DiffRule(column_names=["c"], pad_zeros=4)

        sql = _duckdb().compile_column_predicate(rule, "c")

        assert "SUBSTR(CAST(\"src\".\"c\" AS VARCHAR), 1, 1) IN ('-', '+')" in sql
        assert 'LPAD(SUBSTR(CAST("src"."c" AS VARCHAR), 2), 3, \'0\')' in sql

    def test_it_leaves_over_long_values_untouched(self) -> None:
        """Ensure a length guard precedes any `LPAD`, which would truncate."""
        rule = DiffRule(column_names=["c"], pad_zeros=4)

        sql = _duckdb().compile_column_predicate(rule, "c")

        assert 'CASE WHEN LENGTH(CAST("src"."c" AS VARCHAR)) >= 4' in sql

    def test_it_still_stringifies_at_width_zero(self) -> None:
        """Ensure width zero casts to text without emitting a pad."""
        rule = DiffRule(column_names=["c"], pad_zeros=0)

        sql = _duckdb().compile_column_predicate(rule, "c")

        assert 'CAST("src"."c" AS VARCHAR)' in sql
        assert "LPAD" not in sql


@pytest.mark.unit
@pytest.mark.fast
class TestDatetimeFormatCompilation:
    """Validate stage 6a, whose translation tables DuckDB cannot exercise.

    The differential harness runs DuckDB, which reads Python directives
    natively. These string assertions are the only coverage the Snowflake and
    Databricks format languages get.
    """

    def test_it_translates_a_snowflake_format(self) -> None:
        """Ensure directives become Snowflake elements and literals are quoted."""
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d %H:%M:%S")

        sql = _snowflake().compile_column_predicate(rule, "ts")

        assert 'TRY_TO_TIMESTAMP("src"."ts", \'YYYY"-"MM"-"DD" "HH24":"MI":"SS\')' in sql

    def test_it_translates_a_databricks_format(self) -> None:
        """Ensure directives become Java pattern letters with quoted literals.

        Databricks parses with `DateTimeFormatter`, where every unquoted letter
        is a pattern symbol. The literal quoting is what keeps a separator from
        being read as one.
        """
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d")

        sql = _databricks().compile_column_predicate(rule, "ts")

        # Databricks concatenates adjacent literals, so `''` would drop the
        # quotes the pattern needs; a backslash keeps them inside one literal.
        assert r"try_to_timestamp(`src`.`ts`, 'yyyy\'-\'MM\'-\'dd')" in sql

    def test_it_translates_a_duckdb_format(self) -> None:
        """Ensure DuckDB keeps the Python directives it already understands."""
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d")

        sql = _duckdb().compile_column_predicate(rule, "ts")

        assert 'try_strptime("src"."ts", \'%Y-%m-%d\')' in sql

    def test_it_translates_a_literal_percent(self) -> None:
        """Ensure `%%` becomes a quoted literal rather than a directive."""
        rule = DiffRule(column_names=["ts"], datetime_format="%Y%%")

        assert "'YYYY\"%\"'" in _snowflake().compile_column_predicate(rule, "ts")
        assert r"'yyyy\'%\''" in _databricks().compile_column_predicate(rule, "ts")
        assert "'%Y%%'" in _duckdb().compile_column_predicate(rule, "ts")

    def test_it_rejects_an_untranslatable_directive(self) -> None:
        """Ensure an unsupported directive fails closed.

        A blind substitution would leave `%j` in the emitted format, where the
        warehouse parses nothing and returns NULL for every row -- which reads
        as a clean match rather than as an error.
        """
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%j")

        with pytest.raises(ConfigError, match="%j"):
            _snowflake().compile_column_predicate(rule, "ts")

    def test_it_rejects_an_unquotable_literal(self) -> None:
        """Ensure a literal outside the separator allowlist is refused.

        The allowlist excludes both quote characters, so no literal run can
        close the quoting the translator wraps it in.
        """
        rule = DiffRule(column_names=["ts"], datetime_format="%Y'X")

        with pytest.raises(ConfigError, match="literal character"):
            _snowflake().compile_column_predicate(rule, "ts")

    def test_it_rejects_a_dangling_percent(self) -> None:
        """Ensure a truncated directive is an error, not a silent literal."""
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%")

        with pytest.raises(ConfigError, match="dangling"):
            _snowflake().compile_column_predicate(rule, "ts")

    def test_it_skips_parsing_a_non_text_column(self) -> None:
        """Ensure a native timestamp column is not fed to a text parser."""
        rule = DiffRule(column_names=["ts"], datetime_format="%Y-%m-%d")

        sql = _duckdb().compile_column_predicate(
            rule, "ts", source_dtype=pl.Datetime(), target_dtype=pl.Datetime()
        )

        assert "try_strptime" not in sql


@pytest.mark.unit
@pytest.mark.fast
class TestCTENormalization:
    """Validate that join SQL stays linear after stages 1-7 are projected once."""

    def test_it_projects_each_source_column_once_in_the_normalization_cte(self) -> None:
        """Ensure a fully loaded rule does not re-embed the raw column 24 times.

        Before the CTE rewrite, `_apply_pad_zeros` alone copied its input five
        times, and the mismatch tally then copied the whole predicate again.
        """
        rule = DiffRule(
            column_names=["amount"],
            null_values=["N/A", ""],
            regex_replace={r"\$": "", ",": ""},
            whitespace_mode="both",
            case_insensitive=True,
            value_map={"A": "B"},
            pad_zeros=10,
            cast_to="Float64",
            treat_null_as_equal=True,
        )
        sql = _snowflake().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [rule],
            source_types={"id": pl.Int64(), "amount": pl.String()},
            target_types={"id": pl.Int64(), "amount": pl.String()},
        )

        where_clause = sql.split("WHERE NOT", 1)[1]
        assert "LPAD" not in where_clause
        assert "REGEXP_REPLACE" not in where_clause
        assert 'EQUAL_NULL("src"."amount", "tgt"."amount")' in where_clause
        assert "LPAD" in sql

    def test_it_grows_roughly_linearly_as_rules_are_added(self) -> None:
        """Ensure adding a column adds a bounded amount of SQL, not a nested copy."""
        lengths: list[int] = []
        for count in (1, 2, 4, 8):
            rules = [
                DiffRule(
                    column_names=[f"c{index}"],
                    null_values=["N/A"],
                    regex_replace={r"\$": ""},
                    whitespace_mode="both",
                    pad_zeros=8,
                    cast_to="Float64",
                )
                for index in range(count)
            ]
            sql = _snowflake().compile_query("src_tbl", "tgt_tbl", ["id"], rules)
            lengths.append(len(sql))

        first = lengths[0]
        last = lengths[-1]
        # Eight copies of the same stage list cannot cost eight times a nested
        # rewrite of the first column. A 3x budget over the single-column
        # length times the column count is generous for quoting and aliases.
        assert last < first * 8 * 3
        assert last / first < 12


@pytest.mark.unit
@pytest.mark.fast
class TestEditDistanceCompilation:
    """Validate stage 8's text similarity limits across the dialect table."""

    def test_it_names_an_edit_distance_function_for_every_dialect(self) -> None:
        """Ensure a new dialect cannot inherit another's function name."""
        assert set(_EDIT_DISTANCE_FUNCTIONS) == set(SQLDialect)

    @pytest.mark.parametrize(
        ("compiler", "expected"),
        [
            pytest.param(
                _snowflake(),
                '("src"."name" = "tgt"."name" OR EDITDISTANCE("src"."name", "tgt"."name") <= 2)',
                id="snowflake",
            ),
            pytest.param(
                _databricks(),
                "(`src`.`name` = `tgt`.`name` OR levenshtein(`src`.`name`, `tgt`.`name`) <= 2)",
                id="databricks",
            ),
            pytest.param(
                _duckdb(),
                '("src"."name" = "tgt"."name" OR levenshtein("src"."name", "tgt"."name") <= 2)',
                id="duckdb",
            ),
        ],
    )
    def test_it_matches_text_within_the_edit_distance(
        self, compiler: SQLPushdownCompiler, expected: str
    ) -> None:
        """Ensure equal text matches outright and differing text within the limit."""
        rule = DiffRule(column_names=["name"], max_levenshtein_distance=2)

        assert compiler.compile_column_predicate(rule, "name") == expected

    def test_it_keeps_null_pairs_to_treat_null_as_equal(self) -> None:
        """Ensure the limit sits inside the same null-safe form a tolerance uses."""
        rule = DiffRule(column_names=["name"], max_levenshtein_distance=2, treat_null_as_equal=True)

        sql = _snowflake().compile_column_predicate(rule, "name")

        assert sql == (
            '("src"."name" IS NULL AND "tgt"."name" IS NULL) OR '
            '(("src"."name" = "tgt"."name" OR EDITDISTANCE("src"."name", "tgt"."name") <= 2))'
        )
        assert "EQUAL_NULL" not in sql

    def test_it_loosens_the_join_and_the_tally_alike(self) -> None:
        """Ensure a changed row and the per-column count read the same predicate."""
        rules = [DiffRule(column_names=["name"], max_levenshtein_distance=2)]
        predicate = (
            'COALESCE(("src"."name" = "tgt"."name" OR '
            'EDITDISTANCE("src"."name", "tgt"."name") <= 2), FALSE)'
        )

        query = _snowflake().compile_query("src_tbl", "tgt_tbl", ["id"], rules)
        tally = _snowflake().compile_column_mismatch_query("src_tbl", "tgt_tbl", ["id"], rules)

        assert predicate in query
        assert tally is not None
        assert predicate in tally

    def test_it_refuses_a_jaro_winkler_floor(self) -> None:
        """Ensure a caller using the compiler directly cannot get an approximation."""
        rule = DiffRule(column_names=["name"], min_jaro_winkler_similarity=0.9)

        with pytest.raises(ConfigError, match="min_jaro_winkler_similarity has no SQL"):
            _databricks().compile_column_predicate(rule, "name")
