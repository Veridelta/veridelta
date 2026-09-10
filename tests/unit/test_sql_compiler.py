# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for dialect-specific SQL pushdown compilation."""

import pytest

from veridelta.connectors import SQLDialect, SQLPushdownCompiler
from veridelta.connectors.sql import COUNT_ALIAS
from veridelta.exceptions import ConnectorError
from veridelta.models import DiffRule


def _snowflake() -> SQLPushdownCompiler:
    """Return a Snowflake-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.SNOWFLAKE)


def _databricks() -> SQLPushdownCompiler:
    """Return a Databricks-targeted compiler."""
    return SQLPushdownCompiler(SQLDialect.DATABRICKS)


@pytest.mark.unit
@pytest.mark.fast
class TestSQLDialect:
    """Validate the warehouse dialect enum."""

    def test_it_exposes_snowflake_and_databricks_values(self) -> None:
        """Ensure the enum is a closed set of supported warehouse dialects."""
        assert SQLDialect.SNOWFLAKE.value == "snowflake"
        assert SQLDialect.DATABRICKS.value == "databricks"
        assert {member.value for member in SQLDialect} == {"snowflake", "databricks"}


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
        """Ensure SQL string literals double embedded apostrophes."""
        rule = DiffRule(
            column_names=["status"],
            value_map={"O'Brien": "OB"},
            null_values=["N/A'"],
            regex_replace={"x'": "y'"},
        )
        sql = _snowflake().compile_column_predicate(rule, "status")
        assert "O''Brien" in sql
        assert "N/A''" in sql
        assert "x''" in sql
        assert "y''" in sql


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
        assert "'^\\$'" in sql or r"'^\$'" in sql

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

    def test_it_nests_nullif_for_sentinel_values(self) -> None:
        """Ensure each null sentinel becomes a nested NULLIF on both sides."""
        rule = DiffRule(column_names=["status"], null_values=["N/A", "-999"])
        sql = _snowflake().compile_column_predicate(rule, "status")
        assert sql.count("NULLIF(") == 4
        assert "'N/A'" in sql
        assert "'-999'" in sql

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
        """Ensure numeric compare matches ABS(tgt-src) <= abs + rel * ABS(src)."""
        rule = DiffRule(
            column_names=["amount"],
            absolute_tolerance=0.01,
            relative_tolerance=0.05,
        )
        sql = _snowflake().compile_column_predicate(rule, "amount")
        assert 'ABS("tgt"."amount" - "src"."amount")' in sql
        assert "<= 0.01 + (0.05 * ABS(" in sql

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
        assert sql.startswith('SELECT "src"."id", "src"."line_id"')
        assert 'FROM "analytics"."public"."source_orders" AS "src"' in sql
        assert 'INNER JOIN "analytics"."public"."target_orders" AS "tgt"' in sql
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

    def test_it_omits_where_when_every_rule_is_ignored(self) -> None:
        """Ensure join-only SQL is emitted when no compare columns remain."""
        sql = _databricks().compile_query(
            "src_tbl",
            "tgt_tbl",
            ["id"],
            [DiffRule(column_names=["notes"], ignore=True)],
        )
        assert "INNER JOIN" in sql
        assert "WHERE" not in sql

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
        assert sql.startswith('SELECT "src"."id"')
        assert 'FROM "src_tbl" AS "src"' in sql
        assert 'LEFT JOIN "tgt_tbl" AS "tgt"' in sql
        assert 'ON "src"."id" = "tgt"."id"' in sql
        assert 'WHERE "tgt"."id" IS NULL' in sql
        assert "INNER JOIN" not in sql
        assert "RIGHT JOIN" not in sql

    def test_it_builds_snowflake_right_join_for_added_target_rows(self) -> None:
        """Ensure added rows use RIGHT JOIN with source keys IS NULL."""
        sql = _snowflake().compile_added_query("src_tbl", "tgt_tbl", ["id"])
        assert sql.startswith('SELECT "tgt"."id"')
        assert 'FROM "src_tbl" AS "src"' in sql
        assert 'RIGHT JOIN "tgt_tbl" AS "tgt"' in sql
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
        assert "LEFT JOIN `catalog`.`schema`.`tgt` AS `tgt`" in missing
        assert "WHERE `tgt`.`id` IS NULL" in missing
        assert "RIGHT JOIN `catalog`.`schema`.`tgt` AS `tgt`" in added
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
        assert missing.startswith('SELECT "src"."id", "src"."line_id"')
        assert added.startswith('SELECT "tgt"."id", "tgt"."line_id"')

    def test_it_rejects_empty_primary_keys_on_anti_joins(self) -> None:
        """Ensure anti-joins cannot be compiled without keys."""
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_missing_query("src_tbl", "tgt_tbl", [])
        with pytest.raises(ConnectorError, match="primary key"):
            _snowflake().compile_added_query("src_tbl", "tgt_tbl", [])


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

        assert sql == (
            'SELECT SUM(CASE WHEN COALESCE("src"."amount" = "tgt"."amount", FALSE) '
            'THEN 0 ELSE 1 END) AS "amount" '
            'FROM "src_tbl" AS "src" '
            'INNER JOIN "tgt_tbl" AS "tgt" '
            'ON "src"."id" = "tgt"."id"'
        )

    def test_it_quotes_the_databricks_aggregate_with_backticks(self) -> None:
        """Ensure the tally honors the Databricks quoting style."""
        sql = _databricks().compile_column_mismatch_query(
            "main.default.src", "main.default.tgt", ["id"], [DiffRule(column_names=["amount"])]
        )

        assert sql is not None
        assert "COALESCE(`src`.`amount` = `tgt`.`amount`, FALSE)" in sql
        assert "AS `amount`" in sql
        assert "INNER JOIN `main`.`default`.`tgt` AS `tgt`" in sql

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
        assert "COALESCE(ABS(" in sql
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
        assert '"src"."legacy_amt" = "tgt"."amount"' in sql
        assert 'AS "amount"' in sql
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
        assert sql.count('AS "amount"') == 1
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

    def test_it_rejects_unimplemented_rule_fields(self) -> None:
        """Ensure pad_zeros, datetime, timezone, and cast_to are blocked."""
        compiler = _snowflake()
        with pytest.raises(ConnectorError, match="pad_zeros"):
            compiler.compile_column_predicate(
                DiffRule(column_names=["id"], pad_zeros=5),
                "id",
            )
        with pytest.raises(ConnectorError, match="datetime_format"):
            compiler.compile_column_predicate(
                DiffRule(column_names=["id"], datetime_format="%Y-%m-%d"),
                "id",
            )
        with pytest.raises(ConnectorError, match="timezone"):
            compiler.compile_column_predicate(
                DiffRule(column_names=["id"], timezone="UTC"),
                "id",
            )
        with pytest.raises(ConnectorError, match="cast_to"):
            compiler.compile_column_predicate(
                DiffRule(column_names=["id"], cast_to="Float64"),
                "id",
            )
