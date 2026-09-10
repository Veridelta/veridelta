# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Zero-dependency SQL pushdown compiler for warehouse dialects.

Translates `DiffRule` models into Snowflake and Databricks SQL predicates and
assembles inner-join mismatch queries, per-column mismatch tallies, anti-join
queries for added and removed rows, row counts, and column probes without
extracting source tables.
"""

from enum import Enum

from veridelta.exceptions import ConnectorError
from veridelta.models import SQL_IDENTIFIER_SEGMENT, DiffRule

COUNT_ALIAS = "_veridelta_total"
"""Column alias projected by `compile_count_query` so results stay dialect-neutral."""


class SQLDialect(str, Enum):
    """Warehouse SQL dialects supported by the pushdown compiler."""

    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"


class SQLPushdownCompiler:
    """Compile `DiffRule` semantics into dialect-specific SQL strings."""

    def __init__(self, dialect: SQLDialect) -> None:
        """Initialize a compiler for a single warehouse dialect.

        Args:
            dialect (SQLDialect): Target SQL dialect.
        """
        self.dialect = dialect

    def compile_column_predicate(
        self,
        rule: DiffRule,
        source_column: str,
        target_column: str | None = None,
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
    ) -> str:
        """Compile a boolean match predicate for one source/target column pair.

        Follows the canonical transform order documented on `DiffRule`, which is
        the single source of truth shared with the local engine. This compiler
        implements stages 1 through 4, 8, and 9; the stages it cannot express are
        rejected by `_reject_unimplemented` rather than skipped.

        Args:
            rule (DiffRule): Semantic comparison overrides for the column.
            source_column (str): Column name on the source relation.
            target_column (str | None): Column name on the target relation. Defaults
                to `source_column` when omitted.
            source_alias (str): SQL alias of the source relation.
            target_alias (str): SQL alias of the target relation.

        Returns:
            str: Boolean SQL expression that is true when the column values match.

        Raises:
            ConnectorError: If identifiers are empty or the rule uses unimplemented
                fields (`pad_zeros`, `datetime_format`, `timezone`, `cast_to`).
        """
        self._reject_unimplemented(rule)
        tgt_name = target_column if target_column is not None else source_column
        src_expr = self._qualify(source_alias, source_column)
        tgt_expr = self._qualify(target_alias, tgt_name)

        src_expr = self._apply_null_values(src_expr, rule)
        tgt_expr = self._apply_null_values(tgt_expr, rule)
        src_expr = self._apply_regex_replace(src_expr, rule)
        tgt_expr = self._apply_regex_replace(tgt_expr, rule)
        src_expr = self._apply_whitespace(src_expr, rule)
        tgt_expr = self._apply_whitespace(tgt_expr, rule)
        src_expr = self._apply_case(src_expr, rule)
        tgt_expr = self._apply_case(tgt_expr, rule)
        src_expr = self._apply_value_map(src_expr, rule)

        return self._compare(src_expr, tgt_expr, rule)

    def compile_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rules: list[DiffRule],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
    ) -> str:
        """Assemble a changed-row inner-join query from tables, keys, and rules.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys present on both relations.
            rules (list[DiffRule]): Per-column semantic overrides.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.

        Returns:
            str: `SELECT ... FROM src INNER JOIN tgt ON ...` statement. A `WHERE NOT`
            clause is added when at least one non-ignored column predicate exists.

        Raises:
            ConnectorError: If tables or keys are empty, a rule is pattern-only, or
                `rename_to` is used with multiple `column_names`.
        """
        if not primary_keys:
            raise ConnectorError("At least one primary key is required for pushdown joins.")

        quoted_source = self._quote_relation(source_table)
        quoted_target = self._quote_relation(target_table)
        quoted_src_alias = self._quote_ident(source_alias)
        quoted_tgt_alias = self._quote_ident(target_alias)

        select_list = ", ".join(self._qualify(source_alias, pk) for pk in primary_keys)
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )

        predicates = [
            predicate
            for _, predicate in self._collect_predicates(
                rules, source_alias=source_alias, target_alias=target_alias
            )
        ]

        statement = (
            f"SELECT {select_list} "
            f"FROM {quoted_source} AS {quoted_src_alias} "
            f"INNER JOIN {quoted_target} AS {quoted_tgt_alias} "
            f"ON {on_clause}"
        )
        if predicates:
            joined = " AND ".join(f"({pred})" for pred in predicates)
            statement = f"{statement} WHERE NOT ({joined})"
        return statement

    def compile_missing_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
    ) -> str:
        """Assemble a LEFT JOIN anti-join for rows present only in the source.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys present on both relations.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.

        Returns:
            str: `SELECT src keys FROM src LEFT JOIN tgt ON ... WHERE tgt keys IS NULL`.

        Raises:
            ConnectorError: If tables or keys are empty.
        """
        return self._compile_anti_join(
            source_table,
            target_table,
            primary_keys,
            join_kind="LEFT",
            select_alias=source_alias,
            null_alias=target_alias,
            source_alias=source_alias,
            target_alias=target_alias,
        )

    def compile_added_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
    ) -> str:
        """Assemble a RIGHT JOIN anti-join for rows present only in the target.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys present on both relations.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.

        Returns:
            str: `SELECT tgt keys FROM src RIGHT JOIN tgt ON ... WHERE src keys IS NULL`.

        Raises:
            ConnectorError: If tables or keys are empty.
        """
        return self._compile_anti_join(
            source_table,
            target_table,
            primary_keys,
            join_kind="RIGHT",
            select_alias=target_alias,
            null_alias=source_alias,
            source_alias=source_alias,
            target_alias=target_alias,
        )

    def compile_column_mismatch_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rules: list[DiffRule],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
    ) -> str | None:
        """Assemble a per-column mismatch tally over the joined rows.

        Each column contributes one `SUM(CASE ...)` term, so a single round trip
        fills `DiffSummary.column_mismatches` the way the local engine does.
        `COALESCE(pred, FALSE)` is load-bearing: under three-valued logic a NULL
        predicate is neither true nor false, and without the coalesce those rows
        would silently count as matches instead of mismatches. The local engine
        resolves the same case with `val_match.fill_null(False)`.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys present on both relations.
            rules (list[DiffRule]): Per-column semantic overrides.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.

        Returns:
            str | None: Single-row aggregate statement, or None when no rule
            yields a comparable column, mirroring the local engine's decision to
            skip the tally when there are no match expressions.

        Raises:
            ConnectorError: If tables or keys are empty, a rule is pattern-only, or
                `rename_to` is used with multiple `column_names`.
        """
        if not primary_keys:
            raise ConnectorError("At least one primary key is required for pushdown joins.")

        pairs = self._collect_predicates(
            rules, source_alias=source_alias, target_alias=target_alias
        )
        if not pairs:
            return None

        select_list = ", ".join(
            f"SUM(CASE WHEN COALESCE({predicate}, FALSE) THEN 0 ELSE 1 END) "
            f"AS {self._quote_ident(alias)}"
            for alias, predicate in pairs
        )
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        return (
            f"SELECT {select_list} "
            f"FROM {self._quote_relation(source_table)} AS {self._quote_ident(source_alias)} "
            f"INNER JOIN {self._quote_relation(target_table)} AS {self._quote_ident(target_alias)} "
            f"ON {on_clause}"
        )

    def compile_count_query(self, table: str) -> str:
        """Assemble a total row count query for one relation.

        The count supplies the denominator for `DiffSummary.mismatch_ratio`, so
        warehouse runs honor `threshold` the same way local comparisons do.

        Args:
            table (str): Relation to count (optionally dotted catalog path).

        Returns:
            str: `SELECT COUNT(*) AS alias FROM relation` with the alias quoted
            for the active dialect.

        Raises:
            ConnectorError: If the relation name is empty or not allowlisted.
        """
        return (
            f"SELECT COUNT(*) AS {self._quote_ident(COUNT_ALIAS)} "
            f"FROM {self._quote_relation(table)}"
        )

    def compile_schema_probe_query(self, table: str) -> str:
        """Assemble a zero-row projection used to read a relation's columns.

        Args:
            table (str): Relation to probe (optionally dotted catalog path).

        Returns:
            str: `SELECT * FROM relation WHERE 1 = 0`, which returns column
            metadata without scanning rows.

        Raises:
            ConnectorError: If the relation name is empty or not allowlisted.
        """
        return f"SELECT * FROM {self._quote_relation(table)} WHERE 1 = 0"

    def _compile_anti_join(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        join_kind: str,
        select_alias: str,
        null_alias: str,
        source_alias: str,
        target_alias: str,
    ) -> str:
        """Assemble a LEFT or RIGHT JOIN anti-join selecting keys from one side.

        Args:
            source_table (str): Source relation.
            target_table (str): Target relation.
            primary_keys (list[str]): Join keys present on both relations.
            join_kind (str): `LEFT` or `RIGHT`.
            select_alias (str): Alias whose primary keys are projected.
            null_alias (str): Alias whose keys must be NULL (the missing side).
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.

        Returns:
            str: Anti-join SELECT statement.

        Raises:
            ConnectorError: If tables or keys are empty.
        """
        if not primary_keys:
            raise ConnectorError("At least one primary key is required for pushdown joins.")

        quoted_source = self._quote_relation(source_table)
        quoted_target = self._quote_relation(target_table)
        quoted_src_alias = self._quote_ident(source_alias)
        quoted_tgt_alias = self._quote_ident(target_alias)
        select_list = ", ".join(self._qualify(select_alias, pk) for pk in primary_keys)
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        where_clause = " AND ".join(
            f"{self._qualify(null_alias, pk)} IS NULL" for pk in primary_keys
        )
        return (
            f"SELECT {select_list} "
            f"FROM {quoted_source} AS {quoted_src_alias} "
            f"{join_kind} JOIN {quoted_target} AS {quoted_tgt_alias} "
            f"ON {on_clause} "
            f"WHERE {where_clause}"
        )

    def _join_on_clause(
        self, primary_keys: list[str], *, source_alias: str, target_alias: str
    ) -> str:
        """Build the equality ON clause for warehouse joins.

        Args:
            primary_keys (list[str]): Join keys present on both relations.
            source_alias (str): Source relation alias.
            target_alias (str): Target relation alias.

        Returns:
            str: `src.key = tgt.key` predicates joined with AND.
        """
        return " AND ".join(
            f"{self._qualify(source_alias, pk)} = {self._qualify(target_alias, pk)}"
            for pk in primary_keys
        )

    def _collect_predicates(
        self, rules: list[DiffRule], *, source_alias: str, target_alias: str
    ) -> list[tuple[str, str]]:
        """Expand every rule into deduplicated result-alias and predicate pairs.

        The local engine resolves exactly one effective rule per column, so when
        several rules name the same column the first one wins here too. That also
        keeps aggregate select lists free of duplicate output names.

        Args:
            rules (list[DiffRule]): Rules to compile, in declaration order.
            source_alias (str): Source relation alias.
            target_alias (str): Target relation alias.

        Returns:
            list[tuple[str, str]]: Result alias paired with its match predicate.

        Raises:
            ConnectorError: If a rule is pattern-only or `rename_to` is invalid.
        """
        collected: dict[str, str] = {}
        for rule in rules:
            for alias, predicate in self._predicates_for_rule(
                rule, source_alias=source_alias, target_alias=target_alias
            ):
                collected.setdefault(alias, predicate)
        return list(collected.items())

    def _predicates_for_rule(
        self, rule: DiffRule, *, source_alias: str, target_alias: str
    ) -> list[tuple[str, str]]:
        """Expand one rule into per-column result aliases and match predicates.

        The alias is the target-side name, which is what the local engine reports
        in `column_mismatches` after `rename_to` has been applied.

        Args:
            rule (DiffRule): Rule to compile.
            source_alias (str): Source relation alias.
            target_alias (str): Target relation alias.

        Returns:
            list[tuple[str, str]]: Alias and predicate pairs. Empty when ignored.

        Raises:
            ConnectorError: If the rule is pattern-only or `rename_to` is invalid.
        """
        if rule.pattern is not None and not rule.column_names:
            raise ConnectorError(
                "Pattern-only DiffRule cannot be compiled without a resolved column name."
            )
        if rule.ignore or not rule.column_names:
            return []
        if rule.rename_to is not None and len(rule.column_names) != 1:
            raise ConnectorError("rename_to is only valid when column_names has exactly one entry.")

        pairs: list[tuple[str, str]] = []
        for column in rule.column_names:
            target_column = rule.rename_to if rule.rename_to is not None else column
            pairs.append(
                (
                    target_column,
                    self.compile_column_predicate(
                        rule,
                        column,
                        target_column,
                        source_alias=source_alias,
                        target_alias=target_alias,
                    ),
                )
            )
        return pairs

    def _reject_unimplemented(self, rule: DiffRule) -> None:
        """Raise when the rule uses fields the compiler cannot emit yet.

        Args:
            rule (DiffRule): Rule to inspect.

        Raises:
            ConnectorError: If an unimplemented field is set.
        """
        unimplemented = (
            ("pad_zeros", rule.pad_zeros),
            ("datetime_format", rule.datetime_format),
            ("timezone", rule.timezone),
            ("cast_to", rule.cast_to),
        )
        for field_name, value in unimplemented:
            if value is not None:
                raise ConnectorError(f"SQL pushdown does not support DiffRule.{field_name}.")

    def _quote_ident(self, name: str) -> str:
        """Quote a single SQL identifier for the active dialect.

        Args:
            name (str): Unquoted identifier.

        Returns:
            str: Dialect-quoted identifier.

        Raises:
            ConnectorError: If `name` is not a valid unquoted identifier segment.
        """
        if SQL_IDENTIFIER_SEGMENT.fullmatch(name) is None:
            raise ConnectorError("SQL identifier is not a valid unquoted identifier.")
        if self.dialect is SQLDialect.SNOWFLAKE:
            return '"' + name.replace('"', '""') + '"'
        return "`" + name.replace("`", "``") + "`"

    def _quote_relation(self, name: str) -> str:
        """Quote a possibly dotted table, schema, or catalog path.

        Args:
            name (str): Relation name, optionally `catalog.schema.table`.

        Returns:
            str: Each path segment quoted independently.

        Raises:
            ConnectorError: If the relation name is empty, has more than three
                segments, or contains a disallowed identifier.
        """
        trimmed = name.strip()
        if not trimmed:
            raise ConnectorError("Table name must be a non-empty string.")
        parts = trimmed.split(".")
        if len(parts) > 3:
            raise ConnectorError("Table name must have at most three dotted segments.")
        return ".".join(self._quote_ident(part) for part in parts)

    def _qualify(self, alias: str, column: str) -> str:
        """Return `alias.column` with both parts quoted.

        Args:
            alias (str): Relation alias.
            column (str): Column name.

        Returns:
            str: Qualified, quoted column reference.
        """
        return f"{self._quote_ident(alias)}.{self._quote_ident(column)}"

    def _literal(self, value: str) -> str:
        """Render a single-quoted SQL string literal.

        Args:
            value (str): Raw Python string.

        Returns:
            str: Quoted SQL literal with escaped apostrophes.
        """
        return "'" + value.replace("'", "''") + "'"

    def _number(self, value: float) -> str:
        """Render a numeric SQL literal.

        Args:
            value (float): Tolerance or coefficient.

        Returns:
            str: Portable decimal literal.
        """
        return repr(value)

    def _apply_null_values(self, expr: str, rule: DiffRule) -> str:
        """Wrap an expression in nested `NULLIF` calls for sentinel strings.

        Args:
            expr (str): SQL expression to sanitize.
            rule (DiffRule): Rule providing `null_values`.

        Returns:
            str: Expression with sentinels coerced to NULL.
        """
        if not rule.null_values:
            return expr
        wrapped = expr
        for sentinel in rule.null_values:
            wrapped = f"NULLIF({wrapped}, {self._literal(sentinel)})"
        return wrapped

    def _apply_regex_replace(self, expr: str, rule: DiffRule) -> str:
        """Apply `REGEXP_REPLACE` for each pattern/replacement pair.

        Args:
            expr (str): SQL expression to sanitize.
            rule (DiffRule): Rule providing `regex_replace`.

        Returns:
            str: Nested `REGEXP_REPLACE` expression.
        """
        if not rule.regex_replace:
            return expr
        wrapped = expr
        for pattern, replacement in rule.regex_replace.items():
            wrapped = (
                f"REGEXP_REPLACE({wrapped}, {self._literal(pattern)}, {self._literal(replacement)})"
            )
        return wrapped

    def _apply_whitespace(self, expr: str, rule: DiffRule) -> str:
        """Apply dialect-neutral trim functions for `whitespace_mode`.

        Args:
            expr (str): SQL expression to trim.
            rule (DiffRule): Rule providing `whitespace_mode`.

        Returns:
            str: Trimmed expression, or `expr` when mode is unset/`none`.
        """
        mode = rule.whitespace_mode
        if mode == "left":
            return f"LTRIM({expr})"
        if mode == "right":
            return f"RTRIM({expr})"
        if mode == "both":
            return f"TRIM({expr})"
        return expr

    def _apply_case(self, expr: str, rule: DiffRule) -> str:
        """Lowercase an expression when `case_insensitive` is enabled.

        Args:
            expr (str): SQL expression to normalize.
            rule (DiffRule): Rule providing `case_insensitive`.

        Returns:
            str: `LOWER(expr)` or the original expression.
        """
        if rule.case_insensitive:
            return f"LOWER({expr})"
        return expr

    def _apply_value_map(self, expr: str, rule: DiffRule) -> str:
        """Map source values using Snowflake `IFF` or Databricks `CASE`.

        Args:
            expr (str): Source-side SQL expression.
            rule (DiffRule): Rule providing `value_map`.

        Returns:
            str: Crosswalk expression, or `expr` when no map is set.
        """
        if not rule.value_map:
            return expr
        if self.dialect is SQLDialect.SNOWFLAKE:
            wrapped = expr
            for key, value in reversed(list(rule.value_map.items())):
                wrapped = f"IFF({expr} = {self._literal(key)}, {self._literal(value)}, {wrapped})"
            return wrapped

        branches = " ".join(
            f"WHEN {expr} = {self._literal(key)} THEN {self._literal(value)}"
            for key, value in rule.value_map.items()
        )
        return f"CASE {branches} ELSE {expr} END"

    def _has_tolerance(self, rule: DiffRule) -> bool:
        """Return whether numeric tolerance predicates should be emitted.

        Args:
            rule (DiffRule): Rule providing absolute/relative tolerances.

        Returns:
            bool: True when either tolerance is a positive number.
        """
        abs_tol = rule.absolute_tolerance or 0.0
        rel_tol = rule.relative_tolerance or 0.0
        return abs_tol != 0.0 or rel_tol != 0.0

    def _numeric_predicate(self, src_expr: str, tgt_expr: str, rule: DiffRule) -> str:
        """Build the engine-equivalent absolute/relative tolerance predicate.

        Args:
            src_expr (str): Transformed source expression.
            tgt_expr (str): Transformed target expression.
            rule (DiffRule): Rule providing tolerances.

        Returns:
            str: `ABS(tgt - src) <= abs + (rel * ABS(src))`.
        """
        abs_tol = self._number(rule.absolute_tolerance or 0.0)
        rel_tol = self._number(rule.relative_tolerance or 0.0)
        return f"ABS({tgt_expr} - {src_expr}) <= {abs_tol} + ({rel_tol} * ABS({src_expr}))"

    def _compare(self, src_expr: str, tgt_expr: str, rule: DiffRule) -> str:
        """Build the final match predicate, including null-safe equality.

        Args:
            src_expr (str): Fully transformed source expression.
            tgt_expr (str): Fully transformed target expression.
            rule (DiffRule): Rule providing comparison and null semantics.

        Returns:
            str: Boolean SQL expression.
        """
        if self._has_tolerance(rule):
            numeric = self._numeric_predicate(src_expr, tgt_expr, rule)
            if rule.treat_null_as_equal:
                return f"({src_expr} IS NULL AND {tgt_expr} IS NULL) OR ({numeric})"
            return numeric

        if rule.treat_null_as_equal:
            if self.dialect is SQLDialect.SNOWFLAKE:
                return f"EQUAL_NULL({src_expr}, {tgt_expr})"
            return f"{src_expr} <=> {tgt_expr}"

        return f"{src_expr} = {tgt_expr}"
