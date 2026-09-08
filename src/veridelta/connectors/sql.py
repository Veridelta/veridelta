# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Zero-dependency SQL pushdown compiler for warehouse dialects.

Translates `DiffRule` models into Snowflake and Databricks SQL predicates and
assembles inner-join queries that isolate changed rows without extracting data.
"""

from enum import Enum

from veridelta.exceptions import ConnectorError
from veridelta.models import DiffRule


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

        Transform order matches the local engine: null sentinels, regex replace,
        whitespace/case, source-side value map, comparison, then null-safe equality.

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
        on_clause = " AND ".join(
            f"{self._qualify(source_alias, pk)} = {self._qualify(target_alias, pk)}"
            for pk in primary_keys
        )

        predicates: list[str] = []
        for rule in rules:
            predicates.extend(
                self._predicates_for_rule(
                    rule, source_alias=source_alias, target_alias=target_alias
                )
            )

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

    def _predicates_for_rule(
        self, rule: DiffRule, *, source_alias: str, target_alias: str
    ) -> list[str]:
        """Expand one rule into per-column match predicates.

        Args:
            rule (DiffRule): Rule to compile.
            source_alias (str): Source relation alias.
            target_alias (str): Target relation alias.

        Returns:
            list[str]: Match predicates. Empty when the rule is ignored.

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

        predicates: list[str] = []
        for column in rule.column_names:
            target_column = rule.rename_to if rule.rename_to is not None else column
            predicates.append(
                self.compile_column_predicate(
                    rule,
                    column,
                    target_column,
                    source_alias=source_alias,
                    target_alias=target_alias,
                )
            )
        return predicates

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
            ConnectorError: If `name` is empty or whitespace-only.
        """
        trimmed = name.strip()
        if not trimmed:
            raise ConnectorError("SQL identifier must be a non-empty string.")
        if self.dialect is SQLDialect.SNOWFLAKE:
            return '"' + trimmed.replace('"', '""') + '"'
        return "`" + trimmed.replace("`", "``") + "`"

    def _quote_relation(self, name: str) -> str:
        """Quote a possibly dotted table, schema, or catalog path.

        Args:
            name (str): Relation name, optionally `catalog.schema.table`.

        Returns:
            str: Each path segment quoted independently.

        Raises:
            ConnectorError: If the relation name is empty.
        """
        trimmed = name.strip()
        if not trimmed:
            raise ConnectorError("Table name must be a non-empty string.")
        return ".".join(self._quote_ident(part) for part in trimmed.split("."))

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
