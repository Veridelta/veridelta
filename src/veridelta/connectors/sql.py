# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Zero-dependency SQL pushdown compiler for warehouse dialects.

Translates `DiffRule` models into Snowflake and Databricks SQL predicates and
assembles inner-join mismatch queries, per-column mismatch tallies, anti-join
queries for added and removed rows, row counts, and column probes without
extracting source tables.

Every dialect-specific spelling lives in a table at module scope rather than in
the method that needs it. Adding a dialect is then a matter of filling in the
tables, and a missing entry fails loudly instead of inheriting some other
dialect's syntax.
"""

from collections.abc import Mapping, Sequence
from enum import Enum
from typing import Final

import polars as pl

from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import SQL_IDENTIFIER_SEGMENT, CastTarget, DiffRule, SentinelValue
from veridelta.sentinels import usable_sentinels

ColumnTypes = Mapping[str, pl.DataType]
"""Column name to dtype, as probed from a warehouse relation."""

COUNT_ALIAS = "_veridelta_total"
"""Column alias projected by `compile_count_query` so results stay dialect-neutral."""


class SQLDialect(str, Enum):
    """Warehouse SQL dialects supported by the pushdown compiler.

    `DUCKDB` has no connector or config model. It exists so the differential
    test harness can execute real compiler output instead of a rewritten
    approximation of it.
    """

    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    DUCKDB = "duckdb"


_CAST_KEYWORDS: Final[dict[SQLDialect, dict[CastTarget, str]]] = {
    SQLDialect.SNOWFLAKE: {
        "Int64": "BIGINT",
        "Float64": "FLOAT",
        "String": "VARCHAR",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
        "Datetime": "TIMESTAMP_NTZ",
    },
    SQLDialect.DATABRICKS: {
        "Int64": "BIGINT",
        "Float64": "DOUBLE",
        "String": "STRING",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
        "Datetime": "TIMESTAMP",
    },
    SQLDialect.DUCKDB: {
        "Int64": "BIGINT",
        "Float64": "DOUBLE",
        "String": "VARCHAR",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
        "Datetime": "TIMESTAMP",
    },
}
"""`cast_to` value to the type keyword each dialect spells it with.

A type name is the one thing in a `CAST` that cannot be quoted or bound as a
parameter, so this table is the boundary that keeps configuration text out of
the emitted SQL grammar. Nothing outside it ever reaches a `CAST`.
"""

_IDENTIFIER_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "`",
    SQLDialect.DUCKDB: '"',
}
"""Character each dialect quotes identifiers with, doubled to escape itself."""

_STRPTIME_DIRECTIVES: Final[dict[SQLDialect, dict[str, str]]] = {
    SQLDialect.SNOWFLAKE: {
        "Y": "YYYY",
        "m": "MM",
        "d": "DD",
        "H": "HH24",
        "M": "MI",
        "S": "SS",
        "f": "FF6",
        "z": "TZHTZM",
        "%": '"%"',
    },
    SQLDialect.DATABRICKS: {
        "Y": "yyyy",
        "m": "MM",
        "d": "dd",
        "H": "HH",
        "M": "mm",
        "S": "ss",
        "f": "SSSSSS",
        "z": "XX",
        "%": "'%'",
    },
    SQLDialect.DUCKDB: {
        "Y": "%Y",
        "m": "%m",
        "d": "%d",
        "H": "%H",
        "M": "%M",
        "S": "%S",
        "f": "%f",
        "z": "%z",
        "%": "%%",
    },
}
"""Python `strptime` directive to its spelling in each dialect's format language.

Three different languages: Snowflake's own, Java `DateTimeFormatter` for
Databricks, and Python's own for DuckDB. Membership here is the allowlist, and
anything absent is refused rather than passed through. A directive that survives
translation unrecognized parses to NULL, which reads as a clean match rather
than as an error.
"""

_FORMAT_LITERAL_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "'",
    SQLDialect.DUCKDB: "",
}
"""Character each dialect wraps a literal run of a format string in.

DuckDB reads Python directives directly, so its literals need no wrapper.
"""

_FORMAT_LITERALS: Final[frozenset[str]] = frozenset(" -/:.,_T")
"""Characters allowed between directives in a `datetime_format`.

Small on purpose. Every separator in a real timestamp format is here, and the
set excludes both quote characters, so a literal run can be wrapped in either
dialect's quoting without any escaping and without a way to close the quote
early.
"""

_PARSE_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "TRY_TO_TIMESTAMP",
    SQLDialect.DATABRICKS: "try_to_timestamp",
    SQLDialect.DUCKDB: "try_strptime",
}
"""Each dialect's non-throwing parse, matching Polars `strptime(strict=False)`.

The strict variants abort the whole statement on one unparseable row where the
local engine yields a null and keeps going.
"""


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
        source_dtype: pl.DataType | None = None,
        target_dtype: pl.DataType | None = None,
    ) -> str:
        """Compile a boolean match predicate for one source/target column pair.

        Follows the canonical transform order documented on `DiffRule`, which is
        the single source of truth shared with the local engine. All nine stages
        compile, so a pushdown run and a local run evaluate the same pipeline
        rather than differing by whatever the warehouse happened to support.

        Args:
            rule (DiffRule): Semantic comparison overrides for the column.
            source_column (str): Column name on the source relation.
            target_column (str | None): Column name on the target relation. Defaults
                to `source_column` when omitted.
            source_alias (str): SQL alias of the source relation.
            target_alias (str): SQL alias of the target relation.
            source_dtype (pl.DataType | None): Probed source dtype, used to drop
                sentinels the column cannot hold. Each side is filtered
                separately because the two relations can disagree on a type.
                When None, sentinels are emitted unfiltered.
            target_dtype (pl.DataType | None): Probed target dtype.

        Returns:
            str: Boolean SQL expression that is true when the column values match.

        Raises:
            ConfigError: If `datetime_format` uses a directive this dialect
                cannot express.
            ConnectorError: If identifiers are empty or not allowlisted.
        """
        tgt_name = target_column if target_column is not None else source_column
        src_expr = self._normalize_expr(
            self._qualify(source_alias, source_column),
            rule,
            source_dtype,
            is_source=True,
        )
        tgt_expr = self._normalize_expr(
            self._qualify(target_alias, tgt_name),
            rule,
            target_dtype,
            is_source=False,
        )
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
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
    ) -> str:
        """Assemble a changed-row inner-join query from tables, keys, and rules.

        Stages 1 through 7 run once per column in a pair of CTEs. The join and
        the match predicates then read those projected values, so adding a
        stage no longer copies the entire expression tree.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys present on both relations.
            rules (list[DiffRule]): Per-column semantic overrides.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes, used to drop
                null sentinels the column cannot hold. When None, sentinels are
                emitted unfiltered.
            target_types (ColumnTypes | None): Probed target dtypes.

        Returns:
            str: `SELECT ... FROM src INNER JOIN tgt ON ...` statement. A `WHERE NOT`
            clause is added when at least one non-ignored column predicate exists.

        Raises:
            ConnectorError: If tables or keys are empty, a rule is pattern-only, or
                `rename_to` is used with multiple `column_names`.
        """
        if not primary_keys:
            raise ConnectorError("At least one primary key is required for pushdown joins.")

        compared = self._compared_columns(rules)
        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            primary_keys,
            compared,
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        select_list = ", ".join(self._qualify(source_alias, pk) for pk in primary_keys)
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        statement = (
            f"{with_clause} "
            f"SELECT {select_list} "
            f"FROM {self._quote_ident('_src_normalized')} AS {self._quote_ident(source_alias)} "
            f"INNER JOIN {self._quote_ident('_tgt_normalized')} AS {self._quote_ident(target_alias)} "
            f"ON {on_clause}"
        )
        predicates = [
            self._compare(
                self._qualify(source_alias, target_column),
                self._qualify(target_alias, target_column),
                rule,
            )
            for _source_column, target_column, rule in compared
        ]
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
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
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
            source_types (ColumnTypes | None): Probed source dtypes, used to drop
                null sentinels the column cannot hold. When None, sentinels are
                emitted unfiltered.
            target_types (ColumnTypes | None): Probed target dtypes.

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

        compared = self._compared_columns(rules)
        if not compared:
            return None

        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            primary_keys,
            compared,
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        terms: list[str] = []
        for _source_column, target_column, rule in compared:
            predicate = self._compare(
                self._qualify(source_alias, target_column),
                self._qualify(target_alias, target_column),
                rule,
            )
            alias = self._quote_ident(target_column)
            terms.append(
                f"SUM(CASE WHEN COALESCE({predicate}, FALSE) THEN 0 ELSE 1 END) AS {alias}"
            )
        select_list = ", ".join(terms)
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        return (
            f"{with_clause} "
            f"SELECT {select_list} "
            f"FROM {self._quote_ident('_src_normalized')} AS {self._quote_ident(source_alias)} "
            f"INNER JOIN {self._quote_ident('_tgt_normalized')} AS {self._quote_ident(target_alias)} "
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

    def _normalize_expr(
        self,
        expr: str,
        rule: DiffRule,
        dtype: pl.DataType | None,
        *,
        is_source: bool,
    ) -> str:
        """Apply stages 1 through 7 to one side of a comparison.

        Stage 6b, `timezone`, emits nothing on purpose. See
        `_reject_unzoned_timezone` in the engine for why.

        Args:
            expr (str): Qualified column or already-wrapped expression.
            rule (DiffRule): Rule supplying the transform fields.
            dtype (pl.DataType | None): Probed dtype for this side.
            is_source (bool): True when this is the source side, which is the
                only side that receives a `value_map`.

        Returns:
            str: Expression after every compiled stage.
        """
        expr = self._apply_null_values(expr, self._sentinels_for(rule, dtype))
        if self._is_text_side(dtype):
            expr = self._apply_regex_replace(expr, rule)
            expr = self._apply_whitespace(expr, rule)
            expr = self._apply_case(expr, rule)
            if is_source:
                expr = self._apply_value_map(expr, rule)
        expr = self._apply_pad_zeros(expr, rule)
        expr = self._apply_datetime_format(expr, rule, dtype)
        return self._apply_cast(expr, rule, dtype)

    def _compared_columns(self, rules: list[DiffRule]) -> list[tuple[str, str, DiffRule]]:
        """Resolve the columns that a join query will actually compare.

        First rule to name a target-side alias wins, matching the local engine.

        Args:
            rules (list[DiffRule]): Rules to expand.

        Returns:
            list[tuple[str, str, DiffRule]]: Source name, target name, rule.

        Raises:
            ConnectorError: If a rule is pattern-only or `rename_to` is invalid.
        """
        collected: dict[str, tuple[str, str, DiffRule]] = {}
        for rule in rules:
            if rule.pattern is not None and not rule.column_names:
                raise ConnectorError(
                    "Pattern-only DiffRule cannot be compiled without a resolved column name."
                )
            if rule.ignore or not rule.column_names:
                continue
            if rule.rename_to is not None and len(rule.column_names) != 1:
                raise ConnectorError(
                    "rename_to is only valid when column_names has exactly one entry."
                )
            for column in rule.column_names:
                target_column = rule.rename_to if rule.rename_to is not None else column
                collected.setdefault(target_column, (column, target_column, rule))
        return list(collected.values())

    def _normalized_with_clause(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        compared: list[tuple[str, str, DiffRule]],
        *,
        source_alias: str,
        target_alias: str,
        source_types: ColumnTypes | None,
        target_types: ColumnTypes | None,
    ) -> str:
        """Build the CTE pair that applies stages 1-7 once per column.

        Nesting the same expression into every later predicate used to copy the
        source column once per stage that repeats its input. Projecting the
        normalized value once keeps later SQL linear in the number of columns.

        Args:
            source_table (str): Source relation.
            target_table (str): Target relation.
            primary_keys (list[str]): Keys passed through untransformed.
            compared (list[tuple[str, str, DiffRule]]): Columns to normalize.
            source_alias (str): Alias of the source relation inside its CTE.
            target_alias (str): Alias of the target relation inside its CTE.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.

        Returns:
            str: `WITH src AS (...), tgt AS (...)` prefix, no trailing keyword.
        """
        src_select = self._normalized_select(
            source_table,
            source_alias,
            primary_keys,
            compared,
            types=source_types,
            is_source=True,
        )
        tgt_select = self._normalized_select(
            target_table,
            target_alias,
            primary_keys,
            compared,
            types=target_types,
            is_source=False,
        )
        return (
            f"WITH {self._quote_ident('_src_normalized')} AS ({src_select}), "
            f"{self._quote_ident('_tgt_normalized')} AS ({tgt_select})"
        )

    def _normalized_select(
        self,
        table: str,
        alias: str,
        primary_keys: list[str],
        compared: list[tuple[str, str, DiffRule]],
        *,
        types: ColumnTypes | None,
        is_source: bool,
    ) -> str:
        """Project keys plus one normalized expression per compared column.

        Args:
            table (str): Relation to read.
            alias (str): Alias assigned to that relation.
            primary_keys (list[str]): Keys passed through as-is.
            compared (list[tuple[str, str, DiffRule]]): Columns to normalize.
            types (ColumnTypes | None): Probed dtypes for this side.
            is_source (bool): True when projecting the source relation.

        Returns:
            str: `SELECT ... FROM relation AS alias` body for one CTE.
        """
        projections = [
            f"{self._qualify(alias, pk)} AS {self._quote_ident(pk)}" for pk in primary_keys
        ]
        for source_column, target_column, rule in compared:
            raw_name = source_column if is_source else target_column
            dtype = None if types is None else types.get(raw_name)
            normalized = self._normalize_expr(
                self._qualify(alias, raw_name), rule, dtype, is_source=is_source
            )
            projections.append(f"{normalized} AS {self._quote_ident(target_column)}")
        return (
            f"SELECT {', '.join(projections)} "
            f"FROM {self._quote_relation(table)} AS {self._quote_ident(alias)}"
        )

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
        quote = _IDENTIFIER_QUOTES[self.dialect]
        return quote + name.replace(quote, quote * 2) + quote

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

    def _apply_null_values(self, expr: str, sentinels: Sequence[SentinelValue]) -> str:
        """Coerce sentinel values to NULL with a single `CASE` expression.

        Sentinels are stage 1, so `expr` is always a bare qualified column here
        and repeating it costs nothing.

        Args:
            expr (str): SQL expression to sanitize.
            sentinels (Sequence[SentinelValue]): Sentinels already filtered to
                the ones this column's type can hold.

        Returns:
            str: `CASE WHEN expr IN (...) THEN NULL ELSE expr END`, or the
            original expression when no sentinel applies.
        """
        if not sentinels:
            return expr
        rendered = ", ".join(self._sentinel_literal(value) for value in sentinels)
        return f"CASE WHEN {expr} IN ({rendered}) THEN NULL ELSE {expr} END"

    def _sentinel_literal(self, value: SentinelValue) -> str:
        """Render one sentinel as a SQL literal of its own type.

        Numbers and booleans must not be quoted. Emitting `'-999'` against a
        numeric column reintroduces the cast error that type filtering exists to
        prevent. Only strings can carry SQL text, and those still go through
        `_literal` for apostrophe escaping.

        Args:
            value (SentinelValue): Configured sentinel.

        Returns:
            str: Dialect-portable literal.
        """
        # bool first, since isinstance(True, int) is True in Python.
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return self._number(value)
        return self._literal(value)

    def _sentinels_for(self, rule: DiffRule, dtype: pl.DataType | None) -> list[SentinelValue]:
        """Select the sentinels applicable to one side of a comparison.

        Args:
            rule (DiffRule): Rule providing `null_values`.
            dtype (pl.DataType | None): Probed dtype for that side. When None,
                no schema was supplied and every sentinel is emitted as-is.

        Returns:
            list[SentinelValue]: Sentinels to emit for this side.
        """
        if not rule.null_values:
            return []
        if dtype is None:
            return list(rule.null_values)
        return usable_sentinels(rule.null_values, dtype)

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
        """Map source values with nested `IFF` on Snowflake, `CASE` elsewhere.

        The two forms are equivalent. This is the one place the dialects differ
        structurally rather than by a keyword, so it is a branch instead of a
        table: Databricks and DuckDB both take the `CASE` path, DuckDB because
        it has no `IFF` at all.

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

    def _is_text_side(self, dtype: pl.DataType | None) -> bool:
        """Return whether stages 2 through 4 apply to one side of a comparison.

        Matches the local engine's gate exactly, including its narrowness:
        `Categorical` and `Enum` are excluded there because the Polars `.str`
        namespace rejects them, so a warehouse that would happily run `TRIM` on
        the same column must decline too. Parity is with the engine's behavior,
        not with what the dialect could manage.

        Args:
            dtype (pl.DataType | None): Probed dtype, or None when no schema was
                supplied and the caller has taken responsibility for the types.

        Returns:
            bool: True when the text stages should be emitted for this side.
        """
        return dtype is None or isinstance(dtype, (pl.String, pl.Utf8))

    def _apply_pad_zeros(self, expr: str, rule: DiffRule) -> str:
        """Left-pad with zeros the way Python's `str.zfill` does.

        `LPAD` alone is not a substitute. It pads in front of a sign, turning
        `-12` into `0-12` where Polars produces `-012`, and it truncates input
        longer than the target width where Polars leaves it untouched. Both
        divergences are silent, so the padding is spelled out instead.

        The cast to text happens even at width zero, because the local engine
        stringifies unconditionally and later stages branch on whether the
        column is text by then.

        Args:
            expr (str): SQL expression to pad.
            rule (DiffRule): Rule providing `pad_zeros`.

        Returns:
            str: Padded expression, or `expr` when `pad_zeros` is unset. NULL
            input stays NULL: every branch below propagates it.
        """
        if rule.pad_zeros is None:
            return expr
        text = f"CAST({expr} AS {self._cast_keyword('String')})"
        width = rule.pad_zeros
        if width == 0:
            return text
        sign = f"SUBSTR({text}, 1, 1)"
        return (
            f"CASE WHEN LENGTH({text}) >= {width} THEN {text} "
            f"WHEN {sign} IN ('-', '+') "
            f"THEN {sign} || LPAD(SUBSTR({text}, 2), {width - 1}, '0') "
            f"ELSE LPAD({text}, {width}, '0') END"
        )

    def _apply_datetime_format(self, expr: str, rule: DiffRule, dtype: pl.DataType | None) -> str:
        """Parse text timestamps with the dialect's non-throwing parser.

        Gated on the value being text by this point, exactly as the local engine
        gates it: either the column is text or stage 5 stringified it.

        Args:
            expr (str): SQL expression to parse.
            rule (DiffRule): Rule providing `datetime_format`.
            dtype (pl.DataType | None): Probed dtype for this side.

        Returns:
            str: Parse call, or `expr` when the stage does not apply.

        Raises:
            ConfigError: If the format uses a directive or literal character
                this dialect cannot express.
        """
        if not rule.datetime_format:
            return expr
        if rule.pad_zeros is None and not self._is_text_side(dtype):
            return expr
        pattern = self._translate_datetime_format(rule.datetime_format)
        return f"{_PARSE_FUNCTIONS[self.dialect]}({expr}, {self._literal(pattern)})"

    def _translate_datetime_format(self, fmt: str) -> str:
        """Rewrite a Python `strptime` format in the dialect's format language.

        Walks the string one token at a time against `_STRPTIME_DIRECTIVES`
        rather than substituting patterns over the whole string. A substitution
        pass has no way to tell a directive from the same letters appearing as
        literal text, and it leaves anything it does not recognize in place,
        where it becomes part of the emitted format.

        Runs of literal text are wrapped in the dialect's quoting so a separator
        can never be mistaken for a format element.

        Args:
            fmt (str): Python/C `strptime` format string from the config.

        Returns:
            str: Equivalent format in the active dialect's language.

        Raises:
            ConfigError: If a directive is unsupported, a literal character is
                outside `_FORMAT_LITERALS`, or the string ends mid-directive.
        """
        directives = _STRPTIME_DIRECTIVES[self.dialect]
        quote = _FORMAT_LITERAL_QUOTES[self.dialect]
        out: list[str] = []
        literal: list[str] = []

        def flush() -> None:
            if literal:
                out.append(f"{quote}{''.join(literal)}{quote}")
                literal.clear()

        index = 0
        while index < len(fmt):
            char = fmt[index]
            if char != "%":
                if char not in _FORMAT_LITERALS:
                    allowed = "".join(sorted(_FORMAT_LITERALS))
                    raise ConfigError(
                        f"datetime_format '{fmt}' contains the literal character "
                        f"'{char}', which SQL pushdown cannot quote. Allowed "
                        f"separators: {allowed!r}."
                    )
                literal.append(char)
                index += 1
                continue

            if index + 1 >= len(fmt):
                raise ConfigError(f"datetime_format '{fmt}' ends with a dangling '%'.")
            code = fmt[index + 1]
            mapped = directives.get(code)
            if mapped is None:
                supported = ", ".join(f"%{key}" for key in directives)
                raise ConfigError(
                    f"datetime_format '{fmt}' uses '%{code}', which SQL pushdown "
                    f"cannot translate for {self.dialect.value}. Supported "
                    f"directives: {supported}."
                )
            flush()
            out.append(mapped)
            index += 2

        flush()
        return "".join(out)

    def _apply_cast(self, expr: str, rule: DiffRule, dtype: pl.DataType | None) -> str:
        """Cast to the configured target type using the dialect's keyword.

        Args:
            expr (str): SQL expression to cast.
            rule (DiffRule): Rule providing `cast_to`.
            dtype (pl.DataType | None): Probed dtype for this side, used to
                detect the float-to-integer case below.

        Returns:
            str: `CAST(expr AS keyword)`, or `expr` when `cast_to` is unset.

        Raises:
            ConnectorError: If `cast_to` has no keyword for this dialect.
        """
        if rule.cast_to is None:
            return expr
        if rule.cast_to == "Int64" and self._precast_is_float(rule, dtype):
            # Polars truncates a float toward zero on the way to an integer.
            # Snowflake and DuckDB round instead, so 10.7 would compare as 11
            # under pushdown and 10 locally. Truncate explicitly rather than
            # inherit whichever behavior the warehouse happens to have.
            expr = f"CASE WHEN {expr} < 0 THEN CEIL({expr}) ELSE FLOOR({expr}) END"
        return f"CAST({expr} AS {self._cast_keyword(rule.cast_to)})"

    def _precast_is_float(self, rule: DiffRule, dtype: pl.DataType | None) -> bool:
        """Return whether stage 7 receives a floating-point value on this side.

        Only floats need the truncation guard. `Decimal` rounds to integers in
        Polars exactly as SQL does, and every earlier stage that fires leaves
        text or a timestamp behind rather than a float.

        Args:
            rule (DiffRule): Rule whose earlier stages may have changed the type.
            dtype (pl.DataType | None): Probed dtype, or None when unprobed.

        Returns:
            bool: True only when the value reaching the cast is still a float.
        """
        if rule.pad_zeros is not None or rule.datetime_format or rule.timezone:
            return False
        return dtype is not None and dtype.is_float()

    def _cast_keyword(self, target: CastTarget) -> str:
        """Look up the dialect keyword for a cast target.

        Args:
            target (CastTarget): Validated `cast_to` value.

        Returns:
            str: SQL type keyword for the active dialect.

        Raises:
            ConnectorError: If the target has no mapping. Unreachable through a
                validated config, and deliberately fatal if a new cast target is
                ever added without a keyword for every dialect.
        """
        keyword = _CAST_KEYWORDS[self.dialect].get(target)
        if keyword is None:
            raise ConnectorError(f"SQL pushdown has no {self.dialect.value} type for '{target}'.")
        return keyword

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
            if self.dialect is SQLDialect.DATABRICKS:
                return f"{src_expr} <=> {tgt_expr}"
            return f"{src_expr} IS NOT DISTINCT FROM {tgt_expr}"

        return f"{src_expr} = {tgt_expr}"
