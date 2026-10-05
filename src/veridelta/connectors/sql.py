# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Zero-dependency SQL pushdown compiler for warehouse dialects.

Translates `DiffRule` models into Snowflake, Databricks, BigQuery, and Postgres SQL predicates and
assembles inner-join mismatch queries, per-column mismatch tallies, anti-join
queries for added and removed rows, row counts, and column probes without
extracting source tables. It also compiles the one statement a database source
reads its `table` with, so every SQL string Veridelta builds is assembled here.

Every dialect-specific spelling lives in a table at module scope rather than in
the method that needs it. Adding a dialect is then a matter of filling in the
tables, and a missing entry fails loudly instead of inheriting some other
dialect's syntax.
"""

import re
from collections.abc import Collection, Mapping, Sequence
from enum import StrEnum
from typing import Final, NamedTuple

import polars as pl

from veridelta.exceptions import ConfigError, ConnectorError
from veridelta.models import SQL_IDENTIFIER_SEGMENT, CastTarget, DiffRule, SentinelValue
from veridelta.sentinels import usable_sentinels

ColumnTypes = Mapping[str, pl.DataType]
"""Column name to dtype, as probed from a warehouse relation."""

COUNT_ALIAS = "_veridelta_total"
"""Column alias projected by `compile_count_query` so results stay dialect-neutral."""

SCHEMA_ALIAS = "_veridelta_schema"
"""Derived-table alias used by `compile_result_schema_query` around a prior statement."""

DUPLICATE_ROWS_ALIAS = "_veridelta_rows"
"""Per-key row count that `compile_duplicate_key_query` sums over duplicated keys."""

KEYS_ALIAS = "_veridelta_keys"
"""Derived-table alias for the normalized keys `compile_duplicate_key_query` groups."""

DUPLICATES_ALIAS = "_veridelta_duplicates"
"""Derived-table alias for the key groups that occur more than once."""

VALUE_MAP_COLUMN_ALIAS = "_veridelta_column"
"""Integer label of the candidate column a `compile_value_map_query` row counts."""

VALUE_MAP_SOURCE_ALIAS = "_veridelta_source_value"
"""Source value, as the `value_map` stage sees it, in a value map evidence row."""

VALUE_MAP_TARGET_ALIAS = "_veridelta_target_value"
"""Target value that source value lines up with."""

VALUE_MAP_ROWS_ALIAS = "_veridelta_value_rows"
"""Joined rows holding the source value, whatever their target."""

VALUE_MAP_AGREEING_ALIAS = "_veridelta_agreeing_rows"
"""Those rows whose target is the target value."""

SAMPLE_KEY_PREFIX = "_veridelta_key_"
"""Positional alias prefix for each primary key in a changed-row sample."""

SAMPLE_SOURCE_PREFIX = "_veridelta_source_"
"""Positional alias prefix for each compared column's source value in a sample."""

SAMPLE_TARGET_PREFIX = "_veridelta_target_"
"""Positional alias prefix for each compared column's target value in a sample."""

SAMPLE_MATCH_PREFIX = "_veridelta_match_"
"""Positional alias prefix for each compared column's match flag in a sample."""


class SampleQuery(NamedTuple):
    """A changed-row sample statement and the names its positional aliases stand for.

    Attributes:
        statement (str): The SQL to run.
        renames (dict[str, str]): Positional alias to the name the local engine
            gives that column: each key's own name, then `{column}_source`,
            `{column}_target`, and `{column}_is_match` per compared column.
    """

    statement: str
    renames: dict[str, str]


SAMPLE_BUCKETS: Final = 1_000_000
"""Resolution of a value map sample: rows whose key hash falls in the first
`sample_fraction * SAMPLE_BUCKETS` buckets are kept, locally and in SQL."""

_Projection = tuple[str, str, DiffRule | None]
"""Stored source name, projected name, and the rule normalizing it, if any."""


class SQLDialect(StrEnum):
    """Warehouse SQL dialects supported by the pushdown compiler.

    `DUCKDB` has no connector or config model. It exists so the differential
    test harness can execute real compiler output instead of a rewritten
    approximation of it. `POSTGRES` runs where two database sources on one
    Postgres connection opt into pushdown.
    """

    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"
    POSTGRES = "postgres"


# `cast_to` reaches SQL only through this table, since SQL cannot quote or bind a type name.
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
    # DATETIME has no zone, like Polars' `Datetime`; TIMESTAMP is an instant.
    SQLDialect.BIGQUERY: {
        "Int64": "INT64",
        "Float64": "FLOAT64",
        "String": "STRING",
        "Boolean": "BOOL",
        "Date": "DATE",
        "Datetime": "DATETIME",
    },
    SQLDialect.POSTGRES: {
        "Int64": "BIGINT",
        "Float64": "DOUBLE PRECISION",
        "String": "TEXT",
        "Boolean": "BOOLEAN",
        "Date": "DATE",
        "Datetime": "TIMESTAMP",
    },
}
"""`cast_to` value to the type keyword each dialect spells it with."""

# Postgres casts only `integer` to `boolean`, and BigQuery only `INT64` and `STRING` to `BOOL`.
_ZERO_TEST_BOOLEANS: Final[frozenset[SQLDialect]] = frozenset(
    {SQLDialect.POSTGRES, SQLDialect.BIGQUERY}
)
"""Dialects that turn a number into a boolean by comparing it with zero."""

_IDENTIFIER_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "`",
    SQLDialect.DUCKDB: '"',
    SQLDialect.BIGQUERY: "`",
    SQLDialect.POSTGRES: '"',
}
"""Character each dialect quotes identifiers with."""

_LITERAL_ESCAPES: Final[dict[SQLDialect, tuple[tuple[str, str], ...]]] = {
    # Snowflake and Databricks read backslash escapes, so a trailing backslash would escape the
    # closing quote. Backslashes double first, before any other escape adds one.
    SQLDialect.SNOWFLAKE: (("\\", "\\\\"), ("'", "''")),
    # Databricks reads `''` as two adjacent literals and joins them, dropping the apostrophe.
    SQLDialect.DATABRICKS: (("\\", "\\\\"), ("'", "\\'")),
    # DuckDB follows the SQL standard, where a backslash is ordinary text.
    SQLDialect.DUCKDB: (("'", "''"),),
    # BigQuery reads backslash escapes too, rejects unknown ones, and refuses a raw line break.
    SQLDialect.BIGQUERY: (("\\", "\\\\"), ("'", "\\'"), ("\n", "\\n"), ("\r", "\\r")),
    # Postgres follows the standard too; its session checks `standard_conforming_strings` is on.
    SQLDialect.POSTGRES: (("'", "''"),),
}
"""Replacements that keep text inside a single-quoted literal, applied in order."""

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
    # No `f`: BigQuery spells a fraction only as part of its seconds, `%E*S`.
    # `%Ez` also reads `+05:30`, as Polars' `%z` does; BigQuery's `%z` does not.
    SQLDialect.BIGQUERY: {
        "Y": "%Y",
        "m": "%m",
        "d": "%d",
        "H": "%H",
        "M": "%M",
        "S": "%S",
        "z": "%Ez",
        "%": "%%",
    },
}
"""Python `strptime` directive to its spelling in each dialect's format language."""

_FORMAT_LITERAL_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "'",
    # DuckDB and BigQuery mark directives with `%`, so their literals need no wrapper.
    SQLDialect.DUCKDB: "",
    SQLDialect.BIGQUERY: "",
}
"""Character each dialect wraps a literal run of a format string in."""

# The set excludes both quote characters, so a literal run cannot close its quotes early.
_FORMAT_LITERALS: Final[frozenset[str]] = frozenset(" -/:.,_T")
"""Characters allowed between directives in a `datetime_format`."""

_PARSE_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "TRY_TO_TIMESTAMP",
    SQLDialect.DATABRICKS: "try_to_timestamp",
    SQLDialect.DUCKDB: "try_strptime",
    SQLDialect.BIGQUERY: "SAFE.PARSE_DATETIME",
}
"""Each dialect's non-throwing parse, matching Polars `strptime(strict=False)`."""

_BIGQUERY_OFFSET_PARSE: Final = "SAFE.PARSE_TIMESTAMP"
"""BigQuery's non-throwing parse for a format that reads a UTC offset."""

_INFINITY_LITERALS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "'inf'::FLOAT",
    SQLDialect.DATABRICKS: "CAST('Infinity' AS DOUBLE)",
    SQLDialect.DUCKDB: "'inf'::DOUBLE",
    SQLDialect.BIGQUERY: "CAST('inf' AS FLOAT64)",
    SQLDialect.POSTGRES: "'Infinity'::double precision",
}
"""Each dialect's positive infinity, which bounds the finite values a tolerance may apply to."""

_EDIT_DISTANCE_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    # Snowflake's optional third argument caps the result, and Databricks' returns -1 above it
    # and needs Runtime 13.3, so neither is portable.
    SQLDialect.SNOWFLAKE: "EDITDISTANCE",
    SQLDialect.DATABRICKS: "levenshtein",
    # DuckDB counts UTF-8 bytes, not characters, so the parity tests compare ASCII text.
    SQLDialect.DUCKDB: "levenshtein",
    SQLDialect.BIGQUERY: "EDIT_DISTANCE",
}
"""Each dialect's Levenshtein distance, always called with two arguments."""


_REGEX_REPLACE_FLAGS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "",
    SQLDialect.DATABRICKS: "",
    SQLDialect.DUCKDB: ", 'g'",
    SQLDialect.BIGQUERY: "",
    SQLDialect.POSTGRES: ", 'g'",
}
"""Trailing `REGEXP_REPLACE` arguments to replace every match, as Polars' `replace_all` does."""


_REFERENCE_NAME: Final = re.compile(r"[_0-9A-Za-z]+")
"""The name after a bare `$` in a Polars replacement: the longest run of these characters."""

_GROUP_NUMBER: Final = re.compile(r"[0-9]+")
"""A reference name that Polars reads as a group number rather than a group name."""

_HIGHEST_GROUP: Final = 9
"""The highest group a warehouse replacement can refer to: their references are one digit."""


def _reference_at(replacement: str, index: int) -> tuple[str, int] | None:
    """Read the group reference that starts at a `$` in a Polars replacement."""
    if replacement.startswith("{", index + 1):
        close = replacement.find("}", index + 2)
        if close == -1:
            return None
        return replacement[index + 2 : close], close + 1
    # Polars follows the `regex` crate: `$1a` names the group `1a`, not group 1.
    name = _REFERENCE_NAME.match(replacement, index + 1)
    if name is None:
        return None
    return name.group(), name.end()


def _replacement_tokens(pattern: str, replacement: str) -> list[str | int]:
    """Split a Polars replacement into runs of plain text and group numbers."""
    tokens: list[str | int] = []
    text: list[str] = []
    index = 0
    while index < len(replacement):
        if replacement.startswith("$$", index):
            text.append("$")
            index += 2
            continue
        reference = _reference_at(replacement, index) if replacement[index] == "$" else None
        if reference is None:
            text.append(replacement[index])
            index += 1
            continue
        name, index = reference
        if text:
            tokens.append("".join(text))
            text.clear()
        tokens.append(_group_number(pattern, replacement, name))
    if text:
        tokens.append("".join(text))
    return tokens


def _group_number(pattern: str, replacement: str, name: str) -> int:
    """Return the group a reference names, if a warehouse can refer to it."""
    where = f"regex_replace replacement {replacement!r} for pattern {pattern!r}"
    if _GROUP_NUMBER.fullmatch(name) is None:
        raise ConfigError(
            f"{where} refers to a group by name (${{{name}}}), which SQL pushdown cannot "
            "write: warehouses refer to groups by number only. Use the group's number, "
            "and braces to keep it apart from text that follows, as in ${1}a."
        )
    number = int(name)
    if number > _HIGHEST_GROUP:
        raise ConfigError(
            f"{where} refers to group {number}, but warehouses refer to groups 0 through 9 only."
        )
    return number


# Zero-width spaces and byte-order marks are not whitespace to Polars, so they stay out.
_WHITESPACE_CHARACTERS: Final = (
    "\t\n\x0b\x0c\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006"
    "\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
)
"""The characters Polars' `strip_chars` removes: Unicode's `White_Space` set."""

_TRIM_FUNCTIONS: Final[dict[str, str]] = {"left": "LTRIM", "right": "RTRIM", "both": "TRIM"}
"""The trim function for each `whitespace_mode` that strips something."""

_TRIM_SIDES: Final[dict[str, str]] = {"left": "LEADING", "right": "TRAILING", "both": "BOTH"}
"""The side keyword of the standard `TRIM(side characters FROM value)` form, by mode."""


# Thirty-eight digits hold every difference of two 64-bit values, signed or not, and `ABS` of
# the smallest `BIGINT`, which overflows in the column's own type.
_WIDE_INTEGER_TYPES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "NUMBER(38, 0)",
    SQLDialect.DATABRICKS: "DECIMAL(38, 0)",
    SQLDialect.DUCKDB: "DECIMAL(38, 0)",
    # NUMERIC keeps 29 integer digits, more than any INT64 difference needs.
    SQLDialect.BIGQUERY: "NUMERIC",
    SQLDialect.POSTGRES: "NUMERIC(38, 0)",
}
"""An exact integer type wide enough to subtract any two stored integers in."""


# No engine hashes as Polars does: a warehouse sample is repeatable but differs from a local one.
_SAMPLE_HASH_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "HASH",
    SQLDialect.DATABRICKS: "xxhash64",
    SQLDialect.DUCKDB: "hash",
    SQLDialect.BIGQUERY: "FARM_FINGERPRINT",
    SQLDialect.POSTGRES: "hashtextextended",
}
"""Each dialect's hash of several values, for sampling value map evidence by key."""


def _reads_offset(fmt: str) -> bool:
    """Return whether a `strptime` format reads a UTC offset with `%z`."""
    return "%z" in re.findall(r"%.", fmt)


_DATABASE_IDENTIFIER_QUOTES: Final[dict[str, tuple[str, str]]] = {
    "clickhouse": ("`", "`"),
    "mssql": ("[", "]"),
    "mysql": ("`", "`"),
    "oracle": ('"', '"'),
    "postgres": ('"', '"'),
    "postgresql": ('"', '"'),
    "redshift": ('"', '"'),
    "sqlite": ('"', '"'),
}
"""Opening and closing identifier quote for each database URI scheme a `table` may name."""


def _relation_segments(name: str) -> list[str]:
    """Split a possibly dotted relation name into allowlisted segments."""
    trimmed = name.strip()
    if not trimmed:
        raise ConnectorError("Table name must be a non-empty string.")
    parts = trimmed.split(".")
    if len(parts) > 3:
        raise ConnectorError("Table name must have at most three dotted segments.")
    for part in parts:
        if SQL_IDENTIFIER_SEGMENT.fullmatch(part) is None:
            raise ConnectorError("SQL identifier is not a valid unquoted identifier.")
    return parts


def compile_database_select(scheme: str, table: str) -> str:
    """Compile the statement that reads a database source's `table` whole.

    Args:
        scheme (str): Lowercase URI scheme, such as `postgresql`.
        table (str): One to three dotted identifier segments.

    Returns:
        str: `SELECT * FROM` the table, each segment quoted for the database.

    Raises:
        ConfigError: If Veridelta has no quoting for the scheme.
        ConnectorError: If the table name falls outside the identifier allowlist.
    """
    return f"SELECT * FROM {_quoted_database_relation(scheme, table)}"


def _quoted_database_relation(scheme: str, table: str) -> str:
    """Quote each segment of a database source's `table` for the scheme's database."""
    quotes = _DATABASE_IDENTIFIER_QUOTES.get(scheme)
    if quotes is None:
        known = ", ".join(sorted(_DATABASE_IDENTIFIER_QUOTES))
        raise ConfigError(
            f"'table' needs a URI scheme Veridelta knows how to quote; '{scheme}' is not "
            f"one ({known}). Write the statement in 'query' instead."
        )
    opening, closing = quotes
    return ".".join(f"{opening}{part}{closing}" for part in _relation_segments(table))


def compile_database_null_count(scheme: str, table: str, column: str) -> str:
    """Compile a count of a database table's rows whose partition column is NULL.

    ConnectorX writes the partition column into each range's statement without
    quotes, so the count names it the same way, after the same allowlist. The
    database then resolves it as ConnectorX's ranges will, folding its case.

    Args:
        scheme (str): Lowercase URI scheme, such as `postgresql`.
        table (str): One to three dotted identifier segments.
        column (str): One identifier segment.

    Returns:
        str: The count of the table's rows whose `column` is NULL, as `null_rows`.

    Raises:
        ConfigError: If Veridelta has no quoting for the scheme.
        ConnectorError: If the table or column name falls outside the allowlist.
    """
    relation = _quoted_database_relation(scheme, table)
    if SQL_IDENTIFIER_SEGMENT.fullmatch(column) is None:
        raise ConnectorError("SQL identifier is not a valid unquoted identifier.")
    return f"SELECT COUNT(*) AS null_rows FROM {relation} WHERE {column} IS NULL"


def compile_postgres_columns_query(table: str) -> str:
    """Compile a catalog query for a Postgres table's columns and numeric declarations.

    The quoted relation reaches `regclass` as a string literal, so Postgres
    resolves it as the table read does, search path included. The identifier
    allowlist admits no quote character, so the literal cannot be closed early.

    Args:
        table (str): One to three dotted identifier segments.

    Returns:
        str: A query returning `attname`, `atttypmod`, and `is_numeric` for
            each column, in table order.

    Raises:
        ConnectorError: If the table name falls outside the identifier allowlist.
    """
    relation = _quoted_database_relation("postgresql", table)
    return (
        "SELECT attname, atttypmod, atttypid = 'numeric'::regtype AS is_numeric "
        f"FROM pg_attribute WHERE attrelid = '{relation}'::regclass "
        "AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
    )


def compile_postgres_text_select(
    table: str, columns: Sequence[str], as_text: Collection[str], *, probe: bool = False
) -> str:
    """Compile a Postgres table read that returns some columns as text.

    Column names come from Postgres' own catalog, not from configuration, so
    they are quoted by doubling any `"` rather than checked against the
    allowlist. Doubling is the whole escape grammar of a quoted Postgres name.

    Args:
        table (str): One to three dotted identifier segments.
        columns (Sequence[str]): Every column of the table, in table order.
        as_text (Collection[str]): Columns to cast to text.
        probe (bool): Whether to return no rows, for a schema check.

    Returns:
        str: The table read, with the chosen columns cast to text.

    Raises:
        ConnectorError: If the table name falls outside the identifier allowlist.
    """
    projections: list[str] = []
    for name in columns:
        quoted = '"' + name.replace('"', '""') + '"'
        projections.append(f"CAST({quoted} AS TEXT) AS {quoted}" if name in as_text else quoted)
    relation = _quoted_database_relation("postgresql", table)
    read = f"SELECT {', '.join(projections)} FROM {relation}"
    return f"{read} WHERE 1 = 0" if probe else read


def compile_database_probe(scheme: str, table: str) -> str:
    """Compile a statement that returns a database table's columns and no rows.

    Args:
        scheme (str): Lowercase URI scheme, such as `postgresql`.
        table (str): One to three dotted identifier segments.

    Returns:
        str: The table read, filtered by a condition no row satisfies.

    Raises:
        ConfigError: If Veridelta has no quoting for the scheme.
        ConnectorError: If the table name falls outside the identifier allowlist.
    """
    return f"{compile_database_select(scheme, table)} WHERE 1 = 0"


class SQLPushdownCompiler:
    """Compile `DiffRule` semantics into dialect-specific SQL strings.

    One instance targets one `SQLDialect`. The engine drives a warehouse run
    with these statements, in this order:

    1. `compile_schema_probe_query` per side, to learn column names and types.
    2. `compile_duplicate_key_query` per side, to assert normalized keys are
       unique before anything joins on them.
    3. `compile_count_query` per side, for the `threshold` denominator.
    4. `compile_query` for inner-join rows where a compared column differs.
    5. `compile_added_query` and `compile_missing_query` for the anti-joins.
    6. `compile_column_mismatch_query` for the per-column tally.

    A value map proposal runs steps 1 and 2, then `compile_value_map_query`.

    Every join reads keys through the same stages 1-7 as compared columns,
    driven by `key_rules`, so rows match on the keys the local engine sees.

    `compile_result_schema_query` wraps any of the above so a connector can
    describe a result without re-running it. Rules reach the compiler already
    folded over the configuration's `default_*` settings, so every compared
    column arrives as one fully specified `DiffRule`.

    The compiler is also the security boundary for warehouse SQL. Identifiers
    are allowlisted segment by segment and then dialect-quoted, data literals
    are escaped through `_literal`, and every dialect keyword comes from a
    module-level table keyed by `SQLDialect`, so an unsupported combination
    raises rather than borrowing another dialect's spelling.

    Attributes:
        dialect (SQLDialect): Target dialect for quoting, casts, and functions.
    """

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
        rather than differing by whatever the warehouse happened to support. The
        one setting refused is `min_jaro_winkler_similarity`, which no supported
        warehouse scores the way the local engine does.

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
                cannot express, or the rule sets `min_jaro_winkler_similarity`.
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
        key_rules: Sequence[DiffRule] | None = None,
        wide_integers: frozenset[str] = frozenset(),
        type_drift: frozenset[str] = frozenset(),
    ) -> str:
        """Assemble a changed-row inner-join query from tables, keys, and rules.

        Stages 1 through 7 run once per column in a pair of CTEs, keys
        included. The join and the match predicates then read those projected
        values, so each stage appears once in the statement however many
        predicates read it.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            rules (list[DiffRule]): Per-column semantic overrides.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes, used to drop
                null sentinels the column cannot hold. When None, sentinels are
                emitted unfiltered.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): One rule per key that needs
                normalizing, naming the stored source column and, for a renamed
                key, its `rename_to`. Keys without one are joined as stored.
            wide_integers (frozenset[str]): Compared columns, by target name,
                that hold integers on both sides after normalization. Their
                tolerance is measured in `_WIDE_INTEGER_TYPES`, so a difference
                wider than the stored type neither wraps nor overflows.
            type_drift (frozenset[str]): Compared columns, by target name, that
                `strict_types` fails because the two sides hold different
                types after normalization. No value of theirs ever matches, and
                two NULLs meet only under `treat_null_as_equal`.

        Returns:
            str: `SELECT ... FROM src INNER JOIN tgt ON ... WHERE NOT (...)` statement
            keeping the joined rows where at least one compared column differs.
            Each predicate is wrapped in `COALESCE(pred, FALSE)` so a one-sided
            NULL reads as a mismatch rather than as an unknown that `WHERE`
            drops, matching the local engine's `fill_null(False)`. When no
            column is compared the statement selects no rows, since without
            match expressions the local engine reports nothing as changed.

        Raises:
            ConfigError: If a rule sets `min_jaro_winkler_similarity`, or a
                `datetime_format` uses a directive this dialect cannot express.
            ConnectorError: If tables or keys are empty, a rule is pattern-only,
                `rename_to` is used with multiple `column_names`, or a key rule
                does not name exactly one primary key.
        """
        keys = self._key_columns(primary_keys, key_rules)
        compared = self._compared_columns(rules)
        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            [*keys, *compared],
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        select_list = ", ".join(self._qualify(source_alias, pk) for pk in primary_keys)
        join = self._normalized_join(
            "INNER", primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        statement = f"{with_clause} SELECT {select_list} {join}"
        predicates = self._column_predicates(
            compared,
            source_alias=source_alias,
            target_alias=target_alias,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )
        if not predicates:
            # Nothing to compare means nothing can have changed. Without this
            # guard the bare join would report every shared key as drift.
            return f"{statement} WHERE 1 = 0"
        return f"{statement} WHERE {self._changed_condition(predicates)}"

    def compile_changed_sample_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rules: list[DiffRule],
        *,
        limit: int,
        source_alias: str = "src",
        target_alias: str = "tgt",
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
        key_rules: Sequence[DiffRule] | None = None,
        wide_integers: frozenset[str] = frozenset(),
        type_drift: frozenset[str] = frozenset(),
    ) -> SampleQuery | None:
        """Assemble a query for the first changed rows, with both sides' values.

        This is `compile_query` with values: the same normalized CTEs, join,
        and WHERE clause, so every sampled row is one `compile_query` reports
        as changed, and each match flag is the predicate that decided it. Each
        output column takes a positional alias, so a long column name cannot
        pass an identifier limit and a key cannot clash with a suffixed column;
        `SampleQuery.renames` gives the local engine's names back. Rows come in
        key order, so the same tables give the same sample.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            rules (list[DiffRule]): Per-column semantic overrides.
            limit (int): Most rows to return, at least 1.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes, as for
                `compile_query`.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.
            wide_integers (frozenset[str]): Integer columns to measure in a wide
                type, as for `compile_query`.
            type_drift (frozenset[str]): Columns `strict_types` fails, as for
                `compile_query`.

        Returns:
            SampleQuery | None: The statement and its alias names, or None when
            no column is compared, since then no row can have changed.

        Raises:
            ConfigError: If a rule sets `min_jaro_winkler_similarity`, or a
                `datetime_format` uses a directive this dialect cannot express.
            ConnectorError: If `limit` is not a positive `int`, tables or keys
                are empty, a rule is pattern-only, `rename_to` is used with
                multiple `column_names`, or a key rule does not name exactly one
                primary key.
        """
        rendered_limit = self._integer(limit)
        if limit < 1:
            raise ConnectorError(f"A row sample needs a LIMIT of at least 1, got {limit}.")
        keys = self._key_columns(primary_keys, key_rules)
        compared = self._compared_columns(rules)
        if not compared:
            return None

        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            [*keys, *compared],
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        predicates = self._column_predicates(
            compared,
            source_alias=source_alias,
            target_alias=target_alias,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )
        renames: dict[str, str] = {}
        projections: list[str] = []
        for index, key in enumerate(primary_keys):
            alias = f"{SAMPLE_KEY_PREFIX}{index}"
            projections.append(f"{self._qualify(source_alias, key)} AS {self._quote_ident(alias)}")
            renames[alias] = key
        for index, ((_source_column, target_column, _rule), predicate) in enumerate(
            zip(compared, predicates, strict=True)
        ):
            outputs = (
                (SAMPLE_SOURCE_PREFIX, self._qualify(source_alias, target_column), "source"),
                (SAMPLE_TARGET_PREFIX, self._qualify(target_alias, target_column), "target"),
                (SAMPLE_MATCH_PREFIX, f"COALESCE({predicate}, FALSE)", "is_match"),
            )
            for prefix, expression, suffix in outputs:
                alias = f"{prefix}{index}"
                projections.append(f"{expression} AS {self._quote_ident(alias)}")
                renames[alias] = f"{target_column}_{suffix}"
        join = self._normalized_join(
            "INNER", primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        order = ", ".join(
            self._quote_ident(f"{SAMPLE_KEY_PREFIX}{index}") for index in range(len(primary_keys))
        )
        statement = (
            f"{with_clause} SELECT {', '.join(projections)} {join} "
            f"WHERE {self._changed_condition(predicates)} ORDER BY {order} LIMIT {rendered_limit}"
        )
        return SampleQuery(statement, renames)

    def compile_missing_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
        key_rules: Sequence[DiffRule] | None = None,
    ) -> str:
        """Assemble a LEFT JOIN anti-join for rows present only in the source.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.

        Returns:
            str: Source keys whose normalized value has no target counterpart.

        Raises:
            ConnectorError: If tables or keys are empty, or a key rule does not
                name exactly one primary key.
        """
        return self._compile_anti_join(
            source_table,
            target_table,
            primary_keys,
            join_kind="LEFT",
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
        )

    def compile_added_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        source_alias: str = "src",
        target_alias: str = "tgt",
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
        key_rules: Sequence[DiffRule] | None = None,
    ) -> str:
        """Assemble a RIGHT JOIN anti-join for rows present only in the target.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.

        Returns:
            str: Target keys whose normalized value has no source counterpart.

        Raises:
            ConnectorError: If tables or keys are empty, or a key rule does not
                name exactly one primary key.
        """
        return self._compile_anti_join(
            source_table,
            target_table,
            primary_keys,
            join_kind="RIGHT",
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
            key_rules=key_rules,
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
        key_rules: Sequence[DiffRule] | None = None,
        wide_integers: frozenset[str] = frozenset(),
        type_drift: frozenset[str] = frozenset(),
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
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.
            wide_integers (frozenset[str]): Integer columns to measure in a wide
                type, as for `compile_query`.
            type_drift (frozenset[str]): Columns `strict_types` fails, as for
                `compile_query`.

        Returns:
            str | None: Single-row aggregate statement, or None when no rule
            yields a comparable column, mirroring the local engine's decision to
            skip the tally when there are no match expressions.

        Raises:
            ConfigError: If a rule sets `min_jaro_winkler_similarity`, or a
                `datetime_format` uses a directive this dialect cannot express.
            ConnectorError: If tables or keys are empty, a rule is pattern-only,
                `rename_to` is used with multiple `column_names`, or a key rule
                does not name exactly one primary key.
        """
        keys = self._key_columns(primary_keys, key_rules)
        compared = self._compared_columns(rules)
        if not compared:
            return None

        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            [*keys, *compared],
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        predicates = self._column_predicates(
            compared,
            source_alias=source_alias,
            target_alias=target_alias,
            wide_integers=wide_integers,
            type_drift=type_drift,
        )
        terms = [
            f"SUM(CASE WHEN COALESCE({predicate}, FALSE) THEN 0 ELSE 1 END) "
            f"AS {self._quote_ident(target_column)}"
            for (_source_column, target_column, _rule), predicate in zip(
                compared, predicates, strict=True
            )
        ]
        join = self._normalized_join(
            "INNER", primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        return f"{with_clause} SELECT {', '.join(terms)} {join}"

    def compile_value_map_query(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        rules: list[DiffRule],
        *,
        min_support: int,
        sample_fraction: float = 1.0,
        source_alias: str = "src",
        target_alias: str = "tgt",
        source_types: ColumnTypes | None = None,
        target_types: ColumnTypes | None = None,
        key_rules: Sequence[DiffRule] | None = None,
    ) -> str | None:
        """Assemble one statement that counts how source and target values line up.

        Keys and candidate columns are normalized in the usual CTE pair and
        joined once. Each column then contributes one `UNION ALL` branch,
        labeled by its position in `rules` rather than by name, so no column
        name becomes a string literal. A branch leaves out NULL sources and
        values the column's existing map produced, as the local engine does.

        Only exact predicates run here: the target differs from the source, at
        least `min_support` rows agree, and the agreeing rows are more than
        half of the source value's rows. Every confidence floor is above one
        half, so this keeps a superset of what qualifies, and the engine
        applies the floor itself, in floating point exactly as locally.

        Args:
            source_table (str): Source relation (optionally dotted catalog path).
            target_table (str): Target relation (optionally dotted catalog path).
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            rules (list[DiffRule]): One rule per candidate column, naming its
                stored source column and, when renamed, the target's name.
            min_support (int): Agreeing rows a pair needs.
            sample_fraction (float): Share of source keys to read, chosen by a
                hash of the normalized keys. 1 reads every row.
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.

        Returns:
            str | None: A statement returning `VALUE_MAP_COLUMN_ALIAS`,
            `VALUE_MAP_SOURCE_ALIAS`, `VALUE_MAP_TARGET_ALIAS`,
            `VALUE_MAP_ROWS_ALIAS`, and `VALUE_MAP_AGREEING_ALIAS` per pair, or
            None when there is no candidate column.

        Raises:
            ConnectorError: If tables or keys are empty, a rule is pattern-only,
                a key rule does not name exactly one primary key, or
                `min_support` is not an integer.
        """
        keys = self._key_columns(primary_keys, key_rules)
        compared = self._compared_columns(rules)
        if not compared:
            return None
        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            [*keys, *compared],
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        joined = self._value_map_joined(
            [target for _source, target, _rule in compared],
            primary_keys,
            sample_fraction,
            source_alias=source_alias,
            target_alias=target_alias,
        )
        branches = " UNION ALL ".join(
            self._value_map_branch(label, rule)
            for label, (_source, _target, rule) in enumerate(compared)
        )
        return f"{with_clause}, {joined} {self._value_map_tally(branches, min_support)}"

    def _value_map_joined(
        self,
        columns: list[str],
        primary_keys: list[str],
        sample_fraction: float,
        *,
        source_alias: str,
        target_alias: str,
    ) -> str:
        """Build the CTE pairing each candidate's normalized values on the keys."""
        projections = ", ".join(
            f"{self._qualify(source_alias, column)} AS {self._quote_ident(f'_veridelta_source_{label}')}, "
            f"{self._qualify(target_alias, column)} AS {self._quote_ident(f'_veridelta_target_{label}')}"
            for label, column in enumerate(columns)
        )
        join = self._normalized_join(
            "INNER", primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        sample = ""
        if sample_fraction < 1:
            keys = [self._qualify(source_alias, key) for key in primary_keys]
            cutoff = self._integer(round(sample_fraction * SAMPLE_BUCKETS))
            sample = f" WHERE {self._sample_bucket(keys)} < {cutoff}"
        return f"{self._quote_ident('_veridelta_joined')} AS (SELECT {projections} {join}{sample})"

    def _sample_bucket(self, keys: list[str]) -> str:
        """Hash qualified keys into one of `SAMPLE_BUCKETS` buckets."""
        joined = ", ".join(keys)
        function = _SAMPLE_HASH_FUNCTIONS[self.dialect]
        buckets = self._integer(SAMPLE_BUCKETS)
        if self.dialect is SQLDialect.DATABRICKS:
            # pmod is always non-negative.
            return f"pmod({function}({joined}), {buckets})"
        if self.dialect is SQLDialect.DUCKDB:
            # DuckDB's hash is unsigned.
            return f"{function}({joined}) % {buckets}"
        # These hashes are signed, and MOD keeps the dividend's sign. FARM_FINGERPRINT
        # takes one string, and hashtextextended one text and a seed.
        hashed = {
            SQLDialect.SNOWFLAKE: f"{function}({joined})",
            SQLDialect.BIGQUERY: f"{function}(TO_JSON_STRING(STRUCT({joined})))",
            SQLDialect.POSTGRES: f"{function}(ROW({joined})::text, 0)",
        }[self.dialect]
        return f"MOD(MOD({hashed}, {buckets}) + {buckets}, {buckets})"

    def _value_map_branch(self, label: int, rule: DiffRule) -> str:
        """Select one candidate's pairs, labeled, without NULL or already mapped sources."""
        source = self._quote_ident(f"_veridelta_source_{label}")
        target = self._quote_ident(f"_veridelta_target_{label}")
        conditions = [f"{source} IS NOT NULL"]
        if rule.value_map:
            outputs = ", ".join(
                self._literal(value) for value in dict.fromkeys(rule.value_map.values())
            )
            conditions.append(f"{source} NOT IN ({outputs})")
        return (
            f"SELECT {self._integer(label)} AS {self._quote_ident(VALUE_MAP_COLUMN_ALIAS)}, "
            f"{source} AS {self._quote_ident(VALUE_MAP_SOURCE_ALIAS)}, "
            f"{target} AS {self._quote_ident(VALUE_MAP_TARGET_ALIAS)} "
            f"FROM {self._quote_ident('_veridelta_joined')} WHERE {' AND '.join(conditions)}"
        )

    def _value_map_tally(self, branches: str, min_support: int) -> str:
        """Count the labeled pairs and keep the ones that can qualify."""
        column = self._quote_ident(VALUE_MAP_COLUMN_ALIAS)
        source = self._quote_ident(VALUE_MAP_SOURCE_ALIAS)
        target = self._quote_ident(VALUE_MAP_TARGET_ALIAS)
        rows = self._quote_ident(VALUE_MAP_ROWS_ALIAS)
        agreeing = self._quote_ident(VALUE_MAP_AGREEING_ALIAS)
        count_type = self._cast_keyword("Int64")
        grouped = (
            f"SELECT {column}, {source}, {target}, COUNT(*) AS {agreeing} "
            f"FROM ({branches}) AS {self._quote_ident('_veridelta_pairs')} "
            f"GROUP BY {column}, {source}, {target}"
        )
        # The per-value total wraps the pair count, a form every supported dialect accepts.
        counted = (
            f"SELECT {column}, {source}, {target}, {agreeing}, "
            f"SUM({agreeing}) OVER (PARTITION BY {column}, {source}) AS {rows} "
            f"FROM ({grouped}) AS {self._quote_ident('_veridelta_groups')}"
        )
        # A NULL target forms its own group: it counts toward the total, then `<>` drops it.
        return (
            f"SELECT {column}, {source}, {target}, "
            f"CAST({rows} AS {count_type}) AS {rows}, "
            f"CAST({agreeing} AS {count_type}) AS {agreeing} "
            f"FROM ({counted}) AS {self._quote_ident('_veridelta_counts')} "
            f"WHERE {target} <> {source} AND {agreeing} >= {self._integer(min_support)} "
            f"AND 2 * {agreeing} > {rows}"
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

    def compile_duplicate_key_query(
        self,
        table: str,
        primary_keys: list[str],
        *,
        is_source: bool,
        key_rules: Sequence[DiffRule] | None = None,
        types: ColumnTypes | None = None,
    ) -> str:
        """Assemble a count of the rows whose normalized key is not unique.

        The local engine asserts uniqueness after normalization and reports
        every row that shares its key with another. Summing the size of each
        key group larger than one is that same number, and `GROUP BY` puts NULL
        keys in one group, as Polars counts them as duplicates of each other.

        Args:
            table (str): Relation to check (optionally dotted catalog path).
            primary_keys (list[str]): Keys, spelled as the target stores them.
            is_source (bool): Whether the relation is the source, which reads a
                renamed key under its stored name and applies `value_map`.
            key_rules (Sequence[DiffRule] | None): Key normalization, as for
                `compile_query`.
            types (ColumnTypes | None): Probed dtypes for this relation.

        Returns:
            str: A single-row statement whose `COUNT_ALIAS` column is zero when
            every normalized key is unique.

        Raises:
            ConnectorError: If the table or keys are empty, or a key rule does
                not name exactly one primary key.
        """
        keys = self._key_columns(primary_keys, key_rules)
        alias = "src" if is_source else "tgt"
        normalized = self._normalized_select(table, alias, keys, types=types, is_source=is_source)
        key_list = ", ".join(self._quote_ident(pk) for pk in primary_keys)
        rows = self._quote_ident(DUPLICATE_ROWS_ALIAS)
        return (
            f"SELECT COALESCE(SUM({rows}), 0) AS {self._quote_ident(COUNT_ALIAS)} "
            f"FROM (SELECT COUNT(*) AS {rows} "
            f"FROM ({normalized}) AS {self._quote_ident(KEYS_ALIAS)} "
            f"GROUP BY {key_list} HAVING COUNT(*) > 1) AS {self._quote_ident(DUPLICATES_ALIAS)}"
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

    def compile_result_schema_query(self, statement: str) -> str:
        """Wrap a previously compiled statement so only its column metadata returns.

        Connectors use this for `fetch_schema` after `execute_pushdown`. It is
        the one place a full statement is nested inside another, so it lives
        here with the rest of the SQL assembly rather than in a connector.

        Args:
            statement (str): SQL produced by this compiler. Never user text.

        Returns:
            str: `SELECT * FROM (statement) AS alias LIMIT 0`, with the alias
            quoted for the active dialect.
        """
        return f"SELECT * FROM ({statement}) AS {self._quote_ident(SCHEMA_ALIAS)} LIMIT 0"

    def _compile_anti_join(
        self,
        source_table: str,
        target_table: str,
        primary_keys: list[str],
        *,
        join_kind: str,
        source_alias: str,
        target_alias: str,
        source_types: ColumnTypes | None,
        target_types: ColumnTypes | None,
        key_rules: Sequence[DiffRule] | None,
    ) -> str:
        """Assemble a LEFT or RIGHT JOIN anti-join selecting keys from one side."""
        keys = self._key_columns(primary_keys, key_rules)
        with_clause = self._normalized_with_clause(
            source_table,
            target_table,
            keys,
            source_alias=source_alias,
            target_alias=target_alias,
            source_types=source_types,
            target_types=target_types,
        )
        select_alias, null_alias = (
            (source_alias, target_alias) if join_kind == "LEFT" else (target_alias, source_alias)
        )
        select_list = ", ".join(self._qualify(select_alias, pk) for pk in primary_keys)
        join = self._normalized_join(
            join_kind, primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        where_clause = " AND ".join(
            f"{self._qualify(null_alias, pk)} IS NULL" for pk in primary_keys
        )
        return f"{with_clause} SELECT {select_list} {join} WHERE {where_clause}"

    def _normalized_join(
        self, join_kind: str, primary_keys: list[str], *, source_alias: str, target_alias: str
    ) -> str:
        """Join the two normalized CTEs on their keys."""
        on_clause = " AND ".join(
            f"{self._qualify(source_alias, pk)} = {self._qualify(target_alias, pk)}"
            for pk in primary_keys
        )
        return (
            f"FROM {self._quote_ident('_src_normalized')} AS {self._quote_ident(source_alias)} "
            f"{join_kind} JOIN {self._quote_ident('_tgt_normalized')} "
            f"AS {self._quote_ident(target_alias)} ON {on_clause}"
        )

    def _normalize_expr(
        self,
        expr: str,
        rule: DiffRule,
        dtype: pl.DataType | None,
        *,
        is_source: bool,
    ) -> str:
        """Apply stages 1 through 7 to one side of a comparison."""
        # With no probed dtype, every sentinel is emitted as configured.
        sentinels = (
            list(rule.null_values or [])
            if dtype is None
            else usable_sentinels(rule.null_values, dtype)
        )
        expr = self._apply_null_values(expr, sentinels)
        if self._is_text_side(dtype):
            expr = self._apply_regex_replace(expr, rule)
            expr = self._apply_whitespace(expr, rule)
            expr = self._apply_case(expr, rule)
            if is_source:
                expr = self._apply_value_map(expr, rule)
        expr = self._apply_pad_zeros(expr, rule)
        expr = self._apply_datetime_format(expr, rule, dtype)
        # Stage 6b, `timezone`, emits no SQL; see `_reject_unzoned_timezone` in the engine.
        return self._apply_cast(expr, rule, dtype)

    def _key_columns(
        self, primary_keys: list[str], key_rules: Sequence[DiffRule] | None
    ) -> list[_Projection]:
        """Pair each primary key with its stored source name and normalizing rule."""
        if not primary_keys:
            raise ConnectorError("At least one primary key is required for pushdown joins.")
        by_key: dict[str, DiffRule] = {}
        for rule in key_rules or ():
            if len(rule.column_names) != 1:
                raise ConnectorError("A key rule must name exactly one stored column.")
            key = rule.rename_to or rule.column_names[0]
            if key not in primary_keys:
                raise ConnectorError(f"Key rule target '{key}' is not a primary key.")
            by_key[key] = rule
        return [
            (by_key[key].column_names[0] if key in by_key else key, key, by_key.get(key))
            for key in primary_keys
        ]

    def _column_predicates(
        self,
        compared: list[tuple[str, str, DiffRule]],
        *,
        source_alias: str,
        target_alias: str,
        wide_integers: frozenset[str],
        type_drift: frozenset[str],
    ) -> list[str]:
        """Build each compared column's match predicate over the normalized CTEs."""
        return [
            self._compare(
                self._qualify(source_alias, target_column),
                self._qualify(target_alias, target_column),
                rule,
                wide=target_column in wide_integers,
                drift=target_column in type_drift,
            )
            for _source_column, target_column, rule in compared
        ]

    @staticmethod
    def _changed_condition(predicates: list[str]) -> str:
        """Join match predicates into the condition a changed row meets."""
        joined = " AND ".join(f"COALESCE({predicate}, FALSE)" for predicate in predicates)
        return f"NOT ({joined})"

    def _compared_columns(self, rules: list[DiffRule]) -> list[tuple[str, str, DiffRule]]:
        """Resolve the columns that a join query compares."""
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
                # The first rule to name a target column wins, as in the local engine.
                collected.setdefault(target_column, (column, target_column, rule))
        return list(collected.values())

    def _normalized_with_clause(
        self,
        source_table: str,
        target_table: str,
        columns: Sequence[_Projection],
        *,
        source_alias: str,
        target_alias: str,
        source_types: ColumnTypes | None,
        target_types: ColumnTypes | None,
    ) -> str:
        """Build the CTE pair that applies stages 1-7 once per column."""
        # Projecting each normalized value once keeps later SQL linear in the number of columns.
        src_select = self._normalized_select(
            source_table, source_alias, columns, types=source_types, is_source=True
        )
        tgt_select = self._normalized_select(
            target_table, target_alias, columns, types=target_types, is_source=False
        )
        return (
            f"WITH {self._quote_ident('_src_normalized')} AS ({src_select}), "
            f"{self._quote_ident('_tgt_normalized')} AS ({tgt_select})"
        )

    def _normalized_select(
        self,
        table: str,
        alias: str,
        columns: Sequence[_Projection],
        *,
        types: ColumnTypes | None,
        is_source: bool,
    ) -> str:
        """Project one normalized expression per key and compared column."""
        projections: list[str] = []
        for source_column, target_column, rule in columns:
            raw_name = source_column if is_source else target_column
            expr = self._qualify(alias, raw_name)
            if rule is not None:
                dtype = None if types is None else types.get(raw_name)
                expr = self._normalize_expr(expr, rule, dtype, is_source=is_source)
            projections.append(f"{expr} AS {self._quote_ident(target_column)}")
        return (
            f"SELECT {', '.join(projections)} "
            f"FROM {self._quote_relation(table)} AS {self._quote_ident(alias)}"
        )

    def _quote_ident(self, name: str) -> str:
        """Quote a single SQL identifier for the active dialect."""
        if SQL_IDENTIFIER_SEGMENT.fullmatch(name) is None:
            raise ConnectorError("SQL identifier is not a valid unquoted identifier.")
        # The allowlist admits no quote character, so the name needs no escaping.
        quote = _IDENTIFIER_QUOTES[self.dialect]
        return f"{quote}{name}{quote}"

    def _quote_relation(self, name: str) -> str:
        """Quote a possibly dotted table, schema, or catalog path."""
        return ".".join(self._quote_ident(part) for part in _relation_segments(name))

    def _qualify(self, alias: str, column: str) -> str:
        """Return `alias.column` with both parts quoted."""
        return f"{self._quote_ident(alias)}.{self._quote_ident(column)}"

    def _literal(self, value: str) -> str:
        """Render a single-quoted SQL string literal for the active dialect."""
        escaped = value
        for raw, replacement in _LITERAL_ESCAPES[self.dialect]:
            escaped = escaped.replace(raw, replacement)
        return f"'{escaped}'"

    def _integer(self, value: object) -> str:
        """Render an integer SQL operand, refusing anything that is not an `int`."""
        # Other numbers render through `repr`, so `True` or a NumPy scalar would reach SQL.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConnectorError(f"SQL integer operands must be int, got {value!r}.")
        return str(value)

    def _apply_null_values(self, expr: str, sentinels: Sequence[SentinelValue]) -> str:
        """Coerce sentinel values to NULL with a single `CASE` expression."""
        if not sentinels:
            return expr
        rendered = ", ".join(self._sentinel_literal(value) for value in sentinels)
        # Sentinels are stage 1, so `expr` is a bare column and repeating it costs nothing.
        return f"CASE WHEN {expr} IN ({rendered}) THEN NULL ELSE {expr} END"

    def _sentinel_literal(self, value: SentinelValue) -> str:
        """Render one sentinel as a SQL literal of its own type."""
        # bool first, since isinstance(True, int) is True in Python.
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        # A quoted number such as `'-999'` brings back the cast error that type filtering prevents.
        if isinstance(value, (int, float)):
            return repr(value)
        return self._literal(value)

    def _apply_regex_replace(self, expr: str, rule: DiffRule) -> str:
        """Apply `REGEXP_REPLACE` for each pattern/replacement pair, to every match."""
        if not rule.regex_replace:
            return expr
        wrapped = expr
        flags = _REGEX_REPLACE_FLAGS[self.dialect]
        for pattern, replacement in rule.regex_replace.items():
            written = self._regex_replacement(pattern, replacement)
            wrapped = (
                f"REGEXP_REPLACE({wrapped}, {self._literal(pattern)}, "
                f"{self._literal(written)}{flags})"
            )
        return wrapped

    def _regex_replacement(self, pattern: str, replacement: str) -> str:
        """Rewrite a Polars replacement in the dialect's replacement syntax."""
        written: list[str] = []
        follows_group = False
        for token in _replacement_tokens(pattern, replacement):
            if isinstance(token, int):
                written.append(self._group_reference(token))
                follows_group = True
                continue
            # Polars reads a backslash as plain text; each dialect reads it as an escape.
            if self.dialect is SQLDialect.DATABRICKS:
                # Databricks follows Java: `$` starts a group, and a digit right after a group
                # extends its number.
                token = token.replace("\\", "\\\\").replace("$", "\\$")
                if follows_group and token[:1].isdigit():
                    token = f"\\{token}"
            else:
                token = token.replace("\\", "\\\\")
            written.append(token)
            follows_group = False
        return "".join(written)

    def _group_reference(self, group: int) -> str:
        """Write a reference to a numbered group in the dialect's replacement syntax."""
        if self.dialect is SQLDialect.DATABRICKS:
            return f"${group}"
        # Postgres reads `\0` as plain text and spells the whole match `\&`.
        if group == 0 and self.dialect is SQLDialect.POSTGRES:
            return "\\&"
        return f"\\{group}"

    def _apply_whitespace(self, expr: str, rule: DiffRule) -> str:
        """Trim the characters Polars strips, from the side `whitespace_mode` names."""
        mode = rule.whitespace_mode
        if mode not in _TRIM_FUNCTIONS:
            return expr
        # A bare `TRIM` strips only spaces on Snowflake, Databricks, and DuckDB.
        characters = self._literal(_WHITESPACE_CHARACTERS)
        # Databricks' two-argument `ltrim` and `rtrim` take characters first and are deprecated.
        if self.dialect is SQLDialect.DATABRICKS:
            return f"TRIM({_TRIM_SIDES[mode]} {characters} FROM {expr})"
        return f"{_TRIM_FUNCTIONS[mode]}({expr}, {characters})"

    def _apply_case(self, expr: str, rule: DiffRule) -> str:
        """Lowercase an expression when `case_insensitive` is enabled."""
        if rule.case_insensitive:
            return f"LOWER({expr})"
        return expr

    def _apply_value_map(self, expr: str, rule: DiffRule) -> str:
        """Map source values with nested `IFF` on Snowflake, `CASE` elsewhere."""
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
        """Return whether stages 2 through 4 apply to one side of a comparison."""
        # Polars' `.str` rejects `Categorical` and `Enum`, so pushdown skips them too, for parity.
        return dtype is None or isinstance(dtype, pl.String)

    def _apply_pad_zeros(self, expr: str, rule: DiffRule) -> str:
        """Left-pad with zeros the way Python's `str.zfill` does."""
        if rule.pad_zeros is None:
            return expr
        text = f"CAST({expr} AS {self._cast_keyword('String')})"
        width = rule.pad_zeros
        # Width zero still casts, as the local engine does, because later stages branch on text.
        if width == 0:
            return text
        # `LPAD` alone pads before a sign, so `-12` becomes `0-12`, not `-012`, and it truncates
        # longer input, which Polars leaves untouched.
        sign = f"SUBSTR({text}, 1, 1)"
        return (
            f"CASE WHEN LENGTH({text}) >= {width} THEN {text} "
            f"WHEN {sign} IN ('-', '+') "
            f"THEN {sign} || LPAD(SUBSTR({text}, 2), {width - 1}, '0') "
            f"ELSE LPAD({text}, {width}, '0') END"
        )

    def _apply_datetime_format(self, expr: str, rule: DiffRule, dtype: pl.DataType | None) -> str:
        """Parse text timestamps with the dialect's non-throwing parser."""
        if not rule.datetime_format:
            return expr
        if rule.pad_zeros is None and not self._is_text_side(dtype):
            return expr
        parse_function = _PARSE_FUNCTIONS.get(self.dialect)
        if parse_function is None:
            raise ConfigError(
                f"datetime_format cannot be pushed down to {self.dialect.value}: it has no "
                "parse that returns NULL for a value it cannot read, as a local run does, so "
                "one bad row would fail the whole statement. Compare locally instead "
                "(pushdown: false)."
            )
        pattern = self._translate_datetime_format(rule.datetime_format)
        if self.dialect is SQLDialect.BIGQUERY:
            # BigQuery takes the format first, and only a TIMESTAMP holds an offset.
            parse = (
                _BIGQUERY_OFFSET_PARSE if _reads_offset(rule.datetime_format) else parse_function
            )
            return f"{parse}({self._literal(pattern)}, {expr})"
        return f"{parse_function}({expr}, {self._literal(pattern)})"

    def _translate_datetime_format(self, fmt: str) -> str:
        """Rewrite a Python `strptime` format in the dialect's format language."""
        directives = _STRPTIME_DIRECTIVES[self.dialect]
        quote = _FORMAT_LITERAL_QUOTES[self.dialect]
        out: list[str] = []
        literal: list[str] = []

        # Quoting each literal run keeps a separator from reading as a format element.
        def flush() -> None:
            if literal:
                out.append(f"{quote}{''.join(literal)}{quote}")
                literal.clear()

        # A substitution pass would mistake literal letters for directives and keep unknown ones.
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
            # An unknown directive parses to NULL, which reads as a clean match, not an error.
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
        """Cast to the configured target type using the dialect's keyword."""
        if rule.cast_to is None:
            return expr
        # Every earlier stage that fires on a number leaves text or a timestamp
        # behind, so the probed dtype reaches the cast only when none of them does.
        earlier = rule.pad_zeros is not None or rule.datetime_format or rule.timezone
        precast = None if earlier else dtype
        if (
            rule.cast_to == "Boolean"
            and self.dialect in _ZERO_TEST_BOOLEANS
            and precast is not None
            and precast.is_numeric()
        ):
            # Polars reads every nonzero number as true, NaN included, as `<> 0` does.
            return f"({expr} <> 0)"
        if rule.cast_to == "Int64" and precast is not None and precast.is_float():
            # Polars truncates a float toward zero on the way to an integer.
            # Snowflake and DuckDB round instead, so 10.7 would compare as 11
            # under pushdown and 10 locally. Truncate explicitly rather than
            # inherit whichever behavior the warehouse happens to have. Only
            # floats need this: `Decimal` rounds to integers in Polars exactly
            # as SQL does.
            expr = f"CASE WHEN {expr} < 0 THEN CEIL({expr}) ELSE FLOOR({expr}) END"
        return f"CAST({expr} AS {self._cast_keyword(rule.cast_to)})"

    def _cast_keyword(self, target: CastTarget) -> str:
        """Look up the dialect keyword for a cast target."""
        keyword = _CAST_KEYWORDS[self.dialect].get(target)
        if keyword is None:
            raise ConnectorError(f"SQL pushdown has no {self.dialect.value} type for '{target}'.")
        return keyword

    def _numeric_predicate(
        self, src_expr: str, tgt_expr: str, rule: DiffRule, *, wide: bool = False
    ) -> str:
        """Build the engine-equivalent absolute/relative tolerance predicate."""
        abs_tol = repr(rule.absolute_tolerance or 0.0)
        rel_tol = repr(rule.relative_tolerance or 0.0)
        if wide:
            wide_type = _WIDE_INTEGER_TYPES[self.dialect]
            src = f"CAST({src_expr} AS {wide_type})"
            tgt = f"CAST({tgt_expr} AS {wide_type})"
            return (
                f"({self._value_equality(src, tgt)} OR "
                f"ABS({tgt} - {src}) <= {abs_tol} + ({rel_tol} * ABS({src})))"
            )
        # The allowance needs a finite source, since `0 * ABS(inf)` is NaN. Every supported engine
        # sorts NaN above all numbers, so `ABS(diff) <= NaN` would accept any target.
        infinity = _INFINITY_LITERALS[self.dialect]
        return (
            f"({self._value_equality(src_expr, tgt_expr)} OR (ABS({src_expr}) < {infinity} AND "
            f"ABS({tgt_expr} - {src_expr}) <= {abs_tol} + ({rel_tol} * ABS({src_expr}))))"
        )

    def _edit_distance_predicate(self, src_expr: str, tgt_expr: str, limit: int) -> str:
        """Build the predicate matching text within a Levenshtein distance."""
        distance = _EDIT_DISTANCE_FUNCTIONS.get(self.dialect)
        if distance is None:
            raise ConfigError(
                f"max_levenshtein_distance cannot be pushed down to {self.dialect.value}: its "
                "levenshtein needs the fuzzystrmatch extension and refuses text longer than "
                "255 characters. Compare locally instead (pushdown: false)."
            )
        return f"({src_expr} = {tgt_expr} OR {distance}({src_expr}, {tgt_expr}) <= {limit!r})"

    def _loosened_predicate(
        self, src_expr: str, tgt_expr: str, rule: DiffRule, *, wide: bool = False
    ) -> str | None:
        """Build the stage 8 predicate for a rule that loosens equality."""
        if rule.min_jaro_winkler_similarity is not None:
            raise ConfigError(
                "min_jaro_winkler_similarity has no SQL translation: Snowflake's "
                "JAROWINKLER_SIMILARITY ignores case and returns a whole number from 0 "
                "to 100, and Databricks has no Jaro-Winkler function. Use "
                "max_levenshtein_distance, or compare the tables locally."
            )
        if rule.absolute_tolerance or rule.relative_tolerance:
            return self._numeric_predicate(src_expr, tgt_expr, rule, wide=wide)
        if rule.max_levenshtein_distance is not None:
            return self._edit_distance_predicate(src_expr, tgt_expr, rule.max_levenshtein_distance)
        return None

    def _compare(
        self,
        src_expr: str,
        tgt_expr: str,
        rule: DiffRule,
        *,
        wide: bool = False,
        drift: bool = False,
    ) -> str:
        """Build the final match predicate, including null-safe equality."""
        if drift:
            if rule.treat_null_as_equal:
                return f"({src_expr} IS NULL AND {tgt_expr} IS NULL)"
            return "FALSE"
        loosened = self._loosened_predicate(src_expr, tgt_expr, rule, wide=wide)
        if loosened is not None:
            if rule.treat_null_as_equal:
                return f"({src_expr} IS NULL AND {tgt_expr} IS NULL) OR ({loosened})"
            return loosened

        if rule.treat_null_as_equal:
            if self.dialect is SQLDialect.SNOWFLAKE:
                return f"EQUAL_NULL({src_expr}, {tgt_expr})"
            if self.dialect is SQLDialect.DATABRICKS:
                return f"{src_expr} <=> {tgt_expr}"
            # DuckDB, BigQuery, and Postgres; BigQuery's also treats two NaNs as equal.
            return f"{src_expr} IS NOT DISTINCT FROM {tgt_expr}"

        return self._value_equality(src_expr, tgt_expr)

    def _value_equality(self, src_expr: str, tgt_expr: str) -> str:
        """Build equality that, like Polars, treats two NaNs as equal."""
        # BigQuery's `=` calls two NaNs different, per IEEE 754. A NULL still compares as unknown.
        if self.dialect is SQLDialect.BIGQUERY:
            return (
                f"({src_expr} = {tgt_expr} OR "
                f"({src_expr} IS NOT DISTINCT FROM {tgt_expr} AND {src_expr} IS NOT NULL))"
            )
        return f"{src_expr} = {tgt_expr}"
