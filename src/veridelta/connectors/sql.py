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
"""`cast_to` value to the type keyword each dialect spells it with.

A type name is the one thing in a `CAST` that cannot be quoted or bound as a
parameter, so this table is the boundary that keeps configuration text out of
the emitted SQL grammar. Nothing outside it ever reaches a `CAST`.
"""

_ZERO_TEST_BOOLEANS: Final[frozenset[SQLDialect]] = frozenset(
    {SQLDialect.POSTGRES, SQLDialect.BIGQUERY}
)
"""Dialects that turn a number into a boolean by comparing it with zero.

Postgres casts only `integer` to `boolean` and refuses `smallint`, `bigint`,
`numeric`, and floats. BigQuery casts only INT64 and STRING to BOOL, so a
FLOAT64 or NUMERIC column fails there too. Polars reads every nonzero number as
true, NaN included, which is exactly what `<> 0` returns for each of them.
"""

_IDENTIFIER_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "`",
    SQLDialect.DUCKDB: '"',
    SQLDialect.BIGQUERY: "`",
    SQLDialect.POSTGRES: '"',
}
"""Character each dialect quotes identifiers with. Names are allowlisted before
they are quoted, so no identifier ever holds a quote character to escape."""

_LITERAL_ESCAPES: Final[dict[SQLDialect, tuple[tuple[str, str], ...]]] = {
    SQLDialect.SNOWFLAKE: (("\\", "\\\\"), ("'", "''")),
    SQLDialect.DATABRICKS: (("\\", "\\\\"), ("'", "\\'")),
    SQLDialect.DUCKDB: (("'", "''"),),
    SQLDialect.BIGQUERY: (("\\", "\\\\"), ("'", "\\'"), ("\n", "\\n"), ("\r", "\\r")),
    SQLDialect.POSTGRES: (("'", "''"),),
}
"""Replacements that keep text inside a single-quoted literal, applied in order.

The dialects disagree on what a string literal is, and every difference is
silent. Snowflake and Databricks read backslash escape sequences inside quotes:
a regex `\\d` arrives as `d`, a `\\N` sentinel as `N`, and a value ending in a
backslash escapes its own closing quote, carrying configuration text out of the
literal and into the statement. Both therefore double backslashes first, before
anything else adds one. Databricks also reads `''` as two adjacent literals and
concatenates them, dropping the apostrophe, so it escapes quotes with a
backslash instead. BigQuery reads backslash escapes too, rejects any it does
not know, and refuses a raw line break inside quotes, so line breaks become
`\\n` and `\\r`. DuckDB follows the SQL standard, where a backslash is
ordinary text and only the quote needs doubling. So does Postgres while
`standard_conforming_strings` is on, its default, which the Postgres session
checks before it runs anything.
"""

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
"""Python `strptime` directive to its spelling in each dialect's format language.

Four different languages: Snowflake's own, Java `DateTimeFormatter` for
Databricks, Python's own for DuckDB, and GoogleSQL's for BigQuery. Membership here is the allowlist, and
anything absent is refused rather than passed through. A directive that survives
translation unrecognized parses to NULL, which reads as a clean match rather
than as an error.
"""

_FORMAT_LITERAL_QUOTES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: '"',
    SQLDialect.DATABRICKS: "'",
    SQLDialect.DUCKDB: "",
    SQLDialect.BIGQUERY: "",
}
"""Character each dialect wraps a literal run of a format string in.

DuckDB and BigQuery mark directives with `%`, so their literals need no wrapper.
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
    SQLDialect.BIGQUERY: "SAFE.PARSE_DATETIME",
}
"""Each dialect's non-throwing parse, matching Polars `strptime(strict=False)`.

The strict variants abort the whole statement on one unparseable row where the
local engine yields a null and keeps going. BigQuery takes the format first, and
parses a format with an offset into a TIMESTAMP, `_BIGQUERY_OFFSET_PARSE`.
Postgres has none: its `to_timestamp` raises, so a dialect missing here refuses
`datetime_format`, and the directive and quoting tables leave it out too.
"""

_BIGQUERY_OFFSET_PARSE: Final = "SAFE.PARSE_TIMESTAMP"
"""BigQuery's non-throwing parse for a format that reads a UTC offset. The result
is an instant, as Polars' is for a `%z` format; PARSE_DATETIME has no zone."""

_INFINITY_LITERALS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "'inf'::FLOAT",
    SQLDialect.DATABRICKS: "CAST('Infinity' AS DOUBLE)",
    SQLDialect.DUCKDB: "'inf'::DOUBLE",
    SQLDialect.BIGQUERY: "CAST('inf' AS FLOAT64)",
    SQLDialect.POSTGRES: "'Infinity'::double precision",
}
"""Each dialect's positive infinity, which bounds the finite values a tolerance
may apply to. `ABS(x) < inf` is false for an infinity and for NaN, whether an
engine treats NaN comparisons as false or sorts NaN above every number.
"""

_EDIT_DISTANCE_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "EDITDISTANCE",
    SQLDialect.DATABRICKS: "levenshtein",
    SQLDialect.DUCKDB: "levenshtein",
    SQLDialect.BIGQUERY: "EDIT_DISTANCE",
}
"""Each dialect's Levenshtein distance, always called with two arguments.
Snowflake's optional third argument caps the result, and Databricks' returns -1
above it and needs Runtime 13.3, so neither is portable. Snowflake,
Databricks, and BigQuery count characters, as the local engine does. DuckDB counts UTF-8
bytes, which is why the parity tests compare ASCII text. Postgres is left out: its
`levenshtein` needs the `fuzzystrmatch` extension and refuses text longer than 255
characters, so a dialect missing here refuses `max_levenshtein_distance`.
"""


_REGEX_REPLACE_FLAGS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "",
    SQLDialect.DATABRICKS: "",
    SQLDialect.DUCKDB: ", 'g'",
    SQLDialect.BIGQUERY: "",
    SQLDialect.POSTGRES: ", 'g'",
}
"""Trailing `REGEXP_REPLACE` arguments that make it replace every match, as Polars'
`replace_all` does. Snowflake, Databricks, and BigQuery already replace every match; DuckDB
and Postgres replace only the first unless given the `'g'` option."""


_REFERENCE_NAME: Final = re.compile(r"[_0-9A-Za-z]+")
"""The name after a bare `$` in a Polars replacement: the longest run of these characters."""

_GROUP_NUMBER: Final = re.compile(r"[0-9]+")
"""A reference name that Polars reads as a group number rather than a group name."""

_HIGHEST_GROUP: Final = 9
"""The highest group a warehouse replacement can refer to: their references are one digit."""

_WHOLE_MATCH_REFERENCES: Final[dict[SQLDialect, str]] = {SQLDialect.POSTGRES: "\\&"}
r"""How a dialect refers to the whole match, where group 0 is not spelled `\0`.

Postgres' `regexp_replace` reads `\0` as plain text and the whole match as `\&`.
"""


def _reference_at(replacement: str, index: int) -> tuple[str, int] | None:
    """Read the group reference that starts at a `$` in a Polars replacement.

    Polars follows the `regex` crate: `${name}` names everything up to the
    closing brace, and a bare `$name` takes the longest run of letters,
    digits, and underscores, so `$1a` names the group `1a`, not group 1.

    Args:
        replacement (str): Replacement text, as Polars reads it.
        index (int): Position of a `$` that is not part of `$$`.

    Returns:
        tuple[str, int] | None: The reference's name and the position after
            it, or None when the `$` starts no reference and is plain text.
    """
    if replacement.startswith("{", index + 1):
        close = replacement.find("}", index + 2)
        if close == -1:
            return None
        return replacement[index + 2 : close], close + 1
    name = _REFERENCE_NAME.match(replacement, index + 1)
    if name is None:
        return None
    return name.group(), name.end()


def _replacement_tokens(pattern: str, replacement: str) -> list[str | int]:
    """Split a Polars replacement into runs of plain text and group numbers.

    `$$` is a dollar sign, and a `$` that starts no reference is plain text,
    as are backslashes. Polars and these rules agree on 1.39.3 and later.

    Args:
        pattern (str): The `regex_replace` key, for the error message.
        replacement (str): Its replacement, as Polars reads it.

    Returns:
        list[str | int]: Plain text and group numbers, in order.

    Raises:
        ConfigError: If a reference names a group, which no warehouse can write
            in a replacement, or a group above 9.
    """
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
    """Return the group a reference names, if a warehouse can refer to it.

    Args:
        pattern (str): The `regex_replace` key, for the error message.
        replacement (str): Its replacement, for the error message.
        name (str): The reference's name, as Polars reads it.

    Returns:
        int: The group number, 0 to 9.

    Raises:
        ConfigError: If the name is not a number, or the number is above 9.
    """
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


_WHITESPACE_CHARACTERS: Final = (
    "\t\n\x0b\x0c\r \x85\xa0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006"
    "\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
)
"""The characters Polars' `strip_chars` removes: Unicode's `White_Space` set.

A bare `TRIM` removes only spaces on Snowflake, Databricks, and DuckDB, so every
dialect is handed this set, and a tab, a line break, or a no-break space is
stripped from the same values in a warehouse as in a local run. Zero-width
spaces and byte-order marks are not whitespace to Polars, so they stay."""

_TRIM_FUNCTIONS: Final[dict[str, str]] = {"left": "LTRIM", "right": "RTRIM", "both": "TRIM"}
"""The trim function for each `whitespace_mode` that strips something."""

_TRIM_SIDES: Final[dict[str, str]] = {"left": "LEADING", "right": "TRAILING", "both": "BOTH"}
"""The side keyword of the standard `TRIM(side characters FROM value)` form, by mode."""


_WIDE_INTEGER_TYPES: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "NUMBER(38, 0)",
    SQLDialect.DATABRICKS: "DECIMAL(38, 0)",
    SQLDialect.DUCKDB: "DECIMAL(38, 0)",
    # NUMERIC keeps 29 integer digits, more than any INT64 difference needs.
    SQLDialect.BIGQUERY: "NUMERIC",
    SQLDialect.POSTGRES: "NUMERIC(38, 0)",
}
"""An exact integer type wide enough to subtract any two stored integers in.
Thirty-eight digits hold every difference of two 64-bit values, signed or not,
and `ABS` of the smallest BIGINT, which overflows in the column's own type."""


_SAMPLE_HASH_FUNCTIONS: Final[dict[SQLDialect, str]] = {
    SQLDialect.SNOWFLAKE: "HASH",
    SQLDialect.DATABRICKS: "xxhash64",
    SQLDialect.DUCKDB: "hash",
    SQLDialect.BIGQUERY: "FARM_FINGERPRINT",
    SQLDialect.POSTGRES: "hashtextextended",
}
"""Each dialect's hash of several values, used to sample value map evidence by
key. Snowflake's `HASH` and Databricks' `xxhash64` return signed 64-bit
integers, so their buckets fold negatives back into range; DuckDB's `hash` is
unsigned. BigQuery's `FARM_FINGERPRINT` is signed and takes one string, so the
keys are hashed as one JSON value, and Postgres' `hashtextextended` likewise hashes
the keys as the text of one row. No engine hashes the way Polars does, so a warehouse sample is a
different, though equally repeatable, set of rows from a local one."""


def _reads_offset(fmt: str) -> bool:
    """Return whether a `strptime` format reads a UTC offset with `%z`.

    Args:
        fmt (str): Python `strptime` format.

    Returns:
        bool: True when a `%z` directive appears outside a `%%` escape.
    """
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
"""Opening and closing identifier quote for each database URI scheme a `table` may name.

Keyed by URI scheme rather than `SQLDialect`, whose members are pushdown
dialects that must fill every table above. A database source is compared
locally and only ever reads `SELECT * FROM <table>`, so quoting is all it
needs from this module. Quoting keeps a name's stored case and lets it be a
reserved word. A scheme missing here refuses `table` instead of borrowing
another database's quote character.
"""


def _relation_segments(name: str) -> list[str]:
    """Split a possibly dotted relation name into allowlisted segments.

    The allowlist admits letters, digits, and underscores only, so no segment
    can contain a quote character of any dialect or close its own quoting.

    Args:
        name (str): Relation name, optionally `catalog.schema.table`.

    Returns:
        list[str]: One to three unquoted identifier segments.

    Raises:
        ConnectorError: If the name is empty, has more than three segments, or
            contains a disallowed identifier.
    """
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
        values, so adding a stage no longer copies the entire expression tree.

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
            select_alias=source_alias,
            null_alias=target_alias,
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
            select_alias=target_alias,
            null_alias=source_alias,
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
        """Build the CTE pairing each candidate's normalized values on the keys.

        Args:
            columns (list[str]): Candidate columns, by their target names.
            primary_keys (list[str]): Join keys.
            sample_fraction (float): Share of source keys to keep.
            source_alias (str): Alias for the source CTE.
            target_alias (str): Alias for the target CTE.

        Returns:
            str: `"_veridelta_joined" AS (SELECT ...)`, with a sample filter
                when the fraction is below 1.
        """
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
        """Hash qualified keys into one of `SAMPLE_BUCKETS` buckets.

        Args:
            keys (list[str]): Qualified, normalized key expressions.

        Returns:
            str: An expression from 0 to `SAMPLE_BUCKETS - 1`.
        """
        hashed = f"{_SAMPLE_HASH_FUNCTIONS[self.dialect]}({', '.join(keys)})"
        buckets = self._integer(SAMPLE_BUCKETS)
        if self.dialect is SQLDialect.BIGQUERY:
            # FARM_FINGERPRINT is signed, takes one string, and MOD keeps the sign.
            hashed = (
                f"{_SAMPLE_HASH_FUNCTIONS[self.dialect]}(TO_JSON_STRING(STRUCT({', '.join(keys)})))"
            )
            return f"MOD(MOD({hashed}, {buckets}) + {buckets}, {buckets})"
        if self.dialect is SQLDialect.POSTGRES:
            # hashtextextended is signed and takes one text and a seed.
            hashed = f"{_SAMPLE_HASH_FUNCTIONS[self.dialect]}(ROW({', '.join(keys)})::text, 0)"
            return f"MOD(MOD({hashed}, {buckets}) + {buckets}, {buckets})"
        if self.dialect is SQLDialect.SNOWFLAKE:
            # HASH is signed, and MOD keeps the dividend's sign.
            return f"MOD(MOD({hashed}, {buckets}) + {buckets}, {buckets})"
        if self.dialect is SQLDialect.DATABRICKS:
            # pmod is always non-negative.
            return f"pmod({hashed}, {buckets})"
        return f"{hashed} % {buckets}"

    def _value_map_branch(self, label: int, rule: DiffRule) -> str:
        """Select one candidate's pairs, labeled, without NULL or already mapped sources.

        Args:
            label (int): The candidate's position among the compared columns.
            rule (DiffRule): Its rule, whose existing map outputs are left out.

        Returns:
            str: One `SELECT` of the `UNION ALL`.
        """
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
        """Count the labeled pairs and keep the ones that can qualify.

        The pair count runs in a subquery and the per-value total in the query
        around it, the form every supported dialect accepts. A NULL target is
        a group of its own, so it counts toward the total, and `<>` then
        drops it along with every identity pair.

        Args:
            branches (str): The `UNION ALL` of labeled pairs.
            min_support (int): Agreeing rows a pair needs.

        Returns:
            str: The statement's `SELECT`, after its `WITH` clause.
        """
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
        counted = (
            f"SELECT {column}, {source}, {target}, {agreeing}, "
            f"SUM({agreeing}) OVER (PARTITION BY {column}, {source}) AS {rows} "
            f"FROM ({grouped}) AS {self._quote_ident('_veridelta_groups')}"
        )
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
        keys in one group just as Polars counts them as duplicates of each other.

        Args:
            table (str): Relation to check (optionally dotted catalog path).
            primary_keys (list[str]): Keys, spelled as the target stores them.
            is_source (bool): True for the source relation, which reads a
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
        select_alias: str,
        null_alias: str,
        source_alias: str,
        target_alias: str,
        source_types: ColumnTypes | None,
        target_types: ColumnTypes | None,
        key_rules: Sequence[DiffRule] | None,
    ) -> str:
        """Assemble a LEFT or RIGHT JOIN anti-join selecting keys from one side.

        Both sides are read through the same key-only CTEs the changed-row
        query uses, so a row counts as added or removed only when its
        normalized key has no match, exactly as in the local engine.

        Args:
            source_table (str): Source relation.
            target_table (str): Target relation.
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            join_kind (str): `LEFT` or `RIGHT`.
            select_alias (str): Alias whose primary keys are projected.
            null_alias (str): Alias whose keys must be NULL (the missing side).
            source_alias (str): Alias assigned to the source relation.
            target_alias (str): Alias assigned to the target relation.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.
            key_rules (Sequence[DiffRule] | None): Key normalization rules.

        Returns:
            str: Anti-join SELECT statement.

        Raises:
            ConnectorError: If tables or keys are empty, or a key rule does not
                name exactly one primary key.
        """
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
        """Join the two normalized CTEs on their keys.

        Args:
            join_kind (str): `INNER`, `LEFT`, or `RIGHT`.
            primary_keys (list[str]): Join keys, spelled as the target stores them.
            source_alias (str): Alias for the source CTE.
            target_alias (str): Alias for the target CTE.

        Returns:
            str: `FROM ... JOIN ... ON ...` clause.
        """
        on_clause = self._join_on_clause(
            primary_keys, source_alias=source_alias, target_alias=target_alias
        )
        return (
            f"FROM {self._quote_ident('_src_normalized')} AS {self._quote_ident(source_alias)} "
            f"{join_kind} JOIN {self._quote_ident('_tgt_normalized')} "
            f"AS {self._quote_ident(target_alias)} ON {on_clause}"
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

    def _key_columns(
        self, primary_keys: list[str], key_rules: Sequence[DiffRule] | None
    ) -> list[_Projection]:
        """Pair each primary key with its stored source name and normalizing rule.

        Args:
            primary_keys (list[str]): Keys, spelled as the target stores them.
            key_rules (Sequence[DiffRule] | None): Rules naming each key's stored
                source column, with `rename_to` set when the spellings differ.

        Returns:
            list[_Projection]: Stored source name, key name, and rule per key.
            A key without a rule is projected unchanged.

        Raises:
            ConnectorError: If no keys are given, or a key rule does not name
                exactly one column or does not resolve to a primary key.
        """
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
        """Build each compared column's match predicate over the normalized CTEs.

        The changed-row query, the per-column tally, and a row sample all read
        their predicates here, so the three never disagree on what matches.

        Args:
            compared (list[tuple[str, str, DiffRule]]): Columns from
                `_compared_columns`.
            source_alias (str): Alias of the normalized source relation.
            target_alias (str): Alias of the normalized target relation.
            wide_integers (frozenset[str]): Integer columns to measure in a wide type.
            type_drift (frozenset[str]): Columns `strict_types` fails.

        Returns:
            list[str]: One predicate per compared column, in order.
        """
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
        """Join match predicates into the condition a changed row meets.

        COALESCE is load-bearing, as in the tally: `NOT (NULL)` is NULL, and
        WHERE drops it, so a one-sided NULL would vanish from the changed set.

        Args:
            predicates (list[str]): At least one match predicate.

        Returns:
            str: `NOT (COALESCE(p1, FALSE) AND ...)`.
        """
        joined = " AND ".join(f"COALESCE({predicate}, FALSE)" for predicate in predicates)
        return f"NOT ({joined})"

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
        columns: Sequence[_Projection],
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
            columns (Sequence[_Projection]): Keys first, then compared columns.
            source_alias (str): Alias of the source relation inside its CTE.
            target_alias (str): Alias of the target relation inside its CTE.
            source_types (ColumnTypes | None): Probed source dtypes.
            target_types (ColumnTypes | None): Probed target dtypes.

        Returns:
            str: `WITH src AS (...), tgt AS (...)` prefix, no trailing keyword.
        """
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
        """Project one normalized expression per key and compared column.

        Args:
            table (str): Relation to read.
            alias (str): Alias assigned to that relation.
            columns (Sequence[_Projection]): Columns to project. The source reads
                each under its stored name, the target under its projected name.
            types (ColumnTypes | None): Probed dtypes for this side.
            is_source (bool): True when projecting the source relation.

        Returns:
            str: `SELECT ... FROM relation AS alias` body for one CTE.
        """
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
        return ".".join(self._quote_ident(part) for part in _relation_segments(name))

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
        """Render a single-quoted SQL string literal for the active dialect.

        Every configured string that reaches SQL as data passes through here:
        regex patterns and replacements, crosswalk keys and values, text
        sentinels, and translated datetime formats. This is where configuration
        text is kept from becoming statement text.

        Args:
            value (str): Raw Python string.

        Returns:
            str: Quoted literal that the dialect decodes back to `value`.
        """
        escaped = value
        for raw, replacement in _LITERAL_ESCAPES[self.dialect]:
            escaped = escaped.replace(raw, replacement)
        return f"'{escaped}'"

    def _integer(self, value: object) -> str:
        """Render an integer SQL operand, refusing anything that is not an `int`.

        `_number` renders whatever `repr` gives, so `True` or a NumPy scalar
        would reach SQL as written. A count or label must be a real integer;
        the parameter is `object` because this is where that is checked.

        Args:
            value (object): Integer to render.

        Returns:
            str: Its decimal digits.

        Raises:
            ConnectorError: If `value` is not an `int`, or is a `bool`.
        """
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConnectorError(f"SQL integer operands must be int, got {value!r}.")
        return str(value)

    def _number(self, value: float) -> str:
        """Render a numeric SQL literal.

        Args:
            value (float): Tolerance, coefficient, or edit-distance limit.

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
        """Apply `REGEXP_REPLACE` for each pattern/replacement pair, to every match.

        Args:
            expr (str): SQL expression to sanitize.
            rule (DiffRule): Rule providing `regex_replace`.

        Returns:
            str: Nested `REGEXP_REPLACE` expression.

        Raises:
            ConfigError: If a replacement refers to a group the warehouse
                cannot write; see `_regex_replacement`.
        """
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
        r"""Rewrite a Polars replacement in the dialect's replacement syntax.

        Polars writes a group as `$1` or `${1}` and reads a backslash as plain
        text. Snowflake, BigQuery, DuckDB, and Postgres write a group as `\1`,
        read `$` as plain text, and need a plain backslash doubled; Postgres
        writes the whole match as `\&`. Databricks follows Java:
        a group is `$1`, a backslash makes the next character plain, and a
        digit right after a group would extend its number, so it is escaped.

        Args:
            pattern (str): The `regex_replace` key, for error messages.
            replacement (str): Its replacement, as Polars reads it.

        Returns:
            str: The replacement as the dialect's `REGEXP_REPLACE` reads it,
                before string-literal escaping.

        Raises:
            ConfigError: If the replacement names a group, or refers to a group
                above 9.
        """
        written: list[str] = []
        follows_group = False
        for token in _replacement_tokens(pattern, replacement):
            if isinstance(token, int):
                written.append(self._group_reference(token))
                follows_group = True
                continue
            if self.dialect is SQLDialect.DATABRICKS:
                token = token.replace("\\", "\\\\").replace("$", "\\$")
                if follows_group and token[:1].isdigit():
                    token = f"\\{token}"
            else:
                token = token.replace("\\", "\\\\")
            written.append(token)
            follows_group = False
        return "".join(written)

    def _group_reference(self, group: int) -> str:
        r"""Write a reference to a numbered group in the dialect's replacement syntax.

        Args:
            group (int): Group number, with 0 for the whole match.

        Returns:
            str: `$N` on Databricks, and `\N` elsewhere, except where
                `_WHOLE_MATCH_REFERENCES` spells the whole match its own way.
        """
        if self.dialect is SQLDialect.DATABRICKS:
            return f"${group}"
        if group == 0 and self.dialect in _WHOLE_MATCH_REFERENCES:
            return _WHOLE_MATCH_REFERENCES[self.dialect]
        return f"\\{group}"

    def _apply_whitespace(self, expr: str, rule: DiffRule) -> str:
        """Trim the characters Polars strips, from the side `whitespace_mode` names.

        Databricks gets the standard `TRIM(side characters FROM value)` form:
        its two-argument `ltrim` and `rtrim` take the characters first and are
        deprecated. The other dialects take the characters as a second argument.

        Args:
            expr (str): SQL expression to trim.
            rule (DiffRule): Rule providing `whitespace_mode`.

        Returns:
            str: Trimmed expression, or `expr` when mode is unset/`none`.
        """
        mode = rule.whitespace_mode
        if mode is None or mode not in _TRIM_FUNCTIONS:
            return expr
        characters = self._literal(_WHITESPACE_CHARACTERS)
        if self.dialect is SQLDialect.DATABRICKS:
            return f"TRIM({_TRIM_SIDES[mode]} {characters} FROM {expr})"
        return f"{_TRIM_FUNCTIONS[mode]}({expr}, {characters})"

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
                detect the float-to-integer and number-to-boolean cases below.

        Returns:
            str: `CAST(expr AS keyword)`, `(expr <> 0)` for a number on a
                dialect in `_ZERO_TEST_BOOLEANS`, or `expr` when `cast_to` is unset.

        Raises:
            ConnectorError: If `cast_to` has no keyword for this dialect.
        """
        if rule.cast_to is None:
            return expr
        precast = self._precast_dtype(rule, dtype)
        if (
            rule.cast_to == "Boolean"
            and self.dialect in _ZERO_TEST_BOOLEANS
            and precast is not None
            and precast.is_numeric()
        ):
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

    def _precast_dtype(self, rule: DiffRule, dtype: pl.DataType | None) -> pl.DataType | None:
        """Return the probed dtype if stage 7 still receives it on this side.

        Every earlier stage that fires on a number leaves text or a timestamp
        behind, so the probed dtype only describes the cast's input when none
        of them does.

        Args:
            rule (DiffRule): Rule whose earlier stages may have changed the type.
            dtype (pl.DataType | None): Probed dtype, or None when unprobed.

        Returns:
            pl.DataType | None: The dtype reaching the cast, or None when an
                earlier stage changed it or the side was not probed.
        """
        if rule.pad_zeros is not None or rule.datetime_format or rule.timezone:
            return None
        return dtype

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

    def _numeric_predicate(
        self, src_expr: str, tgt_expr: str, rule: DiffRule, *, wide: bool = False
    ) -> str:
        """Build the engine-equivalent absolute/relative tolerance predicate.

        Equal values match outright, so NaN meets NaN and an infinity meets
        itself. The allowance applies only to a finite source: `0 * ABS(inf)` is
        NaN, and every supported engine sorts NaN above all numbers, so an
        unguarded `ABS(diff) <= NaN` would accept any target. Integers are
        always finite, so a widened pair needs no guard.

        Args:
            src_expr (str): Transformed source expression.
            tgt_expr (str): Transformed target expression.
            rule (DiffRule): Rule providing tolerances.
            wide (bool): Both sides are integers; cast them to
                `_WIDE_INTEGER_TYPES` before subtracting, as the local engine
                widens them to Int128.

        Returns:
            str: `(src = tgt OR (ABS(src) < inf AND ABS(tgt - src) <= abs + (rel * ABS(src))))`,
                or, widened, `(src = tgt OR ABS(tgt - src) <= abs + (rel * ABS(src)))`.
        """
        abs_tol = self._number(rule.absolute_tolerance or 0.0)
        rel_tol = self._number(rule.relative_tolerance or 0.0)
        if wide:
            wide_type = _WIDE_INTEGER_TYPES[self.dialect]
            src = f"CAST({src_expr} AS {wide_type})"
            tgt = f"CAST({tgt_expr} AS {wide_type})"
            return (
                f"({self._value_equality(src, tgt)} OR "
                f"ABS({tgt} - {src}) <= {abs_tol} + ({rel_tol} * ABS({src})))"
            )
        infinity = _INFINITY_LITERALS[self.dialect]
        return (
            f"({self._value_equality(src_expr, tgt_expr)} OR (ABS({src_expr}) < {infinity} AND "
            f"ABS({tgt_expr} - {src_expr}) <= {abs_tol} + ({rel_tol} * ABS({src_expr}))))"
        )

    def _edit_distance_predicate(self, src_expr: str, tgt_expr: str, limit: int) -> str:
        """Build the predicate matching text within a Levenshtein distance.

        Equal values match outright, as in the local engine, which only scores
        pairs that still differ. The distance of a NULL is NULL, so a one-sided
        NULL stays a mismatch once the caller coalesces the predicate.

        Args:
            src_expr (str): Transformed source expression.
            tgt_expr (str): Transformed target expression.
            limit (int): Most character edits that still match.

        Returns:
            str: `(src = tgt OR <distance>(src, tgt) <= limit)`.

        Raises:
            ConfigError: If the dialect has no faithful edit distance.
        """
        distance = _EDIT_DISTANCE_FUNCTIONS.get(self.dialect)
        if distance is None:
            raise ConfigError(
                f"max_levenshtein_distance cannot be pushed down to {self.dialect.value}: its "
                "levenshtein needs the fuzzystrmatch extension and refuses text longer than "
                "255 characters. Compare locally instead (pushdown: false)."
            )
        return (
            f"({src_expr} = {tgt_expr} OR "
            f"{distance}({src_expr}, {tgt_expr}) <= {self._number(limit)})"
        )

    def _loosened_predicate(
        self, src_expr: str, tgt_expr: str, rule: DiffRule, *, wide: bool = False
    ) -> str | None:
        """Build the stage 8 predicate for a rule that loosens equality.

        Args:
            src_expr (str): Transformed source expression.
            tgt_expr (str): Transformed target expression.
            rule (DiffRule): Rule providing a tolerance or a similarity limit.
            wide (bool): Measure a tolerance in the wide integer type.

        Returns:
            str | None: The tolerance or edit-distance predicate, or None when
                the rule compares by equality alone.

        Raises:
            ConfigError: If the rule sets `min_jaro_winkler_similarity`.
        """
        if rule.min_jaro_winkler_similarity is not None:
            raise ConfigError(
                "min_jaro_winkler_similarity has no SQL translation: Snowflake's "
                "JAROWINKLER_SIMILARITY ignores case and returns a whole number from 0 "
                "to 100, and Databricks has no Jaro-Winkler function. Use "
                "max_levenshtein_distance, or compare the tables locally."
            )
        if self._has_tolerance(rule):
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
        """Build the final match predicate, including null-safe equality.

        Args:
            src_expr (str): Fully transformed source expression.
            tgt_expr (str): Fully transformed target expression.
            rule (DiffRule): Rule providing comparison and null semantics.
            wide (bool): Measure a tolerance in the wide integer type.
            drift (bool): The sides hold different types under `strict_types`,
                so no value matches. The values are never compared, which also
                keeps the warehouse from casting one type to the other.

        Returns:
            str: Boolean SQL expression.

        Raises:
            ConfigError: If the rule sets `min_jaro_winkler_similarity`.
        """
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
        """Build equality that, like Polars, treats two NaNs as equal.

        Args:
            src_expr (str): Fully transformed source expression.
            tgt_expr (str): Fully transformed target expression.

        Returns:
            str: `src = tgt`, or on BigQuery, whose `=` follows IEEE 754 and
                calls two NaNs different, `=` widened to two non-NULL values
                that are not distinct. NULL still compares as unknown.
        """
        if self.dialect is SQLDialect.BIGQUERY:
            return (
                f"({src_expr} = {tgt_expr} OR "
                f"({src_expr} IS NOT DISTINCT FROM {tgt_expr} AND {src_expr} IS NOT NULL))"
            )
        return f"{src_expr} = {tgt_expr}"
