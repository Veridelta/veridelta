# Pushdown

Pushdown compares two tables inside the database that stores them. Veridelta compiles the comparison to SQL, runs it there, and reads back counts and primary keys instead of rows.

Every rule gives the same verdict in a warehouse as in a local run, except where this page says otherwise. The generated SQL has run in DuckDB and in Postgres 16, but not yet in live BigQuery, Databricks, MotherDuck, or Snowflake services.

## When a comparison is pushed down

A pair of warehouse tables is always compared in place. Both sides must use the same warehouse and the same connection, and name two different tables:

| `type` | Fields that must match on both sides |
| :--- | :--- |
| `snowflake` | `account`, `user`, `warehouse`, `database`, `schema_name`, `role`, `password` |
| `databricks` | `server_hostname`, `http_path`, `access_token`, `catalog`, `schema_name` |
| `bigquery` | `project`, `dataset`, `location`, `credentials_path`, `maximum_bytes_billed` |

Naming the same table on both sides raises `ConfigError`, since a table compared with itself always matches. A warehouse paired with a file, a lakehouse table, or a database, or with a different warehouse, raises `ConnectorError`.

Database and DuckDB sources are read into memory and compared locally. Two Postgres tables on one server, or two tables in one DuckDB database, are compared in place when both set `pushdown`; see [Postgres](#postgres) and [DuckDB and MotherDuck](#duckdb-and-motherduck).

## Statements

A pushdown run issues these statements:

1. A zero-row column probe, a duplicate key check, and a `COUNT(*)` for each side.
2. The changed rows: an inner join that finds mismatches.
3. The added rows, found only in the target, and the removed rows, found only in the source.
4. A tally of mismatches per column. It is skipped when no column is compared.

With `pushdown_sample_rows` set, one more statement fetches a sample of the changed rows with their values; see [Row samples](#row-samples).

These counts fill every `DiffSummary` field, `column_mismatches` included. `threshold`, `match_rate_percentage`, and the drift report mean what they mean in a local run.

The duplicate key check costs one grouped scan of each table over its normalized keys. A key that repeats raises `DataIntegrityError` before any count or join runs, as a local run refuses repeated keys.

## Columns

The column probes check `schema_mode` and the primary keys before any comparison runs, and raise `ConfigError` on a violation. Rules that select columns by `pattern` match against the probed names before any SQL is compiled.

Probed names are compared exactly as the compiler quotes them, with no case folding. Write names in the case the warehouse stores them; Snowflake stores unquoted names in uppercase. `normalize_column_names` cannot change that: pushdown raises `ConfigError` if the setting would rename a stored column.

## Rules in SQL

All nine transform stages compile for compared columns, and stages 1 to 7 for primary keys. One setting is refused everywhere: `min_jaro_winkler_similarity` raises `ConfigError` before any comparison runs. Snowflake's `JAROWINKLER_SIMILARITY` ignores case and returns a whole number from 0 to 100, and Databricks has no Jaro-Winkler function. Neither can reproduce a local verdict. Postgres also refuses `datetime_format` and `max_levenshtein_distance`, and DuckDB refuses `max_levenshtein_distance`; see [Postgres](#postgres) and [DuckDB and MotherDuck](#duckdb-and-motherduck).

### Edit distance

`max_levenshtein_distance` compiles to Snowflake's `EDITDISTANCE`, Databricks' `levenshtein`, or BigQuery's `EDIT_DISTANCE`, which count characters as a local run does. A warehouse run needs no `fuzzy` extra. Postgres and DuckDB refuse the setting, each for its own reason.

### Text in SQL

Write `regex_replace` patterns, `value_map` entries, and text `null_values` as you would for a local run. Each is escaped for the warehouse's string literals, so a backslash in `\d` or `\N` and the apostrophe in `O'Brien` arrive intact. Do not double them yourself.

Escaping keeps the text, but each warehouse runs its own regular expression engine. Capture group references in a replacement, written as Polars reads them, are rewritten in the warehouse's spelling, such as `\1` on Snowflake, BigQuery, DuckDB, and Postgres. Refer to groups by number, 0 to 9. No warehouse can refer to a group by name in a replacement, so a named reference raises `ConfigError`. That includes `$1a`, which Polars reads as the group named `1a`.

`whitespace_mode` strips the same characters in every warehouse as in a local run: spaces, tabs, line breaks, no-break spaces, and the rest of Unicode's whitespace.

`pad_zeros` compiles to an expression that keeps the sign in front and never truncates. A bare `LPAD` would pad in front of a minus sign, giving `0-12` where a local run gives `-012`, and would cut characters past the width.

### Dates and timezones

`datetime_format` is translated directive by directive into the warehouse's own format language, from a fixed table. `%Y-%m-%d` becomes `YYYY"-"MM"-"DD` on Snowflake and `yyyy'-'MM'-'dd` on Databricks, whose parser reads Java `DateTimeFormatter` patterns.

The table covers `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%f`, `%z`, and `%%`, separated by spaces or any of `-` `/` `:` `.` `,` `_` `T`. Any other directive raises `ConfigError`. An untranslated directive would parse nothing and return NULL for every row, which would read as a clean match.

`timezone` emits no SQL. In a local run it rewrites only a column's timezone label, and every later cast and comparison reads the underlying UTC instant, so it cannot change a verdict. Warehouses have no per-column label to rewrite, and Spark's `TIMESTAMP` is a bare instant. Functions that look equivalent shift the value to a wall clock time instead, which would make pushdown disagree with a local run. The rule's precondition still holds: a column that is not a timezone-aware timestamp once `pad_zeros` and `datetime_format` apply raises `ConfigError`, as it does locally. Text parsed with a `%z` format is aware, and text parsed without one is naive.

## Differences from a local run

- **Artifacts hold primary keys only**, since the comparison SQL never selects whole rows. They are written as `added_rows_pks_only`, `removed_rows_pks_only`, and `changed_rows_pks_only`, so they cannot be mistaken for local artifacts, which hold whole records. A [row sample](#row-samples), when requested, is written as `changed_rows_sample`.
- **A column of two different types is refused unless both are numeric.** A local run casts the target to the source type. A database converts one side by its own rules, so the text `"007"` against the integer `7`, or a date against a timestamp, could reach another verdict. Pushdown raises `ConfigError` before reading a row, naming each such column. Give it a `cast_to` that brings both sides to one type, or set `strict_types` to fail it; [Upgrading](upgrading.md#0330) shows both. Two numeric types still compare by value.
- **`strict_types` compares the types the driver reports** for each side, after normalization. It fails every row of a column whose two types differ, as a local run does. These are the driver's types, not the declared ones. Snowflake's `NUMBER(38,0)`, for one, arrives as a decimal, so it meets a `NUMBER(38,0)` column but not a `FLOAT`.

## BigQuery

BigQuery differs from the other warehouses in three ways a comparison can notice:

- `datetime_format` cannot use `%f`, since BigQuery spells fractional seconds only as part of the seconds. A format with `%z` parses to an aware timestamp, and one without it to a naive one, as in Polars.
- Comparing columns of different types fails the statement instead of converting one side. Give such a pair a `cast_to`, or set `strict_types`.
- `GEOGRAPHY` and `JSON` columns cannot be compared. Mark them `ignore`.

## Postgres

Two Postgres tables on one server can be compared where they are stored, instead of being read into memory. Set `pushdown: true` on both sides:

```yaml
source:
  type: database
  uri: postgresql://analyst@sales-db.internal:5432/sales
  password: ${SALES_DB_PASSWORD}
  table: legacy.orders
  pushdown: true

target:
  type: database
  uri: postgresql://analyst@sales-db.internal:5432/sales
  password: ${SALES_DB_PASSWORD}
  table: modern.orders
  pushdown: true

primary_keys: ["order_id"]
```

Veridelta then runs each statement inside Postgres through ConnectorX, as in a warehouse, and only counts and primary keys come back. Everything above applies, with these additions:

- Both `uri` values start with `postgresql://` or `postgres://` and match, as do both `password` values, so one connection reaches both tables. Each side names a `table`; a `query` cannot be compared in place. Other databases, Redshift included, are always read and compared locally.
- Set `pushdown` on both sides or on neither. A pair where only one side sets it raises `ConfigError` instead of reading both.
- The server must read string literals by the SQL standard, which is the Postgres default (`standard_conforming_strings` on). Veridelta checks before the first statement and raises `ConnectorError` if it is off, since a backslash in a value would otherwise be read as an escape.
- `datetime_format` and `max_levenshtein_distance` raise `ConfigError` before any statement runs. Postgres has no date parse that returns NULL for text it cannot read, so one bad value would fail the whole statement. Its `levenshtein` needs the `fuzzystrmatch` extension and refuses text longer than 255 characters. Leave `pushdown` off to compare such columns locally; `veridelta validate` warns about both.
- After the column probes, one catalog query per table reads the declared precision and scale of each `numeric` column, which a local read also keeps. `strict_types` tells `numeric(10, 2)` from `numeric(12, 4)`, and `pad_zeros` or `cast_to: String` writes seven in a `numeric(20, 0)` column as `7` with or without `pushdown`.
- A `numeric` declared without a precision, with one above 38, or with a negative scale has the type ConnectorX reads it as: `Decimal(38, 10)`. Turned into text, seven in such a column is `7` with `pushdown` and `7.0000000000` without it.
- Primary keys and row samples come back through ConnectorX too. A `numeric` value among them is rounded to ten decimal places, and one with more than 18 digits before the decimal point fails the statement.

## DuckDB and MotherDuck

Two tables in one DuckDB file or MotherDuck database can be compared inside DuckDB, instead of being read into memory. Set `pushdown: true` on both sides:

```yaml
source:
  type: duckdb
  database: warehouse.duckdb
  table: legacy.orders
  pushdown: true

target:
  type: duckdb
  database: warehouse.duckdb
  table: modern.orders
  pushdown: true

primary_keys: ["order_id"]
```

Veridelta opens one connection, as a read opens it, and runs each statement there. A file opens read-only, and every session reads time in UTC; see [DuckDB and MotherDuck](sources.md#duckdb-and-motherduck). Everything above applies, with these additions:

- Both sides name a `table`, and their `database` and `motherduck_token` values match, so one connection reaches both tables. Write `database` the same way on both sides, since `./warehouse.duckdb` and `warehouse.duckdb` count as different. A `query` cannot be compared in place.
- Set `pushdown` on both sides or on neither. A pair where only one side sets it raises `ConfigError` instead of reading both.
- `max_levenshtein_distance` raises `ConfigError` before any statement runs. DuckDB's `levenshtein` counts UTF-8 bytes, not characters, so `é` against `e` is two edits where a local run counts one. Leave `pushdown` off to compare such columns locally; `veridelta validate` warns about it.
- `case_insensitive` lowercases with DuckDB's `lower`, which differs from Polars for a few letters. `İ` becomes `i` rather than `i̇`, and a final `Σ` becomes `σ` rather than `ς`.
- The column probe reads the type of every column, ignored ones included. A column that Polars cannot read, such as an `INTERVAL`, fails the probe. Compare a view that casts it, such as to `VARCHAR`.
- MotherDuck pushdown has not yet run against a live account, and neither have MotherDuck reads.

## Row samples

A pushdown run reads back counts and keys, so its report can say which rows changed but not how. Set `pushdown_sample_rows` to see values for some of them:

```yaml
primary_keys: ["order_id"]
pushdown_sample_rows: 50
```

After the counts, one more statement fetches up to that many changed rows, lowest keys first, so the same tables give the same sample. Each row holds its keys and, for every compared column, `{column}_source`, `{column}_target`, and `{column}_is_match`, as a local run's changed rows do. The values are the ones the comparison saw, after every rule up to the comparison itself, and each flag is the result that decided the row. The counts do not change, because the sample comes from the same changed rows.

The sample reaches these places:

- the HTML report, whose changed rows table shows it in place of the bare keys;
- `DiffResult.changed_sample`;
- with `output_path` set, a `changed_rows_sample` artifact;
- with `--markdown-max-rows` above 0, the [Markdown summary](results.md#markdown-summary), which the [CI integrations](ci.md) post as a pull request comment.

It never reaches a log line or the `--json` summary. Values do leave the warehouse, though, and the GitHub Action uploads the HTML report as a workflow artifact. Set `pushdown_sample_rows` only where everyone who can open the report may read the data. `0`, the default, fetches nothing.
