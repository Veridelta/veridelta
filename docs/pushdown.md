# Pushdown

## When a comparison is pushed down

Same-warehouse SQL pushdown runs only when both sides are Snowflake, both are Databricks, or both are BigQuery, the connection fields match (`account`, `user`, `warehouse`, `database`, `schema_name`, `role`, and `password` for Snowflake; `server_hostname`, `http_path`, `access_token`, `catalog`, and `schema_name` for Databricks; `project`, `dataset`, `location`, `credentials_path`, and `maximum_bytes_billed` for BigQuery), and the `table` names differ; naming the same table twice raises `ConfigError`, since a table compared with itself always matches. Mixed file/lakehouse/database and warehouse backends, or Snowflake paired with Databricks, raise `ConnectorError`. Database sources are read into the local engine like files, unless both are Postgres tables that set `pushdown`; see [Comparing inside Postgres](#postgres).

## Statements

Pushdown issues up to ten statements per run: a zero-row column probe, a duplicate-key check, and a `COUNT(*)` per side, then inner-join mismatches, target-only added rows, source-only removed rows, and a per-column mismatch tally, which is skipped when no column is compared. With `pushdown_sample_rows` set, one more statement fetches a sample of the changed rows with their values; see [Row samples](#row-samples). Those fill every `DiffSummary` field including `column_mismatches`, so `threshold`, `match_rate_percentage`, and the drift report mean the same thing they do for local comparisons. The duplicate-key check costs one grouped scan per relation, over its normalized keys, and raises `DataIntegrityError` before any count or join runs, exactly as a local run refuses keys that repeat.

## Columns

Rules that select columns by `pattern` are matched against the probed column names before any SQL is compiled, so they apply in the warehouse exactly as they do locally.

Every column present on both sides is compared, exactly as it is locally. Columns without an explicit rule inherit the global `default_*` settings, so a `default_absolute_tolerance` applies in the warehouse too. As in a local run, a tolerance only loosens a column that is numeric once normalized, such as a text column with `cast_to: Float64`; text, boolean, and temporal columns are compared exactly. Likewise `max_levenshtein_distance` only loosens a column that is text once normalized, and compiles to Snowflake's `EDITDISTANCE` or Databricks' `levenshtein`, which count characters as a local run does. Columns marked `ignore` are excluded, and `rename_to` pairs a source column with its renamed target counterpart.

The column probes enforce `schema_mode` and primary-key existence before any comparison runs, raising `ConfigError` on drift. Probed names are compared exactly as the compiler quotes them, with no case folding, so YAML identifiers must match the stored column case (Snowflake stores unquoted names uppercase). `normalize_column_names` cannot change that: pushdown raises `ConfigError` if it would rename a stored column.

## Rules

Write `regex_replace` patterns, `value_map` entries, and text `null_values` exactly as you would for a local run. Each is escaped for the target warehouse's string-literal rules, so a backslash in `\d` or `\N` and an apostrophe in `O'Brien` arrive intact; do not double them yourself. Escaping preserves the text, but each warehouse still runs its own regex engine. Write capture-group references in a replacement as Polars reads them, `$1` or `${1}`, with `$0` for the whole match and `$$` for a dollar sign: pushdown rewrites them in each warehouse's own spelling, `\1` on Snowflake, BigQuery, DuckDB, and Postgres. A backslash in a replacement is plain text, as it is in Polars. Refer to groups by number, 0 through 9: no warehouse can refer to a group by name in a replacement, so a named reference raises `ConfigError`. That includes `$1a`, which Polars reads as the group named `1a`; write `${1}a` for group 1 followed by `a`. `whitespace_mode` strips the same characters in every warehouse as in a local run: spaces, tabs, line breaks, no-break spaces, and the rest of Unicode's whitespace.

Two stages need explaining:

- `pad_zeros` compiles to a sign-aware, non-truncating expression rather than a bare `LPAD`, which pads in front of a minus sign (`0-12` where Python gives `-012`) and discards characters past the target width.
- `timezone` emits no SQL. Polars rewrites only a column's timezone label, and every downstream cast and comparison still reads the underlying UTC instant, so the conversion cannot change a verdict. Warehouses have no per-column label to rewrite, and Spark's `TIMESTAMP` is a bare instant. The functions that look equivalent shift the value to a wall clock instead, which would make pushdown disagree with a local run. The rule's precondition is still enforced: a column that is naive or non-temporal in the probed schema raises `ConfigError`, exactly as it would locally.

`datetime_format` is translated directive by directive into each dialect's own format language, from a fixed table. `%Y-%m-%d` becomes `YYYY"-"MM"-"DD` on Snowflake and `yyyy'-'MM'-'dd` on Databricks, whose parser reads Java `DateTimeFormatter` patterns. Directives outside the table raise `ConfigError` rather than being passed through, since an untranslated directive parses nothing and returns NULL for every row, which would read as a clean match.

## Differences from a local run

All nine transform stages compile for compared columns, and stages 1 through 7 for primary keys, so a rule means the same thing in a warehouse as it does locally. The exception is `min_jaro_winkler_similarity`, which pushdown refuses with `ConfigError` before any comparison query runs rather than approximating it; see [Fuzzy Text Matching](rules.md#fuzzy-text-matching). Postgres also refuses `datetime_format` and `max_levenshtein_distance`; see [Comparing inside Postgres](#postgres). These behaviors still differ from the local path that files, lakehouse tables, and databases take:

- Artifacts contain primary keys only, since the comparison SQL never projects full rows. They are written as `added_rows_pks_only`, `removed_rows_pks_only`, and `changed_rows_pks_only` so they cannot be confused with local artifacts, which hold complete records. A [row sample](#row-samples), when asked for, is written as `changed_rows_sample`.
- `strict_types` compares the types the warehouse driver reports for each side, after normalization, and fails every row of a column whose two types differ, as a local run does. Those are the driver's types, not the declared ones: Snowflake's `NUMBER(38,0)`, for one, arrives as a decimal, so it meets a `NUMBER(38,0)` column but not a `FLOAT`.

Parity is verified by a differential test harness that runs both engines over the same frames and compares the results. The harness runs compiled SQL through DuckDB, which catches semantic errors such as null propagation, three-valued logic, and operator precedence, but not differences between vendors. Snowflake, Databricks, and BigQuery spellings are pinned by direct assertions on the emitted SQL instead. Postgres statements run for real: CI repeats the harness against a live Postgres 16, comparing each case inside Postgres and after reading the tables back. DuckDB's `levenshtein` counts bytes rather than characters, so edit-distance parity is checked on ASCII text, where the two agree. A property test also draws random configurations and data, from integers at the edges of their types to NULLs, NaN, and text timestamps, and requires both engines to reach the same counts on each.

## BigQuery

BigQuery differs from the other warehouses in a few ways a comparison can notice:

- `datetime_format` cannot use `%f`, since BigQuery spells fractional seconds only as part of the seconds. A format with `%z` parses to an aware timestamp, and one without it to a naive one, as Polars does.
- Comparing columns of different types fails the statement rather than coercing one side, so give such a pair a `cast_to`, or set `strict_types`.
- `GEOGRAPHY` and `JSON` columns cannot be compared; `ignore` them.

## Postgres

Two Postgres tables on one server can be compared where they are stored instead of read into memory. Set `pushdown: true` on both sides:

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

Veridelta then compiles the comparison to SQL and runs each statement inside Postgres through ConnectorX, as it does in a warehouse, so only counts and primary keys come back. The same requirements and differences apply as for [warehouse pushdown](#when-a-comparison-is-pushed-down), and a few more:

- Both `uri` values start with `postgresql://` or `postgres://` and match, as do both `password` values, so one connection reaches both tables. Each side names a `table`; a `query` cannot be compared in place. Other databases, Redshift included, are always read and compared locally.
- Set `pushdown` on both sides or on neither. A pair where only one side sets it raises `ConfigError` rather than quietly reading both.
- The server must read string literals by the SQL standard, which is the Postgres default (`standard_conforming_strings` on). Veridelta checks before the first statement and raises `ConnectorError` if it is off, since a backslash in a value would otherwise be read as an escape.
- `datetime_format` and `max_levenshtein_distance` raise `ConfigError` before any statement runs, as `min_jaro_winkler_similarity` does in every warehouse. Postgres has no date parse that returns NULL for text it cannot read, so one bad value would fail the whole statement, and its `levenshtein` needs the `fuzzystrmatch` extension and refuses text longer than 255 characters. Leave `pushdown` off to compare such columns locally. `veridelta validate` warns about both.
- ConnectorX reports every `numeric` as `Decimal(38, 10)`, which shows in two places. `strict_types` treats `numeric(10, 2)` and `numeric(12, 4)` as one type, with or without `pushdown`. And a `numeric` turned into text by `pad_zeros` or `cast_to: String` keeps its stored scale inside Postgres but gets ten decimal places when read locally, so seven in a `numeric(20, 0)` column is `7` with `pushdown` and `7.0000000000` without it.

## Row samples

A pushdown run, in a warehouse or inside Postgres, reads back counts and keys, so its report can say which rows changed but not how. Set `pushdown_sample_rows` to see values for some of them:

```yaml
primary_keys: ["order_id"]
pushdown_sample_rows: 50
```

After the counts, one more statement fetches up to that many changed rows, lowest keys first, so the same tables give the same sample. Each row holds its keys and, for every compared column, `{column}_source`, `{column}_target`, and `{column}_is_match`, exactly as a local run's changed rows do. The values are the ones the comparison saw, after every rule up to the comparison itself, and each flag is the predicate that decided the row. The counts do not change: the sample comes from the same changed rows they count.

The sample reaches the HTML report, whose changed-rows table shows it in place of the bare keys; `DiffResult.changed_sample`; and, with `output_path` set, a `changed_rows_sample` artifact. It never reaches a log line, the `--json` summary, or the Markdown summary, which the [CI integrations](ci.md) post as a pull request comment. Values do leave the warehouse, though, and the GitHub Action uploads the HTML report as a workflow artifact, so set it only where everyone who can open the report may read the data. `0`, the default, fetches nothing.
