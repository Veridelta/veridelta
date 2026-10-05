# Configuration Guide

Veridelta is driven by a declarative YAML configuration file or Python object. This specification defines data ingestion parameters, schema alignment constraints, and the semantic rules for engine evaluation.

## Core Architecture

Execution parameters and datasets are defined at the root level of the configuration. The engine requires deterministic identifiers to perform record alignment.

```yaml
source:
  path: "legacy_system.csv"
  format: "csv"

target:
  path: "modern_system.parquet"
  format: "parquet"

# Mandatory alignment key
primary_keys: ["user_id"]
```

`primary_keys` must name at least one column, and together the keys must be unique on each side.

File sources may omit `type` (it defaults to `file`) and continue to use `path`, `format`, and optional `options`.

### File formats

`format` accepts `csv`, `parquet`, `json`, `ndjson`, `arrow`, `avro`, and `excel`. Anything else is rejected when the config loads, rather than partway through a run.

`options` are handed straight to the matching Polars reader, so `{"separator": ";"}` reaches `scan_csv` and `{"sheet_name": "Q3"}` reaches `read_excel`.

Most formats stream. Three do not, because Polars has no lazy reader for them: a `json` document is one array that cannot be parsed incrementally, a spreadsheet is a random-access container, and Polars reads Avro eagerly. All three are read whole into memory. Prefer `ndjson` over `json` for anything large.

Avro files carry their schema, so columns arrive as the writer typed them. `options` takes `columns` and `n_rows`. The reader takes a local path: an object-store URL such as `s3://` is not supported, so copy the file down first.

Excel needs an optional extra:

```bash
uv add 'veridelta[excel]'
```

Discrepancy artifacts write to `csv`, `parquet`, `json`, `ndjson`, or `arrow` via `output_format`. Excel is deliberately absent: writing a workbook needs a second dependency that a discrepancy dump does not justify. So is Avro: the Polars writer cannot store several types a discrepancy frame can hold, such as `Int8`, unsigned integers, nanosecond or timezone-aware timestamps, and all-NULL columns.

### Editor support

Veridelta publishes a JSON Schema for configuration files. Editors that use the YAML language server, such as VS Code with the Red Hat YAML extension, then complete keys, show each field's description, and flag a typo such as `primary_key` or `absolute_tolerence` as you type. Point a file at the schema with a comment on its first line:

```yaml
# yaml-language-server: $schema=https://veridelta.github.io/veridelta/schema/veridelta.schema.json
source:
  path: "legacy_system.csv"

target:
  path: "modern_system.parquet"
  format: "parquet"

primary_keys: ["user_id"]
```

A `$schema:` key does not work: the loader rejects keys it does not know.

The site's copy follows the main branch. To pin the schema to the release you run, use the copy in that release's tag, such as `https://raw.githubusercontent.com/Veridelta/veridelta/v0.11.0/docs/schema/veridelta.schema.json`, or print the installed version's schema and point at the file:

```bash
veridelta schema > veridelta.schema.json
```

The schema is a little stricter than the loader. The loader converts `threshold: "0.1"` to a number, while the schema flags the quotes. In `source` and `target`, every text field accepts a `${NAME}` reference (see [Environment variables](#environment-variables)).

## Warehouse, lakehouse, and database sources

Set `type` on `source` and `target` to select a connector. Warehouse, lakehouse, and database drivers are optional extras:

```bash
uv add 'veridelta[snowflake]'
uv add 'veridelta[databricks]'
uv add 'veridelta[bigquery]'
uv add 'veridelta[delta]'
uv add 'veridelta[iceberg]'
uv add 'veridelta[database]'
uv add 'veridelta[all]'
```

Do not commit `password` or `access_token` in YAML, including a database source's `password`. Write `${NAME}` so the loader reads them from the environment (see [Environment variables](#environment-variables)), or build the connection in Python (for example `SnowflakeConfig(..., password=os.environ["SNOWFLAKE_PASSWORD"])`) and pass it to `DiffEngine.run_from_configs`.

Same-warehouse SQL pushdown runs only when both sides are Snowflake, both are Databricks, or both are BigQuery, the connection fields match (`account`, `user`, `warehouse`, `database`, `schema_name`, `role`, and `password` for Snowflake; `server_hostname`, `http_path`, `access_token`, `catalog`, and `schema_name` for Databricks; `project`, `dataset`, `location`, `credentials_path`, and `maximum_bytes_billed` for BigQuery), and the `table` names differ; naming the same table twice raises `ConfigError`, since a table compared with itself always matches. Mixed file/lakehouse/database and warehouse backends, or Snowflake paired with Databricks, raise `ConnectorError`. Database sources are read into the local engine like files, unless both are Postgres tables that set `pushdown`; see [Comparing inside Postgres](#comparing-inside-postgres).

`table` must be one to three unquoted identifier segments (`EVENTS`, `schema.table`, or `catalog.schema.table`). Rules that select columns by `pattern` are matched against the probed column names before any SQL is compiled, so they apply in the warehouse exactly as they do locally.

Pushdown issues up to ten statements per run: a zero-row column probe, a duplicate-key check, and a `COUNT(*)` per side, then inner-join mismatches, target-only added rows, source-only removed rows, and a per-column mismatch tally, which is skipped when no column is compared. With `pushdown_sample_rows` set, one more statement fetches a sample of the changed rows with their values; see [Row samples](#row-samples). Those fill every `DiffSummary` field including `column_mismatches`, so `threshold`, `match_rate_percentage`, and the drift report mean the same thing they do for local comparisons. The duplicate-key check costs one grouped scan per relation, over its normalized keys, and raises `DataIntegrityError` before any count or join runs, exactly as a local run refuses keys that repeat.

Every column present on both sides is compared, exactly as it is locally. Columns without an explicit rule inherit the global `default_*` settings, so a `default_absolute_tolerance` applies in the warehouse too. As in a local run, a tolerance only loosens a column that is numeric once normalized, such as a text column with `cast_to: Float64`; text, boolean, and temporal columns are compared exactly. Likewise `max_levenshtein_distance` only loosens a column that is text once normalized, and compiles to Snowflake's `EDITDISTANCE` or Databricks' `levenshtein`, which count characters as a local run does. Columns marked `ignore` are excluded, and `rename_to` pairs a source column with its renamed target counterpart.

The column probes enforce `schema_mode` and primary-key existence before any comparison runs, raising `ConfigError` on drift. Probed names are compared exactly as the compiler quotes them, with no case folding, so YAML identifiers must match the stored column case (Snowflake stores unquoted names uppercase). `normalize_column_names` cannot change that: pushdown raises `ConfigError` if it would rename a stored column.

All nine transform stages compile for compared columns, and stages 1 through 7 for primary keys, so a rule means the same thing in a warehouse as it does locally. The exception is `min_jaro_winkler_similarity`, which pushdown refuses with `ConfigError` before any comparison query runs rather than approximating it; see [Fuzzy Text Matching](#8-fuzzy-text-matching). Postgres also refuses `datetime_format` and `max_levenshtein_distance`; see [Comparing inside Postgres](#comparing-inside-postgres). These behaviors still differ from the local path that files, lakehouse tables, and databases take:

- Artifacts contain primary keys only, since the comparison SQL never projects full rows. They are written as `added_rows_pks_only`, `removed_rows_pks_only`, and `changed_rows_pks_only` so they cannot be confused with local artifacts, which hold complete records. A [row sample](#row-samples), when asked for, is written as `changed_rows_sample`.
- `strict_types` compares the types the warehouse driver reports for each side, after normalization, and fails every row of a column whose two types differ, as a local run does. Those are the driver's types, not the declared ones: Snowflake's `NUMBER(38,0)`, for one, arrives as a decimal, so it meets a `NUMBER(38,0)` column but not a `FLOAT`.

Parity is verified by a differential test harness that runs both engines over the same frames and compares the results. The harness executes compiled SQL through DuckDB, which catches semantic errors -- null propagation, three-valued logic, operator precedence -- but cannot catch vendor-specific divergence. Snowflake, Databricks, and BigQuery spellings are pinned by direct assertions on the emitted SQL instead. Postgres statements run for real: CI repeats the harness against a live Postgres 16, comparing each case inside Postgres and after reading the tables back. DuckDB's `levenshtein` counts bytes rather than characters, so edit-distance parity is checked on ASCII text, where the two agree. A property test also draws random configurations and data, from integers at the edges of their types to NULLs, NaN, and text timestamps, and requires both engines to reach the same counts on each.

Write `regex_replace` patterns, `value_map` entries, and text `null_values` exactly as you would for a local run. Each is escaped for the target warehouse's string-literal rules, so a backslash in `\d` or `\N` and an apostrophe in `O'Brien` arrive intact; do not double them yourself. Escaping preserves the text, but each warehouse still runs its own regex engine. Write capture-group references in a replacement as Polars reads them, `$1` or `${1}`, with `$0` for the whole match and `$$` for a dollar sign: pushdown rewrites them in each warehouse's own spelling, `\1` on Snowflake, BigQuery, DuckDB, and Postgres. A backslash in a replacement is plain text, as it is in Polars. Refer to groups by number, 0 through 9: no warehouse can refer to a group by name in a replacement, so a named reference raises `ConfigError`. That includes `$1a`, which Polars reads as the group named `1a`; write `${1}a` for group 1 followed by `a`. `whitespace_mode` strips the same characters in every warehouse as in a local run: spaces, tabs, line breaks, no-break spaces, and the rest of Unicode's whitespace.

Two stages need explaining:

- `pad_zeros` compiles to a sign-aware, non-truncating expression rather than a bare `LPAD`, which pads in front of a minus sign (`0-12` where Python gives `-012`) and discards characters past the target width.
- `timezone` emits no SQL. Polars rewrites only a column's timezone label, and every downstream cast and comparison still reads the underlying UTC instant, so the conversion cannot change a verdict. Warehouses have no per-column label to rewrite -- Spark's `TIMESTAMP` is a bare instant -- and the functions that look like the equivalent instead shift the value to a wall clock, which would make pushdown disagree with a local run. The rule's precondition is still enforced: a column that is naive or non-temporal in the probed schema raises `ConfigError`, exactly as it would locally.

`datetime_format` is translated directive by directive into each dialect's own format language, from a fixed table. `%Y-%m-%d` becomes `YYYY"-"MM"-"DD` on Snowflake and `yyyy'-'MM'-'dd` on Databricks, whose parser reads Java `DateTimeFormatter` patterns. Directives outside the table raise `ConfigError` rather than being passed through, since an untranslated directive parses nothing and returns NULL for every row, which would read as a clean match.

From Python, load YAML then route through the same path the CLI uses:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
summary = result.summary
```

```yaml
source:
  type: snowflake
  table: ANALYTICS.PUBLIC.LEGACY_EVENTS
  account: xy12345
  user: analyst
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

target:
  type: snowflake
  table: ANALYTICS.PUBLIC.MODERN_EVENTS
  account: xy12345
  user: analyst
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

primary_keys: ["event_id"]
```

```yaml
source:
  type: databricks
  table: main.default.legacy_events
  server_hostname: adb.azuredatabricks.net
  http_path: /sql/1.0/warehouses/abc
  catalog: main
  schema_name: default

target:
  type: databricks
  table: main.default.modern_events
  server_hostname: adb.azuredatabricks.net
  http_path: /sql/1.0/warehouses/abc
  catalog: main
  schema_name: default

primary_keys: ["event_id"]
```

Lakehouse tables are scanned as unevaluated Polars LazyFrames (install the `delta` or `iceberg` extra):

```yaml
source:
  type: delta
  table_uri: s3://lake/legacy_events
  version: 12
  storage_options:
    AWS_REGION: us-east-1

target:
  type: iceberg
  table_uri: s3://lake/iceberg/modern_events
  snapshot_id: 883142
  storage_options:
    AWS_REGION: us-east-1

primary_keys: ["event_id"]
```

`storage_options` is a string map passed through to the Delta or Iceberg scanner (credentials, region, and other object-store settings).

### BigQuery

A `bigquery` block names a `project` and a `table`. The project runs the queries and holds the data; it never appears in SQL, so a project id with hyphens is fine. The table is `dataset.table`, or `table` alone when `dataset` names the default dataset.

```yaml
source:
  type: bigquery
  project: analytics-prod
  table: legacy.events
  location: US
  maximum_bytes_billed: 50000000000

target:
  type: bigquery
  project: analytics-prod
  table: modern.events
  location: US
  maximum_bytes_billed: 50000000000

primary_keys: ["event_id"]
```

Credentials come from Application Default Credentials, such as `gcloud auth application-default login` on a workstation or the attached service account on Google Cloud. Set `credentials_path` to a service account key file to use that instead. `maximum_bytes_billed` makes BigQuery refuse any statement that would bill more, which caps what a run can cost. Project ids follow Google's rules: six to thirty lowercase letters, digits, or hyphens. Older domain-scoped ids such as `example.com:project` are refused.

BigQuery differs from the other warehouses in a few ways a comparison can notice:

- `datetime_format` cannot use `%f`, since BigQuery spells fractional seconds only as part of the seconds. A format with `%z` parses to an aware timestamp, and one without it to a naive one, as Polars does.
- Comparing columns of different types fails the statement rather than coercing one side, so give such a pair a `cast_to`, or set `strict_types`.
- `GEOGRAPHY` and `JSON` columns cannot be compared; `ignore` them.

### Database sources

A `database` source reads a table, or the result of a query, from an operational database into Polars through [ConnectorX](https://github.com/sfu-db/connector-x). Install the `database` extra. The comparison runs locally, so a database pairs with a file, a lakehouse table, or another database, and `crosswalk` reads it too.

```yaml
source:
  type: database
  uri: postgresql://analyst@legacy-db.internal:5432/sales
  password: ${LEGACY_DB_PASSWORD}
  table: public.orders

target:
  type: database
  uri: mysql://analyst@modern-db.internal:3306/sales
  password: ${MODERN_DB_PASSWORD}
  query: SELECT order_id, total, status FROM orders WHERE placed >= '2024-01-01'

primary_keys: ["order_id"]
```

- `uri` is a ConnectorX connection string: `postgresql://`, `mysql://` (MariaDB too), `mssql://`, `oracle://`, `redshift://`, `clickhouse://`, or `sqlite://` followed by a file path, as in `sqlite:///srv/data/legacy.db` or, on Windows, `sqlite://C:/data/legacy.db`. A SQLite path must name an existing file; Veridelta refuses a missing one rather than let ConnectorX create an empty database there.
- Set exactly one of `table` and `query`. `table` is one to three identifier segments, each quoted for the database: double quotes for Postgres, Redshift, Oracle, and SQLite, backticks for MySQL and ClickHouse, and brackets for SQL Server. Quoting keeps case, so write names as they are stored. Any other scheme needs `query`.
- `query` is sent to the database exactly as written. Veridelta cannot tell a read from a write, so connect with a role that can only read. It is expanded like any other `source` string, so write a literal `${` inside it as `$${`.
- `password` is percent-encoded into the URI, so it may contain `@`, `:`, `/`, or any other character, and needs a user name in `uri`. A password written into `uri` itself must already be percent-encoded, which an expanded `${VAR}` is not, and setting both fails when the file loads. Credentials passed as URI parameters, such as `?password=`, are not masked in logs or errors, so use `password`.
- The rows are read into memory once, before the comparison starts, because Polars has no lazy database reader. Select and filter in `query` rather than reading a whole table you mostly ignore.
- Column types come from the database driver. For SQLite that means declared types: `INTEGER`, `REAL`, `TEXT`, `DATE`, `DATETIME`, `BOOLEAN`, and `NUMERIC` arrive as Int64, Float64, String, Date, Datetime, Boolean, and Float64. A column declared without a type whose first rows are NULL cannot be typed and fails the read. Every Postgres `numeric` arrives as `Decimal(38, 10)`, whatever its declared precision and scale: values are rounded to ten decimal places, and one with more than 18 digits before the point fails the read.

#### Comparing inside Postgres

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

Veridelta then compiles the comparison to SQL and runs each statement inside Postgres through ConnectorX, as it does in a warehouse, so only counts and primary keys come back. The same requirements and differences apply as for [warehouse pushdown](#warehouse-lakehouse-and-database-sources), and a few more:

- Both `uri` values start with `postgresql://` or `postgres://` and match, as do both `password` values, so one connection reaches both tables. Each side names a `table`; a `query` cannot be compared in place. Other databases, Redshift included, are always read and compared locally.
- Set `pushdown` on both sides or on neither. A pair where only one side sets it raises `ConfigError` rather than quietly reading both.
- The server must read string literals by the SQL standard, which is the Postgres default (`standard_conforming_strings` on). Veridelta checks before the first statement and raises `ConnectorError` if it is off, since a backslash in a value would otherwise be read as an escape.
- `datetime_format` and `max_levenshtein_distance` raise `ConfigError` before any statement runs, as `min_jaro_winkler_similarity` does in every warehouse. Postgres has no date parse that returns NULL for text it cannot read, so one bad value would fail the whole statement, and its `levenshtein` needs the `fuzzystrmatch` extension and refuses text longer than 255 characters. Leave `pushdown` off to compare such columns locally. `veridelta validate` warns about both.
- ConnectorX reports every `numeric` as `Decimal(38, 10)`, which shows in two places. `strict_types` treats `numeric(10, 2)` and `numeric(12, 4)` as one type, with or without `pushdown`. And a `numeric` turned into text by `pad_zeros` or `cast_to: String` keeps its stored scale inside Postgres but gets ten decimal places when read locally, so seven in a `numeric(20, 0)` column is `7` with `pushdown` and `7.0000000000` without it.

### Row samples

A pushdown run, in a warehouse or inside Postgres, reads back counts and keys, so its report can say which rows changed but not how. Set `pushdown_sample_rows` to see values for some of them:

```yaml
primary_keys: ["order_id"]
pushdown_sample_rows: 50
```

After the counts, one more statement fetches up to that many changed rows, lowest keys first, so the same tables give the same sample. Each row holds its keys and, for every compared column, `{column}_source`, `{column}_target`, and `{column}_is_match`, exactly as a local run's changed rows do. The values are the ones the comparison saw, after every rule up to the comparison itself, and each flag is the predicate that decided the row. The counts do not change: the sample comes from the same changed rows they count.

The sample reaches the HTML report, whose changed-rows table shows it in place of the bare keys; `DiffResult.changed_sample`; and, with `output_path` set, a `changed_rows_sample` artifact. It never reaches a log line, the `--json` summary, or the Markdown summary, which the [CI integrations](ci.md) post as a pull request comment. Values do leave the warehouse, though, and the GitHub Action uploads the HTML report as a workflow artifact, so set it only where everyone who can open the report may read the data. `0`, the default, fetches nothing.

### Connection fields

Every connector block is selected by `type` and rejects keys it does not list.

| `type` | Required | Optional |
| :--- | :--- | :--- |
| `file` (default) | `path` | `format` (default `csv`), `options` |
| `snowflake` | `table`, `account`, `user`, `warehouse`, `database`, `schema_name` | `password`, `role` |
| `databricks` | `table`, `server_hostname`, `http_path` | `access_token`, `catalog`, `schema_name` |
| `bigquery` | `table`, `project` | `dataset`, `location`, `credentials_path`, `maximum_bytes_billed` |
| `delta` | `table_uri` | `version`, `storage_options` |
| `iceberg` | `table_uri` | `snapshot_id`, `storage_options` |
| `database` | `uri`, and exactly one of `table` or `query` | `password`, `pushdown` |

`version` and `snapshot_id` must be non-negative integers, and `maximum_bytes_billed` a positive one; a quoted number is rejected rather than coerced, because each is passed straight to a scan or a job. Warehouse, lakehouse, and database blocks are frozen once loaded.

Printing a connection config, or formatting one into a log line, leaves out its credentials: `password` for Snowflake and databases, `access_token` for Databricks, `credentials_path` for BigQuery, `storage_options` for Delta Lake and Iceberg, and a `storage_options` map nested in a file source's `options`, whose other reader options still print. A password written inside a database `uri` prints as `***`, and the rest of the URI prints as written. They stay readable as attributes and in `model_dump()`, because the connectors and readers need them, so log a dump only after removing them.

### Environment variables

Any string inside `source` or `target` can read an environment variable, so credentials and per-environment paths stay out of the file:

```yaml
source: &warehouse
  type: snowflake
  table: ANALYTICS.PUBLIC.LEGACY_EVENTS
  account: xy12345
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  role: ${SNOWFLAKE_ROLE:-ANALYST}
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

target:
  <<: *warehouse
  table: ANALYTICS.PUBLIC.MODERN_EVENTS

primary_keys: ["event_id"]
```

- `${NAME}` is replaced by the variable's value, and can sit inside longer text, as in `s3://${LAKE_BUCKET}/events`. A variable that is set but empty gives empty text.
- `${NAME:-default}` uses `default` when the variable is unset or empty. The default is literal text, and cannot contain `}` or another reference.
- `$${` writes a literal `${`, so a value that should contain `${` must be written this way.
- Values read from the environment are never expanded again, so a secret containing `$` or `${` arrives intact.
- Nested values such as `storage_options` and file `options` are expanded too, but keys, numbers, and booleans are not. Root settings and `rules` are read verbatim, so a `${1}` in a `regex_replace` replacement is left alone.

A reference to an unset variable without a default, or a malformed one (`${1}`, `${NAME`, `${NAME-x}`, or a nested `${A:-${B}}`), raises `ConfigError` when the file loads. The error names the field, such as `source -> password`, and the variable when there is one, but never repeats a value; validation errors for `source` and `target` omit their input for the same reason.

A database `password` is percent-encoded when it joins the URI, but a `${VAR}` expanded inside `uri` is not, so keep database passwords in `password`.

Expanded values are text. `version` and `snapshot_id` accept only YAML integers, so write those literally, and file `options` reach the reader as they are, so an expanded option arrives as a string.

### Connector logging

Connectors log under `veridelta.connectors.warehouse`, `veridelta.connectors.lakehouse`, and `veridelta.connectors.database`, with a `NullHandler` attached so nothing prints unless you opt in. `INFO` records a session or scan opening and closing, and each database read, Postgres pushdown statements included, with its row count and the URI with its password masked; `DEBUG` records each pushdown statement by its round-trip kind (`schema`, `duplicates`, `count`, `mismatch`, `added`, `missing`, `columns`, `samples`) with its duration. Log lines never contain SQL text, row values, `storage_options`, passwords, or tokens. A warehouse session is closed when the run finishes, whether it succeeded or raised.

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("veridelta.connectors").setLevel(logging.DEBUG)
```

## Reading the results

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`. It carries the metrics on `.summary` and the rows behind them on `.added`, `.removed`, and `.changed`, so a notebook never has to export artifacts to disk just to look at the drift.

```python
result = DiffEngine(diff, source_df, target_df).run()

print(result.summary.report_summary)

# Just the rows where one column disagreed, with both values side by side.
result.get_mismatches("total_amount")

# The full changed set as pandas, if you have it installed.
result.to_pandas()
```

`summary` stays a plain Pydantic model, so `summary.model_dump_json()` still produces a clean, frame-free payload for CI logs.

The same standalone HTML report the CLI writes with `--html` is available from Python. It embeds its own styles and script, so it opens offline, and `max_rows` caps every table the way `--html-max-rows` does:

```python
from veridelta.report import write_html, write_markdown

write_html(result, "reports/nightly.html", max_rows=1000)
write_markdown(result, "reports/summary.md")
```

`write_markdown` (and `render_markdown`, which returns the text) produces the short form CI posts to a job summary or a pull request: the verdict, a table of counts, and the drifting columns, limited to `report_top_columns_limit`. Column names are written as code so a name from the data cannot break the table or the page it lands on.

Warehouse pushdown compares in place and never projects values, so its frames hold primary keys alone and the result is flagged `keys_only`. `get_mismatches` there returns every changed key rather than one column's values, and still rejects a column that was not part of the comparison. A run with `pushdown_sample_rows` set also carries `changed_sample`: up to that many changed rows with each compared column's values, laid out like a local run's `changed`; see [Row samples](#row-samples).

### OpenTelemetry metrics

`veridelta run --otel otel-metrics.json` writes the run's metrics for an observability backend such as Datadog or Grafana, through an OpenTelemetry Collector or any OTLP/HTTP endpoint. It needs no OpenTelemetry package. From Python:

```python
from veridelta.telemetry import write_otlp_metrics

write_otlp_metrics(
    result, "reports/otel-metrics.json", config_path="veridelta.yaml", source=source, target=target
)
```

Every metric is a gauge, stamped with the time the file is written, so each run adds one point to each series:

| Metric | Unit | Attributes | Value |
| :--- | :--- | :--- | :--- |
| `veridelta.dataset.rows` | `{row}` | `veridelta.side`: `source` or `target` | Rows in each dataset. |
| `veridelta.diff.rows` | `{row}` | `veridelta.diff.kind`: `added`, `removed`, or `changed` | Rows only in the target, only in the source, or in both with a value that differs. |
| `veridelta.column.mismatched_rows` | `{row}` | `veridelta.column.name` | Changed rows whose value in that column differs. Every compared column reports, so one that stops drifting reads `0` rather than disappearing. |
| `veridelta.diff.mismatch_ratio` | `1` | | Added, removed, and changed rows over the source rows, as `mismatch_ratio` is. |
| `veridelta.diff.match` | | | `1` when the run matched within `threshold`, `0` when it drifted. It has no unit, since a backend such as Prometheus would read a unit of `1` as a ratio. |

Resource attributes say which comparison ran:

- `service.name`, which is `veridelta`, and `service.version`;
- `veridelta.config.path`, the configuration file;
- `veridelta.source.type` and `veridelta.target.type`, such as `file` or `snowflake`;
- `veridelta.source.name` and `veridelta.target.name`: the table, or the file or lakehouse path.

A URL keeps only its scheme, host, and path, so a token in its user part or a signature in its query never leaves the run, and a database `query` source has no name. Like the Markdown summary, the file holds counts and column names, never row values, connection URIs, credentials, or SQL. A run that fails before it has a result writes no file. Veridelta does not read `OTEL_RESOURCE_ATTRIBUTES`; to tag runs with an environment or a team, add attributes in the Collector, with its `resource` processor for one.

The file is one line of JSON in OTLP's JSON encoding, as the [OpenTelemetry file exporter format](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md) specifies, so the Collector's OTLP JSON file receiver, in its contrib distribution, can read it. The same line is the body an OTLP/HTTP endpoint accepts:

```bash
curl --fail -X POST -H "Content-Type: application/json" \
  --data-binary @otel-metrics.json "$OTLP_ENDPOINT/v1/metrics"
```

## Command line

```bash
veridelta --version
veridelta run -c veridelta.yaml
veridelta run -c veridelta.yaml --json
veridelta run -c veridelta.yaml --quiet
veridelta run -c veridelta.yaml --html report.html --html-max-rows 1000
veridelta run -c veridelta.yaml --markdown summary.md
veridelta run -c veridelta.yaml --otel otel-metrics.json
veridelta crosswalk -c veridelta.yaml
veridelta crosswalk -c veridelta.yaml --min-confidence 0.99 --json
veridelta validate -c veridelta.yaml
veridelta validate -c veridelta.yaml --allow-missing-env --json
veridelta validate -c veridelta.yaml --schemas
veridelta schema > veridelta.schema.json
```

`--json` prints `DiffSummary` as JSON on stdout. `--quiet` suppresses progress chatter on stderr (the JSON line still prints). Progress chatter always goes to stderr, so `veridelta run --json | jq` does not have to strip anything first. `--html` writes a standalone report with no CDN references, capped at `--html-max-rows` (zero or more; default 1000) so a large diff cannot produce an unopenable file. `--markdown` writes the Markdown summary described above, which the [CI integrations](ci.md) post, and `--otel` the [OpenTelemetry metrics](#opentelemetry-metrics). Pushdown reports and summaries are labeled as primary-keys-only, and an HTML report shows a [row sample](#row-samples)'s values when the run fetched one.

Exit codes are `0` for a match within `threshold`, `1` for drift or any failure while running, and `2` for invalid command-line arguments.

`crosswalk` proposes `value_map` rules instead of comparing; see [Proposing a value map](#proposing-a-value-map). It exits `0` once the proposals are computed, whether or not it found any, `1` on failure, and `2` for invalid arguments.

`schema` prints the configuration file's JSON Schema; see [Editor support](#editor-support).

### Checking a configuration

`validate` reports what would stop a run, without reading any rows. By default it connects to nothing and checks that:

- the source and target are a pair an engine can compare;
- every optional extra the run needs is installed, in the environment `validate` runs in;
- a database `table` uses a URI scheme Veridelta can quote;
- each `regex_replace` pattern compiles in Polars, whose regular expressions, unlike Python's `re`, have no look-around or backreferences.

On a warehouse pair it also warns about settings the warehouse refuses only for some stored names or types: `normalize_column_names`, `min_jaro_winkler_similarity`, a `datetime_format` with no SQL spelling, and a `regex_replace` replacement that refers to a group by name. A pattern Polars rejects is only a warning there, since the warehouse runs it with its own engine.

`--schemas` also connects and checks the rules against each side's columns, never its rows:

- Files and lakehouse tables are opened as a run opens them and checked with `DiffEngine.validate_rules`. JSON and Excel files have no lazy reader, so they are read whole.
- A database `table` is read with `SELECT * FROM … WHERE 1 = 0`. SQLite reports a `NUMERIC` column as text in that probe, though a full read returns numbers. A `query` is not run, and that side is reported as unchecked.
- A warehouse pair runs the two column probes a run starts with, then compiles every comparison statement without executing it. That settles each warning above one way or the other.

Errors print to stdout as `error:` lines and warnings as `warning:` lines, or, with `--json`, as one object with `config`, `valid`, `errors`, and `warnings`. The verdict goes to stderr, and `--quiet` drops it. `validate` exits `0` when there are no errors, warnings or not, `1` when there are, and `2` for invalid arguments.

`--allow-missing-env` checks a file without its secrets, such as in a pull request job. An unset `${NAME}` with no default is read as the text `NAME`, with a warning. That works for fields that take a name, such as `table`, `account`, `password`, or `path`. A field with a required shape, such as a database `uri`, still fails unless its variable is set.

From Python, `DiffEngine.check_configs(diff, source, target, schemas=False)` returns the same findings as `ConfigFinding` models, and `load_config(path, unset_env=[])` reads unset variables as their names and appends each name to the list.

## Engine Directives

Global directives control the strictness of the underlying Polars evaluation engine.

| Directive | Description |
| :--- | :--- |
| `schema_mode` | Enforces column structure constraints. Options: `intersection` (default, compares common columns only), `exact`, `allow_additions`, `allow_removals`. |
| `strict_types` | If `false` (default), a column stored as different types on the two sides is still compared. Two numeric types compare by value, so an integer `10` and a float `10.7` differ, and a `Float32` `0.1` differs slightly from a `Float64` `0.1`; add a tolerance to forgive precision gaps. Any other pair soft-casts the target to the source type, so text `"10"` matches an integer `10`. If `true`, a column whose two sides hold different types after normalization fails every row, so a `cast_to` or `datetime_format` that brings both sides to one type keeps the column comparable. Under `treat_null_as_equal`, two NULLs still match. |
| `normalize_column_names`| If `true`, strips whitespace and lowercases all column headers prior to schema alignment, on every entry point including `DiffEngine(...).run()` and `validate_schemas`. Configured `primary_keys`, `column_names`, and `rename_to` are normalized the same way; a `pattern` is not, so write it against the lowercase names. Headers that collide once normalized raise `ConfigError`. |
| `threshold` | The allowable mismatch ratio (0.0 to 1.0) before the pipeline exits with a failure code. |
| `default_absolute_tolerance` | Global absolute numeric tolerance. A column without its own `absolute_tolerance` inherits this. Columns that are not numeric after normalization are compared exactly. |
| `default_relative_tolerance` | Global relative numeric tolerance. A column without its own `relative_tolerance` inherits this. Columns that are not numeric after normalization are compared exactly. |
| `default_treat_null_as_equal` | Global `NULL == NULL` policy. Defaults to `true`. A column rule can override it. |
| `default_whitespace_mode` | Global whitespace stripping: `none` (default), `left`, `right`, or `both`. |
| `default_null_values` | Global sentinel list. Applied only to columns whose type can hold each value. |
| `report_top_columns_limit` | How many drifted columns to list in `report_summary`. `0` hides the section. |
| `pushdown_sample_rows` | Warehouse pushdown only. Fetch up to this many changed rows with both sides' values, so the HTML report and the result show values rather than primary keys alone. `0` (default) fetches none, so no value leaves the warehouse. Local runs ignore it, since they hold every row. See [Row samples](#row-samples). |
| `output_path` | Directory to write discrepancy artifacts. Omitted means no files are written. |
| `output_format` | Artifact format: `parquet` (default), `csv`, `json`, `ndjson`, or `arrow`. |

```yaml
primary_keys: ["user_id"]
threshold: 0.01
default_absolute_tolerance: 0.01
default_relative_tolerance: 0.0
default_treat_null_as_equal: true
report_top_columns_limit: 5
output_path: "./artifacts"
output_format: parquet
```

### Schema dry run

`schema_mode` and primary-key presence can be enforced without reading any rows. `DiffEngine.validate_schemas` runs alignment and the schema check on metadata alone, so it accepts zero-row frames or unevaluated scans and is cheap enough to gate a deployment:

```python
import polars as pl

from veridelta import ConfigError, DiffConfig, DiffEngine

contract = DiffConfig(primary_keys=["user_id"], schema_mode="exact")
try:
    DiffEngine.validate_schemas(contract, pl.scan_parquet("source.parquet"), pl.scan_parquet("target.parquet"))
except ConfigError as exc:
    print(exc)
```

It raises `ConfigError` on a violation and returns nothing otherwise.

`DiffEngine.validate_rules` takes the same arguments and goes one step further. It resolves every rule against the aligned columns and builds each column's comparison, still without reading a row, and returns the columns a run would compare. A rule the run could not honor fails here, such as a null sentinel the column's type cannot hold, or a similarity limit without the `fuzzy` extra. Repeated keys and invalid regular expressions only surface once rows are read.

## Column-Level Overrides (Rules)

The `rules` array defines granular, per-column or regex-pattern tolerances. A rule selects columns by exact `column_names` or by a regular expression in `pattern`, and every other field is optional. When a column is named by more than one rule, the first rule listing it by exact name wins, then the first whose `pattern` matches. One rule governs each column, `ignore` included, so an exact-name rule keeps a column that a broader ignore `pattern` would otherwise drop. A renamed column answers to both spellings: a rule listing its target name wins, then the rule listing its source name. Local runs and warehouse pushdown resolve rules the same way.

| Field | Description |
| :--- | :--- |
| `column_names` | Exact source column names this rule governs. |
| `pattern` | Regular expression matched against the start of each column name. |
| `absolute_tolerance` | Maximum absolute numeric difference. Overrides `default_absolute_tolerance`. Must be finite; use `ignore` to stop comparing a column. |
| `relative_tolerance` | Maximum relative numeric difference (`0.01` is 1%). Overrides `default_relative_tolerance`. Must be finite. |
| `max_levenshtein_distance` | Most single-character edits between two text values that still count as a match. Needs the `fuzzy` extra locally, and compiles for warehouse pushdown; see [Fuzzy Text Matching](#8-fuzzy-text-matching). |
| `min_jaro_winkler_similarity` | Lowest Jaro-Winkler similarity, above 0 and at most 1, between two text values that still counts as a match. Needs the `fuzzy` extra, and runs locally only. |
| `case_insensitive` | Lowercase text before comparing. |
| `whitespace_mode` | `none`, `left`, `right`, or `both`. Overrides `default_whitespace_mode`. |
| `regex_replace` | Mapping of regex pattern to replacement, applied in order to text columns. |
| `pad_zeros` | Stringify, then left-pad to this width. |
| `value_map` | Source-side crosswalk from legacy value to target value. |
| `null_values` | Sentinels coerced to NULL. Overrides `default_null_values`; must fit the column's type. |
| `treat_null_as_equal` | Whether `NULL == NULL` matches. Overrides `default_treat_null_as_equal`. |
| `datetime_format` | `strptime` pattern that parses text into timestamps. |
| `timezone` | Zone that timezone-aware timestamps are converted to. |
| `cast_to` | `Int64`, `Float64`, `String`, `Boolean`, `Date`, or `Datetime`. |
| `ignore` | Exclude the columns this rule governs from the comparison entirely. |
| `rename_to` | Target name for a single source column. |

### Transform order

Rules are not applied in the order you write them. Every column follows one fixed pipeline, and both the local engine and the warehouse compiler honor it, so a rule produces the same verdict wherever it runs:

1. Null sentinels (`null_values`)
2. Regex replace (`regex_replace`)
3. Whitespace, then case (`whitespace_mode`, `case_insensitive`)
4. Source-side value map (`value_map`)
5. Pad zeros (`pad_zeros`)
6. Datetime parsing, then timezone (`datetime_format`, `timezone`)
7. Explicit cast (`cast_to`)
8. Comparison (equality, numeric tolerance, or text similarity)
9. Null-safe equality (`treat_null_as_equal`)

Stages 1 through 7 normalize each dataset on its own, before any join, so they apply to primary keys as well, in local runs and warehouse pushdown alike: a `case_insensitive` rule on a key column changes how rows are matched, not just how they are compared. If normalizing a key collapses two rows into one, `DataIntegrityError` is raised rather than allowing a join explosion.

Stages 2, 3, and 4 operate on text and are skipped for non-string columns, so a global `default_whitespace_mode` is safe to set on a mixed schema. Stage 1 is filtered per column instead, as described below.

### 1. Numeric Tolerances
Bypass floating-point anomalies or acceptable system rounding differences.

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

A tolerance only loosens the comparison of finite values. `NaN` matches only `NaN`, and an infinity matches only the same infinity, however wide the tolerance. Integer differences are measured exactly, never wrapped around the column's type, so Int8 `100` and `-100` differ by 200.

### 2. Null Sentinels
Declare the placeholder values a system writes instead of NULL. Sentinels are not limited to text: a list can mix strings, numbers, and booleans.

Each sentinel is applied only to columns whose type can hold it. A text sentinel reaches string, categorical, and enum columns; a number reaches any numeric column, including decimals; a boolean reaches boolean columns only. Sentinels that do not fit a given column are dropped for that column alone, so one global list can cover a mixed schema without failing.

Quoting therefore carries meaning. `-999` is a numeric sentinel that nulls out `-999` in an integer column and is ignored on a text column, while `"-999"` is the text sentinel and behaves the other way around. List both if a value appears in both shapes.

```yaml
default_null_values: ["N/A", "", -999]

rules:
  - column_names: ["is_verified"]
    null_values: [false]
```

The distinction between the global default and an explicit rule is what happens when nothing fits. `default_null_values` is expected to span a mixed schema, so unusable combinations are skipped silently. An explicit `null_values` on a named column is a direct instruction, so if none of its sentinels can apply to that column's type, Veridelta raises `ConfigError` rather than silently doing nothing. Warehouse pushdown enforces the same rule against the probed schema.

`.nan` and `.inf` are rejected at load time. NaN never compares equal to itself, and infinity has no portable SQL literal.

### 3. String Normalization & Sanitization
Execute string mutations before type evaluation. Sanitization always precedes `cast_to`, so text is cleaned before it is coerced.

```yaml
rules:
  - column_names: ["user_email"]
    case_insensitive: true
    whitespace_mode: "both"

  - column_names: ["balance"]
    regex_replace:
      "\\$": ""  # Strip currency symbols before casting
    cast_to: "Float64"
```

`cast_to` accepts a fixed set of Polars type names: `Int64`, `Float64`, `String`, `Boolean`, `Date`, and `Datetime`. Anything else is rejected at load time. Casting a float to `Int64` truncates toward zero, matching Polars rather than SQL's rounding, on both the local and warehouse paths.

### 4. Padding, Dates, and Timezones
Reconcile identifiers and timestamps that two systems store in different shapes.

`pad_zeros` left-pads to a fixed width. The value is stringified first, so a numeric `123` in one system matches a text `"00123"` in the other. The width must be a real integer: `pad_zeros: "5"` is rejected rather than quietly coerced.

`datetime_format` parses text into timestamps using a [strptime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) pattern, so the column is compared as a timestamp instead of as text. Values that do not fit the pattern become NULL, which counts as a mismatch unless `treat_null_as_equal` is set. Warehouse pushdown supports `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%f`, `%z`, and `%%`, separated by spaces or any of `-` `/` `:` `.` `,` `_` `T`. Anything else raises `ConfigError`.

`%f` is a fraction of a second, as in Python, so `.5` is half a second, and both engines read one to six digits. A local run also reads seven to nine digits, keeping microseconds, and parses a value with no fraction at all when the format writes `.%f`; a warehouse may read either as NULL.

`timezone` converts timestamps to a common zone before comparison. It requires timezone-aware data. Naive timestamps raise `ConfigError`, because assuming an origin zone would shift every value by a real offset without telling you. To normalize text timestamps that carry an offset, parse them first with a format containing `%z`.

Note that the conversion is a relabeling. Comparisons and casts read the underlying instant, not the wall-clock reading in the target zone, so a `timezone` rule cannot change a verdict on its own. Its value is in making two differently-zoned columns comparable and in rejecting data that carries no zone at all.

```yaml
rules:
  - column_names: ["account_number"]
    pad_zeros: 10

  - column_names: ["created_at"]
    datetime_format: "%Y-%m-%d %H:%M:%S%z"
    timezone: "UTC"
```

### 5. Value Mapping (Crosswalks)
Translate legacy enumerations or system-specific codes to modern equivalents during evaluation.

```yaml
rules:
  - column_names: ["status_code"]
    value_map:
      "0": "INACTIVE"
      "1": "ACTIVE"
      "2": "PENDING"
```

#### Proposing a value map
Veridelta can draft these entries from the data. `veridelta crosswalk` aligns, normalizes, and joins the configured datasets exactly as `run` does, then proposes each source value for the target value it lines up with:

```bash
veridelta crosswalk -c veridelta.yaml > proposed.yaml
```

The evidence goes to stderr and the rules to stdout, ready to paste into the configuration:

```text
gender: 2 new value_map entries
  'M' -> 'Male': 599 of 600 rows (99.8%)
  'F' -> 'Female': 400 of 400 rows (100.0%)
```

```yaml
rules:
- column_names:
  - gender
  value_map:
    M: Male
    F: Female
```

An entry needs two things:

- **Confidence**, `--min-confidence` (default 0.95): the share of the source value's joined rows whose target is the proposed value. Every row with that source value counts, including rows that already match and rows whose target is NULL, so an entry that would break a matching row pays for it. The floor must be above 0.5, which leaves at most one candidate per source value.
- **Support**, `--min-support` (default 5): how many rows agree, so a coincidence in a handful of rows is never proposed.

Values are read as the `value_map` stage sees them, after null sentinels, `regex_replace`, whitespace, and case folding, so a `case_insensitive` column gets lowercase entries. Only compared text columns qualify, and only when nothing after stage 4 changes the mapped value: a column with `pad_zeros`, `datetime_format`, or a `cast_to` other than `String` is skipped, as are primary keys and ignored columns. A non-text target is read as the text it is compared as, so a `Y`/`N` source against a `1`/`0` target proposes `Y: '1'`, unless `strict_types` rules the pair out.

An existing `value_map` is kept and extended. Rows it already translates are left out of the counts, so a raw value that equals one of its outputs cannot receive an entry. Only one rule governs a column, so when a rule already governs one, the command says so on stderr, even with `--quiet`, and the new entries belong in that rule's `value_map` rather than in a second rule. If that rule also governs other columns, by listing several names or by a `pattern`, the column needs a rule of its own first, since a map merged into a shared rule applies to every column it governs; the note says which case applies.

`--sample-fraction` (default 1.0) reads that share of source rows, picked by a hash of the primary keys, so rerunning on the same data under one Polars version samples the same rows. `--json` prints each proposal with its evidence instead of YAML.

Two tables on one warehouse connection are counted in the warehouse, and no row leaves it. The connection requirements are those of a warehouse run: both sides use one backend, one connection, and two different tables, and a warehouse paired with a file or a database is refused. After the column probes and the duplicate-key checks, one statement counts every candidate column, and Veridelta applies the confidence floor itself, so a warehouse proposes exactly what a local run would from the same rows. Two differences remain:

- Only columns stored as text on both sides qualify. A local run also proposes text for a non-text target, such as `Y: '1'`, but each engine writes numbers and timestamps as text its own way, and the warehouse compares such an entry against the integer column rather than its text.
- A sample hashes the normalized keys with the warehouse's own hash function. Rerunning against the same tables samples the same rows, but not the rows a local run of the same fraction would.

From Python, `DiffEngine(config, source, target).propose_value_maps()` returns `ValueMapProposal` objects, and `DiffEngine.propose_value_maps_from_configs(diff, source, target)` loads a YAML pair first. Each proposal's `to_rule()` returns the standalone rule.

### 6. Exclusion Routing
Explicitly drop volatile or irrelevant columns (e.g., auto-generated timestamps) from the comparison matrix.

```yaml
rules:
  - pattern: "^etl_loaded_at_.*"
    ignore: true
```

### 7. Column rename
`rename_to` maps a source column onto a different target name before comparison. Use it when the same field was renamed between systems.

```yaml
rules:
  - column_names: ["legacy_customer_id"]
    rename_to: "customer_id"
```

The rule's other settings apply to the renamed pair on both sides, so a rename can carry a tolerance or a transform:

```yaml
rules:
  - column_names: ["legacy_amt"]
    rename_to: "amount"
    absolute_tolerance: 0.01
```

`primary_keys` are written with the target spelling, so a renamed key works in local runs and warehouse pushdown alike; pushdown reads it from the source under its stored name.

### 8. Fuzzy Text Matching
Forgive typos in free text, such as names keyed by hand into two systems, without writing a `regex_replace` for each one. Scores come from [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz), an optional extra:

```bash
uv add 'veridelta[fuzzy]'
```

A rule sets one of two limits:

```yaml
rules:
  - column_names: ["customer_name"]
    case_insensitive: true
    max_levenshtein_distance: 1
  - column_names: ["city"]
    min_jaro_winkler_similarity: 0.95
```

- `max_levenshtein_distance` counts single-character insertions, deletions, and substitutions. `Jon` and `John` are one edit apart, so they match at `1`; `kitten` and `sitting` need `3`.
- `min_jaro_winkler_similarity` scores two values from 0 to 1 and rewards a shared prefix. `MARTHA` and `MARHTA` score 0.961, so they match at `0.96` but not at `0.97`.

A limit loosens only columns that are text after normalization, just as a tolerance loosens only numbers. A column cast to a number or parsed as a date is still compared exactly, while `pad_zeros` or `cast_to: String` make a column text. Equal values still match outright, and a missing value is never similar to anything, so `treat_null_as_equal` alone decides NULLs. Scores are case-sensitive: `case_insensitive` lowercases both sides first, in stage 3, which puts `ABD` and `abc` one edit apart. Primary keys are never loosened, because rows join on equal keys.

A rule sets at most one of the two limits, and neither has a global default, since loosening every text column would also forgive identifiers and codes that must match exactly. Without the extra, a run that needs a score raises `ConfigError` with the install command before comparing any rows.

Warehouse pushdown compiles `max_levenshtein_distance` to Snowflake's `EDITDISTANCE` or Databricks' `levenshtein`, which count characters as a local run does, so a warehouse run needs no extra. It refuses `min_jaro_winkler_similarity` with `ConfigError` before any comparison query runs: Snowflake's `JAROWINKLER_SIMILARITY` ignores case and returns a whole number from 0 to 100, and Databricks has no Jaro-Winkler function, so neither can reproduce a local verdict.
