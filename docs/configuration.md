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

File sources may omit `type` (it defaults to `file`) and continue to use `path`, `format`, and optional `options`.

## Warehouse and lakehouse sources

Set `type` on `source` and `target` to select a connector. Warehouse and lakehouse drivers are optional extras:

```bash
uv add 'veridelta[snowflake]'
uv add 'veridelta[databricks]'
uv add 'veridelta[delta]'
uv add 'veridelta[iceberg]'
uv add 'veridelta[all]'
```

Do not commit `password` or `access_token` in YAML. Inject secrets from the environment or your orchestrator's secret store at runtime.

Same-warehouse SQL pushdown runs only when both sides are Snowflake or both sides are Databricks, the connection fields match (account, user, warehouse, database, schema, role, and password for Snowflake; host, HTTP path, token, catalog, and schema for Databricks), and the `table` names differ. Mixed file/lakehouse and warehouse backends, or Snowflake paired with Databricks, raise `ConnectorError`.

`table` must be one to three unquoted identifier segments (`EVENTS`, `schema.table`, or `catalog.schema.table`). Pattern-only `DiffRule` entries are not compiled to SQL; they raise `ConnectorError` on the warehouse path.

Pushdown issues eight statements per run: a zero-row column probe and a `COUNT(*)` per side, then inner-join mismatches, target-only added rows, source-only removed rows, and a per-column mismatch tally. Those fill every `DiffSummary` field including `column_mismatches`, so `threshold`, `match_rate_percentage`, and the drift report mean the same thing they do for local comparisons.

Every column present on both sides is compared, exactly as it is locally. Columns without an explicit rule inherit the global `default_*` settings, so a `default_absolute_tolerance` applies in the warehouse too. Columns marked `ignore` are excluded, and `rename_to` pairs a source column with its renamed target counterpart.

The column probes enforce `schema_mode` and primary-key existence before any comparison runs, raising `ConfigError` on drift. Probed names are compared exactly as the compiler quotes them, with no case folding, so YAML identifiers must match the stored column case (Snowflake stores unquoted names uppercase).

All nine transform stages compile, so a rule means the same thing in a warehouse as it does locally. Two behaviors still differ from the file and lakehouse path:

- Artifacts contain primary keys only, since the comparison SQL never projects full rows. They are written as `added_rows_pks_only`, `removed_rows_pks_only`, and `changed_rows_pks_only` so they cannot be confused with local artifacts, which hold complete records.
- Primary-key uniqueness is not verified, so duplicate keys inflate the inner-join mismatch count instead of raising `DataIntegrityError`.

Parity is verified by a differential test harness that runs both engines over the same frames and compares the results. The harness executes compiled SQL through DuckDB, which catches semantic errors -- null propagation, three-valued logic, operator precedence -- but cannot catch vendor-specific divergence. Snowflake and Databricks spellings are pinned by direct assertions on the emitted SQL instead.

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

Warehouse pushdown compares in place and never projects values, so its frames hold primary keys alone and the result is flagged `keys_only`. `get_mismatches` there returns every changed key rather than one column's values, and still rejects a column that was not part of the comparison.

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

target:
  type: iceberg
  table_uri: s3://lake/iceberg/modern_events
  snapshot_id: 883142

primary_keys: ["event_id"]
```

## Engine Directives

Global directives control the strictness of the underlying Polars evaluation engine.

| Directive | Description |
| :--- | :--- |
| `schema_mode` | Enforces column structure constraints. Options: `intersection` (default, compares common columns only), `exact`, `allow_additions`, `allow_removals`. |
| `strict_types` | If `false` (default), the engine dynamically soft-casts target columns to source types to prevent execution halts on mismatched types. If `true`, type mismatches automatically fail the row. |
| `normalize_column_names`| If `true`, strips whitespace and lowercases all column headers prior to schema alignment. |
| `threshold` | The allowable mismatch ratio (0.0 to 1.0) before the pipeline exits with a failure code. |

## Column-Level Overrides (Rules)

The `rules` array defines granular, per-column or regex-pattern tolerances.

### Transform order

Rules are not applied in the order you write them. Every column follows one fixed pipeline, and both the local engine and the warehouse compiler honor it, so a rule produces the same verdict wherever it runs:

1. Null sentinels (`null_values`)
2. Regex replace (`regex_replace`)
3. Whitespace, then case (`whitespace_mode`, `case_insensitive`)
4. Source-side value map (`value_map`)
5. Pad zeros (`pad_zeros`)
6. Datetime parsing, then timezone (`datetime_format`, `timezone`)
7. Explicit cast (`cast_to`)
8. Comparison (equality, or numeric tolerance)
9. Null-safe equality (`treat_null_as_equal`)

Stages 1 through 7 normalize each dataset on its own, before any join. That means they apply to primary keys as well: a `case_insensitive` rule on a key column changes how rows are matched, not just how they are compared. If normalizing a key collapses two rows into one, `DataIntegrityError` is raised rather than allowing a join explosion.

Stages 2, 3, and 4 operate on text and are skipped for non-string columns, so a global `default_whitespace_mode` is safe to set on a mixed schema. Stage 1 is filtered per column instead, as described below.

### 1. Numeric Tolerances
Bypass floating-point anomalies or acceptable system rounding differences.

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

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

### 6. Exclusion Routing
Explicitly drop volatile or irrelevant columns (e.g., auto-generated timestamps) from the comparison matrix.

```yaml
rules:
  - pattern: "^etl_loaded_at_.*"
    ignore: true
```