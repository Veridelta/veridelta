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

`format` accepts `csv`, `parquet`, `json`, `ndjson`, `arrow`, and `excel`. Anything else is rejected when the config loads, rather than partway through a run.

`options` are handed straight to the matching Polars reader, so `{"separator": ";"}` reaches `scan_csv` and `{"sheet_name": "Q3"}` reaches `read_excel`.

Most formats stream. Two do not, because Polars has no lazy reader for them: a `json` document is one array that cannot be parsed incrementally, and a spreadsheet is a random-access container. Both are read whole into memory. Prefer `ndjson` over `json` for anything large.

Excel needs an optional extra:

```bash
uv add 'veridelta[excel]'
```

Discrepancy artifacts write to `csv`, `parquet`, `json`, `ndjson`, or `arrow` via `output_format`. Excel is deliberately absent: writing a workbook needs a second dependency that a discrepancy dump does not justify.

## Warehouse and lakehouse sources

Set `type` on `source` and `target` to select a connector. Warehouse and lakehouse drivers are optional extras:

```bash
uv add 'veridelta[snowflake]'
uv add 'veridelta[databricks]'
uv add 'veridelta[delta]'
uv add 'veridelta[iceberg]'
uv add 'veridelta[all]'
```

Do not commit `password` or `access_token` in YAML. Write `${NAME}` so the loader reads them from the environment (see [Environment variables](#environment-variables)), or build the connection in Python (for example `SnowflakeConfig(..., password=os.environ["SNOWFLAKE_PASSWORD"])`) and pass it to `DiffEngine.run_from_configs`.

Same-warehouse SQL pushdown runs only when both sides are Snowflake or both sides are Databricks, the connection fields match (`account`, `user`, `warehouse`, `database`, `schema_name`, `role`, and `password` for Snowflake; `server_hostname`, `http_path`, `access_token`, `catalog`, and `schema_name` for Databricks), and the `table` names differ; naming the same table twice raises `ConfigError`, since a table compared with itself always matches. Mixed file/lakehouse and warehouse backends, or Snowflake paired with Databricks, raise `ConnectorError`.

`table` must be one to three unquoted identifier segments (`EVENTS`, `schema.table`, or `catalog.schema.table`). Rules that select columns by `pattern` are matched against the probed column names before any SQL is compiled, so they apply in the warehouse exactly as they do locally.

Pushdown issues up to ten statements per run: a zero-row column probe, a duplicate-key check, and a `COUNT(*)` per side, then inner-join mismatches, target-only added rows, source-only removed rows, and a per-column mismatch tally, which is skipped when no column is compared. Those fill every `DiffSummary` field including `column_mismatches`, so `threshold`, `match_rate_percentage`, and the drift report mean the same thing they do for local comparisons. The duplicate-key check costs one grouped scan per relation, over its normalized keys, and raises `DataIntegrityError` before any count or join runs, exactly as a local run refuses keys that repeat.

Every column present on both sides is compared, exactly as it is locally. Columns without an explicit rule inherit the global `default_*` settings, so a `default_absolute_tolerance` applies in the warehouse too. As in a local run, a tolerance only loosens a column that is numeric once normalized, such as a text column with `cast_to: Float64`; text, boolean, and temporal columns are compared exactly. Likewise `max_levenshtein_distance` only loosens a column that is text once normalized, and compiles to Snowflake's `EDITDISTANCE` or Databricks' `levenshtein`, which count characters as a local run does. Columns marked `ignore` are excluded, and `rename_to` pairs a source column with its renamed target counterpart.

The column probes enforce `schema_mode` and primary-key existence before any comparison runs, raising `ConfigError` on drift. Probed names are compared exactly as the compiler quotes them, with no case folding, so YAML identifiers must match the stored column case (Snowflake stores unquoted names uppercase). `normalize_column_names` cannot change that: pushdown raises `ConfigError` if it would rename a stored column.

All nine transform stages compile for compared columns, and stages 1 through 7 for primary keys, so a rule means the same thing in a warehouse as it does locally. The exception is `min_jaro_winkler_similarity`, which pushdown refuses with `ConfigError` before any comparison query runs rather than approximating it; see [Fuzzy Text Matching](#8-fuzzy-text-matching). These behaviors still differ from the file and lakehouse path:

- Artifacts contain primary keys only, since the comparison SQL never projects full rows. They are written as `added_rows_pks_only`, `removed_rows_pks_only`, and `changed_rows_pks_only` so they cannot be confused with local artifacts, which hold complete records.
- `strict_types` applies to local runs only. When the two relations store a column as different types, the warehouse compares them under its own coercion rules.

Parity is verified by a differential test harness that runs both engines over the same frames and compares the results. The harness executes compiled SQL through DuckDB, which catches semantic errors -- null propagation, three-valued logic, operator precedence -- but cannot catch vendor-specific divergence. Snowflake and Databricks spellings are pinned by direct assertions on the emitted SQL instead. DuckDB's `levenshtein` counts bytes rather than characters, so edit-distance parity is checked on ASCII text, where the two agree.

Write `regex_replace` patterns, `value_map` entries, and text `null_values` exactly as you would for a local run. Each is escaped for the target warehouse's string-literal rules, so a backslash in `\d` or `\N` and an apostrophe in `O'Brien` arrive intact; do not double them yourself. Escaping preserves the text, but each warehouse still runs its own regex engine: keep replacements free of capture-group references, which Polars and Databricks write as `$1` and Snowflake as `\1`. Likewise `whitespace_mode` trims only spaces in a warehouse, where Polars also strips tabs and line breaks.

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

The same standalone HTML report the CLI writes with `--html` is available from Python. It embeds its own styles and script, so it opens offline, and `max_rows` caps every table the way `--html-max-rows` does:

```python
from veridelta.report import write_html

write_html(result, "reports/nightly.html", max_rows=1000)
```

Warehouse pushdown compares in place and never projects values, so its frames hold primary keys alone and the result is flagged `keys_only`. `get_mismatches` there returns every changed key rather than one column's values, and still rejects a column that was not part of the comparison.

## Command line

```bash
veridelta --version
veridelta run -c veridelta.yaml
veridelta run -c veridelta.yaml --json
veridelta run -c veridelta.yaml --quiet
veridelta run -c veridelta.yaml --html report.html --html-max-rows 1000
veridelta crosswalk -c veridelta.yaml
veridelta crosswalk -c veridelta.yaml --min-confidence 0.99 --json
```

`--json` prints `DiffSummary` as JSON on stdout. `--quiet` suppresses progress chatter on stderr (the JSON line still prints). Progress chatter always goes to stderr, so `veridelta run --json | jq` does not have to strip anything first. `--html` writes a standalone report with no CDN references, capped at `--html-max-rows` (zero or more; default 1000) so a large diff cannot produce an unopenable file. Pushdown reports are labeled as primary-keys-only.

Exit codes are `0` for a match within `threshold`, `1` for drift or any failure while running, and `2` for invalid command-line arguments.

`crosswalk` proposes `value_map` rules instead of comparing; see [Proposing a value map](#proposing-a-value-map). It exits `0` once the proposals are computed, whether or not it found any, `1` on failure, and `2` for invalid arguments.

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

### Connection fields

Every connector block is selected by `type` and rejects keys it does not list.

| `type` | Required | Optional |
| :--- | :--- | :--- |
| `file` (default) | `path` | `format` (default `csv`), `options` |
| `snowflake` | `table`, `account`, `user`, `warehouse`, `database`, `schema_name` | `password`, `role` |
| `databricks` | `table`, `server_hostname`, `http_path` | `access_token`, `catalog`, `schema_name` |
| `delta` | `table_uri` | `version`, `storage_options` |
| `iceberg` | `table_uri` | `snapshot_id`, `storage_options` |

`version` and `snapshot_id` must be non-negative integers; a quoted number is rejected rather than coerced, because both are interpolated into scan calls. Warehouse and lakehouse blocks are frozen once loaded.

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

Expanded values are text. `version` and `snapshot_id` accept only YAML integers, so write those literally, and file `options` reach the reader as they are, so an expanded option arrives as a string.

### Connector logging

Connectors log under `veridelta.connectors.warehouse` and `veridelta.connectors.lakehouse`, with a `NullHandler` attached so nothing prints unless you opt in. `INFO` records a session or scan opening and closing; `DEBUG` records each pushdown statement by its round-trip kind (`schema`, `duplicates`, `count`, `mismatch`, `added`, `missing`, `columns`) with its duration. Log lines never contain SQL text, `storage_options`, passwords, or tokens. A warehouse session is closed when the run finishes, whether it succeeded or raised.

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("veridelta.connectors").setLevel(logging.DEBUG)
```

## Engine Directives

Global directives control the strictness of the underlying Polars evaluation engine.

| Directive | Description |
| :--- | :--- |
| `schema_mode` | Enforces column structure constraints. Options: `intersection` (default, compares common columns only), `exact`, `allow_additions`, `allow_removals`. |
| `strict_types` | If `false` (default), a column stored as different types on the two sides is still compared. Two numeric types compare by value, so an integer `10` and a float `10.7` differ, and a `Float32` `0.1` differs slightly from a `Float64` `0.1`; add a tolerance to forgive precision gaps. Any other pair soft-casts the target to the source type, so text `"10"` matches an integer `10`. If `true`, type mismatches automatically fail the row. |
| `normalize_column_names`| If `true`, strips whitespace and lowercases all column headers prior to schema alignment, on every entry point including `DiffEngine(...).run()` and `validate_schemas`. Configured `primary_keys`, `column_names`, and `rename_to` are normalized the same way; a `pattern` is not, so write it against the lowercase names. Headers that collide once normalized raise `ConfigError`. |
| `threshold` | The allowable mismatch ratio (0.0 to 1.0) before the pipeline exits with a failure code. |
| `default_absolute_tolerance` | Global absolute numeric tolerance. A column without its own `absolute_tolerance` inherits this. Columns that are not numeric after normalization are compared exactly. |
| `default_relative_tolerance` | Global relative numeric tolerance. A column without its own `relative_tolerance` inherits this. Columns that are not numeric after normalization are compared exactly. |
| `default_treat_null_as_equal` | Global `NULL == NULL` policy. Defaults to `true`. A column rule can override it. |
| `default_whitespace_mode` | Global whitespace stripping: `none` (default), `left`, `right`, or `both`. |
| `default_null_values` | Global sentinel list. Applied only to columns whose type can hold each value. |
| `report_top_columns_limit` | How many drifted columns to list in `report_summary`. `0` hides the section. |
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

A tolerance only loosens the comparison of finite values. `NaN` matches only `NaN`, and an infinity matches only the same infinity, however wide the tolerance.

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

`--sample-fraction` (default 1.0) reads that share of source rows, picked by a hash of the primary keys, so rerunning on the same data under one Polars version samples the same rows. `--json` prints each proposal with its evidence instead of YAML. Warehouse sources raise `ConnectorError`: proposals read rows locally, so export the tables, or a sample of them, to Parquet first.

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
