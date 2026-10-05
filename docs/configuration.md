# Configuration

Veridelta is driven by a declarative YAML configuration file or Python object. This specification defines data ingestion parameters, schema alignment constraints, and the semantic rules for engine evaluation.

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

## Primary keys

`primary_keys` must name at least one column, and together the keys must be unique on each side.

File sources may omit `type` (it defaults to `file`) and continue to use `path`, `format`, and optional `options`.

## Settings

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
| `pushdown_sample_rows` | Pushdown only. Fetch up to this many changed rows with both sides' values, so the HTML report and the result show values rather than primary keys alone. `0` (default) fetches none, so no value leaves the warehouse. Local runs ignore it, since they hold every row. See [Row samples](pushdown.md#row-samples). |
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

## Environment variables

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

## Loading a file in Python

From Python, load YAML then route through the same path the CLI uses:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
summary = result.summary
```

## Schema checks

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

## Editor support

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

The site's copy follows the main branch. To pin the schema to the release you run, use the copy in that release's tag, such as `https://raw.githubusercontent.com/Veridelta/veridelta/v0.12.0/docs/schema/veridelta.schema.json`, or print the installed version's schema and point at the file:

```bash
veridelta schema > veridelta.schema.json
```

The schema is a little stricter than the loader. The loader converts `threshold: "0.1"` to a number, while the schema flags the quotes. In `source` and `target`, every text field accepts a `${NAME}` reference (see [Environment variables](#environment-variables)).
