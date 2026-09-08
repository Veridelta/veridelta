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

Pushdown issues seven statements per run: a zero-row column probe and a `COUNT(*)` per side, then inner-join mismatches, target-only added rows, and source-only removed rows. Those fill `changed_count`, `added_count`, `removed_count`, `total_rows_source`, and `total_rows_target`, so `threshold` and `match_rate_percentage` mean the same thing they do for local comparisons. `column_mismatches` stays empty because the comparison SQL projects keys only.

The column probes enforce `schema_mode` and primary-key existence before any comparison runs, raising `ConfigError` on drift. Probed names are compared exactly as the compiler quotes them, with no case folding, so YAML identifiers must match the stored column case (Snowflake stores unquoted names uppercase).

Two behaviors differ from the file and lakehouse path. `output_path` produces no artifacts, because pushdown never extracts rows; the CLI reports counts only and `DiffSummary.artifacts_written` stays `False`. Primary-key uniqueness is also not verified, so duplicate keys inflate the inner-join mismatch count instead of raising `DataIntegrityError`.

From Python, load YAML then route through the same path the CLI uses:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
summary = DiffEngine.run_from_configs(diff, source, target)
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

### 1. Numeric Tolerances
Bypass floating-point anomalies or acceptable system rounding differences.

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

### 2. String Normalization & Sanitization
Execute string mutations prior to type evaluation. `regex_replace` is processed first, ensuring text is sanitized before any subsequent `cast_to` operations.

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

### 3. Value Mapping (Crosswalks)
Translate legacy enumerations or system-specific codes to modern equivalents during evaluation.

```yaml
rules:
  - column_names: ["status_code"]
    value_map:
      "0": "INACTIVE"
      "1": "ACTIVE"
      "2": "PENDING"
```

### 4. Exclusion Routing
Explicitly drop volatile or irrelevant columns (e.g., auto-generated timestamps) from the comparison matrix.

```yaml
rules:
  - pattern: "^etl_loaded_at_.*"
    ignore: true
```