# Sources

## Extras

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

Do not commit `password` or `access_token` in YAML, including a database source's `password`. Write `${NAME}` so the loader reads them from the environment (see [Environment variables](configuration.md#environment-variables)), or build the connection in Python (for example `SnowflakeConfig(..., password=os.environ["SNOWFLAKE_PASSWORD"])`) and pass it to `DiffEngine.run_from_configs`.

## Connection fields

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

## Files

`format` accepts `csv`, `parquet`, `json`, `ndjson`, `arrow`, `avro`, and `excel`. Anything else is rejected when the config loads, rather than partway through a run.

`options` are handed straight to the matching Polars reader, so `{"separator": ";"}` reaches `scan_csv` and `{"sheet_name": "Q3"}` reaches `read_excel`.

Most formats stream. Three do not, because Polars has no lazy reader for them: a `json` document is one array that cannot be parsed incrementally, a spreadsheet is a random-access container, and Polars reads Avro eagerly. All three are read whole into memory. Prefer `ndjson` over `json` for anything large.

Avro files carry their schema, so columns arrive as the writer typed them. `options` takes `columns` and `n_rows`. The reader takes a local path: an object-store URL such as `s3://` is not supported, so copy the file down first.

Excel needs an optional extra:

```bash
uv add 'veridelta[excel]'
```

## Lakehouse tables

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

## Databases

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

## Warehouses

`table` must be one to three unquoted identifier segments (`EVENTS`, `schema.table`, or `catalog.schema.table`).

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

## Credentials

Printing a connection config, or formatting one into a log line, leaves out its credentials: `password` for Snowflake and databases, `access_token` for Databricks, `credentials_path` for BigQuery, `storage_options` for Delta Lake and Iceberg, and a `storage_options` map nested in a file source's `options`, whose other reader options still print. A password written inside a database `uri` prints as `***`, and the rest of the URI prints as written. They stay readable as attributes and in `model_dump()`, because the connectors and readers need them, so log a dump only after removing them.

## Logging

Connectors log under `veridelta.connectors.warehouse`, `veridelta.connectors.lakehouse`, and `veridelta.connectors.database`, with a `NullHandler` attached so nothing prints unless you opt in. `INFO` records a session or scan opening and closing, and each database read, Postgres pushdown statements included, with its row count and the URI with its password masked; `DEBUG` records each pushdown statement by its round-trip kind (`schema`, `duplicates`, `count`, `mismatch`, `added`, `missing`, `columns`, `samples`) with its duration. Log lines never contain SQL text, row values, `storage_options`, passwords, or tokens. A warehouse session is closed when the run finishes, whether it succeeded or raised.

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("veridelta.connectors").setLevel(logging.DEBUG)
```
