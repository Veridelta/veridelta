# Sources

A source block describes one side of a comparison: a file, a lakehouse table, a database or DuckDB table or query, or a warehouse table. Its `type` selects the connector, and a block without a `type` is a file.

## Connection fields

Each connector accepts the fields below and rejects any other key:

| `type` | Required | Optional |
| :--- | :--- | :--- |
| `file` (default) | `path` | `format` (default `csv`), `options` |
| `snowflake` | `table`, `account`, `user`, `warehouse`, `database`, `schema_name` | `password`, `private_key_path`, `private_key_passphrase`, `role` |
| `databricks` | `table`, `server_hostname`, `http_path` | `access_token`, `catalog`, `schema_name` |
| `bigquery` | `table`, `project` | `dataset`, `location`, `credentials_path`, `maximum_bytes_billed` |
| `delta` | `table_uri` | `version`, `storage_options` |
| `iceberg` | `table_uri` | `snapshot_id`, `storage_options` |
| `database` | `uri`, and exactly one of `table` or `query` | `password`, `pushdown`, `partition_on`, `partitions` |
| `duckdb` | `database`, and exactly one of `table` or `query` | `motherduck_token`, `pushdown` |

`version` and `snapshot_id` must be non-negative integers, and `maximum_bytes_billed` a positive one. A quoted number is rejected, not converted, because each value goes straight to a scan or a job. Warehouse, lakehouse, database, and DuckDB blocks cannot be changed once loaded.

## Extras

The core package reads files. Every other connector, and Excel files, needs an optional extra:

```bash
uv add 'veridelta[snowflake]'
uv add 'veridelta[databricks]'
uv add 'veridelta[bigquery]'
uv add 'veridelta[delta]'
uv add 'veridelta[iceberg]'
uv add 'veridelta[database]'
uv add 'veridelta[duckdb]'
uv add 'veridelta[excel]'
uv add 'veridelta[all]'
```

## Files

A file source reads `path` in one of these formats: `csv`, `parquet`, `json`, `ndjson`, `arrow`, `avro`, or `excel`. Any other `format` is rejected when the configuration loads.

`options` go to the matching Polars reader. `{"separator": ";"}` reaches `scan_csv`, and `{"sheet_name": "Q3"}` reaches `read_excel`.

Most formats are read lazily, in streaming batches. Three are read whole into memory, because Polars has no lazy reader for them:

- `json`: a JSON document is one array, which cannot be parsed in parts. Prefer `ndjson` for anything large.
- `excel`: a spreadsheet is a random-access container. Reading one needs the `excel` extra.
- `avro`: Polars reads Avro eagerly.

Avro columns keep the types in the file's schema. Its `options` take `columns` and `n_rows`. The Avro reader takes a local path only; copy a file from object storage, such as `s3://`, before reading it.

## Lakehouse tables

A Delta Lake or Iceberg table is scanned lazily, as an unevaluated Polars `LazyFrame`. Install the `delta` or `iceberg` extra. This pair compares version 12 of a Delta table with one snapshot of an Iceberg table:

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

`storage_options` is a map of strings passed to the scanner, such as credentials, the region, and other object store settings.

The scan reads the table's transaction log or metadata when it opens. So a missing table, version, or snapshot fails before the comparison starts, with the table's URI in the error. Checking an Iceberg `snapshot_id` reads one row.

## Databases

A `database` source reads a table, or the result of a query, from an operational database through [ConnectorX](https://github.com/sfu-db/connector-x). Install the `database` extra. The rows are compared locally. A database pairs with a file, a lakehouse table, or another database, and `crosswalk` reads it too. This pair compares a Postgres table with the result of a MySQL query:

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

Two Postgres tables on one server can instead be compared inside Postgres; see [Postgres](pushdown.md#postgres).

### Connection string

`uri` is a ConnectorX connection string that starts with `postgresql://` or `postgres://`, `mysql://` (MariaDB too), `mssql://`, `oracle://`, `redshift://`, `clickhouse://`, or `sqlite://`.

A SQLite URI is followed by a file path, as in `sqlite:///srv/data/legacy.db`, or `sqlite://C:/data/legacy.db` on Windows. The path must name an existing file. Veridelta refuses a missing one, which ConnectorX would otherwise create as an empty database.

A SQL Server URI takes connection options as parameters. `?encrypt=true` requires TLS for the whole connection. A server with a self-signed certificate, such as a development container, also needs `trust_server_certificate=true`, which accepts the certificate without checking it.

### Table or query

Set exactly one of `table` and `query`:

- `table` is one to three identifier segments, such as `public.orders`. Each segment is quoted for its database: double quotes for Postgres, Redshift, Oracle, and SQLite, backticks for MySQL and ClickHouse, and brackets for SQL Server. Quoting keeps case: write names as they are stored. Any other scheme needs `query`.
- `query` is sent to the database as written. Veridelta cannot tell a read from a write: connect with a role that can only read. A `query` is expanded like any other `source` string. Write a literal `${` in it as `$${`.

### Password

`password` is percent-encoded into the URI. It can contain `@`, `:`, `/`, or any other character, and needs a user name in `uri`.

A password written into `uri` itself must already be percent-encoded, which an expanded `${VAR}` is not. Setting both fails when the file loads. Credentials passed as URI parameters, such as `?password=`, are not masked in logs or errors. Use `password` instead.

### Reading and types

The rows are read into memory once, before the comparison starts, because Polars has no lazy database reader. Select columns and filter rows in `query` instead of reading a whole table.

Column types come from the database driver. For SQLite, that means the declared types:

| Declared type | Polars type |
| :--- | :--- |
| `INTEGER` | `Int64` |
| `REAL` | `Float64` |
| `TEXT` | `String` |
| `DATE` | `Date` |
| `DATETIME` | `Datetime` |
| `BOOLEAN` | `Boolean` |
| `NUMERIC` | `Float64` |

A SQLite column declared without a type cannot be typed when its first rows are NULL, and the read fails.

A Postgres `table` keeps the declared precision and scale of each `numeric` column, so `numeric(10, 2)` arrives as `Decimal(10, 2)` with every stored digit. Veridelta reads the declarations from the `pg_attribute` catalog before the rows, so a Postgres-compatible server without that catalog needs a `query`. A `NaN` has no decimal form and fails the read; leave it out with a `query`.

Any other Postgres `numeric` arrives as `Decimal(38, 10)`. Its values are rounded to ten decimal places, and a value with more than 18 digits before the decimal point fails the read. That applies to every column of a `query`, to a `numeric` declared without a precision, and to one with a precision above 38 or a negative scale.

A MySQL or SQL Server `DECIMAL` arrives as `Decimal(38, 10)` too, with the same limits, whether a `table` or a `query` reads it.

MySQL has no boolean type, so a flag arrives as a number. These MySQL types arrive as follows:

| MySQL type | Polars type |
| :--- | :--- |
| `TINYINT(1)`, which `BOOLEAN` stands for | `Int8` |
| `BIT` | `Binary` |
| `INT UNSIGNED` | `UInt32` |
| `DATETIME`, `TIMESTAMP` | `Datetime`, with no time zone |
| `JSON` | `String` |

A `cast_to` rule cannot turn `Binary` into a number or a boolean. To compare a `BIT` column as a number, read it with `CAST(column AS UNSIGNED)` in a `query`.

SQL Server has a boolean type, `BIT`, and these of its types arrive as follows:

| SQL Server type | Polars type |
| :--- | :--- |
| `INT`, `TINYINT` | `Int64` |
| `BIT` | `Boolean` |
| `DATETIME2` | `Datetime`, with no time zone |
| `DATETIMEOFFSET` | `Datetime` in UTC |

### Parallel reads

A large `table` reads faster in ranges, each over its own connection. Name an integer column to split on, and how many ranges to read:

```yaml
source:
  type: database
  uri: postgresql://analyst@legacy-db.internal:5432/sales
  password: ${LEGACY_DB_PASSWORD}
  table: public.orders
  partition_on: order_id
  partitions: 4
```

Veridelta reads the column's lowest and highest values, and ConnectorX splits that span into `partitions` ranges and reads them in parallel. Each range opens its own connection, so the database must accept that many more.

The column must hold integers and no NULL. A NULL falls in no range, so ConnectorX would leave its row out. Veridelta counts the column's NULLs first and fails the read if it finds any. An empty table is read in one piece.

ConnectorX writes the column name into each range's statement without quotes, so the database folds its case as it does for any unquoted name. On Postgres, partition on a column whose name is all lowercase.

Only a `table` read splits. A `query` runs as written, and a `pushdown` table reads no rows. A schema check with `validate --schemas` reads the columns in one statement.

## DuckDB and MotherDuck

A `duckdb` source reads a table, or the result of a query, from a DuckDB file or a MotherDuck database. Install the `duckdb` extra. The rows are compared locally, so a DuckDB source pairs with any source but a warehouse. Two tables in one database can instead be compared inside DuckDB; see [DuckDB and MotherDuck](pushdown.md#duckdb-and-motherduck). This pair compares a DuckDB file with a Parquet export:

```yaml
source:
  type: duckdb
  database: warehouse.duckdb
  table: main.orders

target:
  path: exports/orders.parquet
  format: parquet

primary_keys: ["order_id"]
```

`database` is a file path, which resolves from the working directory, or a MotherDuck database written as `md:name` or `motherduck:name`. Set exactly one of `table` and `query`:

- `table` is one to three identifier segments, such as `main.orders` or `warehouse.main.orders`. Each segment is double-quoted, which keeps its case.
- `query` is sent to DuckDB as written, so it can also read files, as in `SELECT * FROM read_parquet('orders/*.parquet')`. A statement that returns no rows, such as `SET`, fails the read.

### DuckDB files

A file opens read-only, so a `query` that writes fails and the file never changes. A missing file fails the read rather than leaving an empty database behind.

DuckDB lets one process write to a file, or several read it, but not both at once. Close a notebook's read-write connection before a run reads the same file.

### MotherDuck

A MotherDuck database needs a token. Set `motherduck_token`, or the `MOTHERDUCK_TOKEN` or `motherduck_token` environment variable. Without one, MotherDuck opens a browser sign-in, which a CI job cannot finish, so Veridelta refuses to connect. Write `motherduck_token: ${MOTHERDUCK_TOKEN}` rather than the token itself, and never put the token in `database`, which is printed and logged.

The connection opens read-write, because a read-only MotherDuck connection needs a read-scaling token. A `query` therefore runs with all of the token's permissions. Use a read-scaling token for a source, as you would give a database `query` a role that can only read.

Connecting downloads MotherDuck's DuckDB extension, so the machine needs network access to DuckDB's extension repository. The MotherDuck connection is tested with a stand-in for the driver, not against a live account.

### DuckDB types

Column types come from DuckDB: `INTEGER` arrives as `Int32`, `DECIMAL(10, 2)` as `Decimal(10, 2)`, and `TIMESTAMPTZ` as a `Datetime` in UTC. The session reads time in UTC, so a timestamp with a time zone, or a date cast in a `query`, does not depend on the machine.

Polars has no type for `INTERVAL` or `UNION`, alone or inside a `STRUCT`, list, or map. A column holding one fails the read with its name. Cast it in a `query`, as in `CAST(span AS VARCHAR)`, or in a view when the source sets `pushdown`.

## Warehouses

A Snowflake, Databricks, or BigQuery source names a `table` in that warehouse. Two tables on one connection are compared inside the warehouse, and a warehouse table pairs with nothing else; see [Pushdown](pushdown.md).

For Snowflake and Databricks, `table` is one to three unquoted identifier segments: `EVENTS`, `schema.table`, or `catalog.schema.table`. This pair compares two Snowflake tables:

```yaml
source:
  type: snowflake
  table: ANALYTICS.PUBLIC.LEGACY_EVENTS
  account: xy12345
  user: SVC_VERIDELTA
  private_key_path: ${SNOWFLAKE_KEY_FILE}
  private_key_passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

target:
  type: snowflake
  table: ANALYTICS.PUBLIC.MODERN_EVENTS
  account: xy12345
  user: SVC_VERIDELTA
  private_key_path: ${SNOWFLAKE_KEY_FILE}
  private_key_passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

primary_keys: ["event_id"]
```

Snowflake requires strong sign-in for scripted users, so a password alone may be refused. A service user signs in with a key pair instead:

- `private_key_path` names the PEM file of the user's private key.
- `private_key_passphrase` decrypts that file, if it is encrypted.
- A programmatic access token also works in `password`, but by default it needs a network policy that allows the client's address.

Set either `password` or `private_key_path`, not both. In GitHub Actions, write the key from a secret to a file before the step that runs Veridelta:

```yaml
- name: Write the Snowflake key
  run: printf '%s\n' "$SNOWFLAKE_PRIVATE_KEY" > "$RUNNER_TEMP/snowflake_key.p8"
  env:
    SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
```

Then give the step that runs Veridelta `SNOWFLAKE_KEY_FILE: ${{ runner.temp }}/snowflake_key.p8` in its `env`.

This pair compares two Databricks tables:

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

A `bigquery` source names a `project` and a `table`. The project runs the queries and holds the data. It never appears in SQL, so a project id with hyphens works. The table is `dataset.table`, or `table` alone when `dataset` names the default dataset:

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

Credentials come from Application Default Credentials, such as `gcloud auth application-default login` on a workstation or the attached service account on Google Cloud. Set `credentials_path` to a service account key file to use that instead.

`maximum_bytes_billed` makes BigQuery refuse any statement that would bill more bytes, which caps what a run can cost.

Project ids follow Google's rules: six to thirty lowercase letters, digits, or hyphens. Older domain-scoped ids, such as `example.com:project`, are refused.

## Credentials

Do not commit a `password`, a `private_key_passphrase`, an `access_token`, or a `motherduck_token` in YAML, a database `password` included. Write `${NAME}` so the loader reads the value from the environment; see [Environment variables](configuration.md#environment-variables). Or build the connection in Python, as in `SnowflakeConfig(..., password=os.environ["SNOWFLAKE_PASSWORD"])`, and pass it to `DiffEngine.run_from_configs`.

Printing a connection config, or formatting one into a log line, leaves out its credentials:

- `password`, for Snowflake and databases;
- `private_key_path` and `private_key_passphrase`, for Snowflake;
- `access_token`, for Databricks;
- `credentials_path`, for BigQuery;
- `motherduck_token`, for MotherDuck;
- `storage_options`, for Delta Lake and Iceberg;
- a `storage_options` map inside a file source's `options`. The other reader options still print.

A password written inside a database `uri` prints as `***`, and the rest of the URI prints as written. The credentials stay readable as attributes and in `model_dump()`, because the connectors and readers need them. Remove them before logging a dump.

## Logging

Connectors log under `veridelta.connectors.warehouse`, `veridelta.connectors.lakehouse`, `veridelta.connectors.database`, and `veridelta.connectors.duckdb`. Each logger has a `NullHandler` and prints nothing until you configure logging:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("veridelta.connectors").setLevel(logging.DEBUG)
```

- `INFO` records a session or scan opening and closing. It also records each database read, Postgres pushdown statements included, with its row count and the URI with its password masked. Each DuckDB read, pushdown statements included, is recorded with its row count and `database`.
- `DEBUG` records each pushdown statement by its kind, with its duration. The kinds are `schema`, `duplicates`, `count`, `mismatch`, `added`, `missing`, `columns`, and `samples`.

Log lines never contain SQL text, row values, `storage_options`, passwords, or tokens. A warehouse session closes when the run finishes, whether the run succeeded or raised.
