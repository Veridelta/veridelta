# Upgrading

Upgrading moves a project to a newer Veridelta release. Most releases ask nothing of you. This page lists each release that does, newest first, and what to change.

Veridelta is below 1.0, so a release that breaks something raises the minor version, such as 0.27.1 to 0.28.0. A patch release, such as 0.31.0 to 0.31.1, needs no change to your configuration or code, though a fix can change a verdict that was wrong. The [changelog](changelog.md) lists every change in every release.

## How to upgrade

Read the section below for each release between yours and the new one. Then move the version in a uv project:

```bash
uv lock --upgrade-package veridelta
uv sync
```

Check each configuration file against the new release, which reports any setting it now refuses:

```bash
veridelta validate -c veridelta.yaml
```

Then run a comparison whose verdict you already know, and confirm the new release agrees. In CI, move the GitHub Action's `@v` ref, and the tag in the GitLab template's `remote` URL, to the new release, as [CI integrations](ci.md) shows.

## 0.43.0

Nine things change:

- `DiffSummary` carries `threshold`, the configuration's threshold the verdict was held to, so `veridelta run --json` prints one more key, and the Markdown summary's JSON comment holds it too. A script that compares the JSON's keys exactly sees one more.
- The HTML report's tables change shape. One columns table replaces the column-level drift and rules applied tables, the metric cards become a bar of the rows with its counts, and the disclosure under the mismatched values lists the changed rows side by side, one cell per compared column with both values, in place of the `<column>_source`, `<column>_target`, and `<column>_is_match` columns. The mismatched values table is unchanged. A script that reads the report's tables reads the mismatched values, or the JSON summary.
- A DuckDB file's connection reads other files only from the file's own folder and the folders under it, and installs or loads no extension by itself, outside the MCP server too. A view or a `query` that reads a file elsewhere, or a URL, fails with a `Permission Error`, and a `query` that changes a setting with `SET` fails. Move the files it reads beside the database file; see [DuckDB files](sources.md#duckdb-files). The file opens as a DuckDB database, whatever it holds, so a SQLite file fails. A `database` that starts with two or more letters, digits, or underscores and a colon, such as `sqlite:legacy.db`, is refused, since DuckDB would open it through an extension. So is a `file:` URI, such as `file:///data/x.duckdb`: write the path itself, as in `/data/x.duckdb`. So is a path that starts with `~` and a name, such as `~analyst/x.duckdb`, which DuckDB opened as your home folder followed by `analyst`: write the path itself. So is a path that starts with `//`, such as `//data/x.duckdb`, outside the MCP server: start it with one `/`. On Linux and macOS, the MCP server resolves such a path and opens the file it resolves to. Read a SQLite file through a [database source](sources.md#connection-string) with a `sqlite://` URI, and write a DuckDB file whose name holds a colon as `./name`.
- An Iceberg source's `table_uri` names the table's metadata file or folder. A catalog identifier, such as `sales.orders`, is refused, and so is a `storage_options` key that starts with `py-` or ends with `impl`, such as `py-io-impl` or `s3.retry-strategy-impl`. Point `table_uri` at the table's current metadata file, which the catalog's `metadata_location` names, and remove any such key.
- `veridelta mcp` connects to no host that a configuration names unless the server is started with `--allow-host` for it. A server that reads a database, a warehouse, an object store, or a URL needs one `--allow-host` for each host, such as `--allow-host db.internal`. A tool also refuses a BigQuery `credentials_path`, a `storage_options` key other than a credential, a region, an endpoint, a bucket, or an account name, a MotherDuck option other than `attach_mode`, `saas_mode`, and `session_name`, a MotherDuck name with a character other than a letter, a digit, `_`, `-`, and `/`, and a host written other than as DNS labels or an IP address in ASCII with at most a port, such as an endpoint with a login or an `abfss://container@account` location. Write such an Azure location as `az://container/t`, with the account in `storage_options`. The refused keys include `aws_allow_http`, so a Delta Lake table at an endpoint that serves plain HTTP, as a local MinIO often does, no longer opens on the server. A tool refuses a database `uri` that ConnectorX reads as BigQuery or Redshift IAM, such as `bigquery:///sa.json` or `redshift-iam://`, and a `uri` parameter that names a TLS file, such as `sslrootcert`, `ssl-ca`, or `trust_server_certificate_ca`. Use a source of `type: bigquery`, or `redshift://` with a password, instead. A tool also refuses an Oracle `uri` with `alias`, or with a path other than a service name with at most a server type and an instance, as in `/sales:dedicated/sales1`, and a Trino `uri` whose decoded user or password holds a character such as `@`, `/`, `#`, or a space. It also refuses a Snowflake `private_key_path` outside the roots, a `file:` location that names a host other than `localhost`, a `file:`, `fs:`, or `opendal+fs:` location whose path starts with `//`, such as `file:////data/t`, and on Windows a path that starts with two separators, such as `\\host\share\x.csv`, which names another machine. Write a local location with three slashes after the scheme's `:`, as in `file:///data/t`, or as a plain path. [AI agents](agents.md#mcp-server) lists the hosts a side names. The same rule holds each file that a Delta Lake log or Iceberg metadata names, so a table whose files lie in another bucket or on another endpoint needs that host given with `--allow-host` too, and a file named as `abfss://container@account.dfs.core.windows.net` needs its container and that host given too. A Delta Lake table is refused when a deletion vector that any version records lies outside the roots, on a host the server does not allow, or at a relative path whose folder holds other characters than letters and digits, and when a checkpoint in its log names a sidecar. A Delta Lake `table_uri` written as a URI, such as a `file:`, `fs:`, or `s3:` one, is refused when its path holds a `.` or `..` segment, since deltalake removes it before it reads the table. An `fs:` or `opendal+fs:` `table_uri` with a `%` escape is refused too, since deltalake decodes its path twice. Write such a table as a path or a `file:` URI.
- `veridelta mcp` reads an Iceberg table through PyIceberg alone, which opens a file only on this machine, or in S3, Google Cloud Storage, Alibaba Cloud OSS, or Azure at an `abfs://`, `abfss://`, `wasb://`, or `wasbs://` location. A table fails on the server if it, or a file it names, lies anywhere else, even with its host allowed. Such locations include `hf://`, `hdfs://`, `viewfs://`, `http://`, `https://`, and `az://`. Write an Iceberg table in Azure as `abfs://container/t`, with the account's name alone in `adls.account-name`, 3 to 24 lowercase letters and digits, and an account key in base64 as `adls.account-key` or a SAS token as `adls.sas-token`. The server refuses a table in Azure with another account name, with neither credential, or with a `#` in its SAS token, and one that signs in with `adls.client-id`, `adls.client-secret`, and `adls.tenant-id`, which `veridelta run` still reads. It reads a file named `abfss://container@account.dfs.core.windows.net/...`, as Spark and Java Iceberg write them, only in the account `adls.account-name` names.
- `veridelta mcp` holds more of the data on this machine to its roots. Before it reads a Delta Lake or Iceberg table, a tool lists each file the scan reads, and refuses the table if one lies outside every root or the files cannot be listed. A tool also refuses a glob `path` that holds `..` or a name such as `.*`, and a `file:` URI with a glob character. Without `--allow-row-values`, a failed call or a `validate_config` finding leaves out a message that Polars, a file reader, or a database driver wrote, and returns the error's type and a hint. A Delta Lake or Iceberg reader's message is the exception, and it can quote a file the reader opened. Run `veridelta run` or `veridelta validate` to see the whole message.
- A run replaces a link already at the name of a file it writes to `output_path`, such as `added_rows.csv`, instead of writing to the file the link points to. A script that keeps such a link reads the new file in `output_path`; see [Artifacts](results.md#artifacts).
- A SQLite `uri` must start with `sqlite://`, on the command line and on the MCP server. ConnectorX also reads a scheme such as `sqlite+pysqlite` as SQLite, and Veridelta now refuses it. The path after `sqlite://` is a file name: `~` is not expanded, and text such as `file:` or `?mode=ro` is part of the name. Write the file's path after `sqlite://`; see [Connection string](sources.md#connection-string).

## 0.42.0

Four things change:

- The `duckdb` extra installs DuckDB below 2.0, since MotherDuck's extension refuses 2.0. A project that pins DuckDB 2.0 itself keeps it, and cannot reach MotherDuck until MotherDuck supports 2.0.

- A DuckDB source's `query` is refused before it runs when its last statement does not read rows, such as `INSERT`, `UPDATE`, `DELETE`, `CREATE`, or `COPY`. A write with `RETURNING` is refused too, where 0.41 read its rows. Read with a `SELECT`, and make any change to the data before the run.
- `veridelta run --json` prints a `provenance` object, the [run record](results.md#run-record), and the Markdown summary's JSON comment holds it too. A script that compares the JSON's keys exactly sees one more.
- The HTML report opens on a verdict banner, and its changed rows open on their mismatched values. The full changed rows, every column side by side, sit in a disclosure under them. A script that reads the report's tables finds the mismatched values first.

## 0.41.0

Veridelta runs on Polars 2.0 as well as on 1.x, and no longer holds Polars below 2.0. Three things change, on every Polars:

- A categorical column refuses `cast_to: Int64` before a row is read, as it refused `Float64`. Before, the cast returned the dictionary codes, not the values. Convert the column where it is read, such as in a database query.
- A text column cast to `Datetime` reads `2026-01-01T10:30:00`, with optional fractional seconds, as before. Text that ends in `Z` or an offset, such as `2026-01-01T10:30:00+02:00`, which the cast read while dropping the offset, now needs `datetime_format`. [Casts](rules.md#casts) lists the forms a cast reads.
- An optional extra that is installed but fails to import counts as not installed, and a warning names the error. Before, `import veridelta` failed for everyone, whether they used the extra or not.

## 0.40.0

Pushdown refuses a primary key whose two sides hold types a local run cannot pair, as a local run has since 0.33.2. A key pairs only when both sides hold one type, two integer types, or two float types. Any other pair raises `ConfigError` before a row is read, naming each such key with both of its types. Before, a database converted one side and paired the rows. The text `"007"` met the integer `7`, and an integer key met a float key, where a local run refused both.

Give each named key a rule with `cast_to`, so both sides hold one type:

```yaml
rules:
  - column_names: ["id"]
    cast_to: "Int64"
```

A local run compares as before.

## 0.33.0

Pushdown refuses a compared column whose two sides hold different types, unless both are numeric. It raises `ConfigError` before reading a row, and names each such column with both of its types. Before, a database converted one side by its own rules, so pushdown could reach another verdict than a local run. The text `"007"` against the integer `7` matched in DuckDB, where a local run reports drift.

Give each named column a rule with `cast_to`, so both sides hold one type:

```yaml
rules:
  - column_names: ["code"]
    cast_to: "Int64"
```

Or set `strict_types: true` to fail every row of such a column instead. A local run compares as before.

## 0.32.0

`veridelta.config` no longer re-exports the seven source models. Importing `BigQueryConfig`, `DatabaseConfig`, `DatabricksConfig`, `DeltaLakeConfig`, `DuckDBConfig`, `IcebergConfig`, or `SnowflakeConfig` from it raises `ImportError`. Import them from the package root, or from `veridelta.models`:

```python
from veridelta import SnowflakeConfig
```

`veridelta.config` still exports `load_config`, `files_config`, `config_json_schema`, `referenced_variables`, `SCHEMA_URL`, and `SourceRef`.

## 0.28.0

The `compile_` methods of `SQLPushdownCompiler` that read both sides no longer take `source_alias` or `target_alias`. They are `compile_query`, `compile_column_predicate`, `compile_changed_sample_query`, `compile_missing_query`, `compile_added_query`, `compile_column_mismatch_query`, and `compile_value_map_query`. A call that passes either raises `TypeError`. Drop the argument: the aliases were always `src` and `tgt`.

Only code that calls the compiler itself is affected. Configuration files and the command line are not.

## 0.15.0

`DataIngestor` no longer exists. `DiffEngine.run_from_configs` loads, aligns, and compares both sides in its place:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
```

`fetch_schema()` no longer exists on a connector. Read a reader's schema with `lazyframe().collect_schema()`. Probe a warehouse table with `compile_schema_probe_query(table)`, run through `execute_pushdown(sql, query_type="schema")`.

A connector of your own now subclasses one of two bases. A connector that the local engine reads subclasses `ReaderConnector`. A connector that runs compiled SQL subclasses `PushdownSession`, and defines `compiler` and `execute_pushdown()`. A reader has no `execute_pushdown`.

## 0.14.0

`run`, `validate`, and `crosswalk` exit with `3`, not `1`, when they cannot finish, such as when a file is missing. With `--json`, such a failure prints `{"error": {"type": ..., "message": ...}}` on stdout, where it printed nothing.

A CI step that read every nonzero exit as drift still fails, which is right. A script that tells drift from a failure reads `1` as drift and `3` as a comparison that did not run. The [command line](cli.md#exit-codes) page lists every exit code.

## 0.12.0

Veridelta requires Python 3.11 or later.

## 0.11.0

Four comparisons became stricter. A run can report a difference that the release before it missed:

- `%f` in `datetime_format` reads `.5` as 500 milliseconds, not 5 nanoseconds.
- A local integer tolerance uses the true difference, so a pair whose difference overflowed into a match now mismatches.
- `strict_types: true` fails a warehouse column whose type differs after normalization.
- The crosswalk thresholds must be an `int` or a `float`, and not a boolean. `min_support` must be an `int`.

## 0.10.0

- A literal `${` inside `source` or `target` is written `$${`. A single `${` names an environment variable.
- Integer and float columns compare by value, so a pair that differs only in the fraction now mismatches in a local run.
- `load_nyc_taxi` raises `DatasetError`, which `except RuntimeError` no longer catches. Catch `DatasetError`, or `VerideltaError` for every error Veridelta raises.
- The tutorial notebooks were renamed, so their old URLs return 404.

## 0.7.0

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`, where they returned a `DiffSummary`. The summary is on its `summary` attribute:

```python
summary = DiffEngine(config, source_frame, target_frame).run().summary
```

`SourceType` and `output_format` accept only the formats Veridelta reads and writes. A removed name, such as `sql` or `xml`, never worked, and now fails when the configuration loads.

## 0.6.0

`cast_to` accepts only `Int64`, `Float64`, `String`, `Boolean`, `Date`, and `Datetime`. Any other name fails when the configuration loads. Before, an unknown name left the column uncast, so a typo could hide a difference.
