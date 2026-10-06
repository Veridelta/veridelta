# Veridelta

Veridelta compares two datasets on their primary keys and reports every row that differs once the rules you declare are applied. Use it to verify a system migration, a model retrain, or a pipeline change.

<video controls muted playsinline preload="metadata" width="900" height="800">
  <source src="assets/demo.mp4" type="video/mp4">
  <track kind="captions" src="assets/demo.vtt" srclang="en" label="English" default>
  This browser does not play the video. The transcript below shows the same run.
</video>

The recording runs the quick start on two small files, as the transcript shows:

```text
> cat veridelta.yaml
primary_keys: [id]
source:
  path: legacy.csv
target:
  path: modern.csv
> veridelta validate -c veridelta.yaml
veridelta.yaml: valid.
> veridelta run -c veridelta.yaml
Loading configuration from veridelta.yaml...
Executing semantic diff...

Veridelta Execution Summary
===========================
Status:        FAILED
Match Rate:    0.0%
Source Rows:   3
Target Rows:   3
Volume Shift:  +0 rows

Row-Level Discrepancies:
---------------------------
Added:         1
Removed:       1
Changed:       1
Total Issues:  3

Top Column-Level Drifts:
---------------------------
- status: 1 mismatches

> echo $?
1
```

Files, lakehouse tables, databases, and DuckDB files are read and compared on [Polars](https://pola.rs/). Two tables in one warehouse are compared inside it, as are two Postgres or DuckDB tables that set `pushdown`. Only counts and keys come back.

## Install

```bash
uv add veridelta                # or: pip install veridelta
uv add 'veridelta[snowflake]'   # extras: snowflake, databricks, bigquery, delta, iceberg, database, duckdb, excel, fuzzy, all
```

## Where to start

The tutorials build a comparison step by step:

1. [Core concepts](examples/01_core_concepts.ipynb): the Python API, `DiffResult`, and rules.
2. [YAML and CLI](examples/02_yaml_and_cli.ipynb): a configuration file, `--json`, and artifacts.
3. [Advanced rules](examples/03_advanced_rules.ipynb): resolving drift in real data.
4. [HTML reports](examples/04_html_reports.ipynb): a report to hand to reviewers.
5. [Validate and CI](examples/05_validate_and_ci.ipynb): a database source, `veridelta validate`, and the GitHub Action.

The user guide is the reference:

- [Configuration](configuration.md): the file, its settings, and environment variables.
- [Sources](sources.md): files, lakehouse tables, databases, DuckDB, and warehouses.
- [Rules](rules.md): what counts as a match, column by column.
- [Pushdown](pushdown.md): comparing two tables inside the warehouse that stores them.
- [Results](results.md): the summary, reports, metrics, and files a run produces.
- [Command line](cli.md): commands, flags, and exit codes.
- [CI integrations](ci.md): the GitHub Action and the GitLab CI template.

## How it works

```mermaid
flowchart LR
  subgraph sources [Sources]
    files[Files]
    lakehouse[Delta Iceberg]
    databases[Postgres MySQL SQLite]
    duckdb[DuckDB MotherDuck]
    warehouse[Snowflake Databricks BigQuery]
  end
  files --> loader[LoaderFactory]
  lakehouse --> loader
  databases --> loader
  duckdb --> loader
  warehouse --> compiler[SQLPushdownCompiler]
  databases -. pushdown .-> compiler
  duckdb -. pushdown .-> compiler
  loader --> engine["DiffEngine"]
  engine --> result[DiffResult]
  compiler --> warehouseSql[Warehouse SQL]
  warehouseSql --> result
  result --> artifacts[Artifacts]
  result --> reports["HTML JSON"]
  result --> exitCode[Exit code]
```

File, lakehouse, database, and DuckDB sources load through `LoaderFactory` into a local `DiffEngine` run. A pair of tables in one warehouse, or two Postgres or DuckDB tables that set `pushdown`, compiles to SQL and runs in place. Both paths return a `DiffResult`.
