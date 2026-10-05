# Veridelta

[![CI Pipeline](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![codecov](https://codecov.io/gh/veridelta/veridelta/graph/badge.svg)](https://codecov.io/gh/veridelta/veridelta)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Veridelta compares two datasets on their primary keys and reports every row that differs once the rules you declare are applied. Use it to verify a system migration, a model retrain, or a pipeline change.

It runs on [Polars](https://pola.rs/). Read the [documentation](https://veridelta.github.io/veridelta/).

## Features

- **Declared rules.** Tolerances, null sentinels, regular expressions, value maps, date parsing, casts, and fuzzy text matching apply in nine fixed stages. Nothing is forgiven unless a rule says so, and `strict_types` fails a column whose type drifts.
- **The same verdict in the warehouse.** Two tables in Snowflake, Databricks, BigQuery, Postgres, or DuckDB are compared where they are stored. The rules compile to SQL, and only counts and keys come back. [Pushdown](https://veridelta.github.io/veridelta/pushdown/) lists the exceptions.
- **Many sources.** CSV, Parquet, JSON, Arrow, Avro, and Excel files, Delta Lake and Iceberg tables, DuckDB files and MotherDuck databases, and Postgres, MySQL, SQL Server, Oracle, SQLite, and other databases. Files and tables are scanned lazily where Polars can.
- **Built for CI.** Exit codes, a JSON summary, a standalone HTML report, a Markdown summary for pull requests, OpenTelemetry metrics, and files of the rows that differ. A GitHub Action and a GitLab CI template post the summary on each pull request.
- **Checks before a run.** `veridelta validate` reports what would stop a run without reading any rows, and a JSON Schema gives editors completion for configuration files.

## Install

```bash
uv add veridelta                # or: pip install veridelta
uv add 'veridelta[snowflake]'   # extras: snowflake, databricks, bigquery, delta, iceberg, database, duckdb, excel, fuzzy, all
```

## Quick start

In Python, `DiffEngine` compares two `LazyFrame`s:

```python
import polars as pl
from veridelta import DiffConfig, DiffEngine, DiffRule

result = DiffEngine(
    DiffConfig(
        primary_keys=["user_id"],
        rules=[DiffRule(pattern="^AMT_.*", absolute_tolerance=0.05)],
    ),
    pl.scan_parquet("legacy.parquet"),
    pl.scan_parquet("modern.parquet"),
).run()

if not result.summary.is_match:
    raise SystemExit(f"{result.summary.changed_count} rows differ")
```

The same comparison as a YAML file, for the CLI and CI:

```yaml
# veridelta.yaml
primary_keys: ["transaction_id"]
source:
  path: "legacy.parquet"
  format: "parquet"
target:
  path: "modern.parquet"
  format: "parquet"
rules:
  - column_names: ["grand_total"]
    relative_tolerance: 0.01
  - column_names: ["contact_number"]
    regex_replace: {"[^0-9]": ""}
```

`validate` checks the file without reading any rows, and `run` compares the datasets:

```bash
veridelta validate -c veridelta.yaml
veridelta run -c veridelta.yaml
```

## Documentation

- [Tutorials](https://veridelta.github.io/veridelta/examples/01_core_concepts/): five notebooks, from a first comparison in Python to a CI pipeline.
- [User guide](https://veridelta.github.io/veridelta/configuration/): configuration, sources, rules, pushdown, results, and the command line.
- [CI integrations](https://veridelta.github.io/veridelta/ci/): the GitHub Action and the GitLab CI template.
- [API reference](https://veridelta.github.io/veridelta/api/): the public Python interface.
- [Roadmap](https://veridelta.github.io/veridelta/roadmap/): work that is not built yet.

## Contributing

See [CONTRIBUTING.md](https://github.com/Veridelta/veridelta/blob/main/CONTRIBUTING.md) for the development setup and the checks a change must pass.

## License

Apache 2.0.
