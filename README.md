# Veridelta

[![CI Pipeline](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![codecov](https://codecov.io/gh/veridelta/veridelta/graph/badge.svg)](https://codecov.io/gh/veridelta/veridelta)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Veridelta compares two datasets on their primary keys and reports every row that differs under the rules you declare. Nothing is forgiven unless a rule says so, and the exit code tells CI whether the datasets match. Use it to verify a migration or a pipeline change, on a laptop, in CI, or inside a warehouse.

![A terminal prints a five-line veridelta.yaml and two three-row CSV files, validates the configuration, runs the comparison, shows one added, one removed, and one changed row, and prints the exit code for CI, 1, beside what 0, 1, and 3 mean.](https://veridelta.github.io/veridelta/assets/demo.gif)

It runs on [Polars](https://pola.rs/). Read the [documentation](https://veridelta.github.io/veridelta/).

## Features

- **Declared rules.** Tolerances, null sentinels, regular expressions, value maps, date parsing, casts, and fuzzy text matching apply in nine fixed stages. Nothing is forgiven unless a rule says so, and `strict_types` fails a column whose type drifts.
- **Comparison inside the warehouse.** Two tables in Snowflake, Databricks, or BigQuery are compared where they are stored, as are two Postgres or DuckDB tables that set `pushdown`. The rules compile to SQL, and only counts and keys come back. [Pushdown](https://veridelta.github.io/veridelta/pushdown/) lists the exceptions and the services the SQL has run in.
- **Many sources.** CSV, Parquet, JSON, Arrow, Avro, and Excel files, Delta Lake and Iceberg tables, DuckDB files and MotherDuck databases, and Postgres, MySQL, SQL Server, Oracle, SQLite, and other databases. Files and tables are scanned lazily where Polars can.
- **Built for CI.** Exit codes, a JSON summary, a standalone HTML report, a Markdown summary for pull requests, OpenTelemetry metrics, and files of the rows that differ. A GitHub Action and a GitLab CI template post the summary on each pull request.
- **Checks before a run.** `veridelta validate` reports what would stop a run without reading any rows, and a JSON Schema gives editors completion for configuration files.

## Install

```bash
uv add veridelta                # or: pip install veridelta
uv add 'veridelta[snowflake]'   # extras: snowflake, databricks, bigquery, delta, iceberg, database, duckdb, excel, fuzzy, mcp, all
```

## Quick start

The smallest configuration names the two files and the keys that pair their rows. The suffix of each path says what format it is:

```yaml
# veridelta.yaml
primary_keys: [id]
source:
  path: legacy.csv
target:
  path: modern.csv
```

The recording above runs this file on two three-row files. `validate` checks the file without reading any rows, and `run` compares the two files. It exits 0 when they match, 1 when rows differ, and 3 when the run could not finish:

```bash
veridelta validate -c veridelta.yaml
veridelta run -c veridelta.yaml
```

Rules say what counts as a match, column by column. This file forgives one percent on a total and compares phone numbers on their digits alone:

```yaml
primary_keys: ["transaction_id"]
source:
  path: "legacy.parquet"
target:
  path: "modern.parquet"
rules:
  - column_names: ["grand_total"]
    relative_tolerance: 0.01
  - column_names: ["contact_number"]
    regex_replace: {"[^0-9]": ""}
```

In Python, `DiffEngine` compares two `LazyFrame`s with the same models:

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

## Documentation

- [Tutorials](https://veridelta.github.io/veridelta/examples/01_core_concepts/): five notebooks, from a first comparison in Python to a CI pipeline.
- [User guide](https://veridelta.github.io/veridelta/configuration/): configuration, sources, rules, pushdown, results, and the command line.
- [CI integrations](https://veridelta.github.io/veridelta/ci/): the GitHub Action and the GitLab CI template.
- [API reference](https://veridelta.github.io/veridelta/api/): the public Python interface.
- [Roadmap](https://veridelta.github.io/veridelta/roadmap/): work that is not built yet.

## Accessibility

[ACCESSIBILITY.md](https://github.com/Veridelta/veridelta/blob/main/ACCESSIBILITY.md) states what Veridelta aims for, the barriers known today, and how to report one.

## Contributing

See [CONTRIBUTING.md](https://github.com/Veridelta/veridelta/blob/main/CONTRIBUTING.md) for the development setup and the checks a change must pass.

## License

Apache 2.0.
