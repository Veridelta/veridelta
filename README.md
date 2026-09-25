# Veridelta

[![CI Pipeline](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![codecov](https://codecov.io/gh/veridelta/veridelta/graph/badge.svg)](https://codecov.io/gh/veridelta/veridelta)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Veridelta compares two datasets on their keys and reports exactly what changed after applying the variance you declared as expected. It is built for system modernizations, model retrains, and pipeline migrations — anywhere "equal" has to be defined, not assumed.

Powered by [Polars](https://pola.rs/). **[Documentation](https://veridelta.github.io/veridelta)**

## Why

- **Deterministic verdicts.** Nine fixed transform stages. The same rule produces the same result locally and in a warehouse, verified by a differential harness.
- **Scale.** Lazy Polars scans. Warehouse pushdown compiles comparison SQL and never extracts full tables.
- **Exactness.** Nothing is forgiven unless a rule says so. `strict_types` treats type drift as a mismatch, not a cast.
- **CI/CD.** Exit codes 0 (match), 1 (drift or a failure), and 2 (invalid arguments). `--json` on stdout. `--html` writes a standalone report. Artifacts for added, removed, and changed rows.
- **Schema evolution.** `schema_mode` is `intersection`, `exact`, `allow_additions`, or `allow_removals`.
- **Connectors.** Snowflake and Databricks SQL pushdown; Delta Lake and Iceberg scans. Optional extras.

## Install

```bash
uv add veridelta
# or: pip install veridelta
uv add 'veridelta[snowflake]'   # extras: snowflake, databricks, delta, iceberg, excel, all
```

Routing, YAML fields, and time travel: [configuration guide](https://veridelta.github.io/veridelta/configuration/).

## Architecture

```mermaid
flowchart LR
  subgraph sources [Sources]
    files[Files]
    lakehouse[Delta Iceberg]
    warehouse[Snowflake Databricks]
  end
  files --> loader[LoaderFactory]
  lakehouse --> loader
  warehouse --> compiler[SQLPushdownCompiler]
  loader --> engine["DiffEngine"]
  engine --> result[DiffResult]
  compiler --> warehouseSql[Warehouse SQL]
  warehouseSql --> result
  result --> artifacts[Artifacts]
  result --> reports["HTML JSON"]
  result --> exitCode[Exit code]
```

File and lakehouse sources load through `LoaderFactory` into a local `DiffEngine` run. Same-warehouse pairs compile to SQL and execute in place. Both paths return a `DiffResult`.

## Quick start

Python — `DiffEngine` consumes `LazyFrame`s:

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

YAML — the same comparison for CI:

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

```bash
veridelta run -c veridelta.yaml
```

## Where next

- [1. Core Concepts](https://veridelta.github.io/veridelta/examples/01_core_concepts/) — Python API, `DiffResult`, rules.
- [2. YAML and CLI](https://veridelta.github.io/veridelta/examples/02_yaml_and_cli/) — pipeline automation, `--json`, artifacts.
- [3. Advanced Rules](https://veridelta.github.io/veridelta/examples/03_advanced_rules/) — drift resolution on real data.
- [4. HTML Reports](https://veridelta.github.io/veridelta/examples/04_html_reports/) — audit and compliance hand-off.
- [Configuration](https://veridelta.github.io/veridelta/configuration/) — fields, formats, extras, warehouse and lakehouse routing.
- [API Reference](https://veridelta.github.io/veridelta/api/) — public Python surface.
- [Roadmap](https://veridelta.github.io/veridelta/roadmap/) — work that is not built yet.

## License

Apache 2.0.
