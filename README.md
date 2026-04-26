# Veridelta

[![CI](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Semantic diffing for data pipelines.** Define expected variance, validate intentional changes, and detect regressions with confidence.

Veridelta is a Rust-backed (via Polars) data comparison engine designed for large-scale datasets. It enables data engineers to define precise mathematical and semantic rules for comparing datasets, ensuring that expected differences (such as floating-point variance or controlled schema evolution) are ignored while unintended regressions are caught immediately.

**[Read the Documentation](https://veridelta.github.io/veridelta)**

---

## Quick Start

### Installation

Install using `uv` (recommended) or `pip`:

```bash
uv add veridelta
````

---

## Usage

Veridelta supports both configuration-driven workflows and direct Python integration.

### Option A: Configuration (YAML / CLI)

Define comparison rules in a YAML configuration file:

```yaml
# veridelta.yaml
primary_keys: ["transaction_id"]
schema_mode: "intersection"

source:
  path: "legacy_system.parquet"
  format: "parquet"

target:
  path: "modern_system.parquet"
  format: "parquet"

rules:
  - column_names: ["grand_total"]
    relative_tolerance: 0.01

  - column_names: ["contact_number"]
    regex_replace: {"[^0-9]": ""}
```

Run the comparison:

```bash
veridelta run --config veridelta.yaml
```

---

### Option B: Python API

Use Veridelta directly within Python workflows (e.g., Airflow, notebooks, or services):

```python
import polars as pl
from veridelta import DiffConfig, DiffRule, DiffEngine

config = DiffConfig(
    primary_keys=["user_id"],
    default_treat_null_as_equal=True,
    rules=[
        DiffRule(
            pattern="^AMT_.*",
            absolute_tolerance=0.05
        )
    ]
)

engine = DiffEngine(config, source_df, target_df)
summary = engine.run()

if not summary.is_match:
    print(f"Pipeline regression detected: {summary.changed_count} rows differ.")
```

## Documentation & Advanced Features

The examples above just scratch the surface. For full configuration details, the Python API reference, and advanced rule definitions—including schema evolution, whitespace handling, and type coercion—visit the official documentation:

**[veridelta.github.io/veridelta](https://veridelta.github.io/veridelta)**

---

## Roadmap & What's Next

Veridelta is in active development. While the core diffing engine is stable for file-based workflows (CSV, Parquet), we are rapidly expanding the ecosystem. Upcoming priorities include:

- **Warehouse Pushdown:** Direct SQL integrations for Snowflake and Databricks.
- **Lakehouse Native:** First-class support for Delta Lake and Apache Iceberg.
- **Advanced Heuristics:** Fuzzy string matching and ML-driven schema evolution mapping.
- **Reporting:** Interactive HTML diff dashboards and native GitHub Action integrations.

For a detailed look at planned features, check out the [Full Roadmap](https://veridelta.github.io/veridelta/roadmap/) in our official documentation.

---

## License

Veridelta is distributed under the terms of the Apache 2.0 License.
