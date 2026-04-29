# Veridelta

[![CI Pipeline](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![codecov](https://codecov.io/gh/veridelta/veridelta/graph/badge.svg)](https://codecov.io/gh/veridelta/veridelta)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Semantic diffing for data pipelines.** Define expected variance, validate intentional changes, and detect regressions with confidence.

Veridelta is a declarative data comparison engine powered by [Polars](https://pola.rs/).
It applies semantic and mathematical rules to filter out expected pipeline noise,
such as floating-point drift or formatting changes, and programmatically isolates true
data regressions at scale.

**[Documentation & API Reference](https://veridelta.github.io/veridelta)**

---

## Installation

```bash
uv add veridelta
```

---

## Usage

Veridelta supports configuration-driven CLI execution and programmatic Python API integration.

### CLI Configuration (YAML)

Declare comparison rules and tolerances via YAML:

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

Execute the comparison:

```bash
veridelta run --config veridelta.yaml
```

---

### Python API

Integrate directly into programmatic workflows (e.g., Airflow, Databricks). The `DiffEngine` natively consumes Polars `LazyFrame` objects for memory-safe, big-data execution.

```python
import polars as pl
from veridelta import DiffConfig, DiffRule, DiffEngine

source_lazy = pl.scan_parquet("legacy_data.parquet")
target_lazy = pl.scan_parquet("modern_data.parquet")

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

engine = DiffEngine(config, source_lazy, target_lazy)
summary = engine.run()

if not summary.is_match:
    print(f"Regression detected: {summary.changed_count} rows differ.")
```

---

## Core Capabilities

* **Structural Alignment:** Map legacy column names to modern schemas automatically.
* **Semantic Normalization:** Coerce string markers to nulls, standardize whitespace, and cast types dynamically before mathematical comparison.
* **Discrepancy Artifacts:** Export isolated Parquet files detailing `added`, `removed`, and `changed` records for downstream auditing.

---

## Roadmap

Upcoming enterprise integrations:

* **Warehouse Pushdown:** Direct SQL execution for Snowflake and Databricks.
* **Lakehouse Native:** First-class support for Delta Lake and Apache Iceberg.
* **Advanced Heuristics:** Fuzzy string matching and ML-driven schema mapping.
* **Reporting:** Interactive HTML diff dashboards and CI/CD status checks.

[View Detailed Roadmap](https://veridelta.github.io/veridelta/roadmap/)

---

## License

Distributed under the Apache 2.0 License.