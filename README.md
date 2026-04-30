# Veridelta

[![CI Pipeline](https://github.com/veridelta/veridelta/actions/workflows/ci.yml/badge.svg)](https://github.com/veridelta/veridelta/actions)
[![codecov](https://codecov.io/gh/veridelta/veridelta/graph/badge.svg)](https://codecov.io/gh/veridelta/veridelta)
[![PyPI version](https://badge.fury.io/py/veridelta.svg)](https://pypi.org/project/veridelta/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Semantic diffing for mission-critical data pipelines.** Veridelta is a high-performance, declarative data comparison engine powered by [Polars](https://pola.rs/). It applies semantic and mathematical rules to filter out expected pipeline noise—such as floating-point drift, type coercion, or schema evolution—allowing true data regressions to be programmatically isolated at scale.

**[Documentation & API Reference](https://veridelta.github.io/veridelta)**

---

## Why Veridelta?
During system migrations, pipeline refactors, or complex model upgrades, teams often rely on basic `EXCEPT` SQL queries to validate parity. These naive approaches fail spectacularly against acceptable variances (e.g., `0.0001` floating-point rounding, string casing changes, or dropped legacy columns), creating thousands of false-positive alerts. 

Veridelta solves this by letting you define the exact **tolerances and crosswalks** of your data transition, executing massive comparisons entirely out-of-core.

---

## Installation

Install via `uv` (Recommended):
```bash
uv add veridelta
```

Or via standard `pip`:
```bash
pip install veridelta
```

---

## Usage

Veridelta supports configuration-driven CLI execution for CI/CD, and a robust Python API for data orchestrator integration (Airflow, Dagster, Prefect).

### CLI Configuration (YAML)

Declare global tolerances and column-specific comparison rules via YAML:

```yaml
# veridelta.yaml
primary_keys: ["transaction_id"]
schema_mode: "intersection"
default_treat_null_as_equal: true

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

Execute the comparison pipeline:

```bash
veridelta run --config veridelta.yaml
```

---

### Python API

Integrate directly into programmatic workflows. The stateless `DiffEngine` natively consumes Polars `LazyFrame` objects, bypassing disk I/O to evaluate differences via zero-memory computation graphs.

```python
import polars as pl
from veridelta.models import DiffConfig, DiffRule
from veridelta.engine import DiffEngine

# Load data into lazy computation graphs
source_lazy = pl.scan_parquet("legacy_data.parquet")
target_lazy = pl.scan_parquet("modern_data.parquet")

# Define execution schemas and semantic rules
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

# Initialize the stateless engine and evaluate the graphs
engine = DiffEngine(config)
summary = engine.compare_lazyframes(source_lazy, target_lazy)

if not summary.is_match:
    print(f"Regression detected! {summary.changed_count} rows differ.")
    print(summary.report_summary)  # Outputs a pre-formatted Markdown report
```

---

## Core Capabilities

* **Global & Local Tolerances:** Apply mathematical constraints globally, or override them per column via exact names or Regex patterns.
* **Semantic Normalization:** Coerce custom strings (e.g., `"N/A"`) to nulls, standardize whitespace, and execute pre-evaluation regex replacements.
* **Structural Alignment:** Map legacy column names to modern schemas automatically and enforce schema strictness (`exact`, `allow_additions`, `intersection`).
* **Discrepancy Artifacts:** Automatically export isolated Parquet artifacts containing the exact `added`, `removed`, and `changed` records for downstream auditing.

---

## Roadmap

Upcoming enterprise integrations:

* **Warehouse Pushdown:** Direct SQL translation execution for Snowflake and Databricks.
* **Lakehouse Native:** First-class support for Delta Lake and Apache Iceberg table formats.
* **Advanced Heuristics:** ML-driven fuzzy string matching to bypass non-deterministic OCR artifacts.

[View Detailed Roadmap](https://veridelta.github.io/veridelta/roadmap/)

---

## License

Distributed under the Apache 2.0 License. See `LICENSE` for more information.