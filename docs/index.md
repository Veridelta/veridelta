# Welcome to Veridelta

**Semantic diffing for mission-critical data pipelines.**

Veridelta is a high-performance data comparison engine designed to validate changes between datasets. Built on the [Polars](https://pola.rs/) DataFrame library, it enables explicit rule definitions for expected variance—such as floating-point jitter, schema drift, or categorical crosswalks—preventing false positives and isolating true data regressions.

---

## Core Capabilities

* **High-Performance Execution:** Powered by a Rust-backed Polars engine for out-of-core dataset processing.
* **Declarative Configuration:** Define numeric tolerances, string normalization, and type coercion in standardized YAML.
* **Omni-Channel Deployment:** Execute via CLI in CI/CD pipelines (GitHub Actions, GitLab CI) or as a Python library in data orchestrators (Airflow, Dagster).
* **Schema Evolution Support:** Manage structural drift with strict, intersection, or additive schema enforcement modes.
* **Warehouse and Lakehouse Connectors:** Push Snowflake or Databricks comparisons into SQL, or scan Delta Lake and Iceberg tables. See the [Configuration Guide](configuration.md) for YAML, extras, and routing rules.

## Installation

Install via `uv` (Recommended):

```bash
uv add veridelta
```

Or via standard `pip`:

```bash
pip install veridelta
```

## Quick Start

**1. Define the execution specification (`veridelta.yaml`):**

```yaml
source:
  path: "legacy_system.csv"
  format: "csv"
target:
  path: "modern_system.parquet"
  format: "parquet"

primary_keys: ["id"]
rules:
  - column_names: ["revenue"]
    absolute_tolerance: 0.01
```

**2. Execute the validation engine:**

```bash
veridelta run -c veridelta.yaml
```

**3. Review the execution summary:** Evaluate the terminal output. Set `output_path` to write `added` / `removed` / `changed` artifacts; `output_format` selects Parquet, CSV, JSON, NDJSON, or Arrow.

---

## Documentation Directory

* [**Tutorials**](examples/getting_started.ipynb): Progressive guides covering local `DiffResult` access, CLI execution, programmatic Python usage, and advanced semantic rules.
* [**Configuration Guide**](configuration.md): Complete specification for tolerance rules, schema enforcement, connectors, CLI flags, and I/O settings.
* [**API Reference**](api.md): Public Python classes and methods.
* [**Roadmap**](roadmap.md): Unimplemented work such as BigQuery pushdown and fuzzy matching.