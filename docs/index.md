# Welcome to Veridelta

**Semantic diffing for mission-critical data pipelines.**

Veridelta is a high-performance data comparison engine designed to validate changes between datasets. Built on the [Polars](https://pola.rs/) DataFrame library, it enables explicit rule definitions for expected variance—such as floating-point jitter, schema drift, or categorical crosswalks—preventing false positives and isolating true data regressions.

---

## Core Capabilities

* **High-Performance Execution:** Powered by a Rust-backed Polars engine for out-of-core dataset processing.
* **Declarative Configuration:** Define numeric tolerances, string normalization, and type coercion in standardized YAML.
* **Omni-Channel Deployment:** Execute via CLI in CI/CD pipelines (GitHub Actions, GitLab CI) or as a Python library in data orchestrators (Airflow, Dagster).
* **Schema Evolution Support:** Manage structural drift with strict, intersection, or additive schema enforcement modes.

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

**3. Review the execution summary:** Evaluate the terminal output or inspect the generated `.parquet` artifacts for row-level discrepancies.

---

## Documentation Directory

* [**Tutorials**](examples/quickstart_cli.ipynb): Progressive guides covering CLI execution, programmatic Python usage, and advanced semantic rules.
* [**Configuration Guide**](configuration.md): Complete specification for tolerance rules, schema enforcement, and I/O settings.
* [**API Reference**](api.md): Developer documentation for the internal Python classes and methods.
* [**Roadmap**](roadmap.md): Upcoming framework capabilities, including warehouse pushdown and Lakehouse integrations.