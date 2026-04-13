# Welcome to Veridelta

**Semantic diffing for mission-critical data pipelines.**

Veridelta is a high-performance data comparison engine designed to validate changes between datasets. Built on top of [Polars](https://pola.rs/), it allows you to define explicit rules for expected variance, like floating-point jitter or casing differences, so you can ignore the noise and focus on real regressions.

---

## Key Features

* **Blazing Fast:** Powered by a Rust-backed Polars engine for massive dataset handling.
* **Declarative Rules:** Define tolerances, string normalization, and null handling in simple YAML.
* **Dual-Entry:** Use it as a CLI tool in CI/CD pipelines or as a Python library in Airflow/Notebooks.
* **Flexible Schema Modes:** Handle evolving datasets with support for additions, removals, and strict matching.

## Installation

Install via `uv` (recommended):

```bash
uv add veridelta
```

Or using `pip`:

```bash
pip install veridelta
```

## Quick Start

1.  **Define your rules** in a `veridelta.yaml` file.

2.  **Run the comparison**:

    ```bash
    veridelta run --config veridelta.yaml
    ```

3.  **Review the summary** in your terminal or check the `output_path` for detailed Parquet diffs.

-----

## Explore the Docs

  * [**Tutorials**](examples/quickstart.ipynb): Progressive guides covering basic quickstarts, advanced semantic rules, and production CI/CD workflows.
  * [**Configuration Guide**](https://www.google.com/search?q=configuration.md): Learn how to write rules for tolerances, strings, and schemas.
  * [**API Reference**](api.md): Detailed documentation of the Python classes and methods.
  * [**Roadmap**](https://www.google.com/search?q=roadmap.md): Discover upcoming features like warehouse pushdown, Lakehouse support, and advanced ML heuristics.
