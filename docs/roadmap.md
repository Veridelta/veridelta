# Veridelta Roadmap

Veridelta is currently in `v0.1.0` (Alpha). The core semantic diffing engine is stable, but we are actively expanding the ecosystem. Here is what is coming next:

### 1. Expanded Ecosystem Support
While CSV and Parquet cover the majority of file-based workflows, enterprise pipelines often operate directly on data warehouses and lakehouses.
* Native integration for **Delta Lake** and **Apache Iceberg** tables.
* Direct SQL pushdown for **Snowflake** and **Databricks** to diff massive datasets without pulling them into memory.

### 2. Advanced Heuristics
* **Fuzzy String Matching:** Support for Levenshtein distance thresholds to catch minor typos without explicit regex.
* **Schema Evolution ML:** Auto-suggest `value_map` dictionaries based on statistical sampling (e.g., auto-detecting that 'M' maps to 'Male' 99% of the time).

### 3. CI/CD & Reporting
* **GitHub Actions App:** A native GitHub Action that comments on PRs with a mini Veridelta diff summary when pipeline code changes.
* **HTML Dashboards:** An optional `--html` flag in the CLI to generate a standalone, interactive web report of the diff results.