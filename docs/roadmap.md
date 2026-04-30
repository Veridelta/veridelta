# Veridelta Roadmap

The Veridelta core execution engine is stable and production-ready. The following roadmap outlines the strategic expansion of the framework's ecosystem, intelligence, and developer tooling.

## 1. Enterprise Ecosystem Integration
While file-based architectures (CSV, Parquet) are fully supported, mission-critical pipelines often operate directly on distributed storage and warehouse compute.

* **Lakehouse Native:** Direct integration with **Delta Lake** and **Apache Iceberg** table formats.
* **Warehouse Pushdown:** Native SQL translation layers for **Snowflake**, **Databricks**, and **BigQuery**, enabling out-of-core evaluation without extracting data into local memory.

## 2. Advanced Heuristics & ML
* **Fuzzy Matching:** Implementation of Levenshtein and Jaro-Winkler distance thresholds to seamlessly bypass non-deterministic typographical errors and OCR artifacts without requiring explicit regex definitions.
* **Automated Crosswalks:** Statistical sampling heuristics to auto-generate `value_map` proposals (e.g., detecting a 99.9% correlation between legacy `M` and modern `Male`).

## 3. Developer Experience (DX) & Tooling
A framework is only as effective as the developer's ability to interface with it safely and efficiently.

* **VS Code Extension:** A dedicated IDE extension providing domain-specific YAML schema validation, strict type IntelliSense, one-click local test execution, and inline visualization of discrepancy artifacts directly within the editor. This shifts configuration error detection left, preventing wasted compute cycles in production.
* **Interactive Reporting:** A `--html` CLI directive to compile execution summaries and top-n drifts into standalone, interactive dashboards.

## 4. Pipeline Observability
* **Native CI/CD Runners:** First-party **GitHub Actions** and **GitLab CI** plugins to execute comparisons and post execution summaries directly to Pull Request comments.
* **Telemetry Export:** OpenTelemetry-compliant JSON output for ingestion into Datadog, Grafana, or custom data quality dashboards.