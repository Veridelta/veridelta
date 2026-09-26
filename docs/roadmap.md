# Veridelta Roadmap

Veridelta is currently in **v{{ config.extra.version }}**. The core execution engine is stable. The following roadmap outlines the strategic expansion of the framework's ecosystem, intelligence, and developer tooling.

## 1. Enterprise Ecosystem Integration

* **BigQuery Pushdown:** Native SQL translation so comparisons run in BigQuery without extracting tables into local memory.
* Additional warehouse dialects as demand requires.

## 2. Advanced Heuristics & ML
* **Automated Crosswalks:** Statistical sampling heuristics to auto-generate `value_map` proposals (e.g., detecting a 99.9% correlation between legacy `M` and modern `Male`).

## 3. Developer Experience (DX) & Tooling
A framework is only as effective as the developer's ability to interface with it safely and efficiently.

* **VS Code Extension:** A dedicated IDE extension providing domain-specific YAML schema validation, strict type IntelliSense, one-click local test execution, and inline visualization of discrepancy artifacts directly within the editor. This shifts configuration error detection left, preventing wasted compute cycles in production.

## 4. Pipeline Observability
* **Native CI/CD Runners:** First-party **GitHub Actions** and **GitLab CI** plugins to execute comparisons and post execution summaries directly to Pull Request comments.
* **Telemetry Export:** OpenTelemetry-compliant JSON output for ingestion into Datadog, Grafana, or custom data quality dashboards.