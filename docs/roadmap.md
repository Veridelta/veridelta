# Veridelta Roadmap

Veridelta is currently in **v{{ config.extra.version }}**. The core execution engine is stable. The following roadmap outlines the strategic expansion of the framework's ecosystem, developer tooling, and pipeline observability.

## 1. Enterprise Ecosystem Integration

* Additional warehouse dialects, such as Redshift and Synapse, as demand requires.

## 2. Developer Experience (DX) & Tooling
A framework is only as effective as the developer's ability to interface with it safely and efficiently.

* **VS Code Extension:** One-click local runs and inline views of discrepancy artifacts inside the editor. Completion and validation of configuration files already work through the YAML language server and the published JSON Schema (see [Editor support](configuration.md#editor-support)), and `veridelta validate` checks a file before a run.

## 3. Pipeline Observability
* **Telemetry Export:** OpenTelemetry-compliant JSON output for ingestion into Datadog, Grafana, or custom data quality dashboards.