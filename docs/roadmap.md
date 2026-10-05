---
render_macros: true
---

# Roadmap

This page lists work that is not built yet. The current release is v{{ config.extra.version }}.

## Warehouses

- Pushdown for more SQL dialects, such as Redshift and Synapse.

## Editor

- A VS Code extension that runs a comparison and shows its discrepancy files inside the editor. Completion and checking of configuration files already work through the published JSON Schema; see [Editor support](configuration.md#editor-support). `veridelta validate` checks a file before a run.
