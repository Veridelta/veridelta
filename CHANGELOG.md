## 0.3.0 (2026-09-08)

### Feat

- warehouse and lakehouse connectors with SQL pushdown (#5)
- add Snowflake and Databricks warehouse extras with Arrow SQL pushdown
- add Delta Lake and Iceberg lakehouse scans, including Iceberg `snapshot_id` time travel
- route YAML `source`/`target` through a discriminated `SourceRef` union and `DiffEngine.run_from_configs`
- compile warehouse anti-joins and row counts so pushdown reports added, removed, and changed
  counts against exact source and target totals
- enforce `schema_mode` and primary-key existence on warehouse relations via zero-row column probes
- allowlist dotted warehouse table identifiers and reject string-coerced numeric config fields
- report artifact persistence through `DiffSummary.artifacts_written`
- add `make docs` and `make docs-serve`, and build the documentation strictly in CI

### Fix

- stop the CLI from announcing discrepancy artifacts that warehouse pushdown never writes
- keep the Snowflake extra inside the driver's supported pyarrow range on Python 3.14
- update Python classifiers and correct homepage URL in pyproject.toml

## 0.2.0 (2026-04-30)

### Feat

- **core**: migrate to Polars Lazy computation graphs and implement enterprise CI/CD (#4)
- implement core semantic diffing engine and GitOps configuration (v0.1.0-alpha) (#3)
- add dynamic output_format for diff artifacts with strict fallback
- add RELAXED_ORDER schema validation mode
- implement core diff engine, schema validation, and documentation (#2)

### Fix

- update dataset URL to use versioning from package metadata
- satisfy CI requirements and trigger docs deployment
- add missing tests directory and modernize uv config
