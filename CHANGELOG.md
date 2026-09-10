## v0.4.0 (2026-09-09)

Transforms now apply uniformly to every column, primary keys included, before the
joins. A `case_insensitive`, `whitespace_mode`, or `value_map` rule on a key column
therefore changes how rows are matched, not only how they are compared. Key
uniqueness is asserted after normalization, so a rule that collapses two keys into
one raises `DataIntegrityError` instead of exploding the join.

### Feat

- consolidate every column transform into one normalization pass per dataset, executed
  before the joins, and make the `DiffRule` docstring the canonical transform order that
  both the local engine and the SQL compiler follow
- implement `pad_zeros`, which stringifies first so a numeric `123` matches a text `"00123"`
- implement `datetime_format`, which parses text into timestamps so the column is compared
  as a timestamp rather than as text
- implement `timezone`, which converts timezone-aware data only and raises `ConfigError`
  for naive timestamps rather than assuming an origin zone and shifting every value
- compare every shared column on the warehouse path, with global `default_*` settings
  folded in, instead of only columns carrying an explicit rule
- populate `column_mismatches` on warehouse pushdown from a per-column `SUM(CASE ...)`
  tally, using `COALESCE(pred, FALSE)` to match the local engine under SQL three-valued logic
- write warehouse pushdown artifacts through the shared exporter under `_pks_only`
  filenames, since those queries project primary keys only
- reject a `pad_zeros` width supplied as a string or float instead of coercing it

### Fix

- stop applying `regex_replace` twice, once in the `run()` pre-pass and again during
  comparison, which corrupted any non-idempotent pattern
- stop a global `default_null_values` from failing every run containing numeric columns,
  by gating text transforms on string dtypes
- reject an unknown `timezone` name as a `ConfigError` naming the column, rather than
  surfacing a raw Polars error

## v0.3.0 (2026-09-08)

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

## v0.2.0 (2026-04-30)

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
