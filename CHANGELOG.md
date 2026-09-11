## v0.9.0 (2026-09-11)

Internal. The public API does not change. This is the 1.0 candidate: the
breaking-change budget for the 0.7–0.9 line was spent in 0.7.0, 0.8.0 was
additive, and 0.9.0 is compiler and toolchain work.

Warehouse SQL now projects stages 1–7 once per column through a pair of
normalization CTEs. The join and the mismatch tally read those values, so
adding a transform no longer copies the expression tree into every later
predicate. Emitted SQL stays roughly linear in the number of compared
columns; the DuckDB harness is the same check it was in 0.6.0.

`engine.py`, `models.py`, `sentinels.py`, and `connectors/sql.py` are gated
at 100% branch coverage. `cast_to` is resolved through a `TypedDict` and an
exhaustive lookup table. `make lint` runs strict pyright. Global mypy
`ignore_missing_imports` is gone; missing stubs stay on per-module overrides
for optional drivers and test-only imports.

### Refactor

- project warehouse transforms through `_src_normalized` / `_tgt_normalized`
  CTEs so each column is normalized once, verified by the existing DuckDB
  harness and a SQL-length regression test
- convert `_get_effective_rule` to an `EffectiveRule` TypedDict so `cast_to`
  stays a `CastTarget` and `_CAST_TARGETS` is exhaustive

### Test

- fail `make test` and the core CI job unless the four core modules stay at
  100% branch coverage, and drop the dead `raise NotImplementedError`
  coverage exclude

### Chore

- add strict pyright to `make lint` and the CI lint job
- drop the global mypy `ignore_missing_imports` in favor of the existing
  per-module overrides

## v0.8.0 (2026-09-11)

Reporting and CLI polish. Purely additive: new flags default off, and the
exit codes stay 0 for a match and 1 for drift.

`--html PATH` writes one self-contained file. Styles and a 25-row pager are
inlined, so an air-gapped runner can open the report without fetching anything.
Embedded rows are capped (`--html-max-rows`, default 1000) and the truncation is
stated. Pushdown reports are labeled as primary-keys-only. Markup in column
names is escaped, and `<` inside the embedded JSON is rewritten so a value
cannot close the script block.

Progress chatter moved to stderr so `veridelta run --json | jq` does not have
to strip it. `--quiet` silences that chatter. `--version` / `-V` print the
package version.

`docs/configuration.md` is now bound to `DiffConfig`, `DiffRule`, and
`SourceConfig` by a test, so a new field cannot ship undocumented. A Getting
Started notebook covers local diffing and the `DiffResult` accessors added in
0.7.0.

### Feat

- write a standalone HTML report from the Python API or `--html`, with an
  inlined pager and a row cap that is visible in the page
- add `--json` so CI can read `DiffSummary` from stdout, `--quiet` to suppress
  progress, and `--version` / `-V`
- route every progress line to stderr, including the artifact path, so a piped
  `--json` run stays valid JSON
- add a Getting Started notebook and bind `docs/configuration.md` to the three
  config models so missing fields fail the test suite

### Fix

- explain configuration errors as a problem with the file, not the data, and
  point unexpected failures at the issue tracker with the exception class

## v0.7.0 (2026-09-10)

Two breaking changes, both mechanical.

`DiffEngine.run()` and `DiffEngine.run_from_configs()` now return a `DiffResult` rather
than a `DiffSummary`. Existing code reaches the old value through `.summary`:

```python
summary = DiffEngine(config, src, tgt).run().summary
```

`SourceType` and `output_format` are now closed sets. `SourceType` accepts `csv`,
`parquet`, `json`, `ndjson`, `arrow`, and `excel`; `output_format` accepts everything
but `excel`. The removed names (`fixed_width`, `netcdf`, `shapefile`, `geopackage`,
`sql`, `avro`, `xml`, `delta`) never had an implementation and already raised
`ConfigError` at run time, so a config using one was already broken; it now fails when
the config loads instead. Delta Lake is unaffected and still reached through the
`delta_lake` source type.

### Feat

- return `DiffResult` from the engine, carrying the added, removed, and changed frames
  alongside the summary. The rows were already materialized, counted, and discarded, so
  reaching them previously meant configuring `output_path` and reading files back
- add `DiffResult.get_mismatches(column)` to narrow the changed set to one column with
  the source and target values side by side, and `DiffResult.to_pandas()` for notebooks
- record the compared column set on `DiffResult`, so a mistyped column name is rejected
  on the pushdown path too, where the primary-key frames cannot reveal which columns
  were compared
- read JSON, NDJSON, Arrow IPC, and Excel. NDJSON and Arrow scan lazily; JSON and Excel
  are read whole, because Polars has no lazy reader for either
- write discrepancy artifacts as JSON, NDJSON, or Arrow IPC in addition to CSV and
  Parquet
- add an `excel` extra, reporting a missing install as a hint rather than an
  `ImportError` raised from inside Polars
- export the exception hierarchy, the warehouse and lakehouse configs, and the public
  type aliases from the package root. The docs already instructed users to catch
  `VerideltaError` and to construct a `SnowflakeConfig`, neither of which was reachable
  without importing from a submodule

### Fix

- ship the `py.typed` marker. The package advertised the `Typing :: Typed` classifier
  without it, so PEP 561 had every downstream mypy and pyright treat Veridelta as
  unannotated no matter how complete its annotations were
- bind `SourceType` and `ArtifactFormat` to the registries that implement them, with
  tests. A name could previously be advertised in the config schema with nothing behind
  it

### BREAKING CHANGE

- `DiffEngine.run()` and `DiffEngine.run_from_configs()` return `DiffResult`
- `SourceType` and `output_format` are constrained to implemented formats

## v0.6.0 (2026-09-10)

Warehouse pushdown now implements all nine transform stages. `pad_zeros`,
`datetime_format`, `timezone`, and `cast_to` previously raised `ConnectorError` on the
warehouse path, so any rule using them could only run locally.

`cast_to` is now a closed set of `Int64`, `Float64`, `String`, `Boolean`, `Date`, and
`Datetime`. Any other value is rejected at load time rather than silently skipping the
cast, so a config with a typo that previously reported a clean diff will now fail
validation. Configs already using valid Polars type names are unaffected.

### Feat

- compile `pad_zeros` as a sign-aware, non-truncating expression rather than an `LPAD`,
  which pads in front of a minus sign (`0-12` where Python's `zfill` gives `-012`) and
  discards characters past the target width
- compile `cast_to` through a per-dialect keyword table, truncating toward zero on a
  float-to-integer cast to match Polars where Snowflake and DuckDB round
- compile `datetime_format` by translating one directive at a time against a per-dialect
  table, with literal runs restricted to a separator allowlist and wrapped in the
  dialect's quoting. An untranslatable directive raises `ConfigError` rather than passing
  through, since an unrecognized directive parses nothing and returns NULL for every row
- enforce the `timezone` precondition against the probed warehouse schema, so a naive or
  non-temporal column fails the same way it does locally. The conversion itself emits no
  SQL: Polars rewrites only a column's timezone label and every downstream cast still
  reads the UTC instant, while warehouses have no per-column label to rewrite
- add a differential test harness that runs both engines over the same frames and
  compares the results, executing real compiler output through DuckDB

### Fix

- skip the text transform stages on non-text columns during pushdown, matching the local
  engine. A currency string compared against a native float previously asked the
  warehouse to run `REGEXP_REPLACE` over a number

### BREAKING CHANGE

- `cast_to` is constrained to a `Literal` of supported Polars type names. It previously
  accepted any string and resolved it with `getattr`, so an unrecognized name left the
  column uncast and the diff green. The field also reaches SQL as `CAST(x AS <type>)`,
  where a type name cannot be quoted or bound as a parameter

## v0.5.1 (2026-09-10)

Unsupported formats now raise `ConfigError` instead of `NotImplementedError`. Code
catching `NotImplementedError` around `LoaderFactory` or a diff run should catch
`ConfigError`, or `VerideltaError` for all framework failures.

### Fix

- raise `ConfigError` rather than `NotImplementedError` for a source format with no
  loader or an `output_format` with no writer, so the CLI reports "Configuration Error"
  instead of "Unexpected System Error" for an ordinary misconfiguration
- validate the artifact format before writing rather than inside the write loop. The
  check previously ran after the empty-frame guard, so a bad `output_format` was caught
  only when there was drift to write and a clean comparison passed silently
- name the supported formats in both messages, derived from the registry that backs the
  behavior so the message cannot drift from what actually works

## v0.5.0 (2026-09-09)

Null sentinels are no longer strings only. `null_values` and `default_null_values`
accept mixed scalars, and quoting now carries meaning: `-999` nulls out `-999` in a
numeric column and is ignored on a text column, while `"-999"` behaves the other way
around. A list that previously read `["N/A", "-999"]` still works unchanged; add the
unquoted form if the value also appears as a number.

### Feat

- widen `null_values` and `default_null_values` from `list[str]` to a mixed union of
  `str | int | float | bool`, with `strict=True` so Pydantic preserves the exact type
  each sentinel was written as
- filter sentinels against each column's dtype before use, in both the local engine and
  the warehouse compiler, so one global list can span a mixed schema. Text sentinels
  reach string, categorical, and enum columns; numbers reach any numeric column
  including decimals; booleans reach boolean columns only
- carry probed dtypes out of the warehouse schema probe into both pushdown query
  builders, filtering each side independently since the two relations can disagree
- raise `ConfigError` when an explicit per-column `null_values` rule holds no sentinel
  its column's type can match, locally and against the probed warehouse schema. A global
  `default_null_values` still skips silently, since spanning a mixed schema is its purpose
- reject `.nan` and `.inf` sentinels at load time, as NaN never compares equal to itself
  and infinity has no portable SQL literal

### Refactor

- emit one `CASE WHEN col IN (...) THEN NULL ELSE col END` per side instead of nested
  `NULLIF` calls, rendering numbers and booleans unquoted so they cannot break a numeric
  column's cast. Strings still route through `_literal` for apostrophe escaping

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
