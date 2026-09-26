I have the evidence for all three areas. Before writing the report, I'll consult the advisor on structure and on any gaps.
I've gathered what all three workstreams need. Beyond your list, I found two real correctness bugs and two design clashes that should shape the plan. Everything was read on `main` @ 882e1c5 (v0.9.1); experiments were in-memory one-liners with no repo writes.

Files cited by short name below:

| Short name | Absolute path |
|---|---|
| `cli.py`, `config.py`, `models.py`, `engine.py` | `/home/user/veridelta/src/veridelta/` |
| `sql.py`, `warehouse.py` | `/home/user/veridelta/src/veridelta/connectors/` |
| harness | `/home/user/veridelta/tests/integration/duckdb_harness.py` |
| parity tests | `/home/user/veridelta/tests/integration/test_pushdown_parity.py` |
| `test_docs_coverage.py` | `/home/user/veridelta/tests/unit/test_docs_coverage.py` |
| `configuration.md`, `roadmap.md` | `/home/user/veridelta/docs/` |
| `ci.yml`, `release.yml`, `docs.yml` | `/home/user/veridelta/.github/workflows/` |
| `pyproject.toml`, `.gitignore`, `Makefile` | `/home/user/veridelta/` |

## Real bugs a fuzzer will find

1. **Integer tolerance wraps around (`engine.py:2288`).** `tgt - src` overflows in the column's own integer width. With `abs_tol=1`, all of these count as **matches**: Int8 100 vs -100, Int16 ±30000, Int32 ±2e9, Int64 ±2^62 (Polars Int8 100 − (−100) = −56). A global `default_absolute_tolerance` exposes every integer column. DuckDB raises an overflow error on the same data instead.
2. **`%f` in `datetime_format` parses differently.**
   - Polars reads `.%f` as nanoseconds: `".123456"` becomes 123 µs and `".5"` becomes 0. DuckDB `%f`, Snowflake `FF6` and Databricks `SSSSSS` read it as a fraction of a second.
   - Same data gives local changed=1, pushdown changed=0.
   - `configuration.md:406` lists `%f` as supported for pushdown, and no test covers it.
   - Polars' correct spelling `%.f` is rejected by the pushdown translator.
   - It also emits `ChronoFormatWarning`, which fails any test under `filterwarnings = ["error"]`.

## (a) GitHub Action / GitLab template

**Workflows today**
- No `action.yml`, composite action or `.gitlab-ci.yml` exists.
- `ci.yml` runs on push to main and every PR. Every job does `actions/checkout@v5`, then `astral-sh/setup-uv@v7` (cache keyed on `uv.lock`, a `python-version` input), then `uv sync --all-extras`, then `uv run …`.
  - `test-core`: Ubuntu, macOS and Windows × Python 3.10–3.14. It runs `pytest tests/unit tests/integration` (l.103), then a 100% branch-coverage gate on the core modules (l.106).
  - `test-e2e`: Ubuntu and Windows on 3.12, running the CLI as a subprocess.
- `release.yml:3-6` runs `uv build` and `uv publish` on any `v*` tag; commitizen tags releases `v$version` (`pyproject.toml:236`).
  - **A floating `v1` Action tag in this repo matches `v*` and would trigger a PyPI publish.** It would also collide with a future package `v1.x.y` tag.
- `docs.yml` deploys the docs site with `mkdocs gh-deploy` from main.

**CLI contract (`cli.py`)**
- `run` takes `-c/--config` (default `veridelta.yaml`), `--json`, `-q/--quiet`, `--html PATH` and `--html-max-rows N`. Progress goes to stderr.
- `--json` prints `summary.model_dump_json(indent=2)`, which is multi-line (l.204-205). Without it, stdout gets `report_summary`.
- Exit code 0 means match. **Exit code 1 means drift *or* any failure** (l.33-37, 218, 147-175). Exit code 2 is an argument error.
  - On most failures stdout is empty.
  - The JSON is printed *before* the HTML report is written (l.204-216), so an HTML write failure gives valid JSON plus exit 1.
  - The Action therefore has to combine the exit code with the parsed JSON rather than branch on the exit code alone.
- The command dispatch table is at `cli.py:456-459`; a `validate` command plugs in there.

**`DiffSummary` JSON (`models.py:601-734`)**
- 13 keys: `total_rows_source`, `total_rows_target`, `added_count`, `removed_count`, `changed_count`, `column_mismatches`, `is_match`, `total_mismatches`, `mismatch_ratio`, `match_rate_percentage`, `is_perfect_match`, `volume_shift`, `report_summary`.
- Deliberately excluded: `report_limit` and `artifacts_written` (l.640-641).
- Not present at all: the threshold, which engine ran (pushdown / keys-only), the compared columns, the version, timings.
- `report_summary` underlines headings with `===` and `---`. In a GitHub comment those render as headings, so it needs a code fence, or the comment should be built from the fields.
- `DiffSummary.model_json_schema(mode="serialization")` covers all 13 keys and could serve as the Action's typed contract.

**Roadmap §3 (`roadmap.md:15-17`), verbatim:** "**Native CI/CD Runners:** First-party **GitHub Actions** and **GitLab CI** plugins to execute comparisons and post execution summaries directly to Pull Request comments." The second bullet is "**Telemetry Export:** OpenTelemetry-compliant JSON output…".

**The repo is public**, confirmed via the GitHub API rather than the badges: `Veridelta/veridelta`, `"private": false`, `"visibility": "public"`, GitHub Pages enabled, owned by an organization. `uses: Veridelta/veridelta@v1` would resolve once a root `action.yml` and a `v1` ref exist, subject to the tag clash above.

## (b) JSON Schema and `veridelta validate`

**How the YAML is loaded (`config.py`)**
- `load_config` pops `source`/`target` (l.203-204) and validates the rest as `DiffConfig` (l.208).
- For each block it expands `${VAR}` in strings, adds `type: file` when `type` is missing (l.153-154), then validates as `SourceRef`.
- An unset `${VAR}` with no default raises an error (l.84-88). **So a `validate` run in CI without secrets fails on any warehouse config** unless it has a mode that skips expansion.

**One schema can be generated.** A throwaway `class RootConfig(DiffConfig): source: SourceRef; target: SourceRef` produces a ~21 KB schema:
- Six shared definitions: `SourceConfig`, `SnowflakeConfig`, `DatabricksConfig`, `DeltaLakeConfig`, `IcebergConfig`, `DiffRule`.
- The root requires `primary_keys`, `source` and `target`. `extra='forbid'` becomes `additionalProperties: false` at the root, on `DiffRule` and on every connector block.
- `source`/`target` become a `oneOf` plus an OpenAPI-only `discriminator` that standard validators ignore. `type` is not required in any branch.
- `SentinelValue` becomes `anyOf [string, integer, number, boolean]`; because it is `anyOf`, integers matching both "integer" and "number" is not a problem.
- The schema has no `$schema` key.

I compared the schema (`jsonschema` 4.26, Draft 2020-12) against the loader's own logic:

| Case | Schema | Loader |
|---|---|---|
| Snowflake block **without `type`** | accepts (matches by structure) | rejects (treated as `file`) |
| `table: ${SNOWFLAKE_TABLE}` | **rejects** (fails the table-name pattern) | accepts (expands first) |
| `threshold: "0.1"`, `strict_types: 1`, `report_top_columns_limit: "5"` | rejects | accepts (pydantic coerces them) |
| `[.nan]` in `default_null_values`, `.inf` tolerance | accepts | rejects |
| Rule with both similarity limits; `pattern: "[bad"` | accepts | rejects (validators, `models.py:370-449`) |
| `rename_to` with two `column_names` | accepts | accepts, then silently ignored (`engine.py:468-485`, used by both engines) |
| `format: netcdf` | rejects, but with a vague `oneOf` message | rejects |

- Because the root forbids extra keys, a `$schema:` key in the YAML would be rejected; editors would need the `# yaml-language-server: $schema=` comment instead.
- `jsonschema` is only a transitive dev dependency. Runtime dependencies are just polars, pydantic and pyyaml (`pyproject.toml:29`).
- Every model field must appear in `configuration.md`, enforced by `test_docs_coverage.py:38-57`.

**What `validate` could check**
- **Config only, offline:**
  - `load_config` itself.
  - Backend-pairing errors, which are decided before any connection (`engine.py:1503-1531`): mixed backends, cross-dialect, mismatched connections, the same table on both sides.
  - Missing optional extras: `fastexcel`, `rapidfuzz`, the warehouse drivers.
  - Whether each `datetime_format` translates for the target dialect (`sql.py:1233-1296`).
  - Multi-column `rename_to` warnings.
  - Regexes as Polars sees them. Python's `re` accepts lookarounds and backreferences that Polars rejects only when executing. The Polars error does not appear at `collect_schema()`, but a one-row `pl.select(pl.lit("x").str.replace_all(p, r))` catches it cheaply.
- **Files and lakehouse, reading schemas only:**
  - Get schemas with `LoaderFactory.load(ref).collect_schema()`, then run `DiffEngine.validate_schemas` (`engine.py:1805-1822`).
  - The first steps of `run()` (`engine.py:2406-2414`) also only touch schemas and would add the sentinel, timezone and fuzzy-extra checks. The first real data read is the key-uniqueness check at l.2419.
  - `json` and `excel` sources are read into memory in full.
- **Warehouses, credentials required:**
  - `_validate_pushdown_schema` (`engine.py:1296-1333`) does two zero-row probes.
  - `_resolve_pushdown_keys`/`_resolve_pushdown_rules` run on schemas only.
  - Compiling `compile_query` without executing it surfaces untranslatable date formats.
  - Skip the duplicate-key and row-count queries (l.1372-1380); those scan the tables.

## (c) Hypothesis parity tests and `strict_types` in pushdown

**Harness**
- `DuckDBPushdownSession` registers `frame.to_arrow()` and runs `SET TimeZone = 'UTC'` (l.65-67).
- `run_pushdown` calls the real pushdown path; `run_local` calls `DiffEngine(...).run()`.
- `assert_parity` (l.171-201) compares the row totals, added/removed/changed counts, `column_mismatches`, `is_match`, and the set of compared columns.
- Duplicate-key rejections are compared by error message via `_rejection_on_both_paths` (parity tests l.1133-1141).

**Documented local-vs-pushdown differences (`configuration.md:66-80`)**
- Pushdown artifacts hold primary keys only.
- "`strict_types` applies to local runs only."
- Pushdown refuses `min_jaro_winkler_similarity`.
- DuckDB's `levenshtein` counts bytes (l.71; pinned by a test at l.1576-1595).
- Each warehouse runs its own regex engine, and capture references differ (`$1` vs `\1`).
- SQL `TRIM` strips only spaces.
- `datetime_format` goes through a fixed translation table. Snowflake/Databricks spellings are only checked as SQL strings, never executed (harness l.11-15).

**How local `strict_types` works (`engine.py:2272-2282`)**
- It compares the source and target dtypes *after* normalization (the schema is re-read at l.2449), using exact Polars equality. Time unit, time zone, Decimal precision/scale and Enum categories all count.
- If they differ, the value comparison is `False`. With `treat_null`, a NULL–NULL pair still matches.

**Normalized dtype by stage** (measured on Polars 1.40.1), which a pushdown version must predict from the probe for both sides:
- Null sentinels, regex, whitespace, case and `value_map` leave the dtype unchanged. Categorical is not treated as text.
- `pad_zeros` gives `String`.
- `datetime_format` on text gives `Datetime("us")`, or `Datetime("us","UTC")` when the format has `%z`.
- `timezone` gives `Datetime(<input unit>, zone)`, so a nanosecond input stays nanosecond.
- `cast_to` gives the table at `engine.py:979-986`; `cast_to: Datetime` gives a naive `Datetime("us")` even after `timezone`.

**Where the pushdown change goes**
- `_resolve_pushdown_rules` asks `_compares_numerically`/`_compares_as_text` (`engine.py:695-743`) about the **source** dtype only (l.908-910). Those already encode the stage rules above as yes/no answers. One "predict normalized dtype" helper, applied to both sides, would replace both.
- The compiler's `_compare` (`sql.py:1446-1473`) sees only a `DiffRule`. `DiffRule` forbids extra fields (`models.py:283`) and every field must be documented, so a "types differ" signal needs a separate parameter into `compile_query`/`compile_column_mismatch_query`, not a new `DiffRule` field.
- For drifted columns, emit `FALSE`, or `(src IS NULL AND tgt IS NULL)` under `treat_null`. That also avoids today's cross-type conversion errors.

**The gap today** (harness, `strict_types=True`):

| Case | Local changed | Pushdown |
|---|---|---|
| Float64 10.0 vs Int64 10 | 1 | 0 |
| Int64 100 vs String "100" | 1 | 0 |
| `datetime_format` text vs `Datetime("ns")` | 1 | 0 |
| Decimal(10,2) vs Decimal(12,4), equal values | 1 | 0 |
| String "abc" vs Int64 | 1 | error (DuckDB conversion error) |
| Both NULL with `treat_null` | 0 | 0 |

**Dtypes the harness probe reports differently from the original frame**
- Categorical and Enum come back as String.
- A New York time-zone label comes back as UTC.
- `Datetime("ns","UTC")` comes back as microseconds.
- A `Null`-typed column comes back as Int32. `[None]` infers `Null`, so the fuzzer must pass explicit schemas.
- `Duration` makes `pl.from_arrow` panic at harness l.94, outside its error handling.
- Everything else round-trips unchanged: all integer widths, Float32/64, Decimal, String, Boolean, Date, Time, naive Datetime in ms/us/ns, Binary.

**Other harness divergences the fuzzer would trip on (all verified)**
- **DuckDB's three-argument `regexp_replace` replaces only the first match.** `'aa'` becomes `'a'`, and `[$,]` on `$1,000,000` gives local 0 vs pushdown 1.
  - The compiler emits that form for every dialect (`sql.py:1089-1091`). The only regex parity test (l.139-153) uses data with one match per value.
  - A DuckDB-only `'g'` flag would fix it. As far as I know Snowflake and Databricks replace every match by default, but I couldn't verify that here.
- **Regex engines:**
  - `\d` on the Arabic-Indic digit U+0661 gives 0 vs 1.
  - Lookaround fails on both paths, but with different exception types.
- **Text normalization:** `LOWER('İ')` gives 0 vs 1. A tab under `whitespace_mode: both` gives 0 vs 1 (documented).
- **Number and datetime to text** (`cast_to: String`, `pad_zeros`):
  - `2.5e-05` becomes `0.000025` locally but `2.5e-05` in DuckDB.
  - `1e-7` vs `1e-07`, `NaN` vs `nan`.
  - Datetimes: `…05.000000` vs `…05`.
- **Casts:** Polars casts are strict and SQL casts are lenient. `" 1.5 "`→Float64, `"1.0"`→Int64, `"yes"`→Boolean and a timestamp string→Date all raise locally but succeed in DuckDB.
- **Unsigned integers with a tolerance:** DuckDB's `ABS(tgt - src)` (`sql.py:1394`) overflows when target < source, while local guards against it (`engine.py:2286-2288`). DuckDB's `ABS` of the minimum integer also overflows.
- **Column names:** they must match `^[A-Za-z_][A-Za-z0-9_]*$` (`models.py:25`).
- **Already at parity:** NaN/inf under tolerances (l.1385-1428), NaN vs NaN, -0.0 vs 0.0, and exact tolerance boundaries.

**Tooling**
- `hypothesis` is not installed and not in `uv.lock`. It would go in the dev group (`pyproject.toml:64-85`), which CI's `uv sync --all-extras` installs.
- pytest config (`pyproject.toml:199-217`):
  - Runs with `--strict-config`, `--strict-markers` and `filterwarnings = ["error"]`.
  - Registered markers are unit, integration, e2e, smoke, fast and slow; a new marker must be added there.
- Every parity class is marked integration and slow, and CI does no marker filtering, so a property suite would run in all 15 `test-core` jobs.
- The 100% branch gate (`ci.yml:106`, `Makefile:19`) still needs deterministic tests for any new branches, since Hypothesis runs are randomized.
- `.hypothesis/` is not in `.gitignore`.