# Hand-off: release 0.10.0, database sources, then everything else

This is written for a local Claude Code CLI session picking up where a cloud session stopped on 2026-09-26. It is self-contained: read it top to bottom before doing anything.

> **This file and `handoff/` are notes, not product code.** They live on branch `claude/ci-integrations` only so they can be fetched.
>
> **Before building PR C on that branch:**
> 1. Copy both somewhere outside the repo.
> 2. Reset the branch to `origin/main`, or start PR C on a fresh branch from `main`.
>
> Never merge these files into `main`.

## Where things stand

| Item | State |
| --- | --- |
| **R**: release 0.10.0 | [#45](https://github.com/Veridelta/veridelta/pull/45) squash-merged as `9fddf4a`. **The tag is not pushed**, because the cloud proxy refused tag pushes (HTTP 403). PyPI still has 0.9.1. |
| **D**: database sources | [#46](https://github.com/Veridelta/veridelta/pull/46) is open from `claude/admiring-ritchie-vjhbxe`. All 22 checks are green, including the Windows cells running the real SQLite/ConnectorX tests. It waits on review and merge. |
| **C, S, P, A, W, Q** | Not started. Plans are below. |

### Finish the release (the user, or the local session with push rights)

```bash
git fetch origin main
git tag -a v0.10.0 -m v0.10.0 9fddf4af24fb0c292038f9fcd1b62fc5f5697e06
git push origin v0.10.0
git ls-remote --tags origin v0.10.0   # confirm
```

- The push triggers `.github/workflows/release.yml` ("Publish to PyPI"). Earlier runs waited about 15 minutes before starting, probably an environment rule on `pypi`. Approve the run if GitHub asks.
- Confirm with `curl -s https://pypi.org/pypi/veridelta/json | python -c "import json,sys; print(json.load(sys.stdin)['info']['version'])"`.
- Create the GitHub release from the tag, as for every earlier version: Releases → Draft → `v0.10.0` → Generate release notes → Publish.
- `datasets.load_nyc_taxi()` in 0.10.0 downloads from the `v0.10.0` tag, so the tag matters beyond PyPI.

### What #46 contains (merge it before S, which builds on it)

Six commits:
1. `build(deps)`: the `database` extra, with `connectorx>=0.4.5` and `pyarrow>=14.0.1`, added to `all`.
2. `feat(models)`: `DatabaseConfig` (`uri`, `password`, and exactly one of `table` / `query`). `redacted_uri` masks the password; `password` is left out of repr.
3. `feat(connectors)`: `compile_database_select`, with the scheme-keyed `_DATABASE_IDENTIFIER_QUOTES` in `sql.py`. The security rule records this exception.
4. `feat(connectors)`: `DatabaseConnector` in `connectors/database.py`.
   - It reads eagerly, once.
   - It percent-encodes `password`.
   - Errors are scrubbed and raised `from None`.
   - It refuses a missing SQLite file, because ConnectorX would create one.
   - The module is in the 100% coverage gate.
5. `feat(engine)`: `DatabaseConfig` joins `SourceRef`, and `LoaderFactory` reads through the connector. The mixed-backend messages now say "file/lakehouse/database".
6. `docs`: the configuration guide's "Database sources" section, README, and index.

## Ground rules (from `.cursor/rules/*.mdc`, the repo, and the user)

**Toolchain:**
- `uv` only.
- Before every push, run all of these:
  - `make all`: ruff with C901 ≤ 10, strict mypy on src and tests, strict pyright, pytest with `filterwarnings=error`, a 100% branch gate, and `mkdocs build --strict`;
  - `uv run pre-commit run --all-files`;
  - `make notebooks`;
  - `uv run pytest tests/smoke tests/e2e --no-cov`;
  - `uv run cz check --rev-range origin/main..HEAD`.
- The 100% branch gate covers `engine.py`, `models.py`, `sentinels.py`, and `connectors/sql.py`, `warehouse.py`, `lakehouse.py` and `database.py`. The list is in the `Makefile` and in `.github/workflows/ci.yml`; a new gated module goes in both.
- The docs use the mkdocs macros plugin, so never write `{{`, `{%` or `{#` in `docs/`. A page that needs `${{ }}` needs `render_macros: false` front matter.
- The pre-commit mypy environment has only polars, pydantic and pyyaml. Optional imports need a mypy override in `pyproject.toml`.

**Workflow:**
- Tests first; each new test must fail for its stated reason before the fix.
- Every commit is green on its own.
- Conventional commits (`feat`, `fix`, `refactor`, `perf`, `test`, `chore`, `docs`, `build`).
- Feature PRs never touch `CHANGELOG.md`; it is written at release time.
- Every config-model field must appear in `docs/configuration.md` (`tests/unit/test_docs_coverage.py`). Every public export goes in `veridelta/__init__.py`'s sorted `__all__` (a smoke test checks the sorting).

**Security** (`.cursor/rules/300-security.mdc`):
- All SQL is assembled in `connectors/sql.py`.
- Identifiers are allowlisted, then quoted.
- Literals go through `_literal`.
- Numeric SQL operands are strict ints or floats.
- Every dialect spelling lives in a module-scope table keyed by `SQLDialect`, so a missing entry raises.

**PRs:**
- One PR per item, each on its own branch cut from the latest `main`:
  - `claude/ci-integrations`
  - `claude/config-schema`
  - `claude/parity-fuzzing`
  - `claude/avro-format`
  - `claude/warehouse-crosswalks`
  - `claude/bigquery-pushdown`

  Any names work locally; these are the approved ones.
- The user merges these PRs (squash). If an open branch conflicts after a merge, merge `main` into it and regenerate `uv.lock` with `uv lock`. Never rebase a pushed branch.
- The repo has a Codecov patch check, so every new line must be covered.

**Future release procedure**, e.g. 0.11.0 after these land (house style from #10, #12, #22 and #45):
1. On a branch, run `uv run cz bump --version-files-only --yes`. It updates `pyproject.toml` (twice), `__init__.py`, `mkdocs.yml` and `CHANGELOG.md`, with no commit and no tag.
2. Run `uv lock`.
3. Rewrite the generated changelog block in house style: prose first; lowercase bullets under Feat/Fix/Refactor/Chore; no scopes or PR numbers; `### BREAKING CHANGE` last.
4. Commit as `chore(release): X.Y.Z`, open a PR, and squash-merge it.
5. Run `git tag -a vX.Y.Z -m vX.Y.Z <squash sha>`, then push the tag.
- **Never run a bare `cz changelog`**: it rewrites the whole file.

## Known pre-existing bugs (fixed by PR P below)

- **Integer tolerance overflow.** `engine.py` `_build_match_expr` subtracts in the column's own width. With `abs_tol=1`, Int8 `100` vs `-100` matches, and Int64 extremes match. The fix is to widen to `pl.Int128`.
- **`%f` in `datetime_format`** reads nanoseconds in Polars (`.123456` becomes 123 µs, and it emits `ChronoFormatWarning`) but a fraction of a second in SQL.
- **DuckDB harness `regexp_replace`** replaces only the first match unless it gets the `'g'` flag.

## Research reports (in `handoff/research/`)

Paths inside these reports that start with `/home/user/veridelta/` mean the repo root. Line numbers refer to `main` at `882e1c5` or `9fddf4a`.

- `release.md`: the release mechanics, commitizen behavior, and every behavior change since 0.9.1.
- `database.md` and `database_step0.md`: ConnectorX and Polars readers, wheels, and security. From `database_step0.md`: SQLite NUMERIC reads as String in a zero-row probe, and a missing SQLite file gets created.
- `groundwork.md`: the Action/CLI contract, the JSON Schema prototype findings, `validate` checks, the `strict_types` gap table, and the parity divergences a fuzzer will hit.
- `design_dcspa.md`: **the detailed designs for D, C, S, P and A**, with commit-by-commit tests, error wording and file:line anchors. D is done (#46); read C, S, P and A there before coding them.
- `crosswalk.md`: the local crosswalk algorithm, the DuckDB prototype of the warehouse SQL (matched local output exactly), and the stringification divergences.
- `bigquery.md`: the table-by-table BigQuery spellings with sources, the connector constraints (import-time `FutureWarning` on Python 3.10, `to_arrow_iterable`), and `BigQueryConfig` field rules.

## Remaining plan

The sections below are copied from the approved cloud plan. In them:
- "the plan" and "research `<id>`" mean the files above.
- Ignore cloud-only mechanics such as `send_later`, `subscribe_pr_activity` and MCP tool names. Use `gh` locally.

## PR C: CI integrations (`claude/ci-integrations`)
Roadmap §3. Full design is in `handoff/research/design_dcspa.md` §C.
- **C1 `feat(report): render comparison summaries as Markdown`**
  - `render_markdown` and `write_markdown` in `report.py` produce:
    - a verdict heading;
    - a metrics table;
    - a top-drift table limited to `report_limit`;
    - a keys-only note for pushdown.
  - Column names go in adaptive-length code spans with `|` and newlines neutralized, so a name can't break the table or spoof the sticky marker.
  - `veridelta run --markdown PATH`. Tests include the hostile names; `test_cli.py`'s `default_args` fixture gets `markdown=None`.
- **C2 `chore(ci): publish to PyPI only for release tags`**: set `release.yml`'s filter to `v[0-9]+.[0-9]+.[0-9]+`, so a floating tag can't publish.
- **C3 `feat(ci): add a composite GitHub Action`** (`action.yml` at the root)
  - Installs from `$GITHUB_ACTION_PATH` by default, so `@vX.Y.Z` runs exactly that version. An optional `version` installs from PyPI instead.
  - Runs `--json --html --markdown` under `set +e`.
  - `status` output is `match`, `drift` or `error`. Drift means exit 1 with `"is_match": false`.
  - Writes the step summary.
  - Uploads an artifact with `if: always()`; the default name comes from the config path plus the job.
  - Keeps one sticky PR comment through `actions/github-script` (Node 24 major), keyed by a marker per config; a 403 or 404 on forks becomes a warning.
  - A final gate: an error always fails, and drift fails only with `fail-on-mismatch`.
  - **Inputs reach scripts only through `env:`**, which prevents script injection. Third-party actions are SHA-pinned with version comments.
  - A new `ci.yml` job `action` on ubuntu, macOS and Windows runs `uses: ./` against `tests/fixtures/ci/{match,drift,broken}.yaml` and asserts the outputs.
  - `tests/unit/test_ci_integrations.py` checks that:
    - every input is declared;
    - no `${{ inputs.` appears in a `run:`;
    - each run step has a shell;
    - the `veridelta run` line parses with `build_parser()`.
- **C4 `feat(ci): add a GitLab CI template`** (`ci/gitlab/veridelta.yml`)
  - A `spec: inputs` header and a job named from an input.
  - Uses the `ghcr.io/astral-sh/uv:python3.12-bookworm` image and runs `uvx --from "veridelta[…]==<version>"`.
  - Artifacts use `when: always` and `expose_as`.
  - A sticky MR note is posted through the API with Python's stdlib, only when `VERIDELTA_GITLAB_TOKEN` and `CI_MERGE_REQUEST_IID` are both set.
  - Add the template's version to commitizen `version_files`, so future bumps pin it.
  - Tests: the YAML parses, every input is declared, and the CLI line parses. It can't be run here.
- **C5 `docs: document the CI integrations`**
  - A new `docs/ci.md` with `render_macros: false` front matter (it needs `${{ }}`), added to the nav. It covers:
    - pinning to a tag or SHA;
    - the `pull-requests: write` permission;
    - forks;
    - passing secrets to `${VAR}`;
    - GitLab `include: remote` with `inputs`.
  - A README section.
  - Delete the roadmap §3 "Native CI/CD Runners" bullet and keep Telemetry.

## PR S: JSON Schema + `veridelta validate` (`claude/config-schema`)
Full design is in `handoff/research/design_dcspa.md` §S.
- **S1 `feat(config): generate a JSON Schema for configuration files`**
  - `config_json_schema()` builds on a private `_RootConfig(DiffConfig)` with `source`/`target: SourceRef`, and adds `$schema`, `$id` and `title`.
  - Post-processing:
    - `type` is required in non-file branches;
    - pattern and enum strings in source branches also accept `${…}`.
  - `jsonschema` goes in the dev group, with a mypy override.
  - Tests:
    - `check_schema`;
    - schema verdict equals loader verdict on the known pairs;
    - every complete YAML example in the docs and README validates and loads.
  - Documented difference: lax coercions such as `threshold: "0.1"` are rejected by the schema but accepted by the loader.
- **S2 `feat(cli): print the schema with veridelta schema`**
  - Commit `docs/schema/veridelta.schema.json` (about 29 KB), which the docs site publishes.
  - Add a `make schema` target.
  - **A drift test names `make schema`.** Later model-changing PRs (A, W, Q) must regenerate the file.
  - Docs "Editor support": the `# yaml-language-server: $schema=<url>` modeline, since a `$schema:` key is rejected.
- **S3 `refactor(engine): route warehouse pairs through one session registry`**. This is the **only** reshaping of `engine.py:1503-1538`; W and Q just add entries:
  - `_WAREHOUSES`, a registry mapping config type → (fingerprint function, connector class);
  - `_check_backend_pairing(source, target)`, which covers mixed backends, cross-dialect, fingerprints and `_reject_self_comparison`, and runs without connecting;
  - `_with_warehouse_session(source, target, work)`, which connects, then closes in `finally`.

  `_run_warehouse_pushdown` and `validate` both use these, with no change in behavior; the existing routing tests cover it.
- **S4 `refactor(engine): separate run()'s schema-only plan from its row work`**
  - `_plan()`.
  - A public `DiffEngine.validate_rules(config, source, target)` next to `validate_schemas`. It's small and useful from Python too.
- **S5 `feat(cli): validate configurations offline`**: `veridelta validate -c PATH [--allow-missing-env] [--json] [-q]`. Exit 0 means valid (warnings allowed), 1 invalid, 2 bad arguments. Checks:
  - the load, where `--allow-missing-env` substitutes the variable's name and lists it;
  - backend pairing;
  - missing extras for the routes actually used;
  - regexes executed by Polars: an error locally, a warning for pushdown;
  - warnings for warehouse-pair problems:
    - untranslatable `datetime_format`;
    - Jaro-Winkler;
    - `normalize_column_names`;
  - an unknown-scheme database `table`.
- **S6 `feat(cli): check live schemas with validate --schemas`**. It never reads rows:
  - files and lakehouses are loaded, then `validate_rules`;
  - database `table` sources use `compile_database_probe` (`WHERE 1 = 0`), and database `query` sources are skipped with a warning;
  - warehouses run their two probes plus rule resolution, and every statement is compiled but never executed.
- **S7 docs**: the CLI sections, and the roadmap §2 VS Code bullet reworded.

## PR P: parity (`claude/parity-fuzzing`)
Full design is in `handoff/research/design_dcspa.md` §P.

**Step 0:** check that Int128 arithmetic and `%.f` behave on the Polars floor, 1.39.3.
- **P1 `fix(engine): keep integer tolerances from wrapping around`**
  - Add a `_tolerance_match` helper; `_build_match_expr` is already at C901 9.
  - Cast integer pairs to `pl.Int128`. That also fixes `abs(-128)` for relative tolerances.
  - Tests:
    - Int8 100 vs -100 at abs 1 → changed;
    - Int64 extremes;
    - Int8 -128 vs -127 at rel 0.5 → match;
    - UInt64 0 vs max.
- **P2 `fix(engine): read %f as a fraction of a second`**
  - `_polars_datetime_format` maps `.%f` to `%.f` and a bare `%f` to `%6f`, leaving `%%f` alone.
  - This also removes the `ChronoFormatWarning`.
  - Fix `configuration.md:406` and document the remaining difference: `%.f` accepts a missing fraction or 7–9 digits.
  - Add a parity test.
- **P3 `fix(pushdown): replace every regex match on DuckDB`**
  - Add a `_REGEX_REPLACE_FLAGS` table: DuckDB `'g'`, nothing for the others.
  - Parity test: `1-800-555` with `{"-": ""}`, which gives local 0 and pushdown 1 today.
- **P4 `refactor(engine): predict normalized dtypes with one helper`**
  - `_normalized_dtype(...)`, following the measured stage rules.
  - Re-express `_compares_numerically` and `_compares_as_text` through it, and prove equivalence on the existing oracle suite.
- **P5 `fix(pushdown): widen integer operands in tolerance predicates`**
  - Add a `_WIDE_INTEGER_TYPES` table: `NUMBER(38,0)` for Snowflake, `DECIMAL(38,0)` for the others.
  - A `wide_integers` keyword argument on `compile_query` and `compile_column_mismatch_query`; wrap both operands before every `ABS`.
  - Parity for UInt32 5/3, Int8 100/-100 and Int64 extremes. DuckDB raises `OutOfRange` on all three today.
- **P6 `fix(pushdown): enforce strict_types in warehouse comparisons`**
  - A `type_drift` keyword argument, not a `DiffRule` field.
  - `_compare` emits `FALSE`, or `(src IS NULL AND tgt IS NULL)` under `treat_null`.
  - Parity for:
    - Float64 vs Int64;
    - Int64 vs String;
    - Decimal(10,2) vs Decimal(12,4);
    - "abc" vs Int64, which DuckDB errors on today;
    - both NULL.
  - The ns case is out of scope: the harness probe reports us.
  - Replace `configuration.md:69`. Strict mode compares the driver-reported types after normalization.
- **P7 `test(parity): fuzz local and pushdown verdicts with Hypothesis`**
  - Setup: `hypothesis` in the dev group, `.hypothesis/` in `.gitignore`, a `property` marker, a mypy override.
  - Strategies live in `tests/integration/parity_strategies.py`:
    - kind-safe columns and rules;
    - the target derived from the source by row drops, adds, mutations and null flips;
    - documented divergences excluded: tabs and Unicode whitespace, non-ASCII, `\d`/`\w`, float→text, lenient casts, Categorical/Enum/tz-label/ns/Null/Duration dtypes.
  - The property: an identical outcome, meaning the same counts, `column_mismatches` and `compared_columns`, or the same exception class. A `ConnectorError` always fails.
  - The `ci` profile is derandomized with about 50 examples and no deadline; a `deep` profile is selected through an environment variable.
  - **Anything the fuzzer finds** is either fixed in scope, with an `@example` and a deterministic regression test, or constrained, documented and pinned by a test. The 100% gate never relies on Hypothesis.

## PR A: Avro (`claude/avro-format`)
- **A1 `feat(engine): read Avro files`**
  - `AvroLoader`: an eager `pl.read_avro(path, **options).lazy()`, with a docstring note on why it's eager. Register it.
  - Add `"avro"` to `SourceType`.
  - Update the format-list regex in `test_engine.py:55`.
  - Tests:
    - dtype and null round-trips for the dtypes the writer supports;
    - `columns` and `n_rows` pass through;
    - an empty frame keeps its schema;
    - Avro vs Parquet.
  - Docs:
    - local paths only: `bytes` and `s3://` fail;
    - eager.

    Avro stays out of `ArtifactFormat`: the writer panics on Null columns and can't write several dtypes.
  - Regenerate the schema (`make schema`).

---

## PR W: warehouse crosswalks (`claude/warehouse-crosswalks`)
A DuckDB prototype of this shape reproduced the local numbers exactly on text columns (`handoff/research/crosswalk.md`).

**W1 `refactor(engine): prepare pushdown helpers for proposals`:**
- Reuse S3's `_with_warehouse_session` and `_check_backend_pairing` as they are.
- Factor `_resolve_pushdown_rules`' skip/fold/precondition loop so a caller can opt out of the Jaro-Winkler refusal. A crosswalk never compares values, so a Jaro-Winkler rule must not block it, just as it doesn't locally.
- Move `_value_map_proposal` to module level (it only reads `config`).
- Factor the exact confidence filter and sort (engine.py:1642-1647) into one Polars helper that both paths use.

**W2 `fix(engine): reject non-numeric crosswalk thresholds`:**
- `_check_value_map_thresholds` today accepts `True`, `np.float64` and `Decimal`. Require real `int`/`float` values and reject `bool`.

**W3 `feat(pushdown): propose value maps inside the warehouse`:**
- **`sql.py`:** `compile_value_map_query(source_table, target_table, primary_keys, rules, *, min_support, sample_fraction, source_types, target_types, key_rules) -> str | None`.
  - It is **one statement**: the existing `_normalized_with_clause`, plus a `_veridelta_joined` CTE (the inner join on normalized keys), plus a `UNION ALL` of per-column branches.
  - The branches are labeled with compiler-generated **integers**, so no column name becomes a literal.
  - The query filters out null sources, and `NOT IN (<_literal map outputs>)`.
  - It groups by (label, source, target) for `COUNT(*)` as `agreeing_rows`, then computes `SUM(...) OVER (PARTITION BY label, source)` as `rows`.
  - It keeps only exact predicates: `target <> source`, `agreeing >= min_support` and `2 * agreeing > rows`.
  - Counts are `CAST` to the Int64 keyword.
  - **`min_confidence` never reaches SQL.** A float literal can misround at the boundary: 198 of 2003 values failed to round-trip in DuckDB. Instead, `2a > r` returns an exact superset, and the engine applies the shared Polars filter, so decisions match by construction.
  - Add a strict integer renderer, `_integer`, that rejects `bool` and non-`int`. `_number` is `repr` and renders `True` or numpy scalars verbatim.
  - Add `_SAMPLE_HASH` as a per-dialect bucket table, with an explicit branch per dialect:
    - Snowflake: `MOD(MOD(HASH(k…), N) + N, N)`;
    - Databricks: `pmod(xxhash64(k…), N)`;
    - DuckDB: `hash(k…) % N`.

    It applies over the normalized source keys, only when the fraction is below 1.
  - Split the method into small helpers to stay at C901 ≤ 10.
- **`engine.py`:** `propose_value_maps_from_configs` checks thresholds first. It then routes a same-warehouse pair to `_collect_value_map_proposals` through `_with_warehouse_session`, which runs:
  - the schema probes;
  - key rules;
  - candidates (below);
  - duplicate-key checks on both sides, before any "no candidates" exit;
  - one statement.

  It parses the result, raising `ConnectorError` on a malformed one. It drops the label before `ValueMapEntry(**row)`, applies the shared filter, and builds proposals.
- **Scope:** candidates are columns **stored as `String` on both sides** (plus the local `_compares_mapped_text` gates). Float, datetime and NaN stringification differs between Polars and SQL, and a pasted `Y: '1'` for an Int64 target can abort a warehouse run. Local runs keep proposing those; the docs state the difference.
- `connectors/base.py`: add `"value_maps"` to `PushdownQueryType`, and fix the docstring list, which also misses `"duplicates"`.
- **Sampling:** `sample_fraction < 1` gives repeatable proposals, but from a different sample than a local run.

**Tests:**
- Harness parity at `sample_fraction=1.0`, mirroring each `TestValueMapProposals` case: M→Male at 19 of 20 exactly on the 0.95 boundary, `case_insensitive`, merged existing maps, the governing index, and ties in the order.
- SQL string tests per dialect, including `_literal` escaping inside `NOT IN`.
- Routing tests with mocked connectors: fingerprint, dialect, same table, session closed on failure, malformed result.
- Strict-number rejections.
- Sampled runs repeat, with `0 < rows < total`.
- Rewrite `test_it_refuses_warehouse_sources_without_connecting` (test_engine.py:2219-2236) as a mixed-backend refusal.
- Hold 100% branch coverage on every new branch.

**W4 `docs`:** in "#### Proposing a value map":
- replace config.md:467 (warehouses raise) with the warehouse behavior and its connection requirements;
- note the text-only scope;
- say sampled proposals differ between engines.

The CLI `crosswalk` is unchanged.

## PR Q: BigQuery pushdown (`claude/bigquery-pushdown`)
Research in `handoff/research/bigquery.md` has the table-by-table spellings with sources. Its conclusions:

**Q1 `feat(models): add a BigQuery connection config`:**
- `BigQueryConfig` (frozen, `extra="forbid"`, `hide_input_in_errors`):
  - `type: bigquery`.
  - `table`: `table` or `dataset.table`, matching `^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$`.
  - `project`: `^[a-z][a-z0-9-]{4,28}[a-z0-9]$`. Hyphens are legal. It is both the job project and the data project, and it never appears in SQL.
  - `dataset`: optional default dataset, as an allowlisted segment. A validator requires it when `table` has one segment.
  - `location`: optional.
  - `credentials_path`: optional, `repr=False`. Application Default Credentials (ADC) apply when it is unset.
  - `maximum_bytes_billed`: optional, a strict int ≥ 1, as a cost cap.
- Domain-scoped projects and digit-leading datasets are rejected (fail-closed); document this.
- The fingerprint is every field except `table`.

**Q2 `feat(pushdown): compile a BigQuery dialect`:**
- Add `SQLDialect.BIGQUERY` with a row in every table:
  - `_CAST_KEYWORDS`: INT64, FLOAT64, STRING, BOOL, DATE, and **DATETIME** for naive Datetime.
  - Quoting: backticks per segment.
  - `_LITERAL_ESCAPES`: `\`→`\\`, `'`→`\'`, newline→`\n` and CR→`\r`. A raw newline is a syntax error in BigQuery.
  - `_STRPTIME_DIRECTIVES`: Y/m/d/H/M/S/%%, and `z`→`%Ez`, with **no `f`**, so `%f` is refused.
  - `_FORMAT_LITERAL_QUOTES`: `""`.
  - `_INFINITY_LITERALS`: `CAST('inf' AS FLOAT64)`.
  - `_EDIT_DISTANCE_FUNCTIONS`: `EDIT_DISTANCE`, which counts code points.
  - The crosswalk sample hash from PR W: `FARM_FINGERPRINT`.
  - PR P's tables:
    - `_REGEX_REPLACE_FLAGS`: none, since BigQuery replaces every match;
    - `_WIDE_INTEGER_TYPES`: `NUMERIC`, whose 29 integer digits cover any INT64 difference.
- Structural branches, each named explicitly:
  - **Parse templates:** BigQuery puts the format first, as `SAFE.PARSE_DATETIME(fmt, v)`, or `SAFE.PARSE_TIMESTAMP` when a `z` directive was translated. `_translate_datetime_format` reports that.
  - **NaN-safe equality:** BigQuery's `NaN = NaN` is FALSE, so value equality is `(s = t OR (s IS NOT DISTINCT FROM t AND s IS NOT NULL))`, in `_compare` and `_numeric_predicate`.
  - An explicit CASE for `value_map`.
  - Null-safe `IS NOT DISTINCT FROM`.
- Fix the docstrings that stop being true:
  - "doubled to escape itself";
  - "NaN sorts above all numbers";
  - the vendor lists.
- Tests are string assertions only, because DuckDB can't parse backticks:
  - extend the reference lexer in `test_sql_compiler.py` with the BigQuery literal rules;
  - add BIGQUERY to `_BACKSLASH_ESCAPE_DIALECTS`;
  - update the exact dialect-set assertion (79-88);
  - the table-completeness tests pick up the new dialect automatically.

**Q3 `feat(connectors): add BigQueryConnector`** in `warehouse.py`:
- **Import lazily in `connect()`.** google-api-core raises a `FutureWarning` at import on Python 3.10 today, and on 3.11 from 2026-10-24. Under `filterwarnings=error` and `uv sync --all-extras`, a module-level probe would break `import veridelta`. Keep a module attribute `bigquery: Any = None` for mocks; the importer branch needs its own coverage.
- Build the client as `Client(project=..., location=...)`, or `Client.from_service_account_json(path, ...)` when a key file is given.
  - Never use `client_options={"credentials_file": ...}`, which triggers a DeprecationWarning.
- Set `QueryJobConfig(default_dataset=f"{project}.{dataset}", use_legacy_sql=False, maximum_bytes_billed=...)`.
- Fetch results with `client.query(sql, job_config=...).result().to_arrow_iterable()`, then `pl.from_arrow(batches)`.
  - `to_arrow()` emits a `PendingDeprecationWarning` since 3.44.
  - **An empty batch list raises `ConnectorError`**, because `pl.from_arrow([])` has no columns.
- Share the log format and error wrapping with `_run_arrow_query`, and make `close()` idempotent.
- Tests mirror `test_warehouse_execution.py` / `test_connectors.py` with a mocked driver.

**Q4 `feat(engine): route BigQuery pairs to pushdown`:**
- Update `_is_warehouse` and add the BigQuery fingerprint.
- Add a BigQuery entry to S3's `_WAREHOUSES` registry: its fingerprint and `BigQueryConnector`. That registry keeps C901 flat.
- Include BigQuery in the cross-dialect and Jaro-Winkler messages.
- Warehouse crosswalks work for BigQuery through PR W.

**Q5 build + docs:**
- A `bigquery` extra: `google-cloud-bigquery>=<the earliest release with to_arrow_iterable + query_and_wait; check>` plus `pyarrow>=14.0.1`, added to `all`.
- mypy `follow_imports=skip` for `google.cloud.bigquery.*`, `google.api_core.*`, `google.auth.*` and `google.oauth2.*`.
- `__all__` gets `BigQueryConfig`, sorted after `ArtifactFormat`.
- Update the docs-coverage model list and `400-docs.mdc`.
- `configuration.md`:
  - extras, routing and fingerprint fields;
  - the connection-field table;
  - YAML example;
  - credentials (ADC or a key file);
  - `TRIM` strips all whitespace in BigQuery, unlike Snowflake and Databricks;
  - REGEXP_REPLACE backslash rules;
  - `%f` refused;
  - GEOGRAPHY and JSON columns must be ignored;
  - implicit coercion errors become a `ConnectorError`.
- README/index lists and the mermaid diagram.
- Delete the roadmap BigQuery bullet.
- Update the `300-security.mdc` quote and escape lines.
- Regenerate the schema.

**Order:** after S (the session registry), P (the `_normalized_dtype` helper for `strict_types`) and W (the crosswalk hash table).

---

## Verification, beyond the per-push checks above
- **R:**
  - `veridelta --version` prints 0.10.0, and the built wheel and sdist METADATA say 0.10.0.
  - After the tag, the "Publish to PyPI" run succeeds and `pypi.org/pypi/veridelta/json` lists 0.10.0.
- **D:**
  - The real SQLite + connectorx integration suite runs on all 15 CI cells, Windows included.
  - A manual `veridelta run` pits a temporary SQLite database against a Parquet file with `password: ${VAR}`.
  - Force a failure with a bad table, and check that no form of the password appears in the traceback.
- **C:**
  - The `action` CI job runs `uses: ./` on ubuntu, macOS and Windows for match, drift and broken configs.
  - Locally, `veridelta run --markdown` output is checked by eye.
  - The GitLab template gets YAML and parser checks only.
- **S:**
  - `veridelta schema` matches the committed file.
  - The docs YAML examples validate under jsonschema and load.
  - `veridelta validate` exit codes are covered by e2e tests.
  - `--schemas` runs against a warehouse mock and executes only `schema` probes.
- **P:**
  - Every reproduced gap has a parity test that fails first.
  - The Hypothesis `ci` profile finishes in about 30 s, and one `deep` run happens locally before the push.
- **A:** round-trip tests, and a CLI run of Avro vs Parquet.
- **W:** DuckDB parity against local proposals at `sample_fraction=1.0`, and a manual `veridelta crosswalk` on a DuckDB-backed session in a scratch test.
- **Q:**
  - Lexer-checked BigQuery literals.
  - A mocked-client connector suite.
  - `python -W error -c "import veridelta"` on 3.10 with the extra installed must stay clean, which proves the import is lazy.
