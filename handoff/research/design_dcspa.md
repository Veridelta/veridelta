The plan for all five PRs is below. There is no Write or Edit tool in this session, so this message is the plan and no files were created. I ran checks in memory against the repo's `.venv` (Python 3.11, Polars 1.40.1, DuckDB 1.5.5, jsonschema 4.26) with `PYTHONDONTWRITEBYTECODE=1`. connectorx and Polars 1.39.3 are not installed anywhere locally, so those checks are listed as step 0 of D and P, with exact commands.

## 0. Where I would change your proposals

1. **`mariadb://` is not a connectorx scheme.** connectorx's `source_router.rs` matches postgres/postgresql, sqlite, mysql, mssql, oracle, bigquery, duckdb, trino and clickhouse. Python's `rewrite_conn` maps `redshift` to postgresql with the cursor protocol. MariaDB connects through `mysql://`, so drop `mariadb` from the quote table.
2. **`_optional_module` can't be used from a connector.** It lives in `engine.py:47-59`, and engine imports the connectors (`engine.py:22-24`), so importing it back would be circular. Copy the probe pattern from `warehouse.py:30-47` (try/except with `# pragma: no cover`).
3. **Polars leaves the raw password in the exception chain.** `polars/io/database/_utils.py:68-72` scrubs the text but re-raises with `raise type(err)(errmsg) from err`, so the unscrubbed connectorx error is still `__cause__` and prints in any traceback.
   - The connector must raise `ConnectorError(...) from None`.
   - Tests should check `"".join(traceback.format_exception(exc))`, not just `str(exc)`.
4. **The password/URI conflict belongs in a model validator**, not in the connector. It then fails at load time (and later in `validate`). Also require a user name in `uri` whenever `password` is set.
5. **The quote table can't be keyed by `SQLDialect`.** Rule 300 asks for that, but `SQLDialect` names pushdown dialects that must fill every table (casts, parse functions, and so on).
   - Key it by URI scheme in `sql.py` and add a written exception to `300-security.mdc`.
   - Make it a module-level function, because `SQLPushdownCompiler` is one instance per dialect.
6. **Windows SQLite URIs must be percent-encoded.** connectorx's own `ConnectionUrl` does `urllib.parse.quote(str(db_path))`. The Rust side (`get_arrow.rs`) slices `"sqlite://"` off the serialized URL and percent-decodes the rest. A raw `C:\...` can't parse, because `:` and `\` are not allowed in that part of a URL. So `"sqlite://" + quote(path)` is the portable form.
7. **Unsigned and small-integer tolerances hard-fail on DuckDB, not just disagree.** Reproduced through the harness:

   | Case (abs_tol = 1) | Local | DuckDB pushdown |
   | :--- | :--- | :--- |
   | UInt32 5 vs 3 | changed 1 (right) | `OutOfRange: Overflow in subtraction of UINT32` |
   | Int8 100 vs -100 | changed 0 (wrong) | overflow error |
   | Int64 -2⁶³ vs 2⁶³-1 | changed 0 (wrong) | `Overflow on abs(-9223372036854775808)` |

   Widening has to wrap the operands before *every* `ABS`, not only the subtraction. The local bug also breaks `src.abs()`: an Int8 -128 stays -128, so relative tolerances go wrong even without a subtraction overflow.
8. **The `%f` fix leaves a small remaining divergence.** Rewriting `.%f` to chrono's `%.f` is right for 1–6 fractional digits (".5" → 500000 µs, ".123" → 123000 µs; DuckDB agrees). But `%.f` also accepts a missing fraction, and 7–9 digits (truncated), where DuckDB and Python give NULL or an error. A bare `%f` must become `%6f` (exactly six digits, no warning). Document this, and make the fuzz generators emit 1–6 digits, never none.
9. **Avro reading has limits.**
   - `pl.read_avro(bytes)` raises `TypeError: Object does not have a .read() method` despite its annotation.
   - `s3://…` raises `FileNotFoundError`: local files only.
   - The writer can't emit Int8, Int16, any unsigned type, Datetime(ns), tz-aware datetimes, Time, Categorical or Duration.
   - The writer *panics* on a Null column (`PanicException` derives from `BaseException`, so the CLI's `except Exception` would not catch it).

   That is the evidence for keeping `avro` out of `ArtifactFormat`.
10. **The schema is about 29 KB at `indent=2`** (28,902 bytes measured), not 21 KB.
11. **Tests that will break unless updated:**
    - `test_engine.py:55` matches `"arrow, csv, excel"`; adding avro makes it `"arrow, avro, csv, excel"`.
    - The `default_args` fixture at `test_cli.py:25-28` needs `markdown=None`.
    - `test_engine.py:2231` matches `"file or lakehouse sources"`.
    - Four routing tests match `"Mixed file/lakehouse"` (`test_engine_routing.py:835, 846, 855, 1001`). They survive if the new wording keeps that prefix.
    - No test pins Pydantic's tag list, so adding `DatabaseConfig` to `SourceRef` is safe (grep confirmed).
12. **pre-commit's isolated mypy environment** (polars, pydantic, pyyaml only) needs overrides:
    - `connectorx`: in the `follow_imports = "skip"` group, `pyproject.toml:126-144`.
    - `hypothesis`: in the `ignore_missing_imports` group, `pyproject.toml:114-124`.
    - `jsonschema`: same group; 4.26.0 ships no `py.typed`.
13. **`_build_match_expr` is at 9 of the 10 C901 budget** (`ruff --select C901`), so the Int128 change must go into a helper. `_normalize_frame` is already at 10.
14. **The CI docs page needs `render_macros: false` front matter.** `mkdocs_macros/plugin.py:629-682` supports it, and `${{ secrets.X }}` can't be avoided on that page.
15. **One gap-table row can't reach parity in the harness.** For "datetime_format text vs Datetime('ns')", DuckDB's probe reports ns as us, so pushdown correctly sees no drift. Keep it out of parity assertions.
16. **Polars' own read-only guard is first-keyword only** (`_executor.py:43-56, 519-526`) and `read_database_uri` doesn't use it. Don't copy it; a read-only role is the control.

---

## D — Database sources (`type: database`, compared locally)

### Step 0: checks to run before writing code (blocked here)
```bash
uv run --no-project --python 3.12 --with polars==1.40.1 --with 'connectorx>=0.4.5' --with pyarrow python -W error - <<'PY'
import contextlib, os, sqlite3, tempfile, urllib.parse, polars as pl
d = tempfile.mkdtemp(); p = os.path.join(d, "x.db")
with contextlib.closing(sqlite3.connect(p)) as c, c:
    c.execute('CREATE TABLE t (i INTEGER, r REAL, s TEXT, d DATE, dt DATETIME, b BOOLEAN, n NUMERIC)')
    c.execute("INSERT INTO t VALUES (1, 1.5, 'a', '2024-01-02', '2024-01-02 03:04:05', 1, 1.25)")
    c.execute("INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    c.execute('CREATE TABLE "Mixed" (i INTEGER)')
uri = "sqlite://" + urllib.parse.quote(p)
print(pl.read_database_uri('SELECT * FROM t', uri).schema)              # declared-type mapping
print(pl.read_database_uri('SELECT * FROM "t" WHERE 1 = 0', uri).schema)  # zero-row probe keeps types?
print(pl.read_database_uri('SELECT * FROM "Mixed"', uri).schema)         # quoted, case preserved
print(pl.read_database_uri('SELECT * FROM t;', uri).height)              # trailing semicolon?
m = os.path.join(d, "missing.db")
try: pl.read_database_uri('SELECT * FROM t', "sqlite://" + urllib.parse.quote(m))
except Exception as e: print(type(e).__name__, e)
print("stray file created:", os.path.exists(m))   # I suspect rusqlite's default flags include CREATE; unconfirmed
PY
```
Pin the observed dtypes in the integration test. Windows is covered by those tests running on `windows-latest`.

### Design
- **`DatabaseConfig`** in `models.py`, after `IcebergConfig` (`:1005-1032`).
  - Config: `extra="forbid"`, `frozen=True`, `hide_input_in_errors=True`.
  - Fields:
    - `type: Literal["database"]`
    - `uri: str`
    - `password: str | None` with `repr=False`
    - `table: str | None` with `pattern=SQL_RELATION_PATTERN`
    - `query: str | None` with `min_length=1`
  - One `model_validator(mode="after")` enforces: exactly one of table or query; `uri` has a scheme; if `password` is set, the URI has a user and no password of its own. `urlsplit`'s own `ValueError` ("Invalid IPv6 URL") becomes a `ValidationError` and does not echo the input.
  - A `redacted_uri` property (a plain property, not a field) returns `scheme://user:***@host/...`, or the URI unchanged when it holds no password. It is used by `__repr_args__`, which follows the pattern at `models.py:148-166`, and by the connector's log lines.
  - It has to be a public name: pyright strict rejects cross-module use of `_private` helpers, and models can't import connectors.
- **`sql.py`**:
  - A new `_DATABASE_IDENTIFIER_QUOTES: Final[dict[str, tuple[str, str]]]`:
    - `"` for postgresql, postgres, redshift, sqlite, oracle
    - backtick for mysql, clickhouse
    - `[` `]` for mssql
  - A module-level `compile_database_select(scheme, table) -> str`:
    - Allowlists each segment with `SQL_IDENTIFIER_SEGMENT`, allows 1–3 segments, and raises `ConnectorError` on a bad segment (fails closed, same as `_quote_relation` at `:954-973`).
    - Returns `SELECT * FROM <quoted>`.
    - An unknown scheme raises `ConfigError`, with the list of known schemes derived from the table.
  - `urlsplit` already lowercases the scheme (verified: `MySQL://` → `mysql`).
- **`connectors/database.py`** (new): a `connectorx` probe copied from `warehouse.py:30-47`, a logger `veridelta.connectors.database` with a `NullHandler`, and `DatabaseConnector(VerideltaConnector)`.
  - `connect()` steps:
    1. If the probe is `None`, raise the install hint.
    2. Build the statement: the compiled table select, or `query` verbatim.
    3. Build the URI: splice in `quote(password, safe="")`. Verified: `p@ss:w/rd %+&?#` → `p%40ss%3Aw%2Frd%20%25%2B%26%3F%23`.
    4. Call `pl.read_database_uri` once.
    5. On `ImportError`, raise the install hint `from None`. This covers the pyarrow case, which connectorx raises as `ModuleNotFoundError`.
    6. On `Exception`, scrub the message: replace the raw password field, its percent-encoded form, and the URI password (both encoded and decoded), longest first. Then raise `ConnectorError(...) from None`.
    7. On success, `self._frame = frame.lazy()` and log at INFO.
  - `execute_pushdown` always raises. `fetch_schema`, `lazyframe` and `close` mirror `lakehouse.py:260-288`.
  - The class docstring states the eager read (rule 100): Polars has no lazy SQL reader, and `pl.defer` re-ran the query on every schema read.
  - **Recommended, pending step 0:** for `sqlite`, rebuild the URI as `"sqlite://" + quote(unquote(rest))` so users can write `sqlite://C:/data/x.db`. If step 0 confirms the stray-file behavior, also refuse a path that is not an existing file.
- **Engine**: `LoaderFactory.load` (`engine.py:388-415`) gets a branch `with DatabaseConnector(config) as database: database.connect(); return database.lazyframe()`. `_is_warehouse` is unchanged, so database pairs with file, lakehouse or database run locally and `crosswalk` works.

### Commits
- **D1 `build(deps): add the database extra`**
  - `pyproject.toml:37-62`: add `database = ["connectorx>=0.4.5", "pyarrow>=14.0.1"]`. The comment should say: no sdist, cp314 wheels start at 0.4.5, and Polars converts through pyarrow.
  - Add `"veridelta[database]"` to `all`.
  - Add `connectorx`, `connectorx.*` to the mypy override group at `:126-144`.
  - Run `uv lock`. No behavior change.
- **D2 `feat(models): add DatabaseConfig`**
  - Model, validator and `redacted_uri` as above. Not yet in `SourceRef`, so no half-wired YAML route.
  - Export from `__init__.py` (the `__all__` entry goes between `"DataIntegrityError"` at `:45` and `"DatabricksConfig"` at `:46`; Python sorts `DataI` < `Databa` < `Databr`) and from `config.py:28-35`.
  - Add it to `test_docs_coverage.py:129-137`.
  - Tests in `test_models.py`, new class `TestDatabaseConfig`:
    - `test_it_requires_exactly_one_of_table_or_query`
    - `test_it_rejects_a_password_in_both_places`
    - `test_it_requires_a_user_for_the_password`
    - `test_it_requires_a_scheme`
    - `test_it_rejects_a_table_outside_the_allowlist` (`orders; DROP TABLE x`)
    - frozen and extra-forbid checks
    - `test_it_masks_the_password_inside_a_printed_uri`: `repr`, `str` and `str(list(__repr_args__()))` lack the secret, while `model_dump()["uri"]` keeps it.
  - Add `database-uri` and `database-password` params to `test_it_keeps_credentials_out_of_validation_errors` (`:312-356`) and `test_it_keeps_credentials_out_of_printed_configs` (`:357-422`).
- **D3 `feat(connectors): compile database table reads in the SQL module`**
  - `_DATABASE_IDENTIFIER_QUOTES` and `compile_database_select` in `sql.py`, after `:204-214`.
  - Rule change in `300-security.mdc`: the scheme-keyed table exception; `query` is the user's own SQL sent verbatim, never concatenated or logged; the password goes into the URI netloc only, never into SQL; `uri` is not regex-checked (added to the "Do not apply SQL identifier regex" line).
  - Tests in `test_sql_compiler.py`:
    - Parametrized expected SQL per scheme, with 1, 2 and 3 segments.
    - `bigquery` and `duckdb` with `table` raise a `ConfigError` whose sorted scheme list is exact.
    - `"a;b"`, 4 segments and `""` raise `ConnectorError`.
- **D4 `feat(connectors): read database sources through ConnectorX`**
  - The new module; export `DatabaseConnector` from `connectors/__init__.py` (and update the docstring at `:4`); add a third connector family to the docstring at `base.py:52-66`.
  - Add `src/veridelta/connectors/database.py` to the coverage gate at `Makefile:19` and `ci.yml:106`.
  - Loader bullet in `100-engine-polars.mdc`.
  - Tests in `test_connectors.py`, new `TestDatabaseConnector`. Every read test patches `veridelta.connectors.database.connectorx` to a truthy sentinel **and** `veridelta.connectors.database.pl.read_database_uri`; otherwise a venv without the extra hits the install hint first.
    - `test_it_reads_a_table_through_one_select`
    - `test_it_sends_a_query_verbatim`
    - `test_it_percent_encodes_the_password_into_the_uri` (host, port, path and `?sslmode=` preserved)
    - `test_it_explains_a_missing_extra_before_connecting` (read not called)
    - `test_it_explains_missing_pyarrow_as_the_extra`
    - `test_it_refuses_a_table_for_an_unknown_scheme`
    - `test_it_keeps_passwords_out_of_read_failures`, parametrized over the raw, encoded and URI-embedded (`pa:ss`) password. It asserts:
      - the message names the table and the redacted URI;
      - `__cause__ is None` and `__suppress_context__ is True`;
      - the formatted traceback contains no form of the secret.
    - `test_it_logs_reads_without_secrets_or_sql` (caplog INFO and WARNING; query text absent)
    - lifecycle tests (before-connect, idempotent close, reconnect re-reads, context exit on exception, pushdown refusal)
    - SQLite normalization and missing-file tests, if adopted
- **D5 `feat(engine): compare database sources in the local engine`**
  - Add `DatabaseConfig` to `SourceRef` (`models.py:1035-1039`) and the `LoaderFactory` branch.
  - Messages:
    - `engine.py:1506` and `:1538` → `"Mixed file/lakehouse/database and warehouse backends are unsupported."`
    - `:1855-1859` → `"...so both sides must be file, lakehouse, or database sources. ..."`
  - Docstrings at `:340-346`, `:390-401`, `:1669`, `:1738-1746`, `:1781-1787`, `:1839-1840`.
  - Tests:
    - `test_engine_routing.py`:
      - `test_it_loads_a_database_config_through_its_connector_and_closes_it`
      - `test_it_raises_connector_error_for_mixed_database_and_warehouse_backends`, asserting "database" is in the message. It fails before this commit because the old text omits it.
    - `test_engine.py:2231`: update the regex.
    - `test_config.py`: `test_it_loads_a_database_block_with_an_environment_password`. It fails before because the discriminator rejects `database`.
    - New `tests/integration/test_database_sources.py`: real SQLite plus connectorx, no `importorskip`, so a broken lock fails loudly. Fixtures use `contextlib.closing(sqlite3.connect(...))`, ISO strings for dates, and URI `"sqlite://" + quote(str(path))`. Tests:
      - `test_it_reads_declared_sqlite_types` (dtypes from step 0)
      - `test_it_reads_the_same_rows_by_table_and_by_query`
      - `test_it_keeps_the_stored_case_of_a_quoted_table`
      - `test_it_compares_a_database_with_a_parquet_file` (`run_from_configs` counts and `column_mismatches`)
      - `test_it_proposes_value_maps_from_a_database_source`
      - `test_it_names_the_table_when_it_does_not_exist`
    - e2e `test_e2e_database_source_reads_its_uri_from_the_environment` (`uri: ${LEGACY_DB_URI}`, CSV target, expects exit 1).
  - Add the `database` row to the connection-fields table (`configuration.md:206-212`) here, so the route never ships undocumented.
- **D6 `docs: document database sources`**
  - `docs/configuration.md`:
    - Line 42: rename the heading to "Warehouse, lakehouse, and database sources". No inbound links (grep; notebooks link only `#command-line` and `#environment-variables`).
    - Line 46-52: add the extra.
    - Line 54: add `password` to the secrets advice.
    - Line 56: pairing rules.
    - New `### Database sources`:
      - Postgres example with `password: ${PGPASSWORD}`; a MySQL `query`; SQLite (`sqlite:///abs`, relative, Windows).
      - Which schemes accept `table`; quoting preserves case (write names as stored, like `:64`).
      - `query` is sent verbatim: use a read-only role; `${VAR}` expands inside it (`$${` for a literal).
      - The read is eager, so project and filter in `query`.
      - Column types come from connectorx's mapping; a NULL-only column with no declared type errors.
    - Line 216: add the printed-credentials note.
    - Lines 218-249: `password` is percent-encoded; a `${VAR}` inside `uri` is not.
    - Lines 251-260: the new logger.
  - `README.md:17-19, 26, 33-52` and `docs/index.md:12-14, 21, 26-45`: the Connectors bullet ("PostgreSQL, MySQL, SQL Server, Oracle, SQLite and more through ConnectorX"), the extras list, a mermaid `databases[Databases] --> loader` node, and "File, lakehouse, and database sources load through…".
  - Add `DatabaseConfig` to the model list in `400-docs.mdc`.

### Error wording
- `"A database source reads a 'table' or runs a 'query'; set exactly one."`
- `"'uri' needs a scheme such as postgresql:// or sqlite://."`
- `"Set the password in 'password' or inside 'uri', not both."`
- `"'password' needs a user name in 'uri', as in postgresql://analyst@db.internal/sales."`
- `"'table' needs a URI scheme Veridelta knows how to quote; '{scheme}' is not one ({known}). Write the statement in 'query' instead."`
- `"Database extra is not installed. Install it with: uv add 'veridelta[database]'"`
- `"Database read of table 'public.orders' from 'postgresql://analyst:***@db:5432/sales' failed: <scrubbed>"`. For a query source: `"...of the configured query..."`.
- Logs: INFO `"Read %d rows of %s from %s in %.3fs"`; WARNING `"Database read of %s from %s failed after %.3fs"`.

### Risks and decisions
- **`[all]` portability.** connectorx has no sdist and per-version wheels (cp310–cp314), with no musllinux, win-arm64 or future cp315 wheels. `pip install veridelta[all]` will fail on those platforms. Recommendation: keep it in `all` as you decided, and document supported platforms.
- **Credentials in URI query parameters** (`?password=`) are not masked. Recommendation: document it and point users to the `password` field.
- **Self-comparison refusal for database pairs.** Recommendation: don't add it, since local sources never had one.
- **connectorx's SQL parser** may reject some vendor syntax. Record the step 0 findings in the docs.

---

## C — CI integrations

- **C1 `feat(report): render comparison summaries as Markdown`**
  - `report.py`: `render_markdown(result) -> str` and `write_markdown(result, path) -> Path` (creates parent directories, like `:312-329`); update `__all__` at `:332` and the module docstring.
  - Content: `### Veridelta: PASSED|FAILED` (plus "(Perfect Match)"), a metrics table (match rate, source/target rows, added, removed, changed), a top-drift table limited to `summary.report_limit` or "No column-level drift.", and a keys-only note when `result.keys_only`.
  - Column names go in code spans with a fence one backtick longer than any backtick run in the name; `|` becomes `\|` and CR/LF becomes a space. A column name can then never open an HTML comment or spoof the sticky marker.
  - `cli.py:363-395`: `--markdown PATH`. In `run()` after the HTML step (`:214-216`), call `write_markdown` and print progress "Markdown summary saved to: …". Stdout is unchanged.
  - Tests:
    - `test_report.py`: verdicts, perfect match, the report limit, the keys-only note, and `test_it_keeps_column_names_from_breaking_the_markdown` (names containing `|`, a backtick, `<!--`, `__x__`, a newline).
    - `test_cli.py`: add `markdown=None` to the `default_args` fixture (`:25-28`); flag parsing; file written; progress line only on stderr.
  - Docs: `configuration.md:122-132`; the CI/CD bullet at `README.md:17` and `docs/index.md:12`.
- **C2 `chore(ci): publish to PyPI only for release tags`**: change the `release.yml:6` filter to `"v[0-9]+.[0-9]+.[0-9]+"`, so a floating tag can't publish.
- **C3 `feat(ci): add a composite GitHub Action`** (`action.yml` at the repo root)
  - Inputs: `config`, `working-directory`, `extras`, `version` (empty means install from `$GITHUB_ACTION_PATH`), `python-version`, `html-max-rows`, `fail-on-mismatch`, `comment`, `github-token` (default `${{ github.token }}`), `artifact-name`, `upload-artifact`.
  - Outputs: `status` (match / drift / error), `is-match`, `exit-code`, and the paths of the JSON, Markdown and HTML files.
  - Steps, all `shell: bash`, with inputs passed **only through `env:`** to prevent script injection:
    1. `astral-sh/setup-uv` (the repo uses v7).
    2. `uvx --from "$SPEC" veridelta run -c … --json --html … --markdown … > summary.json` with `set +e`.
       - `status`: exit 0 is match; exit 1 with `"is_match": false` in summary.json (fixed by `model_dump_json(indent=2)`) is drift; anything else is error.
       - Append to `$GITHUB_STEP_SUMMARY`, or a failure note that includes the run URL.
    3. `actions/upload-artifact`, node24 from v6 on (v7 is current), with `if: always()` and `if-no-files-found: ignore`.
    4. `actions/github-script` v8, node24 (v9 exists; its only break is `require('@actions/github')`), on `pull_request` events.
       - Maintains one sticky comment, found by the marker `<!-- veridelta:${encodeURIComponent(config)} -->` (update or create).
       - A 403 or 404 (forks) becomes `core.warning`.
    5. A final gate: an error always fails; drift fails only when `fail-on-mismatch` is set.
  - Recommendation: SHA-pin the third-party actions inside `action.yml`, with a version comment, because consumers can't override them.
  - Risk: artifact names collide across matrix jobs. Derive a default from the sanitized config path plus `$GITHUB_JOB`, and document overriding it.
  - Fixtures in `tests/fixtures/ci/`: `match.yaml`, `drift.yaml`, `broken.yaml` and CSVs.
  - New `ci.yml` job `action`, `needs: test-smoke`, on ubuntu, macos and windows:
    - run `uses: ./` against the three fixtures with `comment: false`;
    - the broken case uses `continue-on-error`;
    - assert `is-match`, `status` and `exit-code`, and check `steps.broken.outcome == 'failure'`.
  - `tests/unit/test_ci_integrations.py`:
    - every `${{ inputs.X }}` is declared;
    - no `${{ inputs.` appears inside any `run:` block;
    - every run step has a `shell`;
    - outputs reference existing step ids;
    - the `veridelta run …` line (split with `shlex`, variables replaced by placeholders) parses with `build_parser()`. This fails when a flag is renamed.
- **C4 `feat(ci): add a GitLab CI template`** (`ci/gitlab/veridelta.yml`)
  - Multi-document YAML: a `spec: inputs:` header (config, version, extras, stage, job-name, image, html-max-rows, `fail-on-mismatch` and `comment` as booleans), then `"$[[ inputs.job-name ]]":`.
  - Image: `ghcr.io/astral-sh/uv:python3.12-bookworm`. Use the non-slim variant: slim has no curl, but the note is posted with Python's standard library anyway.
  - The script runs `uvx --from "veridelta[…]==$[[ inputs.version ]]" veridelta run …`.
  - Merge-request note (sticky marker, update or create) through `$CI_API_V4_URL/projects/$CI_PROJECT_ID/merge_requests/$CI_MERGE_REQUEST_IID/notes`, only when both `VERIDELTA_GITLAB_TOKEN` and `CI_MERGE_REQUEST_IID` are set. `CI_JOB_TOKEN` can't post notes.
  - Artifacts: `when: always`, `expose_as: "Veridelta report"`, `paths: [veridelta-report/]`.
  - Add `"ci/gitlab/veridelta.yml:veridelta-version"` to commitizen `version_files`, so `cz bump` pins the template's default version in each release tag.
  - Tests: `yaml.safe_load_all`; every `$[[ inputs.X ]]` is declared; the CLI line parses.
- **C5 `docs: document the CI integrations`**
  - New `docs/ci.md` with `render_macros: false` front matter. Cover: pin `@vX.Y.Z` or a SHA; `permissions: pull-requests: write`; forks; passing secrets to `${VAR}`; `include: remote` + `inputs` for GitLab.
  - Add it to the `mkdocs.yml` nav; add a README section.
  - Delete the "Native CI/CD Runners" bullet from `roadmap.md` §3 and keep Telemetry.
  - Add the canonical page to `400-docs.mdc`.

---

## S — JSON Schema and `veridelta validate`

- **S1 `feat(config): generate a JSON Schema for configuration files`**
  - `config.py`: a private `_RootConfig(DiffConfig)` with `source` and `target: SourceRef`, and a public `config_json_schema()` that adds `$schema`, `$id` (the published URL) and `title`. Post-processing:
    - Add `type` to `required` for every non-file branch.
    - Wrap each source-branch string that has a `pattern` or `enum` (`table`, `format`) in `anyOf: [original, {"type":"string","pattern":"\\$\\{"}]`.
  - Prototype verified with `Draft202012Validator`:
    - The raw schema accepted a Snowflake block without `type` and rejected `table: ${T}`.
    - The patched schema gets both right, and still rejects root extras, rule typos and `format: xls`.
    - A known, documented difference remains: `threshold: "0.1"` is rejected by the schema but coerced by the loader.
  - Add `jsonschema>=4.26.0` to the dev group, plus the mypy override.
  - Tests in `test_config_schema.py`: `check_schema`; the root requires `primary_keys`, `source` and `target`; the parity pairs above (schema verdict equals loader verdict); every complete YAML example in `configuration.md` and `README.md` validates **and** loads (with placeholder env values).
- **S2 `feat(cli): print the schema with veridelta schema`**
  - New subcommand and dispatch (`cli.py:456-459`).
  - Commit `docs/schema/veridelta.schema.json`; mkdocs publishes it as a static file.
  - `make schema` (add to `.PHONY`).
  - Drift test: `json.loads(file) == config_json_schema()`, with a failure message that names `make schema`.
  - Docs: a new "Editor support" section with the `# yaml-language-server: $schema=<url>` modeline (a `$schema:` key is rejected by `extra=forbid`), the tag-pinned raw GitHub URL, and `veridelta schema > …`.
- **S3 `refactor(engine): check backend pairing before any pushdown session`**: extract `_check_backend_pairing(source, target)` from `engine.py:1503-1531`, including `_reject_self_comparison`. This also lowers `_run_warehouse_pushdown`'s complexity (7). Existing routing tests cover it.
- **S4 `refactor(engine): separate run()'s schema-only plan from its row work`**
  - `DiffEngine._plan()` covers `engine.py:2406-2414` (align, validate, normalize, build match expressions); `run()` calls it.
  - Decision: add a public `DiffEngine.validate_rules(config, source, target)` next to `validate_schemas` (`:1804-1822`) and document it under "Schema dry run". My recommendation is public.
- **S5 `feat(cli): validate configurations offline`**
  - `veridelta validate -c PATH [--allow-missing-env] [--json] [-q]`.
  - Findings (errors and warnings) go to stdout. Exit 0 means valid (warnings allowed), 1 invalid, 2 bad arguments.
  - Checks:
    1. A private `config._load_config(path, *, allow_missing_env)`. An unset `${NAME}` expands to the literal `NAME`, which is itself a valid identifier, so `table: ${SNOWFLAKE_TABLE}` still passes the pattern. The substituted names are listed on stderr; malformed references still fail.
    2. `_check_backend_pairing`.
    3. Missing extras for the routes the config will take:
       - excel: the `engine.fastexcel` probe
       - delta: `find_spec("deltalake")`
       - iceberg: `find_spec("pyiceberg")`
       - database: the `database.connectorx` probe
       - warehouses: the `warehouse.py` probes
       - fuzzy rules on local routes: the `rapidfuzz` probe
    4. `regex_replace` patterns run through `pl.select(pl.lit("x").str.replace_all(p, r))`: an error for local routes, a warning for pushdown, because Databricks' Java regex accepts lookarounds that Polars rejects.
    5. For warehouse pairs, type-dependent refusals are **warnings**: untranslatable `datetime_format` (checked by compiling a probe `DiffRule` through `compile_column_predicate`), `min_jaro_winkler_similarity`, and `normalize_column_names`.
    6. A database `table` with an unknown scheme is an error (`compile_database_select`).
  - Keep the orchestration as private helpers in `engine.py` (gated) and the formatting in `cli.py`.
- **S6 `feat(cli): check live schemas with validate --schemas`**
  - Files and lakehouse tables: `LoaderFactory.load(...)`, then `validate_rules`.
  - Database `table` sources: `compile_database_probe(scheme, table)` in `sql.py` (`… WHERE 1 = 0`) and `DatabaseConnector.probe_schema()`.
  - Database `query` sources: a warning that they are skipped. Wrapping them is not portable: Oracle rejects `AS` on a derived table, and SQL Server rejects `ORDER BY` inside one.
  - Warehouses: `_validate_pushdown_schema` (two probes), `_resolve_pushdown_keys` and `_resolve_pushdown_rules`, and compile every statement without running it. Tests assert that only `query_type="schema"` is executed.
- **S7 `docs`**: CLI sections in `configuration.md`, and rewording the `roadmap.md` §2 VS Code bullet to drop the schema-validation claim.
- Coverage: new code in `engine.py`, `sql.py` and `database.py` is gated. `config.py` and `cli.py` are not, but get unit tests plus e2e exit-code tests.

---

## P — Parity bugs, strict_types in pushdown, Hypothesis

**Step 0** (blocked here):
```bash
uv run --no-project --with polars==1.39.3 python -W error -c "import polars as pl; df=pl.DataFrame({'s':pl.Series([100,-128],dtype=pl.Int8),'t':pl.Series([-100,127],dtype=pl.Int8)}); print(df.select((pl.col('t').cast(pl.Int128)-pl.col('s').cast(pl.Int128)).abs().alias('d'), pl.col('s').cast(pl.Int128).abs()*0.5 <= 64.0)); print(pl.Series(['x.5']).str.strptime(pl.Datetime,'x%.f',strict=False).dt.microsecond())"
```
Snowflake `FF6` and Databricks `SSSSSS` acceptance of 1–5 digits is vendor behavior; pin it by string assertion only.

- **P1 `fix(engine): keep integer tolerances from wrapping around`**
  - Extract `_tolerance_match(src, tgt, rule, dtype, tgt_dtype)` from `engine.py:2285-2296`. When both normalized sides are integers, cast both to `pl.Int128`.
  - Verified on 1.40.1: Int8 difference 200; UInt64 0 vs max; Int64 extremes; `abs(-128)` becomes 128.
  - Tests (`test_engine.py`), with what they show before the fix:
    - Int8 100 vs -100 at abs 1 → changed 1 (was 0).
    - Int64 -2⁶³ vs 2⁶³-1 → changed 1 (was 0).
    - Int8 -128 vs -127 at rel 0.5 → match (was a mismatch, because `abs` wraps).
    - UInt64 0 vs max; Int8 vs Int64.
- **P2 `fix(engine): read %f as a fraction of a second`**
  - Add `_polars_datetime_format(fmt)`: a regex tokenizer over `%%|\.%f|%f` with the mapping `%%`→`%%`, `.%f`→`%.f`, `%f`→`%6f` (verified: `%%f` is left alone, `%.f` passes through). Use it at `engine.py:2126`.
  - Docs at `configuration.md:406`: semantics and the remaining divergence.
  - Tests:
    - mapping cases;
    - local ".5", ".123" and ".123456" parse to 500000, 123000 and 123456 µs. Before the fix a `ChronoFormatWarning` is raised as an error, and the values would be 0, 0 and 123 µs;
    - a parity test `test_it_agrees_on_fractional_seconds`.
- **P3 `fix(pushdown): replace every regex match on DuckDB`**
  - `_REGEX_REPLACE_FLAGS: dict[SQLDialect, tuple[str, ...]]` = SNOWFLAKE `()`, DATABRICKS `()`, DUCKDB `("'g'",)`, used in `sql.py:1089-1091`.
  - Tests: a parity test with `1-800-555` and `{"-": ""}` (before the fix: local 0, pushdown 1, reproduced); compiler assertions per dialect. The existing `:215` count assertion still holds.
- **P4 `refactor(engine): predict normalized dtypes with one helper`**
  - `_normalized_dtype(dtype, *, pad_zeros, datetime_format, timezone, cast_to)`. Rules, each measured:
    - stages 1–4 keep the dtype;
    - `pad_zeros` gives String (even from Null);
    - a format on text, or after padding, gives `Datetime("us")`, or `Datetime("us","UTC")` when the format contains a `%z` token;
    - `timezone` gives `Datetime(input unit, zone)`;
    - `cast_to` gives `_CAST_TARGETS` (`:979-986`).
  - Re-express `_compares_numerically` and `_compares_as_text` (`:695-743`) through it. A `None` dtype is treated as text, like `_is_text_side`; that difference is unreachable, because `_resolve_pushdown_rules` always passes probed dtypes.
  - Tests: extend the oracle suite at `test_engine.py:1362-1540` with `_normalized_dtype(...) == compared` over `_NORMALIZER_CASES`, plus cases for `%z`, a tz conversion on ns/ms, tz then cast, and pad on Boolean.
- **P5 `fix(pushdown): widen integer operands in tolerance predicates`**
  - `_WIDE_INTEGER_TYPES`: SNOWFLAKE `NUMBER(38, 0)`, DATABRICKS `DECIMAL(38, 0)`, DUCKDB `DECIMAL(38, 0)` (verified that DuckDB DECIMAL(38,0) handles the UBIGINT difference and `ABS(-128)`).
  - `compile_query` and `compile_column_mismatch_query` take `wide_integers: frozenset[str] = frozenset()`. `_numeric_predicate` (`:1373-1395`) wraps both operands before every `ABS`.
  - The engine computes the set from `_normalized_dtype` for both sides and passes it from `engine.py:1383-1421`.
  - Tests: parity for UInt32 5/3, Int8 100/-100 and Int64 extremes (all three raise DuckDB `OutOfRange` before the fix); compiler strings per dialect.
- **P6 `feat(pushdown): enforce strict_types in warehouse comparisons`**
  - Add `type_drift: frozenset[str]` as a keyword argument, not a `DiffRule` field.
  - `_compare` (`:1446-1473`) emits `FALSE`, or `(src IS NULL AND tgt IS NULL)` under `treat_null`. This also avoids cross-type cast errors.
  - The engine computes `_strict_type_drift(...)` when `diff.strict_types`.
  - Tests:
    - parity for Float64 vs Int64 and Int64 vs String (reproduced 1 vs 0), Decimal(10,2) vs (12,4), String "abc" vs Int64 (which errors on DuckDB before the fix), and both NULL with treat_null;
    - compiler strings;
    - the ns row is out of parity scope (harness limitation).
  - Docs: replace `configuration.md:69`; clarify `:269`. Strict mode compares the types each side reports after normalization, which for a warehouse means its driver's types; for example Snowflake NUMBER(38,0) arrives as Decimal.
- **P7 `test(parity): fuzz local and pushdown verdicts with Hypothesis`**
  - Setup: `hypothesis` in the dev group; `.hypothesis/` in `.gitignore`; a `property` marker in pyproject; the mypy override; a note in rule 200 and `configuration.md:71`.
  - Strategies, in `tests/integration/parity_strategies.py`:
    - 1–4 lowercase `[a-z_][a-z0-9_]*` columns;
    - kinds: Int8/32/64, UInt32, Float64 (with NaN, ±inf, -0.0), Decimal(10,2), Boolean, ASCII text (`[A-Za-z0-9 _$.-]`), Date, naive `Datetime("us")`, UTC datetimes, and text datetimes with 1–6-digit fractions;
    - 0–12 rows with a unique Int64 key (optionally composite, occasionally null);
    - the target derives from the source by dropping rows, adding keys, mutating cells per kind (including tolerance-boundary deltas) and flipping nulls;
    - sibling dtypes only under `strict_types`.
  - Rules come from a kind-safe menu: tolerances, typed sentinels, safe regexes (`[^0-9]`, `-`, `\$`), whitespace and case, value maps drawn from existing values, pad on ints, canonical casts, Levenshtein 1–2, formats, `timezone` on UTC data, and treat_null.
  - Excluded:
    - tabs and Unicode whitespace;
    - non-ASCII text;
    - `\d`, `\w` and `İ`;
    - float-to-text conversions;
    - lenient casts;
    - Categorical, Enum, non-UTC timezones, ns, Null and Duration dtypes.
  - Property: `_outcome(run_local) == _outcome(run_pushdown)`, comparing summary counts, `column_mismatches`, `compared_columns`, or the exception class. A `ConnectorError` is always a failure.
  - Settings profile `ci` (the default): `derandomize=True`, `database=None`, `deadline=None`, about 40–60 examples, health checks `too_slow` and `data_too_large` suppressed. A `deep` profile is selected through an environment variable. The suite runs in all test-core jobs; keep the total under about 30 s.
  - Policy for anything the fuzzer finds: fix it in scope with an `@example` and a deterministic regression test, or constrain the generator, document the divergence and pin it with a test. New branches rely on deterministic tests for the 100% gate, never on Hypothesis.

---

## A — Avro

- **A1 `feat(engine): read Avro files`**
  - `AvroLoader` in `engine.py`, near `:260-299`, calling `pl.read_avro(config.path, **config.options)` then `.lazy()`. Its docstring gives the reason it is eager (no `scan_avro`).
  - Register it at `:358-365`; update the `BaseLoader` docstring at `:180-182` to three eager loaders.
  - Add `"avro"` to `SourceType` (`models.py:34-41`) with a docstring note that it is implemented this time.
  - Regenerate `docs/schema/veridelta.schema.json`; the S drift test enforces this.
  - Tests:
    - add `("avro", write_avro)` to the round-trip parametrization;
    - dtype and null round-trips for the dtypes the writer supports (Int32/64, Float32/64, Boolean, String, Date, Datetime us/ms, Decimal, List, Binary);
    - `columns` (names and indices) and `n_rows` pass through;
    - an empty frame keeps its schema;
    - an Avro vs Parquet `run_from_configs`;
    - update the `:55` regex to `"arrow, avro, csv, excel"`. The registry-binding test at `:59-65` keeps passing.
  - Docs: `configuration.md:28` and `:32` (eager; local paths only; no globs or object stores); the loader bullet in rule 100. `ArtifactFormat` stays unchanged (see correction 9).

---

## Cross-PR dependencies
- **D → C:** the action's `extras` examples and the secrets guidance (`PGPASSWORD`) assume the database extra exists.
- **D → S:** the schema includes `DatabaseConfig` from the start. `validate` uses the `connectorx` probe and `compile_database_select`, and S adds `compile_database_probe` and `DatabaseConnector.probe_schema`.
- **C1 → C3/C4:** both CI integrations call `--markdown`. The commitizen `version_files` entry changes what R's `cz bump` edits.
- **S → A, W, Q:** every model or literal change regenerates the schema file (`make schema`; the drift test enforces it).
- **S → P2:** if `validate` test-parses formats with Polars, P2 must route that check through `_polars_datetime_format`.
- **P → Q:** `_normalized_dtype`, the `type_drift` and `wide_integers` keyword arguments, and the `_REGEX_REPLACE_FLAGS` and `_WIDE_INTEGER_TYPES` tables need BIGQUERY entries; `SQLDialect`-keyed tables fail loudly otherwise. Q should reuse the P4 oracle and the P7 strategy menu.
- **All PRs:** leave `CHANGELOG.md` untouched, and keep every new branch in the 100% gate files covered by deterministic tests.

### Critical Files for Implementation
- /home/user/veridelta/src/veridelta/engine.py
- /home/user/veridelta/src/veridelta/models.py
- /home/user/veridelta/src/veridelta/connectors/sql.py
- /home/user/veridelta/src/veridelta/cli.py
- /home/user/veridelta/docs/configuration.md