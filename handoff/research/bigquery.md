I have everything needed. Nothing was installed and the repo was not modified.

# BigQuery pushdown: research report (sections 1–6)

## How this was verified, and the limits

- **No install happened.** Plan mode forbids system changes, so I did not build the scratch venv. The scratch dir `/tmp/claude-0/-home-user-veridelta/2e6278ba-cc98-589b-aa51-30180cd997d5/scratchpad/bq/` was created before plan mode started and is still empty.
  - Client facts come from streaming PyPI wheels into memory (nothing written to disk), reading their source, and running the stdlib-only parts.
  - Engine-side checks ran with the repo's existing `.venv` (Python 3.11, polars 1.40.1, pyarrow 25.0.1, duckdb 1.5.5) using `python -B`, from `/tmp`.
- **BigQuery's own docs were unreachable.** `docs.cloud.google.com:443` is denied by the egress policy (proxy status shows `connect_rejected`, 403), for both curl and WebFetch, and `cloud.google.com` 301-redirects there. I did not work around the block via mirrors of that site.
- **Source tags used below:**
  - **[GSQL]**: the open-source GoogleSQL reference at `raw.githubusercontent.com/google/zetasql/master/docs/<file>.md:<line>`, plus the GoogleSQL reference-implementation source (`google/googlesql`). BigQuery's GoogleSQL docs derive from these, but product-specific details can differ.
  - **[API]**: the BigQuery v2 and Resource Manager v1 REST discovery documents. These are authoritative for BigQuery product rules.
  - **[src]**: code inside the google-cloud-bigquery 3.45.2 wheel (or the named dependency wheel), cited as `file:line`.
  - **[ran]**: executed here.
  - **[search]**: a web-search summary of a BigQuery docs page (weakest evidence).
  - **[unverified]**: could not confirm.
- **Path legend** (all absolute, under `/home/user/veridelta/`):
  - `sql.py` = `src/veridelta/connectors/sql.py`
  - `warehouse.py` = `src/veridelta/connectors/warehouse.py`
  - `engine.py` = `src/veridelta/engine.py`
  - `models.py` = `src/veridelta/models.py`

---

## 1. Every dialect-specific place in sql.py

### 1.1 The module tables

Entries are exactly as in `sql.py`; I reformatted to one line per dialect.

```python
class SQLDialect(str, Enum):            # sql.py:49-59
    SNOWFLAKE = "snowflake"; DATABRICKS = "databricks"; DUCKDB = "duckdb"

_CAST_KEYWORDS = {                      # sql.py:62-93
  SNOWFLAKE:  {"Int64":"BIGINT","Float64":"FLOAT", "String":"VARCHAR","Boolean":"BOOLEAN","Date":"DATE","Datetime":"TIMESTAMP_NTZ"},
  DATABRICKS: {"Int64":"BIGINT","Float64":"DOUBLE","String":"STRING", "Boolean":"BOOLEAN","Date":"DATE","Datetime":"TIMESTAMP"},
  DUCKDB:     {"Int64":"BIGINT","Float64":"DOUBLE","String":"VARCHAR","Boolean":"BOOLEAN","Date":"DATE","Datetime":"TIMESTAMP"}}
_IDENTIFIER_QUOTES = {SNOWFLAKE: '"', DATABRICKS: "`", DUCKDB: '"'}          # sql.py:95-100
_LITERAL_ESCAPES = {SNOWFLAKE: (("\\","\\\\"),("'","''")),                   # sql.py:102-118
                    DATABRICKS: (("\\","\\\\"),("'","\\'")),
                    DUCKDB: (("'","''"),)}
_STRPTIME_DIRECTIVES = {                                                    # sql.py:120-162
  SNOWFLAKE:  {"Y":"YYYY","m":"MM","d":"DD","H":"HH24","M":"MI","S":"SS","f":"FF6","z":"TZHTZM","%":'"%"'},
  DATABRICKS: {"Y":"yyyy","m":"MM","d":"dd","H":"HH","M":"mm","S":"ss","f":"SSSSSS","z":"XX","%":"'%'"},
  DUCKDB:     {"Y":"%Y","m":"%m","d":"%d","H":"%H","M":"%M","S":"%S","f":"%f","z":"%z","%":"%%"}}
_FORMAT_LITERAL_QUOTES = {SNOWFLAKE: '"', DATABRICKS: "'", DUCKDB: ""}         # sql.py:164-172
_FORMAT_LITERALS = frozenset(" -/:.,_T")                                     # sql.py:174-181 (dialect-neutral)
_PARSE_FUNCTIONS = {SNOWFLAKE: "TRY_TO_TIMESTAMP", DATABRICKS: "try_to_timestamp", DUCKDB: "try_strptime"}  # sql.py:183-192
_INFINITY_LITERALS = {SNOWFLAKE: "'inf'::FLOAT", DATABRICKS: "CAST('Infinity' AS DOUBLE)", DUCKDB: "'inf'::DOUBLE"}  # sql.py:194-202
_EDIT_DISTANCE_FUNCTIONS = {SNOWFLAKE: "EDITDISTANCE", DATABRICKS: "levenshtein", DUCKDB: "levenshtein"}  # sql.py:204-214
```

**Proposed BIGQUERY entries, with evidence:**

1. **`_CAST_KEYWORDS`** → `{"Int64":"INT64","Float64":"FLOAT64","String":"STRING","Boolean":"BOOL","Date":"DATE","Datetime":"DATETIME"}`
   - Type names come from BigQuery's own GoogleSQL type enum [src] `enums.py:266-295`.
   - `Datetime` must map to **DATETIME**, not TIMESTAMP:
     - The local target is `pl.Datetime()`, which is naive microseconds (`engine.py:979-986`).
     - The client maps DATETIME to naive `timestamp('us')` and TIMESTAMP to `timestamp('us', tz='UTC')` [src] `_pyarrow_helpers.py:40-41, 58-59`.
     - Polars casting an aware value to `pl.Datetime()` keeps the UTC wall clock [ran], which matches `CAST(ts AS DATETIME)` under BigQuery's default zone of UTC [search].
   - FLOAT64→INT64 "round[s] away from zero" [GSQL] `conversion_functions.md:1373-1374`. The existing truncation guard at `sql.py:1315-1320` is therefore still required, and `CEIL`/`FLOOR` exist.
   - There is no implicit DATETIME↔TIMESTAMP or STRING→anything coercion [GSQL] `conversion_rules.md` cast/coerce table (21-388). Mixed-type comparisons error instead of coercing.

2. **`_IDENTIFIER_QUOTES`** → `` "`" ``
   - Quoted identifiers use backticks and "have the same escape sequences as string literals" [GSQL] `lexical.md:26-38`. An embedded backtick is written `` \` ``, not doubled.
     - So the docstring "doubled to escape itself" (`sql.py:100`) is false for BigQuery.
     - This is unreachable in practice, because `_quote_ident` allowlists before quoting (`sql.py:949-950`).
   - Per-segment quoting (`` `ds`.`t` `` from `_quote_relation`, `sql.py:954-973`) works. dbt-bigquery renders every relation this way:
     - `quote_character = "`"` (dbt-bigquery `relation.py:31`).
     - `render()` joins the individually quoted parts with `.` (dbt-adapters `base/relation.py:268-270, 319-323`).
   - Whole-path quoting (`` `p.d.t` ``) is also accepted [search].
   - GCP project IDs contain hyphens: "6 to 30 lowercase letters, digits, or hyphens… start with a letter. Trailing hyphens are prohibited." [API] `Project.projectId`.
     - `SQL_RELATION_PATTERN` (`models.py:31`) therefore rejects `my-project.analytics.events` and `bigquery-public-data.samples.shakespeare` [ran].
   - BigQuery naming rules [API]:
     - Dataset IDs: letters/digits/underscore, and may start with a digit (`DatasetReference.datasetId`).
     - Table IDs: may contain dashes and spaces (`TableReference.tableId`).
     - Column names: letters/digits/underscore, starting with a letter or underscore (`TableFieldSchema.name`). This is exactly `SQL_IDENTIFIER_SEGMENT`.
   - Column names are case-insensitive [GSQL] `lexical.md` case table (1197-1320).

3. **`_LITERAL_ESCAPES`** → `(("\\","\\\\"), ("'","\\'"), ("\n","\\n"), ("\r","\\r"))`, applied in that order.
   - Backslashes always start escapes, and "Any sequence not in this table produces an error" [GSQL] `lexical.md:394-396, 503-505`. An unescaped `\d` is a syntax *error* in BigQuery, not a silent `d`.
   - "Quoted strings can't contain newlines, even when preceded by a backslash" (`lexical.md:396`). Newline and CR must therefore become the `\n` / `\r` escapes.
   - `\'` is the quote escape (`lexical.md:556-557`).
   - Adjacent literals concatenate only when separated by whitespace or comments (`lexical.md:429-453`), so a doubled `''` is not an escape.
   - Test impact:
     - Add BIGQUERY to `_BACKSLASH_ESCAPE_DIALECTS` (`/home/user/veridelta/tests/unit/test_sql_compiler.py:19`).
     - Give `_read_literal` (`test_sql_compiler.py:23-56`) a BigQuery branch: `''` with no separator is an error, `\n` and `\r` decode, and a raw newline is an error.

4. **`_STRPTIME_DIRECTIVES`** → `{"Y":"%Y","m":"%m","d":"%d","H":"%H","M":"%M","S":"%S","z":"%Ez","%":"%%"}`, **with no `f` entry**.
   - **No `%f`:** there is no `%f` element; fractions are `%E*S` or `%E<n>S`, and `%z`/`%Ez` are TIMESTAMP-only [GSQL] `format-elements.md` table (9-608).
   - **`%Ez` instead of `%z`:**
     - GoogleSQL's `%z` parses `+HH[MM]` with no colon. `%Ez` accepts an optional colon and `Z` [GSQL src] `googlesql/public/functions/parse_date_time.cc:85-104, 1188-1242`.
     - Polars' `%z` accepts `+0530` and `+05:30` but rejects `Z` [ran]. `%Ez` is the closer superset; it differs only on `Z` and a bare `+HH`.
   - **Why omit `%f` rather than map `%S.%f` → `%E*S`:**
     - Polars reads `%f` as chrono *nanoseconds* and emits `ChronoFormatWarning`: `.123456` parses as 123 µs [ran].
     - So `%f` has no parity with *any* warehouse today, since Snowflake FF6, Databricks SSSSSS and DuckDB `%f` all read microseconds. That is a pre-existing gap.
     - Mapping `%S.%f` → `%E*S` would parse 5.123456 s, which disagrees with local.
     - `/home/user/veridelta/docs/configuration.md:406` currently lists `%f` as supported.

5. **`_FORMAT_LITERAL_QUOTES`** → `""`
   - Literal characters in a format string are taken as-is and `%` is the only escape; `_FORMAT_LITERALS` contains no `%`.
   - Whitespace behaves differently: "One or more consecutive white spaces in the format string matches zero or more…; leading and trailing white spaces … always allowed" [GSQL] `timestamp_functions.md:592-595`, `datetime_functions.md:1187-1191`.
   - Polars accepts a leading space but returns null for a trailing space [ran].

6. **`_PARSE_FUNCTIONS`** — a name table cannot hold BigQuery; it needs a template.
   - BigQuery takes the **format first**:
     - `PARSE_TIMESTAMP(format_string, timestamp_string[, time_zone])` [GSQL] `timestamp_functions.md:546`.
     - `PARSE_DATETIME(format_string, datetime_string)` [GSQL] `datetime_functions.md:1145`.
   - The compiler hard-codes `f"{fn}({expr}, {fmt})"` at `sql.py:1231`.
   - The non-throwing form uses the `SAFE.` prefix [GSQL] `functions-reference.md:153-165`.
   - **Proposed:**
     - Use `SAFE.PARSE_DATETIME({format}, {value})`, which returns naive DATETIME, like Polars without `%z`.
     - Use `SAFE.PARSE_TIMESTAMP({format}, {value})` when a `z` directive was *translated*; Polars returns `Datetime(us,'UTC')` for `%z` formats [ran].
     - Have `_translate_datetime_format` report whether it translated `z`, rather than substring-testing `'%z' in fmt`, because `%%z` is a literal.
     - This choice also avoids BigQuery's missing DATETIME↔TIMESTAMP coercion when the other side is a native column.
   - There is no `TRY_CAST` in GoogleSQL (0 mentions). `SAFE_CAST` exists [GSQL] `conversion_functions.md:2824` but isn't needed, since stage 7 mirrors Polars' strict cast.

7. **`_INFINITY_LITERALS`** → `"CAST('inf' AS FLOAT64)"`
   - "There is no literal representation of NaN or infinity, but … 'inf' … can be explicitly cast to float" [GSQL] `lexical.md` floating-point literals (~650-685) and `conversion_functions.md:1309-1315`.

8. **`_EDIT_DISTANCE_FUNCTIONS`** → `"EDIT_DISTANCE"`
   - Signature, NULL→NULL, same-type requirement, and the optional `max_distance =>` argument [GSQL] `string_functions.md:1060-1095`.
   - STRING is compared in Unicode code points; BYTES byte-by-byte [GSQL src] `googlesql/public/functions/distance.h:88-99`, `reference_impl/function.cc:13644-13680`. That matches the local engine's character count.
   - BigQuery documents the function on its own reference page [search].

### 1.2 Branches and SQL shapes

| Site | Current spelling | BigQuery |
|---|---|---|
| `_apply_value_map` `sql.py:1127-1154` (IFF for SNOWFLAKE at 1144, CASE otherwise) | `CASE WHEN x = 'k' THEN 'v' … ELSE x END` | CASE is fine. Per `/home/user/veridelta/.cursor/rules/300-security.mdc` (Execution boundary: "name every dialect explicitly"), give BIGQUERY an explicit branch rather than the fall-through. |
| `_compare` null-safe `sql.py:1466-1471` (EQUAL_NULL / `<=>` / implicit DuckDB `IS NOT DISTINCT FROM`) | — | `x IS NOT DISTINCT FROM y` is supported and **NaN-safe**: "NaN values are considered to be distinct from non-NaN values, but not other NaN values" [GSQL] `operators.md:3106-3141`. Make the branch explicit. |
| `_compare` plain `sql.py:1473` and `_numeric_predicate` `sql.py:1393` (`src = tgt`) | `=` | **Needs a new structural branch** (§1.3). |
| `_apply_datetime_format` `sql.py:1208-1231` | `fn(expr, fmt)` | Format goes first; choose PARSE_DATETIME or PARSE_TIMESTAMP (item 6). |
| `_apply_whitespace` `sql.py:1094-1111` | `LTRIM/RTRIM/TRIM(x)` | Valid. With no character set, TRIM removes "all whitespace characters" [GSQL] `string_functions.md:4659-4663`; LTRIM/RTRIM are "Identical to TRIM" (2529, 3847). That is closer to Polars `strip_chars()` than Snowflake/Databricks (spaces only), so `configuration.md:73` needs a BigQuery exception. Exact Unicode set parity with Rust's is [unverified]. |
| `_apply_case` `sql.py:1113-1125` | `LOWER(x)` | Valid. Uses the Unicode Character Database with no language-specific mappings [GSQL] `string_functions.md:2406-2410`. Final-sigma parity with Rust is [unverified]. |
| `_apply_regex_replace` `sql.py:1075-1092` | `REGEXP_REPLACE(v, p, r)` | Valid; RE2; replaces all non-overlapping matches; `\1`–`\9`, `\0` [GSQL] `string_functions.md:3493-3526`. A replacement containing `\` not followed by a digit or `\` is an **error** (googlesql `regexp.cc:488-493` → RE2 `re2.cc:971-1003`). `$` is literal (Polars treats `$1` as a group). Because `_literal` doubles backslashes, a configured `\1` becomes a group reference in BigQuery but literal text in Polars. Extend `configuration.md:73`. |
| `_apply_pad_zeros` `sql.py:1174-1206` | `CAST(… AS STRING)`, `SUBSTR`, `LENGTH`, `LPAD`, `\|\|` | Works unchanged. LENGTH and LPAD count characters for STRING; LPAD truncates and errors on a negative length or empty pattern [GSQL] `string_functions.md:2376-2378, 2443-2463`. The `LENGTH >= width` branch already guards truncation. |
| `_apply_cast` `sql.py:1298-1321` | `CAST(x AS kw)` + float truncation guard | Valid. FLOAT64→BOOL is not castable (the DOUBLE row of the cast table omits BOOL), so it errors. STRING→INT64 accepts hex `0x123` [GSQL] `conversion_functions.md:1387-1392`; Polars rejects it. |
| `_sentinel_literal` / `_number` `sql.py:1006-1056` | `repr(float)`, TRUE/FALSE | Output like `1e-05` matches the grammar `DIGITSe[+-]DIGITS` [GSQL] `lexical.md` (~650-685). INT64 `IN (FLOAT64)` coerces to DOUBLE (coercion table). |
| `_edit_distance_predicate` `sql.py:1397-1416` | `fn(a,b) <= n` | `EDIT_DISTANCE(a, b) <= n`. |
| Jaro-Winkler refusal text `sql.py:1433-1439`, `engine.py:746-762` | names two vendors | Add BigQuery to the text; whether BigQuery has a Jaro-Winkler function is [unverified]. |
| `compile_count_query` `sql.py:566-585` | `COUNT(*) AS alias` | Valid (INT64). |
| `compile_column_mismatch_query` `sql.py:558-560` | `SUM(CASE WHEN COALESCE(p,FALSE) THEN 0 ELSE 1 END)` | Valid. `COUNTIF` also exists [GSQL] `aggregate_functions.md:1412-1441` but isn't needed. SUM over zero rows returns NULL, which `engine.py:946-949` already handles. |
| `compile_duplicate_key_query` `sql.py:587-630` | GROUP BY/HAVING | Valid. FLOAT64 is groupable with NaNs grouped together; GEOGRAPHY is not groupable [GSQL] `data-types.md:367-390`. |
| `compile_schema_probe_query` `sql.py:632-645` | `SELECT * … WHERE 1 = 0` | Syntax is valid. Whether it is billed is [unverified]; see §2. |
| `compile_result_schema_query` `sql.py:647-661` | `SELECT * FROM (WITH …) AS a LIMIT 0` | Valid. "LIMIT 0 returns 0 rows" [GSQL] `query-syntax.md:5551-5553`; WITH inside a FROM subquery is shown at `query-syntax.md:5672-5693`. |
| CTEs, backtick aliases, RIGHT JOIN (`sql.py:663-744, 862-935`) | standard | Standard GoogleSQL; not separately verified. |
| `_is_text_side` `sql.py:1156-1172` | String → text stages | GEOGRAPHY and JSON arrive as Polars `String` [ran]. The client maps both to `pa.string` (`_pyarrow_helpers.py:77, 84`), and the Arrow field metadata it attaches (`_pandas_helpers.py:228-239`) is dropped by Polars. TRIM, `=` and GROUP BY on GEOGRAPHY then fail; GEOGRAPHY is not comparable [GSQL] `data-types.md:417-434`. JSON comparability is [unverified]. Users must `ignore` these columns. |

**Docstrings that stop being true once BigQuery is added:**
- `sql.py:4-15`: "Snowflake and Databricks".
- `sql.py:100`: "doubled to escape itself".
- `sql.py:199-202`: the `_INFINITY_LITERALS` docstring.
- `sql.py:1376-1379`: "every supported engine sorts NaN above all numbers".
  - In GoogleSQL, NaN sorts **below** `-inf`, right after NULL [GSQL] `data-types.md:314-322`.
  - The guard is still safe, because NaN comparisons return FALSE.
- `sql.py:209-214`: the character-counting note.

### 1.3 NaN-safe equality (a new structural need)

- **The divergence:** BigQuery comparisons follow IEEE-754: `NaN = any` is FALSE and `NaN < any` is FALSE [GSQL] `data-types.md:1954-1997`.
- **The local engine matches NaN pairs** both without tolerance (`changed_count == 0`) and with tolerance [ran]. The parity case `nan-pair` expects a match (`/home/user/veridelta/tests/integration/test_pushdown_parity.py:1402`).
- **Proposed BigQuery value equality**, used at `sql.py:1473` and inside `_numeric_predicate` at `sql.py:1393`:

  `({s} = {t} OR ({s} IS NOT DISTINCT FROM {t} AND {s} IS NOT NULL))`

- **Truth table** (the result is then wrapped in `COALESCE(…, FALSE)`):

| s / t | Result | After COALESCE |
|---|---|---|
| NULL / NULL | NULL OR (TRUE AND FALSE) = NULL | FALSE (same as local with `treat_null` off) |
| NaN / NaN | FALSE OR (TRUE AND TRUE) | TRUE |
| 1 / NULL | NULL OR (FALSE AND TRUE) = NULL | FALSE |
| NULL / 1 | NULL OR FALSE = NULL | FALSE |
| NaN / 1 | FALSE | FALSE |
| −0.0 / +0.0 | `=` is TRUE (`data-types.md:1988-1991`) | TRUE (Polars also says True) |

- It is type-agnostic: it works for any groupable type (`operators.md:3133-3136`).
- The alternative, `OR (IS_NAN(s) AND IS_NAN(t))`, needs FLOAT64 input [GSQL] `mathematical_functions.md:2225-2233`, so the compiler would have to predict the post-normalization type.
- Join keys (`_join_on_clause`, `sql.py:746-762`) still use `=`, so NaN float keys won't join. This is an edge case and was not tested locally.

---

## 2. The warehouse.py connector pattern and what BigQuery must do differently

### 2.1 The current pattern

- **Driver presence probe at import time:** `try: import … except ImportError` assigns a module attribute typed `Any`, marked `# pragma: no cover` (`warehouse.py:30-47`).
- **Install hints and messages** (`warehouse.py:49-57`): `_SNOWFLAKE_EXTRA`, `_DATABRICKS_EXTRA`, `_UNCONNECTED`, `_NO_STATEMENT`, `_NON_TABULAR`.
- **Shared helpers:**
  - `_lazy_from_arrow` (`warehouse.py:69-86`): Arrow → `pl.from_arrow` → `.lazy()`.
  - `_schema_from_arrow` (`warehouse.py:89-109`).
  - `_run_arrow_query(session, statement, fetch_method, *, backend, query_type, fetch_kwargs)` (`warehouse.py:112-166`). It is cursor-based. Logs are DEBUG on success and WARNING on failure, carrying backend, query type and timing, never SQL. A non-`ConnectorError` is wrapped as `ConnectorError(f"Warehouse statement failed: {exc}")`. The cursor is closed in `finally`.
  - `_close_session` (`warehouse.py:169-188`) logs a close failure instead of raising.
- **Class shape** (`SnowflakeConnector`, `warehouse.py:191-319`; Databricks at `322-445`):
  - `__init__` sets `self.compiler = SQLPushdownCompiler(SQLDialect.X)`, `_session`, `_last_statement`.
  - `connect()`: raises the extra hint if the driver is absent; re-raises `ConnectorError` unchanged; wraps anything else as `"Failed to connect to X: {exc}"`; logs at INFO with no secrets.
  - `execute_pushdown()`: `_require_session`, run, remember the statement, return a LazyFrame.
  - `fetch_schema()`: wraps `_last_statement` in `compile_result_schema_query`.
  - `close()`: idempotent and resets state.
- **Test mocking:**
  - `mocker.patch("veridelta.connectors.warehouse.snowflake_connector", driver)` with `driver.connect → session`, `session.cursor → cursor` (`/home/user/veridelta/tests/unit/test_warehouse_execution.py:67-88`).
  - `_snowflake_fetch_contract` (`test_warehouse_execution.py:51-64`) mimics the driver's zero-row behavior; copy that habit.
  - Missing-extra tests patch the attribute to `None` (`/home/user/veridelta/tests/unit/test_connectors.py:128-154`).

### 2.2 Where BigQuery breaks this pattern

1. **There is no Arrow cursor.** `google.cloud.bigquery.dbapi.Cursor` exposes execute/fetchone/fetchmany/fetchall and nothing that returns Arrow [src] `dbapi/cursor.py`.
   - `_run_arrow_query`'s `fetch_method` shape can't be reused.
   - Add a `_run_bigquery_query(client, statement, job_config, backend, query_type)` helper. Use `client.query(...).result()` or `client.query_and_wait(...)`, and share `_lazy_from_arrow`, the log format, and the error wrapping.

2. **Arrow retrieval is soft-deprecated (blocking).** Since 3.44.0, `RowIterator.to_arrow` always emits a `PendingDeprecationWarning`: "Retrieving PyArrow Tables via google-cloud-bigquery is deprecated… call 'pandas_gbq.arrow.read_bigquery_…'" [src] `table.py:121-125, 2471-2475`.
   - 3.43.0 does not have this warning [src].
   - With the default `create_bqstorage_client=True` and no storage extra, `_should_use_bqstorage` also emits a `UserWarning` "BigQuery Storage module not found…" [src] `table.py:2125-2132`. With neither a storage client nor the create flag, it returns early without warning (`table.py:2107-2109`).
   - **Recommended option: `rows.to_arrow_iterable()`**, which carries no warning [src] `table.py:2219-2304`, then `pl.from_arrow(list_of_batches)`.
     - The REST path builds one batch per page from the BigQuery schema [src] `_pandas_helpers.py:728-741, 744-776`, so an empty result should give one zero-row batch with its schema. That is read from source, not executed.
     - Polars handles a list of batches, and a single zero-row batch keeps its schema [ran].
     - **Guard the empty list:** `pl.from_arrow([])` returns a frame with **no columns** [ran]. A probe would then look like a table with zero columns. Raise `ConnectorError` instead.
   - Second option: `to_arrow(create_bqstorage_client=False)` inside a scoped `warnings.filterwarnings("ignore", message="Retrieving PyArrow Tables")`. It handles empty results using the schema (`table.py:2518-2530`), but relies on a deprecated API and a process-global filter.
   - Third option: pandas-gbq. It pulls pandas, numpy and db-dtypes, which contradicts the zero-bloat rule; its API was not checked.

3. **Credentials:**
   - ADC is the default. `Client(credentials=None)` calls `google.auth.default(scopes=…)` [src] google-cloud-core `client/__init__.py:210-218`.
   - For a key file, use the type-specific loaders:
     - `bigquery.Client.from_service_account_json(path, project=…, location=…)` [src] google-cloud-core `client/__init__.py:114-138`. It calls `from_service_account_info` (84-111), which raises `TypeError` if `credentials=` is also passed and fills `project` from the key if omitted.
     - Or `google.oauth2.service_account.Credentials.from_service_account_file(filename)` [src] google-auth `service_account.py:261`.
   - **Do not** use `client_options={"credentials_file": …}`. It routes to `google.auth.load_credentials_from_file`, which emits a security `DeprecationWarning` [src] google-auth `_default.py:67-127, 175`.
   - ADC with gcloud user credentials emits a `UserWarning` [src] `_default.py:103-113, 589`. This is production-only.
   - The default-project lookup calls `google.auth.default()` [src] google-cloud-core `_helpers/__init__.py:157-170`. Require `project` explicitly.

4. **Always set `use_legacy_sql=False`.**
   - The REST default is `true` [API] `JobConfigurationQuery.useLegacySql`.
   - The Python client currently defaults it to False [src] `_job_helpers.py:288` (jobs.query) and `:330` (jobs.insert).
   - Backticks are invalid in legacy SQL, so a change in that client default would be a silent regression.

5. **`close()`:** `Client.close()` closes the transports [src] `client.py:348-358`.

6. **Schema probe cost (optional improvement):**
   - A dry run returns the result schema: "Present only for successful dry run of non-legacy SQL queries" [API] `JobStatistics2.schema`; exposed as `QueryJob.schema` [src] `job/query.py:1153-1162`.
   - But it returns `SchemaField`s, not Arrow, and the client only maps them privately (`_pandas_helpers.bq_to_arrow_schema`).
   - Recommend keeping the zero-row query for v1, since it mirrors the other connectors. Whether `WHERE 1 = 0` or `LIMIT 0` is billed is [unverified].
   - Optionally add `maximum_bytes_billed` ("fail … without incurring a charge") [API].

7. **Import-time warnings (blocking; see §4.1).** Don't copy the module-level import probe.
   - **Recommended:** import lazily in `connect()`, mapping `ImportError` to the install hint. This is the lakehouse precedent (`/home/user/veridelta/src/veridelta/connectors/lakehouse.py:52-70, 147-171`).
   - Keep a module attribute `bigquery: Any = None` that `connect()` uses when set, so `mocker.patch("veridelta.connectors.warehouse.bigquery", driver)` still works. Tests would set `driver.Client` / `driver.Client.from_service_account_json`, `client.query(...).result().to_arrow_iterable` (or `client.query_and_wait`), and `driver.QueryJobConfig`.
   - The lazy importer needs its own branch coverage for the 100% gate.
   - `importlib.util.find_spec("google.cloud.bigquery")` can serve as a no-import presence check, but it raises `ModuleNotFoundError` if `google.cloud` itself is missing, so wrap it.
   - **Weaker alternative:** wrap the module-level import in `warnings.catch_warnings()`. It is process-global, not thread-safe, and silently suppresses a warning class the repo treats as an error.

---

## 3. Engine routing, and every file that needs a BigQuery entry

### 3.1 Routing today

- **`_is_warehouse`** (`engine.py:418-427`) is `isinstance(config, (SnowflakeConfig, DatabricksConfig))`. It gates:
  - `run_from_configs` (`engine.py:1794-1797`).
  - The proposal refusal in `propose_value_maps_from_configs` (`engine.py:1854-1859`); BigQuery is refused automatically once `_is_warehouse` includes it.
- **Fingerprints:**
  - `_snowflake_fingerprint` (`engine.py:430-447`): account, user, warehouse, database, schema_name, password, role.
  - `_databricks_fingerprint` (`engine.py:450-465`): hostname, http_path, token, catalog, schema_name.
- **`_reject_self_comparison`** (`engine.py:1462-1481`) compares the raw `table` strings.
- **`_run_warehouse_pushdown`** (`engine.py:1484-1538`):
  - Mixed-backend check at 1505-1506.
  - Cross-dialect check `type(source) is not type(target)` at 1507-1511; its message names only Snowflake and Databricks.
  - One copy-pasted block per vendor: fingerprint, self-comparison check, then connect/try/finally close (1512-1537).
  - Final raise at 1538.
  - A third block brings McCabe complexity to about 9, against the limit of 10. A config-type → (fingerprint, connector) registry would avoid that.
- **No change needed:**
  - `LoaderFactory.load` (`engine.py:389-415`) already falls through to its warehouse error.
  - `_collect_pushdown_summary` (`engine.py:1336-1459`) is dialect-agnostic through `PushdownSession` (`/home/user/veridelta/src/veridelta/connectors/base.py:22-46`).
  - `cli.py` and `report.py` have no warehouse enumeration.
- **Sampling:** local value-map sampling uses `pl.struct(keys).hash(seed=0)` (`engine.py:1976-1978`), which can't be reproduced in SQL.
  - `FARM_FINGERPRINT` exists: INT64, and its output "will never change" [GSQL] `hash_functions.md:78-93`.
  - It is only relevant if warehouse proposals are ever supported. DuckDB lacks it [ran].

### 3.2 Files needing a BigQuery entry (absolute paths)

**Source:**
- `/home/user/veridelta/src/veridelta/connectors/sql.py`: enum at 49-59; the 9 tables at 62-214; branches in §1.2; module docstring at 4-15.
- `/home/user/veridelta/src/veridelta/connectors/warehouse.py`: driver attribute, install hint, `BigQueryConnector`, module docstring at 4-13.
- `/home/user/veridelta/src/veridelta/connectors/__init__.py:6-20`: export.
- `/home/user/veridelta/src/veridelta/connectors/base.py:54-61`: docstring lists the warehouse connectors.
- `/home/user/veridelta/src/veridelta/models.py`:
  - Add `BigQueryConfig` after `DatabricksConfig` (942-972).
  - Update `SourceRef` (1035-1039).
  - Any new patterns go near 25-32.
- `/home/user/veridelta/src/veridelta/config.py`: imports at 19-26 and `__all__` at 28-35.
- `/home/user/veridelta/src/veridelta/__init__.py`: imports at 16-35 and `__all__` at 39-67.
  - The list must stay sorted; `BigQueryConfig` goes between `ArtifactFormat` and `CastTarget`, as enforced by `/home/user/veridelta/tests/smoke/test_installation.py:44-55`.
- `/home/user/veridelta/src/veridelta/engine.py`:
  - Imports at 24 and 26-43.
  - `_is_warehouse` at 418-427 (docstring at 425).
  - A new fingerprint function.
  - `_run_warehouse_pushdown` at 1484-1538, including the messages at 1509-1510 and 1515-1516.
  - The Jaro-Winkler text at 756-761.

**Tests:**
- `/home/user/veridelta/tests/unit/test_sql_compiler.py`:
  - 19 (`_BACKSLASH_ESCAPE_DIALECTS`) and 23-56 (lexer).
  - 59-71 (compiler helpers).
  - **79-88, which asserts the exact dialect set.**
  - 147-149 and 1260-1262 (table-completeness checks, which pick up the new dialect automatically).
  - 151-176 (round-trip test, parametrized over all dialects automatically).
  - 96-133 (quoting).
  - 220-238 (value_map).
  - 291-303 (null-safe equality).
  - 323-339 (infinity).
  - 981-1017 (cast keywords).
  - 1103-1188 (datetime).
  - 1264-1282 (edit distance).
- `/home/user/veridelta/tests/unit/test_warehouse_execution.py`: mirror 20-88 and 91-399.
- `/home/user/veridelta/tests/unit/test_connectors.py`: 30-48, 66-116, 128-154.
- `/home/user/veridelta/tests/integration/test_engine_routing.py`:
  - Helpers at 29-61.
  - Happy paths at 163-292.
  - The `["Snowflake", "Databricks"]` parametrization at 293.
  - Cross-dialect and fingerprint tests at 741-829.
  - Mixed-backend tests at 830-856.
  - YAML round-trips at 924-1003.
- `/home/user/veridelta/tests/unit/test_models.py`: 57-98 and 313-420.
- `/home/user/veridelta/tests/unit/test_config.py`: YAML cases (e.g. 185-211).
- `/home/user/veridelta/tests/unit/test_docs_coverage.py:10-30` (`_CONFIG_MODELS`).
- `/home/user/veridelta/tests/unit/test_engine.py:2219-2236`.
- `/home/user/veridelta/tests/integration/duckdb_harness.py:11-19`: docstring.

**Docs:**
- `/home/user/veridelta/docs/configuration.md`:
  - 44-52 (extras), 54 (credentials), 56 (routing and fingerprint fields), 58 (table segments).
  - 62, 64, 71, 73, 80.
  - 138-178 (YAML examples).
  - 202-216 (the connection-field table, the frozen note and the credential-printing note). Every `BigQueryConfig` field name must appear here, or `test_docs_coverage.py` fails.
  - 251-253 (logging), 406 (`%f`), 525 (fuzzy matching).
- `/home/user/veridelta/README.md`: 19, 26, 36-40 (the mermaid diagram).
- `/home/user/veridelta/docs/index.md`: 14, 21, 31.
- `/home/user/veridelta/docs/roadmap.md:7-8`: delete the BigQuery bullet.
- `docs/api.md` is generated, and `CHANGELOG.md` is written by commitizen on bump; neither should be hand-edited.

**Build, CI and rules:**
- `/home/user/veridelta/pyproject.toml`:
  - Add a `bigquery` extra under 36-62 and add it to `all` (55-62).
  - Add `google.cloud.bigquery(.*)`, `google.api_core.*`, `google.auth.*` and `google.oauth2.*` to the `follow_imports="skip"` block (126-144). The bigquery wheel ships `py.typed`.
- `/home/user/veridelta/uv.lock`: regenerate.
- `/home/user/veridelta/.github/workflows/ci.yml:106` and `/home/user/veridelta/Makefile:19`: change only if the connector lives in a new module; `warehouse.py` is already coverage-gated.
- `/home/user/veridelta/.cursor/rules/300-security.mdc`: 15 (quote characters), 17 (`_LITERAL_ESCAPES` wording; add newline escaping).
- `/home/user/veridelta/.cursor/rules/400-docs.mdc:19`: the list of connection models.

---

## 4. The google-cloud-bigquery client

### 4.1 Import-time FutureWarning (blocking)

- **Where it comes from:** google-api-core 2.39.0 runs `check_python_version` and `check_dependency_versions` at import (`google/api_core/__init__.py:39-40`). google-cloud-bigquery runs them again (`google/cloud/bigquery/__init__.py:139-140`).
- **The rule** (`_python_version_support.py`): a FutureWarning fires from end-of-life − 365 days through end-of-life + 7 days ("deprecated"), and every day after that ("unsupported").
- I executed that module under `-W error` on each installed interpreter [ran]:

| Python | 2026-09-26 (today) | 2026-10-12 | 2026-10-25 |
|---|---|---|---|
| 3.10.20 | **raises FutureWarning** | **raises** | **raises** |
| 3.11.15 | none | none | **raises** (window opens 2026-10-24) |
| 3.12.3 | none | none | none (opens 2027-10-03) |
| 3.13.12 | none | none | none (opens 2028-10-07) |
| 3.14 | not installed here | — | opens 2029-10-07 per the same table |

- **Consequence:**
  - A warning escalated to an error is not an `ImportError`, so the current `except ImportError` probe pattern would break `import veridelta` (`engine.py:24` → `warehouse.py`) on 3.10 today and on 3.11 from 2026-10-24.
  - That happens once the extra is installed, which CI does: `uv sync --all-extras` on 3.10–3.14 with `filterwarnings = ["error"]` (`pyproject.toml:209`).
  - That pytest applies this filter during collection is my understanding of pytest's warnings plugin; I did not re-verify it.
- **Other module-level warning sites** (AST scan):
  - None in google-cloud-core, google-resumable-media, proto-plus, googleapis-common-protos, opentelemetry-api 1.45.0, pyasn1, pyasn1-modules or grpcio-status.
  - Conditional ones:
    - api-core's dependency check warns if protobuf < 6.33.5 or grpcio < 1.83 (`_python_package_support.py`).
    - `google/auth/transport/grpc.py:43` warns if grpcio < 1.83.
    - protobuf falls back with a warning if upb is unavailable (`api_implementation.py:75-104`).
    - `bigquery_v2/__init__.py:39` warns, but nothing in `google.cloud.bigquery` imports `bigquery_v2`.
    - `OpenSSL/rand.py:11` warns only if that module is imported.
  - python-dateutil 2.9.0.post0 doesn't call `utcfromtimestamp` at import.

### 4.2 Versions, wheels, pyarrow

- **google-cloud-bigquery 3.45.2** (released 2026-09-17), `requires-python >=3.10`, pure Python.
  - Base dependencies: `google-api-core[grpc]>=2.28.0,<3`, `google-auth[pyopenssl]>=2.14.1,<3`, `google-cloud-core>=2.4.1,<3`, `google-resumable-media>=2.0.0,<3`, `packaging>=24.2.0`, `python-dateutil`, `requests`.
  - **pyarrow is not a base dependency**; it appears only in the `bqstorage` and `pandas` extras. The client's minimum is 3.0.0 (`_versions_helpers.py:21`). The veridelta extra must list pyarrow itself.
- **Binary wheels:** grpcio 1.84.0, google-crc32c 1.9.0 and cffi 2.1.1 ship cp310–cp315 wheels for Linux x86_64/aarch64, macOS and Windows. protobuf 7.36.2 is cp310-abi3; cryptography 50.0.1 is abi3. The full CI matrix is covered by wheels.
- **pyarrow on 3.14:** `uv.lock` pins pyarrow 23.0.1 on ≥3.14, because of the snowflake extra's `<24`, and 25.0.1 below (`uv.lock:2501-2570, 3989-3990, 4049-4050`).
  - pyarrow ≥22 has cp314 wheels, and google-cloud-bigquery sets no upper bound, so there is **no conflict**.
  - Suggested extra: `google-cloud-bigquery>=3.x` plus `pyarrow>=10.0.1`. I did not check which 3.x release introduced `to_arrow_iterable` or `query_and_wait`; 3.45.2 has both.

### 4.3 Signatures [src]

- `Client(project=None, credentials=None, _http=None, location=None, default_query_job_config=None, default_load_job_config=None, client_info=None, client_options=None, default_job_creation_mode=None)` — `client.py:254-267`.
- `Client.query(query, job_config=None, job_id=None, job_id_prefix=None, location=None, project=None, retry=…, timeout=…, job_retry=…, api_method=INSERT, *, timestamp_precision=None) -> QueryJob` — `client.py:3520-3534`.
- `Client.query_and_wait(query, *, job_config=None, location=None, project=None, api_timeout=…, wait_timeout=…, retry=…, job_retry=…, page_size=None, max_results=None, query_results_format=None, compression_codec=None) -> RowIterator` — `client.py:3665-3680`.
- `QueryJob.result(page_size=None, max_results=None, retry=…, timeout=…, start_index=None, job_retry=…)` — `job/query.py:1569`.
- `QueryJobConfig(**kwargs)` — `job/query.py:395`.
  - The `default_dataset` setter accepts `None`, a string (parsed by `DatasetReference.from_string`), `DatasetReference`, `Dataset` or `DatasetListItem` (`job/query.py:510-523`).
  - Other settable properties: `use_legacy_sql` (697-708), `dry_run` (563-573), `maximum_bytes_billed` (604-613).
- `RowIterator.to_arrow(progress_bar_type=None, bqstorage_client=None, create_bqstorage_client=True, timeout=None)` — `table.py:2408-2414`.
- `RowIterator.to_arrow_iterable(bqstorage_client=None, max_queue_size=…, max_stream_count=None, timeout=None)` — `table.py:2219-2225`.
- Domain-scoped project IDs (`example.com:proj`) are parsed by the client (`_helpers.py:45-48, 999-1014`).

**`to_arrow` without the storage extra:** it works over REST pages. You get the `UserWarning` unless `create_bqstorage_client=False`, plus the `PendingDeprecationWarning` in every case.

### 4.4 How BigQuery types land in Polars [ran]

Measured with pyarrow 25.0.1 and polars 1.40.1, using the client's mapping (`_pyarrow_helpers.py:40-88, 121`). Zero-row tables convert fine.

| BigQuery | Polars |
|---|---|
| INT64 | Int64 |
| FLOAT64 | Float64 |
| NUMERIC | Decimal(38,9) |
| BIGNUMERIC | Decimal(76,38) |
| STRING, GEOGRAPHY, JSON | String |
| BOOL | Boolean |
| BYTES | Binary |
| DATE | Date |
| DATETIME | Datetime(us, None) |
| TIMESTAMP | Datetime(us, 'UTC') |
| TIME | Time |
| REPEATED | List |
| RECORD | Struct |

- Consequences:
  - `_reject_unzoned_timezone` (`engine.py:137-172`) accepts TIMESTAMP and refuses DATETIME, which matches local semantics.
  - Sentinels treat Decimal as numeric (`sentinels.py:49-53`).
- An unknown type such as INTERVAL makes the client emit "Unable to determine Arrow type" and return no schema (`_pandas_helpers.py:242-261`). A table with such a column breaks the zero-row probe path.

---

## 5. Proposed `BigQueryConfig`, relation handling, fingerprint

```python
BIGQUERY_PROJECT_PATTERN = r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$"            # [API] Project.projectId rule
BIGQUERY_TABLE_PATTERN  = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"   # `table` or `dataset.table`

class BigQueryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    type: Literal["bigquery"] = "bigquery"
    table: str = Field(..., pattern=BIGQUERY_TABLE_PATTERN)
    project: str = Field(..., pattern=BIGQUERY_PROJECT_PATTERN)   # job project AND data project
    dataset: str | None = Field(default=None, pattern=SQL_IDENTIFIER_SEGMENT_PATTERN)  # default dataset
    location: str | None = None                                    # "US", "EU", "us-central1"; passed to Client
    credentials_path: str | None = Field(default=None, repr=False) # service-account JSON; ADC when None
    maximum_bytes_billed: int | None = Field(default=None, ge=1, strict=True)  # optional cost cap
    # model_validator: a one-segment `table` requires `dataset`
```

**Why this shape:**
- **1-segment tables need `dataset`.** Without a default dataset, "all table names … must be qualified in the format 'datasetId.tableId'" [API] `QueryRequest.defaultDataset`.
- **`project` must be both the job project and the data project.** `defaultDataset` "does not alter behavior of unqualified dataset names" [API] `JobConfigurationQuery.defaultDataset`, so a 2-segment `dataset.table` resolves against the job's project.
  - The connector would call `Client(project=project, location=location)` or `from_service_account_json(...)`, with `QueryJobConfig(default_dataset=f"{project}.{dataset}", use_legacy_sql=False, maximum_bytes_billed=...)`.
  - The project never appears in SQL, so `SQL_IDENTIFIER_SEGMENT` and `_quote_relation` stay unchanged.
  - Supporting billing ≠ data project later would need a BigQuery-only project-segment allowlist in `_quote_relation`. Backtick-quoting that segment would still be safe, since a hyphen can't break out.
- **Pattern tests [ran]:**
  - Accepted: `tokyo-rain-123`, `my-project`, `abcdef`, 30 characters, `bigquery-public-data`.
  - Rejected: `my-project-`, `abcde`, 31 characters, `1project`, `My-Project`, `example.com:my-proj`.
  - Domain-scoped IDs and digit-leading dataset IDs (both legal in BigQuery) are therefore rejected fail-closed. Document that.
- **Fingerprint:** `(project, dataset, location, credentials_path, maximum_bytes_billed)`, i.e. every field except `table`, following the Snowflake/Databricks convention (`engine.py:430-465`).
  - `project` covers the job and 2-segment resolution.
  - `dataset` covers 1-segment resolution.
  - `location`: one query runs in one location.
  - `credentials_path` is the identity.
  - `maximum_bytes_billed`: the connector is built from `source`, so a different target cap would be silently ignored.
- **`_reject_self_comparison` caveat:** `events` and `analytics.events` with `dataset="analytics"` are the same table but different strings. The same gap already exists for Snowflake (`EVENTS` vs `ANALYTICS.PUBLIC.EVENTS`). Optionally normalize to `dataset.table` before comparing.

---

## 6. What DuckDB cannot validate, so the plan needs string assertions

**DuckDB 1.5.5 cannot run BigQuery SQL at all [ran]:**
- **Backticks are a parse error in DuckDB**, so no BigQuery-compiled statement can execute in the harness. BigQuery coverage has to come from string assertions and mocked-connector tests.
- Missing in DuckDB: `FLOAT64` (INT64, STRING, BOOL and DATETIME aliases do exist), `PARSE_DATETIME`/`PARSE_TIMESTAMP`/`SAFE.`, `EDIT_DISTANCE`, `IS_NAN`, `SAFE_CAST`, `FARM_FINGERPRINT`. `COUNTIF` exists.

**DuckDB semantics differ from BigQuery [ran]:**
- A backslash is literal, and `\'` is a parse error.
- `NaN = NaN` is TRUE and NaN sorts above +inf. The `nan-pair` parity case passes on DuckDB but would fail on BigQuery without §1.3.
- TRIM strips only spaces.

**Must be pinned by string tests:**
- The BigQuery literal escapes (reference lexer), including newlines.
- Per-segment backticks.
- Cast keywords.
- `CAST('inf' AS FLOAT64)`.
- `EDIT_DISTANCE`.
- The explicit CASE and `IS NOT DISTINCT FROM` branches.
- The NaN-safe equality spelling.
- Format-first `SAFE.PARSE_DATETIME` vs `SAFE.PARSE_TIMESTAMP`, `%Ez`, and refusing `%f`.

**Known local-vs-BigQuery divergences to document, not provable either way:**
- Implicit coercion errors (STRING vs INT64, DATETIME vs TIMESTAMP) become a `ConnectorError`, where local soft-casts.
- Columns with `und:ci` collation make `=` and GROUP BY case-insensitive [GSQL] `lexical.md` case table.
- Parse whitespace leniency, e.g. a trailing space parses in BigQuery but is null locally [ran].
- `%Ez` accepts `Z`.
- `CAST(FLOAT64 AS STRING)` returns "an approximate string representation" (Polars gives `'1.0'` [ran]; BigQuery's exact output is [unverified]). This affects `pad_zeros` and `cast_to: String` on floats.
- `CAST(TIMESTAMP AS STRING)` uses UTC and trims subseconds; Polars uses the label's zone with 6 digits [ran].
- STRING→INT64 accepts hex.
- REGEXP_REPLACE backslash and `$` rules.
- GEOGRAPHY, and possibly JSON, columns must be ignored.

**Pre-existing harness gaps (all dialects, noted once):**
- DuckDB `REGEXP_REPLACE` without the `'g'` flag replaces only the **first** match: `'a-b-c'` → `'ab-c'` [ran]. Polars, Snowflake, Databricks and BigQuery replace all matches. The parity test only uses single-match data (`test_pushdown_parity.py:139-151`).
- Polars `%f` is chrono nanoseconds, as described in §1.1 item 4.

---

## Unverified (consolidated)

- Whether zero-row probes (`WHERE 1 = 0` / `LIMIT 0`) are billed, and the per-table minimum charge.
- Whether BigQuery JSON is comparable or groupable.
- BigQuery's exact FLOAT64→STRING rendering.
- Whether `%m`/`%d` accept single digits.
- Whether BigQuery table and dataset names are case-sensitive.
- Whether BigQuery has a Jaro-Winkler function.
- LOWER final-sigma behavior and the exact TRIM whitespace set.
- Which 3.x release introduced `query_and_wait` / `to_arrow_iterable`.
- The pytest collection-time filter behavior.
- The full `import google.cloud.bigquery` chain under `-W error`. Only api-core's check was executed; the rest is source analysis.
- Default time zone UTC and the presence of EDIT_DISTANCE / IS DISTINCT FROM in BigQuery are search-level evidence only.

## Sources (search results relied on)

- [Timestamp functions | BigQuery](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions) — default time zone UTC
- [String functions | BigQuery](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/string_functions) — EDIT_DISTANCE
- [Operators | BigQuery](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/operators) — IS DISTINCT FROM
- [Lexical structure and syntax | BigQuery](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/lexical) — backtick path quoting
- [Create projects | Resource Manager](https://cloud.google.com/resource-manager/docs/creating-managing-projects) — project ID rule (confirmed via the [API] discovery document)
- [Estimate and control costs | BigQuery](https://docs.cloud.google.com/bigquery/docs/best-practices-costs) — inconclusive on zero-row probes