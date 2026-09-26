I didn't create a scratch venv, `requirements.in` or `.db` file, and I didn't install connectorx or ADBC: this session is read-only (plan mode plus a read-only agent). What I did instead:
- **Item 4:** the PyPI JSON API, PEP 658 wheel metadata, and wheel contents read in memory.
- **Item 5:** the stdlib sqlite3 path, run with the project's `.venv/bin/python -B -W error` (Python 3.11.15, polars 1.40.1) against in-memory databases only. For connectorx and ADBC I report what their source and docs say; none of it was executed. Neither package is installed or cached anywhere on this machine. A command to run the real test is at the end of item 5.

HEAD equals origin/main (882e1c5) and the working tree is clean. Paths below are under `/home/user/veridelta/`.

## 1. How lakehouse sources are wired

**Models:** `/home/user/veridelta/src/veridelta/models.py`
- `DeltaLakeConfig` is at 975-1002 and `IcebergConfig` at 1005-1032. Both use:
  ```python
  # Credentials pass through here, and Pydantic quotes raw input in its errors.
  model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
  ```
- Their fields:
  - `type: Literal["delta"|"iceberg"]` with a default.
  - `table_uri: str`, with no pattern.
  - `version` / `snapshot_id: int | None = Field(default=None, ge=0, strict=True)`.
  - `storage_options: dict[str, str] = Field(default_factory=dict, repr=False, ...)`.
- There are no `@field_validator`s; all constraints are at field level.
- The same `repr=False` pattern hides `SnowflakeConfig.password` (934-936) and `DatabricksConfig.access_token` (968-970). Both `table` fields use `pattern=SQL_RELATION_PATTERN` (926, 963).
- `SourceConfig` (123-166) is the model to copy if you want to mask part of a URI rather than hide the whole field. It sets `hide_input_in_errors=True` (138), and a custom `__repr_args__` (148-166) drops only the nested `options["storage_options"]`.
- `SourceRef` (1035-1038):
  ```python
  SourceRef = Annotated[
      SourceConfig | SnowflakeConfig | DatabricksConfig | DeltaLakeConfig | IcebergConfig,
      Field(discriminator="type"),
  ]
  ```
- `SourceType` (34-50) lists file formats only. `tests/unit/test_engine.py:64` pins it with `set(get_args(SourceType)) == set(LoaderFactory._loaders)`. A `database` type belongs in `SourceRef`, not `SourceType`.

**Connector:** `/home/user/veridelta/src/veridelta/connectors/lakehouse.py`
- The module docstring (4-14) says a missing extra and a failed scan are reported separately, and "log lines carry the table URI and pin, never `storage_options`".
- A module logger with a `NullHandler` is set up at 24-25.
- Message constants (27-30) include `_DELTA_EXTRA = "Delta Lake extra is not installed. Install it with: uv add 'veridelta[delta]'"`.
- `DeltaLakeConnector(VerideltaConnector)` is at 33-125. `connect()` (52-76):
  ```python
  try:
      self._frame = pl.scan_delta(self._config.table_uri, version=..., storage_options=storage_options)
  except ImportError as exc:
      raise ConnectorError(_DELTA_EXTRA) from exc
  except Exception as exc:
      logger.warning("Delta Lake scan of %s failed", self._config.table_uri)
      raise ConnectorError(f"Delta Lake scan of '{self._config.table_uri}' failed: {exc}") from exc
  logger.info("Opened Delta Lake scan of %s (version=%s)", ...)
  ```
- The other methods of `DeltaLakeConnector`:
  - `execute_pushdown` always raises (78-95).
  - `fetch_schema` returns `self._frame.collect_schema()` (97-108).
  - `lazyframe()` (110-121).
  - `close()` sets `_frame = None` (123-125).
- `IcebergConnector` (128-226) has the same shape.
- **Probe style:** the lakehouse connectors catch `ImportError` raised from inside the polars call. Rule `100-engine-polars.mdc:30` instead names module-level probes:
  - `_optional_module` and `fastexcel = _optional_module("fastexcel")` in `engine.py` 47-64.
  - `try: import snowflake.connector ... except ImportError:  # pragma: no cover` in `warehouse.py` 30-47, with install hints at 49-54. Tests patch these module attributes.
- **Base class:** `connectors/base.py` 49-143 defines abstract `connect` / `execute_pushdown` / `fetch_schema`, a no-op default `close`, and a context manager whose exit calls `close()`. Its docstring (54-60) describes exactly two connector families.
- **Tests to mirror** in `tests/unit/test_connectors.py`:
  - 279-293: a scan failure is wrapped with the table and the cause.
  - 295-315: `ImportError` becomes `uv add 'veridelta[delta]'`.
  - 317-336: `caplog` checks `"hunter2" not in caplog.text`.
  - 338-375: close, reopen, and context exit.

**Engine:** `/home/user/veridelta/src/veridelta/engine.py`
- `LoaderFactory.load` (388-415) dispatches by `isinstance` on the config:
  - `DeltaLakeConfig` → `DeltaLakeConnector(config)`, then `.connect()`, then `return .lazyframe()`.
  - `IcebergConfig` → the same with `IcebergConnector`.
  - `SourceConfig` → `cls.get_loader(config.format).load(config)`.
  - Anything else → `ConnectorError("Warehouse sources cannot be loaded via LoaderFactory; use DiffEngine.run_from_configs for SQL pushdown.")`.
- The lakehouse connector is never `close()`d in `LoaderFactory.load`. A database connector that opens a real connection would need `try/finally` around the read.
- `_is_warehouse` (418-427) is `return isinstance(config, (SnowflakeConfig, DatabricksConfig))`.
- **Routing:**
  - `run_from_configs` (1776-1802): if either side is a warehouse it calls `_run_warehouse_pushdown`; otherwise `cls(diff, LoaderFactory.load(source), LoaderFactory.load(target)).run()`.
  - `propose_value_maps_from_configs` (1824-1865): any warehouse side raises `ConnectorError("... both sides must be file or lakehouse sources. ...")`.
  - `DataIngestor` (1704-1722) also goes through `LoaderFactory.load`.
- **Rules in `_run_warehouse_pushdown` (1484-1538):**
  - (a) `source_wh != target_wh` raises `ConnectorError("Mixed file/lakehouse and warehouse backends are unsupported.")` (1503-1506).
  - (b) `type(source) is not type(target)` raises "Cross-dialect warehouse pushdown is unsupported..." (1507-1511).
  - (c) A fingerprint mismatch raises "Cross-account..." (1513-1517, 1526-1530). The fingerprints (430-465) include the password and token.
  - (d) The same table on both sides raises `ConfigError` via `_reject_self_comparison` (1462-1481).
  - (e) The fallthrough at 1538 raises the "Mixed file/lakehouse" error again.
  - Tests: `tests/integration/test_engine_routing.py` 830-856.
- **Implication:** if a database source keeps `_is_warehouse` False:
  - database versus file, lakehouse or database runs locally;
  - database versus warehouse hits the "Mixed file/lakehouse" message, whose wording becomes stale;
  - `crosswalk` works for database sources.

**Exports**
- `connectors/__init__.py` 6-20 exports, sorted: `DatabricksConnector`, `DeltaLakeConnector`, `IcebergConnector`, `PushdownQueryType`, `SQLDialect`, `SQLPushdownCompiler`, `SnowflakeConnector`, `VerideltaConnector`.
- `veridelta/__init__.py` exports the config models, not the connectors (imports 16-35, `__all__` 39-67).
- `config.py` 28-35 also re-exports the config models.

**Config parsing:** `/home/user/veridelta/src/veridelta/config.py`
- `_SOURCE_REF_ADAPTER = TypeAdapter(SourceRef, config=ConfigDict(hide_input_in_errors=True))` (37-42). The reason given is that an unknown `type` fails before any model is chosen.
- `_ENV_REFERENCE` (45-52) matches, in order: `$${` as an escape, `${NAME}` or `${NAME:-default}`, and anything else as malformed.
- `_substitute` (55-89) names the variable and its location in errors, never the value. The default applies when the variable is unset or empty.
- `_expand_env` (92-130) expands only string values. Keys are not expanded, substituted text is not rescanned, and YAML-alias cycles are guarded.
- `_parse_source_ref` (133-155) checks the block is a mapping, expands it, defaults `type="file"`, then validates.
- **Scope:** only the `source` and `target` blocks are expanded (203-207). Root settings and `rules` are read verbatim (docstring 166-169).
- A `ValidationError` is turned into a `ConfigError` that shows only `loc` and `msg` (212-217).
- **Relevance to `postgresql://user:${PGPASSWORD}@host/db`:** substitution is plain text and is not percent-encoded. The polars docs say "The caller is responsible for escaping any special characters" (`functions.py` 370-372).

**CLI:** `/home/user/veridelta/src/veridelta/cli.py` has no branching by source type.
- `run` (198-201) calls `load_config`, then `DiffEngine.run_from_configs`.
- `crosswalk` (320-330) calls `propose_value_maps_from_configs`.
- `_report_failure` (147-175) prints `type(exc).__name__` and the full message of any `VerideltaError` to stderr. Whatever driver text a `ConnectorError` carries will be printed.

## 2. Identifier allowlist and security rules

**Patterns:** `/home/user/veridelta/src/veridelta/models.py` 25-32:
```python
SQL_IDENTIFIER_SEGMENT_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"
SQL_IDENTIFIER_SEGMENT = re.compile(SQL_IDENTIFIER_SEGMENT_PATTERN)
SQL_RELATION_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$"
```
- `public.orders` passes. Four-part SQL Server names, `#temp` tables, and names containing `$` or other special characters fail.
- The check runs twice: once in the model (the `pattern=` field constraint) and again at compile time in `connectors/sql.py`:
  - `_quote_ident` (937-952) does a fullmatch or raises `ConnectorError`, then `quote + name.replace(quote, quote*2) + quote`.
  - `_quote_relation` (954-973) allows at most 3 dotted segments.
- **`SQLDialect` is a closed set** (49-59: snowflake, databricks, duckdb) and tests pin it:
  - `tests/unit/test_sql_compiler.py:84-88` asserts the exact value set.
  - `:149` asserts `set(_LITERAL_ESCAPES) == set(SQLDialect)`.
  - `:151` parametrizes over every dialect.
  - `:1262` asserts the same for `_EDIT_DISTANCE_FUNCTIONS`.
- There are 8 per-dialect tables in `sql.py`, at lines 62, 95, 102, 120, 164, 183, 194 and 204. Adding postgres/mysql/mssql members only to quote `SELECT * FROM` would force full rows in all eight. A small separate quoting table in `sql.py` fits better.
- Quoting disables case folding. Existing precedent at `docs/configuration.md:64`: "YAML identifiers must match the stored column case".

**Rules in `/home/user/veridelta/.cursor/rules/300-security.mdc` (verbatim):**
- 13: "Assemble warehouse SQL only in `connectors/sql.py`. Do not f-string or concatenate YAML/user strings in `warehouse.py` or `engine.py`."
- 14: "`execute_pushdown` runs compiler-produced `statement` only. Do not concatenate config fields into a second SQL string."
- 15: "Identifiers: allowlist each segment (...) then dialect-quote (...). Fail closed with `ConnectorError` on disallowed characters."
- 16: "Do not apply SQL identifier regex to ... credentials, hostnames, tokens, or lakehouse `table_uri`." So there should be no regex on `uri`.
- 22: "Do not add sanitization libraries. Quoting plus allowlists plus strict types is the control."
- 26: "Drivers receive compiled SQL or native scan kwargs. Never interpolate passwords, tokens, or URIs into SQL." A raw `query` is neither, so it would need an explicit, documented exception.
- 28: every dialect-specific keyword or quote character "belongs in a module-scope table in `sql.py` keyed by `SQLDialect`".
- 29: "Treat configuration ... as untrusted input. Validate at the boundary."

**Rules in `/home/user/veridelta/.cursor/rules/100-engine-polars.mdc`:**
- 13: prefer lazy / `scan_*`.
- 19: `extra="forbid"`, prefer `frozen=True`.
- 20: raise `VerideltaError` subclasses.
- 21: "Do not invent new packages." A new module `connectors/database.py` is fine.
- 26: `SourceType` is bound to `_loaders` by tests.
- 29: "`json` and `excel` are eager ... both wrap with `.lazy()` and say so in the class docstring. Do not add another eager loader without the same note."
- 30: optional readers use the probe pattern.

**A raw `query` gets no real read-only protection from polars. Observed in memory:**
- The only guard is `_INVALID_QUERY_TYPES` (`polars/io/database/_executor.py` 43-57). It lists ALTER, ANALYZE, CREATE, DELETE, DROP, GRANT, INSERT, REPLACE, REVOKE, UPDATE, UPSERT, USE and VACUUM. It checks the first `\w{3,}` token after stripping `/* */` comments (522-526).
- Only `read_database` runs the guard. `read_database_uri` (connectorx or ADBC) skips it.
- What happened with each query:

| Query | Result |
|---|---|
| `DELETE FROM g` | blocked, `UnsuitableSQLError` |
| `/* c */ DELETE FROM g` | blocked, `UnsuitableSQLError` |
| `SELECT 1 AS x; DELETE FROM g` | blocked by stdlib sqlite3 itself: `ProgrammingError: You can only execute one statement at a time.` |
| `-- comment\nDELETE FROM g` | **DELETE ran** (0 rows left), then `TypeError: 'NoneType' object is not iterable` |
| `WITH x AS (SELECT 1) DELETE FROM g` | **DELETE ran**, same TypeError |

- Mitigation therefore has to be at the connection level: a read-only role, SQLite `mode=ro`, or Postgres read-only transactions. Add documentation and never log the SQL; `configuration.md:253` already promises "Log lines never contain SQL text".

**Credentials in error text:**
- The connectorx path in polars re-raises errors after `re.sub("://[^:]+:[^:]+@", "://***:***@", str(err))` (`polars/io/database/_utils.py` 69-72). The ADBC path does no scrubbing.
- I evaluated the regex: a password containing `:` is not scrubbed (`postgresql://alice:pa:ss@host/db` is unchanged).
- The lakehouse-style `f"... failed: {exc}"` would forward driver text verbatim into the `ConnectorError`, and from there to CLI stderr.

## 3. Polars database readers

Observed with 1.40.1. I also read the pure-Python wheels of 1.39.3 (the project floor) and 1.44.2 (latest) in memory; all logic cited here is identical across them.
```
read_database_uri (query: 'list[str] | str', uri: 'str', *, partition_on: 'str | None' = None, partition_range: 'tuple[int, int] | None' = None, partition_num: 'int | None' = None, protocol: 'str | None' = None, engine: 'DbReadEngine | None' = None, schema_overrides: 'SchemaDict | None' = None, execute_options: 'dict[str, Any] | None' = None, pre_execution_query: 'str | list[str] | None' = None) -> 'DataFrame'
read_database (query: 'str | TextClause | Selectable', connection: 'ConnectionOrCursor | str', *, iter_batches: 'bool' = False, batch_size: 'int | None' = None, schema_overrides: 'SchemaDict | None' = None, infer_schema_length: 'int | None' = 100, execute_options: 'dict[str, Any] | None' = None) -> 'DataFrame | Iterator[DataFrame]'
```
Key docstring points, from `/home/user/veridelta/.venv/lib/python3.11/site-packages/polars/io/database/functions.py`:
- **Default engine** is `connectorx` (496-497). The docs list its backends as "PostgreSQL, Redshift, MySQL, MariaDB, Clickhouse, Oracle, BigQuery, SQL Server". ADBC has "limited support ... relatively small number of drivers" (385-394).
- **connectorx-only options:**
  - `partition_on`, `partition_range`, `partition_num` and `protocol` (373-381).
  - A list of queries is accepted.
  - `pre_execution_query` (Postgres/MySQL, connectorx ≥0.4.2) calls `issue_unstable_warning` (503-506), which becomes an error under `filterwarnings=error`.
- **ADBC-only:** takes a single query string (518-520). `execute_options` means qmark parameters only on ADBC; connectorx raises `ValueError` if it is passed (398-401, 500-502).
- **`schema_overrides`** is applied through `from_arrow(tbl, schema_overrides=...)` (`_utils.py` 74, 120).
- **Install notes** (414-419): "ensure that you have `connectorx>=0.3.2`"; ADBC needs the driver package, and pyarrow if `adbc-driver-manager` < 1.7.0. Passwords must be URL-escaped by the caller (421-428).
- **`read_database`** "will *never* close any other open connection or cursor" (154-156). The connector must close its own connection.

What each engine needs, from the source:
- **connectorx:** polars calls it with `return_type="arrow"` (`_utils.py` 46-68). connectorx 0.4.6 then calls `try_import_module("pyarrow")` and rebuilds the table with `pa.Array._import_from_c` (`connectorx/__init__.py` 413-425, 473-483, read from the wheel). So **pyarrow is required at runtime**.
- **ADBC:**
  - The driver module is `adbc_driver_{scheme}`, with `postgres` mapped to `postgresql` (`_utils.py` 123-132).
  - With `adbc_driver_manager` ≥1.6.0 polars uses `fetch_arrow()`, which needs no pyarrow (110-115).
  - For `sqlite`, `duckdb` and `snowflake`, polars strips the scheme with `re.sub(f"^{driver_name}:/{{,3}}", "", uri)` (184-185).
- **Missing engine (observed):** both raise `ModuleNotFoundError`, an `ImportError` subclass, so the lakehouse `except ImportError` would catch them:
  - "required package 'connectorx' not found. Please install using the command `pip install connectorx`."
  - "ADBC 'adbc_driver_sqlite' driver not detected. ..."

**There is no lazy or scan variant.** polars exports only `read_database` and `read_database_uri`; there is no `scan_database` in 1.39.3–1.44.2. `iter_batches=True` yields DataFrames but is not lazy. `pl.defer(function, *, schema, validate_schema=True)` and `polars.io.plugins.register_io_source` both exist. I tested `pl.defer` with `DiffEngine`:
- A `pl.defer`-wrapped `read_database` ran each side's query **5 times in one `DiffEngine.run()`**. The eager `read_database(...).lazy()` ran it once. On a live table, the 5 reads could see different data.
- `defer` runs the function on a polars worker thread. A sqlite3 connection opened outside it failed with "SQLite objects created in a thread can only be used in that same thread".
- The eager read wrapped with `.lazy()` (rule 100:29) is therefore the right pattern.

## 4. Wheel availability

Source: PyPI JSON plus wheel METADATA.

**At their latest versions, all five packages have wheels for all 15 CI cells.** Those cells are manylinux x86_64, macOS arm64 and win_amd64, each on cp310–cp314.

| Package | Latest (upload date) | requires_python | Wheels | sdist |
|---|---|---|---|---|
| connectorx | 0.4.6 (2026-09-17) | >=3.10 | cp310–314 plus cp314t; manylinux_2_28 x86_64/aarch64, macosx_11_0_arm64, macosx_10_12_x86_64, win_amd64 | **none** |
| adbc-driver-manager | 1.12.0 (2026-07-28) | >=3.10 | cp310–314 plus cp314t; manylinux x86_64/aarch64, macOS arm64/x86_64, win_amd64 | yes |
| adbc-driver-sqlite | 1.12.0 | >=3.10 | `py3-none`; manylinux_2_28 x86_64/aarch64, macOS arm64/x86_64, win_amd64 | yes |
| adbc-driver-postgresql | 1.12.0 | >=3.10 | `py3-none`; same platforms plus manylinux_2_26 | yes |
| pyarrow | 25.0.1 (2026-08-10) | >=3.10 | cp310–314 plus cp314t; manylinux and musllinux x86_64/aarch64, macosx_12_0 arm64/x86_64, win_amd64 | yes |

Linux aarch64 and macOS x86_64 are also covered for all five.

What decides the dependency:
- **connectorx has never shipped an sdist** (checked 0.4.1–0.4.6).
  - cp314 wheels start at **0.4.5** (2026-01-18); 0.4.1–0.4.4 are cp310–cp313 only. Use a floor of `connectorx>=0.4.5`.
  - There are no musllinux or Windows-ARM wheels, so it can't be installed on Alpine or Windows ARM. That limits users, not CI.
- **connectorx 0.4.6 declares no `Requires-Dist` at all**, yet needs pyarrow at runtime. The extra must list pyarrow itself.
- **pyarrow floor:**
  - `uv sync --all-extras` resolves all extras together.
  - `uv.lock` pins pyarrow 23.0.1 for Python ≥3.14 and 25.0.1 below that (lines 3989-3999, 4049-4051), because of snowflake's `"pyarrow>=14.0.1,<24; python_version >= '3.14'"` (`pyproject.toml` 37-44).
  - A new floor must admit 23.x; `>=14.0.1` does.
- **The ADBC route can skip pyarrow** with `adbc-driver-manager>=1.7.0`. Note that polars' own `adbc` extra uses `adbc-driver-manager[dbapi]` and `adbc-driver-sqlite[dbapi]`, which pull in pandas and pyarrow>=14.0.1.
- **ADBC driver coverage on PyPI:**
  - Present: postgresql, sqlite, snowflake, bigquery, flightsql. The duckdb driver ships inside `duckdb`.
  - Missing (404): `adbc-driver-mysql`, `adbc-driver-mssql`, `adbc-driver-sqlserver`.
  - `adbc-driver-oracle` 1.0.0 is third-party (gizmodata); it requires pyarrow>=14.0.1 and adbc-driver-manager>=1.11.0.
  - So covering Postgres, MySQL, SQL Server, SQLite and Oracle with one pip-installable engine means connectorx.
- **No `python_version` or platform markers are needed for CI.**

## 5. SQLite feasibility

**Observed: stdlib `sqlite3` with `pl.read_database`** (SQLite 3.45.1, `-W error`, no warnings raised). No extra dependencies are needed.
- `cursor.description` carries no types (`('id', None, None, None, None, None, None)`), so dtypes come from inferring the first 100 rows.
- For `id INTEGER, amount REAL, name TEXT, created DATE, flag BOOLEAN, qty NUMERIC, big INTEGER, allnull TEXT` the schema was `{'id': Int64, 'amount': Float64, 'name': String, 'created': String, 'flag': Int64, 'qty': Float64, 'big': Int64, 'allnull': Null}`.
  - NULLs are preserved.
  - 2^53+1 stays exact.
  - NUMERIC holding both 3 and 1.25 became Float64.
  - The DATE stored as text stays String; BOOLEAN comes back as 0/1 Int64.
- **A zero-row result makes every column `Null`,** so schema probes on empty tables lose all types.
- **150 leading NULLs followed by text** fails with `ComputeError: could not append value: "x" of type: str to the builder; ... consider increasing infer_schema_length`. `infer_schema_length=None` or a `schema_overrides` entry fixes it.
- A column holding int, text and real values becomes String.
- **`schema_overrides={'created': pl.Date}` (or `pl.Datetime`) on ISO text fails** with the same ComputeError. Dates have to be parsed after the read, e.g. with `.str.to_date(...)` or Veridelta's `datetime_format` rule.

**Hazards for test fixtures under `filterwarnings = ["error"]`** (`pyproject.toml:209`), observed with stdlib only on system Pythons 3.10, 3.12 and 3.13:
- **Python 3.12+:** binding a `datetime.date` parameter emits `DeprecationWarning: The default date adapter is deprecated as of Python 3.12`. Bind ISO strings instead.
- **Python 3.13 only:** a connection garbage-collected while open emits `ResourceWarning: unclosed database`. pytest reports that as an unraisable-exception warning, which `error` turns into a failure. `with sqlite3.connect(...)` does **not** close the connection; only `.close()` or `contextlib.closing` avoided the warning.

**connectorx and ADBC (documented, not observed):**
- **connectorx SQLite** (`docs/databases/sqlite.md` in its repo):
  - Types come from the declared column type: int→int64, bool→bool, real/float/double→float64, char/clob/text→string, `date` and `datetime/timestamp`→datetime, `time`→object.
  - Columns with no declared type use the first row's value type, and it will "Throw an error if first rows of all partitions are NULL for a column."
  - URI form is `'sqlite://' + path`. Windows uses `urllib.parse.quote(path)`.
- **ADBC SQLite** (`docs/source/driver/sqlite.rst`): "The inferred type of each column begins as INT64, and will convert to DOUBLE, then STRING" within the first batch (`adbc.sqlite.query.batch_rows`). There is no date type, and later batches that don't fit raise an error.
- **URI trap** (I evaluated polars' regex from `_utils.py` 184-185):

| URI | polars ADBC opens | connectorx opens |
|---|---|---|
| `sqlite:///abs/path.db` | `abs/path.db` (relative to cwd) | `/abs/path.db` |
| `sqlite:////abs/path.db` | `/abs/path.db` | — |
| `sqlite:///C:/data/x.db` | `C:/data/x.db` | — |
| `sqlite:///:memory:` | `:memory:` | — |

  The same string names different files depending on the engine.

**To run the real engine test** where installs are allowed (use a scratch venv, not the project venv):
```
uv venv -p 3.12 <scratch>/.venv-db && uv pip install -p <scratch>/.venv-db polars connectorx pyarrow adbc-driver-sqlite
<scratch>/.venv-db/bin/python -W error -c "...build t.db with sqlite3 + contextlib.closing...;
  pl.read_database_uri('SELECT * FROM t', 'sqlite://' + abs_path)                 # connectorx
  pl.read_database_uri('SELECT * FROM t', 'sqlite:///' + abs_path, engine='adbc') # 4 slashes on POSIX"
```

## 6. Docs, doc-coverage test and smoke tests

**`/home/user/veridelta/docs/configuration.md`**
- "## Warehouse and lakehouse sources" runs 42-90:
  - the extras block (44-52);
  - credential advice (54);
  - routing and mixed-backend rules (56);
  - the `table` segment rule (58).
- The lakehouse YAML example is at 180-198, and the `storage_options` sentence at 200.
- "### Connection fields" (202-214) is a `type | Required | Optional` table with rows `file`, `snowflake`, `databricks`, `delta` (`table_uri` / `version`, `storage_options`) and `iceberg`.
- 214 covers strict ints and frozen blocks.
- 216 lists, per model, which credential fields are left out of printed configs.
- Environment variables are at 218-249; 249 says "Expanded values are text".
- Connector logging is at 251-260 and names both loggers.

**Extras and connector mentions elsewhere**
- The extras comment `# extras: snowflake, databricks, delta, iceberg, excel, fuzzy, all` is at `README.md:26` and `docs/index.md:21`.
- The "Connectors" bullet is at `README.md:19` and `docs/index.md:14`.
- Mermaid source nodes are at README 37-38 and index 30-31.
- The routing sentence is at README 52 and index 45.
- Extras are defined in `pyproject.toml` 36-62.
- `docs/api.md` auto-renders `veridelta.connectors`, `veridelta.models`, `veridelta.engine` and `veridelta.config`.
- `docs/roadmap.md` has nothing on databases to remove.

**`/home/user/veridelta/tests/unit/test_docs_coverage.py`**
- 22-30: `_CONFIG_MODELS = (DiffConfig, DiffRule, SourceConfig, SnowflakeConfig, DatabricksConfig, DeltaLakeConfig, IcebergConfig)`.
- 46-52: the check is a plain substring test (`if field not in guide`). Names like `type`, `table`, `uri` or `query` already appear in the guide, so they would pass without real documentation.
- `.cursor/rules/400-docs.mdc:19` names the same list of models.

**Other lists a new model must join**
- `tests/unit/test_models.py` 312-344 checks that validation errors never contain the secret.
- The same file at 359-422 checks that printed configs hide the secret (`field not in repr`), while it stays in `model_dump()` and still distinguishes two configs.

**Smoke tests:** `/home/user/veridelta/tests/smoke/test_installation.py`
- 44-55: every `veridelta.__all__` name must resolve, and `veridelta.__all__ == sorted(...)`. A `DatabaseConfig` would sort after `DataIntegrityError` and before `DatabricksConfig`.
- 57-71: imports the documented names.
- No test enumerates `connectors.__all__`.

**CI and type checking**
- The 100% branch-coverage gate lists connector modules by explicit path (`ci.yml:106`, `Makefile:19`). A new `connectors/database.py` is not gated unless it is added there.
- Every CI job runs `uv sync --all-extras` (`ci.yml` 29, 52, 73, 100, 137, 158); the matrix is at `ci.yml` 85-86.
- mypy's optional-module override (`pyproject.toml` ~125-144, `follow_imports = "skip"`) would need the new driver modules if they are imported directly. The connectorx wheel ships `py.typed` and `connectorx.pyi`.