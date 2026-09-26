I ran a DuckDB prototype, and it reproduces the local crosswalk numbers exactly on columns stored as text on both sides. My recommendations: one SQL statement for all columns, keep `min_confidence` out of SQL, and limit warehouse crosswalks to text targets.

Plan mode was active, so I created nothing on disk: no scratch directory and no script files. The prototypes ran as inline `PYTHONDONTWRITEBYTECODE=1 /home/user/veridelta/.venv/bin/python -c '...'` against in-memory DuckDB 1.5.5 and Polars 1.40.1, and the repo is unchanged. The Snowflake, Databricks and Spark doc sites are blocked by the proxy and no Spark is installed, so every Snowflake or Databricks claim below is marked **unverified recall**.

**File legend** (short names used below):
- `engine.py` = /home/user/veridelta/src/veridelta/engine.py
- `sql.py` = /home/user/veridelta/src/veridelta/connectors/sql.py
- `models.py` = /home/user/veridelta/src/veridelta/models.py
- `cli.py` = /home/user/veridelta/src/veridelta/cli.py
- `base.py` = /home/user/veridelta/src/veridelta/connectors/base.py
- `harness` = /home/user/veridelta/tests/integration/duckdb_harness.py
- `config.md` = /home/user/veridelta/docs/configuration.md
- `test_engine.py` = /home/user/veridelta/tests/unit/test_engine.py

---

## 1. The local algorithm

**Constants**
- `DEFAULT_MIN_CONFIDENCE = 0.95` (engine.py:1541-1544). The floor is above 0.5, so at most one target can qualify per source value.
- `DEFAULT_MIN_SUPPORT = 5` (1546-1547).
- `_SAMPLE_BUCKETS = 1_000_000` (1549-1551).

**`_check_value_map_thresholds`** (1554-1575)
- Range checks only: `0.5 < min_confidence <= 1`, `min_support >= 1`, `0 < sample_fraction <= 1`. NaN fails the chained comparison.
- There is no type check. I verified it accepts `True` for both thresholds, and also `np.float64`, `np.float32`, `Decimal("0.95")` and `np.int64(5)`. This is an existing local quirk.

**`propose_value_maps`** (1867-1931), in order:
1. Checks thresholds (1902), then works on a copy (1903).
2. Runs `_align_structure` and `_validate_schema` (1904-1905).
3. Records `stored`, the source schema after rename and drop but before normalization (1906).
4. Runs `_normalize_frame` on both sides (1907-1908, body at 2033-2082).
5. Runs `_check_uniqueness` on the normalized keys (1909, 2014-2031). This happens before the "no candidate columns, return `[]`" exit (1911-1913).
6. Joins (`_value_map_join`, 1914), runs `pl.collect_all` over one `_value_map_query` per column (1915-1926), then builds proposals (1927-1931).

**`_value_map_columns`** (1933-1957) picks candidate columns:
- Walks `stored` in source order and skips primary keys, columns missing from the target, and ignored columns.
- Requires `_compares_mapped_text` (1578-1597):
  - the stored source dtype is exactly `pl.String` (Categorical and Enum are excluded);
  - `pad_zeros is None`;
  - no `datetime_format`;
  - `cast_to` is `None` or `"String"`.
- Under `strict_types`, the **normalized** target dtype must be String (1943, 1951-1955). So a non-text target with `cast_to: String` still qualifies.

**What the source value has seen.** The task framing says stages 1-3; that is not quite right. The source frame has also passed stage 4:
- `_normalize_frame(is_source=True)` applies `expr.replace(value_map)` (2173-2175).
- Stages 5 and 6a cannot fire for a candidate.
- A `timezone` rule on a String column raises `ConfigError` (2229-2233).
- `cast_to: String` is a no-op.

Survivors still equal their stage-3 value (sentinels → regex → whitespace, which strips all whitespace, → lowercase). That is only because `_value_map_query` drops rows whose stage-4 value is in the existing map's outputs (1637):
- a stage-3 value that is a map **key** becomes an output and is dropped;
- a raw value that **equals** an output is dropped too.

The docs mention this second case (config.md:465).

**What the target value has seen**
- Stage 1: sentinels filtered to the target dtype (`usable_sentinels`, 2108-2113).
- Stages 2-3 only if the target is String (2106, 2115). There is no stage 4.
- An optional stage-7 cast.
- Then `cast(pl.String, strict=False)` (1635). This mirrors the soft cast in `_build_match_expr` (2281-2282).

**Counting** (`_value_map_query`, 1600-1648)
- The source is sampled first by `pl.struct(keys).hash(seed=0) % 1e6 < round(fraction * 1e6)` on the **normalized** source keys, only when the fraction is below 1. Then it is inner-joined with the target (1976-1979).
- Excluded from all counts:
  - null source rows, including values a sentinel turned into NULL;
  - rows whose value is an existing map output;
  - rows with no match on the other side.
- `agreeing_rows = pl.len()` per (source, target) pair (1638-1639). A NULL target forms its own group.
- `rows = sum(agreeing_rows) over source_value` (1640). This **includes identity rows and null-target rows**; they count but are never proposed.
- The final filter keeps pairs where:
  - `target != source` (a NULL target is dropped, and so are identity pairs);
  - `agreeing >= min_support`;
  - `agreeing / rows >= min_confidence`, as Float64 division (1642-1646).
- Sort order is `agreeing_rows` descending, then `source_value` ascending (byte order) (1647).
- Reported confidence is the computed field `agreeing_rows / rows` (models.py:841-849).

**Models**
- `ValueMapEntry` (models.py:818-865): `extra="forbid"` and frozen (832); `rows >= 1`, `agreeing_rows >= 1`; a validator enforces `agreeing_rows <= rows`.
- `ValueMapProposal` (868-900):
  - `column` is the name after rename;
  - `value_map` is `{**existing, **new}`;
  - `entries` are the new ones only;
  - `governing_rule_index`;
  - `to_rule()` returns `DiffRule(column_names=[column], value_map=...)`.

**Governing rule** (`_value_map_proposal`, 1981-2001)
- Found with `_match_rule(config.rules, column)` on the post-rename name.
- Precedence comes from `_rule_spellings` (488-510, 513-535): exact target spelling first, then the source spelling (unless that spelling is itself renamed away), then the first matching pattern.
- `governing_rule_index = rules.index(governing)`, or None when no rule matches.

**CLI `crosswalk`** (cli.py:306-341; parser at 397-442)
- Argument validators: `_confidence` (0.5, 1] at 90-105, `_share` (0, 1] at 108-123, `_support` int ≥ 1 at 126-144.
- Calls `propose_value_maps_from_configs`. Errors go to `_report_failure` with exit 1 (147-175).
- `--json` prints `model_dump(mode="json")`. Otherwise the evidence goes to stderr and the YAML rules to stdout (224-303). The governing-rule note is printed even with `--quiet`.
- Exits 0 either way.

## 2. Reusable compiler machinery

**`_normalize_expr`** (sql.py:764-796), stage by stage:

| Stage | Method |
|---|---|
| 1. Null sentinels | `_apply_null_values` (1017-1035); sentinels chosen by `_sentinels_for` (1058-1073, filtered to the probed dtype) and rendered by `_sentinel_literal` (1037-1056) |
| 2-3. Text stages, only if `_is_text_side` (1156-1172: dtype None or String) | `_apply_regex_replace` (1075-1092), `_apply_whitespace` (1094-1111: `LTRIM`/`RTRIM`/`TRIM`, spaces only), `_apply_case` (1113-1125, `LOWER`) |
| 4. Value map, source side only | `_apply_value_map` (1127-1154): nested `IFF` on Snowflake, `CASE` elsewhere |
| 5. Pad zeros | `_apply_pad_zeros` (1174-1206) |
| 6a. Datetime parsing | `_apply_datetime_format` (1208-1231) |
| 6b. Timezone | Emits nothing; the precondition is enforced in the engine (engine.py:137-172, 662-692) |
| 7. Cast | `_apply_cast` (1298-1321) |

**Can it stop after stage 3?** There is no parameter for that, and none is needed:
- For every column `_compares_mapped_text` admits, stages 5-6b emit nothing, and stage 7 emits at most a no-op `CAST(x AS VARCHAR|STRING)`.
- Keeping stage 4 is correct: the exclusion simply becomes `NOT IN (<map outputs>)`.
- The prototype reused the compiler's normalization unchanged and matched the local numbers.

**Building blocks**
- `_normalized_select` (902-935): the source reads stored names and the target reads post-rename names; both project under the post-rename name.
- `_normalized_with_clause` (862-900) emits `WITH "_src_normalized" AS (…), "_tgt_normalized" AS (…)` with no trailing keyword. It can be extended with `, "_veridelta_joined" AS (…)`.
- `_join_on_clause` (746-762) is plain `=` equality, so NULL keys never join, as in Polars. `_normalized_join` (723-744) builds the FROM/JOIN.
- `_key_columns` (798-829) maps each key to (stored name, key name, rule or None) and raises `ConnectorError` on a bad key rule.
- `compile_duplicate_key_query` (587-630): selects the normalized keys of one side (the source side reads stored names and applies the value map), then `GROUP BY keys HAVING COUNT(*) > 1` and `COALESCE(SUM(...), 0)`.
- Security primitives:
  - `_quote_ident` (937-952) allowlists, then quotes;
  - `_literal` (987-1004);
  - `_number` (1006-1015) is just `repr(value)`, which is **not strict**.

**How keys are normalized in joins**
- `_resolve_pushdown_keys` (engine.py:819-858) builds one `_pushdown_rule` (765-816) per key. It carries stages 1-7, with the stored name found via `_rename_pairs`.
- The compiler projects keys first (`[*keys, *compared]`, sql.py:365-373), so joins compare normalized keys, and source keys get the value map.

**How the engine resolves rules and runs pushdown**
- `_validate_pushdown_schema` (1296-1333): runs zero-row probes, refuses header normalization that would rename a stored column (1268-1293), calls `DiffEngine.validate_schemas`, and returns the raw schemas.
- `_resolve_pushdown_rules` (861-926): skips keys, missing and ignored columns; folds defaults; checks preconditions; gates tolerance and edit distance by type. **It refuses `min_jaro_winkler_similarity` on text columns (911-912).**
- `_collect_pushdown_summary` (1336-1459) runs in this order: schema, key rules, rules, duplicate-key checks (source then target, 1229-1265), counts, joins, tally.
- `_run_warehouse_pushdown` (1484-1538):
  1. mixed file/warehouse backends raise `ConnectorError`;
  2. `type(source) is not type(target)` raises (cross-dialect);
  3. connection fingerprints must match (430-465);
  4. `_reject_self_comparison` raises `ConfigError` for the same table (1462-1481);
  5. connect, run, and close in `finally`.

## 3. Design sketch: `compile_value_map_query`

**Signature**

```python
def compile_value_map_query(self, source_table, target_table, primary_keys, rules, *,
    min_support: int, sample_fraction: float = 1.0, source_alias="src", target_alias="tgt",
    source_types=None, target_types=None, key_rules=None) -> str | None
```

- `rules` holds one resolved rule per candidate column, in source order.
- It returns None when there are no candidates, as `compile_column_mismatch_query` does.
- It should be split into small helpers (joined CTE, per-column branch, sample predicate, per-dialect bucket, strict integer literal) to stay under the C901 limit of 10 (pyproject.toml:185-186).

**Shape: one statement, `UNION ALL` of per-column branches over one joined CTE.** DuckDB spelling:

```sql
WITH "_src_normalized" AS (…), "_tgt_normalized" AS (…)            -- _normalized_with_clause(keys + candidates)
, "_veridelta_joined" AS (
  SELECT "src"."gender" AS "_veridelta_source_0", CAST("tgt"."gender" AS VARCHAR) AS "_veridelta_target_0", …
  FROM "_src_normalized" AS "src" INNER JOIN "_tgt_normalized" AS "tgt" ON <_join_on_clause>
  [WHERE <bucket(src normalized keys)> < <cutoff>])                   -- only when sample_fraction < 1
SELECT "_veridelta_column", "source_value", "target_value",
       CAST("rows" AS BIGINT) AS "rows", CAST("agreeing_rows" AS BIGINT) AS "agreeing_rows"
FROM (SELECT *, SUM("agreeing_rows") OVER (PARTITION BY "_veridelta_column", "source_value") AS "rows"
      FROM (SELECT "_veridelta_column", "source_value", "target_value", COUNT(*) AS "agreeing_rows"
            FROM (SELECT 0 AS "_veridelta_column", "_veridelta_source_0" AS "source_value", "_veridelta_target_0" AS "target_value"
                    FROM "_veridelta_joined" WHERE "_veridelta_source_0" IS NOT NULL [AND … NOT IN (<_literal outputs>)]
                  UNION ALL SELECT 1, … ) AS "_veridelta_pairs"
            GROUP BY 1, 2, 3) AS "_veridelta_groups") AS "_veridelta_counts"
WHERE "target_value" <> "source_value" AND "agreeing_rows" >= <min_support> AND 2 * "agreeing_rows" > "rows"
```

Why each piece is there:
- **Column labels are integers** (compiler-generated), so no column name ever becomes a string literal.
- **The count happens in a subquery and the window in the outer query.** That is the portable form.
- **`<>` drops NULL targets and identity pairs.** NULL targets are still a `GROUP BY` group, so they count toward `rows`, exactly as in Polars.
- **Counts are cast to BIGINT** via `_cast_keyword("Int64")`. DuckDB's SUM is HUGEINT, which arrives as Decimal(38,0) (verified). Snowflake NUMBER(38,0) arrives as Decimal128, per the repo's own note in sentinels.py.
- **The target is cast to text** with `_cast_keyword("String")`, which gives VARCHAR / STRING / VARCHAR. Under the text-only rule below this is a no-op.
- **Per-column statements** would be simpler (the same tail with one branch) but cost one join per column.
- **Joined-CTE reuse:** Snowflake usually computes a CTE referenced several times only once. Spark inlines deterministic CTEs, but that only affects performance, not correctness (unverified recall).

**Thresholds**
- **Option (a), not recommended: render `min_confidence` in SQL.** The form would be `CAST(agreeing AS DOUBLE)/CAST(rows AS DOUBLE) >= <literal>`, with both sides cast because Snowflake integer `/` is fixed-point (unverified recall). The literal is the problem:
  - `_number` is `repr`, and DuckDB types `0.95` as DECIMAL(3,2).
  - Across 2003 random floats in (0.5, 1], **198 did not round-trip** through `CAST(repr AS DOUBLE)`. **0 failed** as a `format(v, ".17e")` literal, which DuckDB reads directly as a DOUBLE.
  - Databricks and Snowflake exponent-literal typing is unverified.
  - A float filter in SQL that drops a boundary row cannot be repaired afterwards.
- **Option (b), recommended: keep `min_confidence` out of SQL.**
  - SQL applies only exact predicates: `agreeing >= min_support`, `2*agreeing > rows`, and `target <> source`.
  - Since `min_confidence > 0.5`, 0.5 is exactly representable and IEEE division rounds monotonically, `agreeing/rows >= c` implies `2*agreeing > rows`. So the SQL result is a guaranteed superset, with at most one row per (column, source value).
  - The engine then applies the exact local filter and sort in Polars, via a helper factored out of engine.py:1642-1647 and shared with `_value_map_query`. That makes the decisions identical by construction.
- **Strict numbers.** Under (b), only `min_support` and compiler-generated integers (column labels, bucket count, cutoff) reach SQL.
  - Add a strict integer renderer in sql.py: reject `bool` and anything that is not an `int`, and emit `str(int(v))`.
  - Do not use `_number`. I verified it renders `True`, `np.int64(5)`, `np.float64(0.95)` and `Decimal('0.95')` as those exact texts.
  - Also add type checks to `_check_value_map_thresholds` (engine.py:1554).

**Deterministic sampling**
- Emit the filter only when the fraction is below 1, with `cutoff = round(fraction * SAMPLE_BUCKETS)` (move the constant into sql.py, or pass integers in).
- Hash the normalized source keys inside `_veridelta_joined`, which is what the local path hashes.
- Put the function names in a module-level table and make the structural difference an explicit per-dialect branch, as `300-security.mdc` requires:

| Dialect | Bucket expression | Status |
|---|---|---|
| Snowflake | `MOD(MOD(HASH(k1, k2), 1000000) + 1000000, 1000000)` | HASH is signed 64-bit (unverified recall) |
| Databricks | `pmod(xxhash64(k1, k2), 1000000)` | signed BIGINT, seed 42; avoid `abs()` (unverified recall) |
| DuckDB | `hash(k1, k2) % 1000000` | returns UBIGINT (verified) |

A warehouse sample can never match the Polars hash sample. Parity tests therefore use `sample_fraction=1.0`, and sampled tests assert only repeatability, `0 < rows < total`, and the SQL spelling.

**Recommend text-only candidates in the warehouse**: stored target dtype `pl.String` as well as stored source. This replaces the `strict_types` branch. Evidence is in section 4. It is a deliberate difference from local runs: with `strict_types=False`, local proposes `Y: '1'` and the warehouse proposes nothing.

**Engine-side result handling**
- Require all five columns; cast the column label and counts to Int64 and the values to String (Snowflake strings may arrive as Categorical).
- Raise `ConnectorError` on a malformed result, following `_column_mismatches_from_frame` (929-956).
- Split by column label and **drop the label before `ValueMapEntry(**row)`**, because `extra="forbid"` rejects it.
- Build proposals with `_value_map_proposal`, moved to module level (it only uses `self.config`).

**Decision needed:** reusing `_resolve_pushdown_rules` brings its Jaro-Winkler refusal (911-912). The warehouse crosswalk would then refuse a config the local crosswalk accepts. The alternative is to factor out its skip/fold/precondition loop and add the candidate filter without that refusal.

**Existing caveats that carry over** (they already apply to warehouse `run`):
- `TRIM` removes only spaces (config.md:73).
- Regex dialects differ.
- A Snowflake VARCHAR delivered as Categorical fails the String check.
- Non-binary collations would merge `GROUP BY` groups and change what `<>` compares.

## 4. Prototype results (DuckDB, in-memory)

**Fixture**
- Composite key: `tenant` plus `code`, where `code` has a `case_insensitive` key rule (source `K0`, target `k0`). There is one source-only and one target-only row.
- `gender` (no rule):

  | Source → target | Rows |
  |---|---|
  | M → Male / M → Man | 19 / 1 |
  | F → Female | 10 |
  | U → Unknown / U → NULL | 7 / 1 |
  | X → X (identity) | 6 |
  | NULL → Male | 3 |
  | Q → Queued / Q → Quit | 3 / 2 |

- `status`: rule with `case_insensitive`, whitespace `both`, and `null_values: ['N/A']`.
- `tier`: rule with `value_map {E: Enterprise}`, plus raw `Enterprise` values that equal the map's output.
- `b95`: 95 of 100 rows agree.
- Non-text targets: Int64, Float64, Boolean, Date, Datetime.
- Thresholds: `min_confidence=0.95`, `min_support=3`.
- Method: `_resolve_pushdown_keys`/`_resolve_pushdown_rules` on DuckDB-probed schemas, the compiler's real `_normalized_with_clause`, `_key_columns` and `_join_on_clause`, plus the hand-written tail above.

**Local output** (16 entries), including:
- `('gender','M','Male',20,19)` (exactly at the 0.95 boundary) and `('gender','F','Female',10,10)`;
- `('status','a','active',7,7)`;
- `('tier','P','Premium',5,5)`, with map `{E: Enterprise, P: Premium}` and governing index 2;
- `('b95','k','K1',100,95)`.

U (7 of 8), X (identity), Q (3 of 5), and all excluded rows produced nothing, as expected.

**Results**
- **Text-target columns (gender, status, tier, b95): identical to local** under both options (a) and (b).
- **All columns including non-text targets:** identical except the timestamp. Local gives `'2024-01-02 03:04:05.000000'`, DuckDB gives `'2024-01-02 03:04:05'`.
- **Entry order:** the SQL `ORDER BY` matched the Polars sort on a fixture full of ties (`c, C, a, b, d`; binary byte order).
- **Sampling** used the compiled statement, hashing normalized keys, over 20k rows. It repeats identically, and at 1.0 it equals local. At 0.5, DuckDB gave M 9,804 / m 239 against local M 9,800 / m 252.
- **Cast-to-text, Polars vs DuckDB:** they agree on Int, Float32, Decimal, Boolean (`'true'`), Date, `1.0`, `0.30000000000000004` and `1e+20`. They **differ** on:

  | Value | Polars | DuckDB |
  |---|---|---|
  | `1e-7` | `'1e-7'` | `'1e-07'` |
  | NaN | `'NaN'` | `'nan'` |
  | Datetime, no fraction | `'…05.000000'` | `'…05'` |
  | Datetime, `.12` seconds | `'…05.120000'` | `'…05.12'` |
  | Datetime, ms unit | `'…05.000'` | `'…05'` |
  | Datetime, UTC | `'…+00:00'` | `'…+00'` |
  | Time `03:04:05.25` | `'03:04:05'` | `'03:04:05.25'` |

  Unverified recall for the real warehouses:
  - Snowflake: FLOAT `1.0` → `'1'`; timestamps follow the `TIMESTAMP_*_OUTPUT_FORMAT` session setting.
  - Databricks: DOUBLE uses Java-style formatting (`'1.0E20'`, and `'1.23456789125E8'` from 1e7 up); fractional seconds are trimmed.
- **Pasting non-text proposals and running pushdown in DuckDB:** the warehouse parses the source text into the target's type, whereas Polars turns the target into text. One unmapped `'N'` against an Int64 target aborted the whole statement ("Could not convert string 'N' to INT64").

**Conclusion:** restrict warehouse crosswalks to columns stored as text on both sides.

## 5. Where the entry points change

- **`propose_value_maps_from_configs`**: replace the refusal at engine.py:1854-1859.
  - Check thresholds first, so bad values fail before a session opens.
  - Then route warehouse pairs to a new `_collect_value_map_proposals`. It should mirror `_collect_pushdown_summary`: schema, key rules, rules, duplicate checks on both sides (before any "no candidates" exit), candidates, one statement, parse, Polars filter.
  - Five statements per run: two probes, two duplicate-key checks, one value-map statement.
- **Session helper**: factor the mixed-backend, dialect, fingerprint, same-table and connect/close checks out of `_run_warehouse_pushdown` (1503-1538) into a generic `_with_warehouse_session(source, target, work)` shared with `run_from_configs`.
- **`base.py:14-16`**: add a `"value_maps"` member to `PushdownQueryType`. The docstring list at base.py:92 already misses `duplicates`.
- **`sql.py`**: the new method, alias constants, and the hash table; update the statement list in the class docstring (220-230).
- **Docstrings in engine.py**: the class bullet at 1744-1747 ("for file and lakehouse `SourceRef` pairs"), the Args/Raises at 1837-1852, and the "one Polars version" wording at 1889-1891.
- **CLI**: `crosswalk` (cli.py:306-341) stays unchanged; optionally reword the `--sample-fraction` help (429-430).
- **Docs**:
  - config.md:467 currently says: *"Warehouse sources raise `ConnectorError`: proposals read rows locally, so export the tables, or a sample of them, to Parquet first."* Replace it with the warehouse behaviour and the same connection requirements as :56.
  - Add a warehouse caveat to :463 (*"A non-text target is read as the text it is compared as, so a `Y`/`N` source against a `1`/`0` target proposes `Y: '1'`…"*).
  - Extend "one Polars version" in :467 to cover warehouse engine versions.
- **Tests**:
  - The exact string "Value map proposals read source and target rows locally" appears only at engine.py:1856; no test asserts it.
  - **`test_it_refuses_warehouse_sources_without_connecting`** (test_engine.py:2219-2236) pairs a Snowflake source with a file target and matches `"file or lakehouse sources"` (2231). It **will fail**, because mixed pairs will raise "Mixed file/lakehouse and warehouse backends are unsupported." Rewrite it as a mixed-backend test; `assert_not_called` still holds.
  - The CLI tests (tests/unit/test_cli.py:343-588, which mock the engine) and the e2e file test (tests/e2e/test_cli_workflow.py:168) are unaffected.
  - Add:
    - harness parity at `sample_fraction=1.0`, one case per `TestValueMapProposals` case (test_engine.py:1981-2217);
    - SQL string tests per dialect, including `_literal` escaping inside `NOT IN`;
    - routing tests with mocked connectors (fingerprint, dialect, same table, session closed on failure, malformed result);
    - rejection tests for non-strict numbers.
  - engine.py and sql.py are held to **100% branch coverage** (/home/user/veridelta/CHANGELOG.md:24-25), so every dialect branch, every rejection, the None path and the malformed-result path need a test.
- **CHANGELOG**: commitizen writes it on bump (/home/user/veridelta/pyproject.toml:233-243), so use a `feat:` commit and do not edit it by hand.
- **/home/user/veridelta/tests/unit/test_docs_coverage.py** only checks that config-model fields appear in the guide, so this change does not affect it.