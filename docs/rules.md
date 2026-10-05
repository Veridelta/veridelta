# Rules

The `rules` array defines granular, per-column or regex-pattern tolerances. A rule selects columns by exact `column_names` or by a regular expression in `pattern`, and every other field is optional. When a column is named by more than one rule, the first rule listing it by exact name wins, then the first whose `pattern` matches. One rule governs each column, `ignore` included, so an exact-name rule keeps a column that a broader ignore `pattern` would otherwise drop. A renamed column answers to both spellings: a rule listing its target name wins, then the rule listing its source name. Local runs and warehouse pushdown resolve rules the same way.

## Rule fields

| Field | Description |
| :--- | :--- |
| `column_names` | Exact source column names this rule governs. |
| `pattern` | Regular expression matched against the start of each column name. |
| `absolute_tolerance` | Maximum absolute numeric difference. Overrides `default_absolute_tolerance`. Must be finite; use `ignore` to stop comparing a column. |
| `relative_tolerance` | Maximum relative numeric difference (`0.01` is 1%). Overrides `default_relative_tolerance`. Must be finite. |
| `max_levenshtein_distance` | Most single-character edits between two text values that still count as a match. Needs the `fuzzy` extra locally, and compiles for warehouse pushdown; see [Fuzzy Text Matching](#fuzzy-text-matching). |
| `min_jaro_winkler_similarity` | Lowest Jaro-Winkler similarity, above 0 and at most 1, between two text values that still counts as a match. Needs the `fuzzy` extra, and runs locally only. |
| `case_insensitive` | Lowercase text before comparing. |
| `whitespace_mode` | `none`, `left`, `right`, or `both`. Overrides `default_whitespace_mode`. |
| `regex_replace` | Mapping of regex pattern to replacement, applied in order to text columns. |
| `pad_zeros` | Stringify, then left-pad to this width. |
| `value_map` | Source-side crosswalk from legacy value to target value. |
| `null_values` | Sentinels coerced to NULL. Overrides `default_null_values`; must fit the column's type. |
| `treat_null_as_equal` | Whether `NULL == NULL` matches. Overrides `default_treat_null_as_equal`. |
| `datetime_format` | `strptime` pattern that parses text into timestamps. |
| `timezone` | Zone that timezone-aware timestamps are converted to. |
| `cast_to` | `Int64`, `Float64`, `String`, `Boolean`, `Date`, or `Datetime`. |
| `ignore` | Exclude the columns this rule governs from the comparison entirely. |
| `rename_to` | Target name for a single source column. |

## Transform order

Rules are not applied in the order you write them. Every column follows one fixed pipeline, and both the local engine and the warehouse compiler honor it, so a rule produces the same verdict wherever it runs:

1. Null sentinels (`null_values`)
2. Regex replace (`regex_replace`)
3. Whitespace, then case (`whitespace_mode`, `case_insensitive`)
4. Source-side value map (`value_map`)
5. Pad zeros (`pad_zeros`)
6. Datetime parsing, then timezone (`datetime_format`, `timezone`)
7. Explicit cast (`cast_to`)
8. Comparison (equality, numeric tolerance, or text similarity)
9. Null-safe equality (`treat_null_as_equal`)

Stages 1 through 7 normalize each dataset on its own, before any join, so they apply to primary keys as well, in local runs and warehouse pushdown alike: a `case_insensitive` rule on a key column changes how rows are matched, not just how they are compared. If normalizing a key collapses two rows into one, `DataIntegrityError` is raised rather than allowing a join explosion.

Stages 2, 3, and 4 operate on text and are skipped for non-string columns, so a global `default_whitespace_mode` is safe to set on a mixed schema. Stage 1 is filtered per column instead, as described below.

## Numeric tolerances

Bypass floating-point anomalies or acceptable system rounding differences.

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

A tolerance only loosens the comparison of finite values. `NaN` matches only `NaN`, and an infinity matches only the same infinity, however wide the tolerance. Integer differences are measured exactly, never wrapped around the column's type, so Int8 `100` and `-100` differ by 200.

## Null sentinels

Declare the placeholder values a system writes instead of NULL. Sentinels are not limited to text: a list can mix strings, numbers, and booleans.

Each sentinel is applied only to columns whose type can hold it. A text sentinel reaches string, categorical, and enum columns; a number reaches any numeric column, including decimals; a boolean reaches boolean columns only. Sentinels that do not fit a given column are dropped for that column alone, so one global list can cover a mixed schema without failing.

Quoting therefore carries meaning. `-999` is a numeric sentinel that nulls out `-999` in an integer column and is ignored on a text column, while `"-999"` is the text sentinel and behaves the other way around. List both if a value appears in both shapes.

```yaml
default_null_values: ["N/A", "", -999]

rules:
  - column_names: ["is_verified"]
    null_values: [false]
```

The distinction between the global default and an explicit rule is what happens when nothing fits. `default_null_values` is expected to span a mixed schema, so unusable combinations are skipped silently. An explicit `null_values` on a named column is a direct instruction, so if none of its sentinels can apply to that column's type, Veridelta raises `ConfigError` rather than silently doing nothing. Warehouse pushdown enforces the same rule against the probed schema.

`.nan` and `.inf` are rejected at load time. NaN never compares equal to itself, and infinity has no portable SQL literal.

## String normalization

Execute string mutations before type evaluation. Sanitization always precedes `cast_to`, so text is cleaned before it is coerced.

```yaml
rules:
  - column_names: ["user_email"]
    case_insensitive: true
    whitespace_mode: "both"

  - column_names: ["balance"]
    regex_replace:
      "\\$": ""  # Strip currency symbols before casting
    cast_to: "Float64"
```

`cast_to` accepts a fixed set of Polars type names: `Int64`, `Float64`, `String`, `Boolean`, `Date`, and `Datetime`. Anything else is rejected at load time. Casting a float to `Int64` truncates toward zero, matching Polars rather than SQL's rounding, on both the local and warehouse paths.

## Padding, dates, and timezones

Reconcile identifiers and timestamps that two systems store in different shapes.

`pad_zeros` left-pads to a fixed width. The value is stringified first, so a numeric `123` in one system matches a text `"00123"` in the other. The width must be a real integer: `pad_zeros: "5"` is rejected rather than quietly coerced.

`datetime_format` parses text into timestamps using a [strptime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) pattern, so the column is compared as a timestamp instead of as text. Values that do not fit the pattern become NULL, which counts as a mismatch unless `treat_null_as_equal` is set. Warehouse pushdown supports `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%f`, `%z`, and `%%`, separated by spaces or any of `-` `/` `:` `.` `,` `_` `T`. Anything else raises `ConfigError`.

`%f` is a fraction of a second, as in Python, so `.5` is half a second, and both engines read one to six digits. A local run also reads seven to nine digits, keeping microseconds, and parses a value with no fraction at all when the format writes `.%f`; a warehouse may read either as NULL.

`timezone` converts timestamps to a common zone before comparison. It requires timezone-aware data. Naive timestamps raise `ConfigError`, because assuming an origin zone would shift every value by a real offset without telling you. To normalize text timestamps that carry an offset, parse them first with a format containing `%z`.

Note that the conversion is a relabeling. Comparisons and casts read the underlying instant, not the wall-clock reading in the target zone, so a `timezone` rule cannot change a verdict on its own. Its value is in making two differently-zoned columns comparable and in rejecting data that carries no zone at all.

```yaml
rules:
  - column_names: ["account_number"]
    pad_zeros: 10

  - column_names: ["created_at"]
    datetime_format: "%Y-%m-%d %H:%M:%S%z"
    timezone: "UTC"
```

## Value maps

Translate legacy enumerations or system-specific codes to modern equivalents during evaluation.

```yaml
rules:
  - column_names: ["status_code"]
    value_map:
      "0": "INACTIVE"
      "1": "ACTIVE"
      "2": "PENDING"
```

### Proposing a value map

Veridelta can draft these entries from the data. `veridelta crosswalk` aligns, normalizes, and joins the configured datasets exactly as `run` does, then proposes each source value for the target value it lines up with:

```bash
veridelta crosswalk -c veridelta.yaml > proposed.yaml
```

The evidence goes to stderr and the rules to stdout, ready to paste into the configuration:

```text
gender: 2 new value_map entries
  'M' -> 'Male': 599 of 600 rows (99.8%)
  'F' -> 'Female': 400 of 400 rows (100.0%)
```

```yaml
rules:
- column_names:
  - gender
  value_map:
    M: Male
    F: Female
```

An entry needs two things:

- **Confidence**, `--min-confidence` (default 0.95): the share of the source value's joined rows whose target is the proposed value. Every row with that source value counts, including rows that already match and rows whose target is NULL, so an entry that would break a matching row pays for it. The floor must be above 0.5, which leaves at most one candidate per source value.
- **Support**, `--min-support` (default 5): how many rows agree, so a coincidence in a handful of rows is never proposed.

Values are read as the `value_map` stage sees them, after null sentinels, `regex_replace`, whitespace, and case folding, so a `case_insensitive` column gets lowercase entries. Only compared text columns qualify, and only when nothing after stage 4 changes the mapped value: a column with `pad_zeros`, `datetime_format`, or a `cast_to` other than `String` is skipped, as are primary keys and ignored columns. A non-text target is read as the text it is compared as, so a `Y`/`N` source against a `1`/`0` target proposes `Y: '1'`, unless `strict_types` rules the pair out.

An existing `value_map` is kept and extended. Rows it already translates are left out of the counts, so a raw value that equals one of its outputs cannot receive an entry. Only one rule governs a column, so when a rule already governs one, the command says so on stderr, even with `--quiet`, and the new entries belong in that rule's `value_map` rather than in a second rule. If that rule also governs other columns, by listing several names or by a `pattern`, the column needs a rule of its own first, since a map merged into a shared rule applies to every column it governs; the note says which case applies.

`--sample-fraction` (default 1.0) reads that share of source rows, picked by a hash of the primary keys, so rerunning on the same data under one Polars version samples the same rows. `--json` prints each proposal with its evidence instead of YAML.

Two tables on one warehouse connection are counted in the warehouse, and no row leaves it. The connection requirements are those of a warehouse run: both sides use one backend, one connection, and two different tables, and a warehouse paired with a file or a database is refused. After the column probes and the duplicate-key checks, one statement counts every candidate column, and Veridelta applies the confidence floor itself, so a warehouse proposes exactly what a local run would from the same rows. Two differences remain:

- Only columns stored as text on both sides qualify. A local run also proposes text for a non-text target, such as `Y: '1'`, but each engine writes numbers and timestamps as text its own way, and the warehouse compares such an entry against the integer column rather than its text.
- A sample hashes the normalized keys with the warehouse's own hash function. Rerunning against the same tables samples the same rows, but not the rows a local run of the same fraction would.

From Python, `DiffEngine(config, source, target).propose_value_maps()` returns `ValueMapProposal` objects, and `DiffEngine.propose_value_maps_from_configs(diff, source, target)` loads a YAML pair first. Each proposal's `to_rule()` returns the standalone rule.

## Excluding columns

Explicitly drop volatile or irrelevant columns (e.g., auto-generated timestamps) from the comparison matrix.

```yaml
rules:
  - pattern: "^etl_loaded_at_.*"
    ignore: true
```

## Renaming columns

`rename_to` maps a source column onto a different target name before comparison. Use it when the same field was renamed between systems.

```yaml
rules:
  - column_names: ["legacy_customer_id"]
    rename_to: "customer_id"
```

The rule's other settings apply to the renamed pair on both sides, so a rename can carry a tolerance or a transform:

```yaml
rules:
  - column_names: ["legacy_amt"]
    rename_to: "amount"
    absolute_tolerance: 0.01
```

`primary_keys` are written with the target spelling, so a renamed key works in local runs and warehouse pushdown alike; pushdown reads it from the source under its stored name.

## Fuzzy text matching

Forgive typos in free text, such as names keyed by hand into two systems, without writing a `regex_replace` for each one. Scores come from [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz), an optional extra:

```bash
uv add 'veridelta[fuzzy]'
```

A rule sets one of two limits:

```yaml
rules:
  - column_names: ["customer_name"]
    case_insensitive: true
    max_levenshtein_distance: 1
  - column_names: ["city"]
    min_jaro_winkler_similarity: 0.95
```

- `max_levenshtein_distance` counts single-character insertions, deletions, and substitutions. `Jon` and `John` are one edit apart, so they match at `1`; `kitten` and `sitting` need `3`.
- `min_jaro_winkler_similarity` scores two values from 0 to 1 and rewards a shared prefix. `MARTHA` and `MARHTA` score 0.961, so they match at `0.96` but not at `0.97`.

A limit loosens only columns that are text after normalization, just as a tolerance loosens only numbers. A column cast to a number or parsed as a date is still compared exactly, while `pad_zeros` or `cast_to: String` make a column text. Equal values still match outright, and a missing value is never similar to anything, so `treat_null_as_equal` alone decides NULLs. Scores are case-sensitive: `case_insensitive` lowercases both sides first, in stage 3, which puts `ABD` and `abc` one edit apart. Primary keys are never loosened, because rows join on equal keys.

A rule sets at most one of the two limits, and neither has a global default, since loosening every text column would also forgive identifiers and codes that must match exactly. Without the extra, a run that needs a score raises `ConfigError` with the install command before comparing any rows.

Warehouse pushdown compiles `max_levenshtein_distance` to Snowflake's `EDITDISTANCE` or Databricks' `levenshtein`, which count characters as a local run does, so a warehouse run needs no extra. It refuses `min_jaro_winkler_similarity` with `ConfigError` before any comparison query runs: Snowflake's `JAROWINKLER_SIMILARITY` ignores case and returns a whole number from 0 to 100, and Databricks has no Jaro-Winkler function, so neither can reproduce a local verdict.
