# Rules

A rule changes how the columns it selects are compared. It can clean values before the comparison, loosen the comparison, rename a column, or leave a column out.

Rules are listed under `rules`. Each one selects columns by name or by pattern, then sets any of the fields below:

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
  - pattern: "^etl_"
    ignore: true
```

## Rule fields

The stage column gives each field's place in the [transform order](#transform-order):

| Field | Stage | Description |
| :--- | :--- | :--- |
| `column_names` | | Exact source column names the rule governs. |
| `pattern` | | Regular expression matched against the start of each column name. |
| `ignore` | | Whether to leave the governed columns out of the comparison. |
| `rename_to` | | Target name for a single source column. |
| `null_values` | 1 | Values to read as NULL. Overrides `default_null_values`, and must fit the column's type. |
| `regex_replace` | 2 | Map of regular expression to replacement, applied in order to text. |
| `whitespace_mode` | 3 | Whitespace to strip from text: `none`, `left`, `right`, or `both`. Overrides `default_whitespace_mode`. |
| `case_insensitive` | 3 | Whether to lowercase text before comparing. |
| `value_map` | 4 | Map from a source value to the target value it stands for. |
| `pad_zeros` | 5 | Width to pad each value to with leading zeros, after converting it to text. |
| `datetime_format` | 6 | `strptime` pattern that parses text into timestamps. |
| `timezone` | 6 | Zone to convert timezone-aware timestamps to. |
| `cast_to` | 7 | Type to convert to: `Int64`, `Float64`, `String`, `Boolean`, `Date`, or `Datetime`. |
| `absolute_tolerance` | 8 | Largest absolute difference between two numbers that still matches. Overrides `default_absolute_tolerance`. |
| `relative_tolerance` | 8 | Largest difference relative to the source value, where `0.01` is 1%. Overrides `default_relative_tolerance`. |
| `max_levenshtein_distance` | 8 | Most single-character edits between two text values that still match. |
| `min_jaro_winkler_similarity` | 8 | Lowest Jaro-Winkler similarity, above 0 and at most 1, between two text values that still match. Local runs only. |
| `treat_null_as_equal` | 9 | Whether two NULLs match. Overrides `default_treat_null_as_equal`. |

## Selecting columns

A rule selects columns with `column_names`, a list of exact source column names, or with `pattern`, a regular expression matched against the start of each name. Every other field is optional.

One rule governs each column. When several rules could select it, the first rule that lists it by name wins, then the first rule whose `pattern` matches it. An `ignore` rule follows the same order: a rule that names a column keeps it even when a broader `ignore` pattern matches it.

A renamed column answers to both of its names. A rule that lists its target name wins, then a rule that lists its source name. Local runs and pushdown resolve rules the same way.

## Excluding columns

`ignore: true` leaves the columns a rule governs out of the comparison. Use it for volatile columns, such as load timestamps:

```yaml
rules:
  - pattern: "^etl_loaded_at_.*"
    ignore: true
```

## Renaming columns

`rename_to` pairs a source column with a target column of a different name. Use it when a field was renamed between systems:

```yaml
rules:
  - column_names: ["legacy_customer_id"]
    rename_to: "customer_id"
```

The rule's other fields apply to the renamed pair on both sides. A rename can carry a tolerance or a transform:

```yaml
rules:
  - column_names: ["legacy_amt"]
    rename_to: "amount"
    absolute_tolerance: 0.01
```

Write a renamed primary key in `primary_keys` with its target name. Local runs and pushdown both accept it, and pushdown reads the source column under its stored name.

## Transform order

Rules apply in a fixed order, not the order they are written in. Each column passes through nine stages, in a local run and in a warehouse alike:

1. Null sentinels (`null_values`)
2. Regular expression replacement (`regex_replace`)
3. Whitespace, then case (`whitespace_mode`, `case_insensitive`)
4. Value map, source side only (`value_map`)
5. Zero padding (`pad_zeros`)
6. Date parsing, then timezone (`datetime_format`, `timezone`)
7. Cast (`cast_to`)
8. Comparison: equality, a numeric tolerance, or a text similarity limit
9. Null equality (`treat_null_as_equal`)

Stages 1 to 7 normalize each side on its own, before rows are paired. They apply to primary keys too: a `case_insensitive` rule on a key column changes which rows pair, not only how they compare. If normalized keys make two rows on one side share a key, `DataIntegrityError` is raised instead of a join that multiplies rows.

Stages 2, 3, and 4 apply to text columns and skip every other type: a global `default_whitespace_mode` is safe on a mixed schema. Stage 1 checks each sentinel against each column's type; see [Null sentinels](#null-sentinels).

## Null sentinels

`null_values` lists the placeholder values a system writes in place of NULL. A list can mix text, numbers, and booleans:

```yaml
default_null_values: ["N/A", "", -999]

rules:
  - column_names: ["is_verified"]
    null_values: [false]
```

Each sentinel applies only to columns whose type can hold it:

- Text reaches string, categorical, and enum columns.
- A number reaches any numeric column, decimals included.
- A boolean reaches boolean columns only.

Quoting therefore decides the type. `-999` is a number: it nulls `-999` in an integer column and is skipped on a text column. `"-999"` is text and behaves the other way around. List both if a value appears in both forms.

`default_null_values` spans a mixed schema, and it skips each sentinel that does not fit a column. An explicit `null_values` on a named column is a direct instruction: if none of its sentinels fits the column's type, Veridelta raises `ConfigError`. Pushdown applies the same check to the probed schema.

`.nan` and `.inf` are rejected when the file loads. NaN never equals itself, and infinity has no portable SQL literal.

## Regular expressions

`regex_replace` maps patterns to replacements and applies them in order to text columns. It runs before `cast_to`, which converts the cleaned text:

```yaml
rules:
  - column_names: ["balance"]
    regex_replace:
      "\\$": ""  # Strip currency symbols before casting
    cast_to: "Float64"
```

Patterns use the regular expression syntax of Polars, which has no look-around and no backreferences, unlike Python's `re`.

A replacement refers to capture groups as Polars does: `$1` or `${1}`, with `$0` for the whole match and `$$` for a dollar sign. `$1a` refers to a group named `1a`; write `${1}a` for group 1 followed by `a`. A backslash in a replacement is plain text. Pushdown rewrites each reference for the warehouse's SQL; see [Rules in SQL](pushdown.md#rules-in-sql).

## Whitespace and case

`whitespace_mode` strips whitespace from the `left` end, the `right` end, or `both` ends of text. It strips spaces, tabs, line breaks, no-break spaces, and every other Unicode whitespace character. `case_insensitive` then lowercases the text:

```yaml
rules:
  - column_names: ["user_email"]
    case_insensitive: true
    whitespace_mode: "both"
```

## Value maps

`value_map` translates source values into the target's terms before the comparison, such as legacy status codes into names. It applies to the source side of text columns. A value without an entry stays as it is:

```yaml
rules:
  - column_names: ["status_code"]
    value_map:
      "0": "INACTIVE"
      "1": "ACTIVE"
      "2": "PENDING"
```

The map reads values after stages 1 to 3: a `case_insensitive` column needs lowercase keys.

### Proposing a value map

`veridelta crosswalk` drafts `value_map` entries from the data. It aligns, normalizes, and pairs the configured datasets as `run` does, then proposes each source value for the target value it lines up with:

```bash
veridelta crosswalk -c veridelta.yaml > proposed.yaml
```

The evidence goes to stderr:

```text
gender: 2 new value_map entries
  'M' -> 'Male': 599 of 600 rows (99.8%)
  'F' -> 'Female': 400 of 400 rows (100.0%)
```

The rules go to stdout, ready to paste into the configuration:

```yaml
rules:
- column_names:
  - gender
  value_map:
    M: Male
    F: Female
```

An entry needs both of these:

- **Confidence**, set by `--min-confidence` (default 0.95): the share of the source value's paired rows whose target is the proposed value. Every row with that source value counts, including rows that already match and rows whose target is NULL. An entry that would break a matching row pays for it. The floor must be above 0.5, which leaves at most one candidate per source value.
- **Support**, set by `--min-support` (default 5): how many rows agree. A coincidence in a handful of rows is never proposed.

Values are read as the `value_map` stage sees them, after null sentinels, `regex_replace`, whitespace, and case. A `case_insensitive` column gets lowercase entries.

Only compared text columns qualify, and only when no later stage changes the mapped value. A column with `pad_zeros`, `datetime_format`, or a `cast_to` other than `String` is skipped, as are primary keys and ignored columns. A non-text target is read as the text it is compared as: a `Y`/`N` source against a `1`/`0` target proposes `Y: '1'`, unless `strict_types` rules the pair out.

An existing `value_map` is kept and extended. Rows it already translates are left out of the counts, so a raw value equal to one of its outputs cannot receive an entry.

One rule governs each column. When a rule already governs a column, the command says so on stderr, even with `--quiet`, and the new entries belong in that rule's `value_map`. When that rule also governs other columns, through several names or a `pattern`, the column needs a rule of its own first. A map merged into a shared rule applies to every column it governs. The note says which case applies.

`--sample-fraction` (default 1.0) reads that share of the source rows, chosen by a hash of the primary keys. Rerunning on the same data with the same Polars version samples the same rows. `--json` prints each proposal with its evidence instead of YAML.

Two tables on one warehouse connection are counted inside the warehouse, and no row leaves it. The connection rules are those of a warehouse run: one backend, one connection, and two different tables. A warehouse paired with a file or a database is refused. After the column probes and the duplicate key checks, one statement counts every candidate column. Veridelta then applies the confidence floor itself, so a warehouse proposes exactly what a local run would from the same rows. Two differences remain:

- Only columns stored as text on both sides qualify. A local run also proposes text entries for a non-text target, such as `Y: '1'`. Each engine writes numbers and timestamps as text its own way, and a warehouse compares such an entry against the integer column, not its text.
- A sample hashes the normalized keys with the warehouse's own hash function. Rerunning against the same tables samples the same rows, but not the rows a local run of the same fraction samples.

In Python, `DiffEngine(config, source, target).propose_value_maps()` returns `ValueMapProposal` objects. `DiffEngine.propose_value_maps_from_configs(diff, source, target)` takes what `load_config` returns and reads the data first. Each proposal's `to_rule()` returns it as a standalone rule.

## Zero padding

`pad_zeros` converts each value to text and pads it with leading zeros to a fixed width. A numeric `123` in one system then matches the text `"00123"` in the other:

```yaml
rules:
  - column_names: ["account_number"]
    pad_zeros: 10
```

A negative value keeps its sign in front, as in `-012`, and a value longer than the width stays whole. The width must be a YAML integer: `pad_zeros: "5"` is rejected, not converted.

## Dates and timezones

`datetime_format` parses text into timestamps with a [strptime](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) pattern, and the column is compared as timestamps. `timezone` then converts timezone-aware timestamps to one zone:

```yaml
rules:
  - column_names: ["created_at"]
    datetime_format: "%Y-%m-%d %H:%M:%S%z"
    timezone: "UTC"
```

A value that does not fit the pattern becomes NULL. Two such values match only under `treat_null_as_equal`, and one against a parsed timestamp is a mismatch. Pushdown translates a fixed set of directives into each warehouse's format language; see [Rules in SQL](pushdown.md#rules-in-sql).

`%f` is a fraction of a second, as in Python: `.5` is half a second. A local run and a warehouse both read one to six digits. A local run also reads seven to nine digits, keeping microseconds, and parses a value with no fraction at all when the format writes `.%f`. A warehouse may read either as NULL.

`timezone` requires timezone-aware data. Naive timestamps raise `ConfigError`, because assuming a zone for them would shift every value by a real offset without a warning. To compare text timestamps that carry an offset, parse them with a format that contains `%z`.

The conversion changes only the timezone label. Comparisons and casts read the underlying instant, not the wall clock time in the new zone, so a `timezone` rule cannot change a verdict on its own. It makes two differently zoned columns comparable, and it rejects data that carries no zone.

## Casts

`cast_to` converts a column to `Int64`, `Float64`, `String`, `Boolean`, `Date`, or `Datetime`. Any other type name is rejected when the file loads. A cast the column's type cannot take, such as a date or text to `Boolean`, or binary data to a number, stops the run before it reads a row, and `validate --schemas` reports it.

Converting a float to `Int64` truncates toward zero, as Polars does, where SQL would round. Local runs and pushdown both truncate.

## Numeric tolerances

A tolerance forgives small differences between numbers, such as rounding between two systems:

```yaml
rules:
  - column_names: ["total_amount", "tax"]
    absolute_tolerance: 0.01
    relative_tolerance: 0.005
```

Two numbers match when they are equal, or when their difference is at most `absolute_tolerance + relative_tolerance * |source|`. A tolerance loosens only columns that are numeric after normalization, such as a text column with `cast_to: Float64`. Text, boolean, and temporal columns are compared exactly.

Only finite values are loosened. `NaN` matches only `NaN`, and an infinity matches only the same infinity, however wide the tolerance. Integer differences are exact and never wrap around the column's type: Int8 `100` and `-100` differ by 200.

A tolerance must be finite. To stop comparing a column, use `ignore`.

## Fuzzy text matching

A similarity limit forgives typos in free text, such as names typed by hand into two systems, without a `regex_replace` for each one. Scores come from [RapidFuzz](https://github.com/rapidfuzz/RapidFuzz), which the `fuzzy` extra installs:

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

- `max_levenshtein_distance` counts single-character insertions, deletions, and substitutions. `Jon` and `John` are one edit apart and match at `1`. `kitten` and `sitting` need `3`.
- `min_jaro_winkler_similarity` scores two values from 0 to 1 and rewards a shared prefix. `MARTHA` and `MARHTA` score 0.961: they match at `0.96` but not at `0.97`.

A limit loosens only columns that are text after normalization, as a tolerance loosens only numbers. A column cast to a number or parsed as a date is still compared exactly, while `pad_zeros` or `cast_to: String` makes a column text.

Equal values still match outright. A NULL is never similar to anything, so `treat_null_as_equal` alone decides NULLs. Scores are case-sensitive: `case_insensitive` lowercases both sides first, in stage 3, which puts `ABD` and `abc` one edit apart. Primary keys are never loosened, because rows pair on equal keys.

A rule sets at most one of the two limits, and neither has a global default. Loosening every text column would also forgive identifiers and codes that must match exactly. Without the extra, a run that needs a score raises `ConfigError` with the install command before comparing any rows.

Pushdown compiles `max_levenshtein_distance` for Snowflake, Databricks, and BigQuery, while Postgres and DuckDB refuse it. Every warehouse refuses `min_jaro_winkler_similarity`; see [Rules in SQL](pushdown.md#rules-in-sql).

## Null equality

`treat_null_as_equal` decides whether two NULLs match. A rule without it uses `default_treat_null_as_equal`, which is `true`. A NULL never matches a value, whatever the setting. A value that an earlier stage turns into NULL, such as text that `datetime_format` cannot parse, follows the same rule.
