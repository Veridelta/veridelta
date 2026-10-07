# Configuration

A configuration file declares the two datasets to compare, the primary keys that pair their rows, and the rules that decide when two values match. In Python, the same settings are the fields of `DiffConfig`.

The smallest file names a source, a target, and the primary keys:

```yaml
source:
  path: "legacy_system.csv"

target:
  path: "modern_system.parquet"

primary_keys: ["user_id"]
```

A source without a `type` is a file, read from `path` in the `format` its suffix names, or the one you give, with optional reader `options`. Set `type` to read a lakehouse table, a database, or a warehouse table instead; see [Sources](sources.md). To change how particular columns are compared, add `rules`; see [Rules](rules.md).

## Primary keys

`primary_keys` names the columns that pair each source row with its target row. It must name at least one column, and the keys together must be unique on each side. A key that repeats raises `DataIntegrityError` before any values are compared.

Rules on key columns apply before rows are paired: a `case_insensitive` key pairs `ABC` with `abc`. See [Transform order](rules.md#transform-order). Write a renamed key with its target name; see [Renaming columns](rules.md#renaming-columns).

## Settings

Every setting except `primary_keys` is optional:

| Setting | Default | Description |
| :--- | :--- | :--- |
| `primary_keys` | required | Columns that pair rows. See [Primary keys](#primary-keys). |
| `schema_mode` | `intersection` | Which columns both sides must have. See [Schema mode](#schema-mode). |
| `strict_types` | `false` | Whether a column stored as two different types fails. See [Column types](#column-types). |
| `normalize_column_names` | `false` | Whether to strip and lowercase column names before the sides are aligned. See [Column names](#column-names). |
| `threshold` | `0.0` | Largest mismatch ratio, from 0.0 to 1.0, that still counts as a match. The ratio is added, removed, and changed rows over source rows. |
| `default_absolute_tolerance` | `0.0` | Absolute tolerance for each numeric column without its own `absolute_tolerance`. |
| `default_relative_tolerance` | `0.0` | Relative tolerance for each numeric column without its own `relative_tolerance`. |
| `default_treat_null_as_equal` | `true` | Whether two NULLs match, for each column without its own `treat_null_as_equal`. |
| `default_whitespace_mode` | `none` | Whitespace to strip from each text column without its own `whitespace_mode`: `none`, `left`, `right`, or `both`. |
| `default_null_values` | `[]` | Values to read as NULL. Each applies only to columns whose type can hold it. |
| `rules` | `[]` | Rules for particular columns. See [Rules](rules.md). |
| `report_top_columns_limit` | `5` | Drifting columns to list in `report_summary`. `0` hides the list. |
| `pushdown_sample_rows` | `0` | Pushdown only. Changed rows to fetch with their values. `0` fetches none, and no value leaves the warehouse. Local runs ignore it, since they hold every row. See [Row samples](pushdown.md#row-samples). |
| `output_path` | none | Directory for discrepancy files. Without it, no files are written. See [Artifacts](results.md#artifacts). |
| `output_format` | `parquet` | Format of the discrepancy files: `parquet`, `csv`, `json`, `ndjson`, or `arrow`. |

A `default_*` setting fills in for every column whose rule leaves that field unset. The tolerances loosen only columns that are numeric after normalization; every other column is compared exactly.

This file passes when at most 1% of rows differ, forgives numeric differences up to 0.01, and writes the differing rows to `./artifacts`:

```yaml
primary_keys: ["user_id"]
threshold: 0.01
default_absolute_tolerance: 0.01
default_relative_tolerance: 0.0
default_treat_null_as_equal: true
report_top_columns_limit: 5
output_path: "./artifacts"
output_format: parquet
```

### Schema mode

`schema_mode` sets which columns the two sides must share. Every mode compares only the columns both sides have, and every mode requires the primary keys on both sides.

| Mode | Passes when |
| :--- | :--- |
| `intersection` | Always. Columns on one side only are left out. |
| `exact` | Both sides have the same set of columns. Column order is not compared. |
| `allow_additions` | The target has every source column. It may add more. |
| `allow_removals` | The target adds no column. It may drop source columns. |

A violation raises `ConfigError` before any rows are read.

### Column types

With `strict_types: false`, a column stored as different types on the two sides is still compared:

- Two numeric types compare by value. An integer `10` and a float `10.7` differ, and a `Float32` `0.1` differs slightly from a `Float64` `0.1`. Add a tolerance to forgive precision gaps.
- Any other pair casts the target to the source type: the text `"10"` matches the integer `10`.

With `strict_types: true`, a column whose two sides hold different types after normalization fails every row. A `cast_to` or `datetime_format` that brings both sides to one type keeps such a column comparable. Under `treat_null_as_equal`, two NULLs still match.

### Column names

`normalize_column_names: true` strips whitespace from every column name and lowercases it before the two sides are aligned. It applies on every entry point, `DiffEngine(...).run()` and `validate_schemas` included.

The names in `primary_keys`, `column_names`, and `rename_to` are normalized the same way. A `pattern` is not: write it against the lowercase names. Two names that become equal once normalized raise `ConfigError`. Pushdown refuses the setting when it would rename a stored column; see [Columns](pushdown.md#columns).

## Environment variables

Any string inside `source` or `target` can read an environment variable, which keeps credentials and per-environment paths out of the file:

```yaml
source: &warehouse
  type: snowflake
  table: ANALYTICS.PUBLIC.LEGACY_EVENTS
  account: xy12345
  user: ${SNOWFLAKE_USER}
  password: ${SNOWFLAKE_PASSWORD}
  role: ${SNOWFLAKE_ROLE:-ANALYST}
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema_name: PUBLIC

target:
  <<: *warehouse
  table: ANALYTICS.PUBLIC.MODERN_EVENTS

primary_keys: ["event_id"]
```

- `${NAME}` is replaced by the variable's value. It can sit inside longer text, as in `s3://${LAKE_BUCKET}/events`. A variable that is set but empty gives empty text.
- `${NAME:-default}` uses `default` when the variable is unset or empty. The default is literal text and cannot contain `}` or another reference.
- `$${` writes a literal `${`. Write any value that contains `${` this way.
- A value read from the environment is never expanded again. A secret that contains `$` or `${` arrives intact.
- Nested values such as `storage_options` and file `options` are expanded too. Keys, numbers, and booleans are not.
- Root settings and `rules` are read as written. A `${1}` in a `regex_replace` replacement stays as it is.

A reference to an unset variable without a default raises `ConfigError` when the file loads. So does a malformed reference: `${1}`, `${NAME`, `${NAME-x}`, or a nested `${A:-${B}}`. The error names the field, such as `source -> password`, and the variable, but never a value. Validation errors for `source` and `target` leave out their input for the same reason.

Expanded values are text:

- `version` and `snapshot_id` accept only YAML integers. Write them literally.
- File `options` reach the reader as they are: an expanded option arrives as a string.
- A database `password` is percent-encoded when it joins the URI, but a `${VAR}` expanded inside `uri` is not. Keep database passwords in `password`.

## Loading a file in Python

`load_config` reads a file into the three objects a run needs, and `DiffEngine.run_from_configs` runs them through the same path as the CLI:

```python
from veridelta import DiffEngine, load_config

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
summary = result.summary
```

`DiffEngine(config, source_frame, target_frame).run()` compares two Polars frames you have already loaded.

## Schema checks

`DiffEngine.validate_schemas` checks `schema_mode` and the primary keys against two schemas without reading any rows. It accepts zero-row frames and unevaluated scans, which makes it cheap enough to gate a deployment:

```python
import polars as pl

from veridelta import ConfigError, DiffConfig, DiffEngine

contract = DiffConfig(primary_keys=["user_id"], schema_mode="exact")
try:
    DiffEngine.validate_schemas(contract, pl.scan_parquet("source.parquet"), pl.scan_parquet("target.parquet"))
except ConfigError as exc:
    print(exc)
```

It raises `ConfigError` on a violation and returns nothing otherwise.

`DiffEngine.validate_rules` takes the same arguments and goes one step further. It resolves every rule against the aligned columns and builds each column's comparison, still without reading a row, then returns the columns a run would compare. A rule the run could not honor fails here, such as a null sentinel the column's type cannot hold, or a similarity limit without the `fuzzy` extra. Repeated keys and invalid regular expressions surface only once rows are read.

To check a whole configuration file from the command line, use `veridelta validate`; see [Checking a configuration](cli.md#checking-a-configuration).

## Editor support

Veridelta publishes a JSON Schema for configuration files. An editor that uses the YAML language server, such as VS Code with the `redhat.vscode-yaml` extension, then completes keys, shows each field's description, and flags a typo such as `primary_key` or `absolute_tolerence` as you type.

Point a file at the schema with a comment on its first line:

```yaml
# yaml-language-server: $schema=https://veridelta.github.io/veridelta/schema/veridelta.schema.json
source:
  path: "legacy_system.csv"

target:
  path: "modern_system.parquet"
  format: "parquet"

primary_keys: ["user_id"]
```

A `$schema:` key does not work, because the loader rejects keys it does not know.

To apply the schema to every configuration in a workspace without a modeline, map a file pattern to it in the YAML extension's settings, such as in `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "https://veridelta.github.io/veridelta/schema/veridelta.schema.json": "**/veridelta*.yaml"
  }
}
```

The site's copy of the schema follows the main branch. To pin it to the release you run, use the copy in that release's tag, such as `https://raw.githubusercontent.com/Veridelta/veridelta/v0.19.9/docs/schema/veridelta.schema.json`. Or print the installed version's schema to a file and point at that:

```bash
veridelta schema > veridelta.schema.json
```

The schema is slightly stricter than the loader. The loader converts `threshold: "0.1"` to a number, and the schema flags the quotes. In `source` and `target`, every text field also accepts a `${NAME}` reference.

### Validate from a task

The schema flags a wrong key or value as you type. `veridelta validate` also checks what the schema cannot, such as a missing extra or a pattern Polars rejects; see [Checking a configuration](cli.md#checking-a-configuration). This VS Code task runs it on the file in the editor and lists the verdict in the Problems panel. Save it as `.vscode/tasks.json`, then run it from the Terminal menu with Run Task:

```json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "Veridelta: validate this file",
      "type": "shell",
      "command": "veridelta",
      "args": ["validate", "-c", "${file}"],
      "presentation": {
        "reveal": "always",
        "clear": true
      },
      "problemMatcher": [
        {
          "owner": "veridelta",
          "source": "veridelta",
          "severity": "error",
          "fileLocation": ["autoDetect", "${workspaceFolder}"],
          "pattern": {
            "regexp": "^(.+?): (\\d+ errors?, \\d+ warnings?\\.)$",
            "kind": "file",
            "file": 1,
            "message": 2
          }
        },
        {
          "owner": "veridelta",
          "source": "veridelta",
          "severity": "warning",
          "fileLocation": ["autoDetect", "${workspaceFolder}"],
          "pattern": {
            "regexp": "^(.+?): (valid, with \\d+ warnings?\\.)$",
            "kind": "file",
            "file": 1,
            "message": 2
          }
        }
      ]
    }
  ]
}
```

The panel shows one entry per file, with its counts of errors and warnings. The terminal shows each finding, as the command prints it. A valid file with no warnings adds no entry.
