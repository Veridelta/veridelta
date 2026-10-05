# Command line

The `veridelta` command runs a comparison, checks a configuration, proposes value maps, and prints the configuration schema. Each command reads `veridelta.yaml` unless `-c` names another file.

| Command | Description |
| :--- | :--- |
| `veridelta run` | Compare the two datasets and report the result. |
| `veridelta validate` | Report what would stop a run, without reading any rows. |
| `veridelta crosswalk` | Propose `value_map` entries from the data. |
| `veridelta schema` | Print the configuration file's JSON Schema. |
| `veridelta --version` | Print the installed version. |

## Running a comparison

`veridelta run` compares the configured datasets, prints a report, and exits with a [status code](#exit-codes):

```bash
veridelta run -c veridelta.yaml
veridelta run -c veridelta.yaml --json
veridelta run -c veridelta.yaml --html report.html --markdown summary.md --otel otel-metrics.json
```

| Flag | Description |
| :--- | :--- |
| `-c`, `--config PATH` | Configuration file. Default `veridelta.yaml`. |
| `--json` | Print the [summary](results.md#summary) as JSON on stdout instead of the text report. |
| `-q`, `--quiet` | Suppress progress messages on stderr. The report or the JSON still prints. |
| `--html PATH` | Also write a standalone [HTML report](results.md#html-report), which loads nothing from a CDN. |
| `--html-max-rows N` | Rows per table in the HTML report, zero or more. Default 1000, so a large diff cannot produce a file too large to open. |
| `--markdown PATH` | Also write the [Markdown summary](results.md#markdown-summary) that the [CI integrations](ci.md) post. |
| `--otel PATH` | Also write the run's [OpenTelemetry metrics](results.md#opentelemetry-metrics). |

Progress messages always go to stderr, so `veridelta run --json | jq` needs no filtering. Reports and summaries from a pushdown run are labeled as holding primary keys only. An HTML report shows a [row sample](pushdown.md#row-samples)'s values when the run fetched one.

## Exit codes

Every command exits `2` for invalid arguments:

| Code | `run` | `validate` | `crosswalk` |
| :--- | :--- | :--- | :--- |
| `0` | A match within `threshold`. | No errors. Warnings are allowed. | Proposals computed, whether or not any were found. |
| `1` | Drift, or any failure while running. | At least one error. | A failure. |
| `2` | Invalid arguments. | Invalid arguments. | Invalid arguments. |

## Checking a configuration

`veridelta validate` reports what would stop a run, without reading any rows:

```bash
veridelta validate -c veridelta.yaml
veridelta validate -c veridelta.yaml --allow-missing-env --json
veridelta validate -c veridelta.yaml --schemas
```

| Flag | Description |
| :--- | :--- |
| `-c`, `--config PATH` | Configuration file. Default `veridelta.yaml`. |
| `--schemas` | Also connect, read each side's columns, never its rows, and check the rules against them. |
| `--allow-missing-env` | Read an unset `${NAME}` as the text `NAME`, with a warning, instead of failing. |
| `--json` | Print the findings as one JSON object on stdout. |
| `-q`, `--quiet` | Suppress the verdict line on stderr. |

By default, `validate` connects to nothing. It checks that:

- the source and target are a pair an engine can compare;
- every optional extra the run needs is installed, in the environment `validate` runs in;
- a database `table` uses a URI scheme Veridelta can quote;
- each `regex_replace` pattern compiles in Polars, whose regular expressions have no look-around and no backreferences, unlike Python's `re`.

On a warehouse pair, it also warns about settings the warehouse refuses only for some stored names or types: `normalize_column_names`, `min_jaro_winkler_similarity`, a `datetime_format` with no SQL spelling, and a `regex_replace` replacement that refers to a group by name. A pattern Polars rejects is only a warning there, since the warehouse runs it with its own engine.

With `--schemas`, `validate` also connects and checks the rules against each side's columns:

- Files and lakehouse tables are opened as a run opens them, then checked with `DiffEngine.validate_rules`. JSON, Excel, and Avro files have no lazy reader, so they are read whole.
- A database `table` is read as a run reads it, with `WHERE 1 = 0` added, so no row is fetched. SQLite reports a `NUMERIC` column as text in that probe, although a full read returns numbers. A `query` is not run, and that side is reported as unchecked.
- A warehouse pair runs the column probes a run starts with, then compiles every comparison statement without running it. That settles each warning above one way or the other.

Errors print to stdout as `error:` lines and warnings as `warning:` lines. With `--json`, they print as one object with `config`, `valid`, `errors`, and `warnings`. The verdict goes to stderr.

`--allow-missing-env` checks a file without its secrets, such as in a pull request job. An unset `${NAME}` with no default is read as the text `NAME`, with a warning. That works for fields that take a name, such as `table`, `account`, `password`, or `path`. A field with a required shape, such as a database `uri`, still fails unless its variable is set.

In Python, `DiffEngine.check_configs(diff, source, target, schemas=False)` returns the same findings as `ConfigFinding` models. `load_config(path, unset_env=[])` reads unset variables as their names and appends each name to the list.

## Proposing value maps

`veridelta crosswalk` prints `value_map` entries that the data supports, as YAML rules ready to paste into the configuration. [Proposing a value map](rules.md#proposing-a-value-map) explains how entries are chosen:

```bash
veridelta crosswalk -c veridelta.yaml
veridelta crosswalk -c veridelta.yaml --min-confidence 0.99 --json
```

| Flag | Description |
| :--- | :--- |
| `-c`, `--config PATH` | Configuration file. Default `veridelta.yaml`. |
| `--min-confidence SHARE` | Share of a source value's rows that must agree on one target value, above 0.5. Default 0.95. |
| `--min-support N` | Agreeing rows an entry needs. Default 5. |
| `--sample-fraction SHARE` | Share of source rows to read, chosen by primary key. Default 1.0. |
| `--json` | Print the proposals and their evidence as JSON on stdout instead of YAML. |
| `-q`, `--quiet` | Suppress progress and evidence on stderr. |

## Printing the schema

`veridelta schema` prints the configuration file's JSON Schema, which editors use to complete and check a file; see [Editor support](configuration.md#editor-support):

```bash
veridelta schema > veridelta.schema.json
```
