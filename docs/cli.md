# Command line

```bash
veridelta --version
veridelta run -c veridelta.yaml
veridelta run -c veridelta.yaml --json
veridelta run -c veridelta.yaml --quiet
veridelta run -c veridelta.yaml --html report.html --html-max-rows 1000
veridelta run -c veridelta.yaml --markdown summary.md
veridelta run -c veridelta.yaml --otel otel-metrics.json
veridelta crosswalk -c veridelta.yaml
veridelta crosswalk -c veridelta.yaml --min-confidence 0.99 --json
veridelta validate -c veridelta.yaml
veridelta validate -c veridelta.yaml --allow-missing-env --json
veridelta validate -c veridelta.yaml --schemas
veridelta schema > veridelta.schema.json
```

## Running a comparison

`--json` prints `DiffSummary` as JSON on stdout. `--quiet` suppresses progress chatter on stderr (the JSON line still prints). Progress chatter always goes to stderr, so `veridelta run --json | jq` does not have to strip anything first. `--html` writes a standalone report with no CDN references, capped at `--html-max-rows` (zero or more; default 1000) so a large diff cannot produce an unopenable file. `--markdown` writes the Markdown summary described above, which the [CI integrations](ci.md) post, and `--otel` the [OpenTelemetry metrics](results.md#opentelemetry-metrics). Pushdown reports and summaries are labeled as primary-keys-only, and an HTML report shows a [row sample](pushdown.md#row-samples)'s values when the run fetched one.

## Exit codes

Exit codes are `0` for a match within `threshold`, `1` for drift or any failure while running, and `2` for invalid command-line arguments.

## Checking a configuration

`validate` reports what would stop a run, without reading any rows. By default it connects to nothing and checks that:

- the source and target are a pair an engine can compare;
- every optional extra the run needs is installed, in the environment `validate` runs in;
- a database `table` uses a URI scheme Veridelta can quote;
- each `regex_replace` pattern compiles in Polars, whose regular expressions, unlike Python's `re`, have no look-around or backreferences.

On a warehouse pair it also warns about settings the warehouse refuses only for some stored names or types: `normalize_column_names`, `min_jaro_winkler_similarity`, a `datetime_format` with no SQL spelling, and a `regex_replace` replacement that refers to a group by name. A pattern Polars rejects is only a warning there, since the warehouse runs it with its own engine.

`--schemas` also connects and checks the rules against each side's columns, never its rows:

- Files and lakehouse tables are opened as a run opens them and checked with `DiffEngine.validate_rules`. JSON and Excel files have no lazy reader, so they are read whole.
- A database `table` is read with `SELECT * FROM … WHERE 1 = 0`. SQLite reports a `NUMERIC` column as text in that probe, though a full read returns numbers. A `query` is not run, and that side is reported as unchecked.
- A warehouse pair runs the two column probes a run starts with, then compiles every comparison statement without executing it. That settles each warning above one way or the other.

Errors print to stdout as `error:` lines and warnings as `warning:` lines, or, with `--json`, as one object with `config`, `valid`, `errors`, and `warnings`. The verdict goes to stderr, and `--quiet` drops it. `validate` exits `0` when there are no errors, warnings or not, `1` when there are, and `2` for invalid arguments.

`--allow-missing-env` checks a file without its secrets, such as in a pull request job. An unset `${NAME}` with no default is read as the text `NAME`, with a warning. That works for fields that take a name, such as `table`, `account`, `password`, or `path`. A field with a required shape, such as a database `uri`, still fails unless its variable is set.

From Python, `DiffEngine.check_configs(diff, source, target, schemas=False)` returns the same findings as `ConfigFinding` models, and `load_config(path, unset_env=[])` reads unset variables as their names and appends each name to the list.

## Proposing value maps

`crosswalk` proposes `value_map` rules instead of comparing; see [Proposing a value map](rules.md#proposing-a-value-map). It exits `0` once the proposals are computed, whether or not it found any, `1` on failure, and `2` for invalid arguments.

## Printing the schema

`schema` prints the configuration file's JSON Schema; see [Editor support](configuration.md#editor-support).
