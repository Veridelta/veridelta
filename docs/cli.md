# Command line

The `veridelta` command runs a comparison, checks a configuration, proposes value maps, prints the configuration schema, and serves its checks to an AI agent. `run`, `validate`, and `crosswalk` read `veridelta.yaml` unless `-c` names another file.

| Command | Description |
| :--- | :--- |
| `veridelta run` | Compare the two datasets and report the result. |
| `veridelta validate` | Report what would stop a run, without reading any rows. |
| `veridelta crosswalk` | Propose `value_map` entries from the data. |
| `veridelta schema` | Print the configuration file's JSON Schema. |
| `veridelta mcp` | Serve the checks to an AI agent as Model Context Protocol tools, over stdio. |
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
| `-v`, `--verbose` | Print each file opened, connection, read, and pushdown statement on stderr; see [Logging](#logging). |
| `--json` | Print the [summary](results.md#summary) as JSON on stdout instead of the text report. |
| `-q`, `--quiet` | Suppress progress messages on stderr. The report or the JSON still prints. |
| `--html PATH` | Also write a standalone [HTML report](results.md#html-report), which loads nothing from a CDN. |
| `--html-max-rows N` | Rows per table in the HTML report, zero or more. Default 1000, so a large diff cannot produce a file too large to open. |
| `--markdown PATH` | Also write the [Markdown summary](results.md#markdown-summary) that the [CI integrations](ci.md) post. |
| `--markdown-max-rows N` | Changed values to list in the Markdown summary, lowest keys first. Default 0, which lists none. |
| `--otel PATH` | Also write the run's [OpenTelemetry metrics](results.md#opentelemetry-metrics) to a file. |
| `--otel-send` | Also send the metrics to an OTLP/HTTP endpoint. See [Sending to an endpoint](results.md#sending-to-an-endpoint). |

Progress messages always go to stderr, so `veridelta run --json | jq` needs no filtering. Reports and summaries from a pushdown run are labeled as holding primary keys only. An HTML report shows a [row sample](pushdown.md#row-samples)'s values when the run fetched one, and so does a Markdown summary that lists values.

## Exit codes

Every command exits `2` for invalid arguments, and `3` when it cannot finish:

| Code | `run` | `validate` | `crosswalk` | `mcp` |
| :--- | :--- | :--- | :--- | :--- |
| `0` | A match within `threshold`. | No errors. Warnings are allowed. | Proposals computed, whether or not any were found. | The host disconnected, or Ctrl-C stopped the server. |
| `1` | Drift. | At least one error. | Not used. | Not used. |
| `2` | Invalid arguments. | Invalid arguments. | Invalid arguments. | Invalid arguments, such as a `--root` that is not a directory. |
| `3` | The run could not finish, such as on a configuration error, an unreachable source, a missing extra, or metrics `--otel-send` could not send. | The check could not finish, such as when `--schemas` cannot reach a source. | The proposals could not be computed. | The server could not start, such as without the `mcp` extra. |

A command that cannot finish explains why on stderr. With `--json`, it also prints the error on stdout, as one JSON object in place of its usual output:

```json
{
  "error": {
    "type": "ConfigError",
    "message": "Environment variable 'SNOWFLAKE_PASSWORD' is not set, but source -> password references it. Set it, or write ${NAME:-default} to give a fallback."
  }
}
```

`type` names the error. A `ConfigError` is a problem with the configuration file. A `ConnectorError` comes from a source, or from the endpoint `--otel-send` posts to. A `DataIntegrityError` comes from a source's data. Any other type is a bug or an unsupported input: please [report it](https://github.com/Veridelta/veridelta/issues). An error `validate` finds in the file is a finding at exit `1`, in its usual output, not an error object.

## Logging

`-v` or `--verbose` prints Veridelta's own log lines on stderr, for `run`, `validate`, `crosswalk`, and `mcp`. Each line records a file opened, a connection, a read, or a pushdown statement. Reads and statements carry their timings:

```text
INFO veridelta.connectors.database: Read 1200 rows of table 'orders' from postgresql://analyst@db.internal/sales in 0.412s
```

A line names its source by host, account, project, database, or table, and masks a password inside a URI. It never holds SQL text, row values, or a credential. [Logging](sources.md#logging) lists what each level records. The drivers' own loggers stay silent.

When a read or a statement fails, a warning names it and how long it ran, and the error follows. `--verbose` changes nothing else: stdout keeps the report or the JSON, and `--quiet` still drops the progress messages.

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
| `-v`, `--verbose` | Print each file opened, connection, read, and pushdown statement on stderr; see [Logging](#logging). |
| `--schemas` | Also connect, read each side's columns, never its rows, and check the rules against them. |
| `--allow-missing-env` | Read an unset `${NAME}` as the text `NAME`, with a warning, instead of failing. |
| `--json` | Print the findings as one JSON object on stdout. |
| `-q`, `--quiet` | Suppress the verdict line on stderr. |

By default, `validate` connects to nothing. It checks that:

- the source and target are a pair an engine can compare;
- every optional extra the run needs is installed, in the environment `validate` runs in;
- a database `table` uses a URI scheme Veridelta can quote;
- each `regex_replace` pattern compiles in Polars, whose regular expressions have no look-around and no backreferences, unlike Python's `re`.

On a pair compared in place, such as two warehouse tables, it also warns about settings the backend refuses only for some stored names or types:

- `normalize_column_names`;
- `min_jaro_winkler_similarity`;
- a `datetime_format` the backend cannot parse with, such as any `datetime_format` on Postgres;
- `max_levenshtein_distance` on Postgres and DuckDB;
- a `regex_replace` replacement that refers to a group by name.

A pattern Polars rejects is only a warning there, since the backend runs it with its own engine.

With `--schemas`, `validate` also connects and checks the rules against each side's columns:

- Files and lakehouse tables are opened as a run opens them, then checked with `DiffEngine.validate_rules`. JSON, Excel, and Avro files have no lazy reader, so they are read whole.
- A database `table` is read as a run reads it, with `WHERE 1 = 0` added, so no row is fetched. SQLite reports a `NUMERIC` column as text in that probe, although a full read returns numbers. A `query` is not run. If either side reads one, the rules are checked against neither side, and a warning names each `query` side.
- A pair compared in place runs the column probes a run starts with, then compiles every comparison statement without running it. That settles each warning above one way or the other.

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
| `-v`, `--verbose` | Print each file opened, connection, read, and pushdown statement on stderr; see [Logging](#logging). |
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

## Serving tools to an agent

`veridelta mcp` serves Veridelta's checks to an AI agent as [Model Context Protocol](https://modelcontextprotocol.io/) tools. The agent's host, such as Claude Code or Cursor, starts the command and speaks to it over stdin and stdout. It needs the `mcp` extra, and [AI agents](agents.md#mcp-server) shows how to register it with a host and lists its tools. This serves the configuration files in the current directory:

```bash
veridelta mcp --root .
```

| Flag | Description |
| :--- | :--- |
| `--root DIR` | A folder the tools may read configuration files and data on this machine from. Repeat it for more folders. Default: the current directory. |
| `--allow-row-values` | Let `read_discrepancies` and `propose_value_maps` return values from the data. Off by default. |
| `--max-rows N` | The most rows, or value map entries, one call returns, at least 1. Default: 50. |
| `-v`, `--verbose` | Print each file opened, connection, read, and pushdown statement on stderr; see [Logging](#logging). |

A tool refuses a configuration file, or data on this machine, outside every root, and a tool that runs the comparison refuses a configuration whose `output_path` lies outside them. Without `--allow-row-values`, no tool returns a value from the data. The server runs in the first root, so a relative path in a tool call, or in a configuration file, resolves there. Stdout carries the protocol and nothing else, and log lines go to stderr, which the host keeps. The server stops when the host disconnects, or on Ctrl-C.
