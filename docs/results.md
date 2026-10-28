# Results

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`. It holds a summary of what differs on `summary`, and the rows behind it on `added`, `removed`, and `changed`, so a notebook can inspect the drift without writing files.

The summary prints as a plain-text report, and one call isolates the rows where a single column disagrees:

```python
import polars as pl

from veridelta import DiffConfig, DiffEngine

diff = DiffConfig(primary_keys=["trip_id"])
source_df = pl.scan_parquet("legacy_trips.parquet")
target_df = pl.scan_parquet("modern_trips.parquet")
result = DiffEngine(diff, source_df, target_df).run()

print(result.summary.report_summary)

# The rows where one column disagreed, with both values side by side.
result.get_mismatches("total_amount")

# The changed rows as a pandas DataFrame, if pandas is installed.
result.to_pandas()
```

## Summary

`summary` is a `DiffSummary`, a Pydantic model without any frames. `summary.model_dump_json()` returns the same JSON that `veridelta run --json` prints:

| Field | Description |
| :--- | :--- |
| `total_rows_source` | Rows in the source. |
| `total_rows_target` | Rows in the target. |
| `added_count` | Rows only in the target. |
| `removed_count` | Rows only in the source. |
| `changed_count` | Rows in both with at least one compared value that differs. |
| `column_mismatches` | For each column with at least one mismatch, the number of changed rows where it differs. |
| `total_mismatches` | Added, removed, and changed rows together. |
| `mismatch_ratio` | `total_mismatches` over `total_rows_source`. |
| `match_rate_percentage` | One minus `mismatch_ratio`, as a percentage rounded to two places, and never below 0. |
| `is_match` | Whether `mismatch_ratio` is at most `threshold`. |
| `accepted_count` | Rows of drift a [baseline](cli.md#accepting-drift) accepted, which the counts above leave out. `0` without one. |
| `is_perfect_match` | Whether nothing differs. |
| `volume_shift` | `total_rows_target` minus `total_rows_source`. |
| `report_summary` | A plain-text report of these counts and the most drifting columns. |
| `provenance` | The [run record](#run-record): what the run read and where it ran. `veridelta run` fills it, and a run from Python leaves it `null` unless given one. |

[`schema/run.schema.json`](schema/run.schema.json) is the JSON Schema of this object, and `veridelta schema run` prints it; see [Printing the schema](cli.md#printing-the-schema).

### Run record

The run record ties a verdict to the files it judged, the commit they came from, and the CI run that judged them. Git and GitHub keep who changed a rule and who approved a run. The record's hashes and commit point at those, so a reviewer can check that a report judged what they approve. `veridelta run` writes it under `provenance`:

```json
"provenance": {
  "veridelta_version": "0.41.2",
  "started_at": "2026-10-07T08:50:13Z",
  "config_file": {"path": "veridelta.yaml", "sha256": "9f2c...e41a"},
  "baseline_file": null,
  "commit_sha": "abc1234...",
  "working_tree_clean": true,
  "ci_run_url": "https://github.com/acme/orders/actions/runs/123/attempts/1"
}
```

| Field | Description |
| :--- | :--- |
| `veridelta_version` | The Veridelta release that ran. |
| `started_at` | When the run started, in UTC. |
| `config_file` | The configuration file's path as given and the SHA-256 of its bytes. `null` for a run of two files and `--key`. |
| `baseline_file` | The `--baseline` file's path and SHA-256, or `null`. |
| `commit_sha` | The commit checked out where the run started, from `git rev-parse HEAD`. Without git, GitHub Actions' `GITHUB_SHA`. `null` outside both. |
| `working_tree_clean` | Whether no tracked file differed from that commit, from `git status`. Untracked files, such as the run's own reports, do not count. `null` when git cannot tell. |
| `ci_run_url` | The GitHub Actions run, built from `GITHUB_SERVER_URL`, `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, and `GITHUB_RUN_ATTEMPT`. `null` outside GitHub Actions, or when a variable has an unexpected shape. |

The record holds no user name, and a path stays as it was given, so a relative path names no folder outside the project. A field Veridelta cannot fill stays `null` and never fails the run. To check a hash, run `sha256sum` on the file at the recorded commit:

```bash
git show abc1234:veridelta.yaml | sha256sum
```

`DiffEngine.run_from_configs` takes a record as `provenance`, and sets it on the summary unchanged. `HashedFile` and `RunProvenance` import from `veridelta.models`.

## Rows

`added` holds the rows found only in the target, and `removed` the rows found only in the source. `changed` holds the rows found in both with at least one difference. Each changed row carries its primary keys and, for every compared column, `{column}_source`, `{column}_target`, and `{column}_is_match`.

`rules_applied` holds a `ColumnRules` for each compared column: the tolerances, whitespace and case settings, cast, date format, time zone, and fuzzy thresholds it ran under, as both engines resolve them. A value map, regex replacements, and null sentinels appear only as counts, since they can hold data or describe its shape.

`get_mismatches(column)` returns the primary keys and both values for the rows where that column differs. It raises `ConfigError` for a column that was not compared. `to_pandas()` converts `changed` to pandas; it needs pandas and pyarrow, which Veridelta does not install.

### Pushdown results

[Pushdown](pushdown.md) compares in place and never selects values, so its frames hold primary keys only and the result sets `keys_only`. `get_mismatches` then returns every changed key instead of one column's values, and still rejects a column that was not compared.

A run with `pushdown_sample_rows` set also carries `changed_sample`: up to that many changed rows with each compared column's values, laid out like a local run's `changed`. See [Row samples](pushdown.md#row-samples).

## HTML report

![The top of an HTML report from a comparison of 120 orders: a FAILED banner that says 15 rows differ and 2 columns drift, a match rate of 87.5% with a bar, 120 source and 121 target rows, 3 added, 2 removed, and 10 changed, drift in status in 60% of the changed rows and in amount in 40%, an absolute tolerance of 0.01 on amount as the one rule applied, and tabs for the changed, added, and removed rows, open on the changed rows.](assets/report-light.png#only-light)
![The top of an HTML report from a comparison of 120 orders: a FAILED banner that says 15 rows differ and 2 columns drift, a match rate of 87.5% with a bar, 120 source and 121 target rows, 3 added, 2 removed, and 10 changed, drift in status in 60% of the changed rows and in amount in 40%, an absolute tolerance of 0.01 on amount as the one rule applied, and tabs for the changed, added, and removed rows, open on the changed rows.](assets/report-dark.png#only-dark)

`write_html` writes the standalone report that `veridelta run` writes with `--html`. It embeds its own styles and script, so it opens offline. It opens on the verdict in words, with how many rows differ and how many columns drift, then the [run record](#run-record) when the result has one, and the counts. The column-level drift table gives each column's share of the changed rows. The rules applied table lists, for each column with a rule, what it forgives or changes, such as `absolute tolerance 0.01` or `value map of 3 entries`, and counts the columns compared exactly. The changed, added, and removed rows sit behind three tabs. Without JavaScript, the three tables follow one another.

The changed rows open on their patterns: changes that two or more mismatched values share, such as six statuses that changed to `cancelled`, or four amounts that each rose by `1.00`. Each names its first keys, and with JavaScript, a button filters the list to it. Patterns come from the values the report lists, so a capped list says so. Under them, the mismatched values follow, one row per value that differs: the primary keys, the column, the source and target values as compared after the rules, and the difference. A difference is target minus source for numbers, as decimals of the values shown, and for dates and times. Filters by column and by text in the key narrow the list, and a filtered view's address carries them in its fragment, such as `report.html#column=amount`, so a link opens the same view. `--html-max-rows` caps the values listed, and the report says when it stops short. The full changed rows, every column side by side with its match flag, sit in a disclosure under the list, which opens without JavaScript. A pushdown run lists the values of its sample, and without one, its keys. Every row it shows is in the page itself, and the script splits long tables into pages of 25 rows. `max_rows` caps every table, as `--html-max-rows` does:

```python
from veridelta.report import write_html, write_markdown

write_html(result, "reports/nightly.html", max_rows=1000)
write_markdown(result, "reports/summary.md")
```

## Markdown summary

`write_markdown` writes the short summary that CI posts to a job summary or a pull request, and `render_markdown` returns it as text. It holds the verdict, a table of counts, a line naming the [run record](#run-record)'s hashes, commit, and CI run, and the drifting columns, up to `report_top_columns_limit`. Column names are written as code, so a name from the data cannot break the table or the page it lands on.

The summary ends with the same counts as JSON, inside an HTML comment that GitHub and GitLab hide from readers. Its first line names the [run schema](schema/run.schema.json) the JSON follows:

```text
<!-- veridelta-summary https://veridelta.github.io/veridelta/schema/run.schema.json
{"total_rows_source":3,"total_rows_target":3,"added_count":1,"removed_count":1,"changed_count":1,"column_mismatches":{"status":1},"is_match":false,...}
-->
```

The JSON is what `veridelta run --json` prints, except that `column_mismatches` holds only the columns the drift table lists, and is left out when `report_top_columns_limit` is `0`. A column name's `<`, `>`, and `&` are written as the JSON escapes `\u003c`, `\u003e`, and `\u0026`, so a name cannot close the comment, and a JSON parser reads them back as the same name. A script, or an agent that reads a pull request comment through the API, parses the line after the marker:

```python
import json
import re

found = re.search(r"^<!-- veridelta-summary \S+\n(.+)$", body, re.MULTILINE)
summary = json.loads(found.group(1))
```

The summary lists no values unless asked. With `max_rows` above 0, or `--markdown-max-rows` on the command line, a "Changed values" table follows the drift table. It has one row per differing value: the primary keys, the column, and the source and target values, lowest keys first. From Python:

```python
from veridelta.report import write_markdown

write_markdown(result, "summary.md", max_rows=20)
```

Each value is written as code. Text is quoted, so a trailing space or an empty string shows, and NULL reads as _null_. A value longer than 60 characters is cut and ends in `...`. The table stops at `max_rows` values, or earlier if the summary, its JSON comment included, would exceed 60,000 bytes. That keeps a pull request comment under GitHub's limit. When the table leaves values out, a last line says how many it showed. A pushdown run lists the values of its [row sample](pushdown.md#row-samples), so it needs `pushdown_sample_rows` too.

CI posts the summary where more people may read it than may read the data. Ask for values only where every reader of the job summary and the pull request may see them.

## OpenTelemetry metrics

`veridelta run --otel otel-metrics.json` writes the run's metrics to a file, and `--otel-send` sends them to an OTLP/HTTP endpoint. Either reaches an observability backend, such as Datadog or Grafana, through an OpenTelemetry Collector or the backend's own OTLP intake. Neither needs an OpenTelemetry package. From Python:

```python
from veridelta import DiffEngine, load_config
from veridelta.telemetry import send_otlp_metrics, write_otlp_metrics

diff, source, target = load_config("veridelta.yaml")
result = DiffEngine.run_from_configs(diff, source, target)
write_otlp_metrics(
    result, "reports/otel-metrics.json", config_path="veridelta.yaml", source=source, target=target
)
send_otlp_metrics(result, config_path="veridelta.yaml", source=source, target=target)
```

Every metric is a gauge stamped with the time of the run's export, so each run adds one point to each series:

| Metric | Unit | Attributes | Value |
| :--- | :--- | :--- | :--- |
| `veridelta.dataset.rows` | `{row}` | `veridelta.side`: `source` or `target` | Rows in each dataset. |
| `veridelta.diff.rows` | `{row}` | `veridelta.diff.kind`: `added`, `removed`, or `changed` | Rows only in the target, only in the source, or in both with a value that differs. |
| `veridelta.column.mismatched_rows` | `{row}` | `veridelta.column.name` | Changed rows whose value in that column differs. Every compared column reports, so a column that stops drifting reads `0` instead of disappearing. |
| `veridelta.diff.mismatch_ratio` | `1` | | Added, removed, and changed rows over the source rows, as `mismatch_ratio` is. |
| `veridelta.diff.match` | | | `1` when the run matched within `threshold`, `0` when it drifted. It has no unit, since a backend such as Prometheus reads a unit of `1` as a ratio. |

Resource attributes say which comparison ran:

- `service.name`, which is `veridelta` unless the environment renames it, and `service.version`;
- `veridelta.config.path`, the configuration file;
- `veridelta.source.type` and `veridelta.target.type`, such as `file` or `snowflake`;
- `veridelta.source.name` and `veridelta.target.name`: the table, or the file or lakehouse path.

A URL keeps only its scheme, host, and path, so a token in its user part or a signature in its query never leaves the run. An Azure `abfss://container@account` path keeps its container, which sits where a user would. A database `query` source has no name.

Like the Markdown summary by default, the export holds counts and column names, never row values, connection URIs, credentials, or SQL. A run that fails before it has a result writes no file and sends nothing.

The file is one line of JSON in OTLP's JSON encoding, as the [OpenTelemetry file exporter format](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md) specifies. The Collector's OTLP JSON file receiver, in its contrib distribution, can read it. The same line is the body `--otel-send` posts.

### Sending to an endpoint

`--otel-send` posts the export to an OTLP/HTTP endpoint, such as a Collector or a vendor's OTLP intake. The standard OpenTelemetry variables configure it:

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Base URL. `/v1/metrics` is added to its path. |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | | Full URL, used as written in place of the base URL. |
| `OTEL_EXPORTER_OTLP_HEADERS` | | Request headers, such as an API key, as comma-separated `key=value` items. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | Milliseconds to wait for the endpoint. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/json` | The protocol. Only `http/json` is supported. |

`HEADERS`, `TIMEOUT`, and `PROTOCOL` also have a metrics form, such as `OTEL_EXPORTER_OTLP_METRICS_HEADERS`. The metrics form wins over the general one, as it does in the OpenTelemetry SDKs.

Header keys and values are percent-encoded, as in `OTEL_RESOURCE_ATTRIBUTES`, so `Bearer <token>` is written `Bearer%20<token>`. No header value appears in a log line or an error, and neither does the endpoint's query. The send follows no redirect, so a header never reaches a second host. Over HTTPS, the endpoint's certificate is checked against the system's trust store.

The send is one attempt, with no retry. When it fails, such as on an unreachable endpoint, an HTTP error, or the timeout, the run exits `3` after writing its files. Under `--json`, stdout then holds the error object instead of the summary. An unusable variable, or a protocol other than `http/json`, fails the run the same way before anything is sent.

`--otel` and `--otel-send` together write and send the same export.

### Attributes from the environment

The standard OpenTelemetry variables add resource attributes, so a run can carry its environment or its team:

- `OTEL_RESOURCE_ATTRIBUTES` holds comma-separated `key=value` items, such as `deployment.environment=prod,team=data`. Percent-encode a comma, an equals sign, or a percent sign inside a key or value.
- `OTEL_SERVICE_NAME` replaces `service.name`. It wins over a `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, as the OpenTelemetry specification requires.

When the environment sets a key that Veridelta also sets, Veridelta's value wins, except for `service.name`. The instrumentation scope stays `veridelta`. The values are exported as written, so keep secrets out of them.

A malformed `OTEL_RESOURCE_ATTRIBUTES`, such as an item without `=` or a `%` that starts no valid escape, is ignored whole, as the specification recommends. The `veridelta.telemetry` logger then warns without repeating the value. On the command line, `--verbose` prints that warning. Without it, the variable is dropped silently.

## Artifacts

With `output_path` set, a run writes the differing rows to that directory as `added_rows`, `removed_rows`, and `changed_rows`, in the `output_format`. An empty set writes no file. Pushdown writes primary keys only, under other names; see [Differences from a local run](pushdown.md#differences-from-a-local-run).

`output_format` is `parquet` (the default), `csv`, `json`, `ndjson`, or `arrow`. CSV and JSON have no type for bytes, so `csv`, `json`, and `ndjson` write a binary column, such as a MySQL `BIT`, as hexadecimal text, and refuse a column that nests binary values in a list or a struct. Excel is not offered, because writing a workbook needs a second dependency that a discrepancy file does not justify. Neither is Avro: the Polars writer cannot store several types a discrepancy frame can hold, such as `Int8`, unsigned integers, nanosecond or timezone-aware timestamps, and all-NULL columns.
