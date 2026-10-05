# Results

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`. It carries the metrics on `.summary` and the rows behind them on `.added`, `.removed`, and `.changed`, so a notebook never has to export artifacts to disk just to look at the drift.

## Rows

```python
result = DiffEngine(diff, source_df, target_df).run()

print(result.summary.report_summary)

# Just the rows where one column disagreed, with both values side by side.
result.get_mismatches("total_amount")

# The full changed set as pandas, if you have it installed.
result.to_pandas()
```

`summary` stays a plain Pydantic model, so `summary.model_dump_json()` still produces a clean, frame-free payload for CI logs.

## Pushdown results

Warehouse pushdown compares in place and never projects values, so its frames hold primary keys alone and the result is flagged `keys_only`. `get_mismatches` there returns every changed key rather than one column's values, and still rejects a column that was not part of the comparison. A run with `pushdown_sample_rows` set also carries `changed_sample`: up to that many changed rows with each compared column's values, laid out like a local run's `changed`; see [Row samples](pushdown.md#row-samples).

## HTML report

The same standalone HTML report the CLI writes with `--html` is available from Python. It embeds its own styles and script, so it opens offline, and `max_rows` caps every table the way `--html-max-rows` does:

```python
from veridelta.report import write_html, write_markdown

write_html(result, "reports/nightly.html", max_rows=1000)
write_markdown(result, "reports/summary.md")
```

## Markdown summary

`write_markdown` (and `render_markdown`, which returns the text) produces the short form CI posts to a job summary or a pull request: the verdict, a table of counts, and the drifting columns, limited to `report_top_columns_limit`. Column names are written as code so a name from the data cannot break the table or the page it lands on.

## OpenTelemetry metrics

`veridelta run --otel otel-metrics.json` writes the run's metrics for an observability backend such as Datadog or Grafana, through an OpenTelemetry Collector or any OTLP/HTTP endpoint. It needs no OpenTelemetry package. From Python:

```python
from veridelta.telemetry import write_otlp_metrics

write_otlp_metrics(
    result, "reports/otel-metrics.json", config_path="veridelta.yaml", source=source, target=target
)
```

Every metric is a gauge, stamped with the time the file is written, so each run adds one point to each series:

| Metric | Unit | Attributes | Value |
| :--- | :--- | :--- | :--- |
| `veridelta.dataset.rows` | `{row}` | `veridelta.side`: `source` or `target` | Rows in each dataset. |
| `veridelta.diff.rows` | `{row}` | `veridelta.diff.kind`: `added`, `removed`, or `changed` | Rows only in the target, only in the source, or in both with a value that differs. |
| `veridelta.column.mismatched_rows` | `{row}` | `veridelta.column.name` | Changed rows whose value in that column differs. Every compared column reports, so one that stops drifting reads `0` rather than disappearing. |
| `veridelta.diff.mismatch_ratio` | `1` | | Added, removed, and changed rows over the source rows, as `mismatch_ratio` is. |
| `veridelta.diff.match` | | | `1` when the run matched within `threshold`, `0` when it drifted. It has no unit, since a backend such as Prometheus would read a unit of `1` as a ratio. |

Resource attributes say which comparison ran:

- `service.name`, which is `veridelta`, and `service.version`;
- `veridelta.config.path`, the configuration file;
- `veridelta.source.type` and `veridelta.target.type`, such as `file` or `snowflake`;
- `veridelta.source.name` and `veridelta.target.name`: the table, or the file or lakehouse path.

A URL keeps only its scheme, host, and path, so a token in its user part or a signature in its query never leaves the run; an Azure `abfss://container@account` path keeps its container, which sits where a user would. A database `query` source has no name. Like the Markdown summary, the file holds counts and column names, never row values, connection URIs, credentials, or SQL. A run that fails before it has a result writes no file. Veridelta does not read `OTEL_RESOURCE_ATTRIBUTES`; to tag runs with an environment or a team, add attributes in the Collector, with its `resource` processor for one.

The file is one line of JSON in OTLP's JSON encoding, as the [OpenTelemetry file exporter format](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md) specifies, so the Collector's OTLP JSON file receiver, in its contrib distribution, can read it. The same line is the body an OTLP/HTTP endpoint accepts:

```bash
curl --fail -X POST -H "Content-Type: application/json" \
  --data-binary @otel-metrics.json "$OTLP_ENDPOINT/v1/metrics"
```

## Artifacts

Discrepancy artifacts write to `csv`, `parquet`, `json`, `ndjson`, or `arrow` via `output_format`. Excel is deliberately absent: writing a workbook needs a second dependency that a discrepancy dump does not justify. So is Avro: the Polars writer cannot store several types a discrepancy frame can hold, such as `Int8`, unsigned integers, nanosecond or timezone-aware timestamps, and all-NULL columns.
