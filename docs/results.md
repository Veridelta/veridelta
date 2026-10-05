# Results

`DiffEngine.run()` and `DiffEngine.run_from_configs()` return a `DiffResult`. It holds a summary of what differs on `summary`, and the rows behind it on `added`, `removed`, and `changed`, so a notebook can inspect the drift without writing files.

The summary prints as a plain-text report, and one call isolates the rows where a single column disagrees:

```python
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
| `column_mismatches` | For each column with at least one mismatch, the changed rows where it differs. |
| `total_mismatches` | Added, removed, and changed rows together. |
| `mismatch_ratio` | `total_mismatches` over `total_rows_source`. |
| `match_rate_percentage` | One minus `mismatch_ratio`, as a percentage rounded to two places. |
| `is_match` | Whether `mismatch_ratio` is at most `threshold`. |
| `is_perfect_match` | Whether nothing differs. |
| `volume_shift` | `total_rows_target` minus `total_rows_source`. |
| `report_summary` | A plain-text report of these counts and the most drifting columns. |

## Rows

`added` holds the rows found only in the target, and `removed` the rows found only in the source. `changed` holds the rows found in both with at least one difference. Each changed row carries its primary keys and, for every compared column, `{column}_source`, `{column}_target`, and `{column}_is_match`.

`get_mismatches(column)` returns the primary keys and both values for the rows where that column differs. It raises `ConfigError` for a column that was not compared. `to_pandas()` converts `changed` to pandas; it needs pandas and pyarrow, which Veridelta does not install.

### Pushdown results

[Pushdown](pushdown.md) compares in place and never selects values, so its frames hold primary keys only and the result sets `keys_only`. `get_mismatches` then returns every changed key instead of one column's values, and still rejects a column that was not compared.

A run with `pushdown_sample_rows` set also carries `changed_sample`: up to that many changed rows with each compared column's values, laid out like a local run's `changed`. See [Row samples](pushdown.md#row-samples).

## HTML report

`write_html` writes the standalone report that `veridelta run --html` writes. It embeds its own styles and script, so it opens offline. `max_rows` caps every table, as `--html-max-rows` does:

```python
from veridelta.report import write_html, write_markdown

write_html(result, "reports/nightly.html", max_rows=1000)
write_markdown(result, "reports/summary.md")
```

## Markdown summary

`write_markdown` writes the short summary that CI posts to a job summary or a pull request, and `render_markdown` returns it as text. It holds the verdict, a table of counts, and the drifting columns, up to `report_top_columns_limit`. Column names are written as code, so a name from the data cannot break the table or the page it lands on.

## OpenTelemetry metrics

`veridelta run --otel otel-metrics.json` writes the run's metrics for an observability backend, such as Datadog or Grafana, through an OpenTelemetry Collector or any OTLP/HTTP endpoint. It needs no OpenTelemetry package. From Python:

```python
from veridelta.telemetry import write_otlp_metrics

write_otlp_metrics(
    result, "reports/otel-metrics.json", config_path="veridelta.yaml", source=source, target=target
)
```

Every metric is a gauge stamped with the time the file is written, so each run adds one point to each series:

| Metric | Unit | Attributes | Value |
| :--- | :--- | :--- | :--- |
| `veridelta.dataset.rows` | `{row}` | `veridelta.side`: `source` or `target` | Rows in each dataset. |
| `veridelta.diff.rows` | `{row}` | `veridelta.diff.kind`: `added`, `removed`, or `changed` | Rows only in the target, only in the source, or in both with a value that differs. |
| `veridelta.column.mismatched_rows` | `{row}` | `veridelta.column.name` | Changed rows whose value in that column differs. Every compared column reports, so a column that stops drifting reads `0` instead of disappearing. |
| `veridelta.diff.mismatch_ratio` | `1` | | Added, removed, and changed rows over the source rows, as `mismatch_ratio` is. |
| `veridelta.diff.match` | | | `1` when the run matched within `threshold`, `0` when it drifted. It has no unit, since a backend such as Prometheus reads a unit of `1` as a ratio. |

Resource attributes say which comparison ran:

- `service.name`, which is `veridelta`, and `service.version`;
- `veridelta.config.path`, the configuration file;
- `veridelta.source.type` and `veridelta.target.type`, such as `file` or `snowflake`;
- `veridelta.source.name` and `veridelta.target.name`: the table, or the file or lakehouse path.

A URL keeps only its scheme, host, and path, so a token in its user part or a signature in its query never leaves the run. An Azure `abfss://container@account` path keeps its container, which sits where a user would. A database `query` source has no name.

Like the Markdown summary, the file holds counts and column names, never row values, connection URIs, credentials, or SQL. A run that fails before it has a result writes no file.

Veridelta does not read `OTEL_RESOURCE_ATTRIBUTES`. To tag runs with an environment or a team, add attributes in the Collector, such as with its `resource` processor.

The file is one line of JSON in OTLP's JSON encoding, as the [OpenTelemetry file exporter format](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md) specifies. The Collector's OTLP JSON file receiver, in its contrib distribution, can read it. The same line is the body an OTLP/HTTP endpoint accepts:

```bash
curl --fail -X POST -H "Content-Type: application/json" \
  --data-binary @otel-metrics.json "$OTLP_ENDPOINT/v1/metrics"
```

## Artifacts

With `output_path` set, a run writes the differing rows to that directory as `added_rows`, `removed_rows`, and `changed_rows`, in the `output_format`. An empty set writes no file. Pushdown writes primary keys only, under other names; see [Differences from a local run](pushdown.md#differences-from-a-local-run).

`output_format` is `parquet` (the default), `csv`, `json`, `ndjson`, or `arrow`. Excel is not offered, because writing a workbook needs a second dependency that a discrepancy file does not justify. Neither is Avro: the Polars writer cannot store several types a discrepancy frame can hold, such as `Int8`, unsigned integers, nanosecond or timezone-aware timestamps, and all-NULL columns.
