---
type: Feature Map
title: The feature map
description: Every feature and every roadmap item against the use case it serves, the page that describes it, and the metric that proves it.
status: draft
generated: { by: claude-code, at: 2026-10-06T16:05:00Z }
---

# The feature map

Every feature Veridelta ships, and every item on [the roadmap](../docs/roadmap.md), sits here against the use case it serves in [who Veridelta serves](USERS.md), the page that describes it, and the metric in [the key metrics](KEY_METRICS.md) that proves it. A feature that serves no use case is justified at the end, or cut. A pull request names the use case its change serves, and a test holds the roadmap to the same rule.

## Shipped at 0.14.0

### Reading data

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| CSV, Parquet, JSON, NDJSON, Arrow, Avro, and Excel files, read lazily where Polars can | UC-01 | [Sources](../docs/sources.md) | NS-01, DR-01 |
| `format` and reader `options` on a file source | UC-01 | [Sources](../docs/sources.md) | DR-01, DR-02 |
| Delta Lake and Iceberg tables, at a version or a snapshot | UC-01, for a table read like a file | [Sources](../docs/sources.md) | GR-02 |
| Postgres, MySQL, SQL Server, Oracle, SQLite, and other databases through ConnectorX, as a table or a query, in partitions | UC-01, for a table read like a file | [Sources](../docs/sources.md) | GR-02 |
| DuckDB files and MotherDuck databases | UC-01 | [Sources](../docs/sources.md) | GR-02 |
| `${NAME}` references to environment variables in any source field, so no credential sits in the file | UC-02 | [Configuration](../docs/configuration.md) | GR-02 |

### Declaring what counts as a match

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| Rules selected by `column_names` or a `pattern`, with `ignore` and `rename_to` | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Null sentinels with `null_values`, typed, and a default for every column | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Regular expression replacement, whitespace stripping, and case folding | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Value maps, and `crosswalk` to propose them from the data with their evidence | UC-01 | [Rules](../docs/rules.md), [Command line](../docs/cli.md) | GR-02 |
| Zero padding, date parsing with `datetime_format` and `timezone`, and `cast_to` | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Absolute and relative tolerances, with defaults | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Fuzzy text matching by edit distance or Jaro-Winkler similarity, with the `fuzzy` extra | UC-01 | [Rules](../docs/rules.md) | GR-02 |
| Null equality, `strict_types`, and `schema_mode` for columns on one side only | UC-01 | [Rules](../docs/rules.md), [Configuration](../docs/configuration.md) | GR-02 |

### Comparing

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| `DiffEngine` on two `LazyFrame`s, with `DiffConfig`, `DiffRule`, and `DiffResult` | UC-01 | [API reference](../docs/api.md) | GR-02 |
| `veridelta run`, with the text summary and exit codes 0, 1, and 3 | UC-01, UC-02 | [Command line](../docs/cli.md) | NS-01, DR-02 |
| `veridelta validate`, with `--schemas` and `--allow-missing-env`, before any row is read | UC-02, UC-04 | [Command line](../docs/cli.md) | DR-02 |
| Pushdown inside Snowflake, Databricks, BigQuery, Postgres, and DuckDB, with only counts and keys coming back | UC-05 | [Pushdown](../docs/pushdown.md) | GR-01 |
| `--verbose`, printing connector activity on stderr, never a credential | UC-02 | [Command line](../docs/cli.md) | DR-02 |

### Reading the result

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| The JSON summary with `--json`, and one JSON object for a failure | UC-04 | [Command line](../docs/cli.md), [Results](../docs/results.md) | DR-02 |
| The HTML report, readable without JavaScript and by keyboard, checked with axe-core | UC-03 | [Results](../docs/results.md) | GR-03 |
| The Markdown summary for a pull request, with row values off unless asked | UC-02, UC-03 | [Results](../docs/results.md) | GR-03 |
| Discrepancy files of the rows that differ, as CSV, Parquet, JSON, NDJSON, or Arrow | UC-01, UC-03 | [Results](../docs/results.md) | GR-02 |

### Running it elsewhere

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| The GitHub Action, with one comment per configuration on the pull request | UC-02 | [CI integrations](../docs/ci.md) | GR-02, DR-03 |
| The JSON Schema for configuration files, for completion and checking in an editor | UC-01 | [Configuration](../docs/configuration.md) | DR-02 |
| The AI agents page, `llms.txt`, and the installable agent skill | UC-04 | [AI agents](../docs/agents.md) | DR-02 |

## On the roadmap

| Item | Use case | Metric it would move |
| :--- | :--- | :--- |
| Pushdown for more SQL dialects, such as Redshift and Synapse | UC-05 | GR-01 |
| The schema wired into VS Code, then the extension, its run command, and its MCP registration | UC-01, UC-03, UC-04 | NS-01, DR-02 |
| Schemas for the JSON the commands print | UC-04 | DR-02 |
| A tutorial comparing two model evaluation runs | UC-01 | NS-01 |
| A parsable section of the Action's pull request comment, and a recipe for an agent's fix loop | UC-02, UC-04 | DR-02 |
| `veridelta mcp` and its guardrails | UC-04 | DR-02 |
| `veridelta suggest`, proposing rules from the pairs that differ, with no model | UC-01 | NS-01 |
| Accepted drift with `run --baseline` | UC-02 | GR-02 |
| Matching text by meaning, and a run summary written by a model | None yet, as the roadmap says | None |

## Served by no use case, and why they stay

| Feature | Why it stays |
| :--- | :--- |
| The GitLab CI template | Frozen by [a decision record](../decisions/gitlab-template-is-frozen.md): nobody has named GitLab, removal would break a user nobody has seen, and keeping it costs nothing until an Action input needs mirroring. |
| OpenTelemetry metrics, written with `--otel` or sent with `--otel-send` | No persona watches a dashboard yet. They stay for the migration engineer's CI, and the first user who reads them gives them a use case. |
| `DataIngestor` in the Python API | Used by no code in the repository, and its own docstring warns against feeding its output to `DiffEngine`. A deprecation candidate, recorded on the cleanup issue, #124. |
| The NYC taxi sample in `veridelta.datasets` | Tutorial data, not a feature. |
