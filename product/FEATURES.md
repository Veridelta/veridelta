---
type: Feature Map
title: The feature map
description: Every feature and every roadmap item against the use case it serves, the page that describes it, and the metric that proves it.
status: draft
generated: { by: claude-code, at: 2026-10-11T11:42:32Z }
---

# The feature map

Every feature Veridelta ships, and every item on [the roadmap](../docs/roadmap.md), sits here against the use case it serves in [who Veridelta serves](USERS.md), the page that describes it, and the metric in [the key metrics](KEY_METRICS.md) that proves it. A feature that serves no use case is justified at the end, or cut. A pull request names the use case its change serves, and a test holds the roadmap to the same rule.

## Shipped

### Reading data

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| CSV, Parquet, JSON, NDJSON, Arrow, Avro, and Excel files, read lazily where Polars can | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Sources](../docs/sources.md) | [NS-01](metrics/NS-01.md), [DR-01](metrics/DR-01.md) |
| `format`, read from the path's suffix unless written, and reader `options` on a file source | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Sources](../docs/sources.md) | [DR-01](metrics/DR-01.md), [DR-02](metrics/DR-02.md) |
| Delta Lake and Iceberg tables, at a version or a snapshot | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), for a table read like a file | [Sources](../docs/sources.md) | [GR-02](metrics/GR-02.md) |
| Postgres, MySQL, SQL Server, Oracle, SQLite, and other databases through ConnectorX, as a table or a query, in partitions | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), for a table read like a file | [Sources](../docs/sources.md) | [GR-02](metrics/GR-02.md) |
| DuckDB files and MotherDuck databases | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Sources](../docs/sources.md) | [GR-02](metrics/GR-02.md) |
| `${NAME}` references to environment variables in any source field, so no credential sits in the file | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Configuration](../docs/configuration.md) | [GR-02](metrics/GR-02.md) |

### Declaring what counts as a match

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| Rules selected by `column_names` or a `pattern`, with `ignore` and `rename_to` | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md) | [GR-02](metrics/GR-02.md) |
| Null sentinels with `null_values`, typed, and a default for every column | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md) | [GR-02](metrics/GR-02.md) |
| Regular expression replacement, whitespace stripping, and case folding | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md) | [GR-02](metrics/GR-02.md) |
| Value maps, and `crosswalk` to propose them from the data with their evidence | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md), [Command line](../docs/cli.md) | [GR-02](metrics/GR-02.md) |
| `veridelta suggest`, which suggests a numeric tolerance, trimming, case folding, a date format, or null sentinels from the pairs that differ, with the rows each explains, and calls no model | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Command line](../docs/cli.md), [Rules](../docs/rules.md) | [NS-01](metrics/NS-01.md) |
| Accepted drift with `run --baseline`, which fails only on drift the file does not list, on a local run and on a pair compared where it is stored, and `--save-baseline`, which writes the file from a local run | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Command line](../docs/cli.md) | [GR-02](metrics/GR-02.md) |
| Zero padding, date parsing with `datetime_format` and `timezone`, and `cast_to` | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md) | [GR-02](metrics/GR-02.md) |
| Absolute and relative tolerances, with defaults | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md) | [GR-02](metrics/GR-02.md) |
| Fuzzy text matching by edit distance or Jaro-Winkler similarity, with the `fuzzy` extra | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md), [tutorial 6](../docs/examples/06_model_evaluation_runs.ipynb) | [GR-02](metrics/GR-02.md) |
| Null equality, `strict_types`, and `schema_mode` for columns on one side only | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Rules](../docs/rules.md), [Configuration](../docs/configuration.md) | [GR-02](metrics/GR-02.md) |

### Comparing

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| `DiffEngine` on two `LazyFrame`s, with `DiffConfig`, `DiffRule`, and `DiffResult` | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [API reference](../docs/api.md) | [GR-02](metrics/GR-02.md) |
| `veridelta run`, with the text summary and exit codes 0, 1, and 3 | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Command line](../docs/cli.md) | [NS-01](metrics/NS-01.md), [DR-02](metrics/DR-02.md) |
| `veridelta run SOURCE TARGET --key COLUMN`, two files compared with no configuration file | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Command line](../docs/cli.md) | [NS-01](metrics/NS-01.md) |
| `veridelta validate`, with `--schemas` and `--allow-missing-env`, before any row is read, whose `--json` places each finding in the file by path, or by line and column | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request), [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Command line](../docs/cli.md) | [DR-02](metrics/DR-02.md) |
| Pushdown inside Snowflake, Databricks, BigQuery, Postgres, SQL Server with Azure SQL and Synapse, Redshift, and DuckDB, with only counts and keys coming back | [UC-05](USERS.md#uc-05-compare-two-tables-where-they-are-stored) | [Pushdown](../docs/pushdown.md) | [GR-01](metrics/GR-01.md) |
| `--verbose`, printing connector activity on stderr, never a credential | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Command line](../docs/cli.md) | [DR-02](metrics/DR-02.md) |

### Reading the result

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| The JSON summary with `--json`, and one JSON object for a failure | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Command line](../docs/cli.md), [Results](../docs/results.md) | [DR-02](metrics/DR-02.md) |
| A published JSON Schema for what `run`, `validate`, `crosswalk`, and `suggest` print with `--json`, for the error object, and for a baseline file, printed by `veridelta schema NAME` | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Command line](../docs/cli.md), [AI agents](../docs/agents.md) | [DR-02](metrics/DR-02.md) |
| The HTML report, readable without JavaScript and by keyboard, checked with axe-core: the verdict in words first, then each mismatched value with its difference, filtered by column and key in a view a link can carry | [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Results](../docs/results.md) | [GR-03](metrics/GR-03.md) |
| The Markdown summary for a pull request, with row values off unless asked | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request), [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Results](../docs/results.md) | [GR-03](metrics/GR-03.md) |
| The Markdown summary's counts as JSON, in an HTML comment readers never see, for an agent that reads the pull request | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request), [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Results](../docs/results.md), [AI agents](../docs/agents.md) | [DR-02](metrics/DR-02.md) |
| The run record: hashes of the configuration and the baseline, the commit, and the CI run, in `run --json`, the HTML report, and the pull request comment, so a verdict traces to the files it judged ([decision](../decisions/git-and-github-are-the-ledger.md)) | [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone), [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Results](../docs/results.md), [CI integrations](../docs/ci.md) | [GR-03](metrics/GR-03.md) |
| Discrepancy files of the rows that differ, as CSV, Parquet, JSON, NDJSON, or Arrow | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Results](../docs/results.md) | [GR-02](metrics/GR-02.md) |

### Running it elsewhere

| Feature | Use case | Described in | Metric |
| :--- | :--- | :--- | :--- |
| The GitHub Action, with one comment per configuration on the pull request, and a `baseline` input that accepts the drift a file lists | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [CI integrations](../docs/ci.md) | [GR-02](metrics/GR-02.md), [DR-03](metrics/DR-03.md) |
| The JSON Schema for configuration files, for completion and checking in an editor | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Configuration](../docs/configuration.md) | [DR-02](metrics/DR-02.md) |
| The schema mapped to `veridelta*.yaml` in VS Code, the YAML extension recommended, and a task that runs `veridelta validate` on the open file with its verdict in the Problems panel | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Configuration](../docs/configuration.md) | [DR-02](metrics/DR-02.md) |
| The VS Code extension's validate command, released from its own repository, which runs `veridelta validate --json` on the open file and puts each finding in the Problems panel on the line it points to | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Configuration](../docs/configuration.md) | [DR-02](metrics/DR-02.md) |
| The extension's run command, which runs the comparison behind a progress notification, opens the HTML report in an editor tab, and opens the discrepancy files the configuration names | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Configuration](../docs/configuration.md) | [DR-02](metrics/DR-02.md) |
| The extension's registration of `veridelta mcp` with agent mode, one root per workspace folder, with its settings as the server's flags | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [AI agents](../docs/agents.md) | [DR-02](metrics/DR-02.md) |
| The AI agents page, `llms.txt`, and the installable agent skill | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [AI agents](../docs/agents.md) | [DR-02](metrics/DR-02.md) |
| The fix loop for an agent: change the pipeline, run the comparison, read the counts, fix the cause, and stop on a match or on drift the user asked for | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [AI agents](../docs/agents.md) | [DR-02](metrics/DR-02.md) |
| `veridelta mcp`, which serves `validate_config`, `run_comparison`, `describe_schema`, `read_discrepancies`, `propose_value_maps`, and `suggest_rules` to an agent's host over stdio, held to the folders it was started with, with row values off unless allowed and then capped | [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [AI agents](../docs/agents.md), [Command line](../docs/cli.md) | [DR-02](metrics/DR-02.md) |

## On the roadmap

| Item | Use case | Metric it would move |
| :--- | :--- | :--- |
| Matching text by meaning, and a run summary written by a model | None yet, as the roadmap says | None |
| Mismatch patterns and the rules applied in the report, and a page on tracing a verdict to a signed commit and an approved review ([issue 440](https://github.com/Veridelta/veridelta/issues/440)) | [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone), [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | None measures sign-off yet. [GR-03](metrics/GR-03.md) must stay at zero as the report changes. |

## Served by no use case, and why they stay

| Feature | Why it stays |
| :--- | :--- |
| The GitLab CI template | Frozen by [a decision record](../decisions/gitlab-template-is-frozen.md): nobody has named GitLab, removal would break a user nobody has seen, and keeping it costs nothing until an Action input needs mirroring. |
| OpenTelemetry metrics, written with `--otel` or sent with `--otel-send` | No persona watches a dashboard yet. They stay for the migration engineer's CI, and the first user who reads them gives them a use case. |
| The NYC taxi sample in `veridelta.datasets` | Tutorial data, not a feature. |
