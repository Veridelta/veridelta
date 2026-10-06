---
type: Users
title: Who Veridelta serves
description: The personas Veridelta serves, the users it refuses, and the use cases each persona brings, each with the evidence for it.
status: draft
generated: { by: claude-code, at: 2026-10-06T09:45:00Z }
---

# Who Veridelta serves

This document is the first of the product bundle, and the rest point back at it: a feature names the use case it serves, and a metric in [the key metrics](KEY_METRICS.md) names the use cases it proves. Each persona and use case carries its evidence and what is still unknown, because Veridelta has no telemetry and few named users. The maintainer confirms or cuts each one. This revision holds one persona and one use case; the others follow once the maintainer's answers are in.

## The setting

Veridelta compares two datasets on their primary keys and reports every row that differs once the declared rules apply. It runs on a laptop, in CI, or inside a warehouse, where only counts and keys come back. The people around it are the engineer who runs the comparison, the reviewer who reads its report, and the agent that drives its command line.

## Personas

### P-01: The engineer verifying a migration

- **Role and setting.** A data or analytics engineer moving a table or a pipeline from one system to another, who has to show that the new one matches the old one before anyone switches over.
- **What they compare, and where it lives.** Two tables in one warehouse (Snowflake, Databricks, BigQuery, Postgres, or DuckDB), or exports of them as files on disk.
- **The tools they use today.** A hand-written SQL `EXCEPT` or `MINUS` query, `pandas.DataFrame.compare`, a spreadsheet, or a notebook they eyeball.
- **What they read from Veridelta.** The exit code and the summary counts in CI, the Markdown summary on the pull request, and the HTML report when something differs.
- **What they must never be shown.** Credentials or SQL in any output, and row values in a log or a pull request comment unless they raised `markdown-max-rows` above zero.
- **Tolerances.** A tolerance is theirs to declare, column by column. A match that forgives something no rule names is never acceptable.
- **Trust posture.** They believe a verdict they can audit: the rules are in a file under version control, the counts come back, and a local run and a pushdown run agree.
- **Success, in one sentence.** The migrated table is signed off from one CI run, with a report to hand over, and no row leaves the warehouse.
- **Evidence.** The README's first sentence and its "same verdict in the warehouse" feature; [the pushdown guide](../docs/pushdown.md); tutorial 5, which validates a configuration and runs the GitHub Action; `action.yml`; the `snowflake`, `databricks`, and `bigquery` extras.
- **Unknown.** Who they are by name, the size of their team, how many tables they move, and whether their CI is GitHub Actions, GitLab, or neither. To learn it: the maintainer's six answers, then the bug report and feature request forms, which ask for the environment.

## Anti-personas

None is written yet. The next revision names who Veridelta refuses, such as a team that wants a general data quality framework, profiling, or streaming checks, and how the README's first sentence turns them away.

## Use cases

### UC-01: A first verdict on two files

- **The persona.** P-01, on a laptop, before any CI exists.
- **The moment.** An export of the old table and one of the new table sit in two files. Someone asks what changed, and cutover waits on the answer.
- **The question, in their words.** "Does the new file match the old one on the keys, and where does it differ?"
- **The data, and where it lives.** Two files on disk, in any format Veridelta reads: CSV, Parquet, JSON, Arrow, Avro, or Excel. The keys are known.
- **Required latency.** Seconds to a few minutes, on the laptop.
- **What a wrong answer costs, and to whom.** A false match ships a broken migration to everyone downstream. A false mismatch costs the engineer a day of hunting. The first is never acceptable.
- **The alternative it beats.** A SQL `EXCEPT` in DuckDB, which needs the same shape on both sides and shows no counts by column; `pandas.DataFrame.compare`, which needs identical labels and holds both files in memory; a notebook.
- **Refusal behavior.** A missing or unreadable file, a repeated key, or a cast the column's type cannot take is named before the comparison starts, with exit code 3.
- **The docs that describe it.** The README's quick start, tutorial 2, and [the command line](../docs/cli.md).
- **The metric that proves it.** DR-01, the keys written before the first verdict, and NS-01.

## Considered and cut

Nothing yet. A use case that does not beat the tool a person opens today moves here with the reason.

## Traceability

| Use case | Persona | Feature and docs page | Metric |
| :--- | :--- | :--- | :--- |
| UC-01 | P-01 | `veridelta validate` and `veridelta run` on two files: the README's quick start, [the command line](../docs/cli.md) | DR-01, NS-01 |
