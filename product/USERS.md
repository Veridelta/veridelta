---
type: Users
title: Who Veridelta serves
description: The personas Veridelta serves, the users it refuses, and the use cases each persona brings, each with the evidence for it.
status: draft
generated: { by: claude-code, at: 2026-10-06T15:10:00Z }
---

# Who Veridelta serves

This document is the first of the product bundle, and the rest point back at it: a feature names the use case it serves, and a metric in [the key metrics](KEY_METRICS.md) names the use cases it proves. Each persona and use case carries its evidence and what is still unknown, because Veridelta has no telemetry and one known user. The maintainer confirms or cuts each one in review.

## The setting

Veridelta compares two datasets on their primary keys and reports every row that differs once the declared rules apply. It runs on a laptop, in CI, or inside a warehouse, where only counts and keys come back. At 0.14.0 it has one known user: its maintainer, who compares CSV files on a laptop, in notebooks, and in GitHub Actions, and who has never used a warehouse. Nobody else is known to have run it. So one persona has a person behind it, and the others are drawn from what the product can do. Each card says which.

### What the maintainer said

Asked six questions on 2026-10-06, the maintainer answered on [issue 123](https://github.com/Veridelta/veridelta/issues/123#issuecomment-6013919964). The answers, word for word:

1. Who has run Veridelta besides you? "No one."
2. Where: GitHub Actions, GitLab, a laptop, somewhere else? "Local and GitHub and notebooks. I use what I make but tbh I'm not all that of a power user myself."
3. On what data? "I'm only using csvs for the most part. I have never touched a data lake or a ware house in my life."
4. What did they ask for? "Not sure what you are asking."
5. What did they give up on, or never finish? "Not sure what you are asking."
6. What must Veridelta never become? "Veridelta can never be riddled with bugs and can cater to super users but cannot discourage newcomers (high ceiling is okay as long as the barrier of entry stays small)."

Questions 4 and 5 were put badly, and with no other user they have no subject. They return as two questions to the maintainer alone: what they check when they run Veridelta, and what has made them stop or look something up.

The last answer is the product's constraint, read as three rules. A newcomer reaches a correct verdict with nothing to learn first. A power user can declare every rule the comparison needs. A wrong verdict is a bug before it is anything else. [The key metrics](KEY_METRICS.md) measure the first and guard the third.

## Personas

[P-02](#p-02-the-developer-comparing-two-csv-files) is the primary persona, the one with a person behind it. [P-01](#p-01-the-engineer-verifying-a-migration) keeps the id it had in the first revision.

### P-01: The engineer verifying a migration

- **Role and setting.** A data or analytics engineer moving a table or a pipeline from one system to another, who has to show that the new one matches the old one before anyone switches over.
- **What they compare, and where it lives.** Two tables in one warehouse (Snowflake, Databricks, BigQuery, Postgres, or DuckDB), or exports of them as files on disk.
- **The tools they use today.** A hand-written SQL `EXCEPT` or `MINUS` query, `pandas.DataFrame.compare`, a spreadsheet, or a notebook they eyeball.
- **What they read from Veridelta.** The exit code and the summary counts in CI, the Markdown summary on the pull request, and the HTML report when something differs.
- **What they must never be shown.** Credentials or SQL in any output, and row values in a log or a pull request comment unless they raised `markdown-max-rows` above zero.
- **Tolerances.** A tolerance is theirs to declare, column by column. A match that forgives something no rule names is never acceptable.
- **Trust posture.** They believe a verdict they can audit: the rules are in a file under version control, the counts come back, and a local run and a pushdown run agree.
- **Success, in one sentence.** The migrated table is signed off from one CI run, with a report to hand over, and no row leaves the warehouse.
- **Evidence.** The product, not a person: the README's "comparison inside the warehouse" feature, [the pushdown guide](../docs/pushdown.md), tutorial 5, `action.yml`, and the `snowflake`, `databricks`, and `bigquery` extras. The maintainer has never used a warehouse (answer 3), and chose to verify these backends against the live services anyway. That run is [issue 111](https://github.com/Veridelta/veridelta/issues/111), and releases ship before it by the maintainer's decision of 2026-10-06.
- **Unknown.** Whether this person exists, their team, how many tables they move, and which CI they run. To learn it: the bug report and feature request forms, which ask for the environment, and Discussions.

### P-02: The developer comparing two CSV files

- **Role and setting.** A developer or analyst who changed something, a script, a query, or an export, and holds the before and the after as two CSV files. They work alone and do not call themselves a power user.
- **What they compare, and where it lives.** Two CSV files on a laptop, in a notebook, or in a GitHub Actions job.
- **The tools they use today.** `diff`, a spreadsheet, `pandas`, or reading both files by eye.
- **What they read from Veridelta.** The summary on the terminal, the exit code in CI, and the HTML report or the discrepancy files when the counts are not zero.
- **What they must never be shown.** A traceback where an error belongs, an error that names a symptom instead of its cause, or a step they have to look up before the first run.
- **Tolerances.** None at first. A rule arrives when a float differs in its last digit or a date is written two ways, and it goes in the YAML file.
- **Trust posture.** They believe a verdict when the rows it names are the rows they expected, so the discrepancy files matter as much as the counts.
- **Success, in one sentence.** They know what changed between the two files after one command, without reading the docs first.
- **Evidence.** The maintainer's answers 1 to 3 and 6 above; `tests/fixtures/ci/`, which compares CSV files; tutorial 2, which runs the command line on files.
- **Unknown.** How large the files are, how often they compare, what they do with the result, and what has made them stop. To learn it: the two questions above, then the bug report form.

### P-03: The reviewer who reads the report

- **Role and setting.** Someone who did not run the comparison and has to sign off on it: a teammate on the pull request, a lead, or an auditor. They may read with a screen reader or a keyboard alone.
- **What they compare, and where it lives.** Nothing themselves. They read what [P-01](#p-01-the-engineer-verifying-a-migration) or [P-02](#p-02-the-developer-comparing-two-csv-files) produced: the Markdown summary on the pull request, or the HTML report.
- **The tools they use today.** The pull request's diff, a screenshot pasted into a chat, a spreadsheet someone exported.
- **What they read from Veridelta.** The verdict in words, PASSED or FAILED, the counts by column, and the rows that differ, up to the limit the runner set.
- **What they must never be shown.** Credentials, SQL, or a row value the runner did not choose to share.
- **Tolerances.** Whatever the runner declared. They need to see which rules applied.
- **Trust posture.** They believe a report that names its rules and its counts, and that reads without color or a mouse.
- **Success, in one sentence.** They can approve or refuse the change from the report alone.
- **Evidence.** The product, not a person: `src/veridelta/report.py` and the Markdown summary, `action.yml`, which posts the summary on the pull request, and `ACCESSIBILITY.md`. No reviewer is known; the maintainer works alone (answer 1).
- **Unknown.** Whether such a reader exists yet, and what they ask of the report. To learn it: the accessibility and feature request forms, and the first team that adopts the Action.

### P-04: The AI coding agent driving the command line

- **Role and setting.** A coding agent, such as Claude Code or Cursor, running Veridelta for a person: in a chat, in a fix loop, or on a schedule.
- **What they compare, and where it lives.** Whatever the person's configuration names. The agent reads the file, not the data.
- **The tools they use today.** The command line with `--json`, the exit codes, and the published `llms.txt`.
- **What they read from Veridelta.** The JSON summary, the exit code, and `validate --json` before a run.
- **What they must never be shown.** Row values and credentials, so that neither ends up in a transcript or a pull request comment.
- **Tolerances.** The person's. The agent proposes a rule and shows its evidence, as `crosswalk` does, and the person adds it.
- **Trust posture.** It trusts a stable JSON shape and a documented exit code, and nothing it has to parse from prose.
- **Success, in one sentence.** It runs a comparison and reports the counts without a person reading the docs for it.
- **Evidence.** [The AI agents page](../docs/agents.md), `skills/veridelta/SKILL.md`, the `llms.txt` hook, and this repository's own development: agents in Claude Code sessions run `veridelta` here, and measured the metrics in this bundle.
- **Unknown.** Which agents people use, and whether a person lets one add a rule. To learn it: the roadmap's MCP server, once it exists, and the issue forms.

## Anti-personas

### AP-01: The team wanting a data quality framework

A team that wants expectations on one dataset, profiling, schedules, and alerts. Veridelta compares two datasets and never profiles one: the README's first sentence says so, and `validate` has no expectation to declare. They are better served by a tool built for that, and Veridelta does not grow toward them.

### AP-02: The user wanting a model to decide what counts as drift

Someone who wants to hand two datasets to a language model and ask whether the differences matter. Veridelta's promise is that nothing is forgiven unless a rule says so, and a model that forgives drift cannot show why. No model call ever decides a verdict. A model may sit on top, driving the command line or proposing a rule with its evidence, as the roadmap's AI workflows say.

## Use cases

### UC-01: A first verdict on two files

- **The persona.** [P-02](#p-02-the-developer-comparing-two-csv-files), on a laptop, before any CI exists. [P-01](#p-01-the-engineer-verifying-a-migration) too, on exports of two tables.
- **The moment.** Something changed, a script, a query, or a migration, and the before and the after sit in two files. Someone asks what changed, and the next step waits on the answer.
- **The question, in their words.** "Does the new file match the old one on the keys, and where does it differ?"
- **The data, and where it lives.** Two files on disk, in any format Veridelta reads: CSV, Parquet, JSON, Arrow, Avro, or Excel. The keys are known.
- **Required latency.** Seconds to a few minutes, on the laptop.
- **What a wrong answer costs, and to whom.** A false match ships a broken change to everyone downstream. A false mismatch costs the person a day of hunting. The first is never acceptable.
- **The alternative it beats.** `diff`, which knows nothing about keys or columns; a SQL `EXCEPT` in DuckDB, which needs the same shape on both sides and shows no counts by column; `pandas.DataFrame.compare`, which needs identical labels and holds both files in memory.
- **Refusal behavior.** A missing or unreadable file, a repeated key, or a cast the column's type cannot take is named before the comparison starts, with exit code 3.
- **The docs that describe it.** The README's quick start, tutorial 2, and [the command line](../docs/cli.md).
- **The metric that proves it.** [NS-01](metrics/NS-01.md), the steps to a correct verdict; [DR-01](metrics/DR-01.md), the keys written; [DR-02](metrics/DR-02.md), the mistakes that name their cause.

### UC-02: The same verdict on every pull request

- **The persona.** [P-02](#p-02-the-developer-comparing-two-csv-files), once the comparison runs in GitHub Actions. [P-01](#p-01-the-engineer-verifying-a-migration) in CI.
- **The moment.** The comparison worked once on a laptop. Now every change to the script or the export should be checked before it merges, without anyone remembering to run it.
- **The question, in their words.** "Did this pull request change the output, and by how much?"
- **The data, and where it lives.** The same two files, or a file and its regenerated copy, in the repository or produced by the job.
- **Required latency.** Inside the pull request's checks: minutes.
- **What a wrong answer costs, and to whom.** A false match merges a regression. A false mismatch blocks a correct change and teaches the team to ignore the check.
- **The alternative it beats.** A script in the workflow that runs `diff` and fails on any byte, or reviewers reading the files.
- **Refusal behavior.** The job fails with exit code 3 and the error in the log when the configuration or a file is wrong, and never reports a match it did not compute.
- **The docs that describe it.** [CI integrations](../docs/ci.md) and tutorial 5.
- **The metric that proves it.** [GR-02](metrics/GR-02.md), no wrong verdict open at a release; [DR-03](metrics/DR-03.md), the days to fix one.

### UC-03: Sign off from the report alone

- **The persona.** [P-03](#p-03-the-reviewer-who-reads-the-report).
- **The moment.** A pull request carries the summary comment, or someone sends the HTML report. The reviewer has minutes and did not run anything.
- **The question, in their words.** "What differs, under which rules, and can I approve this?"
- **The data, and where it lives.** The report and the summary, never the raw files.
- **Required latency.** The time it takes to read a page.
- **What a wrong answer costs, and to whom.** An approval of a wrong verdict ships it. A report that hides a rule or a column misleads the one person who could catch it.
- **The alternative it beats.** Asking the runner to explain, a screenshot, or a spreadsheet of both files.
- **Refusal behavior.** The report shows nothing it was not given, and says PASSED or FAILED in words, never by color alone.
- **The docs that describe it.** [Results](../docs/results.md), tutorial 4, and `ACCESSIBILITY.md`.
- **The metric that proves it.** [GR-03](metrics/GR-03.md), no accessibility violation on the report; [GR-02](metrics/GR-02.md), no wrong verdict open.

### UC-04: Let an agent run the comparison

- **The persona.** [P-04](#p-04-the-ai-coding-agent-driving-the-command-line), for [P-02](#p-02-the-developer-comparing-two-csv-files) or [P-01](#p-01-the-engineer-verifying-a-migration).
- **The moment.** A person asks their coding agent to check whether a change altered an output, or to fix a pipeline until the comparison passes.
- **The question, in their words.** "Run the comparison and tell me the counts, and what to change if it fails."
- **The data, and where it lives.** Whatever the configuration file names.
- **Required latency.** One command, inside the agent's loop.
- **What a wrong answer costs, and to whom.** The agent acts on it: it edits the pipeline against a false mismatch, or stops against a false match.
- **The alternative it beats.** The agent writing its own comparison in `pandas`, which nobody reviewed.
- **Refusal behavior.** `validate --json` reports what would stop a run before any row is read, and a failure prints one JSON object with its type and message.
- **The docs that describe it.** [The AI agents page](../docs/agents.md) and `skills/veridelta/SKILL.md`.
- **The metric that proves it.** [DR-02](metrics/DR-02.md), mistakes that name their cause, which an agent can act on; [NS-01](metrics/NS-01.md), the steps it runs.

### UC-05: Compare two tables where they are stored

- **The persona.** [P-01](#p-01-the-engineer-verifying-a-migration).
- **The moment.** The tables are too large to export, or the data may not leave the warehouse.
- **The question, in their words.** "Do these two tables match, without pulling a row out?"
- **The data, and where it lives.** Two tables in one of Snowflake, Databricks, BigQuery, Postgres, DuckDB, or MotherDuck.
- **Required latency.** A warehouse query: seconds to minutes.
- **What a wrong answer costs, and to whom.** A verdict that differs from the local engine's is a bug that nobody can see from the counts alone.
- **The alternative it beats.** A hand-written `EXCEPT` query per table pair, with no rules and no report.
- **Refusal behavior.** A rule the dialect cannot run is refused before the query, and [the pushdown guide](../docs/pushdown.md) lists the known differences.
- **The docs that describe it.** [Pushdown](../docs/pushdown.md) and [Sources](../docs/sources.md).
- **The metric that proves it.** [GR-01](metrics/GR-01.md), backends with the same verdict locally and in pushdown.

## Considered and cut

- **A platform owner whose CI is GitLab.** Nobody has named GitLab: no issue, no discussion, and not the maintainer, who uses GitHub (answer 2). The template in `ci/gitlab/veridelta.yml` stays until the feature map decides its fate, and this entry is its evidence.
- **Profiling one dataset.** The ask of [AP-01](#ap-01-the-team-wanting-a-data-quality-framework). Veridelta compares two datasets, and the README's first sentence draws the line.

## Traceability

| Use case | Persona | Feature and docs page | Metric |
| :--- | :--- | :--- | :--- |
| [UC-01](#uc-01-a-first-verdict-on-two-files) | [P-02](#p-02-the-developer-comparing-two-csv-files), [P-01](#p-01-the-engineer-verifying-a-migration) | `veridelta validate` and `veridelta run` on two files: the README's quick start, [the command line](../docs/cli.md) | [NS-01](metrics/NS-01.md), [DR-01](metrics/DR-01.md), [DR-02](metrics/DR-02.md) |
| [UC-02](#uc-02-the-same-verdict-on-every-pull-request) | [P-02](#p-02-the-developer-comparing-two-csv-files), [P-01](#p-01-the-engineer-verifying-a-migration) | The GitHub Action: [CI integrations](../docs/ci.md) | [GR-02](metrics/GR-02.md), [DR-03](metrics/DR-03.md) |
| [UC-03](#uc-03-sign-off-from-the-report-alone) | [P-03](#p-03-the-reviewer-who-reads-the-report) | The HTML report and the Markdown summary: [Results](../docs/results.md) | [GR-03](metrics/GR-03.md), [GR-02](metrics/GR-02.md) |
| [UC-04](#uc-04-let-an-agent-run-the-comparison) | [P-04](#p-04-the-ai-coding-agent-driving-the-command-line) | `--json`, the exit codes, and `validate --json`: [the AI agents page](../docs/agents.md) | [DR-02](metrics/DR-02.md), [NS-01](metrics/NS-01.md) |
| [UC-05](#uc-05-compare-two-tables-where-they-are-stored) | [P-01](#p-01-the-engineer-verifying-a-migration) | Pushdown: [Pushdown](../docs/pushdown.md) | [GR-01](metrics/GR-01.md) |
