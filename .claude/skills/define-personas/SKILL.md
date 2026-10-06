---
name: define-personas
description: Writes product/USERS.md, the personas Veridelta serves, the users it refuses, and the use cases each persona brings, each with its evidence and its id, as a concept of the product bundle. Use when writing or revising the personas or the use cases, when a feature needs the use case it serves, or when a persona's evidence changes.
metadata:
  version: "1.0.0"
---

# Define the personas and use cases

`product/USERS.md` says who Veridelta serves, who it refuses, and what each person asks of it. Every later document points back here: a feature names the use case it serves, and a metric names the use cases it proves. The document is a concept of the product bundle and carries `type: Users`. `AGENTS.md` has the bundle's rules, and `tests/unit/test_product_bundle.py` holds them.

## Evidence before invention

Veridelta has no telemetry and few named users, so a persona is a hypothesis with its evidence. Every persona and use case carries two fields:

- **Evidence:** what in this repository or on GitHub shows this person exists and wants this: a tutorial, a docs page, a connector, an issue, a discussion.
- **Unknown:** what has not been seen, and how to learn it: a question to the maintainer, an issue form field, a discussion.

Write nothing a field cannot back. A field with no evidence says `Unknown` and names the way to learn it. The maintainer confirms or cuts each persona in review.

## Ids

A heading defines an id, and defines it once: `### P-01: <name>` for a persona, `### AP-01: <who is refused>` for an anti-persona, `### UC-01: <the question, in a few words>` for a use case. Every other mention names the id. A metric proves a use case by its id in `proves`, the feature map names the use case a feature serves, and the test fails on an id that is defined nowhere or defined twice.

## Persona card

For each persona, under its heading, in this order:

- **Role and setting.** Who they are at work, and what they are responsible for.
- **What they compare, and where it lives.** Files, tables, warehouses: the sources Veridelta reads for them.
- **The tools they use today.** What they open before Veridelta exists for them.
- **What they read from Veridelta.** The exit code, the summary, the report, the pull request comment.
- **What they must never be shown.** Row values where they did not ask, credentials, SQL.
- **Tolerances.** What they declare as acceptable drift, and what is never acceptable.
- **Trust posture.** What makes them believe a verdict.
- **Success, in one sentence.**
- **Evidence.**
- **Unknown.**

An anti-persona names who Veridelta refuses, and how the README, the docs or the command line turn them away.

## Use case card

For each use case, under its heading:

- **The persona.** By id.
- **The moment.** What they were doing, and what made them ask.
- **The question, in their words.**
- **The data, and where it lives.**
- **Required latency.**
- **What a wrong answer costs, and to whom.** A false match and a false mismatch cost different things; say both.
- **The alternative it beats.** Name it: a hand-written SQL `EXCEPT`, `pandas.DataFrame.compare`, a notebook, a spreadsheet.
- **Refusal behavior.** What Veridelta refuses to answer here, and how.
- **The docs that describe it.** The page or the tutorial.
- **The metric that proves it.** By id, from `product/KEY_METRICS.md`.

A use case survives the question "would they choose this over what they open today?", or moves to "Considered and cut" with the reason.

## Sections of `product/USERS.md`

1. The setting. 2. Personas. 3. Anti-personas. 4. Use cases. 5. Considered and cut. 6. Traceability: a table of use case, persona, feature with its docs page, and metric.

## Frontmatter, log and index

The file opens with `type: Users`, `title`, a one-sentence `description`, `status`, and `generated: { by: <producer>, at: <time> }`. The status is `draft` until the maintainer approves the text, then `stable`. The producer is `claude-code` or `human:<id>`, never a model. The time is ISO 8601 with an offset. A change updates `generated` and leaves `status` alone. Add a line to `product/log.md` under today's date, newest first, and keep `product/index.md` listing the file.

## Acceptance

- [ ] Every persona and use case has every field, and a field with no evidence says `Unknown` and how to learn it
- [ ] Every use case names the alternative it beats and the metric that proves it, by id
- [ ] The cut list has reasons
- [ ] The frontmatter is complete, `product/log.md` has the entry, and `uv run pytest tests/unit/test_product_bundle.py` passes
