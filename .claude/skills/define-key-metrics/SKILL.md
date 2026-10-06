---
name: define-key-metrics
description: Writes product/KEY_METRICS.md and one concept for each key metric under product/metrics/, with one north star, three to four drivers and two to three guardrails, each measured by a command this repository can run. Use when defining or revising the key metrics, when a use case needs the metric that proves it, or when a measured value is refreshed.
metadata:
  version: "1.0.0"
---

# Define the key metrics

`product/KEY_METRICS.md` is the overview: one north star, the drivers that move it, and the guardrails that stop it from being gamed. Each metric is a concept of its own under `product/metrics/`, named by its id: `NS-01.md` for the north star, `DR-01.md`, `DR-02.md` and so on for the drivers, `GR-01.md` and so on for the guardrails. The overview carries `type: Key Metrics` and each metric `type: Metric`. `tests/unit/test_product_bundle.py` holds the ids, the roles and the links.

## Actionable, not vanity

Veridelta has no telemetry and adds none. A metric counts only if this repository can measure it today: a CI job, the parity suite, a workflow run, the GitHub API, a test, a command. A number that nothing here can produce is not a metric. Downloads, stars, counts of connectors or lines of code say nothing anyone acts on; list them under "Considered and rejected" with the reason.

State the unit and hold it. A share of backends and a share of parity cases are different metrics, and a target written for one is wrong for the other.

## Where a metric sits

Three frontmatter fields place a metric, and the test reads them:

| Field | Holds | Example |
| :--- | :--- | :--- |
| `role` | `north-star`, `driver` or `guardrail` | `role: driver` |
| `supports` | On a driver or a guardrail: the north star's id | `supports: NS-01` |
| `proves` | The use cases whose promise the metric proves | `proves: [UC-01, UC-03]` |

There is one north star, and every driver and guardrail supports it. A use case named in `proves` is defined in `product/USERS.md`.

## Metric card

The card is the body of the metric's concept. Every field is mandatory:

- **Name.**
- **The promise it proves.** The use cases, by id.
- **Definition and formula.**
- **Unit and window.** Per release, per pull request, per day.
- **How it is measured.** The exact command, query or page, runnable from this repository, and where the number is computed.
- **Current value.** `Measured <date>: <value>`, from a run of that command. Never a placeholder written as a result.
- **Target, and the reasoning.**
- **What gaming it looks like, and the guardrail against it.**
- **What decision changes when it moves.**

## Rubric

- One north star, three to four drivers, two to three guardrails.
- No metric without a source that exists.
- A reviewer can reproduce every current value from the card alone.

## Sections of `product/KEY_METRICS.md`

1. The overview table: id, role, supports, proves, and a link to each metric. 2. North star. 3. Drivers. 4. Guardrails. 5. Considered and rejected, with reasons. 6. Where each metric is read: the CI job, the workflow, or the command that shows it.

Then return to `product/USERS.md`: each use case names the metric that proves it, by id.

## Frontmatter, log and index

Each file opens with its `type`, `title`, a one-sentence `description`, `status`, and `generated: { by: <producer>, at: <time> }`. The status is `draft` until the maintainer approves the text. The producer is `claude-code` or `human:<id>`, never a model. The time is ISO 8601 with an offset. A change updates `generated`. Add a line to `product/log.md` under today's date, and keep `product/index.md` and `product/metrics/index.md` listing every concept.

## Acceptance

- [ ] Every card is complete, and every current value names its date and its command
- [ ] One north star; each driver and guardrail names it in `supports`
- [ ] Every id in `proves` is a use case defined in `product/USERS.md`
- [ ] `product/log.md` has the entry, the indexes list every metric, and `uv run pytest tests/unit/test_product_bundle.py` passes
