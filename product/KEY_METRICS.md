---
type: Key Metrics
title: The key metrics
description: One north star, the drivers that move it, and the guardrails against gaming it, each measured by a command this repository can run.
status: draft
generated: { by: claude-code, at: 2026-10-06T15:10:00Z }
---

# The key metrics

Veridelta has no telemetry and adds none, so every metric here is measured from what the repository and its GitHub project can see: the command line itself, CI jobs, the parity suite, the Live Warehouses workflow, and the issue tracker. Each metric is a concept under [metrics/](metrics/index.md), named by its id. The set follows the maintainer's constraint in [who Veridelta serves](USERS.md#what-the-maintainer-said): the barrier of entry stays small, and Veridelta is never riddled with bugs.

## Overview

| Id | Role | Supports | Proves | Metric |
| :--- | :--- | :--- | :--- | :--- |
| [NS-01](metrics/NS-01.md) | north star | | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Steps to a correct verdict on the quick start](metrics/NS-01.md) |
| [DR-01](metrics/DR-01.md) | driver | [NS-01](metrics/NS-01.md) | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files) | [Keys written before the first verdict](metrics/DR-01.md) |
| [DR-02](metrics/DR-02.md) | driver | [NS-01](metrics/NS-01.md) | [UC-01](USERS.md#uc-01-a-first-verdict-on-two-files), [UC-04](USERS.md#uc-04-let-an-agent-run-the-comparison) | [Quick-start mistakes that name their cause](metrics/DR-02.md) |
| [DR-03](metrics/DR-03.md) | driver | [NS-01](metrics/NS-01.md) | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request) | [Days from a wrong verdict reported to its fix on PyPI](metrics/DR-03.md) |
| [GR-01](metrics/GR-01.md) | guardrail | [NS-01](metrics/NS-01.md) | [UC-05](USERS.md#uc-05-compare-two-tables-where-they-are-stored) | [Backends with the same verdict locally and in pushdown](metrics/GR-01.md) |
| [GR-02](metrics/GR-02.md) | guardrail | [NS-01](metrics/NS-01.md) | [UC-02](USERS.md#uc-02-the-same-verdict-on-every-pull-request), [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Wrong verdicts open at a release](metrics/GR-02.md) |
| [GR-03](metrics/GR-03.md) | guardrail | [NS-01](metrics/NS-01.md) | [UC-03](USERS.md#uc-03-sign-off-from-the-report-alone) | [Accessibility violations on the site and the report](metrics/GR-03.md) |

## North star

[NS-01](metrics/NS-01.md) counts the steps between a fresh machine and a correct verdict on the quick start: the keys a newcomer writes plus the shell lines they type. It is 7 today, and 5 is the target. The maintainer chose it over parity across backends on 2026-10-06, because the one known user is a newcomer with two CSV files, and the promise to them is a verdict with nothing to learn first.

## Drivers

- [DR-01](metrics/DR-01.md) counts the keys in the smallest configuration that compares two Parquet files: 5 today, and 3 once the format follows the file's suffix.
- [DR-02](metrics/DR-02.md) counts the quick-start mistakes whose error names the cause and not a symptom: 3 of 4 today.
- [DR-03](metrics/DR-03.md) measures how long a reported wrong verdict stays unfixed on PyPI. No such report exists yet.

## Guardrails

- [GR-01](metrics/GR-01.md) counts the pushdown backends on which the parity suite reached the same verdict as a local run: 2 of 6 today, and 6 of 6 before a release. A faster verdict that differs by backend is worth nothing.
- [GR-02](metrics/GR-02.md) counts the wrong verdicts reported and still open at a release: 0 today, and 0 at every release.
- [GR-03](metrics/GR-03.md) counts the accessibility violations on the docs site and the HTML report: 0 today, and 0 at every release.

## Considered and rejected

- **PyPI downloads.** They count mirrors, bots, and CI reinstalls, and nothing the project does on purpose moves them.
- **GitHub stars.** Attention, not use.
- **The number of connectors or rules.** A feature nobody uses costs maintenance and proves nothing. The high ceiling the maintainer asked for is held by the rules a use case needs, not by a count.
- **Lines of code and the test count.** Both grow with every change and say nothing about a user.
- **Branch coverage of the core modules.** `make test` already fails below 100%, so it is a gate, not a number to watch.

## Where each metric is read

| Metric | Where |
| :--- | :--- |
| [NS-01](metrics/NS-01.md), [DR-01](metrics/DR-01.md), [DR-02](metrics/DR-02.md) | `veridelta run` on the configurations each card gives. The recording in the README, once it exists, shows [NS-01](metrics/NS-01.md). |
| [DR-03](metrics/DR-03.md), [GR-02](metrics/GR-02.md) | `gh issue list --label bug`, and the release dates on PyPI |
| [GR-01](metrics/GR-01.md) | The `test-core` and `test-postgres` jobs of the CI Pipeline on the release commit, and the latest run of the Live Warehouses workflow |
| [GR-03](metrics/GR-03.md) | The Accessibility job of the CI Pipeline on the release commit |
