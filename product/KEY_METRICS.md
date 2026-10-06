---
type: Key Metrics
title: The key metrics
description: One north star, the drivers that move it, and the guardrails against gaming it, each measured by a command this repository can run.
status: draft
generated: { by: claude-code, at: 2026-10-06T09:45:00Z }
---

# The key metrics

Veridelta has no telemetry and adds none, so every metric here is measured from what the repository and its GitHub project can see: CI jobs, the parity suite, the Live Warehouses workflow, the GitHub API, and the command line itself. Each metric is a concept under [metrics/](metrics/index.md), named by its id. This revision holds the north star and one driver; the other drivers and the guardrails follow.

## Overview

| Id | Role | Supports | Proves | Metric |
| :--- | :--- | :--- | :--- | :--- |
| NS-01 | north star | | UC-01 | [Backends with the same verdict locally and in pushdown](metrics/NS-01.md) |
| DR-01 | driver | NS-01 | UC-01 | [Keys written before the first verdict](metrics/DR-01.md) |

## North star

[NS-01](metrics/NS-01.md) counts the pushdown backends on which the parity suite reached the same verdict as a local run, on the released version. The promise behind it is the README's: the same verdict in the warehouse as on a laptop.

## Drivers

[DR-01](metrics/DR-01.md) counts the keys a user writes before a first verdict on two Parquet files. The fewer, the sooner a new user reaches the promise.

## Guardrails

None is written yet. Candidates: accessibility violations on the site and the report, which the Accessibility CI job counts; branch coverage of the core modules, which `make test` holds at 100%; and releases shipped without a green Live Warehouses run.

## Considered and rejected

- **PyPI downloads.** They count mirrors, bots, and CI reinstalls, and nothing the project does on purpose moves them.
- **GitHub stars.** Attention, not use.
- **The number of connectors.** A connector nobody uses costs maintenance and proves nothing.
- **Lines of code and the test count.** Both grow with every change and say nothing about a user.

## Where each metric is read

| Metric | Where |
| :--- | :--- |
| NS-01 | The `test-core` and `test-postgres` jobs of the CI Pipeline on the release commit, and the latest run of the Live Warehouses workflow |
| DR-01 | `veridelta run` on the smallest configuration that compares two Parquet files; the card has the commands |
