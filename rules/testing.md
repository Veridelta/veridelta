---
type: Rule
title: Testing rules
description: How the suite is laid out, where fixtures and markers go, how the parity suite is run, and the coverage gate, under tests/.
status: stable
generated: { by: claude-code, at: 2026-10-07T01:07:07Z }
applies_to: [tests/]
---

# Testing rules

These rules apply to every change under `tests/`, on top of the root [AGENTS.md](../AGENTS.md).

- Framework: `pytest` via `uv run pytest` or `make test`.
- Organization:
  - `tests/unit/`: isolated logic; no I/O or network.
  - `tests/integration/`: module interaction and dataset ingestion.
  - `tests/smoke/`: installation and packaging.
  - `tests/e2e/`: full CLI and end-to-end workflows.
- Shared fixtures belong in the `conftest.py` of the suite that uses them, such as `tests/integration/conftest.py`. Type every fixture return value (typically `pl.DataFrame`).
- Mark each module once with `pytestmark`, such as `pytestmark = [pytest.mark.unit, pytest.mark.fast]`. A class or test that needs more, such as `property` or `skip_on`, adds its own decorator.
- `tests/integration/test_parity_fuzz.py` draws cases from `parity_strategies.py` and requires the local engine and DuckDB pushdown to agree. Its `ci` profile is derandomized; run `VERIDELTA_HYPOTHESIS_PROFILE=deep` before changing either engine. A case it finds is fixed with a deterministic regression test, or, for a documented difference, kept out of the generator and pinned by a test. New branches are covered by deterministic tests, never by Hypothesis alone.
- `tests/integration/test_seeded_drift.py` holds both engines to a ledger of drift seeded on purpose, built by `seeded_drift.py`. A change to what a rule forgives adds a pair of cases there: the drift reported with no rule, and forgiven under it.
- Mock only external side effects. Use real `polars.DataFrame` instances for tabular computations.
- Coverage gates: 90% of the package (`fail_under` in `pyproject.toml`), and 100% branch coverage of the modules `CORE_MODULES` names in the `Makefile`, which `make test` and CI both enforce.
