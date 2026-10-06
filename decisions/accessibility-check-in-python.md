---
type: Decision
title: The accessibility check runs axe-core from Python, with a pinned download
description: Why the Accessibility job drives Chromium from pytest and fetches axe-core by version and digest, rather than with a Node toolchain or a Python wrapper that bundles axe-core.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-06T03:06:27-05:00 }
---

**Claim:** `tests/accessibility/` drives Chromium through Python's `playwright`, from the `accessibility` dependency group. It runs axe-core 4.14.0, fetched from npm and checked against its pinned sha512 digest.

**Evidence:** Every other check runs through uv and pytest, as `AGENTS.md` asks. `axe-playwright-python` 0.1.8, released 2026-07-24, bundles axe-core 4.12.1, two minor releases behind 4.14.0. The download checks itself in the `axe_source` fixture of `tests/accessibility/test_accessibility.py`.

**Alternative considered:** A `package.json` with `axe-core` and `@axe-core/playwright`, run with Node.

**Why rejected:** It adds a second toolchain, a second lockfile, and a second Dependabot ecosystem for one CI job. Every contributor would need Node to run `make accessibility`.

**How to reverse:** Replace the `axe_source` fixture with a Node script that runs the same pages, point the `accessibility` target in the `Makefile` at it, and drop the `accessibility` group from `pyproject.toml`.
