---
type: Decision
title: engine.py keeps DiffEngine, and its helpers live in private modules beside it
description: Why the 3,421-line engine.py is split into private sibling modules that never import it, rather than turned into an engine package.
status: stable
decided: 2026-10-09
generated: { by: claude-code, at: 2026-10-09T02:10:40Z }
---

**Claim:** `src/veridelta/engine.py` holds `DiffEngine` and re-exports the public engine names through `__all__`. Its helpers move into private modules beside it, `src/veridelta/_*.py`, one job each: rule resolution, `suggest`, local matching, reading, warehouse routing, results, pushdown, config checks, and value maps. A private module never imports `veridelta.engine`, and a name a test patches lives in the module that reads it.

**Evidence:** At commit `3f2f2af`, `engine.py` was 3,421 lines, with `DiffEngine` about 1,035 of them and the rest helpers in about a dozen groups. `rules/security.md`, the Makefile's `CORE_MODULES`, and `ci.yml`'s coverage gate name `src/veridelta/engine.py` as a file, and `tests/unit/test_rules.py` and `tests/unit/test_ci_integrations.py` fail when a listed path is gone. The tests patch more than 60 module globals by the string `veridelta.engine.<name>`, each of which works only while the code that reads the name looks it up there. `veridelta.connectors` already re-exports through `__all__`, and the built API page renders each such name.

**Alternative considered:** An `engine` package: `src/veridelta/engine/__init__.py` holding `DiffEngine`, with a submodule for each group, as `veridelta.connectors` is laid out.

**Why rejected:** `rules/engine.md` asks for no new packages, and the file's path is named by the security rule and both coverage lists, so the move would change all three for no gain over modules beside it. Private names keep the public surface exactly what `__all__` lists, where submodules of a public package read as API.

**How to reverse:** Move the definitions in the `src/veridelta/_*.py` modules back into `src/veridelta/engine.py`, in the order their imports allow. Then drop their paths from `CORE_MODULES` in the Makefile and `ci.yml` and from `rules/security.md`, and point the tests' imports and patch targets back at `veridelta.engine`.
