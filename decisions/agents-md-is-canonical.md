---
type: Decision
title: AGENTS.md holds the rules for coding agents
description: Why contributors' coding agents read one rules file at the repository root, which CLAUDE.md imports.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-05T22:53:01-05:00 }
---

**Claim:** `AGENTS.md` is the one file of rules for coding agents. `CLAUDE.md` is the single line `@AGENTS.md`, and the detailed rules stay in `.cursor/rules`, which `AGENTS.md` links.

**Evidence:** Claude Code reads `CLAUDE.md` and follows its `@` import, and Cursor and other agents read `AGENTS.md`. `tests/unit/test_docs_links.py` holds `CLAUDE.md` to the one import, and requires `AGENTS.md` to link every rules file.

**Alternative considered:** A `CLAUDE.md` with rules of its own, beside `.cursor/rules`.

**Why rejected:** Two files of rules drift apart, and each agent follows only the one it reads.

**How to reverse:** Write the rules into `CLAUDE.md`, and change the test that holds it to one line.
