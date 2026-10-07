---
type: Decision
title: A tool has a file of its own only where it reads no shared one
description: Which agent tools the repository supports, and the files that stay specific to one tool, each with why no shared file does the job and what would let it go.
status: stable
decided: 2026-10-07
generated: { by: claude-code, at: 2026-10-07T01:10:55Z }
---

**Claim:** "The supported tools are Claude Code and Cursor, plus any tool that reads `AGENTS.md`", as the ai-kit plan the maintainer approved puts it. What the repository gives them is the same for all: `AGENTS.md`, the rules under `rules/`, the bundle under `product/` and `decisions/`, and the skills. A file for one tool stays only where a supported tool reads no shared one. There are two:

| File | For | Why no shared file does it | What would let it go |
| :--- | :--- | :--- | :--- |
| `.claude/skills/` | Claude Code and Cursor | The name is Claude Code's, but it is the one skill folder both read | Claude Code reading `.agents/skills/`, which the other tools share |
| The `.gitignore` line for `.claude/settings.local.json` | Claude Code | Claude Code writes that file into the project when a contributor grants a permission, and nothing else does | Claude Code keeping its permissions outside the project |

**Evidence:** Claude Code's documentation, read 2026-10-07, lists `.claude/skills/<name>/SKILL.md` as the project's skills folder and no other, and `.agents/skills/` is not read (anthropics/claude-code issue 16345 is open). Cursor loads skills from `.agents/skills/`, `.cursor/skills/`, `.claude/skills/`, and `.codex/skills/`, as the ai-kit plan's check of 2026-10-06 found in Cursor's documentation. What went: `CLAUDE.md` in #177, since it switched `AGENTS.md` off; `.cursor/rules` in #175 and #177; `.cursorignore` and `.claude/settings.json` in #175; `src/AGENTS.md` and `tests/AGENTS.md` in #181. `tests/unit/test_docs_links.py` holds `.claude/` to the skills and the one local settings file, and the checkout to no `.cursor/`, `.cursorignore`, `.mcp.json`, or `CLAUDE.md` of any case.

**Alternative considered:** A neutral folder for the skills, with a symlink or a copy at `.claude/skills/`, or `.agents/skills/` today. Or a file for every tool that has a place of its own.

**Why rejected:** The maintainer, 2026-10-06: "i dont want to suport every ai harness. this is why I like the OFK standard since it is harness agnostic." A checkout on Windows turns a symlink into a text file, a copy drifts, and Claude Code does not read `.agents/skills/`. Any tool that reads `AGENTS.md`, `rules/`, and the bundle gets the repository's instructions and knowledge with no file of its own.

**How to reverse:** Each row says what would let its file go. To support one more tool, give it a file only where it reads none of the shared ones, add a row here, and widen the test that holds the layout.
