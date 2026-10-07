---
type: Decision
title: Each rule is one concept of the bundle
description: Why the detailed rules sit under rules/ as concepts that name the paths they govern, reached through the table in AGENTS.md, and not in an AGENTS.md beside the code.
status: stable
decided: 2026-10-07
generated: { by: claude-code, at: 2026-10-07T01:08:53Z }
---

**Claim:** The detailed rules are concepts under `rules/`, one file per subject, each naming in `applies_to` the paths it governs. `AGENTS.md` names each in its "Rules by path" table, to read before writing or changing a file under those paths. No `AGENTS.md` sits below the root.

**Evidence:** Claude Code's documentation, read 2026-10-07: "Reading `AGENTS.md` directly requires Claude Code v2.1.277 or later", and a subdirectory's `AGENTS.md` loads "when Claude opens a file there with the Read tool", which this repository saw happen for `tests/AGENTS.md` on 2026-10-07; the changelog's 2.1.282 entry extends the support to Bedrock, Vertex, Foundry, gateways, and telemetry-off sessions. The ai-kit plan's check of Cursor's documentation on 2026-10-06 found a nested `AGENTS.md` "scoped by folder only", with "a loading bug reported in 2025" not confirmed fixed. ai-kit v0.9.0 keeps each language's rules once, as a `Rule` concept in its bundle, which its `AGENTS.md` says to read "before you write or change a Python file". `tests/unit/test_rules.py` holds each rule's frontmatter, every `applies_to` path, the index, the table, and the absence of a nested `AGENTS.md`.

**Alternative considered:** `src/AGENTS.md` and `tests/AGENTS.md` beside the code, as the record this one replaces chose. Claude Code and Codex attach one when they read a file in its folder, which no concept under `rules/` gets.

**Why rejected:** The maintainer, 2026-10-07: "use this plan as a template to create our own to update the veridelta repo with the best practices i just learned and am implementing in ai-kit", and on 2026-10-06: "i dont want to suport every ai harness. this is why I like the OFK standard since it is harness agnostic." The ai-kit record gives the reasons: a nested file "scopes by folder, not by glob", and "it breaks the rule that nothing is written beside the code", and its plan adds that Cursor's loading of such files "has an open bug report". The cost is the auto-load: an agent reads a rule when `AGENTS.md` sends it there, and the gate catches what a check can catch.

**How to reverse:** Restore `src/AGENTS.md` and `tests/AGENTS.md` from the commit `git log -- src/AGENTS.md` names, delete `rules/` and `tests/unit/test_rules.py`, and restore the two tests in `tests/unit/test_docs_links.py` from the same commit.
