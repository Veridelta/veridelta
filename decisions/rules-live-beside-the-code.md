---
type: Decision
title: The agent rules live in AGENTS.md files beside the code
description: Why the scoped rules sit in src/AGENTS.md and tests/AGENTS.md, with no CLAUDE.md and no .cursor folder, instead of in one harness's own rules format.
status: stable
decided: 2026-10-06
generated: { by: claude-code, at: 2026-10-06T23:40:00Z }
---

**Claim:** The rules for coding agents are `AGENTS.md` at the root, `src/AGENTS.md`, and `tests/AGENTS.md`, and nothing else: no `CLAUDE.md`, and no `.cursor` folder.

**Evidence:** Claude Code 2.1.277 and later read `AGENTS.md` when there is no `CLAUDE.md`, and only `CLAUDE.md` when both exist. Cursor, Codex, and Claude Code load a nested `AGENTS.md` scoped to its folder, which is what the `globs` of a `.cursor/rules` file did. `tests/unit/test_docs_links.py` holds the root file to every nested one and the repository to no `CLAUDE.md`, and `tests/unit/test_docs_style.py` reads all three.

**Alternative considered:** Keeping `.cursor/rules` as the store of the scoped rules, with `AGENTS.md` linking it, as the record this one replaces chose.

**Why rejected:** Only one harness loaded those files by itself, every other agent reached them through a link, and their content named no harness. The maintainer supports no harness in particular, so the rules take the one form every harness reads.

**How to reverse:** Restore `.cursor/rules` and `CLAUDE.md` from the last commit that held them, which `git log -- CLAUDE.md` names, move the bodies of the two nested files back into them, and restore the two tests in `tests/unit/test_docs_links.py` from the same commit.
