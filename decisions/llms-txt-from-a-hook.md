---
type: Decision
title: llms.txt comes from a hook in the repository
description: Why the docs build writes llms.txt and llms-full.txt with its own MkDocs hook, not with the mkdocs-llmstxt plugin.
status: stable
decided: 2026-10-06
---

**Claim:** `hooks/llms_txt.py`, registered under `hooks:` in `mkdocs.yml`, writes `llms.txt` and `llms-full.txt` into the site when the docs build.

**Evidence:** The `mkdocs-llmstxt` README says the project is in maintenance mode, and its last release, 0.5.0, came on 2025-11-20. The hook reads each page after the macros plugin renders it, and `tests/unit/test_agent_guide.py` checks its functions and its registration.

**Alternative considered:** The `mkdocs-llmstxt` plugin, with a `sections` list in `mkdocs.yml`.

**Why rejected:** It adds four packages to the docs build, with no maintainer to answer a breaking MkDocs release. It also turns each page's HTML back into Markdown, while the hook copies the Markdown as written.

**How to reverse:** Remove the `hooks:` entry and `hooks/llms_txt.py`, and delete the hook's tests in `test_agent_guide.py`. Then add `mkdocs-llmstxt` to the dev group, with a `sections` list.
