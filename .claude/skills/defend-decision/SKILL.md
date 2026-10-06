---
name: defend-decision
description: Writes a decision record under decisions/ (claim, evidence, alternative considered, why rejected, how to reverse) and adds it to the settled list in AGENTS.md. Use when a choice is non-obvious, likely to be questioned again, or made by an agent that will not remember why. Never for a small choice, and never to backfill an old one.
metadata:
  version: "1.1.0"
---

# Record a decision

A record stops a settled choice from being argued again, by a person or by an agent that cannot remember why it was made. Write one only when the choice is non-obvious, likely to be questioned again, or made by an agent. A small choice needs no record, and an old one is not backfilled.

## The file

One Markdown file under `decisions/`, named in lowercase words joined by hyphens, such as `decisions/llms-txt-from-a-hook.md`. It opens with this front matter:

```yaml
---
type: Decision
title: <the claim as a short sentence>
description: <one sentence on one line: why this, and what it was chosen over>
status: stable
decided: <YYYY-MM-DD>
generated: { by: <producer>, at: <time> }
---
```

`status` is `draft`, `stable`, or `deprecated`. `generated` says who wrote the text and when: the producer, such as `claude-code`, or a person as `human:<id>`, and a time in ISO 8601 with an offset. Leave out any field that names the model that wrote the record.

## The body

Write this block and nothing else:

```markdown
**Claim:** <one sentence of what was decided>

**Evidence:** <a file path, a test, a command and its output, or a cited source>

**Alternative considered:** <the strongest rejected option>

**Why rejected:** <one or two sentences, with its cost or risk>

**How to reverse:** <the smallest change that undoes it>
```

## Rules

- A claim without evidence is not a decision. Gather the evidence, or drop the record.
- The alternative is one a reviewer would propose, not a straw man.
- "How to reverse" names a file, a setting, or a test, never "rewrite the system".
- Add the record's line to "Settled" in `AGENTS.md`, linked. `tests/unit/test_decisions.py` fails while the two disagree.
- The record follows the writing rules in `CONTRIBUTING.md`, as every other text does.
- To overturn a settled choice, ask the maintainer first. Then set the old record's `status` to `deprecated`, and write a new one.
