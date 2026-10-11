---
type: Decision
title: No model decides a verdict
description: Why no code in the package calls a language model and no MCP tool writes a file or applies a rule, chosen over a rule a model judges or a model that picks the drift that matters.
status: stable
decided: 2026-10-11
generated: { by: claude-code, at: 2026-10-11T05:56:58Z }
---

**Claim:** No code under `src/veridelta/` calls a language model, and no tool of `veridelta mcp` writes a file or applies a rule. A model may drive the command line, explain a run, or propose a rule with its evidence. Only a rule the user declares in the configuration changes what matches.

**Evidence:** `pyproject.toml` names no model SDK in its dependencies or its extras. `DiffEngine.suggest_rules` in `src/veridelta/engine.py` ends its description with "No model is called.", and `suggest` and `crosswalk` print proposals that the user copies into the file. The six tools `build_server` registers in `src/veridelta/mcp_server.py` read, run, describe, and propose, and none takes a rule, a tolerance, or a baseline as an argument. [AP-02](../product/USERS.md#ap-02-the-user-wanting-a-model-to-decide-what-counts-as-drift) names the user this refuses, and the roadmap's AI workflows state the rule. `tests/unit/test_no_model_inside_the_verdict.py` holds all of it. It scans the package's imports against a list of model SDKs, and the packaging against the same list. It checks each tool's name and arguments, and runs every tool on a folder whose bytes it then finds unchanged.

**Alternative considered:** A rule kind under which a model judges two values equal, or a model that reads the differences and decides which drift matters.

**Why rejected:** A model that forgives drift cannot show why, answers differently between runs and providers, and turns a wrong verdict into an opinion. [GR-02](../product/metrics/GR-02.md) counts a wrong verdict as a bug, and a bug needs a cause the report can show.

**How to reverse:** Write the use case first, in `product/USERS.md`. Then set this record's `status` to `deprecated`, name the module and its SDK in `_ALLOWED` in `tests/unit/test_no_model_inside_the_verdict.py`, and put the SDK in an extra of its own.
