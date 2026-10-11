---
type: Decision
title: The live agent eval runs on the Claude Agent SDK
description: Why the model-driven run of the agent eval goes through the Claude Agent SDK, with the model id given at dispatch, rather than a tool loop written on the Messages API.
status: stable
decided: 2026-10-11
generated: { by: claude-code, at: 2026-10-11T06:51:15Z }
---

**Claim:** The live run of the agent eval, `python -m evals.agent --agent live`, drives `veridelta mcp` through the Claude Agent SDK, which runs the Claude Code CLI it bundles. The SDK sits in the `evals` dependency group, which never ships. The model id is a required input of the `Agent Evals` workflow and of the command, and no model id is committed to the repository.

**Evidence:** The SDK's 0.2.165 wheel for Linux, read from PyPI on 2026-10-11, carries the CLI at `claude_agent_sdk/_bundled/claude`, so a runner needs no other install. `evals/agent/live.py` hands the SDK the server over stdio with the fixture's folder as its root, the six server tools as the allowed tools, a file tool only for the two task kinds that edit a configuration, and the task's turn and budget limits, and reads the tool calls and the answer back from what the SDK streams. `tests/unit/test_ci_integrations.py` holds the workflow to a required `model` input with no default, the `evals` environment, a read-only token, and inputs that reach the script through `env`, and scans the repository for a committed model id. `tests/unit/test_no_model_inside_the_verdict.py` lets the SDK into the `evals` group alone.

**Alternative considered:** A tool loop written in `evals/` on the Messages API: send the prompt and the tool schemas, run each tool call against the in-memory server, and feed the results back until the model stops.

**Why rejected:** It measures a loop no user runs. A person who gives their coding agent the server gets the agent's own loop, its permission handling, and its MCP transport, and the eval exists to exercise exactly those. The loop would also re-implement the client plumbing the SDK already has, and every difference between the two would be a question the score cannot answer.

**How to reverse:** Write one more `Agent` in `evals/agent/`, since `plan_session` and `run_from_messages` in `evals/agent/live.py` already keep the SDK's types out of the rest of the eval, and point the `evals` group at the other dependency.
