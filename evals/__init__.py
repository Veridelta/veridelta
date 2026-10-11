# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Evaluations of how Veridelta is used, run from the repository and never shipped.

`evals.agent` grades an agent that drives `veridelta mcp` through fixed tasks
and fixed checks. No model ever grades, and no model is needed to run the
controls: `python -m evals.agent --agent scripted` and `--agent wrong` play a
right and a wrong performance of every task, so the graders are proven before
a live run spends anything.
"""
