# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The agent task eval: fixed tasks, fixed graders, and two controls that need no model.

A task gives an agent a folder and a request, and the agent may use only the
tools of `veridelta mcp`. The graders then read what the agent called and what
it answered, and compare both with what is true of the folder by construction.
`DR-04` in the product bundle records the score.
"""
