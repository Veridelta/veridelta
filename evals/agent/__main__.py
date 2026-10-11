# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run the agent eval from the command line.

    python -m evals.agent --agent scripted --runs 3 --out .evals/scripted

The controls, `scripted` and `wrong`, need no model and no key. The `live`
agent drives the server through the Claude Agent SDK and needs `--model` and
an `ANTHROPIC_API_KEY` in the environment. The command prints one summary
line, writes `report.json` and `report.md` into `--out`,
and, with `--expect all` or `--expect none`, exits 1 unless every task or no
task passed, which is how `make eval-agent` holds the controls.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from evals.agent.agents import CONTROLS
from evals.agent.report import write_report
from evals.agent.runner import run_eval
from evals.agent.tasks import load_tasks, select

if TYPE_CHECKING:
    from evals.agent.agents import Agent


def build_parser() -> argparse.ArgumentParser:
    """Build the command's parser."""
    parser = argparse.ArgumentParser(
        prog="python -m evals.agent", description="Grade an agent on the fixed Veridelta tasks."
    )
    parser.add_argument(
        "--agent",
        choices=[*sorted(CONTROLS), "live"],
        required=True,
        help="Which agent to run: a control, or `live` for a model through the SDK.",
    )
    parser.add_argument("--model", default=None, help="The model id the live agent runs.")
    parser.add_argument(
        "--budget-usd",
        type=float,
        default=10.0,
        help="The most the whole live run may spend. Default 10.",
    )
    parser.add_argument("--runs", type=int, default=3, help="Attempts per task. Default 3.")
    parser.add_argument(
        "--tasks", default="all", help="`all`, or ids such as T01,T07. Default all."
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="Where to write the report. Default .evals/<agent>."
    )
    parser.add_argument(
        "--expect",
        choices=["all", "none"],
        default=None,
        help="Exit 1 unless every task passes, or none does, for a control run in CI.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the eval and return the exit code."""
    args = build_parser().parse_args(argv)
    if args.runs < 1:
        print("--runs must be at least 1.", file=sys.stderr)
        return 2
    tasks = select(load_tasks(), args.tasks)
    agent: Agent
    if args.agent == "live":
        if not args.model:
            print("--model is required with --agent live.", file=sys.stderr)
            return 2
        from evals.agent.live import LiveAgent

        agent = LiveAgent(args.model, args.budget_usd)
    else:
        agent = CONTROLS[args.agent]()
    report = run_eval(agent, tasks, args.runs)
    folder = args.out if args.out is not None else Path(".evals") / args.agent
    as_json, as_markdown = write_report(report, folder)
    print(report.summary())
    print(f"Report: {as_markdown} and {as_json}")
    expected = {"all": len(report.scores), "none": 0}.get(args.expect)
    if expected is not None and report.passed_tasks != expected:
        print(
            f"Expected {args.expect} of the tasks to pass, not {report.passed_tasks}.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
