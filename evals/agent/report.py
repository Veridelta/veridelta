# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Write an eval's report as JSON for machines and Markdown for people."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from evals.agent.runner import Report


def report_document(report: Report) -> dict[str, Any]:
    """Return the report as one JSON object."""
    return {
        "agent": report.agent,
        "runs": report.runs,
        "passed_tasks": report.passed_tasks,
        "total_tasks": len(report.scores),
        "passed_attempts": report.passed_attempts,
        "total_attempts": report.total_attempts,
        "cost_usd": round(report.cost_usd, 4),
        "tasks": [
            {
                "id": score.task.id,
                "kind": score.task.kind,
                "title": score.task.title,
                "passed": score.passed,
                "attempts": [asdict(attempt) for attempt in score.attempts],
            }
            for score in report.scores
        ],
    }


def report_markdown(report: Report) -> str:
    """Return the report as a Markdown page: the summary, a table, and the failures."""
    lines = [
        f"# Agent eval: {report.agent}",
        "",
        report.summary(),
        "",
        "| Task | Kind | Title | "
        + " | ".join(f"Run {n}" for n in range(1, report.runs + 1))
        + " | Passed |",
        "| :--- | :--- | :--- | " + " | ".join(":---" for _ in range(report.runs)) + " | :--- |",
    ]
    for score in report.scores:
        runs = " | ".join("pass" if attempt.passed else "fail" for attempt in score.attempts)
        lines.append(
            f"| {score.task.id} | {score.task.kind} | {score.task.title} | {runs} | "
            f"{'yes' if score.passed else 'no'} |"
        )
    failures = [
        f"- {attempt.task_id} run {attempt.index}: {check.name}. {check.detail}".rstrip(". ") + "."
        for score in report.scores
        for attempt in score.attempts
        for check in attempt.failures
    ]
    if failures:
        lines += ["", "## Failures", "", *failures]
    return "\n".join(lines) + "\n"


def write_report(report: Report, folder: Path) -> tuple[Path, Path]:
    """Write `report.json` and `report.md` into a folder, creating it.

    Args:
        report (Report): The scores.
        folder (Path): Where to write.

    Returns:
        tuple[Path, Path]: The JSON file, then the Markdown file.
    """
    folder.mkdir(parents=True, exist_ok=True)
    as_json = folder / "report.json"
    as_markdown = folder / "report.md"
    as_json.write_text(json.dumps(report_document(report), indent=2) + "\n", encoding="utf-8")
    as_markdown.write_text(report_markdown(report), encoding="utf-8")
    return as_json, as_markdown
