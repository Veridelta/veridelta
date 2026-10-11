# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the agent eval's graders to its two controls, with no model and no key.

The scripted control performs every task as the docs ask and must pass each
one. The wrong control makes each kind's characteristic mistake and must pass
none. Together they show that the graders tell a right performance from a
wrong one before a live run spends anything, and that the tasks are the fixed
files the metric DR-04 says they are.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from evals.agent.__main__ import main
from evals.agent.agents import AgentRun, ScriptedAgent, WrongAgent
from evals.agent.fixtures import FIXTURES
from evals.agent.graders import Check, result_of
from evals.agent.report import write_report
from evals.agent.runner import Attempt, Report, TaskScore, attempt, run_eval
from evals.agent.tasks import RESULT_MARKER, Task, load_tasks, select

if TYPE_CHECKING:
    from pathlib import Path

    from evals.agent.fixtures import Fixture

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_TASKS = load_tasks()
_KINDS = {"verdict", "fix_config", "write_config", "propose_rule", "canary", "refusal"}


@pytest.fixture(scope="module")
def scripted() -> Report:
    """Run the scripted control once over every task."""
    return run_eval(ScriptedAgent(), _TASKS, runs=1)


class _Crashing:
    """An agent that never answers."""

    name = "crashing"

    def run(self, task: Task, fixture: Fixture) -> AgentRun:
        """Fail before any tool call."""
        raise RuntimeError(f"no answer for {task.id} in {fixture.root.name}")


class TestTasks:
    """Keep the task set the fixed files the metric describes."""

    def test_there_are_twenty_tasks_of_every_kind_with_a_fixture_each(self) -> None:
        """Ensure each task names a fixture that exists, and the set covers every kind."""
        assert [task.id for task in _TASKS] == [f"T{n:02d}" for n in range(1, 21)]
        assert {task.kind for task in _TASKS} == _KINDS
        assert all(task.fixture in FIXTURES for task in _TASKS)
        assert all(RESULT_MARKER in task.prompt for task in _TASKS)
        assert {task.fixture for task in _TASKS} == set(FIXTURES), "A fixture no task uses."

    def test_select_names_tasks_in_the_order_given_and_refuses_an_unknown_one(self) -> None:
        """Ensure `--tasks` picks and orders, and a typo fails rather than silently runs nothing."""
        assert [task.id for task in select(_TASKS, "T07, T01")] == ["T07", "T01"]
        assert select(_TASKS, "all") == _TASKS
        with pytest.raises(ValueError, match="T99"):
            select(_TASKS, "T99")

    def test_a_task_file_must_carry_its_own_id(self, tmp_path: Path) -> None:
        """Ensure a renamed file cannot quietly change which task its contents grade."""
        (tmp_path / "T02.yaml").write_text(
            "id: T01\nkind: verdict\ntitle: t\nfixture: clean\nprompt: p\n", encoding="utf-8"
        )

        with pytest.raises(ValueError, match=r"T02\.yaml holds the task T01"):
            load_tasks(tmp_path)


class TestControls:
    """The two controls prove the graders in both directions."""

    def test_the_scripted_control_passes_every_task(self, scripted: Report) -> None:
        """Ensure a right performance of each kind passes every check."""
        failures = [
            (run.task_id, [(check.name, check.detail) for check in run.failures])
            for score in scripted.scores
            for run in score.attempts
            if not run.passed
        ]

        assert failures == []
        assert (scripted.passed_tasks, scripted.passed_attempts) == (20, 20)

    def test_the_wrong_control_fails_every_task_on_a_named_check(self) -> None:
        """Ensure each kind's characteristic mistake fails, and the report says which check caught it."""
        report = run_eval(WrongAgent(), _TASKS, runs=1)

        assert report.passed_tasks == 0
        assert all(run.failures for score in report.scores for run in score.attempts)
        caught = {
            score.task.kind: {check.name for run in score.attempts for check in run.failures}
            for score in report.scores
        }
        assert "validate_config_before_run_comparison" in caught["verdict"]
        assert "files_unchanged" in caught["fix_config"]
        assert "smallest_config" in caught["write_config"]
        assert "files_unchanged" in caught["propose_rule"]
        assert "no_canary_in_answer" in caught["canary"]
        assert {"status_refused", "files_unchanged"} <= caught["refusal"]


class TestScoring:
    """Keep the arithmetic of a score, and the shape of a report, fixed."""

    def test_a_task_passes_by_a_majority_of_its_attempts(self) -> None:
        """Ensure two of three pass, one of two does not, and an error is a fail."""
        passing = Attempt("T01", 1, [Check("c", True)], 0.0)
        failing = Attempt("T01", 2, [Check("c", False, "why")], 0.0)
        crashed = Attempt("T01", 3, [Check("c", True)], 0.0, error="boom")

        assert TaskScore(_TASKS[0], [passing, failing, passing]).passed
        assert not TaskScore(_TASKS[0], [passing, failing]).passed
        assert not crashed.passed
        assert failing.failures == [Check("c", False, "why")]

    def test_an_agent_that_crashes_is_one_failed_attempt(self) -> None:
        """Ensure a crash never stops the eval, and the report names it."""
        graded = attempt(_Crashing(), _TASKS[0], 1)

        assert not graded.passed
        assert graded.error is not None
        assert "no answer for T01" in graded.error
        assert [check.name for check in graded.checks] == ["agent_finished"]

    def test_the_result_line_is_the_last_json_object_in_the_answer(self) -> None:
        """Ensure prose before the line is ignored, and a broken line reads as none."""
        answer = f'Counts follow.\n{RESULT_MARKER} {{"verdict": "match", "added_count": 0}}\n'

        assert result_of(AgentRun(answer)) == {"verdict": "match", "added_count": 0}
        assert result_of(AgentRun(f"{RESULT_MARKER} not json")) is None
        assert result_of(AgentRun(f"{RESULT_MARKER} [1, 2]")) is None
        assert result_of(AgentRun("no line at all")) is None

    def test_the_report_names_every_task_and_the_score(
        self, scripted: Report, tmp_path: Path
    ) -> None:
        """Ensure both files are written, and the Markdown lists each task with its verdict."""
        as_json, as_markdown = write_report(scripted, tmp_path / "out")
        page = as_markdown.read_text(encoding="utf-8")

        assert as_json.is_file()
        assert "20 of 20 tasks passed by majority of 1 runs" in page
        assert all(f"| {task.id} |" in page for task in _TASKS)
        assert "## Failures" not in page

    def test_the_command_runs_a_control_and_holds_it_to_an_expectation(
        self, tmp_path: Path
    ) -> None:
        """Ensure `python -m evals.agent` exits 0 on a met expectation and 1 on a missed one."""
        passed = main(
            [
                "--agent",
                "scripted",
                "--runs",
                "1",
                "--tasks",
                "T01",
                "--out",
                str(tmp_path),
                "--expect",
                "all",
            ]
        )
        missed = main(
            [
                "--agent",
                "wrong",
                "--runs",
                "1",
                "--tasks",
                "T01",
                "--out",
                str(tmp_path),
                "--expect",
                "all",
            ]
        )

        assert (passed, missed) == (0, 1)
        assert (tmp_path / "report.md").is_file()
        assert main(["--agent", "scripted", "--runs", "0"]) == 2
