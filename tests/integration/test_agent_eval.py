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

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from evals.agent.__main__ import main
from evals.agent.agents import AgentRun, ScriptedAgent, ToolCall, WrongAgent
from evals.agent.fixtures import FIXTURES, build_fixture
from evals.agent.graders import Check, result_of
from evals.agent.live import (
    FILE_TOOLS,
    SERVER_TOOLS,
    TOOL_PREFIX,
    LiveAgent,
    plan_session,
    run_from_messages,
)
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


@dataclass
class ToolUseBlock:
    """A tool call as the SDK streams it."""

    id: str
    name: str
    input: dict[str, object]


@dataclass
class ToolResultBlock:
    """A tool's answer as the SDK streams it."""

    tool_use_id: str
    content: str | list[dict[str, object]] | None
    is_error: bool | None


@dataclass
class TextBlock:
    """Text the model wrote."""

    text: str


@dataclass
class _Message:
    """An assistant or user message: a list of blocks."""

    content: list[object]


@dataclass
class _Result:
    """The SDK's closing message."""

    subtype: str
    is_error: bool
    num_turns: int
    total_cost_usd: float | None
    result: str | None


def _task(kind: str) -> Task:
    """Return the first task of a kind."""
    return next(task for task in _TASKS if task.kind == kind)


class TestLiveAgent:
    """Hold the live agent's plan and its reading of the SDK's messages, with no SDK and no key."""

    def test_a_verdict_task_gets_the_server_tools_and_nothing_else(self, tmp_path: Path) -> None:
        """Ensure the model sees the six tools, no built-in tool, and a server rooted at the folder."""
        task = _task("verdict")
        fixture = build_fixture(task.fixture, tmp_path)

        session = plan_session(task, fixture, "model-x")

        assert session.tools == []
        assert session.allowed_tools == [f"{TOOL_PREFIX}{name}" for name in SERVER_TOOLS]
        assert session.mcp_servers == {
            "veridelta": {
                "type": "stdio",
                "command": "veridelta",
                "args": ["mcp", "--root", str(fixture.root)],
            }
        }
        assert (session.cwd, session.model) == (str(fixture.root), "model-x")
        assert (session.max_turns, session.max_budget_usd) == (task.max_turns, task.budget_usd)
        assert session.permission_mode == "dontAsk"
        assert session.prompt == task.prompt

    def test_an_editing_task_gets_the_file_tools_too(self, tmp_path: Path) -> None:
        """Ensure only a task that fixes or writes a configuration can touch a file."""
        for kind in ("fix_config", "write_config"):
            task = _task(kind)
            (tmp_path / kind).mkdir()
            session = plan_session(task, build_fixture(task.fixture, tmp_path / kind), "m")

            assert session.tools == list(FILE_TOOLS)
            assert session.allowed_tools[-3:] == list(FILE_TOOLS)

    def test_a_task_that_allows_row_values_starts_the_server_with_the_flag(
        self, tmp_path: Path
    ) -> None:
        """Ensure the row-value gate follows the task, as the controls' server does."""
        task = next(task for task in _TASKS if task.allow_row_values)

        session = plan_session(task, build_fixture(task.fixture, tmp_path), "m")

        assert session.mcp_servers["veridelta"]["args"][-1] == "--allow-row-values"

    def test_it_reads_the_calls_the_answer_and_the_cost_from_the_messages(self) -> None:
        """Ensure each tool use pairs with its result under the graders' name, and the result closes the run."""
        messages = [
            _Message(
                [
                    TextBlock("Checking."),
                    ToolUseBlock("u1", f"{TOOL_PREFIX}validate_config", {"path": "veridelta.yaml"}),
                ]
            ),
            _Message([ToolResultBlock("u1", [{"type": "text", "text": '{"valid": true}'}], False)]),
            _Message([ToolUseBlock("u2", "Read", {"file_path": "x"})]),
            _Message([ToolResultBlock("u2", "denied", True)]),
            _Message([TextBlock("Done.")]),
            _Result("success", False, 4, 0.0125, "Done.\nVERIDELTA_RESULT: {}"),
        ]

        run = run_from_messages(messages)

        assert run.tool_calls == [
            ToolCall("validate_config", {"path": "veridelta.yaml"}, None, False, '{"valid": true}'),
            ToolCall("Read", {"file_path": "x"}, None, True, "denied"),
        ]
        assert run.final_answer == "Done.\nVERIDELTA_RESULT: {}"
        assert (run.cost_usd, run.turns, run.error) == (0.0125, 4, None)

    def test_a_result_that_reports_an_error_becomes_the_runs_error(self) -> None:
        """Ensure a turn or budget limit the SDK hit is recorded, with the last text as the answer."""
        messages = [
            _Message([TextBlock("Still working.")]),
            _Result("error_max_turns", True, 12, 0.3, None),
        ]

        run = run_from_messages(messages)

        assert run.error == "error_max_turns"
        assert run.final_answer == "Still working."
        assert run.cost_usd == 0.3

    def test_a_spent_budget_stops_the_run_before_the_sdk_is_touched(self, tmp_path: Path) -> None:
        """Ensure the hard stop holds for every attempt together, whatever the SDK's own cap does."""
        task = _task("verdict")
        agent = LiveAgent("model-x", budget_usd=0.0)

        run = agent.run(task, build_fixture(task.fixture, tmp_path))

        assert run.error is not None
        assert "budget" in run.error
        assert run.tool_calls == []

    def test_the_command_needs_a_model_for_the_live_agent(self) -> None:
        """Ensure no live run starts on a model nobody named."""
        assert main(["--agent", "live", "--runs", "1"]) == 2
