# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Run an agent through the tasks, several times each, and score the attempts.

Each attempt gets a fresh folder from the task's fixture. A task passes when a
majority of its attempts pass every check, so one lucky or unlucky run does
not decide it. The raw pass rate over every attempt is kept too.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from evals.agent.fixtures import build_fixture
from evals.agent.graders import Check, grade

if TYPE_CHECKING:
    from collections.abc import Sequence

    from evals.agent.agents import Agent
    from evals.agent.tasks import Task


@dataclass(frozen=True)
class Attempt:
    """One run of one task.

    Attributes:
        task_id (str): The task.
        index (int): Which run, from 1.
        checks (list[Check]): Every check, in order.
        cost_usd (float): What the attempt cost.
        error (str | None): Why the agent did not finish, if it did not.
    """

    task_id: str
    index: int
    checks: list[Check]
    cost_usd: float
    error: str | None = None

    @property
    def passed(self) -> bool:
        """Return whether every check held."""
        return self.error is None and all(check.passed for check in self.checks)

    @property
    def failures(self) -> list[Check]:
        """Return the checks that did not hold."""
        return [check for check in self.checks if not check.passed]


@dataclass(frozen=True)
class TaskScore:
    """Every attempt at one task.

    Attributes:
        task (Task): The task.
        attempts (list[Attempt]): Its attempts, in order.
    """

    task: Task
    attempts: list[Attempt]

    @property
    def passed(self) -> bool:
        """Return whether a majority of the attempts passed."""
        return sum(attempt.passed for attempt in self.attempts) * 2 > len(self.attempts)


@dataclass(frozen=True)
class Report:
    """The score of one agent over the tasks.

    Attributes:
        agent (str): The agent's name.
        runs (int): Attempts per task.
        scores (list[TaskScore]): One per task, in task order.
    """

    agent: str
    runs: int
    scores: list[TaskScore]

    @property
    def passed_tasks(self) -> int:
        """Return the tasks passed by majority."""
        return sum(score.passed for score in self.scores)

    @property
    def passed_attempts(self) -> int:
        """Return the attempts that passed every check."""
        return sum(attempt.passed for score in self.scores for attempt in score.attempts)

    @property
    def total_attempts(self) -> int:
        """Return every attempt made."""
        return sum(len(score.attempts) for score in self.scores)

    @property
    def cost_usd(self) -> float:
        """Return what every attempt cost together."""
        return sum(attempt.cost_usd for score in self.scores for attempt in score.attempts)

    def summary(self) -> str:
        """Write the one line the command prints."""
        return (
            f"{self.agent}: {self.passed_tasks} of {len(self.scores)} tasks passed by majority of "
            f"{self.runs} runs; {self.passed_attempts} of {self.total_attempts} attempts passed; "
            f"${self.cost_usd:.2f}."
        )


def attempt(agent: Agent, task: Task, index: int) -> Attempt:
    """Build the task's fixture in a fresh folder, run the agent, and grade it.

    Args:
        agent (Agent): The agent.
        task (Task): The task.
        index (int): Which run this is, from 1.

    Returns:
        Attempt: The graded attempt. An agent that raises is one failed check.
    """
    with tempfile.TemporaryDirectory(prefix=f"veridelta-eval-{task.id}-") as folder:
        fixture = build_fixture(task.fixture, Path(folder))
        try:
            run = agent.run(task, fixture)
        except Exception as exc:
            return Attempt(
                task.id, index, [Check("agent_finished", False, str(exc))], 0.0, str(exc)
            )
        return Attempt(task.id, index, grade(task, fixture, run), run.cost_usd, run.error)


def run_eval(agent: Agent, tasks: Sequence[Task], runs: int) -> Report:
    """Run every task `runs` times and score the agent.

    Args:
        agent (Agent): The agent.
        tasks (Sequence[Task]): The tasks, in order.
        runs (int): Attempts per task, at least 1.

    Returns:
        Report: The scores.
    """
    scores = [
        TaskScore(task, [attempt(agent, task, index) for index in range(1, runs + 1)])
        for task in tasks
    ]
    return Report(agent.name, runs, scores)
