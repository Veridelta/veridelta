# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The fixed tasks of the agent eval, read from `tasks/*.yaml`.

Each task names its kind, the fixture that builds its working folder, and the
prompt a live agent receives. The graders read nothing from an agent but its
tool calls and its final answer, so a task is a fixed file and never a
judgment, and the set changes only through a reviewed pull request.
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Final, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

Kind = Literal["verdict", "fix_config", "write_config", "propose_rule", "canary", "refusal"]
"""What a task asks of the agent, which picks its graders."""

TASKS_FOLDER: Final = Path(__file__).resolve().parent / "tasks"
"""Where the task files live, one per task, named by its id."""

RESULT_MARKER: Final = "VERIDELTA_RESULT:"
"""The prefix of the one line an agent ends its answer with, followed by a JSON object."""


class Task(BaseModel):
    """One task of the eval, as its file states it.

    Attributes:
        id (str): `T01` to `T99`, which is also the file's stem.
        kind (Kind): What the task asks, which picks its graders.
        title (str): One line for the report.
        fixture (str): The builder in `fixtures.py` that writes the working folder.
        prompt (str): What a live agent is told, ending with the result line it owes.
        allow_row_values (bool): Whether the server lets tools return row values.
        max_turns (int): The most turns a live agent gets.
        budget_usd (float): The most a live run of this task may cost.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^T\d\d$")
    kind: Kind
    title: str
    fixture: str
    prompt: str
    allow_row_values: bool = False
    max_turns: int = Field(default=12, ge=1)
    budget_usd: float = Field(default=0.5, gt=0)


def load_tasks(folder: Path = TASKS_FOLDER) -> list[Task]:
    """Read every task file in order, and hold each file to the id it carries.

    Args:
        folder (Path): The folder of task files.

    Returns:
        list[Task]: The tasks, in id order.

    Raises:
        ValueError: If a file holds another task's id, or two files share one.
    """
    tasks: list[Task] = []
    for path in sorted(folder.glob("T*.yaml")):
        task = Task.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        if task.id != path.stem:
            raise ValueError(f"{path.name} holds the task {task.id}.")
        tasks.append(task)
    if len({task.id for task in tasks}) != len(tasks):
        raise ValueError("Two task files share an id.")
    return tasks


def select(tasks: Sequence[Task], ids: str) -> list[Task]:
    """Return the tasks `ids` names: `all`, or a list such as `T01,T07`.

    Args:
        tasks (Sequence[Task]): Every task.
        ids (str): `all`, or comma-separated ids.

    Returns:
        list[Task]: The named tasks, in the order named.

    Raises:
        ValueError: If an id names no task.
    """
    if ids == "all":
        return list(tasks)
    wanted = [part.strip() for part in ids.split(",") if part.strip()]
    by_id = {task.id: task for task in tasks}
    unknown = [name for name in wanted if name not in by_id]
    if unknown:
        raise ValueError(f"No task is named {', '.join(unknown)}.")
    return [by_id[name] for name in wanted]
