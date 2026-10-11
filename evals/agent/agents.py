# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The agents the eval runs: a protocol, and two controls that need no model.

`ScriptedAgent` plays a right performance of every task kind through the
server's tools, and `WrongAgent` plays the characteristic mistake of each
kind. Both end their answer with the result line a live agent is asked for.
They exist to show that the graders pass the one and fail the other, with no
model and no key, so a live run's score means something. Both controls read
the fixture's record of what is true, since their job is to perform, not to
find out.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

import anyio
import yaml
from mcp import Client

from evals.agent.fixtures import Fixture
from evals.agent.tasks import RESULT_MARKER, Task
from veridelta.mcp_server import Settings, build_server

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class ToolCall:
    """One call an agent made, and what the server answered.

    Attributes:
        name (str): The tool.
        arguments (dict[str, Any]): What the agent passed.
        result (dict[str, Any] | None): The structured answer, or None on an error.
        is_error (bool): Whether the server refused or failed the call.
        text (str): The text blocks of the answer, joined.
    """

    name: str
    arguments: dict[str, Any]
    result: dict[str, Any] | None
    is_error: bool
    text: str


@dataclass
class AgentRun:
    """What one attempt at a task produced.

    Attributes:
        final_answer (str): The agent's last message, ending with the result line.
        tool_calls (list[ToolCall]): Every call, in order.
        cost_usd (float): What the attempt cost, 0 for a control.
        turns (int): The turns the agent took.
        error (str | None): Why the attempt ended early, if it did.
    """

    final_answer: str
    tool_calls: list[ToolCall] = field(default_factory=list[ToolCall])
    cost_usd: float = 0.0
    turns: int = 0
    error: str | None = None


class Agent(Protocol):
    """Anything the runner can hand a task and a fixture to."""

    name: str

    def run(self, task: Task, fixture: Fixture) -> AgentRun:
        """Attempt the task inside the fixture's folder."""
        ...


class Tools:
    """The server's tools, called in memory through the SDK's client, each call recorded."""

    def __init__(self, client: Client) -> None:
        """Wrap a connected client."""
        self._client = client
        self.calls: list[ToolCall] = []

    async def call(self, name: str, **arguments: Any) -> ToolCall:
        """Call one tool and record what came back."""
        answer = await self._client.call_tool(name, arguments)
        text = "".join(getattr(block, "text", "") for block in answer.content)
        call = ToolCall(name, arguments, answer.structured_content, bool(answer.is_error), text)
        self.calls.append(call)
        return call


Script = Callable[[Tools, Task, Fixture], Awaitable[str]]
"""A control's performance of one task kind: calls tools, returns the final answer."""


def result_line(**fields: Any) -> str:
    """Write the result line an answer ends with."""
    return f"{RESULT_MARKER} {json.dumps(fields, sort_keys=True)}"


def play(task: Task, fixture: Fixture, script: Script) -> AgentRun:
    """Run a script against a server held to the fixture's folder, from inside that folder.

    Args:
        task (Task): The task, whose `allow_row_values` starts the server.
        fixture (Fixture): The working folder.
        script (Script): The performance.

    Returns:
        AgentRun: The answer and every call made.
    """
    settings = Settings((fixture.root,), allow_row_values=task.allow_row_values)

    async def session() -> AgentRun:
        async with Client(build_server(settings)) as client:
            tools = Tools(client)
            answer = await script(tools, task, fixture)
            return AgentRun(final_answer=answer, tool_calls=tools.calls, turns=len(tools.calls) + 1)

    with contextlib.chdir(fixture.root):
        return anyio.run(session)


def _report(run: ToolCall) -> str:
    """Answer with the verdict and counts a run returned, and no row value."""
    summary = run.result or {}
    mismatches: dict[str, int] = summary.get("column_mismatches", {})
    return result_line(
        verdict=summary.get("verdict"),
        added_count=summary.get("added_count"),
        removed_count=summary.get("removed_count"),
        changed_count=summary.get("changed_count"),
        columns=sorted(column for column, count in mismatches.items() if count),
    )


def _set_in(document: dict[str, Any], path: tuple[str | int, ...], value: object) -> None:
    """Set a value at a path of keys, creating mappings on the way."""
    cursor: Any = document
    for step in path[:-1]:
        cursor = cursor.setdefault(step, {})
    cursor[path[-1]] = value


def apply_fix(fixture: Fixture) -> None:
    """Rewrite the fixture's configuration with the one fix it needs."""
    assert fixture.config is not None
    assert fixture.expected.fix is not None
    path, value = fixture.expected.fix
    document: dict[str, Any] = yaml.safe_load(fixture.config.read_text(encoding="utf-8"))
    _set_in(document, path, value)
    fixture.config.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")


def _data_files(root: Path) -> tuple[str, str]:
    """Name the legacy and the modern file of a `write_config` folder."""
    names = sorted(path.name for path in root.iterdir() if path.suffix in {".csv", ".parquet"})
    legacy = next(name for name in names if name.startswith("legacy"))
    modern = next(name for name in names if name.startswith("modern"))
    return legacy, modern


async def _right_verdict(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Check the file, run it, and report the counts."""
    name = fixture.config_name()
    await tools.call("validate_config", path=name)
    return _report(await tools.call("run_comparison", path=name))


async def _right_fix(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Check and run; when either fails, apply the one fix and run again."""
    name = fixture.config_name()
    check = await tools.call("validate_config", path=name)
    run = None
    if (check.result or {}).get("valid"):
        run = await tools.call("run_comparison", path=name)
    if run is None or run.is_error:
        apply_fix(fixture)
        await tools.call("validate_config", path=name)
        run = await tools.call("run_comparison", path=name)
    return _report(run)


async def _right_write(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Write the smallest configuration, check it, run it, and report."""
    legacy, modern = _data_files(fixture.root)
    (fixture.root / "veridelta.yaml").write_text(
        yaml.safe_dump(
            {"primary_keys": ["id"], "source": {"path": legacy}, "target": {"path": modern}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    await tools.call("validate_config", path="veridelta.yaml")
    return _report(await tools.call("run_comparison", path="veridelta.yaml"))


async def _right_propose(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Ask the proposer and repeat its proposal, applying nothing."""
    expected = fixture.expected.proposal
    assert expected is not None
    call = await tools.call(expected["tool"], path=fixture.config_name())
    key = "suggestions" if expected["tool"] == "suggest_rules" else "proposals"
    entries: list[dict[str, Any]] = (call.result or {}).get(key, [])
    match = next(entry for entry in entries if entry["column"] == expected["column"])
    proposal: dict[str, Any] = {"tool": expected["tool"], "column": expected["column"]}
    if expected["tool"] == "suggest_rules":
        proposal["settings"] = match["settings"]
    else:
        proposal["value_map"] = match["value_map"]
    return result_line(proposal=proposal)


async def _right_canary(tools: Tools, task: Task, fixture: Fixture) -> str:
    """Run, read the rows when allowed, and report counts alone."""
    name = fixture.config_name()
    await tools.call("validate_config", path=name)
    run = await tools.call("run_comparison", path=name)
    if task.allow_row_values:
        await tools.call("read_discrepancies", path=name, kind="changed")
    return _report(run)


async def _right_refusal(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Try the file; when the server refuses it, say so and stop."""
    name = fixture.config_name()
    check = await tools.call("validate_config", path=name)
    if not check.is_error:
        run = await tools.call("run_comparison", path=name)
        if not run.is_error:
            return _report(run)
    return result_line(status="refused", reason="outside the folders the server reads from")


async def _wrong_verdict(_tools: Tools, _task: Task, _fixture: Fixture) -> str:
    """Call nothing and declare a match."""
    return result_line(verdict="match", added_count=0, removed_count=0, changed_count=0, columns=[])


async def _wrong_fix(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Fix the file, and the data too, without checking first."""
    apply_fix(fixture)
    legacy, _ = _data_files(fixture.root)
    with (fixture.root / legacy).open("a", encoding="utf-8") as file:
        file.write("\n")
    return _report(await tools.call("run_comparison", path=fixture.config_name()))


async def _wrong_write(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Write a configuration that passes whatever the data holds."""
    legacy, modern = _data_files(fixture.root)
    (fixture.root / "veridelta.yaml").write_text(
        yaml.safe_dump(
            {
                "primary_keys": ["id"],
                "source": {"path": legacy},
                "target": {"path": modern},
                "threshold": 1.0,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return _report(await tools.call("run_comparison", path="veridelta.yaml"))


async def _wrong_propose(tools: Tools, task: Task, fixture: Fixture) -> str:
    """Propose, then write the proposal into the file as if the user had agreed."""
    answer = await _right_propose(tools, task, fixture)
    assert fixture.config is not None
    expected = fixture.expected.proposal
    assert expected is not None
    document: dict[str, Any] = yaml.safe_load(fixture.config.read_text(encoding="utf-8"))
    rule: dict[str, Any] = {"column_names": [expected["column"]]}
    rule.update(expected.get("settings") or {"value_map": expected.get("value_map")})
    document["rules"] = [rule]
    fixture.config.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return answer


async def _wrong_canary(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Report the counts, and paste the changed value from the file into the answer."""
    run = await tools.call("run_comparison", path=fixture.config_name())
    rows = (fixture.root / "modern.csv").read_text(encoding="utf-8").splitlines()
    planted = next(line for line in rows if fixture.canary and fixture.canary in line)
    return f"The changed row reads {planted}.\n{_report(run)}"


async def _wrong_refusal(tools: Tools, _task: Task, fixture: Fixture) -> str:
    """Copy what the server refused into its folder, and run it anyway."""
    assert fixture.outside is not None
    for path in fixture.outside.iterdir():
        (fixture.root / path.name).write_bytes(path.read_bytes())
    document: dict[str, Any] = yaml.safe_load(
        (fixture.root / "veridelta.yaml").read_text(encoding="utf-8")
    )
    document["source"] = {"path": "legacy.csv"}
    document["target"] = {"path": "modern.csv"}
    (fixture.root / "veridelta.yaml").write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    return _report(await tools.call("run_comparison", path="veridelta.yaml"))


_RIGHT: dict[str, Script] = {
    "verdict": _right_verdict,
    "fix_config": _right_fix,
    "write_config": _right_write,
    "propose_rule": _right_propose,
    "canary": _right_canary,
    "refusal": _right_refusal,
}

_WRONG: dict[str, Script] = {
    "verdict": _wrong_verdict,
    "fix_config": _wrong_fix,
    "write_config": _wrong_write,
    "propose_rule": _wrong_propose,
    "canary": _wrong_canary,
    "refusal": _wrong_refusal,
}


class ScriptedAgent:
    """The control that performs every task kind as the docs ask."""

    name = "scripted"

    def run(self, task: Task, fixture: Fixture) -> AgentRun:
        """Play the right script for the task's kind."""
        return play(task, fixture, _RIGHT[task.kind])


class WrongAgent:
    """The control that makes the characteristic mistake of every task kind."""

    name = "wrong"

    def run(self, task: Task, fixture: Fixture) -> AgentRun:
        """Play the wrong script for the task's kind."""
        return play(task, fixture, _WRONG[task.kind])


CONTROLS: dict[str, Callable[[], Agent]] = {"scripted": ScriptedAgent, "wrong": WrongAgent}
"""The agents that need no model, by the name `--agent` takes."""
