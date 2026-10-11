# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""The live agent: a model that drives `veridelta mcp` through the Claude Agent SDK.

The SDK runs the Claude Code CLI it bundles, so the model plays the user of
Veridelta the way a Claude Code user does. It sees the server's six tools and
nothing else of the machine, gets a file tool only for the two task kinds that
ask it to fix or write a configuration, and is held to the task's turns and
budget. The SDK is imported only when an attempt runs, so the controls, the
tests, and `make eval-agent` need neither it nor a key. The model id comes
from the command line alone, and no run starts without one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

import anyio

from evals.agent.agents import AgentRun, ToolCall

if TYPE_CHECKING:
    from collections.abc import Iterable

    from evals.agent.fixtures import Fixture
    from evals.agent.tasks import Task

SERVER_NAME: Final = "veridelta"
"""How the SDK knows the server, and the middle of each tool's name."""

TOOL_PREFIX: Final = f"mcp__{SERVER_NAME}__"
"""What the SDK puts before a server tool's name."""

SERVER_TOOLS: Final = (
    "validate_config",
    "run_comparison",
    "describe_schema",
    "read_discrepancies",
    "propose_value_maps",
    "suggest_rules",
)
"""The six tools `veridelta mcp` serves, which every task may call."""

EDITING_KINDS: Final = frozenset({"fix_config", "write_config"})
"""The task kinds that ask the agent to change or create the configuration file."""

FILE_TOOLS: Final = ("Read", "Edit", "Write")
"""The built-in tools an editing task gets, and no other task does."""

WALL_CLOCK_SECONDS: Final = 600
"""The longest one attempt may run before it is stopped and counted as failed."""

SYSTEM_PROMPT: Final = (
    "You check datasets with Veridelta for a user. Use the Veridelta tools for every "
    "comparison: validate_config before run_comparison, and the proposal tools when the user "
    "asks what rule would explain a difference. Change a configuration file only when the "
    "task asks you to fix or write one, and never add a rule the user did not ask for. Report "
    "verdicts, counts, and column names, and never repeat a value from the data. If the "
    "server refuses a file, stop and say so. End every answer with the result line the task "
    "describes."
)
"""What the model is told before the task's prompt."""


@dataclass(frozen=True)
class Session:
    """Everything one attempt hands the SDK, free of SDK types so a test can read it.

    Attributes:
        prompt (str): The task's prompt.
        cwd (str): The fixture's folder, where the model and the server work.
        model (str): The model id, as given at dispatch.
        max_turns (int): The task's turn limit.
        max_budget_usd (float): The task's budget, which the SDK stops at.
        tools (list[str]): The built-in tools the model has at all: the file
            tools for an editing task, none otherwise.
        allowed_tools (list[str]): The tools that run without a prompt: every
            server tool, and the file tools for an editing task. Any other tool
            is denied, since the permission mode never asks.
        mcp_servers (dict[str, dict[str, Any]]): The one server, started over
            stdio from the fixture's folder with its folder as the root.
        system_prompt (str): What the model is told first.
        permission_mode (str): `dontAsk`, so nothing outside `allowed_tools` runs.
    """

    prompt: str
    cwd: str
    model: str
    max_turns: int
    max_budget_usd: float
    tools: list[str]
    allowed_tools: list[str]
    mcp_servers: dict[str, dict[str, Any]]
    system_prompt: str = SYSTEM_PROMPT
    permission_mode: str = "dontAsk"


def plan_session(task: Task, fixture: Fixture, model: str) -> Session:
    """Decide what the SDK gets for one attempt, from the task and the fixture alone.

    Args:
        task (Task): The task, whose kind decides whether a file tool is given.
        fixture (Fixture): The working folder.
        model (str): The model id.

    Returns:
        Session: The options, as data.
    """
    editing = task.kind in EDITING_KINDS
    flags = ["--allow-row-values"] if task.allow_row_values else []
    return Session(
        prompt=task.prompt,
        cwd=str(fixture.root),
        model=model,
        max_turns=task.max_turns,
        max_budget_usd=task.budget_usd,
        tools=list(FILE_TOOLS) if editing else [],
        allowed_tools=[f"{TOOL_PREFIX}{name}" for name in SERVER_TOOLS]
        + (list(FILE_TOOLS) if editing else []),
        mcp_servers={
            SERVER_NAME: {
                "type": "stdio",
                "command": "veridelta",
                "args": ["mcp", "--root", str(fixture.root), *flags],
            }
        },
    )


def _text_of(content: object) -> str:
    """Join the text parts of a tool result, which the SDK gives as text or as blocks."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        blocks: list[object] = list(content)  # pyright: ignore[reportUnknownArgumentType]
        return "".join(
            str(part.get("text", ""))  # pyright: ignore[reportUnknownMemberType]
            for part in blocks
            if isinstance(part, dict)
        )
    return ""


def _short_name(name: str) -> str:
    """Name a server tool as the graders know it, and a built-in tool as itself."""
    return name.removeprefix(TOOL_PREFIX)


@dataclass
class _Reading:
    """The calls, the answer, and the cost, while the messages are read."""

    pending: dict[str, tuple[str, dict[str, Any]]] = field(default_factory=dict)
    calls: list[ToolCall] = field(default_factory=list)
    texts: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    turns: int = 0
    error: str | None = None


def run_from_messages(messages: Iterable[Any]) -> AgentRun:
    """Read what the SDK streamed into the run the graders read.

    Each assistant message's tool uses are paired with the tool results the
    next user message carries, in order. The final answer is the result
    message's text when it gives one, else the last assistant text. A result
    that reports an error, such as the turn limit or the budget, becomes the
    run's error.

    Args:
        messages (Iterable[Any]): The SDK's messages, in order.

    Returns:
        AgentRun: The answer, the calls, the cost, and the turns.
    """
    reading = _Reading()
    final: str | None = None
    for message in messages:
        for block in getattr(message, "content", None) or []:
            if hasattr(block, "name") and hasattr(block, "input") and hasattr(block, "id"):
                reading.pending[str(block.id)] = (_short_name(str(block.name)), dict(block.input))
            elif hasattr(block, "tool_use_id"):
                name, arguments = reading.pending.pop(
                    str(block.tool_use_id), (f"unknown tool {block.tool_use_id}", {})
                )
                reading.calls.append(
                    ToolCall(name, arguments, None, bool(block.is_error), _text_of(block.content))
                )
            elif hasattr(block, "text") and type(block).__name__ == "TextBlock":
                reading.texts.append(str(block.text))
        if hasattr(message, "total_cost_usd") and hasattr(message, "num_turns"):
            reading.cost_usd = float(message.total_cost_usd or 0.0)
            reading.turns = int(message.num_turns)
            if message.is_error:
                reading.error = str(message.result or message.subtype)
            elif message.result:
                final = str(message.result)
    answer = final if final is not None else (reading.texts[-1] if reading.texts else "")
    return AgentRun(
        final_answer=answer,
        tool_calls=reading.calls,
        cost_usd=reading.cost_usd,
        turns=reading.turns,
        error=reading.error,
    )


class LiveAgent:
    """A model driving the server through the SDK, held to one budget for the whole run."""

    name = "live"

    def __init__(self, model: str, budget_usd: float) -> None:
        """Name the model and the most the whole run may spend.

        Args:
            model (str): The model id, as given at dispatch.
            budget_usd (float): The hard stop for every attempt together.
        """
        self.model = model
        self.budget_usd = budget_usd
        self.spent = 0.0

    def run(self, task: Task, fixture: Fixture) -> AgentRun:
        """Attempt the task, unless the run's budget is already spent."""
        if self.spent >= self.budget_usd:
            return AgentRun("", error=f"The budget of ${self.budget_usd:.2f} is spent.")
        session = plan_session(task, fixture, self.model)
        try:
            run = anyio.run(self._query, session)
        except TimeoutError:
            return AgentRun("", error=f"The attempt ran past {WALL_CLOCK_SECONDS} seconds.")
        self.spent += run.cost_usd
        return run

    async def _query(self, session: Session) -> AgentRun:
        """Send one prompt through the SDK and read everything it streams back."""
        from claude_agent_sdk import ClaudeAgentOptions, query

        options = ClaudeAgentOptions(
            cwd=session.cwd,
            model=session.model,
            max_turns=session.max_turns,
            max_budget_usd=session.max_budget_usd,
            tools=session.tools,
            allowed_tools=session.allowed_tools,
            mcp_servers=session.mcp_servers,
            strict_mcp_config=True,
            permission_mode=session.permission_mode,
            system_prompt=session.system_prompt,
        )
        with anyio.fail_after(WALL_CLOCK_SECONDS):
            messages: list[Any] = [
                message async for message in query(prompt=session.prompt, options=options)
            ]
        return run_from_messages(messages)
