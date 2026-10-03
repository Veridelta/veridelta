# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Static checks for the GitHub Action and the GitLab CI template.

Neither can run inside the test suite, so these pin the contracts a typo would
break: every input a step reads is declared, no input is expanded inside a
shell script, third-party actions are pinned, and the `veridelta run` command
line they build still parses with the CLI's own parser.
"""

import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

from veridelta.cli import build_parser

_ROOT = Path(__file__).resolve().parents[2]
_ACTION = _ROOT / "action.yml"


def _action() -> dict[str, Any]:
    """Load the composite action definition."""
    loaded: dict[str, Any] = yaml.safe_load(_ACTION.read_text(encoding="utf-8"))
    return loaded


def _steps() -> list[dict[str, Any]]:
    """Return the action's steps."""
    steps: list[dict[str, Any]] = _action()["runs"]["steps"]
    return steps


def _run_script() -> str:
    """Return the script of the step that runs Veridelta."""
    script: str = next(step for step in _steps() if step.get("id") == "run")["run"]
    return script


def _cli_arguments(script: str) -> list[str]:
    """Extract the `veridelta run` arguments from a script, with variables filled in.

    Args:
        script (str): Shell script containing one `veridelta run` invocation.

    Returns:
        list[str]: The arguments after `veridelta`, as the CLI parser sees them.
    """
    line = next(line for line in script.splitlines() if "veridelta run" in line)
    command = line[line.index("veridelta run") :].split(">", 1)[0]
    # Row caps must be numbers for the parser; every other value is a path or name.
    filled = re.sub(
        r"\$\{?[A-Z_]*MAX_ROWS\}?|\$\[\[\s*inputs\.html-max-rows\s*\]\]", "1000", command
    )
    filled = re.sub(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", "placeholder", filled)
    filled = re.sub(r"\$\[\[\s*inputs\.[A-Za-z0-9_-]+\s*\]\]", "placeholder", filled)
    return shlex.split(filled)[1:]


@pytest.mark.unit
@pytest.mark.fast
class TestGitHubAction:
    """Pin the composite action's contract."""

    def test_it_declares_every_input_it_reads(self) -> None:
        """Ensure a renamed or misspelled input fails here rather than reading as empty."""
        declared = set(_action()["inputs"])
        # `if:` conditions read inputs without `${{ }}`, so match the bare form.
        referenced = set(re.findall(r"\binputs\.([A-Za-z0-9_-]+)", _ACTION.read_text()))

        assert referenced <= declared, referenced - declared
        assert referenced == declared, declared - referenced

    def test_it_never_expands_an_expression_inside_a_shell_script(self) -> None:
        """Ensure inputs reach scripts through `env` only, so none can inject shell syntax."""
        for step in _steps():
            if "run" in step:
                assert "${{" not in step["run"], step["name"]

    def test_it_names_a_shell_for_every_script(self) -> None:
        """Ensure composite steps run under bash on every runner, Windows included."""
        for step in _steps():
            if "run" in step:
                assert step.get("shell") == "bash", step["name"]

    def test_it_pins_third_party_actions_to_commits(self) -> None:
        """Ensure a moved tag upstream cannot change what this action runs."""
        for step in _steps():
            if "uses" in step:
                assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", step["uses"]), step["uses"]

    def test_its_outputs_are_written_by_the_run_step(self) -> None:
        """Ensure each declared output is one the script actually sets."""
        script = _run_script()
        for name, output in _action()["outputs"].items():
            match = re.fullmatch(r"\$\{\{ steps\.run\.outputs\.([a-z-]+) \}\}", output["value"])
            assert match is not None, name
            assert f'echo "{match.group(1)}=' in script, name

    def test_its_command_line_parses_with_the_cli(self) -> None:
        """Ensure a renamed CLI flag breaks this test instead of every user's workflow."""
        arguments = _cli_arguments(_run_script())

        parsed = build_parser().parse_args(arguments)

        assert parsed.command == "run"
        assert parsed.json is True
        assert parsed.html == "placeholder/report.html"
        assert parsed.markdown == "placeholder/summary.md"
