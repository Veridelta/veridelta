# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the editor support to the schema and the CLI.

`.vscode/settings.json` maps configuration files to the schema the checkout
ships, `.vscode/extensions.json` and the dev container recommend the YAML
extension that reads it, and `.vscode/tasks.json` runs `veridelta validate` on
the open file with a problem matcher on its verdict line. The configuration
page shows users the same setting, with the site's copy of the schema, and the
same task. These tests keep the files, the page, and the verdict together.
"""

import json
import re
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import pytest

from veridelta.cli import build_parser, validate

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_SETTINGS = _ROOT / ".vscode" / "settings.json"
_EXTENSIONS = _ROOT / ".vscode" / "extensions.json"
_DEVCONTAINER = _ROOT / ".devcontainer" / "devcontainer.json"
_TASKS = _ROOT / ".vscode" / "tasks.json"
_PAGE = _ROOT / "docs" / "configuration.md"
_SCHEMA = _ROOT / "docs" / "schema" / "veridelta.schema.json"
_SITE = "https://veridelta.github.io/veridelta/"
_YAML_EXTENSION = "redhat.vscode-yaml"
_JSON_BLOCK = re.compile(r"^```json\n(.*?)^```$", re.MULTILINE | re.DOTALL)

_UNSET = "VERIDELTA_UNSET_FOR_THE_TASK_TEST"
_CLEAN = "primary_keys: [id]\nsource:\n  path: a.csv\ntarget:\n  path: b.csv\n"
_BROKEN = _CLEAN + "  format: parquett\n"
_REFERENCING = _CLEAN.replace("a.csv", f"${{{_UNSET}}}.csv")


def _read(path: Path) -> Any:
    """Parse a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def _page_blocks() -> list[Any]:
    """Parse every JSON block on the configuration page, in order."""
    page = _PAGE.read_text(encoding="utf-8")
    return [json.loads(block) for block in _JSON_BLOCK.findall(page)]


def _matchers() -> list[dict[str, Any]]:
    """Return the task's problem matchers."""
    (task,) = _read(_TASKS)["tasks"]
    return list(task["problemMatcher"])


class TestSchemaWiring:
    """Hold the settings and the page to the published schema."""

    def test_the_checkout_maps_configuration_files_to_its_schema(self) -> None:
        """Ensure the workspace setting reads the schema file the repository ships.

        The pattern names `.yaml` files only, so `ci/gitlab/veridelta.yml`, the
        GitLab template, is never checked against the configuration schema.
        """
        ((schema, pattern),) = _read(_SETTINGS)["yaml.schemas"].items()

        assert (_ROOT / schema).resolve() == _SCHEMA
        assert _SCHEMA.is_file()
        assert fnmatch("demo/veridelta.yaml", pattern)
        assert not fnmatch("ci/gitlab/veridelta.yml", pattern)

    def test_the_page_maps_the_same_files_to_the_site_copy(self) -> None:
        """Ensure a user's setting names the published schema and the checkout's pattern."""
        block = next(block for block in _page_blocks() if "yaml.schemas" in block)
        (pattern,) = _read(_SETTINGS)["yaml.schemas"].values()
        url = _SITE + _SCHEMA.relative_to(_ROOT / "docs").as_posix()

        assert block == {"yaml.schemas": {url: pattern}}

    def test_the_yaml_extension_is_recommended_where_the_page_names_it(self) -> None:
        """Ensure the workspace and the dev container recommend the extension the page names."""
        recommended = _read(_EXTENSIONS)["recommendations"]
        container = _read(_DEVCONTAINER)["customizations"]["vscode"]["extensions"]

        assert _YAML_EXTENSION in recommended
        assert container == recommended
        assert f"`{_YAML_EXTENSION}`" in _PAGE.read_text(encoding="utf-8")


class TestValidateTask:
    """Hold the task and its problem matchers to the CLI."""

    def test_the_page_shows_the_committed_task(self) -> None:
        """Ensure the task users copy is the one the checkout runs."""
        assert _read(_TASKS) in _page_blocks()

    def test_the_task_runs_validate_on_the_open_file(self) -> None:
        """Ensure the command parses with the CLI and names the file in the editor."""
        (task,) = _read(_TASKS)["tasks"]

        args = build_parser().parse_args(task["args"])

        assert task["command"] == "veridelta"
        assert args.config == "${file}"

    def test_each_matcher_reads_one_line_per_file(self) -> None:
        """Ensure each matcher reports a file, not a location.

        The verdict line carries no line number, and VS Code refuses a pattern
        without a line group unless its kind is `file`.
        """
        patterns = [matcher["pattern"] for matcher in _matchers()]

        assert [matcher["severity"] for matcher in _matchers()] == ["error", "warning"]
        assert all(pattern["kind"] == "file" for pattern in patterns)
        assert all({"file", "message"} <= pattern.keys() for pattern in patterns)

    @pytest.mark.parametrize(
        ("text", "flags", "severity", "verdict"),
        [
            pytest.param(_BROKEN, [], "error", "1 error, 0 warnings.", id="error"),
            pytest.param(
                _REFERENCING,
                ["--allow-missing-env"],
                "warning",
                "valid, with 1 warning.",
                id="warning",
            ),
            pytest.param(_CLEAN, [], None, "valid.", id="valid"),
        ],
    )
    def test_the_matchers_read_the_verdict_the_cli_prints(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        text: str,
        flags: list[str],
        severity: str | None,
        verdict: str,
    ) -> None:
        """Ensure the verdict line, and only it, reaches the panel with its file and severity."""
        monkeypatch.delenv(_UNSET, raising=False)
        path = tmp_path / "veridelta.yaml"
        path.write_text(text, encoding="utf-8")

        validate(build_parser().parse_args(["validate", "-c", str(path), *flags]))

        captured = capsys.readouterr()
        lines = [*captured.out.splitlines(), *captured.err.splitlines()]
        found = {
            matcher["severity"]: [
                (
                    match.group(matcher["pattern"]["file"]),
                    match.group(matcher["pattern"]["message"]),
                )
                for line in lines
                if (match := re.compile(matcher["pattern"]["regexp"]).search(line))
            ]
            for matcher in _matchers()
        }
        expected: dict[str, list[tuple[str, str]]] = {"error": [], "warning": []}
        if severity is not None:
            expected[severity] = [(str(path), verdict)]

        assert f"{path}: {verdict}" in lines
        assert found == expected
