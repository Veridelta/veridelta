# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep every version pin outside the package in step with the package.

A release writes the new version into the files that `[tool.commitizen]
version_files` lists in `pyproject.toml`, each with a pattern for the lines
that carry a pin. These tests hold the list and the files together: every line
a pattern selects carries the current version, and no pin of a shape a user
copies sits in a file the list does not cover. `CHANGELOG.md` is history and
is left as written.
"""

import re
import tomllib
from pathlib import Path

import pytest

from veridelta import __version__

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_PIN_SHAPE = re.compile(
    r"(?:Veridelta/veridelta@v|uvx veridelta@|Veridelta/veridelta/v"
    r"|veridelta(?:\[[a-z,]+\])?==|veridelta )(\d+\.\d+\.\d+)"
)
"""The ways a user copies a version: an Action ref, a `uvx` spec, a raw URL, a uv spec, and
the bug form's placeholder. A password such as `veridelta@127.0.0.1` matches none of them."""
_SWEPT = ("README.md", "CONTRIBUTING.md", "ACCESSIBILITY.md", "action.yml", "ci", "docs", ".github")
_SUFFIXES = {".md", ".yml", ".yaml", ".ipynb", ".toml"}
_VERSION_SHAPE = re.compile(r"\b\d+\.\d+\.\d+\b")


def _label(path: Path) -> str:
    """Name a file relative to the repository root, with forward slashes on every OS."""
    return path.relative_to(_ROOT).as_posix()


def _version_files() -> list[tuple[Path, re.Pattern[str]]]:
    """Return each file commitizen bumps, with the pattern that selects its lines."""
    with (_ROOT / "pyproject.toml").open("rb") as handle:
        entries: list[str] = tomllib.load(handle)["tool"]["commitizen"]["version_files"]
    return [
        (_ROOT / path, re.compile(pattern))
        for path, _, pattern in (entry.partition(":") for entry in entries)
    ]


def _swept_files() -> list[Path]:
    """Return every text file a user might copy a pin from."""
    files: list[Path] = []
    for name in _SWEPT:
        base = _ROOT / name
        if base.is_file():
            files.append(base)
        else:
            files.extend(path for path in sorted(base.rglob("*")) if path.suffix in _SUFFIXES)
    return files


class TestVersionPins:
    """Hold the pins a release must bump to the version the package reports."""

    def test_it_finds_the_files_it_checks(self) -> None:
        """Ensure a moved file cannot silently empty either check."""
        listed = {_label(path) for path, _ in _version_files()}
        swept = {_label(path) for path in _swept_files()}

        assert {"action.yml", "docs/ci.md", "ci/gitlab/veridelta.yml"} <= listed
        assert {"README.md", "docs/examples/05_validate_and_ci.ipynb"} <= swept

    @pytest.mark.parametrize(
        ("path", "pattern"),
        _version_files(),
        ids=[f"{path.name}:{pattern.pattern}" for path, pattern in _version_files()],
    )
    def test_each_listed_line_carries_the_current_version(
        self, path: Path, pattern: re.Pattern[str]
    ) -> None:
        """Ensure a pattern still selects lines, and each pin that follows a match is this version.

        Commitizen's own patterns are broad: `version` selects `pyarrow>=23.0.1; python_version`
        too. A version that sits before the match, or no version at all, pins nothing.
        """
        lines = [
            line for line in path.read_text(encoding="utf-8").splitlines() if pattern.search(line)
        ]
        stale = [
            line.strip()[:100]
            for line in lines
            for match in [pattern.search(line)]
            if match is not None
            and any(found != __version__ for found in _VERSION_SHAPE.findall(line[match.end() :]))
        ]

        assert lines, f"`{pattern.pattern}` selects no line of {_label(path)}."
        assert not stale, f"These lines of {_label(path)} pin another version:\n" + "\n".join(stale)

    def test_every_pin_a_user_copies_is_current(self) -> None:
        """Ensure no example, template, or form pins a version the package does not report."""
        stale = [
            f"{_label(path)}:{number}: {match.group(0)}"
            for path in _swept_files()
            if path.name != "CHANGELOG.md"
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
            for match in _PIN_SHAPE.finditer(line)
            if match.group(1) != __version__
        ]

        assert not stale, "These pins are behind the package:\n" + "\n".join(stale)
