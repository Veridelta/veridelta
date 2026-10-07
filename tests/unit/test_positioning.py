# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold every short statement of what Veridelta does to one wording.

The sentence is `project.description` in pyproject.toml, which PyPI shows as
the summary. The docs site description, which llms.txt quotes, the package
docstring, and `veridelta --help` repeat it. The README, which is also the
PyPI page, and the docs home open with one paragraph.
"""

import ast
import re
import tomllib
from pathlib import Path

import pytest

from veridelta.cli import build_parser

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]


def _summary() -> str:
    """Read the sentence PyPI shows as the summary."""
    with (_ROOT / "pyproject.toml").open("rb") as file:
        return str(tomllib.load(file)["project"]["description"])


def _opening(path: Path) -> str:
    """Return the first line of a page that starts with the project's name."""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("Veridelta "):
            return line
    raise AssertionError(f"{path.name} has no paragraph that starts with 'Veridelta '.")


class TestPositioning:
    """Keep each copy of what Veridelta does the same."""

    def test_the_docs_site_describes_itself_as_pypi_does(self) -> None:
        """Ensure `site_description`, which llms.txt quotes, is the PyPI summary.

        `mkdocs.yml` is read as text: its `!!python/name:` tags need a custom YAML loader.
        """
        text = (_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
        found = re.search(r"^site_description: (.+)$", text, re.MULTILINE)

        assert found is not None
        assert found.group(1) == _summary()

    def test_the_package_docstring_is_the_summary(self) -> None:
        """Ensure `help(veridelta)` and the API page open with the PyPI summary."""
        source = (_ROOT / "src" / "veridelta" / "__init__.py").read_text(encoding="utf-8")

        assert ast.get_docstring(ast.parse(source)) == _summary()

    def test_the_command_line_help_is_the_summary(self) -> None:
        """Ensure `veridelta --help` opens with the PyPI summary."""
        assert build_parser().description == _summary()

    def test_the_readme_and_the_docs_home_open_alike(self) -> None:
        """Ensure the PyPI page and the docs home open with the same paragraph."""
        readme = _opening(_ROOT / "README.md")

        assert readme == _opening(_ROOT / "docs" / "index.md")
        assert readme.startswith("Veridelta compares two datasets")
