# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold user-facing text to the writing rules a machine can check.

The rules are in CONTRIBUTING.md, under "Writing documentation". This module
checks four of them across the docs, the README, the tutorials, docstrings, CLI
help, schema descriptions, and the CI templates: no dash characters, no spaced
double hyphen used as a dash, no marketing words, and no list that Python
Markdown would render as part of a paragraph. CHANGELOG.md is history and is
left as written.
"""

import ast
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest

_ROOT = Path(__file__).resolve().parents[2]

_RULES = "See 'Writing documentation' in CONTRIBUTING.md."

_DASHES = re.compile("[\u2014\u2013]")
"""Em and en dashes. A colon or a period does the em dash's job; ranges use "to"."""

_SPACED_DOUBLE_HYPHEN = re.compile(r"\S -- \S")
"""Two hyphens standing in for a dash. A CLI flag such as `--json` never matches."""

_MARKETING = re.compile(
    r"\b(mission-critical|enterprise-grade|seamless(?:ly)?|blazing(?:ly)?|powerful"
    r"|high-performance|leverag(?:e|es|ed|ing)|simply|cutting-edge)\b",
    re.IGNORECASE,
)
"""Words that claim rather than describe."""

_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_CODE = re.compile(r"(`+).*?\1")
_LIST_ITEM = re.compile(r"([-*+]|\d+\.) ")
_NOT_PARAGRAPH = re.compile(r"\s|#|\||>|<|---|([-*+]|\d+\.) ")
"""Line starts that are not paragraph text: indentation, headings, tables,
quotes, HTML, rules, and list items."""


class _Text(NamedTuple):
    """A piece of user-facing text and where it comes from."""

    where: str
    body: str
    markdown: bool


def _markdown_files() -> list[Path]:
    """Return the Markdown pages in scope."""
    pages = sorted((_ROOT / "docs").rglob("*.md"))
    return [_ROOT / "README.md", _ROOT / "CONTRIBUTING.md", _ROOT / "SECURITY.md", *pages]


def _notebook_cells() -> Iterator[_Text]:
    """Yield the Markdown cells of every tutorial."""
    for notebook in sorted((_ROOT / "docs" / "examples").glob("*.ipynb")):
        cells = json.loads(notebook.read_text(encoding="utf-8"))["cells"]
        for index, cell in enumerate(cells):
            if cell["cell_type"] == "markdown":
                body = "".join(cell["source"])
                yield _Text(f"{notebook.relative_to(_ROOT)} cell {index}", body, markdown=True)


def _python_text() -> Iterator[_Text]:
    """Yield docstrings and the `help=` and `description=` strings users read."""
    for module in sorted((_ROOT / "src" / "veridelta").rglob("*.py")):
        where = module.relative_to(_ROOT)
        tree = ast.parse(module.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
                docstring = ast.get_docstring(node, clean=False)
                if docstring:
                    line = getattr(node, "lineno", 1)
                    yield _Text(f"{where}:{line}", docstring, markdown=False)
            if isinstance(node, ast.keyword) and node.arg in {"help", "description"}:
                value = node.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    yield _Text(f"{where}:{value.lineno}", value.value, markdown=False)


def _template_text() -> Iterator[_Text]:
    """Yield the CI templates and the package metadata whole."""
    for name in ("action.yml", "ci/gitlab/veridelta.yml", "pyproject.toml", "mkdocs.yml"):
        yield _Text(name, (_ROOT / name).read_text(encoding="utf-8"), markdown=False)


def _all_text() -> Iterator[_Text]:
    """Yield every piece of user-facing text in scope."""
    for page in _markdown_files():
        yield _Text(str(page.relative_to(_ROOT)), page.read_text(encoding="utf-8"), markdown=True)
    yield from _notebook_cells()
    yield from _python_text()
    yield from _template_text()


def _prose_lines(text: _Text) -> Iterator[tuple[int, str]]:
    """Yield each line outside code, with inline code removed from Markdown.

    Args:
        text (_Text): The text to read.

    Yields:
        tuple[int, str]: The 1-based line number within the text, and the line.
    """
    fenced = False
    for number, line in enumerate(text.body.splitlines(), start=1):
        if text.markdown and _FENCE.match(line):
            fenced = not fenced
            continue
        if fenced:
            continue
        yield number, _INLINE_CODE.sub("", line) if text.markdown else line


def _violations(pattern: re.Pattern[str], *, prose_only: bool) -> list[str]:
    """List every line in scope that matches a pattern.

    Args:
        pattern (re.Pattern[str]): What the rules forbid.
        prose_only (bool): Whether to skip fenced and inline code in Markdown.

    Returns:
        list[str]: `where line N: excerpt` for each match.
    """
    found: list[str] = []
    for text in _all_text():
        lines = _prose_lines(text) if prose_only else enumerate(text.body.splitlines(), start=1)
        for number, line in lines:
            match = pattern.search(line)
            if match:
                excerpt = line.strip()[:100]
                found.append(f"{text.where} line {number}: {excerpt}")
    return found


@pytest.mark.unit
@pytest.mark.fast
class TestDocumentationStyle:
    """Keep user-facing text free of the habits the writing rules forbid."""

    def test_it_finds_the_text_it_checks(self) -> None:
        """Ensure a moved folder cannot silently empty the scope of every check."""
        sources = {text.where.split(" ")[0].split(":")[0] for text in _all_text()}

        assert {"README.md", "docs/configuration.md", "src/veridelta/models.py"} <= sources
        assert any(source.endswith(".ipynb") for source in sources)

    def test_it_uses_no_dash_characters(self) -> None:
        """Ensure no em or en dash appears, in code or prose."""
        found = _violations(_DASHES, prose_only=False)

        assert not found, f"{_RULES}\n" + "\n".join(found)

    def test_it_uses_no_double_hyphen_as_a_dash(self) -> None:
        """Ensure prose does not stand two hyphens in for a dash."""
        found = _violations(_SPACED_DOUBLE_HYPHEN, prose_only=True)

        assert not found, f"{_RULES}\n" + "\n".join(found)

    def test_it_uses_no_marketing_words(self) -> None:
        """Ensure prose describes behavior instead of praising it."""
        found = _violations(_MARKETING, prose_only=True)

        assert not found, f"{_RULES}\n" + "\n".join(found)

    def test_it_starts_every_list_after_a_blank_line(self) -> None:
        """Ensure each docs page list renders as a list.

        Python Markdown, which builds the site, reads a list that directly
        follows a line of text as more of that paragraph.
        """
        found: list[str] = []
        for page in sorted((_ROOT / "docs").rglob("*.md")):
            text = _Text(str(page.relative_to(_ROOT)), page.read_text(encoding="utf-8"), True)
            previous = ""
            for number, line in _prose_lines(text):
                if _LIST_ITEM.match(line) and previous and not _NOT_PARAGRAPH.match(previous):
                    found.append(f"{text.where} line {number}: {line.strip()[:100]}")
                previous = line

        assert not found, f"{_RULES}\n" + "\n".join(found)
