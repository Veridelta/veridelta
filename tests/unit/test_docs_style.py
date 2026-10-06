# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold user-facing text to the writing rules a machine can check.

The rules are in CONTRIBUTING.md, under "Writing documentation". This module
checks four of them across the docs, the README, AGENTS.md, the tutorials,
docstrings, CLI help, schema descriptions, the CI templates, and the issue and
pull request templates: no dash characters, no spaced double hyphen used as a
dash, no marketing words, and no list that Python Markdown would render as part
of a paragraph. CHANGELOG.md is history, and CODE_OF_CONDUCT.md is the
Contributor Covenant as published. Both are left as written.
"""

import ast
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.fast]

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


def _label(path: Path) -> str:
    """Name a file relative to the repository root, with forward slashes on every OS."""
    return path.relative_to(_ROOT).as_posix()


class _Text(NamedTuple):
    """A piece of user-facing text and where it comes from."""

    where: str
    body: str
    markdown: bool


def _markdown_files() -> list[Path]:
    """Return the Markdown pages in scope."""
    pages = sorted((_ROOT / "docs").rglob("*.md"))
    root = [
        _ROOT / name
        for name in ("README.md", "CONTRIBUTING.md", "SECURITY.md", "AGENTS.md", "ACCESSIBILITY.md")
    ]
    records = sorted((_ROOT / "decisions").glob("*.md"))
    skills = sorted([*_ROOT.glob("skills/*/SKILL.md"), *_ROOT.glob(".claude/skills/*/SKILL.md")])
    return [*root, _ROOT / ".github" / "pull_request_template.md", *pages, *records, *skills]


def _notebook_cells() -> Iterator[_Text]:
    """Yield the Markdown cells of every tutorial."""
    for notebook in sorted((_ROOT / "docs" / "examples").glob("*.ipynb")):
        cells = json.loads(notebook.read_text(encoding="utf-8"))["cells"]
        for index, cell in enumerate(cells):
            if cell["cell_type"] == "markdown":
                body = "".join(cell["source"])
                yield _Text(f"{_label(notebook)} cell {index}", body, markdown=True)


def _python_text() -> Iterator[_Text]:
    """Yield docstrings and the `help=` and `description=` strings users read."""
    modules = [*(_ROOT / "src" / "veridelta").rglob("*.py"), *(_ROOT / "hooks").glob("*.py")]
    for module in sorted(modules):
        where = _label(module)
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
    """Yield the CI templates, the issue forms, and the package metadata whole."""
    names = ["action.yml", "ci/gitlab/veridelta.yml", "pyproject.toml", "mkdocs.yml"]
    forms = sorted((_ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"))
    for path in [*(_ROOT / name for name in names), *forms]:
        where = _label(path)
        yield _Text(where, path.read_text(encoding="utf-8"), markdown=False)


def _all_text() -> Iterator[_Text]:
    """Yield every piece of user-facing text in scope."""
    for page in _markdown_files():
        yield _Text(_label(page), page.read_text(encoding="utf-8"), markdown=True)
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


class TestDocumentationStyle:
    """Keep user-facing text free of the habits the writing rules forbid."""

    def test_it_finds_the_text_it_checks(self) -> None:
        """Ensure a moved folder cannot silently empty the scope of every check."""
        sources = {text.where.split(" ")[0].split(":")[0] for text in _all_text()}

        assert {
            "README.md",
            "AGENTS.md",
            "ACCESSIBILITY.md",
            ".github/ISSUE_TEMPLATE/accessibility.yml",
            "docs/configuration.md",
            "src/veridelta/models.py",
            "skills/veridelta/SKILL.md",
        } <= sources
        assert any(source.startswith("decisions/") for source in sources)
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
            text = _Text(_label(page), page.read_text(encoding="utf-8"), True)
            previous = ""
            for number, line in _prose_lines(text):
                if _LIST_ITEM.match(line) and previous and not _NOT_PARAGRAPH.match(previous):
                    found.append(f"{text.where} line {number}: {line.strip()[:100]}")
                previous = line

        assert not found, f"{_RULES}\n" + "\n".join(found)
