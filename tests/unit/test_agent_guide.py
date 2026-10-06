# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the AI agents page and the `llms.txt` hook in step with the CLI and the site.

The agents page tells an agent which commands to run, so each command line on
it must still parse. The hook that writes `llms.txt` and `llms-full.txt` runs
only inside a docs build, so these tests load it by path, as MkDocs does, and
check its functions on sample pages.
"""

import importlib.util
import json
import re
import shlex
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import yaml

from veridelta.cli import build_parser

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_PAGE = _ROOT / "docs" / "agents.md"
_USER_SKILL = _ROOT / "skills" / "veridelta" / "SKILL.md"
_SKILLS = [_USER_SKILL, *sorted((_ROOT / ".claude" / "skills").glob("*/SKILL.md"))]
"""The skill users install, then the skills that contributors' agents load."""
_SKILL_NAME = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
_SITE = "https://example.org/docs/"
_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_COMMAND = re.compile(r"`(veridelta [^`]+)`")


def _load_hook() -> ModuleType:
    """Import `hooks/llms_txt.py` by path, as MkDocs does."""
    spec = importlib.util.spec_from_file_location("llms_txt", _ROOT / "hooks" / "llms_txt.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hook = _load_hook()


def _command_lines(path: Path) -> list[str]:
    """Return each `veridelta` command line on a page, in code blocks and inline code."""
    lines: list[str] = []
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        elif fenced and line.strip().startswith("veridelta "):
            lines.append(line.strip())
        elif not fenced:
            lines.extend(_INLINE_COMMAND.findall(line))
    return lines


def _page(source: str, text: str, title: str = "Page") -> Any:
    """Build a page as the hook sees it, published under `_SITE`."""
    return hook.SitePage(title=title, url=_SITE + hook.page_url(source), source=source, text=text)


def _notebook(*markdown_cells: str) -> str:
    """Write a notebook's JSON with a code cell, then the given Markdown cells."""
    cells = [{"cell_type": "code", "source": ["print(1)"]}]
    cells += [{"cell_type": "markdown", "source": [cell]} for cell in markdown_cells]
    return json.dumps({"cells": cells})


def _sample_pages() -> list[Any]:
    """Return a home page, a notebook, a guide page, and an API page, in nav order."""
    return [
        _page("index.md", "# Home\n\nVeridelta compares two datasets. More text.", "Home"),
        _page(
            "examples/01_core_concepts.ipynb",
            _notebook("# 1. Core concepts\n\nThis tutorial covers the basics. More."),
            "1. Core concepts",
        ),
        _page(
            "cli.md",
            "# Command line\n\nThe command runs a comparison. See [results](results.md).",
            "Command line",
        ),
        _page("api.md", "# API reference\n\nThe public interface.\n\n::: veridelta.models", "API"),
    ]


class TestAgentsPage:
    """Keep the commands the AI agents page gives an agent valid."""

    @pytest.mark.parametrize("path", [_PAGE, _USER_SKILL], ids=["page", "skill"])
    def test_its_command_lines_parse_with_the_cli(self, path: Path) -> None:
        """Ensure a renamed command or flag fails here, in the page and the skill alike."""
        lines = _command_lines(path)
        refused = []
        for line in lines:
            try:
                build_parser().parse_args(shlex.split(line)[1:])
            except SystemExit:
                refused.append(line)

        assert len(lines) >= 3, lines
        assert not refused, "The CLI refuses these command lines:\n" + "\n".join(refused)

    def test_the_site_lists_it_and_runs_the_hook(self) -> None:
        """Ensure the page is in the user guide's nav, and the build writes `llms.txt`.

        `mkdocs.yml` is read as text: its `!!python/name:` tags need a custom
        YAML loader, which mypy cannot check where PyYAML has no stubs.
        """
        text = (_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
        guide = text.split("  - User guide:\n", 1)[1].split("\n  - ", 1)[0]

        assert "\n    - AI agents: agents.md" in f"\n{guide}"
        assert "\nhooks:\n  - hooks/llms_txt.py\n" in text


class TestAgentSkills:
    """Hold each skill to the format agents read: a name, a description, and a version."""

    def test_it_finds_the_skills(self) -> None:
        """Ensure a moved folder cannot empty the checks below."""
        assert _USER_SKILL in _SKILLS
        assert len(_SKILLS) >= 2

    @pytest.mark.parametrize("path", _SKILLS, ids=lambda path: path.parent.name)
    def test_each_skill_names_itself_and_says_when_to_use_it(self, path: Path) -> None:
        """Ensure an agent can find the skill by its folder and tell when to load it."""
        text = path.read_text(encoding="utf-8")
        front_matter: dict[str, Any] = yaml.safe_load(text.split("---\n", 2)[1])

        assert set(front_matter) == {"name", "description", "metadata"}
        assert front_matter["name"] == path.parent.name
        assert _SKILL_NAME.fullmatch(front_matter["name"])
        assert "Use when" in front_matter["description"]
        assert len(front_matter["description"]) <= 1024
        assert re.fullmatch(r"\d+\.\d+\.\d+", front_matter["metadata"]["version"])


class TestLlmsTxt:
    """Validate the files the docs build writes for language models."""

    @pytest.mark.parametrize(
        ("markdown", "sentence"),
        [
            (
                "# Rules\n\nA rule changes a column. It can clean values.",
                "A rule changes a column.",
            ),
            (
                "# Results\n\n`DiffEngine.run()` returns a `DiffResult`. It holds rows.",
                "`DiffEngine.run()` returns a `DiffResult`.",
            ),
            ("# X\n\nRead [the rules](rules.md#order) first. Then run.", "Read the rules first."),
            (
                "# CI\n\nThe Action runs `veridelta run`:\n\n```yaml\nx: 1\n```",
                "The Action runs `veridelta run`.",
            ),
            ("# T\n\nThe default is 0.95 of rows. More.", "The default is 0.95 of rows."),
            (
                "# T\n\n!!! note\n    An aside.\n\n- a list\n\n| a | b |\n\nThe paragraph. More.",
                "The paragraph.",
            ),
            ("# Title only", ""),
        ],
    )
    def test_it_notes_a_page_with_its_opening_sentence(self, markdown: str, sentence: str) -> None:
        """Ensure the note is the sentence that defines the page, with links reduced to text."""
        assert hook.opening_sentence(markdown) == sentence

    def test_it_notes_a_notebook_with_its_first_markdown_cell(self) -> None:
        """Ensure a tutorial's note comes from its prose, not from its JSON."""
        page = _page(
            "examples/01.ipynb", _notebook("# 1. Basics\n\nIt checks data. More.", "# Next")
        )

        assert hook.page_note(page) == "It checks data."

    @pytest.mark.parametrize(
        ("source", "url"),
        [
            ("index.md", ""),
            ("cli.md", "cli/"),
            ("guide/index.md", "guide/"),
            ("examples/01_core_concepts.ipynb", "examples/01_core_concepts/"),
            ("schema/veridelta.schema.json", "schema/veridelta.schema.json"),
        ],
    )
    def test_it_publishes_pages_where_mkdocs_does(self, source: str, url: str) -> None:
        """Ensure each link names the URL MkDocs builds with directory URLs."""
        assert hook.page_url(source) == url

    def test_it_makes_relative_links_absolute(self) -> None:
        """Ensure a link in `llms-full.txt` works away from the page it came from."""
        markdown = (
            "See [rules](rules.md#order), [codes](#exit-codes), "
            "[a tutorial](examples/01.ipynb), and [Polars](https://pola.rs/)."
        )

        assert hook.absolute_links(markdown, "cli.md", _SITE) == (
            f"See [rules]({_SITE}rules/#order), [codes]({_SITE}cli/#exit-codes), "
            f"[a tutorial]({_SITE}examples/01/), and [Polars](https://pola.rs/)."
        )
        assert hook.absolute_links("[up](../rules.md)", "guide/setup.md", _SITE) == (
            f"[up]({_SITE}rules/)"
        )

    def test_it_leaves_code_as_written(self) -> None:
        """Ensure a link inside inline code or a code block is not rewritten."""
        markdown = "Write `[x](y.md)` as is.\n\n```markdown\n[rules](rules.md)\n```"

        assert hook.absolute_links(markdown, "cli.md", _SITE) == markdown

    def test_it_lists_prose_under_docs_and_the_rest_as_optional(self) -> None:
        """Ensure a model short of context can skip notebooks and the API reference."""
        text = hook.render_index("Veridelta", "Compare two datasets.", _SITE, _sample_pages())
        docs, optional = text.split("## Optional")

        assert text.startswith("# Veridelta\n\n> Compare two datasets.\n\n")
        assert f"[AI agents]({_SITE}agents/)" in text
        assert f"- [Home]({_SITE}): Veridelta compares two datasets." in docs
        assert f"- [Command line]({_SITE}cli/): The command runs a comparison." in docs
        assert (
            f"- [1. Core concepts]({_SITE}examples/01_core_concepts/): "
            "This tutorial covers the basics." in optional
        )
        assert f"- [API]({_SITE}api/): The public interface." in optional

    def test_it_copies_only_prose_into_the_full_text(self) -> None:
        """Ensure `llms-full.txt` holds each guide page whole, and no notebook JSON or directive."""
        text = hook.render_full("Veridelta", "Compare two datasets.", _SITE, _sample_pages())

        assert f"Source: {_SITE}cli/\n\n# Command line\n\nThe command runs" in text
        assert f"[results]({_SITE}results/)" in text
        assert "::: veridelta.models" not in text
        assert '"cells"' not in text

    def test_its_handlers_write_both_files(self, tmp_path: Path) -> None:
        """Ensure the handlers take MkDocs's keyword arguments and write into the site."""
        page = SimpleNamespace(
            title="Command line", url="cli/", file=SimpleNamespace(src_uri="cli.md", name="cli")
        )
        nav = SimpleNamespace(pages=[page])
        config = SimpleNamespace(
            site_url=_SITE,
            site_name="Veridelta",
            site_description="Compare two datasets.",
            site_dir=str(tmp_path),
        )
        markdown = "# Command line\n\nThe command runs."

        assert hook.on_nav(nav, config=config, files=None) is nav
        assert hook.on_page_markdown(markdown, page=page, config=config, files=None) == markdown
        hook.on_post_build(config=config)

        index = (tmp_path / "llms.txt").read_text(encoding="utf-8")
        assert f"- [Command line]({_SITE}cli/): The command runs." in index
        assert markdown in (tmp_path / "llms-full.txt").read_text(encoding="utf-8")
