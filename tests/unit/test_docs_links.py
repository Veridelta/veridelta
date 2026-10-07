# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Resolve the documentation links that `mkdocs build --strict` cannot see.

The strict build checks every link between Markdown pages, anchors included.
It does not read notebook Markdown, which mkdocs-jupyter renders on its own, or
the README, which GitHub and PyPI show away from the site. Both link to the
published site by absolute URL, and this module resolves each URL against the
page and the heading it names. Notebook links must be absolute: mkdocs-jupyter
leaves a link as written, so `../rules.md` would point inside the notebook's
own folder on the site. `AGENTS.md` links files in the repository instead, and
this module resolves those too.
"""

import json
import os
import re
import unicodedata
from collections.abc import Iterator
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_DOCS = _ROOT / "docs"
_GENERATED = {name: _ROOT / "hooks" / "llms_txt.py" for name in ("llms.txt", "llms-full.txt")}
"""Files the docs build writes, each mapped to the hook that writes it."""

_SITE_URL = re.compile(
    r"https://veridelta\.github\.io/veridelta/([^)\s#\"'`<>]*)(?:#([^)\s\"'`<>]*))?",
    re.IGNORECASE,
)
_LINK_TARGET = re.compile(r"\]\(([^)\s]+)\)")
_REPOSITORY_URL = re.compile(
    r"https://github\.com/Veridelta/veridelta/blob/main/([^)\s#\"'`<>]+)(?:#([^)\s\"'`<>]+))?"
)
_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_FRONT_MATTER = re.compile(r"\A---\n.*?\n---\n", re.DOTALL)
_AGENTS = _ROOT / "AGENTS.md"
_SKIPPED = frozenset({".git", ".venv", "site", "node_modules", ".cache", "__pycache__"})
"""Folders no tool reads instructions from, left out of the walk."""
_SHADOWING = frozenset({"claude.md", "claude.local.md"})
"""The names, lowercased, of the files that stop Claude Code from reading `AGENTS.md`."""
_TOOL_FILE = re.compile(r"claude.*\.md|\.cursorignore|\.mcp\.json", re.IGNORECASE)
"""A file that one agent tool reads and the other supported tool does not."""


def _files_under(root: Path) -> Iterator[Path]:
    """Yield every file below a folder by its name on disk, pruning folders no tool reads."""
    for folder, subfolders, names in os.walk(root):
        subfolders[:] = sorted(name for name in subfolders if name not in _SKIPPED)
        yield from (Path(folder, name) for name in sorted(names))


def _slug(heading: str) -> str:
    """Return the anchor Python Markdown's `toc` extension gives a heading.

    Args:
        heading (str): Heading text as written, after the `#` marks.

    Returns:
        str: The heading's id, before any suffix that makes it unique.
    """
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def _anchors(page: Path) -> set[str]:
    """Return the id of every heading on a Markdown page.

    Args:
        page (Path): A page under `docs/`.

    Returns:
        set[str]: Heading ids, with `_1`, `_2`, and so on for repeated headings.
    """
    text = _FRONT_MATTER.sub("", page.read_text(encoding="utf-8"))
    ids: set[str] = set()
    fenced = False
    for line in text.splitlines():
        if _FENCE.match(line):
            fenced = not fenced
            continue
        heading = None if fenced else _HEADING.match(line)
        if heading:
            base = _slug(heading.group(2))
            anchor, count = base, 0
            while anchor in ids:
                count += 1
                anchor = f"{base}_{count}"
            ids.add(anchor)
    return ids


def _resolve(path: str) -> Path | None:
    """Return the source file a site path is built from, if any.

    Args:
        path (str): The URL path after the site root, such as `rules/`.

    Returns:
        Path | None: A Markdown page, a notebook, a published file, or the hook
            that writes a generated file.
    """
    if path in _GENERATED:
        return _GENERATED[path]
    if "." in path.rsplit("/", 1)[-1]:
        candidates = [_DOCS / path]
    else:
        stem = path.strip("/") or "index"
        candidates = [_DOCS / f"{stem}.md", _DOCS / f"{stem}.ipynb"]
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def _notebook_markdown() -> Iterator[tuple[str, str]]:
    """Yield each tutorial's Markdown cells with where they come from."""
    for notebook in sorted((_DOCS / "examples").glob("*.ipynb")):
        cells = json.loads(notebook.read_text(encoding="utf-8"))["cells"]
        for index, cell in enumerate(cells):
            if cell["cell_type"] == "markdown":
                yield f"{notebook.relative_to(_ROOT)} cell {index}", "".join(cell["source"])


def _texts() -> Iterator[tuple[str, str]]:
    """Yield every text that can link to the site by absolute URL."""
    for name in ("README.md", "CONTRIBUTING.md", "ACCESSIBILITY.md"):
        yield name, (_ROOT / name).read_text(encoding="utf-8")
    for page in sorted(_DOCS.rglob("*.md")):
        yield str(page.relative_to(_ROOT)), page.read_text(encoding="utf-8")
    yield from _notebook_markdown()
    for module in sorted((_ROOT / "src" / "veridelta").rglob("*.py")):
        yield str(module.relative_to(_ROOT)), module.read_text(encoding="utf-8")


def _site_links() -> list[tuple[str, str, str | None]]:
    """List every absolute link to the site as (where, path, anchor)."""
    return [
        (where, match.group(1), match.group(2) or None)
        for where, text in _texts()
        for match in _SITE_URL.finditer(text)
    ]


def _repository_links(page: Path) -> list[str]:
    """Return the links on a Markdown file that point inside the repository.

    Args:
        page (Path): A Markdown file.

    Returns:
        list[str]: Each relative link target, with its `#anchor` if it has one.
    """
    text = page.read_text(encoding="utf-8")
    return [target for target in _LINK_TARGET.findall(text) if "://" not in target]


def _repository_urls() -> list[tuple[str, str, str | None]]:
    """List every link to a file on the repository's `main` branch as (where, path, anchor)."""
    pages = [
        _ROOT / "README.md",
        _AGENTS,
        *sorted(_DOCS.rglob("*.md")),
        *sorted((_ROOT / "product").rglob("*.md")),
        *sorted((_ROOT / "decisions").glob("*.md")),
        *sorted((_ROOT / "rules").glob("*.md")),
    ]
    return [
        (page.relative_to(_ROOT).as_posix(), path, anchor or None)
        for page in pages
        for path, anchor in _REPOSITORY_URL.findall(page.read_text(encoding="utf-8"))
    ]


class TestDocumentationLinks:
    """Keep links outside the strict build pointing at real pages and headings."""

    def test_it_finds_the_links_it_checks(self) -> None:
        """Ensure a changed URL pattern cannot silently empty every check."""
        sources = {where.split(" ")[0] for where, _, _ in _site_links()}

        assert "README.md" in sources
        assert any(source.endswith(".ipynb") for source in sources)

    def test_it_resolves_every_site_url(self) -> None:
        """Ensure each absolute site URL names a page or file that exists."""
        broken = [f"{where}: {path}" for where, path, _ in _site_links() if _resolve(path) is None]

        assert not broken, "These site URLs name no page:\n" + "\n".join(broken)

    def test_it_resolves_every_site_anchor(self) -> None:
        """Ensure each `#anchor` in a site URL names a heading on that page."""
        broken = []
        for where, path, anchor in _site_links():
            target = _resolve(path)
            if anchor is None or target is None:
                continue
            if target.suffix != ".md":
                broken.append(f"{where}: {path}#{anchor} (link to the notebook page instead)")
            elif anchor not in _anchors(target):
                broken.append(f"{where}: {path}#{anchor}")

        assert not broken, "These anchors name no heading:\n" + "\n".join(broken)

    def test_the_site_loads_every_stylesheet_and_script_it_lists(self) -> None:
        """Ensure each `extra_css` and `extra_javascript` file exists, which the build never checks.

        `mkdocs.yml` is read as text: its `!!python/name:` tags need a custom YAML loader.
        """
        text = (_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
        blocks = re.findall(r"^extra_(?:css|javascript):\n((?:  - .+\n)+)", text, re.MULTILINE)
        listed = [line.removeprefix("  - ") for block in blocks for line in block.splitlines()]

        assert listed == ["stylesheets/accessibility.css", "javascripts/accessibility.js"]
        assert [name for name in listed if not (_DOCS / name).is_file()] == []

    def test_notebooks_link_only_by_absolute_url(self) -> None:
        """Ensure no notebook link breaks once mkdocs-jupyter renders the page."""
        relative = [
            f"{where}: {target}"
            for where, text in _notebook_markdown()
            for target in _LINK_TARGET.findall(text)
            if not target.startswith("https://")
        ]

        assert not relative, "Use the published URL for these links:\n" + "\n".join(relative)

    def test_the_readme_links_only_by_absolute_url(self) -> None:
        """Ensure each README link works on PyPI, which has no other pages."""
        text = (_ROOT / "README.md").read_text(encoding="utf-8")
        relative = [
            target for target in _LINK_TARGET.findall(text) if not target.startswith("https://")
        ]

        assert not relative, "Use an absolute URL for these README links:\n" + "\n".join(relative)

    def test_every_repository_url_names_a_file_and_a_heading(self) -> None:
        """Ensure a link to a file on `main`, such as a use case card, survives a move or a retitle.

        GitHub slugs a heading as Python Markdown does for the headings linked here:
        lowercase, punctuation dropped, spaces to hyphens.
        """
        links = _repository_urls()
        broken = [
            f"{where}: {path}#{anchor}" if anchor else f"{where}: {path}"
            for where, path, anchor in links
            if not (_ROOT / path).is_file()
            or (anchor is not None and anchor not in _anchors(_ROOT / path))
        ]

        assert links, "No link to a file on main was found."
        assert not broken, "These links name no file or heading on main:\n" + "\n".join(broken)

    @pytest.mark.parametrize(
        ("heading", "anchor"),
        [
            ("Proposing a value map", "proposing-a-value-map"),
            ("8. Fuzzy Text Matching", "8-fuzzy-text-matching"),
            ("String Normalization & Sanitization", "string-normalization-sanitization"),
            ("`pad_zeros` and `cast_to`", "pad_zeros-and-cast_to"),
            (
                "Warehouse, lakehouse, and database sources",
                "warehouse-lakehouse-and-database-sources",
            ),
        ],
    )
    def test_it_slugs_headings_as_python_markdown_does(self, heading: str, anchor: str) -> None:
        """Ensure the slug rule matches the ids the site was built with."""
        assert _slug(heading) == anchor


class TestAgentInstructions:
    """Keep `AGENTS.md` the one instruction file the tools read, with every link resolving."""

    def test_it_resolves_every_repository_link(self) -> None:
        """Ensure each file and heading that `AGENTS.md` links to exists."""
        broken = []
        for target in _repository_links(_AGENTS):
            path, _, anchor = target.partition("#")
            linked = _ROOT / path
            if not linked.is_file() or (anchor and anchor not in _anchors(linked)):
                broken.append(target)

        assert not broken, "These AGENTS.md links name nothing:\n" + "\n".join(broken)

    def test_no_claude_md_shadows_it(self) -> None:
        """Ensure no `CLAUDE.md` exists anywhere, so Claude Code reads `AGENTS.md`.

        Claude Code 2.1.277 and later read `AGENTS.md` only while no `CLAUDE.md`,
        `.claude/CLAUDE.md`, or `CLAUDE.local.md` is in the working directory or any
        directory above it, and `~/.claude/CLAUDE.md` does not count. A nested one
        would be a second instruction file for one tool, and a `claude.md` shadows
        on a case-insensitive disk, so the walk compares lowercased names.
        """
        found = [
            path.relative_to(_ROOT).as_posix()
            for path in _files_under(_ROOT)
            if path.name.lower() in _SHADOWING
        ]

        assert not found, (
            "Claude Code reads AGENTS.md only while no CLAUDE.md, .claude/CLAUDE.md, or"
            " CLAUDE.local.md is in the working directory or above it. Delete:\n" + "\n".join(found)
        )

    def test_a_tool_file_exists_only_where_its_tool_needs_it(self) -> None:
        """Ensure a file one agent tool reads cannot appear without a record that says why.

        The two supported tools, Claude Code and Cursor, both read `AGENTS.md`, the
        rules, the bundle, and `.claude/skills/`. The record
        `decisions/a-tool-file-only-where-the-tool-needs-it.md` names what stays for
        one tool and what would let each go.
        """
        claude = {child.name for child in (_ROOT / ".claude").iterdir()}
        stray = [
            path.relative_to(_ROOT).as_posix()
            for path in _files_under(_ROOT)
            if _TOOL_FILE.fullmatch(path.name) or ".cursor" in path.relative_to(_ROOT).parts
        ]

        assert "skills" in claude
        assert claude <= {"skills", "settings.local.json"}, f".claude/ holds {sorted(claude)}."
        assert not stray, (
            "A tool's own file lives here only with a record in decisions/ that says why:\n"
            + "\n".join(stray)
        )
