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
_FENCE = re.compile(r"^\s*(```|~~~)")
_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_FRONT_MATTER = re.compile(r"\A---\n.*?\n---\n", re.DOTALL)
_AGENTS = _ROOT / "AGENTS.md"


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
    """Keep `AGENTS.md` pointing at the rules files and headings that exist."""

    def test_it_links_every_rules_file(self) -> None:
        """Ensure a new file in `.cursor/rules` cannot ship without a row in `AGENTS.md`."""
        rules = {path.relative_to(_ROOT).as_posix() for path in _ROOT.glob(".cursor/rules/*.mdc")}
        linked = {target.partition("#")[0] for target in _repository_links(_AGENTS)}

        assert rules, "No rules files found under .cursor/rules."
        assert rules <= linked, "Link these from AGENTS.md:\n" + "\n".join(sorted(rules - linked))

    def test_it_resolves_every_repository_link(self) -> None:
        """Ensure each file and heading that `AGENTS.md` links to exists."""
        broken = []
        for target in _repository_links(_AGENTS):
            path, _, anchor = target.partition("#")
            linked = _ROOT / path
            if not linked.is_file() or (anchor and anchor not in _anchors(linked)):
                broken.append(target)

        assert not broken, "These AGENTS.md links name nothing:\n" + "\n".join(broken)

    def test_claude_code_reads_it(self) -> None:
        """Ensure `CLAUDE.md` only imports `AGENTS.md`, so the rules live in one file."""
        assert (_ROOT / "CLAUDE.md").read_text(encoding="utf-8").split() == ["@AGENTS.md"]
