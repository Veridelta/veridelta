# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Write `llms.txt` and `llms-full.txt` into the built site, for language models.

`llms.txt` follows https://llmstxt.org/: the site's name and description, a
short guide, then a link to every page in navigation order, each noted with
the page's opening sentence. `llms-full.txt` holds every page of prose in one
file, with its links made absolute. Notebooks and the API reference are linked
but not copied, since their sources are notebook JSON and `:::` directives.

MkDocs runs a hook's handlers after those of the plugins in `mkdocs.yml`, so
each page arrives here with its macros rendered.
"""

import json
import posixpath
import re
from pathlib import Path
from typing import Any, NamedTuple

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.nav import Navigation
from mkdocs.structure.pages import Page

_GUIDE = (
    "Veridelta compares two datasets under the rules in a YAML file and reports what "
    "differs. Check a file with `veridelta validate --json`, run it with "
    "`veridelta run --json`, and read the exit code. The [AI agents]({site}agents/) page "
    "gives the steps, and [llms-full.txt]({site}llms-full.txt) holds every page under "
    "Docs in one file."
)
"""The paragraph under the summary in `llms.txt`, with `{site}` for the site's URL."""

_BLANK_LINE = re.compile(r"\n\s*\n")
_FENCE = re.compile(r"^\s*(```|~~~)")
_LIST_ITEM = re.compile(r"([-*+]|\d+\.)\s")
_NOT_PARAGRAPH = ("#", "|", "```", "~~~", "!!!", ":::", "<", ">")
_DIRECTIVE = re.compile(r"^:::", re.MULTILINE)
_LINK = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_CODE = re.compile(r"`+[^`]*`+")
_SENTENCE_END = re.compile(r"[.!?](?=\s|$)|:$")
_CODE_OR_RELATIVE_LINK = re.compile(
    r"(`+[^`]*`+)|\]\((?![a-z][a-z0-9+.-]*:)([^)\s]+)\)", re.IGNORECASE
)
"""Inline code, which stays as written, or a link target with no scheme."""


class SitePage(NamedTuple):
    """A built page, as the two files describe it."""

    title: str
    url: str
    """The page's absolute URL."""
    source: str
    """The page's path under `docs/`, such as `cli.md`."""
    text: str
    """The page's Markdown after macros, or a notebook's JSON."""


def is_prose(page: SitePage) -> bool:
    """Return whether a page is Markdown prose that `llms-full.txt` copies.

    Args:
        page (SitePage): A built page.

    Returns:
        bool: False for a notebook, or a page of `:::` API directives.
    """
    return page.source.endswith(".md") and not _DIRECTIVE.search(page.text)


def opening_sentence(markdown: str) -> str:
    """Return the first sentence of the first paragraph, with links reduced to their text.

    A sentence that leads into a code block ends in a colon, which becomes a period.

    Args:
        markdown (str): A page's Markdown, its heading included.

    Returns:
        str: The sentence, or an empty string when no paragraph is found.
    """
    for block in _BLANK_LINE.split(markdown):
        text = " ".join(block.split())
        if not text or text.startswith(_NOT_PARAGRAPH) or _LIST_ITEM.match(text):
            continue
        text = _LINK.sub(r"\1", text)
        masked = _CODE.sub(lambda code: "x" * len(code.group(0)), text)
        end = _SENTENCE_END.search(masked)
        sentence = text[: end.end()] if end else text
        return sentence[:-1] + "." if sentence.endswith(":") else sentence
    return ""


def page_note(page: SitePage) -> str:
    """Return the sentence that notes a page's link in `llms.txt`.

    Args:
        page (SitePage): A built page.

    Returns:
        str: The opening sentence of a page, or of a notebook's first Markdown cell.
    """
    if not page.source.endswith(".ipynb"):
        return opening_sentence(page.text)
    cells: list[dict[str, Any]] = json.loads(page.text)["cells"] if page.text else []
    markdown = ["".join(cell["source"]) for cell in cells if cell["cell_type"] == "markdown"]
    return opening_sentence(markdown[0]) if markdown else ""


def page_url(source: str) -> str:
    """Return the path a docs file is published at, as MkDocs builds directory URLs.

    Args:
        source (str): A path under `docs/`, such as `examples/01_core_concepts.ipynb`.

    Returns:
        str: The path after the site's URL, such as `examples/01_core_concepts/`.
    """
    stem, suffix = posixpath.splitext(source)
    if suffix not in {".md", ".ipynb"}:
        return source
    if stem == "index" or stem.endswith("/index"):
        return stem[: -len("index")]
    return f"{stem}/"


def absolute_links(markdown: str, source: str, site_url: str) -> str:
    """Point a page's relative links at the published site, leaving code as written.

    Args:
        markdown (str): The page's Markdown.
        source (str): The page's path under `docs/`, which relative links start from.
        site_url (str): The site's URL, ending in a slash.

    Returns:
        str: The Markdown with every relative link, anchors included, made absolute.
    """

    def absolute(match: re.Match[str]) -> str:
        if match.group(1):
            return match.group(0)
        path, mark, anchor = match.group(2).partition("#")
        target = posixpath.normpath(posixpath.join(posixpath.dirname(source), path))
        return f"]({site_url}{page_url(target if path else source)}{mark}{anchor})"

    lines: list[str] = []
    fenced = False
    for line in markdown.splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        elif not fenced:
            line = _CODE_OR_RELATIVE_LINK.sub(absolute, line)
        lines.append(line)
    return "\n".join(lines)


def render_index(name: str, description: str, site_url: str, pages: list[SitePage]) -> str:
    """Write `llms.txt`: the site, a short guide, and a noted link to every page.

    Args:
        name (str): The site's name.
        description (str): The site's one-line description.
        site_url (str): The site's URL, ending in a slash.
        pages (list[SitePage]): Every page, in navigation order.

    Returns:
        str: Prose pages under `Docs`, and notebooks and the API reference under
            `Optional`, which a model short of context can skip.
    """

    def entry(page: SitePage) -> str:
        note = page_note(page)
        return f"- [{page.title}]({page.url}): {note}" if note else f"- [{page.title}]({page.url})"

    parts = [f"# {name}", f"> {description}", _GUIDE.format(site=site_url)]
    parts += ["## Docs", "\n".join(entry(page) for page in pages if is_prose(page))]
    optional = [entry(page) for page in pages if not is_prose(page)]
    if optional:
        parts += ["## Optional", "\n".join(optional)]
    return "\n\n".join(parts) + "\n"


def render_full(name: str, description: str, site_url: str, pages: list[SitePage]) -> str:
    """Write `llms-full.txt`: every page of prose in navigation order, links made absolute.

    Args:
        name (str): The site's name.
        description (str): The site's one-line description.
        site_url (str): The site's URL, ending in a slash.
        pages (list[SitePage]): Every page, in navigation order.

    Returns:
        str: Each prose page after a `Source:` line naming its URL.
    """
    parts = [f"# {name}", f"> {description}"]
    for page in pages:
        if is_prose(page):
            text = absolute_links(page.text.strip(), page.source, site_url)
            parts.append(f"Source: {page.url}\n\n{text}")
    return "\n\n".join(parts) + "\n"


_nav: list[Page] = []
"""The site's pages in navigation order, from the last build."""

_texts: dict[str, str] = {}
"""Each page's Markdown after macros, or a notebook's JSON, by its path under `docs/`."""


def on_nav(nav: Navigation, **_: object) -> Navigation:
    """Keep the site's pages in navigation order."""
    _nav[:] = nav.pages
    return nav


def on_page_markdown(markdown: str, *, page: Page, **_: object) -> str:
    """Keep each page's text once the macros plugin has rendered it."""
    _texts[str(page.file.src_uri)] = markdown
    return markdown


def on_post_build(*, config: MkDocsConfig, **_: object) -> None:
    """Write `llms.txt` and `llms-full.txt` into the built site."""
    site_url = str(config.site_url or "")
    pages = [
        SitePage(
            title=str(page.title or page.file.name),
            url=f"{site_url}{page.url}",
            source=str(page.file.src_uri),
            text=_texts.get(str(page.file.src_uri), ""),
        )
        for page in _nav
    ]
    name, description = str(config.site_name), str(config.site_description or "")
    site = Path(config.site_dir)
    (site / "llms.txt").write_text(
        render_index(name, description, site_url, pages), encoding="utf-8"
    )
    (site / "llms-full.txt").write_text(
        render_full(name, description, site_url, pages), encoding="utf-8"
    )
