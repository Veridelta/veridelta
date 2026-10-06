# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the product bundle to its format and its trace.

`product/` keeps what the project knows about its users and its goals as an
Open Knowledge Format bundle: one concept per Markdown file, each opening with
frontmatter that says what it is, who wrote it, and where it stands. Ids tie
the concepts together: a persona is `P-01`, a use case `UC-01`, a metric
`NS-01`, `DR-01`, or `GR-01`. These tests check the frontmatter, the indexes,
the log, the links, and the trace from the north star down to the use cases,
the features that serve them, and the roadmap items that name them.
"""

import re
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_BUNDLE = _ROOT / "product"
_ROADMAP = _ROOT / "docs" / "roadmap.md"
_NO_USE_CASE = "No use case yet"
"""What a roadmap item says instead of a use case, with the reason it stays listed."""
_RESERVED = {"index.md", "log.md"}
_PAGES = sorted(_BUNDLE.rglob("*.md"))
_CONCEPTS = [page for page in _PAGES if page.name not in _RESERVED]
_METRICS = [concept for concept in _CONCEPTS if concept.parent == _BUNDLE / "metrics"]
_DIRECTORIES = sorted({concept.parent for concept in _CONCEPTS})

_FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)
_REQUIRED = {"type", "title", "description", "status", "generated"}
_STATUSES = {"draft", "stable", "deprecated"}
_PRODUCER = re.compile(r"(human|process):[a-z0-9._@-]+|[a-z][a-z0-9-]*")
"""A person, a job, or a tool by its name alone. A slash would carry a version or a model."""

_WHOLE_ID = re.compile(r"[A-Z]{1,5}-\d{2,}")
_ID = re.compile(r"(?<![A-Za-z0-9-])([A-Z]{1,5}-\d{2,})(?![A-Za-z0-9])")
_HEADING_ID = re.compile(r"^#{1,6}\s+([A-Z]{1,5}-\d{2,})\b")
_HEADING = re.compile(r"^#{1,6}\s")
_DATE_HEADING = re.compile(r"^## (\d{4}-\d{2}-\d{2})$")
_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_CODE = re.compile(r"(`+).*?\1")
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_LINKED = re.compile(r"\[[^\]]*\]\([^)\s]*\)")
_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_LIST_ITEM = re.compile(r"^\s*[-*] ")
_ROLE_BY_PREFIX = {"NS": "north-star", "DR": "driver", "GR": "guardrail"}


def _label(path: Path) -> str:
    """Name a file relative to the repository root, with forward slashes on every OS."""
    return path.relative_to(_ROOT).as_posix()


def _front_matter(path: Path) -> dict[str, object]:
    """Return a concept's frontmatter as YAML reads it."""
    match = _FRONT_MATTER.match(path.read_text(encoding="utf-8"))
    assert match is not None, f"{_label(path)} opens with no frontmatter."
    loaded: dict[str, object] = yaml.safe_load(match.group(1))
    return loaded


def _prose(path: Path) -> list[str]:
    """Return a page's lines after its frontmatter and outside code, with inline code removed."""
    lines: list[str] = []
    fenced = False
    for line in _FRONT_MATTER.sub("", path.read_text(encoding="utf-8")).splitlines():
        if _FENCE.match(line):
            fenced = not fenced
        elif not fenced:
            lines.append(_INLINE_CODE.sub("", line))
    return lines


def _slug(heading: str) -> str:
    """Return the anchor GitHub and Python Markdown give a heading: lowercase, punctuation dropped."""
    plain = re.sub(r"`([^`]*)`", r"\1", heading)
    plain = re.sub(r"[^\w\s-]", "", plain).strip().lower()
    return re.sub(r"[-\s]+", "-", plain)


def _anchors(page: Path) -> set[str]:
    """Return the anchor of every heading on a page, with the suffix a repeated one gets."""
    anchors: set[str] = set()
    for line in _prose(page):
        heading = _HEADING.match(line)
        if heading is None:
            continue
        base = anchor = _slug(heading.group(2))
        count = 0
        while anchor in anchors:
            count += 1
            anchor = f"{base}_{count}"
        anchors.add(anchor)
    return anchors


def _definitions() -> dict[str, list[str]]:
    """Return each defined id with where it is defined: a heading, or a metric's file name.

    A metric's own heading may repeat the id its file is named after; that is one definition.
    """
    defined: dict[str, list[str]] = {}
    for concept in _CONCEPTS:
        if _WHOLE_ID.fullmatch(concept.stem):
            defined.setdefault(concept.stem, []).append(f"{_label(concept)} (file name)")
        for number, line in enumerate(_prose(concept), start=1):
            heading = _HEADING_ID.match(line)
            if heading and heading.group(1) != concept.stem:
                defined.setdefault(heading.group(1), []).append(f"{_label(concept)} line {number}")
    return defined


def _mentions() -> list[tuple[str, str]]:
    """Return every id named in prose across the bundle, with where it is named."""
    return [
        (f"{_label(page)} line {number}", identifier)
        for page in _PAGES
        for number, line in enumerate(_prose(page), start=1)
        for identifier in _ID.findall(line)
    ]


def _sections(path: Path, prefix: str) -> dict[str, str]:
    """Return the text under each heading that defines an id with a prefix, up to the next heading."""
    sections: dict[str, str] = {}
    current = ""
    for line in _prose(path):
        if _HEADING.match(line):
            heading = _HEADING_ID.match(line)
            current = (
                heading.group(1) if heading and heading.group(1).startswith(f"{prefix}-") else ""
            )
            if current:
                sections[current] = ""
        elif current:
            sections[current] += f"{line}\n"
    return sections


def _north_stars() -> list[str]:
    """Return the id of every metric whose role is the north star."""
    return [metric.stem for metric in _METRICS if _front_matter(metric).get("role") == "north-star"]


class TestProductBundle:
    """Hold each concept to the format, and the bundle to its trace."""

    def test_it_finds_the_bundle(self) -> None:
        """Ensure a moved folder cannot empty every other check."""
        assert _BUNDLE / "USERS.md" in _CONCEPTS
        assert _BUNDLE / "KEY_METRICS.md" in _CONCEPTS
        assert _BUNDLE / "FEATURES.md" in _CONCEPTS
        assert _METRICS

    @pytest.mark.parametrize("concept", _CONCEPTS, ids=_label)
    def test_each_concept_opens_with_its_frontmatter(self, concept: Path) -> None:
        """Ensure a concept says what it is, where it stands, and who wrote it, but never which model."""
        fields = _front_matter(concept)

        assert set(fields) >= _REQUIRED, f"{_label(concept)} lacks {_REQUIRED - set(fields)}."
        assert isinstance(fields["type"], str)
        assert isinstance(fields["title"], str)
        assert isinstance(fields["description"], str)
        assert "\n" not in fields["description"].strip()
        assert fields["status"] in _STATUSES
        generated = fields["generated"]
        assert isinstance(generated, dict)
        assert set(generated) == {"by", "at"}
        assert _PRODUCER.fullmatch(str(generated["by"])), f"{generated['by']} is not a producer."
        moment = generated["at"]
        parsed = moment if isinstance(moment, datetime) else datetime.fromisoformat(str(moment))
        assert parsed.tzinfo is not None, f"{moment} has no offset."

    @pytest.mark.parametrize("metric", _METRICS, ids=lambda path: path.stem)
    def test_each_metric_states_its_place(self, metric: Path) -> None:
        """Ensure a metric's file name is its id, its role fits the id, and it supports the north star."""
        fields = _front_matter(metric)
        prefix = metric.stem.partition("-")[0]

        assert _WHOLE_ID.fullmatch(metric.stem), f"{_label(metric)} is not named by an id."
        assert fields.get("role") == _ROLE_BY_PREFIX.get(prefix)
        if fields["role"] == "north-star":
            assert "supports" not in fields
        else:
            assert fields.get("supports") == _north_stars()[0]
        proves = fields.get("proves")
        assert isinstance(proves, list)
        assert proves
        assert all(str(use_case).startswith("UC-") for use_case in proves)

    def test_there_is_one_north_star(self) -> None:
        """Ensure the bundle names one north star, and only one."""
        assert len(_north_stars()) == 1

    def test_the_overview_links_every_metric(self) -> None:
        """Ensure the key metrics page lists each metric concept."""
        targets = set(_LINK.findall((_BUNDLE / "KEY_METRICS.md").read_text(encoding="utf-8")))
        missing = [metric.stem for metric in _METRICS if f"metrics/{metric.name}" not in targets]

        assert not missing, "KEY_METRICS.md does not link these metrics:\n" + "\n".join(missing)

    def test_each_id_is_defined_once(self) -> None:
        """Ensure no id names two things."""
        repeated = {
            identifier: places for identifier, places in _definitions().items() if len(places) > 1
        }

        assert not repeated, f"These ids are defined more than once: {repeated}"

    def test_every_named_id_is_defined(self) -> None:
        """Ensure a persona, a use case, or a metric is named only once it exists.

        A word with the shape of an id counts only when its capitals start an id the bundle
        defines, so a hash name such as SHA-256 is never reported.
        """
        defined = _definitions()
        prefixes = {identifier.partition("-")[0] for identifier in defined}
        dangling = sorted(
            {
                f"{where}: {identifier}"
                for where, identifier in _mentions()
                if identifier not in defined and identifier.partition("-")[0] in prefixes
            }
        )

        assert not dangling, "These ids are defined nowhere:\n" + "\n".join(dangling)

    def test_each_use_case_names_a_persona_and_a_metric(self) -> None:
        """Ensure every use case says whom it serves and what proves it worked."""
        sections = _sections(_BUNDLE / "USERS.md", "UC")
        incomplete = [
            use_case
            for use_case, text in sections.items()
            if not any(named.startswith("P-") for named in _ID.findall(text))
            or not any(named.partition("-")[0] in _ROLE_BY_PREFIX for named in _ID.findall(text))
        ]

        assert sections, "USERS.md defines no use case."
        assert not incomplete, "These use cases name no persona or no metric:\n" + "\n".join(
            incomplete
        )

    def test_each_persona_serves_a_use_case(self) -> None:
        """Ensure no persona is written without a use case that names it."""
        personas = _sections(_BUNDLE / "USERS.md", "P")
        named = {
            identifier
            for text in _sections(_BUNDLE / "USERS.md", "UC").values()
            for identifier in _ID.findall(text)
        }
        unused = sorted(set(personas) - named)

        assert personas, "USERS.md defines no persona."
        assert not unused, "No use case names these personas:\n" + "\n".join(unused)

    def test_every_use_case_is_served_by_a_feature(self) -> None:
        """Ensure the feature map names every use case, so none is a promise nothing keeps."""
        use_cases = set(_sections(_BUNDLE / "USERS.md", "UC"))
        served = set(_ID.findall("\n".join(_prose(_BUNDLE / "FEATURES.md"))))
        unserved = sorted(use_cases - served)

        assert use_cases, "USERS.md defines no use case."
        assert not unserved, "No feature serves these use cases:\n" + "\n".join(unserved)

    def test_every_roadmap_item_names_a_use_case(self) -> None:
        """Ensure each roadmap item serves a use case, or says that none asks for it yet."""
        use_cases = set(_sections(_BUNDLE / "USERS.md", "UC"))
        items = [line for line in _prose(_ROADMAP) if _LIST_ITEM.match(line)]
        unjustified = [
            line.strip()[:80]
            for line in items
            if not (set(_ID.findall(line)) & use_cases) and _NO_USE_CASE not in line
        ]

        assert items, "The roadmap lists nothing."
        assert not unjustified, "These roadmap items name no use case:\n" + "\n".join(unjustified)

    @pytest.mark.parametrize("directory", _DIRECTORIES, ids=lambda path: path.name)
    def test_each_directory_index_lists_its_concepts(self, directory: Path) -> None:
        """Ensure a reader can find every concept from the index a level above it."""
        index = directory / "index.md"
        assert index.is_file(), f"{_label(directory)} has no index.md."
        targets = {
            target.partition("#")[0] for target in _LINK.findall(index.read_text(encoding="utf-8"))
        }
        missing = [
            concept.name
            for concept in _CONCEPTS
            if concept.parent == directory and concept.name not in targets
        ]

        assert not missing, f"{_label(index)} does not list:\n" + "\n".join(missing)
        if directory != _BUNDLE:
            parent_index = (directory.parent / "index.md").read_text(encoding="utf-8")
            assert f"{directory.name}/index.md" in parent_index

    def test_the_log_runs_newest_first(self) -> None:
        """Ensure each heading of the log is a date, and the dates run newest first."""
        lines = _prose(_BUNDLE / "log.md")
        headings = [line for line in lines if line.startswith("## ")]
        dates = [
            date.fromisoformat(match.group(1))
            for line in headings
            if (match := _DATE_HEADING.match(line))
        ]

        assert headings, "The log has no entry."
        assert len(dates) == len(headings), "Every `##` heading of the log is a date."
        assert dates == sorted(set(dates), reverse=True)

    @pytest.mark.parametrize("page", _PAGES, ids=_label)
    def test_every_link_leads_to_a_file(self, page: Path) -> None:
        """Ensure each link on a page names a file that exists, inside the bundle or beside it."""
        broken = []
        for line in _prose(page):
            for target in _LINK.findall(line):
                path = target.partition("#")[0]
                if "://" in target or not path:
                    continue
                linked = _BUNDLE / path.lstrip("/") if path.startswith("/") else page.parent / path
                if not linked.is_file():
                    broken.append(target)

        assert not broken, f"These links on {_label(page)} lead nowhere:\n" + "\n".join(broken)

    @pytest.mark.parametrize("page", _PAGES, ids=_label)
    def test_every_anchor_names_a_heading(self, page: Path) -> None:
        """Ensure each `#anchor` in a link names a heading on the page it points at."""
        broken = []
        for line in _prose(page):
            for target in _LINK.findall(line):
                path, _, anchor = target.partition("#")
                if "://" in target or not anchor:
                    continue
                linked = page.parent / path if path else page
                if not linked.is_file() or anchor not in _anchors(linked):
                    broken.append(target)

        assert not broken, f"These anchors on {_label(page)} name no heading:\n" + "\n".join(broken)

    @pytest.mark.parametrize("page", [*_PAGES, _ROADMAP], ids=_label)
    def test_every_named_id_is_a_link(self, page: Path) -> None:
        """Ensure an id outside a heading links to its card, so a reader reaches it in one click."""
        unlinked = [
            f"{identifier} in: {line.strip()[:80]}"
            for line in _prose(page)
            if not line.startswith("#")
            for identifier in _ID.findall(_LINKED.sub("", line))
        ]

        assert not unlinked, f"Link these ids on {_label(page)} to their cards:\n" + "\n".join(
            unlinked
        )
