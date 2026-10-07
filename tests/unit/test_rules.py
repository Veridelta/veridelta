# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the rules under `rules/` in their fixed shape, and the root table true to them.

Each file under `rules/` is one concept of the knowledge bundle: a rule an agent
reads before changing a file under the paths its front matter names. These tests
check that front matter, that every path exists, that the index and the "Rules by
path" table in `AGENTS.md` name every rule with the same paths, and that no
`AGENTS.md` sits below the root, where one tool would load a rule and the rest
would miss it.
"""

import os
import re
from datetime import datetime
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_RULES = sorted(path for path in (_ROOT / "rules").glob("*.md") if path.name != "index.md")
_FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)
_FIELDS = {"type", "title", "description", "status", "generated", "applies_to"}
_PRODUCER = re.compile(r"(human|process):[a-z0-9._@-]+|[a-z][a-z0-9-]*")
"""A person, a job, or a tool by its name alone, as the product bundle names one."""
_LINK = re.compile(r"\]\(([^)\s#]+)")
_ROW = re.compile(r"^\| \[[^\]]+\]\((rules/[^)#]+)\)[^|]*\| ([^|]*) \|$", re.MULTILINE)
_SKIPPED = frozenset({".git", ".venv", "site", "node_modules", ".cache", "__pycache__"})
"""Folders no tool reads instructions from, left out of the walk."""


def _front_matter(rule: Path) -> dict[str, object]:
    """Return a rule's front matter as YAML reads it."""
    match = _FRONT_MATTER.match(rule.read_text(encoding="utf-8"))
    assert match is not None, f"{rule.name} opens with no front matter."
    loaded: dict[str, object] = yaml.safe_load(match.group(1))
    return loaded


def _applies_to(rule: Path) -> list[str]:
    """Return the paths a rule's front matter says it governs."""
    paths = _front_matter(rule).get("applies_to")
    assert isinstance(paths, list), f"{rule.name} has no list in applies_to."
    assert paths, f"{rule.name} names no path in applies_to."
    return [str(path) for path in paths]


def _table() -> dict[str, set[str]]:
    """Return each rule the root table links, with the paths its row names in backticks."""
    text = (_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    section = text.split("## Rules by path", 1)[1].split("\n## ", 1)[0]
    return {link: set(re.findall(r"`([^`]+)`", cell)) for link, cell in _ROW.findall(section)}


class TestRules:
    """Hold each rule to the bundle's shape, and the root table to the rules."""

    def test_it_finds_the_rules(self) -> None:
        """Ensure a moved folder cannot empty every other check."""
        assert {rule.name for rule in _RULES} >= {"security.md"}

    @pytest.mark.parametrize("rule", _RULES, ids=lambda path: path.stem)
    def test_each_rule_opens_with_its_front_matter(self, rule: Path) -> None:
        """Ensure a rule says what it is, who wrote it, and what it governs, but never which model."""
        fields = _front_matter(rule)

        assert set(fields) == _FIELDS, f"{rule.name} differs in {sorted(set(fields) ^ _FIELDS)}."
        assert fields["type"] == "Rule"
        assert fields["status"] in {"draft", "stable", "deprecated"}
        assert "\n" not in str(fields["description"]).strip()
        generated = fields["generated"]
        assert isinstance(generated, dict)
        assert set(generated) == {"by", "at"}
        assert _PRODUCER.fullmatch(str(generated["by"])), (
            "The producer is named without a version or a model."
        )
        moment = generated["at"]
        parsed = moment if isinstance(moment, datetime) else datetime.fromisoformat(str(moment))
        assert parsed.tzinfo is not None, f"{moment} has no offset."

    @pytest.mark.parametrize("rule", _RULES, ids=lambda path: path.stem)
    def test_each_rule_applies_to_paths_that_exist(self, rule: Path) -> None:
        """Ensure a rule cannot keep pointing at code that moved."""
        missing = [
            path
            for path in _applies_to(rule)
            if not (_ROOT / path).exists() or (path.endswith("/") and not (_ROOT / path).is_dir())
        ]

        assert not missing, f"These applies_to paths of {rule.name} name nothing:\n" + "\n".join(
            missing
        )

    def test_the_index_lists_every_rule(self) -> None:
        """Ensure a new rule cannot ship without its line in the index."""
        index = (_ROOT / "rules" / "index.md").read_text(encoding="utf-8")

        assert set(_LINK.findall(index)) == {rule.name for rule in _RULES}, (
            "rules/index.md and rules/ differ."
        )

    def test_agents_md_names_the_paths_each_rule_applies_to(self) -> None:
        """Ensure the root table and the front matter give an agent the same scope."""
        rows = _table()

        assert set(rows) == {f"rules/{rule.name}" for rule in _RULES}, (
            "The rows and the rules differ."
        )
        for rule in _RULES:
            assert rows[f"rules/{rule.name}"] == set(_applies_to(rule)), (
                f"The row for {rule.name} and its applies_to disagree."
            )

    def test_no_agents_md_sits_below_the_root(self) -> None:
        """Ensure a rule cannot return beside the code, where one tool loads it and the rest miss it."""
        nested: list[str] = []
        for folder, subfolders, names in os.walk(_ROOT):
            subfolders[:] = sorted(name for name in subfolders if name not in _SKIPPED)
            if Path(folder) != _ROOT:
                nested.extend(
                    Path(folder, name).relative_to(_ROOT).as_posix()
                    for name in names
                    if name == "AGENTS.md"
                )

        assert not nested, "Move these into rules/ and link them from AGENTS.md:\n" + "\n".join(
            nested
        )
