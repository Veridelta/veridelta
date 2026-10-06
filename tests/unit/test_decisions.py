# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the decision records in their light, fixed shape.

Each file under `decisions/` records one settled choice, as the
`defend-decision` skill writes it. These tests check its front matter and its
five labels, and that the settled list in `AGENTS.md` names every record. They
read no quotation and keep no index, so a record stays cheap to write.
"""

import re
from datetime import date
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_RECORDS = sorted((_ROOT / "decisions").glob("*.md"))
_FRONT_MATTER = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)
_FIELDS = {"type", "title", "description", "status", "decided"}
_LABELS = ["Claim", "Evidence", "Alternative considered", "Why rejected", "How to reverse"]


def _front_matter(record: Path) -> dict[str, object]:
    """Return a record's front matter as YAML reads it."""
    match = _FRONT_MATTER.match(record.read_text(encoding="utf-8"))
    assert match is not None, f"{record.name} opens with no front matter."
    loaded: dict[str, object] = yaml.safe_load(match.group(1))
    return loaded


class TestDecisionRecords:
    """Hold each record to the shape the `defend-decision` skill writes."""

    def test_it_finds_the_records(self) -> None:
        """Ensure a moved folder cannot empty every other check."""
        assert _RECORDS

    @pytest.mark.parametrize("record", _RECORDS, ids=lambda path: path.stem)
    def test_each_record_has_its_front_matter(self, record: Path) -> None:
        """Ensure a record says what it is, when it was decided, and nothing about who wrote it."""
        fields = _front_matter(record)

        assert set(fields) == _FIELDS
        assert fields["type"] == "Decision"
        assert fields["status"] in {"draft", "stable", "deprecated"}
        assert isinstance(fields["decided"], date)
        assert "\n" not in str(fields["description"]).strip()

    @pytest.mark.parametrize("record", _RECORDS, ids=lambda path: path.stem)
    def test_each_record_holds_the_five_labels_in_order(self, record: Path) -> None:
        """Ensure each record answers the same five questions, in the same order."""
        body = _FRONT_MATTER.sub("", record.read_text(encoding="utf-8"))

        assert re.findall(r"^\*\*(.+?):\*\*", body, re.MULTILINE) == _LABELS

    def test_agents_md_settles_every_record(self) -> None:
        """Ensure the settled list and the folder name the same records."""
        settled = (_ROOT / "AGENTS.md").read_text(encoding="utf-8").split("## Settled", 1)[1]
        linked = set(re.findall(r"\]\((decisions/[^)#]+\.md)\)", settled))

        assert linked == {record.relative_to(_ROOT).as_posix() for record in _RECORDS}
