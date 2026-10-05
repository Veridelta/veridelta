# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Keep the issue forms valid for GitHub.

GitHub leaves a malformed form out of the issue chooser without an error, so
these checks cover the keys its form schema requires.
"""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_TEMPLATES = Path(__file__).resolve().parents[2] / ".github" / "ISSUE_TEMPLATE"

_FIELD_TYPES = {"input", "textarea", "dropdown", "checkboxes"}
"""Form elements that collect an answer. A `markdown` element only shows text."""

_FIELD_ID = re.compile(r"^[A-Za-z0-9_-]+$")


def _forms() -> list[Path]:
    """Return every issue form, leaving out the chooser's own configuration."""
    return [path for path in sorted(_TEMPLATES.glob("*.yml")) if path.name != "config.yml"]


def _load(path: Path) -> Any:
    """Parse one YAML file under `.github/ISSUE_TEMPLATE`."""
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class TestIssueForms:
    """Validate each issue form against the rules GitHub enforces."""

    def test_it_offers_a_form_for_each_kind_of_report(self) -> None:
        """Ensure the chooser lists bugs, feature requests, and documentation."""
        assert [path.stem for path in _forms()] == [
            "bug_report",
            "documentation",
            "feature_request",
        ]

    @pytest.mark.parametrize("path", _forms(), ids=lambda path: path.name)
    def test_it_has_the_keys_github_requires(self, path: Path) -> None:
        """Ensure a form names itself, describes itself, and has a body."""
        form = _load(path)

        assert {"name", "description", "body"} <= set(form)
        assert form["body"], f"{path.name} has no elements."

    @pytest.mark.parametrize("path", _forms(), ids=lambda path: path.name)
    def test_it_labels_every_field_with_a_unique_id(self, path: Path) -> None:
        """Ensure each answer field has a label and an id GitHub accepts."""
        ids: list[str] = []
        for element in _load(path)["body"]:
            if element["type"] == "markdown":
                assert element["attributes"]["value"]
                continue
            assert element["type"] in _FIELD_TYPES
            assert element["attributes"]["label"]
            assert _FIELD_ID.match(element["id"]), element["id"]
            ids.append(element["id"])

        assert len(ids) == len(set(ids)), f"{path.name} repeats an id: {ids}"

    def test_it_links_questions_and_security_reports_elsewhere(self) -> None:
        """Ensure the chooser sends questions and vulnerabilities out of the tracker."""
        chooser = _load(_TEMPLATES / "config.yml")

        assert isinstance(chooser["blank_issues_enabled"], bool)
        assert [link["name"] for link in chooser["contact_links"]] == [
            "Question",
            "Security vulnerability",
        ]
        for link in chooser["contact_links"]:
            assert link["url"].startswith("https://github.com/Veridelta/veridelta/")
            assert link["about"]
