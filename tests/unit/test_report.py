# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the standalone HTML report generator."""

import json
import re
from pathlib import Path

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffResult, DiffSummary
from veridelta.report import render_html, write_html


def _result() -> DiffResult:
    """Run a comparison with drift in one column.

    Returns:
        DiffResult: Result with one added, one removed, and one changed row.
    """
    src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})
    return DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()


@pytest.mark.unit
@pytest.mark.fast
class TestHTMLReport:
    """Validate the rendered document and its self-contained guarantee."""

    def test_it_renders_a_complete_document(self) -> None:
        """Ensure the output is a whole HTML file, not a fragment."""
        document = render_html(_result())

        assert document.startswith("<!DOCTYPE html>")
        assert document.rstrip().endswith("</html>")

    def test_it_references_nothing_external(self) -> None:
        """Ensure the report renders on an air-gapped CI runner.

        A CDN link degrades to unstyled text exactly when someone needs to
        read the report, so styles and script are inlined.
        """
        document = render_html(_result())

        assert "http://" not in document
        assert "https://github.com/Veridelta" in document or "https://" not in document
        assert not re.search(r"<link[^>]+href", document)
        assert not re.search(r"<script[^>]+src=", document)

    def test_it_reports_the_verdict_and_headline_metrics(self) -> None:
        """Ensure a reader sees the outcome without scrolling to a table."""
        document = render_html(_result())

        assert "FAILED" in document
        assert "Match rate" in document
        assert "Changed" in document

    def test_it_embeds_the_rows_as_json_for_the_pager(self) -> None:
        """Ensure table rows travel as data rather than as pre-rendered markup."""
        document = render_html(_result())

        payloads = re.findall(
            r'<script type="application/json">(.*?)</script>', document, re.DOTALL
        )

        assert payloads
        assert all("rows" in json.loads(payload) for payload in payloads)

    def test_it_caps_embedded_rows_and_says_so(self) -> None:
        """Ensure a large diff cannot produce an unopenable file.

        Without the cap, a ten-million-row comparison would embed every row.
        Truncating silently would be worse than not truncating at all, so the
        report states the count it is showing against the total.
        """
        src = pl.DataFrame({"id": list(range(50)), "val": ["A"] * 50})
        tgt = pl.DataFrame({"id": list(range(50)), "val": ["B"] * 50})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        document = render_html(result, max_rows=10)
        payload = json.loads(
            re.findall(r'<script type="application/json">(.*?)</script>', document, re.DOTALL)[0]
        )

        assert len(payload["rows"]) == 10
        assert "Showing the first 10 of 50 rows" in document

    def test_it_labels_a_pushdown_report_as_keys_only(self) -> None:
        """Ensure nobody reads a primary-key table as though it held values."""
        summary = DiffSummary(
            total_rows_source=2,
            total_rows_target=2,
            added_count=0,
            removed_count=0,
            changed_count=1,
            is_match=False,
        )
        result = DiffResult(
            summary=summary,
            added=pl.DataFrame({"id": []}),
            removed=pl.DataFrame({"id": []}),
            changed=pl.DataFrame({"id": [2]}),
            primary_keys=("id",),
            compared_columns=("val",),
            keys_only=True,
        )

        assert "primary keys rather than values" in render_html(result)

    def test_it_omits_the_keys_only_note_for_a_local_run(self) -> None:
        """Ensure the caveat appears only where it applies."""
        assert "primary keys rather than values" not in render_html(_result())

    def test_it_handles_a_perfect_match(self) -> None:
        """Ensure empty frames render as an explicit statement, not a broken table."""
        frame = pl.DataFrame({"id": [1], "val": ["A"]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), frame.lazy(), frame.lazy()).run()

        document = render_html(result)

        assert "PASSED" in document
        assert "No rows." in document
        assert "No column-level drift." in document

    def test_it_escapes_markup_in_the_data(self) -> None:
        """Ensure a value cannot break out of its cell into the document.

        Column names reach the table header as text, so a crafted name would
        otherwise be parsed as markup by the browser.
        """
        src = pl.DataFrame({"id": [1], "<script>x</script>": ["A"]})
        tgt = pl.DataFrame({"id": [1], "<script>x</script>": ["B"]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        document = render_html(result)

        assert "<script>x</script>_source" not in document
        assert "&lt;script&gt;" in document

    def test_it_keeps_embedded_data_from_closing_the_script_block(self) -> None:
        """Ensure a value containing a closing tag cannot inject markup.

        The rows travel inside a script element, so an unescaped `</script>`
        in the data would terminate it early and hand the remainder to the
        HTML parser.
        """
        src = pl.DataFrame({"id": [1], "val": ["</script><img onerror=x>"]})
        tgt = pl.DataFrame({"id": [1], "val": ["B"]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        document = render_html(result)

        assert "</script><img" not in document
        assert r"\u003c/script>" in document

    def test_it_writes_the_file_and_creates_parent_directories(self, tmp_path: Path) -> None:
        """Ensure a nested output path does not require pre-creating the tree."""
        destination = tmp_path / "reports" / "nested" / "diff.html"

        written = write_html(_result(), destination)

        assert written == destination
        assert destination.read_text(encoding="utf-8").startswith("<!DOCTYPE html>")
