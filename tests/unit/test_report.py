# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the standalone HTML report generator."""

import json
import re
from pathlib import Path

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.exceptions import ConfigError
from veridelta.models import DiffConfig, DiffResult, DiffSummary
from veridelta.report import render_html, render_markdown, write_html, write_markdown

pytestmark = [pytest.mark.unit, pytest.mark.fast]


def _result() -> DiffResult:
    """Run a comparison with drift in one column.

    Returns:
        DiffResult: Result with one added, one removed, and one changed row.
    """
    src = pl.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    tgt = pl.DataFrame({"id": [2, 3], "val": ["CHANGED", "C"]})
    return DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()


def _embedded_rows(document: str) -> list[list[object]]:
    """Decode every embedded table the way the page's `JSON.parse` would.

    Python's `json.loads` accepts `NaN` and `Infinity`, which `JSON.parse`
    rejects, so this decoder refuses them too.

    Args:
        document (str): Rendered HTML report.

    Returns:
        list[list[object]]: The rows of each embedded table, in page order.
    """

    def refuse(token: str) -> object:
        raise ValueError(f"JSON.parse rejects the token {token}")

    payloads = re.findall(r'<script type="application/json">(.*?)</script>', document, re.DOTALL)
    return [json.loads(payload, parse_constant=refuse)["rows"] for payload in payloads]


def _changed_only(changed: pl.DataFrame) -> DiffResult:
    """Wrap a changed-rows frame in an otherwise empty result.

    Args:
        changed (pl.DataFrame): Rows to report as changed.

    Returns:
        DiffResult: Result whose added and removed tables are empty.
    """
    return DiffResult(
        summary=_result().summary,
        added=pl.DataFrame(),
        removed=pl.DataFrame(),
        changed=changed,
    )


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

    def test_it_embeds_non_finite_floats_as_text(self) -> None:
        """Ensure NaN and infinities cannot stop the page from rendering.

        Python's `json` writes bare `NaN` and `Infinity`, which `JSON.parse`
        rejects. One such cell used to leave its table, and every table after
        it, empty. Values nested in list and struct columns are covered too.
        """
        changed = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "ratio": [float("nan"), float("inf"), float("-inf")],
                "samples": [[0.5, float("nan")], [], [1.0]],
                "pair": [{"a": float("inf")}, {"a": 1.0}, {"a": None}],
            }
        )

        (rows,) = _embedded_rows(render_html(_changed_only(changed)))

        assert rows == [
            [1, "nan", [0.5, "nan"], {"a": "inf"}],
            [2, "inf", [], {"a": 1.0}],
            [3, "-inf", [1.0], {"a": None}],
        ]

    def test_it_embeds_integers_beyond_javascript_precision_as_text(self) -> None:
        """Ensure a large identifier displays exactly rather than rounded.

        A JavaScript number holds integers exactly only up to 2**53 - 1, so
        two different keys past that could render as the same value.
        """
        big = 2**53 + 1
        changed = pl.DataFrame({"id": [big, -big, 7]})

        (rows,) = _embedded_rows(render_html(_changed_only(changed)))

        assert rows == [[str(big)], [str(-big)], [7]]

    def test_it_rejects_a_negative_row_cap(self) -> None:
        """Ensure a negative cap fails rather than embedding all but the last rows."""
        with pytest.raises(ConfigError, match="max_rows"):
            render_html(_result(), max_rows=-5)

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

    def test_it_shows_sampled_values_for_a_pushdown_run(self) -> None:
        """Ensure a fetched sample replaces the changed keys and says how much it shows."""
        summary = DiffSummary(
            total_rows_source=5,
            total_rows_target=5,
            added_count=0,
            removed_count=0,
            changed_count=3,
            is_match=False,
        )
        result = DiffResult(
            summary=summary,
            added=pl.DataFrame({"id": []}),
            removed=pl.DataFrame({"id": []}),
            changed=pl.DataFrame({"id": [2, 3, 4]}),
            primary_keys=("id",),
            compared_columns=("val",),
            keys_only=True,
            changed_sample=pl.DataFrame(
                {
                    "id": [2, 3],
                    "val_source": ["b", "c"],
                    "val_target": ["B", "C"],
                    "val_is_match": [False, False],
                }
            ),
        )

        document = render_html(result)

        assert "values for the first 2 of 3" in document
        assert "primary keys rather than values" not in document
        assert "<th>val_source</th><th>val_target</th>" in document
        assert _embedded_rows(document) == [[[2, "b", "B", False], [3, "c", "C", False]]]

    def test_it_handles_a_perfect_match(self) -> None:
        """Ensure empty frames render as an explicit statement, not a broken table."""
        frame = pl.DataFrame({"id": [1], "val": ["A"]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), frame.lazy(), frame.lazy()).run()

        document = render_html(result)

        assert "PASSED" in document
        assert "No rows." in document
        assert "No column-level drift." in document

    @pytest.mark.parametrize(
        ("column", "value", "raw", "escaped"),
        [
            # Column names reach the table header as text, so an unescaped name parses as markup.
            pytest.param(
                "<script>x</script>", "A", "<script>x</script>_source", "&lt;script&gt;", id="name"
            ),
            # The rows travel inside a script element, which an unescaped `</script>` ends early.
            pytest.param(
                "val", "</script><img onerror=x>", "</script><img", r"\u003c/script>", id="cell"
            ),
        ],
    )
    def test_it_escapes_markup_in_the_data(
        self, column: str, value: str, raw: str, escaped: str
    ) -> None:
        """Ensure a column name or a value cannot inject markup into the document."""
        src = pl.DataFrame({"id": [1], column: [value]})
        tgt = pl.DataFrame({"id": [1], column: ["B"]})
        result = DiffEngine(DiffConfig(primary_keys=["id"]), src.lazy(), tgt.lazy()).run()

        document = render_html(result)

        assert raw not in document
        assert escaped in document

    def test_it_writes_the_file_and_creates_parent_directories(self, tmp_path: Path) -> None:
        """Ensure a nested output path does not require pre-creating the tree."""
        destination = tmp_path / "reports" / "nested" / "diff.html"

        written = write_html(_result(), destination)

        assert written == destination
        assert destination.read_text(encoding="utf-8").startswith("<!DOCTYPE html>")


def _summary(**overrides: object) -> DiffSummary:
    """Build a summary with drift, overridable per field."""
    fields: dict[str, object] = {
        "total_rows_source": 1000,
        "total_rows_target": 1001,
        "added_count": 3,
        "removed_count": 2,
        "changed_count": 5,
        "column_mismatches": {"amount": 4, "status": 1},
        "is_match": False,
    }
    fields.update(overrides)
    return DiffSummary.model_validate(fields)


def _with_summary(summary: DiffSummary, *, keys_only: bool = False) -> DiffResult:
    """Wrap a summary in a result with empty row frames."""
    empty = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
    return DiffResult(
        summary=summary,
        added=empty,
        removed=empty,
        changed=empty,
        primary_keys=("id",),
        compared_columns=tuple(summary.column_mismatches),
        keys_only=keys_only,
    )


class TestMarkdownSummary:
    """Validate the Markdown summary CI posts to job summaries and pull requests."""

    def test_it_reports_the_verdict_and_every_count(self) -> None:
        """Ensure the summary carries what a reviewer needs without the HTML report."""
        document = render_markdown(_with_summary(_summary()))

        assert document.startswith("### Veridelta: FAILED\n")
        for row in (
            "| Match rate | 99.0% |",
            "| Source rows | 1,000 |",
            "| Target rows | 1,001 |",
            "| Volume shift | +1 |",
            "| Added | 3 |",
            "| Removed | 2 |",
            "| Changed | 5 |",
        ):
            assert row in document
        assert document.index("| `amount` | 4 |") < document.index("| `status` | 1 |")
        # Markdown setext underlines would turn the plain-text report into headings.
        assert "===" not in document

    def test_it_marks_a_perfect_match(self) -> None:
        """Ensure a clean run reads as clean, with no drift table."""
        summary = _summary(
            added_count=0, removed_count=0, changed_count=0, column_mismatches={}, is_match=True
        )

        document = render_markdown(_with_summary(summary))

        assert document.startswith("### Veridelta: PASSED (Perfect Match)\n")
        assert "No column-level drift." in document

    def test_it_caps_the_drift_table_at_the_report_limit(self) -> None:
        """Ensure a wide drift lists the top columns and says how many it left out."""
        mismatches = {f"col_{index}": index for index in range(1, 8)}
        summary = _summary(column_mismatches=mismatches, report_limit=3)

        document = render_markdown(_with_summary(summary))

        assert "| `col_7` | 7 |" in document
        assert "| `col_5` | 5 |" in document
        assert "col_4" not in document
        assert "Showing the top 3 of 7 columns with drift." in document

    def test_it_leaves_out_the_drift_section_when_the_limit_is_zero(self) -> None:
        """Ensure `report_top_columns_limit: 0` hides column names, as in the text report."""
        document = render_markdown(_with_summary(_summary(report_limit=0)))

        assert "amount" not in document
        assert "drift" not in document.lower()

    def test_it_says_when_artifacts_hold_primary_keys_only(self) -> None:
        """Ensure a pushdown run is not mistaken for one that extracted rows."""
        document = render_markdown(_with_summary(_summary(), keys_only=True))

        assert "primary keys only" in document

    @pytest.mark.parametrize(
        ("column", "cell"),
        [
            pytest.param("a|b", r"`a\|b`", id="pipe"),
            pytest.param("we`ird", "``we`ird``", id="backtick"),
            pytest.param("`edge`", "`` `edge` ``", id="edge-backticks"),
            pytest.param("<!-- veridelta:x -->", "`<!-- veridelta:x -->`", id="marker"),
            pytest.param("__bold__", "`__bold__`", id="emphasis"),
            pytest.param("line\nbreak", "`line break`", id="newline"),
        ],
    )
    def test_it_keeps_column_names_from_breaking_the_markdown(self, column: str, cell: str) -> None:
        """Ensure a column name renders as literal text inside its table cell.

        Column names come from the data, and the summary is posted to pull
        requests, so a name must never close the table, start a comment that
        could spoof the sticky-comment marker, or turn into formatting.
        """
        document = render_markdown(_with_summary(_summary(column_mismatches={column: 2})))

        assert f"| {cell} | 2 |" in document

    def test_it_writes_the_file_and_creates_parent_directories(self, tmp_path: Path) -> None:
        """Ensure a nested output path does not require pre-creating the tree."""
        destination = tmp_path / "reports" / "summary.md"

        written = write_markdown(_result(), destination, max_rows=5)

        assert written == destination
        assert destination.read_text(encoding="utf-8") == render_markdown(_result(), max_rows=5)


_CHANGED = pl.DataFrame(
    {
        "id": [2, 1],
        "status_source": ["open", "open"],
        "status_target": [None, "closed"],
        "status_is_match": [False, False],
        "amount_source": [5.0, 10.0],
        "amount_target": [5.0, 10.5],
        "amount_is_match": [True, False],
    }
)
"""Two changed rows, out of key order, holding three differing values."""


def _with_changes(changed: pl.DataFrame) -> DiffResult:
    """Wrap changed rows keyed by `id` in a local result whose counts match them."""
    columns = [name.removesuffix("_is_match") for name in changed.columns if "_is_match" in name]
    mismatches = {column: int((~changed[f"{column}_is_match"]).sum()) for column in columns}
    return DiffResult(
        summary=_summary(changed_count=changed.height, column_mismatches=mismatches),
        added=pl.DataFrame(),
        removed=pl.DataFrame(),
        changed=changed,
        primary_keys=("id",),
        compared_columns=tuple(columns),
    )


def _pushdown(sample: pl.DataFrame | None) -> DiffResult:
    """Wrap a pushdown run with three changed keys and, optionally, a sample of them."""
    return DiffResult(
        summary=_summary(changed_count=3, column_mismatches={"val": 3}),
        added=pl.DataFrame({"id": []}),
        removed=pl.DataFrame({"id": []}),
        changed=pl.DataFrame({"id": [2, 3, 4]}),
        primary_keys=("id",),
        compared_columns=("val",),
        keys_only=True,
        changed_sample=sample,
    )


class TestMarkdownValues:
    """Validate the changed values a Markdown summary lists on request."""

    def test_it_lists_no_values_by_default(self) -> None:
        """Ensure values reach a pull request only when someone asks for them."""
        document = render_markdown(_with_changes(_CHANGED))

        assert "Changed values" not in document
        assert "closed" not in document

    def test_it_rejects_a_negative_row_cap(self) -> None:
        """Ensure a negative cap fails loudly, as the HTML report's does."""
        with pytest.raises(ConfigError, match="max_rows must be zero or more"):
            render_markdown(_with_changes(_CHANGED), max_rows=-1)

    def test_it_lists_each_differing_value_in_key_order(self) -> None:
        """Ensure each differing value gets a row, lowest keys first, and nothing else does."""
        document = render_markdown(_with_changes(_CHANGED), max_rows=10)

        assert document.endswith(
            "#### Changed values\n"
            "\n"
            "| `id` | Column | Source | Target |\n"
            "| :--- | :--- | :--- | :--- |\n"
            "| `1` | `status` | `'open'` | `'closed'` |\n"
            "| `1` | `amount` | `10.0` | `10.5` |\n"
            "| `2` | `status` | `'open'` | _null_ |\n"
        )

    def test_it_stops_at_the_row_cap_and_says_so(self) -> None:
        """Ensure a capped list says how many values it left out."""
        document = render_markdown(_with_changes(_CHANGED), max_rows=2)

        assert "| `1` | `amount` | `10.0` | `10.5` |" in document
        assert "| `2` |" not in document
        assert document.endswith("\n\n_Showing 2 of 3 changed values._\n")

    def test_it_reads_the_rows_a_local_run_produces(self) -> None:
        """Ensure the summary reads a real run's changed rows."""
        document = render_markdown(_result(), max_rows=5)

        assert "| `2` | `val` | `'B'` | `'CHANGED'` |" in document

    @pytest.mark.parametrize(
        ("value", "cell"),
        [
            pytest.param("a|b", r"`'a\|b'`", id="pipe"),
            pytest.param("we`ird", "``'we`ird'``", id="backtick"),
            pytest.param("<!-- veridelta:x -->", "`'<!-- veridelta:x -->'`", id="marker"),
            pytest.param("@octocat", "`'@octocat'`", id="mention"),
            pytest.param("line\nbreak", r"`'line\nbreak'`", id="newline"),
            pytest.param("ACME ", "`'ACME '`", id="trailing-space"),
            pytest.param("", "`''`", id="empty"),
            pytest.param("x" * 100, "`'" + "x" * 59 + "...`", id="long"),
        ],
    )
    def test_it_keeps_values_from_breaking_the_markdown(self, value: str, cell: str) -> None:
        """Ensure a value renders as literal text, quoted so whitespace shows, and cut when long.

        The summary is posted to pull requests, so a value must never close the
        table, mention someone, or spoof the sticky-comment marker.
        """
        changed = pl.DataFrame(
            {"id": [1], "note_source": [value], "note_target": ["x"], "note_is_match": [False]}
        )

        document = render_markdown(_with_changes(changed), max_rows=1)

        assert f"| `1` | `note` | {cell} | `'x'` |" in document

    def test_it_stops_before_a_comment_grows_too_long(self) -> None:
        """Ensure the summary stays under GitHub's comment limit, counting multibyte text."""
        rows = 5_000
        changed = pl.DataFrame(
            {
                "id": list(range(rows)),
                "note_source": ["\u00e9" * 80] * rows,
                "note_target": ["e" * 80] * rows,
                "note_is_match": [False] * rows,
            }
        )

        document = render_markdown(_with_changes(changed), max_rows=rows)

        shown = document.count("`'\u00e9")
        assert 0 < shown < rows
        assert len(document.encode()) <= 60_000
        assert document.endswith(f"\n_Showing {shown:,} of 5,000 changed values._\n")

    def test_it_lists_a_pushdown_sample_and_counts_what_it_left_out(self) -> None:
        """Ensure a pushdown run lists its sample, and counts every value the warehouse found."""
        sample = pl.DataFrame(
            {
                "id": [3, 2],
                "val_source": ["c", "b"],
                "val_target": ["C", "B"],
                "val_is_match": [False, False],
            }
        )

        document = render_markdown(_pushdown(sample), max_rows=10)

        assert document.index("| `2` | `val` | `'b'` | `'B'` |") < document.index(
            "| `3` | `val` | `'c'` | `'C'` |"
        )
        assert document.endswith("\n_Showing 2 of 3 changed values._\n")

    def test_it_asks_for_a_sample_under_pushdown(self) -> None:
        """Ensure a pushdown run without a sample says how to get values."""
        document = render_markdown(_pushdown(None), max_rows=10)

        assert document.endswith(
            "#### Changed values\n\nSet `pushdown_sample_rows` to list values here.\n"
        )

    def test_it_leaves_out_the_section_when_nothing_changed(self) -> None:
        """Ensure a run with no changed rows adds no empty section."""
        summary = _summary(
            added_count=0, removed_count=0, changed_count=0, column_mismatches={}, is_match=True
        )

        assert "Changed values" not in render_markdown(_with_summary(summary), max_rows=10)
