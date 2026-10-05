# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Standalone HTML reports and Markdown summaries for comparison results.

Renders a `DiffResult` into one self-contained file: no CDN reference, no
build step, no runtime dependency. Veridelta runs in CI, and CI runners are
often air-gapped, where a report that fetches a stylesheet from the internet
renders as unstyled text at exactly the moment someone needs to read it.

The Markdown summary is the short form CI posts to a job summary or a pull
request comment: the verdict, the counts, and the columns that drifted. It
lists changed values only when asked.
"""

import html
import json
import math
import re
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

import polars as pl

from veridelta.exceptions import ConfigError
from veridelta.models import DiffResult

DEFAULT_MAX_ROWS: Final[int] = 1000
"""Rows embedded per table before truncation.

A diff of ten million rows would otherwise produce an HTML file nobody can
open. The report states when it has truncated, so a reader never mistakes a
capped table for the whole story.
"""

_JS_SAFE_INTEGER: Final[int] = 2**53 - 1
"""Largest integer a JavaScript number holds exactly (`Number.MAX_SAFE_INTEGER`)."""

_STYLE: Final[str] = """
:root {
  --bg: #ffffff; --fg: #1b1f24; --muted: #5c6773; --line: #d8dee4;
  --pass: #1a7f37; --fail: #cf222e; --chip: #f2f4f7;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #30363d;
    --pass: #3fb950; --fail: #f85149; --chip: #161b22;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 2rem; background: var(--bg); color: var(--fg);
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}
h1 { font-size: 1.5rem; margin: 0 0 .25rem; }
h2 { font-size: 1.1rem; margin: 2rem 0 .75rem; }
.sub { color: var(--muted); margin: 0 0 1.5rem; font-size: .9rem; }
.verdict { font-weight: 600; }
.verdict.pass { color: var(--pass); }
.verdict.fail { color: var(--fail); }
.cards { display: flex; flex-wrap: wrap; gap: .75rem; margin-bottom: 1rem; }
.card {
  border: 1px solid var(--line); border-radius: 6px; padding: .75rem 1rem;
  min-width: 8.5rem; background: var(--chip);
}
.card .label { color: var(--muted); font-size: .8rem; text-transform: uppercase; }
.card .value { font-size: 1.35rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.note {
  border-left: 3px solid var(--line); padding: .5rem .9rem; margin: 1rem 0;
  color: var(--muted); font-size: .9rem;
}
table { border-collapse: collapse; width: 100%; font-size: .9rem; }
th, td {
  border-bottom: 1px solid var(--line); padding: .4rem .6rem;
  text-align: left; white-space: nowrap;
}
th { color: var(--muted); font-weight: 600; }
td.null { color: var(--muted); font-style: italic; }
.wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 6px; }
.pager { display: flex; align-items: center; gap: .5rem; margin-top: .6rem; }
.pager button {
  border: 1px solid var(--line); background: var(--chip); color: var(--fg);
  border-radius: 5px; padding: .3rem .7rem; cursor: pointer; font-size: .85rem;
}
.pager button:disabled { opacity: .45; cursor: default; }
.pager .status { color: var(--muted); font-size: .85rem; }
.empty { color: var(--muted); padding: .75rem; }
"""

_SCRIPT: Final[str] = """
const PAGE = 25;
function render(root) {
  const data = JSON.parse(root.querySelector('script[type="application/json"]').textContent);
  const body = root.querySelector('tbody');
  const status = root.querySelector('.status');
  const [prev, next] = root.querySelectorAll('button');
  const pages = Math.max(1, Math.ceil(data.rows.length / PAGE));
  let page = 0;
  function draw() {
    const slice = data.rows.slice(page * PAGE, page * PAGE + PAGE);
    body.replaceChildren(...slice.map(row => {
      const tr = document.createElement('tr');
      row.forEach(cell => {
        const td = document.createElement('td');
        if (cell === null) { td.textContent = 'null'; td.className = 'null'; }
        else { td.textContent = String(cell); }
        tr.appendChild(td);
      });
      return tr;
    }));
    status.textContent = `Page ${page + 1} of ${pages} \\u00b7 ${data.rows.length} rows`;
    prev.disabled = page === 0;
    next.disabled = page >= pages - 1;
  }
  prev.addEventListener('click', () => { if (page > 0) { page--; draw(); } });
  next.addEventListener('click', () => { if (page < pages - 1) { page++; draw(); } });
  draw();
}
document.querySelectorAll('[data-table]').forEach(render);
"""


def _escape(value: object) -> str:
    """Escape a value for embedding in HTML text."""
    return html.escape(str(value))


def _json_cell(value: object) -> object:
    """Make one cell safe for the page's `JSON.parse` without changing what it shows."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > _JS_SAFE_INTEGER:
        return str(value)
    if isinstance(value, list):
        return [_json_cell(item) for item in cast("list[object]", value)]
    if isinstance(value, dict):
        return {key: _json_cell(item) for key, item in cast("dict[str, object]", value).items()}
    return value


def _table(title: str, frame: pl.DataFrame, max_rows: int) -> str:
    """Render one paginated table section."""
    if frame.height == 0 or not frame.columns:
        return f"<h2>{_escape(title)}</h2>\n<p class='empty'>No rows.</p>"

    shown = frame.head(max_rows)
    truncated = ""
    if frame.height > max_rows:
        truncated = (
            f"<p class='note'>Showing the first {max_rows:,} of {frame.height:,} rows. "
            "Export artifacts with <code>output_path</code> for the complete set.</p>"
        )

    header = "".join(f"<th>{_escape(name)}</th>" for name in shown.columns)
    rows = [[_json_cell(cell) for cell in row] for row in shown.iter_rows()]
    # `<` is escaped so no string can close the script element and inject markup.
    # A non-finite float raises rather than producing JSON the browser cannot parse.
    payload = json.dumps({"rows": rows}, default=str, allow_nan=False).replace("<", "\\u003c")

    return (
        f"<h2>{_escape(title)}</h2>\n{truncated}"
        f"<div data-table>\n"
        f'<script type="application/json">{payload}</script>\n'
        f"<div class='wrap'><table><thead><tr>{header}</tr></thead><tbody></tbody></table></div>\n"
        "<div class='pager'><button type='button'>Previous</button>"
        "<button type='button'>Next</button><span class='status'></span></div>\n"
        "</div>"
    )


def _card(label: str, value: object) -> str:
    """Render one headline metric."""
    return (
        f"<div class='card'><div class='label'>{_escape(label)}</div>"
        f"<div class='value'>{_escape(value)}</div></div>"
    )


def render_html(result: DiffResult, *, max_rows: int = DEFAULT_MAX_ROWS) -> str:
    """Render a comparison result as a standalone HTML document.

    Args:
        result (DiffResult): Completed comparison.
        max_rows (int): Rows to embed per table before truncating.

    Returns:
        str: A complete HTML document with no external references.

    Raises:
        ConfigError: If `max_rows` is negative.
    """
    if max_rows < 0:
        raise ConfigError(f"max_rows must be zero or more, got {max_rows}.")
    summary = result.summary
    verdict = "PASSED" if summary.is_match else "FAILED"
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    cards = "".join(
        [
            _card("Match rate", f"{summary.match_rate_percentage}%"),
            _card("Source rows", f"{summary.total_rows_source:,}"),
            _card("Target rows", f"{summary.total_rows_target:,}"),
            _card("Added", f"{summary.added_count:,}"),
            _card("Removed", f"{summary.removed_count:,}"),
            _card("Changed", f"{summary.changed_count:,}"),
        ]
    )

    changed = result.changed
    keys_note = ""
    if result.keys_only and result.changed_sample is not None:
        changed = result.changed_sample
        keys_note = (
            "<p class='note'>This comparison ran as warehouse pushdown, which "
            f"evaluates in place. Changed rows show values for the first {changed.height:,} "
            f"of {summary.changed_count:,}, in key order, fetched because "
            "<code>pushdown_sample_rows</code> is set. Added and removed rows list "
            "primary keys.</p>"
        )
    elif result.keys_only:
        keys_note = (
            "<p class='note'>This comparison ran as warehouse pushdown, which "
            "evaluates in place and never extracts rows. The tables below list "
            "primary keys rather than values.</p>"
        )

    drift = "<p class='empty'>No column-level drift.</p>"
    if summary.column_mismatches:
        ranked = sorted(summary.column_mismatches.items(), key=lambda item: -item[1])
        rows = "".join(
            f"<tr><td>{_escape(col)}</td><td>{count:,}</td></tr>" for col, count in ranked
        )
        drift = (
            "<div class='wrap'><table><thead><tr><th>Column</th>"
            f"<th>Mismatches</th></tr></thead><tbody>{rows}</tbody></table></div>"
        )

    tables = "\n".join(
        [
            _table("Changed rows", changed, max_rows),
            _table("Added rows", result.added, max_rows),
            _table("Removed rows", result.removed, max_rows),
        ]
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Veridelta Report &mdash; {verdict}</title>
<style>{_STYLE}</style>
</head>
<body>
<h1>Veridelta Report</h1>
<p class="sub">
  <span class="verdict {verdict.lower()}">{verdict}</span> &middot; generated {generated}
</p>
{keys_note}
<div class="cards">{cards}</div>
<h2>Column-level drift</h2>
{drift}
{tables}
<script>{_SCRIPT}</script>
</body>
</html>
"""


def write_html(result: DiffResult, path: str | Path, *, max_rows: int = DEFAULT_MAX_ROWS) -> Path:
    """Write a standalone HTML report to disk.

    Args:
        result (DiffResult): Completed comparison.
        path (str | Path): Destination file. Parent directories are created.
        max_rows (int): Rows to embed per table before truncating.

    Returns:
        Path: The file that was written.

    Raises:
        ConfigError: If `max_rows` is negative.

    Examples:
        >>> import polars as pl
        >>> from veridelta.engine import DiffEngine
        >>> from veridelta.models import DiffConfig
        >>> source = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 20.0]})
        >>> target = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 21.5]})
        >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
        >>> render_html(result).startswith("<!DOCTYPE html>")
        True
        >>> path = write_html(result, "reports/orders.html")  # doctest: +SKIP
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_html(result, max_rows=max_rows), encoding="utf-8")
    return destination


_BACKTICK_RUN = re.compile(r"`+")
"""A run of backticks, whose length sets the fence of a Markdown code span."""

_MARKDOWN_BUDGET: Final[int] = 60_000
"""UTF-8 bytes a Markdown summary may reach while it lists changed values.

GitHub refuses a comment over 65,536 characters, and the Action adds a marker line.
"""

_MARKDOWN_VALUE_WIDTH: Final[int] = 60
"""Characters of a key or value the Markdown summary shows before cutting it."""


def _markdown_code(name: str) -> str:
    """Render a column name or a value as literal text inside a Markdown table cell."""
    # Names and values come from the data, and a code span keeps one from opening an HTML
    # comment that can spoof the sticky-comment marker.
    flat = " ".join(name.splitlines())
    longest = max((len(run) for run in _BACKTICK_RUN.findall(flat)), default=0)
    fence = "`" * (longest + 1)
    pad = " " if flat.startswith("`") or flat.endswith("`") else ""
    return f"{fence}{pad}{flat}{pad}{fence}".replace("|", "\\|")


def render_markdown(result: DiffResult, *, max_rows: int = 0) -> str:
    """Render a comparison result as a short Markdown summary.

    Args:
        result (DiffResult): Completed comparison.
        max_rows (int): Changed values to list, lowest keys first. 0, the
            default, lists none, since CI posts the summary where more people
            may read it than may read the data.

    Returns:
        str: The verdict, a table of counts, and the top drifting columns,
            limited to the configured `report_top_columns_limit`, then any
            changed values asked for.

    Raises:
        ConfigError: If `max_rows` is negative.
    """
    if max_rows < 0:
        raise ConfigError(f"max_rows must be zero or more, got {max_rows}.")
    summary = result.summary
    verdict = "PASSED" if summary.is_match else "FAILED"
    perfect = " (Perfect Match)" if summary.is_perfect_match else ""
    lines = [
        f"### Veridelta: {verdict}{perfect}",
        "",
        "| Metric | Value |",
        "| :--- | ---: |",
        f"| Match rate | {summary.match_rate_percentage}% |",
        f"| Source rows | {summary.total_rows_source:,} |",
        f"| Target rows | {summary.total_rows_target:,} |",
        f"| Volume shift | {summary.volume_shift:+,} |",
        f"| Added | {summary.added_count:,} |",
        f"| Removed | {summary.removed_count:,} |",
        f"| Changed | {summary.changed_count:,} |",
    ]
    if result.keys_only:
        lines += [
            "",
            "> Warehouse pushdown compared these tables in place, so its artifacts list "
            "primary keys only.",
        ]
    if summary.report_limit > 0:
        lines += ["", "#### Column-level drift", ""]
        lines += _drift_lines(summary.column_mismatches, summary.report_limit)
    if max_rows > 0 and summary.changed_count > 0:
        lines += ["", "#### Changed values", ""]
        used = len("\n".join(lines).encode()) + 1
        lines += _value_lines(result, max_rows, _MARKDOWN_BUDGET - used)
    return "\n".join(lines) + "\n"


def _drift_lines(mismatches: dict[str, int], limit: int) -> list[str]:
    """Render the top drifting columns as a Markdown table."""
    if not mismatches:
        return ["No column-level drift."]
    ranked = sorted(mismatches.items(), key=lambda item: -item[1])
    lines = ["| Column | Mismatches |", "| :--- | ---: |"]
    lines += [f"| {_markdown_code(column)} | {count:,} |" for column, count in ranked[:limit]]
    if len(ranked) > limit:
        lines += ["", f"_Showing the top {limit} of {len(ranked)} columns with drift._"]
    return lines


def _markdown_row(cells: list[str]) -> str:
    """Join rendered cells into one Markdown table row."""
    return "| " + " | ".join(cells) + " |"


def _markdown_value(value: object) -> str:
    """Render one key or value as literal text, quoting text so its whitespace shows."""
    if value is None:
        return "_null_"
    text = repr(value) if isinstance(value, str) else str(value)
    if len(text) > _MARKDOWN_VALUE_WIDTH:
        text = text[:_MARKDOWN_VALUE_WIDTH] + "..."
    return _markdown_code(text)


def _differing_values(
    rows: pl.DataFrame, keys: list[str], columns: tuple[str, ...]
) -> Iterator[list[str]]:
    """Yield the rendered cells of each differing value, row by row."""
    for record in rows.iter_rows(named=True):
        for column in columns:
            # A NULL flag is not a mismatch, as the column counts treat it.
            if record[f"{column}_is_match"] is False:
                yield [
                    *(_markdown_value(record[key]) for key in keys),
                    _markdown_code(column),
                    _markdown_value(record[f"{column}_source"]),
                    _markdown_value(record[f"{column}_target"]),
                ]


def _value_lines(result: DiffResult, max_rows: int, budget: int) -> list[str]:
    """List changed values as a Markdown table, within `max_rows` rows and `budget` bytes."""
    changed = result.changed_sample if result.keys_only else result.changed
    if changed is None:
        return ["Set `pushdown_sample_rows` to list values here."]
    keys = list(result.primary_keys)
    lines = [
        _markdown_row([*map(_markdown_code, keys), "Column", "Source", "Target"]),
        _markdown_row([":---"] * (len(keys) + 3)),
    ]
    total = sum(result.summary.column_mismatches.values())
    closing = f"_Showing {total:,} of {total:,} changed values._"
    budget -= sum(len(line.encode()) + 1 for line in lines) + len(closing) + 2
    shown = 0
    rows = changed.lazy().sort(keys).head(max_rows).collect()
    for cells in _differing_values(rows, keys, result.compared_columns):
        line = _markdown_row(cells)
        budget -= len(line.encode()) + 1
        if shown == max_rows or budget < 0:
            break
        lines.append(line)
        shown += 1
    if shown < total:
        lines += ["", f"_Showing {shown:,} of {total:,} changed values._"]
    return lines


def write_markdown(result: DiffResult, path: str | Path, *, max_rows: int = 0) -> Path:
    """Write the Markdown summary to disk.

    Args:
        result (DiffResult): Completed comparison.
        path (str | Path): Destination file. Parent directories are created.
        max_rows (int): Changed values to list, as for `render_markdown`.

    Returns:
        Path: The file that was written.

    Raises:
        ConfigError: If `max_rows` is negative.

    Examples:
        >>> import polars as pl
        >>> from veridelta.engine import DiffEngine
        >>> from veridelta.models import DiffConfig
        >>> source = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 20.0]})
        >>> target = pl.LazyFrame({"id": [1, 2], "amount": [10.0, 21.5]})
        >>> result = DiffEngine(DiffConfig(primary_keys=["id"]), source, target).run()
        >>> print(render_markdown(result).splitlines()[0])
        ### Veridelta: FAILED
        >>> path = write_markdown(result, "summary.md")  # doctest: +SKIP
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_markdown(result, max_rows=max_rows), encoding="utf-8")
    return destination


__all__: Final[list[str]] = [
    "DEFAULT_MAX_ROWS",
    "render_html",
    "render_markdown",
    "write_html",
    "write_markdown",
]
