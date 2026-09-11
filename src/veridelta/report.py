# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Standalone HTML reporting for comparison results.

Renders a `DiffResult` into one self-contained file: no CDN reference, no
build step, no runtime dependency. Veridelta runs in CI, and CI runners are
often air-gapped, where a report that fetches a stylesheet from the internet
renders as unstyled text at exactly the moment someone needs to read it.
"""

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import polars as pl

from veridelta.models import DiffResult

DEFAULT_MAX_ROWS: Final[int] = 1000
"""Rows embedded per table before truncation.

A diff of ten million rows would otherwise produce an HTML file nobody can
open. The report states when it has truncated, so a reader never mistakes a
capped table for the whole story.
"""

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
    """Escape a value for embedding in HTML text.

    Args:
        value (object): Value to render.

    Returns:
        str: HTML-safe text.
    """
    return html.escape(str(value), quote=True)


def _embed_json(payload: object) -> str:
    """Serialize a payload for a `<script type="application/json">` block.

    Args:
        payload (object): JSON-serializable data.

    Returns:
        str: JSON with `<` escaped, so a string in the data cannot close the
        script element and inject markup into the document.
    """
    return json.dumps(payload, default=str).replace("<", "\\u003c")


def _table(title: str, frame: pl.DataFrame, max_rows: int) -> str:
    """Render one paginated table section.

    Args:
        title (str): Section heading.
        frame (pl.DataFrame): Rows to embed.
        max_rows (int): Cap on embedded rows.

    Returns:
        str: HTML fragment. Rows travel as JSON and are drawn by the pager,
        so the document stays small even when the table is wide.
    """
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
    payload = _embed_json({"rows": [list(row) for row in shown.iter_rows()]})

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
    """Render one headline metric.

    Args:
        label (str): Metric name.
        value (object): Metric value, formatted by the caller.

    Returns:
        str: HTML fragment.
    """
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
    """
    summary = result.summary
    verdict = "PASSED" if summary.is_match else "FAILED"
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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

    keys_note = ""
    if result.keys_only:
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
            _table("Changed rows", result.changed, max_rows),
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
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_html(result, max_rows=max_rows), encoding="utf-8")
    return destination


__all__: Final[list[str]] = ["DEFAULT_MAX_ROWS", "render_html", "write_html"]
