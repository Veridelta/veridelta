# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Standalone HTML reports and Markdown summaries for comparison results.

Renders a `DiffResult` into one self-contained file: no CDN reference, no
build step, no runtime dependency. Veridelta runs in CI, and CI runners are
often air-gapped, where a report that fetches a stylesheet from the internet
renders as unstyled text at exactly the moment someone needs to read it.

The Markdown summary is the short form CI posts to a job summary or a pull
request comment: the verdict, the counts, and the columns that drifted. It
lists changed values only when asked, and ends with the same counts as JSON in
an HTML comment, which a reader never sees and a script can parse.
"""

import base64
import html
import json
import math
import re
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from importlib.resources import files
from itertools import islice
from pathlib import Path
from typing import Any, Final, NamedTuple

import polars as pl

from veridelta.exceptions import ConfigError
from veridelta.models import ColumnRules, DiffResult, DiffSummary, HashedFile, RunProvenance
from veridelta.outputs import output_schema_url

_NUMBERS: Final = (int, float, Decimal)
"""The value types whose difference is target minus source, as a decimal."""

DEFAULT_MAX_ROWS: Final[int] = 1000
"""Rows embedded per table before truncation.

A diff of ten million rows would otherwise produce an HTML file nobody can
open. The report states when it has truncated, so a reader never mistakes a
capped table for the whole story.
"""

_PAGE_SIZE: Final[int] = 25
"""Rows the report's script shows at a time in each table.

Every row is in the markup, so a reader without JavaScript sees them all.
"""

_SYMBOL: Final[str] = base64.b64encode(
    files("veridelta").joinpath("symbol.png").read_bytes()
).decode()
"""The Veridelta symbol, embedded in the page as a data URI, so the file stays standalone.

`symbol.png` is `docs/assets/veridelta-symbol.png` resized to 64 pixels tall,
twice the height it shows at, which keeps the page under 10 KB larger. An
inline SVG replaces it once the brand has one.
"""

_DARK_TOKENS: Final[str] = """
    color-scheme: dark;
    --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #30363d;
    --pass: #3fb950; --fail: #f85149; --chip: #161b22; --link: #4493f8;
    --matched: #8b949e; --removed: #e6edf3; --added: #f85149;
"""
"""The colors of the dark theme, which the system's preference or the theme control selects."""

_STYLE: Final[str] = f"""
:root {{
  color-scheme: light;
  --bg: #ffffff; --fg: #1b1f24; --muted: #5c6773; --line: #d8dee4;
  --pass: #1a7f37; --fail: #cf222e; --chip: #f2f4f7; --link: #0969da;
  --matched: #6e7781; --removed: #1b1f24; --added: #cf222e;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{{_DARK_TOKENS}  }}
}}
:root[data-theme="dark"] {{{_DARK_TOKENS}}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; padding: 2rem; background: var(--bg); color: var(--fg);
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}}
h1 {{ font-size: 1.35rem; margin: 0; }}
.brand {{ display: flex; align-items: center; gap: .75rem; }}
.symbol {{ height: 2rem; width: auto; flex: none; }}
a {{ color: var(--link); }}
h2 {{ font-size: 1.1rem; margin: 2rem 0 .5rem; }}
h3 {{ font-size: 1rem; margin: 1.25rem 0 .25rem; }}
code {{ font-size: .9em; }}
.top {{
  display: flex; flex-wrap: wrap; justify-content: space-between; align-items: flex-end;
  gap: .5rem 1.5rem; padding-bottom: .75rem; border-bottom: 1px solid var(--line);
}}
.sub {{ color: var(--muted); margin: 0; font-size: .9rem; overflow-wrap: anywhere; }}
.theme {{ font-size: .85rem; color: var(--muted); }}
.theme select {{
  font: inherit; color: var(--fg); background: var(--bg); border: 1px solid var(--line);
  border-radius: 5px; padding: .2rem .4rem; margin-left: .3rem;
}}
.banner {{
  display: flex; align-items: flex-start; gap: 1rem; margin: 1.25rem 0 0;
  border: 2px solid var(--line); border-radius: 8px; padding: .9rem 1.2rem;
}}
.banner.pass {{ border-color: var(--pass); }}
.banner.fail {{ border-color: var(--fail); }}
.banner > div {{ flex: 1; min-width: 0; }}
.banner p {{ margin: 0; }}
.glyph {{ font-size: 2rem; font-weight: 700; line-height: 1; }}
.banner.pass .glyph {{ color: var(--pass); }}
.banner.fail .glyph {{ color: var(--fail); }}
.verdict {{ font-size: 1.6rem; font-weight: 700; letter-spacing: .02em; line-height: 1.2; }}
.verdict.pass {{ color: var(--pass); }}
.verdict.fail {{ color: var(--fail); }}
.headline {{ font-size: 1.05rem; }}
.meter {{ display: block; width: 100%; max-width: 32rem; height: 8px; margin-top: .6rem; }}
.meter .track {{ fill: var(--line); }}
.banner.pass .meter .filled {{ fill: var(--pass); }}
.banner.fail .meter .filled {{ fill: var(--fail); }}
.meter .tick {{ fill: var(--fg); }}
.hint {{ color: var(--muted); font-size: .85rem; margin: 0 0 .6rem; }}
.wrap + .hint {{ margin-top: .4rem; }}
.ledger {{ display: block; width: 100%; height: 18px; border-radius: 4px; margin: .25rem 0 .5rem; }}
.ledger .matched {{ fill: var(--matched); }}
.ledger .changed {{ fill: var(--fail); }}
.ledger .removed {{ fill: var(--removed); }}
.ledger .accepted {{ fill: var(--line); }}
.ledger .added {{ fill: var(--added); fill-opacity: .35; }}
.legend {{ display: flex; flex-wrap: wrap; gap: .4rem 1.5rem; margin: 0; padding: 0; list-style: none; }}
.legend li {{ display: flex; align-items: center; gap: .45rem; }}
.legend .count {{ font-weight: 600; font-variant-numeric: tabular-nums; }}
.legend .absent {{ color: var(--muted); }}
.swatch {{ width: 12px; height: 12px; border-radius: 2px; flex: none; }}
.swatch.matched {{ background: var(--matched); }}
.swatch.changed {{ background: var(--fail); }}
.swatch.removed {{ background: var(--removed); }}
.swatch.accepted {{ background: var(--line); }}
.swatch.added {{ background: var(--added); opacity: .35; }}
.bar {{ display: inline-block; width: 6rem; height: 6px; margin: 0 .6rem 0 0; vertical-align: middle; }}
.bar .track {{ fill: var(--line); }}
.bar .filled {{ fill: var(--muted); }}
td.share {{ min-width: 9rem; }}
tr.quiet td {{ color: var(--muted); }}
[role="tablist"] {{
  display: flex; flex-wrap: wrap; gap: .25rem; margin-top: 2rem;
  border-bottom: 1px solid var(--line);
}}
[role="tab"] {{
  border: 0; border-bottom: 3px solid transparent; background: none; color: var(--muted);
  padding: .45rem .9rem; font: inherit; font-size: .95rem; cursor: pointer; margin-bottom: -1px;
}}
[role="tab"][aria-selected="true"] {{ color: var(--fg); font-weight: 600; border-bottom-color: var(--link); }}
[role="tabpanel"] > h2 {{ margin-top: 1rem; }}
.sr-only {{
  position: absolute; width: 1px; height: 1px; margin: -1px; padding: 0; overflow: hidden;
  clip: rect(0 0 0 0); white-space: nowrap; border: 0;
}}
.record {{
  margin: 2rem 0 0; padding: .9rem 1.2rem; border: 1px dashed var(--line); border-radius: 6px;
}}
.record h2 {{ margin-top: 0; }}
.record dl {{
  display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: .3rem 1rem;
  margin: 0; font-size: .9rem;
}}
.record dt {{ color: var(--muted); }}
.record dd {{ margin: 0; min-width: 0; overflow-wrap: anywhere; }}
.record .absent {{ color: var(--muted); }}
.note {{
  border-left: 3px solid var(--line); padding: .5rem .9rem; margin: 1rem 0;
  color: var(--muted); font-size: .9rem;
}}
table {{ border-collapse: collapse; width: 100%; font-size: .9rem; }}
th, td {{
  border-bottom: 1px solid var(--line); padding: .4rem .6rem;
  text-align: left; white-space: nowrap;
}}
th {{ color: var(--muted); font-weight: 600; }}
td.null {{ color: var(--muted); font-style: italic; }}
.wrap {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 6px; }}
.pager {{ display: flex; align-items: center; gap: .5rem; margin-top: .6rem; }}
.pager[hidden] {{ display: none; }}
.pager button {{
  border: 1px solid var(--line); background: var(--chip); color: var(--fg);
  border-radius: 5px; padding: .3rem .7rem; cursor: pointer; font-size: .85rem;
}}
.pager button[aria-disabled="true"] {{ opacity: .45; cursor: default; }}
.pager .status {{ color: var(--muted); font-size: .85rem; }}
.empty {{ color: var(--muted); padding: .75rem; }}
td.differs {{ font-weight: 600; background: var(--chip); }}
td.differs .sign {{ color: var(--fail); margin-right: .15rem; }}
td.differs .was {{ font-weight: 400; color: var(--muted); margin-right: .35rem; }}
td.forgiven .sign {{ color: var(--muted); margin: 0 .15rem; }}
td.difference {{ font-variant-numeric: tabular-nums; }}
.filters {{ display: flex; flex-wrap: wrap; align-items: center; gap: .5rem 1rem; margin: 0 0 .5rem; }}
.filters label {{ font-size: .85rem; color: var(--muted); }}
.filters select, .filters input, .filters button {{
  font: inherit; font-size: .85rem; color: var(--fg); background: var(--bg);
  border: 1px solid var(--line); border-radius: 5px; padding: .25rem .5rem;
}}
.filters button {{ background: var(--chip); cursor: pointer; }}
.filter-status {{ color: var(--muted); font-size: .85rem; margin: 0 0 .5rem; }}
details.full-rows {{ margin-top: 1.25rem; }}
ul.patterns {{ margin: 0 0 .5rem; padding-left: 1.2rem; font-size: .9rem; }}
ul.patterns li {{ margin: .3rem 0; }}
ul.patterns.chips {{ display: flex; flex-wrap: wrap; gap: .4rem; padding: 0; list-style: none; }}
ul.patterns.chips li {{ margin: 0; }}
.chip {{
  font: inherit; font-size: .85rem; color: var(--fg); background: var(--bg); cursor: pointer;
  border: 1px solid var(--line); border-radius: 999px; padding: .2rem .7rem;
}}
.chip[aria-pressed="true"] {{ background: var(--fg); color: var(--bg); border-color: var(--fg); }}
details.full-rows summary {{ cursor: pointer; color: var(--link); margin-bottom: .6rem; }}
@media (max-width: 40rem) {{
  body {{ padding: 1rem; }}
  .record dl {{ grid-template-columns: minmax(0, 1fr); }}
  .record dd {{ margin-bottom: .4rem; }}
  .verdict {{ font-size: 1.35rem; }}
}}
"""

_SCRIPT: Final[str] = """
// A theme control in the header: the system's preference, light, or dark. The
// choice is kept in this browser, and without the script the page follows the system.
function theme() {
  const key = 'veridelta-theme';
  let chosen = '';
  try { chosen = localStorage.getItem(key) || ''; } catch (error) { /* Storage may be off. */ }
  const label = document.createElement('label');
  label.className = 'theme';
  label.textContent = 'Theme';
  const select = document.createElement('select');
  [['', 'System'], ['light', 'Light'], ['dark', 'Dark']].forEach(([value, name]) => {
    select.append(new Option(name, value, false, value === chosen));
  });
  function apply() {
    if (select.value) document.documentElement.dataset.theme = select.value;
    else delete document.documentElement.dataset.theme;
    try {
      if (select.value) localStorage.setItem(key, select.value);
      else localStorage.removeItem(key);
    } catch (error) { /* The choice lasts for this page alone. */ }
  }
  select.addEventListener('change', apply);
  label.append(select);
  document.querySelector('.top').append(label);
  apply();
}
theme();

// Pages a table's rows, counting only the rows its filters keep. A table of
// one page has no pager, but its rows still follow the filters.
function paginate(root) {
  const pager = root.querySelector('.pager');
  const size = Number(root.dataset.pageSize);
  const unit = root.dataset.unit;
  const rows = Array.from(root.querySelectorAll('tbody tr'));
  let page = 0;
  function draw() {
    const kept = rows.filter(row => !row.dataset.filteredOut);
    const pages = Math.max(1, Math.ceil(kept.length / size));
    page = Math.min(page, pages - 1);
    rows.forEach(row => { row.hidden = true; });
    kept.forEach((row, index) => { row.hidden = Math.floor(index / size) !== page; });
    if (!pager) return;
    const [prev, next] = pager.querySelectorAll('button');
    const status = pager.querySelector('.status');
    // The status is a live region, so its text changes only with the page.
    const text = `Page ${page + 1} of ${pages} \\u00b7 ${kept.length.toLocaleString('en-US')} ${unit}`;
    if (status.textContent !== text) status.textContent = text;
    // aria-disabled keeps a button focusable, so focus stays put on the last page.
    prev.setAttribute('aria-disabled', String(page === 0));
    next.setAttribute('aria-disabled', String(page === pages - 1));
  }
  root.redraw = () => { page = 0; draw(); };
  if (pager) {
    const [prev, next] = pager.querySelectorAll('button');
    prev.addEventListener('click', () => { if (page > 0) { page--; draw(); } });
    next.addEventListener('click', () => { page++; draw(); });
    pager.hidden = false;
  }
  draw();
}
document.querySelectorAll('[data-table]').forEach(paginate);

// Filters for the mismatched values: a column, text in the key, and a pattern,
// which becomes a pressed chip. The state lives in the address's fragment, so
// a filtered view can be linked.
function filters(root) {
  const rows = Array.from(root.querySelectorAll('tbody tr'));
  const columns = [...new Set(rows.map(row => row.dataset.column))].sort();
  const group = document.createElement('div');
  group.className = 'filters';
  group.setAttribute('role', 'group');
  group.setAttribute('aria-label', 'Filter mismatched values');
  const columnLabel = document.createElement('label');
  columnLabel.textContent = 'Column ';
  const column = document.createElement('select');
  column.id = 'filter-column';
  column.append(new Option('All columns', ''));
  columns.forEach(name => column.append(new Option(name, name)));
  columnLabel.append(column);
  const keyLabel = document.createElement('label');
  keyLabel.textContent = 'Key contains ';
  const key = document.createElement('input');
  key.type = 'search';
  key.id = 'filter-key';
  keyLabel.append(key);
  const clear = document.createElement('button');
  clear.type = 'button';
  clear.textContent = 'Clear filters';
  const status = document.createElement('p');
  status.className = 'filter-status';
  status.setAttribute('role', 'status');
  group.append(columnLabel, keyLabel, clear);
  root.before(group, status);
  const all = rows.length.toLocaleString('en-US');
  // A pattern narrows the values to one new value or one difference in a column.
  let pattern = {column: '', target: '', difference: ''};
  const panel = root.closest('section') || document;
  const chips = Array.from(panel.querySelectorAll('li.pattern')).map(item => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'chip';
    chip.textContent = `${item.dataset.label} \\u00b7 ${item.dataset.count} values`;
    chip.setAttribute('aria-pressed', 'false');
    item.textContent = '';
    item.append(chip);
    return [chip, item.dataset];
  });
  panel.querySelectorAll('ul.patterns').forEach(list => list.classList.add('chips'));
  const samePattern = data => data.column === pattern.column
    && (data.target || '') === pattern.target && (data.difference || '') === pattern.difference;
  function apply() {
    const wanted = key.value.trim().toLowerCase();
    let kept = 0;
    rows.forEach(row => {
      const keep = (!column.value || row.dataset.column === column.value)
        && (!wanted || row.dataset.key.toLowerCase().includes(wanted))
        && (!pattern.target || row.dataset.target === pattern.target)
        && (!pattern.difference || row.dataset.difference === pattern.difference);
      if (keep) { delete row.dataset.filteredOut; kept++; } else { row.dataset.filteredOut = '1'; }
    });
    const scope = pattern.target || pattern.difference ? ', in one pattern' : '';
    status.textContent = `Showing ${kept.toLocaleString('en-US')} of ${all} mismatched values${scope}`;
    chips.forEach(([chip, data]) => chip.setAttribute('aria-pressed', String(samePattern(data))));
    root.redraw();
    const state = new URLSearchParams();
    if (column.value) state.set('column', column.value);
    if (wanted) state.set('key', key.value.trim());
    if (pattern.target) state.set('target', pattern.target);
    if (pattern.difference) state.set('difference', pattern.difference);
    const fragment = state.toString();
    try {
      history.replaceState(null, '', fragment ? `#${fragment}` : location.pathname + location.search);
    } catch (error) {
      // A page shown inside an editor may refuse a new address; the filter still applies.
    }
  }
  const linked = new URLSearchParams(location.hash.slice(1));
  if (columns.includes(linked.get('column'))) column.value = linked.get('column');
  key.value = linked.get('key') || '';
  pattern = {
    column: column.value,
    target: linked.get('target') || '',
    difference: linked.get('difference') || '',
  };
  const ownFilter = () => { pattern = {column: '', target: '', difference: ''}; apply(); };
  column.addEventListener('change', ownFilter);
  key.addEventListener('input', ownFilter);
  clear.addEventListener('click', () => { column.value = ''; key.value = ''; ownFilter(); });
  chips.forEach(([chip, data]) => chip.addEventListener('click', () => {
    // Pressing a chip again lets go of the column it chose as well as the pattern.
    if (samePattern(data)) { column.value = ''; ownFilter(); return; }
    column.value = data.column;
    key.value = '';
    pattern = {column: data.column, target: data.target || '', difference: data.difference || ''};
    apply();
  }));
  apply();
}
document.querySelectorAll('[data-filterable]').forEach(filters);

// One tab per kind of row. Arrow keys, Home, and End move between tabs, and
// only the selected tab takes Tab focus, as the ARIA tabs pattern asks. A
// panel's own heading leaves the screen, since its tab now names it.
function tabs(root) {
  const panels = Array.from(root.querySelectorAll(':scope > section[data-kind]'));
  if (panels.length < 2) return;
  const list = document.createElement('div');
  list.setAttribute('role', 'tablist');
  list.setAttribute('aria-label', 'Rows by kind');
  const buttons = panels.map(panel => {
    const button = document.createElement('button');
    button.type = 'button';
    button.id = panel.id + '-tab';
    button.setAttribute('role', 'tab');
    button.setAttribute('aria-controls', panel.id);
    const count = Number(panel.dataset.count).toLocaleString('en-US');
    button.textContent = `${panel.dataset.kind} \\u00b7 ${count}`;
    panel.setAttribute('role', 'tabpanel');
    panel.setAttribute('aria-labelledby', button.id);
    panel.querySelector(':scope > h2').classList.add('sr-only');
    list.append(button);
    return button;
  });
  function select(index, focus) {
    buttons.forEach((button, at) => {
      const selected = at === index;
      button.setAttribute('aria-selected', String(selected));
      button.tabIndex = selected ? 0 : -1;
      panels[at].hidden = !selected;
    });
    if (focus) buttons[index].focus();
  }
  list.addEventListener('keydown', event => {
    const current = buttons.indexOf(document.activeElement);
    const last = buttons.length - 1;
    const target = {ArrowRight: current + 1, ArrowLeft: current - 1, Home: 0, End: last}[event.key];
    if (current < 0 || target === undefined) return;
    event.preventDefault();
    select((target + buttons.length) % buttons.length, true);
  });
  buttons.forEach((button, index) => button.addEventListener('click', () => select(index, false)));
  // A count in the row ledger links to its panel, so the link opens that tab.
  document.querySelectorAll('.legend a').forEach(link => link.addEventListener('click', () => {
    const index = panels.findIndex(panel => `#${panel.id}` === link.getAttribute('href'));
    if (index >= 0) select(index, false);
  }));
  root.prepend(list);
  const firstWithRows = panels.findIndex(panel => Number(panel.dataset.count) > 0);
  select(Math.max(firstWithRows, 0), false);
}
document.querySelectorAll('.kinds').forEach(tabs);
"""


def _escape(value: object) -> str:
    """Escape a value for embedding in HTML text."""
    return html.escape(str(value))


def _cell(value: object) -> str:
    """Render one table cell: `null` for a missing value, and booleans in lowercase."""
    if value is None:
        return "<td class='null'>null</td>"
    if isinstance(value, bool):
        return f"<td>{'true' if value else 'false'}</td>"
    return f"<td>{_escape(value)}</td>"


def _scroll_region(heading_id: str, table: str) -> str:
    """Wrap a table that may scroll sideways in a region named by its heading.

    The region takes keyboard focus, so arrow keys scroll a wide table.
    """
    return (
        f"<div class='wrap' role='region' aria-labelledby='{heading_id}' tabindex='0'>{table}</div>"
    )


def _slug(title: str) -> str:
    """Turn a title into the id its heading carries, such as `changed-rows`."""
    return re.sub(r"\W+", "-", title.lower())


def _table(title: str, frame: pl.DataFrame, max_rows: int, *, hint: str = "") -> str:
    """Render one table section under its heading, with every embedded row in the markup."""
    heading_id = _slug(title)
    heading = f"<h2 id='{heading_id}'>{_escape(title)}</h2>"
    if hint:
        heading += f"\n<p class='hint'>{hint}</p>"
    return f"{heading}\n{_table_body(heading_id, title, frame, max_rows)}"


def _table_body(heading_id: str, title: str, frame: pl.DataFrame, max_rows: int) -> str:
    """Render a table its heading names, paged by the script, with every embedded row."""
    if frame.height == 0 or not frame.columns:
        return "<p class='empty'>No rows.</p>"

    shown = frame.head(max_rows)
    truncated = ""
    if frame.height > max_rows:
        truncated = (
            f"<p class='note'>Showing the first {max_rows:,} of {frame.height:,} rows. "
            "Export artifacts with <code>output_path</code> for the complete set.</p>"
        )

    header = "".join(f"<th scope='col'>{_escape(name)}</th>" for name in shown.columns)
    body = "".join(f"<tr>{''.join(map(_cell, row))}</tr>" for row in shown.iter_rows())
    table = f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
    return truncated + _paged(heading_id, title, table, shown.height, "rows")


def _paged(heading_id: str, title: str, table: str, rows: int, unit: str, extra: str = "") -> str:
    """Wrap a table in the block the script pages, with a pager it shows past one page.

    Without JavaScript, every row shows at once.
    """
    pager = ""
    pages = math.ceil(rows / _PAGE_SIZE)
    if pages > 1:
        label = _escape(title.lower())
        pager = (
            "\n<div class='pager' hidden>"
            f"<button type='button' aria-label='Previous page of {label}'>Previous</button>"
            f"<button type='button' aria-label='Next page of {label}'>Next</button>"
            f"<span class='status' role='status'>Page 1 of {pages} &middot; "
            f"{rows:,} {unit}</span></div>"
        )
    return (
        f"<div data-table data-page-size='{_PAGE_SIZE}' data-unit='{unit}'{extra}>\n"
        f"{_scroll_region(heading_id, table)}{pager}\n"
        "</div>"
    )


class _MismatchedValue(NamedTuple):
    """One value that differs: its row's key, its column, and the value on each side."""

    key_values: tuple[object, ...]
    column: str
    source_value: object
    target_value: object


def _mismatched_values(
    rows: pl.DataFrame, keys: Sequence[str], columns: Sequence[str]
) -> Iterator[_MismatchedValue]:
    """Yield each value whose match flag is false, row by row, in the columns' order.

    A NULL flag is not a mismatch, as the column counts treat it.
    """
    for record in rows.iter_rows(named=True):
        for column in columns:
            if record[f"{column}_is_match"] is False:
                yield _MismatchedValue(
                    tuple(record[key] for key in keys),
                    column,
                    record[f"{column}_source"],
                    record[f"{column}_target"],
                )


def _signed_interval(delta: timedelta) -> str:
    """Write a time interval with its sign in front, such as `-1:30:00`."""
    sign = "-" if delta < timedelta(0) else "+"
    return f"{sign}{abs(delta)}"


def _difference(source_value: object, target_value: object) -> str:
    """Write target minus source for numbers, dates, and times, or nothing for other values.

    Numbers subtract as decimals of the text shown, so `65.99` to `66.99` reads
    `+1.00`, never a float's rounding error. A boolean, a NULL, text, or a value
    with no finite difference has none.
    """
    if source_value is None or target_value is None:
        return ""
    if isinstance(source_value, bool) or isinstance(target_value, bool):
        return ""
    if isinstance(source_value, _NUMBERS) and isinstance(target_value, _NUMBERS):
        try:
            delta = Decimal(str(target_value)) - Decimal(str(source_value))
        except InvalidOperation:
            return ""
        return f"{delta:+}" if delta.is_finite() else ""
    if isinstance(source_value, datetime) and isinstance(target_value, datetime):
        try:
            return _signed_interval(target_value - source_value)
        except TypeError:
            # A zoned time minus a naive one has no difference.
            return ""
    if isinstance(source_value, date) and isinstance(target_value, date):
        days = (target_value - source_value).days
        return f"{days:+,} {'day' if abs(days) == 1 else 'days'}"
    if isinstance(source_value, timedelta) and isinstance(target_value, timedelta):
        return _signed_interval(target_value - source_value)
    return ""


def _differing_cell(value: object) -> str:
    """Render the target's differing value, marked with a sign and weight as well as color."""
    text = "null" if value is None else _escape(value)
    null = " null" if value is None else ""
    return f"<td class='differs{null}'><span class='sign' aria-hidden='true'>&#8800;</span> {text}</td>"


_PATTERN_LIMIT: Final = 10
"""Patterns the report names, largest first, before it says how many more it found."""

_PATTERN_SAMPLE_KEYS: Final = 3
"""Keys a pattern names before it counts the rest."""


class _Pattern(NamedTuple):
    """A change that two or more mismatched values share, in one column.

    `kind` is `target` for a shared new value, `rise` or `fall` for a shared
    difference between numbers, and `move` for one between dates or times.
    """

    column: str
    kind: str
    shown_value: str
    difference: str
    keys: list[str]


def _value_text(value: object) -> str:
    """Write a value as the text a filter compares, with `null` for a missing one."""
    return "null" if value is None else str(value)


def _patterns(listed: Sequence[_MismatchedValue]) -> list[_Pattern]:
    """Group mismatched values that share a new value or a difference, largest group first.

    A group needs two values. Ties sort by column, then kind, then value, so the
    order never depends on the data's order.
    """
    groups: dict[tuple[str, str, str, str], list[str]] = {}
    for value in listed:
        key_text = ", ".join(map(str, value.key_values))
        target_text = _value_text(value.target_value)
        groups.setdefault((value.column, "target", target_text, ""), []).append(key_text)
        difference = _difference(value.source_value, value.target_value)
        if not difference:
            continue
        if isinstance(value.source_value, _NUMBERS):
            kind = "rise" if difference.startswith("+") else "fall"
            shown = difference[1:]
        else:
            kind, shown = "move", difference
        groups.setdefault((value.column, kind, shown, difference), []).append(key_text)
    shared = [
        _Pattern(column, kind, shown, difference, keys)
        for (column, kind, shown, difference), keys in groups.items()
        if len(keys) >= 2
    ]
    return sorted(
        shared,
        key=lambda pattern: (-len(pattern.keys), pattern.column, pattern.kind, pattern.shown_value),
    )


_PATTERN_VERBS: Final = {
    "target": "changed to",
    "rise": "rose by",
    "fall": "fell by",
    "move": "moved by",
}
"""How a pattern's sentence says what its values share."""


def _pattern_sentence(pattern: _Pattern) -> str:
    """Say a pattern in words: how many values, which column, what they share, and which keys."""
    count = len(pattern.keys)
    sample = ", ".join(_escape(key) for key in pattern.keys[:_PATTERN_SAMPLE_KEYS])
    rest = count - _PATTERN_SAMPLE_KEYS
    keys = f"Keys {sample}, and {rest:,} more." if rest > 0 else f"Keys {sample}."
    return (
        f"{count:,} values in <code>{_escape(pattern.column)}</code> "
        f"{_PATTERN_VERBS[pattern.kind]} <code>{_escape(pattern.shown_value)}</code>. {keys}"
    )


def _pattern_label(pattern: _Pattern) -> str:
    """Say a pattern in a few words, for the chip the script makes of it."""
    return f"{pattern.column} {_PATTERN_VERBS[pattern.kind]} {pattern.shown_value}"


def _patterns_html(listed: Sequence[_MismatchedValue], total: int) -> str:
    """List the patterns among the listed values, each tagged with the filter that shows it.

    Without the script, a pattern is a sentence. The script turns each into a
    chip that filters the mismatched values to it.
    """
    heading = "<h3 id='patterns'>Patterns</h3>"
    scope = "<p class='hint'>Changes that two or more of the mismatched values share.</p>"
    if len(listed) < total:
        scope = (
            f"<p class='note'>Patterns among the first {len(listed):,} of {total:,} "
            "mismatched values.</p>"
        )
    found = _patterns(listed)
    if not found:
        return f"{heading}\n{scope}\n<p class='empty'>No two values share a change.</p>"
    items = "".join(
        f"<li class='pattern' data-column='{_escape(pattern.column)}' "
        f"data-label='{_escape(_pattern_label(pattern))}' data-count='{len(pattern.keys)}' "
        + (
            f"data-target='{_escape(pattern.shown_value)}'>"
            if pattern.kind == "target"
            else f"data-difference='{pattern.difference}'>"
        )
        + f"{_pattern_sentence(pattern)}</li>"
        for pattern in found[:_PATTERN_LIMIT]
    )
    more = ""
    if len(found) > _PATTERN_LIMIT:
        more = (
            f"<p class='hint'>{len(found) - _PATTERN_LIMIT:,} smaller patterns are not shown.</p>"
        )
    return f"{heading}\n{scope}\n<ul class='patterns'>{items}</ul>{more}"


def _listed_values(
    changed: pl.DataFrame, result: DiffResult, max_rows: int
) -> list[_MismatchedValue]:
    """List the mismatched values the report shows, in key order, up to `max_rows` of them.

    `max_rows` caps the values listed, not rows times columns.
    """
    keys = list(result.primary_keys)
    ordered = changed.sort(keys) if keys else changed
    return list(
        islice(_mismatched_values(ordered.head(max_rows), keys, result.compared_columns), max_rows)
    )


def _mismatch_view(listed: Sequence[_MismatchedValue], result: DiffResult) -> str:
    """List each mismatched value of the changed rows, the report's default view of them.

    The view says when it stops short of every mismatch the run counted. Each
    row carries its column and key as data, which the script's filters read.
    """
    keys = list(result.primary_keys)
    total = sum(result.summary.column_mismatches.values())
    heading = "<h3 id='mismatched-values'>Mismatched values</h3>"
    hint = "<p class='hint'>One row per value that differs, as compared after the rules.</p>"
    if not listed:
        return f"{heading}\n<p class='empty'>No mismatched values.</p>"
    truncated = ""
    if len(listed) < total:
        truncated = (
            f"<p class='note'>Showing the first {len(listed):,} of {total:,} mismatched values.</p>"
        )
    header = "".join(
        f"<th scope='col'>{_escape(name)}</th>"
        for name in [*keys, "Column", "Source", "Target", "Difference"]
    )
    body = "".join(
        f"<tr data-column='{_escape(value.column)}' "
        f"data-key='{_escape(', '.join(map(str, value.key_values)))}' "
        f"data-target='{_escape(_value_text(value.target_value))}' "
        f"data-difference='{_difference(value.source_value, value.target_value)}'>"
        f"{''.join(map(_cell, value.key_values))}<td>{_escape(value.column)}</td>"
        f"{_cell(value.source_value)}{_differing_cell(value.target_value)}"
        f"<td class='difference'>{_difference(value.source_value, value.target_value)}</td></tr>"
        for value in listed
    )
    table = f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
    return (
        f"{_patterns_html(listed, total)}\n{heading}\n{hint}\n{truncated}"
        f"{_paged('mismatched-values', 'Mismatched values', table, len(listed), 'values', ' data-filterable')}"
    )


def _pair_cell(source_value: object, target_value: object, *, differs: bool) -> str:
    """Render one compared column of a changed row, with both values where they read apart.

    A value that differs carries the source value, muted, before the sign and
    the target value. A value the rules made match, such as `10.0` and `10.004`
    within a tolerance, shows both with an approximation sign. Words a screen
    reader hears replace what the signs say.
    """
    source_text = _value_text(source_value)
    target_text = _value_text(target_value)
    if differs:
        return (
            f"<td class='differs'><span class='was'><span class='sr-only'>source </span>"
            f"{_escape(source_text)}</span><span class='sign' aria-hidden='true'>&#8800;</span> "
            f"<span class='sr-only'>target </span>{_escape(target_text)}</td>"
        )
    if source_text != target_text:
        return (
            f"<td class='forgiven'>{_escape(source_text)}<span class='sign' aria-hidden='true'>"
            f"&#8776;</span><span class='sr-only'>matches, within the rules, </span>"
            f"{_escape(target_text)}</td>"
        )
    return _cell(target_value)


def _side_by_side(changed: pl.DataFrame, result: DiffResult, max_rows: int) -> str:
    """Render the changed rows one per row, each compared column in one cell with both values.

    The last column names the columns that differ, so a reader hears them
    without reading every cell.
    """
    keys = list(result.primary_keys)
    shown = (changed.sort(keys) if keys else changed).head(max_rows)
    truncated = ""
    if changed.height > max_rows:
        truncated = (
            f"<p class='note'>Showing the first {max_rows:,} of {changed.height:,} rows. "
            "Export artifacts with <code>output_path</code> for the complete set.</p>"
        )
    header = "".join(
        f"<th scope='col'>{_escape(name)}</th>"
        for name in [*keys, *result.compared_columns, "Differs in"]
    )
    rows: list[str] = []
    for record in shown.iter_rows(named=True):
        cells = "".join(_cell(record[key]) for key in keys)
        differing: list[str] = []
        for column in result.compared_columns:
            differs = record[f"{column}_is_match"] is False
            if differs:
                differing.append(column)
            cells += _pair_cell(
                record[f"{column}_source"], record[f"{column}_target"], differs=differs
            )
        rows.append(f"<tr>{cells}<td>{_escape(', '.join(differing))}</td></tr>")
    table = f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    return truncated + _paged("changed-rows", "Changed rows", table, shown.height, "rows")


def _changed_section(
    changed: pl.DataFrame,
    result: DiffResult,
    max_rows: int,
    listed: Sequence[_MismatchedValue] | None,
) -> str:
    """Render the changed rows: mismatched values first, and the rows side by side one click away.

    A pushdown run without a sample holds keys alone, so it keeps the one table.
    """
    if listed is None or changed.height == 0:
        return _table("Changed rows", changed, max_rows)
    return (
        "<h2 id='changed-rows'>Changed rows</h2>\n"
        f"{_mismatch_view(listed, result)}\n"
        "<details class='full-rows'><summary>Show the changed rows side by side, every "
        f"column with both values ({changed.height:,} rows)</summary>\n"
        f"{_side_by_side(changed, result, max_rows)}\n</details>"
    )


def _accepted_section(result: DiffResult) -> str:
    """List the drift the baseline accepted, by kind and key, with the columns of a changed row.

    A result built without the baseline's entries says so, since the count on
    its summary still names them.
    """
    heading = "<h2 id='accepted-rows'>Accepted rows</h2>"
    hint = (
        "<p class='hint'>Drift the baseline accepts, which the counts and the verdict leave out."
        "</p>"
    )
    accepted = result.accepted
    if accepted is None:
        return f"{heading}\n{hint}\n<p class='empty'>The baseline's entries were not kept.</p>"
    keys = accepted.primary_keys
    entries = [
        *(("Added", key, "whole row") for key in accepted.added),
        *(("Removed", key, "whole row") for key in accepted.removed),
        *((("Changed", entry.key, ", ".join(entry.columns))) for entry in accepted.changed),
    ]
    header = "".join(
        f"<th scope='col'>{_escape(name)}</th>" for name in ["Kind", *keys, "Columns accepted"]
    )
    body = "".join(
        f"<tr><td>{kind}</td>{''.join(_cell(key[name]) for name in keys)}"
        f"<td>{_escape(columns)}</td></tr>"
        for kind, key, columns in entries
    )
    table = f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
    return f"{heading}\n{hint}\n{_paged('accepted-rows', 'Accepted rows', table, len(entries), 'rows')}"


_ABSENT: Final = "<span class='absent'>None recorded</span>"
"""What the run record shows for a field it has no value for."""

_TREE_STATES: Final = {
    True: "tracked files match this commit",
    False: "tracked files differ from this commit",
    None: "whether tracked files match is unknown",
}
"""The words for `working_tree_clean`, so the state never rests on a symbol."""

_SHORT_SHA: Final = 7
"""Characters of a commit hash the header shows. The run record holds the whole hash."""


def _hashed_file_html(file: HashedFile | None) -> str:
    """Render a file the run read as its path and SHA-256, or as absent."""
    if file is None:
        return _ABSENT
    return f"<code>{_escape(file.path)}</code> &middot; SHA-256 <code>{file.sha256}</code>"


def _run_record_html(provenance: RunProvenance | None) -> str:
    """Render what the run read and where it ran, or nothing for a result without a record.

    Each value is escaped, and the CI run's URL is one the model holds to https
    with no quote, so it can sit in an `href`.
    """
    if provenance is None:
        return ""
    commit = _ABSENT
    if provenance.commit_sha is not None:
        tree_state = _TREE_STATES[provenance.working_tree_clean]
        commit = f"<code>{provenance.commit_sha}</code>, {tree_state}"
    ci_run = _ABSENT
    if provenance.ci_run_url is not None:
        url = _escape(provenance.ci_run_url)
        ci_run = f'<a href="{url}">{url}</a>'
    started = provenance.started_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    fields = [
        ("Configuration", _hashed_file_html(provenance.config_file)),
        ("Baseline", _hashed_file_html(provenance.baseline_file)),
        ("Commit", commit),
        ("CI run", ci_run),
        ("Run by", f"Veridelta {_escape(provenance.veridelta_version)}, started {started}"),
    ]
    rows = "".join(f"<dt>{label}</dt><dd>{value}</dd>" for label, value in fields)
    return (
        '<section class="record" aria-labelledby="run-record">'
        f'<h2 id="run-record">Run record</h2><dl>{rows}</dl></section>'
    )


def _header(provenance: RunProvenance | None, generated: str) -> str:
    """Render the title line: when the page was made, and what the run read, in a few words.

    The run record at the foot of the page holds the whole of each value.
    """
    parts = [f"Generated {generated}"]
    if provenance is not None:
        parts.append(f"Veridelta {_escape(provenance.veridelta_version)}")
        if provenance.config_file is not None:
            parts.append(f"<code>{_escape(provenance.config_file.path)}</code>")
        if provenance.commit_sha is not None:
            parts.append(f"commit <code>{provenance.commit_sha[:_SHORT_SHA]}</code>")
        parts.append('<a href="#run-record">Run record</a>')
    return (
        '<header class="top"><div class="brand">'
        f'<img class="symbol" src="data:image/png;base64,{_SYMBOL}" alt="" width="69" height="64">'
        "<div><h1>Veridelta report</h1>"
        f'<p class="sub">{" &middot; ".join(parts)}</p></div></div></header>'
    )


def _bar(percentage: float) -> str:
    """Draw a share as an SVG bar a screen reader skips, since the number beside it says it.

    The bar's length is the rect's `width` attribute, because a strict content
    security policy refuses a `style` attribute.
    """
    return (
        '<svg class="bar" viewBox="0 0 100 6" preserveAspectRatio="none" aria-hidden="true">'
        f'<rect class="track" width="100" height="6"></rect>'
        f'<rect class="filled" width="{percentage}" height="6"></rect></svg>'
    )


def _counted(count: int, singular: str, plural: str) -> str:
    """Write a count with the noun that agrees with it, such as `1 row differs`."""
    return f"{count:,} {singular if count == 1 else plural}"


def _percent(ratio: float, places: int) -> str:
    """Write a ratio as a percentage with up to `places` decimals, such as `12.5%` or `0%`.

    A ratio too small for the places shows as under the smallest it could show,
    so a run that differs never reads as one that does not.
    """
    rounded = round(Decimal(str(ratio)) * 100, places)
    if rounded == 0 and ratio > 0:
        return f"under {Decimal(1).scaleb(-places)}%"
    return f"{format(rounded.normalize(), 'f')}%"


def _headline(summary: DiffSummary, *, columns: bool = True) -> str:
    """Say in a sentence how much differs, against what, so the verdict needs no table to read it.

    The Markdown summary leaves the count of drifting columns out when
    `report_top_columns_limit` is 0, as it leaves out the drift table.
    """
    if summary.is_perfect_match:
        return "Every row matches."
    rows = _counted(summary.total_mismatches, "row differs", "rows differ")
    side = "within" if summary.is_match else "above"
    sentence = (
        f"{rows}, {_percent(summary.mismatch_ratio, 2)} of the {summary.total_rows_source:,} "
        f"source rows, {side} the threshold of {_percent(summary.threshold, 4)}."
    )
    if columns and summary.column_mismatches:
        drifting = len(summary.column_mismatches)
        sentence += f" {_counted(drifting, 'column drifts', 'columns drift')}."
    if summary.accepted_count:
        more = _counted(summary.accepted_count, "more row was", "more rows were")
        sentence += f" {more} accepted by the baseline."
    return sentence


def _meter(summary: DiffSummary) -> str:
    """Draw the mismatch ratio against the threshold, which the headline says in words.

    The scale runs to four times the threshold, so a pass shows the room left,
    or to the whole when the threshold is zero, and always holds the ratio.
    """
    scale = max(summary.mismatch_ratio, 4 * summary.threshold if summary.threshold else 1.0)
    filled = round(100 * summary.mismatch_ratio / scale, 1)
    tick = round(100 * summary.threshold / scale, 1)
    return (
        '<svg class="meter" viewBox="0 0 100 8" preserveAspectRatio="none" aria-hidden="true">'
        '<rect class="track" width="100" height="8"></rect>'
        f'<rect class="filled" width="{filled}" height="8"></rect>'
        f'<rect class="tick" x="{min(tick, 99.4)}" width="0.6" height="8"></rect></svg>'
    )


def _banner(summary: DiffSummary) -> str:
    """Render the verdict as the page's largest text, a word beside a glyph, and its headline."""
    verdict = "PASSED" if summary.is_match else "FAILED"
    state = "pass" if summary.is_match else "fail"
    glyph = "&#10003;" if summary.is_match else "&#10005;"
    meter = "" if summary.is_perfect_match else _meter(summary)
    return (
        f'<section class="banner {state}" aria-labelledby="verdict">'
        f'<span class="glyph" aria-hidden="true">{glyph}</span>'
        f'<div><p class="verdict {state}" id="verdict">{verdict}</p>'
        f'<p class="headline">{_headline(summary)}</p>{meter}</div></section>'
    )


class _Ledger(NamedTuple):
    """The rows of a run by what became of them, which together are every key on either side."""

    matched: int
    changed: int
    removed: int
    accepted: int
    added: int


def _ledger_counts(result: DiffResult) -> _Ledger:
    """Count the rows by kind, so the five counts add up to the keys on either side.

    The source rows are the matched, changed, and removed rows, plus the rows
    the baseline accepted on the source side. The added rows, and any accepted
    added rows, are on the target side alone. A result without the baseline's
    entries counts every accepted row on the source side.
    """
    summary = result.summary
    accepted_added = len(result.accepted.added) if result.accepted is not None else 0
    in_source = summary.accepted_count - accepted_added
    matched = max(
        summary.total_rows_source - summary.changed_count - summary.removed_count - in_source, 0
    )
    return _Ledger(
        matched,
        summary.changed_count,
        summary.removed_count,
        summary.accepted_count,
        summary.added_count,
    )


_LEDGER_WORDS: Final = {
    "matched": ("matched", ""),
    "changed": ("changed", "changed-rows-panel"),
    "removed": ("removed, only in the source", "removed-rows-panel"),
    "accepted": ("accepted by the baseline", "accepted-rows-panel"),
    "added": ("added, only in the target", "added-rows-panel"),
}
"""Each kind in the ledger: its words, and the panel its count links to."""


def _ledger_html(result: DiffResult) -> str:
    """Render the rows as one bar of every key on either side, and a legend of counts.

    Added rows have no source row, so their segment is an outline. The legend
    carries each count in text, and a count links to the rows it counts.
    """
    summary = result.summary
    keys = ", ".join(f"<code>{_escape(key)}</code>" for key in result.primary_keys)
    shift = summary.volume_shift
    difference = (
        "the same count"
        if shift == 0
        else _counted(abs(shift), "row more", "rows more")
        if shift > 0
        else _counted(abs(shift), "row fewer", "rows fewer")
    )
    hint = (
        f"{summary.total_rows_source:,} source rows and {summary.total_rows_target:,} target rows, "
        f"{difference}" + (f", matched on {keys}." if keys else ".")
    )
    counts = _ledger_counts(result)
    total = sum(counts)
    bar = ""
    if total:
        position = 0.0
        segments = ""
        for kind, count in zip(_Ledger._fields, counts, strict=True):
            if not count:
                continue
            width = round(100 * count / total, 2)
            segments += f'<rect class="{kind}" x="{position}" width="{width}" height="18"></rect>'
            position = round(position + width, 2)
        bar = (
            '<svg class="ledger" viewBox="0 0 100 18" preserveAspectRatio="none" '
            f'aria-hidden="true">{segments}</svg>'
        )
    items = ""
    for kind, count in zip(_Ledger._fields, counts, strict=True):
        if kind == "accepted" and not count:
            continue
        words, panel = _LEDGER_WORDS[kind]
        number = f"<span class='count'>{count:,}</span>"
        text = (
            f"{number} {words}"
            if count or not panel
            else f"{number} <span class='absent'>{words}</span>"
        )
        if count and panel:
            text = f"<a href='#{panel}'>{text}</a>"
        items += f"<li><span class='swatch {kind}' aria-hidden='true'></span>{text}</li>"
    return (
        '<section aria-labelledby="rows"><h2 id="rows">Rows</h2>'
        f"<p class='hint'>{hint}</p>{bar}<ul class='legend'>{items}</ul></section>"
    )


_WHITESPACE_WORDS: Final = {
    "left": "whitespace trimmed on the left",
    "right": "whitespace trimmed on the right",
    "both": "whitespace trimmed at both ends",
}
"""How a whitespace mode reads in the columns table. `none` trims nothing, so it is no rule."""


_RULE_PHRASES: Final[dict[str, Callable[[Any], str]]] = {
    "absolute_tolerance": lambda tolerance: f"absolute tolerance {tolerance}",
    "relative_tolerance": lambda tolerance: f"relative tolerance {tolerance}",
    "null_matches_null": lambda _: "NULL does not match NULL",
    "case_insensitive": lambda _: "case-insensitive",
    "whitespace_mode": lambda mode: _WHITESPACE_WORDS[mode],
    "regex_replacement_count": lambda count: _counted(
        count, "regex replacement", "regex replacements"
    ),
    "value_map_size": lambda size: f"value map of {_counted(size, 'entry', 'entries')}",
    "null_value_count": lambda count: _counted(count, "null sentinel", "null sentinels"),
    "pad_zeros": lambda width: f"padded to {width} digits",
    "datetime_format": lambda text_format: f"parsed as {text_format}",
    "timezone": lambda zone: f"read in {zone}",
    "cast_to": lambda target: f"cast to {target}",
    "max_levenshtein_distance": lambda edits: f"up to {_counted(edits, 'edit', 'edits')} apart",
    "min_jaro_winkler_similarity": lambda floor: f"Jaro-Winkler similarity at least {floor}",
}
"""How each `ColumnRules` setting reads, in the order a run applies them.

A setting at its default, such as a tolerance of 0, changes no match, so it
reads as no rule.
"""


def _rule_words(rules: ColumnRules) -> list[str]:
    """Say each setting that loosens or changes a column's match."""
    return [
        phrase(getattr(rules, name))
        for name, phrase in _RULE_PHRASES.items()
        if getattr(rules, name) != ColumnRules.model_fields[name].default
    ]


def _common_change(pattern: _Pattern | None, mismatches: int) -> str:
    """Say a column's largest pattern in a few words, or that its values share none."""
    if pattern is None:
        if mismatches == 1:
            return "a single value"
        return "no shared change" if mismatches else "matches in every row"
    return (
        f"{len(pattern.keys):,} {_PATTERN_VERBS[pattern.kind]} "
        f"<code>{_escape(pattern.shown_value)}</code>"
    )


class _ColumnsTable(NamedTuple):
    """What the columns table shows of each column, and which of its columns the run can fill."""

    mismatches: dict[str, int]
    rules: dict[str, ColumnRules]
    ruled: dict[str, list[str]]
    patterns: dict[str, _Pattern]
    changed_rows: int
    counted: bool
    has_values: bool

    def headers(self) -> list[str]:
        """Name the table's columns, in order."""
        return [
            "Column",
            *(["Compared"] if self.rules else []),
            "Rows mismatched",
            "Share of changed rows",
            *(["Values forgiven"] if self.counted else []),
            *(["Most common change"] if self.has_values else []),
        ]

    def row(self, column: str) -> str:
        """Render one column's row, quiet when it has no drift."""
        count = self.mismatches.get(column, 0)
        cells = f"<td>{_escape(column)}</td>"
        if self.rules:
            cells += f"<td>{_escape('; '.join(self.ruled.get(column, ['exactly'])))}</td>"
        cells += f"<td>{count:,}</td>"
        share = ""
        if count:
            percentage = 100 * count / self.changed_rows
            share = f"{_bar(round(percentage, 1))}{round(percentage):d}%"
        cells += f"<td class='share'>{share}</td>"
        if self.counted:
            rules = self.rules.get(column)
            forgiven = None if rules is None else rules.forgiven_count
            cells += f"<td>{'' if forgiven is None else f'{forgiven:,}'}</td>"
        if self.has_values:
            cells += f"<td>{_common_change(self.patterns.get(column), count)}</td>"
        state = "" if count else " class='quiet'"
        return f"<tr{state}>{cells}</tr>"


def _columns_table(result: DiffResult, listed: Sequence[_MismatchedValue] | None) -> _ColumnsTable:
    """Gather what the columns table shows from the result and the values the report lists."""
    rules = {rules.column: rules for rules in result.rules_applied}
    patterns: dict[str, _Pattern] = {}
    if listed is not None:
        # Patterns come largest first, so the last one kept per column is its largest.
        for pattern in reversed(_patterns(listed)):
            patterns[pattern.column] = pattern
    return _ColumnsTable(
        mismatches=result.summary.column_mismatches,
        rules=rules,
        ruled={column: words for column, item in rules.items() if (words := _rule_words(item))},
        patterns=patterns,
        changed_rows=max(result.summary.changed_count, 1),
        # A run with `count_forgiven_values` counted each ruled column; any other run counted none.
        counted=any(item.forgiven_count is not None for item in rules.values()),
        has_values=listed is not None,
    )


def _columns_section(result: DiffResult, listed: Sequence[_MismatchedValue] | None) -> str:
    """Render one table of the compared columns: each column's rules, its drift, and its pattern.

    Drifting columns come first, most mismatches first, then columns a rule
    reaches without drift, so a reviewer sees in one row what a rule forgave.
    Columns compared exactly that match in every row are counted in one line.
    A run with no record of its rules shows its drift alone, and a pushdown run
    without a sample shows no pattern, since it lists no values.
    """
    table = _columns_table(result, listed)
    heading = "<h2 id='columns'>Columns</h2>"
    drifting = sorted(table.mismatches.items(), key=lambda item: -item[1])
    columns = [
        *(column for column, _ in drifting),
        *sorted(set(table.ruled) - set(table.mismatches)),
    ]
    compared = len(result.rules_applied) or len(result.compared_columns)
    hint = ""
    if compared:
        hint = (
            f"<p class='hint'>{_counted(compared, 'column compared', 'columns compared')}, "
            f"{_counted(len(table.mismatches), 'drifts', 'drift')}.</p>"
        )
    if not columns:
        words = "No column-level drift"
        if table.rules:
            words += ", and every compared column is compared exactly"
        return f"{heading}\n{hint}<p class='empty'>{words}.</p>"
    header = "".join(f"<th scope='col'>{name}</th>" for name in table.headers())
    rows = "".join(table.row(column) for column in columns)
    region = _scroll_region(
        "columns", f"<table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>"
    )
    rest = ""
    exact = compared - len(columns)
    if exact > 0:
        rest = (
            "\n<p class='hint'>"
            + _counted(
                exact,
                "other column is compared exactly and matches in every row.",
                "other columns are compared exactly and match in every row.",
            )
            + "</p>"
        )
    return f"{heading}\n{hint}{region}{rest}"


def _kind_section(title: str, count: int, content: str) -> str:
    """Wrap one kind of row in a section the script turns into a tab, named and counted."""
    return (
        f"<section class='kind' id='{_slug(title)}-panel' data-kind='{_escape(title)}' "
        f"data-count='{count}'>\n{content}\n</section>"
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

    changed = result.changed
    keys_note = ""
    if result.keys_only and result.changed_sample is not None:
        changed = result.changed_sample
        keys_note = (
            "<p class='note'>This comparison ran as pushdown, inside the database that "
            f"stores both tables. Changed rows show values for the first {changed.height:,} "
            f"of {summary.changed_count:,}, in key order, fetched because "
            "<code>pushdown_sample_rows</code> is set. Added and removed rows list "
            "primary keys.</p>"
        )
    elif result.keys_only:
        keys_note = (
            "<p class='note'>This comparison ran as pushdown, inside the database that "
            "stores both tables, and never extracts rows. The tables below list "
            "primary keys rather than values.</p>"
        )
    has_values = bool(result.compared_columns) and all(
        f"{column}_is_match" in changed.columns for column in result.compared_columns
    )
    listed = _listed_values(changed, result, max_rows) if has_values else None

    # Without the script, the sections stay stacked with every row shown.
    kinds = [
        _kind_section(
            "Changed rows",
            summary.changed_count,
            _changed_section(changed, result, max_rows, listed),
        ),
        _kind_section(
            "Added rows",
            summary.added_count,
            _table("Added rows", result.added, max_rows, hint="Rows only in the target."),
        ),
        _kind_section(
            "Removed rows",
            summary.removed_count,
            _table("Removed rows", result.removed, max_rows, hint="Rows only in the source."),
        ),
    ]
    if summary.accepted_count:
        kinds.append(
            _kind_section("Accepted rows", summary.accepted_count, _accepted_section(result))
        )
    kinds_html = "\n".join(kinds)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Veridelta Report &mdash; {verdict}</title>
<style>{_STYLE}</style>
</head>
<body>
<main>
{_header(summary.provenance, generated)}
{_banner(summary)}
{keys_note}
{_ledger_html(result)}
{_columns_section(result, listed)}
<div class="kinds">
{kinds_html}
</div>
{_run_record_html(summary.provenance)}
</main>
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
The JSON comment at the end counts toward it.
"""

_SUMMARY_COMMENT: Final[str] = "<!-- veridelta-summary"
"""Opens the HTML comment that holds the summary as JSON, ahead of its schema's URL."""

_JSON_IN_HTML: Final = str.maketrans({"<": "\\u003c", ">": "\\u003e", "&": "\\u0026"})
"""JSON escapes for the characters that could close the comment, which a parser reads back."""

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
        str: The verdict and its sentence, a table of counts, and the top
            drifting columns, limited to the configured
            `report_top_columns_limit`, then any changed values asked for.

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
        _headline(summary, columns=summary.report_limit > 0),
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
    if summary.accepted_count:
        lines.append(f"| Accepted by the baseline | {summary.accepted_count:,} |")
    lines += _run_record_lines(summary.provenance)
    if result.keys_only:
        lines += [
            "",
            "> Pushdown compared these tables inside the database that stores them, so its "
            "artifacts list primary keys only.",
        ]
    if summary.report_limit > 0:
        lines += ["", "#### Column-level drift", ""]
        lines += _drift_lines(summary)
    comment = ["", *_summary_comment(summary)]
    if max_rows > 0 and summary.changed_count > 0:
        lines += ["", "#### Changed values", ""]
        used = len("\n".join([*lines, *comment]).encode()) + 1
        lines += _value_lines(result, max_rows, _MARKDOWN_BUDGET - used)
    return "\n".join([*lines, *comment]) + "\n"


def _run_record_lines(provenance: RunProvenance | None) -> list[str]:
    """Name the run record's hashes, commit, and CI run in one line, or nothing when it has none."""
    if provenance is None:
        return []
    parts: list[str] = []
    if provenance.config_file is not None:
        config = provenance.config_file
        parts.append(f"{_markdown_code(config.path)} SHA-256 `{config.sha256}`")
    if provenance.baseline_file is not None:
        baseline = provenance.baseline_file
        parts.append(f"baseline {_markdown_code(baseline.path)} SHA-256 `{baseline.sha256}`")
    if provenance.commit_sha is not None:
        parts.append(f"commit `{provenance.commit_sha}`")
    if provenance.ci_run_url is not None:
        parts.append(f"[CI run]({provenance.ci_run_url})")
    return ["", f"Run record: {', '.join(parts)}."] if parts else []


def _top_columns(summary: DiffSummary) -> list[tuple[str, int]]:
    """Rank the drifting columns by mismatches, as `report_summary` does, and keep the top ones."""
    ranked = sorted(summary.column_mismatches.items(), key=lambda item: -item[1])
    return ranked[: summary.report_limit]


def _drift_lines(summary: DiffSummary) -> list[str]:
    """Render the top drifting columns as a Markdown table."""
    mismatches = summary.column_mismatches
    if not mismatches:
        return ["No column-level drift."]
    lines = ["| Column | Mismatches |", "| :--- | ---: |"]
    lines += [
        f"| {_markdown_code(column)} | {count:,} |" for column, count in _top_columns(summary)
    ]
    if len(mismatches) > summary.report_limit:
        lines += [
            "",
            f"_Showing the top {summary.report_limit} of {len(mismatches)} columns with drift._",
        ]
    return lines


def _summary_comment(summary: DiffSummary) -> list[str]:
    """Hold the summary as JSON in an HTML comment, under the URL of the schema it follows.

    The JSON is what `veridelta run --json` prints, except that
    `column_mismatches` holds only the columns the drift table lists, and is
    left out when the table is, so the comment names no column the page hides.
    `<`, `>`, and `&` are written as JSON escapes, so a column name cannot close
    the comment or open another.
    """
    payload = summary.model_dump(mode="json")
    if summary.report_limit > 0:
        payload["column_mismatches"] = dict(_top_columns(summary))
    else:
        del payload["column_mismatches"]
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return [f"{_SUMMARY_COMMENT} {output_schema_url('run')}", text.translate(_JSON_IN_HTML), "-->"]


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
    """Yield the rendered Markdown cells of each mismatched value, row by row."""
    for value in _mismatched_values(rows, keys, columns):
        yield [
            *map(_markdown_value, value.key_values),
            _markdown_code(value.column),
            _markdown_value(value.source_value),
            _markdown_value(value.target_value),
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
