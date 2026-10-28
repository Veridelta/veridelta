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

import html
import json
import math
import re
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from itertools import islice
from pathlib import Path
from typing import Final, NamedTuple

import polars as pl

from veridelta.exceptions import ConfigError
from veridelta.models import DiffResult, DiffSummary, HashedFile, RunProvenance
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

_STYLE: Final[str] = """
:root {
  --bg: #ffffff; --fg: #1b1f24; --muted: #5c6773; --line: #d8dee4;
  --pass: #1a7f37; --fail: #cf222e; --chip: #f2f4f7; --link: #0969da;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #30363d;
    --pass: #3fb950; --fail: #f85149; --chip: #161b22; --link: #4493f8;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 2rem; background: var(--bg); color: var(--fg);
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}
h1 { font-size: 1.5rem; margin: 0 0 .25rem; }
a { color: var(--link); }
h2 { font-size: 1.1rem; margin: 2rem 0 .75rem; }
.sub { color: var(--muted); margin: 0; font-size: .9rem; }
.banner {
  display: flex; align-items: center; gap: 1rem; margin: .75rem 0 1.5rem;
  border: 2px solid var(--line); border-radius: 8px; padding: .9rem 1.2rem;
}
.banner.pass { border-color: var(--pass); }
.banner.fail { border-color: var(--fail); }
.banner p { margin: 0; }
.glyph { font-size: 2rem; font-weight: 700; line-height: 1; }
.banner.pass .glyph { color: var(--pass); }
.banner.fail .glyph { color: var(--fail); }
.verdict { font-size: 1.6rem; font-weight: 700; letter-spacing: .02em; line-height: 1.2; }
.verdict.pass { color: var(--pass); }
.verdict.fail { color: var(--fail); }
.headline { font-size: 1.05rem; }
.bar { display: block; width: 100%; height: 6px; margin-top: .4rem; border-radius: 3px; }
.bar .track { fill: var(--line); }
.bar .filled { fill: var(--muted); }
td.share { min-width: 9rem; }
td.share .bar { display: inline-block; width: 6rem; margin: 0 .6rem 0 0; vertical-align: middle; }
[role="tablist"] {
  display: flex; flex-wrap: wrap; gap: .25rem; margin-top: 2rem;
  border-bottom: 1px solid var(--line);
}
[role="tab"] {
  border: 0; border-bottom: 3px solid transparent; background: none; color: var(--muted);
  padding: .45rem .9rem; font: inherit; font-size: .95rem; cursor: pointer; margin-bottom: -1px;
}
[role="tab"][aria-selected="true"] { color: var(--fg); font-weight: 600; border-bottom-color: var(--link); }
[role="tabpanel"] > h2 { margin-top: 1rem; }
.cards { display: flex; flex-wrap: wrap; gap: .75rem; margin-bottom: 1rem; }
.card {
  border: 1px solid var(--line); border-radius: 6px; padding: .75rem 1rem;
  min-width: 8.5rem; background: var(--chip);
}
.card .label { color: var(--muted); font-size: .8rem; text-transform: uppercase; }
.card .value { font-size: 1.35rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.record { margin: 0 0 1.5rem; }
.record h2 { margin-top: 0; }
.record dl {
  display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: .3rem 1rem;
  margin: 0; font-size: .9rem;
}
.record dt { color: var(--muted); }
.record dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
.record .absent { color: var(--muted); }
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
.pager[hidden] { display: none; }
.pager button {
  border: 1px solid var(--line); background: var(--chip); color: var(--fg);
  border-radius: 5px; padding: .3rem .7rem; cursor: pointer; font-size: .85rem;
}
.pager button[aria-disabled="true"] { opacity: .45; cursor: default; }
.pager .status { color: var(--muted); font-size: .85rem; }
.empty { color: var(--muted); padding: .75rem; }
h3 { font-size: 1rem; margin: 1.25rem 0 .25rem; }
.hint { color: var(--muted); font-size: .85rem; margin: 0 0 .6rem; }
td.differs { font-weight: 600; background: var(--chip); }
td.differs .sign { color: var(--fail); margin-right: .15rem; }
td.difference { font-variant-numeric: tabular-nums; }
.filters { display: flex; flex-wrap: wrap; align-items: center; gap: .5rem 1rem; margin: 0 0 .5rem; }
.filters label { font-size: .85rem; color: var(--muted); }
.filters select, .filters input, .filters button {
  font: inherit; font-size: .85rem; color: var(--fg); background: var(--bg);
  border: 1px solid var(--line); border-radius: 5px; padding: .25rem .5rem;
}
.filters button { background: var(--chip); cursor: pointer; }
.filter-status { color: var(--muted); font-size: .85rem; margin: 0 0 .5rem; }
details.full-rows { margin-top: 1.25rem; }
ul.patterns { margin: 0 0 .5rem; padding-left: 1.2rem; font-size: .9rem; }
ul.patterns li { margin: .3rem 0; }
.show-pattern {
  font: inherit; font-size: .8rem; color: var(--link); background: none; cursor: pointer;
  border: 1px solid var(--line); border-radius: 5px; padding: .1rem .5rem; margin-left: .25rem;
}
details.full-rows summary { cursor: pointer; color: var(--link); margin-bottom: .6rem; }
@media (max-width: 40rem) {
  body { padding: 1rem; }
  .card { min-width: 0; flex: 1 1 8rem; }
  .record dl { grid-template-columns: minmax(0, 1fr); }
  .record dd { margin-bottom: .4rem; }
  .verdict { font-size: 1.35rem; }
}
"""

_SCRIPT: Final[str] = """
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

// Filters for the mismatched values: a column, and text in the key. The state
// lives in the address's fragment, so a filtered view can be linked.
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
  let pattern = {target: '', difference: ''};
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
  pattern = {target: linked.get('target') || '', difference: linked.get('difference') || ''};
  const ownFilter = () => { pattern = {target: '', difference: ''}; apply(); };
  column.addEventListener('change', ownFilter);
  key.addEventListener('input', ownFilter);
  clear.addEventListener('click', () => { column.value = ''; key.value = ''; ownFilter(); });
  const panel = root.closest('section') || document;
  panel.querySelectorAll('li.pattern').forEach(item => {
    const show = document.createElement('button');
    show.type = 'button';
    show.className = 'show-pattern';
    show.textContent = 'Show these values';
    // The name starts with the visible words, for voice control, and the
    // sentence's first clause tells one button from the next.
    show.setAttribute('aria-label', `Show these values: ${item.textContent.split('. ')[0]}`);
    show.addEventListener('click', () => {
      column.value = item.dataset.column;
      key.value = '';
      pattern = {target: item.dataset.target || '', difference: item.dataset.difference || ''};
      apply();
    });
    item.append(' ', show);
  });
  apply();
}
document.querySelectorAll('[data-filterable]').forEach(filters);

// One tab per kind of row. Arrow keys, Home, and End move between tabs, and
// only the selected tab takes Tab focus, as the ARIA tabs pattern asks.
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
    button.textContent = `${panel.dataset.kind} \u00b7 ${count}`;
    panel.setAttribute('role', 'tabpanel');
    panel.setAttribute('aria-labelledby', button.id);
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


def _table(title: str, frame: pl.DataFrame, max_rows: int) -> str:
    """Render one table section under its heading, with every embedded row in the markup."""
    heading_id = _slug(title)
    heading = f"<h2 id='{heading_id}'>{_escape(title)}</h2>"
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


def _patterns_html(listed: Sequence[_MismatchedValue], total: int) -> str:
    """List the patterns among the listed values, each tagged with the filter that shows it.

    Without the script, a pattern is a sentence. The script adds a button that
    filters the mismatched values to it.
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


def _mismatch_view(changed: pl.DataFrame, result: DiffResult, max_rows: int) -> str:
    """List each mismatched value of the changed rows, the report's default view of them.

    `max_rows` caps the values listed, not rows times columns, and the view says
    when it stops short of every mismatch the run counted. Each row carries its
    column and key as data, which the script's filters read.
    """
    keys = list(result.primary_keys)
    ordered = changed.sort(keys) if keys else changed
    listed = list(
        islice(_mismatched_values(ordered.head(max_rows), keys, result.compared_columns), max_rows)
    )
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


def _changed_section(
    changed: pl.DataFrame, result: DiffResult, max_rows: int, *, has_values: bool
) -> str:
    """Render the changed rows: mismatched values first, and the full rows one click away.

    A pushdown run without a sample holds keys alone, so it keeps the one table.
    """
    if not has_values or changed.height == 0:
        return _table("Changed rows", changed, max_rows)
    full_rows = _table_body("changed-rows", "Changed rows", changed, max_rows)
    return (
        "<h2 id='changed-rows'>Changed rows</h2>\n"
        f"{_mismatch_view(changed, result, max_rows)}\n"
        "<details class='full-rows'><summary>Show the full changed rows, every column side by "
        f"side ({changed.height:,} rows)</summary>\n{full_rows}\n</details>"
    )


_ABSENT: Final = "<span class='absent'>None recorded</span>"
"""What the run record shows for a field it has no value for."""

_TREE_STATES: Final = {
    True: "tracked files match this commit",
    False: "tracked files differ from this commit",
    None: "whether tracked files match is unknown",
}
"""The words for `working_tree_clean`, so the state never rests on a symbol."""


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


def _card(label: str, value: object, bar: str = "") -> str:
    """Render one headline metric, with a bar under it when one is given."""
    return (
        f"<div class='card'><div class='label'>{_escape(label)}</div>"
        f"<div class='value'>{_escape(value)}</div>{bar}</div>"
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


def _headline(summary: DiffSummary) -> str:
    """Say in one sentence how much differs, so the verdict needs no table to read it."""
    if summary.is_perfect_match:
        return "Every row matches."
    rows = _counted(summary.total_mismatches, "row differs", "rows differ")
    if summary.is_match:
        return f"{rows}, within the threshold"
    parts = [rows]
    if summary.column_mismatches:
        drifting = len(summary.column_mismatches)
        parts.append(_counted(drifting, "column drifts", "columns drift"))
    if summary.accepted_count:
        parts.append(f"{summary.accepted_count:,} accepted by the baseline")
    return " &middot; ".join(parts)


def _banner(summary: DiffSummary, generated: str) -> str:
    """Render the verdict as the page's largest text, a word beside a glyph, and its headline."""
    verdict = "PASSED" if summary.is_match else "FAILED"
    state = "pass" if summary.is_match else "fail"
    glyph = "&#10003;" if summary.is_match else "&#10005;"
    return (
        f'<header class="banner {state}">'
        f'<span class="glyph" aria-hidden="true">{glyph}</span>'
        f'<div><p class="verdict {state}">{verdict}</p>'
        f'<p class="headline">{_headline(summary)}</p>'
        f'<p class="sub">Generated {generated}</p></div></header>'
    )


def _drift_table(summary: DiffSummary) -> str:
    """Rank the drifting columns, each with its share of the changed rows in words and as a bar."""
    if not summary.column_mismatches:
        return "<p class='empty'>No column-level drift.</p>"
    ranked = sorted(summary.column_mismatches.items(), key=lambda item: -item[1])
    changed_rows = max(summary.changed_count, 1)
    rows = "".join(
        f"<tr><td>{_escape(column)}</td><td>{count:,}</td>"
        f"<td class='share'>{_bar(round(100 * count / changed_rows, 1))}"
        f"{round(100 * count / changed_rows):d}%</td></tr>"
        for column, count in ranked
    )
    return _scroll_region(
        "column-level-drift",
        "<table><thead><tr><th scope='col'>Column</th><th scope='col'>Mismatches</th>"
        "<th scope='col'>Share of changed rows</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>",
    )


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

    cards = "".join(
        [
            _card(
                "Match rate",
                f"{summary.match_rate_percentage}%",
                _bar(summary.match_rate_percentage),
            ),
            _card("Source rows", f"{summary.total_rows_source:,}"),
            _card("Target rows", f"{summary.total_rows_target:,}"),
            _card("Added", f"{summary.added_count:,}"),
            _card("Removed", f"{summary.removed_count:,}"),
            _card("Changed", f"{summary.changed_count:,}"),
            *([_card("Accepted", f"{summary.accepted_count:,}")] if summary.accepted_count else []),
        ]
    )

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

    # Without the script, the three sections stay stacked with every row shown.
    kinds = "\n".join(
        [
            _kind_section(
                "Changed rows",
                summary.changed_count,
                _changed_section(
                    changed,
                    result,
                    max_rows,
                    has_values=not result.keys_only or result.changed_sample is not None,
                ),
            ),
            _kind_section(
                "Added rows", summary.added_count, _table("Added rows", result.added, max_rows)
            ),
            _kind_section(
                "Removed rows",
                summary.removed_count,
                _table("Removed rows", result.removed, max_rows),
            ),
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
<main>
<h1>Veridelta Report</h1>
{_banner(summary, generated)}
{keys_note}
{_run_record_html(summary.provenance)}
<div class="cards">{cards}</div>
<h2 id="column-level-drift">Column-level drift</h2>
{_drift_table(summary)}
<div class="kinds">
{kinds}
</div>
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
