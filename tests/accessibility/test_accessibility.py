# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Accessibility checks on the documentation site and the HTML report, in Chromium.

axe-core checks every page the docs build writes, and a sample report, against
the WCAG 2.2 A and AA rules and its own best practices. Each page runs in light
and dark mode, at desktop and phone widths. Only the local server answers, so a
font or a diagram from a CDN never changes a result. Then a keyboard drives the
report's pager, and the report is read with JavaScript off.

These tests need a browser, so only `make accessibility` and the Accessibility
CI job run them. Every other run skips them.
"""

import base64
import functools
import hashlib
import http.server
import io
import os
import subprocess
import sys
import tarfile
import threading
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffResult
from veridelta.report import write_html

if TYPE_CHECKING:
    from playwright.sync_api import Browser, Page

pytestmark = [pytest.mark.integration, pytest.mark.slow]

if os.environ.get("VERIDELTA_ACCESSIBILITY") != "1":
    pytest.skip("These checks need a browser; run `make accessibility`.", allow_module_level=True)

_ROOT = Path(__file__).resolve().parents[2]

_AXE_CORE = "https://registry.npmjs.org/axe-core/-/axe-core-4.14.0.tgz"
_AXE_CORE_INTEGRITY = "sha512-9WTZxEjsZ7b13TH8JPmbV2z8CHbl80/2hm3XPEG4JgNdQLK81IBRXmSxHfMAOkSqQeRxT/0dwNDz2GOm3zzpcQ=="
"""axe-core's npm release, and the registry's `dist.integrity` for it.

Dependabot cannot bump a download, so `CONTRIBUTING.md` says how to.
"""

_AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa", "best-practice"]
"""The rules axe-core runs: WCAG 2.2 Level A and AA, and its best practices."""

_RUN_AXE = """tags => axe.run(document, {runOnly: {type: 'tag', values: tags}}).then(result =>
  result.violations.map(v => `${v.id} (${v.impact}) at ${v.nodes.slice(0, 3).map(n => n.target.join(' ')).join(', ')}`))"""
"""Run axe-core on the page, and describe each violation in one line."""

_WIDTHS = {"desktop": {"width": 1280, "height": 800}, "phone": {"width": 375, "height": 812}}
"""Viewport sizes. A narrow screen makes tables and code scroll sideways."""

_PAGE_STATUS = "[data-table] .status"
"""The live status of the first paged table, the report's changed rows."""


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    """Serve files without printing a line per request."""

    def log_message(self, format: str, *args: object) -> None:
        """Keep request lines out of the test output."""


def _sample_result() -> DiffResult:
    """Compare frames whose changed rows fill three pages, in a table wider than any screen.

    Returns:
        DiffResult: 55 changed rows, 5 added, and 5 removed.
    """
    rows = 60
    wide = {
        f"column_with_a_long_name_{i}": [f"value {j} in column {i}" for j in range(rows)]
        for i in range(12)
    }
    source = pl.DataFrame(
        {"id": list(range(rows)), "amount": [float(j) for j in range(rows)], **wide}
    )
    target = pl.DataFrame(
        {
            "id": list(range(5, rows + 5)),
            "amount": [float(j) + j % 2 for j in range(5, rows + 5)],
            **{
                name: [
                    f"{value} changed" if j % 3 == 0 else value for j, value in enumerate(values)
                ]
                for name, values in wide.items()
            },
        }
    )
    return DiffEngine(DiffConfig(primary_keys=["id"]), source.lazy(), target.lazy()).run()


@pytest.fixture(scope="session")
def axe_source() -> str:
    """Download axe-core, check it against its pinned digest, and return its script."""
    with urllib.request.urlopen(_AXE_CORE, timeout=60) as response:
        archive: bytes = response.read()
    digest = "sha512-" + base64.b64encode(hashlib.sha512(archive).digest()).decode("ascii")
    assert digest == _AXE_CORE_INTEGRITY, "The axe-core download does not match its pinned digest."
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        script = bundle.extractfile("package/axe.min.js")
        assert script is not None
        return script.read().decode("utf-8")


@pytest.fixture(scope="session")
def site(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build the docs site under `veridelta/`, as GitHub Pages serves it, with a sample report.

    The 404 page loads its styles by that path, so the folder name matters.
    """
    root = tmp_path_factory.mktemp("served")
    built = root / "veridelta"
    subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "--quiet", "--site-dir", str(built)],
        cwd=_ROOT,
        check=True,
    )
    write_html(_sample_result(), built / "report.html")
    return built


@pytest.fixture(scope="session")
def base_url(site: Path) -> Iterator[str]:
    """Serve the built site on a free local port until the session ends."""
    handler = functools.partial(_QuietHandler, directory=str(site.parent))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/veridelta/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="session")
def browser(base_url: str) -> Iterator["Browser"]:
    """Launch Chromium once. It depends on the server, so it closes first."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        chromium = playwright.chromium.launch()
        try:
            yield chromium
        finally:
            chromium.close()


def _page_paths(site: Path) -> list[str]:
    """List every page the docs build wrote, then the 404 page and the report.

    Args:
        site (Path): The built site.

    Returns:
        list[str]: Each page's path below the site's root URL.
    """
    folders = sorted(
        index.parent.relative_to(site).as_posix() for index in site.rglob("index.html")
    )
    return [
        *("" if folder == "." else f"{folder}/" for folder in folders),
        "404.html",
        "report.html",
    ]


def _open_local_only(browser: "Browser", base_url: str, **options: Any) -> "Page":
    """Open a page whose requests reach only the local server.

    A font or a diagram fetched from a CDN could change layout, and so a result.
    """
    context = browser.new_context(**options)
    origin = base_url.split("/veridelta/", 1)[0]
    context.route(
        "**/*",
        lambda route: route.continue_() if route.request.url.startswith(origin) else route.abort(),
    )
    return context.new_page()


class TestAxe:
    """Hold every page and the report to zero axe-core violations."""

    def test_it_finds_every_page(self, site: Path) -> None:
        """Ensure a moved folder cannot empty the check below."""
        paths = _page_paths(site)

        assert "" in paths
        assert "api/" in paths
        assert any(path.startswith("examples/") for path in paths)

    @pytest.mark.parametrize("width", sorted(_WIDTHS))
    @pytest.mark.parametrize("scheme", ["light", "dark"])
    def test_no_page_has_a_violation(
        self,
        browser: "Browser",
        base_url: str,
        site: Path,
        axe_source: str,
        scheme: str,
        width: str,
    ) -> None:
        """Ensure no page fails a WCAG 2.2 A or AA rule, or one of axe-core's best practices."""
        page = _open_local_only(browser, base_url, color_scheme=scheme, viewport=_WIDTHS[width])
        found: list[str] = []
        for path in _page_paths(site):
            page.goto(base_url + path, wait_until="networkidle")
            page.add_script_tag(content=axe_source)
            found += [f"/{path}: {line}" for line in page.evaluate(_RUN_AXE, _AXE_TAGS)]
        page.context.close()

        assert not found, "axe-core found:\n" + "\n".join(found)


class TestReport:
    """Drive the HTML report as a keyboard user, and read it with JavaScript off."""

    def test_it_shows_every_row_without_javascript(self, browser: "Browser", base_url: str) -> None:
        """Ensure a reader without JavaScript sees all 65 rows, and no pager that does nothing."""
        page = _open_local_only(browser, base_url, java_script_enabled=False)
        page.goto(base_url + "report.html")

        shown = page.evaluate(
            """() => ({
              rows: [...document.querySelectorAll('[data-table] tbody tr')]
                .filter(row => row.getBoundingClientRect().height > 0).length,
              pagers: [...document.querySelectorAll('.pager')]
                .filter(pager => pager.getBoundingClientRect().height > 0).length,
            })"""
        )
        page.context.close()

        assert shown == {"rows": 65, "pagers": 0}

    def test_a_keyboard_pages_a_table_and_hears_each_page(
        self, browser: "Browser", base_url: str
    ) -> None:
        """Ensure Tab reaches Next, Enter pages, the status speaks once per page, and focus stays.

        At the last page, Next is marked disabled but keeps focus, so the
        keyboard is never dropped back to the top of the page.
        """
        page = _open_local_only(browser, base_url)
        page.goto(base_url + "report.html")
        assert page.text_content(_PAGE_STATUS) == "Page 1 of 3 · 55 rows"
        page.evaluate(
            """selector => {
              window.statusChanges = [];
              const status = document.querySelector(selector);
              new MutationObserver(() => window.statusChanges.push(status.textContent))
                .observe(status, {childList: true, characterData: true, subtree: true});
            }""",
            _PAGE_STATUS,
        )

        names: list[str | None] = []
        while len(names) < 20 and "Next page of changed rows" not in names:
            page.keyboard.press("Tab")
            names.append(page.evaluate("() => document.activeElement.getAttribute('aria-label')"))
        for _ in range(3):
            page.keyboard.press("Enter")
        after = page.evaluate(
            """() => ({
              focused: document.activeElement.getAttribute('aria-label'),
              disabled: document.activeElement.getAttribute('aria-disabled'),
              ring: getComputedStyle(document.activeElement).outlineStyle,
              shown: [...document.querySelectorAll('[data-table] tbody tr')]
                .slice(0, 55).map((row, index) => row.hidden ? null : index).filter(i => i !== null),
              changes: window.statusChanges,
            })"""
        )
        page.context.close()

        assert names[-1] == "Next page of changed rows", names
        assert after["focused"] == "Next page of changed rows"
        assert after["disabled"] == "true"
        assert after["ring"] != "none"
        assert after["shown"] == list(range(50, 55))
        assert after["changes"] == ["Page 2 of 3 · 55 rows", "Page 3 of 3 · 55 rows"]

    def test_arrow_keys_scroll_a_wide_table(self, browser: "Browser", base_url: str) -> None:
        """Ensure a table wider than the screen takes focus, so arrow keys can scroll it."""
        page = _open_local_only(browser, base_url)
        page.goto(base_url + "report.html")
        region = "[aria-labelledby='changed-rows']"

        page.focus(region)
        page.keyboard.press("ArrowRight")
        page.wait_for_function(
            "selector => document.querySelector(selector).scrollLeft > 0", arg=region
        )
        page.context.close()
