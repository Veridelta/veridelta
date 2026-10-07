# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Capture the HTML report in light and dark, and render the link preview card.

`make screenshots` runs this with the `accessibility` group, whose Playwright
drives Chromium. It runs a comparison of its own, so every number in an image
comes from a real run, and writes three files under `docs/assets/`:

- `report-light.png` and `report-dark.png`, the top of the HTML report;
- `social-card.png`, 1280 by 640, the canonical sentence beside the report,
  from `demo/social-card.html`, for link previews and the repository's social
  preview.
"""

import sys
import tempfile
from pathlib import Path

import polars as pl
from playwright.sync_api import sync_playwright

from veridelta.engine import DiffEngine
from veridelta.models import DiffConfig, DiffResult, DiffRule
from veridelta.report import write_html

_ROOT = Path(__file__).resolve().parents[1]
_ASSETS = _ROOT / "docs" / "assets"
_CARD = Path(__file__).resolve().parent / "social-card.html"
_VIEWPORT = {"width": 1280, "height": 800}


def _orders() -> DiffResult:
    """Compare two exports of 120 orders, with drift a migration review would find."""
    statuses = ["open", "shipped", "delivered", "returned"]
    ids = range(1001, 1121)
    source = pl.DataFrame(
        {
            "order_id": list(ids),
            "status": [statuses[i % 4] for i in ids],
            "amount": [round(10 + i * 37 % 500 + 0.99, 2) for i in ids],
            "currency": ["USD" if i % 5 else "EUR" for i in ids],
        }
    )
    kept = source.filter(~pl.col("order_id").is_in([1017, 1088]))
    added = pl.DataFrame(
        {
            "order_id": [1121, 1122, 1123],
            "status": ["open", "open", "shipped"],
            "amount": [42.99, 17.5, 230.0],
            "currency": ["USD", "USD", "EUR"],
        }
    )
    target = pl.concat([kept, added]).with_columns(
        status=pl.when(pl.col("order_id") % 17 == 0)
        .then(pl.lit("cancelled"))
        .otherwise(pl.col("status")),
        amount=pl.when(pl.col("order_id") % 29 == 0)
        .then(pl.col("amount") + 1.0)
        .otherwise(pl.col("amount") + 0.004),
    )
    config = DiffConfig(
        primary_keys=["order_id"],
        rules=[DiffRule(column_names=["amount"], absolute_tolerance=0.01)],
    )
    return DiffEngine(config, source.lazy(), target.lazy()).run()


def main() -> int:
    """Write the screenshots and the card.

    Returns:
        int: 0 once every image is written.
    """
    with tempfile.TemporaryDirectory() as scratch, sync_playwright() as playwright:
        report = write_html(_orders(), Path(scratch) / "report.html")
        browser = playwright.chromium.launch()
        try:
            for scheme in ("light", "dark"):
                page = browser.new_page(viewport=_VIEWPORT, color_scheme=scheme)
                page.goto(report.as_uri())
                page.screenshot(path=_ASSETS / f"report-{scheme}.png")
                page.close()
            card = browser.new_page(viewport={"width": 1280, "height": 640})
            card.goto(_CARD.as_uri())
            card.screenshot(path=_ASSETS / "social-card.png")
        finally:
            browser.close()
    for name in ("report-light.png", "report-dark.png", "social-card.png"):
        print(f"Wrote docs/assets/{name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
