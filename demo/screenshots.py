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

With `--promo`, it writes one image for promotional video instead, which git
ignores: `demo/video/promo-report.png`, 1920 by 1080, the report of the run
`demo/promo/accounts-baseline.tape` types, on the accounts in `demo/`.
`make demo-video` runs it so.
"""

import argparse
import contextlib
import sys
import tempfile
from pathlib import Path

import polars as pl
from playwright.sync_api import sync_playwright

from veridelta.config import load_config
from veridelta.engine import DiffEngine
from veridelta.models import Baseline, DiffConfig, DiffResult, DiffRule
from veridelta.report import write_html

_ROOT = Path(__file__).resolve().parents[1]
_DEMO = _ROOT / "demo"
_ASSETS = _ROOT / "docs" / "assets"
_VIDEO = _DEMO / "video"
_CARD = _DEMO / "social-card.html"
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


def _accounts() -> DiffResult:
    """Run the comparison `demo/promo/accounts-baseline.tape` types: four rules and a baseline.

    The configuration names its files relative to `demo/`, where the tape runs.
    """
    with contextlib.chdir(_DEMO):
        diff, source, target = load_config("accounts_rules.yaml")
        baseline = Baseline.read("accepted.json")
        return DiffEngine.run_from_configs(diff, source, target, baseline=baseline)


def _promo() -> int:
    """Write the report of the accounts run, 1920 by 1080, for promotional video.

    Returns:
        int: 0 once the image is written.
    """
    _VIDEO.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as scratch, sync_playwright() as playwright:
        report = write_html(_accounts(), Path(scratch) / "report.html")
        browser = playwright.chromium.launch()
        try:
            # 1280 by 720 at 1.5 times, so the report lays out as on a laptop and stays sharp.
            page = browser.new_page(
                viewport={"width": 1280, "height": 720},
                device_scale_factor=1.5,
                color_scheme="light",
            )
            page.goto(report.as_uri())
            page.screenshot(path=_VIDEO / "promo-report.png")
        finally:
            browser.close()
    print("Wrote demo/video/promo-report.png")
    return 0


def main() -> int:
    """Write the screenshots and the card, or with `--promo`, the image for video.

    Returns:
        int: 0 once every image is written.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--promo", action="store_true", help="Write demo/video/promo-report.png alone."
    )
    if parser.parse_args().promo:
        return _promo()
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
