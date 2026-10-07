# Copyright 2026 The Veridelta Contributors
# SPDX-License-Identifier: Apache-2.0

"""Hold the report screenshots and the link preview card to what uses them.

`make screenshots` writes the images with a browser, so these tests read only
what it wrote: the card says what PyPI says, every image has the size its use
takes, and the docs site and the results page point at them.
"""

import re
import struct
import tomllib
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.fast]

_ROOT = Path(__file__).resolve().parents[2]
_ASSETS = _ROOT / "docs" / "assets"


def _size(png: Path) -> tuple[int, int]:
    """Return a PNG's width and height, from its header."""
    data = png.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", f"{png.name} is not a PNG."
    width, height = struct.unpack(">II", data[16:24])
    return width, height


def test_the_card_says_what_pypi_says() -> None:
    """Ensure the link preview card shows the summary PyPI shows, word for word."""
    with (_ROOT / "pyproject.toml").open("rb") as file:
        summary = tomllib.load(file)["project"]["description"]
    card = (_ROOT / "demo" / "social-card.html").read_text(encoding="utf-8")

    assert f"<p>{summary}</p>" in card


def test_the_card_has_the_size_link_previews_take() -> None:
    """Ensure the card is 1280 by 640, the 2:1 image GitHub and social sites show whole."""
    assert _size(_ASSETS / "social-card.png") == (1280, 640)


def test_every_page_names_the_card_in_its_link_preview() -> None:
    """Ensure the docs site's link preview tags name the card, through the theme override."""
    template = (_ROOT / "overrides" / "main.html").read_text(encoding="utf-8")
    mkdocs = (_ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert re.search(r"^  custom_dir: overrides$", mkdocs, re.MULTILINE)
    assert 'property="og:image" content="{{ config.site_url }}assets/social-card.png"' in template
    assert 'property="og:description" content="{{ config.site_description }}"' in template
    assert 'name="twitter:card" content="summary_large_image"' in template


@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_the_results_page_shows_the_report_in_each_scheme(scheme: str) -> None:
    """Ensure the results page shows the screenshot that matches the reader's color scheme."""
    page = (_ROOT / "docs" / "results.md").read_text(encoding="utf-8")

    assert f"](assets/report-{scheme}.png#only-{scheme})" in page
    assert _size(_ASSETS / f"report-{scheme}.png") == (1280, 800)
